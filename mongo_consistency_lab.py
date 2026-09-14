#!/usr/bin/env python3
"""Small MongoDB client-centric-consistency experiment harness.

The program never creates a partition or stops a node. It only performs scoped
test reads/writes and records JSONL evidence. Fault injection stays a deliberate,
manual action documented in README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bson import json_util
from pymongo import MongoClient, ReadPreference
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import Secondary
from pymongo.write_concern import WriteConcern


DB_NAME = os.getenv("MONGO_DB", "consistency_lab")
TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "10000"))
RESULTS = Path(os.getenv("MONGO_RESULTS", "results.jsonl"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(event: str, **fields: Any) -> dict[str, Any]:
    row = {"time_utc": utc_now(), "event": event, **fields}
    text = json_util.dumps(row, json_options=json_util.RELAXED_JSON_OPTIONS)
    print(text, flush=True)
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS.open("a", encoding="utf-8") as fh:
        fh.write(text + "\n")
    return row


def need_uri(value: str | None = None) -> str:
    uri = value or os.getenv("MONGO_URI")
    if not uri:
        raise SystemExit("Set MONGO_URI or pass --uri")
    return uri


def client(uri: str | None = None) -> MongoClient:
    return MongoClient(
        need_uri(uri),
        serverSelectionTimeoutMS=TIMEOUT_MS,
        connectTimeoutMS=TIMEOUT_MS,
        socketTimeoutMS=max(TIMEOUT_MS, 20000),
        retryWrites=False,
        appName="client-consistency-lab",
    )


def wc(name: str) -> WriteConcern:
    if name == "majority":
        return WriteConcern(w="majority", j=True, wtimeout=TIMEOUT_MS)
    if name == "one":
        return WriteConcern(w=1, j=False, wtimeout=TIMEOUT_MS)
    raise ValueError(name)


def collection(
    c: MongoClient,
    *,
    read: str = "local",
    write: str = "one",
    preference: Any = ReadPreference.PRIMARY,
) -> Collection:
    return c[DB_NAME].get_collection(
        "observations",
        read_concern=ReadConcern(read),
        write_concern=wc(write),
        read_preference=preference,
    )


def hello(c: MongoClient) -> dict[str, Any]:
    h = c.admin.command("hello")
    return {
        "me": h.get("me"),
        "primary": h.get("primary"),
        "setName": h.get("setName"),
        "isWritablePrimary": h.get("isWritablePrimary", False),
        "secondary": h.get("secondary", False),
    }


def guarded(label: str, fn: Any) -> int:
    start = time.monotonic_ns()
    try:
        details = fn()
        emit(label, ok=True, elapsed_ms=(time.monotonic_ns() - start) / 1e6, **details)
        return 0
    except PyMongoError as exc:
        emit(
            label,
            ok=False,
            elapsed_ms=(time.monotonic_ns() - start) / 1e6,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return 2


def cmd_topology(args: argparse.Namespace) -> int:
    uris = args.uri or [u for u in os.getenv("MONGO_NODE_URIS", "").split(";") if u]
    if not uris:
        uris = [need_uri()]
    rc = 0
    for uri in uris:
        rc |= guarded("topology", lambda uri=uri: {"uri_index": uris.index(uri), **hello(client(uri))})
    return rc


def cmd_prepare(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        c = client(args.uri)
        col = collection(c, read="majority", write="majority")
        col.delete_many({"run": args.run})
        col.insert_many(
            [
                {"_id": f"{args.run}:ryw", "run": args.run, "kind": "ryw", "version": 0},
                {"_id": f"{args.run}:mr", "run": args.run, "kind": "mr", "version": 0},
                {"_id": f"{args.run}:source", "run": args.run, "kind": "source", "version": 0},
            ]
        )
        return {"run": args.run, "node": hello(c), "initialized_version": 0}

    return guarded("prepare", work)


def direct_secondary(uri: str) -> MongoClient:
    return MongoClient(
        uri,
        read_preference=ReadPreference.SECONDARY,
        serverSelectionTimeoutMS=TIMEOUT_MS,
        connectTimeoutMS=TIMEOUT_MS,
        socketTimeoutMS=max(TIMEOUT_MS, 20000),
        retryWrites=False,
        appName="client-consistency-lab-direct",
    )


def next_version() -> int:
    return time.time_ns()


def cmd_ryw_weak(args: argparse.Namespace) -> int:
    """Majority write, then non-causal local read from a deliberately stale secondary."""
    def work() -> dict[str, Any]:
        cluster = client(args.uri)
        writer = collection(cluster, read="local", write="majority")
        version = next_version()
        writer.update_one({"_id": f"{args.run}:ryw"}, {"$set": {"version": version}}, upsert=True)

        stale = direct_secondary(args.stale_uri)
        reader = collection(stale, read="local", write="one", preference=ReadPreference.SECONDARY)
        doc = reader.find_one({"_id": f"{args.run}:ryw"}, max_time_ms=TIMEOUT_MS)
        observed = None if doc is None else doc.get("version")
        return {
            "run": args.run,
            "write_version": version,
            "read_version": observed,
            "violation": observed is None or observed < version,
            "write_node": hello(cluster),
            "read_node": hello(stale),
            "configuration": "no session; wc=majority; rc=local; direct secondary",
        }

    return guarded("ryw_weak", work)


def secondary_preference(tag: str | None) -> Any:
    return Secondary(tag_sets=[{"labNode": tag}]) if tag else ReadPreference.SECONDARY


def cmd_ryw_strong(args: argparse.Namespace) -> int:
    """Causal session + majority concerns; stale read must wait/fail, not go backwards."""
    def work() -> dict[str, Any]:
        c = client(args.uri)
        writer = collection(c, read="majority", write="majority")
        reader = collection(
            c,
            read="majority",
            write="majority",
            preference=secondary_preference(args.tag),
        )
        version = next_version()
        with c.start_session(causal_consistency=True) as s:
            writer.update_one(
                {"_id": f"{args.run}:ryw"},
                {"$set": {"version": version}},
                upsert=True,
                session=s,
            )
            doc = reader.find_one(
                {"_id": f"{args.run}:ryw"}, session=s, max_time_ms=TIMEOUT_MS
            )
        observed = None if doc is None else doc.get("version")
        return {
            "run": args.run,
            "write_version": version,
            "read_version": observed,
            "violation": observed is None or observed < version,
            "target_tag": args.tag,
            "configuration": "causal session; wc=majority; rc=majority; secondary read",
        }

    return guarded("ryw_strong", work)


def cmd_mr_weak(args: argparse.Namespace) -> int:
    """Read new state on primary, then old state on a stale secondary."""
    def work() -> dict[str, Any]:
        cluster = client(args.uri)
        writer = collection(cluster, read="local", write="majority")
        version = next_version()
        writer.update_one({"_id": f"{args.run}:mr"}, {"$set": {"version": version}}, upsert=True)
        first_doc = collection(cluster, read="local", write="one").find_one(
            {"_id": f"{args.run}:mr"}, max_time_ms=TIMEOUT_MS
        )

        stale = direct_secondary(args.stale_uri)
        second_doc = collection(
            stale, read="local", write="one", preference=ReadPreference.SECONDARY
        ).find_one({"_id": f"{args.run}:mr"}, max_time_ms=TIMEOUT_MS)
        first = None if first_doc is None else first_doc.get("version")
        second = None if second_doc is None else second_doc.get("version")
        return {
            "run": args.run,
            "first_read": first,
            "second_read": second,
            "violation": first is not None and (second is None or second < first),
            "first_node": hello(cluster),
            "second_node": hello(stale),
            "configuration": "no session; rc=local; primary then direct secondary",
        }

    return guarded("mr_weak", work)


def cmd_mr_strong(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        c = client(args.uri)
        writer = collection(c, read="majority", write="majority")
        primary_reader = collection(c, read="majority", write="majority")
        secondary_reader = collection(
            c,
            read="majority",
            write="majority",
            preference=secondary_preference(args.tag),
        )
        version = next_version()
        with c.start_session(causal_consistency=True) as s:
            writer.update_one(
                {"_id": f"{args.run}:mr"},
                {"$set": {"version": version}},
                upsert=True,
                session=s,
            )
            d1 = primary_reader.find_one(
                {"_id": f"{args.run}:mr"}, session=s, max_time_ms=TIMEOUT_MS
            )
            d2 = secondary_reader.find_one(
                {"_id": f"{args.run}:mr"}, session=s, max_time_ms=TIMEOUT_MS
            )
        first = None if d1 is None else d1.get("version")
        second = None if d2 is None else d2.get("version")
        return {
            "run": args.run,
            "first_read": first,
            "second_read": second,
            "violation": first is not None and (second is None or second < first),
            "target_tag": args.tag,
            "configuration": "causal session; wc=majority; rc=majority; primary then secondary",
        }

    return guarded("mr_strong", work)


def cmd_mw_write(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        c = client(args.uri)
        col = collection(c, read="local", write=args.wc)
        doc_id = f"{args.run}:mw:w{args.step}"
        col.replace_one(
            {"_id": doc_id},
            {"_id": doc_id, "run": args.run, "kind": "mw", "step": args.step},
            upsert=True,
        )
        return {
            "run": args.run,
            "step": args.step,
            "write_concern": args.wc,
            "node": hello(c),
            "acknowledged": True,
        }

    return guarded("mw_write", work)


def cmd_mw_check(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        c = client(args.uri)
        col = collection(c, read="majority", write="majority")
        w1 = col.find_one({"_id": f"{args.run}:mw:w1"}, max_time_ms=TIMEOUT_MS)
        w2 = col.find_one({"_id": f"{args.run}:mw:w2"}, max_time_ms=TIMEOUT_MS)
        return {
            "run": args.run,
            "w1_present": w1 is not None,
            "w2_present": w2 is not None,
            "violation_if_both_writes_were_acknowledged": w1 is None and w2 is not None,
            "node": hello(c),
        }

    return guarded("mw_check", work)


def save_token(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_util.dumps(value), encoding="utf-8")


def load_token(path: Path) -> dict[str, Any]:
    return json_util.loads(path.read_text(encoding="utf-8"))


def cmd_wfr_read(args: argparse.Namespace) -> int:
    """Create/read the source and persist causal metadata for phase 2."""
    def work() -> dict[str, Any]:
        c = client(args.uri)
        col = collection(c, read=args.rc, write=args.wc)
        version = next_version()
        with c.start_session(causal_consistency=args.causal) as s:
            col.update_one(
                {"_id": f"{args.run}:source"},
                {"$set": {"run": args.run, "kind": "source", "version": version}},
                upsert=True,
                session=s,
            )
            doc = col.find_one(
                {"_id": f"{args.run}:source"}, session=s, max_time_ms=TIMEOUT_MS
            )
            token = {
                "run": args.run,
                "source_version": None if doc is None else doc.get("version"),
                "cluster_time": s.cluster_time,
                "operation_time": s.operation_time,
                "causal": args.causal,
            }
        save_token(args.token, token)
        return {
            "run": args.run,
            "source_version": token["source_version"],
            "write_concern": args.wc,
            "read_concern": args.rc,
            "causal": args.causal,
            "token": str(args.token),
            "node": hello(c),
        }

    return guarded("wfr_read", work)


def cmd_wfr_write(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        token = load_token(args.token)
        c = client(args.uri)
        col = collection(c, read="majority", write=args.wc)
        with c.start_session(causal_consistency=args.resume_causal) as s:
            if args.resume_causal:
                if token.get("cluster_time"):
                    s.advance_cluster_time(token["cluster_time"])
                if token.get("operation_time"):
                    s.advance_operation_time(token["operation_time"])
            doc_id = f"{token['run']}:derived"
            col.replace_one(
                {"_id": doc_id},
                {
                    "_id": doc_id,
                    "run": token["run"],
                    "kind": "derived",
                    "source_version": token["source_version"],
                },
                upsert=True,
                session=s,
            )
        return {
            "run": token["run"],
            "source_version_used": token["source_version"],
            "write_concern": args.wc,
            "resumed_causal_metadata": args.resume_causal,
            "node": hello(c),
        }

    return guarded("wfr_write", work)


def cmd_wfr_check(args: argparse.Namespace) -> int:
    def work() -> dict[str, Any]:
        c = client(args.uri)
        col = collection(c, read="majority", write="majority")
        source = col.find_one({"_id": f"{args.run}:source"}, max_time_ms=TIMEOUT_MS)
        derived = col.find_one({"_id": f"{args.run}:derived"}, max_time_ms=TIMEOUT_MS)
        source_v = None if source is None else source.get("version")
        used_v = None if derived is None else derived.get("source_version")
        violation = derived is not None and (
            used_v is None or source_v is None or source_v < used_v
        )
        return {
            "run": args.run,
            "source_version_now": source_v,
            "derived_source_version": used_v,
            "derived_present": derived is not None,
            "violation": violation,
            "node": hello(c),
        }

    return guarded("wfr_check", work)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    q = sub.add_parser("topology")
    q.add_argument("--uri", action="append", help="URI; repeat for each direct node")
    q.set_defaults(func=cmd_topology)

    q = sub.add_parser("prepare")
    q.add_argument("--run", required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_prepare)

    for name, func in (("ryw-weak", cmd_ryw_weak), ("mr-weak", cmd_mr_weak)):
        q = sub.add_parser(name)
        q.add_argument("--run", required=True)
        q.add_argument("--uri")
        q.add_argument("--stale-uri", required=True)
        q.set_defaults(func=func)

    for name, func in (("ryw-strong", cmd_ryw_strong), ("mr-strong", cmd_mr_strong)):
        q = sub.add_parser(name)
        q.add_argument("--run", required=True)
        q.add_argument("--uri")
        q.add_argument("--tag", help="labNode tag value of the deliberately stale secondary")
        q.set_defaults(func=func)

    q = sub.add_parser("mw-write")
    q.add_argument("--run", required=True)
    q.add_argument("--step", type=int, choices=(1, 2), required=True)
    q.add_argument("--wc", choices=("one", "majority"), required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_mw_write)

    q = sub.add_parser("mw-check")
    q.add_argument("--run", required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_mw_check)

    q = sub.add_parser("wfr-read")
    q.add_argument("--run", required=True)
    q.add_argument("--wc", choices=("one", "majority"), required=True)
    q.add_argument("--rc", choices=("local", "majority"), required=True)
    q.add_argument("--causal", action="store_true")
    q.add_argument("--token", type=Path, required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_wfr_read)

    q = sub.add_parser("wfr-write")
    q.add_argument("--wc", choices=("one", "majority"), default="majority")
    q.add_argument("--resume-causal", action="store_true")
    q.add_argument("--token", type=Path, required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_wfr_write)

    q = sub.add_parser("wfr-check")
    q.add_argument("--run", required=True)
    q.add_argument("--uri")
    q.set_defaults(func=cmd_wfr_check)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
