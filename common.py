#!/usr/bin/env python3
"""
common.py

Shared helpers for the client-centric consistency experiments against a
MongoDB replica set that is deployed across three physical machines
(connected through a phone hotspot LAN) plus one client machine.

All four experiment scripts (task1..task4) import from this module so that
the connection string, database/collection names and a few utility functions
live in exactly one place.

Edit NODES below to match the actual IP addresses of your three servers.
"""

import datetime
import json
import os
import time

from pymongo import MongoClient
from pymongo import errors as mongo_errors
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern


# ============================================================
# Cluster configuration
# ============================================================

# The three mongod instances that form replica set "rs0".
# These are the LAN IPs handed out by the phone hotspot in our setup.
# Change them to match your own machines.
NODES = [
    "172.20.10.2:27017",
    "172.20.10.3:27017",
    "172.20.10.5:27017",
]

REPLICA_SET = "rs0"

DB_NAME = "consistency_test"


def build_uri():
    """Build a standard replica-set connection URI from the NODES list."""
    hosts = ",".join(NODES)
    return f"mongodb://{hosts}/?replicaSet={REPLICA_SET}"


URI = build_uri()


# ============================================================
# Connection helpers
# ============================================================

def get_client(timeout_ms=5000):
    """
    Create a MongoClient bound to the whole replica set and force an
    initial topology discovery by pinging the admin database.

    Returning the connected client lets each experiment script share the
    exact same connection logic.
    """
    client = MongoClient(URI, serverSelectionTimeoutMS=timeout_ms)

    # Force the driver to discover primary + secondaries before we start.
    client.admin.command("ping")
    return client


def make_pin_selector(target):
    """
    Custom pymongo server_selector that pins SECONDARY reads to one explicit
    (host, port) target while leaving primary selection (writes) untouched.

    Why: with plain readPreference=secondary the driver picks WHICH secondary
    serves each read on its own SDAM heartbeat schedule (~10s), so it can
    stick to a healthy node for a whole run and hide the staleness of the
    node we deliberately delayed / failed / partitioned.

    If the target is currently not selectable (stopped or partitioned) an
    empty candidate list is returned, so the driver raises
    ServerSelectionTimeoutError instead of silently reading another node;
    the experiment scripts record that as an availability loss.
    """

    def selector(server_descriptions):
        # A candidate list containing the primary means this selection is for
        # a write (or a primary read): never interfere with those.
        if any(
            s.server_type == SERVER_TYPE.RSPrimary
            for s in server_descriptions
        ):
            return server_descriptions
        return [s for s in server_descriptions if s.address == target]

    return selector


def get_pinned_client(target, timeout_ms=5000):
    """
    Replica-set client whose secondary reads are pinned to `target`
    ((host, port)). Writes still go to the primary as usual, so a causal
    session on this client works across the write -> read pair.
    """
    return MongoClient(
        URI,
        serverSelectionTimeoutMS=timeout_ms,
        server_selector=make_pin_selector(target),
    )


# ============================================================
# Error classification
# ============================================================

# Errors that mean "the operation could not complete in time / no suitable
# node was available" - i.e. the system gave up AVAILABILITY. Under the
# strong (causal + majority) configurations during a node failure or a
# network partition these are the EXPECTED outcome, and they must never be
# counted as consistency violations.
TIMEOUT_ERRORS = (
    mongo_errors.ServerSelectionTimeoutError,
    mongo_errors.NetworkTimeout,
    mongo_errors.ExecutionTimeout,
    mongo_errors.WTimeoutError,
    mongo_errors.AutoReconnect,
)


def classify_error(exc):
    """Return 'availability_loss' for timeout-like errors, 'error' otherwise."""
    return "availability_loss" if isinstance(exc, TIMEOUT_ERRORS) else "error"


# ============================================================
# Diagnostics
# ============================================================

def print_topology(client):
    """Print which node is primary and which nodes are secondaries."""
    print("Connected to replica set:", REPLICA_SET)
    print("Primary    :", client.primary)
    print("Secondaries:", client.secondaries)
    print()


def print_replication_lag(client):
    """
    Print a rough per-secondary replication lag by reading the replica set
    status from the primary. Useful to correlate observed staleness with the
    actual lag while a node is failing or the network is partitioned.
    """
    try:
        status = client.admin.command("replSetGetStatus")
    except Exception as exc:  # noqa: BLE001 - diagnostics only
        print("Could not read replSetGetStatus:", exc)
        return

    # Find the primary's optime, then report each secondary's delta.
    primary_optime = None
    for member in status.get("members", []):
        if member.get("stateStr") == "PRIMARY":
            primary_optime = member.get("optimeDate")

    print("Replica set member states / lag:")
    for member in status.get("members", []):
        name = member.get("name")
        state = member.get("stateStr")
        optime = member.get("optimeDate")

        if primary_optime and optime and state == "SECONDARY":
            lag = (primary_optime - optime).total_seconds()
            print(f"  {name:<22} {state:<10} lag={lag:.3f}s")
        else:
            print(f"  {name:<22} {state:<10}")
    print()


# ============================================================
# Trial evidence log
# ============================================================

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


class TrialLogger:
    """
    Append-only JSONL log with one line per trial (run id, config, target
    node, latencies, outcome classification) so the report can cite raw
    evidence instead of only console summaries.

    Logging must never break an experiment: every file operation degrades to
    a printed warning.
    """

    def __init__(self, task):
        self.path = None
        self._fh = None
        try:
            os.makedirs(RESULTS_DIR, exist_ok=True)
            fname = f"{task}_{time.strftime('%Y%m%d-%H%M%S')}.jsonl"
            self.path = os.path.join(RESULTS_DIR, fname)
            self._fh = open(self.path, "a", encoding="utf-8")
            print(f"[log] per-trial evidence -> {self.path}")
            print()
        except OSError as exc:
            print(
                f"[log] WARNING: cannot open trial log ({exc}); "
                f"continuing without file evidence."
            )
            print()

    def log(self, **fields):
        if self._fh is None:
            return
        fields.setdefault(
            "ts", datetime.datetime.now(datetime.timezone.utc).isoformat()
        )
        try:
            self._fh.write(json.dumps(fields, default=str) + "\n")
            self._fh.flush()
        except OSError as exc:
            print(f"[log] WARNING: write failed ({exc}); disabling log.")
            self._fh = None

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None


# ============================================================
# Cleanup
# ============================================================

def robust_delete(client, collection_name, filter_doc):
    """
    Best-effort deletion that NEVER raises.

    Tries a durable majority delete first; when the cluster currently has no
    majority (a node-failure / partition scenario still in effect) falls back
    to w=1, and if even that fails just prints a warning - a broken cleanup
    must not swallow the experiment output that precedes it.
    """
    for w in ("majority", 1):
        try:
            coll = client[DB_NAME].get_collection(
                collection_name,
                write_concern=WriteConcern(w=w, wtimeout=5000),
            )
            coll.delete_many(filter_doc)
            return True
        except mongo_errors.PyMongoError as exc:
            print(f"[cleanup] delete with w={w!r} failed: {exc}")

    print(
        f"[cleanup] WARNING: could not delete {filter_doc!r} from "
        f"'{collection_name}'; remove it manually once the cluster is healthy."
    )
    return False


def cleanup(client, collection_name, run_ids):
    """Delete all documents produced by the given run_ids (best effort)."""
    run_ids = [r for r in run_ids if r]
    if run_ids:
        robust_delete(client, collection_name, {"run_id": {"$in": run_ids}})


def timed_ms(func, *args, **kwargs):
    """Run func and return (result, elapsed_milliseconds)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = (time.perf_counter() - start) * 1000.0
    return result, elapsed
