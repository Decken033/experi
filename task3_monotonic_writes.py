#!/usr/bin/env python3
"""
task3_monotonic_writes.py

Client-centric consistency model: MONOTONIC-WRITES (MW).

Definition
----------
Two writes by the same client are serialised in the order the client issued
them: write W1 must be completed (visible everywhere W2 is visible) before W2
takes effect. On any replica, if W2 is visible then W1 must also be visible.

How we test it on MongoDB
-------------------------
A replica set has a SINGLE primary and replicates through an ordered oplog.
Secondaries apply oplog entries strictly in order, so if a secondary has
applied entry #k it has necessarily applied every entry < k. This strongly
suggests MW should hold even with w:1.

We stress-test that claim:
  * The client issues an ordered sequence of writes W0, W1, ..., W(N-1),
    each to its own document doc_i (with seq = i), all from one client.
  * After each write, we read the whole sequence back from a SECONDARY and
    look for a "hole": a document doc_i that is visible while some earlier
    doc_j (j < i) is NOT visible. A hole would mean writes became visible out
    of order -> a monotonic-writes violation.

Config A (weak): writeConcern=w:1, readConcern=local, readPreference=secondary.
Config B (strong): causal session + majority (also expected to hold).

Prediction: MW holds in BOTH configs, because a single primary orders the
writes and secondaries replay them in that order. The experiment is designed
to confirm the prediction (and would catch a violation if one occurred, e.g.
around a failover with rollback).
"""

import uuid

from pymongo import ReadPreference
from pymongo import errors as mongo_errors
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


COLLECTION_NAME = "mw_test"

# Number of ordered writes in the sequence.
N = 150


def first_hole(seqs_present, up_to, known_failed=frozenset()):
    """
    Given the set of seq values currently visible on the secondary and the
    highest seq we have written so far (up_to), return the (present, missing)
    pair describing the first out-of-order gap, or None if writes are in order.

    A violation looks like: some seq i is present but an earlier seq j < i is
    missing.

    `known_failed` is the set of seq values whose WRITE never succeeded (the
    client got an error/timeout back for it, e.g. during a primary failover).
    Those are excluded from the "must be visible" check: a doc that was never
    durably written is not a monotonic-writes violation when it is absent,
    it is simply absent.
    """
    highest_present = -1
    for s in seqs_present:
        highest_present = max(highest_present, s)

    if highest_present < 0:
        return None

    # Every seq strictly below the highest visible one must also be visible,
    # UNLESS its write never actually succeeded.
    for j in range(highest_present):
        if j not in seqs_present and j not in known_failed:
            return (highest_present, j)
    return None


def run_config(client, db, causal, logger=None, scenario_label="unknown"):
    config_tag = "B" if causal else "A"
    label = "CONFIG B: causal + majority" if causal else "CONFIG A: w:1 + local"
    print("=" * 60)
    print(f"TASK 3 / {label}")
    print("readPreference=secondary")
    print("=" * 60)

    if causal:
        coll = db.get_collection(
            COLLECTION_NAME,
            read_preference=ReadPreference.SECONDARY,
            read_concern=ReadConcern("majority"),
            write_concern=WriteConcern(w="majority", j=True, wtimeout=10000),
        )
    else:
        coll = db.get_collection(
            COLLECTION_NAME,
            read_preference=ReadPreference.SECONDARY,
            read_concern=ReadConcern("local"),
            write_concern=WriteConcern(w=1),
        )

    run_id = str(uuid.uuid4())
    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    failed_writes = set()

    session_cm = (
        client.start_session(causal_consistency=True)
        if causal else _NullSession()
    )

    with session_cm as session:
        for i in range(N):
            doc_id = f"{run_id}-{i}"

            # Ordered write W_i issued by this single client.
            try:
                coll.insert_one(
                    {"_id": doc_id, "run_id": run_id, "seq": i},
                    session=session,
                )
            except mongo_errors.PyMongoError as exc:
                # Expected during a primary failure / network partition: the
                # primary is briefly unreachable or mid-election. The write
                # never happened, so seq=i must NOT be expected to appear in
                # the hole check below - that would be an availability
                # outcome, not a monotonic-writes violation.
                kind = common.classify_error(exc)
                counters["availability_losses" if kind == "availability_loss"
                         else "errors"] += 1
                failed_writes.add(i)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d} write failed: {type(exc).__name__}: {exc}")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label,
                                config=config_tag, run_id=run_id, i=i,
                                stage="write", outcome=kind)
                continue

            # Read back everything for this run from a secondary and look for a
            # gap that would indicate writes becoming visible out of order.
            try:
                docs = coll.find({"run_id": run_id}, session=session, max_time_ms=10000)
                seqs_present = {d["seq"] for d in docs}
            except mongo_errors.PyMongoError as exc:
                kind = common.classify_error(exc)
                counters["availability_losses" if kind == "availability_loss"
                         else "errors"] += 1
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d} read-back failed: {type(exc).__name__}: {exc}")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label,
                                config=config_tag, run_id=run_id, i=i,
                                stage="read", outcome=kind)
                continue

            hole = first_hole(seqs_present, i, known_failed=failed_writes)
            if hole is not None:
                counters["violations"] += 1
                present, missing = hole
                print(f"[MW VIOLATION] i={i:03d} seq {present} visible but "
                      f"earlier seq {missing} missing")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label,
                                config=config_tag, run_id=run_id, i=i,
                                outcome="violation", present=present, missing=missing)
            elif logger:
                logger.log(task="task3_mw", scenario=scenario_label,
                            config=config_tag, run_id=run_id, i=i, outcome="ok")

    effective = N - counters["availability_losses"] - counters["errors"]
    print()
    print(f"Ordered writes      : {N}")
    print(f"Availability losses : {counters['availability_losses']} "
          f"(primary/secondary unreachable or timed out - expected during "
          f"failure/partition scenarios, NOT counted as a consistency violation)")
    print(f"Other errors        : {counters['errors']}")
    print(f"Effective writes    : {effective}")
    print(f"Violations          : {counters['violations']} "
          f"({(counters['violations'] / effective * 100) if effective else 0.0:.1f}% "
          f"of effective writes)")
    if counters["violations"] == 0 and effective == 0:
        print("RESULT              : no effective iterations completed (node "
              "unavailable for the whole run) - an availability outcome, not "
              "evidence either way about MW")
    else:
        print("RESULT              :",
              "PASS - monotonic writes maintained" if counters["violations"] == 0
              else "FAIL - MW violation detected")
    print()
    return run_id, {
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "effective": effective,
        "iterations": N,
    }


class _NullSession:
    """A no-op context manager so config A can share the same code path."""
    def __enter__(self):
        return None

    def __exit__(self, *args):
        return False


def print_scenario_summary(scenario_results):
    print()
    print("=" * 60)
    print("CROSS-SCENARIO SUMMARY (Task 3: Monotonic Writes)")
    print("=" * 60)
    header = (f"{'Scenario':<38}{'A viol':<8}{'A avail-loss':<14}"
              f"{'B viol':<8}{'B avail-loss':<14}")
    print(header)
    print("-" * len(header))
    for label, result in scenario_results.items():
        if not result:
            print(f"{label:<38}{'n/a':<8}{'n/a':<14}{'n/a':<8}{'n/a':<14}")
            continue
        stats_a = result["config_a"]
        stats_b = result["config_b"]
        print(f"{label:<38}{stats_a['violations']:<8}{stats_a['availability_losses']:<14}"
              f"{stats_b['violations']:<8}{stats_b['availability_losses']:<14}")
    print()


def main():
    client = common.get_client()
    common.print_topology(client)
    common.print_replication_lag(client)

    db = client[common.DB_NAME]
    logger = common.TrialLogger("task3_mw")
    all_run_ids = []

    def run_experiment(scenario_label="unknown"):
        run_id_a, stats_a = run_config(client, db, causal=False,
                                        logger=logger, scenario_label=scenario_label)
        all_run_ids.append(run_id_a)
        run_id_b, stats_b = run_config(client, db, causal=True,
                                        logger=logger, scenario_label=scenario_label)
        all_run_ids.append(run_id_b)
        return {"config_a": stats_a, "config_b": stats_b}

    try:
        # Requirements.md asks for experiments under several scenarios:
        # normal operation, node failure, and a network partition.
        scenario_results = scenarios.run_under_scenarios(
            client, run_experiment, logger=logger
        )
        print_scenario_summary(scenario_results)
    finally:
        common.cleanup(client, COLLECTION_NAME, all_run_ids)
        logger.close()
        client.close()


if __name__ == "__main__":
    main()
