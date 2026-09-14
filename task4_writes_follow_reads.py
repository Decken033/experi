#!/usr/bin/env python3
"""
task4_writes_follow_reads.py

Client-centric consistency model: WRITES-FOLLOW-READS (WFR).

Definition
----------
A write performed by a client on a data item, following a read of that item by
the same client, is guaranteed to take place on the same or a more recent
version of the value that was read. Informally: if you read version k of x and
then write y based on it, then anyone who sees your write of y must also see
version k (or newer) of x. Effects never appear before their causes.

How we test it on MongoDB
--------------------------
We model the classic "reply must not appear before the post it replies to"
scenario using TWO INDEPENDENT clients/sessions, all within one process
using threads and separate pymongo Session objects:

  * A background writer keeps advancing a shared item x (x.seq = 1,2,3,...)
    on the primary with a majority write.
  * Client B ("reader-then-writer"):
        1. reads x from a secondary            -> observes x.seq = r
        2. writes y with {based_on: r}         (a write that FOLLOWS the read)
  * Client O ("observer", a SEPARATE client/session from B):
        3. reads y from a secondary
        4. reads x from a (possibly different) secondary -> observes x.seq = o
     If y is visible to O with based_on = r but O sees x.seq = o < r, the
     cause (x reaching r) appears AFTER the effect (y) -> WFR violation.

IMPORTANT DESIGN NOTE (why this version uses TWO sessions, not one)
---------------------------------------------------------------------
An earlier version of this experiment ran client B's read/write AND the
observer's reads inside the SAME pymongo causal session. That does not
actually test writes-follow-reads: MongoDB's causal-session guarantee
already keeps every operation on ONE session internally ordered, so of
course a single shared session never showed a violation - the test was
inadvertently checking a much weaker property (basically monotonic-reads /
read-your-writes on a single client) instead of whether causality is
preserved when B's write is observed by an INDEPENDENT client O.

WFR is fundamentally a statement about two different clients. To test it
honestly we now give B and O their own separate `ClientSession` objects
(and, in the "no propagation" config, this also means they do not
automatically share any causal information). We test THREE configurations:

  Config A - WEAK (independent clients, no sessions at all):
      writeConcern=w:1, readConcern=local, readPreference=secondary.
      B and O use no session/causal token whatsoever.
      Expected: WFR can be VIOLATED - y can replicate to the secondary O
      reads from before x's newer version reaches the secondary O reads x
      from.

  Config B - CAUSAL SESSIONS, NOT PROPAGATED (the key control condition):
      writeConcern=majority, readConcern=majority, readPreference=secondary.
      B and O EACH use their own causal session (causal_consistency=True),
      but B's cluster time / operation time is NEVER communicated to O -
      i.e. two independent application processes that each "do the right
      thing" locally but never exchange a causal token with each other.
      Expected: WFR can STILL be violated. This isolates and demonstrates
      that per-session causal consistency does NOT automatically extend
      across independent clients; propagation must be explicit.

  Config C - CAUSAL SESSIONS, EXPLICITLY PROPAGATED (expected to UPHOLD WFR):
      Same write/read concerns as Config B, but immediately after B writes
      y, we take B's session.cluster_time / session.operation_time and hand
      them to O's session via session.advance_cluster_time(...) and
      session.advance_operation_time(...) - exactly what a real distributed
      application would do by forwarding a causal token (e.g. in an HTTP
      header or message payload) from the writer to the next reader.
      Expected: every read of y by O is guaranteed to be accompanied by a
      view of x that is at least as new as what B saw -> WFR holds.

Comparing B vs C in the same run is the core of this experiment: any
difference between them isolates the effect of explicit causal-token
propagation, with the write/read concern held constant.
"""

import threading
import time
import uuid

from pymongo import ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


COLLECTION_NAME = "wfr_test"

ROUNDS = 200
WRITER_SECONDS = 30


class XWriter(threading.Thread):
    """Background thread that advances shared item x.seq on the primary."""

    def __init__(self, db, x_id):
        super().__init__(daemon=True)
        self.x_id = x_id
        # Majority write so x's new version is durable, but replicas can still
        # observe it at different times.
        self.coll = db.get_collection(
            COLLECTION_NAME, write_concern=WriteConcern(w="majority")
        )
        # Do NOT call this self._stop - threading.Thread has an internal
        # private _stop() method, and shadowing it with an Event makes
        # writer.join() crash with "TypeError: 'Event' object is not
        # callable" as soon as the thread is asked to stop (task2's Writer
        # class carries this exact same warning in a comment).
        self._stop_event = threading.Event()
        # Exposed so the report can tell "the writer stalled because the
        # primary was down" apart from "there just weren't any violations".
        self.last_seq = 0
        self.write_count = 0
        self.write_errors = 0

    def run(self):
        seq = 0
        deadline = time.time() + WRITER_SECONDS
        while not self._stop_event.is_set() and time.time() < deadline:
            seq += 1
            try:
                self.coll.update_one(
                    {"_id": self.x_id},
                    {"$set": {"seq": seq}},
                    upsert=True,
                )
            except PyMongoError:
                # During an injected node failure / network partition the
                # primary can be briefly unreachable (e.g. while a new one
                # is being elected). Retry the SAME seq instead of letting
                # the daemon thread die silently, so the writer keeps going
                # once the cluster recovers.
                self.write_errors += 1
                seq -= 1
                time.sleep(0.2)
                continue
            self.last_seq = seq
            self.write_count += 1
            time.sleep(0.01)

    def stop(self):
        self._stop_event.set()


class _NullSession:
    """No-op context manager so the weak config shares the same code path."""
    def __enter__(self):
        return None

    def __exit__(self, *args):
        return False


def _open_session(client, causal):
    """Return a context manager yielding a session (causal) or None (weak)."""
    if causal:
        return client.start_session(causal_consistency=True)
    return _NullSession()


def run_config(client, db, x_id, run_id, mode, logger=None, scenario_label="unknown"):
    """
    mode: one of
        "weak"                 -> Config A
        "causal_no_propagate"  -> Config B
        "causal_propagate"     -> Config C
    """
    labels = {
        "weak": "CONFIG A: no session (independent clients, no causal token)",
        "causal_no_propagate": "CONFIG B: separate causal sessions, "
                                "NOT propagated between B and observer",
        "causal_propagate": "CONFIG C: separate causal sessions, "
                             "cluster/operation time EXPLICITLY propagated",
    }
    config_tag = {"weak": "A", "causal_no_propagate": "B", "causal_propagate": "C"}[mode]
    print("=" * 70)
    print(f"TASK 4 / {labels[mode]}")
    print("readPreference=secondary")
    print("=" * 70)

    causal = mode != "weak"
    read_concern = ReadConcern("majority") if causal else ReadConcern("local")
    write_concern = (
        WriteConcern(w="majority", j=True, wtimeout=10000) if causal else WriteConcern(w=1)
    )

    coll = db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=read_concern,
        write_concern=write_concern,
    )

    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    checked = 0
    rounds_attempted = 0

    def _op_failed(k, stage, exc):
        kind = common.classify_error(exc)
        counters["availability_losses" if kind == "availability_loss"
                 else "errors"] += 1
        tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
        print(f"[{tag}] round={k:03d} stage={stage} raised "
              f"{type(exc).__name__}: {exc}")
        if logger:
            logger.log(task="task4_wfr", scenario=scenario_label, config=config_tag,
                        run_id=run_id, k=k, stage=stage, outcome=kind)

    # B and O ALWAYS get their own, independent session objects (or no
    # session at all in "weak" mode). This is what makes the test honest:
    # any causal guarantee that shows up has to come from something we
    # explicitly did (propagation), not from sharing one session.
    with _open_session(client, causal) as session_b, \
            _open_session(client, causal) as session_o:

        for k in range(ROUNDS):
            rounds_attempted += 1

            # --- Client B: read-then-write ---------------------------------
            # 1) Client B reads x from a secondary.
            try:
                x_doc = coll.find_one({"_id": x_id}, session=session_b, max_time_ms=10000)
            except PyMongoError as exc:
                # Expected during a node-failure / network-partition scenario
                # if the secondary B reads from is down/isolated: an
                # availability outcome, not evidence about WFR.
                _op_failed(k, "read_x_by_b", exc)
                time.sleep(0.005)
                continue
            if not x_doc or "seq" not in x_doc:
                time.sleep(0.005)
                continue
            r = x_doc["seq"]

            # 2) Client B writes y, which FOLLOWS the read of x.
            y_id = f"{run_id}-y-{k}"
            try:
                coll.insert_one(
                    {"_id": y_id, "run_id": run_id, "based_on": r},
                    session=session_b,
                )
            except PyMongoError as exc:
                # Expected if the primary is briefly unreachable during a
                # primary-failure scenario.
                _op_failed(k, "write_y_by_b", exc)
                time.sleep(0.005)
                continue

            # In Config C only: hand B's causal token to the observer's
            # session, simulating an application explicitly forwarding the
            # causal token from the writer to the next reader (e.g. via a
            # message/HTTP header carrying $clusterTime / operationTime).
            if mode == "causal_propagate":
                if session_b.cluster_time is not None:
                    session_o.advance_cluster_time(session_b.cluster_time)
                if session_b.operation_time is not None:
                    session_o.advance_operation_time(session_b.operation_time)

            # --- Client O: independent observer -----------------------------
            # 3) Observer reads y back from a secondary.
            try:
                y_seen = coll.find_one({"_id": y_id}, session=session_o, max_time_ms=10000)
            except PyMongoError as exc:
                _op_failed(k, "read_y_by_observer", exc)
                time.sleep(0.005)
                continue
            if not y_seen:
                # y not yet visible to the observer on the chosen secondary;
                # nothing to check on this round.
                time.sleep(0.005)
                continue

            # 4) Observer reads x from a (possibly different) secondary.
            try:
                x_seen = coll.find_one({"_id": x_id}, session=session_o, max_time_ms=10000)
            except PyMongoError as exc:
                _op_failed(k, "read_x_by_observer", exc)
                time.sleep(0.005)
                continue
            o = x_seen.get("seq", -1) if x_seen else -1

            checked += 1
            # WFR requires: if y (based_on=r) is visible to O, x must be at
            # r or newer FOR THAT SAME OBSERVER.
            is_violation = o < r
            if is_violation:
                counters["violations"] += 1
                print(f"[WFR VIOLATION] round={k:03d} y.based_on={r} "
                      f"but observer sees x.seq={o}")
            if logger:
                logger.log(task="task4_wfr", scenario=scenario_label, config=config_tag,
                            run_id=run_id, k=k, r=r, o=o,
                            outcome="violation" if is_violation else "ok")
            time.sleep(0.005)

    effective_rounds = rounds_attempted - counters["availability_losses"] - counters["errors"]
    print()
    print(f"Rounds attempted    : {rounds_attempted}")
    print(f"Availability losses : {counters['availability_losses']} "
          f"(a secondary/primary involved in this round was unreachable or "
          f"timed out - expected during failure/partition scenarios, NOT "
          f"counted as a consistency violation)")
    print(f"Other errors        : {counters['errors']}")
    print(f"Effective rounds    : {effective_rounds}")
    print(f"Rounds checked      : {checked} "
          f"(both x and y happened to be visible to the observer)")
    print(f"Violations          : {counters['violations']} "
          f"({(counters['violations'] / checked * 100) if checked else 0.0:.1f}% "
          f"of checked rounds)")
    if mode == "causal_propagate":
        print("RESULT              :",
              "PASS - writes-follow-reads maintained" if counters["violations"] == 0
              else "FAIL - WFR violation detected despite explicit propagation")
    elif mode == "causal_no_propagate":
        print("RESULT              :",
              "WFR VIOLATION observed (as expected: causal sessions do not "
              "automatically span independent clients)" if counters["violations"] > 0
              else "no violation this run; causal tokens were still never "
                   "exchanged, so this config still does NOT guarantee WFR "
                   "in general")
    else:
        print("RESULT              :",
              "WFR VIOLATION observed" if counters["violations"] > 0
              else "no violation this run, but config does NOT guarantee WFR")
    print()
    return {
        "checked": checked,
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "effective_rounds": effective_rounds,
        "rounds_attempted": rounds_attempted,
    }


def print_scenario_summary(scenario_results):
    print()
    print("=" * 90)
    print("CROSS-SCENARIO SUMMARY (Task 4: Writes-Follow-Reads)")
    print("=" * 90)
    header = (f"{'Scenario':<34}{'A viol/checked':<18}{'A avail-loss':<14}"
              f"{'B viol/checked':<18}{'B avail-loss':<14}"
              f"{'C viol/checked':<18}{'C avail-loss':<14}")
    print(header)
    print("-" * len(header))
    for label, result in scenario_results.items():
        if not result:
            print(f"{label:<34}{'n/a':<18}{'n/a':<14}{'n/a':<18}{'n/a':<14}"
                  f"{'n/a':<18}{'n/a':<14}")
            continue
        a, b, c = result["config_a"], result["config_b"], result["config_c"]
        sa = f"{a['violations']}/{a['checked']}"
        sb = f"{b['violations']}/{b['checked']}"
        sc = f"{c['violations']}/{c['checked']}"
        print(f"{label:<34}{sa:<18}{a['availability_losses']:<14}"
              f"{sb:<18}{b['availability_losses']:<14}"
              f"{sc:<18}{c['availability_losses']:<14}")
    print()
    print("Legend: A = no session (weak) | B = separate causal sessions, NOT")
    print("        propagated | C = separate causal sessions, propagated")
    print()


def main():
    client = common.get_client()
    common.print_topology(client)
    common.print_replication_lag(client)

    db = client[common.DB_NAME]
    logger = common.TrialLogger("task4_wfr")
    all_run_ids = []

    def run_one(mode, x_id, run_id, scenario_label):
        writer = XWriter(db, x_id)
        writer.start()
        time.sleep(1)
        try:
            stats = run_config(client, db, x_id, run_id, mode,
                                logger=logger, scenario_label=scenario_label)
        finally:
            writer.stop()
            writer.join()
            print(f"[writer] x_id={x_id!r} stopped (final seq={writer.last_seq}, "
                  f"writes={writer.write_count}, write errors={writer.write_errors})")
        return stats

    def run_experiment(scenario_label="unknown"):
        # Fresh run_id/x_id per scenario so results never mix across runs.
        run_id = str(uuid.uuid4())
        x_id = f"{run_id}-x"
        all_run_ids.append(run_id)

        empty_stats = {"checked": 0, "violations": 0, "availability_losses": 0,
                        "errors": 0, "effective_rounds": 0, "rounds_attempted": 0}
        stats_a = stats_b = stats_c = empty_stats
        try:
            stats_a = run_one("weak", x_id, run_id, scenario_label)
            stats_b = run_one("causal_no_propagate", x_id, run_id, scenario_label)
            stats_c = run_one("causal_propagate", x_id, run_id, scenario_label)
        finally:
            try:
                coll = db.get_collection(
                    COLLECTION_NAME, write_concern=WriteConcern(w="majority")
                )
                coll.delete_many({"run_id": run_id})
                coll.delete_one({"_id": x_id})
            except PyMongoError as exc:
                print(f"[cleanup] WARNING: could not clean up x_id={x_id}: {exc}")

        return {
            "config_a": stats_a,
            "config_b": stats_b,
            "config_c": stats_c,
        }

    try:
        # Requirements.md asks for experiments under several scenarios:
        # normal operation, node failure, and a network partition.
        scenario_results = scenarios.run_under_scenarios(
            client, run_experiment, logger=logger
        )
        print_scenario_summary(scenario_results)
    finally:
        logger.close()
        client.close()


if __name__ == "__main__":
    main()

