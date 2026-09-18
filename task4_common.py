#!/usr/bin/env python3
"""
task4_common.py

Shared library for Task 4: client-centric consistency model
WRITES-FOLLOW-READS (WFR).

This module holds everything that does NOT depend on which scenario is
being run: the background XWriter thread, Config A/B/C implementations,
pinned-target helpers, summary printers, and `run_one_scenario(scenario_name)`
- the single entry point each of the four `task4_scenario_*.py` launcher
scripts calls to run Task 4 under exactly one named scenario ("normal",
"secondary_failure", "primary_failure",
"network_partition_minority_isolated").

Run one of the launcher scripts directly, e.g.:
    python task4_scenario_normal.py
    python task4_scenario_secondary_failure.py
    python task4_scenario_primary_failure.py
    python task4_scenario_partition.py

Splitting the four scenarios into separate script invocations (instead of
one script that loops through all four, as the original
task4_writes_follow_reads.py did) means you only have to babysit ONE manual
docker stop/start or network-partition step per run, and a mistake,
Ctrl-C, or crash during one scenario's manual step doesn't discard the
results already collected for the others - each scenario writes its own
timestamped JSONL log file (see common.TrialLogger).

Definition
----------
A write performed by a client on a data item, following a read of that item
by the same client, is guaranteed to take place on the same or a more
recent version of the value that was read. Informally: if you read version k
of x and then write y based on it, then anyone who sees your write of y must
also see version k (or newer) of x. Effects never appear before their
causes.

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

We test THREE configurations:

  Config A - WEAK (independent clients, no sessions at all):
      writeConcern=w:1, readConcern=local, readPreference=secondary.
      B and O use no session/causal token whatsoever.
      Expected: WFR can be VIOLATED.

  Config B - CAUSAL SESSIONS, NOT PROPAGATED (the key control condition):
      writeConcern=majority, readConcern=majority, readPreference=secondary.
      B and O EACH use their own causal session (causal_consistency=True),
      but B's cluster time / operation time is NEVER communicated to O.
      Expected: WFR can STILL be violated - per-session causal consistency
      does NOT automatically extend across independent clients.

  Config C - CAUSAL SESSIONS, EXPLICITLY PROPAGATED (expected to UPHOLD WFR):
      Same write/read concerns as Config B, but immediately after B writes
      y, we hand B's session.cluster_time / session.operation_time to O's
      session via advance_cluster_time(...) / advance_operation_time(...) -
      exactly what a real distributed application would do by forwarding a
      causal token from the writer to the next reader.
      Expected: WFR holds.

Revision notes (why this version differs from a naive single-file
task4_writes_follow_reads.py implementation) - mirrors task1_common.py /
task3_common.py
-----------------------------------------------------------------------------
1. PINNED READ TARGET, with an explicit trade-off for THIS task. As in
   task1/task3, letting the driver's own server-selection logic (SDAM) pick
   which secondary answers each read risks quietly avoiding the exact node
   scenarios.py is about to fail/isolate, hiding the fault being tested.
   `run_one_scenario(scenario_name, pin_reads=True)` supports the same
   `pin_reads` toggle as task1/task3:

     pin_reads=True  - B's and O's sessions share ONE pinned_client, pinned
                        to the EXACT node this scenario is about to
                        fail/isolate (or freshly discovered for "normal" /
                        "primary_failure"). This guarantees the experiment
                        actually exercises reads against the node under
                        test. IMPORTANT CAVEAT SPECIFIC TO TASK 4: because
                        B and O then read from the SAME single node, and a
                        single node's oplog is applied strictly in order,
                        Config A's classic violation mode (y replicates to
                        the node O reads before x's newer value does)
                        becomes much less likely to show up WITHIN that one
                        node - the cross-secondary staleness that produces
                        most real-world WFR violations requires B and O to
                        land on DIFFERENT secondaries. Pinning is still
                        useful here to guarantee the failure/partition
                        scenario is actually exercised (you will cleanly
                        see availability losses once the pinned node goes
                        down), but it is not the mode you want if your goal
                        is to see Config A actively violate WFR.

     pin_reads=False - B and O each use a plain, UNPINNED client, exactly
                        like the original task4_writes_follow_reads.py.
                        Read selection is left to the driver's own SDAM
                        heuristics for every call, so B and O CAN land on
                        different secondaries within the same round - this
                        is what actually reproduces the "cause appears
                        after its effect" violation in Config A, and lets
                        you observe what happens to that behaviour once the
                        driver fails over away from a node this scenario
                        takes down. This is the recommended mode for most
                        runs of Task 4; pin_reads=True is offered mainly so
                        the failure/partition scenarios can also be run in
                        "guaranteed to hit the target node" mode for
                        completeness, matching task1/task3's interface.

2. AVAILABILITY LOSS IS NOT A CONSISTENCY VIOLATION, exactly as in
   task1/task3: `common.classify_error` buckets a pinned/failed node's
   ServerSelectionTimeoutError (or similar) as `availability_loss`,
   separate from WFR violations.
3. PER-ROUND EVIDENCE. Every checked round is written to the TrialLogger as
   a single JSON line (scenario, config, round index, r, o, outcome), not
   just scenario start/end markers.
4. `run_one_scenario(scenario_name, pin_reads=True)` lets you run exactly
   one scenario per script invocation (see task4_scenario_*.py).
5. BUGFIX - PER-CONFIG run_id / x_id NAMESPACES. In an earlier version of
   this file, Config A, B, and C (within the SAME call to
   `run_experiment`) shared exactly one `run_id` (and therefore one
   `x_id`, and - critically - the SAME sequence of y-document ids
   `f"{run_id}-y-{k}"` for k = 0..ROUNDS-1). Because the three configs run
   SEQUENTIALLY against the SAME collection and cleanup only happens once
   at the very end of `run_experiment`, Config A's y-documents were still
   present in the collection when Config B started, so every single one
   of Config B's `insert_one` calls for round k collided with the y
   document Config A had already inserted for that same k and raised
   DuplicateKeyError - and likewise for Config C. This silently reduced
   Config B's and C's `checked` count to 0 for every run, and because the
   summary logic reports "no violation" / "PASS" whenever
   `violations == 0` (regardless of `checked`), it produced a false-
   positive "PASS - writes-follow-reads maintained" verdict for Config C
   even though NO rounds were ever actually observed.

   Fix: `run_experiment` now derives a SEPARATE `run_id`/`x_id` for each
   mode - `f"{base_run_id}-{mode}"` / `f"{base_run_id}-{mode}-x"` - so
   Config A's, B's, and C's y-documents (and x-documents) can never
   collide with one another, and cleanup deletes each mode's documents by
   its own run_id.
"""

import threading
import time
import uuid
from datetime import datetime

from pymongo import ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


COLLECTION_NAME = "wfr_test"

ROUNDS = 150
WRITER_SECONDS = 30


# --------------------------------------------------------------------------
# Small helpers for readable output
# --------------------------------------------------------------------------

def _now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _fmt_addr(address):
    if address is None:
        return "unknown"
    host, port = address
    return f"{host}:{port}"


def _section(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def _subsection(title):
    print("-" * 70)
    print(title)
    print("-" * 70)


# --------------------------------------------------------------------------
# Pinned-target helpers (identical in spirit to task1_common.py)
# --------------------------------------------------------------------------

def _parse_node_str(node_str):
    """Turn a "host:port" string (as used by scenarios.py) into the
    (host, port) tuple pymongo addresses use."""
    host, port_str = node_str.rsplit(":", 1)
    return (host, int(port_str))


def _discover_pin_target(client):
    """
    Fallback pin-target discovery: pick `sorted(client.secondaries)[0]` from
    whatever the topology currently looks like.

    Only safe for "normal" and "primary_failure" - see the long explanation
    in task1_common.py's `_discover_pin_target`. For "secondary_failure"
    and "network_partition_minority_isolated", scenarios.py hands us the
    exact target node string decided BEFORE the fault was injected instead.

    Returns (host, port) tuple, or None if no secondary is currently known
    (falls back to un-pinned behaviour).
    """
    secondaries = sorted(client.secondaries)
    if not secondaries:
        return None
    return secondaries[0]


# --------------------------------------------------------------------------
# Background writer for the shared item x
# --------------------------------------------------------------------------

class XWriter(threading.Thread):
    """Background thread that advances shared item x.seq on the primary.

    Always uses a plain (unpinned) db handle, exactly like task1's
    BackgroundLoadGenerator: writes are routed to the primary by the driver
    regardless of read preference, so there is nothing to pin here, and
    keeping this connection separate from whatever pinned_client Config
    A/B/C is using means the writer keeps running even while a pinned
    secondary is down/isolated.
    """

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
        # callable" as soon as the thread is asked to stop.
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


# --------------------------------------------------------------------------
# Config A / B / C (all three share this one function, parameterised by mode)
# --------------------------------------------------------------------------

def run_config(pinned_client, pinned_db, pinned_target, x_id, run_id, mode,
                logger=None, scenario_label="unknown"):
    """
    mode: one of
        "weak"                 -> Config A
        "causal_no_propagate"  -> Config B
        "causal_propagate"     -> Config C

    `pinned_client` / `pinned_db` are either the plain, unpinned
    common.get_client() client/db (when pin_reads=False, or when no
    secondary was discoverable to pin to), or a client/db pinned to one
    specific node via common.get_pinned_client (when pin_reads=True). B's
    and O's sessions are both started from `pinned_client`, so whichever
    mode is active applies identically to both of them.

    `run_id` / `x_id` MUST be unique to this specific call (i.e. unique per
    mode within a given scenario run) - see the module docstring's revision
    note 5 for why sharing them across Config A/B/C causes spurious
    DuplicateKeyError failures and a false "PASS" verdict.
    """
    labels = {
        "weak": "CONFIG A: no session (independent clients, no causal token)",
        "causal_no_propagate": "CONFIG B: separate causal sessions, "
                                "NOT propagated between B and observer",
        "causal_propagate": "CONFIG C: separate causal sessions, "
                             "cluster/operation time EXPLICITLY propagated",
    }
    config_tag = {"weak": "A", "causal_no_propagate": "B", "causal_propagate": "C"}[mode]
    _section(f"TASK 4 / {labels[mode]}")
    print("readPreference=secondary")
    print(f"Pinned node: {_fmt_addr(pinned_target) if pinned_target else '(none - un-pinned, SDAM decides per-call)'}")
    print(f"Rounds     : {ROUNDS}")
    print(f"Started at : {_now()}")

    causal = mode != "weak"
    read_concern = ReadConcern("majority") if causal else ReadConcern("local")
    write_concern = (
        WriteConcern(w="majority", j=True, wtimeout=10000) if causal else WriteConcern(w=1)
    )

    coll = pinned_db.get_collection(
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
                        run_id=run_id, k=k, pinned_target=_fmt_addr(pinned_target),
                        stage=stage, outcome=kind)

    # B and O ALWAYS get their own, independent session objects (or no
    # session at all in "weak" mode). This is what makes the test honest:
    # any causal guarantee that shows up has to come from something we
    # explicitly did (propagation), not from sharing one session.
    with _open_session(pinned_client, causal) as session_b, \
            _open_session(pinned_client, causal) as session_o:

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
            #
            # y_id is namespaced by `run_id`, which the caller is required
            # to make unique PER CONFIG (see run_one_scenario /
            # run_experiment below) - otherwise Config B/C would collide
            # with y-documents Config A already inserted for the same
            # round index k and every insert_one here would fail with
            # DuplicateKeyError (see revision note 5 above).
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
                            run_id=run_id, k=k, pinned_target=_fmt_addr(pinned_target),
                            r=r, o=o, outcome="violation" if is_violation else "ok")
            time.sleep(0.005)

    effective_rounds = rounds_attempted - counters["availability_losses"] - counters["errors"]
    print()
    _subsection(f"Config {config_tag} summary")
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
    if checked == 0:
        # A zero-checked run is NOT evidence of anything either way - most
        # commonly this means every round hit an error/availability-loss
        # path (e.g. the DuplicateKeyError collision this file used to
        # have before revision note 5's fix, or a genuine outage during a
        # failure/partition scenario). Say so explicitly instead of
        # letting the "no violation" branches below imply a clean PASS.
        print("RESULT              :",
              "INCONCLUSIVE - 0 rounds were actually checked, so this run "
              "provides NO evidence for or against WFR under this config "
              "(see 'Other errors' / 'Availability losses' above for why)")
    elif mode == "causal_propagate":
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
    print(f"Finished at         : {_now()}")
    print()
    return {
        "checked": checked,
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "effective_rounds": effective_rounds,
        "rounds_attempted": rounds_attempted,
    }


# --------------------------------------------------------------------------
# Cross-scenario summary
# --------------------------------------------------------------------------

def print_scenario_summary(scenario_results):
    _section("CROSS-SCENARIO SUMMARY (Task 4: Writes-Follow-Reads)")
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


# --------------------------------------------------------------------------
# Main entry point (per-scenario)
# --------------------------------------------------------------------------

# Valid scenario names, in the order requirements.md expects them to be
# discussed. Each has a matching function in scenarios.SCENARIO_RUNNERS.
SCENARIO_NAMES = (
    "normal",
    "secondary_failure",
    "primary_failure",
    "network_partition_minority_isolated",
)


def run_one_scenario(scenario_name, pin_reads=False):
    """
    Run Task 4 (Config A vs B vs C, read-then-write observed by an
    independent client) under exactly ONE named scenario, then clean up
    and return that scenario's result dict (or None if the scenario had to
    be skipped - e.g. no secondary currently discoverable to fail/isolate).

    This is the single entry point each of the task4_scenario_*.py
    launcher scripts calls, mirroring task1_common.run_one_scenario /
    task3_common.run_one_scenario:
      - you only have to babysit ONE manual stop/start or
        network-partition step per run, instead of four in a row;
      - a mistake, Ctrl-C, or crash during one scenario's manual step
        doesn't discard the results already collected for the others -
        each scenario writes its own timestamped JSONL log file (see
        common.TrialLogger) and prints its own full Config A/B/C report.

    pin_reads (default False - see the long note near the top of this
    file for why False, unlike task1/task3, is the RECOMMENDED default
    here):
        True  - B's and O's sessions share ONE client pinned to the EXACT
                node secondary_failure/partition is about to fail/isolate.
                Guarantees the fault is actually exercised, but collapses
                B and O onto the SAME node, which suppresses the classic
                cross-secondary-staleness violation Config A is meant to
                demonstrate (a single node's oplog is always applied in
                order). Use this if what you want to measure is
                availability behaviour (or a same-node visibility check)
                under the fault, not WFR violations per se.

        False - B and O each get a plain, unpinned client/session, exactly
                like the original task4_writes_follow_reads.py. The
                driver's own SDAM decides which secondary answers each
                read, so B and O CAN land on different secondaries within
                the same round, which is what actually reproduces WFR
                violations in Config A and lets you observe what a
                secondary failure/partition does to that behaviour via
                natural failover. Recommended default for Task 4.

    Before calling this for `secondary_failure`, `primary_failure`, or
    `network_partition_minority_isolated`, make sure the replica set is
    currently healthy (all three nodes up, no lingering partition from a
    previous run) - each scenario assumes it's starting from a normal
    topology so it can correctly identify which node to fail/isolate.
    """
    if scenario_name not in SCENARIO_NAMES:
        raise ValueError(
            f"unknown scenario {scenario_name!r}; expected one of {SCENARIO_NAMES}"
        )

    client = common.get_client()

    mode_suffix = "" if pin_reads else "_unpinned"
    mode_label = "PINNED to the exact failing/isolated node (B and O share it)" \
        if pin_reads else \
        "UNPINNED - driver's own SDAM server selection decides per read (B/O may differ)"

    _section(f"TASK 4: WRITES-FOLLOW-READS - SCENARIO '{scenario_name}' "
             f"({'pinned' if pin_reads else 'unpinned'} reads)")
    print(f"Run started at: {_now()}")
    print(f"Read targeting : {mode_label}")
    print("Cluster topology and starting replication lag:")
    common.print_topology(client)
    common.print_replication_lag(client)

    db = client[common.DB_NAME]
    logger = common.TrialLogger(f"task4_wfr_{scenario_name}{mode_suffix}")
    all_run_ids = []

    def run_one_mode(mode, x_id, run_id, scenario_label, pinned_client, pinned_db, pinned_target):
        # The background writer always uses the plain, unpinned db handle
        # (see XWriter's docstring): writes always go to the primary
        # regardless of read preference, so there is nothing to pin, and
        # keeping it independent means it survives a pinned secondary
        # going down.
        writer = XWriter(db, x_id)
        writer.start()
        time.sleep(1)
        try:
            stats = run_config(pinned_client, pinned_db, pinned_target, x_id, run_id,
                                mode, logger=logger, scenario_label=scenario_label)
        finally:
            writer.stop()
            writer.join()
            print(f"[writer] x_id={x_id!r} stopped (final seq={writer.last_seq}, "
                  f"writes={writer.write_count}, write errors={writer.write_errors})")
        return stats

    def run_experiment(scenario_label="unknown", target_node=None):
        if not pin_reads:
            # Deliberately skip pinning: B and O each use the plain,
            # unpinned `client`, so readPreference=SECONDARY is resolved
            # by the driver's normal SDAM logic on every single read - the
            # same behaviour as the original task4_writes_follow_reads.py.
            pinned_target = None
            pinned_client = client
            print(f"[pin] pin_reads=False for scenario '{scenario_label}': "
                  f"B and O are UNPINNED. The driver's own server selection "
                  f"(SDAM) decides which secondary answers each read, so B "
                  f"and O can land on different secondaries - this is what "
                  f"lets Config A actually demonstrate a WFR violation.")
        else:
            # Pin B's and O's shared client to a specific node, so
            # "secondary_failure" and "network_partition_minority_isolated"
            # actually exercise reads against the node under test.
            #
            # When scenarios.py hands us `target_node`, it is the exact
            # "host:port" it decided on BEFORE injecting the fault - use it
            # as-is. Only fall back to a fresh discovery
            # (`_discover_pin_target`) when no specific node is under test
            # ("normal", "primary_failure").
            if target_node is not None:
                pinned_target = _parse_node_str(target_node)
            else:
                pinned_target = _discover_pin_target(client)
            if pinned_target is None:
                print("[pin] WARNING: no secondary currently discoverable; "
                      "falling back to an un-pinned client for this "
                      "scenario (less reproducible).")
                pinned_client = client
            else:
                print(f"[pin] pinning Config A/B/C secondary reads (both B "
                      f"and O) to {_fmt_addr(pinned_target)} for scenario "
                      f"'{scenario_label}'")
                pinned_client = common.get_pinned_client(pinned_target)

        pinned_db = pinned_client[common.DB_NAME]

        # Scenario-level id, used only for human-readable bookkeeping/
        # logging of "this call to run_experiment" as a whole.
        base_run_id = str(uuid.uuid4())
        all_run_ids.append(base_run_id)

        # ------------------------------------------------------------------
        # BUGFIX (see module docstring revision note 5): Config A, B, and C
        # each need their OWN run_id/x_id namespace. If they shared one
        # run_id, the y-documents Config A inserts as
        # f"{run_id}-y-0" .. f"{run_id}-y-{ROUNDS-1}" would still be sitting
        # in the collection (cleanup only happens once, after all three
        # configs finish) when Config B tries to insert_one() the SAME
        # _id for the SAME round index - guaranteed DuplicateKeyError on
        # every round, for both Config B and Config C. That silently
        # collapses `checked` to 0 for B and C, and because the reporting
        # logic below only distinguishes "no violation" from "violation"
        # (not "no rounds were ever actually observed"), it used to print
        # a false "PASS - writes-follow-reads maintained" for Config C
        # even though zero rounds had been checked.
        #
        # Fix: derive a distinct run_id/x_id PER MODE from base_run_id.
        # ------------------------------------------------------------------
        mode_run_ids = {}

        def _mode_ids(mode):
            mode_run_id = f"{base_run_id}-{mode}"
            mode_x_id = f"{mode_run_id}-x"
            mode_run_ids[mode] = mode_run_id
            return mode_run_id, mode_x_id

        empty_stats = {"checked": 0, "violations": 0, "availability_losses": 0,
                        "errors": 0, "effective_rounds": 0, "rounds_attempted": 0}
        stats_a = stats_b = stats_c = empty_stats
        try:
            run_id_a, x_id_a = _mode_ids("weak")
            stats_a = run_one_mode("weak", x_id_a, run_id_a, scenario_label,
                                    pinned_client, pinned_db, pinned_target)

            run_id_b, x_id_b = _mode_ids("causal_no_propagate")
            stats_b = run_one_mode("causal_no_propagate", x_id_b, run_id_b, scenario_label,
                                    pinned_client, pinned_db, pinned_target)

            run_id_c, x_id_c = _mode_ids("causal_propagate")
            stats_c = run_one_mode("causal_propagate", x_id_c, run_id_c, scenario_label,
                                    pinned_client, pinned_db, pinned_target)
        finally:
            try:
                cleanup_coll = db.get_collection(
                    COLLECTION_NAME, write_concern=WriteConcern(w="majority")
                )
                # Each mode wrote its y-documents tagged with its OWN
                # run_id, and its own x-document at f"{mode_run_id}-x", so
                # clean up each mode's documents individually rather than
                # relying on a single shared run_id/x_id.
                for mode_run_id in mode_run_ids.values():
                    cleanup_coll.delete_many({"run_id": mode_run_id})
                    cleanup_coll.delete_one({"_id": f"{mode_run_id}-x"})
            except PyMongoError as exc:
                print(f"[cleanup] WARNING: could not clean up "
                      f"base_run_id={base_run_id}: {exc}")
            if pinned_client is not client:
                pinned_client.close()

        # Record the concrete per-mode ids too, so the final CLEANUP
        # section's printout accurately reflects what was actually
        # inserted/deleted (rather than just the umbrella base_run_id).
        all_run_ids.extend(mode_run_ids.values())

        return {
            "config_a": stats_a,
            "config_b": stats_b,
            "config_c": stats_c,
        }

    result = None
    try:
        runner = scenarios.SCENARIO_RUNNERS[scenario_name]
        result = runner(client, run_experiment, logger=logger)
        if result is None:
            print(f"[scenario] '{scenario_name}' was skipped (see message "
                  f"above) - no Config A/B/C data was collected this run.")
    finally:
        _section("CLEANUP")
        print(f"[{_now()}] removed test documents for run_ids: {all_run_ids} "
              f"(cleaned up per-scenario/per-config as each run finished)")
        logger.close()
        client.close()
        print(f"[{_now()}] connection closed. Run complete.")

    return result
