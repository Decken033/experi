#!/usr/bin/env python3
"""
task3_common.py

Shared library for Task 3: client-centric consistency model
MONOTONIC-WRITES (MW).

This module holds everything that does NOT depend on which scenario is
being run: Config A / Config B implementations, the hole-detection logic,
pinned-target helpers, comparison/summary printers, and
`run_one_scenario(scenario_name)` - the single entry point each of the
four `task3_scenario_*.py` launcher scripts calls to run Task 3 under
exactly one named scenario ("normal", "secondary_failure",
"primary_failure", "network_partition_minority_isolated").

Run one of the launcher scripts directly, e.g.:
    python task3_scenario_normal.py
    python task3_scenario_secondary_failure.py
    python task3_scenario_primary_failure.py
    python task3_scenario_partition.py

Splitting the four scenarios into separate script invocations (instead of
one script that loops through all four, as the original
task3_monotonic_writes.py did) means you only have to babysit ONE manual
docker stop/start or network-partition step per run, and a mistake,
Ctrl-C, or crash during one scenario's manual step doesn't discard the
results already collected for the others - each scenario writes its own
timestamped JSONL log file (see common.TrialLogger).

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
    doc_j (j < i) is NOT visible. A hole would mean writes became visible
    out of order -> a monotonic-writes violation.

  Config A (weak, still expected to UPHOLD MW):
      writeConcern   = w:1        (ack after primary only)
      readConcern    = local
      readPreference = secondary  (read from a specific, pinned secondary)
      NO causal session

  Config B (strong, expected to UPHOLD MW):
      writeConcern   = majority, j:true
      readConcern    = majority
      readPreference = secondary  (same pinned secondary)
      causal session (causal_consistency=True)

Prediction: MW holds in BOTH configs, because a single primary orders the
writes and secondaries replay them in that order. The experiment is
designed to confirm the prediction (and would catch a violation if one
occurred, e.g. around a failover with rollback) - which is exactly why we
also run it under node-failure and network-partition scenarios.

Revision notes (why this version differs from a naive single-file
task3_monotonic_writes.py implementation) - mirrors task1_common.py
-----------------------------------------------------------------------------
1. PINNED READ TARGET. Letting the driver's own server-selection logic
   (SDAM) pick which secondary answers each read is not good enough here:
   SDAM can stick to whichever secondary looks healthiest and simply avoid
   the one that scenarios.py is about to fail or network-partition, which
   would silently hide the very condition the scenario is meant to create.
   For "secondary_failure" and "network_partition_minority_isolated",
   scenarios.py passes in the EXACT "host:port" it decided to fail/isolate
   -- chosen BEFORE the fault was injected -- and all Config A/B reads for
   that run are pinned to that exact node via `common.get_pinned_client`.
   (For "normal" and "primary_failure", where no specific secondary is
   under test, the pin target is instead discovered fresh from the current
   topology.) As in task1, re-discovering "the current secondary" from
   scratch during secondary_failure/partition would NOT work: by the time
   run_experiment() runs, the failed/isolated node has already dropped out
   of the topology, so a fresh discovery would silently pin to the
   surviving node instead of the one under test.
2. AVAILABILITY LOSS IS NOT A CONSISTENCY VIOLATION. When the pinned node
   is down or unreachable, the custom selector yields no candidates and the
   driver raises a ServerSelectionTimeoutError (or similar). `common.
   classify_error` is used to bucket that separately as `availability_loss`
   rather than lumping it into "MW violations" - that would overstate the
   violation rate. Writes that never succeeded (e.g. during a primary
   failover) are recorded in `failed_writes` and excluded from the
   hole-check, since an absent document whose write never happened is not
   a monotonic-writes violation.
3. j=True on Config B's write concern, matching Config B in task1/task3.
4. PER-ITERATION EVIDENCE. Every iteration of both configs is written to
   the TrialLogger as a single JSON line (scenario, config, iteration
   index, pinned target, outcome), not just scenario start/end markers, so
   the report can cite raw per-trial evidence instead of only console
   summaries.
5. `run_one_scenario(scenario_name, pin_reads=True)` lets you run exactly
   one scenario per script invocation (see task3_scenario_*.py), and
   supports the same `pin_reads=False` escape hatch as Task 1 for the
   network-partition scenario: with pinning, the pinned node going fully
   down collapses BOTH configs to 100% availability loss / 0 effective
   iterations once it drops out, so pin_reads=False lets the driver's own
   SDAM failover onto a healthy secondary instead, letting you observe
   whether MW still holds through that failover transition.
"""

import time
import uuid
from datetime import datetime

from pymongo import ReadPreference
from pymongo import errors as mongo_errors
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


COLLECTION_NAME = "mw_test"

# Number of ordered writes in the sequence issued by the single client.
N = 150

# Print a progress line every PROGRESS_EVERY iterations.
PROGRESS_EVERY = 25


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


def _classify_and_count(exc, availability_counter_name, other_counter_name, counters):
    """Bucket an exception into availability_loss vs a generic error bucket."""
    kind = common.classify_error(exc)
    if kind == "availability_loss":
        counters[availability_counter_name] += 1
    else:
        counters[other_counter_name] += 1
    return kind


# --------------------------------------------------------------------------
# Hole detection (unchanged logic from task3_monotonic_writes.py)
# --------------------------------------------------------------------------

def first_hole(seqs_present, up_to, known_failed=frozenset()):
    """
    Given the set of seq values currently visible on the secondary and the
    highest seq we have written so far (up_to), return the (present, missing)
    pair describing the first out-of-order gap, or None if writes are in
    order.

    A violation looks like: some seq i is present but an earlier seq j < i is
    missing.

    `known_failed` is the set of seq values whose WRITE never succeeded (the
    client got an error/timeout back for it, e.g. during a primary
    failover). Those are excluded from the "must be visible" check: a doc
    that was never durably written is not a monotonic-writes violation when
    it is absent, it is simply absent.
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


# --------------------------------------------------------------------------
# Config A
# --------------------------------------------------------------------------

def run_config_a(pinned_db, pinned_target, logger, scenario_label):
    """Weak configuration: w:1 / local, no causal session, reads pinned to
    a specific secondary. Still expected to UPHOLD MW (single-primary,
    ordered oplog)."""
    _section("TASK 3 / CONFIG A: w:1 + local (expected to UPHOLD MW)")
    print("Settings   : writeConcern=w:1  readConcern=local  readPreference=secondary")
    print(f"Pinned node: {_fmt_addr(pinned_target) if pinned_target else '(none - un-pinned)'}")
    print("Prediction : a single primary orders every write and secondaries replay the")
    print("             oplog strictly in order, so we EXPECT no out-of-order visibility")
    print("             even without a causal session.")
    print(f"Iterations : {N}")
    print(f"Started at : {_now()}")

    coll = pinned_db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern("local"),
        write_concern=WriteConcern(w=1),
    )

    run_id = str(uuid.uuid4())
    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    failed_writes = set()
    violation_indices = []

    _subsection("Running ordered write -> read-back iterations")
    for i in range(N):
        doc_id = f"{run_id}-{i}"

        # Ordered write W_i issued by this single client. Always routed to
        # the primary by the driver regardless of the collection's read
        # preference, so the pin only affects the read-back below.
        try:
            coll.insert_one({"_id": doc_id, "run_id": run_id, "seq": i})
        except mongo_errors.PyMongoError as exc:
            kind = _classify_and_count(
                exc, "availability_losses", "errors", counters)
            failed_writes.add(i)
            tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
            print(f"[{tag}] i={i:03d}  write failed: {exc!r}")
            if logger:
                logger.log(task="task3_mw", scenario=scenario_label, config="A",
                            run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                            stage="write", outcome=kind)
            continue

        # Read the whole sequence back from the pinned secondary and look
        # for a gap that would indicate writes becoming visible out of
        # order.
        try:
            docs = coll.find({"run_id": run_id}, max_time_ms=10000)
            seqs_present = {d["seq"] for d in docs}
        except mongo_errors.PyMongoError as exc:
            kind = _classify_and_count(
                exc, "availability_losses", "errors", counters)
            tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
            print(f"[{tag}] i={i:03d}  read-back failed: {type(exc).__name__}: {exc}")
            if logger:
                logger.log(task="task3_mw", scenario=scenario_label, config="A",
                            run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                            stage="read", outcome=kind)
            continue

        hole = first_hole(seqs_present, i, known_failed=failed_writes)
        if hole is not None:
            counters["violations"] += 1
            present, missing = hole
            violation_indices.append(i)
            print(f"[MW VIOLATION] i={i:03d}  seq {present} visible but "
                  f"earlier seq {missing} missing "
                  f"(running total: {counters['violations']})")
            if logger:
                logger.log(task="task3_mw", scenario=scenario_label, config="A",
                            run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                            outcome="violation", present=present, missing=missing)
        else:
            if logger:
                logger.log(task="task3_mw", scenario=scenario_label, config="A",
                            run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                            outcome="ok")
            if (i + 1) % PROGRESS_EVERY == 0:
                print(f"[{_now()}] progress {i + 1:03d}/{N}  "
                      f"violations so far={counters['violations']}  "
                      f"availability losses so far={counters['availability_losses']}  "
                      f"errors so far={counters['errors']}")

    effective_n = N - counters["availability_losses"] - counters["errors"]
    print()
    _subsection("Config A summary")
    print(f"Iterations           : {N}")
    print(f"Availability losses  : {counters['availability_losses']} "
          f"(pinned node unreachable - expected during failure/partition, "
          f"NOT counted as a consistency violation)")
    print(f"Other errors         : {counters['errors']}")
    print(f"Effective iterations : {effective_n}")
    print(f"MW violations        : {counters['violations']} "
          f"({(counters['violations'] / effective_n * 100) if effective_n else 0.0:.1f}% "
          f"of effective iterations)")
    if violation_indices:
        preview = violation_indices[:10]
        suffix = " ..." if len(violation_indices) > 10 else ""
        print(f"Violations at indices: {preview}{suffix}")
    print("Interpretation       :", end=" ")
    if counters["violations"] > 0:
        print("MW VIOLATION observed - unexpected under a single-primary,")
        print("                       ordered-oplog architecture; investigate (e.g. a")
        print("                       rollback around a failover) before writing this up.")
    elif effective_n == 0:
        print("no effective iterations completed (pinned node was unavailable")
        print("                       for the whole run) - an availability outcome, not")
        print("                       evidence either way about MW.")
    else:
        print("PASS - monotonic writes held for every effective iteration, as")
        print("                       predicted.")
    print(f"Finished at          : {_now()}")

    return run_id, {
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "iterations": N,
    }


# --------------------------------------------------------------------------
# Config B
# --------------------------------------------------------------------------

def run_config_b(pinned_client, pinned_db, pinned_target, logger, scenario_label):
    """Strong configuration: causal session + majority (w + j), reads
    pinned to the same secondary. Also expected to UPHOLD MW."""
    _section("TASK 3 / CONFIG B: causal session + majority (expected to UPHOLD MW)")
    print("Settings   : writeConcern=majority,j=true  readConcern=majority  "
          "readPreference=secondary")
    print(f"Pinned node: {_fmt_addr(pinned_target) if pinned_target else '(none - un-pinned)'}")
    print("Prediction : same single-primary/ordered-oplog argument as Config A, now")
    print("             additionally reinforced by a causal session, so we EXPECT no")
    print("             out-of-order visibility here either.")
    print(f"Iterations : {N}")
    print(f"Started at : {_now()}")

    coll = pinned_db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern("majority"),
        write_concern=WriteConcern(w="majority", j=True, wtimeout=15000),
    )

    run_id = str(uuid.uuid4())
    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    failed_writes = set()
    violation_indices = []

    _subsection("Running ordered write -> read-back iterations (single causal session)")
    with pinned_client.start_session(causal_consistency=True) as session:
        for i in range(N):
            doc_id = f"{run_id}-{i}"

            try:
                coll.insert_one(
                    {"_id": doc_id, "run_id": run_id, "seq": i}, session=session)
            except mongo_errors.PyMongoError as exc:
                kind = _classify_and_count(
                    exc, "availability_losses", "errors", counters)
                failed_writes.add(i)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  write failed: {exc!r}")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="write", outcome=kind)
                continue

            try:
                docs = coll.find(
                    {"run_id": run_id}, session=session, max_time_ms=15000)
                seqs_present = {d["seq"] for d in docs}
            except mongo_errors.PyMongoError as exc:
                kind = _classify_and_count(
                    exc, "availability_losses", "errors", counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  read-back failed: {type(exc).__name__} "
                      f"(pinned secondary unreachable or could not catch up in time)")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="read", outcome=kind)
                continue

            hole = first_hole(seqs_present, i, known_failed=failed_writes)
            if hole is not None:
                counters["violations"] += 1
                present, missing = hole
                violation_indices.append(i)
                print(f"[MW VIOLATION] i={i:03d}  seq {present} visible but "
                      f"earlier seq {missing} missing "
                      f"(running total: {counters['violations']})")
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="violation", present=present, missing=missing)
            else:
                if logger:
                    logger.log(task="task3_mw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="ok")
                if (i + 1) % PROGRESS_EVERY == 0:
                    print(f"[{_now()}] progress {i + 1:03d}/{N}  "
                          f"violations so far={counters['violations']}  "
                          f"availability losses so far={counters['availability_losses']}  "
                          f"errors so far={counters['errors']}")

    effective_n = N - counters["availability_losses"] - counters["errors"]
    print()
    _subsection("Config B summary")
    print(f"Iterations           : {N}")
    print(f"Availability losses  : {counters['availability_losses']} "
          f"(pinned node unreachable / could not catch up in time - expected "
          f"during failure/partition, NOT counted as a consistency violation)")
    print(f"Other errors         : {counters['errors']}")
    print(f"Effective iterations : {effective_n}")
    print(f"MW violations        : {counters['violations']} "
          f"({(counters['violations'] / effective_n * 100) if effective_n else 0.0:.1f}% "
          f"of effective iterations)")
    if violation_indices:
        preview = violation_indices[:10]
        suffix = " ..." if len(violation_indices) > 10 else ""
        print(f"Violations at indices: {preview}{suffix}")
    print("Interpretation       :", end=" ")
    if counters["violations"] == 0 and effective_n > 0:
        print("PASS - monotonic writes held for every effective iteration, as")
        print("                       predicted.")
    elif effective_n == 0:
        print("no effective iterations completed (pinned node was unavailable")
        print("                       for the whole run) - an availability outcome, not")
        print("                       evidence either way about MW.")
    else:
        print("MW VIOLATION observed even with a causal session, which")
        print("                       contradicts the prediction and should be investigated")
        print("                       (e.g. driver/server version, or a rollback around a")
        print("                       failover).")
    print(f"Finished at          : {_now()}")

    return run_id, {
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "iterations": N,
    }


# --------------------------------------------------------------------------
# Comparisons / summaries
# --------------------------------------------------------------------------

def print_final_comparison(stats_a, stats_b):
    """Side-by-side summary of both configurations for quick reporting."""
    _section("FINAL COMPARISON: Config A (weak) vs Config B (strong)")

    viol_a = stats_a["violations"]
    avail_a = stats_a["availability_losses"]
    err_a = stats_a["errors"]
    n_a = stats_a["iterations"]
    eff_a = n_a - avail_a - err_a

    viol_b = stats_b["violations"]
    avail_b = stats_b["availability_losses"]
    err_b = stats_b["errors"]
    n_b = stats_b["iterations"]
    eff_b = n_b - avail_b - err_b

    header = f"{'Metric':<32}{'Config A (weak)':<22}{'Config B (strong)':<22}"
    print(header)
    print("-" * len(header))
    print(f"{'Iterations':<32}{n_a:<22}{n_b:<22}")
    print(f"{'Availability losses':<32}{avail_a:<22}{avail_b:<22}")
    print(f"{'Effective iterations':<32}{eff_a:<22}{eff_b:<22}")
    print(f"{'MW violations':<32}{viol_a:<22}{viol_b:<22}")
    pct_a = f"{viol_a / eff_a * 100:.1f}%" if eff_a else "n/a"
    pct_b = f"{viol_b / eff_b * 100:.1f}%" if eff_b else "n/a"
    print(f"{'Violation rate (of eff.)':<32}{pct_a:<22}{pct_b:<22}")
    print(f"{'Other read/write errors':<32}{err_a:<22}{err_b:<22}")

    print()
    print("Takeaway:")
    if viol_a == 0 and viol_b == 0 and (eff_a > 0 or eff_b > 0):
        print("  Results match the prediction: monotonic writes held in BOTH the weak")
        print("  (w:1 / local, no causal session) and strong (majority+j / majority /")
        print("  causal session) configurations, consistent with a single-primary,")
        print("  ordered-oplog architecture.")
    elif viol_a > 0 or viol_b > 0:
        print("  An MW violation was observed; see the [MW VIOLATION] lines above and")
        print("  investigate before writing this up as a clean result (e.g. check for a")
        print("  rollback around a failover, or a driver/server bug).")
    else:
        print("  One or both configs spent this run mostly unavailable (pinned node")
        print("  down/partitioned); see 'Availability losses' above before drawing any")
        print("  conclusion about consistency from this particular scenario run.")


def print_scenario_summary(scenario_results):
    """
    Cross-scenario summary: MW violation count and availability-loss counts
    for Config A / Config B under each injected scenario, so the report can
    show whether the injected failures change the observed behaviour.
    """
    _section("CROSS-SCENARIO SUMMARY (Task 3: Monotonic Writes)")
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


def run_one_scenario(scenario_name, pin_reads=True):
    """
    Run Task 3 (Config A vs Config B, ordered write -> read-back) under
    exactly ONE named scenario, then clean up and return that scenario's
    result dict (or None if the scenario had to be skipped - e.g. no
    secondary currently discoverable to fail/isolate).

    This is the single entry point each of the task3_scenario_*.py
    launcher scripts calls, mirroring task1_common.run_one_scenario:
      - you only have to babysit ONE manual stop/start or
        network-partition step per run, instead of four in a row;
      - a mistake, Ctrl-C, or crash during one scenario's manual step
        doesn't discard the results already collected for the others -
        each scenario writes its own timestamped JSONL log file (see
        common.TrialLogger) and prints its own full Config A/B report.

    pin_reads (default True):
        True  - Config A/B reads are pinned to the EXACT node
                secondary_failure/partition is about to fail/isolate (via
                a custom server_selector - see common.get_pinned_client).
                This guarantees the experiment actually exercises reads
                against the node under test, rather than letting the
                driver's own server selection (SDAM) quietly drift onto a
                healthy node and hide the fault entirely. The trade-off:
                once that pinned node is fully down, EVERY read fails with
                ServerSelectionTimeoutError - Config A and Config B both
                collapse to 100% availability loss / 0 effective
                iterations, so this mode cannot show whether MW holds on
                a SURVIVING node while another one is down.

        False - Config A/B use a completely UNPINNED client. Read
                selection is left entirely to the driver's own SDAM
                heuristics, exactly like a normal production client would
                behave, so reads automatically fail over to a surviving
                secondary once the driver marks the old target
                unavailable. This answers a complementary question: "once
                the driver has failed over to a healthy secondary, does MW
                still hold there (or does the churn from the fresh
                failover expose an out-of-order read)?" - at the cost of
                NOT guaranteeing every read actually hit the failing node
                while it was still reachable.

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
    mode_label = "PINNED to the exact failing/isolated node" if pin_reads \
        else "UNPINNED - driver's own SDAM server selection decides"

    _section(f"TASK 3: MONOTONIC-WRITES - SCENARIO '{scenario_name}' "
             f"({'pinned' if pin_reads else 'unpinned'} reads)")
    print(f"Run started at: {_now()}")
    print(f"Read targeting : {mode_label}")
    print("Cluster topology and starting replication lag:")
    common.print_topology(client)
    common.print_replication_lag(client)

    logger = common.TrialLogger(f"task3_mw_{scenario_name}{mode_suffix}")
    all_run_ids = []

    def run_experiment(scenario_label="unknown", target_node=None):
        if not pin_reads:
            # Deliberately skip pinning: use the plain, un-pinned `client`
            # so Config A/B's readPreference=SECONDARY is resolved by the
            # driver's normal SDAM logic on every single read. If the node
            # currently answering reads is the one about to be stopped/
            # isolated, subsequent reads should automatically fail over to
            # whichever secondary is still healthy - that failover
            # behavior, and whatever it does to MW during the transition,
            # is exactly what this mode is measuring.
            pinned_target = None
            pinned_client = client
            print(f"[pin] pin_reads=False for scenario '{scenario_label}': "
                  f"reads are UNPINNED. The driver's own server selection "
                  f"(SDAM) will choose which secondary answers each read, "
                  f"and should move off a node once its heartbeat marks "
                  f"that node unavailable - that's the transition this run "
                  f"is meant to observe.")
        else:
            # Pin Config A/B reads to a specific node, so "secondary_failure"
            # and "network_partition_minority_isolated" actually exercise
            # reads against the node under test instead of leaving node
            # selection to the driver's SDAM heuristics (which would happily
            # drift onto whichever secondary is still healthy).
            #
            # When scenarios.py hands us `target_node`, it is the exact
            # "host:port" it decided on BEFORE injecting the fault - use it
            # as-is. Only fall back to a fresh discovery
            # (`_discover_pin_target`) when no specific node is under test
            # ("normal", "primary_failure"): re-discovering during
            # secondary_failure/partition would find the failed/isolated
            # node already missing from the topology and silently pin to
            # the surviving node instead.
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
                print(f"[pin] pinning Config A/B secondary reads to "
                      f"{_fmt_addr(pinned_target)} for scenario "
                      f"'{scenario_label}'")
                pinned_client = common.get_pinned_client(pinned_target)

        pinned_db = pinned_client[common.DB_NAME]

        try:
            run_id_a, stats_a = run_config_a(
                pinned_db, pinned_target, logger, scenario_label)
            all_run_ids.append(run_id_a)

            print()
            print("Replication lag after Config A (before starting Config B):")
            try:
                common.print_replication_lag(client)
            except mongo_errors.PyMongoError as exc:
                print(f"[lag] could not read replication lag: {exc}")

            run_id_b, stats_b = run_config_b(
                pinned_client, pinned_db, pinned_target, logger, scenario_label)
            all_run_ids.append(run_id_b)
        finally:
            if pinned_client is not client:
                pinned_client.close()

        print_final_comparison(stats_a, stats_b)
        return {"config_a": stats_a, "config_b": stats_b}

    result = None
    try:
        runner = scenarios.SCENARIO_RUNNERS[scenario_name]
        result = runner(client, run_experiment, logger=logger)
        if result is None:
            print(f"[scenario] '{scenario_name}' was skipped (see message "
                  f"above) - no Config A/B data was collected this run.")
    finally:
        _section("CLEANUP")
        print(f"[{_now()}] removing test documents for run_ids: {all_run_ids}")
        common.cleanup(client, COLLECTION_NAME, all_run_ids)
        logger.close()
        client.close()
        print(f"[{_now()}] connection closed. Run complete.")

    return result

