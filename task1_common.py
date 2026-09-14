#!/usr/bin/env python3
"""
task1_common.py

Shared library for Task 1: client-centric consistency model
READ-YOUR-WRITES (RYW).

This module holds everything that does NOT depend on which scenario is
being run: Config A / Config B implementations, the background load
generator, pinned-target helpers, comparison/summary printers, and
`run_one_scenario(scenario_name)` - the single entry point each of the
four `task1_scenario_*.py` launcher scripts calls to run Task 1 under
exactly one named scenario ("normal", "secondary_failure",
"primary_failure", "network_partition_minority_isolated").

Run one of the launcher scripts directly, e.g.:
    python task1_scenario_normal.py
    python task1_scenario_secondary_failure.py
    python task1_scenario_primary_failure.py
    python task1_scenario_partition.py

Splitting the four scenarios into separate script invocations (instead of
one script that loops through all four, as the original
task1_read_your_writes.py did) means you only have to babysit ONE manual
docker stop/start or network-partition step per run, and a mistake,
Ctrl-C, or crash during one scenario's manual step doesn't discard the
results already collected for the others - each scenario writes its own
timestamped JSONL log file (see common.TrialLogger).

Definition
----------
If a single client writes a value to a data item x, then any subsequent read
of x by that same client must return the value just written (or a newer one).
A client must never see a version of x older than its own most recent write.

How we test it on MongoDB
-------------------------
Every write in a replica set is applied on the PRIMARY first and then
replicated to the SECONDARIES asynchronously. If a client writes to the
primary and immediately reads from a secondary, the secondary may not have
received the write yet -> the client fails to read its own write.

We run two configurations:

  Config A (weak, expected to VIOLATE RYW):
      writeConcern   = w:1        (ack after primary only)
      readConcern    = local
      readPreference = secondary  (read from a specific, pinned secondary)
      NO causal session

  Config B (strong, expected to UPHOLD RYW):
      writeConcern   = majority, j:true
      readConcern    = majority
      readPreference = secondary  (same pinned secondary)
      causal session (causal_consistency=True)

The causal session attaches the operation time of the write to the following
read (afterClusterTime), forcing the secondary to wait until it has replicated
at least up to that point before answering.

Revision notes (why this version differs from a naive read_preference=SECONDARY
implementation)
-----------------------------------------------------------------------------
1. PINNED READ TARGET. Letting the driver's own server-selection logic (SDAM)
   pick which secondary answers each read is not good enough here: SDAM can
   stick to whichever secondary looks healthiest and simply avoid the one
   that scenarios.py is about to fail or network-partition, which would
   silently hide the very condition the scenario is meant to create. For
   "secondary_failure" and "network_partition_minority_isolated",
   scenarios.py passes in the EXACT "host:port" it decided to fail/isolate
   -- chosen BEFORE the fault was injected -- and all Config A/B reads for
   that run are pinned to that exact node via `common.get_pinned_client`.
   (For "normal" and "primary_failure", where no specific secondary is under
   test, the pin target is instead discovered fresh from the current
   topology.) Note that re-discovering "the current secondary" from scratch
   during secondary_failure/partition would NOT work: by the time
   run_experiment() runs, the failed/isolated node has already dropped out
   of the topology, so a fresh discovery would silently pin to the
   surviving node instead of the one under test.
2. AVAILABILITY LOSS IS NOT A CONSISTENCY VIOLATION. When the pinned node is
   down or unreachable, the custom selector yields no candidates and the
   driver raises a ServerSelectionTimeoutError (or similar). `common.
   classify_error` is used to bucket that separately as `availability_loss`
   rather than lumping it into "stale reads" (Config A) or "RYW failures"
   (Config B) - that would overstate the violation rate. This is exactly the
   distinction common.py's TIMEOUT_ERRORS/classify_error was built for.
3. j=True on Config B's write concern. Most WiredTiger deployments default
   writeConcernMajorityJournalDefault=true, so w="majority" alone is USUALLY
   already durable, but making j=True explicit means the "strong" config
   does not silently degrade if that server default is ever changed, and it
   matches the configuration described in the report as "majority + j:true".
4. PER-ITERATION EVIDENCE. Every iteration of both configs is now written to
   the TrialLogger as a single JSON line (scenario, config, iteration index,
   pinned target, outcome, latency), not just scenario start/end markers, so
   the report can cite raw per-trial evidence instead of only console
   summaries.
5. REDUCED LOAD FOR AN UNSTABLE PHONE-HOTSPOT LAN. The original N=200 with
   an uncapped, un-throttled background-load generator (50-doc batches back
   to back) pushes a sustained, fairly high volume of traffic through a
   phone hotspot for the whole run. On a hotspot that can itself drop
   client Wi-Fi connections under sustained load, that traffic is a
   plausible cause of otherwise-unexplained disconnects that would then be
   misread as "node failures". To make results more trustworthy in this
   environment:
     - N was lowered from 200 to 30 (still enough write/read pairs per
       config/scenario to get a meaningful violation rate, but a fraction
       of the total requests).
     - PROGRESS_EVERY was lowered from 20 to 5 to match the smaller N.
     - The background write-load generator now writes smaller batches
       (BACKGROUND_LOAD_BATCH_SIZE, 10 instead of 50) and sleeps
       BACKGROUND_LOAD_SLEEP_SECONDS (0.3s) between batches instead of
       looping as fast as possible, so it still creates enough replication
       lag to exercise Config A without saturating the hotspot's bandwidth.
     - A few socket-level timeouts (find_one max_time_ms, write wtimeout)
       were increased slightly so that ordinary hotspot latency/jitter is
       less likely to be misclassified as an availability loss.
   If you re-run this on a more stable network (e.g. a wired switch), you
   can safely raise N and BACKGROUND_LOAD_BATCH_SIZE back up for a larger,
   more statistically robust sample.
"""

import statistics
import threading
import time
import uuid
from datetime import datetime

from pymongo import ReadPreference
from pymongo import errors as mongo_errors
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


COLLECTION_NAME = "ryw_test"
LOAD_COLLECTION_NAME = "ryw_background_load"

# Number of write-then-read iterations per configuration.
# Lowered from 200 -> 30: on a phone-hotspot LAN, 200 iterations x 2 configs
# x 4 scenarios adds up to a lot of sustained traffic, which can destabilise
# the hotspot itself (and get misread as a "node failure"). 30 is still
# enough to see a clear violation rate on Config A while keeping each
# scenario run short. Raise this back up if you move to a wired network.
N = 30

# Print a progress line every PROGRESS_EVERY iterations.
PROGRESS_EVERY = 5

# Whether to run a background write-load generator during Config A to induce
# realistic secondary replication lag. Set to False to reproduce the
# original (often inconclusive) behaviour.
ENABLE_BACKGROUND_LOAD = True

# Background-load throttling: smaller batches with a short pause between
# them, instead of looping insert_many() as fast as possible. This still
# keeps the secondaries busy enough to lag behind the primary (which is the
# whole point of the load generator) without saturating the hotspot's
# bandwidth for the entire run.
BACKGROUND_LOAD_BATCH_SIZE = 10
BACKGROUND_LOAD_SLEEP_SECONDS = 0.3


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


def _latency_stats(latencies_ms):
    """Return a dict of min/avg/median/p95/max, or None if list is empty."""
    if not latencies_ms:
        return None
    sorted_lat = sorted(latencies_ms)
    p95_idx = max(0, int(len(sorted_lat) * 0.95) - 1)
    return {
        "min": sorted_lat[0],
        "avg": statistics.mean(sorted_lat),
        "median": statistics.median(sorted_lat),
        "p95": sorted_lat[p95_idx],
        "max": sorted_lat[-1],
    }


def _print_latency_stats(label, latencies_ms):
    stats = _latency_stats(latencies_ms)
    if stats is None:
        print(f"{label}: no successful reads recorded, no latency data available")
        return
    print(f"{label} (ms, n={len(latencies_ms)}):")
    print(f"    min={stats['min']:.2f}  median={stats['median']:.2f}  "
          f"avg={stats['avg']:.2f}  p95={stats['p95']:.2f}  max={stats['max']:.2f}")


class BackgroundLoadGenerator:
    """Continuously writes to a throwaway collection with w:1 to keep the
    secondaries busy applying oplog entries, increasing the odds that a
    concurrently-running Config A read will observe a lagging secondary.

    Throttled (small batches + a short sleep between them) so that it still
    produces enough replication lag to be useful without saturating a
    phone-hotspot LAN's bandwidth for the whole run.
    """

    def __init__(self, db):
        self._coll = db.get_collection(
            LOAD_COLLECTION_NAME,
            write_concern=WriteConcern(w=1),
        )
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._docs_written = 0
        self._batches_written = 0

    def _run(self):
        while not self._stop_event.is_set():
            batch = [
                {"ts": time.time(), "junk": "x" * 512}
                for _ in range(BACKGROUND_LOAD_BATCH_SIZE)
            ]
            try:
                self._coll.insert_many(batch, ordered=False)
                self._docs_written += len(batch)
                self._batches_written += 1
            except mongo_errors.PyMongoError:
                # Load generator errors are not part of the measured result;
                # just keep trying.
                pass
            # Throttle: without this the loop hammers the hotspot as fast as
            # the network allows, which is more traffic than needed just to
            # keep the secondaries a little behind the primary.
            self._stop_event.wait(BACKGROUND_LOAD_SLEEP_SECONDS)

    def start(self):
        print(f"[{_now()}] starting background write-load generator "
              f"(collection '{LOAD_COLLECTION_NAME}', batches of "
              f"{BACKGROUND_LOAD_BATCH_SIZE} docs every "
              f"{BACKGROUND_LOAD_SLEEP_SECONDS}s, w:1)")
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=5)
        print(f"[{_now()}] stopped background load generator: "
              f"{self._docs_written} docs written across {self._batches_written} batches")

    @property
    def docs_written(self):
        return self._docs_written


# --------------------------------------------------------------------------
# Pinned-target helpers
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

    This is ONLY safe to use when no specific node is under test - i.e. for
    the "normal" scenario, and for "primary_failure" (there the primary is
    what's down, so any currently-known secondary is a fine, uncontroversial
    read target).

    It must NOT be used to try to guess which node "secondary_failure" or
    "network_partition_minority_isolated" are currently exercising: by the
    time run_experiment() executes, that node has already been stopped or
    isolated and settled for SETTLE_SECONDS, so SDAM will typically have
    already dropped it out of `client.secondaries` - a fresh discovery at
    that point would silently return the SURVIVING secondary instead of the
    one actually under test. For those two scenarios, scenarios.py passes
    the exact target node string it decided on BEFORE injecting the fault;
    see the `target_node` parameter of run_experiment() below.

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
# Config A
# --------------------------------------------------------------------------

def run_config_a(pinned_db, pinned_target, background_db, logger, scenario_label):
    """Weak configuration: no causal session, read from a pinned secondary.
    May violate RYW."""
    _section("TASK 1 / CONFIG A: no causal consistency (expected to VIOLATE RYW)")
    print("Settings   : writeConcern=w:1  readConcern=local  readPreference=secondary")
    print(f"Pinned node: {_fmt_addr(pinned_target) if pinned_target else '(none - un-pinned)'}")
    print("Prediction : since the write is only acknowledged by the primary and the")
    print("             read is forced onto a secondary with no causal token, we")
    print("             EXPECT some reads to miss the document the client just wrote.")
    print(f"Iterations : {N}")
    print(f"Started at : {_now()}")

    coll = pinned_db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern("local"),
        write_concern=WriteConcern(w=1),
    )

    loader = None
    if ENABLE_BACKGROUND_LOAD:
        loader = BackgroundLoadGenerator(background_db)
        loader.start()

    run_id = str(uuid.uuid4())
    counters = {"stale_reads": 0, "read_errors": 0, "availability_losses": 0}
    latencies_ms = []
    stale_indices = []

    _subsection("Running write -> read iterations")
    try:
        for i in range(N):
            doc_id = f"{run_id}-{i}"
            doc = {"_id": doc_id, "run_id": run_id, "seq": i, "value": f"value-{i}"}

            t_write_start = time.perf_counter()
            try:
                # WRITE goes to the primary and is acknowledged after the
                # primary alone has applied it (w:1). The pin selector never
                # interferes with primary selection, so this still works
                # even while the pinned secondary is down/partitioned.
                coll.insert_one(doc)
            except mongo_errors.PyMongoError as exc:
                kind = _classify_and_count(
                    exc, "availability_losses", "read_errors", counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  write failed: {exc!r}")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="A",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="write", outcome=kind)
                continue

            # READ is forced onto the pinned secondary, which may still be
            # catching up (Config A) or may be down/partitioned (failure
            # scenarios) - both are meaningful outcomes, classified below.
            try:
                result = coll.find_one({"_id": doc_id}, max_time_ms=8000)
            except mongo_errors.PyMongoError as exc:
                kind = _classify_and_count(
                    exc, "availability_losses", "read_errors", counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  read raised {type(exc).__name__}: {exc}")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="A",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="read", outcome=kind)
                continue

            # If the client cannot see the document it just wrote, RYW is broken.
            if result is None:
                counters["stale_reads"] += 1
                stale_indices.append(i)
                print(f"[STALE] i={i:03d}  own write not visible on pinned secondary "
                      f"(running total: {counters['stale_reads']})")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="A",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="stale")
            else:
                latency_ms = (time.perf_counter() - t_write_start) * 1000.0
                latencies_ms.append(latency_ms)
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="A",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="ok", latency_ms=latency_ms)
                if (i + 1) % PROGRESS_EVERY == 0:
                    running_rate = counters["stale_reads"] / (i + 1) * 100
                    print(f"[{_now()}] progress {i + 1:03d}/{N}  "
                          f"stale so far={counters['stale_reads']} ({running_rate:.1f}%)  "
                          f"availability losses so far={counters['availability_losses']}  "
                          f"errors so far={counters['read_errors']}")
    finally:
        if loader is not None:
            loader.stop()

    effective_n = N - counters["availability_losses"] - counters["read_errors"]
    print()
    _subsection("Config A summary")
    print(f"Iterations           : {N}")
    print(f"Availability losses  : {counters['availability_losses']} "
          f"(pinned node unreachable - expected during failure/partition, "
          f"NOT counted as a consistency violation)")
    print(f"Read/write errors    : {counters['read_errors']} (non-timeout errors)")
    print(f"Effective iterations : {effective_n} (comparable outcome obtained)")
    print(f"Stale reads          : {counters['stale_reads']} "
          f"({(counters['stale_reads'] / effective_n * 100) if effective_n else 0.0:.1f}% "
          f"of effective iterations)")
    if stale_indices:
        preview = stale_indices[:10]
        suffix = " ..." if len(stale_indices) > 10 else ""
        print(f"Stale at indices     : {preview}{suffix}")
    _print_latency_stats("Write -> visible-read latency", latencies_ms)
    print("Interpretation       :", end=" ")
    if counters["stale_reads"] > 0:
        print("RYW VIOLATION observed - the weak configuration failed to")
        print("                       guarantee that the client could read its own write,")
        print("                       exactly as predicted.")
    elif effective_n == 0:
        print("no effective iterations completed (pinned node was unavailable")
        print("                       for the whole run) - this is an availability outcome,")
        print("                       not evidence either way about RYW.")
    else:
        print("no stale read this run. This does NOT mean the configuration")
        print("                       guarantees RYW - it only means replication happened to")
        print("                       catch up before every read in this particular run; the")
        print("                       configuration still offers no such guarantee in general.")
    print(f"Finished at          : {_now()}")

    return run_id, {
        "stale_reads": counters["stale_reads"],
        "read_errors": counters["read_errors"],
        "availability_losses": counters["availability_losses"],
        "latencies_ms": latencies_ms,
        "iterations": N,
    }


# --------------------------------------------------------------------------
# Config B
# --------------------------------------------------------------------------

def run_config_b(pinned_client, pinned_db, pinned_target, logger, scenario_label):
    """Strong configuration: causal session + majority (w + j). Expected to
    uphold RYW."""
    _section("TASK 1 / CONFIG B: causal session (expected to UPHOLD RYW)")
    print("Settings   : writeConcern=majority,j=true  readConcern=majority  "
          "readPreference=secondary")
    print(f"Pinned node: {_fmt_addr(pinned_target) if pinned_target else '(none - un-pinned)'}")
    print("Prediction : the causal session propagates the write's operation time to the")
    print("             following read (afterClusterTime), forcing the secondary to wait")
    print("             until it has applied that write before answering, so we EXPECT")
    print("             every read to see the client's own write.")
    print(f"Iterations : {N}")
    print(f"Started at : {_now()}")

    coll = pinned_db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern("majority"),
        write_concern=WriteConcern(w="majority", j=True, wtimeout=15000),
    )

    run_id = str(uuid.uuid4())
    counters = {"failures": 0, "errors": 0, "availability_losses": 0}
    latencies_ms = []
    failure_indices = []

    _subsection("Running write -> read iterations (single causal session)")
    with pinned_client.start_session(causal_consistency=True) as session:
        for i in range(N):
            doc_id = f"{run_id}-{i}"
            doc = {"_id": doc_id, "run_id": run_id, "seq": i, "value": f"value-{i}"}

            t_write_start = time.perf_counter()
            try:
                coll.insert_one(doc, session=session)
            except mongo_errors.PyMongoError as exc:
                kind = _classify_and_count(
                    exc, "availability_losses", "errors", counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  write failed: {exc!r}")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="write", outcome=kind)
                continue

            try:
                result = coll.find_one({"_id": doc_id}, session=session, max_time_ms=15000)
            except mongo_errors.PyMongoError as exc:
                # A timeout here can mean the pinned secondary is down/
                # partitioned (availability_loss) OR that it could not catch
                # up to the causal read point within max_time_ms in time
                # (also classified as availability_loss by common.py, since
                # both are "the system could not complete the op in time"
                # rather than a wrong-answer consistency violation).
                kind = _classify_and_count(
                    exc, "availability_losses", "errors", counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] i={i:03d}  read raised {type(exc).__name__} "
                      f"(pinned secondary unreachable or could not catch up in time)")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                stage="read", outcome=kind)
                continue

            if result is None or result.get("value") != f"value-{i}":
                counters["failures"] += 1
                failure_indices.append(i)
                print(f"[FAIL] i={i:03d}  own write not visible "
                      f"(running total: {counters['failures']})")
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="fail")
            else:
                latency_ms = (time.perf_counter() - t_write_start) * 1000.0
                latencies_ms.append(latency_ms)
                if logger:
                    logger.log(task="task1_ryw", scenario=scenario_label, config="B",
                                run_id=run_id, i=i, pinned_target=_fmt_addr(pinned_target),
                                outcome="ok", latency_ms=latency_ms)
                if (i + 1) % PROGRESS_EVERY == 0:
                    print(f"[{_now()}] progress {i + 1:03d}/{N}  "
                          f"failures so far={counters['failures']}  "
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
    print(f"Failures (RYW)       : {counters['failures']} "
          f"({(counters['failures'] / effective_n * 100) if effective_n else 0.0:.1f}% "
          f"of effective iterations)")
    if failure_indices:
        preview = failure_indices[:10]
        suffix = " ..." if len(failure_indices) > 10 else ""
        print(f"Failed at indices    : {preview}{suffix}")
    _print_latency_stats("Write -> visible-read latency", latencies_ms)
    print("Interpretation       :", end=" ")
    if counters["failures"] == 0:
        print("PASS - read-your-writes held for every effective iteration, as")
        print("                       predicted by the causal-session + majority(j=true)")
        print("                       configuration.")
    else:
        print("FAIL - an RYW violation was detected even with a causal session,")
        print("                       which contradicts the prediction and should be")
        print("                       investigated (e.g. driver/server version, clock skew,")
        print("                       or a bug in the causal-token handling).")
    print(f"Finished at          : {_now()}")

    return run_id, {
        "failures": counters["failures"],
        "errors": counters["errors"],
        "availability_losses": counters["availability_losses"],
        "latencies_ms": latencies_ms,
        "iterations": N,
    }


# --------------------------------------------------------------------------
# Comparisons / summaries
# --------------------------------------------------------------------------

def print_final_comparison(stats_a, stats_b):
    """Side-by-side summary of both configurations for quick reporting."""
    _section("FINAL COMPARISON: Config A (weak) vs Config B (strong)")

    stale_a = stats_a["stale_reads"]
    avail_a = stats_a["availability_losses"]
    err_a = stats_a["read_errors"]
    n_a = stats_a["iterations"]
    eff_a = n_a - avail_a - err_a

    fail_b = stats_b["failures"]
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
    print(f"{'RYW violations':<32}{stale_a:<22}{fail_b:<22}")
    pct_a = f"{stale_a / eff_a * 100:.1f}%" if eff_a else "n/a"
    pct_b = f"{fail_b / eff_b * 100:.1f}%" if eff_b else "n/a"
    print(f"{'Violation rate (of eff.)':<32}{pct_a:<22}{pct_b:<22}")
    print(f"{'Other read/write errors':<32}{err_a:<22}{err_b:<22}")

    stats_a_lat = _latency_stats(stats_a["latencies_ms"])
    stats_b_lat = _latency_stats(stats_b["latencies_ms"])
    avg_a = f"{stats_a_lat['avg']:.2f} ms" if stats_a_lat else "n/a"
    avg_b = f"{stats_b_lat['avg']:.2f} ms" if stats_b_lat else "n/a"
    print(f"{'Avg write->read latency':<32}{avg_a:<22}{avg_b:<22}")

    print()
    print("Takeaway:")
    if stale_a > 0 and fail_b == 0:
        print("  Results match the prediction: the weak configuration (w:1 / local /")
        print("  no causal session) exposed read-your-writes violations, while the")
        print("  strong configuration (majority+j / majority / causal session) did not,")
        print("  at the cost of somewhat higher write->read latency.")
    elif stale_a == 0 and eff_a > 0:
        print("  Config A did not produce any observed violation in this run. This")
        print("  does not disprove the weak configuration's lack of an RYW guarantee -")
        print("  consider increasing N, increasing background load, or introducing")
        print("  artificial network delay between primary and the pinned secondary.")
    elif fail_b > 0:
        print("  Config B unexpectedly showed a violation; see the [FAIL] lines above")
        print("  and investigate before writing this up as a clean result.")
    else:
        print("  One or both configs spent this run mostly unavailable (pinned node")
        print("  down/partitioned); see 'Availability losses' above before drawing any")
        print("  conclusion about consistency from this particular scenario run.")


def print_scenario_summary(scenario_results):
    """
    Cross-scenario summary: RYW violation rate (of effective iterations) and
    availability-loss counts for Config A / Config B under each injected
    scenario, so the report can show whether the injected failures change
    the observed behaviour.
    """
    _section("CROSS-SCENARIO SUMMARY (Task 1: Read-Your-Writes)")
    header = (f"{'Scenario':<38}{'A viol%':<10}{'A avail-loss':<14}"
              f"{'B viol%':<10}{'B avail-loss':<14}")
    print(header)
    print("-" * len(header))
    for label, result in scenario_results.items():
        if not result:
            print(f"{label:<38}{'n/a':<10}{'n/a':<14}{'n/a':<10}{'n/a':<14}")
            continue
        stats_a = result["config_a"]
        stats_b = result["config_b"]
        eff_a = stats_a["iterations"] - stats_a["availability_losses"] - stats_a["read_errors"]
        eff_b = stats_b["iterations"] - stats_b["availability_losses"] - stats_b["errors"]
        pct_a = (stats_a["stale_reads"] / eff_a * 100) if eff_a else 0.0
        pct_b = (stats_b["failures"] / eff_b * 100) if eff_b else 0.0
        print(f"{label:<38}{f'{pct_a:.1f}%':<10}{stats_a['availability_losses']:<14}"
              f"{f'{pct_b:.1f}%':<10}{stats_b['availability_losses']:<14}")
    print()


# --------------------------------------------------------------------------
# Main
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
    Run Task 1 (Config A vs Config B, write-then-read) under exactly ONE
    named scenario, then clean up and return that scenario's result dict
    (or None if the scenario had to be skipped - e.g. no secondary
    currently discoverable to fail/isolate).

    This is the single entry point each of the task1_scenario_*.py
    launcher scripts calls. Splitting scenarios into separate script
    invocations means:
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
                This is what every task1_scenario_*.py script has used so
                far: it guarantees the experiment actually exercises reads
                against the node under test, rather than letting the
                driver's own server selection (SDAM) quietly drift onto a
                healthy node and hide the fault entirely. The trade-off:
                once that pinned node is fully down, EVERY read fails with
                ServerSelectionTimeoutError - Config A and Config B both
                collapse to 100% availability loss / 0 effective
                iterations, so this mode cannot show whether RYW holds on
                a SURVIVING node while another one is down.

        False - Config A/B use a completely UNPINNED client. Read
                selection is left entirely to the driver's own SDAM
                heuristics, exactly like a normal production client would
                behave. When the node currently serving reads is the one
                that secondary_failure/partition takes down, the driver's
                heartbeat mechanism (~10s interval, or immediate on a
                failed op) marks it unavailable and later reads should
                automatically move to a surviving secondary instead of
                failing outright. This answers a different, complementary
                question from the pinned mode: "once the driver has
                failed over to a healthy secondary, does RYW still hold
                there (or does the churn from the fresh failover make
                Config A even more likely to serve a stale read)?" - at
                the cost of NOT guaranteeing every read actually hit the
                failing node while it was still reachable (some early
                reads, right after the fault is injected but before SDAM's
                heartbeat notices, may still land on the dying node and
                show up as availability losses too - that's expected and
                is exactly the failover transition being observed, not a
                bug).

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

    _section(f"TASK 1: READ-YOUR-WRITES - SCENARIO '{scenario_name}' "
             f"({'pinned' if pin_reads else 'unpinned'} reads)")
    print(f"Run started at: {_now()}")
    print(f"Read targeting : {mode_label}")
    print("Cluster topology and starting replication lag:")
    common.print_topology(client)
    common.print_replication_lag(client)

    db = client[common.DB_NAME]
    logger = common.TrialLogger(f"task1_ryw_{scenario_name}{mode_suffix}")
    all_run_ids = []

    def run_experiment(scenario_label="unknown", target_node=None):
        if not pin_reads:
            # Deliberately skip pinning: use the plain, un-pinned `client`
            # so Config A/B's readPreference=SECONDARY is resolved by the
            # driver's normal SDAM logic on every single read. If the node
            # currently answering reads is the one about to be stopped/
            # isolated, subsequent reads should automatically fail over to
            # whichever secondary is still healthy - that failover
            # behavior, and whatever it does to RYW during the transition,
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
                pinned_db, pinned_target, db, logger, scenario_label)
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
        try:
            client[common.DB_NAME][LOAD_COLLECTION_NAME].drop()
            print(f"[{_now()}] dropped background-load collection "
                  f"'{LOAD_COLLECTION_NAME}'")
        except mongo_errors.PyMongoError as exc:
            print(f"[{_now()}] could not drop background-load collection: {exc!r}")
        logger.close()
        client.close()
        print(f"[{_now()}] connection closed. Run complete.")

    return result
