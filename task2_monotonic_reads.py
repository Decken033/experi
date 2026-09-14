#!/usr/bin/env python3
"""
task2_monotonic_reads.py

Client-centric consistency model: MONOTONIC READS (MR).

Definition
----------
If a client reads the value of a data item x, then any successive read of x
by that same client should return the same value or a more recent value.

In other words, once a client has observed version k, it should never later
observe an older version such as k-1.

How we test it on MongoDB
-------------------------
With readPreference=SECONDARY, reads are served by eligible secondary members.

If different secondaries have different replication lag, consecutive reads
may observe different replica states. For example:

    secondary A: seq = 42
    secondary B: seq = 37

If the client first reads seq=42 and then later reads seq=37, time appears
to go backwards. This is a monotonic-reads violation.

Setup
-----
* A background writer thread continuously increments a counter document on
  the primary:

      seq = 0, 1, 2, 3, ...

* A reader repeatedly reads that same document from secondary replicas.

Config A (weak configuration)
-----------------------------
    readConcern = local
    readPreference = secondary
    no causal session

This configuration does NOT guarantee monotonic reads.

If different secondaries have different replication lag, a client may read
a newer version from one secondary and then an older version from another.

IMPORTANT IMPLEMENTATION NOTE
------------------------------
Letting the MongoDB driver's automatic server-selection logic pick which
secondary serves each read (SDAM) is NOT reliable for this experiment: the
driver only re-evaluates server latency/state on its own heartbeat schedule
(default ~10s), which is much longer than a short read loop. In practice this
means the driver can "stick" to a single secondary for the whole loop even
though a second, lagging secondary exists, in which case reads never bounce
between the fresh and stale replicas and no violation can be observed.

To make the intended divergence deterministic and script-controllable, this
version opens ONE direct connection (directConnection=True) per secondary and
explicitly alternates reads between them from user code. This still measures
the same phenomenon (reading readPreference=secondary-eligible nodes with
different replication state) but removes the driver's internal scheduling as
a confound.

Config B (stronger configuration)
---------------------------------
    readConcern = majority
    readPreference = secondary
    causal_consistency = True

The causal session carries forward causal information between operations.
Subsequent reads must observe a state that satisfies the session's causal
ordering, preventing the client from observing an older version after it
has already observed a newer one.
"""

import copy
import threading
import time
import uuid
from collections import Counter

from pymongo import MongoClient, ReadPreference
from pymongo import errors as mongo_errors
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

import common
import scenarios


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

COLLECTION_NAME = "mr_test"

# Number of read attempts for each configuration.
READS = 400

# Maximum amount of time the writer runs.
WRITER_SECONDS = 30

# Writer interval: ~200 writes per second.
WRITE_INTERVAL = 0.005

# Reader interval.
READ_INTERVAL = 0.005

# If True, Config A makes one secondary lag so an MR violation actually
# becomes observable. Set to False to run the "pure" weak config (which may
# legitimately report zero violations on a low-lag cluster).
INJECT_LAG = True

# How to create the lag:
#   "delay"     - CLIENT ONLY. Reconfigure one secondary as a delayed member
#                 via replSetReconfig (no server restart, no test commands,
#                 no shell on the nodes). Reverted automatically afterwards.
#                 This is the default and the recommended method.
#   "failpoint" - Pause one secondary's oplog application with a server
#                 failpoint. Requires mongod started with enableTestCommands=1.
LAG_METHOD = "delay"

# For LAG_METHOD="delay": how many seconds the delayed secondary trails the
# primary once it reaches steady state.
DELAY_SECS = 8

# How long to wait after reconfiguring, before starting to read, so the
# delayed secondary has time to reach ~DELAY_SECS of steady-state lag
# (secondaryDelaySecs ramps up to this value; it does not appear instantly).
LAG_WARMUP_SECS = DELAY_SECS + 2

# How often (in number of reads) to print a progress line during a read loop.
# Set to 0 / None to disable progress printing.
PROGRESS_EVERY = 50

# If True, print every single read (seq value + which node served it).
# Useful for close inspection, but very verbose for READS=400.
VERBOSE_EVERY_READ = False


# ------------------------------------------------------------
# Small output helpers
# ------------------------------------------------------------

def _fmt_addr(address):
    if address is None:
        return "unknown"
    host, port = address
    return f"{host}:{port}"


def _print_progress(i, total, seq, address, violations):
    """Print a periodic progress line during a read loop."""
    if not PROGRESS_EVERY:
        return
    if (i + 1) % PROGRESS_EVERY == 0 or (i + 1) == total:
        print(
            f"    ... progress {i + 1:>4}/{total}  "
            f"last_seq={seq!s:<6} from={_fmt_addr(address):<20} "
            f"violations_so_far={violations}"
        )


def _classify_and_count(exc, counters):
    """
    Bucket a read/write exception into 'availability_loss' (the pinned/
    selected node was unreachable or timed out - e.g. during an injected
    node failure or network partition) vs a generic 'error', and bump the
    matching counter in-place. Mirrors task1's error handling so results
    from all four tasks stay comparable in the report.
    """
    kind = common.classify_error(exc)
    if kind == "availability_loss":
        counters["availability_losses"] += 1
    else:
        counters["errors"] += 1
    return kind


# ------------------------------------------------------------
# Writer thread
# ------------------------------------------------------------

class Writer(threading.Thread):
    """
    Background writer that continuously increments seq.

    Writes use w=1 so the primary acknowledges the write immediately without
    waiting for all replicas. This allows secondaries to temporarily have
    different replication progress, which creates the opportunity to observe
    a monotonic-reads violation in Config A.
    """

    def __init__(self, db, doc_id):
        super().__init__(daemon=True)

        self.doc_id = doc_id

        self.coll = db.get_collection(
            COLLECTION_NAME,
            write_concern=WriteConcern(w=1),
        )

        # Do NOT call this self._stop.
        #
        # threading.Thread internally has a private _stop() method.
        # Replacing it with threading.Event() causes writer.join() to fail
        # with:
        #
        # TypeError: 'Event' object is not callable
        self._stop_event = threading.Event()

        # Exposed so the main thread can report how far the writer got.
        self.last_seq = 0
        self.write_count = 0
        # Writes that failed (primary unreachable/mid-election during a
        # node-failure or partition scenario). Exposed so the report can
        # tell "the writer stalled because the primary was down" apart from
        # "the writer stalled because the reader loop found zero violations".
        self.write_errors = 0

    def run(self):
        seq = 0
        deadline = time.time() + WRITER_SECONDS

        print(f"[writer] started for doc_id={self.doc_id!r} (w=1, up to {WRITER_SECONDS}s)")

        while (
            not self._stop_event.is_set()
            and time.time() < deadline
        ):
            seq += 1

            try:
                self.coll.update_one(
                    {"_id": self.doc_id},
                    {"$set": {"seq": seq}},
                    upsert=True,
                )
            except mongo_errors.PyMongoError as exc:
                # During an injected primary failure the primary can be
                # briefly unreachable while a new one is elected. Retry the
                # SAME seq instead of letting the thread die silently (a
                # silent death would look, in the report, exactly like "no
                # MR violation" instead of "the writer stopped advancing").
                self.write_errors += 1
                seq -= 1
                print(f"[writer] WARNING: write failed ({type(exc).__name__}: {exc}); "
                      f"retrying seq={seq + 1}")
                time.sleep(0.2)
                continue

            self.last_seq = seq
            self.write_count += 1

            time.sleep(WRITE_INTERVAL)

        print(
            f"[writer] stopped for doc_id={self.doc_id!r} "
            f"(wrote {self.write_count} updates, final seq={self.last_seq})"
        )

    def stop(self):
        """Request the writer thread to stop."""
        self._stop_event.set()


# ------------------------------------------------------------
# Replication-lag injection
# ------------------------------------------------------------
#
# A zero-violation run of Config A is a valid outcome, but it only happens
# because on a fast LAN both secondaries stay fully caught up (lag ~= 0), so
# there is never a "backwards in time" window to catch.
#
# To make the violation observable we deliberately create divergence between
# the two secondaries. Two ways to do that are implemented below; both are
# wrapped in a context manager so cleanup always happens, even if the
# experiment body raises.


class DelayedSecondaryInjector:
    """
    Context manager: temporarily turn one secondary into a delayed member
    using ONLY replSetReconfig (no failpoints, no enableTestCommands, no
    server restart, no shell access to the nodes needed).

    While active, the target secondary keeps replicating, but its applied
    oplog position stays `delay_secs` seconds behind the primary's wall
    clock. The other secondary keeps up normally.

    On exit, the original member config (priority, no delay) is restored.
    """

    def __init__(self, client, host_port, delay_secs=DELAY_SECS):
        self.client = client
        self.host, self.port = host_port
        self.member_host = f"{self.host}:{self.port}"
        self.delay_secs = delay_secs
        self._original_priority = None
        self._applied = False

    def _get_config(self):
        result = self.client.admin.command("replSetGetConfig")
        return result["config"]

    def _reconfig(self, config):
        config = copy.deepcopy(config)
        config["version"] = config["version"] + 1
        self.client.admin.command({"replSetReconfig": config})

    def _find_member(self, config):
        for member in config["members"]:
            if member["host"] == self.member_host:
                return member
        raise RuntimeError(
            f"member '{self.member_host}' not found in replica set config; "
            f"known members: {[m['host'] for m in config['members']]}"
        )

    def __enter__(self):
        print(f"[lag] preparing to inject lag on {self.member_host} "
              f"(target={self.delay_secs}s, method=replSetReconfig)")
        try:
            config = self._get_config()
            member = self._find_member(config)

            self._original_priority = member.get("priority", 1)
            print(
                f"[lag] current config for {self.member_host}: "
                f"priority={self._original_priority}, "
                f"secondaryDelaySecs={member.get('secondaryDelaySecs', 0)}"
            )

            # secondaryDelaySecs requires priority=0.
            member["priority"] = 0
            member["secondaryDelaySecs"] = self.delay_secs

            self._reconfig(config)
            self._applied = True

            print(
                f"[lag] reconfigured {self.member_host} as a delayed member "
                f"(secondaryDelaySecs={self.delay_secs}) via replSetReconfig"
            )
        except Exception as exc:  # noqa: BLE001 - fall back gracefully
            print(
                f"[lag] WARNING: replSetReconfig failed ({exc}); "
                f"Config A will run without injected lag."
            )
            self._applied = False

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._applied:
            print(f"[lag] nothing to restore for {self.member_host} "
                  f"(injection was never applied)")
            return False

        try:
            config = self._get_config()
            member = self._find_member(config)
            member["priority"] = self._original_priority
            member.pop("secondaryDelaySecs", None)
            self._reconfig(config)

            print(
                f"[lag] restored {self.member_host} to a normal secondary "
                f"(priority={self._original_priority}, delay removed)"
            )
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup
            print(f"[lag] WARNING: could not restore replica set config: {exc}")

        # Do not suppress exceptions from the experiment body.
        return False


# Failpoint names that pause oplog application. The name has changed across
# MongoDB versions, so we try them in order and use whichever the server
# accepts. Kept as an alternative to DelayedSecondaryInjector; requires the
# target mongod to be started with enableTestCommands=1.
_LAG_FAILPOINTS = ["rsSyncApplyStop", "pauseBatchApplication"]


class FailpointLagInjector:
    """
    Context manager: pause oplog application on one secondary for its lifetime
    using a server failpoint. Requires enableTestCommands=1 on the target node.
    """

    def __init__(self, client, host_port):
        self.host, self.port = host_port
        self.direct_client = None
        self.failpoint = None

    def __enter__(self):
        print(f"[lag] preparing to pause oplog application on "
              f"{self.host}:{self.port} (method=failpoint)")

        self.direct_client = MongoClient(
            self.host,
            self.port,
            directConnection=True,
            serverSelectionTimeoutMS=5000,
        )

        last_exc = None
        for fp in _LAG_FAILPOINTS:
            try:
                self.direct_client.admin.command(
                    {"configureFailPoint": fp, "mode": "alwaysOn"}
                )
                self.failpoint = fp
                print(
                    f"[lag] paused oplog application on "
                    f"{self.host}:{self.port} via failpoint '{fp}'"
                )
                return self
            except Exception as exc:  # noqa: BLE001 - probing which fp exists
                print(f"[lag] failpoint '{fp}' not usable ({exc}), trying next...")
                last_exc = exc

        print(
            f"[lag] WARNING: could not enable a lag failpoint on "
            f"{self.host}:{self.port} ({last_exc}); "
            f"Config A will run without injected lag."
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.direct_client is not None and self.failpoint is not None:
            try:
                self.direct_client.admin.command(
                    {"configureFailPoint": self.failpoint, "mode": "off"}
                )
                print(
                    f"[lag] resumed oplog application on "
                    f"{self.host}:{self.port}"
                )
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                print(f"[lag] WARNING: could not disable failpoint: {exc}")

        if self.direct_client is not None:
            self.direct_client.close()

        return False


def make_lag_injector(client, host_port):
    """Factory: build the configured lag-injection context manager."""
    if LAG_METHOD == "delay":
        return DelayedSecondaryInjector(client, host_port)
    if LAG_METHOD == "failpoint":
        return FailpointLagInjector(client, host_port)
    raise ValueError(f"unknown LAG_METHOD: {LAG_METHOD!r}")


def _print_served_by(served_by):
    """Show how reads were distributed across the secondaries."""
    print("Served by     :")
    if not served_by:
        print("    (no reads were attributed to a specific node)")
        return
    total = sum(served_by.values())
    for (host, port), count in sorted(served_by.items()):
        pct = 100.0 * count / total if total else 0.0
        print(f"    {host}:{port:<7} {count:>4} reads  ({pct:5.1f}%)")


# ------------------------------------------------------------
# Direct per-secondary read helper
# ------------------------------------------------------------
#
# Relying on the driver's automatic SECONDARY server selection to "naturally"
# bounce between two secondaries turned out to be unreliable in practice: the
# driver only refreshes its view of server state/latency on its own heartbeat
# schedule (default ~10s), which is much longer than a short read loop. That
# let it stick to a single (fresh) secondary for an entire run, so the
# lagging node was never actually read from and no violation could occur.
#
# To make the experiment deterministic, we instead open one directConnection
# client per secondary and alternate between them explicitly from Python.


def _make_direct_secondary_client(host_port):
    host, port = host_port
    print(f"[setup] opening direct connection to secondary {host}:{port}")
    return MongoClient(
        host,
        port,
        directConnection=True,
        # directConnection pins the socket to this exact node; we still need
        # read_preference != PRIMARY or the driver will refuse to read from
        # a secondary.
        read_preference=ReadPreference.SECONDARY_PREFERRED,
        serverSelectionTimeoutMS=5000,
    )


# ------------------------------------------------------------
# Config A
# ------------------------------------------------------------

def run_config_a(client, db, doc_id, lag_secondary=None, normal_secondary=None,
                  logger=None, scenario_label="unknown"):
    """
    Config A:

        readConcern = local
        readPreference = secondary
        no causal session

    This configuration does not guarantee monotonic reads.

    lag_secondary, normal_secondary : (host, port) or None
        When both are given, reads explicitly alternate between these two
        secondaries (see module docstring for why this replaces relying on
        the driver's automatic server selection). When either is missing,
        falls back to a single collection with readPreference=secondary and
        lets the driver choose (violations are then unlikely / a valid
        "no violation" result).
    """

    print("=" * 60)
    print("TASK 2 / CONFIG A")
    print("No causal session - expected to allow MR violations")
    print("readConcern=local")
    print("readPreference=secondary")
    print(f"doc_id        = {doc_id!r}")
    if lag_secondary is not None:
        print(
            f"injected lag  : delaying {lag_secondary[0]}:{lag_secondary[1]} "
            f"via {LAG_METHOD}"
        )
    if lag_secondary is not None and normal_secondary is not None:
        print(
            f"read pattern  : explicitly alternating between "
            f"{normal_secondary[0]}:{normal_secondary[1]} (fresh) and "
            f"{lag_secondary[0]}:{lag_secondary[1]} (lagging)"
        )
    else:
        print("read pattern  : driver-selected secondary (SDAM default)")
    print("=" * 60)

    highest_seen = -1
    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    successful_reads = 0
    served_by = Counter()

    def record_read(i, doc, address):
        nonlocal highest_seen, successful_reads

        if address is not None:
            served_by[address] += 1

        if not doc or "seq" not in doc:
            if VERBOSE_EVERY_READ:
                print(f"    [read {i + 1:>4}] from={_fmt_addr(address):<20} doc not found yet")
            _print_progress(i, READS, highest_seen, address, counters["violations"])
            if logger:
                logger.log(task="task2_mr", scenario=scenario_label, config="A",
                            doc_id=doc_id, i=i, served_by=_fmt_addr(address),
                            outcome="not_found")
            return

        successful_reads += 1
        seq = doc["seq"]

        is_violation = seq < highest_seen

        if VERBOSE_EVERY_READ:
            marker = " <-- VIOLATION" if is_violation else ""
            print(
                f"    [read {i + 1:>4}] from={_fmt_addr(address):<20} "
                f"seq={seq}{marker}"
            )

        if is_violation:
            counters["violations"] += 1
            print(
                f"[MR VIOLATION] read #{i + 1}: "
                f"saw seq={seq} after previously seeing {highest_seen} "
                f"(served by {_fmt_addr(address)})"
            )

        if logger:
            logger.log(task="task2_mr", scenario=scenario_label, config="A",
                        doc_id=doc_id, i=i, served_by=_fmt_addr(address),
                        seq=seq, highest_before=highest_seen,
                        outcome="violation" if is_violation else "ok")

        highest_seen = max(highest_seen, seq)
        _print_progress(i, READS, seq, address, counters["violations"])

    def _read_error(i, address, exc):
        kind = _classify_and_count(exc, counters)
        tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
        print(f"[{tag}] read #{i + 1} from={_fmt_addr(address):<20} "
              f"raised {type(exc).__name__}: {exc}")
        if logger:
            logger.log(task="task2_mr", scenario=scenario_label, config="A",
                        doc_id=doc_id, i=i, served_by=_fmt_addr(address),
                        outcome=kind)

    def read_loop_alternating(coll_normal, coll_lagged):
        # Alternate: fresh, lagged, fresh, lagged, ...
        # This deterministically creates "new value, then old value" windows.
        colls = [coll_normal, coll_lagged]
        addrs = [normal_secondary, lag_secondary]

        print(f"[read] starting alternating read loop ({READS} reads)...")
        for i in range(READS):
            coll = colls[i % 2]
            address = addrs[i % 2]

            try:
                doc = coll.find_one(
                    {"_id": doc_id},
                    max_time_ms=5000,
                )
            except mongo_errors.PyMongoError as exc:
                # Expected when the pinned node (e.g. the one we just
                # delayed, or one that a node-failure/partition scenario is
                # currently failing/isolating) cannot answer in time - an
                # availability outcome, not proof of a consistency violation.
                _read_error(i, address, exc)
                time.sleep(READ_INTERVAL)
                continue
            record_read(i, doc, address)
            time.sleep(READ_INTERVAL)
        print("[read] alternating read loop finished")

    def read_loop_driver_selected(coll):
        print(f"[read] starting driver-selected read loop ({READS} reads)...")
        for i in range(READS):
            try:
                cursor = coll.find({"_id": doc_id}).limit(1).max_time_ms(5000)
                doc = next(cursor, None)
                address = cursor.address
            except mongo_errors.PyMongoError as exc:
                _read_error(i, None, exc)
                time.sleep(READ_INTERVAL)
                continue
            record_read(i, doc, address)
            time.sleep(READ_INTERVAL)
        print("[read] driver-selected read loop finished")

    if lag_secondary is not None and normal_secondary is not None:
        with make_lag_injector(client, lag_secondary):
            # Give the lagging node time to reach steady-state lag before we
            # start reading, so every read during the loop sees a real gap.
            print(f"[lag] warming up for {LAG_WARMUP_SECS}s so lag reaches steady state...")
            time.sleep(LAG_WARMUP_SECS)

            print("Lag after injection (lagging node should be > 0):")
            common.print_replication_lag(db.client)

            client_normal = _make_direct_secondary_client(normal_secondary)
            client_lagged = _make_direct_secondary_client(lag_secondary)

            try:
                coll_normal = client_normal[common.DB_NAME].get_collection(
                    COLLECTION_NAME,
                    read_concern=ReadConcern("local"),
                )
                coll_lagged = client_lagged[common.DB_NAME].get_collection(
                    COLLECTION_NAME,
                    read_concern=ReadConcern("local"),
                )
                read_loop_alternating(coll_normal, coll_lagged)
            finally:
                print("[setup] closing direct secondary connections")
                client_normal.close()
                client_lagged.close()
    else:
        coll = db.get_collection(
            COLLECTION_NAME,
            read_preference=ReadPreference.SECONDARY,
            read_concern=ReadConcern("local"),
        )
        read_loop_driver_selected(coll)

    effective = READS - counters["availability_losses"] - counters["errors"]
    print()
    print("-" * 60)
    print("CONFIG A SUMMARY")
    print("-" * 60)
    print(f"Read attempts       : {READS}")
    print(f"Availability losses : {counters['availability_losses']} "
          f"(pinned/selected node unreachable or timed out - expected during "
          f"failure/partition scenarios, NOT counted as a consistency violation)")
    print(f"Other errors        : {counters['errors']}")
    print(f"Effective reads     : {effective}")
    print(f"Successful (found)  : {successful_reads}")
    print(f"Highest seq         : {highest_seen}")
    print(f"Violations          : {counters['violations']} "
          f"({(counters['violations'] / effective * 100) if effective else 0.0:.1f}% "
          f"of effective reads)")
    _print_served_by(served_by)

    if counters["violations"] > 0:
        print("RESULT              : MR VIOLATION observed")
    elif effective == 0:
        print("RESULT              : no effective reads completed (node unavailable "
              "for the whole run) - an availability outcome, not evidence either "
              "way about MR")
    else:
        print(
            "RESULT              : no violation observed in this run; "
            "Config A does NOT guarantee MR"
        )

    print()

    return {
        "reads": READS,
        "successful": successful_reads,
        "highest_seq": highest_seen,
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "effective": effective,
    }


# ------------------------------------------------------------
# Config B
# ------------------------------------------------------------

def run_config_b(client, db, doc_id, logger=None, scenario_label="unknown"):
    """
    Config B:

        readConcern = majority
        readPreference = secondary
        causal_consistency = True

    The same session is used for all reads.

    The causal session carries causal ordering information forward between
    operations so the client should not observe a version older than one
    already observed during that session.
    """

    print("=" * 60)
    print("TASK 2 / CONFIG B")
    print("Causal session - expected to uphold MR")
    print("readConcern=majority")
    print("readPreference=secondary")
    print("causal_consistency=True")
    print(f"doc_id        = {doc_id!r}")
    print("=" * 60)

    coll = db.get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern("majority"),
    )

    highest_seen = -1
    counters = {"violations": 0, "availability_losses": 0, "errors": 0}
    successful_reads = 0
    served_by = Counter()

    print(f"[read] starting causal-session read loop ({READS} reads)...")

    with client.start_session(
        causal_consistency=True
    ) as session:

        for i in range(READS):
            try:
                cursor = (
                    coll.find({"_id": doc_id}, session=session)
                    .limit(1)
                    .max_time_ms(10000)
                )
                doc = next(cursor, None)
                address = cursor.address
            except mongo_errors.PyMongoError as exc:
                # A timeout/unreachable-node error here means the selected
                # secondary could not be reached or could not catch up to
                # the causal read point in time - an availability outcome
                # (expected during a node-failure/partition scenario), not a
                # wrong-answer consistency violation.
                kind = _classify_and_count(exc, counters)
                tag = "AVAILABILITY" if kind == "availability_loss" else "ERROR"
                print(f"[{tag}] read #{i + 1} raised {type(exc).__name__}: {exc}")
                if logger:
                    logger.log(task="task2_mr", scenario=scenario_label, config="B",
                                doc_id=doc_id, i=i, outcome=kind)
                time.sleep(READ_INTERVAL)
                continue

            if address is not None:
                served_by[address] += 1

            if not doc or "seq" not in doc:
                if VERBOSE_EVERY_READ:
                    print(f"    [read {i + 1:>4}] from={_fmt_addr(address):<20} doc not found yet")
                _print_progress(i, READS, highest_seen, address, counters["violations"])
                if logger:
                    logger.log(task="task2_mr", scenario=scenario_label, config="B",
                                doc_id=doc_id, i=i, served_by=_fmt_addr(address),
                                outcome="not_found")
                time.sleep(READ_INTERVAL)
                continue

            successful_reads += 1

            seq = doc["seq"]
            is_violation = seq < highest_seen

            if VERBOSE_EVERY_READ:
                marker = " <-- VIOLATION" if is_violation else ""
                print(
                    f"    [read {i + 1:>4}] from={_fmt_addr(address):<20} "
                    f"seq={seq}{marker}"
                )

            if is_violation:
                counters["violations"] += 1
                print(
                    f"[MR VIOLATION] read #{i + 1}: "
                    f"saw seq={seq} after previously seeing {highest_seen} "
                    f"(served by {_fmt_addr(address)})"
                )

            if logger:
                logger.log(task="task2_mr", scenario=scenario_label, config="B",
                            doc_id=doc_id, i=i, served_by=_fmt_addr(address),
                            seq=seq, highest_before=highest_seen,
                            outcome="violation" if is_violation else "ok")

            highest_seen = max(highest_seen, seq)
            _print_progress(i, READS, seq, address, counters["violations"])

            time.sleep(READ_INTERVAL)

    print("[read] causal-session read loop finished")

    effective = READS - counters["availability_losses"] - counters["errors"]
    print()
    print("-" * 60)
    print("CONFIG B SUMMARY")
    print("-" * 60)
    print(f"Read attempts       : {READS}")
    print(f"Availability losses : {counters['availability_losses']} "
          f"(secondary unreachable or could not catch up in time - expected "
          f"during failure/partition scenarios, NOT counted as a consistency "
          f"violation)")
    print(f"Other errors        : {counters['errors']}")
    print(f"Effective reads     : {effective}")
    print(f"Successful (found)  : {successful_reads}")
    print(f"Highest seq         : {highest_seen}")
    print(f"Violations          : {counters['violations']} "
          f"({(counters['violations'] / effective * 100) if effective else 0.0:.1f}% "
          f"of effective reads)")
    _print_served_by(served_by)

    if counters["violations"] == 0:
        print("RESULT              : PASS - monotonic reads maintained")
    else:
        print("RESULT              : FAIL - MR violation detected")

    print()

    return {
        "reads": READS,
        "successful": successful_reads,
        "highest_seq": highest_seen,
        "violations": counters["violations"],
        "availability_losses": counters["availability_losses"],
        "errors": counters["errors"],
        "effective": effective,
    }


# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------

def run_with_writer(db, doc_id, read_function):
    """
    Start a writer, run one experiment, and always stop the writer afterwards.

    Returns whatever read_function() returns, so callers can collect summary
    stats for a final cross-config comparison.
    """

    writer = Writer(db, doc_id)
    writer.start()

    try:
        # Give the writer some time to create the document and generate
        # replication activity before the reader starts.
        print(f"[setup] letting writer warm up for 1s before reading (doc_id={doc_id!r})")
        time.sleep(1)

        result = read_function()

    finally:
        writer.stop()
        writer.join()
        print(
            f"[setup] writer for doc_id={doc_id!r} fully stopped "
            f"(final writer seq={writer.last_seq}, total writes={writer.write_count}, "
            f"write errors={writer.write_errors})"
        )
        if writer.write_errors > 0:
            print(
                f"[setup] NOTE: the writer hit {writer.write_errors} error(s) "
                f"(likely the primary being briefly unreachable during a "
                f"node-failure/partition scenario) - factor this in before "
                f"reading too much into a low violation count for this run."
            )

    return result


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def _fmt_result(name, res):
    if res is None:
        print(f"{name:<10}: (did not complete)")
        return
    print(
        f"{name:<10}: reads={res['reads']:<4} "
        f"avail_loss={res['availability_losses']:<4} "
        f"errors={res['errors']:<4} "
        f"effective={res['effective']:<4} "
        f"successful={res['successful']:<4} "
        f"highest_seq={res['highest_seq']:<6} "
        f"violations={res['violations']}"
    )


def print_config_comparison(result_a, result_b):
    """Config A vs Config B comparison for a single scenario run."""
    print()
    print("-" * 60)
    print("CONFIG A vs CONFIG B (this scenario)")
    print("-" * 60)
    _fmt_result("Config A", result_a)
    _fmt_result("Config B", result_b)

    if result_a is not None and result_b is not None:
        eff_a = result_a["effective"]
        eff_b = result_b["effective"]
        if eff_a == 0 or eff_b == 0:
            print(
                "\nConclusion: one or both configs spent this run mostly "
                "unavailable (node down/partitioned); see availability losses "
                "above before drawing any consistency conclusion from this run."
            )
        elif result_a["violations"] > 0 and result_b["violations"] == 0:
            print(
                "\nConclusion: Config A (no causal session) allowed monotonic-reads "
                "violations, while Config B (causal session) upheld monotonic reads, "
                "as expected."
            )
        elif result_a["violations"] == 0:
            print(
                "\nConclusion: No violation was observed in Config A this run "
                "(replication was likely too fast / lag injection ineffective); "
                "this does not mean Config A guarantees MR in general."
            )
        elif result_b["violations"] > 0:
            print(
                "\nConclusion: Unexpectedly, Config B also showed violations. "
                "Check causal session wiring and read/write concern settings."
            )
    print()


def print_scenario_summary(scenario_results):
    print()
    print("#" * 60)
    print("# CROSS-SCENARIO SUMMARY (Task 2: Monotonic Reads)")
    print("#" * 60)
    header = (f"{'Scenario':<38}{'A viol':<8}{'A avail-loss':<14}"
              f"{'B viol':<8}{'B avail-loss':<14}")
    print(header)
    print("-" * len(header))
    for label, result in scenario_results.items():
        if not result:
            print(f"{label:<38}{'n/a':<8}{'n/a':<14}{'n/a':<8}{'n/a':<14}")
            continue
        res_a = result["config_a"]
        res_b = result["config_b"]
        print(f"{label:<38}{res_a['violations']:<8}{res_a['availability_losses']:<14}"
              f"{res_b['violations']:<8}{res_b['availability_losses']:<14}")
    print()


def main():
    print("#" * 60)
    print("# TASK 2: MONOTONIC READS EXPERIMENT")
    print("#" * 60)
    print()

    client = common.get_client()

    # Display replica-set topology and lag before running the experiment.
    print(">>> Replica set topology before experiment:")
    common.print_topology(client)
    print(">>> Replication lag before experiment:")
    common.print_replication_lag(client)
    print()

    db = client[common.DB_NAME]
    logger = common.TrialLogger("task2_mr")
    all_doc_ids = []

    def run_experiment(scenario_label="unknown"):
        # Secondaries are (re)discovered on every call, not once up front,
        # because during a node-failure / network-partition scenario the
        # set of currently reachable secondaries can differ from "normal".
        secondaries = sorted(client.secondaries)
        print(f">>> Discovered secondaries: {[_fmt_addr(s) for s in secondaries]}")

        # Only apply task2's OWN artificial-lag injection (DelayedSecondaryInjector
        # / FailpointLagInjector) during the "normal" scenario. During
        # secondary_failure / primary_failure / network_partition_minority_isolated,
        # scenarios.py has already injected a real fault that provides its own
        # divergence between replicas; re-discovering secondaries here and
        # additionally delaying one of them (possibly a completely different,
        # unrelated node from the one the current scenario is failing/isolating,
        # since this task does not pin to that specific node) would stack two
        # independent, uncoordinated faults on top of each other. That makes any
        # violation/pass observed in those scenarios impossible to attribute
        # cleanly to "the scenario's own fault" versus "task2's extra lag" -
        # exactly the kind of muddled result that should not go into the report.
        apply_lag_injection = INJECT_LAG and scenario_label == "normal"
        if INJECT_LAG and scenario_label != "normal":
            print(
                f">>> Scenario '{scenario_label}' already injects its own fault; "
                f"skipping task2's additional artificial-lag injection to avoid "
                f"stacking two independent faults (the scenario's replication "
                f"divergence is used as-is instead).\n"
            )

        if apply_lag_injection and len(secondaries) >= 2:
            lag_secondary = secondaries[0]
            normal_secondary = secondaries[1]
            print(
                f">>> Will inject lag on {_fmt_addr(lag_secondary)} and keep "
                f"{_fmt_addr(normal_secondary)} as the fresh baseline.\n"
            )
        else:
            lag_secondary = None
            normal_secondary = None
            if apply_lag_injection:
                print(
                    "[lag] fewer than two healthy secondaries available right now; "
                    "running Config A without injected lag (violations unlikely).\n"
                )
            elif not INJECT_LAG:
                print(">>> INJECT_LAG is False; running Config A without injected lag.\n")

        # IMPORTANT: a fresh run_id (and therefore fresh documents) every
        # time this closure runs, so scenarios never share/contaminate
        # each other's counters, and Config A / Config B use different
        # documents since each Writer restarts seq from 0.
        run_id = str(uuid.uuid4())
        print(f">>> Experiment run_id = {run_id}\n")

        doc_id_a = f"{run_id}-config-a-counter"
        doc_id_b = f"{run_id}-config-b-counter"
        all_doc_ids.extend([doc_id_a, doc_id_b])

        print(">>> Running Config A (weak: readConcern=local, no causal session)...\n")
        result_a = run_with_writer(
            db,
            doc_id_a,
            lambda: run_config_a(
                client,
                db,
                doc_id_a,
                lag_secondary=lag_secondary,
                normal_secondary=normal_secondary,
                logger=logger,
                scenario_label=scenario_label,
            ),
        )

        print("\n>>> Running Config B (strong: readConcern=majority, causal session)...\n")
        result_b = run_with_writer(
            db,
            doc_id_b,
            lambda: run_config_b(
                client,
                db,
                doc_id_b,
                logger=logger,
                scenario_label=scenario_label,
            ),
        )

        print_config_comparison(result_a, result_b)
        return {"config_a": result_a, "config_b": result_b}

    try:
        # Requirements.md asks for experiments under several scenarios:
        # normal operation, node failure, and a network partition.
        scenario_results = scenarios.run_under_scenarios(
            client, run_experiment, logger=logger
        )
        print_scenario_summary(scenario_results)
    finally:
        # Best-effort cleanup: must NEVER raise past this point, or
        # logger.close()/client.close() below would be skipped (leaking the
        # connection and the trial-log file handle) - e.g. if a majority
        # delete times out right after a partition/failure scenario before
        # the cluster has fully re-stabilised. Try majority first (durable),
        # fall back to w=1, and just warn (never crash) if even that fails -
        # mirroring common.robust_delete's degrade-gracefully approach.
        if all_doc_ids:
            print(">>> Cleaning up experiment documents...")
            deleted = False
            for w in ("majority", 1):
                try:
                    cleanup_coll = db.get_collection(
                        COLLECTION_NAME,
                        write_concern=WriteConcern(w=w, wtimeout=5000),
                    )
                    cleanup_result = cleanup_coll.delete_many(
                        {"_id": {"$in": all_doc_ids}})
                    print(f">>> Deleted {cleanup_result.deleted_count} "
                          f"experiment document(s) (w={w!r}).")
                    deleted = True
                    break
                except mongo_errors.PyMongoError as exc:
                    print(f">>> WARNING: cleanup delete with w={w!r} failed: {exc}")
            if not deleted:
                print(">>> WARNING: could not delete experiment documents; "
                      "remove them manually once the cluster is healthy.")
        logger.close()
        client.close()
        print(">>> MongoDB client closed.")


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

if __name__ == "__main__":
    main()

