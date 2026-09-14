#!/usr/bin/env python3
"""
mock_consistency.py

A single-machine SIMULATION of a MongoDB-style replica set, used to sanity
check that the four client-centric consistency experiments are logically
correct WITHOUT needing three real servers.

It models:
  * one PRIMARY that owns an ordered oplog (each entry gets a monotonically
    increasing cluster time `ts`);
  * several SECONDARIES, each with its own background applier thread that
    replays the oplog with a different, random lag (so replicas diverge);
  * readPreference = secondary (a read is served by ONE randomly chosen
    secondary snapshot);
  * writeConcern majority (a write waits until a majority of nodes applied it);
  * causal sessions (afterClusterTime): a causal read waits until the chosen
    secondary has replicated up to the session's cluster time, and every read
    advances that cluster time to what it observed.

We then run the four consistency checks against this mock in two configs:
  * WEAK   -> no causal session, w:1              (expected to expose violations)
  * STRONG -> causal session + majority           (expected to uphold the model)

Because the WEAK config relies on replication lag, violations are PROBABILISTIC
(just like on a real LAN): re-running may show different counts, and MW should
never violate. The point is that STRONG must ALWAYS report zero violations.

Run:  python3 mock_consistency.py
"""

import random
import threading
import time


# ============================================================
# Mock replica set
# ============================================================

class Oplog:
    """The primary's ordered operation log. Each append gets a fresh ts."""

    def __init__(self):
        self._entries = []          # list of dicts: {ts, key, value}
        self._lock = threading.Lock()

    def append(self, key, value):
        with self._lock:
            ts = len(self._entries) + 1     # 1,2,3,... = cluster time
            self._entries.append({"ts": ts, "key": key, "value": value})
            return ts

    def snapshot(self):
        with self._lock:
            return list(self._entries)

    def __len__(self):
        with self._lock:
            return len(self._entries)


class Secondary(threading.Thread):
    """
    A replica that applies oplog entries in order, but slowly. `base_lag`
    controls how far behind it runs, so different secondaries diverge and a
    client that bounces between them can observe stale / out-of-order data.
    """

    def __init__(self, oplog, base_lag):
        super().__init__(daemon=True)
        self.oplog = oplog
        self.base_lag = base_lag
        self.store = {}             # key -> {"value": v, "ts": ts}
        self.applied_ts = 0         # highest oplog ts applied so far
        self._applied_index = 0
        self._schedule = []         # per-entry-index wall-clock time it becomes visible
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def run(self):
        # Time-delayed replication: when this replica first *sees* an oplog
        # entry it schedules it to become visible base_lag seconds later. So
        # the replica stays roughly base_lag behind under load (bounded lag)
        # and always catches up once writes stop. Different base_lag values
        # make replicas diverge, which is what exposes weak-config violations.
        while not self._stop_event.is_set():
            entries = self.oplog.snapshot()
            now = time.time()

            # Schedule any newly discovered entries.
            while len(self._schedule) < len(entries):
                self._schedule.append(now + self.base_lag * random.uniform(0.5, 1.5))

            # Apply, strictly in oplog order, every entry whose time has come.
            with self._lock:
                while (self._applied_index < len(entries)
                       and now >= self._schedule[self._applied_index]):
                    entry = entries[self._applied_index]
                    self.store[entry["key"]] = {
                        "value": entry["value"],
                        "ts": entry["ts"],
                    }
                    self.applied_ts = entry["ts"]
                    self._applied_index += 1
            time.sleep(0.001)

    def read(self, key):
        """Return a consistent snapshot (value, ts) of one key on this replica."""
        with self._lock:
            doc = self.store.get(key)
            return (dict(doc) if doc else None, self.applied_ts)

    def read_all(self, key_filter):
        """Return all docs whose key passes key_filter, plus this node's applied_ts."""
        with self._lock:
            docs = [
                {"key": k, **v}
                for k, v in self.store.items()
                if key_filter(k)
            ]
            return docs, self.applied_ts

    def stop(self):
        self._stop_event.set()


class Session:
    """A (optionally) causally-consistent session tracking cluster time."""

    def __init__(self, causal):
        self.causal = causal
        self.last_op_ts = 0         # afterClusterTime the next op must respect


class MockCluster:
    """
    Ties together a primary oplog + N secondaries and exposes the small API
    the experiments need: write / read_one / read_many / session.
    """

    def __init__(self, secondary_lags=(0.002, 0.020, 0.060)):
        self.oplog = Oplog()
        self.primary = {}           # key -> {"value", "ts"}
        self.secondaries = [Secondary(self.oplog, lag) for lag in secondary_lags]
        for s in self.secondaries:
            s.start()

    # ---- writes ----------------------------------------------------------

    def write(self, key, value, session=None, majority=False, wtimeout=5.0):
        """Apply a write on the primary, replicate via oplog, optionally wait
        for a majority to acknowledge, and advance the session cluster time."""
        ts = self.oplog.append(key, value)
        self.primary[key] = {"value": value, "ts": ts}

        if majority:
            self._wait_majority(ts, wtimeout)

        if session and session.causal:
            session.last_op_ts = max(session.last_op_ts, ts)
        return ts

    def _wait_majority(self, ts, wtimeout):
        needed = (len(self.secondaries) + 1) // 2 + 1   # includes primary
        deadline = time.time() + wtimeout
        while time.time() < deadline:
            acks = 1  # primary already has it
            acks += sum(1 for s in self.secondaries if s.applied_ts >= ts)
            if acks >= needed:
                return
            time.sleep(0.001)

    # ---- reads (readPreference = secondary) ------------------------------

    def _pick_secondary(self):
        return random.choice(self.secondaries)

    def read_one(self, key, session=None, timeout=5.0):
        """Read one key from a randomly chosen secondary.

        In a causal session we first wait until that secondary has caught up to
        the session's cluster time (afterClusterTime), then advance the session
        clock to what we observed."""
        sec = self._pick_secondary()

        if session and session.causal and session.last_op_ts:
            deadline = time.time() + timeout
            while sec.applied_ts < session.last_op_ts and time.time() < deadline:
                time.sleep(0.001)

        doc, op_time = sec.read(key)

        if session and session.causal:
            # A read advances the session clock to the snapshot it saw, so a
            # later read cannot travel back in time.
            session.last_op_ts = max(session.last_op_ts, op_time)
        return doc

    def read_many(self, key_filter, session=None, timeout=5.0):
        """Read all matching docs from ONE secondary snapshot (used by MW)."""
        sec = self._pick_secondary()

        if session and session.causal and session.last_op_ts:
            deadline = time.time() + timeout
            while sec.applied_ts < session.last_op_ts and time.time() < deadline:
                time.sleep(0.001)

        docs, op_time = sec.read_all(key_filter)

        if session and session.causal:
            session.last_op_ts = max(session.last_op_ts, op_time)
        return docs

    def start_session(self, causal):
        return Session(causal)

    def close(self):
        for s in self.secondaries:
            s.stop()
        for s in self.secondaries:
            s.join(timeout=1.0)


# ============================================================
# Task 1: Read-your-writes
# ============================================================

def check_ryw(cluster, causal, n=300):
    """Write a key, immediately read it back from a secondary; count reads that
    miss the client's own write."""
    session = cluster.start_session(causal) if causal else None
    violations = 0
    for i in range(n):
        key = f"ryw-{i}"
        cluster.write(key, f"value-{i}", session=session, majority=causal)
        doc = cluster.read_one(key, session=session)
        if doc is None or doc["value"] != f"value-{i}":
            violations += 1
    return violations


# ============================================================
# Task 2: Monotonic-reads
# ============================================================

def check_mr(cluster, causal, reads=400):
    """A background writer advances a counter; a reader flags any read whose
    value is OLDER than one it already observed."""
    key = "mr-counter"
    stop = threading.Event()

    def writer():
        seq = 0
        while not stop.is_set():
            seq += 1
            cluster.write(key, seq, majority=False)
            time.sleep(0.003)

    t = threading.Thread(target=writer, daemon=True)
    t.start()
    time.sleep(0.2)

    session = cluster.start_session(causal) if causal else None
    highest = -1
    violations = 0
    for _ in range(reads):
        doc = cluster.read_one(key, session=session)
        if doc is None:
            continue
        seq = doc["value"]
        if seq < highest:               # time went backwards -> MR violation
            violations += 1
        highest = max(highest, seq)
        time.sleep(0.003)

    stop.set()
    t.join(timeout=1.0)
    return violations


# ============================================================
# Task 3: Monotonic-writes
# ============================================================

def check_mw(cluster, causal, n=150):
    """Issue ordered writes W0..W(n-1) to distinct keys; after each, read the
    sequence from a secondary and flag any 'hole' (a later write visible while
    an earlier one is not)."""
    session = cluster.start_session(causal) if causal else None
    prefix = f"mw-{random.randint(0, 1_000_000)}"
    violations = 0

    for i in range(n):
        cluster.write(f"{prefix}-{i}", i, session=session, majority=causal)

        docs = cluster.read_many(lambda k: k.startswith(prefix + "-"),
                                 session=session)
        seqs = {d["value"] for d in docs}
        if seqs:
            highest = max(seqs)
            # Everything below the highest visible write must also be visible.
            for j in range(highest):
                if j not in seqs:
                    violations += 1
                    break
    return violations


# ============================================================
# Task 4: Writes-follow-reads
# ============================================================

def check_wfr(cluster, causal, rounds=250):
    """Background writer advances x. Client reads x (=r) then writes y{based_on:r}.
    Observer that sees y then reads x; flag any case where observed x < r."""
    x_key = "wfr-x"
    stop = threading.Event()

    def writer():
        seq = 0
        while not stop.is_set():
            seq += 1
            cluster.write(x_key, seq, majority=causal)
            time.sleep(0.004)

    t = threading.Thread(target=writer, daemon=True)
    t.start()
    time.sleep(0.2)

    session = cluster.start_session(causal) if causal else None
    violations = 0
    checked = 0

    for k in range(rounds):
        x_doc = cluster.read_one(x_key, session=session)
        if x_doc is None:
            continue
        r = x_doc["value"]

        y_key = f"wfr-y-{k}"
        cluster.write(y_key, {"based_on": r}, session=session, majority=causal)

        # The observer sees the "reply" y only once it has replicated. Poll a
        # secondary (bounded) until y is visible; if it never shows up in time,
        # this round contributes nothing to check.
        y_seen = None
        deadline = time.time() + 0.2
        while time.time() < deadline:
            y_seen = cluster.read_one(y_key, session=session)
            if y_seen is not None:
                break
        if y_seen is None:
            continue

        # Now that the effect (y) is visible, read its cause (x) from a
        # secondary. WFR requires x to be at r or newer.
        x_seen = cluster.read_one(x_key, session=session)
        o = x_seen["value"] if x_seen else -1

        checked += 1
        if o < r:                       # effect visible while cause is stale -> WFR violation
            violations += 1
        time.sleep(0.002)

    stop.set()
    t.join(timeout=1.0)
    return violations, checked


# ============================================================
# Driver
# ============================================================

def run_case(name, fn, expect_weak_violation):
    """Run one check in WEAK and STRONG configs and report."""
    cluster = MockCluster()
    try:
        weak = fn(cluster, causal=False)
    finally:
        cluster.close()

    cluster = MockCluster()
    try:
        strong = fn(cluster, causal=True)
    finally:
        cluster.close()

    # Some checks return a tuple (violations, checked); normalise.
    weak_v = weak[0] if isinstance(weak, tuple) else weak
    strong_v = strong[0] if isinstance(strong, tuple) else strong

    print(f"--- {name} ---")
    print(f"  WEAK   (no session, w:1)   : {weak_v} violation(s)"
          f"  {'<- violation exposed (expected)' if weak_v > 0 else '(none this run)'}")
    print(f"  STRONG (causal + majority) : {strong_v} violation(s)"
          f"  {'OK' if strong_v == 0 else 'UNEXPECTED - logic bug!'}")

    # Correctness verdict for the STRONG config, which must always hold.
    ok = (strong_v == 0)
    note = ""
    if expect_weak_violation and weak_v == 0:
        note = " (weak showed none this run; lag too small — re-run to observe)"
    print(f"  VERDICT: {'PASS' if ok else 'FAIL'}{note}")
    print()
    return ok


def main():
    random.seed()  # comment out / set a fixed seed for reproducibility
    print("=" * 64)
    print("Mock replica-set consistency check (single machine, no MongoDB)")
    print("=" * 64)
    print()

    results = []
    results.append(run_case("Task 1  Read-your-writes",  check_ryw, True))
    results.append(run_case("Task 2  Monotonic-reads",   check_mr,  True))
    results.append(run_case("Task 3  Monotonic-writes",  check_mw,  False))
    results.append(run_case("Task 4  Writes-follow-reads", check_wfr, True))

    print("=" * 64)
    if all(results):
        print("ALL STRONG CONFIGS PASSED — the four checks behave as designed.")
        print("MW is expected to show 0 weak violations (single ordered oplog).")
        print("RYW / MR / WFR weak violations are probabilistic — re-run to see them.")
    else:
        print("A STRONG config reported a violation — that indicates a logic bug.")
    print("=" * 64)


if __name__ == "__main__":
    main()
