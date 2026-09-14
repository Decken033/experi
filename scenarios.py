#!/usr/bin/env python3
"""
scenarios.py

Reusable "failure scenario" injectors for the client-centric consistency
experiments (task1_read_your_writes.py .. task4_writes_follow_reads.py).

Requirements.md asks for experiments under THREE scenarios:

    1. normal operation                 (already implemented: Config A vs B)
    2. failure of one or more nodes     (NOT YET implemented -> this module)
    3. a network partition              (NOT YET implemented -> this module)

This module does NOT rerun the consistency logic itself. It only provides
two context managers -

    NodeFailureInjector       -- stop/restart a mongod process
    NetworkPartitionInjector  -- cut/restore TCP connectivity between nodes

-and a small `run_under_scenarios()` helper that wraps your EXISTING
`run_config_a` / `run_config_b` functions from task1..task4 and runs them
three times: once normally, once during a node failure, once during a
network partition. This keeps the amount of new code small and reuses all
the consistency-checking / statistics logic you already wrote.

Two deployment modes are supported, chosen by DEPLOYMENT_MODE below:

    "ssh"    -- three separate physical/virtual machines, each running a
                bare `mongod` managed by systemd. Failure/partition actions
                are executed over SSH (key-based auth assumed, no
                password prompt).
    "docker" -- three mongod instances running as Docker containers on one
                host (or reachable via `docker` CLI / docker-compose).
                Failure/partition actions use the `docker` CLI.

Only ONE of the two branches will actually run depending on how you deployed
your cluster; the other is left in as a documented alternative.

IMPORTANT SAFETY NOTES
-----------------------
* All commands here only ever target machines/containers that belong to
  YOUR OWN replica set (as configured in common.NODES). Nothing here
  touches any third-party host.
* Every injector is a context manager with a `finally`-guaranteed restore
  step, and additionally re-verifies cluster health with
  `replSetGetStatus` after restoring, so a crashed experiment never leaves
  your cluster half-broken.
* SSH commands assume passwordless sudo (or being already root over SSH)
  for `systemctl` / `iptables`. Set that up beforehand, or adapt the
  command lists below to however your machines are administered.
"""

import inspect
import subprocess
import time

from pymongo.errors import PyMongoError

import common


# ============================================================
# Configuration - EDIT THESE to match your deployment
# ============================================================

# "ssh"    -> three bare-metal / VM machines, one mongod each, systemd-managed
# "docker" -> three containers on ONE Docker host (single machine)
# "manual" -> three separate machines each running mongod in Docker, with NO
#             passwordless SSH between them. The script prints the exact
#             command to run on the right machine and pauses (input()) until
#             you confirm you've done it. Use this when SSH-ing between the
#             machines (e.g. 2x Windows + 1x Mac, each with its own local
#             Docker Desktop) isn't set up yet.
DEPLOYMENT_MODE = "manual"

# Only used when DEPLOYMENT_MODE == "manual": which local Docker container
# name each NODES entry corresponds to ON ITS OWN MACHINE (you run the
# printed `docker` command on that machine, not on the machine running this
# script). Edit to match your actual container names, e.g. from
# `docker ps` on each host.
MANUAL_CONTAINER = {
    "172.20.10.2:27017": "mongo1",
    "172.20.10.3:27017": "mongo2",
    "172.20.10.5:27017": "mongo3",
}

# Friendly machine labels shown in the manual prompts, so you know which
# physical machine to go do the action on.
MANUAL_MACHINE_LABEL = {
    "172.20.10.2:27017": "Windows #1 (172.20.10.2)",
    "172.20.10.3:27017": "Windows #2 (172.20.10.3)",
    "172.20.10.5:27017": "Mac (172.20.10.5)",
}


def _flush_stdin():
    """
    Best-effort: discard any keystrokes already sitting in the input
    buffer before we display a new prompt.

    Why this matters: on Windows consoles (and most others), input() reads
    one buffered line per call. If you press Enter twice at one prompt
    (e.g. because the terminal seemed unresponsive), the SECOND Enter is
    NOT consumed by that same input() call - it stays queued and gets
    silently consumed by the very NEXT input() call instead, letting that
    next prompt "pass" without you actually confirming it. That is exactly
    the failure mode that made 172.20.10.2 look like it never rejoined:
    the "docker start mongo1" prompt was skipped by a stray leftover Enter
    from the earlier "docker stop mongo1" prompt, so the action was never
    actually performed before the script moved on to checking health.

    This function clears any such pending input right before we print a
    new prompt, so only keystrokes typed AFTER seeing that specific
    prompt can confirm it. It's a no-op (and never raises) on platforms/
    terminals where the low-level check isn't available (e.g. when stdin
    is redirected from a file/pipe rather than a real console).
    """
    try:
        import msvcrt  # Windows-only
        while msvcrt.kbhit():
            msvcrt.getch()
        return
    except ImportError:
        pass
    try:
        import sys
        import termios
        termios.tcflush(sys.stdin, termios.TCIFLUSH)
    except Exception:
        pass


def _manual_pause(instructions):
    """Print a numbered set of instructions and block until the user
    TYPES 'done' (not just presses Enter) to confirm they've carried them
    out on the right machine.

    Why require typing a word instead of just pressing Enter: on Windows,
    msvcrt-based console flushing (see _flush_stdin above) operates on the
    raw console input queue, but Python's input() reads through the C
    runtime's own stdio line buffer - a SEPARATE buffer that msvcrt can't
    see into. If an extra Enter you pressed earlier (e.g. because a prior
    prompt seemed unresponsive) has already been transferred into that
    stdio buffer as a queued blank line, _flush_stdin() cannot detect or
    remove it, and the next input() call silently returns that blank line
    instantly - skipping the pause without you ever seeing it happen.
    Requiring the literal word 'done' closes that gap: a stray blank line
    (or any other leftover keystroke) simply won't match, so the prompt
    re-asks instead of silently passing.

    Never raises; a stray Ctrl-C/EOF just falls through as "continue after
    a short pause" rather than blocking forever.
    """
    _flush_stdin()
    print()
    print("    " + "=" * 60)
    print("    MANUAL ACTION REQUIRED")
    print("    " + "=" * 60)
    for line in instructions:
        print(f"    {line}")
    print("    " + "-" * 60)
    while True:
        try:
            response = input(
                "    Type 'done' and press ENTER once you've done this "
                "on the correct machine: "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("    (no input available - continuing after a short pause)")
            time.sleep(2)
            break
        if response == "done":
            break
        print(f"    (typed {response!r} - type exactly 'done' once you've "
              f"completed the action above)")
    print()

# Map each NODES entry ("host:port") to the SSH-reachable hostname/IP used
# to administer that machine. In our 3-machine setup this is identical to
# the mongod host, but kept separate in case your SSH hostname differs
# (e.g. a jump host, a different NIC, etc).
SSH_ADMIN_HOST = {
    "172.20.10.2:27017": "172.20.10.2",
    "172.20.10.3:27017": "172.20.10.3",
    "172.20.10.5:27017": "172.20.10.5",
}

# Map each NODES entry to its Docker container name (only used when
# DEPLOYMENT_MODE == "docker").
DOCKER_CONTAINER = {
    "172.20.10.2:27017": "mongo1",
    "172.20.10.3:27017": "mongo2",
    "172.20.10.5:27017": "mongo3",
}

# How long to wait after stopping/starting a node, or after cutting/
# restoring a partition, before the replica set is expected to have
# noticed and stabilised (new primary elected, member marked (UN)REACHABLE,
# etc). Tune based on your electionTimeoutMillis (default 10s).
SETTLE_SECONDS = 12

# How long to wait for a downed / rejoined node to fully catch up on the
# oplog before starting the next scenario (avoids cross-contaminating
# results between scenarios).
RECOVERY_SECONDS = 15


# ============================================================
# Low-level command runners
# ============================================================

def _run(cmd, check=True):
    """Run a local shell command (used to invoke `ssh` or `docker`)."""
    print(f"    $ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check, capture_output=True, text=True)
    if result.stdout.strip():
        print(f"      stdout: {result.stdout.strip()}")
    if result.returncode != 0 and result.stderr.strip():
        print(f"      stderr: {result.stderr.strip()}")
    return result


def _ssh(host, *remote_cmd):
    return _run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                 host, *remote_cmd], check=False)


# ============================================================
# Node failure injector
# ============================================================

class NodeFailureInjector:
    """
    Context manager: stop a single mongod node for the duration of the
    `with` block, then restart it and wait for it to rejoin.

    Usage:
        with NodeFailureInjector(client, "172.20.10.3:27017"):
            # run experiment while that node is DOWN
            ...
        # node has been restarted and given RECOVERY_SECONDS to catch up
    """

    def __init__(self, client, node, settle_seconds=SETTLE_SECONDS,
                 recovery_seconds=RECOVERY_SECONDS):
        self.client = client
        self.node = node
        self.settle_seconds = settle_seconds
        self.recovery_seconds = recovery_seconds
        self._stopped = False

    def _stop_node(self):
        if DEPLOYMENT_MODE == "docker":
            container = DOCKER_CONTAINER[self.node]
            _run(["docker", "stop", container])
        elif DEPLOYMENT_MODE == "ssh":
            host = SSH_ADMIN_HOST[self.node]
            _ssh(host, "sudo", "systemctl", "stop", "mongod")
        elif DEPLOYMENT_MODE == "manual":
            container = MANUAL_CONTAINER[self.node]
            machine = MANUAL_MACHINE_LABEL.get(self.node, self.node)
            _manual_pause([
                f"On {machine}, run:",
                f"    docker stop {container}",
                "(This stops that node's mongod so it counts as DOWN for this scenario.)",
            ])
        else:
            raise ValueError(f"unknown DEPLOYMENT_MODE: {DEPLOYMENT_MODE!r}")

    def _start_node(self):
        if DEPLOYMENT_MODE == "docker":
            container = DOCKER_CONTAINER[self.node]
            _run(["docker", "start", container])
        elif DEPLOYMENT_MODE == "ssh":
            host = SSH_ADMIN_HOST[self.node]
            _ssh(host, "sudo", "systemctl", "start", "mongod")
        elif DEPLOYMENT_MODE == "manual":
            container = MANUAL_CONTAINER[self.node]
            machine = MANUAL_MACHINE_LABEL.get(self.node, self.node)
            _manual_pause([
                f"On {machine}, run:",
                f"    docker start {container}",
                "(This brings that node's mongod back up so it can rejoin and catch up.)",
            ])

    def __enter__(self):
        was_primary = self.client.primary is not None and (
            f"{self.client.primary[0]}:{self.client.primary[1]}" == self.node
        )
        role = "PRIMARY" if was_primary else "SECONDARY"
        print(f"[failure] stopping {role} node {self.node} "
              f"(mode={DEPLOYMENT_MODE})")
        self._stop_node()
        self._stopped = True

        print(f"[failure] waiting {self.settle_seconds}s for the replica "
              f"set to notice and (if needed) elect a new primary...")
        time.sleep(self.settle_seconds)

        print("[failure] topology after the failure:")
        common.print_topology(self.client)
        common.print_replication_lag(self.client)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._stopped:
            return False
        print(f"[failure] restarting node {self.node}")
        self._start_node()

        print(f"[failure] waiting {self.recovery_seconds}s for {self.node} "
              f"to rejoin and catch up on the oplog...")
        time.sleep(self.recovery_seconds)

        try:
            common.print_topology(self.client)
            common.print_replication_lag(self.client)
        except PyMongoError as exc:
            print(f"[failure] WARNING: could not confirm recovery: {exc}")

        # Do not suppress exceptions raised inside the `with` block.
        return False


# ============================================================
# Network partition injector
# ============================================================

class NetworkPartitionInjector:
    """
    Context manager: isolate one node from the rest of the replica set at
    the network level (both directions), using iptables over SSH, or
    Docker network disconnect if DEPLOYMENT_MODE == "docker".

    This creates a classic minority/majority partition:
        isolated_node          -> the minority side (1 node)
        every other NODES entry -> the majority side (N-1 nodes)

    Usage:
        with NetworkPartitionInjector(client, "172.20.10.5:27017"):
            # 172.20.10.5 can reach NOBODY (majority can reach each other)
            ...
        # connectivity restored
    """

    def __init__(self, client, isolated_node,
                 settle_seconds=SETTLE_SECONDS,
                 recovery_seconds=RECOVERY_SECONDS):
        self.client = client
        self.isolated_node = isolated_node
        self.other_nodes = [n for n in common.NODES if n != isolated_node]
        self.settle_seconds = settle_seconds
        self.recovery_seconds = recovery_seconds
        self._applied = False

    # -- iptables (ssh mode) --------------------------------------------
    def _iptables_block(self, on_host, other_ip):
        _ssh(on_host, "sudo", "iptables", "-A", "INPUT",  "-s", other_ip, "-j", "DROP")
        _ssh(on_host, "sudo", "iptables", "-A", "OUTPUT", "-d", other_ip, "-j", "DROP")

    def _iptables_unblock(self, on_host, other_ip):
        _ssh(on_host, "sudo", "iptables", "-D", "INPUT",  "-s", other_ip, "-j", "DROP")
        _ssh(on_host, "sudo", "iptables", "-D", "OUTPUT", "-d", other_ip, "-j", "DROP")

    def _apply_ssh(self):
        isolated_host = SSH_ADMIN_HOST[self.isolated_node]
        isolated_ip = self.isolated_node.split(":")[0]
        for other in self.other_nodes:
            other_host = SSH_ADMIN_HOST[other]
            other_ip = other.split(":")[0]
            # Block in both directions so the partition is symmetric.
            self._iptables_block(isolated_host, other_ip)
            self._iptables_block(other_host, isolated_ip)

    def _revert_ssh(self):
        isolated_host = SSH_ADMIN_HOST[self.isolated_node]
        isolated_ip = self.isolated_node.split(":")[0]
        for other in self.other_nodes:
            other_host = SSH_ADMIN_HOST[other]
            other_ip = other.split(":")[0]
            self._iptables_unblock(isolated_host, other_ip)
            self._iptables_unblock(other_host, isolated_ip)

    # -- docker network mode ---------------------------------------------
    def _apply_docker(self):
        container = DOCKER_CONTAINER[self.isolated_node]
        # Assumes all mongod containers share one user-defined network.
        # Disconnecting removes the isolated container from that network
        # entirely, cutting it off from every peer (and from the client,
        # if the client also connects to that Docker network).
        network = _run(
            ["docker", "inspect", "-f",
             "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
             container]
        ).stdout.strip()
        self._docker_network = network
        _run(["docker", "network", "disconnect", network, container])

    def _revert_docker(self):
        container = DOCKER_CONTAINER[self.isolated_node]
        _run(["docker", "network", "connect", self._docker_network, container])

    def _apply_manual(self):
        container = MANUAL_CONTAINER[self.isolated_node]
        machine = MANUAL_MACHINE_LABEL.get(self.isolated_node, self.isolated_node)
        _manual_pause([
            f"On {machine}, run ONE of the following to cut {container} off from",
            f"the other two nodes ({', '.join(self.other_nodes)}):",
            f"  Option A (simplest - disconnects it from all networks):",
            f"    docker network disconnect bridge {container}",
            f"    (replace 'bridge' with your compose network name if different,",
            f"     e.g. `docker network ls` to check)",
            f"  Option B (if the mongod client on this script's machine ALSO needs",
            f"    to keep reaching {container} for topology checks, use iptables",
            f"    INSIDE that machine's OS instead, blocking only the other two IPs):",
            f"    " + " && ".join(
                f"sudo iptables -A INPUT -s {o.split(':')[0]} -j DROP && "
                f"sudo iptables -A OUTPUT -d {o.split(':')[0]} -j DROP"
                for o in self.other_nodes
            ),
        ])

    def _revert_manual(self):
        container = MANUAL_CONTAINER[self.isolated_node]
        machine = MANUAL_MACHINE_LABEL.get(self.isolated_node, self.isolated_node)
        _manual_pause([
            f"On {machine}, restore connectivity for {container}:",
            f"  If you used Option A:",
            f"    docker network connect bridge {container}",
            f"  If you used Option B (iptables), remove the rules you added:",
            f"    " + " && ".join(
                f"sudo iptables -D INPUT -s {o.split(':')[0]} -j DROP && "
                f"sudo iptables -D OUTPUT -d {o.split(':')[0]} -j DROP"
                for o in self.other_nodes
            ),
        ])

    def __enter__(self):
        print(f"[partition] isolating {self.isolated_node} from "
              f"{self.other_nodes} (mode={DEPLOYMENT_MODE})")
        if DEPLOYMENT_MODE == "docker":
            self._apply_docker()
        elif DEPLOYMENT_MODE == "ssh":
            self._apply_ssh()
        elif DEPLOYMENT_MODE == "manual":
            self._apply_manual()
        else:
            raise ValueError(f"unknown DEPLOYMENT_MODE: {DEPLOYMENT_MODE!r}")

        self._applied = True

        print(f"[partition] waiting {self.settle_seconds}s for the replica "
              f"set to detect the split (heartbeat timeout / election)...")
        time.sleep(self.settle_seconds)

        print("[partition] topology after the split "
              "(NOTE: this client itself must be on the MAJORITY side to "
              "see this call succeed at all):")
        try:
            common.print_topology(self.client)
            common.print_replication_lag(self.client)
        except PyMongoError as exc:
            print(f"[partition] (expected if the client is on the minority "
                  f"side) could not reach the primary: {exc}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._applied:
            return False
        print(f"[partition] restoring connectivity to {self.isolated_node}")
        try:
            if DEPLOYMENT_MODE == "docker":
                self._revert_docker()
            elif DEPLOYMENT_MODE == "manual":
                self._revert_manual()
            else:
                self._revert_ssh()
        except Exception as exc:  # noqa: BLE001 - best-effort restore
            print(f"[partition] WARNING: restore command failed: {exc}")

        print(f"[partition] waiting {self.recovery_seconds}s for the "
              f"rejoined node to catch up...")
        time.sleep(self.recovery_seconds)

        try:
            common.print_topology(self.client)
            common.print_replication_lag(self.client)
        except PyMongoError as exc:
            print(f"[partition] WARNING: could not confirm recovery: {exc}")

        return False


# ============================================================
# Scenario runner: wraps your EXISTING config-A/config-B functions
# ============================================================

def pick_failure_target(client, prefer_primary):
    """Return the NODES entry to fail: current primary, or a secondary."""
    if prefer_primary and client.primary is not None:
        return f"{client.primary[0]}:{client.primary[1]}"
    secs = sorted(client.secondaries)
    if not secs:
        raise RuntimeError("no secondaries discovered; cannot pick a target")
    host, port = secs[0]
    return f"{host}:{port}"


def _param_count(fn):
    """Number of parameters `fn` declares, or 0 if introspection fails."""
    try:
        return len(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        return 0


def _call_with_optional_label(fn, label, target_node=None):
    """
    Call `fn()`, `fn(label)`, or `fn(label, target_node)` depending on its
    signature, so a task can opt in to knowing which scenario is running
    (and, when relevant, the exact "host:port" string of the node this
    scenario is failing/isolating) just by adding parameters to its
    `run_experiment`, without breaking tasks that don't need them.

    `target_node` is only meaningful for scenarios that fail/isolate a
    SPECIFIC node the caller already picked before entering the fault
    context manager (secondary_failure, network_partition_*). It is `None`
    for "normal" and for "primary_failure" (there, the thing being failed is
    the primary, not a secondary a read could be pinned to, so the task
    should fall back to its own fresh secondary discovery instead).

    IMPORTANT: this must be called with the label/target that were decided
    BEFORE the fault was injected. Re-discovering "which node is currently a
    secondary" from inside `fn`, after the fault has already been applied
    and the replica set has settled, would silently pick the SURVIVING node
    instead of the one under test - which is exactly the bug this parameter
    exists to avoid.
    """
    n_params = _param_count(fn)
    if n_params >= 2:
        return fn(label, target_node)
    if n_params == 1:
        return fn(label)
    return fn()


def run_under_scenarios(client, run_experiment, logger=None):
    """
    Run `run_experiment` four times, tagging each run with a scenario
    label: "normal", "secondary_failure", "primary_failure",
    "network_partition_minority_isolated".

    `run_experiment` may be a zero-arg callable (the original contract), a
    one-arg callable `run_experiment(scenario_label)`, or a two-arg
    callable `run_experiment(scenario_label, target_node)` that runs one
    full Config A + Config B pass (e.g. wrap your task's `run_config_a` /
    `run_config_b` calls in a small closure). It is called once per
    scenario; nothing about the consistency-checking logic needs to change.

    `target_node`, when not None, is the exact "host:port" string of the
    node THIS scenario is about to fail/isolate, decided BEFORE the fault
    is injected. A task that pins reads to a specific node (to make sure it
    is actually exercising reads against the node under test rather than
    whichever healthy node the driver's SDAM happens to prefer) should use
    this value directly instead of re-discovering "the current secondary"
    from inside `run_experiment` - by that point the fault has already been
    applied and settled, so a fresh discovery would find the node under
    test missing from the topology and silently pin to the SURVIVING node
    instead, defeating the purpose of the scenario.

    `logger`, if given, should be a `common.TrialLogger`; each scenario's
    start/end is recorded so the report can cite exactly when each
    scenario ran.

    Returns a dict {scenario_label: whatever run_experiment(...) returned}.
    """
    results = {}

    def _tagged(label, fn):
        print()
        print("#" * 70)
        print(f"# SCENARIO: {label}")
        print("#" * 70)
        if logger:
            logger.log(scenario=label, phase="start")
        try:
            results[label] = fn()
        finally:
            if logger:
                logger.log(scenario=label, phase="end")

    # 1. Normal operation (baseline, no injected failure). No specific node
    #    is under test, so target_node stays None (task falls back to its
    #    own fresh discovery, which is safe here since nothing is down).
    _tagged("normal", lambda: _call_with_optional_label(run_experiment, "normal"))

    # 2. Secondary node failure. `target` is decided HERE, while the
    #    topology is still normal, and threaded through to run_experiment
    #    unchanged - it is the node this scenario fails.
    #
    # pick_failure_target() raises RuntimeError if no secondary is currently
    # discoverable (e.g. the cluster has not fully settled since a previous
    # scenario, or is genuinely down to fewer nodes than expected). That
    # must NOT be allowed to escape this function: an uncaught exception
    # here would abort run_under_scenarios entirely and discard `results`,
    # including the "normal" scenario that already completed successfully.
    # Skip the scenario instead, exactly like the network-partition case
    # below already does when it has no secondary to isolate.
    try:
        target = pick_failure_target(client, prefer_primary=False)
    except RuntimeError as exc:
        print(f"[failure] cannot run 'secondary_failure' scenario: {exc}")
    else:
        _tagged(
            "secondary_failure",
            lambda: _with(NodeFailureInjector(client, target), run_experiment,
                           "secondary_failure", target_node=target),
        )

    # 3. Primary node failure (forces an election). The failed node is the
    #    PRIMARY, not a secondary a read could be pinned to, so target_node
    #    stays None here too - the task should fresh-discover whichever
    #    secondary exists post-election. Same crash-guard as (2): a missing
    #    primary/secondary at this point must skip the scenario, not blow
    #    away every result collected so far.
    try:
        primary_target = pick_failure_target(client, prefer_primary=True)
    except RuntimeError as exc:
        print(f"[failure] cannot run 'primary_failure' scenario: {exc}")
    else:
        _tagged(
            "primary_failure",
            lambda: _with(NodeFailureInjector(client, primary_target), run_experiment,
                           "primary_failure"),
        )

    # 4. Network partition: isolate one secondary (client stays on the
    #    majority side, since `client` is what NODES/URI point at). Same
    #    principle as (2): `isolated` is decided before the partition is
    #    applied and passed straight through.
    secs = sorted(client.secondaries)
    if secs:
        host, port = secs[0]
        isolated = f"{host}:{port}"
        _tagged(
            "network_partition_minority_isolated",
            lambda: _with(NetworkPartitionInjector(client, isolated), run_experiment,
                           "network_partition_minority_isolated", target_node=isolated),
        )
    else:
        print("[partition] no secondary available to isolate; skipping "
              "network-partition scenario")

    return results


def _with(context_manager, fn, label, target_node=None):
    """Small helper so lambdas above can combine a `with` and a call,
    forwarding the scenario label (and, where applicable, the exact node
    this scenario is failing/isolating) into `fn`."""
    with context_manager:
        return _call_with_optional_label(fn, label, target_node)


# ============================================================
# Single-scenario runners (for running each scenario as its own script)
# ============================================================
#
# run_under_scenarios() above runs all four scenarios back-to-back in one
# process. When each scenario needs a manual pause/resume step on separate
# physical machines, it's often more convenient to run ONE scenario per
# script invocation instead - you don't have to babysit a single long
# session through all four fault injections in one sitting, and a mistake
# or Ctrl-C during one scenario doesn't lose the results of the others
# (which are already independent per-file JSONL logs anyway).
#
# Each function below wraps exactly ONE of the four scenario branches from
# run_under_scenarios(), with the same tagging/logging around it, and
# returns whatever `run_experiment(...)` returned for that single scenario
# (or None if the scenario had to be skipped, e.g. no secondary currently
# discoverable to fail/isolate).

def _run_single(label, logger, body):
    """Print the same '# SCENARIO: ...' banner used by run_under_scenarios,
    log start/end if a logger is given, run `body()`, and return its
    result. Shared by all four run_scenario_* functions below."""
    print()
    print("#" * 70)
    print(f"# SCENARIO: {label}")
    print("#" * 70)
    if logger:
        logger.log(scenario=label, phase="start")
    try:
        return body()
    finally:
        if logger:
            logger.log(scenario=label, phase="end")


def run_scenario_normal(client, run_experiment, logger=None):
    """Baseline: no injected failure. No specific node is under test, so
    target_node is None (task falls back to its own fresh discovery)."""
    return _run_single(
        "normal", logger,
        lambda: _call_with_optional_label(run_experiment, "normal"),
    )


def run_scenario_secondary_failure(client, run_experiment, logger=None):
    """Stop one secondary for the duration of the experiment.

    Returns None (and prints why) if no secondary is currently
    discoverable - this must not raise, so a standalone script calling
    this can still exit cleanly instead of crashing with a traceback.
    """
    try:
        target = pick_failure_target(client, prefer_primary=False)
    except RuntimeError as exc:
        print(f"[failure] cannot run 'secondary_failure' scenario: {exc}")
        return None
    return _run_single(
        "secondary_failure", logger,
        lambda: _with(NodeFailureInjector(client, target), run_experiment,
                       "secondary_failure", target_node=target),
    )


def run_scenario_primary_failure(client, run_experiment, logger=None):
    """Stop the current primary (forces an election) for the duration of
    the experiment. target_node stays None: the failed node is the
    primary, not a secondary a read could be pinned to, so the task should
    fresh-discover whichever secondary exists post-election.

    Returns None (and prints why) if no primary is currently discoverable.
    """
    try:
        primary_target = pick_failure_target(client, prefer_primary=True)
    except RuntimeError as exc:
        print(f"[failure] cannot run 'primary_failure' scenario: {exc}")
        return None
    return _run_single(
        "primary_failure", logger,
        lambda: _with(NodeFailureInjector(client, primary_target), run_experiment,
                       "primary_failure"),
    )


def run_scenario_partition(client, run_experiment, logger=None):
    """Isolate one secondary from the rest of the replica set (client
    stays on the majority side) for the duration of the experiment.

    Returns None (and prints why) if no secondary is currently
    discoverable to isolate.
    """
    secs = sorted(client.secondaries)
    if not secs:
        print("[partition] no secondary available to isolate; skipping "
              "network-partition scenario")
        return None
    host, port = secs[0]
    isolated = f"{host}:{port}"
    return _run_single(
        "network_partition_minority_isolated", logger,
        lambda: _with(NetworkPartitionInjector(client, isolated), run_experiment,
                       "network_partition_minority_isolated", target_node=isolated),
    )


# Convenience lookup so a generic "run this named scenario" entry point
# (see task1_common.py's run_one_scenario()) doesn't need an if/elif chain.
SCENARIO_RUNNERS = {
    "normal": run_scenario_normal,
    "secondary_failure": run_scenario_secondary_failure,
    "primary_failure": run_scenario_primary_failure,
    "network_partition_minority_isolated": run_scenario_partition,
}

