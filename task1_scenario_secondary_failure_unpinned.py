#!/usr/bin/env python3
"""
task1_scenario_secondary_failure_unpinned.py

Run Task 1 (Read-Your-Writes: Config A vs Config B) under the
"secondary_failure" scenario, but WITHOUT pinning reads to the specific
secondary being stopped.

Compare this against task1_scenario_secondary_failure.py (the pinned
version):

  PINNED   (task1_scenario_secondary_failure.py):
      Reads are forced onto the exact node scenarios.py is about to stop,
      via a custom server_selector. This guarantees the experiment truly
      exercises the failing node, but once that node is fully down, EVERY
      read fails with ServerSelectionTimeoutError - both Config A and
      Config B collapse to 100% availability loss / 0 effective
      iterations. That run answers "what happens if you keep hammering a
      dead node for reads" - not very informative about RYW itself, but
      important to show the availability cost of a naive pin.

  UNPINNED (this script):
      Reads use a plain client with NO custom server_selector - exactly
      like a normal application would connect. readPreference=SECONDARY
      is resolved by the driver's own server selection (SDAM) on every
      read. When the node currently answering reads is the one that gets
      stopped, the driver's heartbeat mechanism should notice within a
      few seconds and move subsequent reads to the OTHER, still-healthy
      secondary automatically - the same way a real client would fail
      over in production. This lets you observe:
        - whether reads keep succeeding at all once failover completes
          (i.e. does the driver actually move traffic to the surviving
          secondary, or does it also see extended availability loss
          during the failover transition);
        - once reads land on the surviving secondary, does Config A still
          show RYW violations there, and does Config B still hold RYW -
          this is the "does the conclusion still hold on a node that
          ISN'T the one being killed" comparison point.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and run `docker stop <container>`,
then later `docker start <container>` - at each prompt, only after you've
actually done it on the correct machine, type 'done' and press ENTER to
confirm.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to fail based on the CURRENT topology at startup.

This writes its own separate JSONL log
(results/task1_ryw_secondary_failure_unpinned_<timestamp>.jsonl) so it
never overwrites or gets mixed up with the pinned run's log.

Usage:
    python task1_scenario_secondary_failure_unpinned.py
"""

from task1_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("secondary_failure", pin_reads=False)

