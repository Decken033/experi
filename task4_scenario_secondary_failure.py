#!/usr/bin/env python3
"""
task4_scenario_secondary_failure.py

Run Task 4 (Writes-Follow-Reads: Config A vs B vs C) under the
"secondary_failure" scenario ONLY: one currently-healthy secondary is
stopped for the duration of the experiment, then restarted afterwards.

Uses pin_reads=False (the default, and the RECOMMENDED mode for Task 4 -
see the note in task4_common.py): B and O each use an unpinned
client/session, so the driver's own SDAM decides which secondary answers
each read and can fail over naturally once the stopped node's heartbeat
times out. This is what lets you see both (a) Config A's classic
cross-secondary WFR violation pattern, and (b) how failover affects it.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and stop/start its container - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up) - the script picks which secondary to fail based on the
CURRENT topology at startup.

Usage:
    python task4_scenario_secondary_failure.py
"""

from task4_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("secondary_failure")

