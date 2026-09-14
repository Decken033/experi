#!/usr/bin/env python3
"""
task3_scenario_secondary_failure.py

Run Task 3 (Monotonic-Writes: Config A vs Config B) under the
"secondary_failure" scenario ONLY: one currently-healthy secondary is
stopped for the duration of the experiment (Config A/B reads are pinned to
that exact node), then restarted afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and stop/start its container - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up) - the script picks which secondary to fail based on the
CURRENT topology at startup.

Usage:
    python task3_scenario_secondary_failure.py
"""

from task3_common import run_one_scenario

if __name__ == "__main__":
    #run_one_scenario("secondary_failure")
    run_one_scenario("secondary_failure", pin_reads=False)

