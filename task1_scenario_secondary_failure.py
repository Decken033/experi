#!/usr/bin/env python3
"""
task1_scenario_secondary_failure.py

Run Task 1 (Read-Your-Writes: Config A vs Config B) under the
"secondary_failure" scenario ONLY: one currently-healthy secondary is
stopped for the duration of the experiment, then restarted afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and run `docker stop <container>`,
then later `docker start <container>` - press ENTER at each prompt once
you've done it on the correct machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to fail based on the CURRENT topology at startup.

Usage:
    python task1_scenario_secondary_failure.py
"""

from task1_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("secondary_failure")
