#!/usr/bin/env python3
"""
task1_scenario_primary_failure.py

Run Task 1 (Read-Your-Writes: Config A vs Config B) under the
"primary_failure" scenario ONLY: the CURRENT primary is stopped (forcing
an election among the remaining two nodes) for the duration of the
experiment, then restarted afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and run `docker stop <container>`,
then later `docker start <container>` - press ENTER at each prompt once
you've done it on the correct machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which node is "primary" based on the CURRENT topology at startup.

Usage:
    python task1_scenario_primary_failure.py
"""

from task1_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("primary_failure")
