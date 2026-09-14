#!/usr/bin/env python3
"""
task3_scenario_primary_failure.py

Run Task 3 (Monotonic-Writes: Config A vs Config B) under the
"primary_failure" scenario ONLY: the current primary is stopped (forcing an
election) for the duration of the experiment, then restarted afterwards.
No specific secondary is under test here, so Config A/B reads fall back to
a fresh discovery of whichever secondary exists after the election.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and stop/start its container - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

IMPORTANT: run this only when the replica set is currently healthy (a
primary is elected, all three nodes up) - the script picks which node is
currently primary based on the CURRENT topology at startup.

Usage:
    python task3_scenario_primary_failure.py
"""

from task3_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("primary_failure")

