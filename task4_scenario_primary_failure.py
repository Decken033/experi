#!/usr/bin/env python3
"""
task4_scenario_primary_failure.py

Run Task 4 (Writes-Follow-Reads: Config A vs B vs C) under the
"primary_failure" scenario ONLY: the current primary is stopped (forcing an
election) for the duration of the experiment, then restarted afterwards.

Uses pin_reads=False (the default - see task4_common.py): B's and O's reads
are unpinned, and the background writer (and B's write of y) automatically
retries against whichever node the driver elects as the new primary.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and stop/start its container - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

IMPORTANT: run this only when the replica set is currently healthy (a
primary is elected, all three nodes up) - the script picks which node is
currently primary based on the CURRENT topology at startup.

Usage:
    python task4_scenario_primary_failure.py
"""

from task4_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("primary_failure")

