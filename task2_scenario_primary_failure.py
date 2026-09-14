#!/usr/bin/env python3
"""
task2_scenario_primary_failure.py

Run Task 2 (Monotonic Reads: Config A vs Config B) under the
"primary_failure" scenario ONLY: the current primary is stopped, forcing an
election, for the duration of the experiment, then restarted afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and run `docker stop <container>`,
then later `docker start <container>` - type 'done' at each prompt once
you've done it on the correct machine.

Task2 does NOT stack its own artificial-lag injection on top of this
scenario's fault: the election and subsequent catch-up already create real
replication divergence, so adding another, unrelated delayed member would
confound the result (see task2_common.py's run_experiment for the rationale).

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering fault from a previous run) - the script picks
the primary to stop based on the CURRENT topology at startup.

Usage:
    python task2_scenario_primary_failure.py
"""

from task2_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("primary_failure")

