#!/usr/bin/env python3
"""
task2_scenario_secondary_failure.py

Run Task 2 (Monotonic Reads: Config A vs Config B) under the
"secondary_failure" scenario ONLY: one currently-healthy secondary is
stopped for the duration of the experiment, then restarted afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and run `docker stop <container>`,
then later `docker start <container>` - type 'done' at each prompt once
you've done it on the correct machine.

Task2 does NOT stack its own artificial-lag injection (DelayedSecondaryInjector)
on top of this scenario's fault: the stopped secondary already creates real
replication divergence between the surviving nodes, so adding another,
unrelated delayed member would confound the result (see task2_common.py's
run_experiment for the rationale).

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering fault from a previous run) - the script picks
which secondary to stop based on the CURRENT topology at startup.

Usage:
    python task2_scenario_secondary_failure.py
"""

from task2_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("secondary_failure")

