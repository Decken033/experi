#!/usr/bin/env python3
"""
task2_scenario_partition.py

Run Task 2 (Monotonic Reads: Config A vs Config B) under the
"network_partition_minority_isolated" scenario ONLY: one currently-healthy
secondary is cut off from the other two nodes at the network level (the
client stays on the majority side) for the duration of the experiment, then
connectivity is restored afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and either disconnect its Docker
network or add iptables rules, then later reverse that action - type 'done'
at each prompt once you've done it on the correct machine.

Task2 does NOT stack its own artificial-lag injection on top of this
scenario's fault: the isolated node already creates real replication
divergence between it and the majority side, so adding another, unrelated
delayed member would confound the result (see task2_common.py's
run_experiment for the rationale).

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to isolate based on the CURRENT topology at startup.

Usage:
    python task2_scenario_partition.py
"""

from task2_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("network_partition_minority_isolated")

