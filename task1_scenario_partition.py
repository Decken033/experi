#!/usr/bin/env python3
"""
task1_scenario_partition.py

Run Task 1 (Read-Your-Writes: Config A vs Config B) under the
"network_partition_minority_isolated" scenario ONLY: one currently-healthy
secondary is cut off from the other two nodes at the network level (the
client stays on the majority side) for the duration of the experiment,
then connectivity is restored afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and either disconnect its Docker
network or add iptables rules, then later reverse that action - press
ENTER at each prompt once you've done it on the correct machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to isolate based on the CURRENT topology at startup.

Usage:
    python task1_scenario_partition.py
"""

from task1_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("network_partition_minority_isolated")
