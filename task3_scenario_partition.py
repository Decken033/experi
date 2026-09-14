#!/usr/bin/env python3
"""
task3_scenario_partition.py

Run Task 3 (Monotonic-Writes: Config A vs Config B) under the
"network_partition_minority_isolated" scenario ONLY: one currently-healthy
secondary is cut off from the other two nodes at the network level (the
client stays on the majority side) for the duration of the experiment,
then connectivity is restored afterwards.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and either disconnect its Docker
network or add iptables rules, then later reverse that action - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

pin_reads=False is used here (same choice as task1_scenario_partition.py):
if reads stayed pinned to the isolated node, both configs would collapse to
100% availability loss for the whole run once the node drops out, telling
us nothing about MW. With unpinned reads, the driver fails over to a
healthy secondary and we can observe whether MW still holds through that
transition.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to isolate based on the CURRENT topology at startup.

Usage:
    python task3_scenario_partition.py
"""

from task3_common import run_one_scenario

if __name__ == "__main__":
    #run_one_scenario("network_partition_minority_isolated", pin_reads=False)
    run_one_scenario("network_partition_minority_isolated")

