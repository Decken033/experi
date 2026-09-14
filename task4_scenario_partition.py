#!/usr/bin/env python3
"""
task4_scenario_partition.py

Run Task 4 (Writes-Follow-Reads: Config A vs B vs C) under the
"network_partition_minority_isolated" scenario ONLY: one currently-healthy
secondary is cut off from the other two nodes at the network level (the
client stays on the majority side) for the duration of the experiment,
then connectivity is restored afterwards.

Uses pin_reads=False (the default, and the RECOMMENDED mode for Task 4 -
same reasoning as task1_scenario_partition.py's choice, plus the
task-4-specific caveat in task4_common.py): if B and O were pinned to the
isolated node, both would collapse to 100% availability loss for the whole
run once it drops out. With unpinned reads, the driver fails over to a
healthy secondary and we can observe whether WFR still holds - and whether
Config A's classic violation pattern appears - through that transition.

In DEPLOYMENT_MODE = "manual" (see scenarios.py), you will be prompted
mid-run to go to the affected machine and either disconnect its Docker
network or add iptables rules, then later reverse that action - type
'done' and press ENTER at each prompt once you've done it on the correct
machine.

IMPORTANT: run this only when the replica set is currently healthy (all
three nodes up, no lingering partition from a previous run) - the script
picks which secondary to isolate based on the CURRENT topology at startup.

Usage:
    python task4_scenario_partition.py
"""

from task4_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("network_partition_minority_isolated")

