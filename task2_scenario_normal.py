#!/usr/bin/env python3
"""
task2_scenario_normal.py

Run Task 2 (Monotonic Reads: Config A vs Config B) under the "normal"
scenario ONLY - no injected node failure or network partition.

This is the baseline run: use it first, before running any of the other
three task2_scenario_*.py scripts, so you have an un-faulted reference
result to compare the failure/partition scenarios against.

Under "normal", task2's own artificial-lag injection (DelayedSecondaryInjector,
see task2_common.py) is applied: one secondary is temporarily reconfigured as
a delayed member so Config A actually has a chance to observe a
monotonic-reads violation on an otherwise low-lag LAN. The lag is kept ACTIVE
across both Config A and Config B (not reverted in between), and Config B
explicitly alternates its causal-session reads between the same fresh/lagging
pair Config A used, so the two configurations are compared under identical
replication conditions.

Usage:
    python task2_scenario_normal.py
"""

from task2_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("normal")
