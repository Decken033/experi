#!/usr/bin/env python3
"""
task4_scenario_normal.py

Run Task 4 (Writes-Follow-Reads: Config A vs B vs C) under the "normal"
scenario ONLY - no injected node failure or network partition.

This is the baseline run: use it first, before running any of the other
three task4_scenario_*.py scripts, so you have an un-faulted reference
result to compare the failure/partition scenarios against.

Usage:
    python task4_scenario_normal.py
"""

from task4_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("normal")

