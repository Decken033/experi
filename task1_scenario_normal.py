#!/usr/bin/env python3
"""
task1_scenario_normal.py

Run Task 1 (Read-Your-Writes: Config A vs Config B) under the "normal"
scenario ONLY - no injected node failure or network partition.

This is the baseline run: use it first, before running any of the other
three task1_scenario_*.py scripts, so you have an un-faulted reference
result to compare the failure/partition scenarios against.

Usage:
    python task1_scenario_normal.py
"""

from task1_common import run_one_scenario

if __name__ == "__main__":
    run_one_scenario("normal")
