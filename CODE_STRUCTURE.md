# Code File Structure

This document summarizes every Python file in this directory, grouped by role, and
how they depend on each other.

## Dependency graph

```
common.py  <-  scenarios.py  <-  task{1,2,3,4}_common.py  <-  task{1,2,3,4}_scenario_*.py
```

- `common.py` has no local dependencies (only stdlib + pymongo).
- `scenarios.py` imports `common`.
- Each `taskN_common.py` imports both `common` and `scenarios`.
- Each `taskN_scenario_*.py` only imports `run_one_scenario` from its matching
  `taskN_common.py`.

## Shared infrastructure

| File | Role |
|---|---|
| `common.py` | Lowest-level utilities: MongoDB connections, write-concern / read-concern helpers. Used by every task. |
| `scenarios.py` | Fault-injection and network-partition control (primary failure, secondary failure, partition). Depends on `common.py`; used by all four `taskN_common.py` modules. |

## Task 1 — Read-Your-Writes (Config A vs Config B)

| File | Role |
|---|---|
| `task1_common.py` | Core experiment logic. Imports `common`, `scenarios`. |
| `task1_scenario_normal.py` | Baseline run, no fault injected. |
| `task1_scenario_primary_failure.py` | Primary node is stopped during the run. |
| `task1_scenario_secondary_failure.py` | A secondary is stopped; reads are pinned to that exact node. |
| `task1_scenario_secondary_failure_unpinned.py` | Same secondary-failure scenario, but reads are NOT pinned to the failing node (control comparison). |
| `task1_scenario_partition.py` | A secondary is network-partitioned from the other two nodes. |

## Task 2

| File | Role |
|---|---|
| `task2_common.py` | Core experiment logic. Imports `common`, `scenarios`. |
| `task2_scenario_normal.py` | Baseline run, no fault injected. |
| `task2_scenario_primary_failure.py` | Primary node is stopped during the run. |
| `task2_scenario_secondary_failure.py` | A secondary is stopped during the run. |
| `task2_scenario_partition.py` | A secondary is network-partitioned from the other two nodes. |

## Task 3 — Monotonic Writes

| File | Role |
|---|---|
| `task3_common.py` | Core experiment logic. Imports `common`, `scenarios`. |
| `task3_scenario_normal.py` | Baseline run, no fault injected. |
| `task3_scenario_primary_failure.py` | Primary node is stopped during the run. |
| `task3_scenario_secondary_failure.py` | A secondary is stopped during the run. |
| `task3_scenario_partition.py` | A secondary is network-partitioned from the other two nodes. |

## Task 4 — Writes Follow Reads

| File | Role |
|---|---|
| `task4_common.py` | Core experiment logic. Imports `common`, `scenarios`. |
| `task4_scenario_normal.py` | Baseline run, no fault injected. |
| `task4_scenario_primary_failure.py` | Primary node is stopped during the run. |
| `task4_scenario_secondary_failure.py` | A secondary is stopped during the run. |
| `task4_scenario_partition.py` | A secondary is network-partitioned from the other two nodes. |
