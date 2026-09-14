# Investigating Client-Centric Consistency in a Distributed Database

**Course / Project:** _<course name / project number>_
**Group members:** _<name 1>_, _<name 2>_, _<name 3>_
**Date:** _<submission date>_

---

## 1. Introduction

_Briefly state the goal: install a distributed database with tunable
consistency and experimentally investigate the four client-centric consistency
models — read-your-writes, monotonic-reads, monotonic-writes, and
writes-follow-reads — under different configurations and failure scenarios._

---

## 2. Chosen Database and Deployment

### 2.1 Database
- **System:** MongoDB (replica set)
- **Version:** _<output of `mongod --version` / `mongosh --version`>_
- **Driver:** PyMongo _<version>_, Python _<version>_
- **Why MongoDB:** tunable per-operation consistency via `writeConcern`,
  `readConcern`, `readPreference` and causally-consistent sessions.

### 2.2 Architecture
- 3 physical machines forming replica set `rs0` (1 primary + 2 secondaries),
  connected over a phone-hotspot LAN; 1 separate client machine.

| Role | Hostname | LAN IP:Port |
|------|----------|-------------|
| Primary (initial)   | _<host>_ | 172.20.10.2:27017 |
| Secondary           | _<host>_ | 172.20.10.3:27017 |
| Secondary           | _<host>_ | 172.20.10.5:27017 |
| Client              | _<host>_ | _<ip>_ |

_Include a simple architecture diagram here._

### 2.3 Installation Procedure
_Summarise the steps (see README): install MongoDB on each server; start each
`mongod --replSet rs0 --bind_ip localhost,<LAN-IP>`; open firewall port 27017;
`rs.initiate(...)` once; verify with `rs.status()`; install PyMongo on the
client._

---

## 3. Consistency Configurations Explored

### 3.1 Relevant parameters
- **writeConcern `w`** — `1` (primary only) vs `majority` (durable on a
  majority before acknowledging).
- **readConcern** — `local` (whatever the queried node has) vs `majority`
  (only majority-committed data).
- **readPreference** — `primary` vs `secondary` (reads from a possibly-lagging
  replica).
- **Causal consistency session** (`causal_consistency=True`) — attaches
  operation/cluster time so later operations observe data at least as recent as
  earlier ones in the session.

### 3.2 Configurations tested

| Config | writeConcern | readConcern | readPreference | Causal session |
|--------|--------------|-------------|----------------|----------------|
| A (weak)   | `w:1`      | `local`    | `secondary` | no  |
| B (strong) | `majority` | `majority` | `secondary` | yes |

---

## 4. Predictions

_State, per model, whether you expect it to hold under each configuration and
why. Suggested starting predictions:_

| Model | Config A (weak) | Config B (causal + majority) | Reasoning |
|-------|-----------------|------------------------------|-----------|
| Read-your-writes (RYW)   | **May violate** | Holds | Secondary can lag behind own write; causal session forces `afterClusterTime`. |
| Monotonic-reads (MR)     | **May violate** | Holds | Round-robin over secondaries with different lag can go backwards; causal cluster time only advances. |
| Monotonic-writes (MW)    | Holds | Holds | Single primary + ordered oplog; secondaries replay in order. |
| Writes-follow-reads (WFR)| **May violate** | Holds | Follow-up write can replicate before the read's version reaches another replica; causal session stamps the write with the read's time. |

---

## 5. Experiment Design

_For each task, describe what the script does and how a violation is detected._

- **Task 1 — RYW:** write to primary, immediately read same key from a
  secondary; count reads that fail to see the client's own write.
- **Task 2 — MR:** background writer increments a counter; reader reads the
  counter repeatedly and flags any read whose value is **older** than one
  already seen.
- **Task 3 — MW:** issue ordered writes W0..W(N-1) to distinct docs; after each
  write, read the sequence from a secondary and flag any "hole" (a later write
  visible while an earlier one is not).
- **Task 4 — WFR:** background writer advances item `x`; client reads `x`
  (value `r`) then writes `y = {based_on: r}`; an observer that sees `y` then
  reads `x` and flags any case where `x.seq < r`.

_Parameters used (iteration counts, durations), and the metric reported
(number of violations / stale reads)._

---

## 6. Results

_Fill in from the console output of each run. Repeat the table per scenario._

### 6.1 Scenario: Normal operation

| Task | Config A violations | Config B violations | Notes |
|------|---------------------|---------------------|-------|
| 1 RYW | _<n>_ | _<n>_ | |
| 2 MR  | _<n>_ | _<n>_ | |
| 3 MW  | _<n>_ | _<n>_ | |
| 4 WFR | _<n>_ | _<n>_ | |

### 6.2 Scenario: One secondary down / failover
_How you injected the fault; observed replication lag; results table._

### 6.3 Scenario: Network partition
_How you partitioned (firewall rule); observed lag growth; results table._

_Include representative console excerpts and/or plots of violations vs lag._

---

## 7. Discussion

- _Do the observations match the predictions in Section 4? Where they differ,
  explain why (e.g. MW never violated because of the single-primary oplog;
  weak config sometimes showed no violation because the LAN lag was too small —
  note that "no violation observed" ≠ "guaranteed")._
- _Effect of failure/partition on the number and severity of violations._

### 7.1 Limitations
- _Small cluster and low-latency LAN limit how often weak configs actually
  violate; violations are probabilistic, not guaranteed._
- _Single-process "observer" approximates a multi-client scenario._
- _Clock/lag measurements are coarse (`replSetGetStatus`)._

---

## 8. Conclusion

_One paragraph summarising which configurations upheld which client-centric
consistency models, and the key takeaway about tunable consistency._

---

## 9. References

1. MongoDB Manual — Read Concern. https://www.mongodb.com/docs/manual/reference/read-concern/
2. MongoDB Manual — Write Concern. https://www.mongodb.com/docs/manual/reference/write-concern/
3. MongoDB Manual — Read Preference. https://www.mongodb.com/docs/manual/core/read-preference/
4. MongoDB Manual — Causal Consistency and Read/Write Concerns. https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/
5. MongoDB Manual — Replica Set Oplog. https://www.mongodb.com/docs/manual/core/replica-set-oplog/
6. Tanenbaum & van Steen, *Distributed Systems*, client-centric consistency models.
7. _AI usage:_ portions of the code and this report were drafted with the
   assistance of Claude (Anthropic) and subsequently reviewed and verified by
   the group.

---

## Appendix A — Reproduction instructions
_See `README.md`. Commands to set up the replica set and run each task script._

## Appendix B — Source files
`common.py`, `task1_read_your_writes.py`, `task2_monotonic_reads.py`,
`task3_monotonic_writes.py`, `task4_writes_follow_reads.py`.
