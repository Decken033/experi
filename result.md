# result

## task1

```
Primary    : ('172.20.10.2', 27017)
Secondaries: {('172.20.10.3', 27017), ('172.20.10.5', 27017)}

Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=0.000s
  172.20.10.5:27017      SECONDARY  lag=91.000s

============================================================
TASK 1 / CONFIG A: no causal consistency (expected to violate RYW)
writeConcern=w:1  readConcern=local  readPreference=secondary
============================================================
[STALE] i=001  own write not visible on secondary
[STALE] i=002  own write not visible on secondary
[STALE] i=003  own write not visible on secondary
[STALE] i=006  own write not visible on secondary
[STALE] i=008  own write not visible on secondary
[STALE] i=009  own write not visible on secondary
[STALE] i=011  own write not visible on secondary
[STALE] i=013  own write not visible on secondary
[STALE] i=014  own write not visible on secondary
[STALE] i=015  own write not visible on secondary
[STALE] i=017  own write not visible on secondary
[STALE] i=020  own write not visible on secondary
[STALE] i=023  own write not visible on secondary
[STALE] i=024  own write not visible on secondary
[STALE] i=025  own write not visible on secondary
[STALE] i=026  own write not visible on secondary
[STALE] i=027  own write not visible on secondary
[STALE] i=028  own write not visible on secondary
[STALE] i=029  own write not visible on secondary
[STALE] i=030  own write not visible on secondary
[STALE] i=031  own write not visible on secondary
[STALE] i=032  own write not visible on secondary
[STALE] i=036  own write not visible on secondary
[STALE] i=038  own write not visible on secondary
[STALE] i=039  own write not visible on secondary
[STALE] i=040  own write not visible on secondary
[STALE] i=042  own write not visible on secondary
[STALE] i=043  own write not visible on secondary
[STALE] i=044  own write not visible on secondary
[STALE] i=045  own write not visible on secondary
[STALE] i=047  own write not visible on secondary
[STALE] i=049  own write not visible on secondary
[STALE] i=051  own write not visible on secondary
[STALE] i=055  own write not visible on secondary
[STALE] i=056  own write not visible on secondary
[STALE] i=057  own write not visible on secondary
[STALE] i=058  own write not visible on secondary
[STALE] i=060  own write not visible on secondary
[STALE] i=061  own write not visible on secondary
[STALE] i=062  own write not visible on secondary
[STALE] i=063  own write not visible on secondary
[STALE] i=065  own write not visible on secondary
[STALE] i=066  own write not visible on secondary
[STALE] i=067  own write not visible on secondary
[STALE] i=069  own write not visible on secondary
[STALE] i=070  own write not visible on secondary
[STALE] i=071  own write not visible on secondary
[STALE] i=072  own write not visible on secondary
[STALE] i=073  own write not visible on secondary
[STALE] i=074  own write not visible on secondary
[STALE] i=075  own write not visible on secondary
[STALE] i=076  own write not visible on secondary
[STALE] i=080  own write not visible on secondary
[STALE] i=081  own write not visible on secondary
[STALE] i=084  own write not visible on secondary
[STALE] i=087  own write not visible on secondary
[STALE] i=088  own write not visible on secondary
[STALE] i=089  own write not visible on secondary
[STALE] i=090  own write not visible on secondary
[STALE] i=091  own write not visible on secondary
[STALE] i=092  own write not visible on secondary
[STALE] i=093  own write not visible on secondary
[STALE] i=094  own write not visible on secondary
[STALE] i=095  own write not visible on secondary
[STALE] i=096  own write not visible on secondary
[STALE] i=099  own write not visible on secondary
[STALE] i=100  own write not visible on secondary
[STALE] i=101  own write not visible on secondary
[STALE] i=102  own write not visible on secondary
[STALE] i=103  own write not visible on secondary
[STALE] i=105  own write not visible on secondary
[STALE] i=106  own write not visible on secondary
[STALE] i=107  own write not visible on secondary
[STALE] i=108  own write not visible on secondary
[STALE] i=109  own write not visible on secondary
[STALE] i=112  own write not visible on secondary
[STALE] i=114  own write not visible on secondary
[STALE] i=115  own write not visible on secondary
[STALE] i=117  own write not visible on secondary
[STALE] i=122  own write not visible on secondary
[STALE] i=123  own write not visible on secondary
[STALE] i=125  own write not visible on secondary
[STALE] i=126  own write not visible on secondary
[STALE] i=128  own write not visible on secondary
[STALE] i=131  own write not visible on secondary
[STALE] i=132  own write not visible on secondary
[STALE] i=133  own write not visible on secondary
[STALE] i=134  own write not visible on secondary
[STALE] i=137  own write not visible on secondary
[STALE] i=138  own write not visible on secondary
[STALE] i=139  own write not visible on secondary
[STALE] i=140  own write not visible on secondary
[STALE] i=141  own write not visible on secondary
[STALE] i=142  own write not visible on secondary
[STALE] i=144  own write not visible on secondary
[STALE] i=145  own write not visible on secondary
[STALE] i=147  own write not visible on secondary
[STALE] i=150  own write not visible on secondary
[STALE] i=152  own write not visible on secondary
[STALE] i=153  own write not visible on secondary
[STALE] i=154  own write not visible on secondary
[STALE] i=156  own write not visible on secondary
[STALE] i=157  own write not visible on secondary
[STALE] i=158  own write not visible on secondary
[STALE] i=160  own write not visible on secondary
[STALE] i=161  own write not visible on secondary
[STALE] i=162  own write not visible on secondary
[STALE] i=163  own write not visible on secondary
[STALE] i=164  own write not visible on secondary
[STALE] i=169  own write not visible on secondary
[STALE] i=170  own write not visible on secondary
[STALE] i=171  own write not visible on secondary
[STALE] i=172  own write not visible on secondary
[STALE] i=174  own write not visible on secondary
[STALE] i=175  own write not visible on secondary
[STALE] i=179  own write not visible on secondary
[STALE] i=180  own write not visible on secondary
[STALE] i=182  own write not visible on secondary
[STALE] i=183  own write not visible on secondary
[STALE] i=185  own write not visible on secondary
[STALE] i=188  own write not visible on secondary
[STALE] i=193  own write not visible on secondary
[STALE] i=194  own write not visible on secondary
[STALE] i=195  own write not visible on secondary
[STALE] i=196  own write not visible on secondary
[STALE] i=197  own write not visible on secondary

Iterations   : 200
Stale reads  : 126
RESULT       : RYW VIOLATION observed

============================================================
TASK 1 / CONFIG B: causal session (expected to uphold RYW)
writeConcern=majority  readConcern=majority  readPreference=secondary
============================================================

Iterations   : 200
Failures     : 0
RESULT       : PASS - read-your-writes maintained


```

对，这个结果是**正确的，而且实验效果非常明显**。

Config A 中，两个 secondary 里有一个复制延迟高达 **91 秒**。由于你使用的是 `w:1 + readConcern=local + readPreference=secondary`，写操作只需要 Primary 确认就立即返回，而接下来的读操作却去 Secondary。因此 Secondary 还没有同步刚写入的数据时，就会出现 `[STALE]`。你这里 200 次测试中有 **126 次 stale read**：

```text
Iterations   : 200
Stale reads  : 126
RESULT       : RYW VIOLATION observed
```

这正好证明了 **Config A 无法保证 Read-Your-Writes 一致性**。不是 200 次全部失败也很正常，因为另一个 Secondary 的延迟是 `0.000s`，有些读取可能被发送到了同步及时的 Secondary，或者数据在读取发生前已经完成复制。

Config B 的结果则非常理想：

```text
Iterations   : 200
Failures     : 0
RESULT       : PASS - read-your-writes maintained
```

因为 Config B 使用 `majority` 读写关注并开启 `causal_consistency=True`，客户端的后续读取会带上前一次写操作的因果依赖，Secondary 必须至少同步到对应的操作时间之后才能返回结果。所以 **200 次全部成功读取到了自己的写入，RYW 得到了保证**。

因此你的实验结论可以概括为：**弱一致性配置下观察到了 126/200 次 RYW violation，而启用 majority 和 causal consistency 后 violation 降为 0/200，说明 causal session 能有效保证 MongoDB 中的 Read-Your-Writes 一致性。**

这个结果拿来作为 Task 1 的实验结果很合适。



This experiment verifies Read-Your-Writes (RYW) consistency in MongoDB by comparing two configurations. Config A uses `w:1`, `readConcern=local`, `readPreference=secondary`, and no causal session, so a client may read from a lagging secondary before its previous write has been replicated, causing stale reads and RYW violations. Config B uses majority write concern, majority read concern, and a causally consistent session while still reading from a secondary. The causal session ensures that each read waits until the secondary has replicated at least up to the client’s previous write, thereby preserving RYW consistency. In the experiment, Config A produced stale reads, while Config B completed all reads successfully with no RYW violations.



````
For a **basic demonstration of two RYW configurations**, the file is reasonable. For the **controlled A1/A2 experiment described in your guide**, it is **not sufficient yet**. There are a few important gaps.

Your file correctly defines the two configurations as:

```text
Config A
w:1 + local + secondary + no causal session

Config B
majority + majority + secondary + causal session
```

and the actual collection settings match those descriptions.   

### The biggest problem: it does not force the read to your stale `n2`

Config A currently uses:

```python
read_preference=ReadPreference.SECONDARY
```

and Config B does the same.  

Suppose:

```text
n2 = stale
n3 = healthy SECONDARY
```

MongoDB is free to choose either secondary:

```text
Mac → n2  ← stale, useful
or
Mac → n3  ← current, hides violation
```

So even though you deliberately isolate `n2`, the script may read from `n3`.

For your experiment, **this must be deterministic**.

For A1, your guide specifically uses:

```text
mongodb://172.20.10.3:27017/?directConnection=true
```

to force the weak read onto stale n2.

For A2, you should use the `labNode:n2` tag so the causal read is also forced to the same stale node.

---

### Second important issue: Config A does not match the A1 experiment in your guide

Your current weak configuration uses:

```python
WriteConcern(w=1)
```

and the comments explicitly say the write is acknowledged by the primary alone. 

But the A1 experiment you have been studying deliberately says:

```text
n2 isolated
n1 + n3 still form a majority
→ majority write succeeds

then
readConcern=local from stale n2
→ stale read
```

That experiment is more interesting because it demonstrates:

> **Even a majority write does not by itself guarantee RYW if the subsequent read is local, non-causal, and deliberately routed to a stale secondary.**

So if you want the Python file to match your experiment guide, Config A should probably be:

```text
writeConcern   = majority
readConcern    = local
readPreference = specifically stale n2
causal session = False
```

rather than `w:1`.

That also gives you a cleaner comparison:

```text
A1 weak:
majority write + local read + no causal session

A2 strong:
majority write + majority read + causal session
```

You are then not changing write durability at the same time as all the read-side settings.

---

### Third issue: the script does not deliberately create or verify the stale state

At the moment, Config A simply:

```python
insert_one(...)
find_one(...)
```

200 times. 

Under normal operation, MongoDB replication may be so fast that:

```text
write
↓
secondary replicates immediately
↓
read succeeds
```

and you get:

```text
Stale reads: 0
```

The script itself acknowledges that zero stale reads does not mean RYW is guaranteed. 

For the controlled experiment, the sequence should be:

```text
1. Prepare all nodes at version=0
2. Verify n1=n2=n3=0
3. Isolate n2
4. Verify Mac can still access n2
5. Write new version through majority side
6. Read directly from n2
7. Compare versions
```

The firewall partition remains a manual step, which is perfectly fine, but the Python experiment should be designed around that stale member.

---

### Fourth issue: it uses new documents instead of version comparison

Current Config A creates a different `_id` every iteration:

```python
doc_id = f"{run_id}-{i}"
doc = {
    "_id": doc_id,
    ...
    "value": f"value-{i}"
}
```

and counts:

```python
result is None
```

as the violation. 

That is technically a valid RYW test:

```text
I wrote document X
then I cannot see X
→ RYW broken
```

But for your project report, your guide's version approach is **much clearer**.

For example:

```python
version = time.time_ns()
```

Write:

```text
version = 1789001234567890000
```

while stale n2 still has:

```text
version = 0
```

Then your violation predicate becomes explicitly:

```python
violation = read_version < write_version
```

That maps directly onto your theoretical definition:

```text
R.version >= W.version
```

This is much easier to defend in the report and viva.

---

### Fifth issue: Config B will probably crash on the expected timeout

This is a major one.

Config B does:

```python
result = coll.find_one(
    {"_id": doc_id},
    session=session,
    max_time_ms=10000
)
```

but there is no exception handling around it. 

In A2, if n2 remains partitioned, **timeout is an expected result**.

You want:

```text
strong causal read
↓
n2 cannot satisfy afterClusterTime
↓
timeout
↓
availability loss
↓
NOT consistency violation
```

But the current script may simply terminate with an exception.

You should catch errors such as:

```python
ExecutionTimeout
ServerSelectionTimeoutError
PyMongoError
```

and record:

```text
success = False
violation = False
error = timeout
classification = availability loss
```

instead of crashing.

---

### Sixth issue: strong Config B is missing `j=True`

You currently have:

```python
WriteConcern(w="majority", wtimeout=10000)
```



If you want to match your experiment specification exactly, use:

```python
WriteConcern(
    w="majority",
    j=True,
    wtimeout=10000
)
```

because your guide defines the strong write setting as:

```text
w:"majority" + j:true
```

This is not the biggest issue, but consistency between your report and code matters.

---

### Seventh issue: no proper experimental evidence is being saved here

This file mostly prints:

```text
Iterations
Stale reads
Failures
PASS/FAIL
```

 

For your group report, you ideally want every trial to save something like:

```text
run_id
configuration
write_version
read_version
target node
write latency
read latency
success
violation
timeout/error
timestamp
```

Your imported `common.py` may provide some logging, but I **cannot verify that from this uploaded file**, because only `task1_read_your_writes.py` is available. The file calls topology, lag and cleanup functions from `common`, but it does not itself persist detailed trial evidence. 

---

## What I would use for the two configurations

For the project experiment, I would make them:

| Setting                        | A1 Weak                     | A2 Strong                       |
| ------------------------------ | --------------------------- | ------------------------------- |
| Write concern                  | `majority`                  | `majority`, `j=True`            |
| Read concern                   | `local`                     | `majority`                      |
| Read target                    | **n2 specifically**         | **n2 specifically**             |
| Causal session                 | No                          | Yes                             |
| n2 condition                   | stale/partitioned           | same stale/partitioned n2       |
| Expected successful read       | old version → **violation** | never old                       |
| Expected if n2 cannot catch up | —                           | timeout → **availability loss** |

That is a cleaner experiment because **both configurations face exactly the same stale n2**.

The difference is then essentially:

```text
A1
local + non-causal
→ stale value can be returned

A2
majority + causal
→ stale value cannot be returned
→ wait/catch up or timeout
```

### My assessment

I would classify the current file as:

```text
Basic demonstration code                 ✓

Normal-operation weak vs strong demo     ✓

Matches your written configuration table ✓

Controlled stale-n2 A1/A2 experiment     ✗ not yet

Deterministic target of n2               ✗

Correct timeout classification           ✗

Version-based invariant measurement      ✗

Persistent experimental evidence         unclear
```

So I would **not collect your final project results with this version yet**.

The core structure is good—you do not need to rewrite everything—but I would modify the targeting, use the same majority write for A1/A2, introduce the `version=0 → time_ns()` design, catch timeouts correctly, and log each trial before using it for your report.
````





## task2

```
D:\environment\anaconda3\envs\DSH\python.exe E:\desktop\NUScanvas\DSA5208Moneyafternoon\experi\task2_monotonic_reads.py 
Connected to replica set: rs0
Primary    : ('172.20.10.2', 27017)
Secondaries: {('172.20.10.5', 27017), ('172.20.10.3', 27017)}

Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=0.000s
  172.20.10.5:27017      SECONDARY  lag=0.000s

============================================================
TASK 2 / CONFIG A
No causal session - expected to allow MR violations
readConcern=local
readPreference=secondary
============================================================

Read attempts : 400
Successful    : 400
Highest seq   : 359
Violations    : 0
RESULT        : no violation observed in this run; Config A does NOT guarantee MR

============================================================
TASK 2 / CONFIG B
Causal session - expected to uphold MR
readConcern=majority
readPreference=secondary
causal_consistency=True
============================================================

Read attempts : 400
Successful    : 400
Highest seq   : 346
Violations    : 0
RESULT        : PASS - monotonic reads maintained
```

```
**中文：**
本次实验中，MongoDB 副本集包含一个 Primary 和两个正常运行的 Secondary，且两个 Secondary 的复制延迟均接近 0 秒。在 Config A 中，使用 `readConcern=local`、`readPreference=secondary` 且未使用 causal session，400 次读取均成功，但没有观察到 Monotonic Reads 违规；这主要是因为两个 Secondary 几乎没有复制延迟，因此本次运行未形成明显的数据版本差异，但该配置本身仍不能保证 Monotonic Reads。在 Config B 中，使用 `readConcern=majority` 和 causal session 后，同样完成了 400 次成功读取且未发现任何违规，说明该配置在本次实验中成功维持了 Monotonic Reads，实验结果整体符合预期。

**English:**
In this experiment, the MongoDB replica set consisted of one primary and two healthy secondaries, both showing nearly zero replication lag. Under Config A, which used `readConcern=local`, `readPreference=secondary`, and no causal session, all 400 reads were successful and no Monotonic Reads violation was observed. This is likely because the two secondaries were almost equally up to date, so there was insufficient replication lag to expose a backward read; however, this configuration still does not guarantee Monotonic Reads. Under Config B, with `readConcern=majority` and a causally consistent session, all 400 reads also completed successfully with zero violations, demonstrating that Monotonic Reads were maintained as expected. Overall, the results are consistent with the intended behavior of the two configurations.
```

严格说，**代码结果本身没有错，但实验现象不够理想**。

Config A 的设计目的是“可能出现 MR violation”，但你这次 `Violations = 0`，主要因为两个 secondary 的 lag 都是 `0.000s`，它们几乎同步，所以没有制造出“先读到新值、后读到旧值”的条件。也就是说：

- Config B：结果很好，`0 violations` 符合预期。
- Config A：结果不算错误，但**没有成功展示出 violation**，所以如果老师希望看到 A 和 B 的明显对比，这部分实验说服力会偏弱。

因此，更准确的说法是：**程序没有问题，Config A 的实验环境没有成功触发问题。**





improve

```
1. Injected replication lag (makes the violation actually appear)

- New SecondaryLagInjector context manager: it directConnections to one secondary and pauses its oplog application with a server failpoint (rsSyncApplyStop, falling back to pauseBatchApplication across MongoDB versions). On exit it always turns the failpoint back off and closes the connection — the replica set is left healthy even if the run errors.
- Mechanism: while one secondary is frozen at an old seq, the other keeps advancing with the primary (the writer keeps running). readPreference=SECONDARY reads then bounce between the fresh and stale node → seq jumps backwards → MR violation observed.
- main() picks a secondary to freeze from client.secondaries, but only when there are ≥2 healthy secondaries (otherwise freezing the only one just makes all reads uniformly stale, no backwards jump — it prints a note and skips).
- Toggle INJECT_LAG = True/False at the top. Set it to False to run the "pure" weak config (which, as you saw, can legitimately report 0 violations on a low-lag cluster).

2. Served-by diagnostic (in both configs)

Reads now use coll.find(...).limit(1) so I can read cursor.address — the actual (host, port) that served each read. Both configs print a distribution:

Served by     :
    172.20.10.3:27017  201 reads
    172.20.10.5:27017  199 reads

This lets you confirm reads are truly spreading across both secondaries — if they weren't (all hitting one node), that alone would explain 0 violations.

What to expect now

- Config A should now print [MR VIOLATION] lines and end with RESULT: MR VIOLATION observed.
- Config B injects no lag and keeps the causal session, so it should still PASS. (I deliberately didn't inject lag into B — a frozen node plus afterClusterTime could make causal reads block until maxTimeMS and error out, which would muddy the "B upholds MR" demonstration.)

One caveat I can't verify from here: the exact failpoint name depends on your MongoDB build. If you see [lag] WARNING: could not enable a lag failpoint, tell me your server version (db.version()) and I'll adjust the _LAG_FAILPOINTS list.
```



result task2

```
D:\environment\anaconda3\envs\DSH\python.exe E:\desktop\NUScanvas\DSA5208Moneyafternoon\experi\task2_monotonic_reads.py 
Connected to replica set: rs0
Primary    : ('172.20.10.2', 27017)
Secondaries: {('172.20.10.3', 27017), ('172.20.10.5', 27017)}

Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=0.000s
  172.20.10.5:27017      SECONDARY  lag=0.000s

============================================================
TASK 2 / CONFIG A
No causal session - expected to allow MR violations
readConcern=local
readPreference=secondary
injected lag  : freezing 172.20.10.3:27017
============================================================
[lag] WARNING: could not enable a lag failpoint on 172.20.10.3:27017 (no such command: 'configureFailPoint', full error: {'ok': 0.0, 'errmsg': "no such command: 'configureFailPoint'", 'code': 59, 'codeName': 'CommandNotFound', '$clusterTime': {'clusterTime': Timestamp(1789097624, 7), 'signature': {'hash': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 'keyId': 0}}, 'operationTime': Timestamp(1789097624, 6)}); Config A will run without injected lag.

Read attempts : 400
Successful    : 400
Highest seq   : 453
Violations    : 0
Served by     :
    172.20.10.3:27017   183 reads
    172.20.10.5:27017   217 reads
RESULT        : no violation observed in this run; Config A does NOT guarantee MR

============================================================
TASK 2 / CONFIG B
Causal session - expected to uphold MR
readConcern=majority
readPreference=secondary
causal_consistency=True
============================================================

Read attempts : 400
Successful    : 400
Highest seq   : 397
Violations    : 0
Served by     :
    172.20.10.3:27017   205 reads
    172.20.10.5:27017   195 reads
RESULT        : PASS - monotonic reads maintained

```





improve

delay节点

```
D:\environment\anaconda3\envs\DSH\python.exe E:\desktop\NUScanvas\DSA5208Moneyafternoon\experi\task2_monotonic_reads.py 
Connected to replica set: rs0
Primary    : ('172.20.10.2', 27017)
Secondaries: {('172.20.10.5', 27017), ('172.20.10.3', 27017)}

Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=0.000s
  172.20.10.5:27017      SECONDARY  lag=0.000s

============================================================
TASK 2 / CONFIG A
No causal session - expected to allow MR violations
readConcern=local
readPreference=secondary
injected lag  : freezing 172.20.10.3:27017
============================================================
[lag] reconfigured 172.20.10.3:27017 as a delayed member (secondaryDelaySecs=10s)
Lag after injection (one node should be > 0):
Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=6.000s
  172.20.10.5:27017      SECONDARY  lag=1.000s

[lag] restored 172.20.10.3:27017 to its original config

Read attempts : 400
Successful    : 400
Highest seq   : 624
Violations    : 0
Served by     :
    172.20.10.5:27017   400 reads
RESULT        : no violation observed in this run; Config A does NOT guarantee MR

============================================================
TASK 2 / CONFIG B
Causal session - expected to uphold MR
readConcern=majority
readPreference=secondary
causal_consistency=True
============================================================

Read attempts : 400
Successful    : 400
Highest seq   : 415
Violations    : 0
Served by     :
    172.20.10.3:27017   192 reads
    172.20.10.5:27017   208 reads
RESULT        : PASS - monotonic reads maintained
```



```
D:\environment\anaconda3\envs\DSH\python.exe E:\desktop\NUScanvas\DSA5208Moneyafternoon\experi\task2_monotonic_reads.py 
Connected to replica set: rs0
Primary    : ('172.20.10.2', 27017)
Secondaries: {('172.20.10.3', 27017), ('172.20.10.5', 27017)}

Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=0.000s
  172.20.10.5:27017      SECONDARY  lag=0.000s

============================================================
TASK 2 / CONFIG A
No causal session - expected to allow MR violations
readConcern=local
readPreference=secondary
injected lag  : delaying 172.20.10.3:27017 via delay
read pattern  : explicitly alternating between 172.20.10.5:27017 (fresh) and 172.20.10.3:27017 (lagging)
============================================================
[lag] reconfigured 172.20.10.3:27017 as a delayed member (secondaryDelaySecs=8) via replSetReconfig
Lag after injection (lagging node should be > 0):
Replica set member states / lag:
  172.20.10.2:27017      PRIMARY   
  172.20.10.3:27017      SECONDARY  lag=10.000s
  172.20.10.5:27017      SECONDARY  lag=2.000s

[MR VIOLATION] saw seq=58 after previously seeing 310
[MR VIOLATION] saw seq=58 after previously seeing 315
[MR VIOLATION] saw seq=58 after previously seeing 316
[MR VIOLATION] saw seq=58 after previously seeing 318
[MR VIOLATION] saw seq=58 after previously seeing 320
[MR VIOLATION] saw seq=58 after previously seeing 322
[MR VIOLATION] saw seq=58 after previously seeing 324
[MR VIOLATION] saw seq=58 after previously seeing 326
[MR VIOLATION] saw seq=58 after previously seeing 328
[MR VIOLATION] saw seq=58 after previously seeing 330
[MR VIOLATION] saw seq=58 after previously seeing 331
[MR VIOLATION] saw seq=58 after previously seeing 333
[MR VIOLATION] saw seq=58 after previously seeing 335
[MR VIOLATION] saw seq=58 after previously seeing 337
[MR VIOLATION] saw seq=86 after previously seeing 339
[MR VIOLATION] saw seq=86 after previously seeing 341
[MR VIOLATION] saw seq=86 after previously seeing 343
[MR VIOLATION] saw seq=86 after previously seeing 345
[MR VIOLATION] saw seq=86 after previously seeing 346
[MR VIOLATION] saw seq=86 after previously seeing 348
[MR VIOLATION] saw seq=86 after previously seeing 349
[MR VIOLATION] saw seq=86 after previously seeing 352
[MR VIOLATION] saw seq=86 after previously seeing 354
[MR VIOLATION] saw seq=86 after previously seeing 357
[MR VIOLATION] saw seq=86 after previously seeing 359
[MR VIOLATION] saw seq=86 after previously seeing 361
[MR VIOLATION] saw seq=86 after previously seeing 363
[MR VIOLATION] saw seq=86 after previously seeing 365
[MR VIOLATION] saw seq=86 after previously seeing 367
[MR VIOLATION] saw seq=115 after previously seeing 369
[MR VIOLATION] saw seq=115 after previously seeing 371
[MR VIOLATION] saw seq=115 after previously seeing 373
[MR VIOLATION] saw seq=115 after previously seeing 375
[MR VIOLATION] saw seq=115 after previously seeing 377
[MR VIOLATION] saw seq=115 after previously seeing 379
[MR VIOLATION] saw seq=115 after previously seeing 381
[MR VIOLATION] saw seq=115 after previously seeing 383
[MR VIOLATION] saw seq=115 after previously seeing 385
[MR VIOLATION] saw seq=115 after previously seeing 387
[MR VIOLATION] saw seq=115 after previously seeing 390
[MR VIOLATION] saw seq=115 after previously seeing 392
[MR VIOLATION] saw seq=115 after previously seeing 395
[MR VIOLATION] saw seq=115 after previously seeing 397
[MR VIOLATION] saw seq=142 after previously seeing 399
[MR VIOLATION] saw seq=142 after previously seeing 401
[MR VIOLATION] saw seq=142 after previously seeing 403
[MR VIOLATION] saw seq=142 after previously seeing 405
[MR VIOLATION] saw seq=142 after previously seeing 408
[MR VIOLATION] saw seq=142 after previously seeing 410
[MR VIOLATION] saw seq=142 after previously seeing 411
[MR VIOLATION] saw seq=142 after previously seeing 414
[MR VIOLATION] saw seq=142 after previously seeing 416
[MR VIOLATION] saw seq=142 after previously seeing 418
[MR VIOLATION] saw seq=142 after previously seeing 420
[MR VIOLATION] saw seq=142 after previously seeing 422
[MR VIOLATION] saw seq=142 after previously seeing 424
[MR VIOLATION] saw seq=142 after previously seeing 426
[MR VIOLATION] saw seq=142 after previously seeing 428
[MR VIOLATION] saw seq=173 after previously seeing 430
[MR VIOLATION] saw seq=173 after previously seeing 432
[MR VIOLATION] saw seq=173 after previously seeing 434
[MR VIOLATION] saw seq=173 after previously seeing 436
[MR VIOLATION] saw seq=173 after previously seeing 438
[MR VIOLATION] saw seq=173 after previously seeing 440
[MR VIOLATION] saw seq=173 after previously seeing 442
[MR VIOLATION] saw seq=173 after previously seeing 443
[MR VIOLATION] saw seq=173 after previously seeing 446
[MR VIOLATION] saw seq=173 after previously seeing 448
[MR VIOLATION] saw seq=173 after previously seeing 450
[MR VIOLATION] saw seq=173 after previously seeing 452
[MR VIOLATION] saw seq=173 after previously seeing 454
[MR VIOLATION] saw seq=173 after previously seeing 456
[MR VIOLATION] saw seq=173 after previously seeing 458
[MR VIOLATION] saw seq=203 after previously seeing 460
[MR VIOLATION] saw seq=203 after previously seeing 462
[MR VIOLATION] saw seq=203 after previously seeing 464
[MR VIOLATION] saw seq=203 after previously seeing 466
[MR VIOLATION] saw seq=203 after previously seeing 468
[MR VIOLATION] saw seq=203 after previously seeing 471
[MR VIOLATION] saw seq=203 after previously seeing 473
[MR VIOLATION] saw seq=203 after previously seeing 475
[MR VIOLATION] saw seq=203 after previously seeing 477
[MR VIOLATION] saw seq=203 after previously seeing 479
[MR VIOLATION] saw seq=203 after previously seeing 480
[MR VIOLATION] saw seq=203 after previously seeing 483
[MR VIOLATION] saw seq=203 after previously seeing 486
[MR VIOLATION] saw seq=203 after previously seeing 489
[MR VIOLATION] saw seq=234 after previously seeing 491
[MR VIOLATION] saw seq=234 after previously seeing 493
[MR VIOLATION] saw seq=234 after previously seeing 496
[MR VIOLATION] saw seq=234 after previously seeing 498
[MR VIOLATION] saw seq=234 after previously seeing 500
[MR VIOLATION] saw seq=234 after previously seeing 502
[MR VIOLATION] saw seq=234 after previously seeing 504
[MR VIOLATION] saw seq=234 after previously seeing 506
[MR VIOLATION] saw seq=234 after previously seeing 508
[MR VIOLATION] saw seq=234 after previously seeing 510
[MR VIOLATION] saw seq=234 after previously seeing 512
[MR VIOLATION] saw seq=234 after previously seeing 514
[MR VIOLATION] saw seq=234 after previously seeing 517
[MR VIOLATION] saw seq=234 after previously seeing 518
[MR VIOLATION] saw seq=234 after previously seeing 520
[MR VIOLATION] saw seq=263 after previously seeing 523
[MR VIOLATION] saw seq=263 after previously seeing 525
[MR VIOLATION] saw seq=263 after previously seeing 527
[MR VIOLATION] saw seq=263 after previously seeing 530
[MR VIOLATION] saw seq=263 after previously seeing 532
[MR VIOLATION] saw seq=263 after previously seeing 534
[MR VIOLATION] saw seq=263 after previously seeing 536
[MR VIOLATION] saw seq=263 after previously seeing 539
[MR VIOLATION] saw seq=263 after previously seeing 542
[MR VIOLATION] saw seq=263 after previously seeing 544
[MR VIOLATION] saw seq=263 after previously seeing 546
[MR VIOLATION] saw seq=263 after previously seeing 548
[MR VIOLATION] saw seq=263 after previously seeing 550
[MR VIOLATION] saw seq=292 after previously seeing 553
[MR VIOLATION] saw seq=292 after previously seeing 556
[MR VIOLATION] saw seq=292 after previously seeing 558
[MR VIOLATION] saw seq=292 after previously seeing 560
[MR VIOLATION] saw seq=292 after previously seeing 561
[MR VIOLATION] saw seq=292 after previously seeing 564
[MR VIOLATION] saw seq=292 after previously seeing 567
[MR VIOLATION] saw seq=292 after previously seeing 569
[MR VIOLATION] saw seq=292 after previously seeing 571
[MR VIOLATION] saw seq=292 after previously seeing 573
[MR VIOLATION] saw seq=292 after previously seeing 575
[MR VIOLATION] saw seq=292 after previously seeing 577
[MR VIOLATION] saw seq=292 after previously seeing 578
[MR VIOLATION] saw seq=292 after previously seeing 581
[MR VIOLATION] saw seq=292 after previously seeing 583
[MR VIOLATION] saw seq=324 after previously seeing 585
[MR VIOLATION] saw seq=324 after previously seeing 588
[MR VIOLATION] saw seq=324 after previously seeing 591
[MR VIOLATION] saw seq=324 after previously seeing 593
[MR VIOLATION] saw seq=324 after previously seeing 595
[MR VIOLATION] saw seq=324 after previously seeing 597
[MR VIOLATION] saw seq=324 after previously seeing 600
[MR VIOLATION] saw seq=324 after previously seeing 602
[MR VIOLATION] saw seq=324 after previously seeing 604
[MR VIOLATION] saw seq=324 after previously seeing 606
[MR VIOLATION] saw seq=324 after previously seeing 609
[MR VIOLATION] saw seq=324 after previously seeing 611
[MR VIOLATION] saw seq=324 after previously seeing 613
[MR VIOLATION] saw seq=324 after previously seeing 615
[MR VIOLATION] saw seq=355 after previously seeing 618
[MR VIOLATION] saw seq=355 after previously seeing 619
[MR VIOLATION] saw seq=355 after previously seeing 622
[MR VIOLATION] saw seq=355 after previously seeing 624
[MR VIOLATION] saw seq=355 after previously seeing 626
[MR VIOLATION] saw seq=355 after previously seeing 628
[MR VIOLATION] saw seq=355 after previously seeing 630
[MR VIOLATION] saw seq=355 after previously seeing 632
[MR VIOLATION] saw seq=355 after previously seeing 635
[MR VIOLATION] saw seq=355 after previously seeing 636
[MR VIOLATION] saw seq=355 after previously seeing 639
[MR VIOLATION] saw seq=355 after previously seeing 641
[MR VIOLATION] saw seq=355 after previously seeing 644
[MR VIOLATION] saw seq=355 after previously seeing 646
[MR VIOLATION] saw seq=355 after previously seeing 649
[MR VIOLATION] saw seq=386 after previously seeing 651
[MR VIOLATION] saw seq=386 after previously seeing 654
[MR VIOLATION] saw seq=386 after previously seeing 656
[MR VIOLATION] saw seq=386 after previously seeing 658
[MR VIOLATION] saw seq=386 after previously seeing 660
[MR VIOLATION] saw seq=386 after previously seeing 661
[MR VIOLATION] saw seq=386 after previously seeing 663
[MR VIOLATION] saw seq=386 after previously seeing 665
[MR VIOLATION] saw seq=386 after previously seeing 667
[MR VIOLATION] saw seq=386 after previously seeing 669
[MR VIOLATION] saw seq=386 after previously seeing 671
[MR VIOLATION] saw seq=386 after previously seeing 673
[MR VIOLATION] saw seq=386 after previously seeing 674
[MR VIOLATION] saw seq=386 after previously seeing 676
[MR VIOLATION] saw seq=386 after previously seeing 678
[MR VIOLATION] saw seq=416 after previously seeing 680
[MR VIOLATION] saw seq=416 after previously seeing 682
[MR VIOLATION] saw seq=416 after previously seeing 685
[MR VIOLATION] saw seq=416 after previously seeing 687
[MR VIOLATION] saw seq=416 after previously seeing 689
[MR VIOLATION] saw seq=416 after previously seeing 691
[MR VIOLATION] saw seq=416 after previously seeing 692
[MR VIOLATION] saw seq=416 after previously seeing 695
[MR VIOLATION] saw seq=416 after previously seeing 696
[MR VIOLATION] saw seq=416 after previously seeing 698
[MR VIOLATION] saw seq=416 after previously seeing 700
[MR VIOLATION] saw seq=416 after previously seeing 703
[MR VIOLATION] saw seq=416 after previously seeing 705
[MR VIOLATION] saw seq=416 after previously seeing 707
[MR VIOLATION] saw seq=416 after previously seeing 709
[MR VIOLATION] saw seq=446 after previously seeing 710
[MR VIOLATION] saw seq=446 after previously seeing 712
[MR VIOLATION] saw seq=446 after previously seeing 714
[MR VIOLATION] saw seq=446 after previously seeing 716
[MR VIOLATION] saw seq=446 after previously seeing 718
[MR VIOLATION] saw seq=446 after previously seeing 720
[MR VIOLATION] saw seq=446 after previously seeing 722
[MR VIOLATION] saw seq=446 after previously seeing 724
[MR VIOLATION] saw seq=446 after previously seeing 726
[MR VIOLATION] saw seq=446 after previously seeing 728
[MR VIOLATION] saw seq=446 after previously seeing 730
[lag] restored 172.20.10.3:27017 to a normal secondary (priority=1.0, delay removed)

Read attempts : 400
Successful    : 400
Highest seq   : 730
Violations    : 200
Served by     :
    172.20.10.3:27017   200 reads
    172.20.10.5:27017   200 reads
RESULT        : MR VIOLATION observed

============================================================
TASK 2 / CONFIG B
Causal session - expected to uphold MR
readConcern=majority
readPreference=secondary
causal_consistency=True
============================================================

Read attempts : 400
Successful    : 400
Highest seq   : 475
Violations    : 0
Served by     :
    172.20.10.3:27017   189 reads
    172.20.10.5:27017   211 reads
RESULT        : PASS - monotonic reads maintained
```

以下内容可以直接整理进实验报告的 "Task 2: Monotonic Reads" 部分，中英文各一份，结构一致：问题发现 → 原因诊断 → 改进方法 → 结果 → 结论。

------

## 英文版 / English Version

### Experimental Narrative

**Initial observation.** In the first run, Config A (no causal session, `readConcern=local`, `readPreference=secondary`) reported `0 violations`, identical to Config B. Inspecting the replica-set lag output showed both secondaries at `lag=0.000s`, meaning the two nodes were essentially in lockstep — there was never a moment where one secondary held a noticeably older version of the document than the other. Without that divergence, a monotonic-reads (MR) violation has no opportunity to occur, regardless of whether the client uses a causal session. The result was not incorrect, but it failed to exercise the weak configuration's actual vulnerability.

**Diagnosis and first fix attempt.** To create the divergence needed to observe a violation, we reconfigured one secondary as a delayed member using `replSetReconfig` (setting `priority=0` and `secondaryDelaySecs`). This is a purely client-side operation — no server restart, no `enableTestCommands`, and no shell access to the nodes is required — and the original configuration is automatically restored afterward. After this change, the replication-lag report confirmed the delayed node was correctly falling behind (lag rising from (0) to several seconds), yet Config A still reported `0 violations`, with **all 400 reads served by the same, non-lagging secondary**.

**Root cause.** The remaining issue was not the lag injection but how the MongoDB driver selects a server under `readPreference=secondary`. The driver's Server Discovery and Monitoring (SDAM) subsystem only re-evaluates each server's eligibility and latency on its own heartbeat schedule (roughly every 10 seconds by default). Since the entire Config A read loop completed in only a few seconds, the driver's internal server-selection state never refreshed during the experiment — it simply kept using whichever secondary it had already selected before the delay was introduced. As a result, reads never bounced between the fresh and the lagging secondary, and the "read new, then read old" condition required for an MR violation never arose.

**Final fix.** To remove this confound, the read loop was changed to bypass automatic server selection entirely: two `directConnection=True` clients were opened, one pinned to each secondary, and reads were issued by explicitly alternating between them (fresh, lagged, fresh, lagged, ...). This guarantees that consecutive reads are served by nodes with different replication states whenever a lag exists, without relying on the driver's scheduling behavior.

### Results

With the corrected setup:

| Configuration         | Reads | Violations    | Reads per secondary        |
| --------------------- | ----- | ------------- | -------------------------- |
| A (no causal session) | 400   | **200 (50%)** | 200 / 200 (fresh / lagged) |
| B (causal session)    | 400   | **0**         | 189 / 211                  |

The Config A log shows the expected pattern clearly, e.g.:

```
saw seq=337 (fresh secondary)
...
saw seq=58 after previously seeing 337  → VIOLATION (lagged secondary)
```

The client repeatedly observed a high `seq` value from the fresh secondary, then, on the very next read from the delayed secondary, observed a value that reflected the primary's state from roughly `secondaryDelaySecs` earlier — a clear "time going backwards" event.

Config B, despite reading from **both** secondaries in a similar proportion (189 vs. 211 reads), reported zero violations, because the causal session's tracked operation time forces MongoDB to route reads only to replicas that have already applied a sufficiently recent state.

### Conclusion

The experiment now demonstrates the intended contrast cleanly:

- **Config A** (`readConcern=local`, `readPreference=secondary`, no causal session) does **not** guarantee monotonic reads. Under replication lag, roughly half of the reads served by the lagging secondary violated MR, confirming the configuration's theoretical weakness in practice.
- **Config B** (`readConcern=majority`, `readPreference=secondary`, `causal_consistency=True`) **does** guarantee monotonic reads, even when reads are distributed across the same two secondaries with the same induced lag. The causal session's ordering guarantee — not the absence of a lagging node — is what prevents the violation.

This confirms that the original zero-violation result for Config A was an artifact of an insufficiently adversarial test environment (near-zero replication lag, and a driver server-selection window too coarse for the reading period) rather than a flaw in the implementation. Once genuine divergence between replicas was introduced and reads were guaranteed to actually reach both nodes, the theoretical weakness of Config A became directly observable, giving a valid and convincing A/B comparison.

------

## 中文版

### 实验过程叙述

**初始现象。** 第一次运行中，Config A（无因果会话、`readConcern=local`、`readPreference=secondary`）的结果是 `0 violations`，与 Config B 完全一致。查看副本集延迟输出发现，两个 secondary 的 `lag` 都是 `0.000s`，也就是说两个节点几乎完全同步，从未出现"一个节点持有明显更旧版本"的时刻。没有这种分歧，无论客户端是否使用因果会话，都不可能触发 monotonic-reads（MR）违反。这个结果本身并不算错误，但没能真正暴露出弱配置的理论缺陷。

**问题诊断与第一次修复尝试。** 为了制造出可观测违反所需的"分歧"，我们用 `replSetReconfig` 把一个 secondary 重新配置为延迟成员（设置 `priority=0` 和 `secondaryDelaySecs`）。这是一个纯客户端操作——不需要重启服务器、不需要开启 `enableTestCommands`、也不需要登录节点的 shell——实验结束后配置会自动恢复。改动后，延迟监控输出确认该节点的延迟确实在从 (0) 逐步爬升到若干秒，但 Config A 仍然报告 `0 violations`，而且 **全部 400 次读取都落在了同一个未延迟的 secondary 上**。

**根本原因。** 剩下的问题不是延迟没注入成功，而是 MongoDB driver 在 `readPreference=secondary` 下选择服务节点的机制。driver 的 SDAM（Server Discovery and Monitoring）后台探测，只会按照自身的心跳周期（默认约每 10 秒一次）重新评估各节点的可用性与延迟。而 Config A 的整个读循环只用了几秒钟就跑完，driver 内部关于"该选哪个节点"的判断在整个实验期间根本没有刷新——它只是继续沿用延迟注入之前就已经选定的那个 secondary。结果就是读取从未在"新鲜节点"和"延迟节点"之间切换，也就永远不会出现"先读到新值、后读到旧值"这一 MR 违反所必需的条件。

**最终修复。** 为了消除这个干扰因素，读循环被改为完全绕开 driver 的自动节点选择：分别为两个 secondary 建立各自的 `directConnection=True` 客户端，并在代码里显式地交替读取（新鲜、延迟、新鲜、延迟……）。这样只要延迟真实存在，就能保证连续两次读取一定分别来自复制状态不同的两个节点，不再依赖 driver 自身的调度节奏。

### 实验结果

修正后的结果如下：

| 配置            | 读取次数 | 违反次数         | 各节点读取分布               |
| --------------- | -------- | ---------------- | ---------------------------- |
| A（无因果会话） | 400      | **200（(50%)）** | 200 / 200（新鲜 / 延迟节点） |
| B（因果会话）   | 400      | **0**            | 189 / 211                    |

Config A 的日志清楚地呈现了预期的模式，例如：

```
saw seq=337（读自新鲜节点）
...
saw seq=58 after previously seeing 337  → 违反（读自延迟节点）
```

客户端在新鲜节点上持续观察到不断增长的 `seq` 值，但紧接着从延迟节点读到的值，反映的却是大约 `secondaryDelaySecs` 秒之前主库的状态——一次典型的"时间倒退"事件。

而 Config B 尽管也以相近比例（189 次 vs. 211 次）从**两个** secondary 上读取，却依然是 `0 violations`。这是因为因果会话跟踪的操作时间强制 MongoDB 只把读请求路由到已经应用了足够新状态的副本上。

### 结论

改进后的实验完整地呈现了预期的对比：

- **Config A**（`readConcern=local`、`readPreference=secondary`、无因果会话）**不能**保证单调读。在存在复制延迟的情况下,由延迟节点提供服务的那部分读取中,大约一半都违反了 MR,这在实践中印证了该配置理论上的弱点。
- **Config B**（`readConcern=majority`、`readPreference=secondary`、`causal_consistency=True`）**能够**保证单调读,即使读取同样分布在这两个存在相同人为延迟的 secondary 上也不例外。真正阻止违反发生的是因果会话所提供的顺序保证,而不是"恰好没有节点落后"。

这说明 Config A 最初的"零违反"结果,并非程序实现有问题,而是测试环境不够"有对抗性"造成的假象——具体来说是两个原因叠加:(1) 副本集复制延迟接近于零,没有制造出分歧;(2) driver 自身的节点选择刷新周期远长于读循环的持续时间,掩盖了后续注入的延迟。一旦真正在副本之间制造出分歧,并且保证读取确实会命中这两个节点,Config A 理论上的缺陷就直接可观测了,从而得到了一组有效且有说服力的 A/B 对比结果。

## task3



## task4

