# 05 — Apache Spark Memory Management: JVM, Python, Native Memory, and State

> **Version baseline:** Spark 4.3.0 unless noted. Memory defaults and container accounting vary by Spark release and cluster manager; validate against the deployed runtime.
>
> **Related guides:** [architecture](01-Core-Architecture.md), [partitioning and shuffle](03-Partitioning-Shuffling.md), [cluster sizing](10-Cluster-Resource-Tuning.md), [streaming state](08-Structured-Streaming.md), [debugging](11-Debugging-Monitoring.md).

## Table of Contents
1. Why Memory Management Is Central to Spark
2. The Full Memory Budget of an Executor (the big picture)
3. The Unified Memory Manager — Execution vs Storage
4. On-Heap vs Off-Heap Memory (internals)
5. Memory Overhead — What's Actually In It
6. Driver Memory Management
7. PySpark / Python Worker Memory (deep dive)
8. Shuffle Memory & Spill Mechanics
9. Broadcast Variables & Memory
10. Storage Levels & Caching Strategy
11. Complete Configuration Reference Table
12. Failure Modes & Diagnostic Playbook
13. Tuning Checklist / Best Practices
14. Interview Quick-Reference (rapid fire)
15. Serialization & Memory: Kryo vs Java
16. Structured Streaming State Store Memory
17. Executor Memory Sanity Checks & Practical Gotchas

## 1. Why Memory Management Is Central to Spark

Spark's core performance advantage over traditional MapReduce execution is the ability to pipeline many transformations and reuse data in memory rather than materializing every intermediate result to durable storage. Spark still uses disk for shuffle, spill, persistence, and recovery; it is not an in-memory-only engine. That advantage is also its biggest operational risk: nearly every hard failure in a Spark job — OOM errors, container kills, mysterious slowness, GC storms — traces back to memory being misallocated, undersized, or consumed by something unexpected. Understanding memory management isn't a niche topic in Spark; it's close to the center of "can this person actually run Spark in production," which is exactly why it's such a heavily weighted interview area.

Two facts drive almost everything below:
- Spark manages memory **per executor process**, and each executor is a JVM — so JVM memory behavior (heap, GC, native memory) matters even if you never write a line of Java/Scala.
- Spark's memory is **shared** across very different consumers (caching, shuffle, your own code, and — in PySpark — an entirely separate Python process) that all compete for the same finite box.

## 2. The Full Memory Budget of an Executor

This is the total memory a cluster manager (YARN container / Kubernetes pod / Standalone worker) actually reserves for one executor — it is larger than just `spark.executor.memory`, which is the single most common misunderstanding in memory tuning.

```
TOTAL EXECUTOR MEMORY (requested from YARN/K8s)
==================================================
   =  spark.executor.memory                 (ON-HEAP, JVM heap)
   +  spark.memory.offHeap.size             (OFF-HEAP, only if offHeap.enabled=true)
   +  spark.executor.memoryOverhead         (JVM internals, native overhead, NIO buffers)
   +  spark.executor.pyspark.memory         (PySpark workers — only if explicitly set;
                                              otherwise drawn from memoryOverhead)

┌────────────────────────────────────────────────────────────────────┐
│ ON-HEAP  (spark.executor.memory) — JVM heap, GC-managed             │
│                                                                      │
│  ├─ Reserved Memory (~300MB fixed) — Spark's own internal bookkeeping │
│  │                                                                  │
│  └─ Usable Memory = (heap − reserved) × spark.memory.fraction      │
│       (default 0.6)                                                │
│        │                                                            │
│        ├─ ON-HEAP EXECUTION  — shuffle, sort, join, aggregation    │
│        ├─ ON-HEAP STORAGE    — cached data, broadcast variables     │
│        │   (dynamically share the pool; split ratio governed by    │
│        │    spark.memory.storageFraction, default 0.5; execution   │
│        │    can evict storage, never the reverse)                  │
│        │                                                            │
│        └─ USER MEMORY (remaining 0.4 of usable, default)           │
│             — your own objects, UDF state, non-Spark structures.   │
│             NOT tracked by the Unified Memory Manager.             │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│ OFF-HEAP  (spark.memory.offHeap.size) — additive, opt-in             │
│  Native/direct memory, NOT GC-managed. Runs its OWN parallel        │
│  Execution/Storage split (same storageFraction logic as on-heap;    │
│  no "reserved" or "user" concept off-heap — purely Exec + Storage). │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│ OVERHEAD (spark.executor.memoryOverhead)                             │
│  Common Spark 4.x default = max(minMemoryOverhead,                │
│    memoryOverheadFactor × executor.memory); factor defaults       │
│    to 0.10, or 0.40 for non-JVM Kubernetes jobs.                   │
│  Covers: JVM thread stacks, metaspace/class metadata, shared native │
│  libraries, NIO direct buffers for shuffle transfer, and (if not    │
│  separately configured) PySpark worker process memory.             │
│  Exhausting THIS — not the JVM heap — is what gets the whole        │
│  container killed by YARN/K8s with "exceeded memory limits."       │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│ PYSPARK MEMORY (spark.executor.pyspark.memory) — optional, additive │
│  Dedicated allowance for Python worker subprocesses. If unset,      │
│  Python worker memory is paid for out of Overhead above instead.    │
└────────────────────────────────────────────────────────────────────┘
```

**One-sentence summary:** total executor footprint = on-heap + off-heap (if enabled) + overhead + PySpark memory (if separately set) — and both on-heap and off-heap are each internally split into Execution vs Storage by the Unified Memory Manager, while overhead and PySpark memory sit outside that model entirely as flat allowances.

The **driver** has a smaller, analogous structure: `spark.driver.memory` (its own on-heap Execution/Storage/User split, used mainly for broadcast preparation and result aggregation) + `spark.driver.memoryOverhead`, plus a driver-specific safety valve: `spark.driver.maxResultSize` (default 1g) — a hard cap on total serialized result size collected back to the driver (e.g. via `collect()`), which throws its own specific error rather than a generic heap OOM.

## 3. The Unified Memory Manager — Execution vs Storage

Since **Spark 1.6**, on-heap (and off-heap) usable memory is managed by the **UnifiedMemoryManager**, replacing the older static split.

**Before 1.6 (StaticMemoryManager, legacy/historical):**
- Execution and Storage each got a fixed, non-negotiable fraction of memory.
- Simple but wasteful — an idle cache sat on reserved memory a running shuffle couldn't touch, and vice versa.

**1.6+ (UnifiedMemoryManager):**
- Execution and Storage share one pool and **borrow from each other dynamically**.
- `spark.memory.storageFraction` (default 0.5) defines the **protected storage region** as a fraction of the unified memory pool. Execution cannot evict cached blocks occupying this protected region; storage blocks above it may be evicted when execution needs memory.
- Storage may use otherwise-free execution memory, but it cannot forcibly evict memory currently in use by execution. The pools therefore share capacity dynamically while active execution allocations remain protected.
- This setting is not a promise that the protected region will be filled or that data survives executor loss, explicit `unpersist`, storage-level limitations, or other lifecycle events.
- **Practical consequence:** a job that heavily caches data *and* runs a large shuffle/join in the same stage can see its cached blocks silently evicted mid-run, causing expensive recomputation from lineage the next time that data is needed — a classic "why is my cached DataFrame being recomputed?" production mystery.

**Why this design:** it maximizes utilization (unused Storage capacity is available to Execution and vice versa) while still protecting a baseline amount of cache from being wiped out entirely by one large job.

## 4. On-Heap vs Off-Heap Memory (Internals)

**On-heap (default)**
- Standard JVM heap; every Spark object here is a normal Java/Scala object with JVM object headers, references, and boxing overhead.
- Subject to full JVM garbage collection — large heaps holding many live objects (big caches, wide shuffle buffers) can trigger long stop-the-world GC pauses, which stall tasks and can cause an executor to be marked unresponsive and killed by the driver.

**Off-heap (opt-in via `spark.memory.offHeap.enabled=true`, sized by `spark.memory.offHeap.size`)**
- Allocated as native/direct memory (`sun.misc.Unsafe`), completely outside JVM heap accounting — the garbage collector never scans it.
- Off-heap allocation is an optional memory-management choice, not a synonym for Tungsten. Tungsten-era execution improvements include compact binary row formats (`UnsafeRow`), whole-stage code generation, and more efficient memory management; Spark SQL can use these techniques while much of its execution memory remains on-heap.
- Off-heap size is **additive** to `spark.executor.memory` — it is not carved out of the JVM heap, it's a second, independent pool (see the diagram in §2).
- Trade-offs: reduced tooling visibility (standard JVM heap dumps won't show it), and it's one more pool that must be explicitly sized rather than inheriting a single `executor.memory` setting.

**Interview framing:** off-heap exists specifically to reduce GC pressure and per-row object overhead for large, long-running, or shuffle/sort-heavy jobs — it's a performance and stability lever, not a default-on setting, and most clusters run without it enabled unless GC has become a measured, specific problem.

## 5. Memory Overhead — What's Actually In It

`spark.executor.memoryOverhead` (commonly derived from `spark.executor.memoryOverheadFactor` with a configurable minimum; defaults are cluster-manager and Spark-version dependent) is frequently treated as a vague "buffer," but it has specific, nameable contents:

- **JVM internals:** thread stacks (one per concurrent task/thread), metaspace (class metadata, grows with the number of classes loaded — can matter with many UDFs/dependencies).
- **Native/shared libraries** the JVM or its dependencies load.
- **NIO direct buffers** used during shuffle block transfer over the network.
- **PySpark worker process memory** — *by default*, if `spark.executor.pyspark.memory` is not explicitly set, all Python worker memory usage is paid for out of this overhead pool (see §7).

**Why this matters operationally:** when a container is killed with a message like *"Container killed by YARN for exceeding memory limits... consider boosting spark.executor.memoryOverhead"* — that is **not** a JVM heap OOM. It's the cluster manager enforcing the total container size, and the fix is raising `memoryOverhead` (or `pyspark.memory` specifically, if Python is the actual driver of the overage) — increasing `spark.executor.memory` alone does nothing for this failure mode.

## 6. Driver Memory Management

The driver is structurally similar to an executor (its own JVM, its own on-heap Execution/Storage/User split via `spark.driver.memory`, plus `spark.driver.memoryOverhead`) but with a different workload profile — it doesn't run task computation, but it does:
- Build and hold the DAG/execution plan.
- Prepare and hold broadcast variables before distributing them.
- Aggregate/collect results from actions.
- Track task metadata, accumulators, and (in Structured Streaming) the query's running state metadata.

**Driver-specific failure modes:**
- **`collect()`/`toPandas()` on large data** — pulls the entire result set into driver heap; the single most common driver OOM cause.
- **`spark.driver.maxResultSize`** (default 1g) — a hard cap independent of heap size; exceeding it throws its own explicit error, distinguishable from a generic heap OOM, and exists specifically to fail fast before a runaway `collect()` can take down the whole application.
- **Oversized broadcasts** — broadcasting a table larger than expected (misjudged `broadcast()` hint, or an auto-broadcast join threshold matching a table that grew past expectations) loads that full data into driver memory first, before shipping it to executors.
- **Client deploy mode risk** — in `client` mode the driver runs on the submitting machine, which is often not sized like a cluster node; the same job might run fine in `cluster` mode purely because the driver gets proper cluster-grade memory there.

## 7. PySpark / Python Worker Memory (Deep Dive)

This is the area most interview answers get shallow on, and it deserves its own full treatment because it doesn't fit the JVM-centric picture above at all.

### 7.1 The architecture
A PySpark executor is **not just a JVM** — it's a JVM process plus one or more **separate Python worker subprocesses** (forked OS processes, each with its own independent memory space, not threads inside the JVM).

- For the **DataFrame/SQL API** with no Python UDFs, execution happens almost entirely inside the JVM via Catalyst's optimized plan and Tungsten's binary execution — Python is only involved in building the plan, not running it. This is why "avoid Python UDFs" is such standard tuning advice: it keeps you entirely inside the fast, well-instrumented JVM path.
- For **Python UDFs, `RDD.map`/`mapPartitions` with Python functions, or Pandas UDFs**, every row (or batch of rows) must cross the JVM↔Python boundary:
  `JVM serializes → sends over a local socket/pipe → Python deserializes → executes Python code → Python serializes result → sends back → JVM deserializes`.
- This round trip is why Python UDFs are historically much slower than native Spark SQL functions — the serialization tax, not just "Python is slower," is the dominant cost for row-at-a-time UDFs.

### 7.2 Why it breaks the JVM-centric memory picture
- Each Python worker's memory usage is **completely invisible to `spark.executor.memory`** — the JVM heap has no idea how much memory the Python interpreter next to it is consuming.
- The Spark UI's memory metrics (execution/storage memory, GC time) are **JVM-centric** — a job can look perfectly healthy in the UI while a Python worker silently balloons in memory (e.g., loading a large ML model per task, accumulating large Python-native lists/dicts, or inefficient pandas/numpy usage inside a UDF) right up until the container is killed.
- This is precisely why the failure surfaces as a **cluster-manager container kill** ("exceeded memory limits"), not a JVM `OutOfMemoryError` — the JVM heap itself may have plenty of headroom; it's the *total* container memory (JVM + Python workers + overhead) that's exhausted.

### 7.3 Serialization path: Pickle vs Arrow (a major, specific, testable distinction)
- **Legacy row-at-a-time Python UDFs** use Python's `pickle` serialization, row by row — high per-row overhead, both in CPU (serialize/deserialize cost) and memory (temporary object churn on both sides of the boundary).
- **Pandas UDFs (a.k.a. vectorized UDFs)** use **Apache Arrow** as the serialization format instead. Arrow is a columnar, zero-copy-friendly in-memory format — data is transferred in batches (as pandas Series/DataFrames on the Python side) rather than row-by-row Python objects, dramatically cutting both serialization overhead and peak memory churn.
- **Practical guidance, in order of preference:** built-in SQL/DataFrame functions (no Python involvement) > Pandas UDFs (Arrow, vectorized) > row-at-a-time Python UDFs (pickle, slowest, most memory-churn-prone) — this ordering itself is a common interview question.

### 7.4 Configs specific to Python worker memory
| Config | What it controls |
|---|---|
| `spark.executor.pyspark.memory` | Optional executor-wide Python-worker memory allowance. When configured, it is accounted for in addition to executor overhead and can be enforced on supported platforms; when unset, Spark does not impose a Python limit and workers share the container/overhead budget with other non-JVM processes. Enforcement is Python-`resource`-module/platform dependent (not supported on Windows; not enforced on macOS). |
| `spark.python.worker.memory` | Memory used by the Python worker's *own* internal aggregation/sort buffering before it spills to disk on the Python side (default 512MB) — the Python-side analogue of `spark.memory.fraction`'s role in the JVM. |
| `spark.executor.memoryOverhead` | Non-heap/container allowance; when `spark.executor.pyspark.memory` is unset, Python-worker memory is generally paid from this container budget. |
| `spark.sql.execution.arrow.pyspark.enabled` | Enables Arrow optimization for supported PySpark operations such as `toPandas()`; Pandas UDFs already use Arrow as their interchange mechanism. Keep operation-specific defaults and fallback behavior in mind; benchmark and size batches rather than blindly enabling a setting. |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | Caps how many rows go into a single Arrow batch sent to/from Python — tuning this trades off per-batch overhead against peak memory per batch. |

**Spark 4.3 memory-limit change:** `spark.executor.pyspark.memory` is an executor-wide allocation. Spark divides it across the executor's concurrent task capacity rather than simply dividing by raw executor cores; `spark.task.cpus` and custom resource profiles can therefore change each Python worker's effective share. With dynamic allocation and a custom resource limiting concurrency, fewer concurrently running workers share the configured allocation. A positive amount that leaves less than 1 MiB per concurrent task slot now fails the Python task instead of silently running without a limit; `spark.executor.pyspark.memory=0` disables the limit. Re-check this calculation when upgrading to Spark 4.3 or changing task/resource profiles.

### 7.5 Concurrency multiplies the risk
- Spark can run multiple Python worker processes concurrently on an executor for Python-heavy stages (the concurrency is driven by available task slots and task CPU requirements). Each worker has its own memory footprint, which contributes to process/container usage and must be budgeted at executor level.
- If per-worker memory is the binding constraint, **reducing `spark.executor.cores`** (fewer concurrent Python workers per executor) can fix a container-kill failure just as effectively as raising the memory allowance — and is often cheaper than requesting bigger nodes.

### 7.6 Mitigation checklist, specific to PySpark memory issues
1. Replace Python UDFs with built-in SQL functions wherever the logic allows it — eliminates the boundary crossing entirely.
2. Where a UDF is unavoidable, use a **Pandas UDF** over a row-at-a-time UDF to get Arrow's batching/columnar efficiency.
3. Consider setting `spark.executor.pyspark.memory` when the target platform supports its accounting/enforcement and you have measured Python-worker pressure. Otherwise budget Python memory in executor overhead; do not assume the setting is a universal hard limit.
4. If a UDF loads large objects (ML models, big lookup tables) per task, load them **once per partition** (`mapPartitions`-style patterns) or broadcast them from the driver, rather than reloading per row.
5. If concurrency is the driver of memory pressure, reduce `spark.executor.cores` to shrink the number of simultaneous Python workers per executor.
6. Watch executor and node-level OS memory metrics (not just the Spark UI), since Python worker memory won't appear in Spark's own JVM-centric instrumentation.

## 8. Shuffle Memory & Spill Mechanics

Shuffle is one of the two biggest memory consumers (alongside caching), and it interacts with Execution memory specifically.

- During a shuffle (wide transformation), each task **writes** its output partitioned by key to local disk (shuffle write) and downstream tasks **read** the relevant partitions across the network (shuffle read).
- Sorting/aggregating this data uses Execution memory. When the data for a task exceeds its share of Execution memory, Spark **spills** the excess to local disk rather than failing outright — this is by design, a graceful degradation, not an error.
- The Spark UI exposes this directly: **Spill (Memory)** and **Spill (Disk)** metrics per stage/task. Non-zero, especially *growing*, spill is an early warning sign of memory pressure well before it becomes a hard OOM or a "No space left on device" disk failure (shuffle spill and OOM-avoidance are two sides of the same coin: not enough memory, so it goes to disk instead — until the disk runs out too).
- `spark.sql.shuffle.partitions` (default 200, often far from ideal) controls the number of partitions after a shuffle in Spark SQL — too few means each partition (and its spill footprint) is large; too many creates scheduling overhead and many small files. **Adaptive Query Execution (AQE)**, enabled by default in modern Spark, can dynamically coalesce shuffle partitions at runtime, reducing the need to hand-tune this value.
- **Skew** compounds all of this: one disproportionately large partition (a hot key) can blow past the memory available to a single task even when the *average* partition size looks completely reasonable — this is why per-task/per-partition metrics, not stage averages, are what you check when diagnosing memory-related shuffle failures.

## 9. Broadcast Variables & Memory

- `broadcast()` (explicit) or an automatic broadcast join (when a table is under `spark.sql.autoBroadcastJoinThreshold`, default 10MB) sends a full copy of the data to **every executor**, held in Storage memory there, to avoid a shuffle entirely for that join.
- The data must first be **collected to the driver**, then distributed — so an oversized broadcast is a driver memory risk *before* it's an executor memory risk.
- Because it's held in Storage memory per executor, a broadcast that's larger than expected (a table that grew since the threshold was tuned) can itself trigger the Execution-evicts-Storage dynamic from §3, or contribute to executor OOM if many large broadcasts are held simultaneously.
- **Tuning lever:** lowering (or disabling, by setting to `-1`) `autoBroadcastJoinThreshold` protects against an unpredictable table silently growing into a memory problem; explicit `broadcast()` hints give you deliberate control instead of relying on the auto-threshold.

## 10. Storage Levels & Caching Strategy

| Storage Level | Form | Spills to Disk? | Notes |
|---|---|---|---|
| `MEMORY_ONLY` | In-memory only; representation depends on API/language | No (drops data instead) | For Scala/Java RDDs this stores deserialized JVM objects; PySpark RDD data is serialized on the Python side. Partitions that do not fit are recomputed from lineage when needed. |
| `MEMORY_AND_DISK` | Memory with disk fallback; representation depends on API/language | Yes | For Scala/Java RDDs this is deserialized in-memory storage with disk fallback; PySpark's corresponding storage-level constants use Python-side serialization. Not the modern PySpark DataFrame default. |
| `MEMORY_AND_DISK_DESER` | DataFrame/Dataset default storage-level descriptor | Yes | Default for PySpark DataFrame `cache()`/`persist()` in Spark 3.0+; Spark SQL manages the actual cached columnar/binary representation. Verify the exact API's documented default. |
| `MEMORY_ONLY_SER` | Serialized bytes in memory | No | Primarily a Scala/Java RDD persistence level; compact but costs CPU to serialize/deserialize. Do not assume this named constant is exposed by PySpark. |
| `MEMORY_AND_DISK_SER` | Serialized in memory, spills to disk | Yes | Primarily a Scala/Java RDD persistence level; compact and avoids recomputation. PySpark's StorageLevel API exposes a smaller set of constants, and Python objects are serialized across the JVM boundary. |
| `DISK_ONLY` | Serialized on disk only | Always | For when memory is too constrained to hold any of it but recomputation would be too expensive to redo. |
| `*_2` suffix | Any of the above, replicated to 2 executors | — | Adds resilience for the cached data itself (survives losing one executor), at double the storage cost. |
| `OFF_HEAP` | Serialized, in the off-heap pool | Depends on off-heap sizing | Requires `spark.memory.offHeap.enabled=true` (§4). Keeps cached data out of the GC-managed heap entirely — see §17 for detail. Missing from most summaries but a real, usable level. |

**API-specific default:** `DataFrame.cache()` / `DataFrame.persist()` in modern PySpark defaults to `MEMORY_AND_DISK_DESER`; PySpark RDD `cache()` defaults to `MEMORY_ONLY`. The Scala/Java RDD defaults and the PySpark representations differ, so don't apply one table row to every API. PySpark's `StorageLevel` constants do not include the `_SER` names shown for Scala/Java RDD persistence because Python objects are already serialized on the Python side.

**When to cache:** the same DataFrame/RDD feeds **multiple actions or branches**, and/or the upstream computation producing it is expensive relative to how often it's reused. Caching reflexively ("just in case") wastes Storage memory and increases the odds of the Execution-eviction dynamic from §3 kicking in mid-job.

**Always `unpersist()`** once data is no longer needed — especially in long-running applications (Structured Streaming jobs, long notebook sessions) where cached blocks otherwise accumulate indefinitely and pressure both Storage and, indirectly, Execution memory.

## 11. Complete Configuration Reference Table

| Config | Default | Purpose |
|---|---|---|
| `spark.executor.memory` | 1g | On-heap JVM memory per executor |
| `spark.executor.memoryOverhead` | Derived from factor and minimum; platform/version dependent | Native/JVM-internal overhead; also PySpark fallback |
| `spark.executor.pyspark.memory` | unset | Dedicated Python worker memory allowance |
| `spark.python.worker.memory` | 512MB | Python-side aggregation/sort buffer before spilling |
| `spark.memory.offHeap.enabled` | false | Turns on the off-heap pool |
| `spark.memory.offHeap.size` | 0 | Size of the off-heap pool (additive to executor.memory) |
| `spark.memory.fraction` | 0.6 | Share of (heap − reserved) usable for Execution+Storage combined |
| `spark.memory.storageFraction` | 0.5 | Fraction of unified memory forming the storage region protected from execution-driven eviction |
| `spark.driver.memory` | 1g | On-heap memory for the driver |
| `spark.driver.memoryOverhead` | Derived from driver overhead factor and minimum; platform/version dependent | Driver non-heap/container overhead; verify the effective request for the cluster manager |
| `spark.driver.maxResultSize` | 1g | Hard cap on total collected result size to the driver |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Table size below which Spark auto-broadcasts for joins |
| `spark.sql.shuffle.partitions` | 200 | Number of partitions after a shuffle in Spark SQL |
| `spark.sql.adaptive.enabled` | true (modern versions) | Enables AQE — dynamic coalescing of shuffle partitions, skew join handling, etc. |
| `spark.sql.execution.arrow.pyspark.enabled` | false (version-dependent) | Enables Arrow-based columnar transfer for Pandas UDFs/`toPandas()` |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | 10000 | Rows per Arrow batch between JVM and Python |
| `spark.executor.cores` | varies | Concurrent task slots per executor — also controls concurrent Python worker count |
| `spark.dynamicAllocation.enabled` | false | Scales executor count up/down based on pending work |

## 12. Failure Modes & Diagnostic Playbook

| Symptom | Most Likely Cause | Where to Look | Typical Fix |
|---|---|---|---|
| Driver `OutOfMemoryError` | `collect()`/`toPandas()` on large data; oversized broadcast prep | Driver logs, code review for actions | Avoid full collects; increase `driver.memory` only if genuinely needed |
| `...bigger than spark.driver.maxResultSize` | Collected results exceed the explicit cap | Error message itself is specific | Reduce collected data volume; raise the cap deliberately if justified |
| Executor `OutOfMemoryError` (JVM heap) | Skewed partition, `groupByKey`, too many cores per executor, over-caching | Spark UI Stages tab: per-task shuffle read/write and spill sizes | Repartition, use `reduceByKey`/`aggregateByKey`, handle skew, reduce cores-per-executor |
| Container killed: "exceeded memory limits" | Container budget exhausted (possibly native or Python-worker memory), not necessarily JVM heap | Executor/node OS-level memory monitoring, not just Spark UI | Measure the source; adjust `memoryOverhead` and, only on supported platforms, consider `spark.executor.pyspark.memory` accounting/limits |
| Slow stage with high Spill (Memory/Disk) but no failure yet | Early memory pressure, not yet fatal | Stages tab spill columns | Tune proactively (repartition/memory) before it becomes a hard failure |
| "No space left on device" | Shuffle/spill files or logs filling local disk (not RAM at all) | `df -h` on node, `spark.local.dir` config, per-task shuffle write size | Bigger/more local disks, fix skew, reduce spill via memory/partitioning |
| Long GC pauses / executor marked unresponsive | Large on-heap footprint, high live-object churn | GC logs, executor UI "GC Time" column | Consider off-heap memory, reduce cache size, increase parallelism to shrink per-task heap use |
| Job "fine" in Spark UI but node OOM-kills the process anyway | Python worker memory may be underrepresented by JVM-centric metrics | OS-level (`top`/`ps`) memory on the node, not the Spark UI | Measure worker RSS, budget it in overhead, and consider fewer concurrent Python tasks; explicit `pyspark.memory` is platform-dependent |

## 13. Tuning Checklist / Best Practices

1. **Size `memoryOverhead` deliberately** for PySpark workloads rather than accepting a default blindly. Consider `spark.executor.pyspark.memory` only where its platform-specific resource limiting is supported; otherwise budget Python workers within overhead.
2. **Prefer DataFrame/SQL built-ins over UDFs**; prefer Pandas (Arrow) UDFs over row-at-a-time Python UDFs when a UDF is unavoidable.
3. **Cache deliberately, not reflexively** — and always `unpersist()` when done, especially in long-running apps.
4. **Watch spill metrics as a leading indicator**, not just failures as a lagging one.
5. **Diagnose skew at the per-task level**, not stage averages — a single hot partition can dominate memory pressure while the "average" looks fine.
6. **Balance `executor.cores` against `executor.memory`**, not just maximize cores — more concurrent tasks (or, for PySpark, more concurrent Python workers) means more simultaneous consumers of the same fixed memory pool.
7. **Use `cluster` deploy mode for production** so the driver gets proper resources, not whatever machine happened to submit the job.
8. **Set `spark.driver.maxResultSize` intentionally** rather than leaving the default as an accidental, hard-to-explain failure trigger.
9. **Turn on AQE** (default in modern Spark) rather than hand-tuning `shuffle.partitions` from scratch — let the runtime coalesce partitions and handle skewed joins where possible.
10. **Monitor at the OS/node level for PySpark**, not just the Spark UI — the UI's memory metrics are JVM-centric and blind to Python worker processes.

## 14. Interview Quick-Reference (Rapid Fire)

- **Total executor memory** = `executor.memory` (on-heap) + `offHeap.size` (if enabled) + `memoryOverhead` + `pyspark.memory` (if set).
- **Unified Memory Manager** (since 1.6): Execution and Storage share one pool; Execution can evict Storage, never the reverse; `storageFraction` sets Storage's protected minimum.
- **On-heap vs off-heap:** off-heap is additive, native, GC-free, used by Tungsten; exists to cut GC pauses and per-object overhead.
- **`memoryOverhead` exhaustion → container killed** by the cluster manager; this is different from a JVM heap `OutOfMemoryError`.
- **PySpark workers are separate OS processes** with memory outside the JVM heap; unless `spark.executor.pyspark.memory` is set, their memory usage shares the executor overhead budget. The Python memory cap depends on Python's `resource` support (notably unsupported on Windows and not enforced on macOS).
- **Pickle vs Arrow:** row-at-a-time Python UDFs use pickle (slow, high overhead); Pandas UDFs use Arrow (columnar, batched, much faster and lighter).
- **Spill is a warning, not a failure** — Spark spills Execution memory overflow to disk gracefully; rising spill predicts future OOM or disk-space failures.
- **`spark.driver.maxResultSize`** is a distinct, deliberate cap — different failure signature from a driver heap OOM.
- **Order of preference for Python logic:** built-in SQL functions > Pandas UDFs (Arrow) > row-at-a-time Python UDFs (pickle).
- **Caching best practice:** cache only what's reused across multiple actions/branches; always `unpersist()` when done.

## 15. Serialization & Memory: Kryo vs Java

This was missing from the original file, but it's a direct, frequently-tested lever on memory footprint — not just a performance detail.

- Spark's **default general-purpose serializer is Java serialization** for RDD/object serialization; Spark SQL uses its own internal representations and execution paths for many operations, so this setting does not describe every SQL shuffle or cached DataFrame.
- **Kryo** (`spark.serializer=org.apache.spark.serializer.KryoSerializer`) produces **significantly more compact** serialized representations and serializes/deserializes faster. This matters for memory management specifically because:
  - For RDDs that are persisted in serialized storage levels, Kryo can reduce serialized object size compared with Java serialization. The exact benefit depends on object shape and registration/configuration.
  - Kryo may reduce serialization bytes in RDD/object shuffle paths; do not extrapolate this to Spark SQL `UnsafeRow`/Tungsten execution.
  - **Broadcast variables** are also serialized to be shipped to executors — a large broadcast table serializes smaller (and ships faster) under Kryo.
- **Trade-off:** Kryo doesn't serialize arbitrary classes as seamlessly out of the box — for best results (and to avoid falling back to a slower reflection-based path) you register your custom classes with `spark.kryo.registrationRequired=true` and `conf.registerKryoClasses(Array(classOf[MyClass]))`. Without registration it still works, just less optimally.
- **Practical guidance:** benchmark Kryo for RDDs or custom JVM object workflows that actually use Spark serialization heavily. It does not switch Spark SQL internal row encoding or make every DataFrame shuffle smaller, so it is not a universal SQL tuning win. Use the Spark UI and serializer metrics to justify it.

## 16. Structured Streaming State Store Memory (Advanced)

This is a distinct memory consumer that the original file didn't cover at all, and it's a common source of OOM specifically in **stateful streaming jobs** (windowed aggregations, `mapGroupsWithState`, stream-stream joins, deduplication).

- Stateful operations in Structured Streaming must keep **running state between micro-batches** (e.g., partial aggregates for an open window, or rows buffered for a stream-stream join waiting on a match). This state lives in a **State Store**, one per stateful partition.
- **Default state store**: an in-memory `HashMap`-backed store, checkpointed to HDFS/durable storage between batches. This means the *live* working state for every open group/window sits in **executor JVM heap** (Execution/Storage-adjacent, but tracked separately from the Unified Memory Manager pools above) — it competes for the same heap as everything else on that executor.
- **The failure mode:** if windows are large, watermarking is too loose (or absent), or cardinality of grouping keys is very high, state can grow **unbounded** across batches, since Spark can't evict state it doesn't yet know is safe to drop (no watermark) — this is a classic, slow-building OOM that looks fine for hours before it fails, distinct from the shuffle/cache-driven OOMs covered above.
- **RocksDB state store** (`spark.sql.streaming.stateStore.providerClass = RocksDBStateStoreProvider`) is the mitigation: it keeps state **off-heap and spillable to local disk**, dramatically raising how much state a job can hold before hitting JVM heap limits — at some cost in per-access latency versus the pure in-memory default.
- **Tuning levers specific to this:** set a **watermark** (`withWatermark`) so old state can actually be dropped; bound window sizes appropriately; consider RocksDB for any job with large or unbounded key cardinality; monitor the Spark UI's Structured Streaming tab for **state row count and memory used per operator**, which is where this problem shows up before it becomes a failure.

## 17. Executor Memory Sanity Checks & Practical Gotchas

A few small, concrete facts that didn't fit elsewhere but are worth knowing:

- Spark enforces a **minimum executor memory** relative to the ~300 MiB reserved memory. The Spark 4.3 configuration reference lists a minimum of 450 MiB for `spark.executor.memory` (about 1.5 × the reserved amount); setting it below the supported minimum fails at startup rather than producing a normal runtime heap OOM. Verify the target release's minimum when using custom distributions.
- **`spark.rdd.compress`** (default `false`) controls compression of serialized RDD partitions; it can shrink persisted RDD data at some CPU cost. It is not a universal compression switch for every DataFrame cache representation.
- **Off-heap has its own dedicated persistence level**, `StorageLevel.OFF_HEAP` — caching with this level requires `spark.memory.offHeap.enabled=true` and stores cached data in the off-heap pool (§4) rather than on-heap Storage memory. It is a real, usable option for jobs that already run with off-heap enabled and want cached data to avoid GC/heap pressure too.
- Memory-related config changes (`executor.memory`, `memory.offHeap.*`, `executor.memoryOverhead`) require an **application restart** to take effect — they cannot be changed on a running `SparkContext`/`SparkSession`, unlike some SQL-level configs (e.g., `shuffle.partitions`) which can be set per-query at runtime.


## 18. Memory Budgeting: A Worked Process and Anti-Patterns

Use this order when sizing or diagnosing an executor:

1. Start with the **container/pod limit**, not just `spark.executor.memory`. Record heap, memory overhead, explicit off-heap size, separately configured PySpark memory, and any platform-specific additions. Avoid double-counting PySpark memory: when it is not separately set, Python processes commonly consume overhead.
2. Estimate concurrent task slots (`floor(executor cores / spark.task.cpus)`, before custom-resource constraints) and the peak per-task working set. A 4-core executor can have several tasks, Python workers, shuffle buffers, and cached blocks active at once.
3. Separate **JVM heap OOM** from **container OOM kill**. Inspect executor stderr, cluster-manager events, process-tree/RSS metrics, GC logs, native memory, Python workers, and RocksDB/Arrow usage.
4. Look at task-level spill and skew before blindly increasing heap. More memory does not fix a single pathological hot-key partition or an unbounded streaming state store.
5. Cache only reusable expensive results; select an appropriate storage level and unpersist deliberately. Caching competes with execution memory and can increase recomputation.

**Common incorrect mental models to avoid:** Spark is not an in-memory-only engine; not every DataFrame is cached; off-heap is not automatically enabled; the UI's JVM heap metrics do not represent Python/native process memory; and setting Kryo does not change all Spark SQL internal serialization.
