# 14 — Apache Spark Interview Question Bank: Junior to Senior

> **How to use this bank:** answer each question aloud in 1–3 minutes, then explain one real example, failure mode, or trade-off. The “what a strong answer covers” column is a grading rubric, not a complete textbook answer.
>
> **Version baseline:** Spark 4.3.0 concepts; identify version-dependent answers (Spark 3.5.x vs 4.x, Structured Streaming Real-time Mode, ANSI behavior, AQE support) rather than quoting a default as universal.
>
> **Core study guides:** [architecture](01-Core-Architecture.md), [programming APIs](02-RDDs-DataFrames-Datasets.md), [shuffle](03-Partitioning-Shuffling.md), [joins](04-Joins.md), [memory](05-Memory-Management.md), [performance](06-Performance-Tuning.md), [Catalyst](07-Spark-SQL-Catalyst-Internals.md), [streaming](08-Structured-Streaming.md), [fault tolerance](09-Fault-Tolerance.md), [resource tuning](10-Cluster-Resource-Tuning.md), [debugging](11-Debugging-Monitoring.md), [architecture trade-offs](12-Architecture-Design-Tradeoffs.md), [system-design scenarios](13-Scenario-Based-System-Design.md).

## Table of Contents

1. Junior — Foundations and APIs
2. Mid-Level — Execution, Shuffle, Joins, and Performance
3. Senior — Memory, Reliability, Streaming, and Operations
4. Architecture and System Design
5. Hands-on / Debugging Questions
6. Behavioral and Project Deep-Dive Prompts
7. Final Interview Scorecard

---

## 1. Junior — Foundations and APIs

| # | Question | What a strong answer covers |
|---:|---|---|
| 1 | What is Apache Spark? | Distributed data-parallel engine; driver/executors; batch, SQL, streaming, APIs; not a database or storage system. |
| 2 | Explain driver, executor, and cluster manager. | Driver plans/schedules; executors run tasks and store blocks; cluster manager allocates resources. |
| 3 | What is a Spark application? | One driver and SparkContext with its executors; isolated application lifecycle. |
| 4 | What is the difference between SparkContext and SparkSession? | Context connects to cluster and exposes RDD/scheduler functionality; session is SQL/DataFrame entry point wrapping context. |
| 5 | What are RDD, DataFrame, and Dataset? | Distributed collection vs schema table vs JVM typed Dataset; Python does not have typed Dataset[T]. |
| 6 | What is lazy evaluation? | Transformations construct lineage/plan; action or write triggers execution and optimization. |
| 7 | Give examples of transformations and actions. | `filter`, `map`, `select`, `groupBy` vs `count`, `collect`, `show`, `write`; note show is action. |
| 8 | What is a partition? | Logical unit of parallel work; task per partition per stage attempt; not tied permanently to executor. |
| 9 | What is a task? | Smallest scheduled unit; processes one partition for one stage attempt. |
| 10 | What are job, stage, and task? | Action creates job; shuffles split stages; tasks operate on partitions. |
| 11 | What is lineage? | Dependency graph used to recompute lost partitions; checkpoint can truncate lineage. |
| 12 | What is the difference between RDD and DataFrame? | DataFrame structured/optimizer-visible; RDD lower-level objects/custom operations; prefer DataFrame for ETL. |
| 13 | What is `map` vs `flatMap`? | One output vs zero-to-many outputs per input. |
| 14 | What is `mapPartitions`? | Function per partition; amortize setup; task retries/speculation may rerun side effects. |
| 15 | What is `reduceByKey` vs `groupByKey`? | Map-side combine vs shuffling all values; choose aggregate primitive for reductions. |
| 16 | What is a DataFrame schema? | Column names/types/nullability metadata; explicit schema helps ingestion stability. |
| 17 | Why use `select` and `filter` early? | Reduce read/transport/work; enable column pruning and predicate pushdown where source supports it. |
| 18 | What is a Spark SQL temporary view? | Session-scoped logical view; not necessarily materialized or cached. |
| 19 | What is a UDF? | User-defined expression; Python boundary/optimizer visibility costs; use built-ins first. |
| 20 | Why is `collect()` dangerous? | Brings all rows to driver; memory/network bottleneck; use bounded sample/aggregation/write. |
| 21 | What does `cache()` do? | Persists computed partitions lazily using default storage level; data can be evicted/lost; unpersist. |
| 22 | What is the difference between `repartition` and `coalesce`? | Shuffle/rebalance vs narrow partition reduction that can retain skew. |
| 23 | What is Parquet and why is it common with Spark? | Columnar, compressed, typed, column pruning and statistics; compare to CSV/JSON row parsing. |
| 24 | What is the difference between `union` and `unionByName`? | Position vs column-name alignment; schema compatibility and missing columns. |
| 25 | How do you handle NULL values? | SQL three-valued logic; `isNull`, `coalesce`, null-safe equality; do not silently convert to zero. |

### Junior practical checks

- Write a filter/select/groupBy aggregation using `pyspark.sql.functions`.
- Read a CSV with an explicit schema and write Parquet.
- Explain why calling `collect()` on 500 million rows is unsafe.
- Explain how a `withColumn()` transformation can be lazy even though Python code has run.

---

## 2. Mid-Level — Execution, Shuffle, Joins, and Performance

| # | Question | What a strong answer covers |
|---:|---|---|
| 26 | What causes a shuffle? | Wide dependencies such as groupBy, join, distinct, repartition; network + disk + serialization costs. |
| 27 | What is narrow vs wide transformation? | Output partition depends on one/few input partitions vs many/all; stage pipelining vs shuffle boundary. |
| 28 | How does Spark execute a job? | Driver builds DAG; DAGScheduler creates stages; TaskScheduler assigns tasks; executor runs and reports. |
| 29 | What is a shuffle map stage? | Map tasks write outputs partitioned for downstream reduce partitions; output fetched later. |
| 30 | What is `spark.sql.shuffle.partitions`? | Default SQL shuffle partition count; not source partition count; AQE can alter eligible post-shuffle count. |
| 31 | How do you choose partition count? | Data size, target partition size, task duration, cores, skew, shuffle, source splitting; benchmark. |
| 32 | Why can a 20 GB gzip file use one task? | Gzip text stream generally unsplittable; need splittable input/format or upstream splitting. |
| 33 | How is file input partitioning determined? | File size/splittability, maxPartitionBytes, open cost, file grouping, source-specific rules. |
| 34 | What is AQE? | Runtime re-optimization at query-stage boundaries using materialized statistics; coalescing, skew handling, runtime broadcast. |
| 35 | Does AQE fix all skew? | No; supported cases/thresholds only; hot keys and non-SQL/RDD or other workloads may need data handling/salting. |
| 36 | Explain broadcast hash join. | Broadcast build relation; avoid shuffling streamed side; driver and executor memory risk; join-type restrictions. |
| 37 | Explain sort-merge join. | Shuffle both sides by key, sort each partition, merge; common for large equi-joins; skew/spill issues. |
| 38 | Explain shuffle hash join. | Shuffle both sides; hash-build smaller partition; per-partition memory sensitivity; planner/hint criteria. |
| 39 | What is broadcast nested-loop join? | Nested-loop comparisons with broadcast side; can support non-equi and some outer cases; potentially expensive. |
| 40 | Why can a small table still fail to broadcast? | Serialized/build size, timeout, driver memory, executor memory, stats inaccurate, unsupported join type. |
| 41 | What is a left semi join? | Return left rows with a match; no right columns; equivalent to existence semantics when NULL semantics align. |
| 42 | What is a left anti join? | Return left rows without a match; compare carefully with `NOT IN` when NULL appears. |
| 43 | How can duplicate keys affect a join? | Multiplicative cardinality; m×n pair explosion per key; validate uniqueness and grain. |
| 44 | What is data skew? | Unequal key/partition distribution; few tasks dominate; inspect per-task shuffle size/duration. |
| 45 | How do you fix skew? | AQE skew join where applicable; filter invalid hot keys; split hot keys; salting with small-side replication; redesign key. |
| 46 | What is dynamic partition pruning? | Runtime filtering of partitioned fact-side scans using selective dimension-side values, when supported. |
| 47 | What is bucketing? | Hash-based table layout; may reduce shuffle in compatible joins, but depends on metadata and planning. |
| 48 | What is predicate pushdown? | Send filter to source so fewer rows are read; verify source support and pushed filters in plan. |
| 49 | What is column pruning? | Read only required columns from columnar source; inspect scan schema. |
| 50 | What is the difference between cache and checkpoint? | Cache is performance reuse/evictable; checkpoint materializes durable lineage/state boundary. |
| 51 | What is the Spark UI SQL tab useful for? | Query execution plan, operator metrics, stages, shuffle, final AQE plan. |
| 52 | What is `explain("formatted")`? | Physical-plan details; not necessarily final AQE plan before execution. |
| 53 | How do you investigate a slow stage? | Task distribution, shuffle, spill, GC, input/output, skew, executor loss, CPU/network/source. |
| 54 | What are small files and why do they hurt? | Metadata/listing/planning/open/task overhead; compact and control output writer layout. |
| 55 | What is the difference between `partitionBy` and `repartition`? | Directory/table partitioning on write vs distributed task partitioning/shuffle in memory. |

### Mid-level practical checks

- Inspect a physical plan and identify `Exchange`, `BroadcastExchange`, and the join operator.
- Explain why a broadcast hint can make a query worse.
- Given a stage where two tasks take 30× longer, identify the metrics and key-frequency checks to run.
- Design a safe output layout that avoids one file per tiny upstream partition.

---

## 3. Senior — Memory, Reliability, Streaming, and Operations

| # | Question | What a strong answer covers |
|---:|---|---|
| 56 | Explain Unified Memory Manager. | Execution/storage share managed pool; eviction direction and storage threshold; user/native memory outside pool. |
| 57 | What is executor memory overhead? | Container memory beyond heap; native, direct buffers, thread stacks, Python workers; platform/version factors. |
| 58 | Why can executor be OOMKilled while heap looks healthy? | Container RSS includes Python, native, direct buffers, RocksDB, off-heap, overhead; inspect process tree. |
| 59 | How do Python workers affect executor sizing? | Separate processes, concurrent tasks, Arrow/pandas working sets; per-process peaks multiply. |
| 60 | What does Kryo improve? | Serialization for RDD/object workflows; not a universal DataFrame SQL serialization switch; benchmark. |
| 61 | What is Tungsten? | Execution-engine effort: compact binary rows, memory management, codegen; not simply “off-heap mode.” |
| 62 | What happens when a task fails? | Retry within task failure limits; deterministic vs data errors; stage/application failure path. |
| 63 | What happens when an executor is lost? | Running tasks rerun; cached blocks lost; shuffle outputs may need recomputation unless retained/available. |
| 64 | What is a FetchFailedException? | Downstream cannot fetch shuffle block; scheduler resubmits upstream map stage; identify executor/disk/network loss. |
| 65 | What is speculative execution? | Run duplicate attempt for slow task; helps stragglers, not deterministic slow logic; side effects must be safe. |
| 66 | What is the role of output committers? | Prevent partial/duplicate output publication; filesystem/committer-specific atomicity. |
| 67 | Explain Spark checkpointing in Structured Streaming. | Offset log, commit log, state store versions; restart source offsets and state from checkpoint. |
| 68 | Is Structured Streaming exactly-once? | Engine processing semantics vs end-to-end effects; source replay, state, sink transaction/idempotency. |
| 69 | Why is Kafka sink at-least-once? | Retry/commit failure window; downstream idempotency/transaction strategy needed. |
| 70 | What is a watermark? | Event-time progress estimate and late-data/state cleanup bound; not a guarantee of all late records being accepted. |
| 71 | Watermark vs processing time? | Event timestamp from records vs wall-clock processing; late arrivals and out-of-order data semantics. |
| 72 | What is stateful streaming? | Maintains state across triggers; aggregation, dedup, stream-stream joins, keyed state; checkpoint/state store. |
| 73 | How do you avoid unbounded streaming state? | Watermark, event-time key, time constraints, TTL/timeouts, bounded cardinality, output mode. |
| 74 | HDFS state store vs RocksDB? | JVM map vs native RocksDB, GC trade-off, disk and container memory budgets, changelog checkpointing. |
| 75 | What changes if a stateful checkpoint's shuffle partition count changes? | Checkpoint/state compatibility; cannot casually repartition; controlled migration or new checkpoint/replay. |
| 76 | What is `foreachBatch`? | Micro-batch callback; batch DataFrame; retries can repeat; idempotent sink or transactions required. |
| 77 | What is the difference between continuous processing and Real-time Mode? | Separate execution modes/generations, guarantees, operator support; RTM version boundaries. |
| 78 | Does Spark 4.3 support stateful RTM in PySpark? | Limited support; JVM `transformWithState` in RTM, not PySpark counterpart; check operator matrix. |
| 79 | Does AQE work in Structured Streaming? | Spark 4.1+ AQE for stateless streaming workloads; stateful micro-batch limitations; not in RTM as of 4.3. |
| 80 | How do you design CDC/SCD2? | Source order/LSN, idempotency, deletes, effective intervals, atomic merge, late/out-of-order handling. |
| 81 | How do you design a safe backfill with a live stream? | Isolated staging/new target, partition ownership, source precedence, transaction, reconciliation. |
| 82 | How do you size executors? | Task concurrency, CPU/memory per task, heap vs overhead/native, disk/network, failure isolation; workload benchmark. |
| 83 | When is dynamic allocation beneficial or harmful? | Variable workloads and utilization vs executor ramp-up, locality, shuffle service, streaming/state behavior. |
| 84 | How do you operate a multi-tenant Spark cluster? | Isolation, queues/pools, quotas, job clusters, auth, noisy-neighbor, cost/observability. |
| 85 | How do you make a Spark pipeline idempotent? | Stable keys/run IDs, deterministic transformations, atomic/merge writes, staged publish, replay tests. |

---

## 4. Architecture and System Design

| # | Question | What a strong answer covers |
|---:|---|---|
| 86 | Design a 1 TB/day lakehouse ingestion pipeline. | Requirements, raw/silver/gold, incremental ingestion, file layout, quality, transactions, orchestration, monitoring. |
| 87 | When do you choose Spark vs Flink? | Throughput/latency, state semantics, ecosystem, operational skill, connector support, cost. |
| 88 | Explain Lambda vs Kappa architecture. | Two computation paths vs replayable stream path; consistency and operational trade-offs. |
| 89 | Design near-real-time clickstream analytics. | Kafka, event time, dedup/watermark, aggregation, sink, replay, lag and state monitoring. |
| 90 | Design SCD Type 2 processing with CDC. | Event order, no-op changes, close/insert, deletes, atomicity, late events, constraints. |
| 91 | How would you maintain a seven-year table efficiently? | Retention, partitioning, compaction, lifecycle, stats, query patterns, backfill. |
| 92 | How do you handle source schema evolution? | Contracts, compatible changes, explicit schema, versioning, quarantine, rollout. |
| 93 | How do you recover if object storage/checkpoint is unavailable? | Durable storage, retry/fallback, RPO/RTO, source retention, backup/DR. |
| 94 | How do you control cost without missing SLA? | Workload separation, scheduled vs always-on, autoscaling, file layout, cost/interval, retry rate. |
| 95 | How do you secure sensitive lakehouse data? | Workload identities, storage and catalog authorization, encryption, PII controls, audit, network boundaries. |
| 96 | How do you design a shared compute platform? | Tenant isolation, policies, quotas, standard runtime, cost tagging, logs/metrics, platform ownership. |
| 97 | When would you not use Spark? | Dataset scale and latency, interactive serving, OLTP, simpler engine alternatives. |
| 98 | Explain a data contract. | Schema, semantics, keys, null policy, SLA, ownership, compatibility, quality rules. |
| 99 | How would you choose a table format? | Workload operations, transactions, concurrency, multi-engine needs, catalog, compatibility, maintenance. |
| 100 | Design an end-to-end reliability model. | Retry levels, source replay, checkpoint, idempotent sink, backfill, reconciliation, observability, DR. |

---

## 5. Hands-on / Debugging Questions

| # | Prompt | Expected approach |
|---:|---|---|
| 101 | A job has 2,000 tasks and only three are slow. | Inspect skew, per-task shuffle/input bytes, hot keys, stragglers, executor health. |
| 102 | A stage has 1 task for a 15 GB CSV. | Check compression codec/splittability and file layout; convert/split upstream. |
| 103 | Driver crashes during `collect()`. | Bound driver results; aggregate or write distributed; increase driver only if justified. |
| 104 | Executors die with exit 137. | Container memory vs heap; process tree, Python/native, overhead, per-task memory. |
| 105 | A join unexpectedly returns 10× more rows. | Validate duplicate keys, many-to-many cardinality, condition, casts, NULL semantics. |
| 106 | Streaming query's state rows keep growing. | Check watermark progress, event-time distribution, key cardinality, output mode, state operator metrics. |
| 107 | Restart causes `AnalysisException` with old checkpoint. | Query/checkpoint compatibility, source/schema/operator changes, partition metadata; migration/replay plan. |
| 108 | Output has thousands of tiny files. | Writer partition count, partitionBy cardinality, trigger frequency; compact and redesign layout. |
| 109 | Broadcast join times out. | Broadcast build stage and timeout; stats/actual bytes; upstream filter cost; driver/executor resource. |
| 110 | Query slowed after Spark upgrade. | Compare Spark/Java/Scala/connector versions, ANSI default, plan changes, config defaults, code compatibility. |

---

## 6. Behavioral and Project Deep-Dive Prompts

Prepare a real project story for each:

1. A Spark performance issue you diagnosed. What was the evidence, root cause, change, and measured before/after result?
2. A data-quality or correctness issue that reached production. How did you detect, contain, and prevent recurrence?
3. A Spark job or streaming query that failed repeatedly. How did you determine whether the issue was code, data, resource, or infrastructure?
4. An architecture decision where Spark was not the best fit. What did you choose instead and why?
5. A migration between Spark versions, cluster managers, or table formats. What compatibility risks did you test?
6. A cost reduction you delivered. Which cost metric changed, and did the SLA or correctness regress?
7. A disagreement about partitioning, join strategy, streaming, or table design. How did you resolve it using measurements?
8. A backfill or replay that had to coexist with live data. How did you prevent overwrites and duplicates?

Use the STAR structure (Situation, Task, Action, Result), but include technical evidence: input volume, runtime, task count, shuffle/spill, latency, failure rate, or cost.

---

## 7. Final Interview Scorecard

Score yourself 0–2 on each area (0 = cannot explain, 1 = theory only, 2 = can explain with evidence and trade-offs).

| Area | Score / 2 | Evidence you should be able to provide |
|---|---:|---|
| Architecture and execution model |  | Trace an action from driver to tasks and stages |
| PySpark / DataFrame APIs |  | Write correct transformations and handle NULL/types |
| Partitioning and shuffle |  | Diagnose a shuffle/skew issue using UI metrics |
| Joins and SQL optimizer |  | Read plan and justify strategy |
| Memory and resource sizing |  | Distinguish heap OOM vs container OOM |
| Performance engineering |  | Show measured improvement, not config folklore |
| Fault tolerance |  | Explain retry and sink failure windows |
| Structured Streaming |  | Explain event time, watermark, state, checkpoint, sink semantics |
| Architecture and data modeling |  | Design batch/CDC/lakehouse system with trade-offs |
| Production operations |  | Monitoring, runbooks, cost, security, backfill |

**Target:** junior roles require strong foundations and practical API fluency; mid-level roles require independent debugging and performance work; senior roles require correctness, system design, operational ownership, and reasoned trade-offs. A high score is not memorizing every config default—it is knowing how to verify the right value for the deployed release.
