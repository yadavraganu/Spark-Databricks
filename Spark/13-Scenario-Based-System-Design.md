# 13 — Apache Spark Scenario-Based System Design for Data Engineer Interviews

> **Audience:** mid-level to senior data engineers. These are interview practice cases, not one-size-fits-all blueprints. Adjust the design to the constraints given by the interviewer and make assumptions explicit.
>
> **Version baseline:** Spark 4.3.0 concepts; verify connector, table-format, and deployment-specific behavior. This guide assumes a lakehouse-style target where appropriate.
>
> **Related guides:** [architecture design trade-offs](12-Architecture-Design-Tradeoffs.md), [Structured Streaming](08-Structured-Streaming.md), [fault tolerance](09-Fault-Tolerance.md), [performance tuning](06-Performance-Tuning.md).

## Table of Contents

1. A Repeatable System-Design Answer Framework
2. Scenario 1 — Build a Daily Petabyte-Scale Lakehouse ETL
3. Scenario 2 — Kafka Events to Near-Real-Time Aggregates
4. Scenario 3 — Diagnose a Spark Job That Became 5× Slower
5. Scenario 4 — Fix a Skewed Large-Large Join
6. Scenario 5 — Ingest a Large Relational Table Through JDBC
7. Scenario 6 — Design CDC and SCD Type 2 Dimensions
8. Scenario 7 — Exactly-Once Effects From Kafka to a Database
9. Scenario 8 — Small Files and Slow Lake Queries
10. Scenario 9 — Unbounded Streaming State and Memory Growth
11. Scenario 10 — Backfill Years of History While Streaming Continues
12. Scenario 11 — Multi-Tenant Shared Spark Platform
13. Scenario 12 — Schema Drift and Malformed Records
14. Senior Interview Follow-up Questions
15. Common Weak Answers

---

## 1. A Repeatable System-Design Answer Framework

Start with questions. Do not jump straight to a cluster size or a Spark API.

### 1.1 Requirements checklist

- **Scale:** average and peak input bytes/rows per second or day; total history; expected growth.
- **Freshness:** maximum end-to-end latency and p95/p99 latency; daily or intraday SLA.
- **Semantics:** duplicates, ordering, late arrival, deletes, corrections, event-time vs processing-time.
- **Data model:** source schema, keys, grain, dimensions, output contracts, consumers.
- **Availability:** source retention, checkpoint durability, recovery time, RPO/RTO.
- **Operations:** orchestration, deployment, monitoring, backfill, schema change, access control.
- **Cost:** compute budget, storage lifecycle, database load, always-on vs scheduled capacity.

If the interviewer omits values, state assumptions and continue. Example: “I will assume 2 TB/day of compressed source files, a 2-hour batch SLA, seven years of retained history, and a daily correction window. I would revisit sizing after measuring actual scan and shuffle throughput.”

### 1.2 Structure the answer

1. Requirement clarification and assumptions.
2. High-level architecture diagram.
3. Data ingestion and raw retention.
4. Processing transformations and state/key semantics.
5. Storage/table layout and serving model.
6. Correctness, retries, checkpoints, and idempotency.
7. Scale and performance design.
8. Monitoring, security, cost, and operational recovery.
9. Trade-offs and alternatives.

### 1.3 Always state the grain and key

For every table or aggregate, say what one row represents and what uniquely identifies it. “Daily sales” is incomplete; “one row per business date, store ID, product ID, and currency” is testable. The grain determines deduplication, joins, aggregation, and reconciliation.

---

## 2. Scenario 1 — Build a Daily Petabyte-Scale Lakehouse ETL

**Prompt:** A retailer receives clickstream, orders, inventory, and customer data. It must process a large daily volume, publish curated tables before business hours, preserve history, and support BI and ML.

### Clarify

Ask for compressed vs uncompressed size, daily peak, lateness, full-refresh vs incremental source capability, required dimensions, query patterns, data retention, SLA, and whether a transactional table format is available.

### Reference design

```text
Sources: Kafka / CDC / object files / JDBC snapshots
                     |
          Immutable landing (Bronze)
                     |
        Spark schema + quality validation
                     |
      Silver tables (typed, deduped, CDC applied)
                     |
        Spark joins / aggregations / windows
                     |
      Gold fact, dimension, and aggregate tables
                     |
              BI / ML / extracts

Catalog + orchestration + data quality + lineage + monitoring
```

### Design choices

- Land original data and source metadata before complex transformation. Retain raw data long enough for corrections and rebuilds.
- Prefer incremental ingestion when the source supplies a trustworthy change stream or high-water mark. Use periodic full reconciliation to detect source-side misses.
- Use Spark DataFrames/SQL for transformations. Push filters/projections close to reads. Avoid unnecessary Python UDFs.
- Use an appropriate table format/catalog for transactional updates, snapshots, schema/partition evolution, and concurrency. Plain Parquet folders alone do not provide table-level ACID semantics.
- Partition by coarse, common filters (often date) and use compaction/appropriate clustering where the format supports it. Avoid high-cardinality directory partitioning.
- Collect table statistics and inspect the executed plans for fact-dimension joins. Broadcast only genuinely small dimensions.
- Use a run ID, source watermark, table snapshot/version, quality metrics, and a publish/commit boundary.

### Scaling and reliability

Estimate input split count based on splittability and target scan partition size; choose shuffle parallelism based on shuffle volume, skew, and task runtime. Benchmark on a representative slice and extrapolate carefully. Use AQE for eligible batch SQL plans, but still measure skewed keys. Design retries to be idempotent; stage outputs and publish atomically where supported.

### Interview follow-ups

- How do you rerun one historical day without overwriting newer records?
- What if a source file arrives late after the gold table is published?
- How do you prove that revenue totals reconcile from raw to gold?
- How do you change the partition strategy when table size grows?

---

## 3. Scenario 2 — Kafka Events to Near-Real-Time Aggregates

**Prompt:** Process order events from Kafka, deduplicate producer retries, calculate five-minute sales metrics by region, and publish results with a freshness target of a few minutes.

### Clarify

Ask the event schema, Kafka partition count, peak rate, event-time reliability, late-arrival distribution, duplicate identifier, required output mode, sink semantics, and recovery retention.

### Reference design

```text
Producers → Kafka topic (keyed by order/event key)
                  |
       Structured Streaming micro-batch
                  |
   Parse + validate + quarantine malformed events
                  |
     Deduplicate + watermark + window aggregate
                  |
       Transactional/idempotent serving sink
                  |
       BI / alerting / downstream consumers
```

### Core query shape

```python
from pyspark.sql import functions as F

parsed = raw.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("event")
).select("event.*")

metrics = (
    parsed
    .withWatermark("event_time", "30 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
    .groupBy(F.window("event_time", "5 minutes"), "region")
    .agg(
        F.sum("order_amount").alias("sales_amount"),
        F.count("*").alias("deduplicated_order_events"),
    )
)
```

`dropDuplicatesWithinWatermark` is available in modern Spark releases (introduced in Spark 3.5); verify the target runtime and semantics. The example counts the deduplicated event rows, not arbitrary distinct business keys. For distinct business-entity metrics, design explicit bounded deduplication/state or a downstream batch/table aggregation; do not assume streaming `countDistinct` is supported.

### Correctness and operations

- Set a durable checkpoint path unique to this query.
- Use the Kafka source offsets/checkpoint for replay; configure `startingOffsets` only for a new query without a checkpoint. Existing checkpoints determine recovery offsets.
- Choose append/update/complete output mode according to the aggregate and sink semantics.
- Specify what happens to events later than the watermark: dropped from stateful results, routed to a separate late-event path, or handled by periodic correction jobs.
- Monitor Kafka lag, input rate, processing rate, batch duration, watermark, state size, failures, and sink latency.
- Kafka sink delivery is at-least-once. A downstream table or database needs idempotency/transaction handling.

### RTM decision

Use micro-batch as the default. Consider Spark Real-time Mode only after confirming the target Spark release supports every required operator, language API, source, and sink. As of Spark 4.3, RTM supports a limited set of stateful operations; it is not a drop-in replacement for all Structured Streaming queries.

---

## 4. Scenario 3 — Diagnose a Spark Job That Became 5× Slower

**Prompt:** A daily ETL used to take 20 minutes and now takes 100 minutes. No code change was deployed.

### Investigation sequence

1. Compare input bytes and rows, file count, file-size distribution, schema, and data skew between runs.
2. Compare the slowest jobs/stages and per-task duration distribution in Spark UI/History Server.
3. Inspect shuffle read/write, remote reads, spill, GC time, executor loss, and task retries.
4. Compare initial and final executed plans: join strategy, partition pruning, pushed filters, AQE skew/coalescing decisions, and statistics.
5. Compare executor count/cores/memory, queue contention, node type, autoscaling delay, and storage/network service health.
6. Reproduce on a stable data snapshot and change one factor at a time.

### Symptom-to-cause map

| Evidence | Likely cause | Validation |
|---|---|---|
| Same code, much larger input | Data growth | Input metrics and file count |
| A few tasks dominate stage time | Data skew, hot key, or single large/unsplittable input | Per-task input/shuffle bytes and duration |
| High disk spill and GC | Partitions too large, cache pressure, low memory per task | Task-level spill, GC, executor memory, partition sizes |
| More shuffle bytes | Join cardinality changed, plan changed, unnecessary repartition | SQL plan and intermediate row counts |
| Fewer executors or long gaps before tasks | Resource allocation or cluster contention | Executors tab, scheduler/cluster-manager events |
| File scan much slower, many tiny files | Small-file/listing/metadata overhead | File counts, scan planning time, table maintenance history |
| Plan changed to sort-merge instead of broadcast | Statistics changed or table grew | Compare statistics, explain plans, actual sizes |

Do not start by doubling the cluster. More executors cannot fix a skewed single partition, bad join cardinality, missing partition pruning, or a slow serial source query.

---

## 5. Scenario 4 — Fix a Skewed Large-Large Join

**Prompt:** Two large fact-like datasets are joined on `customer_id`. Most partitions finish quickly, but a few tasks run for 45 minutes and spill heavily.

### Diagnose

- Compare task-level shuffle read sizes and duration percentiles; inspect keys with the largest frequency.
- Check whether NULL, default, or sentinel keys form a hot group.
- Check join cardinality and duplicate keys; a many-to-many join can legitimately multiply records.
- Inspect if AQE skew-join handling is enabled and actually split the relevant skewed shuffle partitions.

### Mitigation ladder

1. Filter irrelevant rows and project only required columns before the join.
2. Fix the data model if a sentinel key or bad default value creates artificial skew.
3. Let AQE handle eligible sort-merge join skew where possible; verify the final plan.
4. Salt only the hot keys: replicate the corresponding small-side rows across salt buckets and assign stable salt values to large-side rows. Preserve the original join semantics and remove the salt helper column after the join.
5. Consider separate hot-key and normal-key branches if skew is highly concentrated and a general salting approach is too expensive.
6. Consider broadcast only if one filtered side is truly small enough for the driver and each executor.

### Salting concept

```python
from pyspark.sql import functions as F

salt_buckets = 16
large_salted = large_df.withColumn(
    "salt", F.pmod(F.xxhash64("stable_row_id"), F.lit(salt_buckets))
)
small_salted = (
    small_df
    .withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(salt_buckets - 1))))
)
joined = large_salted.join(small_salted, ["customer_id", "salt"], "inner")
```

This is illustrative: `stable_row_id` must exist and identify rows deterministically. Replication multiplies the small-side data by the salt-bucket count; choose the count based on observed skew and cost. If the small side is not small, salting can worsen the workload. Test row counts against an unsalted reference on a small dataset.

---

## 6. Scenario 5 — Ingest a Large Relational Table Through JDBC

**Prompt:** Load hundreds of millions of rows from PostgreSQL/Oracle into a data lake without overwhelming the source database.

### Design

- Agree on a consistent source snapshot or extraction boundary (snapshot isolation, CDC position, or reliable update timestamp).
- Select a supported split column with balanced distribution and suitable numeric/date/timestamp type.
- Set `numPartitions` to a conservative concurrent-connection budget—not merely the Spark core count.
- Push source filters and project only needed columns; use a source-side query where safe.
- Avoid a single highly skewed partition range; inspect per-task row counts and elapsed time.
- Use a staging location/table and record the extraction watermark.

### Critical JDBC gotcha

`lowerBound` and `upperBound` are used to calculate partition stride; they do **not** filter the source rows. Add an explicit `WHERE` predicate for a bounded extract. Too many JDBC partitions can cause connection exhaustion, lock pressure, query contention, and worse source performance.

### Alternatives

For very large or frequent extracts, evaluate CDC/log-based replication, source-side export to object storage, database-native unload, or a read replica. The best option depends on source load policy, latency, and operational control.

---

## 7. Scenario 6 — Design CDC and SCD Type 2 Dimensions

**Prompt:** A customer table emits insert, update, and delete events. The analytics team needs the current record and full historical versions.

### Required event fields

Primary key, operation (`I/U/D`), changed fields or after-image, source transaction/LSN/sequence, event or source commit time, and schema version. Preserve source ordering; processing timestamp alone is not a reliable event order.

### SCD Type 2 target

Typical columns: business key, descriptive attributes, `effective_from`, `effective_to`, `is_current`, source sequence, and audit columns. Define an interval convention such as `[effective_from, effective_to)` to avoid overlap ambiguity.

### Processing design

1. Land CDC records immutably and deduplicate using a source-native event identity/sequence.
2. Order events by source sequence within a business key; detect gaps or out-of-order records.
3. Resolve multiple changes for the same key in one micro-batch according to source ordering.
4. Compare new attributes with the current row; avoid creating a new version for a no-op change if the contract says not to.
5. Close the prior current row and insert a new current row in a transactional table operation or controlled staged process.
6. Handle deletes explicitly (end-date current record, create a tombstone, or physically remove according to policy).
7. Reconcile one-current-row-per-key, no-overlap intervals, and source-to-target counts.

A Spark DataFrame transformation by itself cannot make a two-step update+insert atomic in a plain Parquet directory. Use a table format/database with the required transactional merge semantics or build a staged, recoverable publish protocol.

---

## 8. Scenario 7 — Exactly-Once Effects From Kafka to a Database

**Prompt:** A streaming application consumes Kafka and writes each event to an external relational database. Duplicates are unacceptable.

### Explain the failure window

Suppose Spark sends a database insert for batch 42 and the database commits it. Before Spark records batch 42 as committed in its checkpoint, the driver crashes. When the query restarts, it may replay batch 42. A normal insert can duplicate records.

### Design options

- **Idempotent upsert:** use a stable event ID as a unique constraint and upsert. Replaying the same event produces the same final database state.
- **Transactional batch key:** write the batch's data and a batch-completion marker in one database transaction. On restart, check the marker before reapplying. This must be safe with concurrent/retried writers.
- **Staging + merge:** write each batch to a staging table with deterministic IDs, then execute a database-native merge/transaction and mark the batch complete.
- **Use a transactional lakehouse sink:** when consumers can read the lakehouse table and the integration supports the required semantics.

### Avoid

- Claiming exactly-once because the source is Kafka or because a checkpoint exists.
- Using `foreachBatch` without idempotency/transaction logic.
- Performing one database insert per row from a Python UDF.
- Assuming Spark's task retry protocol can roll back external commits.

State the actual guarantee precisely: “The source is replayable; the Spark query restores offsets from a durable checkpoint; the sink enforces unique event IDs with an atomic upsert, so retries are idempotent and the final database state has one logical row per event.”

---

## 9. Scenario 8 — Small Files and Slow Lake Queries

**Prompt:** A table contains tens of millions of Parquet files, many only a few kilobytes, and Spark queries spend minutes planning before scanning.

### Root causes

Too many small upstream writes, over-partitioned output paths, frequent tiny streaming commits, too many writer tasks, or partition columns with high cardinality. The cost appears in listing, metadata, task scheduling, and file open overhead—not only in bytes scanned.

### Remediation

- Measure file count and size distribution by table partition.
- Compact small files using the table format's supported maintenance process or a controlled rewrite.
- Choose a realistic target file size based on storage, workload, and engine; do not blindly enforce one universal number.
- Reduce high-cardinality `partitionBy` columns; consider clustering/sorting only when supported and justified by query patterns.
- Control writer task count and small incremental commits; avoid `coalesce(1)` for large tables.
- Retain atomic commit semantics and avoid rewriting partitions while readers depend on an inconsistent path state.

Monitor planning time, files scanned, scan bytes, task count, and data-skipping/pruning metrics after compaction.

---

## 10. Scenario 9 — Unbounded Streaming State and Memory Growth

**Prompt:** A Structured Streaming aggregate runs well for several days, then executor memory grows until the application fails.

### Diagnose state first

Inspect `stateOperators` in query progress: rows total/updated/removed, memory, custom state metrics, and watermark. Also check input event-time distribution and whether the watermark advances.

### Common causes

- No watermark on a stateful operator.
- Watermark column missing, incorrectly typed, or not advancing.
- Deduplication key does not include/bind event time correctly and no `dropDuplicatesWithinWatermark` is used.
- Stream-stream join lacks watermarks or a time-range condition.
- Complete output mode intentionally retains the full aggregate result.
- Very high-cardinality grouping key or a hot key creates enormous state.
- State store reloads repeatedly due poor locality or executor churn.

### Fixes

Define event-time and late-data requirements with the business, add supported watermark and time constraints, constrain state with timeouts/TTL where applicable, reduce cardinality, choose a state store such as RocksDB only when suitable, and budget native memory in container overhead. A larger heap does not repair logically unbounded state.

---

## 11. Scenario 10 — Backfill Years of History While Streaming Continues

**Prompt:** A real-time pipeline writes current transactions to a curated table. The business requests a three-year backfill and the backfill must not overwrite newer streaming data.

### Design questions

- Is the target partitioned by event date and is the backfill limited to closed historical intervals?
- Do historical and streaming records have a source event ID and source precedence/sequence?
- Can the backfill write to a separate table/version and publish through a controlled swap/merge?
- Is there a period where both jobs write the same table partitions?

### Safer patterns

1. Backfill to an isolated target or staging table, validate, then publish atomically or merge into the production target with a source-sequence precedence rule.
2. Backfill date/key ranges that do not overlap the active stream's write horizon, then reconcile late-arrival windows separately.
3. Coordinate with the streaming job through orchestration, table transactions, and partition-level ownership; avoid concurrent non-transactional writes to the same folder.
4. Record the backfill run ID, source snapshot/offset, table version, and reconciliation results.

A blind `overwrite` of a shared output path is not a safe backfill strategy.

---

## 12. Scenario 11 — Multi-Tenant Shared Spark Platform

**Prompt:** Multiple teams submit Spark jobs to one shared cluster. One large job starves others and some tenants need sensitive-data access controls.

### Design

- Separate workloads by queue, pool, namespace, or cluster policy according to latency, data sensitivity, and resource profile.
- Apply per-tenant CPU, memory, concurrency, and cost quotas.
- Use fair scheduling when concurrent jobs within one Spark application need sharing; understand it does not by itself provide isolation across separate applications.
- Prefer ephemeral job clusters for strongly isolated workloads; shared clusters can reduce startup and idle cost but require governance.
- Enforce table/catalog authorization and storage-layer identity permissions; do not assume catalog controls block direct object-store access.
- Standardize Spark/Scala/Python/Java/connector versions, dependency packaging, event logs, metrics, and runbook conventions.
- Monitor queue wait, executor allocation delay, per-team cost, failure rate, and starvation.

### Interview trade-off

A shared cluster is operationally efficient but increases noisy-neighbor and security complexity. Job clusters give stronger isolation but add startup time and infrastructure overhead. Match the choice to service-level objectives and trust boundaries.

---

## 13. Scenario 12 — Schema Drift and Malformed Records

**Prompt:** An upstream JSON producer adds fields, changes `amount` from string to number, and occasionally sends malformed records. The pipeline currently fails or silently creates NULLs.

### Strategy

1. Preserve original payloads in a raw/bronze location for reprocessing.
2. Use an explicit expected schema and a schema version/compatibility policy.
3. Parse with a clear corrupt-record policy; route malformed data and schema violations into quarantine with error metadata.
4. Distinguish harmless additive optional fields from breaking type/meaning changes.
5. Validate mandatory fields, keys, enum values, timestamp parse rate, and decimal precision.
6. Alert when quarantine count/rate exceeds the accepted threshold; do not silently discard malformed data.
7. Coordinate producer and consumer rollout; test old and new schema versions before deployment.

A type-compatible schema change can still be semantically breaking. For example, an amount changing from dollars to cents remains a numeric type but changes every business metric.

---

## 14. Senior Interview Follow-up Questions

For each case, be ready to answer:

- What assumptions did you make, and which metric would you collect first to validate them?
- What is the data grain and business key?
- How do you handle duplicates, NULLs, updates, deletes, and out-of-order records?
- What is the maximum recovery/replay window, and does source retention cover it?
- Which operation causes the largest shuffle? How would you prove it?
- What happens if the driver dies after the sink commits but before checkpoint commit?
- How does the design behave when one key has 20% of all records?
- What happens when one source sends no data, too much data, or a breaking schema change?
- How do you backfill safely while the pipeline continues to run?
- What is the biggest cost driver and how do you measure cost per output interval?
- What is the operational runbook if the query is behind by 45 minutes?
- When would you choose a different engine or simplify the architecture?

---

## 15. Common Weak Answers

| Weak answer | Why it is weak | Senior answer should include |
|---|---|---|
| “Add more executors.” | Does not identify the bottleneck | Stage/task evidence, skew, source/sink, shuffle, utilization, plan |
| “Use broadcast join.” | No size/build-side or join-type reasoning | Actual small-side size, driver/executor memory, plan, hint caution |
| “Checkpoint gives exactly-once.” | Checkpoint does not transact arbitrary sinks | Replay source + state + sink atomicity/idempotency |
| “Set watermark to 10 minutes.” | No event-lateness distribution or business policy | Late-data SLA, state size, measured lag, correction strategy |
| “Partition by customer ID.” | Often creates high-cardinality small files | Query predicates, cardinality, pruning benefit, layout cost |
| “Cache the DataFrame.” | Cache consumes memory and may be evicted | Reuse count, recompute cost, cache footprint, unpersist |
| “Use Kryo to speed SQL.” | Kryo does not switch all SQL internals | Which serializer/path is expensive and measured evidence |
| “Spark is real-time.” | Latency depends on mode and workload | Micro-batch/RTM operator constraints, p99 latency, alternatives |
| “Just overwrite the table.” | May destroy newer data or expose partial results | Staging, transactions, versioning, partition ownership, validation |

**Best practice:** answer with the trade-off, the failure mode it addresses, the evidence you would inspect, and how you would verify correctness after the change.
