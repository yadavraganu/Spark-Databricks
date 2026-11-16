# 15 — Apache Spark Hands-on Labs and End-to-End Capstone

> **Purpose:** turn the guides into practical skill. The labs progress from basic DataFrame programming to performance, reliability, streaming, and system design. Each lab includes an objective, work items, and acceptance criteria so you can assess whether you have mastered the topic.
>
> **Environment:** use Spark 4.3.0 or a managed runtime with a documented Spark 3.5.x/4.x compatibility profile. Exact APIs and defaults may differ. Spark itself is not installed in this archive; use a local Docker/runtime, standalone cluster, cloud environment, or managed Spark service.

## Suggested Environment

- Java version supported by the selected Spark distribution.
- Python version supported by the selected PySpark package.
- PySpark matching the target Spark runtime; avoid mismatched client/server and Scala binary versions.
- A local filesystem/object-store test area, plus a test database/Kafka broker only for the relevant labs.
- Spark UI and event logging enabled for labs that require plan and stage diagnosis.
- A Git repository for code, README, run parameters, and expected outputs.

## Lab 1 — DataFrame ETL Foundations

**Objective:** ingest semi-structured raw files and produce a clean typed dataset.

**Tasks:** create explicit schemas; parse timestamps and decimals; quarantine malformed rows; normalize strings; filter invalid data; write Parquet; record counts and quality metrics.

**Acceptance criteria:**

- No unbounded `collect()`.
- Input schema and timezone are explicit.
- Malformed rows are measurable and recoverable.
- Valid output has documented types and stable column names.
- Re-running the same input interval produces the same logical output.

**Interview extension:** explain why a DataFrame transformation is lazy and which source optimizations appear in `explain("formatted")`.

---

## Lab 2 — RDD vs DataFrame Word Count

**Objective:** implement the same problem with RDD and DataFrame APIs.

**Tasks:** count words with RDD `flatMap` + `map` + `reduceByKey`; implement a DataFrame version with `explode`, `groupBy`, and `count`; compare plan, code complexity, runtime, and resource use on a representative dataset.

**Acceptance criteria:**

- Both outputs match after sorting by key.
- RDD version uses `reduceByKey`, not `groupByKey` for the reduction.
- DataFrame version has a schema and no unnecessary Python UDF.
- You can explain why DataFrame is the default for structured data.

---

## Lab 3 — Partitioning, Repartition, Coalesce, and Small Files

**Objective:** observe how partition count changes execution and output file layout.

**Tasks:** generate/read a dataset with multiple GB or a scaled test volume; inspect source partitions; run `repartition(n)`, `repartition(n, key)`, and `coalesce(n)`; write partitioned/unpartitioned Parquet outputs; inspect task count and file-size distribution.

**Acceptance criteria:**

- Explain which operation shuffles and why.
- Compare output file counts and sizes.
- Identify skew and imbalance using per-task metrics.
- Explain why `partitionBy()` and `repartition()` are different.

---

## Lab 4 — Joins and Skew

**Objective:** compare broadcast, sort-merge, and skew-handling strategies.

**Tasks:** create a large fact dataset and smaller dimension; inspect join plans with/without statistics and hints; introduce a hot key; measure task duration, shuffle, and spill; test AQE and (optionally) salting.

**Acceptance criteria:**

- Correct row cardinality is validated, including duplicate keys and NULLs.
- The executed plan matches the explanation.
- The broadcast build side fits the target runtime's memory budget.
- Salting is applied only when justified and results match the reference output.

---

## Lab 5 — Memory, Spill, and Executor Sizing

**Objective:** learn to distinguish JVM memory pressure from total-container pressure.

**Tasks:** run a shuffle-heavy aggregation with different partition counts and executor layouts; observe spill/GC; optionally run a Python UDF or Pandas UDF to create Python-worker memory pressure; collect process-tree metrics and executor logs.

**Acceptance criteria:**

- Explain heap vs overhead vs off-heap vs Python memory.
- Identify one memory bottleneck from observed metrics.
- Propose a measured fix (partition sizing, concurrency, cache strategy, or overhead) and show its impact.
- Do not use increasing heap as the only answer to an exit-137/container-kill issue.

---

## Lab 6 — Catalyst and Query Plan Investigation

**Objective:** understand logical and physical plans and runtime adaptation.

**Tasks:** compare SQL and DataFrame expressions; inspect `explain("extended")`, `explain("formatted")`, and `explain("cost")`; test filter/column pushdown; refresh table statistics; compare initial and final AQE plan.

**Acceptance criteria:**

- Identify analysis, logical optimization, and physical planning outputs.
- Explain at least one `Exchange`, join operator, and scan operator.
- Verify whether predicates/columns are pushed down.
- Explain why AQE changes the plan at runtime and why not every plan is adaptive.

---

## Lab 7 — Reliable Batch Pipeline and Idempotent Writes

**Objective:** implement a daily batch pipeline that can be safely rerun.

**Tasks:** ingest a source interval with run ID; write to staging; validate schema/counts/reconciliation; publish by transactional merge or controlled atomic promotion; simulate failure before and after publication; rerun.

**Acceptance criteria:**

- Reprocessing the same source interval does not create duplicate business records.
- Failed runs do not publish partial results.
- Output has deterministic keys and clear overwrite/merge semantics.
- Run metadata records source interval, input version, target version, and validation results.

---

## Lab 8 — Structured Streaming with Event Time and Watermarks

**Objective:** create a streaming aggregation that handles duplicate and late events.

**Tasks:** use Kafka or a rate/file source; parse explicit schema; define event-time windows; configure watermark and deduplication; write to a supported sink/checkpoint; simulate delayed and duplicated messages; restart the query.

**Acceptance criteria:**

- Explain the watermark and late-data policy.
- State size remains bounded under the test's event-time assumptions.
- Restart recovers from checkpoint without silently resetting to latest offsets.
- Sink semantics are described accurately (do not claim exactly-once without a supported sink protocol/idempotency).
- Monitor input rate, processing rate, batch duration, watermark, and state metrics.

---

## Lab 9 — JDBC Extraction Without Overloading the Source

**Objective:** parallelize a relational table read under source-side connection and load constraints.

**Tasks:** benchmark a single-partition read and multiple `numPartitions` values; inspect database active connections and query load; compare balanced vs skewed split column; test explicit filtering independent of partition bounds.

**Acceptance criteria:**

- Explain that `lowerBound`/`upperBound` determine stride and do not filter rows.
- Choose a safe concurrency setting with the DB owner.
- Validate no gaps/duplicates in the extracted source interval.
- Explain when CDC/export is a better approach than JDBC scans.

---

## Lab 10 — CDC and SCD Type 2

**Objective:** maintain a historical dimension from a CDC stream/file.

**Tasks:** build test CDC events with insert/update/delete, duplicate deliveries, out-of-order sequences, and no-op changes; apply SCD2; validate effective-date intervals and current flag.

**Acceptance criteria:**

- One current row per key where the contract requires it.
- No overlapping validity intervals.
- Deletes/tombstones are handled explicitly.
- Replayed events produce the same final state.
- The publish step uses transactional semantics or a safe staged protocol.

---

## Lab 11 — Observability and Runbook

**Objective:** provide enough evidence to diagnose an incident after it occurs.

**Tasks:** enable event logging; inspect Spark UI and History Server; export metrics; create a dashboard for duration, input/output, shuffle, spill, task retries, executor loss, and streaming lag/state; write runbooks for OOM, FetchFailed, skew, source outage, and checkpoint failure.

**Acceptance criteria:**

- A completed run can be investigated after the live application has ended.
- Alerts include data freshness/completeness, not only job success.
- Runbook specifies first checks, likely causes, safe mitigations, owner, and recovery validation.

---

## Lab 12 — Senior-Level Capstone: Design a Retail Data Platform

**Scenario:** ingest orders, customers, inventory, and clickstream. The platform needs daily finance-grade reporting, near-real-time order metrics, SCD2 customer history, seven-year retention, replay/backfill, and multiple teams consuming the data.

**Deliverables:**

1. A high-level architecture diagram and system-boundary description.
2. Data contracts and schemas for bronze, silver, and gold tables.
3. Batch and streaming ingestion design with keys, offsets/watermarks, and retention.
4. Join and aggregation plan with partition/file layout and table-format choice.
5. CDC/SCD2 strategy including deletes, late events, and source ordering.
6. Checkpoint, idempotency, transaction, and backfill/replay model.
7. Executor/driver sizing approach and cost assumptions (do not invent fixed sizing without measurements).
8. Monitoring SLIs/SLOs, alert rules, operational runbooks, and DR plan.
9. Security/governance model for PII and multi-team use.
10. Trade-off record: why Spark, why selected streaming mode, why selected table/catalog, and what alternatives were rejected.

**Capstone review rubric:** correctness and recovery (25%), data model/contracts (15%), scalability/performance (15%), operations/observability (15%), security/governance (10%), cost and trade-offs (10%), clarity of communication (10%).

---

## Completion Checklist

- [ ] I can build a DataFrame pipeline with explicit schema, quality checks, and safe writes.
- [ ] I can explain and inspect Spark's job/stage/task execution model.
- [ ] I can diagnose a shuffle, skew, bad join, OOM, and small-file problem using evidence.
- [ ] I can design an idempotent batch rerun and a streaming restart.
- [ ] I can explain event time, watermark, state, checkpoint, and sink guarantees.
- [ ] I can design CDC/SCD2 and a safe historical backfill.
- [ ] I can reason about Spark vs other engines and managed vs self-managed trade-offs.
- [ ] I have one end-to-end capstone architecture I can explain in a senior interview.
