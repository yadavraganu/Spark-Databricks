# 12 — Spark Architecture Design and Engineering Trade-offs

> **Audience:** mid-level through senior data engineers. This guide moves beyond knowing Spark APIs to choosing Spark in a larger data platform, designing robust pipelines, and defending trade-offs in architecture interviews.
>
> **Version baseline:** Spark 4.3.0 as the reference point. Spark 3.5.x remains common in managed environments. Cloud provider distributions and lakehouse platforms may add, backport, or change capabilities; separate open-source Spark behavior from vendor-specific features.
>
> **Related guides:** [core architecture](01-Core-Architecture.md), [performance](06-Performance-Tuning.md), [Structured Streaming](08-Structured-Streaming.md), [fault tolerance](09-Fault-Tolerance.md), [system-design scenarios](13-Scenario-Based-System-Design.md).

## Table of Contents

1. Start With Requirements, Not With Spark
2. When Spark Is the Right Tool
3. Choosing Between Processing Engines
4. Batch, Micro-Batch, and Low-Latency Processing
5. Lambda, Kappa, and Unified Architecture
6. Lakehouse and Medallion Architecture
7. End-to-End Data Pipeline Architecture
8. Ingestion Pattern Trade-offs
9. Data Layout: Partitioning, Clustering, Compaction, and Files
10. Table Formats and Catalogs
11. Incremental Processing, CDC, and SCD
12. Data Quality, Contracts, and Schema Evolution
13. Orchestration and Dependency Management
14. Reliability, Replay, and Backfill Design
15. Security, Governance, and Multi-Tenancy
16. Observability, SLAs, and Operations
17. Cost and Capacity Trade-offs
18. Deployment and Platform Choices
19. Architecture Anti-Patterns
20. Senior-Level Decision Framework

---

## 1. Start With Requirements, Not With Spark

A senior engineer does not begin with “we need Spark.” Begin by making the workload measurable.

| Requirement | Questions to ask | Why it changes the design |
|---|---|---|
| Volume | Bytes/day, rows/day, peak burst, growth rate, historical backfill size? | Determines parallelism, storage layout, and engine sizing. |
| Latency | Seconds, minutes, hourly, daily? What is p95/p99 end-to-end latency? | Separates batch, micro-batch, and low-latency architectures. |
| Throughput | Average and peak events/sec or MB/sec? | Determines source partitions, task slots, and state capacity. |
| Correctness | At-most-once, at-least-once, exactly-once effect? Deduplication key? | Drives source replay, checkpoint, transaction, and idempotency design. |
| Time | Event time, processing time, allowed lateness, late-data policy? | Critical for windows, watermarks, and state cleanup. |
| Availability | RPO/RTO, recovery time, maintenance windows? | Determines replication, checkpoints, replay, HA, and retention. |
| Data shape | Structured, nested JSON, CDC, files, relational tables, unbounded events? | Affects schema, parsing, source connectors, and table format. |
| Consumers | BI, ML, APIs, downstream jobs, operational applications? | Influences serving layer, freshness, concurrency, and data contracts. |
| Governance | PII, residency, lineage, audit, access policies, retention? | Influences catalog, encryption, masking, and ownership. |
| Cost | Budget, compute/storage split, idle clusters, egress, licensing? | May favor serverless, shared clusters, scheduled batch, or different engine. |

Capture assumptions in an architecture decision record (ADR). “A lot of data” and “real time” are not engineering requirements.

---

## 2. When Spark Is the Right Tool

Spark is a strong fit when the workload needs distributed processing over large datasets, rich SQL/DataFrame operations, complex ETL, joins/aggregations, batch + streaming APIs, or integration with data-lake storage. It is not automatically the best engine for every data task.

**Good fit**

- Large-scale batch ETL and ELT on files or lakehouse tables.
- Large joins, aggregations, window calculations, and repartitioning.
- Reusable data engineering pipelines with many sources/sinks.
- Structured Streaming where micro-batch latency is acceptable, or Real-time Mode where supported operations meet requirements.
- Feature generation and preprocessing for distributed machine learning workflows.

**Question the choice**

- A small dataset or single-node workload may be cheaper and simpler in SQL, Python, DuckDB, or a database.
- Interactive low-latency SQL against curated tables may be better served by Trino/Presto, a warehouse, or a query service.
- Strict event-by-event stateful processing with very low latency, complex timers, and mature event-time semantics may favor Apache Flink or another dedicated stream processor, depending on the environment.
- A simple Kafka transformation with limited state may fit Kafka Streams.
- A transactional OLTP application is not a Spark workload.
- Spark is not a message broker, workflow orchestrator, metadata catalog, or table format by itself.

The choice should be made using latency, semantics, ecosystem, operational skill, cost, and failure model—not raw benchmark throughput alone.

---

## 3. Choosing Between Processing Engines

| Technology | Strong use case | Key advantage | Trade-off / concern |
|---|---|---|---|
| **Apache Spark** | Large batch ETL, SQL analytics, micro-batch streaming, data-lake processing | Unified distributed engine and broad ecosystem | Micro-batch model has latency/operational costs; stateful streaming has constraints |
| **Apache Flink** | Stateful, event-time stream processing and low-latency event pipelines | Native continuous stream processing, event-time/state capabilities | Different operational model and skillset; batch support and ecosystem fit vary |
| **Kafka Streams** | App-embedded processing over Kafka topics | Lightweight deployment inside services; Kafka-centric semantics | Tied closely to Kafka; not a general distributed lakehouse ETL engine |
| **Trino / Presto** | Interactive federated SQL queries over data sources | Fast ad-hoc SQL and federation | Not usually the main tool for complex stateful ETL or heavy writes |
| **Warehouse / SQL engine** | Governed SQL analytics and BI serving | Managed concurrency, governance, SQL experience | Cost model, lock-in, and non-SQL processing needs may matter |
| **Beam** | Portable programming model across runners | Runner abstraction and unified programming model | Actual semantics/operations depend on the chosen runner |

A defensible interview answer names both the preferred engine and the conditions under which you would choose another one.

---

## 4. Batch, Micro-Batch, and Low-Latency Processing

### 4.1 Batch

Use batch when data can be processed on a schedule or when a finite input snapshot is available. Batch is often simpler to replay, test, backfill, and reason about. It can still run frequently (for example, every five minutes); schedule frequency alone does not make it event streaming.

### 4.2 Structured Streaming micro-batch

Spark models a stream as an unbounded table and incrementally processes new data. Micro-batch supports broad DataFrame operations and checkpoint-based recovery. Trigger intervals are not hard latency guarantees: input availability, state, shuffle, scheduling, sink commits, and backpressure all contribute to latency.

### 4.3 Real-time Mode

Spark Real-time Mode is version-gated and supports a restricted set of operations. As of Spark 4.3, RTM documents Kafka as its source, requires `update` output mode, and supports stateless operators plus stateful aggregation, `dropDuplicates`, and JVM `transformWithState` (not the PySpark `transformWithState` API). Stream-stream joins, `flatMapGroupsWithState`, `dropDuplicatesWithinWatermark`, and distinct aggregates are not supported in that release. Supported sinks include Kafka, `ForeachWriter`/`foreach`, console, and memory; `foreachBatch` is not supported. Validate the exact support matrix and benchmark with the real source/sink. Do not choose RTM solely because the name sounds faster.

### 4.4 Decision matrix

| Need | Starting point | Important design questions |
|---|---|---|
| Daily warehouse fact table | Scheduled batch | Snapshot consistency, idempotent overwrite/merge, backfill strategy |
| Fresh analytics within minutes | Structured Streaming micro-batch or frequent incremental batch | Source replay, checkpoints, trigger interval, sink commit semantics |
| Seconds-level stateful event-time analytics | Evaluate Spark RTM and Flink | State/timer support, latency tail, recovery semantics, operator coverage |
| Millisecond service response | Serving database/cache/API | Do not place Spark in the synchronous request path without a compelling reason |
| Historical replay + continuous events | Unified table with batch + streaming ingestion | Duplicate control, transactional commit, schema evolution, event time |

---

## 5. Lambda, Kappa, and Unified Architecture

### 5.1 Lambda architecture

Lambda keeps a **batch layer** for complete/correct historical computation and a **speed layer** for low-latency updates. A serving layer merges both outputs.

Advantages: clear separation of historical recomputation and low-latency output; recovery can rebuild the batch view. Disadvantages: duplicate logic in batch and streaming, two result paths, reconciliation complexity, and operational overhead.

### 5.2 Kappa architecture

Kappa uses a replayable event log as the source of truth and one stream-processing path. Historical rebuilds replay the log from an earlier offset (or rebuild a new target table).

Advantages: one processing model and fewer duplicated transformations. Disadvantages: event-log retention may be insufficient for large historical rebuilds; replay can be expensive; source schema/version changes require care.

### 5.3 Unified batch + streaming table architecture

A modern lakehouse may use one transactional table as the durable data contract, with batch jobs for large backfills and streaming jobs for incremental ingestion. It is not “no architecture”: replay, offset/commit alignment, schema evolution, and table maintenance still need explicit design.

---

## 6. Lakehouse and Medallion Architecture

A lakehouse aims to combine low-cost data-lake storage with table-level metadata and transactional capabilities. Common layers:

| Layer | Responsibility | Quality expectation | Typical consumers |
|---|---|---|---|
| **Bronze / raw** | Preserve source data and ingestion metadata with minimal transformation | Traceable, replayable, source-faithful; malformed records retained/quarantined | Reprocessing, audit, debugging |
| **Silver / standardized** | Parse, type, deduplicate, conform keys, apply quality rules | Validated schema, normalized columns, stable business keys | Data engineering, downstream data products |
| **Gold / serving** | Business aggregates, dimensional models, metrics, feature tables | Business definitions, reconciled totals, clear freshness | BI, analytics, ML, applications |

The naming is a pattern, not a standard. Some data products need only raw + curated; others have additional domain layers. Separate physical storage and ownership boundaries according to data access, quality, and lifecycle requirements.

### 6.1 Raw-zone preservation

Keep enough original payload and metadata to replay or reinterpret data: source identifier, source offset/sequence or file path, ingest timestamp, event timestamp, schema/version, and ingestion run ID where relevant. Avoid silently discarding malformed or unknown fields in the only retained copy.

### 6.2 Gold is a contract

A gold table should define grain, business keys, metric definitions, update mode, late-arrival policy, freshness SLA, owners, and known limitations. “One row per customer” is incomplete until the definition of customer and row selection is specified.

---

## 7. End-to-End Data Pipeline Architecture

A common architecture for a high-volume source:

```text
Operational DB / SaaS / Kafka / Files
                 |
        Ingestion connector
                 |
       Bronze landing / event log
                 |
       Spark validation + parsing
                 |
   Silver transactional lakehouse tables
                 |
    Spark joins / CDC / business rules
                 |
       Gold facts, dimensions, metrics
                 |
       BI / ML / APIs / downstream jobs

Cross-cutting: Catalog + Governance + Orchestration + Data Quality
              + Lineage + Monitoring + Access Control + Cost Controls
```

For each hop specify: owner, schema, partition/ordering key, replay behavior, SLA, failure policy, checkpoint/commit boundary, retention, and access controls.

### 7.1 Batch pipeline flow

1. Identify source snapshot or high-water mark.
2. Land immutable raw data or stage it under a run-specific path.
3. Validate counts, schema, and required fields.
4. Transform in Spark with explicit key, dedup, NULL, and type semantics.
5. Write to a staging/transactional target.
6. Validate row counts, aggregates, uniqueness, and data-quality thresholds.
7. Atomically publish/merge or promote the staged result where supported.
8. Record run metadata, source offsets/watermarks, output version, and metrics.
9. Clean up staging only after successful publication and retention requirements are satisfied.

### 7.2 Streaming pipeline flow

1. Select a replayable source and set retention beyond the expected recovery period.
2. Define unique durable checkpoint storage per query.
3. Parse with explicit schema and quarantine malformed payloads.
4. Define event-time field, watermark, allowed lateness, and state cleanup.
5. Make sink writes idempotent or transactional.
6. Monitor source lag, processing latency, batch duration, state size, watermark movement, and sink throughput.
7. Test restart after partial sink commit and after executor/driver failure.

---

## 8. Ingestion Pattern Trade-offs

| Pattern | Best for | Main risks | Design safeguards |
|---|---|---|---|
| Full snapshot | Small/medium reference tables; periodic rebuild | Expensive source scans; duplicates if append used accidentally | Consistent snapshot, overwrite/replace atomically, run ID |
| Incremental watermark | Tables with reliable monotonic update column | Late updates, deletes, clock issues, tied timestamps | Composite high-water mark, overlap window, dedup, periodic reconciliation |
| CDC log | Inserts/updates/deletes with ordered change stream | Ordering, duplicate events, schema changes, transaction boundaries | Source sequence/LSN, key, operation, transaction metadata, replay strategy |
| Kafka events | Event-driven applications and streaming ingestion | Retention expiry, duplicate/reordered events, hot partitions | Partition key, retention, offsets/checkpoints, idempotent sink |
| File arrival | Batch exchange and data-lake ingestion | Partial writes, duplicate files, late files, small files | Atomic publish protocol, manifest/arrival tracking, file identity |
| JDBC parallel read | Relational source extraction | Overloading DB, skewed split column, connection limits | Tune `numPartitions`, predicate pushdown, read replicas/snapshots |

For a watermark-based incremental extract, do not simply remember `MAX(updated_at)` if updates can share timestamps or arrive late. Use a composite cursor such as `(updated_at, primary_key)`, an overlap/lookback interval, or a CDC log with a source-native sequence.

---

## 9. Data Layout: Partitioning, Clustering, Compaction, and Files

These mechanisms solve related but different problems:

- **Directory/table partitioning:** coarse pruning by low/medium-cardinality columns such as date or region. Avoid extremely high-cardinality partition columns because they create many directories and small files.
- **Clustering / sorting / Z-order-like layout:** groups related values to improve data skipping or locality. Exact capabilities and names depend on table format/platform; do not present one vendor's clustering feature as generic Spark.
- **Bucketing:** hash-distributes table data into a fixed set of buckets; can help compatible repeated joins but depends on metadata, compatible expressions, and optimizer support. It is not a universal shuffle-elimination guarantee.
- **Compaction:** rewrites many small files into fewer appropriately sized files. It reduces listing/planning overhead and improves scan efficiency but consumes compute and can create write amplification.
- **Statistics:** row counts, sizes, min/max, histograms, and table-format metadata help pruning and planning. Refresh them when data changes enough to make them stale.

### 9.1 Choosing partition columns

Choose columns frequently filtered by consumers, with manageable cardinality and reasonably balanced values. `event_date` is common; `customer_id` is usually too high-cardinality for a directory partition column. A table partition strategy should be based on observed query predicates and file layout, not intuition alone.

### 9.2 Small-file problem

Small files increase metadata requests, listing time, task scheduling, open/close overhead, and query planning. Sources include overly frequent micro-batch commits, too many write tasks, high-cardinality `partitionBy`, and tiny incremental updates. Mitigate with compaction, sensible target file sizes, controlled writer parallelism, and table-format maintenance procedures.

---

## 10. Table Formats and Catalogs

Apache Spark supplies execution and data-source APIs. It does **not** make a plain Parquet directory transactional by itself.

| Format | Core idea | Capabilities / design points | Validate before use |
|---|---|---|---|
| **Parquet** | Columnar file format | Column pruning, compression, row-group statistics | No table transaction log or ACID table semantics on its own |
| **Apache Iceberg** | Open table format with metadata manifests/snapshots | Snapshot isolation, schema/partition evolution, hidden partitioning, multi-engine support | Catalog, Spark runtime compatibility, write/merge semantics, maintenance |
| **Delta Lake** | Transaction-log-based lakehouse table format | ACID transaction log, schema enforcement/evolution, time travel, merge/upsert features | Runtime/library compatibility, protocol features, concurrency behavior |
| **Apache Hudi** | Lakehouse table format focused on incremental data management | Upserts, record-level changes, incremental queries, table services | Table type, index, compaction/clustering, connector/version compatibility |

These formats have different metadata models and feature sets. They are not interchangeable “Spark storage formats.” Check the format's own documentation for the exact Spark version and catalog.

### 10.1 Catalog responsibilities

A catalog manages namespaces, table identifiers, metadata, and sometimes authorization. A metastore/catalog is not the same thing as the table format: Iceberg tables, Delta tables, or Hudi tables may be registered in one of several catalogs. Keep catalog ownership, permissions, table metadata, and storage credentials explicit.

---

## 11. Incremental Processing, CDC, and SCD

### 11.1 Change Data Capture

A CDC record commonly includes primary key, operation type (insert/update/delete), before/after image or changed fields, source transaction/sequence, and source commit timestamp. Correctness depends on preserving ordering within a key and applying deletes, not only inserts/updates.

### 11.2 SCD Type 1

Overwrite the current dimension attribute for a key. Use an idempotent merge/upsert keyed by the business key; define how duplicate or out-of-order CDC records are resolved.

### 11.3 SCD Type 2

Preserve history by maintaining effective start/end timestamps and a current-row flag. A correct process must close the previously current row and insert a new version atomically (or use a supported transactional table feature). Handle deletes, no-op changes, late CDC, duplicate events, and source ordering.

### 11.4 Backfills and replay

Backfills must be safe to run alongside or before incremental ingestion. Common patterns include isolated staging tables, date/key-range backfills, new target versions followed by atomic swap, or a controlled merge with deterministic keys. Do not let a historical backfill overwrite newer streaming results without a precedence rule.

---

## 12. Data Quality, Contracts, and Schema Evolution

A data contract should specify schema, field meaning, nullability expectations, keys, allowed value ranges, event-time semantics, update/delete behavior, SLA, owner, and compatibility policy.

**Compatibility questions:** Can a producer add an optional field? Change a field type? Rename or remove a field? Change units or meanings without changing its type? Add a new enum value? Reorder columns? A structural schema change can be backward-compatible while a semantic change is breaking.

Keep a schema registry or versioned schema artifact where appropriate. Use explicit parsing, unknown-field handling, quarantine, and consumer compatibility tests. Do not silently cast a malformed field to NULL and count that as success.

Quality gates should include both technical checks (schema, null rate, duplicate key rate, malformed payload count) and business checks (revenue reconciliation, expected volume, accepted code values, date completeness).

---

## 13. Orchestration and Dependency Management

Spark executes distributed computation. An orchestrator such as Airflow, Dagster, Argo, or a managed workflow service schedules applications, manages dependencies, retries, alerts, backfills, and run metadata.

Use the orchestrator for task-level dependencies and lifecycle; use Spark for data-parallel transformation. Do not build a fragile distributed workflow by having a Spark executor launch external Spark jobs.

Design for:

- Idempotent task boundaries and explicit run IDs.
- Parameterized data intervals and reproducible input snapshots.
- Retries that do not duplicate output.
- Sensor/event-driven triggers where appropriate, with timeout and stale-event handling.
- Backfill windows, concurrency limits, and priority classes.
- Clear separation of orchestration retry from Spark task retry.

---

## 14. Reliability, Replay, and Backfill Design

A reliable data product needs multiple recovery levels:

1. **Task retry:** recover transient task/worker failures inside one application.
2. **Stage retry:** recompute lost shuffle output.
3. **Application retry:** rerun failed job with the same logical input interval.
4. **Streaming restart:** recover source offsets and state from a compatible checkpoint.
5. **Historical replay/backfill:** rebuild derived data from retained raw sources or event logs.
6. **Disaster recovery:** recover from loss of a region, account, catalog, checkpoint store, or source system.

For each level define input retention, output idempotency, maximum retry time, and validation. A checkpoint is not a substitute for raw-data retention or disaster-recovery backups.

---

## 15. Security, Governance, and Multi-Tenancy

Spark is part of a data platform's security boundary. Design for:

- **Authentication:** user/service identity, workload identity, Kerberos or platform-specific methods.
- **Authorization:** catalog/table/column/row policies, storage permissions, and least privilege.
- **Secrets:** secret manager or credential passthrough; do not expose secrets in notebook output, logs, environment dumps, or Spark UI configuration.
- **Encryption:** TLS in transit, encryption at rest, and managed keys where required.
- **PII handling:** classification, masking/tokenization, retention/deletion, audit, and controlled exports.
- **Network isolation:** private subnets, endpoint policies, firewall rules, executor-to-driver reachability, and controlled egress.
- **Multi-tenancy:** queues/pools, workload isolation, cluster policies, fair scheduling where useful, and capacity quotas.
- **Audit and lineage:** who accessed data, what version was read/written, and which transformation produced a data product.

A Spark job running with broad storage credentials can bypass catalog-level controls if those controls are not enforced at the storage layer or by the platform. Align both layers.

---

## 16. Observability, SLAs, and Operations

Track service-level indicators at the data-product boundary, not only Spark job duration.

| Category | Useful indicators |
|---|---|
| Freshness | Event-to-table lag, latest processed event timestamp, batch completion time |
| Completeness | Expected vs actual file/partition/row counts, missing intervals, source offsets |
| Correctness | Duplicate keys, null rates, reconciliation deltas, data-quality failures |
| Performance | Runtime, input/output bytes, task duration percentiles, shuffle, spill, skew |
| Streaming health | Batch duration, input rate, processing rate, source lag, watermark, state rows/bytes |
| Reliability | Failure rate, retry count, restart count, recovery time, checkpoint age |
| Cost | Executor-hours, cluster-hours, storage, data transfer, failed/retried compute |

Define alert thresholds with the owner and business impact. For example, a “job succeeded” alert is insufficient if it wrote zero rows for an expected data interval.

---

## 17. Cost and Capacity Trade-offs

- **Compute vs storage:** storing curated columnar data often reduces repeated scan/compute cost, but compaction and retention consume resources.
- **Batch vs always-on:** scheduled clusters reduce idle spend for non-urgent jobs; continuously running streaming queries need reserved capacity or autoscaling that can meet latency constraints.
- **Autoscaling vs predictable latency:** dynamic allocation/autoscaling improves utilization but may introduce executor startup delay and shuffle/state locality changes.
- **Large vs small executors:** larger executors reduce JVM/process overhead but can amplify GC, contention, and failure blast radius; smaller executors can increase isolation and parallel process count.
- **Data egress:** cross-zone/region movement, external database extraction, and shuffle traffic can be material costs.
- **Retries:** repeated failures and speculative copies consume compute; monitor failure rates, not only successful run costs.
- **SLA-aware optimization:** optimize the expensive bottleneck that threatens the SLA, not every metric at once.

Build a baseline cost per successful data interval and track cost per TB/row/business unit alongside latency and correctness.

---

## 18. Deployment and Platform Choices

Spark can run locally, on Standalone, YARN, Kubernetes, or managed cloud platforms. The architecture is similar, but resource requests, authentication, shuffle service, dynamic allocation, networking, autoscaling, and observability are not identical.

When choosing a deployment model, evaluate: cluster lifecycle, executor isolation, autoscaling behavior, driver resilience, dependency packaging, access to object storage, network boundaries, catalog integration, audit, monitoring, upgrades, and operational ownership.

Managed platforms can simplify cluster operations but may include proprietary runtimes, catalogs, table optimizations, or configuration behavior. Keep an explicit compatibility matrix for Spark version, Scala binary version, connectors, Python version, Java version, table formats, and catalog.

---

## 19. Architecture Anti-Patterns

| Anti-pattern | Why it fails | Better design |
|---|---|---|
| “Spark because data is big” without workload analysis | Adds distributed-system overhead to small/simple work | Choose the simplest engine that meets throughput/latency needs |
| Lambda by default | Duplicates business logic and increases reconciliation burden | Unified table or a single stream path when it meets recovery requirements |
| Stream directly into a BI dashboard without a serving model | Fragile coupling and unpredictable query load | Curated serving tables or a dedicated query/serving layer |
| Partition by every dimension | Creates tiny directories/files and high metadata cost | A few low-cardinality filter columns and compact files |
| Use one giant `foreachBatch` for all side effects | Unclear retry/transaction boundaries | Idempotent sink-specific operations and bounded responsibilities |
| No raw retention | Makes backfills and incident recovery impossible | Immutable raw/event retention aligned with RPO/RTO |
| Exactly-once asserted without sink design | Retries can duplicate effects | Replayable source + checkpoint + atomic/idempotent sink |
| No data contract | Silent schema/semantic drift | Versioned schema, quality gates, ownership |
| Tune by increasing executors only | Cannot fix skew, serial I/O, driver bottlenecks, or bad plans | Diagnose task and stage evidence first |
| Treat catalog as authorization by itself | Direct storage access may bypass catalog checks | Enforce least privilege at catalog and storage layers |

---

## 20. Senior-Level Decision Framework

For any design question, structure the answer in this order:

1. **Clarify requirements:** volume, peak, freshness, correctness, lateness, retention, security, cost, and RPO/RTO.
2. **Define data contracts:** source schema, keys, event time, update/delete semantics, quality rules, ownership.
3. **Choose engine and mode:** Spark batch/micro-batch/RTM or another tool; explain why.
4. **Draw the path:** sources → raw → standardized → serving → consumers, including catalogs and orchestrator.
5. **Explain data layout:** file/table format, partitioning, clustering, compaction, statistics.
6. **Explain correctness:** deduplication, joins, CDC, late data, transaction boundaries, exactly-once claim.
7. **Explain scaling:** input partitions, shuffle, skew, state size, executor/driver sizing, autoscaling.
8. **Explain operations:** monitoring, alerts, replay, backfill, schema upgrades, disaster recovery.
9. **Explain trade-offs:** latency vs cost, simplicity vs capability, batch vs streaming, managed vs self-managed.
10. **State failure modes:** source outage, executor/driver failure, partial sink commit, late data, schema break, catalog/region outage—and how each is handled.

A senior answer is not just a list of Spark configurations. It demonstrates that you can design a data system whose outputs remain correct and recoverable when assumptions fail.
