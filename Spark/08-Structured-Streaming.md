## Table of Contents

1. Batch vs. Streaming — and Where Structured Streaming Sits
2. The Programming Model and Execution Engine
3. Sources and Sinks
4. Output Modes and Triggers
5. Time Semantics, Latency/Throughput, and Windowing
6. Watermarks in Depth
7. Stateless vs. Stateful Processing and State Stores
8. Stream Joins
9. Checkpointing and the Micro-Batch Recovery Protocol
10. Delivery Semantics (and How Spark Actually Delivers Them)
11. Restarts, Upgrades, and Query Evolution
12. Architectural Patterns: Lambda, Kappa, and the Unified Approach
13. Performance Tuning and Capacity Planning
14. Monitoring and Debugging
15. Testing Streaming Code
16. Configuration Reference
17. Failure and Symptom Diagnosis Table
18. Production Readiness Checklist
19. Common Misconceptions
20. A Concise Mental Model

---

## 1. Batch vs. Streaming — and Where Structured Streaming Sits

**Batch processing** handles data in large, finite groups collected before processing begins and run on a schedule (nightly payroll, monthly billing, end-of-day reports, warehouse ETL). **Stream processing** handles data continuously as it is generated — record-by-record or in small micro-batches — for use cases needing immediate insight (fraud detection, sensor monitoring, brand-sentiment feeds).

| Feature | Batch | Stream |
|---|---|---|
| Data flow | Finite, collected-then-processed | Unbounded, processed as generated |
| Latency | Minutes to hours | Milliseconds to seconds (engine- and design-dependent) |
| Data size | Known and finite | Unknown and continuous |
| Execution | Single scheduled run with a start and an end | Long-running; no defined end |
| Primary use | Historical analysis, reports, non-urgent work | Real-time analytics, alerting, immediate decisions |
| Failure model | Re-run the job | Must recover *mid-flight* without loss or duplication |
| State | Usually none between runs | Often needs durable state across events |

**The key design idea of Structured Streaming** is that these two worlds share one API. You write a DataFrame query as if the input were a static table; Spark runs it **incrementally** as new rows arrive. The same `select`/`filter`/`groupBy`/`join` code works for both, which is why batch and streaming skills transfer, and why "incremental batch" (run the streaming query on a schedule and stop) is a first-class pattern (see §4 and §12).

**Engine modes in Structured Streaming:**

- **Micro-batch (default):** the engine repeatedly plans and runs a small batch job over the new data. Latency is typically **~100 ms to a few seconds**, with exactly-once fault-tolerance semantics available. This is what almost all production Spark streaming uses.
- **Continuous processing (experimental, since 2.3):** long-running tasks process records as they arrive, targeting **~1 ms latency**, but with **at-least-once** guarantees and a restricted set of operations (§4).

> **A note on history.** The legacy **DStream** API (`spark.streaming`) is a separate, older, RDD-based micro-batching library and is not recommended for new work. Several widely-quoted details (e.g., `spark.streaming.backpressure.enabled`, receiver-based sources, `StreamingContext.stop(gracefully)`) belong to DStreams, **not** to Structured Streaming. Be careful when reading older material.

---

## 2. The Programming Model and Execution Engine

### 2.1 The "unbounded table" model

Treat the input stream as a table that is **continuously appended**. A query over that table produces a **result table** that is updated as rows arrive. What actually leaves the engine to the sink on each trigger depends on the **output mode** (§4).

```
Input stream            Unbounded Input Table          Query (DataFrame ops)        Result Table          Sink
 (Kafka, files)   ──►   rows appended over time   ──►  select / agg / join   ──►   (conceptual)   ──►  per output mode
```

### 2.2 A minimal but realistic query

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("event_time", TimestampType()),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "sensor-events")
    .option("startingOffsets", "latest")        # only used on the FIRST start of a query
    .option("maxOffsetsPerTrigger", 500_000)    # caps batch size (rate limiting)
    .option("failOnDataLoss", "true")
    .load())

events = (raw
    .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
    .select("e.*"))

avg_temp = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "sensor_id")
    .agg(F.avg("temperature").alias("avg_temp"), F.count("*").alias("n")))

query = (avg_temp.writeStream
    .outputMode("append")                       # window emitted once the watermark passes it
    .format("parquet")                          # file sink: exactly-once via a metadata log
    .option("path", "s3://lake/tables/sensor_5m")
    .option("checkpointLocation", "s3://lake/chk/sensor_5m")   # unique per query, durable storage
    .trigger(processingTime="30 seconds")
    .start())

query.awaitTermination()
```

Nothing runs until `.start()`. Calling `.show()`, `.count()`, or `.collect()` on a streaming DataFrame raises `AnalysisException: Queries with streaming sources must be executed with writeStream.start()` — a streaming DataFrame has no finite result to materialize.

### 2.3 How the engine runs a micro-batch

Internally, `MicroBatchExecution` loops:

1. **Determine the batch's offset range** — ask each source for its latest available offsets (bounded by rate limits such as `maxOffsetsPerTrigger`).
2. **Write the planned offsets to the checkpoint's offset log** (a write-ahead log) *before* processing anything.
3. **Build an incremental plan** — the logical plan is rewritten for this batch's data (`IncrementalExecution`), replacing streaming relations with the batch's slice and inserting state-aware physical operators where needed.
4. **Run the batch** like any Spark job — Catalyst optimization, shuffles, tasks — reading state from the **last committed state-store version** and writing the next one. Versions advance only when a batch succeeds, which is exactly what makes a replayed batch safe: it re-reads the same prior version and overwrites any partial attempt.
5. **Commit to the sink** for batch id *N*.
6. **Write the commit log entry** for batch *N*. Only now is the batch "done."

Because it is an ordinary Spark job per batch, everything from the other guides applies: partitioning, shuffles, join strategies, memory, executor sizing. What streaming adds is **state**, **time**, and **the recovery protocol**.

**You can see the streaming-specific operators in `explain()`:** `StateStoreRestore`/`StateStoreSave` (streaming aggregation), `StreamingDeduplicate`, `StreamingSymmetricHashJoin` (stream-stream join), `EventTimeWatermark`, `FlatMapGroupsWithState`. Their presence tells you the query is stateful and will carry state in the checkpoint.

### 2.4 What Catalyst does and doesn't do for streaming

- Logical optimizations (predicate pushdown, column pruning, constant folding) apply normally — push filters and projections *before* stateful operators to shrink state.
- **Adaptive Query Execution and the cost-based optimizer are disabled for streaming micro-batch plans.** AQE was explicitly disallowed because it can change the number of shuffle partitions between batches, which would break stateful operators (one state store per shuffle partition). The consequences: shuffle partition count is whatever `spark.sql.shuffle.partitions` was at first start, and skew is not automatically split. In open-source Spark the DataFrame handed to `foreachBatch` inherits the same session settings; some platforms add AQE support inside `foreachBatch`, so verify for yours rather than assuming it.
- `spark.sql.shuffle.partitions` also fixes the **number of state partitions** for stateful operators (§7.5, §11) — an important operational constraint.

### 2.5 Unsupported operations

Some DataFrame operations make no sense on unbounded data and are rejected with an `AnalysisException` ("operation XYZ is not supported with streaming DataFrames/Datasets"):

| Operation | Why / what to use instead |
|---|---|
| `limit()`, `take(n)` | No finite "first N rows" of an unbounded stream |
| `distinct()` | Needs all history; use `dropDuplicates` (ideally with a watermark) |
| Sorting | Allowed **only after an aggregation and only in Complete mode** |
| Several chained stateful operations in Update/Complete mode | See §7.2 for the Append-mode support and workaround |
| Some outer joins | See the support matrix in §8 |
| `count()`, `show()`, `collect()`, `foreach()` (actions) | They execute immediately; use `groupBy().count()` for a running count, the console sink for inspection, and `writeStream.foreach(...)` for row-level output |

A subtle one: registering a streaming DataFrame as a temp view and running SQL on it is fine, but Spark can **inject stateful operators while interpreting SQL**, so check `explain()` for state operators after writing SQL against a streaming view.

---

## 3. Sources and Sinks

### 3.1 Built-in sources

| Source | Replayable / fault-tolerant | Notes |
|---|---|---|
| **Kafka** | Yes | The standard production source. Offsets tracked in the Spark checkpoint (not committed to Kafka consumer groups by default). |
| **File** (Parquet/ORC/JSON/CSV/text) | Yes | Watches a directory for new files; requires an explicit schema by default. Files must appear atomically (write elsewhere, then move/rename in). |
| **Rate** (`rate`) / **rate per micro-batch** (`rate-micro-batch`) | Yes | Synthetic data for tests and benchmarks; `rate-micro-batch` emits a fixed row count per batch regardless of trigger timing or lag, so it is better for reproducible tests. |
| **Socket** | **No** | Demos only; not fault-tolerant. |
| Table sources (Delta/Iceberg/Hudi, `readStream.table`) | Yes | Table formats provide their own incremental read semantics. Connector-specific. |

**Kafka source essentials**

- Output columns: `key`, `value` (both binary), `topic`, `partition`, `offset`, `timestamp`, `timestampType`. **`timestamp` is Kafka's record timestamp, not necessarily your event time** — parse event time from the payload when correctness depends on it.
- `startingOffsets` (`earliest` / `latest` / JSON per-partition) applies **only to a brand-new query**. After restart, the checkpoint's offsets win.
- `maxOffsetsPerTrigger` rate-limits each batch (split proportionally across partitions) — essential to avoid a gigantic first batch when starting from `earliest`.
- `minOffsetsPerTrigger` with `maxTriggerDelay` (3.2+) does the opposite: wait until enough data has accumulated (or the delay expires) before launching a batch, avoiding swarms of tiny batches on low-volume topics.
- **Offsets live in Spark's checkpoint, not in Kafka consumer-group commits by default**, so consumer-group lag dashboards will not reflect Spark's real progress. Compute lag from the progress report (`latestOffset` vs `endOffset`) instead.
- Payloads arrive as bytes: parse with `from_json` (explicit schema required), or `from_avro`/`to_avro` via the Avro module; schema-registry integration is connector/platform specific. Route unparseable records to a dead-letter path instead of letting them fail the query.
- `minPartitions` asks Spark to split Kafka partitions into more Spark partitions for higher read parallelism than the Kafka partition count.
- `failOnDataLoss` (default `true`): if offsets the query needs have been deleted by Kafka retention (e.g., the query was down longer than retention), fail loudly. Setting `false` silently skips the gap — a deliberate, risky choice.
- `startingTimestamp`/`endingOffsets` support time-based starts and bounded (batch) reads.
- Sources can be **cleanly rate-limited by `maxOffsetsPerTrigger` / `maxFilesPerTrigger`**; there is no DStream-style automatic backpressure.

**File source essentials:**

- Files are processed in order of modification time (`latestFirst` reverses this, useful for big backlogs).
- `maxFilesPerTrigger` bounds batch size — **the default is no limit**, so set it explicitly if a backlog could produce a huge first batch. (A default of 1000 belongs to some table-format sources such as Delta, not the built-in file source.)
- `maxFileAge` (default 1 week, measured against the newest file's timestamp) ignores older files after the first batch; `fileNameOnly` compares files by name instead of full path.
- `cleanSource` (`archive`/`delete`, 3.0+) cleans up processed files, at the cost of per-batch overhead; the source path must not be shared with other queries or overlap a sink's output directory.
- The set of seen files is tracked in the checkpoint. Globs are supported, but **not multiple comma-separated paths**.
- Partition discovery (`key=value` directories) happens at start; the partition scheme must stay static — adding `year=2016` next to `year=2015` is fine, introducing a new partition column is not.
- Directory listing on very large object-store prefixes becomes a latency and cost factor.

### 3.2 Built-in sinks and what they guarantee

| Sink | Output modes | Fault-tolerance | Delivery guarantee |
|---|---|---|---|
| **File** (Parquet/ORC/JSON/CSV/text) | Append | Yes | **Exactly-once** — a `_spark_metadata` manifest log records committed files per batch; readers using Spark honor it |
| **Kafka** | Append, Update, Complete | Yes | **At-least-once** — Spark's Kafka sink does not use Kafka transactions; replays can produce duplicate records |
| **foreach** | All | Depends on implementation | At-least-once; dedupe using `(partitionId, epochId)` supplied to `open()` |
| **foreachBatch** | All (modes pass through) | Depends on implementation | At-least-once by default; **exactly-once if the write is idempotent keyed on `batch_id`** or goes to a transactional table |
| **Table formats** (Delta, Iceberg, Hudi) | Typically Append (Update/Complete via `foreachBatch`/merge) | Yes | Idempotent/transactional commits — typically exactly-once |
| **Console** | All | No | Debugging |
| **Memory** | Append, Complete | No (driver memory) | Debugging/tests |

> **Reading a file-sink output directory with a non-Spark tool** (plain `ls`, another engine) can show files from batches that were later replayed or aborted, because only the `_spark_metadata` log is authoritative. Read via Spark (or a table format) when exactness matters.

### 3.3 `foreachBatch` — the escape hatch for everything else

`foreachBatch(func)` hands you each micro-batch as an ordinary **batch DataFrame** plus the **batch id**. It unlocks batch-only writers (JDBC, MERGE/upsert), multiple sinks per batch, and batch-style post-processing (including AQE).

```python
def upsert_to_target(batch_df, batch_id):
    # batch_df is a normal DataFrame; dedupe within the batch before MERGE
    latest = (batch_df
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("id").orderBy(F.col("event_time").desc())))
        .filter("rn = 1").drop("rn"))
    latest.createOrReplaceTempView("updates")
    latest.sparkSession.sql("""
        MERGE INTO silver.customers t
        USING updates u ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(stream.writeStream
    .foreachBatch(upsert_to_target)
    .option("checkpointLocation", "s3://lake/chk/customers_upsert")
    .start())
```

An upsert keyed on a natural key is **idempotent**, so replaying a batch after a crash converges to the same table state. For append-only targets without a native idempotent commit, record `batch_id` in the target (or a side table) and skip batches already applied.

**Caveats**

- If a batch writes to **several targets**, there is no atomicity across them: a crash after the first write replays the whole batch, so *every* write must be independently idempotent.
- Cache the batch DataFrame (`batch_df.persist()` / `unpersist()`) if you write it more than once, otherwise each write recomputes the micro-batch from the source.
- `foreach` (row-level) takes an object with `open(partition_id, epoch_id)`, `process(row)`, and `close(error)` methods; `open` may return `False` to skip a partition/epoch already written, which is how you dedupe on replay.

---

## 4. Output Modes and Triggers

### 4.1 Output modes — *what* is written each trigger

| Mode | What is emitted | Valid for | State/cleanup implications |
|---|---|---|---|
| **Append** | Only **new rows** that will never change | Stateless queries; aggregations **with a watermark** (a window is emitted once the watermark passes it); stream-stream joins; dedup | State cleaned as the watermark advances |
| **Update** | Only rows that **changed** since the last trigger | Aggregations (with or without watermark); behaves like Append for stateless queries | With a watermark, old state is cleaned; without one, state grows |
| **Complete** | The **entire** result table every trigger | **Aggregations only** | **Entire result retained forever; watermark cannot reclaim state** — only safe for small, bounded key spaces |

Consequences worth internalizing:

- **Windowed aggregation in Append mode produces no output for a window until the watermark passes the window's end.** A 5-minute window with a 10-minute watermark delay emits ≥15 minutes after the window start in the best case. This delay is not a bug — it is the price of "never emit a row that might change."
- **Update mode** gives low-latency, repeatedly revised results — the sink must handle updates (e.g., upsert), which excludes the file sink.
- Not every operation is allowed in every mode; the analyzer rejects invalid combinations at `start()` time.
- **Append is the default mode** when none is specified.

**Compatibility matrix**

| Query type | Supported modes | Notes |
|---|---|---|
| Aggregation on event time **with watermark** | Append, Update, Complete | Append waits for the watermark to pass; Complete never drops state |
| Other aggregations (no watermark) | Update, Complete | Append unsupported (aggregates can change); state is never dropped |
| `mapGroupsWithState` | Update | Aggregations not allowed in the same query |
| `flatMapGroupsWithState` | Append or Update (per operation mode) | Aggregations allowed after it only in Append operation mode |
| Joins (stream-stream) | Append | Update and Complete unsupported |
| Session-window aggregation | Append, Complete (not Update) | See §5.3 |
| Other (stateless) queries | Append, Update | Complete unsupported (would require keeping all unaggregated data) |

### 4.2 Triggers — *when* a micro-batch runs

| Trigger | Behavior |
|---|---|
| Default (unspecified) | Start the next micro-batch as soon as the previous one finishes (if data available) — lowest latency, highest overhead |
| `processingTime="30 seconds"` | Fixed interval. If a batch overruns the interval, the next starts immediately after; if there is no data, no real work is done |
| `once=True` | **Deprecated** — one batch, then stop; processes everything in a single batch (can overload memory) |
| **`availableNow=True`** (3.3+) | Process **all data available now**, possibly across *multiple* batches honoring rate limits (`maxOffsetsPerTrigger`, `maxFilesPerTrigger`), then stop. The modern replacement for `once` |
| `continuous="1 second"` | **Experimental** continuous processing; the interval is the *checkpoint* interval, not a batch interval |

`availableNow` is the foundation of **incremental batch**: run a streaming query on a schedule (e.g., hourly) with a persistent checkpoint, process only what's new, and shut down — batch economics with streaming semantics and exactly-once progress tracking.

### 4.3 Continuous processing mode (experimental)

- Delivers ~1 ms latency by running **long-lived tasks** that process records as they arrive, instead of planning a job per batch.
- **At-least-once only**, a narrower set of sources/sinks (e.g., Kafka, rate source in; Kafka, console, memory out) and operations (map-like projections/selections; no aggregations, joins, or stateful operators).
- Requires enough cores to hold all long-running tasks simultaneously.
- Treat it as specialized; most workloads that need "faster than a second" are better served by tuning micro-batch first, or by a record-at-a-time engine if sub-100 ms is a hard requirement.

### 4.4 Empty batches and watermark progress

With `spark.sql.streaming.noDataMicroBatches.enabled=true` (default), the engine runs an extra batch **even when no new data arrived** if the watermark advanced and some stateful operator needs to emit/clean state. This is why windows can finally close after the last events arrived. Disabling it saves resources but can leave results stuck until more data arrives.

---

## 5. Time Semantics, Latency/Throughput, and Windowing

### 5.1 Event time vs. processing time

**Event time** is when the event actually occurred at its source — a timestamp embedded in the record (a car sensor reads 10:00:05). It is independent of arrival. **Processing time** is when the system receives or processes the record by its own clock (that same reading reaches the system at 10:00:30 after network delay).

| Feature | Event time | Processing time |
|---|---|---|
| Source of time | Timestamp embedded in the record | System clock at processing |
| Determinism | Deterministic — same result regardless of arrival order | Non-deterministic — varies with delays/failures |
| Accuracy | High — reflects the true event sequence | Lower — sensitive to latency and reordering |
| Complexity | Higher — must handle late/out-of-order data (watermarks) | Low — trivial to implement |
| Typical use | Time-series analytics, financial, fraud | Simple real-time alerting, rough monitoring |

There is also **ingestion time** — the time the record enters the pipeline (e.g., Kafka's record timestamp). It is monotone per partition and easy to obtain, but it still isn't the true event time.

**Why this matters beyond accuracy:** processing-time results are **not reproducible** — a replay after failure would assign events to different windows. Event-time results are what make replay-based recovery (§9–10) produce the same answer.

> In Spark, `current_timestamp()` in a streaming query is a processing-time value and a **non-deterministic** expression; using it to derive grouping keys, window assignment, or dedup keys makes replays diverge. Derive time from the data.

### 5.2 Latency vs. throughput

**Latency** — delay between an event being generated and its result being produced (ms–s). **Throughput** — volume processed per unit time (records/s or MB/s). They trade off: batching more records per micro-batch amortizes per-batch overhead (planning, offset-log writes, shuffle setup), raising throughput but also raising the latency of each individual record.

| Lever | Lower latency | Higher throughput |
|---|---|---|
| Trigger interval | Shorter | Longer |
| Batch size cap (`maxOffsetsPerTrigger`) | Smaller (bounded work per batch) | Larger |
| Shuffle partitions | Fewer (less per-batch overhead) | Enough to use all cores |
| Watermark delay (event-time results) | Shorter (earlier emission, more drops) | — |
| Engine mode | Continuous processing | Micro-batch |

Every micro-batch has **fixed overhead** — hundreds of milliseconds is common, higher on object-store checkpoints — which sets a latency floor for the micro-batch engine regardless of data volume.

### 5.3 Windowing

An aggregation over an unbounded stream needs finite groups. **Windows** are those groups, defined on event time.

- **Tumbling** — fixed-size, non-overlapping (hourly sales blocks).
- **Sliding (hopping)** — fixed-size, overlapping, advancing by a slide interval (5-minute average every minute). One event belongs to *size/slide* windows, multiplying state and output.
- **Session** — dynamic windows that start with activity and close after a **gap of inactivity** (30 minutes with no clicks). Useful for user behavior.

```python
# Tumbling: 5-minute windows
events.groupBy(F.window("event_time", "5 minutes"), "sensor_id").count()

# Sliding: 10-minute windows, sliding every 5 minutes
events.groupBy(F.window("event_time", "10 minutes", "5 minutes"), "sensor_id").count()

# Session windows (Spark 3.2+): close after 30 minutes of inactivity per key
events.groupBy(F.session_window("event_time", "30 minutes"), "user_id").count()

# Dynamic session gap (per-row gap)
events.groupBy(
    F.session_window("event_time",
        F.when(F.col("tier") == "premium", "60 minutes").otherwise("15 minutes")),
    "user_id").count()
```

- **Session-window restrictions in streaming queries:** Update output mode is not supported, and the grouping key must contain **at least one column besides `session_window`** (a global session window works only in batch). Sessions are not partially aggregated locally by default because that needs an extra sort; if many rows share a key per partition, enabling `spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition` can help.
- The `window` column is a struct with `start` and `end`. To use a window result as the *event-time* input of a **second** windowed aggregation, use `window_time(window)` (3.4+), which returns the window's end minus a microsecond, so it is a valid event-time value.
- A **late event is assigned to the window its event time belongs to**, not the window in which it arrived — as long as that window's state still exists (i.e., the watermark has not passed it). That is the entire point of event-time windows: the 10:00 reading arriving at 10:15 still lands in 10:00–10:05.

---

## 6. Watermarks in Depth

### 6.1 What a watermark is

A **watermark** is the engine's moving estimate of "how far event time has progressed," used to decide (a) when a window/aggregate is final enough to emit in Append mode and (b) when old state can be discarded. In Spark:

```
watermark = (max event time observed so far) − (allowed lateness delay)
```

```python
events.withWatermark("event_time", "10 minutes")
```

**Precision matters.** A watermark is a *heuristic threshold*, not a promise that no earlier events will ever arrive. Spark's actual guarantee is asymmetric:

- Data **no more than `delay` behind** the max observed event time is **guaranteed not to be dropped**.
- Data **older than that** *may or may not* be processed — the engine is **allowed** to drop it (and typically does once the state is gone). Late data dropped by a stateful operator is counted in the `numRowsDroppedByWatermark` progress metric.

### 6.2 How the watermark advances

- It is computed from the **max event time seen in the *previous* batches**, and is applied starting with the *next* batch — so there is inherent lag of at least one trigger between data arrival and state cleanup/emission.
- It only moves **forward** — it never decreases.
- **A stalled source stalls the watermark.** If event time stops advancing (idle partition, low-volume topic, a producer that stopped), windows never close in Append mode, and state is never evicted. This is the single most common "my streaming query produces nothing" cause.
- Watermarks are **global** per query, not per key or per partition: one far-ahead outlier event time (a bad clock) can push the watermark forward and cause legitimate data to be dropped as "late." Validate or clamp event times at ingestion.

### 6.3 Multiple inputs

For queries with several watermarked streams (stream-stream joins, unions), Spark tracks a watermark per input and combines them into one global watermark according to `spark.sql.streaming.multipleWatermarkPolicy`:

- `min` (**default**) — the global watermark is the **slowest** input's. Safest (no data from the slow stream dropped) but a lagging/idle stream holds back state cleanup and output for the whole query.
- `max` — the **fastest** input's. Faster output and cleanup, but data from slower streams can be dropped.

### 6.4 Watermark interactions with output modes

| Mode | Effect of watermark |
|---|---|
| Append | Required for aggregations; output is delayed until watermark passes the window end |
| Update | Optional; bounds state; emits updates promptly; late rows beyond the watermark are dropped |
| Complete | Watermark **cannot** clean state; the whole result is kept |

### 6.5 Choosing the delay: the latency–correctness trade-off

A larger delay accepts more late data (more correct) but **increases state size and delays results**; a smaller delay lowers latency and state but **drops more late events**. Choose from measured lateness: look at the distribution of (arrival − event time), set the delay near a high percentile (p99/p99.9) that the business can accept, and monitor `numRowsDroppedByWatermark` to verify the choice in production.

### 6.6 Conditions for a watermark to actually clean aggregation state

Declaring a watermark is not enough; all of these must hold, or state is never reclaimed (and Append-mode output never appears):

1. **Output mode must be Append or Update.** Complete mode preserves everything.
2. The aggregation must group by the **event-time column or a `window` on it**.
3. `withWatermark` must be applied to **the same column** used in the aggregation. `df.withWatermark("time", "1 min").groupBy("time2").count()` does not bound state.
4. `withWatermark` must be called **before** the aggregation. `df.groupBy("time").count().withWatermark("time", "1 min")` is invalid in Append mode.
5. Calling `withWatermark` on a non-streaming DataFrame is a silent no-op.

The same rule applies to deduplication and joins: the watermark column must be the one participating in the key or time constraint.

---

## 7. Stateless vs. Stateful Processing and State Stores

### 7.1 The distinction

**Stateless** processing treats each record independently: output depends only on the current input. It is simple, trivially parallel, and recovery is just "re-run with the current record." **Stateful** processing uses information from previous events; the system must store, version, and recover that state.

| Feature | Stateless | Stateful |
|---|---|---|
| Dependency | Independent events | Depends on past events |
| Operations | `select`, `filter`, `map`/`flatMap`, column derivations, stream-static joins, union | Aggregations, windows, dedup, stream-stream joins, arbitrary state |
| State management | None | Persistent, versioned, fault-tolerant |
| Scaling | Add workers freely | Constrained by state partitioning and state size |
| Fault tolerance | Replay source offsets | Replay offsets **and** restore state from checkpoint |
| Operational burden | Low | High: state growth, schema evolution, partition count fixed |

### 7.2 Stateful operators in Structured Streaming

| Operator | State held | Cleaned by |
|---|---|---|
| Streaming aggregation (`groupBy().agg()`, windows) | Partial aggregates per group/window | Watermark (window end < watermark) |
| `dropDuplicates` | Keys seen so far | Watermark if the event-time column is part of the key |
| Stream-stream join | Buffered rows from each side | Watermark + event-time constraint |
| `flatMapGroupsWithState` / `mapGroupsWithState` (Scala/Java), **`applyInPandasWithState`** (PySpark, 3.4+) | User-defined per-key state | Your code (timeouts) |
| **`transformWithState`** (Spark 4.0+; `transformWithStateInPandas` in PySpark) | Richer typed state (value/list/map), TTL, timers | TTL/timers |

**Deduplication**

```python
# Events may be delivered more than once (at-least-once upstream). Include the
# event-time column among the keys so state can be dropped with the watermark.
deduped = (events
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id", "event_time"]))

# Spark 3.5+: dedupe on event_id alone, bounded by the watermark window
deduped = (events
    .withWatermark("event_time", "1 hour")
    .dropDuplicatesWithinWatermark(["event_id"]))
```

Without a watermark and an event-time component, `dropDuplicates` keeps **every key ever seen** — unbounded state.

The first form only removes duplicates whose **event times are identical**. If a retrying producer stamps a *different* event time on each attempt of the same logical record, those copies are not recognized as duplicates; that is the case `dropDuplicatesWithinWatermark` exists for. Set the watermark delay longer than the maximum event-time gap between copies of the same record.

**Multiple stateful operators (3.4+).** Earlier versions rejected chained stateful operations (e.g., a windowed aggregation followed by another windowed aggregation or by a join) in many cases. From 3.4, multiple stateful operators are supported in **Append mode** with correct watermark propagation (use `window_time` to carry event time through a window). The documented restrictions:

- Chaining multiple stateful operations is **not supported in Update or Complete mode**.
- `mapGroupsWithState`/`flatMapGroupsWithState` followed by another stateful operation is not supported in Append mode, and they cannot be used before *and* after a join.
- The known workaround is to split the pipeline into **multiple queries with one stateful operation each**, with exactly-once ensured per query (for example, an intermediate table or topic between them).

### 7.3 Where state lives: state store providers

State is partitioned by key (same hash partitioning as the shuffle), one **state store instance per state partition**, hosted on executors. Each batch writes a **new version** (= batch id) of each partition's state, persisted to the checkpoint as delta files plus periodic snapshots.

| Provider | Where live state is | Characteristics |
|---|---|---|
| **HDFS-backed (default)** | In-memory map on the executor **JVM heap**; deltas/snapshots to checkpoint storage | Fast access; state size limited by heap; large state → GC pressure, OOM |
| **RocksDB (3.2+)** — `spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` | **Native/off-heap** RocksDB with spill to local disk; snapshots/changelogs to checkpoint | Handles far larger state; avoids JVM GC; modest per-access overhead; **changelog checkpointing** (3.4+, `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled`) cuts commit latency by uploading only changes |

The memory implications (heap vs. off-heap, overhead sizing, container kills when native memory grows) are covered in the memory-management guide; RocksDB's memory is native and must be accounted for in `memoryOverhead`/off-heap budgeting.

**RocksDB specifics worth knowing**

- **Bounding native memory:** left unbounded, RocksDB memory across instances on a node can grow indefinitely and cause OOM. Set `spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage=true` (default `false`) and size the node-wide cap with `spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB` (default 500). Per-instance limits come from `writeBufferSizeMB` and `maxWriteBufferNumber`.
- **Observability vs. speed:** `spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows` (default `true`) costs an extra lookup per write. Turning it off improves throughput but makes the total row count report as **0** — which defeats the "watch `numRowsTotal` for unbounded growth" advice in §14, so choose consciously.
- **Changelog checkpointing** is backward compatible with traditional snapshot checkpointing and can be toggled in either direction by restarting the query.

**State-store locality.** State store providers are scheduled with a *preferred location* so the same executor hosts the same state partition across batches and reuses its in-memory state. Preferred location is not a hard requirement: if Spark places a task elsewhere, that executor must **reload state from the checkpoint**, which for large state can dominate batch time. Raising `spark.locality.wait` makes Spark wait longer for the preferred executor. For the HDFS-backed provider, watch the `loadedMapCacheHitCount` / `loadedMapCacheMissCount` state metrics; a high miss count means state is being reloaded repeatedly.

### 7.4 State growth: how it happens and how it's bounded

State is bounded only if **something tells the engine when it can be dropped**:

- Aggregations: a watermark + a window (or group key containing event time).
- Dedup: a watermark + event-time in key (or `dropDuplicatesWithinWatermark`).
- Joins: watermark on both sides + an event-time range condition.
- Arbitrary state: explicit timeouts/TTL in your code.
- **Complete mode: never.** The full result is retained.

Classic unbounded-state causes: missing watermark; watermark column not actually advancing; grouping on a high-cardinality key with no time component; Complete mode on non-trivial data; session windows with very long gaps; a join on a hot key.

### 7.5 State partitioning is fixed at first start

The number of state partitions equals `spark.sql.shuffle.partitions` **at the time the query first ran**, and is recorded in the checkpoint. Changing the setting later does **not** repartition existing state and a restart with a different value fails or misbehaves. Consequently:

- **Pick the partition count deliberately before launching a stateful query** — enough to spread state and parallelize (a small multiple of the cores you expect to run), with headroom for growth. The default 200 is often too many for small jobs (many tiny tasks) and sometimes too few for very large state.
- To change it, you must **start a new checkpoint** (re-bootstrap state), or migrate via a replay/backfill.

### 7.6 Inspecting state

The query progress `stateOperators` section reports rows and memory per operator (§14). Spark 4.0 adds a **state data source** (state reader) that lets you query a checkpoint's state as a DataFrame for debugging — check your version's documentation for availability and syntax.

---

## 8. Stream Joins

Structured Streaming supports **stream-static** and **stream-stream** joins. They are very different operations.

### 8.1 Stream-static joins

Joining a streaming DataFrame to a **static** (batch) DataFrame is **stateless** from the stream side: each micro-batch's rows are joined against the static side. The static side is part of every batch's plan, so a table source is re-evaluated per batch (picking up updates) unless you cache it (which freezes it — staleness vs. cost is your trade-off). Small static dimensions are typically broadcast.

| Left | Right | Join types supported |
|---|---|---|
| Stream | Static | Inner, Left Outer, Left Semi |
| Static | Stream | Inner, Right Outer |
| Stream | Static | Right Outer, Full Outer — **not supported** |

Use stream-static joins for enrichment (lookup dimension tables, reference data). No watermark or event-time constraint is needed.

### 8.2 Stream-stream joins

Both inputs are unbounded. Unlike a batch join, an arriving row on either side **might match a row that has not arrived yet**, so the engine must **buffer rows from both sides as state** until it knows no more matches can arrive. Without a bound, state grows forever.

**How it works**

1. A row arrives on one side. The engine looks up matching rows in the **other side's state** (by join key + any time constraint) and emits joined rows for matches.
2. The row is **added to its own side's state** so rows arriving later on the other side can match it.
3. As the watermark advances and the event-time constraint makes old rows ineligible, expired state is **cleaned up**.
4. For **outer** joins, a row that never matched is emitted with NULLs **only after the watermark proves no match can still arrive** — so outer-join "no match" results are intentionally **delayed**.

**Two ingredients bound state — you need both for cleanup:**

- **Watermarks on both inputs** (`withWatermark` on each stream) — says how late data can be on each side.
- **An event-time constraint in the join condition** — says how far apart matching events can be:

```python
impressions = (spark.readStream...                       # ad impressions
    .select(F.col("ad_id").alias("imp_ad_id"), F.col("ts").alias("imp_time"))
    .withWatermark("imp_time", "2 hours"))

clicks = (spark.readStream...                            # ad clicks
    .select(F.col("ad_id").alias("click_ad_id"), F.col("ts").alias("click_time"))
    .withWatermark("click_time", "3 hours"))

joined = impressions.join(
    clicks,
    F.expr("""
        imp_ad_id = click_ad_id AND
        click_time >= imp_time AND
        click_time <= imp_time + interval 1 hour
    """),
    "leftOuter")        # impressions with no click within 1 hour → emitted with NULL click, later
```

Equivalent forms: a time-range condition (`BETWEEN`), or a **time-windowed join** on equal `window` values. Event time — not processing time — drives both.

**Support matrix (Spark 3.1+)**

| Join type | Watermark / time constraint |
|---|---|
| Inner | Optional but strongly recommended (without them state grows unbounded) |
| Left Outer | **Required**: watermark on the right side + time constraint (watermark on the left optional, for full state cleanup) |
| Right Outer | **Required**: watermark on the left side + time constraint |
| Full Outer (3.1+) | **Required**: watermark on **one** side + time constraint for correct results (watermark on the other side optional, for full state cleanup) |
| Left Semi (3.1+) | **Required**: watermark on the **right** side + time constraint (watermark on the left optional, for full state cleanup) |

Stream-stream joins support **Append output mode only.**

**Semantic guarantees and caveats**

- **Inner joins** with a watermark return every pair within the constraint; late rows beyond the watermark can be dropped.
- **Outer joins** are *eventually correct* with respect to the watermark — NULL-padded results can be delayed by up to the watermark delay plus the join's time-range, so they have **higher latency**.
- **Global watermark nuance.** With the default `min` policy the slowest stream holds back state cleanup; with `max`, outer-join NULL results can be emitted for rows whose match is still in flight from the slower stream. Spark guards against some of these correctness issues with `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` (default `true`), which can reject queries whose results would be incorrect under the watermark rules.
- **Idle streams delay outer results.** Watermarks advance at the *end* of a micro-batch and the next batch uses them to clean state and emit NULL-padded rows. If either input stops receiving data for a while, event time stops advancing and outer (and semi) output can be held back indefinitely, even though nothing is wrong.
- `mapGroupsWithState`/`flatMapGroupsWithState` cannot be used both before and after a join.
- Cascaded joins (`df1.join(df2).join(df3)`) are supported; each stage buffers its own state.
- A join on a skewed key concentrates state in one state partition (§13).
- Joins followed by further stateful operators rely on watermark propagation (3.4+ multi-stateful-operator support).

### 8.3 Choosing a join design

| Need | Use |
|---|---|
| Enrich events with a reference table | Stream-static join (broadcast the dimension) |
| Correlate two event streams (impression→click, order→payment) | Stream-stream join with watermarks + time bound |
| One side is slowly-changing and large | Consider loading it as a stream-static join against a table format, or a stateful lookup in `flatMapGroupsWithState`/`transformWithState` |
| Only need "has a match" | Left semi join (smaller output, same state cost) |

---

## 9. Checkpointing and the Micro-Batch Recovery Protocol

### 9.1 What the checkpoint contains

A checkpoint directory (configured per query via `checkpointLocation`) stores **everything needed to resume**:

```
<checkpointLocation>/
  metadata        # query id (stable identity of this query)
  offsets/        # write-ahead log: planned offset range for each batch N (written BEFORE processing)
  commits/        # commit log: batch N completed successfully (written AFTER sink commit)
  sources/        # source-specific metadata (e.g., file source's seen-file log)
  state/          # per-operator, per-partition state store files (deltas + snapshots)
```

**The checkpoint stores offsets, progress, and state — not your code.** Your query logic is re-supplied by the application when it restarts, and must be *compatible* with the checkpoint (§11). The checkpoint is bound to one query: **never share a checkpoint between queries**, and never delete it casually — it *is* the query's identity and progress.

Place it on fault-tolerant, shared storage (HDFS, S3, ADLS, GCS). It must be reachable by the driver after the driver machine is lost.

### 9.2 The protocol (why it yields exactly-once progress)

For batch *N*:

| Step | Written | Meaning |
|---|---|---|
| 1 | `offsets/N` | "Batch N will process exactly this offset range" |
| 2 | State and output computed | Executors produce results; state store version N written |
| 3 | Sink commit for batch id N | Output made visible/durable |
| 4 | `commits/N` | "Batch N is complete" |

### 9.3 Recovery walk-through — every possible crash point

| Crash point | State of checkpoint | On restart | Duplicate/loss risk |
|---|---|---|---|
| **A.** Before `offsets/N` written | `commits/N−1` exists | Re-plan batch N from the latest offsets; may include *more* data than the aborted attempt | None — nothing from N was visible |
| **B.** After `offsets/N`, before sink commit | `offsets/N` exists, no `commits/N` | Re-run batch N with **identical offsets**; state loaded from the last committed version | None — sink had nothing for N |
| **C.** After sink wrote/committed, before `commits/N` | `offsets/N` exists, no `commits/N` | Re-run batch N again with identical offsets | **Duplicates unless the sink is idempotent/transactional per batch id** |
| **D.** After `commits/N` | Both exist | Proceed with batch N+1 | None |

Case C is the entire reason sink idempotency matters. The file sink handles it with its metadata log, table formats with transactional commits keyed by (query id, batch id), and custom `foreachBatch` code with upserts or batch-id tracking.

### 9.4 Which failures are handled where

| Failure | Mechanism |
|---|---|
| **Task fails / executor lost** | Spark's normal task retry/stage recomputation; the micro-batch continues (the batch is not restarted from scratch unless the failure exhausts retries) |
| **Driver fails / application killed** | Restart the application with the same checkpoint (needs supervision: YARN/Kubernetes restart policy, `--supervise`, an orchestrator); engine replays per §9.3 |
| **Source data deleted before read (Kafka retention)** | `failOnDataLoss` fails the query; resolution is a deliberate choice (reset, or accept the gap) |
| **Corrupt/incompatible checkpoint** | Query cannot resume; recover by re-bootstrapping from a known offset/timestamp and accepting reprocessing/duplicates, or restoring a checkpoint backup |
| **Sink unavailable** | Batch fails; retried on restart or per retry policy; no progress committed |

Because executors restore state from the checkpoint, **frequent executor churn** (spot reclamation, dynamic allocation scale-down) causes **state reload** cost; prefer stable executors for stateful streaming and avoid aggressive dynamic allocation scale-down.

### 9.5 Checkpoint hygiene

- Old delta/snapshot and log files are cleaned by a maintenance process; `spark.sql.streaming.minBatchesToRetain` (default 100) governs how many batches' metadata are kept.
- On object stores, many small files and listing/commit latency become real costs; RocksDB changelog checkpointing and larger trigger intervals help.
- Back up checkpoints before risky upgrades if you cannot afford a re-bootstrap.

---

## 10. Delivery Semantics (and How Spark Actually Delivers Them)

### 10.1 The three semantics in general

- **At-most-once** — each message is processed zero or one time. The system acknowledges/commits **before** processing, so a crash after the ack loses the message. Fastest, least reliable. Fits tolerant metrics/logging.
- **At-least-once** — each message is processed one or more times. The system processes **first**, acknowledges **after**; a crash between the two causes reprocessing and **duplicates**. Downstream must be **idempotent** (processing twice has the same effect as once).
- **Exactly-once** — each message's *effect* occurs once: no loss, no duplicates. Generally achieved with transactions/two-phase commit or by combining replay with idempotent/atomic output. Required for money-like correctness.

> "Exactly-once" in practice means **exactly-once *effects*** (state and output), not that records are never physically *attempted* twice. Real systems retry; they make retries harmless.

### 10.2 What Structured Streaming guarantees

Spark's streaming engine is designed around **end-to-end exactly-once** semantics under three conditions:

1. **Replayable source** — it can re-deliver the same data for an offset range (Kafka, files; not the socket source). Spark tracks offsets in its own checkpoint.
2. **Deterministic re-execution** — re-running a batch over the same input produces the same output. Avoid non-determinism (`rand()`, `uuid()`, `current_timestamp()`, side-effecting UDFs, reading mutable external data) in anything that affects output or state.
3. **Idempotent (or transactional) sink** — a replay of batch N (§9.3 case C) leaves the sink in the same state as a single execution.

**Exactly-once for state is internal and automatic:** state versions are tied to batch ids, so a replay overwrites a partial attempt and state never double-counts. Whether the **output** is exactly-once depends on the sink.

When any condition fails, you get **at-least-once**: no loss, possible duplicates. This is the reason for the common statement that "at-least-once is the default" — it is what you get **when the sink isn't idempotent**, not a limitation of the engine's progress tracking.

### 10.3 Mechanisms and their roles

| Mechanism | Role |
|---|---|
| Replayable source + offset log | Defines, durably and *before* processing, exactly which data batch N covers |
| Commit log | Marks completed batches; tells recovery where to resume |
| Versioned state store | Makes state updates replay-safe |
| Idempotent/transactional sink (file manifest log, table-format commits, upsert, batch-id dedup) | Makes a replay of batch N harmless |
| Task retry | Handles transient task/executor failure without restarting the batch |

### 10.4 Sink-by-sink end-to-end summary

| Pipeline | Semantics | Why |
|---|---|---|
| Kafka → Spark → **File/Parquet sink** | Exactly-once | Replayable source + manifest-logged atomic file commits |
| Kafka → Spark → **Delta/Iceberg/Hudi** | Exactly-once | Transactional commits keyed by query/batch |
| Kafka → Spark → **foreachBatch + keyed MERGE/upsert** | Exactly-once (effectively) | Upsert is idempotent |
| Kafka → Spark → **foreachBatch + blind JDBC/API append** | At-least-once | Replay re-inserts unless you track `batch_id` |
| Kafka → Spark → **Kafka sink** | At-least-once | No Kafka transactions; downstream consumers must dedupe (e.g., by a unique key/ID) |
| Socket → Spark → anything | Not fault-tolerant | Source can't replay |

### 10.5 Practical idempotency patterns

- **Upsert/MERGE on a natural key** (best).
- **Dedupe by deterministic record ID** at the consumer (Kafka sink case): generate an ID from event content, not `uuid()`.
- **Track applied batch ids**: store `(query_id, batch_id)` in the target or a side table inside the same transaction as the write; skip if present.
- **Overwrite by partition per batch** (write batch N to a path/partition keyed by N, then atomically swap).

---

## 11. Restarts, Upgrades, and Query Evolution

A restart with an existing checkpoint resumes exactly where the previous run left off — **provided the new query is compatible with what the checkpoint recorded.**

**Generally safe changes**

- Trigger interval/type; `maxOffsetsPerTrigger`/`maxFilesPerTrigger`; most sink options.
- Stateless logic: adding/removing filters, changing projections or UDF bodies — as long as the **schema of data flowing into stateful operators** is unchanged.
- Executor sizing, Spark version upgrades within compatibility bounds (check release notes for state-format changes).

**Changes that break or corrupt (require a new checkpoint)**

- Changing the **number or type of input sources** (e.g., different Kafka topics where offsets are tracked per source).
- Changing **stateful operator definition**: grouping keys, aggregate functions, the set of dedup columns, join keys/conditions, state schema.
- Changing **`spark.sql.shuffle.partitions`** for an existing stateful query (§7.5).
- Changing the watermark *column*; changing the watermark *delay* is technically accepted but alters semantics going forward — be deliberate.
- Some sink-type changes (compatibility is specific; e.g., moving between file and Kafka sinks has documented allowed/disallowed directions).

**Strategies for evolving stateful queries**

1. **Blue/green:** start the new version with a **new checkpoint and new output location**, backfilled from a known point (Kafka offset/timestamp, or table snapshot), run both until validated, then cut over.
2. **State reprocessing window:** if state is derived from a bounded window of history (e.g., the last 24 h), replaying that window rebuilds state.
3. **Design for evolution:** keep stateful operators minimal; push business-logic changes into stateless stages or downstream batch; version state schemas deliberately (e.g., with `transformWithState`'s schema-evolution support in Spark 4.x).

Operational note: **watch for restart storms** — a query that crashes on bad data and is auto-restarted immediately will loop; put poison-record handling (dead-letter, `from_json` with permissive mode and `_corrupt_record` handling) in the pipeline.

---

## 12. Architectural Patterns: Lambda, Kappa, and the Unified Approach

### 12.1 Lambda architecture

A **hybrid** that runs **two pipelines** over the same data:

- **Batch layer (cold path):** processes the complete historical dataset in large batches; high latency, high accuracy; its output is treated as ground truth.
- **Speed layer (hot path):** processes new data in real time; low latency, often approximate; fills the gap until the batch layer catches up, and is superseded by it.
- **Serving layer:** merges batch views and real-time views into a unified query surface.

**Pros:** accurate (batch is authoritative); fault-tolerant because raw data is permanently stored and can be reprocessed.
**Cons:** **two codebases and two systems** to build, keep logically equivalent, and operate; double the infrastructure cost; results from the layers can disagree.

*Illustrative scenario — fraud detection:* a nightly Spark batch job analyzes all historical transactions, builds ML models, and computes definitive per-user risk profiles (hours of compute, high accuracy). A streaming job scores each new transaction against a simpler pre-trained model for an immediate "suspicious/not suspicious" flag so a payment can be held. The serving layer exposes the real-time flag now and the batch-confirmed verdict later, supporting audit and model improvement.

### 12.2 Kappa architecture

Proposed by Jay Kreps, Kappa removes the batch layer. **All data — live and historical — is a single stream**, kept in a durable, **append-only, replayable log** (e.g., Kafka) and processed by **one streaming engine and one codebase**. Reprocessing (bug fix, new algorithm) means **replaying the log** through the new version of the job and switching consumers to its output.

**Pros:** much simpler (one pipeline, one code path); consistently low latency; cheaper to operate.
**Cons:** reprocessing a large history can be slow and expensive; requires **long (or tiered/infinite) log retention**; streaming code must tolerate replay (idempotent outputs, deterministic logic, state that can be rebuilt).

*Illustrative scenario — ride-hailing platform:* driver/rider locations, trip requests, and payments flow into a central log. One streaming job maintains surge pricing and nearest-driver matching. To fix a pricing bug, engineers deploy the corrected job and replay the log from the start into a fresh output, then cut over — no separate batch codebase.

### 12.3 Lambda vs. Kappa

| Feature | Lambda | Kappa |
|---|---|---|
| Pipelines | Two (batch + speed) | One (stream) |
| Goal | Balance low latency and high accuracy | Simplicity and low latency |
| Data flow | Ingested by both pipelines | Everything is a stream |
| Accuracy source | Batch layer is ground truth | Depends on streaming logic and idempotency |
| Complexity | High (two codebases, two systems) | Low |
| Reprocessing | Run the batch layer | Replay the log through the same engine |

### 12.4 The unified / lakehouse approach (what Spark enables in practice)

Structured Streaming's single API removes Lambda's biggest cost — the duplicated logic — without forcing a pure-Kappa log-replay model:

- **Same DataFrame code for batch and streaming.** Write a transformation once; apply it to a batch DataFrame (backfill) or a streaming DataFrame (live).
- **Transactional table formats (Delta/Iceberg/Hudi)** serve as both the streaming sink and the batch source, so downstream consumers get consistent snapshots and incremental reads.
- **Medallion layering:** streaming ingestion into **bronze** (raw, append-only), incremental streaming/`availableNow` jobs to **silver** (cleaned, deduped, conformed), and batch/streaming aggregates to **gold** (business-ready). Each hop is exactly-once and independently re-runnable.
- **Incremental batch:** `trigger(availableNow=True)` on a schedule gives batch cost and operational simplicity with streaming's checkpointed, exactly-once incremental progress — the pragmatic middle for many "hourly is fine" pipelines.

**Backfill and replay with Spark — practical cautions**

- Replay from the beginning uses a **new checkpoint** with `startingOffsets=earliest` (or a timestamp).
- Event-time + watermark queries during a fast replay can **drop data as "late"** if partitions are consumed at uneven rates (one partition's event time races ahead and drags the global watermark). Mitigate with a larger temporary watermark delay, per-partition rate limiting, or a **batch backfill** for history followed by a streaming handoff at a known boundary.
- Verify **log retention** covers the replay horizon (or backfill from the lake instead).
- Keep outputs idempotent so partial overlaps at the handoff don't duplicate.

### 12.5 Choosing a pattern

| Situation | Lean toward |
|---|---|
| Hours-level freshness acceptable; cost matters | Batch or incremental batch (`availableNow`) |
| Need seconds-level freshness and results derived from a durable log; can replay | Kappa-style streaming |
| Need both a rapid approximate answer *and* a periodically recomputed authoritative answer with materially different logic (e.g., heavy ML retraining) | Lambda-style, but unify code via shared libraries/DataFrame logic |
| Historical + live from one platform, with ACID tables | Unified lakehouse (streaming + table formats) |

---

## 13. Performance Tuning and Capacity Planning

### 13.1 Define "keeping up"

A query **keeps up** when batch processing time stays under the trigger interval and source lag doesn't grow. Key signals (from the progress report, §14): `processedRowsPerSecond ≥ inputRowsPerSecond`; `batchDuration < trigger interval`; Kafka `latestOffset − endOffset` stable. If processing is slower than arrival, lag grows without bound — add capacity, reduce work per record, or accept a lower freshness SLA.

### 13.2 Throughput levers

- **Parallelism:** Kafka partitions (and `minPartitions`) set source parallelism; `spark.sql.shuffle.partitions` sets stateful/shuffle parallelism (**and state partitions — decide before first start**). Match to available cores with room to scale.
- **Rate limit batch size:** `maxOffsetsPerTrigger`/`maxFilesPerTrigger` prevent giant batches after downtime and bound per-batch memory/state churn.
- **Reduce data early:** filter and project before stateful operators; parse only needed fields; avoid carrying wide payloads through shuffles and state.
- **Avoid UDFs on the hot path** (Catalyst can't optimize them; Python UDFs cross the JVM boundary — prefer built-ins/Pandas UDFs).
- **Serialization:** Kryo for shuffle-heavy queries.
- **Skew:** a hot key concentrates state and work in one partition and stalls every batch (AQE skew handling does not apply to the streaming plan). Pre-aggregate, salt, or filter the hot key into a separate path.
- **Join design:** broadcast small static sides; bound stream-stream state with tight watermarks and the narrowest workable time constraint.

### 13.3 State and memory

- Use **RocksDB** when state is large or JVM GC is heavy; enable **changelog checkpointing** to reduce commit time. Budget native memory (RocksDB block cache, write buffers) in `memoryOverhead`.
- Tighten watermarks and time constraints to the lowest acceptable lateness.
- Avoid Complete mode beyond tiny results; avoid high-cardinality keys without a time bound.
- Watch executor GC time in the UI; rising GC with rising state size is the classic sign to move to RocksDB.

### 13.4 Latency levers

- Shorter trigger / default trigger; fewer shuffle partitions; avoid unnecessary shuffles; keep checkpoint storage fast (object-store commit latency is a per-batch tax); RocksDB changelog checkpointing; consider continuous mode only for simple stateless pipelines where ~ms matters.
- Understand the **fixed per-batch overhead**: planning, offset-log and commit-log writes, task scheduling, state commit. For very small batches the overhead dominates.

### 13.5 Sink and storage hygiene

- **Small files:** frequent short triggers create many small files. Lengthen triggers, use `repartition`/`coalesce` before write (mind the shuffle), file-sink log compaction (`spark.sql.streaming.fileSink.log.compactInterval`), or rely on table-format compaction (`OPTIMIZE`/rewrite).
- Partition output by low-cardinality columns (date/hour), not high-cardinality keys.
- Keep the `_spark_metadata` log and checkpoint directories maintained (compaction/cleanup settings).

### 13.6 Cluster and resources

- Streaming queries are **long-running**: size executors for steady state plus bursts.
- **Dynamic allocation fits streaming poorly** — executors idle between batches look removable, and removing executors forces state reload; if used, set conservative idle timeouts and minimums. Prefer a right-sized fixed allocation for stateful queries.
- Use **stable (non-preemptible) capacity** for the driver and, ideally, stateful executors; restart policy and external supervision are mandatory for drivers.
- Several queries in one application share executors and the scheduler; use **FAIR scheduler pools** to avoid one query starving another, or separate applications for isolation.

---

## 14. Monitoring and Debugging

### 14.1 Programmatic status

```python
query.status          # {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}
query.lastProgress    # latest progress report (dict)
query.recentProgress  # last N progress reports (spark.sql.streaming.numRecentProgressUpdates, default 100)
query.exception()     # why the query terminated, if it failed
spark.streams.active  # all active queries
```

### 14.2 The progress report — what to read

```json
{
  "id": "…", "runId": "…", "name": "sensor_5m", "batchId": 1042,
  "numInputRows": 480000,
  "inputRowsPerSecond": 16000.0,
  "processedRowsPerSecond": 22000.0,
  "durationMs": { "addBatch": 19800, "getBatch": 12, "latestOffset": 45,
                  "queryPlanning": 120, "triggerExecution": 21900, "walCommit": 210 },
  "eventTime": { "avg": "…", "max": "…", "min": "…", "watermark": "2026-10-09T09:50:00.000Z" },
  "stateOperators": [{ "numRowsTotal": 1250000, "numRowsUpdated": 4800,
                       "numRowsDroppedByWatermark": 37, "memoryUsedBytes": 910000000 }],
  "sources": [{ "startOffset": …, "endOffset": …, "latestOffset": …, "numInputRows": 480000 }],
  "sink": { "numOutputRows": 4800 }
}
```

| Field | What it tells you |
|---|---|
| `inputRowsPerSecond` vs `processedRowsPerSecond` | Arrival vs. processing rate; processing < arrival ⇒ falling behind |
| `durationMs.triggerExecution` vs trigger interval | Headroom per batch |
| `durationMs.addBatch` | Time actually executing the batch (the work); large ⇒ compute/shuffle/state/sink bound |
| `durationMs.walCommit` / `latestOffset` | Offset-log and source metadata cost (spikes on slow object stores or Kafka metadata) |
| `eventTime.watermark` | Current watermark — **if it stops moving, Append-mode windows and state cleanup stall** |
| `stateOperators.numRowsTotal` | State size over time — monotonic growth = unbounded state |
| `stateOperators.numRowsDroppedByWatermark` | Late data being dropped — compare to expectations; tune the delay |
| `stateOperators.memoryUsedBytes` | State memory pressure |
| `sources[].latestOffset − endOffset` | **Source lag** (how far behind real time) |

Export these continuously with a **`StreamingQueryListener`** (Python support in 3.4+) to your metrics system and alert on lag, batch duration vs. trigger, watermark stall, and state growth. `df.observe(...)` (3.0+) adds custom metrics (counts of invalid rows, null rates) that appear in the progress report.

### 14.3 Spark UI

The **Structured Streaming** tab shows per-query input rate, process rate, batch duration, and operation durations over time; the **SQL** tab shows each batch's physical plan and the **Jobs/Stages/Executors** tabs expose skew, spill, GC, and shuffle per batch. Use the history server for post-mortem. Each micro-batch appears as its own job(s) — a rising count of tiny jobs with high scheduling overhead suggests too-small batches.

### 14.4 A debugging order of operations

1. **Is it running and progressing?** `status`, `lastProgress.batchId` increasing, `exception()`.
2. **Is data arriving?** `inputRowsPerSecond`, source offsets, upstream health.
3. **Are results appearing?** In Append mode with windows: check the **watermark** is advancing. Idle partitions, a single stuck source, or a bad clock stalls it.
4. **Is it keeping up?** Compare rates and durations; find which phase dominates `durationMs`.
5. **Is state healthy?** `numRowsTotal` trend, `numRowsDroppedByWatermark`, executor GC.
6. **Is the sink the bottleneck?** Slow `addBatch` with idle executors often means a slow external write or tiny-file explosion.
7. **After restart problems:** check checkpoint compatibility (§11), shuffle-partition count, source/sink changes.

---

## 15. Testing Streaming Code

- **Rate source** (`format("rate")`) for synthetic input; **file source** with a temp directory for deterministic input; Scala's `MemoryStream` for unit tests (not exposed in PySpark).
- **Memory sink** or `foreachBatch` collecting into a list to assert on outputs; call `query.processAllAvailable()` to block until all available input has been processed (tests only — it can block forever on a never-ending source).
- Test **restart behavior**: process some data, stop the query, change nothing, restart with the same checkpoint, assert no loss/duplication; then simulate **crash points** by killing between steps.
- Test **late-data behavior**: events inside/outside the watermark delay, and verify drop counts.
- Test **idempotency of sinks** by deliberately replaying a batch.
- Keep business logic in pure functions of DataFrames (`transform(df) -> df`) so the same function runs under batch tests and streaming.

---

## 16. Configuration Reference

| Config / Option | Default | Purpose |
|---|---|---|
| `checkpointLocation` (query option) | — (required in production) | Durable per-query checkpoint directory |
| `spark.sql.streaming.checkpointLocation` | unset | Default base location if the option isn't given |
| `spark.sql.shuffle.partitions` | 200 | Shuffle **and state** partition count (fixed per stateful checkpoint) |
| `spark.sql.streaming.stateStore.providerClass` | HDFS-backed provider | Switch to RocksDB provider for large state |
| `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled` | false | Upload only state changes per commit (3.4+) |
| `spark.sql.streaming.stateStore.minDeltasForSnapshot` | 10 | Deltas between state snapshots |
| `spark.sql.streaming.stateStore.maintenanceInterval` | 60s | Background state maintenance cadence |
| `spark.sql.streaming.stateStore.compression.codec` | lz4 | Compression of state files |
| `spark.sql.streaming.minBatchesToRetain` | 100 | Batches of metadata/state versions retained |
| `spark.sql.streaming.noDataMicroBatches.enabled` | true | Run extra batches to advance watermark/emit/clean state without new data |
| `spark.sql.streaming.multipleWatermarkPolicy` | min | How multiple input watermarks combine (`min`/`max`) |
| `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` | true | Reject queries whose stateful results would be incorrect |
| `spark.sql.streaming.numRecentProgressUpdates` | 100 | Retained entries in `recentProgress` |
| `spark.sql.streaming.metricsEnabled` | false | Publish streaming metrics to the metrics system |
| `spark.sql.streaming.schemaInference` | false | Allow schema inference for file sources |
| `spark.sql.streaming.fileSink.log.compactInterval` | 10 | File sink metadata log compaction cadence |
| Kafka: `maxOffsetsPerTrigger` | unset | Per-batch record cap (rate limit) |
| Kafka: `minPartitions` | unset | Minimum Spark partitions for Kafka reads |
| Kafka: `startingOffsets` | `latest` (streaming) | First-start position only |
| Kafka: `failOnDataLoss` | true | Fail when needed offsets were deleted |
| File: `maxFilesPerTrigger` | 1000 | Per-batch file cap |
| `trigger(processingTime=…)` / `availableNow=True` / `continuous=…` | default trigger | Batch cadence / incremental batch / continuous mode |

---

## 17. Failure and Symptom Diagnosis Table

| Symptom | Likely cause | What to check / fix |
|---|---|---|
| `Queries with streaming sources must be executed with writeStream.start()` | Calling `show/count/collect` on a streaming DataFrame | Use `writeStream`; for inspection use console/memory sink or `foreachBatch` |
| Append-mode windowed query outputs nothing | Watermark hasn't passed window end; stalled event time; idle partitions | `eventTime.watermark` in progress; data flowing on all partitions; consider Update mode or smaller delay; confirm `noDataMicroBatches` enabled |
| Query falls steadily behind (lag grows) | Processing slower than arrival | `processedRowsPerSecond` vs `inputRowsPerSecond`; scale executors/partitions, remove UDFs/shuffles, tune batch size, fix skew |
| State size (`numRowsTotal`) grows without bound | No watermark / watermark not advancing / Complete mode / high-cardinality key / dedup lacking event-time | Add/repair watermark; include event time in keys; use `dropDuplicatesWithinWatermark`; bound join time constraints |
| Executor OOM or long GC pauses on stateful query | State on JVM heap | Move to RocksDB; reduce state; increase memory; account native memory in overhead |
| Container killed for memory with RocksDB | Native memory exceeds overhead | Raise `memoryOverhead`/off-heap budget; tune RocksDB memory |
| Restart fails after changing `spark.sql.shuffle.partitions` | State partitions fixed at first start | Restore original value, or start a new checkpoint and re-bootstrap |
| Restart fails after code change | Stateful operator/schema/source incompatibility with checkpoint | Revert, or blue/green with a new checkpoint (§11) |
| Duplicate rows in sink after a crash/restart | Non-idempotent sink (blind append, Kafka sink) at replay point C | Make writes idempotent (MERGE/upsert, batch-id tracking), or dedupe downstream |
| Missing rows / "data may have been lost" error | Kafka retention deleted offsets while query was down (`failOnDataLoss`) | Reset deliberately (new checkpoint, chosen start), increase retention, shorten downtime; avoid blindly setting `failOnDataLoss=false` |
| Late events silently missing from results | Beyond watermark delay → dropped | `numRowsDroppedByWatermark`; widen delay; fix upstream lateness; validate event-time clocks |
| Left-outer stream-stream join results arrive very late | Unmatched rows emitted only after watermark proves no match | Expected; shrink watermark delay/time range if the business allows |
| Join produces no output | Missing/too-tight time constraint; watermarks not defined; wrong key | Check join condition, constraint direction, watermark columns, event-time parsing |
| Query rejected at start: unsupported operation | e.g., Complete mode with non-aggregation, `limit`, `distinct`, non-aggregated sort, invalid join/mode combination | Use supported equivalents (`dropDuplicates`, window + aggregation); review mode table |
| Huge number of small output files | Short trigger + many partitions | Longer trigger, reduce partitions before write, compaction/OPTIMIZE |
| Very slow commits / high `walCommit` | Object-store checkpoint latency, small-file metadata | Faster checkpoint storage, RocksDB changelog checkpointing, longer trigger |
| First batch after start is enormous and fails | Starting from `earliest` or after downtime without rate limit | Set `maxOffsetsPerTrigger`/`maxFilesPerTrigger` |
| Same file processed twice / files missed | Files not written atomically; mutated in place; path/glob changes | Write elsewhere then move atomically; don't modify files after landing |
| Results differ on replay | Non-deterministic logic (`rand`, `uuid`, `current_timestamp`, external lookups) | Derive values from event data; seed deterministically; avoid side-effecting UDFs |

---

## 18. Production Readiness Checklist

**Correctness**
- [ ] Source is replayable; offsets managed by Spark's checkpoint.
- [ ] Sink is idempotent/transactional, or batch-id deduplication is implemented and tested.
- [ ] Transformations affecting state/output are deterministic.
- [ ] Event time parsed from data and validated (clamp absurd/future timestamps).
- [ ] Watermark delay chosen from measured lateness; `numRowsDroppedByWatermark` monitored.

**State and evolution**
- [ ] Every stateful operator has a bounded-state mechanism (watermark + time key, TTL, or timeouts).
- [ ] `spark.sql.shuffle.partitions` set deliberately *before* the first start; recorded in runbooks.
- [ ] State provider chosen (RocksDB for large state); native memory budgeted.
- [ ] Upgrade/evolution plan: which changes need a new checkpoint; blue/green procedure documented.

**Operations**
- [ ] Unique, durable checkpoint per query on fault-tolerant storage; backups for critical queries.
- [ ] Driver supervised with automatic restart; stable capacity for driver/executors.
- [ ] Rate limits (`maxOffsetsPerTrigger`/`maxFilesPerTrigger`) set to survive restarts after downtime.
- [ ] Poison-record handling (dead-letter path; permissive parsing) so bad data doesn't cause restart loops.
- [ ] Kafka retention exceeds the longest tolerable outage; `failOnDataLoss` policy deliberate.

**Observability**
- [ ] `StreamingQueryListener`/metrics exported: lag, batch duration vs trigger, watermark progress, state rows/memory, dropped rows.
- [ ] Alerts on: lag growth, watermark stall, state growth, failed/stopped queries, batch duration approaching trigger interval.

**Performance**
- [ ] Filters/projections before stateful operators; no UDFs on hot path; skew assessed.
- [ ] Small-file strategy defined for the sink.
- [ ] Capacity headroom for peak load (e.g., 2× steady state) and catch-up after outages.

---

## 19. Common Misconceptions

| Misconception | Reality |
|---|---|
| "The checkpoint saves my query logic." | It saves **offsets, progress, and state** — not code. You supply the logic on restart, and it must be compatible with the checkpoint. |
| "Spark Streaming is at-least-once by default." | The engine's progress tracking supports exactly-once; you get **at-least-once effects when the sink isn't idempotent** (e.g., Kafka sink, blind appends). |
| "The Kafka sink gives exactly-once." | It does not use Kafka transactions; it's **at-least-once**. Dedupe downstream. |
| "A watermark means anything later is dropped." | It means data within the delay is **guaranteed kept**; older data **may** be dropped. |
| "If a task fails, Spark reprocesses the whole micro-batch." | Task failures are retried at the task/stage level. The batch is **replayed from the offset log** only after a driver/application failure. |
| "Windowed Append-mode output appears when the window closes in wall-clock time." | It appears when the **watermark** (event-time-driven) passes the window end — if event time stalls, nothing is emitted. |
| "Changing `shuffle.partitions` is a harmless tuning step." | For stateful queries it's **fixed in the checkpoint**; changing it breaks restart. |
| "AQE will fix skew in my streaming query." | AQE (and CBO) are **disabled for streaming micro-batch plans**, because changing shuffle partition counts between batches would break state. Handle skew yourself. |
| "Complete mode with a watermark will clean state." | In Complete mode the full result is retained; **the watermark can't reclaim it**. |
| "Backpressure is automatic." | That's DStreams. In Structured Streaming you rate-limit with `maxOffsetsPerTrigger`/`maxFilesPerTrigger`. |
| "Kappa means no batch ever." | Kappa unifies *code and pipeline*; replay through the stream engine plays the role of batch reprocessing — and in Spark, `availableNow` makes incremental batch part of the same model. |
| "Processing time is fine; it's simpler." | It's fine for rough alerting but results are **non-reproducible** and break replay-based correctness. |

---

## 20. A Concise Mental Model

- **Structured Streaming = your batch query, run incrementally.** One DataFrame API, an unbounded input table, a result table, and an output mode that decides what leaves the engine.
- **Each trigger is an ordinary Spark job** with extra bookkeeping: *plan offsets → log them → run (reading/writing versioned state) → commit to sink → log the commit.* Every guarantee flows from that ordering.
- **Offsets are written before work; commits after.** That ordering makes every crash recoverable by **replaying the same offsets** — which is safe for state (versioned) and for sinks that are idempotent.
- **Exactly-once = replayable source + deterministic logic + idempotent sink.** Remove any one and you're at **at-least-once**.
- **Event time + watermark** make results correct and reproducible; the watermark is a *heuristic bound on lateness* that simultaneously controls **when results are emitted** and **when state is freed**. If it stops moving, outputs stop and state balloons.
- **State is the cost center.** Stateful operators are bounded only by watermarks/time constraints/timeouts; their partition count is **fixed at first start**; their memory lives on heap (default) or in RocksDB.
- **Stream-stream joins buffer both sides** until a time bound proves no more matches can arrive — outer joins pay for that in latency.
- **Evolve carefully:** stateless changes are cheap; stateful or partition-count changes mean a new checkpoint and a planned backfill.
- **Pick the pattern to match the freshness need:** batch or `availableNow` for hourly, Kappa-style streaming for seconds, Lambda only when two genuinely different computations are required — and in Spark, unify the code regardless.
- **Observe the watermark, the lag, the state size, and the dropped-row count.** Those four numbers explain most production incidents.
