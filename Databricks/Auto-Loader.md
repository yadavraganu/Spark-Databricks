# Databricks Auto Loader — Deep Dive Guide

## 1. What Auto Loader Is

Auto Loader is Databricks' purpose-built source for **incrementally and efficiently ingesting files from cloud object storage** (S3, ADLS, GCS) into Delta tables. It's exposed as a Spark Structured Streaming source, `cloudFiles`, so you get all the usual streaming guarantees — exactly-once processing, checkpointing, incremental progress — applied specifically to the "new files keep landing in a folder" problem.

What it solves that a plain batch job or naive `spark.read` loop doesn't:
- **Knows what it's already processed.** It never reprocesses a file it's already ingested, without you having to track state yourself.
- **Scales to huge directories.** Works whether a folder has a thousand files or billions, without repeatedly listing the whole directory tree.
- **Infers and evolves schema automatically.** New columns appearing in source data don't silently break the pipeline or get dropped.
- **Rescues data instead of losing it.** Anything that doesn't fit the expected schema is captured, not discarded.

---

## 2. How It Works, Conceptually

```
Cloud Storage (S3 / ADLS / GCS)
        │
        │  new files arrive
        ▼
┌───────────────────────┐
│   File Discovery       │  ← directory listing OR file notifications
│   (what's new?)        │
└───────────────────────┘
        │
        ▼
┌───────────────────────┐
│  cloudFiles source     │  ← tracks processed files, infers/evolves schema
└───────────────────────┘
        │
        ▼
Structured Streaming micro-batches
        │
        ▼
   Delta table (Bronze layer, typically)
```

Two things make this incremental and reliable:
- **A checkpoint** (`checkpointLocation`) tracks which files have already been processed — this is standard Structured Streaming checkpointing, giving exactly-once semantics on restart/failure.
- **A schema location** (`cloudFiles.schemaLocation`) separately tracks the inferred schema over time, so schema changes are remembered across restarts even though it's a distinct concern from checkpointing.

---

## 3. Basic Usage

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema")
    .load("s3://my-bucket/landing/orders/"))

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders/data")
    .trigger(availableNow=True)   # or .trigger(processingTime="1 minute") for continuous
    .toTable("production.bronze.orders"))
```

- `format("cloudFiles")` tells Spark to use Auto Loader as the source.
- `cloudFiles.format` is the **source file format** (not to be confused with the sink's `delta` format on the write side).
- `cloudFiles.schemaLocation` is required whenever schema inference is used — it's where Auto Loader persists what it has learned about the schema.
- The write side is just an ordinary Structured Streaming sink — nothing Auto-Loader-specific happens there.

**Supported source formats:** JSON, CSV, Parquet, Avro, ORC, text, binaryFile, and (on newer runtimes) XML. Schema inference and evolution are supported for JSON, CSV, XML, Avro, and Parquet — the more self-describing formats. Parquet schema evolution and XML support both require specific minimum runtime versions, so check your Databricks Runtime if either doesn't behave as expected.

---

## 4. File Discovery: Two Modes

### Directory listing mode (default)
Auto Loader periodically **lists** the input directory to find new files.

- Simple — works out of the box on any storage, no cloud setup required.
- Cost and latency scale with how many objects are in the directory, since every listing call walks the tree (Auto Loader does this incrementally where possible, but it's still fundamentally a listing operation).
- Fine for low-to-moderate file volumes or less latency-sensitive pipelines.

### File notification mode (recommended for production/scale)
Auto Loader relies on the **cloud provider's native event system** to be told about new files directly, instead of asking repeatedly.

```python
.option("cloudFiles.useNotifications", "true")
```

- Backed by S3 Event Notifications + SQS (AWS), Event Grid + Azure Queue Storage (Azure), or Pub/Sub (GCP).
- Lower latency (near real-time) and lower cloud API cost at scale, since you're not paying for repeated LIST calls.
- Historically required Auto Loader to provision and manage its own cloud notification queue/subscription per stream.

**Managed file events** is the newer, simpler path to the same benefit: when file events are enabled on a Unity Catalog **external location**, Auto Loader can subscribe to a shared, Databricks-managed event pipeline instead of standing up its own cloud resources per stream.

```python
.option("cloudFiles.useManagedFileEvents", "true")
```

For most new production pipelines on Unity Catalog, enabling file events on the external location and using managed file events is the path Databricks recommends — it gets you notification-mode latency without the per-stream cloud infrastructure management of classic `useNotifications`.

---

## 5. Schema Inference and Evolution

On first run, Auto Loader **samples** the source files to infer column names and types, then persists that inferred schema to `cloudFiles.schemaLocation`. From then on, it tracks drift against that stored schema rather than re-inferring from scratch every batch.

```python
.option("cloudFiles.inferColumnTypes", "true")   # infer types, not just treat everything as string
.option("cloudFiles.schemaHints", "order_date DATE, amount DECIMAL(10,2)")  # override inference for specific columns
```

`schemaHints` is useful when inference gets something wrong or ambiguous (e.g., a column that's sometimes numeric, sometimes string) — you tell Auto Loader the type up front instead of fighting its guess.

### Schema evolution modes (`cloudFiles.schemaEvolutionMode`)

| Mode | Behavior when a new column appears |
|---|---|
| `addNewColumns` **(default)** | Stream stops once; the new column is added to the tracked schema; restarting the stream proceeds cleanly with the updated schema. |
| `rescue` | Schema is never evolved. New/unexpected columns are captured in `_rescued_data` instead, and the stream never fails on drift. |
| `failOnNewColumns` | Stream stops and **stays stopped** — requires you to manually update the schema (or remove the offending data) before it will restart. |
| `none` | Schema is never evolved; new columns are silently dropped unless `rescuedDataColumn` is explicitly set, in which case they're rescued instead. |

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

**`addNewColumnsWithTypeWidening`** (public preview, newer runtimes) extends `addNewColumns` to also widen a column's *type* automatically when a stricter-typed value no longer fits (e.g., an `INT` column that starts seeing values that need `BIGINT`), leaning on Delta Lake's type-widening feature rather than failing or rescuing.

### The rescued data column
Regardless of mode, Auto Loader can capture anything that doesn't cleanly match the expected schema — unexpected columns, type mismatches, even case differences — in a `_rescued_data` column (name configurable via `rescuedDataColumn`), stored as a JSON string:

```python
.option("cloudFiles.schemaEvolutionMode", "rescue")
.option("rescuedDataColumn", "_rescued_data")
```

**Production pattern:** even in `addNewColumns` mode, periodically query `_rescued_data` — it quietly catches type-mismatched values that technically "fit" the schema check but weren't what you expected, which `addNewColumns` alone won't surface.

```sql
SELECT _rescued_data FROM production.bronze.orders WHERE _rescued_data IS NOT NULL;
```

---

## 6. Throughput Controls

Auto Loader processes files in micro-batches; you can cap how much each batch takes on:

```python
.option("cloudFiles.maxFilesPerTrigger", "1000")   # default; hard cap on file count per batch
.option("cloudFiles.maxBytesPerTrigger", "1g")      # soft cap on bytes per batch
```

- `maxFilesPerTrigger` is a hard cap — exactly that many files (at most) per micro-batch.
- `maxBytesPerTrigger` is a **soft** limit: Auto Loader never splits a single file across batches, so a batch can slightly exceed the byte target if one file is large.
- Used together, Auto Loader consumes up to whichever limit is hit first.

Tune these down for steadier, more predictable micro-batches on bursty sources; tune them up (or omit them) to let Auto Loader catch up faster on backlog.

---

## 7. Backfills

```python
.option("cloudFiles.backfillInterval", "1 day")
```

Even in file notification mode, cloud notification systems can occasionally miss or drop an event (rare, but possible with any pub/sub system). `backfillInterval` tells Auto Loader to periodically run an asynchronous directory listing anyway, as a safety net, to catch anything notifications missed — giving you notification-mode latency most of the time with directory-listing-mode reliability as a backstop.

---

## 8. Partition-Aware Ingestion

Auto Loader can extract partition values directly from Hive-style folder paths without you parsing them manually:

```
s3://my-bucket/landing/orders/region=EU/year=2026/month=09/orders.json
```

```python
.load("s3://my-bucket/landing/orders/")
# region, year, month become columns automatically if the path follows this pattern
```

---

## 9. Auto Loader vs. `COPY INTO`

Both ingest new files incrementally from cloud storage into Delta, and both avoid reprocessing files already loaded — the choice is about scale and latency, not correctness:

| | Auto Loader | `COPY INTO` |
|---|---|---|
| **Execution model** | Streaming (continuous or scheduled micro-batches) | Batch SQL command, run on a schedule |
| **Scale** | Designed for very large numbers of files (millions to billions) | Best for smaller numbers of files per run (thousands) |
| **File tracking** | Internal state via checkpoint, scales efficiently | Tracks processed files in Delta table history — gets slower as the list grows |
| **Latency** | Can be near-real-time with file notifications | Inherently batch — as fresh as your last scheduled run |
| **Setup complexity** | Streaming concepts (checkpoints, triggers) | Simpler — just a SQL statement |

Rule of thumb: start with `COPY INTO` for small, infrequent, simple loads; move to Auto Loader once file volume grows, latency matters, or you need schema evolution handling beyond what `COPY INTO` offers.

---

## 10. Auto Loader Inside Lakeflow Declarative Pipelines (DLT)

Auto Loader is also the standard ingestion source inside Lakeflow Declarative Pipelines (formerly Delta Live Tables) — and there, the pipeline runtime takes over checkpointing and schema-location management for you:

```python
import dlt

@dlt.table
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("s3://my-bucket/landing/orders/")
    )
```

You still set `cloudFiles.format` and any Auto Loader options you need (schema hints, evolution mode, throughput limits), but you don't manually manage `checkpointLocation` or `schemaLocation` — the pipeline framework handles both as part of its own state management.

---

## 11. Common Production Pattern: Medallion Ingestion

```python
# Bronze: permissive, schema-on-read, rescue liberally
bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/bronze/orders/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("s3://landing/orders/"))

bronze.writeStream \
    .option("checkpointLocation", "/checkpoints/bronze/orders/data") \
    .trigger(availableNow=True) \
    .toTable("production.bronze.orders")
```

A common convention: let **Bronze** stay permissive (`addNewColumns` or `rescue`, new fields flow in freely), while **Silver/Gold** enforce an explicit, intentional schema contract — new columns don't automatically propagate downstream from Bronze until someone (or an automated check) decides they should and updates the Silver transform accordingly.

---

## 12. Troubleshooting & Gotchas

- **Stream stopped unexpectedly after a schema change?** That's expected behavior for `addNewColumns` and `failOnNewColumns` — check the schema location, confirm the new column looks right, and restart the stream.
- **Data missing that you expected to see?** Check `_rescued_data` before assuming it was dropped — in `rescue`/`addNewColumns` modes it's often sitting there, not gone.
- **Reprocessing a file that changed in place?** Auto Loader's default behavior does *not* reprocess a file just because it was appended to or overwritten after initial ingestion — if you need that, you need an explicit backfill or a different ingestion pattern.
- **Schema location vs. checkpoint location are different things** — don't point both at the same path or reuse a schema location across unrelated streams; each stream's schema state should be isolated.
- **Directory listing feels slow/expensive?** That's the signal to switch to file notification mode (ideally managed file events via a Unity Catalog external location) rather than tuning listing further.

---

## 13. Quick Reference

| Option | Purpose |
|---|---|
| `cloudFiles.format` | Source file format (json, csv, parquet, avro, orc, text, binaryFile, xml) |
| `cloudFiles.schemaLocation` | Where inferred/evolved schema is persisted |
| `cloudFiles.inferColumnTypes` | Infer real types instead of treating everything as string |
| `cloudFiles.schemaHints` | Manually specify types for specific columns |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` / `rescue` / `failOnNewColumns` / `none` / `addNewColumnsWithTypeWidening` |
| `rescuedDataColumn` | Rename the rescued-data column (default `_rescued_data`) |
| `cloudFiles.useNotifications` | Enable classic file notification mode |
| `cloudFiles.useManagedFileEvents` | Enable managed file events (via UC external location) |
| `cloudFiles.backfillInterval` | Periodic safety-net directory listing alongside notifications |
| `cloudFiles.maxFilesPerTrigger` | Hard cap on files per micro-batch |
| `cloudFiles.maxBytesPerTrigger` | Soft cap on bytes per micro-batch |
| `checkpointLocation` (write side) | Standard Structured Streaming checkpoint, tracks processed files |
---

*Sources: Databricks and Microsoft Learn official Auto Loader documentation, including options reference, schema evolution, and type-widening pages.*
