# Lakeflow Declarative Pipelines (formerly Delta Live Tables / DLT) — Deep Dive Guide

## 1. What It Is and the Name Change

**Delta Live Tables (DLT)** was renamed **Lakeflow Declarative Pipelines** (also called Lakeflow Spark Declarative Pipelines, or SDP) as part of folding it into Databricks' unified data-engineering product, Lakeflow. The rename is backward-compatible: existing DLT code keeps running unchanged, and the classic compute SKU still shows as `DLT` on your bill. Where things differ is mostly naming — and that naming change tracks a real architectural move: the framework is being aligned with **Apache Spark Declarative Pipelines**, an open-source declarative pipelines framework shipping in Apache Spark itself (from Spark 4.1 onward). Lakeflow Pipelines extends that open standard with Databricks-specific capabilities while staying interoperable with it.

**Core idea, unchanged by the rename:** you declare *what* each table should contain — not the step-by-step imperative logic to compute and refresh it — and the engine figures out execution order, incremental processing, retries, and orchestration between your declared tables automatically.

```python
# Old (still works)
import dlt

@dlt.table
def bronze_orders():
    ...
```

```python
# New preferred style
from pyspark import pipelines as dp

@dp.table
def bronze_orders():
    ...
```

### API name mapping (old → new)
| DLT (old) | Lakeflow Declarative Pipelines (new) |
|---|---|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table` (used for both tables and MVs) | `@dp.table` → now specifically creates a **streaming table**; `@dp.materialized_view` → creates a **materialized view** |
| `@dlt.view` | `@dp.temporary_view` |
| `APPLY CHANGES INTO` | `AUTO CDC INTO` (same syntax, new recommended name) |
| `CREATE OR REFRESH LIVE TABLE` (SQL) | `CREATE OR REFRESH MATERIALIZED VIEW` / `CREATE OR REFRESH STREAMING TABLE` |
| `LIVE.table_name` schema prefix | Not needed — reference tables directly by name |

The old names aren't removed — they're simply what Databricks now calls "legacy" naming, kept for backward compatibility. New pipelines should use the `dp` names.

---

## 2. The Three Dataset Types

| | Streaming Table | Materialized View | View |
|---|---|---|---|
| **Refresh behavior** | Incremental — processes only new/changed records | Auto-selected: incremental where possible, full recompute otherwise | Recomputed on every query, no storage |
| **Storage** | Persisted Delta table | Persisted Delta table | Not persisted at all |
| **Best for** | Bronze ingestion, append-only or CDC sources | Silver/Gold aggregations, joins, reporting | Reusable intermediate logic you don't want materialized |
| **Python decorator** | `@dp.table` | `@dp.materialized_view` | `@dp.temporary_view` |
| **SQL** | `CREATE OR REFRESH STREAMING TABLE` | `CREATE OR REFRESH MATERIALIZED VIEW` | plain `CREATE VIEW` |

```python
from pyspark import pipelines as dp

# Streaming table — incremental ingestion
@dp.table
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://landing/orders/"))

# Materialized view — recomputed/refreshed as a managed aggregate
@dp.materialized_view
def gold_daily_revenue():
    return spark.read.table("silver_orders").groupBy("order_date").sum("amount")

# Temporary view — not persisted, just reused inside the pipeline
@dp.temporary_view
def valid_orders():
    return spark.read.table("bronze_orders").filter("amount > 0")
```

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * FROM STREAM read_files('s3://landing/orders/', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT order_date, SUM(amount) AS revenue
FROM silver_orders
GROUP BY order_date;
```

Use the `PRIVATE` clause (SQL) or equivalent Python option to define an intermediate dataset that other datasets in the pipeline can reference, but that isn't published/queryable outside the pipeline — useful for staging logic you don't want cluttering the catalog.

---

## 3. How Datasets Connect

Unlike legacy DLT, which required a `LIVE.` schema prefix to reference another dataset defined in the same pipeline, current pipelines just reference tables **by their normal name** — the pipeline engine resolves the dependency graph from the query itself (the tables your query reads from become upstream dependencies automatically). The `LIVE` schema syntax still parses in legacy pipelines but is silently ignored in new ones.

```python
@dp.table
def silver_orders():
    return spark.read.table("bronze_orders").filter("status != 'cancelled'")   # just the table name

@dp.materialized_view
def gold_daily_revenue():
    return spark.read.table("silver_orders").groupBy("order_date").sum("amount")
```

This reference graph is what the pipeline engine uses to build its DAG, decide execution order, and figure out what needs to be recomputed when something upstream changes.

---

## 4. Data Quality: Expectations

Expectations declare data quality rules directly on a dataset — validated per-record as data flows through.

```python
@dp.table
@dp.expect("valid_amount", "amount > 0")
def bronze_orders():
    ...
```

Six decorator variants control what happens on a violation:

| Decorator | On violation |
|---|---|
| `@dp.expect(name, constraint)` | Row is kept; violation is logged/counted |
| `@dp.expect_or_drop(name, constraint)` | Row is dropped before writing |
| `@dp.expect_or_fail(name, constraint)` | The flow stops immediately (fails just that flow, not the whole pipeline) |
| `@dp.expect_all(dict)` | Same as `expect`, for multiple named constraints at once |
| `@dp.expect_all_or_drop(dict)` | Same as `expect_or_drop`, for multiple constraints |
| `@dp.expect_all_or_fail(dict)` | Same as `expect_or_fail`, for multiple constraints |

```python
valid_pages = {
    "valid_page_id": "page_id IS NOT NULL",
    "valid_timestamp": "event_time > '2020-01-01'"
}

@dp.table
@dp.expect_all(valid_pages)
def bronze_events():
    ...
```

SQL equivalent:
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
(CONSTRAINT valid_amount EXPECT (amount > 0))
AS SELECT * FROM STREAM read_files(...);
```

Expectation results are tracked as pipeline metrics (pass/fail counts per constraint) and visible in the **Data Quality** tab of the pipeline UI, or queryable directly from the pipeline's event log — useful for monitoring data quality trends over time, not just catching the current run's bad records. For reusable rule sets, teams often centralize expectation definitions in a Unity Catalog table rather than hardcoding them per pipeline, so quality rules are governed and versioned separately from pipeline logic.

Only streaming tables, materialized views, and temporary views support expectations — plain batch reads outside those decorators don't.

---

## 5. Change Data Capture: AUTO CDC

`AUTO CDC` (the renamed, recommended successor to `APPLY CHANGES INTO` — same syntax, still works under the old name) automates the otherwise-fiddly logic of applying a change feed to a target table, handling out-of-order events, deletes, and SCD history for you.

```sql
CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW apply_cdc AS AUTO CDC INTO customers_scd2
FROM stream(customers_cdc_feed)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
[IGNORE NULL UPDATES]
SEQUENCE BY sequence_num
STORED AS SCD TYPE 2;
```

- `FROM stream(source_table)` — wraps the source in `stream(...)` to read it with streaming semantics; omit the wrapper to read it as a static/batch source instead.
- `IGNORE NULL UPDATES` (optional) — when a CDC event has a `NULL` in some columns, keep the target row's existing values for those columns instead of overwriting them with `NULL`.

```python
import dlt  # or: from pyspark import pipelines as dp

dp.create_streaming_table("customers_scd2")

dp.create_auto_cdc_flow(
    target="customers_scd2",
    source="customers_cdc_feed",
    keys=["customer_id"],
    sequence_by="sequence_num",
    apply_as_deletes="operation = 'DELETE'",
    stored_as_scd_type=2
)
```

- **`STORED AS SCD TYPE 1`** (default) — keeps only the latest version of each record, updating in place.
- **`STORED AS SCD TYPE 2`** — keeps full history; the target table needs `__START_AT`/`__END_AT` columns (matching the `sequence_by` column's type) to track each version's validity window.
- **`APPLY AS TRUNCATE WHEN`** — treats a matching event as a full-table truncate; supported for SCD Type 1 only.
- **Bitemporal AUTO CDC** (`STORED AS BITEMPORAL`, newer/beta) — tracks changes across *both* business time and system time, for cases where you need to know not just what a record's value was, but when you *learned* it was that value.

**`AUTO CDC FROM SNAPSHOT`** handles the case where you don't have a true CDC feed, only periodic full snapshots of a source table — it diffs successive in-order snapshots to derive the changes, rather than requiring the source to emit a change stream itself.

**Gotcha:** if Auto Loader is the source feeding your CDC flow, be aware it does **not** guarantee file processing order — make sure your `sequence_by` column reflects the true event order in the data itself, rather than relying on files being picked up in the order they landed.

---

## 6. Pipeline Configuration

A pipeline is configured (via UI, JSON, or a `pipeline.yml` in a bundle) with settings like:

```yaml
resources:
  pipelines:
    orders_pipeline:
      name: orders-pipeline
      catalog: production
      target: silver          # default schema for datasets without an explicit one
      serverless: true         # or configure clusters explicitly
      continuous: false        # triggered vs. continuous execution
      libraries:
        - notebook:
            path: ./pipelines/orders_pipeline.py
      configuration:
        pipelines.cdc.tombstoneGCThresholdInSeconds: "86400"
```

### Execution modes
- **Triggered** — runs once, processes everything new since the last run, then stops. Good fit for scheduled batch-style freshness (hourly, daily).
- **Continuous** — stays running, processing new data with low latency as it arrives. Costs more (compute stays up) but minimizes end-to-end latency.

### Compute
Pipelines can run on **serverless** compute (Databricks manages cluster sizing/scaling for you) or on **classic** pipeline clusters you configure explicitly (node types, autoscaling bounds, instance pools). Serverless is the simpler default for most new pipelines; classic clusters remain useful when you need specific instance types, init scripts, or tighter cost/performance tuning control.

---

## 7. Control Flow (Conditional Pipeline Logic)

Newer Lakeflow Declarative Pipelines support a **`control_flow`** block for conditional logic *inside* the declarative pipeline definition itself — distinct from (and at a different layer than) the `If/else`, `Run if`, and `For each` task types available for orchestration in **Lakeflow Jobs**. Jobs-level control flow decides which *tasks* run; pipeline-level `control_flow` decides which *flows within a pipeline* run, based on conditions evaluated as the pipeline executes.

Think of it as two separate layers:
- **Lakeflow Jobs** — orchestrates across tasks (which can include whole pipeline runs, notebooks, SQL, etc.), with its own branching/looping.
- **Lakeflow Declarative Pipelines `control_flow`** — branching *within* a single pipeline's declared flows.

---

## 8. Monitoring and the Event Log

Every pipeline run produces a structured **event log** (itself a queryable Delta table) capturing:
- Dataset-level lineage and dependency graph
- Expectation pass/fail counts per run
- Flow-level progress, latency, and errors
- Cluster and resource utilization

```sql
SELECT * FROM event_log(TABLE(production.silver.orders))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

This is the mechanism behind the pipeline UI's **Data Quality** tab and lineage graph — both are just views over this event log, so anything the UI shows can also be queried directly for custom dashboards or alerting.

---

## 9. Lakeflow Pipelines vs. Lakeflow Jobs

A common point of confusion: both orchestrate data work, but at different levels of abstraction.

| | Lakeflow Declarative Pipelines | Lakeflow Jobs |
|---|---|---|
| **Paradigm** | Declarative — define *what* tables should contain | Imperative/orchestration — define *what tasks run, in what order* |
| **Best for** | ETL with clear table lineage: Bronze → Silver → Gold | Arbitrary task orchestration: notebooks, SQL, ML training, pipeline *runs*, dbt, etc. |
| **Dependency resolution** | Automatic — inferred from which tables each query reads | Manual — you declare task dependencies explicitly |
| **Typical relationship** | A pipeline is often *one task* inside a larger job | A job can trigger one or more pipelines as part of a broader workflow |

Most real-world setups use both: Lakeflow Jobs orchestrates the overall workflow (maybe including non-pipeline steps like a dbt run or a model training task), with one or more Lakeflow Declarative Pipelines doing the actual Bronze→Silver→Gold transformation work as a task within that job.

---

## 10. Common Production Pattern

```python
from pyspark import pipelines as dp

# Bronze — streaming ingestion via Auto Loader, permissive schema
@dp.table
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("s3://landing/orders/"))

# Silver — cleaned, quality-checked, deduplicated
@dp.table
@dp.expect_or_drop("valid_order", "order_id IS NOT NULL AND amount > 0")
def silver_orders():
    return spark.read.table("bronze_orders").dropDuplicates(["order_id"])

# Gold — aggregated for reporting
@dp.materialized_view
def gold_daily_revenue():
    return (spark.read.table("silver_orders")
        .groupBy("order_date")
        .sum("amount"))
```

This is the medallion architecture expressed directly as a dependency graph — Bronze feeds Silver feeds Gold, with the pipeline engine inferring and executing that order automatically, and expectations enforcing quality at the Silver boundary before bad data ever reaches Gold.

---

## 11. Quick Reference

| Concept | Syntax |
|---|---|
| Streaming table (Python) | `@dp.table` |
| Materialized view (Python) | `@dp.materialized_view` |
| Temporary view (Python) | `@dp.temporary_view` |
| Streaming table (SQL) | `CREATE OR REFRESH STREAMING TABLE` |
| Materialized view (SQL) | `CREATE OR REFRESH MATERIALIZED VIEW` |
| Single expectation | `@dp.expect` / `@dp.expect_or_drop` / `@dp.expect_or_fail` |
| Multiple expectations | `@dp.expect_all` / `@dp.expect_all_or_drop` / `@dp.expect_all_or_fail` |
| CDC ingestion | `AUTO CDC INTO` / `AUTO CDC FROM SNAPSHOT` |
| SCD history | `STORED AS SCD TYPE 1 / 2 / BITEMPORAL` |
| Hide intermediate dataset | `PRIVATE` clause |
| Pipeline-internal branching | `control_flow` block |
| Monitoring | `event_log(TABLE(...))` |

---

*Sources: Databricks and Microsoft Learn official Lakeflow Declarative Pipelines documentation, including the DLT-to-SDP rename notice, AUTO CDC reference, and pipeline expectations reference.*
