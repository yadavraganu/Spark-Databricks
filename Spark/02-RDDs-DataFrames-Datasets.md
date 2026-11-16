# 02 — Spark Programming APIs: RDDs, DataFrames, Datasets, and PySpark

> **Audience:** beginner to senior data engineers. **Code baseline:** PySpark 4.3.0 style; most APIs shown are available in Spark 3.5.x, but check the version notes for newer functions. This guide focuses on how to write correct, maintainable Spark applications—not on internals covered in the architecture and Catalyst guides.
>
> **Related guides:** [Core architecture](01-Core-Architecture.md), [partitioning and shuffling](03-Partitioning-Shuffling.md), [joins](04-Joins.md), [Catalyst internals](07-Spark-SQL-Catalyst-Internals.md), [performance tuning](06-Performance-Tuning.md).

## Table of Contents

1. Choosing the Right API
2. SparkSession and a Reproducible Starting Point
3. RDD Fundamentals
4. DataFrame Fundamentals
5. Schemas, Rows, and Data Types
6. Everyday DataFrame Transformations
7. Aggregations and Grouping
8. Window Functions
9. Joins, Unions, and Deduplication
10. Nested and Complex Data
11. Nulls, Casting, and Data Quality
12. Reading and Writing Data
13. Working with SQL and Temporary Views
14. UDFs, Pandas UDFs, and Python Boundaries
15. JDBC and External Systems
16. Partitioning, Caching, and Execution Discipline
17. Structured Streaming API Primer
18. Common PySpark Patterns
19. Common Mistakes and Interview Traps
20. Practical Exercises
21. API Quick Reference

---

## 1. Choosing the Right API

Spark offers three related abstractions. They do not represent three independent execution engines.

| Abstraction | What it represents | Main strengths | Trade-offs / use cases |
|---|---|---|---|
| **RDD** (*Resilient Distributed Dataset*) | Immutable distributed collection of JVM objects or Python objects, partitioned and processed by tasks | Fine-grained control, custom transformations/partitioners, low-level APIs | Less declarative; fewer SQL optimizer benefits; Python object and serialization overhead can be substantial |
| **DataFrame** | Distributed table with named columns and a schema; in Scala/Java it is `Dataset[Row]` | Catalyst optimization, SQL expressions, column pruning, predicate pushdown, code generation, broad connectors | Preferred for most ETL, analytics, joins, and aggregations |
| **Dataset[T]** | JVM typed distributed collection with a schema-aware SQL execution plan and encoders | Compile-time type safety in Scala/Java while retaining SQL optimization | Available to Scala/Java, not to PySpark or PySpark users as a typed `Dataset[T]` API |

**Default decision:** use DataFrames and Spark SQL for almost all structured data engineering. Use RDDs when you need a low-level operation not reasonably expressible with DataFrame functions, custom partitioner behavior, or direct control over object-level processing. Use typed Datasets when Scala/Java compile-time type safety is valuable. Do not choose RDDs merely because they are older or because a task is “distributed.”

### 1.1 What does “DataFrame is a Dataset” mean?

In Scala, `DataFrame` is an alias for `Dataset[Row]`. A `Row` is a generic, schema-described record. A typed Dataset uses an encoder to convert between JVM objects such as case classes and Spark SQL's internal row representation. PySpark exposes DataFrames but does not expose Scala/Java's compile-time typed Dataset abstraction.

### 1.2 Common execution model

All three APIs are lazy with respect to distributed transformations. You build a lineage or logical plan; an action or write starts execution. The scheduler still divides work into jobs, stages, and tasks. Read [01-Core-Architecture.md](01-Core-Architecture.md) for the full lifecycle and [07-Spark-SQL-Catalyst-Internals.md](07-Spark-SQL-Catalyst-Internals.md) for query planning.

---

## 2. SparkSession and a Reproducible Starting Point

`SparkSession` is the normal entry point for DataFrame, SQL, catalog, and Structured Streaming work.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("customer-etl")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Application ID:", spark.sparkContext.applicationId)
print("Master:", spark.sparkContext.master)
```

- `getOrCreate()` reuses an existing session in the process where one is already active. Builder settings that must be applied before the `SparkContext` starts should be supplied at submission/startup time.
- `spark.conf.set()` changes SQL/session configuration where that setting is modifiable at runtime. It cannot resize executors that are already running.
- `spark.newSession()` creates a separate SQL session state while sharing the same underlying `SparkContext` and application resources.
- In notebooks and managed services, a session may already be created for you. Avoid stopping a shared session unless you own its lifecycle.

### 2.1 Clean shutdown

```python
try:
    # Build transformations and write outputs here.
    pass
finally:
    spark.stop()  # use only when this application/session owns the context
```

For notebook environments, do not call `stop()` at the end of every cell; the interactive environment may manage the session lifecycle.

---

## 3. RDD Fundamentals

An RDD is a fault-tolerant, immutable, partitioned collection. Its resilience comes primarily from lineage: Spark can recompute lost partitions from their parent RDDs, subject to source availability and deterministic transformations.

### 3.1 Creating RDDs

```python
sc = spark.sparkContext

numbers = sc.parallelize([1, 2, 3, 4, 5, 6], numSlices=3)
lines = sc.textFile("s3a://company-raw/logs/*.txt")
```

`parallelize()` is useful for small examples and test values. It is usually not appropriate to load a large production dataset into a Python list on the driver and then call `parallelize`; read distributed storage directly.

### 3.2 Transformations and actions

**Transformations** return a new RDD and are lazy. **Actions** trigger a job and return a result to the driver or write data externally.

| Transformation | Meaning | Example |
|---|---|---|
| `map(f)` | One output per input record | `rdd.map(lambda x: x * 2)` |
| `flatMap(f)` | Zero or more outputs per input | `lines.flatMap(lambda x: x.split())` |
| `filter(f)` | Keep records matching a predicate | `rdd.filter(lambda x: x > 0)` |
| `distinct()` | Remove duplicate values; requires redistribution in general | `rdd.distinct()` |
| `union(other)` | Concatenate two RDD lineages | `rdd1.union(rdd2)` |
| `mapPartitions(f)` | Invoke a function once per partition | `rdd.mapPartitions(parse_partition)` |
| `sortBy(f)` | Sort records by a derived key | `rdd.sortBy(lambda x: x[0])` |

| Action | Meaning | Example |
|---|---|---|
| `count()` | Count all records | `rdd.count()` |
| `first()` / `take(n)` | Return a limited result to driver | `rdd.take(10)` |
| `collect()` | Bring every record to driver | Avoid on large or unbounded data |
| `reduce(f)` | Aggregate records using an associative operation | `rdd.reduce(lambda a, b: a + b)` |
| `foreach(f)` | Execute an external side effect on records | Use only with carefully managed distributed clients and idempotency |
| `saveAsTextFile(path)` | Write text output | `rdd.saveAsTextFile(output_path)` |

**Never use `collect()` as a general way to inspect production-scale data.** It materializes the result in driver memory. Prefer `take(n)`, filtered samples, aggregate metrics, or a bounded write.

### 3.3 Key-value RDDs

A pair RDD contains `(key, value)` records. It exposes operations designed for distributed aggregation and joins.

```python
pairs = sc.parallelize([
    ("apples", 2),
    ("oranges", 5),
    ("apples", 3),
    ("bananas", 4),
])

# Map-side combine is possible; usually preferable to groupByKey for aggregation.
totals = pairs.reduceByKey(lambda left, right: left + right)
print(totals.collect())
```

Important pair-RDD functions:

- `reduceByKey(func)`: combine values for each key using an associative and commutative reducer. Spark can perform map-side combining.
- `aggregateByKey(zeroValue, seqFunc, combFunc)`: use one function to add a value to a per-partition accumulator and another to merge accumulators across partitions.
- `combineByKey(createCombiner, mergeValue, mergeCombiners)`: most general primitive for custom aggregation.
- `groupByKey()`: gathers all values for a key; usually avoid for aggregation because it moves raw values rather than reducing them early.
- `join(other)`, `leftOuterJoin(other)`, `subtractByKey(other)`: pair-RDD joins and set-like operations; they generally involve shuffles unless partitioning can be reused.
- `sortByKey()` and `partitionBy()`: ordering and partitioner control.

### 3.4 `map` versus `flatMap` versus `mapPartitions`

```python
import json

class JsonLineParser:
    def parse(self, line):
        return json.loads(line)

def build_parser():
    # In production, initialize a format-specific parser/client here.
    return JsonLineParser()

words_per_line = lines.map(lambda line: len(line.split()))
all_words = lines.flatMap(lambda line: line.split())

def parse_partition(iterator):
    # Create the parser/client once per partition rather than once per row.
    parser = build_parser()
    for line in iterator:
        record = parser.parse(line)
        if record is not None:
            yield record

parsed = lines.mapPartitions(parse_partition)
```

`mapPartitions` can amortize setup work (for example, loading a model or opening a database connection) across records in a partition. It does **not** mean “one function call for the whole cluster.” The function runs independently on each task attempt; retries and speculation can repeat its side effects. Close external resources reliably and design writes to be idempotent.

### 3.5 `groupByKey` versus `reduceByKey`

Use `reduceByKey`/`aggregateByKey` when you need a reduction. They can combine records locally before sending data over the network. Use `groupByKey` only when the business operation genuinely requires all values for a key and the resulting group fits the memory and processing model. A hot key can still make one reduce partition huge even with combiners.

---

## 4. DataFrame Fundamentals

A DataFrame is a distributed collection of rows with named columns and an explicit or inferred schema. It is not a local pandas DataFrame: transformations build a Spark plan and execute across the cluster only when an action or write runs.

### 4.1 Create DataFrames

```python
from pyspark.sql import Row

rows = [
    Row(customer_id=1, country="IN", amount=125.0),
    Row(customer_id=2, country="US", amount=80.0),
]
small_df = spark.createDataFrame(rows)

# Explicit schema is preferable for production ingestion.
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType
)

schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("country", StringType(), nullable=True),
    StructField("amount", DecimalType(12, 2), nullable=True),
])
```

Inferred schemas are convenient for exploration but can be ambiguous or inconsistent across files and batches. Use explicit schemas at stable ingestion boundaries, especially for streaming and long-lived pipelines.

### 4.2 Inspect without collecting everything

```python
df.printSchema()
df.show(20, truncate=False)
df.limit(20).show()
print(df.columns)
print(df.dtypes)
df.explain("formatted")
```

`show()` is an action and scans enough partitions to produce the requested rows; it is not a free metadata call. `explain()` prints plans and does not normally execute the full query.

### 4.3 Column expressions

Use `pyspark.sql.functions` and `Column` expressions rather than Python loops over rows.

```python
from pyspark.sql import functions as F

result = df.select(
    F.col("customer_id"),
    F.upper(F.trim(F.col("country"))).alias("country_code"),
    (F.col("amount") * F.lit(1.18)).alias("amount_with_tax"),
    F.current_date().alias("process_date"),
)
```

A `Column` is a symbolic expression in a Spark plan, not a local Python value. `F.col("amount") > 100` constructs a filter expression; it does not evaluate the predicate in Python.

---

## 5. Schemas, Rows, and Data Types

Spark SQL types include Boolean, byte/short/int/long, float/double, decimal, string, binary, date, timestamp, arrays, maps, and structs. Schemas are part of the contract between ingestion, transformations, storage, and consumers.

### 5.1 Explicit schema example

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DecimalType, TimestampType, BooleanType
)

orders_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("amount", DecimalType(18, 2), nullable=True),
    StructField("event_time", TimestampType(), nullable=True),
    StructField("is_refund", BooleanType(), nullable=True),
])
```

A `nullable=False` declaration is schema metadata, not a universal guarantee that every data source or transformation will reject every null at runtime. Enforce critical constraints explicitly with validation and quality checks.

### 5.2 Row access

```python
row = df.select("customer_id", "country").first()
if row is not None:
    print(row["customer_id"])
    print(row.country)
```

Rows collected to Python are local objects. Avoid converting large DataFrames to lists of `Row` objects.

### 5.3 Type coercion and decimals

- Use explicit casts where the data contract requires a specific type.
- Prefer `DecimalType(precision, scale)` for exact decimal arithmetic such as money; floating-point types can introduce rounding artifacts.
- Spark 4 enables ANSI SQL behavior by default. Invalid casts and arithmetic overflow can raise errors where older releases might have returned NULL or wrapped behavior. Check `spark.sql.ansi.enabled` and migration guidance before moving jobs across major versions.
- Date/time parsing depends on format and session time zone. Set and document `spark.sql.session.timeZone` for pipelines with timestamp semantics.

---

## 6. Everyday DataFrame Transformations

### 6.1 Select, rename, add, and drop columns

```python
clean = (
    df.select("order_id", "customer_id", "amount", "event_time")
      .withColumnRenamed("customer_id", "customer_key")
      .withColumn("amount_usd", F.col("amount").cast("decimal(18,2)"))
      .drop("amount")
)
```

`withColumn()` returns a new DataFrame; it does not mutate the original. Avoid building hundreds of deeply nested `withColumn()` calls in loops. Prefer a single `select()` with all expressions when practical, because repeated projections can create large logical plans.

### 6.2 Filter and conditional logic

```python
filtered = df.filter(
    (F.col("amount") > 0) &
    F.col("country").isin("IN", "US") &
    F.col("order_id").isNotNull()
)

classified = df.withColumn(
    "amount_band",
    F.when(F.col("amount") >= 1000, "high")
     .when(F.col("amount") >= 100, "medium")
     .otherwise("low")
)
```

Use `&`, `|`, and `~` for boolean combinations of Spark `Column` expressions, with parentheses around each comparison. Python `and`/`or` attempts to evaluate a `Column` locally and is invalid.

### 6.3 Sort and limit

```python
recent = df.orderBy(F.col("event_time").desc()).limit(100)
```

A global `orderBy()` can require a distributed sort and shuffle. If you only need a sorted result within each partition, use `sortWithinPartitions()`. Do not rely on output ordering unless the final query explicitly specifies it.

### 6.4 Rename safely and avoid duplicate names

```python
selected = df.select(
    F.col("id").alias("customer_id"),
    F.col("id").alias("source_customer_id"),
)
```

Prefer explicit projections after joins and unions. Ambiguous column names can make later expressions fail or, worse, be resolved differently than intended.

---

## 7. Aggregations and Grouping

```python
summary = (
    df.groupBy("country")
      .agg(
          F.count("*").alias("order_count"),
          F.countDistinct("customer_id").alias("unique_customers"),
          F.sum("amount").alias("gross_amount"),
          F.avg("amount").alias("avg_amount"),
          F.max("event_time").alias("last_order_time"),
      )
)
```

Common aggregate functions: `count`, `countDistinct`, `sum`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, and `collect_set`. `collect_list`/`collect_set` can create very large per-key results; use only when bounded and necessary.

### 7.1 Global aggregate

```python
overall = df.agg(
    F.count("*").alias("rows"),
    F.sum("amount").alias("total_amount"),
)
```

### 7.2 Grouping sets, rollup, cube

```python
rollup_summary = (
    df.rollup("country", "channel")
      .agg(F.sum("amount").alias("sales"))
)
```

`rollup` computes hierarchical subtotals; `cube` computes combinations of subtotals; `groupingSets` allows explicit grouping sets in SQL. These can increase output cardinality substantially.

### 7.3 Partial aggregation and shuffles

Most distributed aggregations require data with the same grouping key to be co-located. Spark can perform partial aggregation locally, then shuffle partial results, then finish aggregation. The exact plan depends on the operation and physical strategy. See [partitioning and shuffle](03-Partitioning-Shuffling.md).

---

## 8. Window Functions

Window functions calculate values across a related set of rows without collapsing the original rows, unlike `groupBy()`.

```python
from pyspark.sql import Window

by_customer = Window.partitionBy("customer_id").orderBy(F.col("event_time"))

with_metrics = (
    df.withColumn("row_num", F.row_number().over(by_customer))
      .withColumn("previous_amount", F.lag("amount").over(by_customer))
      .withColumn("running_amount", F.sum("amount").over(
          by_customer.rowsBetween(Window.unboundedPreceding, Window.currentRow)
      ))
)
```

Common functions:

| Function | Purpose |
|---|---|
| `row_number()` | Unique sequential number within the window order |
| `rank()` / `dense_rank()` | Ranking with gaps / without gaps for ties |
| `lag()` / `lead()` | Access prior / next row values |
| `sum()` / `avg()` over a window | Running or moving aggregate |
| `first_value()` / `last_value()` | First/last value under the frame semantics |

**Senior-level concerns:** always define deterministic tie-breakers for `row_number`; choose `rowsBetween` vs `rangeBetween` intentionally; large or skewed window partitions can be expensive; a window with `partitionBy` and `orderBy` typically requires shuffle and sort unless compatible partitioning/order already exists.

---

## 9. Joins, Unions, and Deduplication

### 9.1 Join

```python
orders = df_orders.alias("o")
customers = df_customers.alias("c")

joined = (
    orders.join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "left")
          .select(
              F.col("o.order_id"),
              F.col("o.amount"),
              F.col("c.segment").alias("customer_segment"),
          )
)
```

Join types: `inner`, `left`/`left_outer`, `right`/`right_outer`, `full`/`outer`, `left_semi`, and `left_anti`. Use `crossJoin()` only when a Cartesian product is intended. Understand NULL semantics, duplicates, cardinality, and physical strategies before optimizing; see [04-Joins.md](04-Joins.md).

### 9.2 Union

```python
combined = df_a.unionByName(df_b, allowMissingColumns=True)
```

`union()` matches columns by position; `unionByName()` matches by name. Use `unionByName` for datasets whose column order may differ. Validate types and schema drift; `allowMissingColumns=True` fills absent columns with NULL and should be part of an intentional schema-evolution policy.

### 9.3 Deduplication

```python
unique_events = df.dropDuplicates(["event_id"])
```

For batch data, `dropDuplicates` removes duplicate keys but does not promise which non-key values survive when duplicates differ. Define a deterministic survivor rule with a window if “latest record wins” is required:

```python
latest_window = Window.partitionBy("event_id").orderBy(
    F.col("updated_at").desc(), F.col("ingest_id").desc()
)
latest = (
    df.withColumn("rn", F.row_number().over(latest_window))
      .filter(F.col("rn") == 1)
      .drop("rn")
)
```

In streaming, deduplication is stateful; see the Structured Streaming guide for watermark and state-retention semantics.

---

## 10. Nested and Complex Data

Nested fields appear frequently in JSON, event payloads, and semi-structured sources.

### 10.1 Structs

```python
flat = raw.select(
    F.col("payload.customer.id").alias("customer_id"),
    F.col("payload.customer.country").alias("country"),
)
```

### 10.2 Arrays and explode

```python
items = orders.select(
    "order_id",
    F.explode("items").alias("item"),
)

item_fields = items.select(
    "order_id",
    F.col("item.sku").alias("sku"),
    F.col("item.quantity").alias("quantity"),
)
```

`explode()` creates one output row per array element; `explode_outer()` preserves rows with NULL/empty arrays by emitting a NULL element. Array functions include `transform`, `filter`, `exists`, `aggregate`, `flatten`, `array_distinct`, and `arrays_zip`.

### 10.3 Maps

Use `map_keys`, `map_values`, `element_at`, `map_from_arrays`, `map_entries`, and higher-order functions when available. Avoid converting complex columns to Python objects unless there is a compelling reason.

---

## 11. Nulls, Casting, and Data Quality

SQL uses three-valued logic: a predicate can be TRUE, FALSE, or UNKNOWN (NULL). A filter retains rows only when its predicate is TRUE.

```python
checked = df.select(
    F.col("amount").isNull().alias("amount_is_null"),
    F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_filled"),
    F.nanvl(F.col("metric"), F.lit(None)).alias("metric_without_nan"),
)
```

Key functions: `isNull`, `isNotNull`, `coalesce`, `when`/`otherwise`, `nullif` (SQL), `nanvl`, `isnan`, and `eqNullSafe`.

### 11.1 Validate before writing

```python
invalid = df.filter(
    F.col("order_id").isNull() |
    (F.col("amount") < 0) |
    F.col("event_time").isNull()
)

valid = df.join(invalid.select("order_id").distinct(), "order_id", "left_anti")
```

For real pipelines, define a stable business key, handle duplicate IDs carefully, and avoid classifying a NULL amount as zero unless that is the business contract. Common quality metrics include row counts, null rates, distinct key counts, min/max event time, invalid-value counts, and reconciliation totals.

### 11.2 ANSI behavior and malformed data

Spark 4.0+ enables ANSI SQL mode by default. Invalid casts, numeric overflow, division by zero, and other invalid operations can fail the query rather than silently produce a permissive result. Do not disable ANSI just to hide errors without deciding the desired data-quality behavior. Use safe parsing/try-style expressions where supported by the release, or parse into a quarantine path.

---

## 12. Reading and Writing Data

### 12.1 DataFrameReader

```python
orders = (
    spark.read
    .format("parquet")
    .option("mergeSchema", "false")
    .load("s3a://company-lake/silver/orders/")
)

csv_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(orders_schema)
    .csv("s3a://company-landing/orders/")
)
```

Prefer Parquet or another columnar format for analytic tables. Use explicit schemas and controlled schema evolution for long-lived datasets. CSV/JSON schema inference adds cost and can produce inconsistent types.

### 12.2 Read options that matter

| Option / setting | Purpose | Caution |
|---|---|---|
| `header` | Treat first CSV line as column names | Does not validate schema quality |
| `delimiter` / `quote` / `escape` | Control CSV parsing | Match the producer's exact serialization rules |
| `mode=PERMISSIVE/FAILFAST/DROPMALFORMED` | Malformed record policy for supported formats | Dropping malformed rows can hide data loss; capture corrupt records when appropriate |
| `recursiveFileLookup` | Read nested directories | Changes partition discovery behavior; do not enable blindly |
| `basePath` | Preserve partition-column discovery when reading subpaths | Ensure it points at the table root |
| `mergeSchema` | Merge compatible Parquet schemas | Can increase metadata/read planning cost; not a substitute for a schema contract |

### 12.3 DataFrameWriter and write modes

```python
(
    summary.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("business_date")
    .save("s3a://company-lake/gold/daily_summary/")
)
```

Common save modes are `append`, `overwrite`, `error`/`errorifexists`, and `ignore`. They describe path/table behavior; they do not all provide transactions or safe concurrent-writer coordination.

`partitionBy()` creates directory partitioning for file-based output; it is not the same as `repartition()` before a write. Number and size of output files depend on upstream task partitions, writer behavior, dynamic partition distribution, and commit protocol. See the shuffle guide for small-file management.

### 12.4 Table writes

For catalog-managed tables, prefer explicit table DDL/schema, controlled save modes, and a well-defined table format/catalog. Spark's built-in data-source API does not automatically provide ACID transactions for arbitrary Parquet directory writes. Delta Lake, Apache Iceberg, and Apache Hudi rely on their own Spark integrations, catalogs, and commit protocols; they are not interchangeable with a plain Parquet folder.

---

## 13. Working with SQL and Temporary Views

```python
df.createOrReplaceTempView("orders_view")

result = spark.sql("""
    SELECT country, COUNT(*) AS order_count, SUM(amount) AS revenue
    FROM orders_view
    WHERE event_time >= TIMESTAMP '2026-01-01 00:00:00'
    GROUP BY country
""")
```

- A local temporary view is scoped to the session. A global temporary view is scoped to the Spark application and normally referenced through `global_temp.view_name`.
- Views are logical definitions; a temporary view does not necessarily materialize/cache the rows.
- SQL and DataFrame expressions are front ends to the same SQL engine. Use whichever produces clearer maintainable logic, then inspect the plan when performance matters.
- Parameterize values safely when SQL is generated dynamically; do not concatenate untrusted strings into SQL statements.

---

## 14. UDFs, Pandas UDFs, and Python Boundaries

### 14.1 Prefer built-in functions

```python
# Preferred: optimizer-visible native expression
normalized = df.withColumn("email_norm", F.lower(F.trim("email")))
```

### 14.2 Scalar Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def normalize_code(value):
    if value is None:
        return None
    return value.strip().upper()

normalize_code_udf = udf(normalize_code, StringType())
result = df.withColumn("code_norm", normalize_code_udf("code"))
```

A regular Python UDF introduces a JVM↔Python boundary and row-oriented serialization/execution overhead. Catalyst cannot reason through arbitrary Python function internals, so predicate/column optimization may be constrained.

### 14.3 Pandas UDF

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def normalize_amount(values: pd.Series) -> pd.Series:
    return values.fillna(0.0) / 100.0
```

Pandas UDFs operate on batches using Apache Arrow, reducing per-row Python overhead. They still cross a language boundary, require Python worker memory, and may not beat native Spark expressions. Batch size and vectorized memory use matter.

### 14.4 Decision rule

1. Built-in Spark SQL/DataFrame function.
2. Higher-order function or SQL expression.
3. Pandas/Arrow UDF for vectorizable logic not expressible natively.
4. Scalar Python UDF only when simpler alternatives are unavailable and measured cost is acceptable.

Do not use a UDF to wrap a built-in expression. Do not access `spark`, `SparkSession`, or a DataFrame inside a UDF. UDF functions must be serializable and safe to run concurrently and again after task retries.

---

## 15. JDBC and External Systems

### 15.1 Parallel JDBC read

```python
jdbc_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://db.example:5432/sales")
    .option("dbtable", "public.orders")
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "100000000")
    .option("numPartitions", "16")
    .load()
)
```

`lowerBound` and `upperBound` determine stride for partitioning; they **do not filter** rows. Add a source query or predicate for filtering. The partition column must have a supported numeric/date/timestamp type. Too many partitions can overwhelm the database with concurrent queries; coordinate `numPartitions`, database connection limits, source load, and Spark slots.

### 15.2 JDBC write

The number of writer partitions can drive concurrent database connections. Control it intentionally; use a staging table and database-native merge/upsert transaction when the use case requires atomic replacement or deduplication. Spark's generic JDBC writer is not a distributed transaction coordinator for the target database.

### 15.3 Secrets and connections

Never hard-code credentials in source control, notebooks, logs, or Spark configs visible to all users. Use a secret manager or workload identity supported by the platform. Avoid opening a new DB connection for every row; with `foreachPartition`, acquire and close one connection per partition attempt, and make writes safe under retries/speculation.

---

## 16. Partitioning, Caching, and Execution Discipline

```python
# New hash partitioning by customer_id; this is a shuffle.
by_customer = df.repartition(128, "customer_id")

# Reduce partition count without a full shuffle; may retain imbalance.
fewer = by_customer.coalesce(32)

# Cache only when there is real reuse.
reused = df.filter(F.col("is_active") == True).cache()
reused.count()  # materializes cache
# Multiple downstream actions...
reused.unpersist()
```

Important differences:

- `repartition()` changes partition distribution using a shuffle; by-column DataFrame repartition typically hash-partitions by the expressions.
- `coalesce()` reduces the number of partitions without a full redistribution by default; it may create uneven partitions.
- `cache()` is lazy; the first action materializes blocks. `unpersist()` releases cached blocks, not the source data.
- AQE may coalesce eligible shuffle partitions at runtime; the number configured before execution is not always the final number of tasks.
- `partitionBy()` on write lays out output directories by column values; it is not a substitute for choosing a sensible number of writer tasks.
- Never assume DataFrame row order is stable without a final `orderBy()`.

---

## 17. Structured Streaming API Primer

Structured Streaming uses a streaming DataFrame and a `writeStream` sink. Many DataFrame transformations are shared with batch; actions such as `collect()` are not used to materialize an unbounded stream.

```python
from pyspark.sql import functions as F

stream = (
    spark.readStream
    .schema(orders_schema)
    .json("s3a://company-landing/orders/")
)

query = (
    stream.withWatermark("event_time", "20 minutes")
          .groupBy(F.window("event_time", "5 minutes"), "country")
          .count()
          .writeStream
          .outputMode("append")
          .format("parquet")
          .option("path", "s3a://company-lake/aggregates/orders_5m")
          .option("checkpointLocation", "s3a://company-checkpoints/orders_5m")
          .start()
)
```

Understand output modes, triggers, watermarks, state, replay, checkpoint compatibility, and sink guarantees before production. The full guide is [08-Structured-Streaming.md](08-Structured-Streaming.md).

---

## 18. Common PySpark Patterns

### 18.1 Incremental latest-record pipeline

```python
latest_per_key = (
    raw_df
    .filter(F.col("record_id").isNotNull())
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("record_id").orderBy(
                F.col("source_updated_at").desc(),
                F.col("ingest_sequence").desc(),
            )
        ),
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

The tie-breaker must be stable and business-defined. If the ordering columns can tie, add a deterministic secondary key.

### 18.2 Reconciliation counts

```python
checks = df.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("business_key").alias("unique_keys"),
    F.sum(F.col("amount")).alias("amount_total"),
    F.sum(F.col("business_key").isNull().cast("long")).alias("null_key_rows"),
)
```

For financial pipelines, reconcile using the appropriate decimal type and compare at the correct grain. For large data, avoid collecting full tables to the driver; small aggregate results are generally safe to collect.

---

## 19. Common Mistakes and Interview Traps

| Mistake | Why it is wrong | Better approach |
|---|---|---|
| Using Python `and`/`or` on `Column` expressions | Spark columns are symbolic expressions | Use `&`, `|`, `~` with parentheses |
| Calling `collect()` on a large DataFrame | Driver OOM / network bottleneck | Aggregate, sample, filter, or write distributed output |
| Using `groupByKey()` for sum/count | Shuffles raw values and can create large groups | `reduceByKey`, `aggregateByKey`, or DataFrame `groupBy().agg()` |
| Using UDF for `lower`, `trim`, date parsing, etc. | Blocks optimizer visibility and adds serialization | Use built-in functions |
| Assuming transformations execute immediately | Lazy evaluation | Trigger an action/write; inspect UI and plan |
| Assuming `dropDuplicates()` chooses latest row | It does not define a deterministic survivor | Use `row_number()` over deterministic ordering |
| Assuming `union()` aligns by column name | It aligns by position | Use `unionByName()` and validate schema |
| Assuming JDBC bounds are filters | Bounds calculate partition stride only | Add explicit source filtering |
| Assuming `cache()` makes all data permanently resident | Cache is lazy and evictable | Measure reuse, choose storage level, unpersist |
| Assuming output file count equals `repartition(n)` exactly | Dynamic partition values and writer/task behavior affect file count | Inspect output and tune file layout deliberately |
| Assuming Spark SQL DataFrame is typed Dataset in Python | Typed `Dataset[T]` is JVM API | Use explicit DataFrame schema and validation |
| Treating NULL as zero/empty string by default | Changes business semantics | Define NULL policy explicitly |

---

## 20. Practical Exercises

1. Read a folder of CSV files with an explicit schema; count malformed records and quarantine them.
2. Convert the valid rows to Parquet and compare scan bytes with CSV.
3. Implement the same word-count with an RDD and a DataFrame; inspect the plans and compare code complexity.
4. Aggregate orders by customer and month; verify decimal precision and null behavior.
5. Use `row_number`, `lag`, and a running total over a window.
6. Join fact and dimension data; test duplicate keys, NULL keys, missing dimensions, and join cardinality.
7. Explode a nested array of line items and compute revenue by SKU.
8. Read a JDBC table with parallel partitions; measure database load and confirm that bound values do not filter records.
9. Build a streaming file-source aggregation with a watermark; restart it from its checkpoint and inspect state metrics.
10. Replace a scalar Python UDF with built-in functions or a Pandas UDF and compare task duration and process memory.

---

## 21. API Quick Reference

| Need | DataFrame / SQL API | RDD API |
|---|---|---|
| Filter rows | `filter`, `where` | `filter` |
| Project columns | `select` | `map` |
| Add expression | `withColumn`, `select` | `map` |
| Group and aggregate | `groupBy().agg()` | `reduceByKey`, `aggregateByKey` |
| Join | `DataFrame.join()` | Pair-RDD `join()` |
| Remove duplicates | `dropDuplicates` | `distinct` |
| Repartition | `repartition`, `coalesce` | `repartition`, `coalesce`, `partitionBy` |
| Cache | `cache`, `persist`, `unpersist` | `cache`, `persist`, `unpersist` |
| Write files | `DataFrameWriter` | `saveAsTextFile` / output functions |
| Query SQL | `spark.sql()` | Not applicable directly |

**Interview summary:** DataFrames are the default for structured workloads because Spark can optimize declarative expressions. RDDs expose lower-level distributed collection operations. Scala/Java typed Datasets add compile-time type safety; PySpark uses DataFrames. The right API is the highest-level one that expresses the problem clearly and correctly.
