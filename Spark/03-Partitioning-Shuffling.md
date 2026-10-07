# Spark Partitioning & Shuffling — Complete Guide

---

## 1. Why This Topic Sits at the Center of Spark Performance

Partitioning determines how data is split across a cluster; shuffling is what happens when that split has to change mid-job. Nearly every expensive thing that can happen in a Spark application — network-bound stages, disk spill, data skew, OOMs, small-file problems on write — traces back to how data was partitioned and when it had to be reshuffled. Unlike most tuning levers, which optimize one stage, getting partitioning right changes the shape of the *entire* job, because partition count and distribution propagate forward through every subsequent transformation until the next shuffle resets them.

---

## 2. What a Partition Actually Is

- A **partition** is a chunk of a distributed dataset (RDD/DataFrame) that lives on one executor and is processed by one task at a time.
- Partitions are the unit of **parallelism** — the number of partitions roughly caps the number of tasks that can run concurrently for a given stage (bounded further by available executor cores).
- Every partition's data, when an RDD is created from an external source, is typically determined by that source's own splitting logic — e.g., HDFS/S3 file splits, Parquet row-group boundaries, or a JDBC query's partitioning column.
- A partition is not inherently tied to a physical machine permanently — it's a logical division that Spark schedules tasks against; a given partition could be processed on different executors across retries or re-execution.

---

## 3. How Initial Partition Count Is Determined

**Reading files (the common case):**
- For file-based sources (CSV, JSON, Parquet, ORC, text), Spark computes partitions based on total input size divided by `spark.sql.files.maxPartitionBytes` (default 128MB) — roughly, one partition per 128MB of input, subject to file-splittability (a single unsplittable compressed file, e.g., gzip, cannot be divided across partitions no matter how large it is — this is a common, easy-to-miss source of a single oversized partition).
- `spark.sql.files.openCostInBytes` (default 4MB) factors in the overhead of opening each file, so many small files get grouped together into fewer, larger partitions when their combined size is still below the target — this is Spark's built-in mitigation for the small-file problem on read, though it doesn't eliminate the problem on write (see §9).
- `spark.sql.files.maxPartitionBytes` sets a target *ceiling* on partition size; its counterpart, `spark.sql.files.minPartitionNum`, sets a **floor** on the number of partitions produced (defaulting to `spark.default.parallelism`) — relevant for the opposite failure mode from §3.1: a small-ish but splittable file that would otherwise produce too few partitions to use available parallelism well.
- **Object storage (S3, ADLS, GCS) has its own wrinkle worth knowing**, distinct from HDFS: these systems have no native "block" concept the way HDFS does, so Spark's partitioning math is based purely on byte ranges within objects rather than aligning to physical storage blocks — splittability (§3.1) still applies identically, but **listing** a large number of files/objects before any partitioning decision can even be made is itself a real, sometimes underestimated cost on object storage, since it goes through a metadata API rather than a fast local filesystem listing. A directory with a very large number of files can make the driver-side listing step itself a noticeable part of total job startup time, independent of how well the eventual partitioning turns out.

**Reading from JDBC:**
- Without explicit partitioning options, a JDBC read pulls through a **single partition/connection** — a serious bottleneck for large tables. Specifying `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` lets Spark issue parallel queries, each covering a range of the partition column, producing one partition per range.
- An alternative to the bound-based approach is the `predicates` parameter — an explicit list of `WHERE`-clause fragments, one per desired partition, giving direct control over exactly how the data is divided (useful when the partition column isn't a clean numeric range, or when a more deliberate, non-uniform split is wanted).

**Parallelized collections:**
- `sc.parallelize(data, numSlices)` — partition count is explicit, defaulting to `spark.default.parallelism` if not specified (which itself defaults to total available cores across the cluster for most cluster managers).

**After a shuffle:**
- Partition count resets to whatever the shuffle operation specifies — for DataFrame/SQL operations, this is governed by `spark.sql.shuffle.partitions` (default 200), **regardless of the input data's actual size** — a flat default that is very often wrong for both small jobs (200 partitions for a 10MB dataset is wasteful overhead) and large jobs (200 partitions for a 500GB shuffle means every partition is ~2.5GB, likely to spill or OOM).

### 3.1 File Splittability and Compression — Why a "Big" File Can Still Be One Partition

The one-line warning above (`§3`, "a single unsplittable compressed file cannot be divided") is worth unpacking fully, because it's a silent, easy-to-miss cause of a single task becoming a bottleneck for an entire job, with no error raised anywhere — the job just runs, slowly, with one task doing far more work than the rest.

**What "splittable" actually means:** whether Spark (via Hadoop's `InputFormat` machinery underneath) can start reading a file from an arbitrary **byte offset** in the middle of it and still correctly identify where a logical record begins, without needing to have already decoded everything before that offset. If it can, the file can be divided into multiple partitions, each starting at a different offset, processed by different tasks in parallel. If it can't, the entire file must be handed to a **single task**, no matter how large it is or what `spark.sql.files.maxPartitionBytes` is set to — that config only influences splitting *within* files that are actually splittable; it has no power to split an unsplittable one.

**Plain/row-oriented formats (CSV, JSON, text) — splittability depends entirely on compression:**

| Compression | Splittable? | Why |
|---|---|---|
| None (plain `.csv`, `.json`, `.txt`) | Yes | Any byte offset can be scanned forward to the next line/record boundary. |
| **gzip** | **No** | gzip's compressed stream has no internal synchronization points — decoding byte N requires having decoded everything before it, so the whole file must be read sequentially by one task. |
| **Snappy** (raw, on plain text) | No (in practice) | Hadoop's standard Snappy codec, applied to a raw file, has the same sequential-dependency problem as gzip for generic splitting purposes. |
| **bzip2** | **Yes** | bzip2 compresses in independent blocks with byte-aligned synchronization markers between them, so a reader can locate a block boundary and start decoding from there — Hadoop/Spark can exploit this to split a `.bz2` file into multiple partitions. |
| **LZO** | Yes, but only with an index | LZO is block-based like bzip2, but Hadoop needs a separate `.lzo.index` file (built via a one-time indexing job) to know where block boundaries fall — without the index, it behaves as unsplittable. |
| **Zstd** | No (in practice, for raw text) | Same category as gzip/Snappy for plain-file splitting purposes in standard Hadoop/Spark input handling, despite being a strong, fast codec in general. |

**Columnar container formats (Parquet, ORC) — splittability is a property of the format itself, not the compression codec:**
- Parquet and ORC are splittable **regardless of which internal compression codec is used** (gzip, Snappy, Zstd, LZ4 are all common choices here) — this is a frequent point of confusion, since gzip is unsplittable for plain text but perfectly fine inside a Parquet file.
- The reason: these formats don't compress the file as one continuous stream the way a gzipped text file does. They compress **independently per column chunk, within row groups (Parquet) or stripes (ORC)** — and the file's footer metadata records exactly where each row group/stripe begins and ends. A reader can jump straight to a row group boundary (known from the footer, read first) and decompress just that self-contained chunk, with no dependency on anything before it.
- This is a direct, practical reason converting large unsplittable sources (big gzipped CSV/JSON logs, for instance) into Parquet or ORC is such a common first optimization step for a pipeline — it doesn't just add columnar pruning and predicate pushdown, it also fixes the splittability problem at the same time, even if the Parquet file itself still uses gzip internally for its block compression.
- **Avro** behaves similarly — it's a splittable binary container format with internal sync markers between blocks, independent of whichever codec compresses those blocks.

**The practical failure pattern:** a single large gzipped CSV file (say, 20GB) read into Spark produces exactly **one partition**, handled by exactly **one task**, regardless of how many cores or executors are available — every other core sits idle while that one task works through 20GB alone. This often shows up as a job where the Spark UI shows a stage with only 1 task taking the overwhelming majority of the stage's total duration, which is a distinctive, specific symptom of this exact problem rather than generic skew (§13) — there's no "distribution" to fix here, since there's only one partition to begin with.

**Mitigations:**
1. **Convert to a splittable format at ingestion** — Parquet/ORC/Avro, which also brings the columnar/pushdown benefits discussed elsewhere in this guide.
2. **Switch to a splittable compression codec** if staying in a row-oriented format — bzip2 for text, or LZO with its index file built — accepting bzip2's slower compression/decompression speed as the trade-off for parallelism.
3. **Pre-split the file before it reaches Spark** — e.g., splitting one giant gzip file into many smaller gzip files upstream (each individually unsplittable, but now there are enough of them that Spark can at least assign one per task, restoring task-level parallelism even without true in-file splitting).
4. **Decompress to plain text first** (trading storage space for splittability) if converting formats or re-compressing isn't feasible — makes the file trivially splittable again at the cost of disk space and losing compression's storage/IO benefits.

---

## 4. Partitioning Schemes: Hash, Range, and Custom

**Hash Partitioning (the default for most shuffles)**
- Each row's partition is determined by `hash(key) % numPartitions`.
- Simple and fast, and gives a reasonably even distribution **as long as keys themselves are reasonably evenly distributed** — a small number of very frequent keys (skew) breaks this evenness regardless of how good the hash function is, since all rows sharing a key always land in the same partition by construction.
- Used by default for operations like `groupByKey`, `reduceByKey`, `join` (on the shuffle side), and `repartition(n, col1, col2, ...)` **when columns are explicitly specified**.
- **Correction worth being precise about:** `repartition(n)` called **without** any columns does *not* use hash partitioning — it uses round-robin partitioning instead (next section), specifically because hash partitioning alone can't guarantee even distribution, and even distribution is usually the entire point of calling plain `repartition(n)`.

**Range Partitioning**
- Partitions are built from **contiguous ranges of sorted key values**, rather than a hash — partition boundaries are determined by sampling the data to estimate roughly equal-sized ranges.
- Used by `sortBy`/`orderBy`/`repartitionByRange`, and anywhere a globally sorted output across partitions is required (so that partition 0 holds the smallest keys, partition 1 the next range, and so on, with no overlap).
- More expensive to set up (requires a sampling pass to estimate good boundaries) but necessary whenever downstream processing needs sorted, non-overlapping partitions rather than just "spread the data out."

**Round-Robin Partitioning**
- Rows are distributed across the target partition count roughly evenly, **with no regard to key values at all** — this is what plain `repartition(n)` (no columns) actually uses, both at the DataFrame level and, functionally, at the RDD level (`rdd.repartition(n)` is implemented as `coalesce(n, shuffle=true)` with randomized keys to spread data evenly rather than by any meaningful key).
- Produces the most evenly-sized partitions of any scheme, which is exactly why it's the default when no columns are given — but provides **no co-location guarantee whatsoever**. Rows sharing a key can land in completely different partitions, so this is not usable immediately before a key-dependent operation (a join, a groupBy) without another shuffle happening anyway to re-establish co-location.

**Custom Partitioner**
- At the RDD API level, a custom `Partitioner` subclass can implement arbitrary logic for `getPartition(key)` — useful for domain-specific distribution requirements a hash or range partitioner can't express (e.g., co-locating specific known key groups together, or implementing a partitioning scheme that matches an external system's own sharding for efficient joins against it).
- The DataFrame/SQL API doesn't expose custom partitioners directly in the same way, though `repartition(n, col1, col2, ...)` gives hash-based control over which columns drive the partitioning without a fully custom implementation.

---

## 5. Shuffle Mechanics: What Actually Happens on the Wire

A shuffle is triggered by any **wide transformation** — an operation where an output partition can depend on data from many/all input partitions (`groupByKey`, `join`, `distinct`, `repartition`, any aggregation requiring grouping by key). Understanding its internal steps explains most shuffle-related tuning:

1. **Map (write) side:** each task in the upstream stage writes its output records, bucketed by target partition (via the partitioner), to local disk as **shuffle files** — generally one combined, sorted data file plus an index file per map task (modern Spark uses **sort-based shuffle** exclusively, having deprecated the older hash-based shuffle that wrote one file per reduce partition per map task, which didn't scale well with high partition counts).
2. **Shuffle service (optional but important):** the files written by a map task can be served by an **external shuffle service** running independently of the executor process. This matters specifically for **dynamic allocation** — without it, if an executor that produced shuffle data is deallocated, its shuffle output becomes unavailable and must be recomputed; with it, shuffle files remain servable even after the producing executor is gone.
3. **Reduce (read) side:** each task in the downstream stage fetches the relevant portion of every upstream map task's output (the partition(s) assigned to it), over the network, from wherever those shuffle files live.
4. **Aggregation/sort/merge on the reduce side:** depending on the operation, the fetched data is merged, sorted, or aggregated — this is where **spill** occurs if the working set for a given reduce task exceeds its share of Execution memory, forcing partial results to disk mid-operation.

The cost of a shuffle is therefore **disk I/O (write + spill) + network I/O (fetch) + serialization overhead** on top of whatever the actual computation does — which is why shuffles are consistently the most expensive thing in a Spark job relative to narrow transformations, which incur none of this.

**A common production failure this mechanism directly explains:** `FetchFailedException`. If an executor holding shuffle output dies, is preempted (common on spot/preemptible instances), or is removed by dynamic allocation **without** the external shuffle service enabled, a downstream task trying to fetch its assigned partition from that executor fails. Spark's DAG scheduler responds by **recomputing the lost shuffle output** — re-running the upstream stage that produced it — rather than failing the whole job outright, up to a retry limit (`spark.stage.maxConsecutiveAttempts`). This is why a job can appear to "hang" re-running an already-completed-looking stage: it isn't actually hung, it's recovering lost shuffle data, and the real fix is addressing *why* executors are disappearing (spot instance churn, aggressive dynamic allocation scale-down, OOM-triggered executor loss) rather than treating the retry itself as the problem.

**A related internal detail worth knowing:** sort-based shuffle has a **bypass mode** (`spark.shuffle.sort.bypassMergeThreshold`, default 200) — when the number of output (reduce-side) partitions is at or below this threshold, Spark skips the sort step on the write side entirely and just writes one file per partition directly, since sorting isn't worth the overhead at low partition counts. This is also why `spark.sql.shuffle.partitions`' historic default happens to be exactly 200 — set at the boundary where the bypass optimization stops applying.

---

## 6. Narrow vs Wide Transformations, Revisited Through a Shuffle Lens

- **Narrow:** each output partition depends on a known, limited set of input partitions (often exactly one) — no shuffle, can be pipelined within a single stage (`map`, `filter`, `union`).
- **Wide:** output partitions can depend on data from many/all input partitions — requires a shuffle, creates a new stage boundary.
- A job's **stage count** is directly determined by how many wide transformations it contains — each shuffle ends one stage and begins the next. Minimizing unnecessary wide transformations (or replacing them with narrow-equivalent alternatives, like `reduceByKey`'s map-side pre-combine instead of `groupByKey`'s full shuffle-then-group) is one of the highest-leverage structural optimizations available, because it removes an entire category of cost rather than tuning around it.

---

## 7. Repartition vs Coalesce — Not Interchangeable

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | Can increase or decrease partition count | Can only decrease partition count (increasing silently does nothing useful) |
| Shuffle? | Always triggers a **full shuffle** | Avoids a shuffle by **combining existing partitions** without redistributing all data |
| Resulting balance | Even distribution across the new partition count | Uneven — simply merges adjacent partitions, so pre-existing imbalance carries through |
| Cost | Expensive (full shuffle) | Cheap (no network shuffle, just local merging) |
| Typical use | Need genuinely redistributed, evenly-sized partitions, especially before a shuffle-heavy downstream step or before a skewed write | Reducing partition count after a filter that dropped most rows, before writing output, when even distribution isn't critical |

A common mistake: using `coalesce()` to *increase* parallelism (it simply won't, since it can't split a partition further) or using `repartition()` purely to reduce output file count when `coalesce()` would have done the same job far more cheaply. The decision should be driven by whether even redistribution is actually required, not just by desired partition count.

`repartition(n, col)` (hash-partitioning by specific column(s)) is distinct from plain `repartition(n)` (round-robin, §4) — it's used when subsequent operations need data co-located by that column (e.g., before a join, to pre-shuffle both sides consistently, or before a window function partitioned by the same key).

**One more wrinkle:** `coalesce(n, shuffle=True)` forces a shuffle even though `coalesce` avoids one by default — at that point it behaves like `repartition(n)` for the purpose of achieving a precise, rebalanced count, though there's rarely a reason to reach for this spelling over calling `repartition(n)` directly once a shuffle is already being paid for.

**Checking actual partition count directly** (rather than inferring it from configuration) is straightforward and worth knowing: `df.rdd.getNumPartitions()` — useful for confirming what a chain of operations actually produced, since defaults, AQE coalescing, and upstream shuffles can all make the real number diverge from what a config value alone would suggest.

---

## 8. Adaptive Query Execution and Partitioning

AQE (`spark.sql.adaptive.enabled`, default `true` in modern Spark) directly addresses two of the biggest static-partitioning pain points:

- **Coalescing post-shuffle partitions automatically:** rather than being stuck with a flat `spark.sql.shuffle.partitions=200` regardless of actual data size, AQE observes real post-shuffle partition sizes and merges small ones together, targeting a more sensible partition size (governed by `spark.sql.adaptive.coalescePartitions.enabled`, `spark.sql.adaptive.advisoryPartitionSizeInBytes`, default 64MB). This substantially reduces the need to hand-tune `shuffle.partitions` per job.
- **Splitting skewed partitions:** detects a partition dramatically larger than its peers in a sort-merge join (`spark.sql.adaptive.skewJoin.enabled`) and splits it into smaller sub-partitions, processed and joined separately, then unioned — addressing the single most common partitioning-related performance failure without manual salting.

AQE operates at **shuffle (query stage) boundaries only** — it re-plans the remainder of the query after a stage completes, using that stage's actual output statistics; it has no effect on the initial partitioning of a freshly-read source, which is still governed by the read-time logic in §3.

---

## 9. Partitioning on Write, and the Small-File Problem

`df.write.partitionBy("col1", "col2").parquet(path)` physically lays data out into a directory structure (`col1=value1/col2=value2/...`), which is distinct from the in-memory/shuffle partitioning discussed above but closely related in practice:

- **Partition pruning on read:** a query later filtering on the partitioned columns can skip reading entire directories outright, without even opening the files — one of the most effective read-side optimizations available, but only if the write-time partitioning actually matches common filter patterns on that table.
- **The small-file problem:** if the DataFrame being written has many partitions (in the Spark/shuffle sense) relative to the number of distinct `partitionBy` values, each in-memory partition writes its own file per output directory — leading to many small files, which is expensive for downstream reads (file-open overhead, metadata overhead on systems like HDFS/S3) and for the catalog tracking them.
- **Standard mitigation:** `repartition(n, "col1", "col2")` (matching the write-partitioning columns) immediately before the write, so that each output directory gets a controlled, reasonable number of files rather than one per original Spark partition. `coalesce()` can also reduce file count cheaply when even redistribution isn't required.
- **Over-partitioning on write** (too many distinct partition column combinations, e.g., partitioning by a high-cardinality column like a raw timestamp or a user ID) produces an explosion of tiny partitions/files and metadata overhead — a frequent design mistake distinct from the in-memory shuffle-partition-count mistake, but caused by the same underlying "too many small pieces" failure mode.

---

## 10. Data Skew — When Partitioning Breaks Down

Skew is what happens when a chosen partitioning scheme (almost always hash-based) produces a few partitions dramatically larger than the rest, because the underlying key distribution is itself uneven:

- **Symptoms:** a stage where the vast majority of tasks finish quickly and a handful run far longer (or spill/OOM) — visible directly in the Spark UI's per-task duration and shuffle-read-size distribution.
- **Common real-world causes:** a dominant entity (one customer responsible for a huge share of transactions), a default/placeholder value (`0`, `-1`, empty string) used for "unknown," or — especially in joins — a heavily `null`-populated foreign key column, which collapses into a single oversized partition under naive hash partitioning even though those rows often shouldn't meaningfully match anything downstream.
- **Mitigations, roughly by effort:**
  1. Let AQE's skew-join handling manage it automatically where applicable (§8).
  2. **Salting:** append a random suffix to the skewed key on one side, and explode the matching values on the other side across the same suffix range, spreading what was one hot partition across several.
  3. **Isolate and handle the hot key(s) separately** — filter them out, process that subset differently (e.g., with a broadcast join if feasible), and union the results back with the non-skewed majority.
  4. **Pre-aggregate before the shuffle**, where the use case allows it, reducing the absolute row count the skewed key carries into the shuffle even if the relative skew ratio is unchanged.
- Skew is a **data distribution problem**, not a partition-*count* problem — increasing `spark.sql.shuffle.partitions` does nothing for a hash-partitioned hot key, since every row for that key still lands in the same single partition regardless of how many partitions exist in total.

---

## 11. Bucketing: Pre-Partitioning at Rest

Distinct from both in-memory partitioning and directory-based `partitionBy`, **bucketing** (`.bucketBy(n, "key").saveAsTable(...)`) pre-partitions data into a fixed number of hash buckets **at write time**, persisted as part of the table's physical layout:

- When two tables are bucketed identically (same key, same bucket count), Spark can join them **without a shuffle at all**, since matching keys are already guaranteed to reside in correspondingly-numbered files.
- The cost is paid once at write time rather than on every read/join — a strong fit for tables joined repeatedly across many downstream jobs, trading write-time cost for eliminating a recurring runtime cost.
- Unlike `partitionBy`, bucketing doesn't create a directory-per-value structure (avoiding the high-cardinality small-file explosion risk from §9) — it creates a fixed number of files per bucket, regardless of key cardinality.
- Two practical constraints worth knowing: bucketing is only fully supported via `saveAsTable` (a catalog-registered, typically Hive-compatible table) rather than a plain `.parquet(path)` write, since the bucketing metadata needs to be tracked somewhere; and the shuffle-free join optimization itself is gated by `spark.sql.sources.bucketing.enabled` (on by default), which must be true for Spark to actually recognize and exploit matching bucketing between two tables at join time.

---

## 12. Dynamic Partition Pruning (DPP)

A runtime optimization specifically for joining a **partitioned** (via `partitionBy` at write time) fact table against a filtered dimension table:

- If a dimension table is filtered (`WHERE region = 'EU'`) and then joined to a large fact table partitioned by that same column, DPP pushes the dimension-side filter into the fact table's **partition pruning at runtime**, reading only the fact table partitions that could possibly match rather than scanning the whole table.
- Controlled by `spark.sql.optimizer.dynamicPartitionPruning.enabled` (on by default in modern Spark); entirely dependent on the fact table actually being partitioned on the relevant join/filter column at the storage layer — it has no effect otherwise.

---

## 13. Diagnosing Partitioning/Shuffle Problems

| Symptom | Likely Cause | What to Check |
|---|---|---|
| A handful of tasks run far longer than the rest in a shuffle stage | Data skew on the shuffle/join key | Per-task shuffle read size and duration in the Spark UI Stages tab |
| Many tiny tasks, high scheduling overhead relative to actual work | Too many partitions for the data size (over-partitioned) | Partition count vs. total data size; consider `coalesce()` or lowering `shuffle.partitions` |
| A handful of very large tasks, spill or OOM in a shuffle stage | Too few partitions for the data size (under-partitioned) | Average/median partition size vs. the ~128MB-per-partition rule of thumb |
| Output directory has thousands of tiny files | Spark partition count far exceeds distinct `partitionBy` values at write time | `repartition()`/`coalesce()` on the write-partitioning columns immediately before `.write` |
| "No space left on device" during a shuffle-heavy stage | Shuffle write/spill filling local disk, often skew-driven | `spark.local.dir` sizing, spill metrics, skewed partition sizes |
| A read against a huge JDBC table is surprisingly slow and single-threaded | No partitioning options specified for the JDBC read | `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` on the read |
| Repeated joins against the same large tables are consistently expensive | No bucketing; full shuffle cost paid every time | Whether bucketing the frequently-joined tables on the shared key would help |
| A stage appears to re-run or "hang" after seemingly completing | `FetchFailedException` from a lost executor (spot/preemptible termination, OOM kill, aggressive scale-down) forcing shuffle output recomputation | Executor loss events in the Spark UI/logs; whether the external shuffle service is enabled; `spark.stage.maxConsecutiveAttempts` |
| A read stage shows exactly 1 task taking far longer than everything else, on a large single file | The source file is unsplittable (e.g., gzip-compressed CSV/JSON) — the whole file is one partition by construction | File format/compression (§3.1); whether converting to Parquet/ORC or a splittable codec (bzip2, LZO+index) is feasible |

---

## 14. Configuration Reference

| Config | Default | Purpose |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Post-shuffle partition count for joins/aggregations (static; AQE can override dynamically). |
| `spark.sql.files.maxPartitionBytes` | 128MB | Target size per partition when reading splittable files. |
| `spark.sql.files.openCostInBytes` | 4MB | Estimated per-file open overhead, used to pack many small files into fewer partitions on read. |
| `spark.sql.files.minPartitionNum` | `spark.default.parallelism` | Floor on partition count for file-based reads — the counterpart to `maxPartitionBytes`'s ceiling. |
| `spark.default.parallelism` | total cores (cluster-manager dependent) | Default partition count for RDD operations without an explicit shuffle partition count. |
| `spark.sql.adaptive.enabled` | true (modern Spark) | Enables AQE, including dynamic partition coalescing and skew splitting. |
| `spark.sql.adaptive.coalescePartitions.enabled` | true (with AQE) | Enables automatic merging of small post-shuffle partitions. |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | Target partition size AQE aims for when coalescing. |
| `spark.sql.adaptive.skewJoin.enabled` | true (with AQE) | Enables automatic detection/splitting of skewed partitions in sort-merge joins. |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | true (modern Spark) | Enables DPP for partitioned fact-table joins against filtered dimensions. |
| `spark.shuffle.service.enabled` | false | Enables the external shuffle service, decoupling shuffle file availability from executor lifetime. |
| `spark.reducer.maxSizeInFlight` | 48MB | Max data size fetched simultaneously per reduce task during shuffle read, per connection. |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | Reduce-side partition count at/below which shuffle write skips sorting entirely. |
| `spark.stage.maxConsecutiveAttempts` | 4 | Retry limit for a stage before the job fails outright after repeated shuffle fetch failures. |
| `spark.sql.sources.bucketing.enabled` | true | Gates whether Spark recognizes and exploits matching bucketing between two tables for shuffle-free joins. |

---

## 15. Practical Tuning Checklist

- [ ] Target roughly 128MB–256MB of data per partition as a starting heuristic — neither hundreds of MB-sized partitions nor thousands of tiny ones.
- [ ] Check whether `spark.sql.shuffle.partitions` (flat default 200) actually fits the job's data size, or whether AQE coalescing is already handling it adequately.
- [ ] Before writing partitioned output, `repartition()`/`coalesce()` by the write-partitioning columns to avoid a small-file explosion.
- [ ] Check for skew directly (distinct key counts, per-task shuffle size) rather than assuming AQE caught it.
- [ ] Use `coalesce()` instead of `repartition()` when reducing partition count and even redistribution isn't required — it's materially cheaper.
- [ ] For JDBC reads of large tables, always specify partitioning options rather than relying on the single-connection default.
- [ ] For frequently-repeated joins on the same large tables, evaluate whether bucketing removes the shuffle cost entirely.
- [ ] Avoid partitioning writes by high-cardinality columns (raw timestamps, user IDs) — prefer coarser-grained columns (date, region) that still support useful pruning without exploding file/directory count.
- [ ] Confirm the external shuffle service is enabled when using dynamic allocation, so executor deallocation doesn't force shuffle recomputation.
- [ ] If stages appear to silently re-run, check for `FetchFailedException`/executor loss before assuming it's a logic or data issue.
- [ ] Use `df.rdd.getNumPartitions()` to confirm actual partition counts rather than inferring them purely from configuration.
- [ ] For large source files, confirm the format/compression combination is actually splittable (§3.1) before assuming `maxPartitionBytes` will parallelize the read — a single large gzip file won't split no matter what that config says.

---

## 16. A Concise Mental Model

- **Partition count determines parallelism; partition distribution determines whether that parallelism is actually usable.** Even partition counts with skewed data still bottleneck on the largest partition.
- **A shuffle is a stage boundary**, and its cost is disk (write + spill) plus network (fetch) plus serialization — avoiding unnecessary wide transformations removes this cost category entirely rather than just reducing it.
- **`repartition()` reshuffles and rebalances; `coalesce()` merges cheaply without rebalancing** — they solve different problems and aren't interchangeable.
- **AQE fixes the two biggest static-partitioning pain points** (wrong flat shuffle-partition count, and skew) automatically at shuffle boundaries, but doesn't touch initial read-time partitioning.
- **Bucketing and `partitionBy` solve different problems**: bucketing eliminates shuffle cost for repeated joins; `partitionBy` enables partition pruning on filtered reads — and both can go wrong the same way (too many small files) if cardinality isn't considered.
- **Skew is a distribution problem that more partitions can't fix** — it requires addressing the key distribution itself, not the partition count.
- **Round-robin, not hash, is what plain `repartition(n)` actually uses** — hash partitioning only applies once specific columns are given, which is the detail most likely to be misremembered about this pair of APIs.
- **Lost shuffle data triggers recomputation, not just a stage failure** — a seemingly "hanging" stage after apparent completion is frequently `FetchFailedException` recovery, not a logic problem.
