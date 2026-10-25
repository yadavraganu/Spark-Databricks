## 1. What MERGE Does

`MERGE` upserts data from a **source** (table, view, or DataFrame) into a **target Delta table** in a single atomic operation. It supports **inserts, updates and deletes**, and Databricks extends standard SQL with extra clauses (`WHEN NOT MATCHED BY SOURCE`, `UPDATE SET * EXCEPT`, `WITH SCHEMA EVOLUTION`).

The mental model: join source to target on a condition, then for each row decide an action based on **which side the row exists on**:

| Row exists in… | Clause | Allowed actions |
|---|---|---|
| Both (join matched) | `WHEN MATCHED` | `UPDATE`, `DELETE` |
| Source only | `WHEN NOT MATCHED` (= `BY TARGET`) | `INSERT` |
| Target only | `WHEN NOT MATCHED BY SOURCE` | `UPDATE`, `DELETE` |

## 2. Syntax

```sql
[ common_table_expression ]

MERGE [ WITH SCHEMA EVOLUTION ] INTO target_table_name [target_alias]
  USING source_table_reference [source_alias]
  ON merge_condition
  { WHEN MATCHED [ AND matched_condition ] THEN matched_action |
    WHEN NOT MATCHED [BY TARGET] [ AND not_matched_condition ] THEN not_matched_action |
    WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action } [...]

matched_action:
  { DELETE |
    UPDATE SET * [ EXCEPT ( column [, ...] ) ] |
    UPDATE SET { column = { expr | DEFAULT } } [, ...] }

not_matched_action:
  { INSERT * [ EXCEPT ( column [, ...] ) ] |
    INSERT (column1 [, ...]) VALUES ( expr | DEFAULT [, ...] ) }

not_matched_by_source_action:
  { DELETE |
    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
```

**Restrictions on the target:** must be a Delta table; must **not** be a foreign table; the name must not carry an options specification. A SQL `MERGE` can target a **view** only if the view is defined as `CREATE VIEW viewName AS SELECT * FROM deltaTable`.

## 3. Core Example: Upsert

```sql
MERGE INTO main.default.people10m AS people10m
USING main.default.people10mupdates AS people10mupdates
ON people10m.id = people10mupdates.id
WHEN MATCHED THEN
  UPDATE SET
    id = people10mupdates.id,
    firstName = people10mupdates.firstName,
    middleName = people10mupdates.middleName,
    lastName = people10mupdates.lastName,
    gender = people10mupdates.gender,
    birthDate = people10mupdates.birthDate,
    ssn = people10mupdates.ssn,
    salary = people10mupdates.salary
WHEN NOT MATCHED THEN
  INSERT (id, firstName, middleName, lastName, gender, birthDate, ssn, salary)
  VALUES (people10mupdates.id, people10mupdates.firstName, people10mupdates.middleName,
          people10mupdates.lastName, people10mupdates.gender, people10mupdates.birthDate,
          people10mupdates.ssn, people10mupdates.salary);
```

Shorter equivalent when schemas match:
```sql
MERGE INTO people10m t USING people10mupdates s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Python (Delta API):**
```python
from delta.tables import *

target  = DeltaTable.forName(spark, "main.default.people10m")
updates = DeltaTable.forName(spark, "main.default.people10mupdates").toDF()

(target.alias("people")
  .merge(updates.alias("updates"), "people.id = updates.id")
  .whenMatchedUpdate(set={"firstName": "updates.firstName", "salary": "updates.salary"})
  .whenNotMatchedInsert(values={"id": "updates.id", "firstName": "updates.firstName", "salary": "updates.salary"})
  .execute())
```
(`whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()` mirror `UPDATE SET *` / `INSERT *`.)

## 4. Semantics in Detail

### 4.1 Clause evaluation
- Any number of `WHEN MATCHED` and `WHEN NOT MATCHED` clauses are allowed.
- Clauses of the same type are **evaluated in the order written**; the **first whose condition is true wins**.
- **All clauses of a type except the last must have a condition.** Otherwise you get an error:
  - `NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION`
  - `NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION`
  - `NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION`
- If no `WHEN MATCHED` condition is true for a matched pair, the **target row is left unchanged**. Likewise for `NOT MATCHED BY SOURCE` and target rows.
- In the programmatic API, a `whenMatched` clause has **at most one `update` and one `delete`** action.

```sql
MERGE INTO target USING source ON target.key = source.key
WHEN MATCHED AND target.marked_for_deletion THEN DELETE
WHEN MATCHED THEN UPDATE SET target.updated_at = source.updated_at, target.value = DEFAULT;
```

### 4.2 `WHEN MATCHED`
- `UPDATE SET *` updates **all target columns** from same-named source columns; it requires the source to have the **same columns** as the target (otherwise an analysis error — unless schema evolution is on).
- `UPDATE SET col = expr` updates only listed columns.
- `UPDATE SET * EXCEPT (c1, c2)` leaves excluded target columns **set to `NULL`** (not unchanged) — be careful: with `EXCEPT` and schema evolution *off*, the excluded columns are nulled.
- `DEFAULT` sets a column to its default value.
- `DELETE` removes the matched target row.

### 4.3 `WHEN NOT MATCHED [BY TARGET]`
- Only `INSERT` is allowed.
- `INSERT *` needs the source to have the same columns as the target; `INSERT * EXCEPT (…)` nulls excluded columns.
- `INSERT (cols) VALUES (…)`: unspecified columns get their **column default, or `NULL`** if none exists.
- A source row that matches no clause condition is simply ignored.

### 4.4 `WHEN NOT MATCHED BY SOURCE`
- Acts on **target rows with no source match**; allowed actions: `UPDATE` or `DELETE`.
- The condition and expressions can reference **only target columns** — there is no source row to read.
- Example of modifying a target column: `UPDATE SET target.deleted_count = target.deleted_count + 1`.
- **Performance warning:** without a condition this clause can touch **a large part of — or the whole — target**, which is expensive. Always add a condition (e.g. restrict to recent dates) unless you really mean to overwrite the target.

```sql
MERGE INTO target USING source ON source.key = target.key
WHEN MATCHED THEN UPDATE SET target.lastSeen = source.timestamp
WHEN NOT MATCHED THEN INSERT (key, lastSeen, status) VALUES (source.key, source.timestamp, 'active')
WHEN NOT MATCHED BY SOURCE AND target.lastSeen >= (current_date() - INTERVAL '5' DAY)
  THEN UPDATE SET target.status = 'inactive';
```

### 4.5 The one-match rule (most common failure)
**Only one source row may match a given target row.** If several source rows match the same target row *and* the merge would update it, `MERGE` fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`, because it's ambiguous which source row to apply.

- **DBR 16.0+:** the check considers both the `ON` condition **and** the `WHEN MATCHED` condition. **DBR 15.4 LTS and below:** only `ON` is considered.
- **Exception:** an **unconditional `DELETE`** is allowed with multiple matches, because deleting is unambiguous.
- **Fix:** preprocess the source so each key appears once (see §5.4).

---

## 5. Common Patterns

### 5.1 Insert-only merge: deduplicate on ingest
```sql
MERGE INTO logs USING newDedupedLogs
ON logs.uniqueId = newDedupedLogs.uniqueId
WHEN NOT MATCHED THEN INSERT *;
```
The incoming dataset must already be **deduplicated within itself** — merge dedupes new data against the *table*, but duplicates inside the new batch are all inserted.

**Bounded lookback** (if duplicates only appear within a few days, and the table is partitioned or clustered by date):
```sql
MERGE INTO logs USING newDedupedLogs
ON logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS
WHEN NOT MATCHED AND newDedupedLogs.date > current_date() - INTERVAL 7 DAYS THEN INSERT *;
```
This looks for duplicates only in the last 7 days instead of the whole table. An insert-only merge **only appends**, so downstream streams can read the table.

### 5.2 Overwrite a target with a source (including deletes)
```sql
MERGE INTO target USING source ON source.key = target.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```
This rewrites a lot of data; use the incremental version below when changes are time-bound.

### 5.3 Incrementally sync with a moving window
For sources where records change or disappear for a few days and then settle:
```sql
MERGE INTO target AS t
USING (SELECT * FROM source WHERE created_at >= (current_date() - INTERVAL '5' DAY)) AS s
ON t.key = s.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND created_at >= (current_date() - INTERVAL '5' DAY) THEN DELETE;
```
**Use the same boolean filter on the source and target** so that only the window is compared and deleted. Without the conditions the entire target is rewritten.

### 5.4 Apply a CDC feed (keep only the latest change per key) *(pattern — not from the docs)*
Preprocess the change feed so each key has one row, then merge:
```sql
MERGE INTO target t
USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY change_ts DESC) AS rn
    FROM cdc_feed
  ) WHERE rn = 1
) s
ON t.key = s.key
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.change_ts > t.change_ts THEN UPDATE SET *
WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT *;
```
The `s.change_ts > t.change_ts` guard prevents an older event from overwriting a newer row. The docs advise using **`AUTO CDC ... INTO`** in Lakeflow pipelines for SCD Type 1/2 because it handles **out-of-order records** correctly.

### 5.5 Slowly changing dimension, Type 2 *(pattern — not from the docs; prefer AUTO CDC)*
The classic trick unions two views of the staged data: rows keyed to close the current version, and rows with a `NULL` key to insert the new version.
```sql
MERGE INTO dim_customer t
USING (
  SELECT s.customer_id AS merge_key, s.* FROM staged s
  UNION ALL
  SELECT NULL AS merge_key, s.* FROM staged s
  JOIN dim_customer d ON s.customer_id = d.customer_id
  WHERE d.is_current AND s.address <> d.address
) u
ON t.customer_id = u.merge_key AND t.is_current = true
WHEN MATCHED AND t.address <> u.address THEN
  UPDATE SET t.is_current = false, t.end_date = u.effective_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, is_current, effective_date, end_date)
  VALUES (u.customer_id, u.address, true, u.effective_date, NULL);
```
Lakeflow pipelines' `AUTO CDC ... STORED AS SCD TYPE 2` is simpler and handles ordering (see the Lakeflow guide).

### 5.6 Streaming upserts with `foreachBatch` *(pattern; the docs link to a separate streaming page I didn't read)*
```python
def upsert_to_delta(micro_batch_df, batch_id):
    micro_batch_df.createOrReplaceTempView("updates")
    micro_batch_df.sparkSession.sql("""
      MERGE INTO target t USING updates s ON t.key = s.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")

(streamingDF.writeStream
   .foreachBatch(upsert_to_delta)
   .option("checkpointLocation", "<path>")
   .start())
```
Deduplicate **within each micro-batch** first (one row per key), or the one-match rule will fail the batch.

## 6. Schema Evolution with MERGE

By default, `MERGE` does **not** change the target schema. Enable evolution **per statement** (Databricks' recommendation) rather than via the session config:

```sql
MERGE WITH SCHEMA EVOLUTION INTO target USING source ON source.key = target.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```
```python
(targetTable.merge(sourceDF, "source.key = target.key")
   .withSchemaEvolution()
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .whenNotMatchedBySourceDelete()
   .execute())
```
The legacy alternative `spark.databricks.delta.schema.autoMerge.enabled=true` applies to the whole session and is **not recommended for production**; the statement-level syntax takes precedence over it.

### The two cases schema evolution handles
1. **Column in source, not in target:** added to the target **if** it's referenced by *name* in an `UPDATE`/`INSERT` assignment **or** an `UPDATE SET *` / `INSERT *` is present. It applies only when the source column name and structure **exactly match** the target assignment, and the new column must actually exist in the source.
   - Triggers evolution: `UPDATE SET target.newcol = source.newcol`, `UPDATE SET target.newcol = source.newcol + 1`, `UPDATE SET target.somestruct.newfield = source.somestruct.newfield`, `UPDATE SET *`, `INSERT *`.
   - **Does not** trigger evolution: `UPDATE SET target.newcol = source.someothercol`, `... = source.x + source.y`, `... = source.output.newcol` (the source has no `newcol`).
2. **Column in target, not in source:** the target schema is **unchanged**; `UPDATE SET *` **leaves** those columns untouched, `INSERT *` sets them to `NULL`, and they can still be explicitly assigned.

New columns are appended to the **end** of the schema (or the end of the struct), and case is preserved.

### Behavior table (key, value / new_value examples)

| Target / source columns | Without evolution | With evolution |
|---|---|---|
| `key, value` / `key, value, new_value` (`UPDATE SET *`, `INSERT *`) | Schema unchanged; only `key`, `value` written | Schema becomes `key, value, new_value`; values populated |
| `key, old_value` / `key, new_value` (`UPDATE SET *`, `INSERT *`) | **Error** (target column `old_value` missing in source) | Schema becomes `key, old_value, new_value`; matched rows get `new_value`, `old_value` unchanged; new rows get `NULL` `old_value` |
| `key, old_value` / `key, new_value` (`UPDATE SET new_value = s.new_value`) | **Error** (`new_value` not in target) | Column added; unmatched rows get `NULL` |

### `EXCEPT` behaves differently with evolution
- **Evolution off:** `EXCEPT` refers to **target** columns and excludes them from `UPDATE`/`INSERT` (they become `NULL`).
- **Evolution on:** `EXCEPT` refers to **source** columns and excludes them from schema evolution — a new source column listed in `EXCEPT` is not added.

```sql
-- id, title, last_updated in target; id, title, review, last_updated in source
MERGE WITH SCHEMA EVOLUTION INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET last_updated = current_date()
WHEN NOT MATCHED THEN INSERT * EXCEPT (last_updated);
-- Adds `review`; new rows get last_updated = NULL
```

### Version history of merge schema evolution
- DBR 11.3 LTS and below: only `INSERT *` / `UPDATE SET *` can trigger it.
- DBR 12.2 LTS+: source columns and struct fields can be named in assignments.
- DBR 13.3 LTS+: structs nested inside maps (e.g. `map<int, struct<a:int,b:int>>`).
- DBR 15.4 LTS+: `WITH SCHEMA EVOLUTION` / `.withSchemaEvolution()`.

### Other schema warnings
- **Schema updates conflict with all concurrent write operations** — coordinate them.
- Updating a table schema **terminates any streams reading that table**; restart them.
- Type widening can be applied by automatic schema evolution (see the type-widening page).

## 7. Performance

### Make the join selective (you control this)
- **Put pruning predicates in the `ON` clause** (or the source subquery) on partition / clustering columns — e.g. `AND t.date >= current_date() - 7`. A merge whose `ON` clause can't prune must scan the whole target.
- **Filter the source** to only changed data (e.g. the last N days).
- **Always add conditions to `WHEN NOT MATCHED BY SOURCE`** to avoid rewriting most of the table.
- **Deduplicate the source first** (prevents the one-match error and extra work).
- Use only the columns you need in `UPDATE SET` when you don't need `*`.
- Prefer **liquid clustering** on the target for new tables so merges that touch clustered keys read less data (Databricks recommends liquid clustering for all new tables).

### Engine features that help
| Feature | What it does | Notes |
|---|---|---|
| **Low shuffle merge** | Processes **unmodified rows in a separate, streamlined path with no shuffle**, and preserves the existing data layout (including liquid clustering, and Z-order on a best-effort basis) so later queries degrade more slowly; reduces the need to rerun `OPTIMIZE` after merges | **On by default in DBR 10.4+**; in older runtimes enabled with `spark.databricks.delta.merge.enableLowShuffle` (no effect in 10.4+) |
| **Deletion vectors / predictive I/O** *(from search snippet)* | Avoid rewriting whole files during updates, deletes and merges; row-level concurrency requires them | For Delta tables, deletion vectors must be **enabled explicitly**; predictive I/O needs Photon |
| **Dynamic file pruning** *(from search snippet)* | Skips files that can't contain matching rows in joins | `MERGE`, `UPDATE`, `DELETE` need **Photon-enabled compute** for it |
| **Isolation levels / row-level concurrency** *(from search snippet)* | Default isolation is **WriteSerializable**; row-level concurrency resolves many conflicts between concurrent writers on qualifying tables | Check requirements on the dedicated pages |

### Tuning checklist
1. Is the `ON` clause pruning by partition/cluster key?
2. Is the source deduplicated and filtered?
3. Do all `NOT MATCHED BY SOURCE` clauses have conditions?
4. Are deletion vectors, Photon and a recent runtime in use?
5. Is the target on liquid clustering (or a sensible layout) and kept compact (`OPTIMIZE`, predictive optimization)?
6. Check the **query profile** for large "files read / files rewritten" numbers.

## 8. Concurrency and Conflicts

- Concurrent writers to the same table can conflict; `MERGE` is optimistic, so a losing commit may fail or retry depending on isolation level and table features.
- Reduce conflicts by **partitioning/clustering work** so concurrent merges touch different data and by putting that partition/cluster predicate in the `ON` clause *(general guidance)*.
- **Schema changes conflict with every concurrent write** (docs).
- Rerunning a failed batch is safe only if the merge is idempotent (keys unique; ordering guard on updates).

## 9. MERGE vs. Alternatives

| Need | Better tool |
|---|---|
| CDC / SCD Type 1 or 2 with out-of-order events | **`AUTO CDC ... INTO`** in Lakeflow pipelines |
| Replace a known slice of data (e.g. a date range) | `INSERT ... REPLACE WHERE` / selective overwrite *(general knowledge; page not read)* |
| Append-only ingestion of files | `COPY INTO` / Auto Loader |
| Simple "update these rows" | `UPDATE` / `DELETE` |
| Key-aware upserts at very high rates on indexed storage | Hudi-style indexed storage — see the table-format comparison guide |

## 10. Cheat Sheet

| Task | Snippet |
|---|---|
| Upsert all columns | `WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |
| Insert-only (dedupe) | `WHEN NOT MATCHED THEN INSERT *` |
| Conditional update | `WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET *` |
| Delete on flag | `WHEN MATCHED AND s.op = 'D' THEN DELETE` |
| Sync including deletes | `WHEN NOT MATCHED BY SOURCE [AND cond] THEN DELETE` |
| Soft delete unmatched | `WHEN NOT MATCHED BY SOURCE AND cond THEN UPDATE SET t.status = 'inactive'` |
| Exclude a column | `UPDATE SET * EXCEPT (col)` / `INSERT * EXCEPT (col)` |
| Reset to default | `UPDATE SET col = DEFAULT` |
| Evolve schema | `MERGE WITH SCHEMA EVOLUTION INTO …` / `.withSchemaEvolution()` |
| Python clauses | `whenMatchedUpdate/UpdateAll/Delete`, `whenNotMatchedInsert/InsertAll`, `whenNotMatchedBySourceUpdate/Delete` |

---

## 11. Interview-Style Q&A

**Q: What does MERGE do and what are its three clause types?**
A: An atomic upsert from a source into a Delta target: `WHEN MATCHED` (update/delete), `WHEN NOT MATCHED [BY TARGET]` (insert), and `WHEN NOT MATCHED BY SOURCE` (update/delete target-only rows).

**Q: Why does MERGE fail with "multiple source rows matching target row"?**
A: More than one source row matches the same target row, making the update ambiguous. In DBR 16.0+ both the `ON` and `WHEN MATCHED` conditions are considered; earlier only `ON`. Deduplicate the source. Unconditional deletes are exempt.

**Q: Why must non-final clauses have conditions?**
A: Clauses are evaluated in order and the first true one wins; an unconditional clause would shadow every later clause, so Delta requires conditions on all but the last.

**Q: How do you make a MERGE efficient?**
A: Prune in the `ON` clause on partition/cluster keys, filter and dedupe the source, add conditions to `NOT MATCHED BY SOURCE`, use deletion vectors/Photon and liquid clustering, and rely on low shuffle merge (default in DBR 10.4+).

**Q: What is low shuffle merge?**
A: An optimized implementation that processes unmodified rows without a shuffle and preserves their layout, so merges are faster and later reads degrade more slowly.

**Q: How does schema evolution work with MERGE?**
A: Enable it per statement with `WITH SCHEMA EVOLUTION`; new source columns referenced by name or via `*` are added to the target. Target-only columns are left alone (`UPDATE SET *`) or nulled (`INSERT *`). `EXCEPT` flips meaning: it excludes source columns from evolution.

**Q: What does `UPDATE SET * EXCEPT (c)` do?**
A: With evolution off it updates everything except `c`, which ends up `NULL`. With evolution on, `c` is excluded from schema evolution.

**Q: How would you implement SCD Type 2?**
A: In Lakeflow pipelines use `AUTO CDC ... STORED AS SCD TYPE 2`. With plain MERGE, union a keyed set (to close the current version) with a NULL-keyed set (to insert the new version) — a known pattern, but AUTO CDC handles out-of-order data better.

**Q: How do you apply streaming data to a Delta table with MERGE?**
A: Use `foreachBatch` and run a MERGE per micro-batch, deduplicating within the batch first.

**Q: What can't you MERGE into?**
A: Non-Delta tables, foreign tables, and views other than `SELECT * FROM deltaTable`.
