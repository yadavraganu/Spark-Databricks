## 1. REPLACE WHERE (Predicate Match)
- **Best For**: Fixed, explicit matching conditions like a hardcoded date range or status.
- **Behavior**: Atomically replaces rows matching an arbitrary boolean expression. It validates incoming rows by default; if any row falls outside the predicate, the transaction fails.
- **Empty Source**: May delete existing rows in the target table if the source dataset is empty.
```python
df_source.write
  .mode("overwrite")
  .option("replaceWhere", "start_date >= '2026-01-01' AND end_date <= '2026-01-31'")
  .saveAsTable("main.default.events"))
```
```sql
INSERT INTO TABLE main.default.events 
REPLACE WHERE start_date >= '2026-01-01' AND end_date <= '2026-01-31' 
SELECT * FROM df_source;
```
<br></br>
## 2. REPLACE USING (Dynamic Equality Overwrite)
- **Best For**: Dynamic row-level or partition-level overwrites where rows matching specific key columns need replacement.
- **Behavior**: Replaces target table rows when the specified key columns compare equal (=) to the source data.
- **Empty Source**: Does not delete target data if the source is empty.
```python
df_source.write
  .mode("overwrite")
  .option("replaceUsing", "event_id, start_date")
  .saveAsTable("main.default.events"))
```
```sql
INSERT INTO TABLE main.default.events
REPLACE USING (event_id, start_date)
SELECT * FROM df_source;
```
<br></br>
## 3. REPLACE ON (Dynamic Overwrite by Expression)
REPLACE ON selectively overwrites target rows using a user-defined boolean expression. It is ideal for complex matching logic that regular equality comparisons cannot handle, such as treating NULL values as equal.
- **How it works**: It matches incoming rows against the target table using conditional logic (like the <=> null-safe equality operator).
- **Key feature**: If your source query returns zero rows, it will not delete any existing data from the target table.
```python
source_df.alias("src")
  .write
  .format("delta")
  .mode("overwrite")
  .option("targetAlias", "tgt")
  .option("replaceOn", "src.event_id <=> tgt.event_id AND src.country <=> tgt.country")
  .saveAsTable("my_catalog.my_schema.events"))
```
```sql
INSERT INTO TABLE my_catalog.my_schema.events AS t
REPLACE ON (s.event_id <=> t.event_id AND s.country <=> t.country)
SELECT * FROM source_data AS s;
```
<br></br>
## 4. partitionOverwriteMode (Legacy Dynamic Partition Overwrite)
This is an older approach that drops and completely replaces existing data only within the partitions that receive new data from your incoming dataset.
- **How it works**: If your source data contains records for year=2026, it clears out the entire year=2026 partition in the target and populates it with the new data.
- **Critical Danger**: If a single row in your incoming data accidentally contains a wrong or malformed partition value, the entire target partition for that value will be completely wiped out.
```python
df.write
  .format("delta")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")
  .saveAsTable("my_catalog.my_schema.events"))
```
```sql
Step 1: Change the session overwrite mode from 'static' to 'dynamic'
SET spark.sql.sources.partitionOverwriteMode = dynamic;

-- Step 2: Run the standard insert overwrite statement
INSERT OVERWRITE TABLE my_catalog.my_schema.events 
SELECT * FROM incoming_records;
```
