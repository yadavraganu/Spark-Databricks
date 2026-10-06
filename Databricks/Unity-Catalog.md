## 1. What Unity Catalog Is

Unity Catalog (UC) is Databricks' unified governance layer for data and AI assets — tables, views, volumes (files), ML models, functions, dashboards, and more — across every workspace attached to it. Before Unity Catalog, each workspace had its own isolated **Hive Metastore**, so governance, lineage, and permissions didn't carry across workspaces. Unity Catalog replaces that with a single, account-level governance plane that many workspaces can share.

What it gives you in one place:
- A consistent **three-level namespace** (`catalog.schema.table`) instead of Hive's two-level `schema.table`.
- Centralized, ANSI-SQL-style **GRANT/REVOKE** permissions, enforced consistently across SQL, Python, Scala, R, and BI tools.
- **Automatic lineage** down to the column level, across notebooks, jobs, and dashboards.
- **Auditing** of every access to every object.
- **Data discovery** (tags, comments, search) via Catalog Explorer.
- **Delta Sharing** for governed data sharing outside your organization, with no copying.
- A single place to govern non-tabular assets too: files (**Volumes**), ML models, functions, and AI/BI assets.

---

## 2. The Object Model

```
Metastore (one per region, shared by many workspaces)
└── Catalog
    └── Schema (a.k.a. database)
        ├── Table (managed or external)
        ├── View
        ├── Volume (governed access to files)
        ├── Function (UDFs, including ones used in ABAC policies)
        ├── Model (registered MLflow models)
        └── information_schema (auto-generated, read-only metadata schema)
```

Alongside this **securable hierarchy**, there are **non-data securables** that support governance but aren't part of the namespace themselves:
- **Storage credentials** — the cloud auth mechanism (IAM role, managed identity, service principal) UC uses to read/write cloud storage.
- **External locations** — a storage credential + a specific cloud path, registered so UC can govern access to it.
- **Connections** — for Lakehouse Federation (querying external systems like Postgres, MySQL, Snowflake, Redshift in place).
- **Shares / Recipients** — the Delta Sharing objects.
- **Clean rooms** — for privacy-safe multi-party data collaboration.

### Querying the namespace
```sql
SELECT * FROM production.sales.orders;   -- catalog.schema.table

USE CATALOG production;
USE SCHEMA sales;
SELECT * FROM orders;                    -- now resolves via defaults
```

### Privilege inheritance
Privileges flow **down** the hierarchy: granting `SELECT` on a catalog grants it on every schema and table underneath, unless more specific grants/denies override it at a lower level. `USE CATALOG` and `USE SCHEMA` are "pass-through" privileges — a user needs them on every level above an object just to *reach* it, even if they already have `SELECT` on the object itself.

```sql
GRANT USE CATALOG ON CATALOG production TO `data-team`;
GRANT USE SCHEMA ON SCHEMA production.sales TO `data-team`;
GRANT SELECT ON TABLE production.sales.orders TO `data-team`;
```

---

## 3. Metastores

- A metastore is the **top-level container** for all UC metadata (not the data itself) — one per cloud region, typically.
- A workspace must have exactly one metastore **attached** before it can use Unity Catalog at all.
- The metastore has a **root storage location** — a cloud bucket/container UC uses as the default location for managed tables that don't specify their own.
- Many workspaces across an account can attach to the same metastore, which is what makes cross-workspace governance and sharing possible in the first place.

```sql
DESCRIBE METASTORE;
```

```hcl
# Terraform
resource "databricks_metastore" "this" {
  name          = "production-metastore"
  storage_root  = "s3://unity-metastore-prod/"
  region        = "us-east-1"
}
```

---

## 4. Catalogs and Schemas

- **Catalogs** are the first namespace layer — typically one per environment (`dev`, `staging`, `prod`) or per major business domain.
- **Schemas** (databases) sit inside catalogs and group related tables/views/volumes/models — typically by data layer (`bronze`, `silver`, `gold`) or by team/subject area.
- Every workspace gets an auto-created **default workspace catalog**, bound only to that workspace unless you change the binding.
- Every metastore/catalog/schema automatically includes a read-only `information_schema` — the SQL-standard way to introspect UC metadata (tables, columns, grants, etc.) without hitting the REST API.

```sql
CREATE CATALOG production MANAGED LOCATION 's3://my-bucket/production/';

CREATE SCHEMA production.bronze;
CREATE SCHEMA production.silver;
CREATE SCHEMA production.gold;

-- Schema with its own storage location, overriding the catalog default
CREATE SCHEMA production.external_data
  MANAGED LOCATION 's3://my-bucket/external_data/';
```

**Workspace-catalog binding** (covered in depth separately) restricts which workspaces can see a given catalog at all — without it, every catalog in a shared metastore is visible from every attached workspace by default.

---

## 5. Tables: Managed vs. External

| | Managed Tables | External Tables |
|---|---|---|
| **Storage location** | UC-controlled, under the metastore/catalog/schema root | A path you specify (S3/ADLS/GCS), registered via an external location |
| **Format** | Delta (default) or, on newer runtimes, Iceberg via UniForm | Delta, Parquet, CSV, JSON, Avro, ORC, Iceberg, Hudi, and more |
| **Lifecycle** | `DROP TABLE` deletes the underlying data too | `DROP TABLE` only removes the UC registration — the files stay put |
| **When to use** | Default choice — simplest, UC handles optimization/storage for you | Data that must live at a specific path, or is shared with/produced by non-Databricks tools |

```sql
-- Managed table
CREATE TABLE production.gold.daily_revenue (
  date DATE,
  revenue DECIMAL(18,2)
);

-- External table
CREATE TABLE production.bronze.raw_events
  LOCATION 's3://my-bucket/raw/events/';
```

### Governed storage objects
```sql
-- Storage credential: the auth mechanism
CREATE STORAGE CREDENTIAL my_cred
  WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/unity-catalog-role');

-- External location: credential + path, now usable by UC
CREATE EXTERNAL LOCATION raw_data_loc
  URL 's3://my-bucket/raw/'
  WITH (STORAGE CREDENTIAL my_cred);
```

### Constraints and query optimization
UC tables support **primary key** and **foreign key** constraints for data-modeling purposes — but they're *informational only*, not enforced on write the way they would be in a traditional RDBMS:

```sql
ALTER TABLE production.gold.customers
  ADD CONSTRAINT pk_customer PRIMARY KEY (customer_id);

ALTER TABLE production.gold.orders
  ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id)
  REFERENCES production.gold.customers;
```

Two things this buys you beyond documentation:
- Catalog Explorer can render an **entity-relationship diagram** from declared keys.
- Adding `RELY` to a primary key tells the optimizer to trust the constraint and use it for query planning — e.g., eliminating a redundant `DISTINCT` or a join it can prove is unnecessary, which can meaningfully speed up downstream queries.

**Predictive optimization** is UC's self-tuning maintenance: once enabled (at the metastore, catalog, schema, or table level, with child levels inheriting unless overridden), Databricks automatically decides when to run `OPTIMIZE` and `VACUUM` on managed Delta tables based on actual access patterns, instead of you scheduling maintenance jobs yourself.

```sql
ALTER TABLE production.gold.daily_revenue SET PREDICTIVE OPTIMIZATION ENABLED;
```

---

## 6. Volumes

Volumes govern **non-tabular files** (images, PDFs, model checkpoints, CSVs you want to land before loading, arbitrary binary data) under the same three-level namespace and permission model as tables — something Hive Metastore and raw DBFS mounts never offered.

```sql
CREATE VOLUME production.bronze.landing_zone;
```

```python
# Access like a path, governed by UC permissions
df = spark.read.csv("/Volumes/production/bronze/landing_zone/file.csv")
```

Managed volumes use UC-controlled storage; external volumes point at a registered external location, same pattern as tables.

---

## 7. The Permission Model

UC uses a standard ANSI-SQL-style privilege model with principals being users, groups, or service principals.

```sql
GRANT SELECT ON TABLE production.sales.orders TO `analysts`;
GRANT MODIFY ON TABLE production.sales.orders TO `etl-service-principal`;
GRANT ALL PRIVILEGES ON SCHEMA production.sales TO `sales-admins`;
REVOKE SELECT ON TABLE production.sales.orders FROM `contractor-group`;
```

Common privileges: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE MODEL`, `CREATE FUNCTION`, `SELECT`, `MODIFY`, `ALL PRIVILEGES`.

### Ownership and admin roles
Every securable object — catalog, schema, table, volume, function, and so on — has an **owner** (a user, group, or service principal). The owner always implicitly holds full management rights on that object, on top of whatever's explicitly granted, and can transfer ownership to someone else:

```sql
ALTER TABLE production.sales.orders OWNER TO `data-engineering-leads`;
```

Governance of UC itself is layered:
- **Account admins** manage metastores at the account level (creating them, assigning them to workspaces).
- **Metastore admins** have broad administrative rights across an entire metastore — useful for break-glass access and cross-catalog governance, but a role to grant sparingly.
- **Catalog/schema/object owners** govern just their own objects and everything beneath them.

Best practice is to keep the metastore-admin group small and push day-to-day governance down to catalog and schema owners, who understand their own data domain.

### Fine-grained access control (row/column level)
Beyond table-level grants, UC supports:
- **Column masks** — a function that redacts or transforms a column's values based on who's querying.
- **Row filters** — a function that restricts which rows a query returns, based on the querying user.

```sql
CREATE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('hr-admins') THEN ssn ELSE '***-**-****' END;

ALTER TABLE production.hr.employees
ALTER COLUMN ssn SET MASK mask_ssn;
```

### Attribute-Based Access Control (ABAC)
A newer, more scalable alternative to writing a column mask/row filter per table: policies are written **once** against **governed tags** (account-managed tags with their own access controls), and they apply dynamically to every asset carrying that tag — so tagging a new column `pii:true` automatically pulls it under existing masking policies, with nothing to re-wire per table.
- Two policy types: **column masking policies** and **row filter policies**, both UDF-backed.
- Policies are created at catalog, schema, or table level and apply to matching tagged assets underneath.
- Requires governed tags (not ordinary free-form tags) and a recent enough compute runtime with fine-grained access control filtering enabled.
- Can't be applied directly to views — only to the base tables views read from — and there's no information-schema table for policies; you inspect them via the REST API.
- Tags don't propagate from a table down to its columns automatically — a `MATCH COLUMNS` condition only matches tags set directly on the column, not tags on the parent table.

```sql
-- Mask every column tagged 'ssn', anywhere in the catalog, using one policy
CREATE FUNCTION mask_ssn(val STRING) RETURNS STRING
RETURN CASE WHEN is_account_group_member('hr-admins') THEN val ELSE '***-**-****' END;

CREATE POLICY mask_pii
  ON CATALOG production
  COMMENT 'Mask columns tagged ssn for non-HR-admins'
  COLUMN MASK mask_ssn
  MATCH COLUMNS has_tag('ssn') AS ssn_col
  ON COLUMN ssn_col;
```

New columns tagged `ssn` anywhere under the `production` catalog are covered automatically — nothing to re-wire per table.

---

## 8. Tags and Data Discovery

Tags are key-value metadata you can attach to catalogs, schemas, tables, columns, volumes, and models.

- **Ungoverned (regular) tags** — free-form, anyone with permission on the object can add them; good for personal/team categorization.
- **Governed tags** — defined and controlled at the account level via **tag policies**, so there's one authoritative `domain:finance` tag rather than everyone spelling it differently. Governed tags are what ABAC policies key off.

```sql
ALTER TABLE production.sales.orders SET TAGS ('domain' = 'sales', 'pii' = 'false');
ALTER TABLE production.hr.employees ALTER COLUMN ssn SET TAGS ('pii' = 'true');
```

Catalog Explorer (the UI) uses tags, comments, and names for full-text search across all governed assets, so consistent tagging is what makes data actually discoverable rather than just governed.

---

## 9. Lineage

UC automatically captures **column-level lineage** across:
- Notebooks, jobs, and Lakeflow/DLT pipelines
- Dashboards and SQL queries
- `COPY INTO`, `INSERT`, `MERGE`, and streaming writes

No manual annotation required — UC derives lineage by observing actual query execution. This answers "what feeds this table" and "what would break if I change this column" without separate tooling, and is visible directly in Catalog Explorer or queryable via the lineage REST API / system tables.

---

## 10. Auditing and System Tables

Every UC action (query, grant, table creation, access denial) is captured as an audit event. These are surfaced through **system tables** — UC-managed, queryable Delta tables under the `system` catalog — rather than requiring you to ship logs externally:

```sql
SELECT * FROM system.access.audit
WHERE action_name = 'getTable'
ORDER BY event_time DESC;

SELECT * FROM system.billing.usage;          -- cost/usage tracking
SELECT * FROM system.lineage.table_lineage;  -- lineage, queryable as SQL
```

This turns governance questions ("who queried this table last month," "what's driving our DBU spend") into ordinary SQL against tables you already have permission-controlled access to.

---

## 11. Delta Sharing

Delta Sharing is UC's **open protocol** for sharing live data with other organizations (or other UC metastores) **without copying it** — the recipient reads directly from your governed storage.

```sql
CREATE SHARE sales_share;
ALTER SHARE sales_share ADD TABLE production.sales.orders;

CREATE RECIPIENT partner_org USING ID 'partner-sharing-identifier';
GRANT SELECT ON SHARE sales_share TO RECIPIENT partner_org;
```

- Recipients don't need to be on Databricks at all — Delta Sharing has open-source connectors (Python, Spark, Power BI, Tableau, etc.).
- **Databricks-to-Databricks sharing** additionally carries over lineage, and recipients can layer their own ABAC policies on top of shared tables for their own governance.
- This is also the mechanism behind the **Databricks Marketplace**.

---

## 12. Lakehouse Federation

UC can govern **queries against external databases** (Postgres, MySQL, Snowflake, Redshift, SQL Server, BigQuery, and others) without migrating the data first, via **Connections**:

```sql
CREATE CONNECTION pg_conn TYPE postgresql
OPTIONS (host 'db.example.com', port '5432', user 'reader', password secret('scope','pg_pw'));

CREATE FOREIGN CATALOG pg_catalog USING CONNECTION pg_conn
OPTIONS (database 'analytics');
```

Once federated, the external database's tables show up in the UC namespace and are governed by the same GRANT/REVOKE model as native UC tables — useful for querying across systems during a migration, or for data that has to stay in its source system for operational reasons.

### Credential vending
UC isn't limited to governing access from Databricks compute. Through **credential vending**, UC can hand out short-lived, scoped cloud credentials to external engines — a local Spark session, DuckDB, or any client speaking the open Iceberg REST catalog API — so they can read UC-governed data directly from cloud storage while UC's permissions and audit logging still apply. This is what lets non-Databricks tools participate in UC governance instead of bypassing it.

---

## 13. Clean Rooms

For privacy-safe collaboration between organizations: multiple parties contribute data into a secure, UC-governed clean room where they can run **jointly-approved** queries (e.g., overlap analysis, aggregate stats) without either party ever seeing the other's raw, row-level data.

---

## 14. Migrating from Hive Metastore

Databricks provides tooling (`SYNC` command, the UC migration assistant) to move Hive tables into UC:

```sql
SYNC TABLE production.sales.orders FROM hive_metastore.sales.orders;
SYNC SCHEMA production.sales FROM hive_metastore.sales;
```

- `SYNC` is largely non-destructive for external tables (it registers them, the underlying files don't move).
- Managed Hive tables typically need to be **cloned or deep-copied** into UC-managed storage, since UC manages its own storage layout.
- Common approach: migrate schema-by-schema, run both metastores in parallel briefly, cut over consumers (jobs, dashboards, BI tools) once validated, then decommission the Hive references.

---

## 15. Open-Source Unity Catalog

Separate from the governed service described throughout this guide, Databricks has open-sourced **Unity Catalog** as an Apache-licensed standalone project — a REST-based catalog server implementing the same open APIs (including the Iceberg REST catalog protocol) that any engine can run and connect to, independent of the Databricks platform. It's worth distinguishing the two when the topic comes up:
- **Databricks-managed Unity Catalog** (everything above) — the fully governed, account-integrated service with ABAC, lineage, audit, Delta Sharing, and so on.
- **Unity Catalog OSS** — the open-sourced catalog core, usable outside Databricks entirely, aimed at giving any engine a common, vendor-neutral catalog layer.

---

## 16. Common Governance Patterns

**Environment isolation**
```
production catalog  → bound only to prod workspace
dev catalog          → bound only to dev workspace(s)
```

**Medallion architecture inside one catalog**
```
production.bronze   → raw, minimally governed, restricted access
production.silver    → cleaned, broader internal access
production.gold      → curated, widest internal + BI-tool access
```

**Role-based access via groups, not individuals**
```sql
GRANT SELECT ON SCHEMA production.gold TO `bi-analysts`;
GRANT ALL PRIVILEGES ON SCHEMA production.bronze TO `data-engineers`;
```
Always grant to groups/service principals rather than individual users — it's the difference between updating one group membership and rewriting grants across every object when someone joins, leaves, or changes role.

**PII handling**
```
1. Tag sensitive columns (pii=true) — governed tag, account-controlled
2. Write one ABAC masking policy keyed on that tag at the catalog level
3. New tables with that tag are protected automatically — no per-table wiring
```

---

## 17. Quick Reference

| Concept | SQL Keyword / Object |
|---|---|
| Top-level container | `METASTORE` |
| First namespace layer | `CATALOG` |
| Second namespace layer | `SCHEMA` |
| Managed storage auth | `STORAGE CREDENTIAL` |
| Governed cloud path | `EXTERNAL LOCATION` |
| File governance | `VOLUME` |
| Row/column security (manual) | Masking function / row filter |
| Row/column security (scalable) | `CREATE POLICY ... MATCH COLUMNS has_tag(...)` |
| Object ownership | `ALTER ... OWNER TO` |
| Data modeling keys (informational) | `ADD CONSTRAINT ... PRIMARY KEY / FOREIGN KEY` |
| Self-tuning maintenance | `SET PREDICTIVE OPTIMIZATION ENABLED` |
| Cross-org sharing | `SHARE` / `RECIPIENT` |
| External DB querying | `CONNECTION` / foreign catalog |
| Credentials for external engines | Credential vending (Iceberg REST, etc.) |
| Metadata introspection | `information_schema` |
| Audit/lineage/usage data | `system` catalog |

---

*Sources: Databricks and Microsoft Learn official Unity Catalog documentation, including the attribute-based access control (ABAC) and governed tags documentation.*
