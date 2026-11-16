## 1. What a SQL Warehouse Is

A **SQL warehouse** is a compute resource for querying and exploring data on Databricks. It powers Databricks SQL: the query editor, Catalog Explorer, AI/BI dashboards, alerts, Genie, JDBC/ODBC connections from BI tools, and the SQL APIs.

- Most users only *use* warehouses that an admin configured. Creating or configuring them needs elevated permissions.
- "SQL endpoints" is the old name; they were renamed SQL warehouses in 2023. Both terms mean the same thing.
- Databricks **recommends serverless SQL warehouses whenever they're available**.
- Databricks creates a small **Starter Warehouse** automatically to help you get going. You can edit or delete it.
- You can also attach a notebook to a **pro or serverless** SQL warehouse (with some limitations).
- SQL warehouses **do not support credential passthrough**. Use Unity Catalog for governance. Admins can optionally add custom data access configurations alongside (or instead of) Unity Catalog.

## 2. The Warehouse Types

There are three classic-lineage types plus one new specialized type.

| Feature | Serverless | Pro | Classic |
|---|---|---|---|
| Photon engine | ✅ | ✅ | ✅ |
| Predictive IO | ✅ | ✅ | ❌ |
| Intelligent Workload Management (IWM) | ✅ | ❌ | ❌ |
| Where compute runs | Databricks-managed serverless plane | **Your** AWS account | **Your** AWS account |
| Typical startup | **2–6 seconds** | ~4 minutes | ~4 minutes |
| Scaling responsiveness | Rapid up and down | Slower, manual cluster limits | Slower, manual cluster limits |

What the performance features mean:
- **Photon** — Databricks' vectorized query engine; speeds up SQL and DataFrame calls and lowers cost per workload.
- **Predictive IO** — a set of features that speed up *selective scans*.
- **Intelligent Workload Management** — AI-powered prediction and dynamic resource management. It reacts to real demand instead of using static thresholds.

### When to choose each

- **Serverless** — best startup, the most efficient IO, smart handling of spiky demand, rapid autoscaling when queries queue. Good for ETL, BI and exploratory analysis. Lower total cost of ownership because capacity is provisioned and released automatically.
- **Pro** — choose it when serverless isn't available in your region, or when you have **custom-defined networking** and need the warehouse inside your own network (federation to databases in your cloud or on-premises, event buses, hybrid architectures).
- **Classic** — entry-level performance for interactive exploration with core Databricks SQL features.

### Lakehouse Real-Time (Lakehouse//RT) **(Beta)**
A specialized serverless warehouse type for **sub-second, high-concurrency read queries**. See §10.

### Which type do you get by default?

| Situation | Default type |
|---|---|
| UI, region supports serverless and requirements are met | **Serverless** |
| API with default parameters | **Classic** (to get serverless via API, set `enable_serverless_compute` to `true` **and** `warehouse_type` to `pro`) |
| Workspace uses a *legacy external Hive metastore* | Serverless **not supported**; defaults to pro (UI) or classic (API) |
| Region doesn't support serverless | Pro (UI) or classic (API) |

Databricks advises **always setting these API fields explicitly**, especially across many workspaces, to avoid ambiguity. Before creating a serverless warehouse in some regions there may be required admin steps (for example accepting updated terms).

## 3. Using Warehouses Day to Day

- Warehouses you can access appear in the compute drop-down of workspace UIs that support SQL warehouse compute: the query editor, Catalog Explorer, dashboards, and others. The **SQL Warehouses** page lists them, running ones first.
- **Starting:** running a query against a stopped warehouse starts it automatically (if you have access). To start one manually you need at least **CAN MONITOR**.
- A stopped warehouse **auto-restarts** when:
  - you run a query against it,
  - a job assigned to it is scheduled to run,
  - a JDBC/ODBC connection is made to it,
  - a dashboard tied to it (as a dashboard-level warehouse) is opened.
- **Third-party BI:** Power BI and Tableau are the documented examples; many other BI and visualization tools connect via the standard drivers.
- **Developer tools:** SQL Warehouses REST API, Databricks SQL Connector for Python, Databricks SQL CLI, VS Code SQLTools driver, DataGrip, DBeaver, SQL Workbench/J.
- **Contact an admin** if you can't connect to any warehouse, the warehouse is stopped and you can't start it, or you can't access tables/data.

### User-level default warehouse
You can choose which warehouse is selected automatically when you run queries. It overrides any workspace-level default. From the compute drop-down in any Databricks SQL surface (SQL editor, AI/BI dashboards, Genie, Alerts, Catalog Explorer), choose **Customize your default warehouse**, then pick:
- **Workspace default**
- **Last selected**
- **Custom default** (a specific warehouse of your choosing)

## 4. Creating a Warehouse

**Requirements**
- You must be a **workspace admin** or have **unrestricted cluster creation** permission.
- For serverless, region support and possibly some one-time setup steps apply.

**UI flow:** SQL Warehouses → **Create SQL warehouse** → name it → (optional) settings and advanced options → **Create**. The new warehouse starts automatically. You can also create warehouses with the SQL Warehouses API or Terraform.

### Settings

| Setting | What it does | Defaults |
|---|---|---|
| **Cluster size** | Size of driver and worker count per cluster; bigger lowers query latency | Default **X-Large** |
| **Auto Stop** | Stops the warehouse after it's idle for N minutes. Idle warehouses still accrue DBU and cloud charges until they stop | **Pro/classic:** default 45 min, minimum 10. **Serverless:** default 10 min, minimum **5 in the UI**, as low as **1 via the API** |
| **Scaling** | Minimum and maximum number of clusters | Default min = max = 1. Databricks recommends **one cluster per 10 concurrent queries** |
| **Type** | Serverless, pro, or classic | Serverless if enabled |

**Cluster recycling:** to keep performance healthy, Databricks **recycles clusters running longer than 24 hours**. A new cluster comes up, new queries move to it, and the old one is decommissioned once its queries finish. During the transition you may briefly see *more* clusters than your configured maximum (e.g. 2 when max is 1). If a query keeps running on the old cluster past 24 hours it may eventually be **forcefully terminated**, so design long-running work to finish within 24 hours or split it up.

### Advanced options
- **Tags** — key-value pairs to attribute cloud cost to users and groups.
- **Unity Catalog** — on by default for new warehouses if the workspace uses Unity Catalog.
- **Channel** — *Current* (standard) or *Preview* (test new functionality before it becomes the standard). Databricks recommends **against Preview for production**. Because only workspace admins can see a warehouse's channel, put "preview" in the warehouse's name so others don't use it for production.
- **Statement timeout (Beta)** — per-warehouse query timeout, set only through the API (see §7).

### Managing an existing warehouse
Workspace admins and users with **CAN MANAGE** can start/stop, edit, set permissions, **upgrade to serverless**, or delete. Deleted warehouses can be restored by contacting Databricks **within 14 days**.

### Permission levels

| Level | What it allows |
|---|---|
| **Can view** | See the warehouse, query history and query profiles. **Cannot run queries** |
| **Can use** | Run queries |
| **Can monitor** | Run queries **and** monitor the warehouse, query history and profiles; can manually restart it. For power users troubleshooting performance |
| **Can manage** | Responsible for sizing and spend decisions; full management |
| **Is owner** | Automatically applied to the creator |

## 5. Sizing and Scaling

### 5.1 Cluster sizes (classic and pro)

A "cluster" is the unit of compute. The *size* decides the driver and worker count; scaling decides *how many clusters*. For classic and pro, workers are all `i3.2xlarge`:

| Cluster size | Driver instance | Workers |
|---|---|---|
| 2X-Small | i3.2xlarge | 1 |
| X-Small | i3.2xlarge | 2 |
| Small | i3.4xlarge | 4 |
| Medium | i3.8xlarge | 8 |
| Large | i3.8xlarge | 16 |
| X-Large | i3.16xlarge | 32 |
| 2X-Large | i3.16xlarge | 64 |
| 3X-Large | i3.16xlarge | 128 |
| 4X-Large | i3.16xlarge | 256 |
| 5X-Large **(Public Preview)** | i3.16xlarge | 512 |

Notes:
- 5X-Large is in Public Preview for pro and serverless warehouses; workspace admins can control access from the **Previews** page.
- Serverless sizes use the same names but may use **different instance types**. The price/performance ratio is generally similar to pro and classic.
- Classic and pro have a **fixed limit of one cluster per 10 concurrent queries**.
- Pro and classic use **Auto-AZ**: the availability zone is chosen automatically from available IPs in the workspace subnets, and it retries in other zones if AWS reports insufficient capacity.

### 5.2 Sizing a serverless warehouse
- **Start with a single, larger warehouse** and let serverless features manage concurrency. It's usually more efficient to size *down* than to start small and climb.
- **If queries spill to disk, increase the cluster size** (check the query profile).
- **For many concurrent queries, set a sufficient maximum number of clusters.** Watch **Peak Queued Queries**.
- The maximum number of queued queries is **1,000** for every warehouse type.

### 5.3 How serverless scales (Intelligent Workload Management)
1. A new query arrives; IWM **predicts its resource needs** and checks capacity.
2. If capacity exists, it **starts immediately**; otherwise it **queues**.
3. IWM **watches the queue**; rising wait times make the autoscaler quickly **add clusters**.
4. When demand falls, IWM **scales down** while keeping enough capacity for recent peaks (low load for about 15 minutes triggers downscaling).

Result: rapid upscaling for low latency, high throughput (queries admitted as soon as hardware is free), and quick downscaling to save cost.

### 5.4 How classic and pro scale
Classic and pro use a **manual scaling model**: you pick min and max clusters. Autoscaling adds clusters based on the **estimated time to finish all running and queued queries**:

| Estimated query load | Clusters added |
|---|---|
| 2–6 minutes | +1 |
| 6–12 minutes | +2 |
| 12–22 minutes | +3 |
| Over 22 minutes | +3, then +1 for every additional 15 minutes |

Extra rules: a query waiting in the queue for **5 minutes** triggers a scale-up, and if load stays low for **15 consecutive minutes** the warehouse scales down to the minimum needed for that period's peak.

Queries queue when all clusters are at capacity or the warehouse is in the `STARTING` state.

### 5.5 Quick sizing decision guide

| Symptom | Likely fix |
|---|---|
| Queries spill to disk / "Bytes spilled to disk" in the profile | **Increase cluster size** |
| `Peak Queued Queries` consistently above 0 | **Raise max clusters** (or increase size) |
| Slow startup with sporadic use | Move to **serverless** |
| Idle cost too high | **Lower Auto Stop** |

## 6. Controlling Runaway Queries: Statement Timeout **(Beta)**

You can set a timeout on a **single warehouse** so long-running queries on it are cancelled after a duration you choose. Useful when different warehouses serve workloads with different expected runtimes.

- A **workspace admin must enable** the *Warehouse Statement Timeout* preview from the **Previews** page.
- Set it with the `statement_timeout` field in the **Create** or **Update** warehouse API (it is **API-only**, not in the UI).
- **Precedence** when set at several scopes: **session** beats **warehouse**, which beats **workspace**.

## 7. Monitoring

### 7.1 The Monitoring tab
Click a warehouse, then **Monitoring**:
- **Live statistics** — status, running queries, queued queries, current cluster count (can exceed one only if scaling is configured).
- **Time-range filter** — defaults to 8 hours; also 24 hours, 7 days, 14 days or a custom range.
- **Peak query count chart** — max concurrent (running + queued) queries per 5-minute window; excludes metadata queries.
- **Completed query count chart** — completed, canceled and failed queries, averaged per minute.
- **Running clusters chart** — clusters allocated over time (can briefly exceed the max during recycling).
- **Query history table** — start time, duration (hover for scheduling vs. running split), fetch time, query source (pipeline, Genie, dashboard…), and user. Filter by user, duration, status, statement type.

### 7.2 Activity details
Turn on **Activity details** in the Running clusters chart to see *why* the warehouse was up. Data refreshes every 5–10 minutes.
- **Query activity** — running, queued or recently finished queries.
- **Other activity** — fetching results, or open sessions with no active query.
- **Ready** — up and idle, with no query or session activity.

### 7.3 Why is my warehouse still running with nothing executing?
Warehouses stay active while queries are **running or fetching results**. Large result sets, slow clients, or queries a client never closed can keep it awake for minutes.
- Click the **search icon next to Clusters** to list queries in the *fetching* state.
- Open the query's **Query Profile** and click **Stop** next to *Result fetching by client* to end it.
- Gray bars in the Activity-details chart mark fetching time; click one to filter the history table.

### 7.4 Monitoring with system tables

| System table | What it holds |
|---|---|
| `system.compute.warehouse_events` | Start, stop, scale-up and scale-down events |
| `system.compute.warehouses` | Snapshots of warehouse configuration |
| `system.query.history` | Details of every query on SQL warehouses |
| `system.billing.usage` | Billing records for all Databricks usage |

Requirements: access to system tables, and mostly Unity Catalog enabled on the account. Example queries (replace `<warehouse-id>`):

```sql
-- Slowest queries in the last day
SELECT statement_id, executed_by, total_duration_ms,
       waiting_at_capacity_duration_ms, execution_duration_ms, statement_text
FROM system.query.history
WHERE compute.warehouse_id = '<warehouse-id>'
  AND start_time >= NOW() - INTERVAL 1 DAY
ORDER BY total_duration_ms DESC
LIMIT 50;
```

```sql
-- Queries waiting for capacity (signal to raise max clusters)
SELECT statement_id, executed_by, total_duration_ms,
       waiting_at_capacity_duration_ms, execution_duration_ms
FROM system.query.history
WHERE compute.warehouse_id = '<warehouse-id>'
  AND start_time >= NOW() - INTERVAL 7 DAY
  AND waiting_at_capacity_duration_ms > 0
ORDER BY waiting_at_capacity_duration_ms DESC
LIMIT 50;
```

```sql
-- Queries with disk spill (signal to increase size)
SELECT statement_id, executed_by,
       spilled_local_bytes / (1024 * 1024) AS spilled_mb,
       read_bytes / (1024 * 1024) AS read_mb, total_duration_ms
FROM system.query.history
WHERE compute.warehouse_id = '<warehouse-id>'
  AND start_time >= NOW() - INTERVAL 7 DAY
  AND spilled_local_bytes > 0
ORDER BY spilled_local_bytes DESC
LIMIT 50;
```

```sql
-- Daily DBUs and estimated list cost for one warehouse
SELECT usage_date, sku_name,
       ROUND(SUM(usage_quantity), 2) AS total_dbus,
       ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS estimated_list_cost
FROM system.billing.usage
  LEFT JOIN system.billing.list_prices
    ON usage.sku_name = list_prices.sku_name AND price_end_time IS NULL
WHERE usage_metadata.warehouse_id = '<warehouse-id>'
  AND usage_date >= NOW() - INTERVAL 30 DAY
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC;
```

Other documented examples cover performance trends over time (daily counts, p50/p95 duration, queue wait), the most active users, the top client applications (job, dashboard, alert, notebook, Genie, SQL editor), failed or canceled queries, and a query that lines up scaling events with query volume to find cost savings. You can attach **alerts** to any of these.

**Reading the signals:** queue wait (`waiting_at_capacity_duration_ms`) means *add clusters*; disk spill means *add size*.

## 8. Lakehouse Real-Time (Lakehouse//RT) **(Beta)**

A serverless warehouse type for **sub-second SQL reads at high concurrency** — serving data to custom applications, operational analytics, and dashboards with hundreds to thousands of concurrent users. It queries **Unity Catalog** tables in **Delta Lake or Apache Iceberg**. It is in active development; the feature set and performance characteristics will change before GA, and you need your Databricks account team to enable it.

### Enabling and creating
1. Be in a supported region and have the Beta enabled (workspace admin → **Previews** → search *Lakehouse RT*).
2. **Compute → SQL Warehouses → Create → Real-Time.**
3. Choose a **Query size** (Small, Medium, Large, X-Large), the **Autoscaling** maximum (in DBUs), an **Auto stop** value, and a name.

Permissions are the same as ordinary warehouses (Can use / Can monitor / Can manage). You **cannot upgrade** an existing warehouse to Lakehouse//RT or downgrade it to another type.

### Scaling model (different from serverless SQL)

| Setting | Meaning |
|---|---|
| **Query size** | Maximum compute one query can use; also the **minimum compute the warehouse runs on and the minimum you're billed** while it's up |
| **Autoscaling** | Maximum compute the warehouse can scale to, **in DBUs, not clusters**; independent of query size; adds and removes just the compute needed |
| **Auto stop** | Works like serverless SQL |

You can pair a small query size with a high maximum to get high concurrency without giving each query more compute.

### Best practices
- **Validate on serverless SQL first** — queries should already run in a few seconds.
- Use **Unity Catalog managed tables** with **predictive optimization** and **liquid clustering**.
- Make queries **selective**: filter early, select only needed columns, lean on aggregations. For complex or slow joins, use **materialized views** to pre-aggregate.
- Confirm your SQL is **ANSI-compliant** read-only SQL.

### Connectivity
Lakehouse//RT accepts **only Statement Execution API connections**; the legacy Thrift protocol gets a `501` error. Ways to connect:
- Call the **Statement Execution API** directly.
- **Python connector:** `use_kernel=True`; **Node.js driver:** `useKernel: true`; **Go driver:** build with the `databricks_kernel` tag and `WithUseKernel(true)`.
- **JDBC:** `UseThriftClient=0` in the URL.
- **Power BI ADBC driver:** `Implementation=2.0` and `Protocol=REST(SEA)` under Advanced options.
- **Databricks OSS ODBC driver (Beta)** — no extra configuration. The **Simba Spark** and **Databricks ODBC** drivers are *not* supported.

### Supported
- Tools: SQL editor, SQL notebooks, AI/BI dashboards, Catalog Explorer, Alerts.
- Tables: **managed** Delta and Iceberg tables, **materialized views and streaming tables**, **metric views**.

### Limitations (summary)

| Area | Not supported |
|---|---|
| Tools | Genie, Genie Agents, Jobs tasks |
| Tables | System tables, Delta Sharing tables, UC default storage, UC **external** tables (not yet); Hive metastore, foreign tables / Lakehouse Federation, temporary tables, CSV/JSON/Avro/Parquet/ORC/text formats |
| SQL | Any **write, DDL, GRANT/REVOKE**, scripting, stored procedures, transactions, `OPTIMIZE`/`ANALYZE`/`VACUUM`/`REFRESH`; `GEOGRAPHY`/`GEOMETRY` types; AI functions, Python UDFs, spatial and XML/XPath functions; **ABAC** (row filters and column masks) |
| SQL semantics | **ANSI mode only**, cannot be turned off — non-ANSI casts raise errors instead of returning `NULL`. Use explicit `CAST` |
| Network | Outbound Private Link (serverless) and serverless egress control not yet supported; legacy firewall configurations may return `403 Forbidden` |
| Compliance | Compliance security profiles not supported |

### Dashboards on Lakehouse//RT
Best for **new** dashboards built within the limits; migrating existing ones is **not recommended** (they often rely on Genie, external tables, or metric view materialization). Create the warehouse, grant **Can use**, select it as the dashboard's warehouse, then publish.

**Guardrails** (hard limits, may change before GA): query runtime **2 minutes**; statement timeout **1 minute** by default (configurable up to the 2-minute cap); shuffle bytes written per stage **300 GB**; spill **30 GB**; file listing after pruning **10,000 files** (adjustable up to 50,000). Exceeding a guardrail returns an error rather than running slowly, so filter on partition or liquid-clustering columns and pre-aggregate with materialized views.

### Monitoring and pricing
- Queries show up in **query history**, **query profiles**, the **monitoring page** and the **query history system table**.
- Billing appears in billing system tables. The Real-Time page says the `sku_name` is `Lakehouse_Serverless`; the pricing page names the SKU **`LAKEHOUSE_SERVERLESS_COMPUTE`** — filter loosely on both when building cost queries.
- **Beta pricing example (per DBU):** `us-east-1`, `us-east-2`, `us-west-2` are **$0.55 Premium / $0.70 Enterprise**; other regions are higher (e.g. Frankfurt $0.66 / $0.84, Mumbai $0.58 / $0.74, São Paulo $0.87 / $1.11). Check the pricing page for your region.
- **30% introductory discount** until **January 31, 2027**, delivered as a lower DBU emission rate (10 DBUs of work bills as 7).

## 9. Putting It Together: Practical Recommendations

1. **Default to serverless.** Create one warehouse, size it generously, and let IWM scale.
2. **Pick pro only for a reason**: region without serverless, or you need the warehouse inside your own network.
3. **Right-size with evidence**: spill → bigger size; queue → more max clusters.
4. **Set Auto Stop deliberately**: shorter for bursty interactive use (serverless can go as low as 1 minute via API), and watch for *fetching* queries that keep warehouses alive.
5. **Separate workloads** across warehouses when they need different timeouts, permissions, cost attribution or channels, and **tag** every warehouse for cost reporting.
6. **Govern with permissions**: Can use for analysts, Can monitor for power users, Can manage only for those who own spend.
7. **Keep long queries under 24 hours** because of cluster recycling.
8. **Always set `enable_serverless_compute` and `warehouse_type` explicitly** when creating via API or Terraform.
9. **Automate monitoring** with the `system.compute.*`, `system.query.history` and `system.billing.usage` tables and attach alerts.
10. **Treat Lakehouse//RT as an application-serving tier** for selective, read-only, ANSI SQL, not a replacement for serverless SQL.

## 10. Interview-Style Quick Q&A

**Q: Serverless vs pro vs classic?**
A: All have Photon. Pro adds Predictive IO. Serverless adds Intelligent Workload Management and runs in Databricks' compute plane, starting in seconds. Pro and classic run in your AWS account and take about 4 minutes to start.

**Q: When would you still choose pro?**
A: When serverless isn't available in the region, or when the warehouse must sit inside your own network for federation or hybrid connectivity.

**Q: How does a classic or pro warehouse scale?**
A: Manually bounded min/max clusters; one cluster per 10 concurrent queries; adds clusters based on estimated time to clear the queue (2–6 min → +1, 6–12 → +2, 12–22 → +3, then +1 per extra 15 min); scales up if a query waits 5 minutes; scales down after 15 low-load minutes.

**Q: How do you tell whether to grow a warehouse or add clusters?**
A: Disk spill → larger size. Queue waiting (Peak Queued Queries above 0, `waiting_at_capacity_duration_ms`) → more max clusters.

**Q: Why might a warehouse stay up with no queries running?**
A: Clients still fetching results or holding open sessions. Use Activity details and stop the fetching query from the query profile.

**Q: Maximum queue depth?**
A: 1,000 queries for every warehouse type.

**Q: What happens at 24 hours?**
A: Clusters are recycled; queries still on the old cluster after 24 hours can be forcibly terminated.

**Q: Which type does the API create by default?**
A: Classic. Set `enable_serverless_compute=true` and `warehouse_type=pro` for serverless.

**Q: What is Lakehouse//RT?**
A: A Beta serverless warehouse for sub-second, high-concurrency, read-only queries on UC managed Delta/Iceberg tables, using DBU-based autoscaling and the Statement Execution API only.
