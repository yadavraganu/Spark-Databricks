## 1. What Serverless Compute Is

Serverless compute is a **Databricks-managed service** that gives you on-demand compute for notebooks, workflows and Lakeflow pipelines **without provisioning compute in your cloud account**. Databricks allocates and manages the resources, which means:
- faster start-up and scaling,
- minimal idle time,
- far less compute infrastructure for you to manage.

Serverless workloads are protected by multiple layers of security and are designed to be enterprise-ready.

**Availability**
- Available **by default in most workspaces**; no enablement needed. Workspaces with **Unity Catalog** automatically have access.
- **Legacy workspaces without Unity Catalog have no access** — upgrade to Unity Catalog first.

**Versionless product.** Databricks upgrades the serverless runtime automatically; everyone gets the same updates, rolled out over a short period. You always run the **latest runtime** (see the serverless release notes for the current version). You write code against an **environment version** instead of a Databricks Runtime version (see §6).

### Where it's used
| Workload | Page |
|---|---|
| Notebooks | §4 |
| Shared compute for Git folders **(Beta)** | §5 |
| Jobs (Lakeflow Jobs) | configured per job; see §8 and §9 for the performance modes |
| Lakeflow pipelines | configured per pipeline; serverless streaming guidance in §7 |
| AI Runtime (serverless GPU) **(Preview)** | §6 |

### Overview FAQ (condensed)

| Question | Answer |
|---|---|
| How are releases rolled out? | Versionless; automatic upgrades, same updates for all users, rolled out over a short period |
| Which version am I running? | Always the latest; see the serverless release notes |
| How do I estimate cost? | Benchmark a **representative workload**, then analyze the billing system table |
| How do I see DBU usage for a workload? | Query `system.billing.usage`; Databricks also offers a cost observability dashboard |
| Delay before charges appear? | **Up to 24 hours** in the billable usage system table |
| Why billing records for "serverless jobs" when I never ran any? | **Data quality monitoring** and **predictive optimization** run on serverless and bill under the serverless jobs SKU |
| Private repos? | Supported, but **authenticated repositories require a pre-signed URL** |
| Installing libraries for job tasks? | Use **environments** (see §6) |
| Custom data sources? | **No** — only sources supported by Lakehouse Federation |
| Networking? | Serverless runs in the Databricks-managed **serverless compute plane** (see the networking guide) |
| Configure with bundles? | **Yes**, Declarative Automation Bundles can configure jobs on serverless |
| Run from my laptop or app? | **Databricks Connect** can run workloads on serverless |

## 2. Governing Access: Serverless Compute Objects (Admins)

A **serverless compute object** is a resource type that defines the **permission boundary** for serverless workloads. Every workspace automatically has two:

| Default object | Governs |
|---|---|
| **Default Interactive Compute** | Notebooks, serverless GPU notebooks, and Databricks Connect |
| **Default Automated Compute** | Jobs, serverless GPU jobs, and Spark Declarative Pipelines on Lakeflow |

- By default **all workspace users have Can Use** on both, so existing workloads keep working.
- Default objects **cannot be renamed or deleted**.
- Workspace admins have **Can Manage** by default and can grant it to others.

### Permission levels
| Permission | Allows |
|---|---|
| **Can Use** | Run workloads on this compute |
| **Can Manage** | Run workloads **and** edit the compute's permissions |

You can also manage them with the **Permissions API**: the object type is `serverless-compute`, and each object is identified by its ID.

### Restricting access
1. **Compute → Serverless tab → kebab menu → Edit permissions** on the relevant default object.
2. Remove the **All Users** group (or whichever group contains everyone).
3. Add only the users, groups or service principals you want to authorize.

**What happens when access is revoked**
- **Interactive:** notebooks already attached fail to connect on the next cell run; the **Serverless** option disappears from the compute picker; Databricks Connect fails with an error saying the user lacks access to serverless.
- **Automated:** **all of that user's existing jobs and pipelines on serverless fail to run**, and they see an error when trying to run one.

**Before revoking automated access,** audit what the user owns:
```sql
SELECT *
FROM system.billing.usage
WHERE usage_date >= date_add(now(), -30)
  AND billing_origin_product IN ('JOBS', 'DLT')
  AND identity_metadata.run_as = '<user_email>';
```
Check `usage_metadata` for the affected job and pipeline IDs.

### Auditing usage by compute object
Serverless billing records carry `usage_metadata.serverless_compute_id`:
```sql
SELECT usage_metadata.serverless_compute_id,
       identity_metadata.run_as,
       SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE billing_origin_product IN ('JOBS', 'DLT', 'INTERACTIVE')
  AND usage_metadata.serverless_compute_id IS NOT NULL
  AND usage_date >= date_add(now(), -30)
GROUP BY 1, 2
ORDER BY 3 DESC;
```

### What access controls cover

| Covered | Not covered |
|---|---|
| Notebooks and serverless GPU notebooks | Databricks SQL (DBSQL) |
| Databricks Connect | Batch inference (`ai_query()`) |
| Jobs and serverless GPU jobs | Model Serving |
| Spark Declarative Pipelines on Lakeflow | Foundation Model API provisioned throughput endpoints |
| | Lakebase |
| | Databricks Apps |
| | Agent Evaluation and synthetic data |
| | Vector Search indexing |
| | Predictive Optimization |
| | Lakehouse Monitoring |
| | Fine-grained access control on dedicated compute (governed by access to that dedicated compute) |

For anything not listed, ask your Databricks account team.

### Limitations — read these before relying on it
- **Not a spend cap.** If there's a service disruption, **access checks can fail open**, so users who would be denied might start serverless workloads. Don't use this feature to guarantee a maximum bill; Databricks isn't responsible for costs incurred during such an event.
- Jobs or pipelines already configured for serverless **fail for any user who loses Can Use** on Default Automated Compute — review active workloads first.
- **Background compute** (system-initiated jobs) is **exempt** from these controls.

## 3. Rate Limits on Serverless Compute **(Private Preview)**

A **rate limit** caps how much an **automated** workload can spend by limiting **how far it can autoscale**. Admins create **custom automated compute objects**, each with a rate limit; the autoscaler won't scale the workload's **Spark executors** beyond the size set on the object.

**Getting access:** contact your Databricks account team to join the preview, then a workspace admin enables the **Serverless Compute Rate Limit** setting on the **Previews** page.

**Scope**
- Applies only to **automated** workloads (jobs and Spark Declarative Pipelines on Lakeflow). **Notebooks and Databricks Connect are not rate-limited.**
- Caps **executor autoscaling only** — **not** the driver, REPL VMs, GPUs, or materialized view / streaming table refreshes.
- The cap is **per workload**: each job and each pipeline using the object is capped independently (two jobs on the same object each get the full cap). **Within one job the cap is shared by all its tasks.**

**Requirements:** workspace admin or unrestricted cluster creation to manage objects; **Databricks Runtime 17.3.1 or above** (jobs use the latest automatically; for pipelines, opt in to the **preview channel** runtime).

### Sizes

| Size | Approximate cap |
|---|---|
| Small | ~60 DBUs/hour |
| Medium | ~120 DBUs/hour |
| Large | ~240 DBUs/hour |
| X-Large | ~480 DBUs/hour |
| 2X-Large | ~960 DBUs/hour |

These are **directional**; actual usage can fluctuate around them. **Default** applies the workspace default: **Medium for Premium-tier** workspaces, **Large for Enterprise-tier** — matching the existing default maximum autoscaling for automated serverless workloads.

### Create and use
1. **Compute → Serverless tab → Create serverless compute**, name it, choose a **Size** (Small to 2X-Large, or Default), **Create**. It appears with type **Automated**.
2. Grant users **Can Use** or **Can Manage**.
3. Select it for the workload:
   - **Jobs:** the job's **Compute** settings, or the compute's ID in the Jobs API (copy it with **Copy compute ID**).
   - **Pipelines:** choose the automated compute object in the Lakeflow editor.
4. A workload that doesn't name an object falls back to **Default Automated Compute**; if the user lacks access to it, the workload **fails**.

When throttling affects a workload, you can see the impact in query history (or the Lakeflow UI for pipelines).

### Limitations
- Caps executors only; other cost components aren't capped.
- DBU-per-hour caps are **estimates** and can be exceeded.
- At **2X-Large**, physical cluster size is capped at **256 executors**, so performance may fall short, especially if several 2X-Large workloads share a runtime cluster.
- The same workload can show **different DBU usage** under different sizes because of billing implementation details.
- You **can't set a rate limit on a default compute object** or edit its size.

## 4. Serverless Compute for Notebooks

- **Requirement:** Unity Catalog. If serverless interactive compute is enabled, **all workspace users** can use it with no extra permissions (unless an admin restricts Default Interactive Compute — see §2).
- **Attach:** compute drop-down → **Serverless**. New notebooks default to serverless on first run if no other compute is selected.
- For multi-file authoring in a Git folder, use **Git Folder Serverless** (§5).

### Query insights
After a cell runs, click **See performance**; then **See query profile** for a visualization. All serverless queries also appear in **query history**.

Limitations of query insights:
- Metrics update live, but the **complete profile exists only after the query terminates**.
- Only RUNNING, CANCELED, FAILED and FINISHED statuses.
- Running queries can't be canceled from query history (cancel from the notebook or job).
- No verbose metrics; **no profile download**; **no Spark UI**.
- Statement text shows only the **last line** run.

### Overspend protection
Serverless notebooks have a **default execution timeout of 2.5 hours (9,000 seconds)**; longer queries are canceled.
- **Workspace default:** admin **Settings → Compute → Serverless interactive → Serverless interactive execution timeout** (about 5 minutes to propagate).
- **Per notebook:** set `spark.databricks.execution.timeout`; it **overrides** the workspace setting.

### High memory **(Preview)**
Fixes out-of-memory errors in the notebook process. Options: **Standard 16 GB**, **High 32 GB** of total memory. It increases **REPL** memory, **not Spark session** memory, and has a **higher DBU emission rate**. Set it in the **Environment** pane → **Memory** → **High memory** → **Apply**. It also applies to notebook job tasks using the notebook's memory preference (the next job run picks up changes).

## 5. Git Folder Serverless **(Beta)**

One serverless compute resource **shared across notebooks and files in a Git folder**, with the environment managed by **`pyproject.toml`** at the folder root.

**Requirements**
- The project is in a **Git folder**, opened in the **Git folder editor**.
- If a root `pyproject.toml` exists, `environment_version` must be **5 or later**.

**How it behaves**
- Assets in the folder share one compute resource and one environment; **only assets inside that Git folder** can attach.
- **Python variables are not shared** between notebooks.
- A **web terminal** opened from any attached asset runs on the same compute.

**Use it:** workspace → Git folder → **Open in editor** → open a notebook or file → compute drop-down → **Git Folder Serverless** → run.

**Managing dependencies**
- Notebook commands: `%uv add <package>` adds to the root `pyproject.toml`, then `%uv sync` applies it and generates/updates `uv.lock`.
- Or edit `pyproject.toml` directly and click **Apply**:
```toml
[project]
name = "my-project"
version = "0.1.0"
dependencies = ["simplejson==3.18.1"]

[tool.databricks.environment]
environment_version = "5"
```
- If there's no `pyproject.toml`, the **Environment** pane has **Create pyproject.toml**.

**Collaboration:** **only the user who starts the compute can run workloads on it**; other users who open the folder are blocked. Databricks recommends each collaborator **clone the repo into their own personal Git folder** (separate compute and environment per clone) and exchange changes via branches, commits, pushes and pulls.

**Limitation:** compute sharing is scoped to a **serverless usage policy** — assets in one folder using different policies get **separate** compute resources.

## 6. The Serverless Environment (Libraries and Dependencies)

Serverless has **no compute policies and no init scripts**, so everything goes through the **Environment** side pane (notebooks) or **Configure environment** (job tasks). In Git Folder Serverless the environment comes from `pyproject.toml` instead.

### Base environments
| Option | What it is |
|---|---|
| **Standard** | Default, with Databricks-provided libraries |
| **ML** | Python and system packages from Databricks Runtime for ML — use to migrate classic ML workloads |
| **AI** | AI-optimized with ML libraries; **appears only when a GPU accelerator is selected** |
| **More** | Previous versions of Standard/ML/AI, and **Custom** (YAML spec) |
| **Workspace environments** | Compatible base environments configured by an admin |

Databricks recommends the **latest version**. Select: Environment pane → **Base environment** → **Apply**.

### Adding dependencies
- Any format valid in a **`requirements.txt`**; wheels or Python projects (a directory with `pyproject.toml` or `setup.py`) can live in **workspace files** (`/Workspace/...`, absolute path) or **UC volumes** (`/Volumes/<catalog>/<schema>/<volume>/<path>.whl`).
- **Apply** installs and **restarts the Python process**. The **Installed** tab and **pip logs** show what happened.
- **Never install PySpark** or anything that depends on it — it stops the session. Remove it and **reset the environment**.
- **CPU architecture isn't guaranteed** (`aarch64` or `x86_64`, can change between runs). Prefer **pure-Python wheels** (`py3-none-any`) or supply both architecture variants with a `platform_machine` environment marker.
- Dependencies are resolved **before your code runs**; code in a cell can't change how they're fetched, and compute-scoped environment variables (e.g. proxy settings) have no effect. For **private repositories**, put `index-url` / `extra-index-url` in the spec, or ask an admin to configure **default package repositories**.

### Caching and reset
The notebook's virtual environment is **cached**, so dependencies don't reinstall on reopen or after inactivity. Jobs benefit too — tasks with the same dependency set finish faster once the cache is warm in a run. If you change the implementation of a custom package used by a job, **update its version number** so jobs pick it up. To start clean, use **Apply ▸ Reset to defaults**; a new session does **not** clear the whole cache.

### Reusable environments
- **Export environment** (kebab menu) saves a spec to a **workspace file or UC volume** (needs write permission, otherwise a `Forbidden` error); reuse it via **Custom** in the Base environment drop-down.
- **Shared tools:** put helper code in a folder with a `pyproject.toml` (e.g. `helper_utils/` with a `helpers/__init__.py`), make it readable to others, and add `/Workspace/helper_utils` as a dependency.
- **Include in source file exports** stores the base environment and dependencies as **PEP 723** inline metadata, so the configuration survives Git folders and source-file downloads:
```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
```

### Other environment-pane settings
- **AI Runtime (serverless GPU) (Preview):** pick **Serverless GPU**, choose **A10** or **H100**, pick **Standard** or **AI** base environment, **Apply** and **Confirm**.
- **High memory (Preview):** see §4.
- **Serverless usage policy (Preview):** adds custom tags to usage for billing attribution; auto-selected if you have only one policy. Notebooks from Git or without a policy default to your **last chosen** policy.

### Job task environments
Each task runs in an **isolated environment**: base environment (sets Python/Scala runtime version and preinstalled libraries) plus any libraries you add (workspace files, UC volumes, public repositories, or `-r /Workspace/.../requirements.txt`). Only needed dependencies install.
- Notebook tasks default to **Notebook Environment**; override with a **Jobs Environment** (task → **Environment and Libraries**).
- Selecting **managed base environments** for jobs is **Beta** (otherwise you see an **Environment version** drop-down).
- Types: notebook, Python script/wheel, dbt, JAR tasks.
- **Compatibility:** GPU environments don't run on CPU; if you set a hardware accelerator at the **job** level you must also pick the base environment at the **job** level; changing a referenced notebook's compute type (CPU→GPU) can make tasks incompatible; with the API, **validation happens at runtime**, not job creation.
- Environment variables for job tasks: use the job-level environment-variables feature (Beta).

## 7. Streaming on Serverless Compute

**Two patterns**
- **Continuous** — never stops; latency in **seconds**.
- **Incremental (triggered)** — runs on a schedule, processes everything since the last run, then stops; latency in **minutes**.

### Choosing the right configuration
| Use case | Recommended | Why |
|---|---|---|
| Continuous low-latency streaming ETL | **Lakeflow pipelines in continuous mode** | Always-on design; **stream pipelining** runs microbatches concurrently; managed state gives automatic recovery |
| Incremental ingestion from cloud storage | **Auto Loader** in a Lakeflow pipeline (low latency) or in a **serverless job with `Trigger.AvailableNow()`** | Efficient new-file tracking; processes the backlog, then exits |
| SaaS or database CDC ingestion | **Lakeflow Connect** managed connectors | Fully managed, serverless ingestion pipelines, no code |
| SQL-native streaming over Delta | **Streaming tables** | Managed pipelines and refresh; standalone tables via `CREATE OR REFRESH STREAMING TABLE` get a dedicated auto-managed serverless pipeline and start populating immediately |
| Periodic micro-batches in a job/notebook | **Serverless job + `Trigger.AvailableNow()`** | Cost-efficient when minute-level freshness is fine; compute starts fast and exits when done |

Use **materialized views** instead if you want batch-semantics results with managed refresh.

**Continuous setup:** configure the pipeline as serverless, set pipeline mode to **continuous**, use streaming tables. Stream pipelining is on by default in serverless pipelines.

### Triggers
| Trigger | Notebooks/jobs |
|---|---|
| `Trigger.AvailableNow()` | ✅ recommended |
| `Trigger.Once()` | ✅ deprecated — migrate to `AvailableNow` |
| `Trigger.ProcessingTime(interval)` | ❌ |
| `Trigger.Continuous(interval)` | ❌ |
| **No trigger set** | ❌ (Spark defaults to `ProcessingTime("0 seconds")`) |

Unsupported or missing triggers raise **`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`**. Always set one explicitly. Because `AvailableNow` processes **all** available data, cap batches with **`maxFilesPerTrigger`** and/or **`maxBytesPerTrigger`** to avoid out-of-memory errors.

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.maxFilesPerTrigger", 1000)
   .load(source_path)
   .writeStream
   .trigger(availableNow=True)
   .option("checkpointLocation", checkpoint_path)
   .toTable("catalog.schema.target_table"))
```

### Not every "streaming" workload is a pipeline
A **websocket listener, per-user persistent connection, port listener or webhook receiver** is an **application**. Don't run it on a serverless job. Use **Databricks Apps** (FastAPI, Flask, Streamlit, Dash, Gradio, Node.js, Shiny), or terminate webhooks externally and land events in cloud storage or a message bus for a serverless streaming pipeline. For custom token/credential exchange use **service principals with OAuth** or the REST APIs.
- *Unbounded source → sink?* streaming pipeline.
- *Holds a client connection open?* application → Databricks Apps.

All streaming limitations of **standard access mode** also apply.

## 8. Migrating from Classic Compute

Most classic PySpark/SQL workloads migrate with **minimal or no change**. **R and Scala notebooks** can't migrate. Some features (like `df.cache`) aren't supported yet but won't need code changes once available.

### 8.1 Prerequisites
| Prerequisite | Action |
|---|---|
| Unity Catalog | Upgrade from Hive metastore if needed |
| Networking | Replace **VPC peering** with **NCCs, Private Link or firewall rules** |
| Cloud storage access | Use **UC external locations**; replace **DBFS mounts that use instance profiles** |

### 8.2 Update code

| Classic | Serverless |
|---|---|
| `dbfs:/...` paths | **UC volumes** (`/Volumes/...`) |
| Hive metastore tables | UC tables (three-level names) or HMS federation |
| IAM instance profiles | UC external locations |
| Custom JDBC JARs | Lakehouse Federation |
| RDD APIs (`sc.parallelize`, `rdd.map`) | DataFrame APIs |
| `df.cache()` / `persist()` | Remove (or materialize to a Delta table) |
| `spark.sparkContext`, `sqlContext` | `spark` directly |
| Hive variables (`${var}`) | `DECLARE VARIABLE` or Python f-strings |
| Unsupported Spark configs | Remove (serverless auto-tunes) |
| Init scripts, cluster libraries | Serverless environments |
| Maven/JARs | JAR tasks in jobs; PyPI for notebooks |
| Docker containers | Serverless environments for library needs |

Examples of RDD → DataFrame rewrites:
```python
from pyspark.sql import functions as F

# sc.parallelize + rdd.map
df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
result = df.select((F.col("value") * 2).alias("value")).collect()

# rdd.flatMap
df = spark.createDataFrame([("hello world",)], ["line"])
words = df.select(F.explode(F.split("line", " ")).alias("word")).collect()

# rdd.groupByKey
df = spark.createDataFrame([("a", 1), ("b", 2), ("a", 3)], ["key", "value"])
grouped = df.groupBy("key").agg(F.collect_list("value").alias("values")).collect()

# sc.textFile -> spark.read.text
df = spark.read.text("/Volumes/catalog/schema/volume/file.txt")
```
Other replacements: `sc.broadcast` → **broadcast join**; `sc.accumulator` → **DataFrame aggregation**; `sqlContext.sql` → `spark.sql`; and to replace `df.cache()` write the expensive result to a Delta table and read it back.

### 8.3 ANSI-mode behavior changes
Serverless enables **ANSI mode by default** (Databricks Runtime does so only from 17.0). If your classic compute is on an older runtime, previously working queries may now fail:

| Operation | ANSI off (classic) | Serverless |
|---|---|---|
| `CAST('abc' AS INT)` | `NULL` | `CAST_INVALID_INPUT` |
| Integer overflow | Wraps around | `ARITHMETIC_OVERFLOW` |
| Decimal overflow | `NULL` | `NUMERIC_VALUE_OUT_OF_RANGE` |
| Division by zero | `NULL` | `DIVIDE_BY_ZERO` |
| Malformed `parse_url` | `NULL` | `INVALID_URL` |

Use **`try_*`** functions (`try_cast`, `try_add`, `try_divide`, `try_parse_url`) rather than disabling ANSI — it surfaces data-quality issues a silent `NULL` hides. You can set `spark.sql.ansi.enabled=false`, but **ANSI rules for implicit casts on table inserts always apply** (`spark.sql.storeAssignmentPolicy` can't change).

### 8.4 Streaming fixes
Always set `.trigger(availableNow=True)` and a `checkpointLocation`; add `maxFilesPerTrigger`/`maxBytesPerTrigger` for large sources. For continuous needs use Lakeflow pipelines in continuous mode, or a continuous-schedule job with `AvailableNow`.

### 8.5 Test
1. **Quick compatibility test:** run on classic compute in **Standard access mode with Databricks Runtime 14.3 or above**. If it succeeds, it can migrate with no code changes.
2. **A/B comparison** (recommended for production): run classic (control) vs. serverless (experiment) and diff output tables.
3. Set supported Spark configs temporarily during testing, then remove them.

### 8.6 Performance mode, phases, cost
| Mode | Availability | Startup | Best for |
|---|---|---|---|
| Standard | Jobs, Lakeflow pipelines | 4–6 minutes | Cost-sensitive batch |
| Performance-optimized | Notebooks, jobs, pipelines | Seconds | Interactive, latency-sensitive |

**Phase the rollout:** (1) new workloads, (2) low-risk PySpark/SQL already on standard access mode with DBR 14.3+, (3) complex workloads needing code changes, (4) the rest, reviewed periodically. Billing is by **DBU consumption, not uptime** — validate cost with representative workloads first.

### 8.7 Migration agent **(Beta)**
- **Enable:** workspace admin opts into the **Compute Agent** preview. Check by typing `/compute` in **Genie Code**.
- **Run:** `/compute migrate to serverless` for **one notebook or job at a time**. It proposes each change individually; accepted changes are applied **in place** and can be rolled back.
- **What it reviews:** environment and libraries (`%pip`, init scripts, job cluster libraries, private indexes); environment variables (keeps secret references); data and storage paths (`dbfs:/`, mounts and local disk → UC volumes; asks when the target is ambiguous); Spark configs (comments out safe-to-drop, flags and removes unsupported); workload code (RDD → DataFrame, ANSI fixes); custom tags; and a suggested performance mode.
- **Jobs:** it **clones the job**, migrates the clone, enumerates every task (multi-task jobs supported, schedule preserved), and optionally **promotes** the clone onto the original (same ID, schedule, permissions) and deletes the clone. If you keep both, **pause one schedule** or the same trigger fires both and may duplicate writes.
- **It never runs or verifies your workload** — test it, especially production writes. Validate in performance-optimized mode (faster feedback), then switch modes.
- **Admin access recommended** (it inspects workspace-level global init scripts too); `CAN MANAGE` alone may miss libraries, settings or tags.
- **Blockers (it stops by default):** custom images, ML Runtime variants, runtimes **earlier than 13**, instance profiles, unsafe Spark configs, eggs/JARs/Maven, init scripts in **S3 or DBFS**, cache/checkpoint APIs, global temp views, DBFS mount-management calls, Scala/R code. You can tell it to proceed, accepting that those items stay unresolved and may fail.
- **Limits:** **jobs with more than 10 migratable tasks** can't be migrated; one workload at a time (no fleet discovery or bulk migration); doesn't model cluster log delivery or SSH keys or resolve every mount.
- **Rollback:** notebooks → revision history. Promoted jobs → restore from `/Workspace/Users/<you>/serverless-migration/backups/job-<job-id>/<timestamp>/`: apply `job.yaml` back with a **`POST /api/2.2/jobs/reset`**, and copy files back using `mapping.yaml`. The backup is never deleted. Tasks it didn't modify (git, SQL, dbt) are recorded but their files aren't copied.
- If your source of truth is a **bundle or Git folder**, reconcile the in-place changes or a later deploy overwrites them.

## 9. Best Practices

**Dependencies and code**
- **Pin Python package versions** (`numpy==2.2.2`); unpinned packages may resolve differently by environment version and add install latency.
- **Use unique temporary view names.** Spark Connect evaluates views lazily, so reusing a name (e.g. in a loop) can error.
- **Use environments, not init scripts**, to install libraries; environments cache packages, reducing start-up latency. For private repos, use pre-signed URLs.
- **Don't set unsupported Spark configurations** — job runs fail. Databricks removed most manual Spark configs to automate tuning.

**Environment versions**
Serverless uses **environment versions** instead of Databricks Runtime versions. You code against a **stable client API** while Databricks independently ships performance, security and bug fixes. Each version keeps backward compatibility and is **supported for three years** from release.

**Performance mode**
- **Performance-optimized (default):** interactive workloads needing fast start; Databricks keeps a **pool of warm compute**.
- **Standard:** automated batch jobs and pipelines that tolerate **4–6 minute** start-up; can cut cost **by up to 70%**. **Not available for notebooks.**
- For scheduled jobs where start latency doesn't matter, standard typically gives the best value.

**Streaming**
Only `Trigger.AvailableNow` is supported; limit micro-batch size to avoid out-of-memory errors (§7).

**Debugging:** no Spark UI — use the **query profile** from query history.

**Networking and connectivity**
- **VPC peering isn't supported.** For private connectivity create **private endpoints** to resources in your VPC or to AWS-managed resources.
- Otherwise **firewall your resources** to allow Databricks serverless traffic. IPs **change**, so **automate** allowlist updates rather than copying once.
- **S3 and other object stores** are regional services that don't use VPCs and aren't affected.
- **Enterprise apps** (Salesforce) and **managed databases** (MySQL) → **Lakeflow Connect**.
- Restrict and monitor outbound traffic with **serverless egress control** (network policies).

**Ingestion without moving data**
- Ingest with **COPY INTO**, **streaming tables**, **Auto Loader**, partner solutions, or the **add-data UI**.
- To avoid duplication or get the freshest data, Databricks recommends **OpenSharing**.
- For ad hoc reporting and proofs of concept, **Lakehouse Federation** queries external databases in place under Unity Catalog governance.

**Unsupported sinks:** if a system (e.g. **Snowflake**) isn't a supported direct write target, let it **read your Unity Catalog tables through the Unity Catalog Iceberg REST catalog** — no duplication, UC remains the governance layer.

**Cost monitoring**
- **Serverless usage policies** for attribution.
- **System tables** (dashboards, alerts, ad hoc queries).
- **Budget alerts** at the account level, and an importable **usage dashboard**.

## 10. Lakehouse Replay **(Public Preview)**

A Databricks service that **improves runtime quality** by automatically **replaying a small subset of your read-only workloads against upcoming runtime versions** before they ship. If a workload succeeds in production but fails on the upcoming version, Databricks fixes the regression *before release*. No setup, configuration or maintenance — but a workspace admin controls enablement from the **Previews** page.

It samples workloads **only from serverless compute**, but the regressions it catches improve **every** Databricks Runtime release (classic and serverless).

### How it works (shadow execution)
1. Your workloads run in production as usual.
2. Replay selects a **small subset of safe, read-only workloads**.
3. It reruns their **Spark plans** on Databricks-managed **shadow compute** running an **upcoming runtime**.
4. On success-in-production / failure-on-shadow, Databricks investigates and resolves confirmed regressions before release.

Shadow compute runs entirely within your workspace and **doesn't affect** production workloads, schedules or performance.

### What gets replayed
Read-only **SQL and DataFrame** workloads on **serverless** (serverless SQL warehouses, serverless notebooks, serverless jobs) that **read Unity Catalog Delta tables**. Coverage is expanding. For DataFrame workloads **only the submitted Spark plan** is replayed — preceding Python cells are **not** executed.

### Security and privacy
- **No data export:** only **execution status and runtime metrics** are compared; **query results are never read, exported or stored**.
- **Same permissions:** replay runs as the **same user identity** as the original query and respects Unity Catalog permissions at replay time.
- **Isolated:** shadow compute can't access external APIs, databases or other workspaces.

### Billing and visibility
- **You aren't billed** for replay compute. You may see **minimal object-storage API costs**, since replay reads with the same permissions and paths.
- Replayed workloads **don't appear** in job run history or query history. Activity is recorded in the **audit log system table** under the **`lakehouseReplay`** service.

### FAQ
| Question | Answer |
|---|---|
| Do I need to do anything? | No, if enabled it runs automatically |
| Does it affect production? | No |
| How often? | **Probabilistic**, based on traffic and workload type; most replayed **within about an hour** of the original run |
| What if a replay fails? | Databricks investigates and fixes confirmed regressions before release; **you aren't notified** of individual failures unless more context is needed |
| What does it detect? | **Execution failures** (success in production, failure on the new runtime) — not result differences |

## 11. Key Limits Referenced Across These Pages (Quick List)

- **Triggers:** only `AvailableNow` (and deprecated `Once`).
- **No** init scripts, compute policies, Spark UI, RDD APIs, caching APIs (`df.cache()` etc.), R, Scala in notebooks, or custom data sources.
- **DBFS access is limited** — use UC volumes; **VPC peering unsupported**.
- **Notebook timeout:** 2.5 hours default; **High memory** 32 GB (Preview).
- **Migration agent:** one workload at a time; jobs over 10 migratable tasks not supported.
- **Rate limits:** executors only; automated workloads only; Private Preview.
- **Access controls:** not a spend cap, can fail open, exempt background compute.
- **Git Folder Serverless:** only the starter can run; shared compute scoped to one usage policy.

## 12. Admin and Developer Checklists

**Admin**
- ✅ Restrict **Default Interactive / Automated Compute** to the right groups — and audit owned jobs and pipelines with the billing query before revoking.
- ✅ Don't treat access controls as a budget cap; use budgets, alerts and (if in preview) rate limits.
- ✅ Set the **serverless interactive execution timeout** to suit your workloads.
- ✅ Configure **default package repositories** and **workspace base environments**.
- ✅ Roll out **serverless usage policies** for cost attribution.
- ✅ Decide whether to enable **Lakehouse Replay** (Preview) and the **Compute Agent** (Beta).
- ✅ Plan networking: private endpoints and/or **automated** firewall allowlisting.

**Developer**
- ✅ Pin dependencies; use environments; reset the environment if installs break it.
- ✅ Set explicit streaming triggers plus batch caps.
- ✅ Use UC volumes and absolute paths.
- ✅ Prefer `try_*` functions over disabling ANSI.
- ✅ Choose standard mode for scheduled batch, performance-optimized for interactive.
- ✅ Validate migrated workloads on classic standard access mode (DBR 14.3+), then A/B test.

## 13. Interview-Style Quick Q&A

**Q: What is serverless compute?**
A: A Databricks-managed, versionless compute service for notebooks, workflows and Lakeflow pipelines; you provision nothing in your cloud account.

**Q: How do admins control who can use it?**
A: Via two default serverless compute objects — Default Interactive Compute (notebooks, Databricks Connect) and Default Automated Compute (jobs, Lakeflow pipelines) — with Can Use / Can Manage permissions. Revoking automated access makes that user's existing serverless jobs and pipelines fail.

**Q: Can I use that as a spending cap?**
A: No. Access checks can fail open during a disruption, background compute is exempt, and several features (DBSQL, Model Serving, Apps, etc.) aren't covered.

**Q: How do rate limits work?**
A: In Private Preview: custom automated compute objects with a size (Small to 2X-Large, ~60 to ~960 DBUs/hour) that caps Spark executor autoscaling per workload. Default is Medium (Premium) or Large (Enterprise). They don't cap drivers, GPUs or MV/streaming-table refreshes.

**Q: What is Lakehouse Replay?**
A: A Preview service that replays a sample of read-only serverless workloads on upcoming runtimes to catch failures before release. Not billed, no data export, shadow compute isolated, audit-logged under `lakehouseReplay`.

**Q: Which streaming triggers work, and why?**
A: `AvailableNow` (recommended) and deprecated `Once`. Time-based and unset triggers fail with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`; use Lakeflow pipelines in continuous mode for always-on streams.

**Q: How do you install libraries without init scripts?**
A: Environment side pane, custom/workspace base environments, job environments, or `pyproject.toml` in Git Folder Serverless; sources can be workspace files, UC volumes or repositories (private repos via `index-url` or default repositories).

**Q: Standard vs. performance-optimized mode?**
A: Standard is for jobs/pipelines, starts in 4–6 minutes and can cost up to 70% less; performance-optimized starts in seconds, is the default, and is the only mode for notebooks.

**Q: How do you migrate a classic job?**
A: Prerequisites (UC, networking, external locations), code fixes (DBFS → volumes, RDD → DataFrame, triggers, ANSI), a compatibility test on standard-access DBR 14.3+, A/B comparison, phased rollout, cost monitoring; optionally the Beta migration agent.

**Q: How can serverless reach private resources?**
A: Not via VPC peering — private endpoints, firewall allowlisting with automated IP updates, Lakeflow Connect, or Federation; S3 isn't affected.

---

*Sources: docs.databricks.com/aws/en/compute/serverless/ and the pages Manage serverless compute; Lakehouse Replay; Streaming on serverless compute; Migrate from classic compute to serverless compute; Best practices for serverless compute; Configure the serverless environment; Serverless compute for notebooks; Git Folder Serverless.*
