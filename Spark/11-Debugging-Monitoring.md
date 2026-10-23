# 11 — Spark Debugging & Monitoring: The Complete Guide

> **Scope.** How to see what a Spark application is doing (monitoring) and how to find out why it is slow, failing, hanging, or wrong (debugging): the Spark UI and History Server, the REST API, event logs, metrics, logging, JVM/Python profiling, cluster-manager tooling, a catalog of common errors, debugging wrong results, streaming-specific observability, and a production observability design.
> **Version notes.** UI features, metric names, and defaults vary by release (notably Spark 3.x → 4.x). Where a feature is version- or platform-dependent it is flagged; confirm against your version's monitoring docs.
> **Related guides:** memory management (OOM and container kills), partitioning & shuffling (skew, `FetchFailed`), joins and Catalyst (reading plans), cluster & resource tuning (right-sizing loop), fault tolerance (failure classification), Structured Streaming (progress reports).

---

## Table of Contents

1. The Debugging and Monitoring Mindset
2. The Observability Toolbox
3. The Spark UI, Tab by Tab
4. Reading Metrics: From Symptom to Bottleneck
5. The History Server and Event Logs
6. The REST API and Programmatic Access
7. Metrics Systems and Dashboards
8. Logging
9. Debugging Workflows by Failure Class
10. Debugging Slow Jobs
11. Debugging Wrong Results
12. Debugging Tools for Plans, Codegen, and Optimizer Behavior
13. JVM-Level Debugging: Threads, Heaps, GC, Native Memory, Profiling
14. PySpark-Specific Debugging
15. Cluster-Manager Debugging (YARN, Kubernetes, Standalone)
16. Streaming Monitoring and Debugging
17. Common Errors Catalog
18. Monitoring Design: SLIs, Dashboards, Alerts, Runbooks
19. Reproducing and Isolating Problems
20. Configuration Reference
21. Common Misconceptions
22. Debugging and Monitoring Checklist
23. A Concise Mental Model

---

## 1. The Debugging and Monitoring Mindset

Debugging a distributed engine is hard because evidence is scattered (driver, many executors, cluster manager, storage), ephemeral (executors, pods, and their logs vanish), and indirect (the symptom — a slow stage — is often several causes removed from the root problem). Three habits make it tractable:

1. **Classify before you fix.** Is it a *failure* (exception, killed process), a *slowdown* (works but takes too long), a *hang* (no progress, no error), or *wrong output* (finishes with incorrect data)? Each has a different workflow (§9–§11).
2. **Follow the evidence chain, top-down.** Application → job → stage → task → executor/host → JVM/OS → data. The Spark UI is organized in exactly this order.
3. **Change one thing at a time and re-measure.** Tuning by guesswork produces folklore; tuning by measurement produces fixes.

Equally important: **monitoring is what lets you debug after the fact.** If event logs, aggregated logs, and metrics were not captured *before* the incident, the evidence may be gone. Observability is infrastructure you build before you need it (§18).

---

## 2. The Observability Toolbox

| Tool | What it gives you | When it's available |
|---|---|---|
| **Spark UI** (driver, port 4040 by default; increments if taken) | Live and per-job view: jobs, stages, tasks, storage, environment, executors, SQL plans, streaming | While the application runs |
| **History Server** (default port 18080) | The same UI for finished (and crashed) applications, rebuilt from **event logs** | Needs `spark.eventLog.enabled=true` |
| **Event logs** | JSON record of every scheduler/SQL event — the ground truth you can also *query* | Written by the driver |
| **REST API** | Programmatic access to what the UI shows (`/api/v1/...`) on both the live UI and the History Server | Always |
| **Metrics system** (Dropwizard) | Time-series driver/executor/JVM metrics to JMX, Graphite, Prometheus servlet, CSV, etc. | Configured via `metrics.properties`/`spark.metrics.conf.*` |
| **Executor metrics** | Per-executor JVM heap, off-heap, GC time, process-tree memory, peaks per stage | Heartbeat-driven; event log with `logStageExecutorMetrics` |
| **Listeners** (`SparkListener`, `StreamingQueryListener`, `QueryExecutionListener`) | Custom hooks on task/stage/job/query events | Scala/Java for core listeners; Python for streaming listeners (3.4+) |
| **Driver and executor logs** | Stack traces, warnings, your own log lines | Local files, YARN aggregation, Kubernetes logs |
| **Cluster-manager tools** | Why containers/pods died, scheduling status, node health | YARN RM UI/`yarn logs`; `kubectl describe/logs`; Standalone master UI |
| **JVM tools** | Thread dumps, heap dumps/histograms, GC logs, JFR, native memory tracking, profilers | Per-executor JVM |
| **Python tools** | Tracebacks, `faulthandler`, profilers | PySpark workers |
| **Plan inspection** | `explain()`, plan change logs, generated code | In your session |

---

## 3. The Spark UI, Tab by Tab

Access: `http://<driver>:4040` (client mode); via the cluster manager's proxy (YARN ResourceManager "ApplicationMaster" link) or `kubectl port-forward <driver-pod> 4040:4040` on Kubernetes. Behind a proxy, `spark.ui.reverseProxy` helps with Standalone setups.

### 3.1 Jobs tab
- One row per **job** (triggered by an action). Shows status, duration, stages (succeeded/total), tasks (succeeded/total, with in-progress and failed counts).
- **Event timeline** shows executors being added/removed and jobs over time — a quick visual for executor churn, dynamic allocation behavior, and idle gaps.
- **What to look for:** a job with many failed tasks; long gaps between jobs (driver-side work, slow listing, Python code between actions); executors repeatedly removed (OOM/preemption).

### 3.2 Stages tab and the Stage detail page
The most valuable page for performance debugging.

- **Summary Metrics** (quantiles: min / 25th / median / 75th / max) for tasks:
  - *Duration*, *Scheduler Delay* (time waiting to be scheduled — high means driver or slot scarcity), *Task Deserialization Time* (large closures/broadcast), *GC Time*, *Result Serialization Time*, *Getting Result Time*.
  - *Peak Execution Memory*, *Spill (Memory)* and *Spill (Disk)*.
  - *Input Size / Records*, *Output Size / Records*.
  - *Shuffle Read Size / Records*, *Shuffle Read Blocked Time / Fetch Wait Time*, *Shuffle Write Size / Records*, *Shuffle Remote/Local Bytes Read*.
- **The key reading: max vs. median.** If the maximum task duration or input/shuffle size is many times the median, you have **skew** (or a bad node, if the slow tasks cluster by host). Uniformly high numbers mean the stage is simply heavy.
- **Event Timeline (per task):** colored segments show scheduler delay, deserialization, shuffle read, executor computing time, shuffle write, serialization. A wide "computing" bar is CPU work; wide shuffle-read bars are network/disk/skew; wide scheduler delay is scheduling overhead.
- **DAG Visualization:** the stage's operators; check for unexpected shuffles and cached ("green dot") RDDs.
- **Aggregated Metrics by Executor:** compare executors; one executor with far more time/failures than its peers points at a bad host or skewed data placement. Includes links to logs.
- **Tasks table:** sortable; show failed tasks first; the **Errors** column has failure reasons; **Locality Level** shows PROCESS_LOCAL/NODE_LOCAL/RACK_LOCAL/ANY (mostly ANY on object stores — fine).
- **Failed stages** show attempt numbers; a stage with multiple attempts usually means `FetchFailed` recovery.

### 3.3 Storage tab
Cached RDDs/DataFrames: storage level, **fraction cached**, size in memory and on disk, partitions held. A fraction well below 100 % means data doesn't fit or was evicted (recompute cost); a cached dataset nobody reuses is wasted memory.

### 3.4 Environment tab
All effective configuration (Spark properties, system properties, classpath, Hadoop config). **First stop when "my setting isn't working."** Configuration precedence is: values set programmatically on `SparkConf`/`SparkSession.builder` > flags on `spark-submit` > `spark-defaults.conf`. Some settings can't be changed after the JVM exists (e.g., `spark.driver.memory` in client mode must be set before launch, not inside the application), and anything set after the `SparkContext` starts for resource-related configs is ignored.

### 3.5 Executors tab
Per-executor (and driver) rows:
- **Active tasks / failed tasks / completed tasks / total cores**: utilization at a glance. Persistently low active tasks vs. cores = under-parallelization.
- **Storage memory, on-heap/off-heap usage, disk used.**
- **GC Time** (shown red when high relative to task time): memory pressure signal.
- **Input, shuffle read/write, task time**: imbalance across executors.
- **Logs** (stdout/stderr links) and **Thread Dump** (live stack traces per executor, essential for hangs, §13). Recent versions add **heap histogram** and, in newer releases, flame-graph-style views of thread dumps (version dependent).
- **Peak memory metrics columns** (when executor metrics are collected): JVM heap and off-heap peaks, useful for right-sizing.
- **Excluded executors** (if exclusion on failure is enabled) appear flagged.

### 3.6 SQL / DataFrame tab
One entry per SQL/DataFrame query execution.
- **Plan graph with metrics:** scan nodes (files read, size, scan time, partitions read), `Exchange` (shuffle bytes/records, fetch wait), joins (output rows, build/probe sizes), aggregates (peak memory, spill size, time in build), sort (spill, sort time).
- **Row-count flow**: follow "number of output rows" from node to node to find where data explodes (bad join) or vanishes (filter too aggressive).
- **Details** panel: parsed/analyzed/optimized/physical plans as text.
- **AQE queries** show the **final executed plan** (`AdaptiveSparkPlan`, `isFinalPlan=true`) with the runtime changes (broadcast conversion, coalesced partitions, skew splits) — what you want to compare with the initial plan from `explain()`.
- Cross-link: the query → jobs → stages navigation shows which stage implements which operator.

### 3.7 Structured Streaming tab
Per-query charts of input rate, process rate, batch duration, operation durations, and state metrics (see §16).

### 3.8 Others
- **JDBC/ODBC Server** tab (Thrift server): sessions and SQL statements.
- **Connect** tab (Spark Connect): sessions and operations, where available.
- **Spark UI retention** is bounded (`spark.ui.retainedJobs`, `retainedStages`, `retainedTasks`, `spark.sql.ui.retainedExecutions`); on very busy applications, old jobs disappear from the live UI — the event log still has them.

---

## 4. Reading Metrics: From Symptom to Bottleneck

| What you see (Stage page / Executors tab) | What it usually means | Next step |
|---|---|---|
| Max task duration ≫ median; same few tasks slow across stages | **Data skew** on a key | Check max vs. median input/shuffle read; see partitioning and joins guides |
| Slow tasks all on one host/executor | **Bad node/disk/network** or noisy neighbor | Compare per-executor and per-host metrics; exclusion, speculation, node replacement |
| High **Scheduler Delay** | Driver overloaded, too many tiny tasks, or not enough slots | Driver size/cores, fewer larger partitions, event queue drops in logs |
| High **Task Deserialization Time** | Huge closures, large broadcast referenced in task, many tiny tasks | Broadcast big objects deliberately; avoid capturing large objects in lambdas |
| High **GC Time** (more than ~10 % of task time) | Memory pressure: big partitions, too much caching, too many cores per heap | Smaller partitions, fewer cores/executor, more memory, off-heap (memory guide) |
| **Spill (Memory/Disk)** > 0 | Execution memory insufficient for the task's data | More partitions, more memory per core; fix skew |
| High **Shuffle Read Blocked Time / Fetch Wait Time** | Network, remote disk, or skew on the map side | Check shuffle size per task, bandwidth, fetch retries, executor churn |
| Large **Shuffle Write** but small final output | Wide transformation producing/propagating too much data | Filter/project earlier, pre-aggregate, rewrite join/aggregate |
| Stage has multiple **attempts** | `FetchFailed` recovery from lost shuffle data | Why did executors/nodes disappear (fault-tolerance guide) |
| Low **active tasks** vs. total cores | Too few partitions, or stages waiting on a straggler | Partition counts; straggler analysis |
| Many **failed tasks** across executors | Deterministic bug/bad data, or systemic OOM | Error column; executor logs; reproduce on that partition |
| Input size huge but **output tiny** | Filtering late; poor pushdown | `PushedFilters`/`PartitionFilters` in the scan node |
| Cached data **fraction < 100 %** | Not enough storage memory; eviction | Storage level, memory, whether caching is worth it |
| Executor **removed** events on the Jobs timeline | OOM, container kill, preemption, dynamic allocation | Executor exit reasons (§15), memory guide |

---

## 5. The History Server and Event Logs

### 5.1 Enabling event logs
```
spark.eventLog.enabled   true
spark.eventLog.dir       s3a://bucket/spark-events    # or hdfs://..., must be shared and writable
spark.eventLog.rolling.enabled   true                 # long-running apps (streaming): roll files
spark.eventLog.logStageExecutorMetrics   true         # per-stage executor peak metrics (heap, off-heap, process tree)
```
- Without event logs, **a finished or crashed application leaves no UI** — you lose the best evidence.
- Rolling event logs keep a single multi-day streaming log from becoming unmanageably large; a crashed application leaves an `.inprogress` log that the History Server can usually still display.
- `spark.eventLog.compress` reduces size; check your version's default and codec options.

### 5.2 Running the History Server
- `spark.history.fs.logDirectory` points at the same location; it lists **completed and incomplete** applications and rebuilds each UI on demand.
- Operational settings worth knowing: `spark.history.fs.update.interval` (how often it rescans), `spark.history.retainedApplications` (apps cached in memory), `spark.history.ui.maxApplications`, and for large deployments a **disk-based store** (`spark.history.store.path`, optionally the hybrid store) so memory isn't exhausted loading big applications.
- **`spark.history.fs.cleaner.enabled` is false by default** — event logs accumulate forever unless you enable the cleaner (`maxAge`, `maxNum`) or use a storage lifecycle rule.
- **Applications stuck as "incomplete"**: usually the driver died without finalizing the log (`.inprogress`); they are still inspectable.

### 5.3 Event logs are data — query them
Event logs are newline-delimited JSON with one object per event, so Spark itself can analyze them. This is the foundation for fleet-wide monitoring (cost per job, skew detection, regressions) without a commercial tool.

```python
from pyspark.sql import functions as F

ev = spark.read.json("s3a://bucket/spark-events/application_1700000000000_0042*")   # field names follow the event-log format; verify on your version

task_end = (ev.filter(F.col("Event") == "SparkListenerTaskEnd")
    .select(
        F.col("`Stage ID`").alias("stage_id"),
        F.col("`Task Info`.`Host`").alias("host"),
        F.col("`Task Info`.`Executor ID`").alias("executor"),
        F.col("`Task Metrics`.`Executor Run Time`").alias("run_ms"),
        F.col("`Task Metrics`.`JVM GC Time`").alias("gc_ms"),
        F.col("`Task Metrics`.`Memory Bytes Spilled`").alias("spill_mem"),
        F.col("`Task Metrics`.`Disk Bytes Spilled`").alias("spill_disk"),
    ))

# per-stage skew indicator: max task time vs. median
skew = (task_end.groupBy("stage_id")
    .agg(F.max("run_ms").alias("max_ms"),
         F.expr("percentile_approx(run_ms, 0.5)").alias("p50_ms"),
         F.sum("gc_ms").alias("gc_ms_total"),
         F.sum("spill_disk").alias("disk_spill"))
    .withColumn("skew_ratio", F.col("max_ms") / F.col("p50_ms"))
    .orderBy(F.desc("skew_ratio")))
```
Use the same approach for **run-over-run comparison**: store a small summary row per application (duration, core-seconds, bytes shuffled, spill, failed tasks, retries) in a table and alert on regressions against a rolling baseline.

---

## 6. The REST API and Programmatic Access

The same data as the UI is available as JSON from the driver (`:4040/api/v1`) and the History Server (`:18080/api/v1`):

```python
import requests
base = "http://driver-host:4040/api/v1"

app_id = requests.get(f"{base}/applications").json()[0]["id"]
stages = requests.get(f"{base}/applications/{app_id}/stages",
                      params={"status": "complete"}).json()

# Task duration quantiles for one stage (stage 7, attempt 0)
q = requests.get(f"{base}/applications/{app_id}/stages/7/0/taskSummary",
                 params={"quantiles": "0.5,0.95,1.0"}).json()
executors = requests.get(f"{base}/applications/{app_id}/executors").json()
```

Useful endpoints: `/applications`, `/jobs`, `/stages`, `/stages/{id}/{attempt}/taskSummary` and `/taskList`, `/executors` (and `/allexecutors`), `/storage/rdd`, `/environment`, `/sql`, `/streaming/...`. In PySpark, `sc.statusTracker()` exposes active jobs/stages and executor info in-process; there is no Python `SparkListener`, so custom core listeners are written in Scala/Java (registered with `spark.extraListeners`), or you consume the REST API, event logs, or metrics instead. For streaming queries, Python supports `StreamingQueryListener` (3.4+).

---

## 7. Metrics Systems and Dashboards

### 7.1 Spark's metrics system
Spark's Dropwizard-based metrics system emits **sources** (driver, executors, JVM, shuffle service, application-specific) to configurable **sinks**, configured in `metrics.properties` or with `spark.metrics.conf.*` properties. Common sinks: JMX, Graphite, Slf4j, CSV, console, and the **PrometheusServlet** (`org.apache.spark.metrics.sink.PrometheusServlet`, exposed on the driver UI port, e.g., `/metrics/prometheus`).

Additionally, **executor-level metrics** (JVM heap, off-heap, direct/mapped buffers, GC time, and — with `spark.executor.processTreeMetrics.enabled` — process-tree memory/RSS) can be exposed through the UI's Prometheus endpoint `/metrics/executors/prometheus` when `spark.ui.prometheus.enabled=true` (off by default in many releases; check yours), and as per-stage peaks in event logs when `spark.eventLog.logStageExecutorMetrics=true`.

On Kubernetes the usual pattern is a Prometheus Operator `PodMonitor`/scrape config pointed at driver and executor pods; on YARN, push to Graphite/StatsD or scrape through a JMX exporter. Set `spark.metrics.namespace` so metrics are grouped by a stable job name instead of an application ID that changes every run (which explodes cardinality).

### 7.2 Metrics that matter

| Layer | Metrics | Why |
|---|---|---|
| Executor JVM | `JVMHeapMemory`, `JVMOffHeapMemory`, `MinorGCTime`/`MajorGCTime`, process-tree RSS | Right-sizing; early warning for OOM/container kills |
| Executor activity | Active tasks, completed/failed tasks, executor count | Utilization and churn |
| Shuffle | Bytes written/read, fetch wait, spill | Shuffle and memory pressure |
| Driver | Heap, scheduler event-queue drops, job/stage counts | Driver saturation |
| Application | Duration, success/failure, retries, rows in/out | SLAs and data health |
| Cluster | Pending containers/pods, queue wait, node health | Capacity vs. demand |

### 7.3 Custom and data-quality metrics
- **`Observation` / `observe()`** (batch and streaming) attaches named aggregate metrics (row counts, null counts, min/max) to a query and returns them when it completes — ideal for **data-quality metrics without extra passes** over the data.
- **Accumulators** can carry custom counters, but remember they can over-count if used inside transformations that are retried or recomputed (use them from actions, or prefer `observe()`/aggregations).
- Persist **run-level facts** (input rows, output rows, bad-record ratio, watermark/high-water marks, duration, run ID, code version, config hash) to a control table; this turns monitoring into an auditable dataset.

---

## 8. Logging

### 8.1 Where logs live
- **Driver log**: client mode — the console/log file where `spark-submit` ran; cluster mode — in the AM container (YARN), the driver pod (Kubernetes), or a worker (Standalone).
- **Executor logs**: per executor stdout/stderr (YARN containers, Kubernetes pods, Standalone worker `work/` directories). Reachable from the Executors tab while running, but **they vanish with the container or pod** unless aggregated (YARN log aggregation, a Kubernetes log shipper such as a sidecar or node agent). Make log aggregation a prerequisite for production.
- **`print()` and `logging` inside tasks** write to **executor** logs, not the driver console. In PySpark, Python worker output goes to the executor's stderr.

### 8.2 Configuring logging
- Spark (3.3+) uses **Log4j 2** (`conf/log4j2.properties`). Change levels at runtime with `spark.sparkContext.setLogLevel("WARN")` (driver-side effective level; executors read their own log config), or ship a custom `log4j2.properties` with `--files` and set `-Dlog4j.configurationFile` via the driver/executor Java options.
- **Spark 4 writes structured JSON logs by default** (with MDC context fields such as app/executor/task identifiers); this is easier to ship into log platforms, while a plain-text log4j2 template is available if you prefer the older format. Check your version's logging configuration.
- `spark.logConf=true` logs the effective configuration at startup — a quick way to prove what the application actually ran with.
- Control volume: executor log rolling (`spark.executor.logs.rolling.*`) prevents disks filling on long-running applications; avoid logging per-row inside tight loops (it can dominate runtime and disk).

### 8.3 Logging practices that make debugging possible
- Include a **run ID / job name / partition or window** in your application log lines.
- Log **counts and key invariants** at stage boundaries (input rows, post-filter rows, join output rows) — the cheapest way to localize wrong-results problems.
- Don't log secrets or full records; log keys and metadata.
- Log the **first** exception with context, not just the last failure's generic wrapper.

---

## 9. Debugging Workflows by Failure Class

### 9.1 The application won't start or never gets resources
1. Check the cluster manager first (not Spark): YARN application state and diagnostics (`yarn application -status`), RM UI queue limits; Kubernetes `kubectl describe pod` for `FailedScheduling`, `Insufficient memory/cpu`, quota errors, image pull problems; Standalone master UI for available cores/memory.
2. **Compute the actual request** (heap + overhead + off-heap + PySpark; cluster & resource tuning guide) and compare with node capacity, queue limits, `yarn.scheduler.maximum-allocation-mb`, namespace quotas.
3. Kubernetes permissions: driver pod needs RBAC to create/list/delete executor pods (`forbidden` errors in the driver log).
4. Driver ↔ executor connectivity (client mode, containers, firewalls): `spark.driver.host`, `spark.driver.bindAddress`, `spark.driver.port`, `spark.blockManager.port`; errors like "Failed to connect to driver" usually mean networking, not Spark.

### 9.2 The application fails with an exception
1. **Find the root cause, not the last line.** The driver typically ends with `Job aborted due to stage failure: Task N in stage S failed 4 times`. Scroll to the **first** `Caused by:` and the failure reason of the *first* failed attempt (Stages tab → failed tasks → Errors column → executor stderr).
2. **Locate the failing code**: DataFrame stack traces point at Spark internals; use the stage's DAG/SQL tab to map the failing stage to an operator and then to your code (use `.alias()`/descriptive column names, and `spark.sparkContext.setJobDescription("step: join customers")` — job descriptions appear in the UI and are enormously helpful).
3. **Classify:** user-code exception (bad data, null, type), resource kill (OOM, container kill), shuffle loss (`FetchFailed`), environment (classpath, permissions, missing files), infrastructure (node loss).
4. **Transient or deterministic?** The same task failing identically four times is deterministic — fix code/data. Failures that move between executors point at infrastructure.
5. **Reproduce narrowly**: on the failing partition/key/file (the failed task's input split shows which file/range), with a small sample, in `local[*]` mode (§19).

### 9.3 OutOfMemoryError and container kills
- **Where?** Driver or executor? JVM heap `OutOfMemoryError` (executor exit code 52), `GC overhead limit exceeded`, `SparkOutOfMemoryError: Unable to acquire N bytes of memory` (execution memory pool exhausted), or a **container kill** (YARN "exceeding memory limits", Kubernetes `OOMKilled`/exit 137) which is native/overhead memory, not heap.
- **Evidence to gather:** stage page (spill, GC, max task input), Executors tab (heap, GC), `ExecutorMetrics` peaks, and for container kills the **process-tree/RSS** vs. container limit.
- **Heap dumps:** add `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<dir on a volume that survives the container>` to `spark.executor.extraJavaOptions`/`spark.driver.extraJavaOptions`, then analyze with Eclipse MAT or similar. On Kubernetes, write dumps to a mounted volume or they vanish with the pod.
- **Fix paths** (details in the memory guide): smaller partitions/more partitions, skew handling, remove `collect()`/huge broadcasts, fewer cores per executor, larger overhead/`pyspark.memory`, off-heap or RocksDB for state.

### 9.4 Shuffle failures
- `FetchFailedException`, `Connection reset by peer`, `Failed to connect to host`, missing shuffle blocks. Check executor loss events around the same time; the cause is usually *why* the executor/node disappeared (OOM kill, preemption, scale-down without shuffle durability), not the fetch itself.
- Stage with multiple attempts = recovery in progress; correlate with executor-removed events and cluster-manager logs.

### 9.5 Hung jobs (no progress, no error)
1. **Is it really hung?** Jobs/Stages tab: are any tasks running, any task counters moving? Is a stage stuck at the same N−1/N for a long time?
2. **Thread dumps** of the executors running the stuck tasks (Executors tab → Thread Dump, repeated 2–3 times a few seconds apart; threads sitting in the same frame are your culprit): blocked on a lock, waiting for a socket (external service, shuffle fetch), inside a regex/UDF loop, waiting on `Await`.
3. **Driver thread dump** if all tasks are done but nothing advances (driver stuck planning, listing files, collecting, or a deadlock).
4. Check external dependencies (database, API, object store throttling), skew (one huge task), and speculative execution/exclusion settings.
5. Remember: Spark has **no per-task timeout**; protect external calls with client-side timeouts and the orchestrator with job-level timeouts (fault-tolerance guide).

### 9.6 Serialization errors
- JVM: `org.apache.spark.SparkException: Task not serializable` — a lambda captured something non-serializable (SparkContext/SparkSession, a logger, a DB connection, an enclosing class). Move construction inside `mapPartitions`/`foreachPartition`, make the object serializable, or avoid capturing the outer `this`.
- PySpark: pickling errors (`PicklingError`, "Could not serialize object", "It appears that you are attempting to reference SparkContext from a broadcast variable, action, or transformation"). Don't use `spark`/`sc` or DataFrames inside UDFs/lambdas; pass plain data or broadcast variables.
- Kryo: `Buffer overflow`/`Kryo serialization failed` → raise `spark.kryoserializer.buffer.max` or fix oversized records.

### 9.7 Classpath and dependency conflicts
- `NoSuchMethodError`, `ClassNotFoundException`, `NoClassDefFoundError`, `IncompatibleClassChangeError`: almost always version conflicts between your jars and Spark/Hadoop's own. Inspect the **Environment tab classpath**, align library versions with the Spark distribution, **shade/relocate** conflicting dependencies, and use `--packages`/`--jars` deliberately. `spark.driver.userClassPathFirst`/`spark.executor.userClassPathFirst` can help but are experimental and can create new conflicts.
- Python: dependency and version mismatches across driver and executors (§14).

### 9.8 File and data access errors
- `FileNotFoundException ... The underlying files may have been updated`: cached file listings are stale after another job rewrote files — `REFRESH TABLE`/`spark.catalog.refreshTable`, or avoid reading and overwriting the same path in one job.
- Permission and credential errors (expired tokens in long-running apps), object-store throttling (`503 Slow Down`), `Too many open files` (OS `ulimit`), `No space left on device` (local dirs, `/tmp`, logs).
- Schema errors: `Parquet column cannot be converted`, `Failed merging schema`, `CANNOT_PARSE`/ANSI cast errors in Spark 4 — fix the schema/contract or use `try_cast`.

---

## 10. Debugging Slow Jobs

A repeatable flow:

1. **Which stage is slow?** Jobs tab → the longest job → its longest stage. Ignore stages that account for little of the total.
2. **What kind of slow?** Use the Stage detail page quantiles (§3.2, §4):
   - *Skewed* (max ≫ median) → skew mitigation.
   - *Uniformly heavy, high CPU* → more compute/better code/pushdown.
   - *Shuffle-bound* (fetch wait high, large shuffle) → reduce shuffle volume, tune joins/partitions.
   - *Spill/GC-bound* → memory per task.
   - *Scheduler-bound* (scheduler delay, many tiny tasks) → fewer partitions, driver sizing.
   - *Idle* (few active tasks vs. cores, long gaps) → parallelism or driver-side work between jobs.
3. **Read the plan** for that stage in the SQL tab: unexpected `SortMergeJoin` vs. broadcast, missing pushdown (`PushedFilters`, `PartitionFilters`), cartesian/nested-loop joins, repeated scans of the same table, Python UDF nodes (`BatchEvalPython`/`ArrowEvalPython`).
4. **Check input/output volumes** against expectations: scanning 2 TB to produce 10 MB means filtering too late or poor file layout.
5. **Apply the fix from the matching guide**, re-run, compare stage metrics (not just total time).
6. **Compare with a baseline.** If a job *used to be fast*: diff the two runs' event logs — input size, partition counts, AQE decisions, join strategies, executor counts, skew ratios, config changes, data distribution changes, cluster conditions. The cause is usually a data change (growth, new hot key), a plan change (statistics, AQE), or an environment change (new node type, noisy neighbors).

**Driver-side slowness** (a long time with no Spark jobs running): file listing on large object-store prefixes, planning very wide/deep plans, Python code between actions, `collect()`/`toPandas()`, serial per-iteration actions in a driver loop. Thread-dump the driver, and look at gaps on the Jobs timeline.

---

## 11. Debugging Wrong Results

Wrong output is the hardest failure because nothing crashes. Approach it like bisecting a program:

**1. Establish a ground truth.** Reproduce on a small, representative slice (a key, a date, a file) where you know the right answer.

**2. Reconcile at stage boundaries.** Write or inspect intermediates and compare **row counts, distinct keys, sums/checksums** between steps:
```python
df.agg(F.count("*"), F.countDistinct("id"), F.sum(F.hash(*df.columns).cast("long"))).show()
left_only = a.exceptAll(b)           # rows in a not in b (multiset difference)
```
The step where the number changes unexpectedly is your culprit.

**3. Check the usual suspects**
- **Join cardinality**: duplicates in a join key multiply rows (`count` before/after; `groupBy(key).count().filter("count > 1")`); NULL keys never match under `=` (use `<=>`/`eqNullSafe` if intended); implicit casts when joining int to string or decimals of different precision.
- **Nondeterminism**: `rand()`, `uuid()`, `current_timestamp()`, `monotonically_increasing_id()`, `first()`/`last()` without ordering, `dropDuplicates` keeping an arbitrary row, `collect_list` ordering, windows without deterministic `orderBy`, `limit` without ordering. Results can differ between runs or after a recomputation.
- **Time**: `spark.sql.session.timeZone` vs. data timezone; timestamp vs. date conversion; `TimestampNTZ` vs. `TimestampType`.
- **Types and overflow**: integer overflow, decimal precision/scale loss, float comparison. **ANSI mode** (default in Spark 4) turns many silent NULL/overflow cases into errors; toggling `spark.sql.ansi.enabled` quickly shows whether a result difference is an ANSI-semantics difference.
- **Null handling**: `NOT IN` with NULLs, aggregates ignoring NULLs, `count(col)` vs. `count(*)`.
- **Window defaults**: the default frame with an `ORDER BY` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which treats ties as peers.
- **Schema/parse issues**: `PERMISSIVE` parsing nulling out fields; wrong delimiters/quoting; schema inference disagreeing across files; Parquet schema merging.
- **Caching/staleness**: reading cached or file-listing-cached data that changed underneath.
- **Late/duplicate input data** (streaming or incremental batch): watermarks dropping late rows, at-least-once duplicates.

**4. Isolate engine behavior from your logic.** Re-run with features toggled to see whether the result changes: `spark.sql.adaptive.enabled=false`, `spark.sql.codegen.wholeStage=false`, `spark.sql.optimizer.excludedRules=<rule>`, `spark.sql.ansi.enabled`, vectorized reader off. A changed result when disabling an optimization is either a real engine bug (rare; minimize and report) or evidence that your logic depended on unspecified behavior (common).

**5. Verify determinism.** Run twice and compare checksums; if results differ, find and remove the nondeterministic expression.

---

## 12. Debugging Tools for Plans, Codegen, and Optimizer Behavior

| Tool | Use |
|---|---|
| `df.explain()` / `explain("formatted")` / `explain("extended")` | Physical plan; numbered formatted view; parsed → analyzed → optimized → physical |
| `df.explain("cost")` | Plan with statistics (when CBO stats exist) |
| `df.explain("codegen")` | The **generated Java code** for whole-stage codegen (to inspect fused operators and spot a codegen fallback) |
| `spark.sql.planChangeLog.level` (+ `planChangeLog.rules`/`batches`) | Log each optimizer rule that changes the plan — shows *which rule* transformed your query |
| `spark.sql.optimizer.excludedRules` | Disable specific optimizer rules to test whether a rule causes a bug or regression |
| `spark.sql.codegen.wholeStage=false` | Rule out codegen issues (and compare speed) |
| `spark.sql.adaptive.enabled=false` | Compare against the static plan; AQE-final-plan vs. initial plan differences |
| `rdd.toDebugString()` | RDD lineage (shuffle boundaries, cached RDDs) |
| `spark.sql.debug.maxToStringFields` | Avoid plan strings truncating on wide schemas |
| `spark.logConf` / Environment tab | Prove effective configuration |
| `spark.sparkContext.setJobDescription(...)` / `setJobGroup(...)` | Name jobs so UI rows map to your code |
| SQL tab details | Stage↔operator mapping and AQE final plan |

---

## 13. JVM-Level Debugging: Threads, Heaps, GC, Native Memory, Profiling

- **Thread dumps**: Executors tab → *Thread Dump* (driver and each executor), or `jstack <pid>` / `jcmd <pid> Thread.print`. Take several, seconds apart; compare. Look for threads in `task-result-getter`, `Executor task launch worker`, blocked monitors, socket reads, `Await.result`.
- **Heap analysis**: `jcmd <pid> GC.class_histogram` (or the UI's heap histogram where available) for a quick "what is filling the heap"; `jmap -dump` or `-XX:+HeapDumpOnOutOfMemoryError` for deep analysis (MAT, VisualVM).
- **GC logs**: add unified logging on JDK 9+ (`-Xlog:gc*:file=<path>:time,uptime`) through `extraJavaOptions` (the old `-XX:+PrintGCDetails` flags are Java 8-era). Analyze pause times, allocation rate, old-gen occupancy. Long full GCs → heartbeat timeouts and executor loss.
- **Native memory** (container kills with no heap OOM): `-XX:NativeMemoryTracking=summary`, then `jcmd <pid> VM.native_memory summary`; compare **process RSS** (executor metrics with process-tree enabled) to heap + off-heap + overhead; suspects include Netty direct buffers, RocksDB, Arrow, Python workers, thread stacks.
- **CPU profiling**: JDK Flight Recorder (`-XX:StartFlightRecording`) or **async-profiler** attached via `-agentpath` produce flame graphs showing where CPU actually goes (serialization, regex, codegen, Python bridge, shuffle). Profile one representative executor for a stage, not the whole cluster. Recent Spark UIs also render thread-dump-based flame graphs (version dependent), which is a free, low-overhead first look.
- **Constraints:** you cannot set `-Xmx` in `extraJavaOptions` (use `spark.executor.memory`); on Kubernetes make sure dump/profile output paths are on persistent volumes.

---

## 14. PySpark-Specific Debugging

- **Where Python runs:** DataFrame/SQL expressions run in the JVM; Python code (UDFs, `mapPartitions`, `foreach*`, `toPandas`, `applyInPandas`) runs in **Python worker processes** on executors, outside the JVM heap and invisible to JVM metrics.
- **Reading errors:** a Python exception inside a worker surfaces on the driver wrapped as `Py4JJavaError` / `PythonException` with the **worker traceback** embedded; read that traceback, not the Java stack. `spark.sql.pyspark.jvmStacktrace.enabled` includes the JVM stack when you need it; newer versions simplify UDF tracebacks by default.
- **"Python worker exited unexpectedly (crashed)" / "Python worker failed to connect back":**
  - Worker killed by memory (container OOM) — check overhead/`spark.executor.pyspark.memory`, exit code 137.
  - Python version/environment mismatch between driver and executors (`PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, packaged environments): the minor versions must match.
  - Native-library crashes (segfault); newer versions can enable Python `faulthandler` for workers to dump a traceback on crash.
  - Networking/loopback problems on the executor.
- **Debugging UDF logic:** test the Python function standalone on sample data first; log from inside with the `logging` module (appears in executor stderr); use `spark.python.profile`/the newer UDF profilers (version dependent; `cProfile`-style and memory profilers) to find hot spots; verify Arrow/pandas UDF input/output types with a small `limit`ed batch.
- **Arrow/pandas pitfalls:** schema mismatch between declared return type and returned frame; very large batches (`spark.sql.execution.arrow.maxRecordsPerBatch`) blowing Python memory; timestamp timezone handling in `toPandas()`.
- **Common PySpark mistakes visible only in monitoring:** a Python UDF where a built-in exists (UDF node in the plan, slow, memory-heavy), `collect()`/`toPandas()` on large data (driver OOM), per-row object construction (connections/models inside the UDF), driver-side Python loops launching many tiny jobs.
- **Interactive debugging:** attach a remote debugger to a worker only for local-mode reproduction (`local[1]`); in distributed runs prefer logging, sampled data capture, and profilers.

---

## 15. Cluster-Manager Debugging (YARN, Kubernetes, Standalone)

### YARN
- `yarn application -list`, `yarn application -status <app>`, and the **ResourceManager UI** show state, queue, diagnostics (the "Diagnostics" text often states the real reason: container killed, exceeded memory, AM failed).
- **Logs:** `yarn logs -applicationId <id>` (requires log aggregation to have completed), per container with `-containerId`. For a running app use the NodeManager/RM log links.
- **Container exit codes:** `143` (SIGTERM — killed, often preemption or scale-down), `137`/`-9` (SIGKILL — memory), `-100`/`-102` (container released/preempted, e.g., lost node), `1`/`13` (application error).
- Check **queue capacity, AM resource share, and preemption** if applications sit in `ACCEPTED` or lose executors.

### Kubernetes
- `kubectl get pods -l spark-role=driver/executor`, `kubectl describe pod <pod>` (events: `FailedScheduling`, `OOMKilled`, `Evicted`, `ImagePullBackOff`), `kubectl logs <pod>` and `--previous` for crashed containers.
- **Executor pods are deleted on termination by default** — set `spark.kubernetes.executor.deleteOnTermination=false` while investigating, and ship logs/metrics off the cluster continuously.
- Exit code `137` = `OOMKilled` (native/overhead memory) or node memory pressure; `Evicted` with ephemeral-storage reasons = shuffle spill exceeded limits.
- Driver cannot create executors → RBAC/service account/namespace quota; executors cannot reach the driver → headless service, network policy, `spark.driver.host`.

### Standalone
- Master UI (default 8080) for workers/applications; worker logs in `work/<app>/<executor>/` (`stdout`/`stderr`); applications waiting for resources usually exceed available cores/memory or are blocked by another application without `spark.cores.max`.

---

## 16. Streaming Monitoring and Debugging

Structured Streaming exposes per-query progress; the full treatment is in the Structured Streaming guide. The operational essentials:

```python
q = df.writeStream....start()
q.status            # {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}
q.lastProgress      # latest progress dict
q.recentProgress    # recent progress dicts
q.exception()       # reason for termination, if failed
spark.streams.active
```

| Signal | Source | Interpretation |
|---|---|---|
| `inputRowsPerSecond` vs `processedRowsPerSecond` | progress | Processing < input ⇒ falling behind |
| `batchDuration` / `triggerExecution` vs trigger interval | progress | Headroom per batch |
| `durationMs` breakdown | progress | Where batch time goes (`addBatch`, `walCommit`, `latestOffset`, planning) |
| Source `latestOffset − endOffset` | progress `sources` | Lag (Kafka consumer-group lag won't show Spark's progress by default) |
| `eventTime.watermark` | progress | Stalled watermark ⇒ no Append-mode output, state not cleaned |
| `stateOperators.numRowsTotal`, `memoryUsedBytes`, `numRowsDroppedByWatermark` | progress | State growth, memory, late-data drops (note: `numRowsTotal` can read 0 if RocksDB row tracking is off) |
| Number of batches/sec, many tiny batches | UI/progress | Trigger too aggressive for the data rate |

**Python listener** (3.4+) to export progress to your monitoring system:

```python
from pyspark.sql.streaming import StreamingQueryListener

class ProgressExporter(StreamingQueryListener):
    def onQueryStarted(self, event): pass
    def onQueryProgress(self, event):
        p = event.progress
        emit_metric("stream.input_rps", p.inputRowsPerSecond, tags={"query": p.name})
        emit_metric("stream.process_rps", p.processedRowsPerSecond, tags={"query": p.name})
        emit_metric("stream.batch_ms", p.batchDuration, tags={"query": p.name})
    def onQueryIdle(self, event): pass          # newer versions
    def onQueryTerminated(self, event):
        emit_alert("stream.terminated", {"id": str(event.id), "error": event.exception})

spark.streams.addListener(ProgressExporter())
```
(`emit_metric`/`emit_alert` are your hooks into StatsD, Prometheus pushgateway, CloudWatch, etc.)

**Streaming debugging order:** is it running and progressing → is data arriving → are outputs appearing (watermark moving?) → is it keeping up (rates, durations) → is state healthy (rows, memory, drops) → is the sink the bottleneck → after restarts, checkpoint/config compatibility.

---

## 17. Common Errors Catalog

| Error / symptom | Meaning | First checks |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` | JVM heap exhausted (driver or executor) | Which process; partition sizes, skew, caching, `collect()`; heap dump |
| `GC overhead limit exceeded` | JVM spending nearly all time in GC | Same as above; fewer cores/executor, smaller partitions |
| `SparkOutOfMemoryError: Unable to acquire X bytes of memory` | Task couldn't get execution memory (pool exhausted) | Memory per task, skew, concurrent tasks per executor |
| `Container killed by YARN for exceeding memory limits` / `OOMKilled` / exit 137 | Total process memory above the container limit | `memoryOverhead`, PySpark/RocksDB/off-heap budget |
| `ExecutorLostFailure ... Reason: Remote RPC client disassociated` | Executor died or became unreachable | Executor logs, GC logs, cluster-manager events |
| `FetchFailedException` | Reducer couldn't fetch map output | Why the map-side executor/node vanished; shuffle service; retries |
| `Job aborted due to stage failure: Task N in stage S failed 4 times` | One task exhausted `spark.task.maxFailures` | First failure's root cause; deterministic vs. transient |
| `Task not serializable` / `PicklingError` | Closure captured a non-serializable object | Construct inside `mapPartitions`; avoid capturing `spark`/`sc` |
| `Total size of serialized results ... > spark.driver.maxResultSize` | Too much data collected to the driver | Don't `collect()`; write instead; raise the limit deliberately |
| `Could not execute broadcast in N secs` / `TimeoutException` on broadcast | Broadcast side slow to build (timeout, not size) | `spark.sql.broadcastTimeout`, upstream cost, broadcast threshold |
| `AnalysisException: cannot resolve ... / Resolved attribute(s) ... missing` | Column/ambiguity problem (often self-joins) | Aliases, column lineage, case sensitivity |
| `Py4JJavaError` / `PythonException` | Wrapped JVM/Python error | Read the embedded Python traceback |
| `Python worker exited unexpectedly (crashed)` | Worker killed or crashed | Memory (137), Python mismatch, segfault, `faulthandler` |
| `java.io.IOException: No space left on device` | Local disk full (shuffle/spill/logs) | `spark.local.dir`, skew, `/tmp`, ephemeral storage |
| `Too many open files` | OS file descriptor limit | `ulimit -n`, shuffle file counts, connection leaks |
| `NoSuchMethodError` / `ClassNotFoundException` | Dependency version conflict | Environment tab classpath, shading, library versions |
| `StackOverflowError` | Extremely deep plans/lineage or recursion | Checkpoint to cut lineage, simplify iterative logic, driver stack size |
| `Cannot grow BufferHolder` / `NegativeArraySizeException` / array size limit | Single row/array/`collect_list` exceeding ~2 GB limits | Reduce row size, avoid giant aggregations of values per key |
| `Kryo serialization failed: Buffer overflow` | Serialized object larger than the Kryo buffer | `spark.kryoserializer.buffer.max` or reduce record size |
| `FileNotFoundException ... underlying files may have been updated` | Stale cached listing/metadata | `REFRESH TABLE`; avoid read-then-overwrite of same path |
| `Parquet column cannot be converted` / schema merge failure | Schema drift between files/tables | Explicit schema, merge options, source contract |
| `Required executor memory (..) is above the max threshold` | Request exceeds YARN/K8s limits | Container size math vs. `maximum-allocation-mb`/quota |
| `Spark session/context was shut down` / `Job cancelled because SparkContext was stopped` | Application ended externally or driver died | Driver logs, orchestrator/cluster-manager, driver OOM |
| `Dropping SparkListenerEvent because no remaining room in event queue` | Driver can't keep up with events | Fewer tiny tasks, bigger driver, queue capacity, UI retention |

---

## 18. Monitoring Design: SLIs, Dashboards, Alerts, Runbooks

### 18.1 What to measure

**Batch pipelines**
- *Reliability:* success rate, retry counts, failed tasks/stages, executor-loss rate, FetchFailed counts.
- *Timeliness:* start delay (queue wait), end-to-end duration vs. SLA and vs. rolling baseline, **data freshness** (age of the latest output partition).
- *Efficiency:* core-hours, CPU utilization (active tasks ÷ cores), spill bytes, GC %, shuffle bytes, input vs. output size, skew ratio.
- *Data health:* input/output row counts, null/invalid ratios, quarantine volume, duplicate rates, schema changes, reconciliation checks against sources.

**Streaming** — input vs. process rate, batch duration vs. trigger, source lag, watermark delay, state rows/memory, dropped rows, restarts, time since last successful batch.

**Platform** — pending containers/pods and queue wait, node health, disk usage on local dirs, shuffle service health, History Server availability, metastore latency.

### 18.2 Dashboard design
1. **Fleet overview:** last-N runs per job: status, duration vs. baseline, cost, freshness.
2. **Job drill-down:** per-stage durations, skew ratio, spill, GC, shuffle, executor count over time.
3. **Resource view:** requested vs. used cores/memory (right-sizing candidates), idle executor time.
4. **Streaming view:** rates, lag, watermark, state, batch duration.
5. **Data-quality view:** row counts and quality ratios per run and per table.

### 18.3 Alerting principles
- **Alert on symptoms users feel** (late data, failed pipelines, growing lag) and on **leading indicators** (executor loss rate, spill growth, state growth, watermark stalls, duration trending toward SLA).
- **Baselines over fixed thresholds:** alert when a run is, say, 1.5–2× its recent median rather than a hard-coded number that breaks as data grows.
- **Deduplicate and group** (one page per incident, not per task failure). **Every alert needs a runbook link** and an owner.
- **Alert on silence:** no successful run or no streaming progress within the expected window (heartbeat/"dead man's switch"), not just on errors.

### 18.4 Runbooks and post-incident capture
- A runbook maps *symptom → likely causes → evidence to collect → fix/mitigation → escalation*, drawing from §4, §9, §10, §17.
- **Capture evidence before it disappears**: event logs, aggregated driver/executor logs, cluster-manager events, heap/thread dumps, the exact config and code version. Set retention for these deliberately (History Server cleaner, log retention, pod log shipping).
- **Post-incident:** record root cause, detection gap (how late did you notice, and why), and the monitoring or guardrail added (new metric, alert, validation gate, config default).

### 18.5 A reference observability stack
| Concern | Typical building blocks |
|---|---|
| Per-application deep dive | Spark UI + History Server on a shared event-log store |
| Time series | Dropwizard/Prometheus sinks or executor-metric endpoints → Prometheus/Graphite → Grafana |
| Logs | Log aggregation (YARN aggregation, Kubernetes agent/sidecar) → searchable store (structured JSON in Spark 4) |
| Fleet analytics | Event logs parsed by Spark into a "job history" table; run-control table with business metrics |
| Alerting | Alert manager with baselines, SLO burn-rate or freshness checks, on-call routing |
| Cost attribution | Job/team tags on clusters, namespaces, queues; core-hour accounting from event logs/metrics |

---

## 19. Reproducing and Isolating Problems

- **Shrink the problem:** a single failing partition/key/file; a sample (`df.sample(fraction, seed)`, or filter to the failing key); fewer shuffle partitions (`spark.sql.shuffle.partitions`) for speed.
- **Run locally:** `local[*]` for logic bugs; `local[N, F]` (allow F task failures) to exercise retries; `local-cluster[...]` for a mini multi-executor test. Remember local mode does not reproduce memory limits, shuffle service behavior, Python environment differences, or object-store semantics.
- **Make it deterministic:** fixed seeds, explicit ordering, pinned input snapshot (table version/path), same config.
- **Bisect the pipeline:** write intermediate DataFrames to temporary Parquet and test stages independently; binary-search which transformation introduces the issue.
- **Fail fast while debugging:** `spark.task.maxFailures=1` so deterministic failures surface immediately.
- **Toggle features** (§12) to separate your logic from engine behavior.
- **Use unit tests** with small DataFrames for transformation functions (keep logic in pure `df -> df` functions), plus a handful of end-to-end tests with realistic edge cases: nulls, duplicates, empty inputs, late data, schema drift, skewed keys.
- **Reproduce at scale only when needed:** some problems (skew, memory, shuffle loss, timeouts) only appear with full-size data; use a scaled-down but structurally similar dataset, or replay a failing day in a staging environment with the same cluster shape.

---

## 20. Configuration Reference

| Config | Default | Purpose |
|---|---|---|
| `spark.ui.enabled` / `spark.ui.port` | true / 4040 | Web UI |
| `spark.ui.retainedJobs` / `retainedStages` / `retainedTasks` | 1000 / 1000 / 100000 | UI history retained in driver memory |
| `spark.sql.ui.retainedExecutions` | 1000 | SQL executions retained |
| `spark.ui.reverseProxy` | false | Serve worker/app UIs through the master (proxy setups) |
| `spark.eventLog.enabled` / `spark.eventLog.dir` | false / — | Persist events for the History Server |
| `spark.eventLog.rolling.enabled` | false | Roll event logs for long-running apps |
| `spark.eventLog.logStageExecutorMetrics` | false | Per-stage executor peak metrics in event logs |
| `spark.executor.processTreeMetrics.enabled` | false | Process-tree (RSS) memory metrics |
| `spark.history.fs.logDirectory` | — | History Server event-log location |
| `spark.history.fs.cleaner.enabled` | false | Delete old event logs (enable it or use a lifecycle rule) |
| `spark.history.retainedApplications` / `spark.history.store.path` | 50 / unset | In-memory app cache / disk-backed store |
| `spark.metrics.conf.*` / `spark.metrics.namespace` | — | Metrics sinks and stable namespace |
| `spark.ui.prometheus.enabled` | false in many releases | Expose executor metrics at `/metrics/executors/prometheus` |
| `spark.logConf` | false | Log effective configuration at startup |
| `spark.scheduler.listenerbus.eventqueue.capacity` | 10000 | Listener event queue before events are dropped |
| `spark.executor.logs.rolling.*` | disabled | Roll executor logs |
| `spark.executor.extraJavaOptions` / `spark.driver.extraJavaOptions` | — | GC logging, heap dumps, profilers, NMT (not `-Xmx`) |
| `spark.task.maxFailures` | 4 | Set to 1 while debugging to fail fast |
| `spark.sql.planChangeLog.level` | trace | Level at which optimizer plan changes are logged |
| `spark.sql.optimizer.excludedRules` | — | Disable optimizer rules to isolate issues |
| `spark.sql.pyspark.jvmStacktrace.enabled` | false | Include JVM stack in Python exceptions |
| `spark.sql.debug.maxToStringFields` | 25 | Plan-string truncation limit |
| `spark.kubernetes.executor.deleteOnTermination` | true | Keep failed executor pods while investigating (set false) |

---

## 21. Common Misconceptions

| Misconception | Reality |
|---|---|
| "The last error in the driver log is the cause." | It is usually a wrapper (`Job aborted due to stage failure`). The root cause is the first `Caused by` and the first failed attempt's reason. |
| "The Spark UI will be there afterwards." | Only if event logs were enabled. Without them, a finished or crashed application has no UI. |
| "Executor logs are always retrievable." | They disappear with the container or pod unless aggregated or shipped. |
| "`print()` in my UDF shows up in my notebook/driver." | It goes to the **executor's** stdout/stderr. |
| "A stage that re-runs is a bug." | Often `FetchFailed` recovery; fix what destroyed the shuffle data. |
| "High total runtime means everything is slow." | Usually one stage or one skewed task dominates. Find it first. |
| "Average task time tells me about skew." | The **max vs. median** (or p95) shows skew; averages hide it. |
| "My config isn't working, so Spark ignores it." | Check the Environment tab: precedence, late setting, or cluster-manager override is more likely. |
| "OOM always means add memory." | Often partition size, skew, concurrency, or overhead/native memory is the real cause. |
| "If it works in local mode it's correct." | Local mode doesn't reproduce memory limits, shuffle service, Python env differences, or storage semantics. |
| "Accumulators give exact counts." | Only inside actions; inside retried/recomputed transformations they can over-count. |
| "Monitoring means dashboards." | It also means retained evidence (event logs, aggregated logs), baselines, alerts on silence, and runbooks. |

---

## 22. Debugging and Monitoring Checklist

**Before incidents (monitoring foundations)**
- [ ] Event logs enabled (rolling for long-running apps) on shared storage; History Server running; cleaner or lifecycle rule configured.
- [ ] Driver and executor logs aggregated and searchable; retention set; structured logging considered.
- [ ] Metrics exported (executor JVM/GC/off-heap/RSS, shuffle, task failures) with stable namespaces; dashboards for fleet, job drill-down, resources.
- [ ] Run-control table records run ID, window, status, rows in/out, bad-record ratio, duration, code version, config hash.
- [ ] Alerts on failures, SLA/freshness, baseline regressions, executor-loss rate, spill/state growth, streaming lag/watermark stalls, and on silence.
- [ ] Runbooks exist for the top failure classes, each with evidence to collect.
- [ ] Jobs named/described (`setJobDescription`/`setJobGroup`) so UI rows map to code.

**During an incident**
- [ ] Classify: failure, slowdown, hang, or wrong output.
- [ ] Find the root cause (first `Caused by`, first failed attempt), not the wrapper.
- [ ] Identify the slow/failing stage and its bottleneck type from the Stage page quantiles.
- [ ] Read the plan (SQL tab, AQE final plan) and confirm pushdown, join strategy, and shuffles.
- [ ] Capture evidence (logs, event log, thread/heap dumps, cluster events) before it expires.
- [ ] Change one thing; re-measure.

**After**
- [ ] Record root cause and detection gap; add the missing metric, alert, validation, or guardrail.
- [ ] Add a regression test or a baseline check.

---

## 23. A Concise Mental Model

- **Debug top-down along the execution hierarchy:** application → job → stage → task → executor/host → JVM/OS → data. The Spark UI mirrors this order.
- **Max vs. median beats averages.** Skew, bad nodes, and stragglers hide in the tail.
- **Classify before fixing:** failure, slowdown, hang, or wrong output each have different evidence and different workflows.
- **The first error and the first failed attempt are the truth;** the final stack trace is a wrapper.
- **Spark can't see into your code or the Python worker.** UDFs, closures, and external calls are black boxes to Catalyst and invisible to JVM metrics — use logs, profilers, and timeouts there.
- **Evidence is perishable.** Event logs, aggregated logs, and metrics must exist *before* the incident; containers, pods, and UI retention will not wait.
- **Event logs are data.** Query them to find skew, regressions, and cost across the whole fleet, not just one job.
- **Monitor outcomes and leading indicators, against baselines,** and alert on silence as well as errors — with a runbook attached.
- **Isolate engine behavior from your logic by toggling features,** make runs deterministic, and bisect the pipeline; most wrong-result bugs are data, join, null, time, or nondeterminism issues rather than engine bugs.
