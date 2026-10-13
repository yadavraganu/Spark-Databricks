## Table of Contents

1. The Failure Landscape
2. Foundations: Lineage, Immutability, and Determinism
3. Task Failures and Retries
4. Executor and Node Failures
5. Shuffle Data Loss and Stage Retries
6. Driver Failures
7. Cluster Manager and Infrastructure Failures
8. Persistence, Checkpointing, and Their Role in Recovery
9. Speculative Execution
10. Making Output Reliable: Commit Protocols and Write Semantics
11. Bad Data, Corrupt Files, and Fail-Fast vs. Fail-Safe
12. Designing Idempotent, Restartable Pipelines
13. Streaming Fault Tolerance (Summary)
14. Spot/Preemptible Capacity and Graceful Decommissioning
15. Timeouts, Heartbeats, and Cascading Failures
16. Observability and Failure Diagnosis
17. Testing Resilience
18. Failure-Scenario Matrix
19. Configuration Reference
20. Symptom-to-Cause Table
21. Production Reliability Checklist
22. Common Misconceptions
23. A Concise Mental Model

---

## 1. The Failure Landscape

A distributed engine runs on many machines, so partial failure is normal rather than exceptional. Spark's design goal is that **a single failed component should cost time, not correctness** — the job slows down, but the result stays right. It is worth separating *what fails* from *what recovers it*:

| What fails | Typical causes | Primary recovery mechanism |
|---|---|---|
| **A task** | Exception in user code, transient I/O error, bad record | Task retry (same or different executor) |
| **An executor** | JVM OOM, container kill (memory limit), GC pause past the heartbeat timeout, preemption | Executor replaced; its running tasks retried; its shuffle/cache data may be lost |
| **A node/host** | Hardware failure, spot reclamation, node drain | Same as executor loss, but *all* local shuffle files and cached blocks on it are gone — including external shuffle service data on that node |
| **Shuffle data** | Executor/node loss after the map stage finished; disk full; corruption | Recompute lost map outputs by resubmitting the producing stage |
| **The driver** | OOM, uncaught exception, host loss, killed by orchestrator | **No in-run recovery for batch** — the application restarts (or fails); streaming resumes from its checkpoint |
| **The cluster manager** | Master/ResourceManager crash, control-plane outage | Cluster-manager HA; running applications usually continue |
| **Input/output systems** | Missing files, request throttling, slow listings, files changed or deleted mid-job, eventual consistency on some stores (S3 itself has been strongly consistent since 2020), DB outage | Retries inside connectors; snapshot-isolated table formats; your own retry/idempotency design |
| **The data itself** | Corrupt files, schema drift, malformed rows, overflow | Mode settings (permissive/fail-fast), quarantine patterns, validation gates |
| **Your logic** | Non-determinism, non-idempotent side effects | Nothing automatic — this is where reliability is *designed*, not configured |

The most important framing: **Spark recovers computation, not side effects.** It can recompute a lost partition reliably if your code is deterministic. It cannot undo a row you already inserted into an external database. Most "Spark lost/duplicated my data" incidents are failures of the second kind.

---

## 2. Foundations: Lineage, Immutability, and Determinism

### 2.1 Lineage

Every RDD/DataFrame partition knows **how it was derived** from its parents: the chain of transformations is its *lineage*. Datasets are **immutable** and transformations are **lazy**, so Spark never mutates data in place; to recover a lost partition it simply re-runs the recipe against its parents' partitions.

- **Narrow dependencies** (`map`, `filter`, …): a lost partition depends on one parent partition — cheap to recompute, no cross-node data needed.
- **Wide dependencies** (`groupBy`, `join`, `repartition`): a lost partition may depend on *every* parent partition — recomputing it can mean re-running a large part of the upstream stage.

### 2.2 Shuffle files are implicit recovery points

Map-side shuffle output is written to **local disk** at the end of the producing stage and kept until the job (or `ContextCleaner`) releases it. That means recovery usually does **not** replay everything back to the source: it stops at the nearest surviving shuffle output. Lineage recomputation becomes expensive in two situations: when shuffle files are *lost* (node failure, §5), and when lineage is very **long** with no shuffle or checkpoint boundary (iterative algorithms, §8).

### 2.3 Determinism is the contract

Recomputation is only correct if re-running a transformation on the same input yields the same output. Spark assumes this. Things that break it:

- `rand()`, `uuid()`, `current_timestamp()`/`now()`, `monotonically_increasing_id()` used for *values that must be stable*
- UDFs that read mutable external state or have side effects
- Iteration over unordered collections (e.g., a Python `set`/hash order) feeding partitioning decisions
- Round-robin repartitioning, where which row lands in which partition depends on input order

Spark has partial protections: for DataFrames, `repartition(n)` without columns sorts data locally before round-robin distribution by default (`spark.sql.execution.sortBeforeRepartition`) so a recomputed partition receives the same rows; and the scheduler tracks an RDD's **output determinism level** (determinate / unordered / indeterminate). When a stage with *indeterminate* output has to be retried after a fetch failure, Spark must **roll back and re-run downstream stages** (or fail the job if a downstream result stage has already partially committed) rather than risk mixing outputs from two different executions. The practical lesson: *do not depend on non-deterministic values in anything that feeds a shuffle, a join key, or an output you cannot overwrite.* Generate IDs once, deterministically (hash of content), or persist them.

### 2.4 Accumulators and retries

Accumulator updates made inside **actions** are applied exactly once per task. Updates made inside **transformations** can be applied more than once if a task is retried, a stage is recomputed, or a speculative copy runs. Never use transformation-side accumulators for anything that must be exact (billing counts, row-count assertions); use DataFrame aggregations.

---

## 3. Task Failures and Retries

A **task** is the unit of retry. The `TaskSetManager` handles failures per stage attempt:

- A task that fails is rescheduled — preferably on a different executor/host — up to **`spark.task.maxFailures`** attempts (default **4**). If one task exhausts its attempts, the **stage fails and the job aborts**.
- Failure types are not equal:
  - **`ExceptionFailure`** (your code threw): counts toward `maxFailures`. A deterministic bug fails the same way four times, then aborts the job — retries cannot fix logic errors, only transient ones.
  - **`ExecutorLostFailure`**: usually counts, but not when the executor was lost for reasons not attributed to the task (for example preemption/decommission).
  - **`FetchFailed`**: does **not** count toward `maxFailures`; it triggers *stage* resubmission instead (§5).
  - **`TaskKilled`**: speculative duplicate lost the race, or the job was cancelled.
  - **`TaskCommitDenied`**: another attempt already committed output for that partition (§10).
- **Local mode** does not retry by default (`local[N]` runs with `maxFailures=1`; use `local[N, F]` to allow retries). Tests that pass locally may therefore behave differently on a cluster.

### Excluding unhealthy resources (formerly "blacklisting")

`spark.excludeOnFailure.enabled` (default `false`; named `spark.blacklist.*` before 3.1) lets Spark stop scheduling onto executors/nodes that keep failing tasks, with thresholds at three levels:

| Level | Meaning |
|---|---|
| **Task** | A task is not re-run on an executor/node where it already failed (`task.maxTaskAttemptsPerExecutor`, `task.maxTaskAttemptsPerNode`) |
| **Stage** | An executor/node with too many failed tasks in a stage is excluded for that stage |
| **Application** | An executor/node with too many failures across the application is excluded for the exclusion `timeout`, optionally killing excluded executors (`killExcludedExecutors`) |

Use it when failures cluster on a few bad machines (a flaky disk, a misconfigured node) so that four attempts are not all spent on the same host. Combine with cluster-manager-level node health checks. Don't use exclusion to paper over systematic failures — it just shrinks your cluster.

---

## 4. Executor and Node Failures

### 4.1 Detection

Executors send **heartbeats** to the driver (`spark.executor.heartbeatInterval`, default 10 s). The driver declares an executor lost when heartbeats stop beyond `spark.network.timeout` (default 120 s), or when the cluster manager reports the container/pod gone. Common reasons: OOM kill, a long GC pause that blocks the heartbeat thread, node failure, preemption, `kill -9`.

Spark's executors use distinct **exit codes** worth recognizing: **52** = `OutOfMemoryError` in the JVM; **50/51** = uncaught exception; **137** = SIGKILL (Linux OOM killer or Kubernetes `OOMKilled`, i.e., native/overhead memory exhausted — see the memory guide); **143** = SIGTERM (graceful termination, drain, or scale-down); on YARN, "Container killed by YARN for exceeding memory limits" is the cluster manager enforcing container size.

### 4.2 What is lost and what is not

| Lost with the executor | Survives |
|---|---|
| Running tasks (retried elsewhere) | Data in the source system |
| Cached/persisted blocks held by that executor (unless replicated) | Checkpointed data on reliable storage |
| Shuffle output held on that executor's local disk **unless** an external shuffle service (ESS) serves it | Shuffle files served by a still-alive ESS (executor loss only, **not node loss**) |
| Broadcast variable copy (re-fetched from the driver/peers on demand) | Driver-side state |

### 4.3 What Spark does

1. Marks the executor lost, retries its running tasks (not charged against the task's failure count when the loss isn't the task's fault).
2. Requests a replacement from the cluster manager (subject to quotas and application-level failure limits). Since 3.5, **`spark.executor.maxNumFailures`** (default: twice the executor count, minimum 3) together with **`spark.executor.failuresValidityInterval`** caps executor failures on YARN and on Kubernetes with the direct pod allocator, so a systemic problem (bad image, impossible memory request, poisoned data) fails the application instead of replacing executors forever; older YARN releases used `spark.yarn.max.executor.failures`. Check platform support for your version.
3. Removes the lost executor's registered shuffle outputs and cached blocks from tracking; downstream tasks that need them either recompute or trigger stage resubmission (§5).
4. If too many executors fail, the application as a whole fails (on YARN, bounded by executor failure limits; on Kubernetes, by pod-creation failure handling).

**Node loss is worse than executor loss** because the node's local disk goes with it: shuffle files and cached blocks stored there are gone *even if an external shuffle service was serving them*. Systemic mitigations: replicate cached data, use remote/decoupled shuffle storage, or accept recomputation (§5, §14).

---

## 5. Shuffle Data Loss and Stage Retries

### 5.1 `FetchFailedException`

When a reducer cannot fetch a map output (executor/node gone, file missing, corrupt block), the task fails with `FetchFailed`. This is **not** a task retry problem; the DAG scheduler's response is:

1. Mark the map output(s) as lost (potentially all outputs on that executor/host, depending on shuffle-service settings).
2. **Resubmit the producing map stage** for only the missing partitions, then re-run the failed reduce stage.
3. Repeat until success or until a stage has failed **`spark.stage.maxConsecutiveAttempts`** (default 4) times in a row, at which point the job aborts.

A stage visibly **re-running after it looked complete** — in the UI, "skipped"/re-attempted stages with higher attempt numbers — is this recovery in action, not a hang. The real fix is to stop whatever is destroying shuffle data (OOM-killed executors, spot churn, aggressive scale-down, full local disks).

Fetch-level resilience, before a `FetchFailed` is raised: `spark.shuffle.io.maxRetries` (default 3) and `spark.shuffle.io.retryWait` (default 5 s) retry transient network errors on each connection. Longer waits ride out brief node hiccups (GC pauses, short network blips) without invoking expensive stage recomputation. Shuffle block **checksums** (`spark.shuffle.checksum.enabled`, on by default in recent releases) and fetch-time corruption detection (`spark.shuffle.detectCorrupt`, default `true`) help catch corrupted shuffle data rather than silently reading garbage; a detected corruption surfaces as a fetch failure and triggers recomputation.

Scope of what gets recomputed: by default, a `FetchFailed` un-registers only the map outputs of the **exact executor** that failed to serve them. When an external shuffle service serves files for the whole host, `spark.files.fetchFailure.unRegisterOutputOnHostOnFetchFailure` (default `false`) makes Spark un-register **all outputs on that host** at once, which avoids discovering the same host failure one fetch failure at a time.

### 5.2 Ways to make shuffle data survive

| Option | What it protects against | Limits |
|---|---|---|
| **External shuffle service (ESS)** | Executor death or removal (dynamic allocation); files served by a node-level daemon | Lost if the *node* dies; needs ESS deployment (YARN aux service, standalone worker, K8s workarounds) |
| **Shuffle tracking** (`spark.dynamicAllocation.shuffleTracking.enabled`; enabled by default with dynamic allocation and no ESS since 3.4) | Prevents the driver from removing an executor whose shuffle data is still needed | Executors with needed shuffle data linger until the shuffle is no longer needed (cost); does not help if the executor *crashes* |
| **Graceful decommissioning** (`spark.decommission.enabled`, `spark.storage.decommission.*`) | Planned removal (spot notice, drain, scale-down): migrates shuffle and cached blocks to other executors first | Needs warning time; no help for sudden crashes |
| **Push-based shuffle** (`spark.shuffle.push.enabled`, 3.2+, YARN with ESS) | Merges shuffle blocks on remote nodes, giving a second copy and reducing small-block fetch failures | YARN-focused; adds overhead; check support for your platform |
| **Remote/disaggregated shuffle services** (e.g., Apache Celeborn, Apache Uniffle) | Shuffle data stored off the compute nodes; node loss no longer destroys it | Extra infrastructure to operate; plugs in through Spark's shuffle APIs |
| **Decommission fallback storage** (`spark.storage.decommission.fallbackStorage.path`, 3.1+) | When no peer executor can accept migrated shuffle blocks, they are written to a shared filesystem/object store and read from there | Only helps *planned* decommissioning; adds I/O and a cleanup obligation |
| **Kubernetes PVC reuse** (`spark.kubernetes.driver.ownPersistentVolumeClaim`, `spark.kubernetes.driver.reusePersistentVolumeClaim`; PVC reuse since 3.2, shuffle recovery via the K8s local-disk shuffle plugin in later releases) | A replacement executor reuses the lost executor's volume so its shuffle files can be recovered | Kubernetes only; needs dynamically provisioned volumes; verify support in your version |

Pick based on the failure you actually have: executor churn → ESS or shuffle tracking; planned node removal/spot → decommissioning; unexpected node loss at scale → decoupled shuffle.

### 5.3 Why a long shuffle-heavy job is the most fragile kind

A job with many sequential wide stages on cheap/preemptible capacity can enter a **recomputation cascade**: losing a node late in the job loses shuffle files from many *earlier* stages, forcing re-execution of upstream stages whose own inputs may also be lost. Break the chain by materializing intermediate results to durable storage at natural boundaries (§12), which converts "recompute from the start" into "resume from the last durable stage."

---

## 6. Driver Failures

The driver holds the DAG, scheduler state, broadcast data, accumulators, and the result stage. **Batch applications have no in-run driver recovery**: if the driver dies, executors are released and the application ends.

| Deployment | What happens on driver death | What to configure |
|---|---|---|
| **Standalone, cluster mode** | Application ends unless submitted with `--supervise`, which makes the Master restart the driver (the app reruns from the beginning) | `--supervise`; Master HA (§7) |
| **YARN, cluster mode** | The ApplicationMaster hosts the driver; YARN retries the *application attempt* up to `spark.yarn.maxAppAttempts` (bounded by the RM's `am.max-attempts`) — **each attempt starts from scratch** | `spark.yarn.maxAppAttempts`, `spark.yarn.am.attemptFailuresValidityInterval` (so old failures don't permanently count against the limit) |
| **YARN, client mode** | Driver lives on the submitting host; if that host/session dies, the app dies | Prefer cluster mode for production |
| **Kubernetes** | The driver pod is a single pod; if it dies, executors are cleaned up with it | Run via a controller/orchestrator (Kubernetes Job with a restart policy, Spark Operator with `restartPolicy`) |
| **Streaming query** | Application restarts and **resumes from the checkpoint** (offsets and state), if supervision restarts it | Supervision + a durable checkpoint (§13) |

**Spark Connect** moves the session into a server process, so a client (notebook, script) disconnecting no longer implies the same failure as losing a classic driver, and recent releases add *reattachable execution* so a client can reconnect to a running query after a network drop. The Connect server then becomes the component to supervise and size, and a server crash still loses running work. Check your version's behavior before relying on it.

Because a restarted batch driver **re-executes the entire job**, the real recovery strategy for batch is **idempotent, restartable pipelines** (§12), not clever Spark settings.

**Prevent avoidable driver deaths**: size `spark.driver.memory`; cap collected results (`spark.driver.maxResultSize`); don't `collect()`/`toPandas()` large data; bound broadcasts (`spark.sql.autoBroadcastJoinThreshold`, `spark.sql.broadcastTimeout`); watch driver GC; keep the driver off preemptible capacity; avoid excessive numbers of tiny jobs and listener events that bloat driver memory (event queue drops are a symptom).

---

## 7. Cluster Manager and Infrastructure Failures

- **Standalone:** a single Master is a single point of failure for *scheduling new work*. Configure Master HA with ZooKeeper (`spark.deploy.recoveryMode=ZOOKEEPER`, `spark.deploy.zookeeper.url`) or filesystem-based single-node recovery. **Already-running applications keep running** during a Master failover; what is blocked is submitting applications and acquiring new executors until a new leader is elected and recovers state.
- **YARN:** enable ResourceManager HA; node-health scripts and node labels keep applications off unhealthy hosts; capacity/fair scheduler queues plus preemption determine whether containers can be taken away (relevant for executor loss — §14).
- **Kubernetes:** pods can be evicted (node pressure, drains, spot reclamation). Use PodDisruptionBudgets, priority classes, appropriate resource requests/limits, and decommissioning so evictions are graceful.
- **Storage and networking:** object stores throttle (`503 SlowDown`), listings may be slow, namenode failover pauses HDFS clients, DNS and network partitions cause transient `ConnectException`/timeouts. Rely on connector-level retries (`fs.s3a.attempts.maximum`, `fs.s3a.retry.*`) and keep `spark.network.timeout` and shuffle retry settings generous enough to ride out brief outages without escalating to stage recomputation.
- **Catalog/metastore outages:** a Hive Metastore (or other catalog) that is down or slow fails jobs at *planning* time, before any task runs, and can stall many jobs at once. Treat it as a tier-0 dependency: HA, connection retries, and alerting on planning latency.
- **Expiring credentials in long-running applications:** Kerberos delegation tokens, cloud credentials, and database passwords can expire mid-run, producing failures that appear only after hours or days (a classic for streaming drivers). Configure principal/keytab or credential renewal for long-lived apps, and test a run longer than your shortest token lifetime.
- **Kubernetes debugging tip:** executor pods are deleted on termination by default, which erases the evidence. Set `spark.kubernetes.executor.deleteOnTermination=false` while investigating `OOMKilled`/`Evicted` pods, and clean up afterwards.

---

## 8. Persistence, Checkpointing, and Their Role in Recovery

### 8.1 Caching is a performance feature, not durability

`cache()`/`persist()` blocks are **best-effort**: they can be evicted under memory pressure and are lost with their executor. Losing a cached block never changes correctness — the partition is recomputed from lineage — it only costs time.

- Replicated storage levels (`MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`) keep a second copy on another executor, so a single executor loss does not force recomputation of those blocks. Recent releases can **proactively re-replicate** lost block replicas (`spark.storage.replication.proactive`, enabled by default from Spark 4.0).
- Combine with decommissioning (`spark.storage.decommission.rddBlocks.enabled`) so cached blocks migrate off an executor that is about to leave.

### 8.2 Checkpointing truncates lineage and survives failures

A **reliable checkpoint** writes data to durable storage (HDFS/S3/etc.) and **cuts the lineage**: recovery after that point starts from the checkpointed files, not from the original source.

```python
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/job-x")   # durable, shared storage

df = (heavy_transform(df)).persist()     # persist first (see below)
df = df.checkpoint(eager=True)           # Dataset API: materializes now, truncates lineage
```

Points that bite:

- **RDD checkpointing runs a *separate job* after the action** to write the data, and recomputes the RDD unless it is already persisted — so `persist()` before `checkpoint()`. `Dataset.checkpoint(eager=True)` (the default) materializes immediately.
- **`localCheckpoint()`** stores data on executors' local storage (fast, no durable write) and truncates lineage, but it is **not fault-tolerant**: losing the executor loses the checkpoint and the job fails or must recompute. Use it to shorten lineage for planning performance, not for durability.
- Checkpoint files are **not cleaned up automatically** by default (`spark.cleaner.referenceTracking.cleanCheckpoints=false`); manage the directory's lifecycle. Compression of RDD checkpoints (`spark.checkpoint.compress`) is off by default in older releases and on in Spark 4.1.
- Use it where lineage is genuinely long (iterative ML/graph algorithms) or where recomputation after a late-stage failure would be prohibitively expensive.

### 8.3 Checkpoint vs. cache vs. writing a table

| Need | Use |
|---|---|
| Reuse within a job, losing it is fine | `cache()`/`persist()` |
| Bound lineage length in iterative algorithms | `checkpoint()` (or `localCheckpoint()` if durability is unnecessary) |
| Survive driver loss or restart **between pipeline stages** | **Write an intermediate table/files** (a real, named, validated output) — more useful than a checkpoint because it is inspectable, reusable, and not tied to one application |
| Streaming progress/state | Structured Streaming `checkpointLocation` (§13) |

---

## 9. Speculative Execution

**Speculation** (`spark.speculation=true`, default `false`) launches a **duplicate copy** of a slow-running task on another executor; whichever finishes first wins and the other is killed.

- **What it fixes:** *stragglers caused by the environment* — a degraded disk, a noisy neighbor, a temporarily slow node.
- **What it does not fix:** data skew (a copy of a skewed task has the same huge input and is just as slow), bugs, or resource starvation. It also adds load, so enabling it on an already-saturated cluster can make things worse.
- **Tuning knobs:** `spark.speculation.interval` (how often to check, default 100 ms), `spark.speculation.multiplier` and `spark.speculation.quantile` (how much slower than the median a task must be, and how much of the stage must be finished first), and `spark.speculation.minTaskRuntime` (don't speculate tiny tasks). **Defaults changed in Spark 4.0**: multiplier and quantile moved from 1.5 / 0.75 to **3 / 0.9** (more conservative); you can restore the earlier values explicitly. Newer releases also add efficiency-based criteria (`spark.speculation.efficiency.*`) to avoid duplicating tasks that are slow only because they have more data.
- **Safety:** for files written through Hadoop output committers, the **OutputCommitCoordinator** guarantees only one attempt per partition commits, so speculation is safe for normal file/table writes. **It is not safe for external side effects** performed inside tasks (inserting into a database or calling an API from `foreachPartition`): both copies may execute the side effect. Either keep those writes idempotent or leave speculation off for such jobs.
- Speculation interacts with committer choice: algorithm "v2" of the Hadoop `FileOutputCommitter` writes task output directly to the final location and is **unsafe with speculation** or task retries because partial output from losing attempts can remain visible (§10).

---

## 10. Making Output Reliable: Commit Protocols and Write Semantics

Computation retries are only safe if **output commits are atomic per task and per job**. This is where many "Spark duplicated/lost my data" problems originate.

### 10.1 The task/job commit protocol

For file-based sinks, tasks write to a **temporary attempt directory**; a task commit promotes that attempt's files; a job commit finalizes the whole job. The `OutputCommitCoordinator` on the driver authorizes exactly one attempt per partition to commit (other attempts get `TaskCommitDenied`), which is why retries and speculation do not produce duplicate files for normal Spark writes.

### 10.2 Committer algorithms and storage systems

| Committer | Behavior | Reliability notes |
|---|---|---|
| **`FileOutputCommitter` v1** | Task commit renames to a job-attempt directory; **job commit renames to the final location** | Two-step; failed/aborted jobs don't leave final data; but job commit does many renames (slow on object stores) |
| **`FileOutputCommitter` v2** | Task commit moves files **directly to the final directory** | Faster, but a job that fails midway **leaves partial output visible**, and failed/speculative attempts can leave stray files — avoid where correctness matters |
| **Cloud-aware committers** (S3A *staging*/*magic* committers; Hadoop manifest committer for ABFS/GCS) | Avoid rename-as-commit; use multipart-upload completion or a manifest to commit atomically-ish | Required for correct, fast writes on object stores, where **rename is copy+delete and not atomic**. Check your platform's defaults (Spark 4.1 defaults to the Magic committer for S3 buckets) |
| **Table formats (Delta, Iceberg, Hudi)** | Commit = atomic metadata/transaction-log update | Readers see all-or-nothing snapshots; the right answer for production tables |

On every file-based layout, a successful job writes a **`_SUCCESS` marker** at job commit. Downstream consumers should treat a directory without `_SUCCESS` as incomplete. (Table formats make this unnecessary.)

### 10.3 Save modes and what a retry does

| Mode | Retry/rerun consequence |
|---|---|
| `append` | **A rerun appends again → duplicates.** Retrying a whole job after partial success is dangerous unless writes are idempotent or the target is partitioned and overwritten per run |
| `overwrite` (default **static** partition overwrite) | For path-based sinks, Spark **deletes the existing target before writing**; if the job fails mid-write you can be left with **less data than before the run**. Static overwrite of a partitioned table also drops *all* partitions, not just the ones in the new data |
| `overwrite` with `spark.sql.sources.partitionOverwriteMode=dynamic` | Only partitions present in the new data are replaced — much safer for daily/hourly reruns |
| `errorifexists` (default) / `ignore` | Safe against duplicates, but a rerun after partial failure may fail or silently skip — design explicit cleanup |

**Reliable write patterns**

1. **Write-then-publish:** write to a staging path, validate (row counts, null checks, schema), then atomically publish (rename on HDFS, table-format commit, `ALTER TABLE ... ADD PARTITION`/swap a view or pointer).
2. **Partition-scoped, idempotent reruns:** key each run by logical time (date/hour) and overwrite that partition dynamically.
3. **Upsert/MERGE** on natural keys for tables that receive late/corrected data.
4. **Never write the same path concurrently** from two jobs.
5. **Prefer a table format** on object stores; hand-rolled rename-based protocols are the usual source of partial-output incidents.
6. **Keep a rollback path.** Table formats retain prior snapshots (time travel/`RESTORE`, snapshot rollback), so a bad or partial write can be reverted instead of rebuilt. For plain files, keep the previous published version until the new one is validated.

### 10.4 External sinks: JDBC, Kafka, NoSQL, and APIs

Task/job commit protocols protect **files**. They do nothing for external systems, where retries and speculation can repeat a side effect.

- **JDBC writes:** each task writes its partition in its own transaction(s); there is **no job-level transaction**. A failed job leaves behind the data committed by tasks that already succeeded (partial output), and a task that committed but whose executor died before reporting is **retried, inserting duplicates**. Mitigate by writing to a **staging table** and then publishing with a single `MERGE`/`INSERT ... SELECT`/table swap, or by upserting on a natural or idempotency key. Limit `numPartitions` so parallel writers don't overwhelm the database.
- **Kafka batch writes** (`df.write.format("kafka")`): at-least-once; retried or speculated tasks can duplicate records. Include a deterministic key/ID and dedupe downstream.
- **NoSQL and HTTP APIs:** use idempotent operations (put-by-key, conditional writes, idempotency keys), bounded concurrency and exponential backoff so a retry storm doesn't take down the target, and client-side timeouts (§15.1).
- **Pattern:** make external writes the **last** step, perform them via `foreachPartition` (or `foreachBatch` in streaming) with an idempotent upsert, and record a `run_id` for auditing. Turn speculation off for jobs whose external writes cannot be made idempotent.

---

## 11. Bad Data, Corrupt Files, and Fail-Fast vs. Fail-Safe

Not all failures are infrastructure. A single malformed record can fail a four-hour job on its fourth retry. The design choice is **fail-fast** (stop and alert) vs. **fail-safe** (isolate bad data and continue) — and it should be deliberate.

| Source | Controls | Notes |
|---|---|---|
| **CSV/JSON parsing** | `mode`: `PERMISSIVE` (default; nulls plus optional `_corrupt_record` column via `columnNameOfCorruptRecord`), `DROPMALFORMED`, `FAILFAST` | `PERMISSIVE` silently nulls bad fields unless you inspect the corrupt column; `DROPMALFORMED` loses data silently |
| **Corrupt or truncated files** | `spark.sql.files.ignoreCorruptFiles` (default `false`), `spark.files.ignoreCorruptFiles` for RDD APIs | Turning it on skips unreadable files **silently** — convenient, but you can lose data without noticing. Rows already read from a partially corrupt file are still returned, so you may get *part* of a file. If enabled, also count and alert on skipped files. In Spark 4.0+ use the `spark.sql.files.*` settings for DataFrame/SQL reads; the core `spark.files.*` ones apply to RDD APIs |
| **Vanished files** | `spark.sql.files.ignoreMissingFiles` (default `false`) | Helps against files deleted between listing and read; same silent-loss warning |
| **Invalid casts/overflow** | ANSI SQL mode (`spark.sql.ansi.enabled`; **default `true` from Spark 4.0**, `false` before) | ANSI mode throws on overflow and invalid casts instead of returning NULL — a fail-fast safeguard against silent corruption. Use `try_cast`, `try_divide`, etc., where NULL-on-error is intended |
| **Schema drift** | Explicit schemas, schema-merge options, contract checks | Fail early on unexpected types/columns rather than writing garbage |
| **UDF exceptions** | Wrap risky parsing in `try/except`/`Try`, return a status column | An uncaught exception in a UDF fails the task four times and aborts the job |

**The quarantine (dead-letter) pattern**

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType

# The corrupt-record column MUST be part of the schema, otherwise PERMISSIVE mode has nowhere to put it
schema = StructType([
    StructField("id", LongType()),
    StructField("ts", TimestampType()),
    StructField("_corrupt", StringType()),
])

parsed = (raw
    .withColumn("p", F.from_json("value", schema,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"}))
    .select("value", "p.*")
    .cache())                                   # parse once; reused by the counts and both writes

bad = parsed.filter(F.col("_corrupt").isNotNull()).select("value", "_corrupt")
good = parsed.filter(F.col("_corrupt").isNull()).drop("value", "_corrupt")

total, bad_n = parsed.count(), bad.count()

# 1) Keep the evidence first, so a failed gate does not destroy it
bad.write.mode("append").saveAsTable("quarantine.events_bad")

# 2) Fail-fast gate BEFORE publishing anything downstream
assert bad_n / max(total, 1) < 0.01, f"bad-record ratio {bad_n / max(total, 1):.2%} exceeds threshold"

# 3) Publish the good rows (use an idempotent write in real pipelines; see sections 10 and 12)
good.write.mode("append").saveAsTable("silver.events")
parsed.unpersist()
```

Combine the two: *tolerate* a small, measured amount of bad data (quarantined and visible) while **failing loudly** if the ratio crosses a threshold — otherwise an upstream format change quietly turns your table into NULLs. (Some platforms offer built-in bad-record paths, but the pattern above works everywhere.)

---

## 12. Designing Idempotent, Restartable Pipelines

Because a restarted driver reruns the job from the beginning, **batch reliability is mostly a property of your pipeline design**.

**Principles**

1. **Idempotent by construction:** running the same job twice for the same input window produces the same final state. Overwrite-by-partition, MERGE, deterministic IDs, and atomic publish achieve this; blind appends do not.
2. **Deterministic transformations:** derive IDs/timestamps from data, not from `uuid()`/`now()` at run time; seed any randomness; sort before operations that depend on order.
3. **Stage boundaries with durable intermediates:** write bronze → silver → gold (or equivalent) so a failure resumes at the failed stage, not at the source.
4. **Small, window-scoped jobs:** process one date/hour per run; failures and reruns affect one partition.
5. **Run identity and bookmarks:** record `(job, window, run_id, status)` in a control table; skip or overwrite already-completed windows deterministically.
6. **Validation gates:** row counts, uniqueness, null rates, referential checks *before* publishing; fail the run (and keep the previous good data live) when gates fail.
7. **Retries at the right layer:** let Spark retry tasks/stages; let the orchestrator retry **whole jobs** with backoff only if jobs are idempotent; cap retries; alert after the cap.
8. **Side effects last and keyed:** external writes (databases, APIs) should be the final step, idempotent (upsert/`INSERT ... ON CONFLICT`, idempotency keys), and ideally batched through `foreachPartition` with a deduplication key.
9. **Time-bounded everything:** timeouts on external calls inside tasks so a hung dependency does not stall the stage until the heartbeat machinery gives up.

**Exactly-once effects in batch** are therefore achieved by *deterministic recomputation + atomic or idempotent output* — not by anything the engine does automatically for external systems.

---

## 13. Streaming Fault Tolerance (Summary)

The complete treatment is in the Structured Streaming guide; the essentials:

| Mechanism | Role |
|---|---|
| **Replayable source + offset log** | Each micro-batch's offset range is durably logged *before* processing, so a failed batch is replayed with the identical range |
| **Commit log** | Marks batches as complete; tells recovery where to resume |
| **Versioned state store + checkpoint** | State updates are tied to batch versions; a replay overwrites a partial attempt; state is restored from the checkpoint after restart |
| **Idempotent/transactional sink** | Makes replaying a batch harmless (duplicates avoided) |
| **Driver supervision** | Something external must restart a failed streaming driver; the engine then resumes from the checkpoint |

Key gotchas that are fault-tolerance issues: **exactly-once needs deterministic logic and an idempotent sink**; `spark.sql.shuffle.partitions` is **fixed per stateful checkpoint**; checkpoints are bound to query logic compatibility; watch watermark stalls and unbounded state; `failOnDataLoss` guards against silently skipping data that Kafka retention deleted.

---

## 14. Spot/Preemptible Capacity and Graceful Decommissioning

Spot and preemptible instances reduce cost but make executor loss **routine**. Without mitigation, frequent loss produces constant task retries, lost cached blocks, and `FetchFailed`-driven stage recomputation.

**Mitigations (layered)**

1. **Graceful decommissioning:** `spark.decommission.enabled=true` with `spark.storage.decommission.enabled`, `.shuffleBlocks.enabled`, and `.rddBlocks.enabled` lets an executor receiving a termination notice **migrate its shuffle and cached blocks to peers** before dying. Requires a platform that delivers the preemption signal and enough warning time (often tens of seconds to a couple of minutes).
2. **Keep critical components on stable capacity:** driver, and ideally any executors holding irreplaceable state, on on-demand instances; stateless task executors on spot.
3. **Shuffle durability:** external shuffle service / shuffle tracking for executor churn; remote shuffle services when node loss is expected.
4. **Replicate expensive cached data** (`_2` levels) or accept recompute.
5. **Diversify and over-provision:** multiple instance types/zones reduce correlated reclamation; slight over-provisioning absorbs losses.
6. **Make jobs resumable** (§12) — decommissioning shrinks recomputation, it doesn't eliminate it.
7. **Bound the damage:** `spark.stage.maxConsecutiveAttempts` and a sensible `spark.excludeOnFailure` configuration so a pathological churn period fails fast rather than looping.

---

## 15. Timeouts, Heartbeats, and Cascading Failures

Many "mysterious" failures are timeout interactions:

| Setting | Default | Role | Failure when mistuned |
|---|---|---|---|
| `spark.executor.heartbeatInterval` | 10s | Executor → driver heartbeat frequency | Must be **well below** `spark.network.timeout` |
| `spark.network.timeout` | 120s | Default for most network timeouts (RPC ask, shuffle connections) | Too low: GC pauses/transient stalls mark healthy executors lost; too high: real failures detected slowly |
| `spark.rpc.askTimeout` / `spark.rpc.lookupTimeout` | follow `network.timeout` | Driver/executor RPC waits | Cascading `TimeoutException`s under load |
| `spark.shuffle.io.maxRetries` / `retryWait` | 3 / 5s | Transient fetch failure tolerance | Too low → needless stage recomputation |
| `spark.sql.broadcastTimeout` | 300s | Time allowed to build and broadcast a broadcast-join side | "Could not execute broadcast in 300 secs" even when the data is small but its upstream is slow |
| `spark.executor.heartbeat.maxFailures` | 60 | Consecutive failed heartbeats before an executor shuts itself down | Without it, an executor that cannot reach the driver would linger as a zombie |

**A classic cascade:** a long full-GC pause on a large heap → no heartbeat → driver declares executor lost → its shuffle data is gone → `FetchFailed` → stage resubmission → extra load → more GC pressure. The remedy is addressing the root cause (heap sizing, state/partition size, off-heap, G1 tuning) *and* giving the timeouts enough slack to ride out transient pauses, rather than just raising retry counts.

### 15.1 Hung tasks: Spark has no per-task timeout

Heartbeats prove the *executor* is alive, not that a *task* is making progress. A task blocked on a dead network call, a deadlocked library, or an infinite loop can run forever while everything looks healthy: no failure, no retry, a stage stuck at 99%.

- **Put timeouts on every external call inside tasks** (HTTP, JDBC, RPC, object-store clients). This is the primary defense.
- **Speculation can rescue environmental stalls**, but a copy that calls the same hung dependency will hang too.
- **Set a job-level timeout and kill policy in the orchestrator**, sized from historical runtimes, so a stuck application is killed and retried instead of occupying the cluster for days.
- **Alert on long-running tasks** with a `SparkListener` or the metrics system (task running far longer than the stage median), which also catches skew early.
- **Zombie tasks:** when Spark kills a task (speculation loser, cancelled job), the task thread may keep running if user code ignores interruption. `spark.task.reaper.enabled` (default `false`) enables a monitor that watches killed tasks and can escalate (`spark.task.reaper.killTimeout`) to killing the executor JVM so leaked threads don't hold resources.

---

## 16. Observability and Failure Diagnosis

### 16.1 Instrument before you need it

- **Event logs** (`spark.eventLog.enabled=true`, `spark.eventLog.dir`) feed the **History Server**, the only way to inspect a finished or crashed application's stages, tasks, executors, and SQL plans. For long-running applications enable **rolling event logs** (`spark.eventLog.rolling.enabled`) so a single huge file doesn't become unreadable or unmanageable; a crashed application leaves an in-progress log that the History Server can usually still read.
- **Aggregated executor logs** (YARN log aggregation; Kubernetes log shipping) — executor logs vanish with the container otherwise.
- **`SparkListener`** hooks (`onTaskEnd`, `onExecutorRemoved`, `onStageCompleted`, `onJobEnd`) to export failures, retry counts, and executor-loss reasons to your monitoring system.
- **Metrics system** (JVM, executor, shuffle metrics) to Prometheus/Graphite; track executor loss rate, task failure rate, shuffle fetch wait, GC time, and spill.
- **Application-level counters:** rows read/written, bad-record counts, validation results, stored in a run-control table (not transformation-side accumulators).

### 16.2 Diagnose by classifying the failure

1. **Find the root cause, not the last error.** The stack trace that aborts the job is usually `Job aborted due to stage failure: Task N in stage S failed 4 times`; the real problem is the **first** `Caused by:` and the failure reason of the **first failed attempt**.
2. **Classify the failed attempt's reason** (Spark UI → Stages → failed tasks, or driver logs):

| Reason string (typical) | Meaning | Look at |
|---|---|---|
| `ExceptionFailure` / your exception | Bug or bad data | Stack trace; the offending partition/records; deterministic reproduction |
| `ExecutorLostFailure ... Container killed by YARN for exceeding memory limits` / exit 137 / `OOMKilled` | Overhead/native memory over container limit | `memoryOverhead`, PySpark/RocksDB/off-heap use (memory guide) |
| `java.lang.OutOfMemoryError` / exit 52 | Heap exhaustion | Partition size, skew, caching, `collect()`, memory guide |
| `FetchFailedException` | Lost/corrupt shuffle data | Why the executor/node vanished; ESS/decommissioning (§5) |
| `Connection reset`, `TimeoutException`, `Heartbeat ... timed out` | Network stall or GC pause | GC logs, `network.timeout`, node health |
| `No space left on device` | Local disk full (spill/shuffle) | `spark.local.dir`, skew, `/tmp` and log accumulation |
| `Could not execute broadcast in N secs` | Broadcast build too slow | `spark.sql.broadcastTimeout`, upstream cost |
| `TaskCommitDenied`/`FileAlreadyExistsException` | Commit conflicts or stale output dirs | Output committer, leftover `_temporary`, concurrent writers |
| `Total size of serialized results ... bigger than spark.driver.maxResultSize` | Too much data returned to the driver | Avoid `collect()`; raise limit deliberately |
| `SparkException: Job ... cancelled because SparkContext was shut down` | Driver/app terminated externally | Orchestrator/cluster-manager logs, driver OOM |

3. **Decide whether it is transient or deterministic.** If the same task fails four times identically, retries cannot help — fix the code/data. If failures move around between executors, suspect infrastructure.
4. **Check scale effects:** failures that appear only at scale are usually skew, memory, or timeouts rather than logic.

---

## 17. Testing Resilience

Reliability you haven't tested is a hypothesis.

- **Fail-fast in development:** run with `spark.task.maxFailures=1` so deterministic bugs surface immediately rather than after four retries.
- **Kill executors/nodes** during a representative run (`kill -9` an executor JVM, delete a K8s pod, drain a node) and verify: the job completes, results are identical, and recovery time is acceptable.
- **Inject faults in transformations:** throw randomly (seeded) in a test UDF to exercise task retries and confirm there are no side-effect duplicates.
- **Simulate shuffle loss:** remove shuffle files or kill an executor *after* a shuffle stage and confirm stage resubmission behaves.
- **Rerun idempotency tests:** execute the same job twice for the same window, and also run it again after killing it mid-write; assert output equality and no duplicates or partial files.
- **Restart drills for streaming:** stop and restart queries at awkward points; verify no loss/duplication (see the streaming guide's crash-point table).
- **Backfill/disaster drills:** rebuild a table from the source of truth using the same code path production uses.
- **Chaos at the dependencies:** throttle object-store requests, fail a database write, delay a shuffle fetch.

---

## 18. Failure-Scenario Matrix

| Scenario | Detected by | Spark's response | Cost | Make it cheaper |
|---|---|---|---|---|
| Task throws transient exception | Task failure | Retry (up to 4) on another executor | One task re-run | Exclusion on flaky nodes |
| Task throws deterministic exception | Same failure ×4 | Stage fails, job aborts | Wasted retries | Fix code/data; `maxFailures=1` in dev; quarantine bad data |
| Executor OOM / killed | Heartbeat loss / exit code | Replace executor; retry its tasks; lose its shuffle/cache | Tasks + possible stage recompute | Right-size memory/overhead; ESS; replication |
| Node lost (spot/hardware) | Cluster manager + heartbeats | As above, *plus* all node-local shuffle/cache gone | Possibly large recompute | Decommissioning; remote shuffle; replicate cache |
| Shuffle file corrupt/missing | `FetchFailed` | Recompute producing stage's missing partitions | One stage (or a cascade) | Retry waits; checksums; durable shuffle |
| Straggler task | Speculation heuristics | Duplicate task, first wins | Extra resources | Speculation + idempotent side effects |
| Driver dies (batch) | Cluster manager | Application ends / new attempt from scratch | Whole job | Idempotent restartable pipeline; staged outputs |
| Driver dies (streaming) | Supervisor | Restart; resume from checkpoint | Brief gap; replay of in-flight batch | Supervision; idempotent sink |
| Master/RM down | Cluster manager | Running apps continue; no new executors | New work blocked | Master/RM HA |
| Output partially written | Missing `_SUCCESS`; validation | None automatic | Bad/partial data visible | Cloud committers or table formats; write-then-publish |
| Bad records in input | Parse errors / `_corrupt_record` | Depends on `mode` | Silent loss or job failure | Quarantine + ratio gate |
| Source data deleted/changed mid-job | `FileNotFoundException`, inconsistent reads | Fail (or silently skip with `ignoreMissingFiles`) | Failed or wrong job | Snapshot-isolated table formats; immutable inputs |

---

## 19. Configuration Reference

| Config | Default | Purpose |
|---|---|---|
| `spark.task.maxFailures` | 4 | Task attempts before the job aborts |
| `spark.stage.maxConsecutiveAttempts` | 4 | Consecutive stage attempts (fetch-failure retries) before abort |
| `spark.excludeOnFailure.enabled` | false | Stop scheduling on repeatedly failing executors/nodes (3.1+; formerly `spark.blacklist.*`) |
| `spark.excludeOnFailure.timeout` | 1h | How long an excluded resource stays excluded |
| `spark.excludeOnFailure.killExcludedExecutors` | false | Kill executors that become excluded |
| `spark.speculation` | false | Enable speculative duplicate tasks |
| `spark.speculation.multiplier` / `.quantile` | 1.5 / 0.75 (≤3.x); **3 / 0.9 (4.0+)** | How slow and how far into the stage before speculating |
| `spark.speculation.minTaskRuntime` | 60s | Minimum runtime before a task can be speculated |
| `spark.executor.heartbeatInterval` | 10s | Executor heartbeat period |
| `spark.executor.heartbeat.maxFailures` | 60 | Failed heartbeats before an executor exits |
| `spark.executor.maxNumFailures` / `spark.executor.failuresValidityInterval` | 2× executors (min 3) / unset (3.5+) | Cap executor failures on YARN/K8s so systemic problems fail the app |
| `spark.shuffle.detectCorrupt` | true | Detect corrupted shuffle blocks at fetch time |
| `spark.files.fetchFailure.unRegisterOutputOnHostOnFetchFailure` | false | Un-register all shuffle outputs on a host after a fetch failure |
| `spark.storage.decommission.fallbackStorage.path` | unset | Shared storage for shuffle blocks that cannot be migrated to a peer |
| `spark.task.reaper.enabled` | false | Monitor killed tasks and escalate if they won't die |
| `spark.network.timeout` | 120s | Default network/RPC timeout |
| `spark.shuffle.io.maxRetries` / `retryWait` | 3 / 5s | Transient shuffle fetch retries |
| `spark.shuffle.checksum.enabled` | true (recent releases) | Detect corrupted shuffle data |
| `spark.shuffle.service.enabled` | false | External shuffle service (survives executor loss, not node loss) |
| `spark.dynamicAllocation.shuffleTracking.enabled` | true with dynamic allocation and no ESS (3.4+) | Keep executors that hold needed shuffle data |
| `spark.decommission.enabled` | false | Graceful executor decommissioning |
| `spark.storage.decommission.enabled` / `.shuffleBlocks.enabled` / `.rddBlocks.enabled` | false | Migrate cached/shuffle blocks off a leaving executor |
| `spark.storage.replication.proactive` | true (4.0+); false earlier | Re-replicate lost cached block replicas |
| `spark.cleaner.referenceTracking.cleanCheckpoints` | false | Auto-delete checkpoint files when references go away |
| `spark.checkpoint.compress` | false (true in 4.1) | Compress RDD checkpoints |
| `spark.sql.files.ignoreCorruptFiles` / `ignoreMissingFiles` | false / false | Skip unreadable/vanished files (silently) |
| `spark.sql.sources.partitionOverwriteMode` | static | `dynamic` replaces only partitions present in the new data |
| `spark.sql.execution.sortBeforeRepartition` | true | Keep round-robin repartition deterministic under recomputation |
| `spark.sql.ansi.enabled` | false (≤3.x); **true (4.0+)** | Throw on overflow/invalid cast instead of returning NULL |
| `spark.sql.broadcastTimeout` | 300s | Broadcast build timeout |
| `spark.driver.maxResultSize` | 1g | Cap on results collected to the driver |
| `spark.yarn.maxAppAttempts` | RM `am.max-attempts` (typically 2) | YARN application attempts (each reruns from scratch) |
| `spark.yarn.am.attemptFailuresValidityInterval` | unset | Window after which old AM failures stop counting |
| `spark.deploy.recoveryMode` | NONE | Standalone Master HA/recovery (`ZOOKEEPER`, `FILESYSTEM`) |
| `spark.eventLog.enabled` / `spark.eventLog.dir` | false / — | Persist events for the History Server |

---

## 20. Symptom-to-Cause Table

| Symptom | Likely cause | Check / fix |
|---|---|---|
| Stage shows extra attempts or "re-runs" after looking finished | `FetchFailed` recovery from lost shuffle data | Why executors/nodes vanish; ESS/shuffle tracking/decommissioning; retry waits |
| Job aborts: "Task N failed 4 times" | Deterministic exception, bad record, or systemic OOM | First `Caused by`; reproduce on that partition; handle bad data; fix memory |
| Executors repeatedly lost with exit 137 / `OOMKilled` | Native/overhead memory over the container limit | `memoryOverhead`, PySpark/RocksDB memory (memory guide) |
| Executors lost with no OOM; GC logs show multi-second pauses | Heartbeat timeout from GC | Heap/partition sizing, G1 tuning, timeout slack |
| Duplicate rows after a job retry | `append` mode or non-idempotent external writes | Idempotent writes, dynamic overwrite, MERGE, write-then-publish |
| Less data than before after a failed overwrite | Static overwrite deleted target before failed write | Dynamic partition overwrite, table format, staging path |
| Partial/stale files in output directory | Committer v2, aborted job, concurrent writers | Use v1/cloud committers/table format; check `_SUCCESS`; clean `_temporary` |
| Results differ between a run and its recomputation | Non-deterministic logic (rand/uuid/time/ordering) | Deterministic IDs; sorting; persist generated values |
| Accumulator totals inflated | Transformation-side accumulators re-applied on retries/speculation | Use aggregations/counters in actions or a control table |
| External DB shows duplicate inserts when speculation is on | Side effects in tasks run twice | Idempotent upserts or disable speculation |
| Job runs forever after a spot reclamation | Recompute cascade or exclusion/scale-down thrash | Decommissioning, stable capacity for critical parts, durable intermediates |
| App restarts (YARN attempt 2) and reprocesses everything | Driver failure + whole-app retry | Idempotent staged pipeline; fix root driver failure (OOM/maxResultSize) |
| Silent data shrinkage | `ignoreCorruptFiles`/`DROPMALFORMED`/`PERMISSIVE` nulls | Count and alert on skipped/quarantined data; ratio gates |
| Stage stuck at 99% with no errors and healthy executors | Hung task (dead external call, deadlock, infinite loop); no per-task timeout exists | Client-side timeouts, job-level timeout in the orchestrator, long-task alerts (§15.1) |
| Database has duplicate or partial rows after a failed/retried job | JDBC/NoSQL writes commit per task with no job-level transaction | Staging table + single `MERGE`/swap, idempotency keys (§10.4) |
| Long-running app fails after hours or days with auth errors | Expired Kerberos/cloud/database credentials | Credential renewal/keytab, test a run longer than the shortest token lifetime |
| Many jobs fail at planning time simultaneously | Catalog/metastore outage or slowness | Metastore HA and retries; alert on planning latency |
| Streaming query resumes with duplicates in sink | Non-idempotent sink at replay point | Idempotent/transactional sink (see streaming guide) |
| Everything works locally, fails on cluster (or vice versa) | Local mode doesn't retry; different committers/storage semantics | Test with cluster-like settings (`maxFailures`, object-store committers) |

---

## 21. Production Reliability Checklist

**Resilience of the engine**
- [ ] Event logs, History Server, and aggregated executor/driver logs enabled and retained.
- [ ] External shuffle service or shuffle tracking in place when using dynamic allocation.
- [ ] Decommissioning configured where capacity is preemptible; driver on stable capacity.
- [ ] Timeouts/heartbeats consistent (`heartbeatInterval` ≪ `network.timeout`); shuffle retry waits sized to ride out brief stalls.
- [ ] Memory and overhead sized to avoid routine OOM/container kills (memory guide).
- [ ] Driver supervised or restarted by the orchestrator, with attempt limits and validity intervals.

**Resilience of the data path**
- [ ] Output via a table format or a cloud-safe committer; no v2 file committer where correctness matters.
- [ ] Write-then-validate-then-publish for important outputs; consumers check `_SUCCESS`/snapshots.
- [ ] Dynamic partition overwrite or MERGE instead of `append`/static overwrite for rerunnable jobs.
- [ ] Deterministic transformations; no run-time `uuid()`/`now()` feeding keys or partitioning.
- [ ] External side effects idempotent and last; speculation disabled where they cannot be made idempotent.
- [ ] Bad-data policy chosen deliberately: quarantine + ratio gate, ANSI mode considered, silent-skip options understood.

**Operational discipline**
- [ ] Jobs are window-scoped and restartable; run-control table tracks status.
- [ ] Intermediate stage outputs materialized at natural boundaries for long pipelines.
- [ ] Orchestrator retries with backoff and caps, only for idempotent jobs; alerts on exhaustion.
- [ ] Failure drills run (kill executors, restart drivers, rerun windows) and results verified identical.
- [ ] Dashboards/alerts on executor loss rate, task failure rate, FetchFailed counts, stage retry counts, and data-quality metrics.
- [ ] External calls inside tasks have client-side timeouts, and the orchestrator enforces a job-level timeout.
- [ ] Long-running applications have credential/token renewal configured and tested beyond the shortest token lifetime.
- [ ] A rollback path exists for important outputs (table-format snapshots/`RESTORE`, or the previous published version retained).
- [ ] Metastore/catalog and other tier-0 dependencies have HA, retries, and alerting.

---

## 22. Common Misconceptions

| Misconception | Reality |
|---|---|
| "Spark is fault tolerant, so my pipeline is safe to retry." | Spark recovers **computation**. Re-running a job that appends or writes to external systems can duplicate data unless you design idempotent output. |
| "If the driver dies, Spark recovers the job." | For **batch**, the application restarts from scratch (or fails); only streaming resumes from a checkpoint. |
| "The external shuffle service protects against node failure." | It survives **executor** loss; shuffle files on a **lost node** disappear with it. |
| "A stage that re-runs is hung." | It is usually recovering lost shuffle output (`FetchFailed`). Fix the cause of the loss. |
| "More task retries make the job more reliable." | Retries only help transient faults; deterministic bugs fail identically each time. |
| "Caching protects my data." | Cache is best-effort and executor-local. Durable intermediates are tables/files or reliable checkpoints. |
| "`localCheckpoint()` is a cheap `checkpoint()`." | It truncates lineage but is **not fault-tolerant** — losing the executor loses the data. |
| "Speculation fixes slow jobs." | It helps environmental stragglers only; it can't fix skew and can duplicate external side effects. |
| "Accumulators give exact counts." | Only inside actions; in transformations they can over-count on retries/recomputation. |
| "Overwrite is safe to retry." | Static overwrite may delete the target before a failed write; use dynamic partition overwrite or a transactional table. |
| "`ignoreCorruptFiles=true` makes pipelines robust." | It makes failures **silent**. Pair it with counting/alerting or you will lose data unnoticed. |
| "Local mode behaves like a cluster." | Local mode doesn't retry tasks by default and doesn't exercise shuffle service, committers, or node-loss behavior. |

---

## 23. A Concise Mental Model

- **Spark guarantees recomputation, not side effects.** Deterministic transformations + immutable data + lineage ⇒ lost partitions can always be rebuilt; anything outside Spark's control must be made idempotent by you.
- **Failure scope determines cost:** a task failure costs one task; an executor loss costs its tasks plus its local data; a node loss costs *all* node-local data (even with ESS); a driver loss costs the whole batch job.
- **Shuffle files are both the recovery boundary and the weak point.** They stop recomputation from going back to the source — until they are lost, which is when stage recomputation cascades.
- **Retries fix transient problems; they cannot fix deterministic ones.** Fail fast in development, quarantine bad data in production, and make "bad data ratio" a gate.
- **Commits make retries safe.** Task/job commit protocols (and, better, table-format transactions) are what turn "retry" into "exactly one result"; on object stores, use cloud-aware committers or table formats.
- **Batch reliability is pipeline design:** window-scoped, idempotent, staged with durable intermediates, validated before publish, retried by an orchestrator with caps.
- **Streaming reliability is the same idea made explicit:** offsets logged before work, state versioned, commits after, replay safe because sinks are idempotent.
- **Operate for diagnosis:** event logs, aggregated logs, listener metrics, and the habit of classifying every failure (user code, executor loss, fetch failure, driver, commit, data) before reaching for a config.
