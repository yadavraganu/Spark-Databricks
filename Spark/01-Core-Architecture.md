## Table of Contents

1. The Big Picture
2. Design Principles Behind the Architecture
3. The Components: Driver, Executors, Cluster Manager
4. Inside the Driver
5. Inside an Executor
6. SparkContext and SparkSession
7. Data Abstractions: RDD, DataFrame, Dataset
8. Lazy Evaluation, Transformations, and Actions
9. Dependencies: Narrow vs. Wide
10. The Execution Hierarchy: Application → Job → Stage → Task
11. DAG Scheduler vs. Task Scheduler
12. The Life of a Job, End to End
13. Task Scheduling Details: Locality, Pools, Speculation
14. Shuffle Architecture (Overview)
15. Deployment Architectures: Local, Standalone, YARN, Kubernetes
16. Deploy Modes: Client vs. Cluster
17. PySpark Architecture
18. Spark Connect Architecture
19. Concurrency Within an Application
20. Shared Variables: Broadcast and Accumulators
21. A Worked Example: Tracing One Program
22. Limits and Invariants Worth Knowing
23. Configuration Reference
24. Symptom-to-Architecture Table
25. Common Misconceptions
26. Core Architecture Checklist
27. A Concise Mental Model

## 1. The Big Picture

Spark is a **distributed data-parallel execution engine**. You describe a computation as a graph of transformations over immutable, partitioned datasets; Spark breaks that graph into stages and tasks, ships tasks to executor processes running across a cluster, moves data between them when necessary (shuffles), and recovers from failures by recomputing lost pieces.

```
                         ┌──────────────────────────────────────────────┐
  your code ──────────►  │                 DRIVER (JVM)                 │
 (PySpark/Scala/SQL)     │  SparkSession/SparkContext                   │
                         │  DAGScheduler → TaskScheduler → Backend      │
                         │  BlockManagerMaster, MapOutputTracker, UI ...│
                         └───────────┬──────────────────────────────────┘
                                     │ asks for resources / sends tasks (RPC)
                      ┌──────────────▼──────────────┐
                      │       CLUSTER MANAGER       │   Standalone | YARN | Kubernetes
                      └──────────────┬──────────────┘   (launches executor processes)
          ┌──────────────────────────┼──────────────────────────┐
   ┌──────▼───────┐           ┌──────▼───────┐           ┌──────▼───────┐
   │ EXECUTOR JVM │◄─shuffle─►│ EXECUTOR JVM │◄─shuffle─►│ EXECUTOR JVM │
   │ tasks, cache │           │ tasks, cache │           │ tasks, cache │
   │ BlockManager │           │ BlockManager │           │ BlockManager │
   └──────────────┘           └──────────────┘           └──────────────┘
```

Three roles, one rule of thumb: **the driver plans and coordinates, executors compute and hold data, the cluster manager provides and tracks resources.** Everything else in this guide elaborates how they interact.

**Spark vs. Classic MapReduce**

| Aspect | MapReduce | Spark |
|---|---|---|
| Execution model | Fixed map → shuffle → reduce, each job writes to storage | Arbitrary DAG of stages; pipelines operators in memory within a stage |
| Intermediate data | Written to distributed storage between jobs | Kept in memory/local disk; shuffle files only at stage boundaries |
| Process lifetime | New JVM per task (historically) | Long-lived executors reused across tasks and jobs |
| Iteration / reuse | Re-read from storage each pass | Cache datasets in memory across actions |
| Programming model | Low-level map/reduce functions | Rich APIs (SQL, DataFrame, RDD, streaming, ML) over one engine |

## 2. Design Principles Behind the Architecture

1. **Immutable, partitioned datasets.** Data is split into partitions that are processed independently; datasets are never modified in place, only transformed into new ones. This makes parallelism simple and recovery possible.
2. **Lazy evaluation.** Transformations only *describe* work; nothing executes until an action. This lets Spark see the whole computation, optimize it, and pipeline operators.
3. **Lineage-based fault tolerance.** Each dataset records how it was derived. A lost partition is rebuilt by re-running its recipe, rather than by replicating all data.
4. **Coarse-grained operations.** Transformations apply the same function to whole datasets or partitions, so the engine tracks lineage per partition, not per record.
5. **Pipelining within stages, materialization at shuffles.** Narrow operations run back-to-back in one pass over each partition; only wide operations force data to be written and redistributed.
6. **Move computation to data.** The scheduler prefers to run tasks where their input already lives (locality).
7. **Coarse-grained resource model.** Executors are long-lived processes that live for the whole application and run many tasks, instead of launching a process per task.
8. **One engine, many front-ends.** SQL, DataFrames, Datasets, RDDs, streaming, and ML libraries all compile down to the same scheduler and task model.

## 3. The Components: Driver, Executors, Cluster Manager

### 3.1 The driver
The process that runs your application's `main()` (or notebook/session) and owns the application's control plane:
- Creates the `SparkSession`/`SparkContext`.
- Turns your code into a logical/physical plan, then a DAG of **stages**, then **tasks**.
- Negotiates with the cluster manager for executors.
- Schedules tasks onto executors, tracks their status, handles retries.
- Holds metadata: where each shuffle output lives, where each cached block lives, accumulators, broadcast sources, the Web UI state.
- Collects results of actions like `collect()`.

**The driver is a single point of failure for the application.** If it dies, the application dies (batch) or must be restarted (streaming resumes from its checkpoint).

### 3.2 Executors
JVM processes (one per executor, launched on worker nodes) that:
- Run **tasks** assigned by the driver, in a thread pool sized by the executor's core count.
- Store **data**: cached/persisted partitions and shuffle outputs, via the local `BlockManager`.
- Serve data to other executors during shuffles and block fetches.
- Send **heartbeats** and task metrics to the driver.
- Are **long-lived**: allocated for the application (or scaled dynamically), reused across jobs.

For PySpark, an executor additionally spawns **Python worker processes** for Python UDFs/`mapPartitions`-style code (§17).

### 3.3 Cluster manager
A service that owns machines and hands out resources. Spark talks to it through a pluggable scheduler backend:

| Cluster manager | What it allocates | Launches executors as |
|---|---|---|
| **Local** (no real cluster) | Threads in one JVM | In-process executor |
| **Standalone** | Cores/memory on Spark worker daemons | Processes started by workers |
| **YARN** | Containers | Containers launched by NodeManagers |
| **Kubernetes** | Pods | Executor pods created by the driver via the K8s API |

(Apache Mesos support has been removed from recent Spark releases.)

### 3.4 Responsibilities at a glance

| Concern | Driver | Executor | Cluster manager |
|---|---|---|---|
| Plan the computation (DAG, stages) | ✔ | | |
| Schedule tasks | ✔ | | |
| Run tasks, process data | | ✔ | |
| Hold cached data and shuffle files | metadata only | ✔ (data) | |
| Allocate/launch executors | requests | | ✔ |
| Enforce container/pod limits | | | ✔ |
| Detect executor death | via heartbeats/RPC | | via node/container monitoring |
| Serve the Web UI | ✔ | | (proxy only) |

---

## 4. Inside the Driver

Everything the driver needs is wired together in the `SparkContext` and its `SparkEnv`. The main internals:

| Component | Role |
|---|---|
| **DAGScheduler** | High-level, stage-oriented scheduler: builds the stage DAG from RDD lineage, submits stages in dependency order, handles stage-level failures (e.g., lost shuffle output), reports job completion. Runs on a single event-loop thread. |
| **TaskScheduler** (`TaskSchedulerImpl`) | Low-level, task-oriented scheduler: takes a **TaskSet** per stage attempt, matches tasks to free executor slots (locality-aware), retries failed tasks, drives speculation and exclusion. |
| **SchedulerBackend** | Adapter to the cluster manager (`CoarseGrainedSchedulerBackend` and its Standalone/YARN/Kubernetes/Local variants): tracks registered executors, sends `LaunchTask` messages, receives status updates. |
| **TaskSetManager** | Per-stage-attempt bookkeeping inside the TaskScheduler: pending tasks, locality levels, failure counts, speculation candidates. |
| **BlockManagerMaster** | Driver-side registry of which executor holds which cached/broadcast blocks. |
| **MapOutputTrackerMaster** | Registry of where each shuffle map output lives; reducers query it to find their input. |
| **ExecutorAllocationManager** | Implements dynamic allocation (scale up/down requests). |
| **OutputCommitCoordinator** | Ensures only one task attempt commits output per partition. |
| **LiveListenerBus** | Asynchronous event bus feeding the UI, event log, metrics, and custom listeners. |
| **HeartbeatReceiver** | Tracks executor heartbeats; declares executors lost on timeout. |
| **ContextCleaner** | Garbage-collects unreferenced shuffles, RDD blocks, broadcasts, accumulators. |
| **SparkUI / REST** | Web UI and API. |
| **RpcEnv** | Netty-based RPC system used for all driver↔executor messages. |

The driver is deliberately **thin on data, heavy on metadata**: it tracks partitions, locations, and task state — not the data itself (except for results you explicitly collect and data you broadcast).

---

## 5. Inside an Executor

A launched executor process (`CoarseGrainedExecutorBackend`) registers with the driver and then runs an `Executor` object containing:

| Component | Role |
|---|---|
| **Task thread pool** | Each running task occupies one thread (threads named like "Executor task launch worker"). The *scheduler* limits concurrency to `cores ÷ spark.task.cpus` tasks per executor; the pool just runs what it is given. |
| **TaskRunner** | Deserializes a task (its function and RDD/plan) and its dependencies (jars/files), runs it, serializes and returns the result. |
| **BlockManager** | Stores blocks: cached RDD/DataFrame partitions, broadcast pieces, shuffle blocks. Memory store + disk store; fetches remote blocks through a block transfer service. |
| **Shuffle machinery** | Writes map outputs to local disk files; serves them to reducers (directly or through the external shuffle service). |
| **MemoryManager** | The unified memory manager splitting execution vs. storage memory (see the memory guide). |
| **Heartbeater** | Periodically reports liveness and metrics to the driver (`spark.executor.heartbeatInterval`). |
| **MapOutputTracker (worker side)** | Caches shuffle-output locations fetched from the driver. |
| **Plugin hooks** | Executor plugins for custom metrics/init. |

An executor knows nothing about your whole program; it only runs the tasks it is handed and holds the blocks assigned to it.

---

## 6. SparkContext and SparkSession

### 6.1 SparkContext
The original entry point and the **connection to the cluster**: it owns the scheduler stack above, the broadcast/accumulator machinery, and the RDD API (`parallelize`, `textFile`, `broadcast`, `accumulator`). **There is exactly one active `SparkContext` per JVM** (multiple contexts were disallowed outright in Spark 3.0). It exposes `statusTracker()`, `setLogLevel`, `setJobGroup`/`setJobDescription`, `setCheckpointDir`, and so on.

### 6.2 SparkSession
The **unified entry point since Spark 2.0** for DataFrames, Datasets, SQL, streaming, and catalog access. It *wraps* a `SparkContext` and adds two layers of state:

| State | Scope | Contains |
|---|---|---|
| **SharedState** | Shared by all sessions in the application | The `SparkContext`, external catalog (Hive metastore or in-memory), cache manager, global temp views, SQL listeners |
| **SessionState** | Per session | SQL configuration, temporary views, UDF/function registry, the analyzer and optimizer instances, session catalog |

Practical consequences:
- `SparkSession.builder.getOrCreate()` returns the existing session if one exists; configuration passed to the builder after the context started may be **ignored for static/core settings** (executor memory, master, etc.) while session-level SQL configs still apply.
- `spark.newSession()` gives you an isolated **session** (own SQL conf and temp views) that shares the same `SparkContext`, executors, and cache. Multi-tenant servers (e.g., Thrift server, notebooks) rely on this.
- `spark.conf.set(...)` changes **session** SQL configuration at runtime; it cannot change resource settings of already-launched executors.
- `spark.sparkContext` gives access to the context; `spark.stop()` stops the context for everyone.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("core-architecture-demo")
         .config("spark.sql.shuffle.partitions", "64")     # session SQL conf — applies
         .getOrCreate())

sc = spark.sparkContext
print(sc.master, sc.applicationId, sc.defaultParallelism)
```

---

## 7. Data Abstractions: RDD, DataFrame, Dataset

### 7.1 RDD — the foundation
A **Resilient Distributed Dataset** is an immutable, partitioned collection defined by five properties:

1. A **list of partitions**
2. A **compute function** for each partition
3. A list of **dependencies** on parent RDDs (the lineage)
4. An optional **partitioner** (for key-value RDDs: how keys map to partitions)
5. Optional **preferred locations** per partition (for locality)

Everything else — fault tolerance, scheduling, locality, stage splitting — falls out of these five properties.

### 7.2 DataFrame and Dataset
- A **DataFrame** is a distributed table with a schema, represented as a **logical plan** (a tree of relational operators). It is *not* executed directly: Catalyst optimizes it, produces a physical plan, and that plan is ultimately executed as **RDDs of internal binary rows**. So the RDD/stage/task machinery in this guide is exactly what runs your DataFrame code.
- A **Dataset[T]** (Scala/Java only) adds compile-time typing via encoders. In PySpark, only DataFrames exist.
- SQL strings, DataFrame calls, and Dataset operations all converge on the same plan representation (see the SQL & Catalyst guide).

### 7.3 When each matters
| API | Use | Notes |
|---|---|---|
| DataFrame/SQL | Almost everything | Optimizer, codegen, columnar formats, pushdown |
| RDD | Custom partitioning/low-level control, unstructured data, legacy code | No Catalyst optimization; Python RDD code runs row-by-row in Python workers |

---

## 8. Lazy Evaluation, Transformations, and Actions

- **Transformations** (`map`, `filter`, `select`, `join`, `groupBy`, …) return a new dataset and only extend the lineage/plan. No data moves; no job runs.
- **Actions** (`count`, `collect`, `take`, `show`, `write`, `foreach`, `saveAsTextFile`, …) force evaluation: each action submits **one or more jobs**.

```python
rdd = sc.textFile("logs/")                # transformation: nothing read yet
errs = rdd.filter(lambda l: "ERROR" in l) # still lazy
n = errs.count()                          # ACTION → a job runs now
```

Why laziness matters architecturally:
- The whole computation is visible before running, so Spark can **pipeline operators** and (for SQL) **optimize across steps** (pushdown, pruning, join selection).
- **Recomputation is the default.** Every action re-runs the lineage from the sources unless data is cached/persisted or shuffle files exist. Two actions on the same uncached DataFrame read and compute twice.
- **Errors surface late:** a bad transformation fails at the action, not where you wrote it. Use job descriptions to map UI jobs back to code.

**One action can create several jobs.** Examples: `orderBy` needs range boundaries and may run a **sampling job** first; `show()`/`take()`/`limit` scan partitions **incrementally** (starting with one partition and scaling up if more rows are needed); a broadcast join runs a job to build the broadcast side; with AQE, each shuffle "query stage" is materialized as its own job before the next stage is planned.

---

## 9. Dependencies: Narrow vs. Wide

Dependencies between RDDs (and between stages of a plan) determine **how data flows and where stages split**.

### 9.1 Narrow dependencies
Each child partition depends on a **small, fixed set of parent partitions** (usually one). No data needs to move between executors; the child partition can be computed from local parent data, **pipelined in the same task**.

- Examples: `map`, `filter`, `flatMap`, `mapPartitions`, `select`/`withColumn`, `union`, `coalesce` (without shuffle), `sample`, a join of two datasets that are already **co-partitioned** with the same partitioner.
- Recovery: a lost child partition needs only its few parent partitions recomputed.

### 9.2 Wide (shuffle) dependencies
A child partition may depend on **many or all parent partitions**; records must be **redistributed by key** across the cluster.

- Examples: `groupBy`/`groupByKey`/`reduceByKey`/`aggregateByKey`, non-broadcast `join`/`cogroup`, `distinct`, `repartition`, `sortBy`/`orderBy`, `repartitionByRange`, window functions with partitioning, `intersect`/`except`.
- Cost: serialize + write to local disk (map side), fetch over the network (reduce side), deserialize; plus a **barrier** — the next stage cannot start until all map tasks finish.
- Recovery: losing shuffle output can require recomputing many parent partitions.

### 9.3 Impact on execution

| | Narrow | Wide |
|---|---|---|
| Stage boundary | No (same stage) | **Yes** (new stage) |
| Data movement | None | Network + disk |
| Pipelining | Fused into one task per partition | Map side and reduce side are separate tasks in separate stages |
| Parallelism determined by | Parent partition count | Shuffle partition count / partitioner |
| Fault-recovery cost | Low | Higher (needs map outputs) |
| Tuning levers | Partition size, UDF cost | Shuffle volume, partition count, skew, join strategy |

**A key optimization idea:** reduce *what* crosses a wide dependency. `reduceByKey`/`aggregateByKey` pre-combine values on the map side ("map-side combine") before the shuffle; `groupByKey` ships every record. In SQL, partial aggregation, filter/projection pushdown, and broadcast joins all exist to shrink or eliminate shuffles.

---

## 10. The Execution Hierarchy: Application → Job → Stage → Task

```
APPLICATION  (one SparkContext/SparkSession; one driver; set of executors)
  └── JOB        (triggered by one action)
        └── STAGE   (a set of tasks that can run the same code in parallel; split at shuffles)
              └── TASK   (work on ONE partition, run by ONE executor thread)
```

### 10.1 Definitions
- **Application:** everything that runs under one `SparkContext`. Has an ID (`application_...`/`app-...`), a driver, and executors.
- **Job:** the work triggered by **one action**. Has a final stage and the lineage of stages needed to produce it.
- **Stage:** a group of tasks that perform the **same computation** on different partitions and require **no shuffle in between**. Stage boundaries occur at shuffle dependencies. Two types:
  - **ShuffleMapStage**: its tasks (`ShuffleMapTask`) write shuffle output for a downstream stage.
  - **ResultStage**: the last stage; its tasks (`ResultTask`) compute the action's result (e.g., count a partition and return it to the driver, or write a partition's files).
- **Task:** the smallest unit of work: one partition, one executor thread. A stage with *N* output partitions has *N* tasks (a ShuffleMapStage: one per partition of its map-side RDD; a ResultStage: one per partition being computed).

### 10.2 Relationships and counts
- Stages in a job ≈ **(number of shuffle dependencies on the path) + 1**, though stages feeding multiple consumers, and **skipped** stages (shuffle files from an earlier job still exist), change what you see in the UI.
- **Tasks per stage = partitions of the stage's final RDD.** Parallelism per stage is therefore a *partitioning* decision, not a stage decision.
- **Concurrent tasks = executors × cores per executor** (divided by `spark.task.cpus`). If a stage has more tasks than slots, they run in **waves**.
- Stage attempts: a stage can be re-submitted (a new *attempt*) after failures like lost shuffle data; tasks inside have their own attempts.

### 10.3 Why stages exist
A stage is the largest unit that can run **without coordination**: all its tasks are independent, can be pipelined end to end within each partition, and can be scheduled in any order. The shuffle between stages is the one point where tasks need each other's output, so the scheduler runs parent stages first and releases child stages when all map outputs are registered.

---

## 11. DAG Scheduler vs. Task Scheduler

Spark's scheduling is deliberately split into two layers with different responsibilities.

| | **DAGScheduler** | **TaskScheduler** (`TaskSchedulerImpl`) |
|---|---|---|
| Level | Stage / job | Task |
| Input | Jobs (an RDD + function + partitions to compute) | **TaskSets** (one per stage attempt) |
| Core work | Builds the stage DAG from lineage; finds missing parent stages; decides what to run in what order | Matches pending tasks to free executor slots; applies locality; retries failed tasks |
| Knows about | RDD dependencies, shuffle outputs (via MapOutputTracker), cached partitions, preferred locations | Executors, free cores, locality levels, task attempts/failures, scheduling pools |
| Failure handling | **Stage-level**: `FetchFailed` → mark lost map outputs, resubmit the map stage, then the reduce stage; stage attempt limits (`spark.stage.maxConsecutiveAttempts`) | **Task-level**: retry up to `spark.task.maxFailures`; exclude bad executors/nodes; speculative duplicates |
| Threading | One event-loop thread (events: `JobSubmitted`, `CompletionEvent`, `ExecutorLost`, …) | Called from RPC threads and a revive timer |
| Cluster awareness | None (cluster-agnostic) | Via `SchedulerBackend` |
| Output | `TaskSet`s handed to the TaskScheduler; job success/failure to the caller | Task launches to executors; task status events back to DAGScheduler |

**Interaction in one line:** the DAGScheduler decides **what** must run and **in which order**; the TaskScheduler decides **where and when** each task runs and **retries** it; the SchedulerBackend sends it to the cluster.

```
Action ─► DAGScheduler ──TaskSet──► TaskScheduler ──offers/launch──► SchedulerBackend ──RPC──► Executors
              ▲                          │  ▲                                                      │
              │   task completion /      │  └───────────── resource offers (free slots) ───────────┘
              └─── failure events ───────┘ ◄──────────────────── task status updates ──────────────┘
```

---

## 12. The Life of a Job, End to End

### 12.1 From action to results

1. **Action invoked** (`df.write...`, `rdd.count()`). `SparkContext.runJob` forwards to the DAGScheduler, posting a `JobSubmitted` event.
2. **Stage graph creation.** The DAGScheduler creates the final **ResultStage** and walks the RDD lineage backward: wherever it meets a **ShuffleDependency**, it creates (or reuses) a **ShuffleMapStage** for the parent. Narrow chains stay inside one stage.
3. **Submit in dependency order.** It submits the final stage, which first submits any **missing parent stages** (stages whose shuffle output isn't already registered) — recursively, parents first.
4. **Create tasks.** For a stage that's ready, it determines which partitions need computing, computes **preferred locations** for each (cached block locations, input split hosts, shuffle output locations), and creates one task per partition. The stage's RDD and function are **serialized once** (shipped as a broadcast) and referenced by every task.
5. **Hand to the TaskScheduler** as a `TaskSet`. The TaskScheduler wraps it in a `TaskSetManager`, placed in a scheduling pool (FIFO or FAIR).
6. **Resource offers.** The SchedulerBackend offers the TaskScheduler the list of free slots (one per free core) across registered executors; the TaskScheduler assigns tasks to offers, honoring **locality** (waiting up to `spark.locality.wait` for a better placement before relaxing).
7. **Launch.** The backend serializes each `TaskDescription` and sends a `LaunchTask` RPC to the chosen executor.
8. **Execute.** The executor's `TaskRunner` deserializes the task and runs it over its partition: for a ShuffleMapTask it computes the partition and **writes shuffle files** partitioned by the downstream partitioner; for a ResultTask it computes the partition and produces a result.
9. **Report.** The executor sends a status update:
   - **Small results** (≤ `spark.task.maxDirectResultSize`, 1 MiB by default) go directly in the message.
   - **Larger results** are stored in the executor's BlockManager and the driver fetches them (an *indirect* result).
   - Total collected result size is capped by `spark.driver.maxResultSize`.
10. **Bookkeeping.** The TaskSetManager marks the task done; the DAGScheduler is told (`CompletionEvent`). For a ShuffleMapTask, the **output location is registered with the MapOutputTracker**.
11. **Advance.** When every task of a ShuffleMapStage completes, its outputs are complete; child stages whose parents are all done are submitted. Reduce-side tasks then query the MapOutputTracker to find and fetch their input blocks.
12. **Finish.** When all partitions of the ResultStage are done, the job completes and control returns to the caller (or the exception is thrown).

### 12.2 Failure paths inside this flow
- **Task throws** → TaskScheduler retries (another executor if possible) up to `maxFailures`; then the stage and job fail.
- **Executor lost** → its running tasks are rescheduled; its registered shuffle outputs and cached blocks are removed from tracking.
- **FetchFailed** (a reducer can't fetch map output) → DAGScheduler marks the output lost, **resubmits the map stage** for the missing partitions, then retries the reduce stage.
- **Driver lost** → the application ends.

### 12.3 Application lifecycle

```
spark-submit / session start
   └─► driver process starts → creates SparkContext
          └─► registers with cluster manager, requests executors
                 └─► cluster manager launches executors → they register with the driver
                        └─► [jobs run: actions → stages → tasks → results] (repeat)
                               └─► spark.stop() / main() returns
                                      └─► executors released, UI/event log finalized
```

Dynamic allocation can add and remove executors during the middle part; fixed allocation holds them throughout.

---

## 13. Task Scheduling Details: Locality, Pools, Speculation

### 13.1 Data locality levels
Preference order (best → worst):

| Level | Meaning |
|---|---|
| `PROCESS_LOCAL` | Data is in the same executor JVM (e.g., cached block) |
| `NODE_LOCAL` | Data is on the same node (local disk/HDFS block, another executor on the node) |
| `NO_PREF` | No preference (data equally accessible, e.g., object storage) |
| `RACK_LOCAL` | Same rack |
| `ANY` | Anywhere |

**Delay scheduling:** if no slot is available at the best level, the scheduler waits (`spark.locality.wait`, default 3 s, with per-level overrides) before degrading to the next level — trading a little latency for avoiding network transfers. With disaggregated storage (object stores), data locality is mostly irrelevant for scans, and waiting just adds delay.

### 13.2 Scheduling pools
Within one application, jobs from concurrent threads compete for slots. `spark.scheduler.mode=FIFO` (default) favors earlier jobs; `FAIR` shares slots across jobs/pools with weights and minimum shares. Cross-application sharing is the cluster manager's job (queues, quotas).

### 13.3 Task retries, exclusion, speculation
- **Retries:** up to `spark.task.maxFailures` (default 4) attempts per task.
- **Exclusion:** optionally stop scheduling onto repeatedly failing executors/nodes (`spark.excludeOnFailure.*`).
- **Speculation:** optionally launch duplicate copies of slow tasks; first to finish wins; commit coordination ensures a single output (see the fault-tolerance guide).

### 13.4 Barrier execution mode
For workloads needing **gang scheduling** (e.g., distributed ML training where all workers must start together and communicate), `rdd.barrier()` marks a stage as a *barrier stage*: all its tasks are launched simultaneously or not at all, tasks can synchronize via `BarrierTaskContext`, and a failure restarts the whole stage. It requires enough free slots for every task at once.

---

## 14. Shuffle Architecture (Overview)

Detailed treatment is in the partitioning & shuffling guide; the architectural picture:

- **Map side:** each ShuffleMapTask partitions its output by the downstream partitioner and writes a **data file plus an index file** on the executor's local disk (sort-based shuffle). Output sizes are reported to the driver's MapOutputTracker as a `MapStatus`.
- **Reduce side:** each reduce task asks the MapOutputTracker for the locations/sizes of its blocks, then **fetches** them from the executors (or the external shuffle service) that hold them over the block transfer service, merging as it goes.
- **Components:** `ShuffleManager` (the sort-based manager is the only implementation), `ShuffleWriter`/`ShuffleReader`, `MapOutputTracker`, `BlockManager`/block transfer service, optional **external shuffle service** serving files independent of executor lifetime.
- **Why it matters architecturally:** shuffle files on local disk are both the *barrier between stages* and the *implicit recovery points* that stop recomputation from going back to the source.

---

## 15. Deployment Architectures: Local, Standalone, YARN, Kubernetes

### 15.1 Local mode
`--master local[N]` (or `local[*]`): driver and one in-process executor in a single JVM with N task threads. No network, no separate executors, no real shuffle service — ideal for development and tests, but not representative of memory limits, serialization boundaries across JVMs, or failure behavior. (`local[N, F]` allows F task failures; plain `local[N]` does not retry.)

### 15.2 Standalone
Spark's own cluster manager.
- **Master** tracks workers and applications; **Workers** offer cores/memory and launch executors (and drivers in cluster mode).
- Application submission → Master allocates executors on workers (`spark.deploy.spreadOut`), workers launch executor processes, executors register with the driver.
- Simple and good for dedicated Spark clusters; minimal multi-tenancy (`spark.cores.max`).

### 15.3 YARN
```
spark-submit ─► YARN ResourceManager ─► allocates a container for the ApplicationMaster (AM)
     cluster mode: the DRIVER runs inside the AM container
     client mode : the driver runs on the submitting machine; the AM only requests executors
AM ─► asks the RM for executor containers ─► NodeManagers launch executor JVMs ─► executors register with the driver
```
- Resources are containers (memory, vcores); queues, capacity/fair scheduling, and preemption come from YARN.
- The external shuffle service runs as a NodeManager auxiliary service.

### 15.4 Kubernetes
```
spark-submit ─► Kubernetes API server ─► creates the DRIVER pod
driver pod ─► asks the API server for EXECUTOR pods (ExecutorPodsAllocator) ─► executors start and connect back to the driver
driver pod owns the executor pods (cleanup when the driver ends)
```
- Resources are pod requests/limits; quotas, priority, node selectors, and optional batch schedulers govern placement.
- No external shuffle service by default; shuffle durability uses tracking, decommissioning, PVC reuse, or remote shuffle services.

### 15.5 Comparison

| | Local | Standalone | YARN | Kubernetes |
|---|---|---|---|---|
| Resource unit | Threads | Worker cores/memory | Containers | Pods |
| Multi-tenancy | None | Basic | Strong (queues) | Strong (quotas/namespaces) |
| Driver placement | Same JVM | Client or a worker | Client or AM container | Pod (cluster) or client |
| Best for | Dev/test | Dedicated Spark clusters | Hadoop ecosystems | Cloud-native platforms |

---

## 16. Deploy Modes: Client vs. Cluster

| | **Client mode** | **Cluster mode** |
|---|---|---|
| Driver location | The machine running `spark-submit` / the notebook kernel | Inside the cluster (AM container / driver pod / worker) |
| Network | Executors must reach the driver (ports, DNS, firewalls) | Driver is close to executors |
| Lifetime | Application dies if the submitting session dies | Independent of the submitting machine |
| Interactivity | Natural for shells and notebooks | Not interactive |
| Typical use | Development, interactive analysis | Production batch/streaming |
| Cluster capacity | Driver doesn't consume cluster resources | The driver consumes a container/pod you must budget for |

---

## 17. PySpark Architecture

PySpark is a Python API over the same JVM engine, so the architecture above is unchanged; what is added is a **bridge** and **Python worker processes**.

```
Python driver process ──Py4J──► JVM driver (SparkContext, DAGScheduler, Catalyst ...)
                                      │ tasks
                                      ▼
                         Executor JVM ──socket──► Python worker processes (pyspark.daemon forks workers)
```

- **Driver side:** your Python script talks to the JVM through a **Py4J** gateway. DataFrame/SQL operations build a **JVM-side plan**; Python is just a remote control. Calling a DataFrame method costs a small bridge call, not data movement.
- **Executor side:** pure DataFrame/SQL work runs entirely in the JVM (Catalyst/Tungsten). Only **Python code** — Python UDFs, `RDD.map` with Python lambdas, `mapInPandas`/`applyInPandas`, `foreachPartition` — executes in **Python worker processes**, which the executor JVM launches (a daemon forks workers; they can be reused across tasks, `spark.python.worker.reuse`).
- **Data crossing the boundary:** rows are serialized from JVM → Python and back, via pickle (row-at-a-time UDFs) or **Apache Arrow** (columnar batches for Pandas UDFs and `toPandas`/`createDataFrame` conversions). This serialization is the main PySpark overhead.
- **Memory:** Python workers live **outside the JVM heap** and count against the container's non-heap/overhead budget (or `spark.executor.pyspark.memory`); JVM metrics don't see them.
- **Consequences:** prefer built-in functions (stay in the JVM); use Pandas/Arrow UDFs when Python is unavoidable; avoid capturing `spark`/`sc` in UDFs; keep driver and executor Python versions compatible.

---

## 18. Spark Connect Architecture

**Spark Connect** (3.4+) splits the architecture into a **thin client** and a **server**:

```
Client (Python/Scala/Go/…)                         Spark Connect server (a Spark driver + plugin)
  DataFrame API builds an unresolved logical plan ──gRPC + protobuf──►  analyze, optimize, execute
  receives results as Arrow batches  ◄──────────────────────────────  on the usual Spark cluster
```

- The client does **not** embed a JVM driver; it sends plans over gRPC to a long-running Connect server, which is an ordinary Spark application (driver + executors).
- **Benefits:** lightweight clients (no heavy Spark/JVM dependency in apps, notebooks, IDEs, services), stability and upgrade independence (client and server decoupled), multi-language clients, better isolation and security boundary, remote connectivity.
- **Trade-offs:** client code can't touch the JVM internals directly (`sparkContext`, RDD APIs, `SparkContext` configs are limited or unavailable); data transfer for results flows through the server; the server becomes a component to size and supervise. Recent releases (Spark 4.x) expand API parity and provide ways to select Connect vs. classic ("Spark Classic") modes; check your version's documentation.
- Everything *behind* the server — DAGScheduler, stages, tasks, executors, shuffle — is exactly the architecture in this guide.

---

## 19. Concurrency Within an Application

- **Multiple threads can submit jobs concurrently** to one `SparkContext`/`SparkSession`; each action blocks its calling thread, but jobs from different threads run simultaneously, sharing executors per the scheduler mode (FIFO/FAIR).
- **Job metadata per thread:** `setJobGroup`, `setJobDescription`, and local properties (e.g., `spark.scheduler.pool`) are **thread-local**, which is how servers/notebooks label and route concurrent work.
- **Session isolation:** `newSession()` isolates SQL conf and temp views while sharing resources and cache.
- **The driver is the shared bottleneck:** one DAG-scheduler event loop, one set of RPC threads, and driver memory for all concurrent jobs' metadata — a very busy multi-tenant application needs a correspondingly sized driver.
- **Shared mutable state in your own code** (closures capturing mutable objects) is not synchronized across tasks; Spark's shared-variable abstractions (§20) exist for that.

---

## 20. Shared Variables: Broadcast and Accumulators

Normally, a function shipped to executors gets **its own copy** of any variable it references (serialized with the task). Two exceptions exist for efficiency and aggregation:

**Broadcast variables** — read-only data shipped **once per executor** (not per task), stored in the BlockManager and fetched peer-to-peer (torrent-style) so the driver isn't the single source.
```python
lookup = sc.broadcast(dict_of_codes)               # driver-side object → shipped to executors once
rdd.map(lambda k: lookup.value.get(k))             # executors read the local copy
```
Use for lookup tables and models referenced from row/partition-level code. The same mechanism ships each stage's serialized RDD/plan to tasks and powers broadcast joins. Free with `unpersist()`/`destroy()` when done.

**Accumulators** — variables that tasks can only *add* to, and the driver reads. Spark guarantees exactly-once updates **only for updates performed inside actions**; inside transformations, task retries, recomputation, or speculation can apply updates more than once. Treat transformation-side accumulators as approximate diagnostics, not exact counts.

---

## 21. A Worked Example: Tracing One Program

```python
rdd    = sc.textFile("logs/", minPartitions=8)                        # 8 partitions (lazy)
words  = rdd.flatMap(lambda l: l.split())                              # narrow
pairs  = words.map(lambda w: (w, 1))                                   # narrow
counts = pairs.reduceByKey(lambda a, b: a + b, numPartitions=4)        # WIDE  → shuffle #1 (4 reduce partitions)
top    = counts.sortBy(lambda kv: -kv[1], numPartitions=2)             # WIDE  → shuffle #2 (range partitioned)
top.saveAsTextFile("out/")                                             # ACTION
```

**What the scheduler builds**

| Stage | Type | Operators fused in one task | Tasks | Reason it's separate |
|---|---|---|---|---|
| 0 | ShuffleMapStage | `textFile` → `flatMap` → `map` → map-side combine for `reduceByKey`, write shuffle files | 8 (one per input partition) | Ends at shuffle #1 |
| 1 | ShuffleMapStage | Read & merge shuffle #1, finish `reduceByKey`, extract sort key, write range-partitioned shuffle files | 4 | Ends at shuffle #2 |
| 2 | ResultStage | Read shuffle #2, sort within partition, write output files | 2 | Final stage of the job |

- **Narrow operations** (`flatMap`, `map`) were pipelined into Stage 0's tasks — no intermediate dataset was materialized between them.
- **`sortBy` builds a `RangePartitioner`, which samples the data in an extra job** to choose range boundaries. That earlier job computes the `reduceByKey` shuffle; when the main job runs, Stage 0's shuffle files already exist, so the UI shows it as **skipped**. This is why "one action" can show two jobs and a skipped stage.
- The three stages run **in order** (0 → 1 → 2) because each depends on the previous shuffle. Within a stage, all tasks run in parallel up to available slots.
- If an executor holding Stage 0's shuffle files dies after Stage 0 completes, Stage 1 reducers hit `FetchFailed`; the DAGScheduler re-runs only the **missing** Stage 0 tasks, then resumes.

**Reading lineage with `toDebugString()`**
```python
print(top.toDebugString().decode())
# Indentation + "+-" mark shuffle boundaries (= stage boundaries); the number in parentheses is the partition count.
```

**For DataFrames**, `df.explain("formatted")` shows `Exchange` nodes where shuffles (stage boundaries) occur, and the SQL tab shows which stage implements each operator.

---

## 22. Limits and Invariants Worth Knowing

| Item | Fact |
|---|---|
| Active `SparkContext` per JVM | Exactly one |
| Driver | One per application; single point of failure for batch |
| Partitions and tasks | One task per partition per stage; partition count caps parallelism |
| Concurrent tasks | `executors × cores ÷ spark.task.cpus` |
| Task result size | Direct ≤ `spark.task.maxDirectResultSize` (1 MiB default); total collected capped by `spark.driver.maxResultSize` (1 GiB default) |
| RPC message size | `spark.rpc.message.maxSize` (128 MiB default) bounds messages such as direct results/map-status |
| Task binary size | Spark warns when a task's serialized size is very large (~1 MB+): usually a large object captured in a closure — broadcast it instead |
| Block/partition size | Individual blocks near 2 GB can hit limits in some code paths; keep partitions ~100–200 MB |
| Stage retries | Bounded by `spark.stage.maxConsecutiveAttempts` (default 4) |
| Task retries | Bounded by `spark.task.maxFailures` (default 4) |
| Executor liveness | Heartbeat every `spark.executor.heartbeatInterval` (10 s), lost after `spark.network.timeout` (120 s) |
| DAGScheduler threading | Single event-loop thread: extremely large numbers of tasks/stages make the driver the bottleneck |
| Shuffle files | Live on executor local disk until the shuffle is no longer referenced (cleaned by `ContextCleaner`) |

---

## 23. Configuration Reference

| Config | Default | Purpose |
|---|---|---|
| `spark.master` / `--master` | — | Cluster manager (`local[*]`, `spark://…`, `yarn`, `k8s://…`) |
| `spark.submit.deployMode` / `--deploy-mode` | client | Where the driver runs |
| `spark.app.name` | — | Application name (UI, cluster manager) |
| `spark.driver.memory` / `spark.driver.cores` | 1g / 1 | Driver resources |
| `spark.driver.host` / `spark.driver.bindAddress` / `spark.driver.port` | auto | Driver networking (critical for client mode and containers) |
| `spark.blockManager.port` | random | Block transfer port (firewalling) |
| `spark.executor.instances` / `cores` / `memory` | 2 / 1 / 1g | Executor sizing (see the cluster & resource guide) |
| `spark.task.cpus` | 1 | Cores per task |
| `spark.default.parallelism` | total cores (RDD ops) | Default RDD partition count |
| `spark.sql.shuffle.partitions` | 200 | Default SQL shuffle partition count |
| `spark.scheduler.mode` | FIFO | Intra-application scheduling (FIFO/FAIR) |
| `spark.locality.wait` | 3s | Delay scheduling wait per locality level |
| `spark.task.maxFailures` | 4 | Task attempts before the job aborts |
| `spark.stage.maxConsecutiveAttempts` | 4 | Stage retry limit |
| `spark.speculation` | false | Speculative duplicate tasks |
| `spark.task.maxDirectResultSize` | 1m | Largest task result sent directly to the driver |
| `spark.driver.maxResultSize` | 1g | Cap on total collected results |
| `spark.rpc.message.maxSize` | 128 (MiB) | Max RPC message size |
| `spark.executor.heartbeatInterval` / `spark.network.timeout` | 10s / 120s | Liveness detection |
| `spark.dynamicAllocation.enabled` | false | Elastic executor scaling |
| `spark.ui.port` | 4040 | Web UI port |
| `spark.eventLog.enabled` | false | Persist events for the History Server |

---

## 24. Symptom-to-Architecture Table

| Symptom | Architectural cause | Where to look |
|---|---|---|
| Only one task runs in a big stage | One partition (unsplittable file, `coalesce(1)`, default parallelism) | Stage's task count; input format/partitioning |
| Idle cores while a stage runs | Fewer tasks than slots, or waiting on a straggler | Executors tab active tasks; task duration spread |
| Job shows two jobs or a "skipped" stage for one action | Sampling job (range partitioning), AQE query stages, or reuse of existing shuffle output | Jobs tab; stage "skipped" label |
| `show()` triggers several jobs | Incremental partition scan for `limit`/`take` | SQL tab; job count |
| Long gaps between jobs | Driver-side work (planning, listing, Python code, `collect`) | Jobs timeline; driver thread dump |
| "Task not serializable" / huge task size warning | Closure captured a non-serializable or large object | Task/closure contents; use `mapPartitions`/broadcast |
| Executors can't connect to the driver (client mode, containers) | Driver advertised address/ports not reachable | `spark.driver.host`/`bindAddress`/ports, network policy |
| Application hangs waiting for executors | Resource request exceeds capacity; cluster manager can't place it | Cluster manager UI/events; request size math |
| Driver OOM after `collect()` or large broadcast | Driver holds results/broadcast source | `maxResultSize`, driver memory |
| Stage re-runs after completing | `FetchFailed` and map-stage resubmission | Stage attempts; executor-lost events |
| Config change "has no effect" | Static/core config set after the context started, or overridden elsewhere | Environment tab; submit flags vs. builder |
| Behavior differs between `local` and cluster | Local mode: single JVM, no retries by default, no real shuffle transport | Test with cluster-like settings |

---

## 25. Common Misconceptions

| Misconception | Reality |
|---|---|
| "The driver processes the data." | The driver plans and coordinates; executors process data. Collected results and broadcast data pass *through* the driver. |
| "One action = one job = one stage." | One action can create several jobs; each job has multiple stages split at shuffles. |
| "A stage per transformation." | Narrow transformations are **pipelined into one stage**; only shuffles split stages. |
| "Tasks per stage depends on cores." | Tasks per stage = **partitions**. Cores only determine how many run at once. |
| "Executors are created per job." | Executors live for the application (or are scaled dynamically) and run tasks from many jobs. |
| "Transformations run when I call them." | They're lazy; only actions trigger execution. |
| "Caching makes Spark fault-tolerant." | Lineage provides fault tolerance; caching is a performance optimization and is best-effort. |
| "SparkSession replaced SparkContext." | `SparkSession` *wraps* a `SparkContext`; the context is still the engine's connection to the cluster. |
| "DataFrames avoid the RDD machinery." | DataFrame plans execute as RDDs of internal rows through the same scheduler, stages, and tasks. |
| "In PySpark everything runs in Python." | DataFrame/SQL work runs in the JVM; only Python UDF/lambda code runs in Python workers. |
| "The cluster manager schedules my tasks." | It allocates executors. **Spark's own schedulers** place tasks on those executors. |
| "Cluster mode is faster than client mode." | The modes differ in driver *location* (and thus network proximity/lifetime), not in executor processing speed. |

---

## 26. Core Architecture Checklist

**Understanding a program**
- [ ] Identify actions and the jobs they trigger (including hidden sampling/AQE/broadcast jobs).
- [ ] Mark each wide dependency; count stages from them.
- [ ] Determine partition counts at each stage (they give task counts and parallelism).
- [ ] Check the physical plan/lineage (`explain`, `toDebugString`) matches your mental DAG.

**Deploying**
- [ ] Choose the cluster manager and deploy mode deliberately (cluster mode for production).
- [ ] Budget the driver's resources (and in cluster mode, its container/pod).
- [ ] Ensure driver↔executor connectivity (host/bind address, ports, network policy) for client mode and containers.
- [ ] Use one `SparkSession` per application unless you need isolated sessions (`newSession()`).

**Scheduling and reliability**
- [ ] Set partition counts so tasks ≥ slots (2–4× cores) with ~100–200 MB partitions.
- [ ] Choose FIFO vs. FAIR scheduling for concurrent jobs.
- [ ] Know where shuffle data lives and what happens when an executor is lost.
- [ ] Keep closures small; broadcast reference data; don't capture `spark`/`sc`/connections in lambdas.

**PySpark**
- [ ] Prefer built-in functions; use Arrow/Pandas UDFs when needed; budget Python worker memory.
- [ ] Keep Python versions consistent between driver and executors.

---

## 27. A Concise Mental Model

- **Driver plans; executors compute; the cluster manager supplies resources.** The driver is thin on data and heavy on metadata, and it is the application's single point of failure.
- **An action triggers jobs; a shuffle splits a job into stages; a partition becomes a task.** Parallelism is partitions; concurrency is cores; waves are the ratio.
- **Narrow dependencies pipeline inside a stage; wide dependencies materialize shuffle files and create stage boundaries.** Most performance work is about shrinking or eliminating wide dependencies.
- **Two schedulers, two jobs.** The DAGScheduler decides *what* runs and *in which order* (and recovers lost shuffle output); the TaskScheduler decides *where and when* each task runs (and retries it). The SchedulerBackend delivers tasks to the cluster.
- **Lazy evaluation + lineage = optimization and recovery.** Spark sees the whole plan before running anything, and can rebuild any lost partition by replaying its recipe.
- **`SparkSession` is the front door; `SparkContext` is the engine's connection.** Sessions isolate SQL state; they share executors and cache.
- **DataFrames, SQL, and RDDs all execute as RDDs of tasks.** Catalyst changes *what* plan runs, not *how* the scheduler runs it.
- **PySpark and Spark Connect are different front doors to the same JVM engine** — a Py4J bridge plus Python workers for classic PySpark, a gRPC client–server split for Connect.
- **Shuffle files are both the barrier and the recovery point** between stages; understanding where they live explains most scheduling, failure, and tuning behavior.
