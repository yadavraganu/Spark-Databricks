## 1.1 Driver, Executors, and Cluster Managers

**Driver**
- The process that runs your `main()` function and creates the `SparkSession`/`SparkContext`.
- Converts your code into a logical plan, then a physical plan (DAG of stages/tasks).
- Schedules tasks onto executors, tracks their status, and collects results back (for actions like `collect()`).
- A single point of coordination — if the driver dies, the whole application dies (no automatic recovery of driver state, unlike executors).

**Executors**
- JVM processes launched on worker nodes for the lifetime of the application (by default).
- Run the actual tasks assigned by the driver, and store data for caching/shuffle in memory or disk.
- Each executor has a fixed number of cores and a fixed amount of memory — this directly controls how many tasks can run in parallel on that executor.
- Report heartbeats/status back to the driver.

**Cluster Managers**
Cluster managers are responsible for acquiring resources (containers/pods/processes) on which executors run. Spark supports several:

| Cluster Manager | Notes |
|---|---|
| **Standalone** | Spark's own built-in manager. Simple, good for dedicated Spark clusters, limited multi-tenancy features. |
| **YARN** | Common in Hadoop ecosystems. Two deploy modes: `client` (driver runs on the machine that submits the job) and `cluster` (driver runs inside a YARN container). Supports dynamic allocation well. |
| **Kubernetes** | Executors run as pods. Good for cloud-native, containerized environments; supports autoscaling via K8s primitives. No built-in shuffle service historically, though this has improved (e.g., shuffle tracking, external shuffle service options). |
| **Mesos** | Less common today; largely superseded by Kubernetes in new deployments. |

**Deploy modes (important, often missed):**
- `client` mode — driver runs on the machine you submitted from (e.g., your laptop or an edge node). Good for interactive/debugging use, but risky for production (if your machine disconnects, the job dies).
- `cluster` mode — driver runs on a node within the cluster, managed by the cluster manager. Preferred for production jobs since it's not tied to the submitting machine.

## 1.2 SparkContext and SparkSession

**SparkContext**
- The original entry point to Spark functionality (pre-2.0 era, and still underlies everything).
- Responsible for connecting to the cluster manager, creating RDDs, broadcasting variables, and accumulators.
- Only one active `SparkContext` per JVM at a time.

**SparkSession**
- Introduced in Spark 2.0 as a unified entry point, wrapping `SparkContext`, `SQLContext`, and `HiveContext` into one object.
- Used to create DataFrames/Datasets, run SQL queries, read/write data, and manage configuration.
- `SparkSession.builder()` internally creates a `SparkContext` if one doesn't already exist.
- In modern Spark code, you almost always start with `SparkSession`, not `SparkContext` directly.

## 1.3 Execution Hierarchy: Jobs → Stages → Tasks

- **Application** — the overall Spark program (one `SparkContext`/`SparkSession`).
- **Job** — triggered by an **action** (e.g., `collect()`, `count()`, `save()`). Each action creates one job.
- **Stage** — a job is divided into stages at **shuffle boundaries**. All transformations within a stage can be pipelined together without moving data across the network.
- **Task** — the smallest unit of execution; one task runs on one partition of data, on one executor core.

So the chain is:
`Action → Job → Stages (split by shuffles) → Tasks (one per partition)`

A job with no wide transformations at all runs as a single stage.

## 1.4 DAG Scheduler vs Task Scheduler

**DAG Scheduler**
- Takes the logical execution plan and builds a **DAG (Directed Acyclic Graph)** of stages.
- Determines stage boundaries based on shuffle dependencies (wide transformations).
- Submits stages for execution in the correct order, handling stage-level retries (e.g., if a stage fails due to lost shuffle output, it can resubmit just that stage).

**Task Scheduler**
- Takes the tasks within a stage (from the DAG Scheduler) and assigns them to specific executors.
- Handles task-level scheduling concerns: data locality (preferring to run a task where its data already resides), retries of individual failed tasks, and speculative execution of slow tasks.

**In short:** DAG Scheduler = "what stages need to run and in what order," Task Scheduler = "which executor runs which task, right now."


## 1.5 Narrow vs Wide Transformations

**Narrow transformations**
- Each output partition depends on a limited, known set of input partitions (often exactly one).
- No data movement across the network — can be pipelined within a single stage.
- Examples: `map`, `filter`, `flatMap`, `mapPartitions`, `union`.

**Wide transformations**
- Each output partition can depend on data from **many/all** input partitions.
- Requires a **shuffle** — data is repartitioned and moved across the network/disk.
- Creates a new stage boundary.
- Examples: `groupByKey`, `reduceByKey`, `join` (non-broadcast), `distinct`, `repartition`, `sortBy`.

**Impact on execution:**
- More wide transformations → more stages → more shuffles → more network/disk I/O → generally slower and more resource-intensive.
- A key optimization goal is minimizing unnecessary wide transformations, or replacing them with narrow-equivalent alternatives where possible (e.g., `reduceByKey` instead of `groupByKey` because it can pre-aggregate locally before shuffling — a "map-side combine").

## 1.6 Misc

A few pieces that are commonly asked alongside "Core Architecture" but weren't explicitly in your list:

1. **Application lifecycle at a glance:**
   `spark-submit` → Driver starts → Driver requests executors from cluster manager → Executors register with Driver → Driver sends tasks → Executors run tasks and report back → Driver aggregates results → Application ends → Executors released.

2. **Resource allocation basics** (ties directly into architecture, often asked right after this topic):
   - Static allocation: fixed number of executors for the whole application.
   - Dynamic allocation (`spark.dynamicAllocation.enabled`): executors scale up/down based on pending tasks — important for shared/multi-tenant clusters.

3. **Cluster mode vs local mode:**
   - `local[*]` runs everything (driver + "executors") in a single JVM on one machine — used for testing/dev, not real distributed execution.

4. **Data locality levels** (used by the task scheduler when assigning tasks):
   `PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY` — Spark tries to schedule tasks as close to their data as possible before falling back to less local options.

5. **Shuffle service note:** an *external shuffle service* lets executors be removed (e.g., under dynamic allocation) without losing shuffle data they produced, since the shuffle files are served independently of the executor process's lifetime.
