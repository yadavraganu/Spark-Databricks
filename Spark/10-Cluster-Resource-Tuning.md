# 10 — Spark Cluster & Resource Tuning: The Complete Guide

> **Scope.** How much of a cluster a Spark application should get and how to shape it: executor/driver sizing, the memory-to-container math, parallelism vs. allocation, dynamic allocation, scheduling and multi-tenancy, per-cluster-manager specifics (YARN, Kubernetes, Standalone), cloud/cost considerations, GPUs and stage-level scheduling, and a repeatable right-sizing method.
> **Version notes.** A few defaults changed across Spark 3.x and 4.x; they are called out inline.
> **Related guides:** memory management (what happens *inside* the executor heap), partitioning & shuffling (partition counts, shuffle service), fault tolerance (spot capacity, decommissioning), Structured Streaming (always-on resource shape), joins and performance tuning.

---

## Table of Contents

1. What Resource Tuning Is (and Isn't)
2. The Resource Model: What You Actually Request
3. Cluster Managers Compared
4. Executor Sizing: Cores, Memory, Count
5. The Memory-to-Container Math
6. Worked Sizing Examples
7. Sizing From the Workload: Partitions, Tasks, and Waves
8. Driver Sizing
9. Parallelism vs. Allocation
10. Dynamic Allocation
11. Scheduling Inside an Application and Across Applications
12. YARN Specifics
13. Kubernetes Specifics
14. Standalone Specifics
15. Storage, Network, and Local Disk
16. GC and JVM Settings
17. GPUs, Custom Resources, and Stage-Level Scheduling
18. Cloud, Autoscaling, and Cost
19. Workload Shapes: Starting Points
20. Observability and the Right-Sizing Loop
21. Configuration Reference
22. Failure and Inefficiency Patterns
23. Anti-Patterns
24. Tuning Checklist
25. A Concise Mental Model

---

## 1. What Resource Tuning Is (and Isn't)

Memory management, joins, partitioning, and Catalyst internals determine **how well a job uses what it has**. Resource tuning decides **what it has**: how many executors, how many cores and how much memory each, how big the driver is, and how those requests interact with the cluster manager, other tenants, and the hardware. A perfectly tuned query on a starved or badly shaped allocation is still slow or failing; a generous allocation with too few partitions is still idle.

The recurring theme: **allocation, parallelism, and memory per task must agree.**

- *Allocation* (cores/executors) sets the number of simultaneous task slots.
- *Parallelism* (partition count) sets how many tasks a stage has to fill those slots.
- *Memory per task* (executor memory ÷ concurrent tasks) sets whether each task fits without spilling.

Change one in isolation and you usually move the bottleneck rather than remove it.

---

## 2. The Resource Model: What You Actually Request

Every application asks the cluster manager for a driver and a set of executors. Each executor is a JVM process (plus, for PySpark, Python worker processes) with:

| Resource | Config | What it controls |
|---|---|---|
| Executor count | `spark.executor.instances` (static) or dynamic allocation bounds | How many executors exist |
| Cores per executor | `spark.executor.cores` | Concurrent tasks per executor (task slots = cores ÷ `spark.task.cpus`) |
| Heap memory | `spark.executor.memory` | JVM heap shared by all concurrent tasks |
| Non-heap overhead | `spark.executor.memoryOverhead` (or `...Factor`) | Native memory: JVM internals, thread stacks, NIO buffers, and (by default) Python workers |
| Off-heap | `spark.memory.offHeap.size` | Tungsten off-heap pool, **additive** to heap |
| PySpark memory | `spark.executor.pyspark.memory` | Dedicated allowance for Python workers |
| Custom resources | `spark.executor.resource.{name}.amount` | GPUs and other accelerators |

The **container (YARN) or pod (Kubernetes) size is the sum** of heap + overhead + off-heap + PySpark memory (§5). Forgetting that the request is larger than `spark.executor.memory` is the most common source of "why won't my executors fit/start" and "why was my executor killed" confusion.

---

## 3. Cluster Managers Compared

Spark does not own machines; a **cluster manager** places executor processes and enforces limits.

| Aspect | Standalone | YARN | Kubernetes |
|---|---|---|---|
| Unit of placement | Executor process in a worker | Container (memory, vcores) | Pod (CPU/memory requests and limits) |
| Multi-tenancy | Minimal (`spark.cores.max`, FIFO between apps) | Strong: queues, capacity/fair scheduler, preemption, node labels | Namespace quotas, priority classes, node selectors/taints, optional batch schedulers |
| Elasticity | Dynamic allocation within the worker pool | Dynamic allocation; cluster itself usually fixed or autoscaled by the platform | Dynamic allocation plus node autoscaling (Cluster Autoscaler/Karpenter-style) |
| Shuffle durability options | Worker-hosted external shuffle service | NodeManager auxiliary service (external shuffle service) | Shuffle tracking, decommissioning, PVC reuse, remote shuffle services |
| Isolation | Process-level only | cgroups (if enabled), container limits | Strong (container cgroups, quotas) |
| Operational burden | Lowest | Moderate (Hadoop stack) | Moderate–high (K8s expertise, image management) |
| Best fit | Dedicated small/medium Spark cluster, dev/test | Hadoop ecosystem, shared multi-team clusters | Cloud-native platforms, mixed workloads on shared infrastructure |

**Deploy modes**
- `client`: the driver runs on the submitting machine. Fine for interactive work; if that machine disconnects, the application dies, and the driver must have good network connectivity to executors.
- `cluster`: the driver runs inside the cluster (an ApplicationMaster container on YARN, a driver pod on Kubernetes, a worker on Standalone). Preferred for production, and the driver then **consumes cluster capacity** you must budget for.

Mesos support has been removed from recent Spark releases; plan around YARN, Kubernetes, or Standalone.

---

## 4. Executor Sizing: Cores, Memory, Count

For a node with a given amount of CPU and RAM there are many valid ways to carve it into executors. The trade-off is between **"fat"** and **"thin"** executors.

**Fat executors (few, many cores and large heap)**
- *Pros:* less duplicated per-JVM overhead (broadcast variable copies, connection pools, code caches); more tasks share one cached/broadcast copy of data.
- *Cons:*
  - Throughput per executor tends to **plateau** as concurrent tasks grow, notably for I/O-heavy work against HDFS-style clients, and lock/contention overhead rises.
  - **GC pressure** grows with heap size; long pauses make executors look dead (heartbeat timeouts) and amplify failures.
  - Heaps beyond roughly **32 GB lose compressed object pointers**, so every reference doubles in size and a 40 GB heap can hold *less* than a 31 GB one.
  - **Bigger blast radius**: losing one executor loses more tasks, cached blocks, and shuffle files.

**Thin executors (many, 1–2 cores)**
- *Pros:* small blast radius, simple memory-per-task math.
- *Cons:* maximum per-JVM overhead; broadcast variables and cached data are duplicated per executor; no sharing across concurrent tasks; more executors to schedule and track (driver load); each executor's overhead floor (384 MB minimum) is a larger fraction.

**Practical heuristics** (starting points, not laws)
- **About 4–5 cores per executor** — a long-standing sweet spot from early Spark-on-HDFS sizing work. On object storage, where HDFS client contention doesn't apply, somewhat larger executors can be fine; validate by measurement.
- **Keep the heap below ~32 GB** (roughly 30–31 GB to be safe) to preserve compressed pointers. If a node has far more memory, run *more executors per node* rather than one giant one.
- **Memory per core of roughly 4–8 GB** covers most shuffle/aggregation workloads; join- or skew-heavy and cache-heavy workloads want more; simple scans/filters need less.
- **Reserve headroom for the node**: about 1 core and 1–2 GB (more on busy nodes) for the OS and cluster daemons (NodeManager, kubelet, monitoring agents). On Kubernetes also account for DaemonSets and system pods.
- **Leave room for the driver and ApplicationMasters** on the same nodes or queues.

---

## 5. The Memory-to-Container Math

**What the cluster manager is asked for, per executor:**

```
container/pod memory = spark.executor.memory            (JVM heap)
                     + spark.executor.memoryOverhead    (non-heap)
                     + spark.memory.offHeap.size        (if off-heap enabled)
                     + spark.executor.pyspark.memory    (if set)
```

**Overhead default:** `max(384 MiB, spark.executor.memory × spark.executor.memoryOverheadFactor)`, where the factor is **0.10**, except **0.40 for non-JVM jobs (PySpark/R) on Kubernetes**, because Python workers live in the overhead and commonly cause "memory overhead exceeded" kills. (`spark.executor.memoryOverheadFactor` has existed since Spark 3.3, replacing a Kubernetes-only setting. Setting `memoryOverhead` directly overrides the factor.) If `spark.executor.pyspark.memory` is **not** set, Python memory is paid out of overhead; if it **is** set, it is *added* to the request.

**Inside the heap** (see the memory guide for detail):

```
usable for execution + storage = (heap − 300 MB reserved) × spark.memory.fraction  (0.6)
user memory                    = the remaining 40 %
```

**Memory available per concurrent task.** All tasks on an executor share the execution pool. Spark guarantees each active task between roughly **1/(2N) and 1/N** of the execution pool (N = number of currently running tasks), and a task that cannot get what it needs **spills**. So, for planning:

```
approx. execution memory per task ≈ (heap − 0.3 GB) × 0.6 ÷ cores
```

(more if storage memory is idle, since execution can borrow it; less if cached data occupies the storage region and isn't evictable in practice).

**Platform rounding and limits to check:**
- YARN rounds container requests **up** to multiples of `yarn.scheduler.minimum-allocation-mb` and rejects requests above `yarn.scheduler.maximum-allocation-mb` and above a node's `yarn.nodemanager.resource.memory-mb`. A "19.8 GB" request may become 20 GB or 21 GB, which changes how many fit per node.
- Kubernetes sets the pod's memory **request equal to its limit**; there is no memory overcommit for Spark pods, and exceeding it is an immediate `OOMKilled`.

---

## 6. Worked Sizing Examples

*Illustrative methods, not prescriptions: the point is the order of decisions — reserve for the node, choose cores per executor, derive executor count, derive memory, then subtract overhead.*

### Example A — YARN, 16 cores / 64 GB nodes

1. Reserve 1 core and about 4 GB for OS/daemons → advertise **15 vcores / 60 GB** to YARN.
2. Choose **5 cores/executor** → `15 ÷ 5 = 3` executors per node.
3. Container budget per executor = `60 ÷ 3 = 20 GB`.
4. Container = heap + 10 % overhead → heap ≈ `20 ÷ 1.1 ≈ 18.1 GB` → set `spark.executor.memory=18g` (overhead 1.8 GB → 19.8 GB, which YARN rounds up to 20 GB).
5. Check per-task memory: `(18 − 0.3) × 0.6 ÷ 5 ≈ 2.1 GB` per task. If tasks spill, trade cores for memory (4 cores → 2.7 GB/task but only 3 executors × 4 = 12 of 15 cores used) or fix partition sizes (§7).

### Example B — Large cloud nodes, 32 vCPU / 256 GB

Naively, six 5-core executors of ~36 GB heap each would **cross the 32 GB compressed-pointer threshold**. Prefer more, smaller executors:

1. Reserve ~12 GB and 2 vCPU for the node → 30 vCPU / ~244 GB usable.
2. **7 executors × 4 cores = 28 cores**.
3. Container budget ≈ `244 ÷ 7 ≈ 34.8 GB`.
4. Heap `31g` + overhead `3.1g` = 34.1 GB ✔.
5. Per-task memory: `(31 − 0.3) × 0.6 ÷ 4 ≈ 4.6 GB`.

### Example C — PySpark on Kubernetes

`spark.executor.memory=8g`, non-JVM overhead factor 0.40 → overhead `3.2g` → **pod memory = 11.2 GB** (plus `spark.executor.pyspark.memory` if you set it separately). With 4 cores there are up to 4 Python workers in that 3.2 GB; if each worker loads a large model or builds big pandas frames, raise `pyspark.memory`/overhead or reduce cores. Remember the node must have **requests** available: 11.2 GB × executors per node must fit within the node's allocatable memory after system reservations.

### Example D — Fit to a queue/namespace quota

A queue/namespace capped at 400 cores / 1.6 TB must also hold the driver(s). With 5-core, 20 GB containers, `400 ÷ 5 = 80` executors would need 1.6 TB but leave **nothing for the driver**: plan 75 executors plus driver, or fewer cores per executor, *and* expect other applications in the same queue.

---

## 7. Sizing From the Workload: Partitions, Tasks, and Waves

Start from the data, not the hardware, when deciding the total core count.

**Tasks and waves.** A stage with *T* tasks on *C* total cores runs in about `⌈T ÷ C⌉` **waves**. Example: **2 TB** of splittable input at **128 MB** per partition ≈ **16,000 tasks**; with 500 cores that's ~32 waves; if the average task takes 40 s, the scan stage takes roughly `32 × 40 s ≈ 21 minutes`.

**Rules of thumb**
- **Target partition size ~100–200 MB** (read side via `spark.sql.files.maxPartitionBytes`; shuffle side via shuffle partition count or AQE's advisory size).
- **2–4 tasks per core** per stage gives enough slack to smooth stragglers without excessive scheduling overhead.
- **Don't over-parallelize:** tasks under ~100 ms are dominated by scheduling and serialization; thousands of tiny tasks load the driver.
- **Don't under-parallelize:** fewer tasks than cores leaves cores idle regardless of allocation.
- **Memory per task must cover the partition's working set.** Large partitions + low memory per task = spill or OOM, fixable either by more partitions or more memory per core.

**Scaling reality.** Doubling cores rarely halves runtime: stragglers (skew), shuffle bandwidth, serial driver work, and per-stage fixed overhead all cap speedup (Amdahl's law). Track **efficiency** = `core-seconds of useful task time ÷ (cores × wall time)`; falling efficiency as you add cores means you have hit a non-compute bottleneck — fix that, don't buy more cores.

---

## 8. Driver Sizing

The driver is easy to under-provision because it doesn't process bulk data, yet it:

- builds and optimizes plans (large plans and thousands of columns are expensive),
- schedules every task (cost grows with task count × executor count),
- holds broadcast data before shipping it, collected results, and accumulators,
- keeps UI/event state in memory.

**Sizing inputs**
- `spark.driver.memory` and `spark.driver.cores`; overhead follows the same factor rules as executors (0.10; 0.40 for non-JVM on Kubernetes).
- **Broadcast joins**: the broadcast side is collected on the driver first; size heap for it (and for `spark.driver.maxResultSize` if you collect results).
- **Large task counts**: tens of thousands of tasks per stage, or thousands of executors, need more driver CPU and memory. Symptoms of a starved driver: long gaps between stages, a growing "scheduler delay," dropped listener events (`spark.scheduler.listenerbus.eventqueue.capacity`, default 10,000), UI sluggishness.
- **UI retention** (`spark.ui.retainedJobs`/`retainedStages`/`retainedTasks`, `spark.sql.ui.retainedExecutions`) bounds how much history the driver keeps; lower them for long-running or very busy applications.
- In **cluster mode** the driver occupies a container/pod: include it in queue/quota math (§6D) and keep it off preemptible capacity.

---

## 9. Parallelism vs. Allocation

Total task slots = `executors × (cores ÷ spark.task.cpus)`.

| Situation | Symptom | Fix |
|---|---|---|
| Partitions ≪ slots | Low active-task count in the Executors tab, idle cores | Raise partition count (`repartition`, `spark.sql.shuffle.partitions`, smaller `maxPartitionBytes`), or reduce allocation |
| Partitions ≫ slots with tiny tasks | Many sub-100 ms tasks, high scheduler delay, busy driver | Fewer, larger partitions (AQE coalescing helps for batch SQL) |
| Slots plentiful but few *large* tasks | A handful of long tasks, rest idle | Skew: fix distribution (partitioning/joins guides), not allocation |
| Memory per task too small | Spill, GC time, OOM | More partitions (smaller per-task data), more memory per core, or fewer cores per executor |

**`spark.task.cpus`** (default 1) reserves several cores per task for tasks that spawn threads internally (native libraries, some ML code), so the scheduler doesn't oversubscribe the executor.

**Locality:** `spark.locality.wait` (default 3 s) controls how long the scheduler waits for a data-local slot before falling back. On disaggregated storage (object stores), data locality is meaningless for scans, and waiting just adds scheduling delay; lowering it (even to 0) can help scan-heavy jobs, but test, because shuffle-block and cached-block locality also use it.

---

## 10. Dynamic Allocation

Dynamic allocation (`spark.dynamicAllocation.enabled=true`) scales the executor count with pending work instead of fixing it for the application's life.

**Mechanics**
- **Scale-up:** when tasks have been backlogged for `spark.dynamicAllocation.schedulerBacklogTimeout` (1 s), Spark requests executors, then keeps requesting every `sustainedSchedulerBacklogTimeout` (defaults to the same value). Requests grow **exponentially** (1, 2, 4, 8, …) up to `maxExecutors`, so ramp-up is fast but not instantaneous.
- **Scale-down:** an executor idle for `executorIdleTimeout` (60 s) is released; executors holding cached data use `cachedExecutorIdleTimeout` (default infinity, so cached data pins executors unless you lower it).
- **Bounds:** `minExecutors` (default 0), `maxExecutors` (default unlimited), `initialExecutors` (defaults to `minExecutors`; if `spark.executor.instances` is also set, the larger wins).
- **`executorAllocationRatio`** (default 1.0): scales how many executors are requested relative to what the pending tasks could use. Lower values (e.g., 0.5) help when many tiny tasks would otherwise cause over-requesting.

**The shuffle dependency.** Removing an executor orphans any shuffle files it holds, so dynamic allocation needs a way to keep shuffle data available: the **external shuffle service** (`spark.shuffle.service.enabled`), or **shuffle tracking** (`spark.dynamicAllocation.shuffleTracking.enabled`, which since Spark 3.4 is on by default when dynamic allocation runs without a shuffle service), under which executors holding shuffle data still needed are retained (bounded by `shuffleTracking.timeout`). Without one of these, scale-down triggers `FetchFailedException` and stage recomputation (see the fault-tolerance guide).

**When it helps / when it doesn't**
- *Helps:* shared clusters; jobs whose parallelism varies widely by stage (big scan, small aggregation); interactive/notebook sessions with idle periods.
- *Neutral or harmful:* short uniform batch jobs (ramp-up overhead outweighs savings); stateful streaming (executor loss forces state reloads); latency-sensitive jobs where waiting for new executors (especially on autoscaling clusters where nodes must first boot) is costly. For those, a right-sized static allocation is simpler and more predictable.
- **Tune the ramp:** on clusters with slow node startup, raise `minExecutors` or keep a warm pool; on very bursty workloads shorten `executorIdleTimeout` carefully (churn has its own cost).

---

## 11. Scheduling Inside an Application and Across Applications

**Within one application**
- `spark.scheduler.mode=FIFO` (default): jobs get resources in submission order, so a long job can starve later short ones sharing the same `SparkContext`.
- `FAIR`: jobs share resources round-robin; define **pools** (`spark.scheduler.allocation.file`, `spark.scheduler.pool`) with weight and minimum share to prioritize categories of work. Essential for long-lived shared sessions (notebooks, query servers) running concurrent jobs.
- **Speculative execution** (`spark.speculation`) duplicates straggler tasks. It fixes environmental slowness, not data skew, and is unsafe for non-idempotent external side effects (details and the Spark 4.0 default changes are in the fault-tolerance guide).

**Across applications**
- **YARN:** capacity/fair scheduler **queues** with guaranteed capacity, elasticity, user limits, and optional preemption; submit with `--queue` / `spark.yarn.queue`.
- **Kubernetes:** namespace **ResourceQuotas**, LimitRanges, priority classes, node selectors/taints, and optionally a batch scheduler (Volcano, YuniKorn — Spark documents integrations) for **gang scheduling**, so an application starts only when enough resources exist for the driver *and* executors rather than a driver grabbing resources and waiting forever for executors.
- **Standalone:** `spark.cores.max` caps one application's cores; without it, the first application can take the whole cluster.
- **Isolation beyond quotas:** node pools/labels keep latency-sensitive services off nodes where large batch jobs run. Application-level caps (`maxExecutors`) are cooperative; hard caps must be enforced by the cluster manager.

---

## 12. YARN Specifics

- **Resource calculator gotcha:** the Capacity Scheduler's default `DefaultResourceCalculator` schedules on **memory only**; vcores requested via `spark.executor.cores` are not treated as a limit. Configure `DominantResourceCalculator` (and cgroups-based CPU enforcement) if you need cores enforced.
- **ApplicationMaster share:** each application in cluster mode puts its driver in the AM container. `yarn.scheduler.capacity.maximum-am-resource-percent` (default 10 %) caps how much of a queue the AMs may occupy; too many concurrent drivers can fill that share and **deadlock** applications waiting for executors that can never be scheduled. Size AM/driver memory deliberately.
- **Container rounding and caps** (§5): `yarn.scheduler.minimum-allocation-mb`, `maximum-allocation-mb`, `yarn.nodemanager.resource.memory-mb`. A Spark request larger than the maximum is rejected at submission.
- **Queues and preemption:** executors can be preempted when a higher-priority queue reclaims capacity; the loss shows up as executor-lost events (not charged to the task's failure count). Use stable queues for critical jobs.
- **Node labels:** `spark.yarn.executor.nodeLabelExpression` / `spark.yarn.am.nodeLabelExpression` direct executors/AMs to labeled nodes (e.g., on-demand vs. spot, GPU nodes).
- **Application attempts:** `spark.yarn.maxAppAttempts` (see the fault-tolerance guide); each attempt restarts from scratch.
- **External shuffle service** runs as a NodeManager auxiliary service and must be deployed and sized (its own heap and port) to be used with dynamic allocation.

---

## 13. Kubernetes Specifics

- **Requests and limits.** Memory request = limit = heap + overhead (+ off-heap + PySpark). CPU request defaults from `spark.executor.cores` and can be set independently with `spark.kubernetes.executor.request.cores` (fractional allowed), which does **not** change the number of concurrent tasks (that stays `spark.executor.cores`). A CPU **limit** (`spark.kubernetes.executor.limit.cores`) is unset by default; hard CPU limits can introduce CFS throttling that hurts latency-sensitive tasks, so many teams set requests without limits and rely on quotas.
- **Overhead factor:** 0.10 for JVM jobs, **0.40 for PySpark/R** (§5).
- **Pod ramp-up:** `spark.kubernetes.allocation.batch.size` (default 5) and `batch.delay` (1 s) throttle how fast executor pods are requested, which can make a 500-executor start take minutes; raise the batch size for large applications (watching API-server load).
- **Ephemeral storage:** shuffle spill goes to local directories backed by `emptyDir` on node disk (or `tmpfs` if `spark.kubernetes.local.dirs.tmpfs=true`, which counts against memory). Pods exceeding node **ephemeral-storage** limits are **evicted**, so size ephemeral-storage requests/limits for shuffle-heavy jobs or mount dedicated volumes (hostPath/NVMe, PVC) through `spark.kubernetes.executor.volumes.*`.
- **Pod templates** (`spark.kubernetes.driver.podTemplateFile`, `spark.kubernetes.executor.podTemplateFile`) for node selectors, tolerations, affinity, priority class, sidecars, security context.
- **Scheduling:** gang scheduling via Volcano/YuniKorn avoids partial-start deadlocks across competing applications. Use **PodDisruptionBudgets** and priority classes to protect critical drivers.
- **Node autoscaling:** dynamic allocation requests pods; the autoscaler provisions nodes; **node boot time becomes part of your ramp-up**. Over-provisioning ("pause pods"), warm pools, or a higher `minExecutors` can hide it.
- **Shuffle durability:** no external shuffle service by default; use shuffle tracking, graceful decommissioning, PVC reuse, or a remote shuffle service (fault-tolerance guide).
- **Driver pod:** run it via a controller (Job or Spark Operator) with a restart policy, on stable (non-spot) nodes.
- **Debugging:** `spark.kubernetes.executor.deleteOnTermination=false` keeps failed executor pods for inspection (clean up afterwards).

---

## 14. Standalone Specifics

- If `spark.executor.cores` is **not set**, each application gets **one executor per worker using all of that worker's cores**; setting it allows multiple executors per worker and finer packing.
- `spark.deploy.spreadOut` (default `true`) spreads an application's executors across workers (good for data locality and I/O), while `false` packs them onto few workers (good for consolidation).
- Worker capacity comes from `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY`; an application's total share is capped by `spark.cores.max`.
- Applications are scheduled FIFO between each other by default; one large application can monopolize the cluster without `spark.cores.max`.
- Master HA (ZooKeeper/filesystem recovery) is covered in the fault-tolerance guide.

---

## 15. Storage, Network, and Local Disk

- **Local disk drives shuffle and spill.** `spark.local.dir` accepts a comma-separated list of directories; point them at **fast local SSD/NVMe**, spread over multiple disks, and never at a small root/OS volume. (On YARN it's overridden by the NodeManager's local dirs; on Standalone by `SPARK_LOCAL_DIRS`; on Kubernetes use volumes as in §13.) A full local disk is a classic "no space left on device" failure.
- **Shuffle-heavy jobs are network- and disk-bound.** Choose instance types with adequate network bandwidth and local NVMe; on the cloud, "network-optimized" or storage-optimized instances often beat simply adding cores.
- **Disaggregated storage:** object-store reads have different throughput/latency characteristics than HDFS — request-rate limits, listing costs, and no data locality. More executors can push you into throttling rather than speed.
- **Co-located vs. disaggregated shuffle:** remote shuffle services decouple shuffle from executor lifetime (aiding elasticity and spot use) at the price of extra infrastructure.

---

## 16. GC and JVM Settings

- **G1** is the default collector on modern JVMs (Spark 4 targets Java 17+) and a good default for heaps of ~8–32 GB. Frequent or long GC pauses show as high **GC Time** in the Executors tab (a common alarm threshold is GC time above roughly 10 % of task time).
- **Fix causes before flags:** too-large partitions, caching too much, skew, and too many concurrent tasks per heap drive GC more than collector settings do. Reducing cores per executor or increasing partitions often beats tuning flags.
- Set JVM options via **`spark.executor.extraJavaOptions`** / `spark.driver.extraJavaOptions`. You cannot set heap size (`-Xmx`) there; use `spark.executor.memory`. For GC logs on JDK 9+ use unified logging (`-Xlog:gc*`), not the old `-XX:+PrintGCDetails` flags (which only apply to Java 8).
- **Off-heap** (`spark.memory.offHeap.enabled`) moves Tungsten buffers out of the GC-managed heap, reducing GC cost for large working sets; remember it is *additive* to the container size (§5).
- **Executor size vs. GC:** this is one more reason to prefer more, smaller (≤32 GB) executors over giant ones.

---

## 17. GPUs, Custom Resources, and Stage-Level Scheduling

**Accelerator-aware scheduling (Spark 3.0+).** Request GPUs (or other resources) with `spark.executor.resource.gpu.amount` and `spark.task.resource.gpu.amount`, plus a `discoveryScript` for the executor to find devices. The scheduler then places tasks only where the required resource is free. Supported on YARN, Kubernetes, and Standalone with the relevant setup (GPU nodes/labels, device plugins).

**Stage-level scheduling / ResourceProfiles (Spark 3.1+).** Different stages often want different shapes (a CPU-heavy ETL stage followed by a GPU training stage; a memory-heavy aggregation after a light scan). A `ResourceProfile` built with `ResourceProfileBuilder` and attached with `rdd.withResources(profile)` lets specific stages request different executor resources, typically with **dynamic allocation** on YARN/Kubernetes so executors matching the profile can be acquired on demand. It is powerful for mixed pipelines but has restrictions (backend support, merge behavior when profiles conflict — `spark.scheduler.resource.profileMergeConflicts`), so verify against your cluster manager and version.

---

## 18. Cloud, Autoscaling, and Cost

**Instance selection**
- **Memory-optimized** (high GB/core): shuffle-, join-, and cache-heavy workloads.
- **Compute-optimized:** CPU-bound transformations with small working sets.
- **Storage-optimized (NVMe):** shuffle- and spill-heavy jobs.
- Avoid very small instances (fixed per-node overhead) and very large ones (blast radius, GC, compressed-pointer limits).

**Autoscaling.** Managed offerings (EMR managed scaling, Dataproc autoscaling policies, platform autoscalers) and Kubernetes node autoscalers add/remove nodes based on pending demand. Interplay with Spark: dynamic allocation requests executors → the platform adds nodes (minutes) → executors start. Use `minExecutors`/warm capacity for latency-sensitive jobs, and make sure scale-in doesn't remove nodes holding needed shuffle data (decommissioning, shuffle tracking, or remote shuffle).

**Cost levers**
- **Right-size** (§20) before scaling out: over-provisioned memory and cores are the largest hidden waste.
- **Spot/preemptible** for executors, on-demand for drivers and critical state (fault-tolerance guide covers decommissioning).
- **Shorter runtime ≠ lower cost.** Optimize cost = core-hours (and storage/IO), not just wall time; a job that scales poorly can be cheaper on fewer cores.
- **Idle cost:** long-lived interactive clusters idle most of the day; use auto-termination, dynamic allocation with sensible idle timeouts, or serverless/ephemeral job clusters.
- **Data layout wins cost too:** better partitioning, file formats, and pruning reduce the cores needed (performance-tuning guide).

---

## 19. Workload Shapes: Starting Points

| Workload | Characteristics | Resource shape to start from | Watch for |
|---|---|---|---|
| **Large scan/ETL** | Wide scans, simple transforms, writes | Moderate memory/core, many executors, dynamic allocation, ~128 MB partitions | Small files, scan throttling on object stores, driver listing time |
| **Shuffle-heavy joins/aggregations** | Large shuffles, spill risk | Higher memory/core (≥6–8 GB), fast local NVMe, enough partitions, AQE on | Spill, skew, local disk exhaustion, FetchFailed |
| **Cache/ML-iterative** | Reused datasets, iteration | More memory/executor, storage-friendly levels, possibly fewer larger executors, checkpoints for lineage | Eviction, GC, cached-executor idle timeouts |
| **PySpark/UDF-heavy** | Python workers dominate non-heap memory | Higher overhead (0.3–0.4 factor or explicit `pyspark.memory`), fewer cores per executor | Container kills, per-worker memory |
| **Structured Streaming (stateful)** | Always-on, state in executors | Right-sized **static** allocation, stable executors, RocksDB for large state, headroom for catch-up | State reloads on churn, GC, shuffle-partition count fixed at first start |
| **Interactive/notebook/ad-hoc SQL** | Bursty, idle gaps | Dynamic allocation with `minExecutors`, FAIR pools, auto-termination | Idle cost, noisy neighbors, driver memory from large `collect`s |
| **GPU/ML training** | Accelerators, few big tasks | Stage-level resource profiles, `task.resource.gpu.amount`, large driver for coordination | GPU idle time, data loading bottlenecks |

---

## 20. Observability and the Right-Sizing Loop

**Where to look**
- **Executors tab:** active tasks vs. cores (utilization), memory used, GC time, shuffle read/write, failed tasks, and (in recent versions) peak memory metrics per executor.
- **Stages tab:** task duration distribution (skew/stragglers), spill (memory/disk), shuffle read size, scheduler delay.
- **Executor metrics:** enable `spark.eventLog.logStageExecutorMetrics=true` to record per-stage peaks (heap, off-heap, process-tree memory) into event logs; process-tree metrics (`spark.executor.processTreeMetrics.enabled`) expose real container memory, which is exactly what overhead sizing needs.
- **History Server** to compare runs; **metrics sinks** (Prometheus/Graphite) for trends; cluster-manager views for pending vs. granted resources and queue wait.

**The loop**
1. **Baseline:** run with a reasonable shape from §4–§7; record runtime, core-hours, GC %, spill, utilization, failures.
2. **Classify the bottleneck:** idle cores (parallelism), spill/GC/OOM (memory per task), uniform slowness at high utilization (need more compute or better code), a few long tasks (skew), long gaps between stages (driver or scheduling).
3. **Change one dimension:** partitions, cores per executor, memory, executor count, or driver size.
4. **Re-measure** and keep the change only if the targeted metric improved without hurting cost.
5. **Right-size to observed peaks** plus margin (peak heap and overhead from executor metrics), not to worst-case guesses.
6. **Re-check periodically** as data volume grows; an allocation tuned for last year's data silently degrades.

---

## 21. Configuration Reference

**Executor and driver sizing**

| Config | Default | Purpose |
|---|---|---|
| `spark.executor.instances` | 2 (YARN) | Static executor count |
| `spark.executor.cores` | 1 on YARN/K8s; all worker cores on Standalone (if unset) | Cores (task slots) per executor |
| `spark.executor.memory` | 1g | Executor JVM heap |
| `spark.executor.memoryOverhead` | `max(384m, memory × factor)` | Non-heap memory per executor |
| `spark.executor.memoryOverheadFactor` | 0.10; 0.40 for K8s non-JVM jobs | Overhead fraction |
| `spark.memory.offHeap.size` | 0 | Off-heap pool (additive to the container) |
| `spark.executor.pyspark.memory` | unset | Dedicated Python worker memory (added to the request when set) |
| `spark.driver.memory` / `spark.driver.cores` | 1g / 1 | Driver heap and cores |
| `spark.driver.memoryOverhead` / `...Factor` | `max(384m, memory × 0.10)`; 0.40 for K8s non-JVM | Driver non-heap memory |
| `spark.driver.maxResultSize` | 1g | Cap on collected results |
| `spark.task.cpus` | 1 | Cores reserved per task |
| `spark.memory.fraction` / `spark.memory.storageFraction` | 0.6 / 0.5 | Unified memory split |

**Dynamic allocation**

| Config | Default | Purpose |
|---|---|---|
| `spark.dynamicAllocation.enabled` | false | Enable elastic scaling |
| `...minExecutors` / `maxExecutors` / `initialExecutors` | 0 / ∞ / `minExecutors` | Bounds |
| `...executorAllocationRatio` | 1.0 | Scale executor requests relative to pending work |
| `...schedulerBacklogTimeout` / `sustainedSchedulerBacklogTimeout` | 1s / same | Scale-up triggers |
| `...executorIdleTimeout` / `cachedExecutorIdleTimeout` | 60s / ∞ | Scale-down triggers |
| `...shuffleTracking.enabled` / `shuffleTracking.timeout` | true when DA on and no ESS (3.4+) / ∞ | Keep executors that hold needed shuffle data |
| `spark.shuffle.service.enabled` | false | External shuffle service |

**Scheduling and locality**

| Config | Default | Purpose |
|---|---|---|
| `spark.scheduler.mode` | FIFO | Intra-application scheduling (`FAIR` for pools) |
| `spark.scheduler.pool` / `spark.scheduler.allocation.file` | unset | FAIR pool assignment and definition |
| `spark.locality.wait` | 3s | Wait for data-local slots before degrading locality |
| `spark.speculation` | false | Speculative duplicates (see fault-tolerance guide for defaults by version) |
| `spark.scheduler.listenerbus.eventqueue.capacity` | 10000 | Listener event queue; drops events when the driver is overwhelmed |
| `spark.ui.retainedJobs` / `retainedStages` / `retainedTasks` | 1000 / 1000 / 100000 | UI history kept in driver memory |

**Cluster-manager specific**

| Config | Default | Purpose |
|---|---|---|
| `spark.yarn.queue` | `default` | YARN queue |
| `spark.yarn.executor.nodeLabelExpression` / `spark.yarn.am.nodeLabelExpression` | unset | Node-label placement |
| `spark.yarn.maxAppAttempts` | RM `am.max-attempts` | Application attempts |
| `spark.kubernetes.executor.request.cores` | unset (uses `spark.executor.cores`) | Executor pod CPU request |
| `spark.kubernetes.executor.limit.cores` | unset | Executor pod CPU limit |
| `spark.kubernetes.allocation.batch.size` / `batch.delay` | 5 / 1s | Executor pod request pacing |
| `spark.kubernetes.local.dirs.tmpfs` | false | Back scratch dirs with RAM (counts against memory) |
| `spark.kubernetes.executor.deleteOnTermination` | true | Delete executor pods when they end |
| `spark.cores.max` (Standalone) | unset | Cap on total cores per application |
| `spark.deploy.spreadOut` (Standalone master) | true | Spread vs. pack executors |

**GPUs and profiles**

| Config | Purpose |
|---|---|
| `spark.executor.resource.gpu.amount` / `spark.task.resource.gpu.amount` | GPUs per executor / per task |
| `spark.executor.resource.gpu.discoveryScript` | Script to discover devices |
| `spark.scheduler.resource.profileMergeConflicts` | Behavior when stage-level profiles conflict |

**Observability**

| Config | Purpose |
|---|---|
| `spark.eventLog.enabled` / `spark.eventLog.logStageExecutorMetrics` | Persist events and per-stage executor peaks |
| `spark.executor.processTreeMetrics.enabled` | Collect process-tree memory metrics |
| `spark.metrics.conf` | Export metrics to external sinks |

---

## 22. Failure and Inefficiency Patterns

| Symptom | Likely cause | Check / fix |
|---|---|---|
| Executors never start, or the application waits forever | Request exceeds node/queue/namespace capacity (container larger than `maximum-allocation-mb`, pod larger than any node, quota exhausted) | Recompute the request (§5); compare to node allocatable, queue limits |
| Executor killed: "exceeding memory limits" / `OOMKilled` | Overhead too small (native, Python, off-heap) | `memoryOverhead`/factor, `pyspark.memory`; keep off-heap and PySpark in the budget |
| Low active-task count, idle cores | Too few partitions, or skew | Partitioning guide; raise partitions; check task duration spread |
| Spill and high GC, tasks slow | Memory per task too small | More partitions, fewer cores per executor, more memory, off-heap |
| Runtime stops improving as executors are added | Non-compute bottleneck (skew, shuffle, driver, storage throttling) or over-fat executors | Efficiency metric (§7); fix the bottleneck; consider smaller executors |
| Throughput plateau with very large executors | I/O contention, GC, lock overhead | ~4–5 cores per executor; keep heap < ~32 GB |
| Long gaps between stages; "scheduler delay" high | Driver starved or too many tiny tasks | Driver cores/memory; fewer, larger tasks; UI retention |
| Dynamic allocation scale-down causes `FetchFailed` | No shuffle service or shuffle tracking | Enable one; check version defaults |
| Jobs slow to ramp up on Kubernetes | Pod pacing and node boot time | Raise `allocation.batch.size`; warm capacity; higher `minExecutors` |
| Pods evicted for ephemeral storage | Shuffle spill exceeds node/pod ephemeral-storage | Larger limits, dedicated volumes, fix skew/spill |
| Several applications all hang waiting for executors | Drivers hold resources while executors can't be scheduled (AM share exhausted, no gang scheduling) | `maximum-am-resource-percent`, gang scheduling (Volcano/YuniKorn), admission control |
| One application monopolizes a shared cluster | No hard cluster-manager quota | Queues/ResourceQuotas; `spark.cores.max`; `maxExecutors` as a courtesy cap only |
| `spark.executor.cores` has no effect on YARN CPU use | `DefaultResourceCalculator` is memory-only | `DominantResourceCalculator`, CPU cgroups |
| Cost much higher than expected for modest data | Over-provisioned memory/cores, idle long-lived clusters, no auto-termination | Right-size from peaks; dynamic allocation; ephemeral clusters |

---

## 23. Anti-Patterns

- **"Max everything."** Giving every job the biggest executors and most cores hides inefficiency, inflates cost, and increases GC and blast radius.
- **Copying configs between workloads or clusters** without re-deriving them from node shape and data size.
- **1-core executors everywhere** (duplicated overhead, no sharing) or **one giant executor per node** (GC, compressed-pointer loss, plateau).
- **Ignoring overhead**: sizing `spark.executor.memory` to the container and wondering about kills.
- **Heap above ~32 GB** as a "more memory is better" reflex.
- **Fixing skew with more executors.**
- **Dynamic allocation without shuffle durability**, or on stateful streaming.
- **Driver on spot capacity** or left at default size for a large job.
- **Relying on application-level caps** (`maxExecutors`) for isolation on a shared cluster.
- **Never revisiting allocation** as data grows.

---

## 24. Tuning Checklist

**Shape**
- [ ] Executors sized by method: reserve node headroom → ~4–5 cores/executor → derive count → derive memory → subtract overhead.
- [ ] Heap below ~32 GB; more executors per node rather than bigger ones.
- [ ] Container/pod request computed as heap + overhead + off-heap + PySpark and fits node, queue, and quota limits (with YARN rounding in mind).
- [ ] PySpark/R jobs have explicit overhead or `pyspark.memory`, especially on Kubernetes.
- [ ] Driver sized for broadcasts, task count, result size, and UI retention; on stable capacity; budgeted in cluster mode.

**Parallelism and memory per task**
- [ ] Partition counts give 2–4 tasks per core and ~100–200 MB partitions.
- [ ] Memory per task (`≈ (heap − 0.3 GB) × 0.6 ÷ cores`) covers the working set, or partitions are made smaller.
- [ ] Locality wait considered for object-store workloads.

**Elasticity and scheduling**
- [ ] Dynamic allocation used only where workload varies, with shuffle service or shuffle tracking in place; bounds (`min/max/initial`) set deliberately.
- [ ] FAIR scheduling/pools for concurrent jobs in one application.
- [ ] Hard quotas (queues/namespace quotas) enforced by the cluster manager; AM/driver share and gang scheduling considered.

**Infrastructure**
- [ ] `spark.local.dir`/volumes on fast, large local disks; ephemeral-storage limits sized on Kubernetes.
- [ ] Instance family matched to workload (memory, compute, NVMe, network).
- [ ] Autoscaler behavior (boot time, scale-in) accounted for; spot used with decommissioning.

**Validation**
- [ ] Right-sizing loop run on real data; peak memory metrics collected; GC time and spill reviewed.
- [ ] Efficiency tracked as cores are added; scaling stops where non-compute bottlenecks begin.
- [ ] Allocation reviewed periodically as data and workloads change.

---

## 25. A Concise Mental Model

- **The cluster manager decides what Spark can have; Spark's configs decide how to ask for it and how to shape it.**
- **A request is bigger than `spark.executor.memory`**: heap + overhead + off-heap + PySpark. Most "killed" and "won't start" problems come from forgetting this.
- **Executor size is a three-way trade-off** (cores, memory, count). Fat executors pay in GC, contention, and blast radius (and lose compressed pointers past ~32 GB); thin executors pay in duplicated overhead and lost sharing.
- **Allocation, parallelism, and memory per task must agree.** Tasks ÷ cores gives waves; heap × 0.6 ÷ cores gives per-task memory. Fix the one that's actually wrong.
- **Scaling has limits.** Skew, shuffle, driver work, and storage throttling cap speedup; watch efficiency and stop adding cores when it falls.
- **Dynamic allocation is elasticity bolted onto a shuffle-durability problem.** It needs a shuffle service or shuffle tracking, and it suits variable batch and interactive work better than stateful streaming.
- **Isolation is enforced by the cluster manager**, not by cooperative application settings.
- **Measure, change one thing, re-measure.** Right-size to observed peaks, and revisit as data grows.
