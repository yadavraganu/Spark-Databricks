## 1. The Big Picture: Why Catalyst Exists

Everything written against Spark SQL — a `df.select().filter().groupBy()` chain, a raw SQL string, a Dataset of typed objects — ultimately goes through the **same pipeline** before a single task runs. That pipeline is Catalyst: Spark's query optimizer and plan-transformation engine. Catalyst's job is to take whatever was written, figure out what it actually means (resolve names and types against real tables), rewrite it into something more efficient without changing its meaning, decide how to execute it physically, and generate the actual JVM bytecode that will run.

The reason this matters beyond trivia: understanding Catalyst's phases is what lets you read `.explain()` output, predict which transformations will be cheap versus expensive, understand why two logically-identical queries can have wildly different performance, and reason about where a custom optimization or a UDF sits relative to everything Spark already does automatically.

<br></br>

## 2. The Four-Phase Pipeline

Catalyst processes every query through four distinct phases, each producing a different representation of the query:

```
SQL string / DataFrame API calls
        │
        ▼
┌─────────────────────┐
│  1. ANALYSIS        │  Unresolved Logical Plan → Resolved Logical Plan
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  2. LOGICAL         │  Resolved Logical Plan → Optimized Logical Plan
│     OPTIMIZATION    │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  3. PHYSICAL        │  Optimized Logical Plan → one or more Physical Plans
│     PLANNING        │  → best Physical Plan chosen (cost model)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  4. CODE GENERATION   │  Physical Plan → actual JVM bytecode (via Janino)
└─────────────────────┘
        │
        ▼
     RDD execution
```

Each phase is built on the same underlying mechanism: a **tree of nodes** (the plan) and a set of **rules** that pattern-match on parts of that tree and rewrite them. Catalyst applies rules repeatedly, in batches, until the tree stops changing (a fixed point) or a batch's iteration limit is reached.

<br></br>

## 3. Phase 1 — Analysis

**Input:** an *Unresolved Logical Plan* — a tree where column references, table names, and function calls are just strings/identifiers with no connection yet to real schema or catalog metadata (`UnresolvedRelation`, `UnresolvedAttribute`, `UnresolvedFunction` nodes).

**What happens:**
- The **Analyzer** runs a sequence of rule batches that resolve these unknowns against the **Catalog** — Spark's registry of tables, views, databases, functions, and temp views (backed by Hive Metastore, an in-memory catalog, or another external catalog implementation).
- `UnresolvedRelation("sales")` becomes a reference to the actual table's schema.
- `UnresolvedAttribute("revenue")` is matched against the schema of whichever relation(s) are in scope, and resolved to a specific, typed `AttributeReference`.
- Function calls are resolved against the **FunctionRegistry** (built-ins plus any registered UDFs), and type coercion rules are applied where needed (e.g., implicitly casting an `Int` to a `Double` in a comparison against a `Double` column).
- **Self-join and ambiguity handling** happens here too — this is the phase where an `AMBIGUOUS_REFERENCE` error is thrown if a column name can't be resolved unambiguously.

**Output:** a fully **Resolved Logical Plan** — every node knows its exact output schema (column names and types), every function call is bound to a real implementation, and the plan is guaranteed to be semantically well-formed (though not yet efficient).

If analysis fails (a table doesn't exist, a column name is misspelled, types can't be coerced), this is where `AnalysisException` is thrown — before any data is touched, which is why a typo in a column name fails instantly rather than partway through a long-running job.

<br></br>

## 4. Phase 2 — Logical Optimization

**Input:** the Resolved Logical Plan from Analysis.
**Output:** an Optimized Logical Plan — semantically identical, but rewritten to be cheaper to execute.

This phase applies a large, fixed battery of **rule-based** transformations (plus, optionally, cost-based ones). None of these rules know anything about physical execution (partitions, shuffles, memory) — they operate purely on the logical plan tree. The most important rule categories:

**Predicate Pushdown**
- Moves `WHERE`/`filter` conditions as close as possible to the data source — ideally into the scan itself, so rows are discarded before being read into memory at all (and, for formats like Parquet, before being read off disk at all, via the format's own predicate pushdown support).
- Modern Spark extends this idea to **aggregate pushdown** as well (`spark.sql.parquet.aggregatePushdown`, available for Parquet/ORC): simple aggregates like `MIN`, `MAX`, and `COUNT` can, in favorable cases, be answered directly from a file format's own footer statistics without reading the actual row data at all.

**Column Pruning**
- Removes columns from a scan/plan that are never referenced downstream — for a columnar format like Parquet, this means those columns are never even read off disk, not just dropped after reading.

**Constant Folding**
- Evaluates expressions that are fully constant at plan time (e.g., `1 + 2` inside a filter) once, during planning, rather than once per row at execution time.

**Boolean Expression Simplification**
- Simplifies expressions like `a OR true` → `true`, `a AND false` → `false`, redundant `NOT NOT a` → `a`, and similar algebraic simplifications.

**Null Propagation**
- Short-circuits expressions where the result is statically known to be `null` regardless of other inputs (e.g., certain operations on a literal `null`).

**Combining Filters/Projections**
- Merges consecutive `Filter` nodes into a single combined condition, and consecutive `Project` (column-selection) nodes into one — avoiding redundant intermediate steps in the plan tree itself.

**Subquery and Join Rewrites**
- Rewrites correlated subqueries (`EXISTS`, `IN`, scalar subqueries) into joins (typically semi/anti joins) where possible, since joins are generally more efficient to execute than naive correlated subquery evaluation.

**Cost-Based Join Reordering** (only when `spark.sql.cbo.enabled=true` and table statistics are available via `ANALYZE TABLE`)
- For multi-way joins, reorders the join sequence to minimize estimated intermediate result sizes, rather than executing strictly in the order written.

The key property of this entire phase: every rule is required to preserve the plan's **semantics** exactly — the optimizer is free to change *how* the result is computed, never *what* the result is.

<br></br>

## 5. Phase 3 — Physical Planning

**Input:** the Optimized Logical Plan.
**Output:** a single chosen Physical Plan, ready for code generation.

This is where the plan stops being an abstract description of "what to compute" and becomes a concrete description of "how to compute it on a cluster":

- **Strategies** (implementations of `SparkStrategy`) each pattern-match on logical plan nodes and propose one or more candidate physical implementations. For example, a logical `Join` node might be matched by a strategy that proposes a `BroadcastHashJoinExec`, another that proposes `SortMergeJoinExec`, and another that proposes `ShuffledHashJoinExec` — each a legitimate way to physically execute the same logical join.
- Size estimates (from statistics, or from the optimizer's own propagated size estimates through the logical plan) determine which of these candidates is actually chosen — this is the layer where `spark.sql.autoBroadcastJoinThreshold` and similar configs take effect.
- A logical `Aggregate` node similarly maps to a physical aggregation strategy (hash-based or sort-based depending on data characteristics and configuration).
- The output is a tree of `SparkPlan` nodes (`*Exec` classes) — this is exactly what `.explain()` displays, and it's the first point in the pipeline where concepts like partitioning, shuffling (`Exchange` nodes), and specific join/aggregate implementations appear.

**Reading `.explain()` is reading this phase directly** — `.explain()` (simple) shows the physical plan tree; `.explain("extended")` or `.explain("formatted")` shows multiple phases (parsed, analyzed, optimized logical plan, and physical plan) side by side, which is the most direct way to see Catalyst's work at each stage for a specific query.

<br></br>

## 6. Phase 4 — Code Generation (Tungsten & Whole-Stage Codegen)

This is the phase most people have heard of by name ("Tungsten") without necessarily knowing what it actually does.

**The problem it solves:** a naive "Volcano-style" execution model (each operator pulls one row at a time from the operator below it, via virtual function calls) has significant per-row overhead — virtual dispatch, boxed objects, unnecessary intermediate allocations — that adds up enormously at scale.

**Whole-Stage Code Generation** addresses this by **collapsing an entire chain of operators into a single generated Java function**, compiled at runtime via Janino (a lightweight, fast in-memory Java compiler), eliminating virtual call overhead between operators and allowing the JIT compiler to optimize the resulting code as one unit rather than many small disjoint method calls.
- Visible in `.explain()` output as `WholeStageCodegen` wrapping a subtree of operators, with a `(1)`, `(2)`-style numbering showing which generated-code "stage" each operator belongs to.
- Not every operator supports codegen (some, like certain complex UDFs or specific join/aggregate variants, break the chain) — where it breaks, you'll see a non-codegen operator sitting outside any `WholeStageCodegen` wrapper in the plan.

**Tungsten's binary row format (`UnsafeRow`)** is the other half of this phase:
- Instead of representing rows as JVM objects (with per-field boxing, object headers, and pointer indirection), Tungsten represents rows as a **raw, fixed/variable-layout byte format**, manipulated directly via offsets (`sun.misc.Unsafe`), cutting both memory footprint and CPU cost for row access and comparison.
- This binary format is also what makes **off-heap memory** (covered in memory-management material) directly usable for execution buffers — Tungsten's row format doesn't need to be JVM-object-shaped to begin with, so it can live equally well on- or off-heap.
- Memory management at this layer uses `TaskMemoryManager`/`MemoryConsumer` abstractions to track and, when needed, spill these binary structures to disk under pressure (this is the same spill mechanism visible as "Spill (Memory)" / "Spill (Disk)" in the Spark UI for sort/aggregation-heavy stages).

**Vectorized (columnar batch) execution** is a related but distinct optimization, primarily for reading columnar file formats like Parquet and ORC: instead of materializing one row at a time, data is read and processed in **batches of columns** (`ColumnarBatch`), which is both more cache-friendly and lets Spark avoid per-row overhead for the scan portion of a plan specifically. This is controlled by `spark.sql.parquet.enableVectorizedReader` (on by default) and is a separate mechanism from whole-stage codegen, though the two compose together in practice.

### 6.1 The Code Generation Size Limit and Fallback

This is a concrete, practically-encountered failure mode that's worth knowing explicitly rather than just as a footnote to "codegen exists."

- Generated Java methods are subject to the **JVM's hard 64KB bytecode-per-method limit**. A sufficiently wide schema, a very long chain of operators fused into one `WholeStageCodegen` unit, or a deeply nested/complex expression can cause the generated code for a single method to exceed this limit.
- Spark's code generator actively tries to avoid this by **splitting generated code into multiple smaller methods** rather than one giant one, but very large or wide plans can still exceed it.
- When this happens, Spark either falls back to a non-generated, **interpreted execution path** for the affected operator (via `CodegenFallback`), or — if the generated code is large but still under the JVM's absolute limit — logs a performance warning. The telltale message is something like `"Code generated ... grows beyond 64 KB"`, and the practical symptom is a stage that runs noticeably slower than expected with no shuffle or memory cause, because it silently dropped out of the fast generated-code path.
- Relevant configs: `spark.sql.codegen.wholeStage` (toggles whole-stage codegen entirely, default `true`); `spark.sql.codegen.hugeMethodLimit` (the byte threshold, close to the JVM's actual limit, above which Spark gives up on codegen for that method and falls back); `spark.sql.codegen.fallback` (whether silent fallback to interpreted evaluation is allowed at all, default `true`).
- Practically, this tends to surface on **extremely wide tables** (hundreds of columns) or **plans with very large, non-trivial `SELECT` expression lists** — splitting an overly wide transformation into a few narrower stages, or reducing the number of columns carried through a wide intermediate step, is the usual fix.

<br></br>

## 7. Adaptive Query Execution (AQE) — A Fifth, Runtime Phase

Everything above (Analysis through Physical Planning) happens **before execution starts**, using only statically available information. AQE, introduced as a first-class feature in Spark 3.0+ (on by default since 3.2 via `spark.sql.adaptive.enabled`), adds a **runtime re-planning loop** on top of this pipeline:

- Spark executes a query in units bounded by **shuffle boundaries** ("query stages"). After a given stage completes, Spark has *actual* statistics about the data it just shuffled — exact partition sizes, exact row counts — rather than the estimates the static optimizer had to work with.
- Using these real statistics, AQE can **re-invoke parts of physical planning** for the remaining, not-yet-executed portion of the query:
  - Converting a planned sort-merge join into a broadcast join, if the actual size now qualifies.
  - Coalescing many small post-shuffle partitions into fewer, more efficiently-sized ones.
  - Splitting a detected skewed partition into several sub-partitions for a sort-merge join.
- Architecturally, this means the physical plan is no longer a single static artifact decided once — it's a sequence of plan fragments, each potentially re-optimized based on what actually happened in the fragment before it. This is visible in `.explain()` output as `AdaptiveSparkPlan`, and the *actual* plan that ran (versus the initial plan) can differ, which is why re-checking the plan after execution (via the Spark UI's SQL tab, which shows the final executed plan) is more reliable than trusting the pre-execution `.explain()` output alone for an AQE-enabled query.

<br></br>

## 8. The Catalog

The Catalog is the metadata registry Catalyst's Analysis phase depends on entirely — without it, nothing can be resolved.

- Tracks databases, tables (and their schemas, partitioning, storage format, and — if computed — statistics), views, temporary views, and functions (built-in and user-registered).
- Spark ships with an in-memory catalog by default, but commonly connects to an external catalog (Hive Metastore being the most common) so that table definitions persist across sessions/applications and are shared with other engines (Hive, Presto/Trino, etc.) reading the same data.
- `ANALYZE TABLE ... COMPUTE STATISTICS` populates size and row-count statistics (and, optionally, per-column statistics/histograms) into the Catalog — this is the data the cost-based optimizer and physical planner's size estimates draw on; without it, Spark falls back to much rougher heuristic-based size estimation, which is a direct, practical reason accurate statistics matter for plan quality.

<br></br>

## 9. DataFrame, Dataset, and SQL: One Engine Underneath

A common point of confusion worth resolving precisely: DataFrame, Dataset, and raw SQL strings are **not three different execution engines** — they're three different **front-end APIs that all produce the same kind of logical plan**, which then goes through the identical Catalyst pipeline above.

- A SQL string is parsed (via ANTLR-based parsing) directly into an Unresolved Logical Plan.
- DataFrame API calls (`.select()`, `.filter()`, etc.) build the same kind of Unresolved/Resolved Logical Plan tree incrementally, one operator at a time, as each method is called — a DataFrame is, structurally, just a wrapper around a `LogicalPlan` plus a `SparkSession`.
- A Dataset adds a layer of **compile-time type safety** on top of the same underlying plan, using **Encoders** to convert between JVM objects (case classes, for example) and Catalyst's internal `InternalRow`/`UnsafeRow` representation. Encoders are what let Spark generate efficient code for serializing/deserializing typed objects without falling back to generic Java serialization.
- Because all three converge on the same logical plan representation, a SQL query and an equivalent DataFrame chain produce **identical physical plans** and identical performance — there is no inherent performance advantage to one API over another; any difference observed in practice comes from the specific operations used, not the API style chosen to express them.
- **This compile-time typing is a JVM-language concept only.** In Scala, a `DataFrame` is literally a type alias for `Dataset[Row]` — `Row` being Catalyst's untyped, generic row representation. Java has the same distinction. **Python and R have no Dataset API at all**, since neither language has the static types Encoders rely on — PySpark's `DataFrame` is a Python-side wrapper that drives the same underlying JVM logical plan via Py4J, but every row is dynamically typed `Row`-like data; there's no typed equivalent to Scala's `Dataset[CaseClass]` available from PySpark.

<br></br>

## 10. Where User Code Breaks the Model: UDFs

Catalyst's entire optimization machinery — predicate pushdown, constant folding, cost estimation, codegen — works because it can **see into and reason about** the expressions in a query. A user-defined function is, from Catalyst's perspective, an **opaque black box**:

- Catalyst cannot push a filter through a UDF, cannot estimate its selectivity, cannot fold it as a constant even if its inputs are constant, and cannot generate specialized code for its internals — it can only generate code that calls out to the UDF as a black-box function.
- This is the structural reason "prefer built-in functions over UDFs" is such a durable piece of guidance — it's not a vague best practice, it's a direct consequence of which parts of the plan Catalyst can and cannot optimize through.
- **Pandas UDFs (vectorized UDFs)** don't change this opacity, but they do change the *execution* cost around the boundary — operating on batches via Arrow rather than row-by-row — which is a separate, execution-layer improvement from anything Catalyst-the-optimizer does.
- Spark does have a narrow mechanism for recovering some optimization around UDFs: if a UDF is deterministic and its result is reused, common-subexpression elimination can still avoid recomputing an identical UDF call multiple times within the same plan — but this is a much smaller win than what Catalyst achieves for native expressions.

<br></br>

## 11. Extending Catalyst

Catalyst is explicitly designed to be extensible, which is part of why it was built as a general rule-based tree-transformation framework rather than a hardcoded set of passes:

- `SparkSessionExtensions` is the public API for injecting custom logic into the pipeline — custom logical/physical optimizer rules, custom parser logic, or custom planning strategies can all be registered through it at `SparkSession` construction time.
- This is how various external libraries and platform-specific optimizations hook into Spark without forking it — e.g., adding a custom rule that recognizes a specific pattern and rewrites it into a more efficient equivalent, or adding a rule that enforces an organization-specific policy (such as rejecting plans that would trigger a full table scan without a partition filter) before execution is allowed to proceed.
- Because custom rules operate on the same tree structures as Catalyst's own rules, understanding the standard rule-based tree-transformation pattern (match a node shape, produce a rewritten node, return the modified tree) used throughout §4–§5 is directly transferable to writing one.

<br></br>

## 12. A Worked Example: Tracing One Query Through All Four Phases

```sql
SELECT customer_id, SUM(amount)
FROM orders
WHERE amount > 0
GROUP BY customer_id
```

1. **Analysis:** `orders` is resolved against the Catalog to its real schema; `customer_id` and `amount` are resolved to typed `AttributeReference`s; `SUM` is resolved against the FunctionRegistry as the built-in aggregate function.
2. **Logical Optimization:** if `amount > 0` can be pushed into the scan (e.g., a Parquet source supporting predicate pushdown), it's moved there so rows are filtered as early as possible; unused columns beyond `customer_id` and `amount` are pruned from the scan.
3. **Physical Planning:** the logical `Aggregate` is mapped to a physical hash-based aggregation strategy; because this requires grouping by `customer_id` across potentially many partitions, an `Exchange` (shuffle) node is inserted to repartition data by `customer_id` before the final aggregation step; the Catalog's statistics (if present) inform how many shuffle partitions and what aggregation approach is chosen.
4. **Code Generation:** the filter, projection, and aggregation operators that support codegen are fused into a single `WholeStageCodegen` unit per stage, compiled via Janino into actual bytecode that runs against `UnsafeRow`-formatted data.
5. **(If AQE is enabled):** after the shuffle stage completes, Spark observes the actual post-shuffle partition sizes and may coalesce them into fewer, better-sized partitions before the final aggregation executes.

<br></br>

## 13. Quick Reference: What Lives Where

| Concept | Phase | Key Classes/Terms |
|---|---|---|
| Resolving table/column names | Analysis | `Analyzer`, `UnresolvedRelation`, `AttributeReference`, Catalog |
| Predicate/column pushdown, constant folding | Logical Optimization | `Optimizer`, rule batches operating on `LogicalPlan` |
| Choosing broadcast vs sort-merge vs shuffle-hash | Physical Planning | `SparkStrategy`, `SparkPlan`, `*Exec` classes |
| What `.explain()` shows by default | Physical Planning output | `SparkPlan` tree |
| Fusing operators into generated code | Code Generation | `WholeStageCodegenExec`, Janino |
| Compact binary row representation | Code Generation / execution | `UnsafeRow`, `TaskMemoryManager` |
| Batch-oriented columnar file reading | Code Generation / execution | `ColumnarBatch`, vectorized Parquet/ORC reader |
| Runtime plan re-optimization | Post-planning, during execution | `AdaptiveSparkPlan`, query stages |
| Table/column size and row-count metadata | Supports Logical Optimization & Physical Planning | Catalog statistics, `ANALYZE TABLE` |
| Codegen exceeding the 64KB method limit | Code Generation | `CodegenFallback`, `spark.sql.codegen.hugeMethodLimit` |
| Compile-time typed rows (Scala/Java only) | DataFrame/Dataset unification | `Encoder`, `Dataset[T]` — not available in PySpark/R |

<br></br>

## 14. A Concise Mental Model

- Catalyst is a **tree-rewriting engine**, not a single-pass compiler — the same rule-based pattern (match, rewrite, repeat to a fixed point) recurs across Analysis, Logical Optimization, and Physical Planning.
- **Logical plans describe meaning; physical plans describe execution.** Everything before Physical Planning is free to be reshaped as long as meaning is preserved; everything from Physical Planning onward is about concrete execution cost.
- **DataFrame, Dataset, and SQL are front-ends to the same engine** — there's no hidden performance tier among them; differences come from what's expressed, not how it's expressed.
- **Tungsten and whole-stage codegen are about eliminating per-row JVM overhead**, not about choosing a different plan — they're an execution-efficiency layer underneath whatever physical plan was already chosen.
- **AQE adds a feedback loop** that the original Catalyst design didn't have — real statistics from completed shuffle stages feeding back into physical planning decisions for what's left to run.
- **UDFs are the one place this whole machinery goes dark** — which is precisely why minimizing them, or at least making them vectorized, is one of the highest-leverage things a query author controls directly.
