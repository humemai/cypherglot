# Benchmarks

CypherGlot has benchmark entrypoints for compiler, runtime, and schema
experiments, and they answer different
questions:

- `scripts/benchmarks/schema/sqlite_shapes.py` compares alternative
  SQLite storage schemas on the same synthetic graph workload.
- `scripts/benchmarks/compiler/benchmark.py` measures compiler-stage and
  compiler-entrypoint latency.
- `scripts/benchmarks/runtime/sqlite.py` measures SQLite-backed
  compile-plus-execute runtime cost over the graph-to-table schema contract.
- `scripts/benchmarks/runtime/duckdb.py` measures DuckDB-backed
  OLTP and OLAP runtime over the same synthetic graph contract.
- `scripts/benchmarks/runtime/postgresql.py` measures PostgreSQL-
  backed compile-plus-execute runtime cost over the same contract.
- `scripts/benchmarks/runtime/ladybug.py` measures LadybugDB-backed
  direct Cypher runtime over the same synthetic graph contract.

This page documents them separately so each benchmark path has its own scope, inputs,
commands, and output model.

## Schema benchmark

Script:

- `scripts/benchmarks/schema/sqlite_shapes.py`

Supporting files:

- `scripts/benchmarks/results/schema/sqlite_schema_shape_benchmark.json`
- the checked-in repeated-run schema summary Markdown artifact under
  `scripts/benchmarks/results/`

### Schema scope

This harness is for physical-schema experiments inside SQLite. It does not run
CypherGlot compilation. Instead, it builds the same synthetic graph into three
different SQLite layouts and benchmarks a representative set of direct SQL
query shapes against each layout:

- generic compatibility `nodes` and `edges`
- generic typed-property tables
- type-aware per-node-type and per-edge-type tables

The goal is to compare setup cost, database size, point reads, ordered top-k
reads, one-hop adjacency reads, multi-hop traversals, relationship aggregates,
and relationship-heavy projections under the same generated graph.

The default single-run schema benchmark is intentionally broader than the
runtime harness. It uses:

- `10` node types
- `10` edge types
- `5000` nodes per node type
- `4` outgoing edges per source node for each edge type
- a `5`-hop traversal query
- `10` numeric node properties per type

The scale is configurable with:

- `--node-type-count`
- `--edge-type-count`
- `--nodes-per-type`
- `--edges-per-source`
- `--multi-hop-length`
- `--node-numeric-property-count`
- `--node-text-property-count`
- `--node-boolean-property-count`
- `--edge-numeric-property-count`
- `--edge-text-property-count`
- `--edge-boolean-property-count`

### Schema commands

From the repo root:

```bash
python -m scripts.benchmarks.schema.sqlite_shapes
```

By default, the schema benchmark runs all three layouts: `json`, `typed`, and
`typeaware`. Use repeated `--schema` flags only when you want to restrict the
comparison to a subset.

### Schema matrix runner

Script:

- `scripts/benchmarks/schema/matrix.py`

The leaf schema benchmark remains a single-run benchmark. Use the schema matrix
runner when you want repeated runs, worker-level parallelism, per-run logs, and
stable paper-style summaries across fresh process starts.

Each queued job writes:

- its benchmark JSON into `scripts/benchmarks/results/schema/`
- a per-job log file plus a manifest into
  `scripts/benchmarks/results/schema-matrix/<run-stamp>/`

The schema matrix runner uses three named presets:

- `small`: `4` node types, `4` edge types, `1000` nodes per node type, `3`
  outgoing edges per source
- `medium`: `6` node types, `8` edge types, `100000` nodes per node type, `4`
  outgoing edges per source
- `large`: `10` node types, `10` edge types, `1000000` nodes per node type, `8`
  outgoing edges per source

These presets now match the runtime matrix dataset sizes directly.

Suggested repeated-run commands with explicit methodology:

```bash
python -m scripts.benchmarks.schema.matrix \
  --scale small \
  --workers 3 \
  --repeats 3 \
  --iterations 10000 \
  --warmup 200
```

```bash
python -m scripts.benchmarks.schema.matrix \
  --scale medium \
  --workers 3 \
  --repeats 3 \
  --iterations 2000 \
  --warmup 50
```

```bash
python -m scripts.benchmarks.schema.matrix \
  --scale large \
  --workers 3 \
  --repeats 3 \
  --iterations 500 \
  --warmup 10
```

These commands intentionally leave out `--schema`, so each run compares all
three layouts. They also leave the preset batch size unchanged. Small is still
sampled most heavily; medium and large now trade some inner-loop sampling for
lower wall-clock cost while keeping `repeats=3`. The recommended worker count
also drops with scale to reduce machine-level contention during the heaviest
ingest and query phases.

### Schema result summarizer

Script:

- `scripts/benchmarks/schema/summarize_results.py`

This summarizer scans repeated schema benchmark JSON files, groups runs that
share the same benchmark configuration, and emits Markdown tables with repeat-
level means and sample standard deviations.

The repeated-run summary now reports mean/std across runs for:

- setup timings
- RSS checkpoints
- database size
- pooled execute `mean`, `p50`, `p95`, and `p99`
- per-query `mean`, `p50`, `p95`, and `p99`

Each grouped Markdown section also prints the dataset shape for that benchmark
configuration, including the node/edge type counts, nodes per type, edges per
source, multi-hop length, total node/edge counts, and per-entity property
counts.

The query sections are also split into lightweight workload groupings using the
existing schema query set:

- OLTP-leaning: point reads, ordered top-k, and one-hop adjacency reads
- OLAP-leaning: multi-hop traversal, relationship aggregate, and relationship
  projection queries

### Schema output and current evidence

The single-run schema-shape entrypoint still defaults to
`scripts/benchmarks/results/schema/sqlite_schema_shape_benchmark.json`, but the
current repository-level evidence is the checked-in repeated-run schema summary
Markdown artifact under `scripts/benchmarks/results/`.

The single-run JSON records:

- benchmark entrypoint and run status metadata
- benchmark controls such as iterations, warmup, batch size, and selected schemas
- environment metadata
- the generated graph scale and property counts
- the synthetic edge-type routing plan
- per-schema setup timings, RSS snapshots, and database size
- per-schema row counts
- pooled execute summaries
- per-query timing summaries for each schema shape

For current interpretation, prioritize the repeated-run summary over any one
single JSON file. It captures run-to-run means and sample standard deviations
for setup cost, RSS, database size, pooled latency, and per-query latency.

The checked-in repeated-run results currently show the same ordering across the
small, medium, and large datasets: the generated type-aware layout is both the
best general-purpose storage contract and the smallest on-disk shape, while the
typed-property layout remains the expensive middle path that the repo no longer
targets.

Representative large-dataset results from the checked-in summary:

- Type-aware size is about `16050.43 MiB`, versus about `27521.76 MiB` for
  generic JSON and about `88688.90 MiB` for typed-property.
- Type-aware pooled `p50` is about `885.31 ms`, versus about `1385.39 ms` for
  generic JSON and about `3764.79 ms` for typed-property.
- Type-aware `relationship_projection` `p50` is about `9497.18 ms`, versus
  about `24110.96 ms` for generic JSON and about `33892.63 ms` for
  typed-property.
- The typed-property layout remains especially poor for ordered top-k queries:
  large `top_active_score` lands at about `2216.44 ms` `p50`, versus about
  `0.02 ms` for generic JSON and about `0.01 ms` for type-aware.

Schema setup is timed in the standard order
`connect -> schema -> ingest -> index -> analyze`, so `ingest` reflects row
loading before query indexes exist and `index` captures the post-load index
build step explicitly.

The schema benchmark is still primarily a comparative storage-layout experiment
rather than a tail-latency benchmark. The percentile summaries are useful for
compatibility with the other benchmark scripts, but in practice explicit
`repeats` matter more here than driving single-run `iterations` to
runtime-benchmark levels.

## Compiler benchmark

Script:

- `scripts/benchmarks/compiler/benchmark.py`

Supporting files:

- `scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- `scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- `scripts/benchmarks/results/compiler_benchmark.json`
- the checked-in compiler summary Markdown artifact under
  `scripts/benchmarks/results/`
- `scripts/benchmarks/compiler/summarize_results.py`

### Compiler scope

This harness is for compiler latency, not backend execution. It now measures
the general relational IR pipeline plus the current public compiler entrypoints
over the admitted `v0.1.0` subset.

Public entrypoints covered:

- `parse_cypher_text(...)`
- `validate_cypher_text(...)`
- `normalize_cypher_text(...)`
- `to_sqlglot_ast(...)`
- `to_sql(...)`
- `to_sqlglot_program(...)`
- `render_cypher_program_text(...)`

Backend-aware pipeline timings recorded for SQLite, DuckDB, and PostgreSQL:

- IR build
- backend bind
- backend lower
- rendered-program emission
- backend-specific end-to-end raw Cypher to rendered SQL/program text

The same script also runs a separate SQLGlot comparison suite over a
PostgreSQL-to-SQLite SQL corpus using:

- `tokenize(...)`
- `parse_one(...)`
- `parse_one(...).sql(dialect="sqlite")`
- `transpile(..., read="postgres", write="sqlite")`

The compiler corpus intentionally mixes query families rather than timing only a
single read shape. It currently includes ordinary reads, optional reads, `WITH`
queries, grouped aggregation, bounded variable-length reads including zero-hop
coverage, fixed-length multi-hop reads, graph-introspection projections,
metadata projections, `UNWIND`, standalone writes, traversal-backed program
shapes, and vector-aware normalization queries. In the runtime matrix, the
general variable-hop cap is scale-dependent (`2`/`5`/`8` for
small/medium/large), while grouped-rollup variable-hop OLAP queries stay
capped at `min(variable_hop_max, 3)`.

Vector-aware queries are benchmarked only through parse, validate, and
normalize. That matches the current product contract: CypherGlot carries vector
intent for host runtimes, but does not compile vector-aware `CALL` queries to
SQL-backed output directly.

### Compiler commands

From the repo root:

```bash
python -m scripts.benchmarks.compiler.benchmark --iterations 10000 --warmup 200
python -m scripts.benchmarks.compiler.summarize_results
python -m scripts.benchmarks.compiler.summarize_results --output scripts/benchmarks/results/compiler-summary.md
```

The default compiler run uses:

- `10000` measured iterations per query and entrypoint
- `200` warmup iterations per query and entrypoint
- both the installed and pure-Python SQLGlot package layouts for the
  PostgreSQL-to-SQLite comparison

### Compiler output and current evidence

The default compiler entrypoint still writes
`scripts/benchmarks/results/compiler_benchmark.json`, while the current
checked-in human-readable summary lives as a Markdown artifact under
`scripts/benchmarks/results/`.

The checked-in compiler artifacts currently reflect a `22`-query CypherGlot
compiler corpus and a matching `22`-query SQLGlot comparison corpus over the
current type-aware contract:

- node types: `User`, `Company`, `Person`
- edge types: `KNOWS`, `WORKS_AT`, `INTRODUCED`

Those artifacts record:

- a `benchmark_sections` block that declares how to read the result file
- `shared_entrypoint_results` for backend-neutral public compiler entrypoints
- `backend_entrypoint_results` for backend-dependent public compiler entrypoints
  measured once per SQL backend
- per-query summaries across the mixed admitted-subset corpus
- backend-aware IR-build, bind, lower, render, and end-to-end summaries for
  SQLite, DuckDB, and PostgreSQL
- vector-only parse / validate / normalize summaries
- SQLGlot comparison results for compiled and pure-Python installs when enabled,
  including version and module-layout metadata

Shared compiler entrypoint summary from the checked-in summary:

| Entrypoint | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `parse_cypher_text(...)` | `0.54 ms` | `0.90 ms` | `0.93 ms` |
| `validate_cypher_text(...)` | `0.64 ms` | `1.01 ms` | `1.04 ms` |
| `normalize_cypher_text(...)` | `0.70 ms` | `1.14 ms` | `1.20 ms` |

### Compiler result summarizer

Script:

- `scripts/benchmarks/compiler/summarize_results.py`

This summarizer reads one or more compiler benchmark JSON files and renders a
Markdown report. By default it consumes the checked-in single-run baseline at
`scripts/benchmarks/results/compiler_benchmark.json` and emits:

- an overview block with schema and environment metadata
- a shared-entrypoint summary table
- a backend-entrypoint summary table
- a backend-lowering summary table
- SQLGlot comparison tables when `sqlglot_suites` are present in the input

Use `--output` to write the Markdown to a file; otherwise it prints to stdout.

Backend-dependent public entrypoint summary from the same run:

| Entrypoint | SQLite p50 | DuckDB p50 | PostgreSQL p50 | SQLite p95 | DuckDB p95 | PostgreSQL p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `to_sqlglot_ast(...)` | `0.94 ms` | `0.96 ms` | `0.95 ms` | `1.28 ms` | `1.29 ms` | `1.27 ms` |
| `to_sql(...)` | `1.08 ms` | `1.07 ms` | `1.08 ms` | `1.39 ms` | `1.43 ms` | `1.41 ms` |
| `to_sqlglot_program(...)` | `0.85 ms` | `0.85 ms` | `0.85 ms` | `1.27 ms` | `1.27 ms` | `1.27 ms` |
| `render_cypher_program_text(...)` | `0.97 ms` | `0.96 ms` | `0.96 ms` | `1.42 ms` | `1.42 ms` | `1.40 ms` |

Backend pipeline summary from the same run:

| Backend | IR build p50 | Bind p50 | Lower p50 | Render p50 | End-to-end p50 | End-to-end p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | `2.93 us` | `0.38 us` | `65.82 us` | `67.99 us` | `0.96 ms` | `1.44 ms` |
| DuckDB | `2.95 us` | `0.38 us` | `67.28 us` | `67.48 us` | `0.96 ms` | `1.46 ms` |
| PostgreSQL | `2.93 us` | `0.37 us` | `65.87 us` | `66.78 us` | `0.96 ms` | `1.42 ms` |

The current compiler result remains the same at a higher confidence level than
the older single-run tables: SQLite, DuckDB, and PostgreSQL are tightly
clustered in the compiler-only path, and the earlier DuckDB-specific render
gap is not present in the checked-in summary.

What the checked-in compiler summary shows:

- Shared frontend entrypoints remain sub-millisecond at `p50`.
- Backend-dependent public entrypoints stay tightly grouped around
  `0.85 ms` to `1.08 ms` `p50` across SQLite, DuckDB, and PostgreSQL.
- The lowerer-plus-renderer path below the public API remains similarly close:
  backend `end_to_end` `p50` is about `0.96 ms` for all three SQL targets.
- Any remaining backend skew is small enough that runtime benchmarks are the
  more meaningful place to look for backend-specific behavior.

SQLGlot PostgreSQL-to-SQLite comparison summary from the same run:

| Implementation | Method | Queries | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| compiled (`sqlglotc`) | `tokenize(...)` | 22 | `12.26 us` | `26.31 us` | `31.97 us` |
| compiled (`sqlglotc`) | `parse_one(...)` | 22 | `34.63 us` | `82.33 us` | `95.17 us` |
| compiled (`sqlglotc`) | `parse_one(...).sql(...)` | 22 | `100.27 us` | `252.18 us` | `290.36 us` |
| compiled (`sqlglotc`) | `transpile(...)` | 22 | `61.30 us` | `142.93 us` | `166.39 us` |
| pure Python | `tokenize(...)` | 22 | `45.58 us` | `148.63 us` | `167.35 us` |
| pure Python | `parse_one(...)` | 22 | `129.49 us` | `345.77 us` | `390.37 us` |
| pure Python | `parse_one(...).sql(...)` | 22 | `230.01 us` | `615.92 us` | `705.77 us` |
| pure Python | `transpile(...)` | 22 | `166.30 us` | `441.03 us` | `475.67 us` |

Compiled SQLGlot is still clearly faster than the pure-Python build. That gap
remains materially larger than any compiler-side difference between CypherGlot's
SQL backends.

## Runtime benchmark

### Scale presets

| Scale | Shape | Extra properties | Traversal | Batch |
| --- | --- | --- | --- | ---: |
| small | `4` node types, `4` edge types, `1000` nodes per type, `3` edges per source, `uniform` degree | node: `2 text`, `6 numeric`, `2 boolean`; edge: `1 text`, `3 numeric`, `1 boolean` | `--variable-hop-max 2` | `1000` |
| medium | `6` node types, `8` edge types, `100000` nodes per type, `4` edges per source, `skewed` degree | node: `4 text`, `10 numeric`, `4 boolean`; edge: `2 text`, `6 numeric`, `2 boolean` | `--variable-hop-max 5` | `5000` |
| large | `10` node types, `10` edge types, `1000000` nodes per type, `8` edges per source, `skewed` degree | node: `8 text`, `18 numeric`, `8 boolean`; edge: `4 text`, `10 numeric`, `4 boolean` | `--variable-hop-max 8` | `10000` |

### Runtime matrix runner

Script:

- `scripts/benchmarks/runtime/matrix.py`

This runner schedules the current `10` runtime variants through a shuffled job
queue instead of launching a fixed set of terminals by hand. You choose:

- `--scale` as one of `small`, `medium`, or `large`
- `--workers` as the number of concurrent worker threads
- `--repeats` as the number of times to run each selected variant
- optional per-workload overrides via `--oltp-iterations`, `--oltp-warmup`,
  `--olap-iterations`, and `--olap-warmup`

Each queued job writes:

- its benchmark JSON into `scripts/benchmarks/results/runtime/`
- a per-job log file plus a manifest into
  `scripts/benchmarks/results/runtime-matrix/<run-stamp>/`
- any persisted database artifacts under
  `my_test_databases/runtime-<scale>-<run-stamp>/`

The queue is shuffled by default. Use `--shuffle-seed` for a deterministic
order or `--no-shuffle` to preserve the declared variant order.

Use repeated `--variant` flags when you want to run only a subset of the
matrix. The available variant names are the same ones returned by
`python -m scripts.benchmarks.runtime.matrix --list-variants`.

The current runtime matrix variants are:

- `sqlite-indexed`
- `sqlite-unindexed`
- `duckdb-indexed`
- `duckdb-unindexed`
- `postgresql-indexed`
- `postgresql-unindexed`
- `neo4j-indexed`
- `neo4j-unindexed`
- `arcadedb-indexed`
- `arcadedb-unindexed`
- `ladybug-unindexed`

ArcadeDB heap defaults now follow the scale preset automatically:

- `small`: `ARCADEDB_JVM_ARGS='-Xmx4g'`
- `medium`: `ARCADEDB_JVM_ARGS='-Xmx16g'`
- `large`: `ARCADEDB_JVM_ARGS='-Xmx64g'`

Override that default for a given run with `--arcadedb-jvm-args`.

When per-job containers are enabled with `--container-cpus`, you can also pass
`--arcadedb-wheel-path /absolute/path/to/arcadedb_embedded-...whl` to install a
local ArcadeDB wheel into those containers instead of resolving the latest
`arcadedb-embedded` build from PyPI.

Recommended `small` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale small \
  --workers 4 \
  --repeats 3 \
  --oltp-iterations 10000 \
  --oltp-warmup 200 \
  --oltp-timeout-ms 400 \
  --olap-iterations 500 \
  --olap-warmup 20 \
  --olap-timeout-ms 10000 \
  --arcadedb-worker-startup-timeout-s 60 \
  --neo4j-password cypherglot1 \
  --container-cpus 4
```

Recommended `medium` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale medium \
  --workers 4 \
  --repeats 3 \
  --oltp-iterations 5000 \
  --oltp-warmup 100 \
  --oltp-timeout-ms 1000 \
  --olap-iterations 100 \
  --olap-warmup 10 \
  --olap-timeout-ms 100000 \
  --arcadedb-worker-startup-timeout-s 180 \
  --neo4j-password cypherglot1 \
  --container-cpus 4
```

Recommended `large` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale large \
  --workers 4 \
  --repeats 3 \
  --oltp-iterations 2000 \
  --oltp-warmup 20 \
  --oltp-timeout-ms 2000 \
  --olap-iterations 50 \
  --olap-warmup 5 \
  --olap-timeout-ms 200000 \
  --arcadedb-worker-startup-timeout-s 3600 \
  --neo4j-password cypherglot1 \
  --container-cpus 4
```

For runtime runs, keep `repeats=3` across all scales and scale down worker
parallelism plus per-run inner-loop sampling as datasets grow, but not so far
that medium and large OLAP suites become too noisy. The current recommended
methodology is to run the full eleven-variant matrix at each scale:
`sqlite-indexed`, `sqlite-unindexed`, `duckdb-indexed`, `duckdb-unindexed`,
`postgresql-indexed`, `postgresql-unindexed`, `neo4j-indexed`,
`neo4j-unindexed`, `arcadedb-indexed`, `arcadedb-unindexed`, and
`ladybug-unindexed`. The commands above now rely on the matrix runner's default
behavior, which is to queue all eleven variants unless you explicitly narrow the
run with repeated `--variant` flags. They also pin the current runtime
guardrails explicitly: scale-specific OLTP and OLAP query timeouts plus a
separate ArcadeDB worker startup budget so larger ArcadeDB datasets have time
to open before query timing begins. The query timeout limits are the emergency
brake for queries that stop making progress; the ArcadeDB startup timeout only
covers time from ArcadeDB worker process launch until that worker reports
ready, including Python worker startup, opening the ArcadeDB database, and any
pre-ready initialization work. It does not include the startup probe query,
warmup iterations, measured iterations, or their query timeout windows. In
practice, ArcadeDB first waits for worker readiness, then runs the startup
probe, and only then can the OLTP or OLAP query timeout window begin.

Per-iteration progress output from the underlying benchmark scripts is enabled
by default. Use `--no-iteration-progress` when you want quieter worker logs.

### Runtime result summarizer

Script:

- `scripts/benchmarks/runtime/summarize_results.py`

When you run repeated runtime jobs, the per-run JSON files keep each run's own
suite percentiles and setup timings. This summarizer scans those JSON files,
groups runs that share the same benchmark configuration, skips non-completed
checkpoint payloads, and emits Markdown tables with repeat-level means and
sample standard deviations.

The suite tables aggregate the already-recorded suite percentiles, so values
such as `p50`, `p95`, and `p99` are reported as:

- mean across repeated runs
- sample standard deviation across repeated runs

It also aggregates suite setup timings such as `connect_ms`, `schema_ms`,
`ingest_ms`, `index_ms`, `analyze_ms`, `gav_ms`, or `checkpoint_ms` whenever
those fields exist for the grouped backend. Per-query end-to-end percentile
tables are now included by default; use `--no-queries` if you want only the
suite-level tables.

The cross-engine suite tables keep only shared setup phases side by side, so
ArcadeDB's `gav_ms` is not shown there. Instead, the generated report adds an
ArcadeDB-only setup section where `GAV` is broken out explicitly and described
as part of the ArcadeDB setup hierarchy:
`connect/reset -> schema/constraints -> ingest -> index -> GAV -> analyze/checkpoint`.

The ArcadeDB-only worker-startup tables also report `open` timing from the raw
`worker_startup` payloads. Worker close time is not currently recorded, so the
report cannot show it yet.

### Current checked-in repeated-run report

The current checked-in repeated-run runtime summary lives at
the runtime summary Markdown artifact under `scripts/benchmarks/results/`.

That report aggregates `99` completed JSON result files into `33` grouped
configurations for the large runtime preset. The checked-in large dataset uses:

- `10,000,000` total nodes
- `77,790,000` total edges
- `10` node types and `10` edge types
- `61` total property fields across the schema
- `11` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j,
  ArcadeDB Embedded, and LadybugDB

Representative large-dataset findings from the checked-in summary:

- Indexed OLTP `p50` is best for direct runtimes, with ArcadeDB Embedded at
  about `0.09 ms` and Neo4j at about `0.25 ms`; among the compile-plus-execute
  SQL paths, SQLite lands at about `1.27 ms`, PostgreSQL at about `1.54 ms`,
  and DuckDB at about `3.37 ms`.
- Indexed OLAP `p50` is strongest on DuckDB among the SQL backends at about
  `566.42 ms`; LadybugDB lands at about `746.38 ms` on its direct-Cypher path,
  ArcadeDB Embedded at about `3259.27 ms`, SQLite at about `4117.93 ms`,
  PostgreSQL at about `6493.17 ms`, and Neo4j at about `6902.01 ms`.
- The unindexed OLTP penalty is severe for SQLite, PostgreSQL, Neo4j, and
  ArcadeDB Embedded, but much smaller for DuckDB: about `5.80 ms` unindexed
  versus about `3.37 ms` indexed.
- Large-run wall-clock time is dominated by setup and ingest cost. DuckDB
  finishes in roughly `35` minutes, SQLite and PostgreSQL in multiple hours,
  Neo4j and ArcadeDB in roughly `4` to `6.5` hours, and LadybugDB in roughly
  `38` hours.
- Large-run RSS diverges sharply by engine: SQLite remains in the hundreds of
  MiB, PostgreSQL is roughly in the `0.5` to `1.2 GiB` range, DuckDB is around
  `5.7` to `7.1 GiB`, while ArcadeDB Embedded and LadybugDB both reach into the
  tens of GiB.

For cross-engine interpretation, keep the runtime split explicit:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute timings
  through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher execution
  timings.
- The repeated-run summary therefore answers both backend-comparison and
  methodology questions, but it is not a single apples-to-apples leaderboard.

### Runtime caveats

LadybugDB has two known upstream follow-ups that affect the large-runtime benchmark
path. One is the long ingest time on the largest dataset. The other is the grouped
variable-length traversal below:

```cypher
MATCH (a:NodeType01)-[:EdgeType01*0..3]->(b:NodeType01)
RETURN b.active AS active, count(b) AS total, avg(b.score) AS avg_score
ORDER BY total DESC, active
```

Upstream LadybugDB work is expected to address both the large ingest cost and this query
shape.

ArcadeDB also has a known reopen issue when the database is indexed and a persisted GAV
is enabled. Reopening that database can fail in the OLAP path, so the benchmark harness
works around it by keeping one GAV-enabled OLAP worker open for the full timeout-probe
and classification pass instead of reopening the database for each query. A future
ArcadeDB release may make that workaround unnecessary.

## Notes

- The current checked-in experiment summaries were produced on a Linux
  workstation built around a Ryzen 9 7950X. Treat that hardware note as result
  provenance, not as part of the public-facing benchmark naming.
- Percentiles are computed from raw per-iteration latency samples using linear
  interpolation.
- The measured loop disables Python GC to reduce avoidable collection noise.
- Not every query applies to every compiler entrypoint, so the compiler corpus
  explicitly declares valid entrypoints per query shape.
- The compiler benchmark and runtime benchmark answer different questions and
  should not be compared directly.
- The checked-in baselines are repository-local regression anchors, not general
  benchmark claims across machines, operating systems, or Python builds.
- The pure-Python SQLGlot comparison path runs in a subprocess with a temporary
  package copy that excludes compiled `.so` modules, so the active virtualenv is
  not mutated.
