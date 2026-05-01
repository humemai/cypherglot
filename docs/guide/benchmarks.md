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

### Schema output and baseline

The single-run schema-shape baseline now defaults to
`scripts/benchmarks/results/schema/sqlite_schema_shape_benchmark.json`.

That output records:

- benchmark entrypoint and run status metadata
- benchmark controls such as iterations, warmup, batch size, and selected schemas
- environment metadata
- the generated graph scale and property counts
- the synthetic edge-type routing plan
- per-schema setup timings, RSS snapshots, and database size
- per-schema row counts
- pooled execute summaries
- per-query timing summaries for each schema shape

For current result interpretation, prioritize:

- per-schema setup cost (`connect`, `schema`, `ingest`, `index`, `analyze`)
- per-schema RSS checkpoints and database size
- pooled execute `mean`, `p50`, `p95`, and `p99`
- representative query `mean`, `p50`, `p95`, and `p99`
- repeat-level consistency from the matrix summarizer

Schema setup is now timed in the more standard order:

- `connect`
- `schema`
- `ingest`
- `index`
- `analyze`

That means `ingest` reflects row loading before query indexes exist, while
`index` captures the post-load index build step explicitly.

The schema benchmark still remains primarily a comparative storage-layout
experiment rather than a tail-latency benchmark. The percentile summaries are
useful for compatibility with the other benchmark scripts, but in practice
explicit `repeats` still matter more here than driving single-run `iterations`
to runtime-benchmark levels.

## Compiler benchmark

Script:

- `scripts/benchmarks/compiler/benchmark.py`

Supporting files:

- `scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- `scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- `scripts/benchmarks/results/compiler_benchmark.json`
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
shapes, and vector-aware normalization queries.

Vector-aware queries are benchmarked only through parse, validate, and
normalize. That matches the current product contract: CypherGlot carries vector
intent for host runtimes, but does not compile vector-aware `CALL` queries to
SQL-backed output directly.

### Compiler commands

From the repo root:

```bash
python -m scripts.benchmarks.compiler.benchmark --iterations 10000 --warmup 200
python -m scripts.benchmarks.compiler.summarize_results
python -m scripts.benchmarks.compiler.summarize_results --output scripts/benchmarks/results/compiler-results.md
```

The default compiler run uses:

- `10000` measured iterations per query and entrypoint
- `200` warmup iterations per query and entrypoint
- both the installed and pure-Python SQLGlot package layouts for the
  PostgreSQL-to-SQLite comparison

### Compiler output and baseline

The checked-in compiler baseline lives at
`scripts/benchmarks/results/compiler_benchmark.json`.

The current checked-in baseline was produced from a `22`-query CypherGlot
compiler corpus and a matching `22`-query SQLGlot comparison corpus.

The benchmark schema metadata baked into the checked-in run is the current
type-aware contract used by the harness itself:

- node types: `User`, `Company`, `Person`
- edge types: `KNOWS`, `WORKS_AT`, `INTRODUCED`

That baseline records:

- a `benchmark_sections` block that declares how to read the result file
- `shared_entrypoint_results` for backend-neutral public compiler entrypoints
- `backend_entrypoint_results` for backend-dependent public compiler
  entrypoints measured once per SQL backend
- per-query summaries across the mixed admitted-subset corpus
- backend-aware IR-build, bind, lower, render, and end-to-end summaries for
  SQLite, DuckDB, and PostgreSQL
- vector-only parse / validate / normalize summaries
- SQLGlot comparison results for compiled and pure-Python installs when enabled,
  including version and module-layout metadata

Shared compiler entrypoint summary from the current checked-in run:

| Entrypoint | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `parse_cypher_text(...)` | `0.55 ms` | `0.91 ms` | `0.95 ms` |
| `validate_cypher_text(...)` | `0.64 ms` | `1.00 ms` | `1.05 ms` |
| `normalize_cypher_text(...)` | `0.71 ms` | `1.15 ms` | `1.26 ms` |

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
| `to_sqlglot_ast(...)` | `0.96 ms` | `0.94 ms` | `0.93 ms` | `1.28 ms` | `1.26 ms` | `1.26 ms` |
| `to_sql(...)` | `1.08 ms` | `1.07 ms` | `1.07 ms` | `1.39 ms` | `1.41 ms` | `1.38 ms` |
| `to_sqlglot_program(...)` | `0.83 ms` | `0.83 ms` | `0.83 ms` | `1.25 ms` | `1.24 ms` | `1.24 ms` |
| `render_cypher_program_text(...)` | `0.95 ms` | `0.94 ms` | `0.94 ms` | `1.38 ms` | `1.40 ms` | `1.37 ms` |

Backend pipeline summary from the same run:

| Backend | IR build p50 | Bind p50 | Lower p50 | Render p50 | End-to-end p50 | End-to-end p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | `2.87 us` | `0.37 us` | `63.85 us` | `64.14 us` | `0.95 ms` | `1.41 ms` |
| DuckDB | `2.89 us` | `0.37 us` | `64.20 us` | `63.69 us` | `0.94 ms` | `1.40 ms` |
| PostgreSQL | `2.86 us` | `0.37 us` | `63.69 us` | `63.91 us` | `0.95 ms` | `1.38 ms` |

The key current result changed after the render-path update and the SQLGlot
`30.6.0` rerun: the compiler benchmark no longer shows a DuckDB-specific render
penalty. Shared entrypoints, backend-dependent public entrypoints, and the
lowering-layer summaries are now tightly clustered across SQLite, DuckDB, and
PostgreSQL.

What the current compiler run shows:

- DuckDB is no longer materially behind in the compiler-only path.
- `to_sql(...)` now lands at about `1.07 ms` p50 for DuckDB, versus about
  `1.08 ms` for SQLite and about `1.07 ms` for PostgreSQL.
- `render_cypher_program_text(...)` now lands at about `0.94 ms` p50 for
  DuckDB, versus about `0.95 ms` for SQLite and about `0.94 ms` for
  PostgreSQL.
- The backend-lowering table shows the same convergence below the public API:
  DuckDB `render_program` p50 is now about `63.69 us`, essentially aligned
  with SQLite and PostgreSQL.
- This means the old compiler-only DuckDB gap was largely in render-path setup
  behavior, not in IR build, backend binding, or backend lowering.

Known DuckDB follow-up note:

- The previous DuckDB render blow-up seen in older compiler baselines is not
  present in this rerun with SQLGlot `30.6.0` and CypherGlot's renderer reuse.
- CypherGlot now reuses the SQLGlot renderer object for dialect-driven render
  calls. That is not query-result caching or SQL-string memoization; each query
  is still parsed, lowered, and rendered on every call. It only avoids
  rebuilding the same dialect printer object every time.
- Runtime benchmarks remain the place to watch for any DuckDB-specific engine
  cost or memory behavior, because they measure compile plus execute and had a
  separate RSS-accounting bug in older harness versions.
- This section should still be revisited after future SQLGlot upgrades, but
  the current compiler baseline no longer supports the older claim that DuckDB
  render remains materially slower than SQLite and PostgreSQL.

SQLGlot PostgreSQL-to-SQLite comparison summary from the same run:

| Implementation | Method | Queries | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| compiled (`sqlglotc`, `30.6.0`) | `tokenize(...)` | 22 | `12.16 us` | `26.40 us` | `31.44 us` |
| compiled (`sqlglotc`, `30.6.0`) | `parse_one(...)` | 22 | `34.26 us` | `80.43 us` | `91.24 us` |
| compiled (`sqlglotc`, `30.6.0`) | `parse_one(...).sql(...)` | 22 | `95.29 us` | `224.52 us` | `255.07 us` |
| compiled (`sqlglotc`, `30.6.0`) | `transpile(...)` | 22 | `59.14 us` | `135.50 us` | `147.21 us` |
| pure Python (`30.6.0`) | `tokenize(...)` | 22 | `45.28 us` | `116.72 us` | `148.39 us` |
| pure Python (`30.6.0`) | `parse_one(...)` | 22 | `116.76 us` | `292.52 us` | `346.94 us` |
| pure Python (`30.6.0`) | `parse_one(...).sql(...)` | 22 | `210.71 us` | `520.06 us` | `620.75 us` |
| pure Python (`30.6.0`) | `transpile(...)` | 22 | `160.77 us` | `397.67 us` | `463.10 us` |

Compiled SQLGlot is still clearly faster than the pure-Python build, but the
current compiler baseline no longer shows a DuckDB-specific render bottleneck
on the compiled path. In this rerun, the compiler-only results point to the
older gap having been largely a version- and render-setup-sensitive issue
rather than persistent CypherGlot lowering debt.

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
- `large`: `ARCADEDB_JVM_ARGS='-Xmx32g'`

Override that default for a given run with `--arcadedb-jvm-args`.

Recommended `small` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale small \
  --workers 6 \
  --repeats 3 \
  --oltp-iterations 10000 \
  --oltp-warmup 200 \
  --oltp-timeout-ms 200 \
  --olap-iterations 500 \
  --olap-warmup 20 \
  --olap-timeout-ms 10000 \
  --neo4j-password cypherglot1
```

Recommended `medium` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale medium \
  --workers 6 \
  --repeats 3 \
  --oltp-iterations 5000 \
  --oltp-warmup 100 \
  --oltp-timeout-ms 500 \
  --olap-iterations 100 \
  --olap-warmup 10 \
  --olap-timeout-ms 100000 \
  --neo4j-password cypherglot1
```

Recommended `large` run:

```bash
python -m scripts.benchmarks.runtime.matrix \
  --scale large \
  --workers 6 \
  --repeats 3 \
  --oltp-iterations 2000 \
  --oltp-warmup 20 \
  --oltp-timeout-ms 1000 \
  --olap-iterations 50 \
  --olap-warmup 5 \
  --olap-timeout-ms 200000 \
  --neo4j-password cypherglot1
```

For runtime runs, keep `repeats=3` across all scales and scale down worker
parallelism plus per-run inner-loop sampling as datasets grow, but not so far
that medium and large OLAP suites become too noisy. The current recommended
methodology is to run the full ten-variant matrix at each scale:
`sqlite-indexed`, `sqlite-unindexed`, `duckdb-unindexed`,
`postgresql-indexed`, `postgresql-unindexed`, `neo4j-indexed`,
`neo4j-unindexed`, `arcadedb-indexed`, `arcadedb-unindexed`, and
`ladybug-unindexed`. The commands above now rely on the matrix runner's default
behavior, which is to queue all ten variants unless you explicitly narrow the
run with repeated `--variant` flags. They also pin the current runtime
guardrails explicitly: hard query timeouts of `1000 ms` for OLTP and
`20000 ms` for OLAP. The timeout limits are the emergency brake for queries
that stop making progress.

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

### Small runtime dataset

### Medium runtime dataset

### Large runtime dataset

## Notes

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
