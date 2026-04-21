# Benchmarks

CypherGlot has benchmark entrypoints for compiler, runtime, and schema
experiments, and they answer different
questions:

- `scripts/benchmarks/benchmark_sqlite_schema_shapes.py` compares alternative
  SQLite storage schemas on the same synthetic graph workload.
- `scripts/benchmarks/benchmark_compiler.py` measures compiler-stage and
  compiler-entrypoint latency.
- `scripts/benchmarks/benchmark_sqlite_runtime.py` measures SQLite-backed
  compile-plus-execute runtime cost over the graph-to-table schema contract.
- `scripts/benchmarks/benchmark_duckdb_runtime.py` measures DuckDB-backed
  OLTP and OLAP runtime over the same synthetic graph contract.
- `scripts/benchmarks/benchmark_postgresql_runtime.py` measures PostgreSQL-
  backed compile-plus-execute runtime cost over the same contract.
- `scripts/benchmarks/benchmark_ladybug_runtime.py` measures LadybugDB-backed
  direct Cypher runtime over the same synthetic graph contract.

This page documents them separately so each benchmark path has its own scope, inputs,
commands, and output model.

## Schema benchmark

Script:

- `scripts/benchmarks/benchmark_sqlite_schema_shapes.py`

Supporting files:

- `scripts/benchmarks/results/sqlite_schema_shape_benchmark.json`

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

The default synthetic schema stress test is intentionally broader than the
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
python scripts/benchmarks/benchmark_sqlite_schema_shapes.py
```

Useful overrides:

```bash
python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --iterations 10 --warmup 2
python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --schema typeaware
python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --node-type-count 12 --edge-type-count 12 --nodes-per-type 2000 --edges-per-source 6 --multi-hop-length 6
python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --output scripts/benchmarks/results/local-sqlite-schema-shape-benchmark.json
```

### Schema output and baseline

The checked-in schema-shape baseline lives at
`scripts/benchmarks/results/sqlite_schema_shape_benchmark.json` when you choose
to persist one.

That output records:

- environment metadata
- the generated graph scale and property counts
- the synthetic edge-type routing plan
- per-schema setup timings, RSS snapshots, and database size
- per-schema row counts
- pooled execute summaries
- per-query timing summaries for each schema shape

### Small dataset

The current small schema-shape run was executed with:

- `10` node types
- `10` edge types
- `1000` nodes per node type
- `3` outgoing edges per source node for each edge type
- `5` hop multi-hop traversal depth
- `10` measured iterations
- `2` warmup iterations

That corresponds to roughly:

- `10,000` total nodes
- `30,000` total edges

Result summary from `scripts/benchmarks/results/schema-shapes-small.json`:

| Schema | Ingest | Analyze | RSS Connect | RSS Schema | RSS Ingest | RSS Analyze | Size | Pooled Execute Mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `481.29 ms` | `18.44 ms` | `20.85 MiB` | `20.89 MiB` | `27.64 MiB` | `27.64 MiB` | `12.23 MiB` | `1.63 ms` |
| typed-property | `1546.22 ms` | `114.86 ms` | `26.71 MiB` | `26.74 MiB` | `40.14 MiB` | `40.14 MiB` | `38.16 MiB` | `2.05 ms` |
| type-aware | `266.92 ms` | `17.09 ms` | `35.19 MiB` | `35.22 MiB` | `35.23 MiB` | `35.23 MiB` | `7.48 MiB` | `0.62 ms` |

Representative query means from the same run:

| Query | generic JSON | typed-property | type-aware |
| --- | ---: | ---: | ---: |
| `point_lookup` | `0.00 ms` | `0.01 ms` | `0.01 ms` |
| `top_active_score` | `0.02 ms` | `1.85 ms` | `0.01 ms` |
| `multi_hop_chain` | `0.32 ms` | `0.30 ms` | `0.13 ms` |
| `relationship_stats` | `1.87 ms` | `2.00 ms` | `1.00 ms` |
| `relationship_projection` | `7.93 ms` | `8.68 ms` | `2.53 ms` |

On the small dataset, the type-aware layout is already the strongest option:

- lowest ingest cost
- lowest analyze cost among the practical contenders
- smallest on-disk footprint
- best execute-time results on the heavier relationship and multi-hop queries

The benchmark now records RSS snapshots after `connect`, `schema`, `ingest`,
and `analyze`, and the table above shows all four checkpoints.

### Medium dataset

The current medium schema-shape run was executed with:

- `10` node types
- `10` edge types
- `100000` nodes per node type
- `4` outgoing edges per source node for each edge type
- `5` hop multi-hop traversal depth
- `10` measured iterations
- `2` warmup iterations

That corresponds to roughly:

- `1,000,000` total nodes
- `4,000,000` total edges

Result summary from `scripts/benchmarks/results/schema-shapes-medium.json`:

| Schema | Ingest | Analyze | RSS Connect | RSS Schema | RSS Ingest | RSS Analyze | Size | Pooled Execute Mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `61.12 s` | `1.31 s` | `20.85 MiB` | `20.88 MiB` | `31.79 MiB` | `31.79 MiB` | `1580.28 MiB` | `227.99 ms` |
| typed-property | `8.28 min` | `12.73 s` | `33.85 MiB` | `33.88 MiB` | `53.56 MiB` | `53.56 MiB` | `5076.25 MiB` | `326.53 ms` |
| type-aware | `33.34 s` | `935.37 ms` | `41.26 MiB` | `41.29 MiB` | `42.89 MiB` | `42.89 MiB` | `913.54 MiB` | `91.06 ms` |

Representative query means from the same run:

| Query | generic JSON | typed-property | type-aware |
| --- | ---: | ---: | ---: |
| `point_lookup` | `0.01 ms` | `0.01 ms` | `0.01 ms` |
| `top_active_score` | `0.03 ms` | `187.93 ms` | `0.01 ms` |
| `multi_hop_chain` | `1.45 ms` | `1.32 ms` | `0.55 ms` |
| `relationship_stats` | `218.15 ms` | `299.41 ms` | `125.22 ms` |
| `relationship_projection` | `1.15 s` | `1.47 s` | `420.54 ms` |

On the medium dataset, the type-aware layout separates much more clearly from
the other two options:

- lowest ingest cost by a large margin against generic JSON and an even larger
  margin against typed-property storage
- lowest analyze cost
- smallest on-disk footprint
- best execute-time results on the multi-hop and relationship-heavy queries
- no collapse on the ordered top-k read, unlike the typed-property layout

This medium baseline was re-run with the streaming/RSS-enabled harness, so the
setup table now includes the same four RSS checkpoints as the small dataset.

## Compiler benchmark

Script:

- `scripts/benchmarks/benchmark_compiler.py`

Supporting files:

- `scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- `scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- `scripts/benchmarks/results/compiler_benchmark.json`

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
python scripts/benchmarks/benchmark_compiler.py
```

Useful overrides:

```bash
python scripts/benchmarks/benchmark_compiler.py --iterations 1000 --warmup 10
python scripts/benchmarks/benchmark_compiler.py --output scripts/benchmarks/results/local-compiler-benchmark.json
python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode both
python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode off
```

The default compiler run uses:

- `1000` measured iterations per query and entrypoint
- `10` warmup iterations per query and entrypoint
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
| `validate_cypher_text(...)` | `0.65 ms` | `1.03 ms` | `1.09 ms` |
| `normalize_cypher_text(...)` | `0.71 ms` | `1.15 ms` | `1.18 ms` |

Backend-dependent public entrypoint summary from the same run:

| Entrypoint | SQLite p50 | DuckDB p50 | PostgreSQL p50 | SQLite p95 | DuckDB p95 | PostgreSQL p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `to_sqlglot_ast(...)` | `0.95 ms` | `0.96 ms` | `0.95 ms` | `1.28 ms` | `1.33 ms` | `1.28 ms` |
| `to_sql(...)` | `1.09 ms` | `2.58 ms` | `1.15 ms` | `1.40 ms` | `2.96 ms` | `1.46 ms` |
| `to_sqlglot_program(...)` | `0.86 ms` | `0.86 ms` | `0.87 ms` | `1.29 ms` | `1.30 ms` | `1.31 ms` |
| `render_cypher_program_text(...)` | `1.00 ms` | `2.57 ms` | `1.01 ms` | `1.44 ms` | `5.20 ms` | `1.45 ms` |

Backend pipeline summary from the same run:

| Backend | IR build p50 | Bind p50 | Lower p50 | Render p50 | End-to-end p50 | End-to-end p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | `2.96 us` | `0.38 us` | `69.84 us` | `68.41 us` | `1.02 ms` | `1.48 ms` |
| DuckDB unindexed | `2.99 us` | `0.38 us` | `67.07 us` | `1.47 ms` | `2.57 ms` | `5.17 ms` |
| PostgreSQL | `2.98 us` | `0.38 us` | `66.54 us` | `67.53 us` | `1.01 ms` | `1.45 ms` |

The key current result is the same, but the evidence is cleaner now that the
JSON splits shared entrypoints from backend-dependent ones: DuckDB is no longer
meaningfully behind on IR build, backend bind, or backend lower. The remaining
gap is concentrated in the backend-dependent public surface and in
`render_program`.

Why DuckDB is slow right now:

- The bottleneck is `render_program`, not IR build, backend bind, or backend
  lower.
- DuckDB render p50 is about `1.47 ms`, versus about `68 us` for SQLite and
  about `68 us` for PostgreSQL.
- The split public-entrypoint table shows the same pattern directly:
  `to_sql(...)` and `render_cypher_program_text(...)` are the places where
  DuckDB diverges sharply from SQLite and PostgreSQL, while
  `to_sqlglot_ast(...)` and `to_sqlglot_program(...)` stay much closer.
- Representative DuckDB SQL strings remain only modestly larger than the
  SQLite equivalents, so the gap is not mainly query-size bloat from
  CypherGlot.
- Profiling points at SQLGlot's DuckDB dialect generator path: it does more
  parse/tokenize/generator-setup work during render than the SQLite or
  PostgreSQL dialects.
- The benchmark is already running against compiled SQLGlot, so this remaining
  gap is not caused by falling back to the pure-Python SQLGlot build.

SQLGlot PostgreSQL-to-SQLite comparison summary from the same run:

| Implementation | Method | Queries | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| compiled (`sqlglotc`, `30.3.0`) | `tokenize(...)` | 22 | `12.87 us` | `30.27 us` | `33.09 us` |
| compiled (`sqlglotc`, `30.3.0`) | `parse_one(...)` | 22 | `36.57 us` | `96.92 us` | `108.83 us` |
| compiled (`sqlglotc`, `30.3.0`) | `parse_one(...).sql(...)` | 22 | `105.25 us` | `259.13 us` | `291.93 us` |
| compiled (`sqlglotc`, `30.3.0`) | `transpile(...)` | 22 | `63.41 us` | `154.04 us` | `168.80 us` |
| pure Python (`30.3.0`) | `tokenize(...)` | 22 | `45.47 us` | `120.35 us` | `150.79 us` |
| pure Python (`30.3.0`) | `parse_one(...)` | 22 | `125.23 us` | `302.06 us` | `357.42 us` |
| pure Python (`30.3.0`) | `parse_one(...).sql(...)` | 22 | `220.62 us` | `542.73 us` | `643.14 us` |
| pure Python (`30.3.0`) | `transpile(...)` | 22 | `163.94 us` | `407.11 us` | `484.46 us` |

Compiled SQLGlot is clearly faster than the pure-Python build, but the Cypher
to DuckDB render bottleneck persists even on the compiled path. That is why
the current DuckDB compile gap is best understood as a dialect-specific SQLGlot
render cost rather than as leftover CypherGlot lowering debt.

## Runtime benchmark

### Scale presets

| Scale | Shape | Extra properties | Traversal | Batch |
| --- | --- | --- | --- | ---: |
| small | `4` node types, `4` edge types, `1000` nodes per type, `3` edges per source, `uniform` degree | node: `2 text`, `6 numeric`, `2 boolean`; edge: `1 text`, `3 numeric`, `1 boolean` | `--variable-hop-max 2` | `1000` |
| medium | `6` node types, `8` edge types, `100000` nodes per type, `4` edges per source, `skewed` degree | node: `4 text`, `10 numeric`, `4 boolean`; edge: `2 text`, `6 numeric`, `2 boolean` | `--variable-hop-max 5` | `5000` |
| large | `10` node types, `10` edge types, `1000000` nodes per type, `8` edges per source, `skewed` degree | node: `8 text`, `18 numeric`, `8 boolean`; edge: `4 text`, `10 numeric`, `4 boolean` | `--variable-hop-max 8` | `10000` |

### Runtime matrix runner

Script:

- `scripts/benchmarks/run_runtime_matrix.py`

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

ArcadeDB heap defaults now follow the scale preset automatically:

- `small`: `ARCADEDB_JVM_ARGS='-Xmx4g'`
- `medium`: `ARCADEDB_JVM_ARGS='-Xmx8g'`
- `large`: `ARCADEDB_JVM_ARGS='-Xmx32g'`

Override that default for a given run with `--arcadedb-jvm-args`.

Example full-matrix run:

```bash
python scripts/benchmarks/run_runtime_matrix.py \
  --scale small \
  --workers 3 \
  --repeats 3 \
  --oltp-iterations 50000 \
  --oltp-warmup 500 \
  --olap-iterations 2000 \
  --olap-warmup 50 \
  --neo4j-password cypherglot1
```

```bash
python scripts/benchmarks/run_runtime_matrix.py \
  --scale medium \
  --workers 3 \
  --repeats 3 \
  --oltp-iterations 20000 \
  --oltp-warmup 200 \
  --olap-iterations 500 \
  --olap-warmup 20 \
  --neo4j-password cypherglot1
```

```bash
python scripts/benchmarks/run_runtime_matrix.py \
  --scale large \
  --workers 3 \
  --repeats 3 \
  --oltp-iterations 5000 \
  --oltp-warmup 100 \
  --olap-iterations 20 \
  --olap-warmup 3 \
  --neo4j-password cypherglot1
```

Useful overrides:

```bash
python scripts/benchmarks/run_runtime_matrix.py --scale small --workers 10 --repeats 1 --dry-run --neo4j-password cypherglot1
python scripts/benchmarks/run_runtime_matrix.py --scale medium --workers 3 --repeats 5 --shuffle-seed 7 --neo4j-password cypherglot1
python scripts/benchmarks/run_runtime_matrix.py --scale large --workers 2 --repeats 2 --arcadedb-jvm-args '-Xmx48g' --neo4j-password cypherglot1
python scripts/benchmarks/run_runtime_matrix.py --scale small --variant sqlite-indexed --variant sqlite-unindexed --workers 2 --repeats 10
```

### Runtime result summarizer

Script:

- `scripts/benchmarks/summarize_runtime_results.py`

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
those fields exist for the grouped backend. Add `--include-queries` to emit the
same style of Markdown table for per-query end-to-end percentiles.

Examples:

```bash
python scripts/benchmarks/summarize_runtime_results.py
python scripts/benchmarks/summarize_runtime_results.py --include-queries
python scripts/benchmarks/summarize_runtime_results.py scripts/benchmarks/results/runtime --output scripts/benchmarks/results/runtime-summary.md
python scripts/benchmarks/summarize_runtime_results.py scripts/benchmarks/results/runtime/sqlite-indexed-medium-r01-*.json
```

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
