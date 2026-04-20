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
| DuckDB | `2.99 us` | `0.38 us` | `67.07 us` | `1.47 ms` | `2.57 ms` | `5.17 ms` |
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

### Small runtime dataset

The current small runtime matrix used the `small` preset with `1000` measured
iterations and `10` warmup iterations for both OLTP and OLAP.

That corresponds to roughly:

- `4,000` total nodes
- `12,000` total edges
- `7` backend/index combinations across SQLite, DuckDB, PostgreSQL, and Neo4j

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime/`.

For the SQL backends in this refreshed run, setup now follows the more standard
bulk-load sequence: `schema -> ingest -> index -> analyze`. That means the
reported `ingest` step no longer includes index-maintenance cost during row
insertion, and the `index` step now captures post-load index construction.

OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite indexed | `0.51 ms` | `10.58 ms` | `104.25 ms` | `13.64 ms` | `6.66 ms` | `0.98 ms` | `1.18 ms` | `1.43 ms` |
| SQLite unindexed | `0.34 ms` | `20.18 ms` | `108.22 ms` | `1.23 ms` | `0.43 ms` | `1.24 ms` | `1.56 ms` | `2.08 ms` |
| DuckDB | `15.08 ms` | `316.56 ms` | `279.07 ms` | `223.52 ms` | `0.28 ms` | `4.11 ms` | `5.29 ms` | `7.12 ms` |
| PostgreSQL indexed | `4.32 ms` | `504.14 ms` | `295.72 ms` | `423.12 ms` | `86.39 ms` | `1.39 ms` | `2.12 ms` | `2.75 ms` |
| PostgreSQL unindexed | `4.38 ms` | `678.56 ms` | `270.31 ms` | `15.91 ms` | `84.21 ms` | `1.60 ms` | `2.36 ms` | `2.95 ms` |
| Neo4j indexed | `874.79 ms` | `387.98 ms` | `1814.98 ms` | `896.75 ms` | `0.00 ms` | `0.34 ms` | `0.54 ms` | `0.95 ms` |
| Neo4j unindexed | `874.24 ms` | `423.65 ms` | `2081.02 ms` | `0.00 ms` | `0.00 ms` | `0.48 ms` | `0.82 ms` | `1.33 ms` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite indexed | `0.46 ms` | `10.58 ms` | `104.25 ms` | `13.64 ms` | `6.66 ms` | `3.63 ms` | `4.49 ms` | `5.25 ms` |
| SQLite unindexed | `0.43 ms` | `20.18 ms` | `108.22 ms` | `1.23 ms` | `0.43 ms` | `3.74 ms` | `4.69 ms` | `5.37 ms` |
| DuckDB | `15.03 ms` | `295.76 ms` | `827.75 ms` | `207.86 ms` | `0.30 ms` | `3.95 ms` | `4.48 ms` | `4.95 ms` |
| PostgreSQL indexed | `6.23 ms` | `328.51 ms` | `222.09 ms` | `239.57 ms` | `78.42 ms` | `2.94 ms` | `3.68 ms` | `4.21 ms` |
| PostgreSQL unindexed | `5.18 ms` | `334.05 ms` | `216.55 ms` | `8.87 ms` | `90.77 ms` | `2.77 ms` | `3.42 ms` | `3.84 ms` |
| Neo4j indexed | `874.79 ms` | `387.98 ms` | `1814.98 ms` | `896.75 ms` | `0.00 ms` | `2.53 ms` | `3.34 ms` | `3.96 ms` |
| Neo4j unindexed | `874.24 ms` | `423.65 ms` | `2081.02 ms` | `0.00 ms` | `0.00 ms` | `2.60 ms` | `3.56 ms` | `4.20 ms` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite indexed | `90.64 MiB` | `90.75 MiB` | `93.57 MiB` | `93.66 MiB` | `93.66 MiB` | `219.20 MiB` |
| SQLite unindexed | `90.62 MiB` | `90.72 MiB` | `93.55 MiB` | `93.62 MiB` | `93.62 MiB` | `220.32 MiB` |
| DuckDB | `94.48 MiB` | `98.72 MiB` | `153.82 MiB` | `177.64 MiB` | `177.64 MiB` | `2538.07 MiB` |
| PostgreSQL indexed | `121.06 MiB` | `122.92 MiB` | `132.30 MiB` | `132.43 MiB` | `135.20 MiB` | `261.51 MiB` |
| PostgreSQL unindexed | `120.83 MiB` | `123.02 MiB` | `132.14 MiB` | `132.52 MiB` | `134.18 MiB` | `259.63 MiB` |
| Neo4j indexed | `717.80 MiB` | `701.22 MiB` | `1828.05 MiB` | `963.13 MiB` | `963.13 MiB` | `1084.84 MiB` |
| Neo4j unindexed | `693.73 MiB` | `695.05 MiB` | `1757.54 MiB` | `1752.42 MiB` | `1752.42 MiB` | `1023.06 MiB` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite indexed | `90.64 MiB` | `90.75 MiB` | `93.57 MiB` | `93.66 MiB` | `93.66 MiB` | `259.54 MiB` |
| SQLite unindexed | `90.62 MiB` | `90.72 MiB` | `93.55 MiB` | `93.62 MiB` | `93.62 MiB` | `248.84 MiB` |
| DuckDB | `2477.86 MiB` | `2478.96 MiB` | `2518.54 MiB` | `2539.07 MiB` | `2539.07 MiB` | `4395.84 MiB` |
| PostgreSQL indexed | `258.69 MiB` | `259.62 MiB` | `260.46 MiB` | `260.80 MiB` | `262.37 MiB` | `350.64 MiB` |
| PostgreSQL unindexed | `256.47 MiB` | `257.21 MiB` | `259.18 MiB` | `259.21 MiB` | `260.75 MiB` | `348.58 MiB` |
| Neo4j indexed | `717.80 MiB` | `701.22 MiB` | `1828.05 MiB` | `963.13 MiB` | `963.13 MiB` | `1961.34 MiB` |
| Neo4j unindexed | `693.73 MiB` | `695.05 MiB` | `1757.54 MiB` | `1752.42 MiB` | `1752.42 MiB` | `1021.27 MiB` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j numbers are direct Cypher execution timings, so they are not strictly
  comparable to the compile-plus-execute SQL paths.
- DuckDB is a single-path run here. The harness does execute the same
  query-index DDL used by the other SQL backends, but this is not presented as
  a meaningful DuckDB indexed-versus-unindexed comparison, so the matrix keeps
  only one DuckDB row.
- RSS values in these tables are point-in-time resident-memory snapshots taken
  at each named checkpoint, not deltas from the previous step and not
  peak-memory readings.
- Total RSS is the sum of benchmark-process RSS plus database-server RSS when
  the backend is external.

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for
each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `0.98 ms` | `1.18 ms` | `1.43 ms` |
| `oltp/sqlite_unindexed` | `1.24 ms` | `1.56 ms` | `2.08 ms` |
| `oltp/duckdb` | `4.11 ms` | `5.29 ms` | `7.12 ms` |
| `oltp/postgresql_indexed` | `1.39 ms` | `2.12 ms` | `2.75 ms` |
| `oltp/postgresql_unindexed` | `1.60 ms` | `2.36 ms` | `2.95 ms` |
| `oltp/neo4j_indexed` | `0.34 ms` | `0.54 ms` | `0.95 ms` |
| `oltp/neo4j_unindexed` | `0.48 ms` | `0.82 ms` | `1.33 ms` |
| `olap/sqlite_indexed` | `3.63 ms` | `4.49 ms` | `5.25 ms` |
| `olap/sqlite_unindexed` | `3.74 ms` | `4.69 ms` | `5.37 ms` |
| `olap/duckdb` | `3.95 ms` | `4.48 ms` | `4.95 ms` |
| `olap/postgresql_indexed` | `2.94 ms` | `3.68 ms` | `4.21 ms` |
| `olap/postgresql_unindexed` | `2.77 ms` | `3.42 ms` | `3.84 ms` |
| `olap/neo4j_indexed` | `2.53 ms` | `3.34 ms` | `3.96 ms` |
| `olap/neo4j_unindexed` | `2.60 ms` | `3.56 ms` | `4.20 ms` |

#### Small runtime query breakdowns

These tables show per-query end-to-end percentiles for the same 7-way small
runtime matrix.

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_type1_point_lookup` | `0.93 ms` | `1.01 ms` | `2.95 ms` | `1.17 ms` | `1.22 ms` | `0.44 ms` | `0.79 ms` |
| `oltp_type1_neighbors` | `0.97 ms` | `1.23 ms` | `3.70 ms` | `1.40 ms` | `1.54 ms` | `0.40 ms` | `0.62 ms` |
| `oltp_cross_type_lookup` | `1.16 ms` | `1.42 ms` | `3.94 ms` | `1.61 ms` | `1.82 ms` | `0.34 ms` | `0.50 ms` |
| `oltp_update_type1_score` | `0.70 ms` | `0.75 ms` | `3.39 ms` | `0.88 ms` | `1.15 ms` | `0.32 ms` | `0.46 ms` |
| `oltp_create_type1_node` | `0.70 ms` | `0.68 ms` | `3.67 ms` | `0.93 ms` | `0.87 ms` | `0.33 ms` | `0.29 ms` |
| `oltp_create_cross_type_edge` | `1.36 ms` | `1.47 ms` | `5.60 ms` | `1.83 ms` | `2.01 ms` | `0.40 ms` | `0.63 ms` |
| `oltp_delete_type1_edge` | `0.73 ms` | `0.95 ms` | `4.29 ms` | `1.26 ms` | `1.50 ms` | `0.32 ms` | `0.40 ms` |
| `oltp_delete_type1_node` | `0.59 ms` | `1.88 ms` | `2.58 ms` | `0.69 ms` | `1.79 ms` | `0.28 ms` | `0.39 ms` |
| `oltp_program_create_and_link` | `1.74 ms` | `1.82 ms` | `7.99 ms` | `2.38 ms` | `2.56 ms` | `0.28 ms` | `0.39 ms` |
| `oltp_update_cross_type_edge_rank` | `0.92 ms` | `1.17 ms` | `2.96 ms` | `1.73 ms` | `1.56 ms` | `0.26 ms` | `0.37 ms` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_type1_point_lookup` | `1.07 ms` | `1.19 ms` | `3.61 ms` | `1.73 ms` | `1.75 ms` | `0.97 ms` | `1.96 ms` |
| `oltp_type1_neighbors` | `1.06 ms` | `1.47 ms` | `4.50 ms` | `2.12 ms` | `2.21 ms` | `0.60 ms` | `1.00 ms` |
| `oltp_cross_type_lookup` | `1.30 ms` | `1.69 ms` | `5.33 ms` | `2.29 ms` | `2.75 ms` | `0.53 ms` | `0.77 ms` |
| `oltp_update_type1_score` | `0.86 ms` | `0.90 ms` | `4.27 ms` | `1.18 ms` | `2.03 ms` | `0.50 ms` | `0.75 ms` |
| `oltp_create_type1_node` | `0.87 ms` | `0.91 ms` | `5.75 ms` | `1.42 ms` | `1.22 ms` | `0.49 ms` | `0.44 ms` |
| `oltp_create_cross_type_edge` | `1.71 ms` | `1.82 ms` | `7.44 ms` | `3.29 ms` | `3.07 ms` | `0.67 ms` | `0.98 ms` |
| `oltp_delete_type1_edge` | `0.94 ms` | `1.10 ms` | `5.49 ms` | `1.91 ms` | `2.22 ms` | `0.46 ms` | `0.59 ms` |
| `oltp_delete_type1_node` | `0.77 ms` | `2.17 ms` | `3.29 ms` | `1.07 ms` | `2.63 ms` | `0.44 ms` | `0.58 ms` |
| `oltp_program_create_and_link` | `2.13 ms` | `2.65 ms` | `9.62 ms` | `3.48 ms` | `3.56 ms` | `0.40 ms` | `0.63 ms` |
| `oltp_update_cross_type_edge_rank` | `1.05 ms` | `1.74 ms` | `3.56 ms` | `2.68 ms` | `2.14 ms` | `0.38 ms` | `0.53 ms` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_type1_point_lookup` | `1.28 ms` | `1.41 ms` | `4.00 ms` | `2.81 ms` | `2.15 ms` | `1.58 ms` | `3.94 ms` |
| `oltp_type1_neighbors` | `1.29 ms` | `1.87 ms` | `4.97 ms` | `2.56 ms` | `2.66 ms` | `0.87 ms` | `1.58 ms` |
| `oltp_cross_type_lookup` | `1.47 ms` | `2.17 ms` | `17.09 ms` | `2.65 ms` | `3.28 ms` | `0.96 ms` | `1.12 ms` |
| `oltp_update_type1_score` | `1.04 ms` | `1.19 ms` | `4.98 ms` | `1.29 ms` | `3.64 ms` | `0.76 ms` | `1.70 ms` |
| `oltp_create_type1_node` | `1.18 ms` | `1.20 ms` | `7.22 ms` | `2.15 ms` | `1.51 ms` | `0.80 ms` | `0.62 ms` |
| `oltp_create_cross_type_edge` | `2.04 ms` | `2.20 ms` | `8.16 ms` | `5.11 ms` | `3.50 ms` | `1.04 ms` | `1.21 ms` |
| `oltp_delete_type1_edge` | `1.22 ms` | `1.36 ms` | `6.23 ms` | `2.34 ms` | `2.80 ms` | `0.89 ms` | `0.82 ms` |
| `oltp_delete_type1_node` | `1.07 ms` | `2.54 ms` | `3.80 ms` | `1.39 ms` | `3.09 ms` | `1.08 ms` | `0.73 ms` |
| `oltp_program_create_and_link` | `2.41 ms` | `4.57 ms` | `10.71 ms` | `4.19 ms` | `4.43 ms` | `0.78 ms` | `0.93 ms` |
| `oltp_update_cross_type_edge_rank` | `1.28 ms` | `2.27 ms` | `4.05 ms` | `3.03 ms` | `2.41 ms` | `0.73 ms` | `0.62 ms` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_type1_active_leaderboard` | `1.18 ms` | `1.33 ms` | `3.93 ms` | `1.50 ms` | `1.40 ms` | `1.35 ms` | `1.32 ms` |
| `olap_type1_age_rollup` | `1.73 ms` | `1.60 ms` | `3.72 ms` | `1.59 ms` | `1.49 ms` | `0.81 ms` | `0.88 ms` |
| `olap_cross_type_edge_rollup` | `3.35 ms` | `2.58 ms` | `3.37 ms` | `2.58 ms` | `2.39 ms` | `2.28 ms` | `2.51 ms` |
| `olap_variable_length_reachability` | `1.71 ms` | `3.60 ms` | `5.34 ms` | `2.54 ms` | `2.67 ms` | `0.36 ms` | `0.47 ms` |
| `olap_three_type_path_count` | `4.94 ms` | `5.13 ms` | `3.07 ms` | `3.25 ms` | `2.74 ms` | `1.96 ms` | `1.87 ms` |
| `olap_type2_score_distribution` | `1.62 ms` | `1.99 ms` | `3.68 ms` | `1.58 ms` | `1.61 ms` | `0.75 ms` | `0.74 ms` |
| `olap_fixed_length_path_projection` | `6.88 ms` | `6.74 ms` | `3.79 ms` | `5.18 ms` | `4.64 ms` | `4.93 ms` | `4.83 ms` |
| `olap_graph_introspection_rollup` | `1.68 ms` | `2.32 ms` | `3.37 ms` | `2.78 ms` | `2.71 ms` | `2.86 ms` | `2.78 ms` |
| `olap_with_scalar_rebinding` | `2.25 ms` | `2.03 ms` | `3.76 ms` | `2.05 ms` | `2.13 ms` | `0.86 ms` | `0.83 ms` |
| `olap_variable_length_grouped_rollup` | `10.94 ms` | `10.13 ms` | `5.47 ms` | `6.35 ms` | `5.90 ms` | `9.13 ms` | `9.76 ms` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_type1_active_leaderboard` | `1.57 ms` | `1.86 ms` | `4.77 ms` | `2.10 ms` | `1.85 ms` | `1.93 ms` | `1.84 ms` |
| `olap_type1_age_rollup` | `2.43 ms` | `2.03 ms` | `4.42 ms` | `2.20 ms` | `1.97 ms` | `1.28 ms` | `1.48 ms` |
| `olap_cross_type_edge_rollup` | `4.00 ms` | `3.10 ms` | `3.77 ms` | `3.34 ms` | `3.14 ms` | `3.00 ms` | `3.92 ms` |
| `olap_variable_length_reachability` | `2.21 ms` | `5.45 ms` | `6.14 ms` | `3.21 ms` | `3.35 ms` | `0.49 ms` | `0.69 ms` |
| `olap_three_type_path_count` | `6.49 ms` | `6.42 ms` | `3.81 ms` | `4.31 ms` | `3.35 ms` | `3.20 ms` | `3.01 ms` |
| `olap_type2_score_distribution` | `2.33 ms` | `2.64 ms` | `4.06 ms` | `1.97 ms` | `2.03 ms` | `1.23 ms` | `1.19 ms` |
| `olap_fixed_length_path_projection` | `8.25 ms` | `8.04 ms` | `4.13 ms` | `6.23 ms` | `5.54 ms` | `6.33 ms` | `6.81 ms` |
| `olap_graph_introspection_rollup` | `2.34 ms` | `2.87 ms` | `3.67 ms` | `3.43 ms` | `3.28 ms` | `4.00 ms` | `3.62 ms` |
| `olap_with_scalar_rebinding` | `2.88 ms` | `2.67 ms` | `4.05 ms` | `2.45 ms` | `2.80 ms` | `1.35 ms` | `1.30 ms` |
| `olap_variable_length_grouped_rollup` | `12.40 ms` | `11.81 ms` | `5.96 ms` | `7.52 ms` | `6.86 ms` | `10.61 ms` | `11.70 ms` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_type1_active_leaderboard` | `1.81 ms` | `2.70 ms` | `5.40 ms` | `2.78 ms` | `2.14 ms` | `2.25 ms` | `2.18 ms` |
| `olap_type1_age_rollup` | `3.93 ms` | `2.37 ms` | `5.05 ms` | `2.53 ms` | `2.20 ms` | `1.40 ms` | `1.79 ms` |
| `olap_cross_type_edge_rollup` | `4.86 ms` | `3.51 ms` | `4.24 ms` | `3.73 ms` | `3.53 ms` | `3.94 ms` | `4.35 ms` |
| `olap_variable_length_reachability` | `2.60 ms` | `6.86 ms` | `6.67 ms` | `3.59 ms` | `3.73 ms` | `0.62 ms` | `1.02 ms` |
| `olap_three_type_path_count` | `8.10 ms` | `7.20 ms` | `4.69 ms` | `5.11 ms` | `3.73 ms` | `3.92 ms` | `3.50 ms` |
| `olap_type2_score_distribution` | `2.52 ms` | `2.87 ms` | `4.40 ms` | `2.24 ms` | `2.37 ms` | `1.42 ms` | `1.32 ms` |
| `olap_fixed_length_path_projection` | `9.09 ms` | `8.93 ms` | `4.48 ms` | `7.03 ms` | `6.31 ms` | `7.81 ms` | `7.96 ms` |
| `olap_graph_introspection_rollup` | `2.70 ms` | `3.39 ms` | `3.85 ms` | `3.69 ms` | `3.72 ms` | `4.62 ms` | `4.42 ms` |
| `olap_with_scalar_rebinding` | `3.23 ms` | `3.05 ms` | `4.35 ms` | `2.84 ms` | `3.12 ms` | `1.50 ms` | `1.47 ms` |
| `olap_variable_length_grouped_rollup` | `13.64 ms` | `12.82 ms` | `6.38 ms` | `8.53 ms` | `7.54 ms` | `12.13 ms` | `13.97 ms` |

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
