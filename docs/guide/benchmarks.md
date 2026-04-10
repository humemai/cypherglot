# Benchmarks

CypherGlot has three benchmark entrypoints, and they answer different
questions:

- `scripts/benchmarks/benchmark_sqlite_schema_shapes.py` compares alternative
  SQLite storage schemas on the same synthetic graph workload.
- `scripts/benchmarks/benchmark_compiler.py` measures compiler-stage and
  compiler-entrypoint latency.
- `scripts/benchmarks/benchmark_sqlite_runtime.py` measures compile-plus-execute
  runtime cost over the graph-to-table schema contract.

This page documents them separately so each script has its own scope, inputs,
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

- generic JSON-backed `nodes` and `edges`
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
uv run python scripts/benchmarks/benchmark_sqlite_schema_shapes.py
```

Useful overrides:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --iterations 10 --warmup 2
uv run python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --schema typeaware
uv run python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --node-type-count 12 --edge-type-count 12 --nodes-per-type 2000 --edges-per-source 6 --multi-hop-length 6
uv run python scripts/benchmarks/benchmark_sqlite_schema_shapes.py --output scripts/benchmarks/results/local-sqlite-schema-shape-benchmark.json
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
- `scripts/benchmarks/results/compiler_benchmark_baseline.json`

### Compiler scope

This harness is for compiler latency, not backend execution. It measures the
public CypherGlot stages and entrypoints that matter for the current admitted
subset:

- `parse_cypher_text(...)`
- `validate_cypher_text(...)`
- `normalize_cypher_text(...)`
- `to_sqlglot_ast(...)`
- `to_sql(...)`
- `to_sqlglot_program(...)`
- `render_cypher_program_text(...)`

It also runs a separate SQLGlot comparison suite over a PostgreSQL-to-SQLite
SQL corpus using:

- `tokenize(...)`
- `parse_one(...)`
- `parse_one(...).sql(dialect="sqlite")`
- `transpile(..., read="postgres", write="sqlite")`

The compiler corpus intentionally mixes query families rather than timing only a
single read shape. It currently includes ordinary reads, optional reads, `WITH`
queries, grouped aggregation, bounded variable-length reads, graph-introspection
projections, metadata projections, `UNWIND`, standalone writes, traversal-backed
program shapes, and vector-aware normalization queries.

Vector-aware queries are benchmarked only through parse, validate, and
normalize. That matches the current product contract: CypherGlot carries vector
intent for host runtimes, but does not compile vector-aware `CALL` queries to
SQL-backed output directly.

### Compiler commands

From the repo root:

```bash
uv run python scripts/benchmarks/benchmark_compiler.py
```

Useful overrides:

```bash
uv run python scripts/benchmarks/benchmark_compiler.py --iterations 1000 --warmup 10
uv run python scripts/benchmarks/benchmark_compiler.py --output scripts/benchmarks/results/local-compiler-benchmark-baseline.json
uv run python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode both
uv run python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode off
```

The default compiler run uses:

- `1000` measured iterations per query and entrypoint
- `10` warmup iterations per query and entrypoint
- the installed SQLGlot package layout for the PostgreSQL-to-SQLite comparison

### Compiler output and baseline

The checked-in compiler baseline lives at
`scripts/benchmarks/results/compiler_benchmark_baseline.json`.

That baseline records:

- stage-level latency summaries
- per-entrypoint summaries
- per-query summaries
- vector-only normalization summaries
- SQLGlot comparison results for compiled and pure-Python installs when enabled

The current checked-in baseline was produced from a `20`-query CypherGlot
compiler corpus and a matching `20`-query SQLGlot comparison corpus.

Compiler entrypoint summary from the current checked-in run:

| Entrypoint | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `parse_cypher_text(...)` | `0.50 ms` | `0.85 ms` | `0.86 ms` |
| `validate_cypher_text(...)` | `0.62 ms` | `0.93 ms` | `0.95 ms` |
| `normalize_cypher_text(...)` | `0.68 ms` | `1.14 ms` | `1.24 ms` |
| `to_sqlglot_ast(...)` | `1.09 ms` | `1.36 ms` | `1.39 ms` |
| `to_sql(...)` | `1.24 ms` | `1.67 ms` | `1.79 ms` |
| `to_sqlglot_program(...)` | `0.86 ms` | `1.35 ms` | `1.39 ms` |
| `render_cypher_program_text(...)` | `1.07 ms` | `1.59 ms` | `1.65 ms` |

SQLGlot PostgreSQL-to-SQLite comparison summary from the same run:

| Implementation | Method | Queries | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| compiled (`sqlglotc`) | `tokenize(...)` | 20 | `11.83 us` | `26.17 us` | `26.72 us` |
| compiled (`sqlglotc`) | `parse_one(...)` | 20 | `33.39 us` | `79.78 us` | `88.96 us` |
| compiled (`sqlglotc`) | `parse_one(...).sql(...)` | 20 | `91.42 us` | `205.71 us` | `229.78 us` |
| compiled (`sqlglotc`) | `transpile(...)` | 20 | `59.64 us` | `131.33 us` | `143.55 us` |
| pure Python | `tokenize(...)` | 20 | `40.31 us` | `114.72 us` | `123.16 us` |
| pure Python | `parse_one(...)` | 20 | `109.93 us` | `282.94 us` | `310.40 us` |
| pure Python | `parse_one(...).sql(...)` | 20 | `190.04 us` | `498.10 us` | `527.75 us` |
| pure Python | `transpile(...)` | 20 | `153.47 us` | `387.53 us` | `427.76 us` |

Vector-aware normalization queries from the same baseline:

| Query | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `vector_query_nodes_match` | `0.97 ms` | `1.00 ms` | `1.11 ms` |
| `vector_query_nodes_yield_where` | `1.18 ms` | `1.32 ms` | `1.47 ms` |

## Runtime benchmark

Script:

- `scripts/benchmarks/benchmark_sqlite_runtime.py`

Supporting files:

- `scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`

### Runtime scope

This harness is for end-to-end runtime cost over the documented graph schema,
not just compilation. It compiles admitted Cypher queries, executes them on the
target backend, and records timing splits for each phase.

The runtime workload is predefined in Python rather than loaded from a JSON
corpus. It currently contains:

- `13` OLTP-style queries measured on SQLite
- `10` OLAP-style read queries measured on SQLite and DuckDB

The OLTP mix is intentionally broader than reads now. It covers:

- point and adjacency reads
- optional and bounded-traversal reads
- create operations
- update operations
- delete operations

For each backend it:

- creates a temporary file-backed database
- creates the graph tables
- creates the relevant indexes
- ingests a synthetic graph fixture into SQLite
- runs `ANALYZE` on the SQLite source after ingest so the planner has real statistics
- lets DuckDB attach and read that SQLite-ingested dataset for OLAP reads
- records setup timing for connect, schema, index, and ingest

For each query it records:

- CypherGlot compile latency
- backend execution latency
- end-to-end compile-plus-execute latency
- reset latency

Mutation-oriented OLTP queries run inside a SQLite savepoint and are rolled back
after each iteration, so `reset` now measures the rollback cost needed to keep
the seeded graph stable across repeated create, update, and delete samples.

### Runtime shape and scale

The runtime harness currently uses:

- `journal_mode=WAL` for SQLite
- `synchronous=NORMAL` for SQLite
- `foreign_keys=ON` for SQLite
- a file-backed SQLite source database plus a DuckDB reader over that SQLite data
- the current graph-to-table contract: `nodes`, `edges`, and `node_labels`

The default synthetic ingest scale is:

- `100000` users
- `1000` companies
- `100000` `WORKS_AT` edges
- `300000` `KNOWS` edges

The scale is configurable with:

- `--user-count`
- `--company-count`
- `--knows-edges-per-user`
- `--ingest-batch-size`

DuckDB is required for the default full OLAP comparison run. Pass
`--skip-duckdb` if you only want the SQLite portion.

DuckDB is not separately seeded anymore for the OLAP benchmark path. SQLite is
the single ingest path, and DuckDB reads the SQLite-ingested tables.

For SQLite specifically, the benchmark now treats post-ingest `ANALYZE` as part
of normal setup. Without planner statistics, selective graph reads can fall
into much worse edge-first plans and produce misleading execution timings.

### Runtime commands

From the repo root:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py
```

Useful overrides:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --iterations 1000 --warmup 10
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --output scripts/benchmarks/results/local-sqlite-runtime-benchmark-baseline.json
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --query-name oltp_user_point_lookup --query-name olap_user_count
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --skip-duckdb
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --user-count 1000 --company-count 25 --knows-edges-per-user 2 --ingest-batch-size 500
```

### Runtime output and baseline

The checked-in runtime baseline lives at
`scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`.

That output is workload-oriented rather than corpus-oriented. It records:

- graph scale metadata
- workload counts
- per-backend setup summaries
- per-backend pooled compile, execute, end-to-end, and reset summaries
- per-query timing summaries inside each workload/backend suite

Runtime timing summaries are emitted in milliseconds throughout the JSON output
and CLI summary. The CLI summary now prints mean, p50, p95, and p99 for the
pooled suites and for each query entry.

At the moment the runtime page should be read as documenting the current output
shape and workflow, not as promising a stable cross-machine performance claim.
The important comparison is repo-local regression tracking under the same setup.

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
