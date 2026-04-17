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

In addition to those public entrypoints, the harness also records backend-aware
compiler pipeline timings for SQLite, DuckDB, and PostgreSQL across these
stages:

- IR build
- backend bind
- backend lower
- rendered-program emission
- backend-specific end-to-end raw Cypher to rendered target SQL/program text

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
- both the installed and pure-Python SQLGlot package layouts for the
  PostgreSQL-to-SQLite comparison

### Compiler output and baseline

The checked-in compiler baseline lives at
`scripts/benchmarks/results/compiler_benchmark_baseline.json`.

That baseline records:

- stage-level latency summaries
- per-entrypoint summaries
- per-query summaries
- backend-aware lowering and end-to-end summaries for SQLite, DuckDB, and PostgreSQL
- vector-only normalization summaries
- SQLGlot comparison results for compiled and pure-Python installs when enabled

The current checked-in baseline was produced from a `22`-query CypherGlot
compiler corpus and a matching `22`-query SQLGlot comparison corpus.

Compiler entrypoint summary from the current checked-in run:

| Entrypoint | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `parse_cypher_text(...)` | `0.54 ms` | `0.87 ms` | `0.99 ms` |
| `validate_cypher_text(...)` | `0.64 ms` | `1.00 ms` | `1.08 ms` |
| `normalize_cypher_text(...)` | `0.70 ms` | `1.12 ms` | `1.20 ms` |
| `to_sqlglot_ast(...)` | `0.92 ms` | `1.26 ms` | `1.31 ms` |
| `to_sql(...)` | `1.03 ms` | `1.38 ms` | `1.44 ms` |
| `to_sqlglot_program(...)` | `0.84 ms` | `1.23 ms` | `1.27 ms` |
| `render_cypher_program_text(...)` | `0.95 ms` | `1.34 ms` | `1.38 ms` |

SQLGlot PostgreSQL-to-SQLite comparison summary from the same run:

| Implementation | Method | Queries | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| compiled (`sqlglotc`) | `tokenize(...)` | 22 | `12.59 us` | `29.34 us` | `32.90 us` |
| compiled (`sqlglotc`) | `parse_one(...)` | 22 | `33.50 us` | `87.42 us` | `99.38 us` |
| compiled (`sqlglotc`) | `parse_one(...).sql(...)` | 22 | `95.66 us` | `245.78 us` | `289.36 us` |
| compiled (`sqlglotc`) | `transpile(...)` | 22 | `58.27 us` | `141.88 us` | `156.76 us` |
| pure Python | `tokenize(...)` | 22 | `45.72 us` | `122.88 us` | `155.53 us` |
| pure Python | `parse_one(...)` | 22 | `121.56 us` | `314.93 us` | `375.29 us` |
| pure Python | `parse_one(...).sql(...)` | 22 | `217.20 us` | `539.22 us` | `641.75 us` |
| pure Python | `transpile(...)` | 22 | `169.12 us` | `408.76 us` | `474.00 us` |

Vector-aware normalization queries from the same baseline:

| Query | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `vector_query_nodes_match` | `0.98 ms` | `1.03 ms` | `1.60 ms` |
| `vector_query_nodes_yield_where` | `1.15 ms` | `1.26 ms` | `1.49 ms` |

## Runtime benchmark

This benchmark measures end-to-end runtime over the current type-aware graph
contract. The workload is corpus-driven from
`scripts/benchmarks/corpora/sqlite_runtime_benchmark_corpus.json` and split
into two families:

- OLTP: point reads, adjacency reads, cross-type reads, and mutation paths
- OLAP: leaderboard scans, grouped aggregates, multi-hop projections,
  introspection queries, and `WITH`-based rollups

Read the backends this way:

- SQLite suites are compile-plus-execute timings through CypherGlot, reported
  for indexed and unindexed layouts.
- DuckDB is OLAP-only and reads the SQLite-ingested dataset through the SQLite
  extension.
- Neo4j runs the same Cypher corpus directly, so its timings are direct
  execution timings rather than compiler-plus-runtime timings.

Not every suite is present at every scale. The medium and large Neo4j runs omit
`olap/neo4j_unindexed`, and the large DuckDB run skips
`olap_variable_length_reachability`.

### Runtime shape and output

Common harness behavior:

- generated type-aware schema with configurable type counts, property counts,
  and degree profile
- streamed ingest batches so large runs do not materialize the full graph in
  memory first
- SQLite source database with `journal_mode=WAL`, `synchronous=NORMAL`, and
  `foreign_keys=ON`
- post-ingest `ANALYZE` for SQLite so planner statistics are available
- global `--variable-hop-max`, with
  `olap_variable_length_grouped_rollup` capped separately at `3` hops
- JSON output with setup timings, RSS snapshots, storage sizes, pooled
  workload summaries, and per-query timings

Local result artifacts used below:

- SQLite and DuckDB: `runtime-small.json`, `runtime-medium.json`,
  `runtime-large.json`
- Neo4j: `runtime-small-neo4j.json`, `runtime-medium-neo4j.json`,
  `runtime-large-neo4j.json`

The checked-in SQLite baseline remains
`scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`, but the
scale sections below reflect the current local comparison artifacts.

### Runtime commands

SQLite and DuckDB:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  <scale flags> \
  --db-root-dir my_test_databases \
  --output scripts/benchmarks/results/runtime-<scale>.json
```

Neo4j:

```bash
uv run python scripts/benchmarks/benchmark_neo4j_runtime.py \
  --docker \
  --neo4j-password benchmark-pass \
  --docker-bolt-port <bolt-port> \
  --docker-http-port <http-port> \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  <scale flags> \
  --output scripts/benchmarks/results/runtime-<scale>-neo4j.json
```

Scale presets:

| Scale | Shape | Extra properties | Traversal | Batch |
| --- | --- | --- | --- | ---: |
| small | `4` node types, `4` edge types, `1000` nodes per type, `3` edges per source, `uniform` degree | node: `2 text`, `6 numeric`, `2 boolean`; edge: `1 text`, `3 numeric`, `1 boolean` | `--variable-hop-max 2` | `1000` |
| medium | `6` node types, `8` edge types, `100000` nodes per type, `4` edges per source, `skewed` degree | node: `4 text`, `10 numeric`, `4 boolean`; edge: `2 text`, `6 numeric`, `2 boolean` | `--variable-hop-max 5` | `5000` |
| large | `10` node types, `10` edge types, `1000000` nodes per type, `8` edges per source, `skewed` degree | node: `8 text`, `18 numeric`, `8 boolean`; edge: `4 text`, `10 numeric`, `4 boolean` | `--variable-hop-max 8` | `10000` |

If another local Neo4j instance is already using those ports, change the bolt
and HTTP ports to a free pair before running.

All tables below use pooled suite-level end-to-end `p50` and `p95`. SQLite and
DuckDB include compilation; Neo4j does not.

### Small dataset

Shape: roughly `4,000` nodes and `12,000` edges.

| Suite | p50 | p95 |
| --- | ---: | ---: |
| `oltp/sqlite_indexed` | `0.90 ms` | `0.92 ms` |
| `oltp/sqlite_unindexed` | `1.14 ms` | `1.20 ms` |
| `oltp/neo4j_indexed` | `0.51 ms` | `0.81 ms` |
| `oltp/neo4j_unindexed` | `0.45 ms` | `0.65 ms` |
| `olap/sqlite_indexed` | `3.37 ms` | `3.54 ms` |
| `olap/sqlite_unindexed` | `3.38 ms` | `3.59 ms` |
| `olap/duckdb` | `8.83 ms` | `9.29 ms` |
| `olap/neo4j_indexed` | `3.88 ms` | `5.00 ms` |
| `olap/neo4j_unindexed` | `2.83 ms` | `3.43 ms` |

At this scale, all engines stay in the same low-millisecond band on query
latency. The main difference is setup cost: SQLite finishes ingest quickly,
DuckDB adds only a small attach cost, and Neo4j pays noticeably more one-time
work for disposable database reset, ingest, and index creation.

OLTP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: |
| `oltp_type1_point_lookup` | `0.88 ms` | `0.99 ms` | `0.78 ms` | `0.46 ms` |
| `oltp_type1_neighbors` | `0.96 ms` | `1.18 ms` | `0.69 ms` | `0.51 ms` |
| `oltp_cross_type_lookup` | `1.12 ms` | `1.36 ms` | `0.54 ms` | `0.45 ms` |
| `oltp_update_type1_score` | `0.68 ms` | `0.72 ms` | `0.46 ms` | `0.42 ms` |
| `oltp_create_type1_node` | `0.69 ms` | `0.64 ms` | `0.46 ms` | `0.31 ms` |
| `oltp_create_cross_type_edge` | `1.35 ms` | `1.40 ms` | `0.55 ms` | `0.64 ms` |
| `oltp_delete_type1_edge` | `0.75 ms` | `0.94 ms` | `0.42 ms` | `0.48 ms` |
| `oltp_delete_type1_node` | `0.61 ms` | `1.78 ms` | `0.42 ms` | `0.42 ms` |
| `oltp_program_create_and_link` | `1.65 ms` | `1.63 ms` | `0.39 ms` | `0.43 ms` |
| `oltp_update_cross_type_edge_rank` | `0.92 ms` | `1.06 ms` | `0.37 ms` | `0.40 ms` |

OLAP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `1.10 ms` | `1.31 ms` | `4.47 ms` | `3.45 ms` | `1.49 ms` |
| `olap_type1_age_rollup` | `1.50 ms` | `1.45 ms` | `5.42 ms` | `1.81 ms` | `0.90 ms` |
| `olap_cross_type_edge_rollup` | `2.97 ms` | `2.32 ms` | `7.40 ms` | `5.04 ms` | `3.01 ms` |
| `olap_variable_length_reachability` | `1.65 ms` | `3.05 ms` | `10.49 ms` | `0.76 ms` | `0.57 ms` |
| `olap_three_type_path_count` | `4.42 ms` | `4.27 ms` | `8.11 ms` | `3.02 ms` | `1.90 ms` |
| `olap_type2_score_distribution` | `1.39 ms` | `1.58 ms` | `4.82 ms` | `1.06 ms` | `0.92 ms` |
| `olap_fixed_length_path_projection` | `6.07 ms` | `5.82 ms` | `10.75 ms` | `5.45 ms` | `5.17 ms` |
| `olap_graph_introspection_rollup` | `1.71 ms` | `2.14 ms` | `7.47 ms` | `3.91 ms` | `3.36 ms` |
| `olap_with_scalar_rebinding` | `2.12 ms` | `1.90 ms` | `6.86 ms` | `1.23 ms` | `1.00 ms` |
| `olap_variable_length_grouped_rollup` | `9.84 ms` | `9.40 ms` | `14.32 ms` | `13.12 ms` | `9.98 ms` |

### Medium dataset

Shape: roughly `600,000` nodes and `6,223,200` edges.

| Suite | p50 | p95 |
| --- | ---: | ---: |
| `oltp/sqlite_indexed` | `0.97 ms` | `1.08 ms` |
| `oltp/sqlite_unindexed` | `121.49 ms` | `125.43 ms` |
| `oltp/neo4j_indexed` | `0.37 ms` | `0.53 ms` |
| `oltp/neo4j_unindexed` | `86.56 ms` | `98.72 ms` |
| `olap/sqlite_indexed` | `14,566.95 ms` | `14,667.89 ms` |
| `olap/sqlite_unindexed` | `14,163.25 ms` | `14,234.34 ms` |
| `olap/duckdb` | `241.35 ms` | `245.80 ms` |
| `olap/neo4j_indexed` | `15,664.60 ms` | `15,785.09 ms` |
| `olap/neo4j_unindexed` | `not run` | `not run` |

The split is much clearer here. Indexed OLTP stays near `1 ms` on SQLite and
below that on direct Neo4j execution, while unindexed OLTP jumps into the
double-digit to hundreds-of-milliseconds range. For OLAP, DuckDB moves into a
different performance tier, while both SQLite and Neo4j are dominated by the
same harder path and grouped-rollup shapes.

OLTP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: |
| `oltp_type1_point_lookup` | `0.88 ms` | `14.52 ms` | `0.38 ms` | `22.78 ms` |
| `oltp_type1_neighbors` | `0.93 ms` | `89.29 ms` | `0.41 ms` | `258.74 ms` |
| `oltp_cross_type_lookup` | `1.10 ms` | `89.06 ms` | `0.38 ms` | `419.30 ms` |
| `oltp_update_type1_score` | `0.68 ms` | `14.08 ms` | `0.36 ms` | `24.07 ms` |
| `oltp_create_type1_node` | `0.66 ms` | `0.64 ms` | `0.32 ms` | `0.22 ms` |
| `oltp_create_cross_type_edge` | `1.28 ms` | `26.74 ms` | `0.41 ms` | `46.31 ms` |
| `oltp_delete_type1_edge` | `0.72 ms` | `91.09 ms` | `0.33 ms` | `22.60 ms` |
| `oltp_delete_type1_node` | `0.95 ms` | `781.93 ms` | `0.43 ms` | `24.84 ms` |
| `oltp_program_create_and_link` | `1.67 ms` | `15.42 ms` | `0.35 ms` | `22.54 ms` |
| `oltp_update_cross_type_edge_rank` | `0.90 ms` | `94.01 ms` | `0.31 ms` | `24.19 ms` |

OLAP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `1.18 ms` | `17.71 ms` | `26.99 ms` | `66.62 ms` | `n/a` |
| `olap_type1_age_rollup` | `149.84 ms` | `45.44 ms` | `27.59 ms` | `77.04 ms` | `n/a` |
| `olap_cross_type_edge_rollup` | `1462.29 ms` | `371.88 ms` | `94.79 ms` | `594.07 ms` | `n/a` |
| `olap_variable_length_reachability` | `3.09 ms` | `4621.28 ms` | `920.06 ms` | `1.52 ms` | `n/a` |
| `olap_three_type_path_count` | `2810.15 ms` | `2020.06 ms` | `167.24 ms` | `560.71 ms` | `n/a` |
| `olap_type2_score_distribution` | `20.05 ms` | `46.30 ms` | `26.25 ms` | `64.04 ms` | `n/a` |
| `olap_fixed_length_path_projection` | `3330.48 ms` | `3003.61 ms` | `192.15 ms` | `1471.02 ms` | `n/a` |
| `olap_graph_introspection_rollup` | `1.82 ms` | `187.56 ms` | `95.80 ms` | `534.31 ms` | `n/a` |
| `olap_with_scalar_rebinding` | `144.57 ms` | `50.13 ms` | `30.25 ms` | `83.01 ms` | `n/a` |
| `olap_variable_length_grouped_rollup` | `137169.19 ms` | `132075.21 ms` | `792.34 ms` | `153193.65 ms` | `n/a` |

### Large dataset

Shape: roughly `10,000,000` nodes and `77,790,000` edges.

| Suite | p50 | p95 |
| --- | ---: | ---: |
| `oltp/sqlite_indexed` | `0.97 ms` | `1.02 ms` |
| `oltp/sqlite_unindexed` | `1,323.98 ms` | `1,342.57 ms` |
| `oltp/neo4j_indexed` | `0.32 ms` | `0.51 ms` |
| `oltp/neo4j_unindexed` | `1,467.51 ms` | `1,586.33 ms` |
| `olap/sqlite_indexed` | `166,888.63 ms` | `168,485.86 ms` |
| `olap/sqlite_unindexed` | `165,202.30 ms` | `169,128.83 ms` |
| `olap/duckdb` | `1,141.60 ms` | `1,225.66 ms` |
| `olap/neo4j_indexed` | `291,004.70 ms` | `300,428.69 ms` |
| `olap/neo4j_unindexed` | `not run` | `not run` |

At the large preset, workload family matters much more than engine branding.
Indexed OLTP still stays around `1 ms` on SQLite and below that on direct
Neo4j execution, while unindexed OLTP moves into multi-second territory for the
broader suite. OLAP is dominated by large path and grouped variable-length
expansions: DuckDB remains far stronger for the analytical suite, while the
current DuckDB run skips `olap_variable_length_reachability` and the Neo4j run
does not include `olap/neo4j_unindexed`.

OLTP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: |
| `oltp_type1_point_lookup` | `0.85 ms` | `175.00 ms` | `0.34 ms` | `337.29 ms` |
| `oltp_type1_neighbors` | `0.92 ms` | `1100.91 ms` | `0.33 ms` | `3023.03 ms` |
| `oltp_cross_type_lookup` | `1.10 ms` | `1092.96 ms` | `0.38 ms` | `8903.84 ms` |
| `oltp_update_type1_score` | `0.68 ms` | `177.14 ms` | `0.29 ms` | `347.21 ms` |
| `oltp_create_type1_node` | `0.67 ms` | `0.64 ms` | `0.28 ms` | `0.29 ms` |
| `oltp_create_cross_type_edge` | `1.30 ms` | `348.52 ms` | `0.37 ms` | `689.74 ms` |
| `oltp_delete_type1_edge` | `0.72 ms` | `1136.78 ms` | `0.28 ms` | `335.39 ms` |
| `oltp_delete_type1_node` | `0.94 ms` | `7868.18 ms` | `0.38 ms` | `353.68 ms` |
| `oltp_program_create_and_link` | `1.61 ms` | `186.91 ms` | `0.28 ms` | `335.18 ms` |
| `oltp_update_cross_type_edge_rank` | `0.88 ms` | `1152.76 ms` | `0.26 ms` | `349.46 ms` |

OLAP query breakdown, end-to-end `p50`:

| Query | SQLite Indexed | SQLite Unindexed | DuckDB | Neo4j Indexed | Neo4j Unindexed |
| --- | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `3.69 ms` | `200.15 ms` | `99.95 ms` | `1257.72 ms` | `n/a` |
| `olap_type1_age_rollup` | `1953.79 ms` | `486.73 ms` | `96.16 ms` | `1552.36 ms` | `n/a` |
| `olap_cross_type_edge_rollup` | `18194.89 ms` | `4401.65 ms` | `731.87 ms` | `11335.02 ms` | `n/a` |
| `olap_variable_length_reachability` | `4.89 ms` | `131908.86 ms` | `n/a` | `1.90 ms` | `n/a` |
| `olap_three_type_path_count` | `30254.90 ms` | `22790.49 ms` | `1489.54 ms` | `5277.33 ms` | `n/a` |
| `olap_type2_score_distribution` | `185.38 ms` | `542.92 ms` | `96.09 ms` | `1356.01 ms` | `n/a` |
| `olap_fixed_length_path_projection` | `34381.79 ms` | `33025.65 ms` | `1601.05 ms` | `14813.58 ms` | `n/a` |
| `olap_graph_introspection_rollup` | `1.63 ms` | `2212.31 ms` | `771.15 ms` | `7534.76 ms` | `n/a` |
| `olap_with_scalar_rebinding` | `1970.58 ms` | `574.09 ms` | `110.04 ms` | `1530.02 ms` | `n/a` |
| `olap_variable_length_grouped_rollup` | `1581934.76 ms` | `1455880.15 ms` | `5278.55 ms` | `2865388.33 ms` | `n/a` |

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
