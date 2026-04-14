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

Script:

- `scripts/benchmarks/benchmark_sqlite_runtime.py`

Supporting files:

- `scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`

### Runtime scope

This harness is for end-to-end runtime cost over the documented graph schema,
not just compilation. It compiles admitted Cypher queries, executes them on the
target backend, and records timing splits for each phase.

The runtime workload is now corpus-driven. The authoritative query catalog lives
in `scripts/benchmarks/corpora/sqlite_runtime_benchmark_corpus.json` and is
expanded against a generated type-aware schema at runtime.

The OLTP mix is intentionally broader than reads now. It covers:

- point and adjacency reads
- cross-type join reads
- multi-step write paths
- create operations
- update operations
- delete operations

The OLAP mix is intentionally broader than plain scan-and-aggregate reads. It
now covers:

- scan-and-order leaderboard reads
- grouped aggregates
- cross-type join aggregates
- bounded variable-length traversals
- fixed-length multi-hop projections
- graph introspection reads
- `WITH` rebinding plus aggregate rollups

For each backend it:

- generates a configurable multi-type type-aware graph schema
- creates a temporary file-backed SQLite database
- creates the graph tables
- benchmarks both query-driven indexed and unindexed SQLite layouts
- ingests a synthetic graph fixture into SQLite
- runs `ANALYZE` on the SQLite source after ingest so the planner has real statistics
- lets DuckDB attach and read that SQLite-ingested dataset for OLAP reads
- records setup timing for connect, schema, index, ingest, and analyze
- captures RSS snapshots across setup milestones
- records SQLite database and WAL sizes after ingest

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
- the current type-aware runtime contract generated from configurable node and
  edge type counts

The runtime benchmark is now aligned with the repository's type-aware migration
direction. It keeps the focus on end-to-end Cypher runtime cost, while the
separate schema-shape benchmark remains the place for broader storage-layout
comparisons.

The scale is configurable with:

- `--node-type-count`
- `--edge-type-count`
- `--nodes-per-type`
- `--edges-per-source`
- `--edge-degree-profile`
- `--node-extra-text-property-count`
- `--node-extra-numeric-property-count`
- `--node-extra-boolean-property-count`
- `--edge-extra-text-property-count`
- `--edge-extra-numeric-property-count`
- `--edge-extra-boolean-property-count`
- `--variable-hop-max`
- `--ingest-batch-size`

The generator supports both uniform and skewed out-degree profiles. In skewed
mode, most source nodes stay low-degree while a  tail becomes
high-connectivity, which is useful for surfacing supernode and p95/p99 latency
effects.

The benchmark keeps `--variable-hop-max` as the global traversal bound for the
ordinary variable-length queries, but the `olap_variable_length_grouped_rollup`
stress query is capped separately at `3` hops. That keeps one intentionally
hard OLAP shape in the corpus without letting medium and large full-suite runs
be dominated by a single grouped variable-length expansion.

The SQLite ingest path now streams bounded batches instead of materializing the
entire synthetic graph in memory before insertion. That keeps large runs
practical while preserving the same logical dataset shape.

DuckDB is required for the default full OLAP comparison run. Pass
`--skip-duckdb` if you only want the SQLite portion.

DuckDB is not separately seeded anymore for the OLAP benchmark path. SQLite is
the single ingest path, and DuckDB reads the SQLite-ingested tables.

For SQLite specifically, the benchmark now treats post-ingest `ANALYZE` as part
of normal setup. Without planner statistics, selective graph reads can fall
into much worse edge-first plans and produce misleading execution timings.

SQLite benchmarking defaults to `--index-mode both`, which produces separate
indexed and unindexed suites. Use `--index-mode indexed` or
`--index-mode unindexed` if you only want one layout.

### Runtime commands

Recommended small / medium / large full-runtime commands:

Small:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 4 \
  --edge-type-count 4 \
  --nodes-per-type 1000 \
  --edges-per-source 3 \
  --edge-degree-profile uniform \
  --node-extra-text-property-count 2 \
  --node-extra-numeric-property-count 6 \
  --node-extra-boolean-property-count 2 \
  --edge-extra-text-property-count 1 \
  --edge-extra-numeric-property-count 3 \
  --edge-extra-boolean-property-count 1 \
  --variable-hop-max 2 \
  --ingest-batch-size 1000 \
  --db-root-dir my_test_databases \
  --output scripts/benchmarks/results/runtime-small.json
```

Medium:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 6 \
  --edge-type-count 8 \
  --nodes-per-type 100000 \
  --edges-per-source 4 \
  --edge-degree-profile skewed \
  --node-extra-text-property-count 4 \
  --node-extra-numeric-property-count 10 \
  --node-extra-boolean-property-count 4 \
  --edge-extra-text-property-count 2 \
  --edge-extra-numeric-property-count 6 \
  --edge-extra-boolean-property-count 2 \
  --variable-hop-max 5 \
  --ingest-batch-size 5000 \
  --db-root-dir my_test_databases \
  --output scripts/benchmarks/results/runtime-medium.json
```

Large:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 10 \
  --edge-type-count 10 \
  --nodes-per-type 1000000 \
  --edges-per-source 8 \
  --edge-degree-profile skewed \
  --node-extra-text-property-count 8 \
  --node-extra-numeric-property-count 18 \
  --node-extra-boolean-property-count 8 \
  --edge-extra-text-property-count 4 \
  --edge-extra-numeric-property-count 10 \
  --edge-extra-boolean-property-count 4 \
  --variable-hop-max 8 \
  --ingest-batch-size 10000 \
  --db-root-dir my_test_databases \
  --output scripts/benchmarks/results/runtime-large.json
```

### Runtime output and baseline

The checked-in runtime baseline lives at
`scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`.

That output is workload-oriented but corpus-backed. It records:

- graph scale metadata
- graph topology metadata such as edge-degree profile and effective average fanout
- workload counts
- token substitutions used to bind the generated schema to the corpus
- per-backend setup summaries
- per-backend RSS snapshots
- per-backend SQLite storage summaries
- per-backend pooled compile, execute, end-to-end, and reset summaries
- per-query timing summaries inside each workload/backend suite

Runtime timing summaries are emitted in milliseconds throughout the JSON output
and CLI summary. The CLI summary now prints mean, p50, p95, and p99 for the
pooled suites and for each query entry.

If you need visibility into a long-running query while the benchmark is still
executing, pass `--iteration-progress` to either runtime harness. That prints
per-query warmup and measured iteration counters such as `warmup 3/5` and
`iteration 7/10`.

At the moment the runtime page should be read as documenting the current output
shape and workflow, not as promising a stable cross-machine performance claim.
The important comparison is repo-local regression tracking under the same setup.

### Small runtime dataset

The current small full-runtime run was executed with:

- `4` node types
- `4` edge types
- `1000` nodes per node type
- `3` outgoing edges per source node per edge type
- uniform out-degree profile
- `2` extra text properties, `6` extra numeric properties, and `2` extra boolean properties per node type
- `1` extra text property, `3` extra numeric properties, and `1` extra boolean property per edge type
- `2` maximum variable-hop depth
- `100` measured OLTP iterations with `5` warmup iterations
- `5` measured OLAP iterations with `1` warmup iteration

That corresponds to roughly:

- `4,000` total nodes
- `12,000` total edges

Result summary from `scripts/benchmarks/results/runtime-small.json`:

| Suite | Ingest | Analyze | RSS Ingest | Size | Compile p50 | Execute p50 | End-to-End p50 | End-to-End p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `oltp/sqlite_indexed` | `119.93 ms` | `6.19 ms` | `90.73 MiB` | `3.21 MiB` | `0.86 ms` | `0.04 ms` | `0.90 ms` | `0.92 ms` |
| `oltp/sqlite_unindexed` | `84.10 ms` | `0.21 ms` | `90.80 MiB` | `1.92 MiB` | `0.88 ms` | `0.26 ms` | `1.14 ms` | `1.20 ms` |
| `olap/sqlite_indexed` | `119.93 ms` | `6.19 ms` | `90.73 MiB` | `3.21 MiB` | `1.47 ms` | `1.90 ms` | `3.37 ms` | `3.54 ms` |
| `olap/sqlite_unindexed` | `84.10 ms` | `0.21 ms` | `90.80 MiB` | `1.92 MiB` | `1.41 ms` | `1.96 ms` | `3.38 ms` | `3.59 ms` |
| `olap/duckdb` | `attach 51.30 ms` | `n/a` | `115.78 MiB` | `3.21 MiB` | `4.62 ms` | `3.99 ms` | `8.83 ms` | `9.29 ms` |

For DuckDB, that `attach 51.30 ms` entry is the one-time OLAP session setup
cost to connect, load the SQLite extension, attach the SQLite fixture, and
create DuckDB views over the attached tables. The per-query DuckDB latency
columns below exclude that setup step.

Representative OLTP per-query latency from the same run:

| Query | Indexed Compile p50 | Indexed Execute p50 | Indexed End-to-End p50 | Indexed End-to-End p95 | Unindexed Compile p50 | Unindexed Execute p50 | Unindexed End-to-End p50 | Unindexed End-to-End p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `oltp_type1_point_lookup` | `0.83 ms` | `0.01 ms` | `0.84 ms` | `0.91 ms` | `0.82 ms` | `0.07 ms` | `0.89 ms` | `0.97 ms` |
| `oltp_type1_neighbors` | `0.88 ms` | `0.02 ms` | `0.90 ms` | `0.92 ms` | `0.89 ms` | `0.23 ms` | `1.13 ms` | `1.15 ms` |
| `oltp_cross_type_lookup` | `1.04 ms` | `0.02 ms` | `1.06 ms` | `1.09 ms` | `1.06 ms` | `0.23 ms` | `1.29 ms` | `1.31 ms` |
| `oltp_update_type1_score` | `0.62 ms` | `0.02 ms` | `0.64 ms` | `0.65 ms` | `0.62 ms` | `0.07 ms` | `0.70 ms` | `0.71 ms` |
| `oltp_create_type1_node` | `0.61 ms` | `0.03 ms` | `0.63 ms` | `0.65 ms` | `0.61 ms` | `0.02 ms` | `0.63 ms` | `0.64 ms` |
| `oltp_create_cross_type_edge` | `1.22 ms` | `0.03 ms` | `1.25 ms` | `1.27 ms` | `1.23 ms` | `0.14 ms` | `1.37 ms` | `1.44 ms` |
| `oltp_delete_type1_edge` | `0.65 ms` | `0.04 ms` | `0.69 ms` | `0.70 ms` | `0.66 ms` | `0.23 ms` | `0.89 ms` | `0.90 ms` |
| `oltp_delete_type1_node` | `0.43 ms` | `0.11 ms` | `0.54 ms` | `0.56 ms` | `0.45 ms` | `1.28 ms` | `1.74 ms` | `1.82 ms` |
| `oltp_program_create_and_link` | `1.51 ms` | `0.05 ms` | `1.56 ms` | `1.59 ms` | `1.58 ms` | `0.10 ms` | `1.69 ms` | `1.91 ms` |
| `oltp_update_cross_type_edge_rank` | `0.81 ms` | `0.04 ms` | `0.85 ms` | `0.89 ms` | `0.83 ms` | `0.24 ms` | `1.07 ms` | `1.15 ms` |

Representative OLAP per-query end-to-end latency from the same run:

| Query | SQLite Indexed p50 | SQLite Indexed p95 | SQLite Unindexed p50 | SQLite Unindexed p95 | DuckDB p50 | DuckDB p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `1.35 ms` | `1.52 ms` | `1.29 ms` | `1.41 ms` | `4.39 ms` | `4.58 ms` |
| `olap_type1_age_rollup` | `2.11 ms` | `2.46 ms` | `1.42 ms` | `1.54 ms` | `5.59 ms` | `5.62 ms` |
| `olap_cross_type_edge_rollup` | `3.49 ms` | `3.61 ms` | `2.34 ms` | `2.44 ms` | `7.66 ms` | `8.24 ms` |
| `olap_variable_length_reachability` | `1.49 ms` | `1.52 ms` | `3.27 ms` | `3.65 ms` | `10.41 ms` | `11.05 ms` |
| `olap_three_type_path_count` | `4.32 ms` | `4.76 ms` | `4.26 ms` | `4.71 ms` | `7.75 ms` | `7.98 ms` |
| `olap_type2_score_distribution` | `1.54 ms` | `1.73 ms` | `1.64 ms` | `1.99 ms` | `6.13 ms` | `7.51 ms` |
| `olap_fixed_length_path_projection` | `5.93 ms` | `6.03 ms` | `5.72 ms` | `5.76 ms` | `10.87 ms` | `10.98 ms` |
| `olap_graph_introspection_rollup` | `1.48 ms` | `1.53 ms` | `2.08 ms` | `2.15 ms` | `7.90 ms` | `8.31 ms` |
| `olap_with_scalar_rebinding` | `1.97 ms` | `2.02 ms` | `1.81 ms` | `1.84 ms` | `7.44 ms` | `8.01 ms` |
| `olap_variable_length_grouped_rollup` | `10.03 ms` | `10.17 ms` | `9.97 ms` | `10.45 ms` | `20.18 ms` | `20.64 ms` |

### Medium runtime dataset

The current medium full-runtime run was executed with:

- `6` node types
- `8` edge types
- `100000` nodes per node type
- `4` outgoing edges per source node per edge type
- skewed out-degree profile with effective average fanout `7.779`
- `4` extra text properties, `10` extra numeric properties, and `4` extra boolean properties per node type
- `2` extra text properties, `6` extra numeric properties, and `2` extra boolean properties per edge type
- `5` maximum variable-hop depth
- `100` measured OLTP iterations with `5` warmup iterations
- `5` measured OLAP iterations with `1` warmup iteration

That corresponds to roughly:

- `600,000` total nodes
- `6,223,200` total edges

Result summary from `scripts/benchmarks/results/runtime-medium.json`:

| Suite | Ingest | Analyze | RSS Ingest | Size | Compile p50 | Execute p50 | End-to-End p50 | End-to-End p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `oltp/sqlite_indexed` | `91,165.27 ms` | `3,199.75 ms` | `98.57 MiB` | `1,749.66 MiB` | `0.89 ms` | `0.08 ms` | `0.97 ms` | `1.08 ms` |
| `oltp/sqlite_unindexed` | `52,330.56 ms` | `279.39 ms` | `99.05 MiB` | `1,165.93 MiB` | `1.41 ms` | `120.07 ms` | `121.49 ms` | `125.43 ms` |
| `olap/sqlite_indexed` | `91,165.27 ms` | `3,199.75 ms` | `98.57 MiB` | `1,749.66 MiB` | `1.86 ms` | `14,565.03 ms` | `14,566.95 ms` | `14,667.89 ms` |
| `olap/sqlite_unindexed` | `52,330.56 ms` | `279.39 ms` | `99.05 MiB` | `1,165.93 MiB` | `2.08 ms` | `14,161.18 ms` | `14,163.25 ms` | `14,234.34 ms` |
| `olap/duckdb` | `attach 1,972.56 ms` | `n/a` | `117.70 MiB` | `1,749.66 MiB` | `5.01 ms` | `236.33 ms` | `241.35 ms` | `245.80 ms` |

For DuckDB, that `attach 1,972.56 ms` entry is the one-time OLAP session setup
cost to connect, load the SQLite extension, attach the SQLite fixture, and
create DuckDB views over the attached tables. The per-query DuckDB latency
columns below exclude that setup step.

Representative OLTP per-query latency from the same run:

| Query | Indexed Compile p50 | Indexed Execute p50 | Indexed End-to-End p50 | Indexed End-to-End p95 | Unindexed Compile p50 | Unindexed Execute p50 | Unindexed End-to-End p50 | Unindexed End-to-End p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `oltp_type1_point_lookup` | `0.86 ms` | `0.01 ms` | `0.87 ms` | `0.99 ms` | `1.35 ms` | `11.81 ms` | `13.17 ms` | `13.42 ms` |
| `oltp_type1_neighbors` | `0.91 ms` | `0.02 ms` | `0.93 ms` | `1.03 ms` | `1.63 ms` | `90.08 ms` | `91.71 ms` | `96.32 ms` |
| `oltp_cross_type_lookup` | `1.09 ms` | `0.02 ms` | `1.11 ms` | `1.47 ms` | `1.75 ms` | `88.49 ms` | `90.30 ms` | `94.81 ms` |
| `oltp_update_type1_score` | `0.64 ms` | `0.02 ms` | `0.67 ms` | `1.02 ms` | `1.14 ms` | `11.97 ms` | `13.10 ms` | `13.46 ms` |
| `oltp_create_type1_node` | `0.63 ms` | `0.03 ms` | `0.66 ms` | `0.69 ms` | `0.63 ms` | `0.02 ms` | `0.65 ms` | `0.66 ms` |
| `oltp_create_cross_type_edge` | `1.25 ms` | `0.03 ms` | `1.29 ms` | `1.35 ms` | `1.90 ms` | `24.33 ms` | `26.29 ms` | `27.95 ms` |
| `oltp_delete_type1_edge` | `0.67 ms` | `0.04 ms` | `0.71 ms` | `0.73 ms` | `1.27 ms` | `90.66 ms` | `91.96 ms` | `94.66 ms` |
| `oltp_delete_type1_node` | `0.46 ms` | `0.50 ms` | `0.96 ms` | `0.98 ms` | `0.96 ms` | `779.04 ms` | `779.99 ms` | `798.73 ms` |
| `oltp_program_create_and_link` | `1.55 ms` | `0.06 ms` | `1.60 ms` | `1.66 ms` | `2.00 ms` | `12.24 ms` | `14.25 ms` | `15.66 ms` |
| `oltp_update_cross_type_edge_rank` | `0.83 ms` | `0.04 ms` | `0.87 ms` | `0.88 ms` | `1.45 ms` | `92.03 ms` | `93.51 ms` | `98.62 ms` |

Representative OLAP per-query end-to-end latency from the same run:

| Query | SQLite Indexed p50 | SQLite Indexed p95 | SQLite Unindexed p50 | SQLite Unindexed p95 | DuckDB p50 | DuckDB p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `1.18 ms` | `1.33 ms` | `17.31 ms` | `17.79 ms` | `27.21 ms` | `27.95 ms` |
| `olap_type1_age_rollup` | `144.53 ms` | `156.84 ms` | `42.74 ms` | `43.16 ms` | `27.70 ms` | `27.97 ms` |
| `olap_cross_type_edge_rollup` | `1,420.15 ms` | `1,434.52 ms` | `363.59 ms` | `366.74 ms` | `96.85 ms` | `102.55 ms` |
| `olap_variable_length_reachability` | `2.75 ms` | `3.15 ms` | `4,476.34 ms` | `4,500.33 ms` | `943.30 ms` | `961.17 ms` |
| `olap_three_type_path_count` | `2,737.37 ms` | `2,743.31 ms` | `1,964.69 ms` | `1,969.14 ms` | `175.07 ms` | `186.94 ms` |
| `olap_type2_score_distribution` | `20.14 ms` | `20.43 ms` | `45.25 ms` | `46.03 ms` | `25.62 ms` | `26.56 ms` |
| `olap_fixed_length_path_projection` | `3,250.58 ms` | `3,266.84 ms` | `3,134.08 ms` | `3,246.84 ms` | `194.98 ms` | `197.29 ms` |
| `olap_graph_introspection_rollup` | `1.48 ms` | `1.54 ms` | `181.45 ms` | `182.79 ms` | `84.28 ms` | `84.80 ms` |
| `olap_with_scalar_rebinding` | `141.35 ms` | `145.76 ms` | `48.07 ms` | `49.47 ms` | `29.80 ms` | `30.95 ms` |
| `olap_variable_length_grouped_rollup` | `137,949.96 ms` | `138,905.14 ms` | `131,358.92 ms` | `131,921.13 ms` | `808.74 ms` | `811.83 ms` |

At this medium scale, the SQLite indexed OLTP path still stays close to
single-digit milliseconds end to end, but the unindexed OLTP path is no longer
in the same latency class. The OLAP suite summary is dominated by
`olap_variable_length_grouped_rollup`: most of the other indexed OLAP queries
remain in the `1 ms` to low-seconds range, while that one grouped variable-length
traversal lands around `131` to `139` seconds per measured execution.

### Large runtime dataset

The current large full-runtime run was executed with:

- `10` node types
- `10` edge types
- `1000000` nodes per node type
- `8` outgoing edges per source node per edge type
- skewed out-degree profile with effective average fanout `7.779`
- `8` extra text properties, `18` extra numeric properties, and `8` extra boolean properties per node type
- `4` extra text properties, `10` extra numeric properties, and `4` extra boolean properties per edge type
- `8` maximum variable-hop depth
- `100` measured OLTP iterations with `5` warmup iterations
- `5` measured OLAP iterations with `1` warmup iteration

That corresponds to roughly:

- `10,000,000` total nodes
- `77,790,000` total edges

Result summary from `scripts/benchmarks/results/runtime-large.json`:

| Suite | Ingest | Analyze | RSS Ingest | Size | Compile p50 | Execute p50 | End-to-End p50 | End-to-End p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `oltp/sqlite_indexed` | `2,523,162.99 ms` | `39,969.58 ms` | `161.88 MiB` | `32,676.91 MiB` | `0.89 ms` | `0.08 ms` | `0.97 ms` | `1.02 ms` |
| `oltp/sqlite_unindexed` | `1,331,835.37 ms` | `5,161.58 ms` | `158.66 MiB` | `24,665.96 MiB` | `1.47 ms` | `1,322.49 ms` | `1,323.98 ms` | `1,342.57 ms` |
| `olap/sqlite_indexed` | `2,523,162.99 ms` | `39,969.58 ms` | `161.88 MiB` | `32,676.91 MiB` | `2.18 ms` | `166,886.38 ms` | `166,888.63 ms` | `168,485.86 ms` |
| `olap/sqlite_unindexed` | `1,331,835.37 ms` | `5,161.58 ms` | `158.66 MiB` | `24,665.96 MiB` | `2.50 ms` | `165,199.82 ms` | `165,202.30 ms` | `169,128.83 ms` |
| `olap/duckdb` | `attach 12,622.02 ms` | `n/a` | `96.47 MiB` | `32,676.91 MiB` | `5.48 ms` | `1,136.15 ms` | `1,141.60 ms` | `1,225.66 ms` |

For DuckDB, that `attach 12,622.02 ms` entry is the one-time OLAP session setup
cost to connect, load the SQLite extension, attach the SQLite fixture, and
create DuckDB views over the attached tables. The per-query DuckDB latency
columns below exclude that setup step.

Representative OLAP per-query end-to-end latency from the same run:

| Query | SQLite Indexed p50 | SQLite Indexed p95 | SQLite Unindexed p50 | SQLite Unindexed p95 | DuckDB p50 | DuckDB p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `olap_type1_active_leaderboard` | `3.69 ms` | `5.26 ms` | `200.15 ms` | `200.78 ms` | `99.95 ms` | `111.81 ms` |
| `olap_type1_age_rollup` | `1,953.79 ms` | `1,990.67 ms` | `486.73 ms` | `487.44 ms` | `96.16 ms` | `103.38 ms` |
| `olap_cross_type_edge_rollup` | `18,194.89 ms` | `18,396.52 ms` | `4,401.65 ms` | `4,416.23 ms` | `731.87 ms` | `776.38 ms` |
| `olap_variable_length_reachability` | `4.89 ms` | `5.04 ms` | `131,908.86 ms` | `133,223.78 ms` | `skipped` | `skipped` |
| `olap_three_type_path_count` | `30,254.90 ms` | `30,315.65 ms` | `22,790.49 ms` | `23,042.84 ms` | `1,489.54 ms` | `1,632.91 ms` |
| `olap_type2_score_distribution` | `185.38 ms` | `188.30 ms` | `542.92 ms` | `555.79 ms` | `96.09 ms` | `124.51 ms` |
| `olap_fixed_length_path_projection` | `34,381.79 ms` | `34,477.16 ms` | `33,025.65 ms` | `33,296.15 ms` | `1,601.05 ms` | `1,834.47 ms` |
| `olap_graph_introspection_rollup` | `1.63 ms` | `1.86 ms` | `2,212.31 ms` | `2,247.29 ms` | `771.15 ms` | `887.02 ms` |
| `olap_with_scalar_rebinding` | `1,970.58 ms` | `1,980.38 ms` | `574.09 ms` | `581.79 ms` | `110.04 ms` | `153.95 ms` |
| `olap_variable_length_grouped_rollup` | `1,581,934.76 ms` | `1,597,497.77 ms` | `1,455,880.15 ms` | `1,493,236.19 ms` | `5,278.55 ms` | `5,406.54 ms` |

At this large scale, the benchmark splits much more sharply by workload family:

- SQLite indexed OLTP still stays around `1 ms` end to end, while the
  unindexed OLTP path moves into multi-second territory for the broader suite.
- The SQLite OLAP suite is dominated by the fixed-length and grouped
  variable-length path shapes, with suite-level pooled p50 around `165` to
  `167` seconds.
- DuckDB remains far stronger for the broad analytical joins, grouped rollups,
  and fixed-length path projections over the attached SQLite fixture, but the
  current attached-SQLite DuckDB path now skips
  `olap_variable_length_reachability` at this scale because that selective
  bounded variable-length traversal is not a good fit for the current DuckDB
  benchmark architecture.

### Neo4j small comparison run

The repository also includes
`scripts/benchmarks/benchmark_neo4j_runtime.py`, which runs the same runtime
corpus directly against Neo4j Community Edition using the same synthetic graph
shape and query-index toggle. This is useful for Cypher compatibility and
direct-engine runtime checks, but it is not identical to the SQLite runtime
harness: the Neo4j path does not measure CypherGlot compile latency because it
sends the corpus Cypher text directly to Neo4j.

A comparable small Neo4j run used:

```bash
uv run python scripts/benchmarks/benchmark_neo4j_runtime.py \
  --docker \
  --neo4j-password benchmark-pass \
  --docker-bolt-port 17689 \
  --docker-http-port 17476 \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 4 \
  --edge-type-count 4 \
  --nodes-per-type 1000 \
  --edges-per-source 3 \
  --edge-degree-profile uniform \
  --node-extra-text-property-count 2 \
  --node-extra-numeric-property-count 6 \
  --node-extra-boolean-property-count 2 \
  --edge-extra-text-property-count 1 \
  --edge-extra-numeric-property-count 3 \
  --edge-extra-boolean-property-count 1 \
  --variable-hop-max 2 \
  --ingest-batch-size 1000 \
  --output scripts/benchmarks/results/runtime-small-neo4j.json
```

Comparable medium and large Neo4j commands use the same shape as the SQLite
examples, with the Neo4j-specific Docker and authentication flags added:

Medium:

```bash
uv run python scripts/benchmarks/benchmark_neo4j_runtime.py \
  --docker \
  --neo4j-password benchmark-pass \
  --docker-bolt-port 17689 \
  --docker-http-port 17476 \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 6 \
  --edge-type-count 8 \
  --nodes-per-type 100000 \
  --edges-per-source 4 \
  --edge-degree-profile skewed \
  --node-extra-text-property-count 4 \
  --node-extra-numeric-property-count 10 \
  --node-extra-boolean-property-count 4 \
  --edge-extra-text-property-count 2 \
  --edge-extra-numeric-property-count 6 \
  --edge-extra-boolean-property-count 2 \
  --variable-hop-max 5 \
  --ingest-batch-size 5000 \
  --output scripts/benchmarks/results/runtime-medium-neo4j.json
```

Large:

```bash
uv run python scripts/benchmarks/benchmark_neo4j_runtime.py \
  --docker \
  --neo4j-password benchmark-pass \
  --docker-bolt-port 17690 \
  --docker-http-port 17477 \
  --iteration-progress \
  --iterations 100 \
  --warmup 5 \
  --olap-iterations 5 \
  --olap-warmup 1 \
  --index-mode both \
  --node-type-count 10 \
  --edge-type-count 10 \
  --nodes-per-type 1000000 \
  --edges-per-source 8 \
  --edge-degree-profile skewed \
  --node-extra-text-property-count 8 \
  --node-extra-numeric-property-count 18 \
  --node-extra-boolean-property-count 8 \
  --edge-extra-text-property-count 4 \
  --edge-extra-numeric-property-count 10 \
  --edge-extra-boolean-property-count 4 \
  --variable-hop-max 8 \
  --ingest-batch-size 10000 \
  --output scripts/benchmarks/results/runtime-large-neo4j.json
```

If another local Neo4j instance is already using those ports, change
`--docker-bolt-port` and `--docker-http-port` to a free pair before running.

At the moment, only the small Neo4j result artifact is checked in. The medium
and large commands above are included so the doc stays reproducible once those
runs finish.

That run used the same logical small dataset shape as the SQLite command above:

- `4` node types
- `4` edge types
- `1000` nodes per node type
- `3` outgoing edges per source node per edge type
- uniform out-degree profile
- `2` maximum variable-hop depth
- `100` measured OLTP iterations with `5` warmup iterations
- `5` measured OLAP iterations with `1` warmup iteration

The resulting Neo4j run passed all `40` indexed and unindexed query executions
across the `10`-query OLTP corpus and `10`-query OLAP corpus, with
`failure_count = 0` in
`scripts/benchmarks/results/runtime-small-neo4j.json`.

Comparable suite summary:

| Suite | Setup | End-to-End p50 | End-to-End p95 |
| --- | ---: | ---: | ---: |
| `oltp/sqlite_indexed` | ingest `119.93 ms`, analyze `6.19 ms` | `0.90 ms` | `0.92 ms` |
| `oltp/sqlite_unindexed` | ingest `84.10 ms`, analyze `0.21 ms` | `1.14 ms` | `1.20 ms` |
| `oltp/neo4j_indexed` | connect `58.16 ms`, reset `775.03 ms`, constraints `352.99 ms`, ingest `1695.26 ms`, index `820.43 ms` | `0.51 ms` | `0.81 ms` |
| `oltp/neo4j_unindexed` | connect `58.16 ms`, reset `542.60 ms`, constraints `43.37 ms`, ingest `1041.82 ms` | `0.45 ms` | `0.65 ms` |
| `olap/sqlite_indexed` | ingest `119.93 ms`, analyze `6.19 ms` | `3.37 ms` | `3.54 ms` |
| `olap/sqlite_unindexed` | ingest `84.10 ms`, analyze `0.21 ms` | `3.38 ms` | `3.59 ms` |
| `olap/duckdb` | attach `51.30 ms` on the SQLite-ingested dataset | `8.83 ms` | `9.29 ms` |
| `olap/neo4j_indexed` | connect `58.16 ms`, reset `775.03 ms`, constraints `352.99 ms`, ingest `1695.26 ms`, index `820.43 ms` | `3.88 ms` | `5.00 ms` |
| `olap/neo4j_unindexed` | connect `58.16 ms`, reset `542.60 ms`, constraints `43.37 ms`, ingest `1041.82 ms` | `2.83 ms` | `3.43 ms` |

On this small dataset, Neo4j setup cost is substantially higher than the
SQLite path, especially when the disposable Docker server startup and index
creation cost are included. Query latency lands in the same general band,
though the comparison should be read carefully:

- SQLite runtime numbers are compile-plus-execute end-to-end timings through
  CypherGlot.
- Neo4j runtime numbers are direct Cypher execution timings against Neo4j.
- The Neo4j comparison is therefore best read as a compatibility and
  direct-engine runtime check, not as a strict apples-to-apples compiler
  benchmark.

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
