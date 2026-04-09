# Benchmarks

CypherGlot has two benchmark entrypoints, and they answer two different
questions:

- `scripts/benchmarks/benchmark_compiler.py` measures compiler-stage and
  compiler-entrypoint latency.
- `scripts/benchmarks/benchmark_sqlite_runtime.py` measures compile-plus-execute
  runtime cost over the graph-to-table schema contract.

This page documents them separately so each script has its own scope, inputs,
commands, and output model.

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
