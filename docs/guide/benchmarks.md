# Benchmarks

CypherGlot now ships a benchmark harness for the public compiler stages and
entrypoints that matter for the current admitted subset, plus a comparable
SQLGlot workload for PostgreSQL-to-SQLite transpilation, plus a separate
end-to-end SQLite execution benchmark.

The goal is practical and explicit:

- measure stage-level latency for parsing, validation, and normalization
- measure end-to-end latency for `to_sqlglot_ast(...)` and `to_sql(...)`
- measure program-oriented compilation and rendering surfaces where CypherGlot
  actually supports them today
- include vector-aware queries where CypherGlot has a public contract today
- measure comparable SQLGlot PostgreSQL-to-SQLite work across tokenize, parse,
  parse-and-render, and transpile entrypoints
- record a baseline for future regression tracking
- measure realistic compile-plus-execute cost against the documented SQLite
  schema for representative admitted queries

## Scope

The current harness covers these public entrypoints:

- `parse_cypher_text(...)`
- `validate_cypher_text(...)`
- `normalize_cypher_text(...)`
- `to_sqlglot_ast(...)`
- `to_sql(...)`
- `to_sqlglot_program(...)`
- `render_cypher_program_text(...)`

It also benchmarks SQLGlot directly on a separate SQL corpus using:

- `tokenize(...)`
- `parse_one(...)`
- `parse_one(...).sql(dialect="sqlite")`
- `transpile(..., read="postgres", write="sqlite")`

The checked-in corpus intentionally mixes query families instead of benchmarking
only one read shape. It currently covers:

- `MATCH ... RETURN`
- narrow one-hop relationship reads
- narrow `OPTIONAL MATCH ... RETURN`
- narrow `OPTIONAL MATCH` grouped-count reads
- narrow `MATCH ... WITH ... RETURN`
- grouped aggregation
- bounded variable-length reads and grouped variable-length aggregation
- searched `CASE`
- graph-introspection projections
- metadata projections such as `properties(...)`, `labels(...)`, and `keys(...)`
- `UNWIND ... RETURN`
- standalone writes plus traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE`
  program shapes
- vector-aware `CALL db.index.vector.queryNodes(...)` normalization shapes

Vector-aware queries are benchmarked only through parse, validate, and normalize.
That matches the current product contract: CypherGlot carries vector intent
forward for host runtimes, but it does not compile vector-aware `CALL` queries
into SQLGlot-backed output directly.

The SQLGlot corpus is intentionally the same size as the CypherGlot corpus so
that the benchmark runs a comparable number of source queries per suite.

## Files

- benchmark script: `scripts/benchmarks/benchmark_compiler.py`
- compiler corpus: `scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- SQLGlot comparison corpus: `scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- latest checked-in compiler baseline: `scripts/benchmarks/results/compiler_benchmark_baseline.json`
- runtime benchmark script: `scripts/benchmarks/benchmark_sqlite_runtime.py`
- runtime corpus: `scripts/benchmarks/corpora/sqlite_runtime_benchmark_corpus.json`
- default runtime output: `scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`

## Run it

From the repo root:

```bash
uv run python scripts/benchmarks/benchmark_compiler.py
```

That default run currently uses:

- `1000` measured iterations per query and per entrypoint
- `10` warmup iterations per query and per entrypoint
- the installed SQLGlot package layout for the PostgreSQL-to-SQLite suite

Useful overrides:

```bash
uv run python scripts/benchmarks/benchmark_compiler.py --iterations 1000 --warmup 10
uv run python scripts/benchmarks/benchmark_compiler.py --output scripts/benchmarks/results/local-compiler-benchmark-baseline.json
uv run python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode both
uv run python scripts/benchmarks/benchmark_compiler.py --sqlglot-mode off
```

The end-to-end SQLite benchmark uses the documented schema contract, seeds a
small representative fixture per query, compiles each admitted Cypher query,
executes the resulting SQL or rendered program, and records both whole-batch
and per-query timing:

```bash
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --iterations 1000 --warmup 10
uv run python scripts/benchmarks/benchmark_sqlite_runtime.py --output scripts/benchmarks/results/local-sqlite-runtime-benchmark-baseline.json
```

The runtime harness now uses a temporary file-backed SQLite database with:

- `journal_mode=WAL`
- `synchronous=NORMAL`
- `foreign_keys=ON`

Use `--sqlglot-mode both` when you want to compare the installed `sqlglot[c]`
path against a temporary pure-Python SQLGlot import. `sqlglotc` does not expose
its own top-level runtime API; instead it installs compiled modules into the
`sqlglot` package namespace. The benchmark reports which module files were used
for each suite so you can confirm whether the run was compiled or pure Python.

## Current baseline

### Compiler Baseline

Source file: `scripts/benchmarks/results/compiler_benchmark_baseline.json`

The current checked-in compiler baseline is regenerated from the current
20-query corpus. It was produced with:

- compiler corpus: `scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- corpus size: `20` queries
- SQLGlot comparison corpus: `scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- SQLGlot corpus size: `20` queries
- warmup: `10` iterations per query and entrypoint
- measured iterations: `1000` per query and entrypoint
- SQLGlot mode: `both` (`sqlglotc`-backed compiled install plus pure-Python fallback)

Compiler entrypoint summary from the current full run:

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

### Runtime Baseline

Source file: `scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`

SQLite runtime summary from the current full run:

- runtime corpus: `scripts/benchmarks/corpora/sqlite_runtime_benchmark_corpus.json`
- corpus size: `9` queries
- warmup: `10` iterations per query and batch run
- measured iterations: `1000` per query and batch run
- SQLite profile: file-backed temporary database with `WAL` journal mode and `NORMAL` synchronous mode

| Runtime scope | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| full runtime batch | `13.52 ms` | `14.71 ms` | `15.82 ms` |

| Query | Mode | Fixture | p50 | p95 | p99 |
| --- | --- | --- | ---: | ---: | ---: |
| `simple_match_return` | `statement` | `basic_graph` | `1.13 ms` | `1.26 ms` | `1.33 ms` |
| `optional_match_missing` | `statement` | `basic_graph` | `1.25 ms` | `1.36 ms` | `1.42 ms` |
| `with_scalar_rebinding` | `statement` | `basic_graph` | `1.20 ms` | `1.26 ms` | `1.40 ms` |
| `grouped_count` | `statement` | `duplicate_name_graph` | `1.43 ms` | `1.51 ms` | `1.56 ms` |
| `bounded_variable_length` | `statement` | `user_chain_graph` | `2.18 ms` | `2.29 ms` | `2.38 ms` |
| `graph_introspection` | `statement` | `basic_graph` | `1.84 ms` | `2.05 ms` | `2.24 ms` |
| `match_set_node` | `statement` | `basic_graph` | `1.20 ms` | `1.24 ms` | `1.32 ms` |
| `delete_relationship` | `statement` | `basic_graph` | `1.35 ms` | `1.38 ms` | `1.43 ms` |
| `traversal_create_program` | `program` | `basic_graph` | `1.57 ms` | `1.61 ms` | `1.73 ms` |

The exact compiler baseline numbers are recorded in `scripts/benchmarks/results/compiler_benchmark_baseline.json`.
The exact runtime baseline numbers are recorded in `scripts/benchmarks/results/sqlite_runtime_benchmark_baseline.json`.
Those files now store overall summaries plus per-query summaries, so future runs can compare:

- stage-level changes
- end-to-end read entrypoint changes
- program-compilation changes
- vector-aware parse or normalize regressions
- SQLGlot tokenize, parse, render, and transpile changes
- end-to-end SQLite execution changes across representative runtime queries

After another corpus refresh, rerun `scripts/benchmarks/benchmark_compiler.py` to refresh
the checked-in baseline against the current corpus contents.

## Notes

- Percentiles are computed from raw per-iteration latency samples across the
  full corpus, using linear interpolation.
- The harness disables Python GC during the measured loop to reduce avoidable
  noise from collection pauses.
- Not every query applies to every entrypoint. The corpus explicitly declares
  which public entrypoints are valid for each query shape.
- The current baseline should be treated as a repository-local regression anchor,
  not as a cross-machine or cross-runtime performance claim.
- The pure-Python SQLGlot suite runs in a subprocess with a temporary package
  copy that excludes compiled `.so` modules, so the comparison does not mutate
  the active virtualenv.
