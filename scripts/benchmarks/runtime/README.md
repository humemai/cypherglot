# Runtime Benchmarks

This directory contains benchmark entrypoints for runtime execution against the
shared synthetic graph workload.

Contents:

- `sqlite.py`, `duckdb.py`, `postgresql.py`, `turso.py`, `clickhouse.py`:
  SQL-backed compile-plus-execute runtime benchmarks (Cypher lowered to SQL,
  then run).
  - Turso is the Rust SQLite rewrite (driver `pyturso`); it speaks SQLite's
    dialect, so it runs the same lowered SQL and answers "can the SQLite
    successor replace SQLite?"
  - ClickHouse (driver `clickhouse-connect`, columnar OLAP server) tests whether
    the columnar-OLAP traversal win generalizes beyond DuckDB. It is **scoped to
    reads/OLAP**: point updates/deletes are async, non-transactional mutations,
    so write queries are not tagged for it. It has no B-tree point index — the
    MergeTree `ORDER BY` key is the index — so there is no indexed/unindexed
    toggle. Standalone-node `OPTIONAL MATCH` lookups that lower to a
    `LEFT JOIN ... ON (1=1) AND <filter>` are excluded: ClickHouse rejects joins
    without a determinable equality key (`INVALID_JOIN_ON_EXPRESSION`).
- `neo4j.py`, `arcadedb_embedded.py`, `ladybug.py`: direct runtime benchmarks
  against non-SQL backends
- `age.py`: Apache AGE helpers — a native-openCypher **baseline** (PostgreSQL +
  the `age` extension). CypherGlot does **not** lower for AGE; AGE runs the
  Cypher directly via `cypher('graph', $$ ... $$) AS (col agtype, ...)`. Because
  CypherGlot also targets PostgreSQL, this is the cleanest head-to-head: native
  graph extension vs. lowered SQL on the same engine. The mandatory `AS (...)`
  column list is derived automatically from CypherGlot's own frontend. UNION
  queries are unsupported (recorded, not failed). Correctness is proven in
  `tests/test_cypher_conformance_age.py` (AGE == SQLite on a real AGE server).
- `matrix.py`: repeated fresh-process runtime benchmarking with worker queues
- `summarize_results.py`: Markdown summarizer for repeated runtime JSON outputs

Typical usage from the repo root:

```bash
python -m scripts.benchmarks.runtime.sqlite
python -m scripts.benchmarks.runtime.duckdb
python -m scripts.benchmarks.runtime.turso
# ClickHouse auto-provisions a disposable docker server on Linux; force it
# elsewhere with CYPHERGLOT_BENCHMARK_CLICKHOUSE_AUTO_DOCKER=1, or point at an
# existing server via CYPHERGLOT_BENCHMARK_CLICKHOUSE_HOST/PORT.
python -m scripts.benchmarks.runtime.clickhouse
python -m scripts.benchmarks.runtime.matrix --scale small --repeats 3 --workers 2
python -m scripts.benchmarks.runtime.summarize_results --no-queries
```

Output conventions:

- single-run runtime JSON baselines live in `scripts/benchmarks/results/runtime/`
- repeated-run manifests and per-job logs live in
  `scripts/benchmarks/results/runtime-matrix/`
- the current checked-in repeated-run summary lives as a Markdown artifact
  under `scripts/benchmarks/results/`

Use the matrix runner for repeated runs. Keep the leaf scripts focused on one
benchmark execution per invocation.
