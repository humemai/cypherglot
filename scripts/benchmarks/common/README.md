# Common Benchmark Helpers

This directory holds shared code used by the benchmark entrypoints.

Contents:

- `shared.py`: cross-benchmark helpers for progress reporting, synthetic graph
  naming, RSS tracking, and summary formatting
- `cli.py`: shared argparse builders used by the leaf benchmark scripts
- `runtime_core.py`: shared SQL runtime benchmark orchestration for SQLite,
  DuckDB, and PostgreSQL entrypoints
- `runtime_sqlite_backend.py`, `runtime_duckdb_backend.py`,
  `runtime_postgresql_backend.py`: backend-specific SQL runtime execution
  helpers
- `runtime_shared.py`: shared runtime fixture and token helpers used by the
  runtime backends
- `postgres_runtime_support.py`: PostgreSQL-specific support code used by the
  runtime benchmark path

This code is not intended to be invoked directly. Entry points live in the
neighboring `compiler/`, `runtime/`, and `schema/` directories.