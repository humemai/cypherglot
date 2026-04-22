# Runtime Benchmarks

This directory contains benchmark entrypoints for runtime execution against the
shared synthetic graph workload.

Contents:

- `sqlite.py`, `duckdb.py`, `postgresql.py`: SQL-backed compile-plus-execute
  runtime benchmarks
- `neo4j.py`, `arcadedb_embedded.py`, `ladybug.py`: direct runtime benchmarks
  against non-SQL backends
- `matrix.py`: repeated fresh-process runtime benchmarking with worker queues
- `summarize_results.py`: Markdown summarizer for repeated runtime JSON outputs

Typical usage from the repo root:

```bash
python -m scripts.benchmarks.runtime.sqlite
python -m scripts.benchmarks.runtime.duckdb
python -m scripts.benchmarks.runtime.matrix --scale small --repeats 3 --workers 2
python -m scripts.benchmarks.runtime.summarize_results --no-queries
```

Output conventions:

- single-run runtime JSON baselines live in `scripts/benchmarks/results/runtime/`
- repeated-run manifests and per-job logs live in
  `scripts/benchmarks/results/runtime-matrix/`

Use the matrix runner for repeated runs. Keep the leaf scripts focused on one
benchmark execution per invocation.