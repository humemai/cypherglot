# Benchmark Scripts

This directory contains the benchmark tooling used to evaluate CypherGlot's
schema design, compiler behavior, and backend runtime behavior.

The benchmark area is now organized by domain:

- `common/`: shared helpers used by the benchmark entrypoints
- `compiler/`: compiler-only benchmark entrypoint and summarizer
- `runtime/`: backend runtime benchmarks, repeated-run matrix runner, and
  runtime summarizer
- `schema/`: schema-shape benchmark, repeated-run matrix runner, and schema
  summarizer
- `corpora/`: checked-in benchmark query corpora
- `results/`: generated JSON and Markdown benchmark artifacts

Important current entrypoints:

- `compiler/benchmark.py`: compiler-only latency benchmark
- `compiler/summarize_results.py`: compiler benchmark Markdown summarizer
- `schema/sqlite_shapes.py`: schema-shape benchmark used to justify the
  default graph-to-table layout
- `runtime/sqlite.py`, `runtime/duckdb.py`, `runtime/postgresql.py`:
  SQL-backed runtime benchmarks
- `runtime/neo4j.py`, `runtime/arcadedb_embedded.py`, `runtime/ladybug.py`:
  non-SQL runtime benchmarks
- `runtime/matrix.py`: repeated runtime benchmarking with parallel workers
- `schema/matrix.py`: repeated schema benchmarking with parallel workers
- `runtime/summarize_results.py`: runtime repeat summarizer
- `schema/summarize_results.py`: schema repeat summarizer

Typical workflow:

- run a single benchmark entrypoint when you need one JSON result
- use the compiler summarizer to turn compiler JSON output into Markdown when
  you want a human-readable report
- use the matrix runners for repeated fresh-process runs
- feed repeated JSON outputs into the matching summarizer to produce Markdown
  tables
- keep corpora and helper code stable so single-run scripts stay narrow and
  easy to compare across revisions

Conventions:

- leaf benchmarks should stay single-run and focused
- repeated runs should be handled by matrix runners, not by the leaf scripts
- result summarizers should consume JSON outputs rather than re-running
  benchmarks
