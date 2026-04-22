# Benchmark Results

This directory holds generated benchmark artifacts.

Layout:

- `compiler_benchmark.json`: default single-run compiler benchmark output
- `runtime/`: single-run and repeated-run runtime JSON outputs
- `runtime-matrix/`: per-session runtime manifests and job logs
- `schema/`: single-run and repeated-run schema JSON outputs
- `schema-matrix/`: per-session schema manifests and job logs

Conventions:

- treat this directory as generated output, not source code
- prefer current naming conventions from the benchmark entrypoints and
  summarizers over preserving legacy artifact names
- if you persist Markdown summaries manually, use explicit names such as
  `runtime-summary.md` or `schema-summary.md`