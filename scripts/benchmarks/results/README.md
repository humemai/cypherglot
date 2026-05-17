# Benchmark Results

This directory holds generated benchmark artifacts.

Layout:

- `compiler_benchmark.json`: default single-run compiler benchmark output
- checked-in compiler summary Markdown artifact(s)
- checked-in runtime summary Markdown artifact(s)
- checked-in schema summary Markdown artifact(s)
- `runtime/`: single-run and repeated-run runtime JSON outputs
- `runtime-matrix/`: per-session runtime manifests and job logs
- `schema/`: single-run and repeated-run schema JSON outputs
- `schema-matrix/`: per-session schema manifests and job logs

Conventions:

- treat this directory as generated output, not source code
- prefer current naming conventions from the benchmark entrypoints and
  summarizers over preserving legacy artifact names
- hardware or machine details in Markdown artifact filenames are internal
  provenance labels, not part of the public benchmark story
- if you persist Markdown summaries manually, use explicit names such as
  `runtime-summary.md` or `schema-summary.md`
