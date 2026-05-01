# Compiler Benchmarks

This directory contains the compiler-only benchmark entrypoint.

Contents:

- `benchmark.py`: benchmarks parser, validator, normalization, lowering, SQL
  rendering, and optional sqlglot comparison work over the checked-in compiler
  corpora
- `summarize_results.py`: renders compiler benchmark JSON output as a Markdown
  report

Typical usage from the repo root:

```bash
python -m scripts.benchmarks.compiler.benchmark
python -m scripts.benchmarks.compiler.benchmark --iterations 10 --warmup 2
python -m scripts.benchmarks.compiler.benchmark --output scripts/benchmarks/results/compiler_benchmark.json
python -m scripts.benchmarks.compiler.summarize_results
python -m scripts.benchmarks.compiler.summarize_results --output scripts/benchmarks/results/compiler-results.md
```

Default single-run output:

- `scripts/benchmarks/results/compiler_benchmark.json`
- `scripts/benchmarks/results/compiler-results.md` when you run the summarizer

For broader methodology and result interpretation, see
`docs/guide/benchmarks.md`.