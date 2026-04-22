# Compiler Benchmarks

This directory contains the compiler-only benchmark entrypoint.

Contents:

- `benchmark.py`: benchmarks parser, validator, normalization, lowering, SQL
  rendering, and optional sqlglot comparison work over the checked-in compiler
  corpora

Typical usage from the repo root:

```bash
python -m scripts.benchmarks.compiler.benchmark
python -m scripts.benchmarks.compiler.benchmark --iterations 10 --warmup 2
python -m scripts.benchmarks.compiler.benchmark --output scripts/benchmarks/results/compiler_benchmark.json
```

Default single-run output:

- `scripts/benchmarks/results/compiler_benchmark.json`

For broader methodology and result interpretation, see
`docs/guide/benchmarks.md`.