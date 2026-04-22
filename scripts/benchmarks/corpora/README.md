# Benchmark Corpora

This directory contains the checked-in benchmark input corpora.

Contents include:

- compiler benchmark corpora used by `compiler/benchmark.py`
- SQL runtime benchmark corpora used by the runtime entrypoints

Conventions:

- keep corpora stable so results remain comparable across revisions
- prefer additive corpus changes over silent rewrites when benchmark scope
  expands
- update the benchmark guide when a corpus change materially alters workload
  coverage