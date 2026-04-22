# Schema Benchmarks

This directory contains the schema-shape benchmark tooling used to compare
alternative SQLite graph storage layouts.

Contents:

- `sqlite_shapes.py`: single-run schema-shape benchmark over the generated
  multi-type graph workload
- `matrix.py`: repeated fresh-process schema benchmark runner
- `summarize_results.py`: Markdown summarizer for repeated schema JSON outputs,
  including repeat-level mean/std for setup metrics, RSS, size, and query
  percentiles

Typical usage from the repo root:

```bash
python -m scripts.benchmarks.schema.sqlite_shapes
python -m scripts.benchmarks.schema.matrix --scale small --repeats 3 --workers 2
python -m scripts.benchmarks.schema.matrix --scale medium --repeats 3 --workers 2
python -m scripts.benchmarks.schema.matrix --scale large --repeats 2 --workers 1
python -m scripts.benchmarks.schema.summarize_results --no-queries
```

Schema matrix presets use `small`, `medium`, and `large` names and now follow
the same progression style as the runtime matrix:

- `small`: `4` node types, `4` edge types, `1000` nodes per type
- `medium`: `6` node types, `8` edge types, `5000` nodes per type
- `large`: `10` node types, `10` edge types, `100000` nodes per type

The schema path keeps denser graph-shape experimentation tractable, so these
presets follow the runtime naming model without matching every runtime scale
parameter exactly.

Output conventions:

- single-run schema JSON baselines live in `scripts/benchmarks/results/schema/`
- repeated-run manifests and per-job logs live in
  `scripts/benchmarks/results/schema-matrix/`

This benchmark path is for physical-schema experiments, not compiler or
backend-comparison runtime measurements. The schema query set still covers both
OLTP-leaning and OLAP-leaning shapes, but it stays intentionally smaller and
more structural than the runtime workload suite.