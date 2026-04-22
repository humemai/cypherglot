# Scripts

This directory holds project-maintenance scripts that are not part of the
runtime library package itself.

Current subdirectories:

- `benchmarks/`: benchmark entrypoints, benchmark runners, result summarizers,
  corpora, and generated benchmark artifacts
- `dev/`: development and regeneration helpers used while maintaining the repo

Rule of thumb:

- put reproducible measurement code in `benchmarks/`
- put maintainer-only helper scripts in `dev/`

These scripts are intentionally separate from `src/` so benchmark and developer
tooling can evolve without becoming part of the public CypherGlot API.
