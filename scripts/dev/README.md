# Development Scripts

This directory contains maintainer-facing helpers for repository development and
regeneration tasks.

Current scripts:

- `regenerate_cypher_frontend.py`: regenerate frontend artifacts locally
- `regenerate_cypher_frontend_docker.sh`: run the frontend regeneration flow in
  Docker
- `cypher_frontend_regen.Dockerfile`: container image used for frontend
  regeneration
- `consolidate_unary_funcs.py`: helper for maintaining generated frontend or
  grammar-related code
- `run_postgresql_runtime_docker.sh`: convenience helper for PostgreSQL runtime
  work in Docker-backed setups

These scripts are not part of the public package interface and may assume a
maintainer environment, local tooling, or Docker availability.

Rule of thumb:

- if a script exists to help maintain or regenerate project internals, it
  belongs here
- if a script exists to produce benchmark evidence, it belongs in
  `scripts/benchmarks/`
