# Release Candidate

This page captures the current v0.1.0 release-candidate materials for the first
public CypherGlot release.

## Target version

- release version: `v0.1.0`
- package version source: Hatch VCS tags
- source-file bump needed: none

CypherGlot already derives package versions from Git tags, and the current Hatch
fallback version is already `0.1.0`. For the first public release, the version
step is therefore operational rather than code-facing: the release tag should be
`v0.1.0`.

## Release-candidate summary

CypherGlot is ready to present as a compiler-focused package, not as a database
engine and not as a full-Cypher compatibility claim.

The release candidate is centered on:

- repo-owned Cypher parsing
- explicit admitted-subset validation
- repo-owned normalization
- SQLGlot-backed single-statement and multi-step compilation helpers
- a practical mainstream single-hop, read-heavy onboarding subset
- refreshed documentation and README positioning
- a checked-in benchmark harness and baseline

## Changelog summary

The v0.1.0 changelog lives in `CHANGELOG.md` and should be used as the source
summary for the GitHub release body and any PyPI release notes.

## Tag plan

Recommended first-release flow:

1. Confirm the release candidate is still green locally.
2. Confirm CI is green on `main`, especially `test.yml` and `build-docs.yml`.
3. Re-run the generated frontend check if parser artifacts changed: `scripts/dev/regenerate_cypher_frontend_docker.sh --check`.
4. Ensure the checked-in benchmark baseline still reflects the intended release state.
5. Create the annotated Git tag `v0.1.0` from the release commit.
6. Let `publish-pypi.yml` publish the package from that tag.
7. Let `deploy-docs.yml` publish the versioned docs and set the `latest` alias if appropriate.
8. Create the matching GitHub release using the draft notes below.

## Release notes draft

`v0.1.0` is the first public release candidate for CypherGlot, a reusable,
SQLGlot-backed Cypher frontend compiler for the HumemAI stack.

This release intentionally targets a practical mainstream single-hop,
read-heavy subset for Neo4j-style onboarding rather than full Cypher parity.
It includes repo-owned parsing, admitted-subset validation, normalization, and
public compilation helpers for single-statement reads and narrow multi-step
writes. The release also ships refreshed docs, a clearer public contract, and a
checked-in benchmark harness with a baseline corpus for future regression
tracking.

Notably out of scope for `v0.1.0` are broader `MERGE` semantics beyond the
narrow admitted idempotent subset, broader variable-length path semantics
beyond the bounded read-side subset, broader write-side traversal semantics
beyond the narrow traversal-backed alias-reuse subset and one-fresh-endpoint
`MATCH ... CREATE` / `MATCH ... MERGE` subsets,
and backend-native compilation of vector-aware
`CALL db.index.vector.queryNodes(...)` flows.

## Pre-publish checks

- `uv run pytest`
- `scripts/dev/run_postgresql_runtime_docker.sh`
- `uv run mkdocs build --strict`
- `python scripts/benchmarks/benchmark_compiler.py`
- `scripts/dev/regenerate_cypher_frontend_docker.sh --check`
