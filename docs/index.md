# CypherGlot

CypherGlot is the Cypher frontend compiler for the HumemAI stack.

The intended flow is:

```text
raw Cypher string
→ parse
→ validate admitted subset
→ normalize
→ graph-relational IR
→ backend-aware lowering
→ SQLGlot AST or SQL-backed program
```

The repository is intentionally compiler-focused.

- It parses and lowers Neo4j-like Cypher.
- It targets a practical mainstream single-hop read-heavy subset,
  not full Cypher parity.
- It returns SQLGlot AST or SQL-backed compiled programs instead of owning SQL
    execution.
- It can carry vector intent forward, but it does not execute vector search.

## What this repo is for

- a reusable Cypher frontend compiler
- a stable boundary between Cypher parsing and execution
- a SQLGlot-based output that host runtimes can plan and execute

## Current backend direction

CypherGlot uses a backend-neutral IR plus backend-aware lowering.

- SQLite has an executable lowering path through the shared IR.
- DuckDB already has an explicit lowering path from the same shared
    architecture for admitted analytical reads.
- PostgreSQL is part of the same source-first backend path.
- SQLGlot dialect rendering remains useful as an output helper, but rendering
    alone is not the support boundary.
- A backend counts as supported only when admitted Cypher shapes execute
    correctly against that backend's schema and runtime contract.

## What this repo is not

- a database engine
- a SQL executor
- a vector search engine
- a storage system

## Start here

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [Public Entrypoints](guide/public-entrypoints.md)
- [Compiler Contract](guide/compiler-contract.md)
- [Schema Contract](guide/schema-contract.md)
- [Benchmarks](guide/benchmarks.md)
- [Roadmap](guide/roadmap.md)
