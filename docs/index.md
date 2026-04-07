# CypherGlot

CypherGlot is the Cypher frontend compiler for the HumemAI stack.

The intended flow is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot AST
```

The repository is intentionally compiler-focused.

- It parses and lowers Neo4j-like Cypher.
- It returns SQLGlot AST instead of owning SQL execution.
- It can carry vector intent forward, but it does not execute vector search.

## What this repo is for

- a reusable Cypher frontend compiler
- a stable boundary between Cypher parsing and execution
- a SQLGlot-based output that host runtimes can plan and execute

## What this repo is not

- a database engine
- a SQL executor
- a vector search engine
- a storage system

## Start here

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [Compiler Contract](guide/compiler-contract.md)
- [Roadmap](guide/roadmap.md)
