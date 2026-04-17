# Roadmap

The live roadmap is maintained in `things-to-do.md`.

The high-level sequence is:

1. establish the compiler-only repo boundary and API contract
2. own parser generation and artifact verification in-repo
3. build normalization and validation layers
4. compile the admitted Cypher subset to SQLGlot AST
5. move from direct mostly-SQLite lowering toward `Cypher AST -> normalize -> graph-relational IR -> backend-aware lowering -> SQLGlot`
6. land SQLite-through-IR first, then explicit DuckDB and PostgreSQL lowerers from the same architecture
7. broaden the admitted language carefully
8. make the compiler vector-aware without making it vector-executing
9. harden the package for public release
