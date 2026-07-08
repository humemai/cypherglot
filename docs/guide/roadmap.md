# Roadmap

This page is the live high-level roadmap for CypherGlot.

The high-level sequence is:

1. establish the compiler-only repo boundary and API contract
2. own parser generation and artifact verification in-repo
3. build normalization and validation layers
4. compile the admitted Cypher subset to SQLGlot AST
5. move from direct mostly-SQLite lowering toward `Cypher AST -> normalize -> graph-relational IR -> backend-aware lowering -> SQLGlot`
6. land SQLite-through-IR first, then explicit DuckDB and PostgreSQL lowerers from the same architecture
7. broaden the admitted language carefully
8. make the compiler vector-aware without making it vector-executing
9. surface structured diagnostics: named constructs, locations, and porting
   hints on every rejection
10. make repeated compilation cheap by default with a transparent bounded
    compile cache at the string-rendering entrypoints
11. harden the package for public release

Steps 1 through 10 have landed. The near-term language backlog — the families
currently rejected with named diagnostics — is:

- multi-`MATCH` pattern composition
- unbounded, undirected, and multi-type variable-length patterns
- comprehensions, `EXISTS {}` subqueries, and quantified predicates
- `shortestPath()` beyond what bounded variable-length patterns cover
- named paths

Multi-label nodes are a design non-goal rather than backlog; see the
[Admitted Subset](admitted-subset.md) guide.
