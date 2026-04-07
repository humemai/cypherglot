# Compiler Contract

The intended contract for CypherGlot is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot AST
```

That means:

- `cypherglot` owns raw Cypher parsing
- `cypherglot` owns Cypher normalization and lowering
- `cypherglot` returns SQLGlot `Expression` trees
- a host runtime such as `humemdb` owns planning, vector execution, dialect
  generation, and backend execution

## Scope

`cypherglot` should:

- parse Neo4j-like Cypher input
- validate admitted subset boundaries clearly
- lower admitted Cypher into SQLGlot AST

`cypherglot` should not:

- execute SQL
- own graph storage
- execute vector search
- manage vector index lifecycle

## Vector-aware but not vector-executing

For mixed Cypher vector queries, `cypherglot` should parse the ordinary Cypher
structure and carry vector intent forward as metadata or compiler-recognizable
structure. A host runtime should then turn that into vector search plus a
conditioned relational query path.
