# Compiler Contract

The intended contract for CypherGlot is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot-backed output
```

That means:

- `cypherglot` owns raw Cypher parsing
- `cypherglot` owns Cypher normalization and lowering
- `cypherglot` returns either a SQLGlot `Expression` or a small compiled program
  composed of SQLGlot-backed statements
- a host runtime such as `humemdb` owns planning, vector execution, dialect
  generation, and backend execution

Today that contract is already live for the admitted subset: parse, validate,
normalize, and compile are repo-owned boundaries, while execution remains outside
CypherGlot.

The physical graph-to-table layout assumed by current compilation is documented
in the [Schema Contract](schema-contract.md) guide.

## Current backend stance

CypherGlot returns SQLGlot-backed output, but the current compiled query shapes
are intentionally aligned with SQLite-backed host runtimes.

In practice that means:

- the current compiled graph contract is SQLite-first
- the current runtime validation in this repo is SQLite-only
- HumemDB is the main reference runtime for execution
- host runtimes should treat the current graph-to-table schema contract as part
  of the execution boundary, not as an incidental implementation detail
- `to_sql(..., dialect=...)` and `render_cypher_program_text(..., dialect=...)`
  expose SQLGlot rendering controls, but those controls do not by themselves make
  the compiled output backend-neutral
- DuckDB is not the current target for CypherGlot-generated graph SQL, even
  though host runtimes may still use DuckDB elsewhere for other workloads

## Scope

`cypherglot` should:

- parse Neo4j-like Cypher input
- validate admitted subset boundaries clearly
- lower admitted Cypher into SQLGlot-backed compiled output

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

Today that handoff shape is the normalized
`NormalizedQueryNodesVectorSearch` contract, which carries:

- `procedure_kind='queryNodes'`
- `index_name`
- `query_param_name`
- `top_k` as the admitted normalized top-k value
- `candidate_query` as one admitted normalized `MATCH` query built from either
  post-call `MATCH ...` or `YIELD ... WHERE ...`
- `return_items` over `node.id` and/or `score`
- `order_by` over `node.id` and/or `score`

That is intentionally a host-runtime handoff contract, not ordinary SQL lowering.
`compile_cypher_text(...)`, `compile_cypher_program_text(...)`, and the rendering
helpers built on them still reject vector-aware `CALL db.index.vector.queryNodes(...)`
queries so the compiler does not pretend vector planning or execution is backend-native
SQLGlot work.

## Output shapes

CypherGlot exposes two related output contracts:

- single-statement helpers such as `to_sqlglot_ast(...)` and `to_sql(...)` for
  admitted shapes that lower to one SQLGlot expression
- program helpers such as `to_sqlglot_program(...)` and
  `render_cypher_program_text(...)` for admitted shapes that require multiple
  SQL-backed steps

The admitted language boundary is documented in the
[Admitted Subset](admitted-subset.md) guide.

Today that also includes a narrow vector-aware normalization path for
`CALL db.index.vector.queryNodes(...) YIELD node, score ...` queries. Those
queries are validated and normalized so host runtimes can consume their vector
intent, but they are not yet compiled into SQLGlot-backed output directly.

For ordinary non-vector aggregation, the current admitted aggregate contract is
still intentionally narrow compared with full Cypher, but it now includes the
practical grouped families that matter for mainstream onboarding:

- `count(binding_alias)`
- `count(*)`
- `sum(...)`
- `avg(...)`
- `min(...)`
- `max(...)`

Those surfaces remain restricted to already admitted field or scalar-binding
inputs, depending on the query shape.
