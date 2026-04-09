# Changelog

## v0.1.0

First public release candidate for CypherGlot as a compiler-focused,
SQLGlot-backed Cypher frontend.

### Highlights

- repo-owned parsing through the vendored openCypher ANTLR frontend
- explicit admitted-subset validation instead of parser-breadth marketing
- normalization into repo-owned statement objects
- public single-statement compiler entrypoints via `to_sqlglot_ast(...)` and
  `to_sql(...)`
- public multi-step program compilation via `to_sqlglot_program(...)` and
  `render_cypher_program_text(...)`
- practical mainstream single-hop read-heavy onboarding subset for Neo4j-style
  queries, including:
  - ordinary `MATCH ... RETURN`
  - bounded read-side variable-length relationship reads for ordinary
    `MATCH ... RETURN` and `MATCH ... WITH ... RETURN`, including bounded
    zero-hop ranges such as `*0..2`, direct aggregate-only `count(*)` /
    `count(endpoint_alias)` / `aggregate(endpoint.field)` returns, grouped
    endpoint-field aggregates in direct `MATCH ... RETURN`, and syntactic
    syntactic relationship aliases that remain unused downstream
  - narrow traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE` when the
    matched source is a one-hop relationship or fixed-length chain and the
    write reuses already matched node aliases
  - narrow traversal-backed `MATCH ... CREATE` with one fresh labeled endpoint
    node when the other endpoint reuses a matched node alias from the traversal
    source
  - narrow traversal-backed `MATCH ... MERGE` with one fresh labeled endpoint
    node when the other endpoint reuses a matched node alias from the traversal
    source
  - narrow traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE` with one
    fresh unlabeled endpoint node when the other endpoint reuses a matched node
    alias from the traversal source
  - narrow standalone `OPTIONAL MATCH ... RETURN`
  - narrow `MATCH ... WITH ... RETURN`
  - narrow standalone `UNWIND ... RETURN`
  - standalone `CREATE`, narrow standalone `MERGE`, `MATCH ... SET`,
    `MATCH ... DELETE`, narrow `MATCH ... CREATE`, and narrow
    `MATCH ... MERGE`
  - grouped `count(...)`, `count(*)`, `sum(...)`, `avg(...)`, `min(...)`, and
    `max(...)`
  - graph-introspection projections, predicate returns, string functions,
    numeric functions, conversion functions, searched `CASE`, and narrow
    multi-argument string rewrite helpers over admitted inputs
- checked-in benchmark harness, representative corpus, and baseline latency
  artifact for regression tracking
- refreshed README and docs so the public contract, non-goals, and release
  scope are aligned

### Non-goals for v0.1.0

- full Cypher parity
- broader `MERGE` semantics beyond the narrow admitted subset
- broader variable-length path semantics beyond the bounded read-side subset,
  including downstream use of relationship aliases and open-ended ranges
- broader write-side traversal semantics beyond the narrow traversal-backed
  alias-reuse subset and one-fresh-endpoint `MATCH ... CREATE` / `MATCH ... MERGE`
  subsets
- direct SQL execution or storage ownership inside CypherGlot
- backend-native compilation of vector-aware `CALL db.index.vector.queryNodes(...)`
  flows
