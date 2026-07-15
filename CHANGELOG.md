# Changelog

## Unreleased

### Language coverage

- `UNION` / `UNION ALL` of admitted branches (each branch validated and
  compiled independently; `UNION` de-duplicates, matching Cypher)
- undirected one-hop relationship reads `(a)-[r:T]-(b)`, lowered as a
  `UNION ALL` of both orientations with Neo4j row semantics (self-loops emit
  one row)
- multi-type one-hop relationship reads `[:A|B]`, lowered as a `UNION ALL`
  across the edge tables with per-branch `type(r)` constants; composes with
  undirected patterns
- searched `CASE` accepts bare boolean conditions (`WHEN u.active THEN ...`)
  and explicit `ELSE null`
- relationship `MERGE` supports `ON CREATE SET` / `ON MATCH SET` actions
  targeting the relationship alias (`ON MATCH` lowers to an update step before
  the guarded insert; `ON CREATE` folds into the insert columns)
- `OPTIONAL MATCH` after a one-hop mandatory `MATCH` (not just a single node),
  anchored on either mandatory endpoint alias; `WHERE` on the optional side
  stays in the join condition so unmatched anchors survive with NULLs
- `WITH` aggregation stage (the Cypher-HAVING pattern):
  `MATCH ... WITH a, count(b) AS c WHERE c > $k RETURN ...`
- fixed a latent inbound one-hop join miscompilation (`(a)<-[r]-(b)` was
  joined with outbound endpoint columns in one code path)
- fixed `OPTIONAL MATCH ... WHERE` emitting a spurious `(anchor, NULL)` row
  next to qualifying matches when the optional predicate filtered some rows

### Diagnostics

- new `CypherCompileError` (a `ValueError` subclass) carrying `message`,
  `line`, `column`, `construct`, and `hint`; exported from the package root
- unsupported constructs are rejected early by name with a location and a
  porting hint: list/pattern comprehensions, `EXISTS {}` subqueries,
  `all()/any()/none()/single()` predicates, multi-label nodes, named paths,
  `shortestPath()`/`allShortestPaths()`
- subset-boundary gates and syntax errors carry stable `construct` identifiers
  for batch corpus auditing

### Performance

- transparent bounded compile cache at the string-rendering entrypoints
  (`to_sql`, `render_cypher_program_text`); on by default, keyed on the exact
  query text plus backend/dialect/render options/schema context; control it
  with `set_compile_cache(...)`, `clear_compile_cache()`,
  `compile_cache_info()`, or the `CYPHERGLOT_COMPILE_CACHE=0` environment
  variable. Parameterize queries with `$param` (compiled to SQL bind
  parameters) so repeated calls hit the cache

### Packaging and project

- renamed the project from HumemCypher to CypherGlot throughout
- added `py.typed` so type checkers consume the package's annotations
- refreshed dependency pins (sqlglot 30.12.x among others)
- new documentation: porting-from-Neo4j guide, diagnostics guide, per-backend
  dialect notes
- benchmark tooling: the compiler microbenchmark records unsupported
  backend/query combinations as skips instead of aborting; the W3 analyzer
  gained repeat-root pooling (median [min-max] primary statistic, retry
  dedup); checked-in campaign results live under
  `scripts/benchmarks/results/rerun-2026-07-ccx33/`

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
