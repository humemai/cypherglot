# CypherGlot Things To Do

Frontend-compiler roadmap for the separate `cypherglot` repo.

CypherGlot is the Cypher frontend compiler. Its job is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot-backed compiled output
```

It should not:

- execute vector search
- own storage or table execution

Its main product is a single `Expression` tree for single-statement shapes, or a
small compiled program of SQLGlot statements for multi-step writes.

Current estimate: roughly 25% to 30% of practical Cypher usage overall, or

Near-term target: reach roughly 50% practical coverage before pushing for a much
broader 80% claim.

- [x] Add narrow `substring(...)` support for ordinary single-node/relationship
  reads, narrow `OPTIONAL MATCH`, and final `WITH ... RETURN` projections,
  restricted to exactly three arguments with an admitted field/scalar/literal
  primary input plus literal/parameter start and length, with compiler,
  validation, docs, and tests kept in sync.
- [x] Broaden narrow `substring(...)` support to also admit the two-argument
      form across ordinary reads, narrow `OPTIONAL MATCH`, and final
      `WITH ... RETURN` projections while keeping primary inputs and start values
      restricted to the same admitted field/scalar/literal and literal/parameter
      surfaces.
- [x] Add narrow `reverse(...)` support for ordinary reads, narrow
      `OPTIONAL MATCH`, and final `WITH ... RETURN` projections over the same
      admitted entity-field, scalar-binding, and scalar literal/parameter inputs
      already used by the unary string-function family.
- [x] Add narrow `replace(...)` support for ordinary reads, narrow
      `OPTIONAL MATCH`, and final `WITH ... RETURN` projections, restricted to
      admitted field/scalar/literal primary inputs plus literal-or-parameter
      search and replacement arguments.
- [x] Add narrow `left(...)` and `right(...)` support for ordinary reads,
      narrow `OPTIONAL MATCH`, and final `WITH ... RETURN` projections,
      restricted to admitted field/scalar/literal primary inputs plus a
      literal-or-parameter length argument.
- [x] Add narrow `split(...)` support for ordinary reads, narrow
      `OPTIONAL MATCH`, and final `WITH ... RETURN` projections, restricted to
      admitted field/scalar/literal primary inputs plus a
      literal-or-parameter delimiter argument.

What 50% means here:

- cover the common single-user application query shapes that go beyond the
      parser-only milestone
- prioritize query composition, projection breadth, and read semantics before
      harder long-path graph semantics
- keep admitted behavior documented, normalized, compiled or intentionally
      carried forward, and defended by tests

What 80% would require later:

- solid multi-part query flow
- `MERGE`
- broader traversal semantics such as variable-length paths and wider write-side traversal support
- a much stronger evidence base across real query families, not just grammar
  acceptance

## Phase 1

Establish the repo boundary and compiler contract.

Status: complete.

- [x] Make the compiler-only repo scope explicit in code, docs, and API shape.
- [x] Keep raw Cypher string input as the main frontend entrypoint.
- [x] Keep raw-Cypher parsing responsibility inside `cypherglot` instead of expecting
      HumemDB to parse Cypher first and only delegate later.
- [x] Keep SQLGlot as a direct dependency and SQLGlot AST as the main output.
- [x] Avoid introducing a redundant extra AST or IR layer unless a clear later need
      emerges.
- [x] Keep the repo focused on parsing, normalization, validation, and compilation,
      not execution.
- [x] Document the intended relationship with HumemDB clearly: `cypherglot` compiles,
      HumemDB plans and executes.
- [x] Keep the minimal useful public contract explicit: a small compile API that
      accepts raw Cypher and returns SQLGlot-backed compiled output.
- [x] Keep the first public API function-first instead of class-heavy: start with
      narrow entrypoints such as `parse_cypher(...)`, `to_sqlglot_ast(...)`, and later a
      thin `to_sql(..., dialect=...)` convenience layer rather than inventing a broad
      compiler object model too early.
- [x] Keep SQL-string generation as a secondary convenience API on top of the AST
      path, not as the main abstraction of the repo.

## Phase 2

Own the parser-generation workflow.

Status: complete.

- [x] Choose the concrete parser-generation path and keep it Python-first.
- [x] Vendor or generate parser-ready grammar artifacts in-repo instead of depending
      on a low-trust runtime parser package.
- [x] Use openCypher materials as reference input, not as a runtime dependency.
- [x] Keep parser regeneration a documented, owned development workflow.
- [x] Prefer a Docker-first regeneration flow so contributors do not need Java on the
      host.
- [x] Once `cypherglot` owns checked-in generated parser outputs, make those artifacts
      verifiable in CI so they cannot silently drift from the grammar inputs.
- [x] Do not carry a placeholder generated-artifacts workflow before real generated
      outputs exist; add that CI check when parser-generation ownership becomes real in
      this repo.

## Phase 3

Build the normalization and validation boundary.

Status: complete.

- [x] Parse raw Cypher into a stable Cypher-facing internal representation.
- [x] Normalize parser output into repo-owned structures before lowering to SQLGlot.
- [x] Use small repo-owned normalized data structures where they help clarify the
      admitted subset, but do not build a giant full-Cypher class hierarchy up front.
- [x] Keep normalized nodes driven by the admitted subset and real lowering needs,
      for example narrow statement, clause, node-pattern, and relationship-pattern
      structures rather than speculative full-language coverage.
- [x] Keep syntax errors owned by the generated parser path instead of silently
      falling back to ad hoc parsing.
- [x] Separate syntax acceptance from admitted-subset policy validation.
- [x] Reject unsupported constructs clearly instead of pretending to support broad
      Neo4j or openCypher compatibility.

## Phase 4

Compile the admitted Cypher subset to SQLGlot AST.

Status: complete.

- [x] Start with the practical MVP subset: `MATCH`, node labels, single-hop directed
      relationships, simple `WHERE`, `RETURN`, `ORDER BY`, and `LIMIT`.
- [x] Keep the core mapping explicit in compiler work: node patterns lower to
      `nodes`, labeled nodes lower through `node_labels`, relationships lower through
      `edges`, and property access lowers through JSON extraction expressions or dialect
      equivalents.
- [x] Lower node patterns to the agreed relational schema using `nodes`,
      `node_labels`, and `edges`.
- [x] Treat labels as normalized rows and properties as JSON-backed values in the
      compiler assumptions.
- [x] Compile property access through the appropriate JSON extraction expressions.
- [x] Return SQLGlot AST as the compiler product instead of final SQL strings.
- [x] Keep SQLGlot AST broad enough for this role without pretending SQLGlot is
      semantically aware of HumemDB-specific extensions; SQLGlot handles structure while
      HumemDB and `cypherglot` own the language meaning.
- [x] Add dialect-specific SQL rendering only after the AST path is solid, and keep
      it as a thin wrapper around SQLGlot dialect rendering rather than as a competing
      primary API.

## Phase 5

Broaden the admitted language carefully.

Status: complete.

- [x] Expand the admitted subset incrementally instead of trying to claim broad
      Cypher completeness.
- [x] Use TCK-style and grammar-backed evidence to widen support clause family by
      clause family.
- [x] Keep `OPTIONAL MATCH`, variable-length paths, named paths, broad multi-part
      flows, and subqueries out until there is clear evidence and implementation support.
- [x] Add operationally useful breadth before broader path semantics when tradeoffs
      matter.
- [x] Keep the language boundary documented and defended by tests.

## Phase 6

Make the frontend vector-aware without making it vector-executing.

Status: complete.

- [x] Admit the chosen Cypher-side vector syntax under the normal parser boundary.
- [x] Carry vector intent forward as metadata, placeholders, or compiler-recognizable
      structures rather than executing vector logic inside `cypherglot`.
- [x] Keep the mixed-query boundary explicit in normalized and compiled contracts:
      for Neo4j-like Cypher vector search, `cypherglot` should parse the ordinary
      Cypher structure and carry vector intent forward without collapsing it into
      ordinary non-vector SQL semantics.
- [x] Keep the non-goal boundary explicit in docs and compiler behavior: vector-aware
      Cypher forms are admitted and carried forward, but `cypherglot` does not execute
      vector search or own vector-index lifecycle.
- [x] Ensure the compiler contract can participate in the shared SQLGlot-based host
      execution flow without pretending vector-aware Cypher is ordinary backend-native
      non-vector Cypher semantics.

## Phase 7

Harden CypherGlot for the first public release.

Status: complete.

- [x] Stabilize the public compiler entrypoint and output contract.
- [x] Make syntax and subset-policy errors clear and testable.
- [x] Add regression coverage for the admitted Cypher subset and compiler output
      shape.
- [x] Document the admitted subset, non-goals, and HumemDB integration boundary.
- [x] Keep the release claim narrow: useful Cypher frontend compiler, not full Cypher
      compatibility.
- [x] Keep the repo-level release claim explicit too: reusable Cypher frontend
      compiler to SQLGlot-backed compiled output, not a standalone graph database or SQL
      execution engine.

## Phase 8

Reach roughly 50% practical Cypher coverage.

Status: complete.

- [x] Add `WITH` support for narrow single-stream multi-part query flow instead of
      stopping at single-part statements.
- [x] Support alias rebinding through `WITH` for admitted node, relationship, and
      scalar projections.
- [x] Add a narrow `WITH WHERE` slice for post-binding stream filtering over
      scalar aliases and entity-field projections.
- [x] Broaden `RETURN` items beyond plain `alias.field` for the core practical read
      subset: simple scalar aliases, pass-through entity bindings, final
      `RETURN ... AS ...` aliasing, and projected-alias ordering for non-entity
      outputs now work.
- [x] Broaden ordinary `MATCH` and narrow `OPTIONAL MATCH` result shapes with field
      aliasing, whole-entity pass-through returns, and narrow standalone
      `count(...) AS ...` support, including grouped whole-entity projections.
- [x] Broaden ordinary-read expression projections with narrow scalar
      literal/parameter outputs, `size(...)`, `lower(...)`, `upper(...)`,
      `trim(...)`, `ltrim(...)`, `rtrim(...)`, `coalesce(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`,
      `sin(...)`, `cos(...)`, `tan(...)`,
      simple predicate outputs, `id(...)`, and `type(...)`.
- [x] Broaden the admitted `MATCH ... WITH ... RETURN` final `RETURN` surface with
      narrow scalar literal/parameter outputs plus narrow `id(entity_alias)`,
      `type(rel_alias)`, `size(...)` including scalar literal/parameter inputs,
      `lower(...)`, `upper(...)`, `trim(...)`, `ltrim(...)`, `rtrim(...)`, `coalesce(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, and simple predicate outputs.
- [x] Continue broadening common result shapes beyond the current admitted field,
      entity, scalar, aggregate, `size(...)`, `lower(...)`, `upper(...)`,
      `trim(...)`, `ltrim(...)`, `rtrim(...)`, `coalesce(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, simple predicate, `id(...)`, and `type(...)` surfaces.
- [x] Broaden projected-alias ordering to cover admitted whole-entity returns in
      ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`.
- [x] Keep whole-entity projected-alias ordering honest across both admitted
      node and relationship entity bindings in ordinary reads and final
      `MATCH ... WITH ... RETURN`.
- [x] Keep grouped whole-entity aggregation honest across both admitted node
      and relationship bindings, including narrow `count(binding_alias)` and
      `count(*)` forms in ordinary reads and final
      `MATCH ... WITH ... RETURN`.
- [x] Add the first admitted aggregation slice needed for practical app queries,
      starting with common grouped `count(...)` and simple grouped projections.
- [x] Extend the first admitted aggregation slice to cover narrow `count(*)`
      forms in ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `UNWIND` slice for practical list expansion and parameter-driven
      query flows.
- [x] Add a narrow `OPTIONAL MATCH` slice once null propagation and result-shape
      semantics are explicit and testable.
- [x] Keep each newly admitted family documented in the subset guide and defended
      by grammar-backed tests before treating it as part of the practical-coverage
      claim.

## Phase 9

Push beyond 50% toward a broader mainstream subset (target 80%).

Status: in progress.

- [x] Revisit `MERGE` after the multi-part and projection boundary solidifies,
      admitting a narrow idempotent subset: standalone labeled node/relationship
      `MERGE` without `ON CREATE` / `ON MATCH` actions plus
      `MATCH ... MERGE` for relationship merges between already matched node
      aliases.
- [x] Add scalar literal and parameter projections in ordinary read `RETURN`
      clauses.
- [x] Add a narrow `size(...)` computed-projection slice for ordinary and narrow
      optional reads.
- [x] Add a first narrow predicate-return slice for ordinary and narrow optional
      reads.
- [x] Add narrow `id(...)` / `type(...)` function-form aliases for admitted reads.
- [x] Extend the admitted `MATCH ... WITH ... RETURN` final `RETURN` surface with
      narrow scalar literal/parameter, simple predicate, `size(...)`, `id(...)`,
      and `type(...)` outputs, including `size(...)` over scalar
      literal/parameter inputs.
- [x] Extend the admitted `size(...)` slice to cover nested admitted
      `size(id(entity_alias))` and `size(type(rel_alias))` forms in ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `lower(...)` / `upper(...)` computed-projection slice for
      ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`, over admitted entity-field projections plus
      scalar literal/parameter inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `toString(...)` computed-projection slice for ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      over admitted entity-field projections plus scalar literal/parameter
      inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `toInteger(...)` computed-projection slice for ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      over admitted entity-field projections plus scalar literal/parameter
      inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `toFloat(...)` computed-projection slice for ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      over admitted entity-field projections plus scalar literal/parameter
      inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `toBoolean(...)` computed-projection slice for ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      over admitted entity-field projections plus scalar literal/parameter
      inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `trim(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `rtrim(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `coalesce(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections with literal/parameter fallbacks, and
      over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `abs(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `sign(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `round(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `ceil(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `sqrt(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `exp(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `sin(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `cos(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `tan(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `asin(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `acos(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `atan(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `ln(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `log10(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add a narrow `log(...)` computed-projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, over
      admitted entity-field projections plus scalar literal/parameter inputs,
      and over scalar bindings in final `MATCH ... WITH ... RETURN`.
- [x] Add narrow `radians(...)` and `degrees(...)` computed-projection slices
      for ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`, over admitted entity-field projections plus
      scalar literal/parameter inputs, and over scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Extend the admitted predicate-return slice to cover admitted
      `id(entity_alias) OP literal_or_parameter` and
      `type(rel_alias) OP literal_or_parameter` forms in ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`.
- [x] Extend the admitted predicate-return slice to cover admitted
      `size(alias.field) OP literal_or_parameter`,
      `size(id(entity_alias)) OP literal_or_parameter`, and
      `size(type(rel_alias)) OP literal_or_parameter` forms in ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`, plus
      `size(scalar_alias) OP literal_or_parameter` in the final
      `MATCH ... WITH ... RETURN` subset.
- [x] Extend the admitted predicate-return slice to cover narrow string
      `CONTAINS`, `STARTS WITH`, and `ENDS WITH` outputs in ordinary reads, narrow
      optional reads, and final `MATCH ... WITH ... RETURN`.
- [x] Extend the admitted filter/`WHERE` slice to cover admitted
      `id(entity_alias) OP literal_or_parameter` forms in ordinary reads,
      narrow optional reads, and `WITH WHERE`, plus
      `type(rel_alias) = literal_or_parameter` in ordinary reads and
      `WITH WHERE` over relationship bindings.
- [x] Keep the admitted filter/`WHERE` slice honest for compile-safe field
      string and null predicates in ordinary reads, narrow optional reads, and
      `WITH WHERE`, including admitted one-hop relationship property filters.
- [x] Extend the admitted null-predicate slice for compiled behavior that is
      actually supported today: field and nested `size(...)` null predicates in
      ordinary reads and narrow optional reads, plus scalar/entity-field null
      predicates in final `MATCH ... WITH ... RETURN` and `WITH WHERE`.
- [x] Extend the admitted filter/`WHERE` slice to cover compile-safe nested
      `size(...)` field predicates in ordinary reads, narrow optional reads,
      admitted one-hop relationship reads, and `WITH WHERE`, including scalar
      rebound inputs in final `MATCH ... WITH ... RETURN`.
- [x] Add narrow `sum(...)`, `avg(...)`, `min(...)`, and `max(...)` aggregate
      projection slices for ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`, restricted to admitted entity/relationship
      property inputs in ordinary reads and admitted scalar bindings in final
      `MATCH ... WITH ... RETURN`.
- [x] Add a narrow searched `CASE WHEN ... THEN ... ELSE ... END` projection
      slice for ordinary reads, narrow optional reads, and final
      `MATCH ... WITH ... RETURN`, restricted to admitted predicate conditions
      plus admitted field/scalar/literal result surfaces.
- [x] Add a narrow `properties(entity_alias)` projection slice for ordinary
      reads, narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      restricted to admitted node/relationship entity bindings.
- [x] Add a narrow `labels(node_alias)` projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      restricted to admitted node entity bindings.
- [x] Add a narrow `keys(entity_alias)` projection slice for ordinary reads,
      narrow optional reads, and final `MATCH ... WITH ... RETURN`,
      restricted to admitted node/relationship entity bindings.
- [x] Add narrow `startNode(rel_alias)` and `endNode(rel_alias)` projection
      slices for ordinary one-hop relationship reads and final
      `MATCH ... WITH ... RETURN`, restricted to admitted relationship entity
      bindings.
- [x] Relax the explicit final-`RETURN` alias requirement for common graph
      introspection projections such as `id(...)`, `type(...)`,
      `properties(...)`, `labels(...)`, `keys(...)`, `startNode(...)`, and
      `endNode(...)` when they appear in ordinary reads, narrow optional reads,
      or final `MATCH ... WITH ... RETURN` projections.
- [x] Relax the explicit final-`RETURN` alias requirement for common aggregate
      projections such as `count(...)`, `count(*)`, `sum(...)`, `avg(...)`,
      `min(...)`, and `max(...)` in ordinary reads and final
      `MATCH ... WITH ... RETURN` when those admitted aggregate surfaces already
      exist.
- [x] Relax the explicit final-`RETURN` alias requirement for common unary
      computed projections such as `size(...)`, `lower(...)`, `upper(...)`,
      `trim(...)`, `reverse(...)`, `abs(...)`, `sign(...)`, and the
      `toString(...)` / `toInteger(...)` / `toFloat(...)` / `toBoolean(...)`
      family when they target already admitted field or scalar-binding inputs.
- [x] Continue broadening function and computed-expression coverage beyond the
      current narrow admitted slices, including additional `WITH` final-`RETURN`
      expression families, including no-alias admission for narrow multi-arg
      `coalesce(...)`, `replace(...)`, `left(...)`, `right(...)`, `split(...)`,
      and `substring(...)` returns over already admitted field and scalar-binding
      inputs.
- [x] Keep the admitted string and text-rewrite expression families honest
      across relationship property bindings in ordinary one-hop relationship
      reads and final `MATCH ... WITH ... RETURN`.
- [x] Keep the admitted numeric and conversion expression families honest
      across relationship property bindings in ordinary one-hop relationship
      reads and final `MATCH ... WITH ... RETURN`.
- [x] Keep the newer `sqrt(...)` and `exp(...)` relationship-property
      projections honest across ordinary one-hop relationship reads and final
      `MATCH ... WITH ... RETURN`.
- [x] Keep the newer `sin(...)` relationship-property projections honest
      across ordinary one-hop relationship reads and final
      `MATCH ... WITH ... RETURN`.
- [x] Keep the admitted predicate and nested `size(...)` predicate families
      honest across relationship property bindings in ordinary one-hop
      relationship reads and final `MATCH ... WITH ... RETURN`.
- [x] Admit bounded read-side variable-length relationship patterns for
      ordinary `MATCH ... RETURN` and `MATCH ... WITH ... RETURN`, keeping the
      implementation narrow by lowering them to finite fixed-hop expansions.
- [x] Admit bounded read-side variable-length relationship patterns with
      syntactic relationship aliases when downstream `RETURN` and `WITH`
      bindings continue to reference only the endpoint node aliases.
- [x] Extend bounded read-side variable-length relationship patterns to admit
      explicit zero-hop lower bounds such as `*0..2` in ordinary
      `MATCH ... RETURN` and narrow `MATCH ... WITH ... RETURN`.
- [x] Extend bounded read-side variable-length relationship patterns to admit
      direct aggregate-only `MATCH ... RETURN count(*)` and
      `MATCH ... RETURN count(endpoint_alias)` over the matched endpoint rows.
- [x] Extend bounded read-side variable-length relationship patterns to admit
      direct aggregate-only `MATCH ... RETURN sum/avg/min/max(endpoint.field)`
      over the matched endpoint field values.
- [x] Extend bounded read-side variable-length relationship patterns to admit
      direct grouped `MATCH ... RETURN` with endpoint field projections plus
      `count(*)` / `count(endpoint_alias)` / `sum`/`avg`/`min`/`max`
      over endpoint field values.
- [x] Admit narrow traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE`
      writes when the matched side is a one-hop relationship or fixed-length
      chain and the write reuses already matched node aliases exactly.
- [x] Admit narrow traversal-backed `MATCH ... CREATE` with one fresh labeled
      endpoint node when the other endpoint reuses a matched node alias from a
      one-hop relationship or fixed-length chain source.
- [x] Admit narrow traversal-backed `MATCH ... MERGE` with one fresh labeled
      endpoint node when the other endpoint reuses a matched node alias from a
      one-hop relationship or fixed-length chain source.
- [x] Admit narrow traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE`
      with one fresh unlabeled endpoint node when the other endpoint reuses a
      matched node alias from a one-hop relationship or fixed-length chain
      source.
- [ ] Revisit the remaining broader write-side traversal semantics and the
      remaining variable-length cases such as open-ended ranges,
      downstream use of variable-length relationship aliases, fresh endpoint
      creation with broader MERGE semantics, multi-fresh-node traversal writes,
      and other mixed traversal/write shapes once the simpler high-coverage
      read and write families are stable.
- [ ] After the Phase 8 and Phase 9 admitted subset stabilizes, add much broader
      direct-execution unit coverage that actually runs derived SQL against the
      documented SQLite schema for representative admitted reads and writes,
      instead of relying mainly on compile-string assertions.
- [x] Re-estimate practical coverage only from admitted, tested, and documented
      behavior instead of parser breadth alone, and describe the current target
      honestly as a practical mainstream single-hop read-heavy onboarding subset
      rather than full Cypher parity.

## Phase 10

File-size cleanup policy.

Status: planned opportunistically, not as a separate campaign right now.

- [ ] Review oversized handwritten Python modules opportunistically when they are
      touched for substantive feature work, and split or refactor them if they are
      long without a clear structural reason.
- [ ] Treat generated parser artifacts as exempt from the normal handwritten file-size
      threshold.
- [ ] Keep `src/cypherglot/compile.py` from turning into the next monolith by
      splitting it when the next substantial feature forces broader edits there.
- [ ] Prefer finishing remaining roadmap phases before doing line-count-driven cleanup
      in otherwise healthy handwritten modules.

## Phase 11

Wrap things up for the first public release v0.1.0.

Status: planned.

- [x] Make SQLite the explicit tested direct-execution runtime target for the
      current public contract instead of leaving the backend story at the
      looser "SQLite-shaped" level.
- [x] Publish the concrete downstream SQLite schema contract for direct
      execution, including strict JSON checks, foreign keys, and
      `ON DELETE CASCADE` semantics for edge and label cleanup.
- [x] Add SQLite runtime unit tests that execute compiled CypherGlot SQL and
      rendered write programs against the documented schema instead of relying
      only on compile-string expectations.
- [x] Prove the first direct-execution delete invariant in CypherGlot itself:
      compiled `MATCH ... DETACH DELETE node_alias` should remove the matched
      node and let the SQLite schema cascade away dangling edges.
- [x] Add a benchmark script under `scripts/` that measures end-to-end latency of the
      main compiler entrypoint on a representative Cypher query corpus. I wanna measure
      both cypher-to-SQLGlot AST latency and cypher-to-SQL string latency
- [x] Choose and check in or document a small representative benchmark corpus drawn
      from the admitted subset, the TCK, or real-world usage examples.
- [x] Run the benchmark harness for a fixed number of iterations per query and record
      baseline p50, p95, and p99 latency numbers for future regression tracking.
- [x] Refresh the docs for the first release so the public contract, admitted subset,
      non-goals, and release positioning are all consistent.
- [x] Write a better production ready README.md. See how I did for the humemdb repo
  (`/mnt/ssd2/repos/humemdb/README.md`)
- [x] Prepare the v0.1.0 release candidate materials: changelog summary, version bump,
      tag plan, and release notes draft.
- [x] Broaden the SQLite runtime test suite beyond the current smoke coverage so
      common admitted read and write families are exercised by executing the
      compiled SQL against the documented schema, not just by asserting rendered
      SQL text.
- [x] Once the main Phase 8 and Phase 9 query families feel complete enough,
      refresh the existing benchmark corpus and benchmark script so they cover
      the newer admitted query shapes instead of reflecting only the older
      subset.
- [x] Make compiled `MATCH ... DELETE relationship_alias` directly executable on
      SQLite too; the current emitted `DELETE ... USING ...` shape is not
      SQLite-compatible even though the broader delete contract is now tested in
      HumemDB.
- [x] Add direct SQLite runtime coverage for the remaining practical write
      families such as narrow `MATCH ... SET`, direct relationship delete, and
      representative traversal-backed `MATCH ... CREATE` / `MATCH ... MERGE`
      flows once their rendered SQL is SQLite-safe.
- [x] Add a second benchmark focused on end-to-end direct SQLite execution:
      initialize the documented SQLite schema, load representative fixture data,
      compile a fixed corpus of admitted Cypher queries to SQL, execute the full
      batch, and record total as well as per-query timing so we can reason about
      realistic frontend-plus-SQL runtime cost rather than compile latency
      alone.
- [ ] Tag and publish v0.1.0 only after the release-candidate checklist looks clean,
      and then create the matching GitHub release with a brief summary of scope and
      highlights.

## Future phases

- [ ] Revisit compiler performance only after measuring end-to-end request cost in a
      realistic host-runtime path, not just isolated frontend compile latency.
- [ ] Use the end-to-end SQLite execution benchmark to separate setup cost,
      compile cost, and execute cost before drawing performance conclusions or
      chasing optimizations.
- [ ] Add a narrow compiled-query caching layer or cacheability experiment once real
      repeated-query usage patterns are clearer.
- [ ] Profile the hottest CypherGlot parse/validate/normalize/compile paths before
      attempting micro-optimizations, and keep any performance work tied to the
      checked-in benchmark baseline.
- [ ] Clean up the SQLGlot comparison corpus so the benchmark runs without avoidable
      dialect-warning noise while keeping the workload representative.
