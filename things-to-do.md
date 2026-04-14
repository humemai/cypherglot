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

## Type-aware schema migration plan

Replace the current JSON-backed relational assumptions with a generated
type-aware schema that becomes the primary CypherGlot backend contract.

Status: complete.

Release scope for `v0.1.0`:

- full backend support is SQLite
- DuckDB support is read-only and limited to admitted read-path families
- DuckDB write-path support is explicitly out of scope for `v0.1.0`

Working policy for this migration:

- no backward-compatibility work is required for the old generic JSON-backed
      schema because there are no downstream users to preserve yet
- do not spend roadmap time preserving, documenting, or softening the old
      schema as a supported fallback
- when docs or code still mention the old schema, the goal is to delete or
      replace that framing, not to present it as a long-term compatibility story

- [x] Freeze the architecture decision clearly in repo docs: CypherGlot's
      primary SQLite backend contract is a generated type-aware schema, not the
      generic JSON-backed `nodes` / `edges` / `node_labels` layout.
- [x] Define the new schema contract precisely in repo-owned docs and code:
      one table per node type, one table per edge type, typed property columns,
      stable primary keys, and the minimum required index families for lookup,
      one-hop traversal, multi-hop traversal, and relationship-heavy reads.
- [x] Decide the node-label model explicitly for the admitted type-aware path:
      enforce exactly one storage-driving node type label per admitted
      type-aware node for now, and defer any real multi-label strategy until a
      concrete product need justifies comparing options such as secondary-label
      relations, fixed taxonomy columns, or a typed hierarchy model.
- [x] Decide the behavior for unlabeled or polymorphic node reads such as
      `MATCH (n) RETURN n`: reject them in the admitted type-aware path for
      now and require explicit labels for admitted type-aware reads rather than
      quietly compiling polymorphic scans across all `cg_node_*` tables.
- [x] Decide the identifier contract for the admitted type-aware path:
      keep per-table integer ids for table-anchored foreign keys and do not
      define a backend-wide global-id contract yet; only introduce a deliberate
      tagged-id or global-id design later if we actually need cross-type
      generic references or polymorphic endpoints.
- [x] Decide how CypherGlot receives schema metadata: explicit user-supplied
      graph schema, inferred schema materialized before compile time, or a
      hybrid path with a normalized internal schema descriptor.
      Current direction: keep the compiler contract centered on an explicit
      in-memory schema object first (`GraphSchema` / `NodeTypeSpec` /
      `EdgeTypeSpec`), and treat JSON/YAML loaders as optional convenience
      layers on top of that contract rather than as the core compiler API.
- [x] Make the output contract explicit for the type-aware path: removing JSON
      from storage is not enough; the long-term compiler target should also
      stop relying on SQL JSON constructors such as `JSON_OBJECT(...)` and
      `JSON_ARRAY(...)` for ordinary whole-entity and introspection returns,
      so result shaping happens through typed relational outputs or an
      upper-layer runtime contract instead of SQL-side JSON materialization.
      Done: the public type-aware compiler contract is now strict relational
      output only, and admitted compile/render/runtime coverage rejects
      whole-entity and introspection returns that still depend on SQL JSON
      constructors instead of silently baking them into emitted SQL. Decision:
      the long-term type-aware target is strict relational output for emitted
      SQL so broad SQL-dialect portability stays intact; any remaining
      JSON-shaped output is now just residual cleanup, not a public contract or
      compatibility behavior.
- [x] Remove the remaining `JSON_OBJECT(...)` and `JSON_ARRAY(...)` usage from
      the repo entirely once the last temporary compatibility paths are gone,
      including emitted SQL, compile/render expectations, and any other test or
      helper surface that still bakes SQL-side JSON materialization into the
      product path.
      Done: the last remaining product-path constructor usage has been removed.
      Literal `UNWIND` lowering no longer emits `JSON_ARRAY(...)`, the obsolete
      SQLite `JSON_OBJECT(...)` render rewrite has been deleted, dead generic
      JSON-backed whole-entity/write helpers now hard-fail instead of silently
      materializing SQL-side JSON, and the stale compile/render expectations
      that assumed that legacy behavior have been removed. The source and test
      tree is now clean of `JSON_OBJECT(...)` / `JSON_ARRAY(...)` usage, and the
      full `cypherglot` test suite is green.
      not migration of any still-supported public mode.
  - [x] Remove stale JSON-shaped compile/render expectations for strict
        relational-output slices that already either expand into dotted typed
        columns or intentionally reject unsupported `labels(...)` /
        `keys(...)` helper mixes.
  - [x] Remove the remaining JSON-shaped compile/render expectations that
        still covered bounded variable-length plus fixed-length grouped
        helper/entity slices where the repo had not yet been switched over to
        dotted-column output or explicit relational rejection.
  - [x] Remove JSON constructor usage from emitted type-aware SQL on the
        remaining product-path read surfaces, not just from compile/render
        expectations.
        Done: the admitted type-aware read surfaces now either emit typed
        relational columns or reject unsupported helper/entity shapes instead
      of producing SQL-side JSON constructors, the old internal
      `structured_output` switch has been removed from `compile.py`, and the
      curated DuckDB parity corpus has been trimmed so no-longer-admitted
      helper/entity queries are no longer presented as part of the current
      release subset.
  - [x] Hard-cut the public API default output for the type-aware path to
        strict relational output so whole-entity and helper returns no
        longer depend on SQL-side JSON materialization as the repo-wide
        default behavior.
- [x] Replace compiler assumptions that currently lower through generic
      `nodes`, `edges`, `node_labels`, and JSON extraction expressions so
      lowering instead targets generated type-aware tables and typed columns.
      Done for the current admitted type-aware subset: labeled single-node `MATCH` reads and explicit one-hop
      relationship reads now lower through the type-aware schema context,
      including repeated-alias self-loop one-hop reads on homogeneous edge
      types.
      Narrow standalone single-node `OPTIONAL MATCH ... RETURN` reads now lower
      through the same type-aware node path too.
      Narrow `MATCH ... WITH ... RETURN` reads over those same simple node and
      one-hop relationship sources now lower through the type-aware path too,
      including the first scalar-function projection slice over rebound scalar
      bindings and entity fields plus the first admitted aggregate slice over
      rebound entity fields plus the first searched CASE and predicate-return
      slices, and direct plain `MATCH ... RETURN` reads now cover the first
      grouped and ungrouped aggregate slice plus the first scalar-function
      projection slice over typed fields. The first fixed-length outgoing
      multi-hop `MATCH ... RETURN` field-projection slice and the first
      chain-sourced `MATCH ... WITH ... RETURN` node-binding slice now lower
      through the type-aware path too, and the first direct plus rebound
      relationship-endpoint introspection slice over those same fixed-length
      chains is now admitted as well. The first grouped fixed-length multi-hop
      aggregate slice now also lowers through the type-aware path for both
      direct reads and chain-backed `WITH` rebound relationship bindings, and
      the core admitted aggregate family (`count`, `sum`, `avg`, `min`, and
      `max`) now has representative direct plus rebound grouped and ungrouped
      coverage over those same admitted chains, including plain
      relationship-count and `count(*)` shapes. The first bounded outgoing
      variable-length type-aware slice now also lowers for direct plain
      `MATCH ... RETURN` node-field, representative whole-node,
      representative node-helper, representative scalar-function,
      representative id(...), grouped-count, representative grouped whole-node, helper
      (`properties(...)`, `labels(...)`, `keys(...)`), and scalar-function
      reads plus representative grouped id(...), and representative non-count aggregate reads plus the
      matching `MATCH ... WITH ... RETURN` node-binding, representative
      whole-node, representative node-helper, representative scalar-function,
      representative id(...), grouped-count, representative grouped whole-node, helper
      (`properties(...)`, `labels(...)`, `keys(...)`), and scalar-function
      reads plus representative grouped id(...), and representative non-count aggregate reads over homogeneous
      repeated relationship types, and that same admitted slice now also
      covers zero-hop `*0..N` cases whenever both endpoints stay on the same
      typed node table. The admitted direct and rebound relational-output path
      now also keeps endpoint-field introspection such as
      `startNode(rel).name` / `endNode(rel).field` on typed columns instead of
      misrouting those reads through relationship-property handling. Remaining
      JSON-constructor elimination is tracked by the explicit
      `JSON_OBJECT(...)` / `JSON_ARRAY(...)` cleanup item above, and broader
      later traversal breadth belongs under the separate broader traversal
      follow-up rather than this compiler-assumptions box.
- [x] Rework whole-entity reconstruction and graph introspection helpers such
      as `properties(...)`, `labels(...)`, `keys(...)`, `startNode(...)`, and
      `endNode(...)` so they remain correct over the generated type-aware
      schema.
      Done for the admitted type-aware subset: direct single-node and one-hop
      relationship reads now support whole-entity reconstruction plus
      representative `properties(...)`, `labels(...)`, `keys(...)`,
      `startNode(...)`, and `endNode(...)` slices over generated tables, and
      narrow `MATCH ... WITH ... RETURN` now supports the same helper family
      when the needed endpoint entity bindings are explicitly carried forward,
      including representative `id(...)`, `type(...)`, `properties(...)`,
      `keys(...)`, `startNode(...)`, and `endNode(...).id` reads. The admitted
      relational-output slices now also expand representative direct and rebound
      whole-entity, `properties(...)`, and endpoint-entity returns into dotted
      typed columns instead of `JSON_OBJECT(...)` output, while strict
      relational mode continues to treat list/object-shaped helper returns such
      as `labels(...)` and `keys(...)` as unsupported unless a higher runtime
      layer reconstructs them outside emitted SQL. Remaining repo-wide JSON
      constructor elimination is tracked by the explicit `JSON_OBJECT(...)` /
      `JSON_ARRAY(...)` cleanup item above, and broader variable-length/compiler
      breadth remains tracked by the open compiler-assumptions item.
- [x] Revisit write-path lowering assumptions for `CREATE`, `MERGE`, `SET`,
      and `DELETE` so emitted SQL stays correct when node and edge storage is
      split across per-type tables instead of one generic table family.
            Done for the current admitted write subset: standalone labeled `CREATE` node and relationship programs plus
            standalone labeled `MERGE` node and relationship programs plus the first
            direct existing-endpoint and traversal-backed one-hop `MATCH ... CREATE`
            slices plus the first direct existing-endpoint and traversal-backed
            one-hop `MATCH ... MERGE` slices plus the first direct admitted
            `MATCH ... SET` node and one-hop relationship slices plus the first
            direct admitted `MATCH ... DELETE` node and one-hop relationship slices
            now lower into generated `cg_node_*` / `cg_edge_*` tables. The type-aware
            SQLite runtime now executes those admitted `MATCH ... CREATE`,
            `MATCH ... MERGE`, `MATCH ... SET`, and `MATCH ... DELETE` families in
            their current narrow forms, including standalone plus direct and
            traversal-backed `MATCH ... MERGE` idempotence slices, the admitted
            one-hop traversal-backed existing-endpoint `MATCH ... CREATE` /
      `MATCH ... MERGE` forms now lower through the type-aware path too, the
      admitted traversal-backed one-reused-node plus one-fresh-endpoint
      `MATCH ... CREATE` / `MATCH ... MERGE` forms now also lower through the
      same type-aware path in both fresh-right and fresh-left forms, and the
      first representative fresh-right and fresh-left traversal-backed
      `MATCH ... CREATE` / `MATCH ... MERGE` program slices now also have
      focused compile plus type-aware SQLite runtime proof for matched-node
      left-endpoint and right-endpoint property-filter source shapes, and the
      first representative fresh-left relationship-property plus
      left-endpoint-plus-relationship plus right-endpoint-plus-relationship
      plus all-three-filter source shapes, the
      admitted direct matched-node self-loop `MATCH ... CREATE` shape now
      lowers through the type-aware path as well and now has focused compile
      plus SQLite runtime proof too, the admitted traversal-backed
      matched-node self-loop `MATCH ... CREATE` and `MATCH ... MERGE` shapes
      now also lower through the same type-aware path with repeated-alias
      self-loop source handling and now have focused compile plus SQLite
            runtime proof too for both fresh-right and fresh-left endpoint forms plus
            existing-endpoint SQL forms,
            the admitted direct
      one-matched-node plus one-fresh-endpoint `MATCH ... CREATE`
      relationship shape now lowers through that same path too in both
      fresh-right and fresh-left forms, the admitted standalone self-loop
      `MERGE` relationship shape now uses the same type-aware path with
      idempotent existing-node reuse, and the admitted direct matched-node
            self-loop `MATCH ... MERGE` relationship shape now lowers through the
            same type-aware path too, and the admitted direct one-matched-node plus
      one-fresh-endpoint `MATCH ... MERGE` relationship shape now lowers
      through the same type-aware path too in both fresh-right and fresh-left
      forms. The first direct one-matched-node plus one-fresh-endpoint
            `MATCH ... CREATE` / `MATCH ... MERGE` program slices now also have
            focused compile plus type-aware SQLite runtime proof for matched-node
            property-filter source shapes in representative fresh-right and
            fresh-left forms. The direct self-loop relationship
            `MATCH ... SET` and `MATCH ... DELETE` shapes now also compile correctly
            through the generic and type-aware paths instead of emitting duplicate
            endpoint aliases, and now have focused type-aware SQLite runtime
            proof too. The first direct one-hop relationship `MATCH ... SET` and
            `MATCH ... DELETE` slices now also have focused compile plus
            type-aware SQLite runtime proof when the admitted filter is on the
            right endpoint properties instead of only the left endpoint, and
            now also when the admitted filter is on relationship properties,
            including the admitted combined right-endpoint-plus-relationship
            filter forms, the admitted left-endpoint-plus-relationship forms,
            the admitted both-endpoint property filter forms, and the
            admitted all-three-filter forms.
            The first one-hop traversal-backed existing-endpoint
            `MATCH ... CREATE` slices now also have focused compile plus
            type-aware SQLite runtime proof for admitted right-endpoint,
            relationship-property, left-endpoint-plus-relationship,
            both-endpoint, and all-filter source shapes, including a
            representative filtered cross-edge-type slice where the source and
            created relationship tables differ.
            The first one-hop traversal-backed existing-endpoint
            `MATCH ... MERGE` slices now also have focused compile plus
            type-aware SQLite runtime proof for admitted right-endpoint,
            relationship-property, left-endpoint-plus-relationship,
            both-endpoint, and all-filter source shapes, including the same
            representative filtered cross-edge-type slice.
            The first direct separate-node existing-endpoint `MATCH ... CREATE`
            and `MATCH ... MERGE` slices now also have focused compile plus
            type-aware SQLite runtime proof for admitted left-endpoint,
            right-endpoint, and both-endpoint property-filter source shapes.
            The old generic SQLite runtime file now exercises these public write
            entrypoints against generated `cg_node_*` / `cg_edge_*` tables too,
            so the mainstream write-path smoke surface no longer depends on the
            legacy generic schema. Broader traversal-heavy or otherwise
            unadmitted write families can stay with the later broader write-side
            traversal follow-up box instead of blocking this admitted-subset
            migration milestone.
- [x] Introduce a clean schema-generation layer in source code rather than
      scattering table-name and column-name derivation across compiler modules.
- [x] Update fixtures, helper utilities, and test harness setup so compiler and
      rendering tests target the new schema contract instead of the JSON-backed
      one.
      Done for the main compiler/render harnesses: SQLite runtime coverage now
      includes a parallel type-aware
      in-memory harness with seeded generated tables for the first admitted
      single-node read, one-hop read, standalone single-node `OPTIONAL MATCH`,
      narrow `MATCH ... WITH ... RETURN`, the first fixed-length multi-hop
      read and chain-backed `WITH` execution slices, the first grouped
      fixed-length chain aggregate execution slices, grouped aggregate
      execution, the first direct and rebound graph-introspection execution
      slices, the first bounded outgoing type-aware variable-length direct
      and chain-backed `WITH` execution slices, including representative
      node-helper and non-count aggregate execution plus zero-hop `*0..N`
      execution when both endpoints stay on the same typed node table,
      the first direct and rebound fixed-length chain relational
      endpoint execution slices, the first direct and rebound fixed-length
      chain relational whole-entity and `properties(...)` execution slices,
      the first direct and rebound grouped relational fixed-length chain
      whole-entity and `properties(...)` execution slices, the first direct
      and rebound fixed-length chain JSON helper execution slices for
      `properties(...)`, `labels(...)`, `keys(...)`, and endpoint-field
      returns, the first direct and rebound grouped fixed-length chain JSON
      helper execution slices, the next direct and rebound complementary chain
      helper execution slice for relationship `properties(...)`, node
      `keys(...)`, node `labels(...)`, and endpoint id/name fields, the
      matching grouped complementary helper execution slice, direct
      relational whole-entity execution slices, and the next direct plus
      rebound bounded variable-length grouped relational whole-node and
      `properties(...)` execution slice. The same type-aware harness now also
      executes the current narrow admitted write subset for standalone,
      direct-existing-endpoint, traversal-backed existing-endpoint, and
      one-reused-node-plus-one-fresh-endpoint `CREATE` / `MERGE` families plus
      the first direct `SET` / `DELETE` families, while render regression
      coverage already targets `cg_node_*` / `cg_edge_*` output across the
      admitted read subset. The remaining legacy generic-schema expectations
      now fit the cleanup box below rather than the harness/setup box itself.
- [x] Rewrite or replace tests that assert generic-table joins, JSON property
      extraction, generic whole-entity reconstruction semantics, or SQL JSON
      constructor output that is only serving as temporary compatibility
      behavior.
      Done for the current migration phase: the repo now has broad type-aware compile, render, and SQLite
      runtime regression suites over generated `cg_node_*` / `cg_edge_*`
      tables, including strict relational-output checks and the admitted
      write subset, and the old generic SQLite runtime plus DuckDB runtime /
      parity files now run on type-aware generated tables instead of the
      legacy `nodes` / `edges` / `node_labels` layout. The late
      public-entrypoint/default-schema compile-render assertion block is now
      also on the type-aware fixture path, so the remaining JSON-shaped
      variable-length helper/entity expectations and similar fallback-oriented
      assertions now belong under the explicit repo-wide `JSON_OBJECT(...)` /
      `JSON_ARRAY(...)` removal box rather than blocking this broader legacy-
      test cleanup milestone; the mainstream compiler/render suites are mostly
      aligned on the type-aware contract.
- [x] Add focused regression coverage for type-aware lowering across the core
      admitted query families: point reads, one-hop reads, multi-hop reads,
      aggregates, graph introspection, and the admitted write subset.
      Done for the current admitted subset: compile and render regression coverage now includes explicit
      relational-only contract checks for the type-aware path, proving that
      the admitted subset still handles typed scalar returns while rejecting
      whole-entity and introspection shapes that still depend on SQL JSON
      constructors, and the grouped relational regression slice now covers
      direct and rebound whole-node, whole-relationship, and whole-endpoint
      returns so grouped entity expansion stays in dotted-column form instead
      of regressing back to packed JSON expectations. The first type-aware
      standalone single-node `OPTIONAL MATCH` regression slice is now covered
      in compile and render tests for both scalar output and relational entity
      grouping, and SQLite runtime execution now covers the first grouped
      direct aggregate, grouped `MATCH ... WITH ... RETURN`, the first direct
      and rebound graph-introspection helper outputs, the first fixed-length
      multi-hop direct and chain-backed `WITH` slices, the first direct and
      rebound fixed-length chain endpoint/introspection returns, the first
      grouped fixed-length chain aggregate slices, the first direct and
      rebound ungrouped non-count fixed-length chain aggregate slices, the
      first direct and rebound ungrouped fixed-length chain
      relationship-count aggregate slices,
      first direct and rebound fixed-length chain `count(*)` aggregate
      slices in both grouped and ungrouped form, the rebound grouped
      fixed-length chain relationship-count aggregate slice, the direct
      grouped non-count fixed-length chain relationship-field aggregate slice,
      the next representative `min(...)` and `max(...)` fixed-length chain
      aggregate slices on direct grouped or ungrouped reads, the matching
      representative rebound `sum(...)` and `min(...)` fixed-length chain
      aggregate slices on `MATCH ... WITH ... RETURN` reads, the first bounded
      outgoing type-aware variable-length direct scalar and grouped-count
      regression slices plus the matching `MATCH ... WITH ... RETURN`
      node-binding and grouped-count slices, the first direct
      and rebound
      fixed-length chain relational endpoint expansions, the first direct and
      rebound fixed-length chain relational whole-entity and `properties(...)`
      expansions, the first direct and rebound grouped relational fixed-length
      chain whole-entity and `properties(...)` expansions, the first direct
      and rebound fixed-length chain relational rejection coverage for
      unsupported `labels(...)` and `keys(...)` helper returns, the first
      direct and rebound fixed-length chain JSON helper regression slice for
      `properties(...)`, `labels(...)`, `keys(...)`, and endpoint-field
      returns, the first direct and rebound grouped fixed-length chain JSON
      helper regression slice, the next direct and rebound complementary chain
      helper regression slice for relationship `properties(...)`, node
      `keys(...)`, node `labels(...)`, and endpoint id/name fields, the
      matching grouped complementary helper regression slice, grouped
      relational whole-entity slices over the generated tables, and the first
      direct existing-endpoint plus traversal-backed type-aware
      `MATCH ... CREATE` / `MATCH ... MERGE` compile-plus-SQLite-runtime write
      regression slices in the admitted narrow forms, plus the first direct
      type-aware `MATCH ... SET` and `MATCH ... DELETE` node and one-hop
      relationship compile-plus-SQLite-runtime regression slices, including
      one-hop traversal-backed existing-endpoint `MATCH ... CREATE` /
      `MATCH ... MERGE` execution.
- [x] Update docs throughout the repo to remove stale references to JSON-backed
      graph storage, generic `nodes` / `edges` lowering, generic property
      extraction assumptions, and any accidental implication that SQL JSON
      constructors remain part of the intended long-term type-aware contract.
      Done for the current doc set: top-level README, schema/docs guides, the
      public-entrypoints guide, and benchmark docs now consistently treat the
      type-aware schema as the only intended contract instead of presenting the
      old generic JSON-backed layout as something to preserve.
- [x] Update examples so any schema-dependent SQL or explanation text reflects
      generated type-aware tables instead of the generic graph schema.
      Done for the current repo surfaces: there is no top-level `examples/`
      tree in CypherGlot, and the public onboarding pages (`README` plus the
      getting-started docs) are now schema-agnostic or focused on the intended
      type-aware direction rather than preserving old-schema framing.
- [x] Keep the benchmark suite aligned with the architecture decision: retain
      schema-shape benchmarks as evidence, but treat the type-aware layout as
      the product target and update runtime/compiler docs where they still read
      as if generic JSON-backed storage were the main path.
      Done for the current benchmark/docs pass: the schema-shape benchmark is
      still the evidence trail, while the benchmark guide plus compiler/public
      entrypoint docs now point at the type-aware layout as the product target
      instead of keeping old-schema framing around as a compatibility story.
- [x] Keep release-scope wording honest across docs, tests, and examples:
      `v0.1.0` means full SQLite support and DuckDB read-only support, not full
      backend parity.
      Done: the curated DuckDB admitted-read parity suite is green for the
      promised read-only scope, the current tests reflect the admitted `WITH`
      subset instead of over-claiming unsupported shapes, and the current
      README/docs wording is SQLite-first with DuckDB read-only rather than a
      backend-parity claim.
- [x] Add a library-safe logging and diagnostics story for `v0.1.0`:
      use the standard `logging` module, stay silent by default, do not
      configure the root logger, and emit useful debug-level events around
      parse, validate, normalize, compile, render, schema-context selection,
      and backend-dialect decisions.
- [x] Define logging-level semantics clearly for `v0.1.0`: `DEBUG` for compile
      pipeline decisions and lowered-shape details, `INFO` for explicit
      high-level lifecycle events only if they add real operator value,
      `WARNING` for degraded or compatibility paths, and `ERROR` or
      `exception(...)` for internal failures rather than ordinary admitted-
      subset rejection.
- [x] Add tests and docs for logging behavior so CypherGlot remains quiet in
      normal library use but becomes inspectable when a host runtime enables
      debug logging.
- [x] Define the cleanup story for existing test fixtures and temporary generic-
      schema assumptions in the repo: remove them quickly, convert them to the
      type-aware contract, or isolate them only where they are still needed to
      land the remaining compiler migration slices.
      Done for the current migration phase: the repo now clearly treats the
      type-aware compiler/render/runtime suites as the mainstream product-path
      coverage, while the remaining generic-schema SQLite/DuckDB runtimes and
      similar fallback-oriented assertions are the isolated compatibility
      surfaces. New migration slices should convert or delete temporary
      generic-era assertions rather than letting them spread back into the
      mainstream test harnesses.
- [x] Land the migration in small reviewable slices, but do not leave the repo
      in a half-switched state where docs, examples, tests, and compiler output
      disagree about which relational schema CypherGlot actually targets.
      Done for the current migration phase: the migration is landing in small
      slices and the docs, public contract, examples, benchmark guidance,
      mainstream compiler/render suites, and admitted type-aware runtime
      coverage now consistently describe the generated type-aware schema as the
      target. The remaining generic compatibility runtimes are isolated legacy
      surfaces, not repo-wide disagreement about the intended relational
      contract.

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

Status: complete for the current release target.

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
- [x] Add a narrow read-only DuckDB SQL target for admitted Cypher OLAP-style
      reads over the existing `nodes` / `edges` / `node_labels` schema,
      starting with backend-aware JSON property access and whole-entity
      reconstruction instead of treating `to_sql(..., dialect='duckdb')` as a
      free portability claim, then keep closing the remaining gaps until DuckDB
      can execute the full admitted Cypher read subset with SQLite-equivalent
      read semantics.
  - [x] Land the first DuckDB renderer path for admitted reads, including
        backend-aware JSON scalar extraction, whole-entity reconstruction,
        numeric aggregate/comparison handling, numeric property ordering, and
        `min(...)` / `max(...)` over JSON-backed property reads.
  - [x] Add direct DuckDB runtime tests plus a curated SQLite-vs-DuckDB parity
        harness for admitted reads, and keep growing that suite with string,
        numeric, boolean, and relationship-property read shapes.
  - [x] Keep expanding the parity corpus toward the full admitted read subset,
        then fix the next renderer gaps as parity failures expose them.
    - [x] Cover ordinary reads, whole-entity returns, graph introspection,
          grouped counts, grouped `avg(...)`, properties/labels/keys,
          bounded variable-length reads, `MATCH ... WITH ... RETURN` rebound
          projections, parameterized reads, and broad `OPTIONAL MATCH` scalar
          and entity-return families in the curated SQLite-vs-DuckDB harness.
    - [x] Identify the remaining admitted read shapes that still only have
          compile coverage, and convert that list into an explicit parity
          checklist instead of inferring coverage from test files by hand.
    - [x] Add parity coverage for the remaining `MATCH ... WITH ... RETURN`
          filter families, especially `WITH WHERE` over rebound scalars,
          `id(...)` / `type(...)` filters, null checks, string predicates, and
          `size(...)` predicates.
    - [x] Add parity coverage for the remaining `MATCH ... WITH ... RETURN`
          projection families that are still compile-only, especially grouped
          `count(...)` / grouped numeric aggregates such as `max(...)`,
          no-alias outputs, `id(...)` / `type(...)` / `size(...)` variants,
          scalar literal-plus-parameter outputs, predicate outputs, null
          predicate outputs, and the broader relationship-property projection
          suites.
    - [x] Add parity coverage for the remaining plain `MATCH` read families
          that are still compile-only, especially no-alias outputs,
          scalar-literal-only and parameter-only projections,
          `id(...)` / `type(...)` / `size(id(...))` variants,
          grouped relationship plain-read counts, remaining node math/trig/log
          conversion slices that are only compile-asserted, and the broader
          relationship-property string, numeric, predicate, and null-predicate
          suites.
    - [x] Add parity coverage for the remaining `OPTIONAL MATCH` read families
          that are still compile-only, especially grouped count/entity-count
          shapes, scalar-literal-only outputs, `size(id(...))`, `id(...)`, and
          the remaining optional predicate variants beyond the currently covered
          scalar and entity-return cases.
- [x] Revisit the remaining broader write-side traversal semantics and the
      remaining variable-length cases such as open-ended ranges,
      downstream use of variable-length relationship aliases, fresh endpoint
      creation with broader MERGE semantics, multi-fresh-node traversal writes,
      and other mixed traversal/write shapes once the simpler high-coverage
      read and write families are stable.
      Done as a roadmap decision for the current migration phase: the simpler
      admitted traversal-backed write families are now in much better shape,
      including representative existing-endpoint, self-loop, and
      one-fresh-endpoint `MATCH ... CREATE` / `MATCH ... MERGE` slices with
      direct SQLite runtime coverage, and the broader traversal/write surface
      is now explicitly treated as out of scope for `v0.1.0` rather than a
      blocker inside the main type-aware migration checklist.
- [x] After the Phase 8 and Phase 9 admitted subset stabilizes, add much broader
      direct-execution unit coverage that actually runs derived SQL against the
      documented SQLite schema for representative admitted reads and writes,
      instead of relying mainly on compile-string assertions.
      Done for the current admitted subset: the repo now carries broad direct-
      execution SQLite coverage across the legacy documented schema plus the
      generated type-aware schema, including representative admitted reads,
      grouped aggregates, introspection/helpers, bounded variable-length reads,
      `MATCH ... WITH ... RETURN`, relational-output reads, and the current
      narrow admitted write families for `CREATE`, `MERGE`, `SET`, and
      `DELETE`.
- [x] Re-estimate practical coverage only from admitted, tested, and documented
      behavior instead of parser breadth alone, and describe the current target
      honestly as a practical mainstream single-hop read-heavy onboarding subset
      rather than full Cypher parity.

## Phase 10

File-size cleanup policy.

Status: complete.

- [x] Review oversized handwritten Python modules opportunistically when they are
      touched for substantive feature work, and split or refactor them if they are
      long without a clear structural reason.
      Done: the migration work already forced a real review of the handwritten
      module sizes. `compile.py` was the main structural risk and has already
      been split by moving the type-aware variable-length lowering path into a
      dedicated helper module. The remaining larger handwritten modules were
      checked during this pass and are being left alone for now because further
      splitting would be line-count-driven cleanup rather than clearly justified
      product work.
- [x] Treat generated parser artifacts as exempt from the normal handwritten file-size
      threshold.
      Done: checked-in ANTLR outputs already live under `src/cypherglot/generated/`,
      are explicitly labeled as generated artifacts, and have a dedicated
      regeneration/check flow in docs and CI instead of being treated like
      ordinary handwritten modules.
- [x] Keep `src/cypherglot/compile.py` from turning into the next monolith by
      splitting it when the next substantial feature forces broader edits there.
      Done: the type-aware variable-length read and `MATCH ... WITH ... RETURN`
      lowering path now lives in a dedicated helper module instead of staying
      embedded in the main compiler file, so the next traversal-heavy edits do
      not have to keep inflating `compile.py` itself.
- [x] Prefer finishing remaining roadmap phases before doing line-count-driven cleanup
      in otherwise healthy handwritten modules.
      Done: current work is still prioritizing admitted-subset coverage,
      migration, docs, and release-scope follow-through rather than running a
      separate line-count cleanup campaign across healthy handwritten modules.

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
- [ ] Before pushing the remaining benchmark/performance work into `Future
      phases`, do one more focused refresh pass on
      `scripts/benchmarks/benchmark_compiler.py` and
      `scripts/benchmarks/benchmark_sqlite_runtime.py` so their corpora, CLI
      framing, defaults, and checked-in baselines reflect the chosen
      type-aware graph-to-table contract and the admitted current release
      subset. `scripts/benchmarks/benchmark_sqlite_schema_shapes.py` can stay
      as the evidence trail for the schema decision rather than a script we
      need to keep revisiting before the next phase boundary.
      Started: the compiler benchmark corpus no longer includes the old
      JSON-helper metadata query shape and now uses an admitted relational
      introspection query instead, so the remaining refresh work is mostly the
      broader CLI/defaults/baseline pass rather than basic corpus cleanup.

## Phase 12

Release v0.1.0.

Status: planned.

- [ ] Tag and publish v0.1.0 only after the release-candidate checklist looks clean,
      and then create the matching GitHub release with a brief summary of scope and
      highlights.
      Started: the release-candidate page, changelog summary, tag plan, and
      pre-publish check list are already in place, and the publish/docs GitHub
      workflows are wired; local `pytest` and `mkdocs build --strict` are green
      after the final type-aware migration cleanup, so the remaining work is the
      actual green-light, tag, publish, and GitHub release step.

## Phase 13

Refactor CypherGlot for equal multi-dialect SQL support.

Status: planned.

Goal for this phase:

- make equal full-support ambitions for SQLite, PostgreSQL, and DuckDB the
      primary compiler architecture constraint
- stop treating SQLite-shaped lowering plus dialect rewrite patches as the
      long-term product path
- introduce a backend-neutral graph-relational IR between normalized Cypher and
      SQLGlot AST generation so new SQL targets do not require reshaping the
      whole compiler each time
- keep support claims strict: a backend counts as supported only when the
      admitted subset executes correctly against that backend's schema/runtime
      contract, not merely when SQLGlot can render SQL text for it

- [ ] Freeze the new architecture decision clearly in docs and roadmap text:
      multi-dialect SQL support now comes first, and the compiler should evolve
      from `Cypher AST -> mostly SQLite-oriented SQLGlot AST -> dialect tweaks`
      toward `Cypher AST -> logical graph-relational IR -> backend-aware
      lowering -> SQLGlot AST -> dialect SQL text`.
- [ ] Keep the parse, validation, and normalization boundary stable where
      practical while this phase lands; the main refactor target is the SQL
      compilation architecture after normalization, not a broad churn of the
      upstream frontend seam.
- [ ] Define a small backend-neutral logical graph-relational IR for the
      admitted subset instead of lowering normalized Cypher directly into a
      mostly SQLite-shaped SQLGlot AST.
- [ ] Keep that IR driven by real admitted Cypher semantics rather than generic
      SQL completeness: typed node scans, typed edge scans, traversal,
      filtering, projection, aggregation, optional flow, admitted variable-
      length expansion, and the currently admitted write families.
- [ ] Define a backend capability model that makes backend differences explicit
      in one place rather than leaking them through ad hoc dialect conditionals,
      including recursion strategy, write-program shape, returning/id behavior,
      JSON/cast semantics, and other execution-relevant backend differences.
- [ ] Refactor the current SQLite path into an explicit backend lowerer from the
      logical IR to backend-specific SQLGlot AST so SQLite remains the reference
      executable backend without staying the hidden default shape of the whole
      compiler.
- [ ] Replace the current narrow DuckDB strategy of mostly reusing the
      SQLite-shaped AST plus renderer rewrite patches with an explicit DuckDB
      lowering path from the same logical IR, then grow it until DuckDB reaches
      full admitted-subset parity instead of a read-only carveout.
- [ ] Add a full PostgreSQL lowering path from the same logical IR rather than
      treating PostgreSQL support as a mere SQLGlot rendering target, including
      a backend-specific schema/runtime contract for the admitted subset.
- [ ] Add backend-specific schema-generation and DDL support where needed so the
      backend story is complete for SQLite, PostgreSQL, and DuckDB instead of
      assuming one SQLite-first DDL contract underneath all rendered SQL.
- [ ] Keep SQLGlot as the emitted SQL AST and dialect-rendering layer, but make
      backend-specific AST shaping an explicit lowering concern instead of
      relying mainly on renderer-time tweak passes.
- [ ] Retain small backend-specific SQLGlot rewrite passes only where they are
      still the clearest final cleanup step after backend lowering, and do not
      let rewrite growth become the primary multi-backend architecture.
- [ ] Add backend-parity regression layers for the admitted subset:
      normalized-Cypher-to-IR tests, IR-to-backend-SQLGlot lowering tests, and
      direct execution/runtime parity tests for SQLite, PostgreSQL, and DuckDB.
- [ ] Update the public backend support policy and docs so CypherGlot stops
      presenting DuckDB as read-only if that parity work lands, and instead
      documents equal supported-backend expectations across SQLite,
      PostgreSQL, and DuckDB.
- [ ] Revisit benchmark coverage after the IR/lowering split lands so compile
      latency, backend-lowering cost, and backend-specific runtime parity are
      all measured against the new architecture instead of the older
      SQLite-shaped pipeline.

## Future phases

- [ ] Revisit the broader write-side traversal semantics and the remaining
      variable-length cases such as open-ended ranges, downstream use of
      variable-length relationship aliases, fresh endpoint creation with
      broader `MERGE` semantics, multi-fresh-node traversal writes, and other
      mixed traversal/write shapes once the admitted `v0.1.0` subset has been
      out in the world long enough to justify expanding beyond the current
      high-confidence read/write surface.
      Started: release docs and the admitted-subset guide now explicitly treat
      broader write-side traversal semantics plus broader variable-length forms
      as later-phase work, while the current admitted traversal-backed write
      subset already has representative compile and SQLite runtime proof.

- [ ] Revisit compiler performance only after measuring end-to-end request cost in a
      realistic host-runtime path, not just isolated frontend compile latency.
      Started: checked-in compiler and SQLite runtime benchmark baselines now
      give us both isolated compile numbers and end-to-end direct-execution
      numbers, so a later performance pass can anchor on real runtime cost
      instead of frontend-only intuition.
- [x] Use the end-to-end SQLite execution benchmark to separate setup cost,
      compile cost, and execute cost before drawing performance conclusions or
      chasing optimizations.
      Done for the current benchmark harness: the SQLite runtime benchmark now
      reports setup-stage timings plus separate compile, execute, and
      end-to-end percentiles so later performance work has a cleaner baseline.
- [ ] Add a narrow compiled-query caching layer or cacheability experiment once real
      repeated-query usage patterns are clearer.
      Started: the checked-in benchmark corpus and runtime harness now give us a
      reasonable place to measure repeated-query behavior when we decide to test
      cacheability, but no cache layer has been introduced yet.
- [ ] Profile the hottest CypherGlot parse/validate/normalize/compile paths before
      attempting micro-optimizations, and keep any performance work tied to the
      checked-in benchmark baseline.
      Started: the repository now has checked-in compiler/runtime baselines and
      percentile reporting, so a future profiling pass can stay tied to the
      same benchmark anchors rather than ad hoc local timings.
- [ ] Clean up the SQLGlot comparison corpus so the benchmark runs without avoidable
      dialect-warning noise while keeping the workload representative.
      Started: the checked-in benchmark docs and baseline already include a
      dedicated SQLGlot comparison corpus; the remaining work is trimming the
      noisy entries without losing the usefulness of that side-by-side anchor.
- [ ] Our graph-to-table schema is solid, but let's revisit this if we can do better.
      Started: the type-aware graph-to-table contract is now documented in the
      README and schema-contract guide and is exercised across the admitted
      compiler/runtime path, so any revisit can start from a much clearer
      baseline than the earlier JSON-backed transition period.
