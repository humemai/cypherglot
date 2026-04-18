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

The broad type-aware storage migration is now complete for the current
release contract. The reopened follow-up closed the remaining contract
gaps around JSON-valued property types, strict relational helper/output
surfaces, and DuckDB's rewrite-heavy compatibility path.

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

### Post-migration cleanup follow-up

These are follow-up cleanup and evidence items after the migration itself,
not blockers for calling the type-aware schema migration complete.

- [x] Finish the post-migration compiler-contract purge so the type-aware
      schema and general relational IR are the only live internal contract,
      rather than something the top-level compiler enforces while helper layers
      still carry dead schema-less branches.
      Done: `compile.py` had already been pared back so the main admitted
      write paths no longer carried dead schema-less `graph_schema is None`
      branches in `CREATE`, `MERGE`, `SET`, and `DELETE` lowering, and the
      remaining dead schema-less write-helper branches in
      `_compile_write_helpers.py` have now been removed too. The temporary
      `_removed_schema_less_write_sql(...)` shim has been deleted, and the
      live type-aware compiler entrypoints no longer route through a hidden
      schema-less write fallback. Any remaining legacy generic read-helper
      code is now isolated outside the active type-aware compile path rather
      than part of the migration contract itself.
- [x] Re-run and extend the compiler benchmarks after that purge so the repo
      has explicit evidence for what this cleanup buys, especially whether
      DuckDB compile latency drops once the backend no longer pays for legacy
      helper shapes and rewrite-heavy fallback logic.
      Done for the current cleanup pass: the focused compile/render/type-aware
      SQLite runtime suite is green after the helper purge, and the compiler
      benchmark smoke pass completed successfully across the benchmark
      entrypoints plus the SQLite, DuckDB, and PostgreSQL backends, with the
      baseline written to
      `scripts/benchmarks/results/compiler-post-migration-cleanup.json`.

### Reopened follow-up

These are not generic cleanup nice-to-haves anymore. They are the remaining
work needed to make the type-aware migration internally consistent with the
relational-storage decision and the compiler-latency target.

- [x] Remove JSON-valued property types from the public schema contract so the
      generated type-aware path is scalar-relational only rather than still
      admitting `json` as a logical property family in `GraphSchema` and the
      schema text surface.
      Done: `cypherglot.schema` no longer maps a `json` logical type for any
      backend, the graph-native schema text parser no longer accepts `JSON`
      property declarations, the direct schema-contract guide now describes
      typed properties as scalar fields only, and focused schema plus
      schema-command regression coverage now proves that the removed logical
      type is rejected instead of silently carried forward.
- [x] Update the repo docs and public contract wording so the target type-aware
      schema no longer describes properties as scalar-or-JSON-like, and so the
      admitted relational path is explicit that JSON-valued properties are out
      of scope rather than a lingering maybe-supported edge case.
      Done: the README plus the schema-contract, public-entrypoints, and
      admitted-subset guides now describe the product path in typed-column
      terms, remove stale generic JSON-backed walkthrough wording, and reflect
      the current literal-only `UNWIND` admission instead of preserving the old
      parameter-driven JSON-expansion path in the docs.
- [x] Audit the active compile/render/runtime codepaths and delete any
      remaining product-path assumptions that still treat JSON-valued property
      storage as something the type-aware relational contract should carry.
      Done so far: parameter-backed `UNWIND` is no longer admitted, so the last
      live compiler dependency on SQL JSON table expansion (`JSON_EACH(...)`)
      has been removed from `src/cypherglot`; the active compiler source now
      contains no remaining `json` references under `src/cypherglot`, so the
      product-path compiler and lowering code no longer carry JSON-valued
      storage assumptions. The remaining repo-level `json` mentions now sit in
      roadmap history, benchmark/file-format surfaces, and rejection coverage
      that proves removed support stays removed.
- [x] Tighten the remaining helper and projection surfaces so object/list-shaped
      helpers such as `properties(...)`, `labels(...)`, `keys(...)`,
      `startNode(...)`, `endNode(...)`, and whole-entity reconstruction are
      either expressed as strict relational dotted-column output or handled by
      an upper runtime layer, but never by reintroducing JSON-valued storage or
      SQL-side JSON-shaped compatibility behavior.
      Done: the strict relational type-aware path now expands admitted
      whole-entity, `properties(...)`, and whole `startNode(...)` /
      `endNode(...)` endpoint returns into dotted typed columns across direct,
      fixed-length-chain, variable-length, and `WITH` rebound slices, while
      unsupported list/object introspection helpers such as `labels(...)` and
      `keys(...)` are rejected consistently in compile, render, and runtime
      coverage instead of silently routing through SQL-side packaging or legacy
      storage assumptions.
- [x] Replace the current DuckDB post-lowering rewrite pipeline with a direct
      type-aware DuckDB lowering path so DuckDB can stop after the shared IR
      plus backend lowering stage instead of compiling generic SQL first and
      then paying for whole-tree repair passes.
- [x] Eliminate DuckDB rewrite passes incrementally by removing the categories
      of fallback output that force them, starting with JSON-extract-driven
      property access and the extra numeric/order/min-max repairs layered on
      top of that compatibility shape.
      Done so far: the dead JSON-extract-specific DuckDB rewrite branch has now
      been removed because the active type-aware compiler path no longer emits
      `JSON_EXTRACT(...)` ASTs into DuckDB lowering; focused DuckDB render plus
      DuckDB/SQLite parity coverage stayed green after deleting that category.
      The old constant-order cleanup rewrite has now also been removed after
      upstream type-aware lowering stopped emitting no-op literal/parameter
      `ORDER BY` terms, and focused render plus backend coverage stayed green
      with the DuckDB `ORDER` rewrite deleted. The old DuckDB
      `LENGTH(...)`-operand rewrite has now also been removed after admitted
      type-aware `size(...)` lowering started emitting explicit text coercion
      upstream, and focused compile/render coverage stayed green with that
      backend rewrite deleted. The old DuckDB numeric-function rewrite has now
      also been removed after direct type-aware DuckDB lowering started
      emitting explicit numeric coercion for admitted scalar math functions and
      `SUM(...)` / `AVG(...)` aggregates upstream, with focused DuckDB backend,
      parity, runtime, and shared compile/render coverage staying green. The
      old DuckDB numeric-comparison rewrite has now also been removed after the
      shared type-aware predicate lowering started emitting explicit numeric
      coercion for DuckDB typed-column comparisons against numeric literals in
      `WHERE`, rebound `WITH`, and predicate-return slices, with focused
      DuckDB backend, parity, runtime, and shared compile/render coverage
      staying green after deleting that backend pass. The old DuckDB
      integer-cast rewrite has now also been removed after shared type-aware
      read and rebound `WITH` lowering started emitting truncating integer
      casts directly, so DuckDB now runs through the shared backend lowerer
      without any post-lowering AST repair pass.
- [x] Re-benchmark DuckDB compiler latency after each direct-lowering slice so
      the repo has explicit evidence for whether these removals move the
      read-only admitted subset toward the roughly `~1 ms` end-to-end compile
      target.
      Done: refreshed
      `scripts/benchmarks/results/compiler-post-migration-cleanup.json`
      after removing the final DuckDB rewrite pass. In the lightweight
      checkpoint run (`iterations=1`, `warmup=0`, `sqlglot-mode=off`), DuckDB
      backend-lowering mean latency dropped from about `827.5 us` to about
      `113.0 us`, while end-to-end compile mean latency moved from about
      `3403.7 us` to about `3124.6 us`. That confirms the rewrite removals are
      materially reducing DuckDB backend-lowering cost, but the admitted
      compile path still sits above the `~1 ms` target because render-stage
      cost remains the dominant slice.

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
- [x] Before pushing the remaining benchmark/performance work into `Future
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

Refactor CypherGlot for equal multi-dialect SQL support.

Status: in progress.

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

Working policy for this phase:

- do the bold source-first migration on purpose: change the core compiler
  source code and architecture first, then reconcile tests, examples,
  docs, and benchmarks in a follow-up pass rather than trying to keep every
  supporting surface green at each intermediate commit
- accept that the repo may temporarily contain broken tests, stale examples,
  and outdated benchmark assumptions while the source-first refactor is in
  flight, as long as that breakage is in service of landing the new
  multi-dialect architecture faster
- once the source-first architecture pass is materially in place, switch to an
  explicit stabilization pass for tests, examples, docs, and benchmarks
  before calling the phase complete

Recommended implementation order for this phase:

1. define the new logical graph-relational IR and backend capability model
2. route the current SQLite compiler path through that IR first, without trying
   to improve every surrounding surface yet
3. replace the current DuckDB rewrite-heavy path with an explicit DuckDB
   lowering path from the same IR
4. add a first-class PostgreSQL lowering path from the same IR
5. add backend-specific schema-generation and runtime-contract support for all
   three backends
6. only after those core source changes land, run the broad stabilization pass
   across tests, examples, docs, and benchmarks

- [x] Freeze the new architecture decision clearly in docs and roadmap text:
      multi-dialect SQL support now comes first, and the compiler should evolve
      from `Cypher AST -> mostly SQLite-oriented SQLGlot AST -> dialect tweaks`
      toward `Cypher AST -> logical graph-relational IR -> backend-aware
      lowering -> SQLGlot AST -> dialect SQL text`.
      Done: the README, compiler-contract guide, roadmap guide, and Phase 12
      note now all describe the new primary architecture target explicitly:
      shared graph-relational IR first, backend-aware lowerers per backend,
      SQLite-through-IR as the first landed executable milestone, and strict
      support claims based on real backend execution rather than renderer-only
      SQL text.
- [x] Keep the parse, validation, and normalization boundary stable where
      practical while the source-first compiler refactor lands; the main
      refactor target is the SQL compilation architecture after normalization,
      not a broad churn of the upstream frontend seam.
      Done for the current source-first slice: the public compile entrypoints
      still preserve the single-statement-versus-program API split while the
      backend-aware IR/lowering path now sits underneath that seam instead of
      changing the frontend contract at the same time.
- [x] Define a small backend-neutral logical graph-relational IR for the
      admitted subset instead of lowering normalized Cypher directly into a
      mostly SQLite-shaped SQLGlot AST.
      Done for the first source-first slice: `src/cypherglot/ir.py` now owns a
      repo-level graph-relational IR scaffold instead of leaving the compiler
      to dispatch directly from normalized statements alone.
- [x] Keep that IR driven by real admitted Cypher semantics rather than generic
      SQL completeness: typed node scans, typed edge scans, traversal,
      filtering, projection, aggregation, optional flow, admitted variable-
      length expansion, and the currently admitted write families.
      Done: the IR now carries admitted read-shape payloads for single-node,
      optional single-node, one-hop relationship, fixed-length chain, `UNWIND`,
      and `MATCH ... WITH ... RETURN` source forms instead of storing only a
      family label. The admitted write families now also lower from explicit
      write payload IR instead of keeping top-level backend dispatch on
      normalized statements, including standalone `CREATE`, standalone `MERGE`,
      and traversal-backed write families whose source shape now carries the
      same read IR payload instead of a normalized `MATCH` object. The
      top-level admitted SQLite read lowering for those families now consumes
      that read IR directly, including the `MATCH ... WITH ... RETURN` source
      path and `UNWIND`, and the dead generic read entry helpers that
      used to sit beside the IR path have been removed from `compile.py`. The
      SQLite lowerer now also routes those admitted read families through one
      IR-family dispatch helper instead of spelling each read case out inline
      in the main lowering branch chain. The SET/DELETE write slice is now
      also IR-shaped: the backend compiler now consumes explicit write payload
      IR for `set-node`, `set-relationship`, `delete-node`, and
      `delete-relationship` families instead of reading those families directly
      from normalized statement objects in that dispatch path. The match-driven
      fresh-endpoint relationship write slice now also consumes explicit write
      payload IR for direct and traversal-backed `MATCH ... CREATE` /
      `MATCH ... MERGE` relationship families. The remaining two-node
      `MATCH ... MERGE` and `MATCH ... CREATE ... BETWEEN NODES` relationship
      dispatch families now also consume explicit write payload IR at backend
      dispatch time. All stale normalized-statement, read-helper, sql-util, and
      type-aware-common symbols that had accumulated in `compile.py`'s import
      block during the IR-migration were removed; the only public surface that
      still re-exports through `compile.py` is what `__init__.py` and other
      callers need at the module boundary.
- [x] Define a backend capability model that makes backend differences explicit
      in one place rather than leaking them through ad hoc dialect conditionals,
      including recursion strategy, write-program shape, returning/id behavior,
      JSON/cast semantics, and other execution-relevant backend differences.
      Done for the first source-first slice: backend capability metadata now
      lives in `ir.py`, and backend binding happens before lowering instead of
      being implicit in compiler control flow.
- [x] Refactor the current SQLite path into an explicit backend lowerer from the
      logical IR to backend-specific SQLGlot AST so SQLite remains the reference
      executable backend without staying the hidden default shape of the whole
      compiler.
      Done for the first source-first slice: `compile.py` now routes through an
      explicit SQLite IR lowerer instead of owning the top-level direct
      normalized-statement dispatch path, and the public compile/render
      entrypoints now thread an explicit backend choice into that lowering path
      instead of hard-wiring SQLite implicitly at the top level.
- [x] Land the first source-first milestone by routing the current SQLite path
      through the new IR before broad reconciliation work begins elsewhere in
      the repo.
- [x] Keep the Phase 12 execution order explicit in the code migration itself:
      SQLite-through-IR first, then DuckDB-through-IR, then PostgreSQL-through-
      IR, and only then the broader repo-wide cleanup pass.
      Done for the current source-first checkpoint: the roadmap now records the
      order explicitly, and the source tree has landed the first SQLite-through-
      IR step before any broad stabilization pass.
- [x] Replace the current narrow DuckDB strategy of mostly reusing the
      SQLite-shaped AST plus renderer rewrite patches with an explicit DuckDB
      lowering path from the same logical IR, then grow it until DuckDB reaches
      full admitted-subset parity instead of a read-only carveout.
      Started: the shared IR/backend-binding path now includes an explicit
      DuckDB lowerer entrypoint, and the blanket backend-level read-only block
      has now been removed so admitted write families can flow through that
      lowerer instead of being rejected during backend binding. The first large
      render-time rewrite block has already been moved out of `render.py` and
      into compile-time DuckDB lowering so the renderer is no longer the place
      where DuckDB AST shaping primarily lives, and the first focused
      standalone type-aware DuckDB write slices now execute through that shared
      path too, including standalone `CREATE` node, standalone `CREATE`
      relationship, direct `MATCH ... SET` node programs, and the standalone
      property-keyed `MERGE` node slice, which now lowers without the old
      one-row guard loop. The direct distinct-endpoint standalone type-aware
      `MERGE` relationship slice now also lowers without the old guard loop by
      using guarded node inserts plus a guarded edge insert, and the remaining
      fresh-endpoint direct-plus-traversal relationship program assembly has
      been collapsed onto a shared helper instead of staying duplicated across
      multiple write builders. The direct non-type-aware `CREATE` relationship
      and guarded `MERGE` relationship paths now also share the same concrete
      edge-statement builder instead of compiling through an intermediate create
      program and peeling its statements back out, and the standalone
      `CREATE` relationship program no longer routes through its own one-use
      create-step wrapper either. The schema-less
      self-loop `MERGE` relationship existing-node branch now emits a direct
      guarded insert-select statement instead of another compiled loop. The
      merge-relationship program entrypoint itself no longer routes through
      separate one-use type-aware versus schema-less step-builder helpers
      either and now builds those narrow program shapes directly. The
      separate-pattern relationship-create path now also reuses the same
      shared directional edge-binding helper instead of carrying its own alias-
      to-binding wiring block. The fresh-endpoint direct and traversal-backed
      relationship-program helper now also reuses shared create-node steps and
      no longer needs the extra single-use loop-program wrapper or its own
      schema-less create branch. The schema-less distinct-endpoint
      `MERGE` relationship path no longer runs under the old outer guard loop;
      it now follows the same general shape as the type-aware path by ensuring
      endpoint-node existence first and then emitting a direct edge
      insert-select statement, and the schema-less self-loop and
      distinct-endpoint merge-edge insert SQL now also share one helper instead
      of being assembled separately in both paths. The schema-less `MERGE`
      relationship family is now compiled through one shared step builder
      instead of separate distinct-endpoint and self-loop program wrappers, and
      the remaining merge-node guard plus self-loop node-lookup SQL now also
      share one node-source helper instead of duplicating node-match filter
      assembly. The schema-less `MERGE` node path is now statement-based too
      rather than loop-backed, and the schema-less self-loop `MERGE`
      relationship path now also compiles as direct statement steps instead of
      split existing-node versus created-node branches. The direct and
      standalone type-aware `MERGE` node path no longer keeps a separate
      zero-property loop-backed guard branch either and now always compiles
      through the same guarded single-statement insert-select helper used by
      the property-keyed form. The last one-shape node-id binding source helper
      under the fresh-endpoint orchestration path has also been folded back
      into its remaining callers instead of staying as another tiny passthrough,
      and that shared fresh-endpoint orchestration helper no longer takes a
      per-caller all-matched SQL callback either; callers now pass the concrete
      all-matched SQL directly.
      The direct and
      traversal-backed fresh-endpoint relationship write builders now also
      share one endpoint-resolution helper instead of duplicating the same
      alias, binding, and direction rewiring across all four create/merge
      entrypoints, the direct plus traversal-backed fresh-endpoint
      `MATCH ... MERGE` guard-source SQL now also shares one existence/guard
      helper instead of keeping separate near-copy implementations, and the
      remaining direct fresh-endpoint `MATCH ... MERGE` entrypoint now also
      compiles through the same shared orchestration helper, so all four fresh-
      endpoint `MATCH ... CREATE` / `MATCH ... MERGE` program entrypoints now
      share the same all-matched short-circuit, fresh-endpoint resolution, and
      endpoint binding flow. The direct matched-node and traversal-backed
      matched-endpoint type-aware `MATCH ... MERGE` SQL builders now also share
      one typed edge insert-select helper instead of each open-coding the same
      column/value assembly, and the corresponding matched-endpoint type-aware
      `MATCH ... CREATE` builders now reuse that same helper too. The
      remaining shared directional edge-statement wrapper has also been folded
      into its callers, so the standalone create, separate-pattern create, and
      fresh-endpoint relationship builders now all call the common edge-insert
      statement helper directly instead of bouncing through another tiny layer.
      The statement-family program dispatcher now also builds the four narrow
      matched-node and traversal-backed fresh-endpoint relationship program
      shapes directly instead of routing through separate one-use entrypoint
      wrappers first, and it now also builds the standalone `CREATE`
      relationship, separate-pattern `CREATE` relationship, standalone
      `MERGE` node, and standalone `MERGE` relationship program shapes
      directly instead of bouncing through another set of one-use program
      wrappers. The schema-less standalone `MERGE` relationship slice also no
      longer keeps a separate self-loop SQL helper beside the distinct-endpoint
      SQL helper and now compiles both shapes through one shared schema-less
      merge-edge SQL builder instead. The
      schema-less matched CREATE and MERGE relationship builders now also share
      one schema-less edge insert-select helper across both direct and
      traversal-backed paths, and the merge-node insert builders now also
      share one guarded insert-select helper instead of each spelling out the
      same `NOT EXISTS` wrapper. The remaining fresh-endpoint relationship
      loop body now also reuses the shared directional edge-binding helper
      instead of threading explicit `from` / `to` binding strings through its
      match-resolution path, and the remaining zero-property type-aware merge-
      node guard no longer needs its own single-use source-wrapper helper. The
      shared fresh-endpoint orchestration path has now also dropped its extra
      single-use loop-body wrapper and trivial all-matched helper instead of
      routing through those tiny passthrough functions, and the last one-use
      fresh-endpoint match resolver has now been folded back into the shared
      orchestration path too. The remaining loop-backed write families now
      also build their final `CompiledCypherLoop` steps inline instead of
      routing through one extra two-call loop-program wrapper, and the
      zero-property type-aware `MERGE` node loop no longer carries an unused
      `RETURNING id` binding in its body. The schema-less merge-node and
      self-loop merge-edge paths also no longer route through a separate tiny
      node-lookup SQL helper and now build that lookup SQL directly in their
      remaining merge callers instead. The traversal-backed
      fresh-endpoint create/merge entrypoints also no longer need their own
      one-use traversal source wrappers and now call the shared node-id and
      guarded-source builders directly from the entrypoints. The schema-less
      all-matched direct and traversal-backed relationship-create builders now
      also share one resolved edge insert-select tail instead of each
      re-spelling the same label-contract validation and type-aware versus
      schema-less insert selection logic, and the traversal-backed all-matched
      merge builder now reuses that same tail after assembling its merge guard.
      The direct matched-endpoint and matched-node self-loop `MATCH ... MERGE`
      builders now also reuse one shared resolved merge insert-select tail
      instead of each carrying their own backend-specific `NOT EXISTS` guard
      assembly and insert-selection block. The schema-less
      merge-node step builder also no longer routes through separate one-use
      insert and label-insert SQL wrappers and now builds both guarded
      statements directly from one shared existence lookup, and the direct
      matched-endpoint, matched-node self-loop, plus traversal-backed
      `MATCH ... CREATE` builders and the direct, self-loop, plus
      traversal-backed `MATCH ... MERGE`
      builders now also share one resolved matched-relationship write helper
      instead of each re-spelling the same final insert-select call block.
      The schema-less
      direct matched-node self-loop plus fresh-endpoint `MATCH ... CREATE` /
      `MATCH ... MERGE` builders now also share one direct matched-node
      source-parts helper instead of each open-coding the same
      label/property/predicate filter assembly. The shared fresh-endpoint
      relationship program path also no longer takes a per-caller source-SQL
      factory callback and now selects between direct matched-node binding and
      guarded merge source assembly from concrete source parts passed by all
      four callers. The direct matched-endpoint two-node `MATCH ... CREATE` /
      `MATCH ... MERGE` SQL builders now also share one matched-node pair
      source-parts helper instead of duplicating the same left/right
      filter/predicate assembly, and the repeated type-aware relationship
      endpoint schema-contract checks in the remaining write builders now also
      route through one shared validator instead of keeping separate local
      blocks. Those source helpers are now generic node and node-pair source
      builders rather than only matched-node wrappers, and the remaining
      direct matched-node self-loop dispatcher and narrowed SQL builders now
      also reuse one shared direct matched-node relationship source helper
      instead of each reassembling the same alias, source SQL, predicate
      filters, and endpoint resolution locally, and that shared resolved
      matched-relationship write helper now also owns its merge-guard assembly
      directly instead of delegating through another tiny merge-only wrapper.
      The direct matched-endpoint two-node `MATCH ... CREATE` / `MATCH ... MERGE`
      builders now also share one direct matched-node pair relationship source
      helper instead of each rebuilding the same node-pair source and endpoint
      resolution block, and the traversal-backed dispatcher plus narrowed
      `MATCH ... CREATE` / `MATCH ... MERGE` builders now also share one
      traversal relationship source helper instead of each reassembling the
      same traversal source components, alias map, and endpoint resolution.
      The shared resolved matched-relationship write helper also no longer
      routes through a separate one-call resolved edge insert-select helper,
      and the traversal relationship source helper no longer routes through a
      separate one-call traversal source-component wrapper either.
      The standalone type-aware `MERGE` relationship builder now also reuses
      that same shared resolved matched-relationship write helper instead of
      open-coding its own type-aware edge `NOT EXISTS` guard and final insert
      assembly.
      The standalone schema-less `MERGE` relationship builder now also routes
      both its self-loop and distinct-endpoint forms through that same shared
      resolved matched-relationship write helper instead of appending its own
      edge-absence predicate and final insert-select directly.
      The shared fresh-endpoint relationship program helper now also owns the
      guarded `MATCH ... MERGE` source assembly directly instead of routing
      through a separate guarded-source helper plus tiny matched/new endpoint
      edge-column selectors, and it now builds the loop source `SELECT` in one
      place after deciding whether an extra merge guard is needed.
      The remaining
      node-lookup plus distinct-endpoint standalone `MERGE` relationship SQL
      paths now also reuse them instead of keeping their own label/property
      source assembly blocks. The label
      insert no longer routes through its one-use latest-node-id helper. The
      direct matched-node create/merge entrypoints also no longer need their
      own one-use node-id source wrappers and now build those shared source
      SQL calls directly from the entrypoints. The remaining guarded-source
      path also no longer routes through a one-use merge-exists helper and now
      builds that `NOT EXISTS` SQL inline. The shared fresh-endpoint
      orchestration path also no longer needs an endpoint-resolver callback;
      callers now pass the already-resolved endpoint patterns directly. The
      type-aware standalone `MERGE` relationship path also no longer routes
      through its own one-use dispatcher helper and now builds the shared
      resolved-write SQL directly inside the `merge-relationship` branch. The
      schema-less standalone `MERGE` relationship path now does the same, so
      that branch owns both the self-loop lookup form and the distinct-endpoint
      resolved-write form directly instead of routing through another one-use
      helper. The shared resolved matched-relationship write helper also now
      assembles both its schema-less and type-aware edge insert-select tails
      directly instead of routing through separate one-use edge insert helper
      wrappers. Its remaining schema-less relationship-absence predicate is now
      also built inline there instead of routing through another one-use helper.
      The direct matched-node self-loop CREATE/MERGE SQL builders and the
      direct matched-endpoint two-node CREATE/MERGE SQL builders have now also
      each been collapsed onto one shared write builder instead of keeping
      separate create-versus-merge helpers with the same source and final
      resolved-write assembly. The traversal-backed CREATE/MERGE SQL builders
      now also share one traversal write builder instead of keeping separate
      create-versus-merge helpers with the same traversal source and final
      resolved-write assembly. The remaining alias-based endpoint-id plus final
      resolved-write tail is now also shared in one helper across the direct,
      pair, traversal, and standalone distinct-endpoint relationship write
      paths instead of being re-spelled at each caller. The four fresh-endpoint
      direct-versus-traversal CREATE/MERGE dispatcher branches now also share
      two common program builders instead of each reassembling the same source
      extraction, all-matched short-circuit, and message plumbing around the
      shared orchestration helper, and the remaining direct-versus-traversal
      fresh-endpoint CREATE/MERGE message bundles are now centralized too
      instead of being restated inline at each branch; those shared fresh-
      endpoint program builders now own the create-versus-merge message
      selection directly, so the dispatcher branches only choose the mode. The
      direct matched-node, direct matched-endpoint pair, and traversal write
      builders now also own their fixed create-versus-merge validation and
      endpoint-contract wording instead of threading that config through each
      caller. The remaining direct and traversal source-part helpers now also
      share one candidate-based endpoint label resolver instead of each
      re-spelling the same matched-alias fallback logic. Inside the remaining
      fresh-endpoint merge wrapper, the schema-less and type-aware merge-
      existence branches now also share the same alias setup, join target, and
      final `SELECT 1` assembly instead of each rebuilding that tail
      separately, and that wrapper now also shares the edge source plus new-
      endpoint join target before branching only for the backend-specific
      predicate assembly.
      Remaining work is to keep
      tightening DuckDB-specific lowering for the remaining loop-backed write
      families, which are now mainly the underlying match-driven fresh-
      endpoint execution wrapper and the narrower type-aware zero-property
      `MERGE` node guard path, until the read-only carveout disappears in
      practice, not just in the capability gate.
      The type-aware zero-property `MERGE` node guard path has now been
      eliminated: `_compile_merge_node_program` no longer emits a
      `CompiledCypherLoop`; it now returns a single `CompiledCypherStatement`
      using `_compile_type_aware_merge_node_sql` directly (guarded
      `INSERT ... SELECT ... WHERE NOT EXISTS`). At the same time all
      schema-less dead-code branches across `_compile_write_programs.py` were
      removed (442 lines deleted, file down from 1150 to 708 lines): the
      schema-less `else` paths in `_compile_merge_node_guard_source_sql`,
      `_compile_merge_self_loop_node_lookup_source_sql`,
      `_compile_merge_self_loop_edge_insert_statement`,
      `_compile_match_merge_relationship_sql`, and
      `_compile_match_create_relationship_between_nodes_sql` are gone, the
      schema-less path of `_compile_merge_relationship_program` is gone, and
      the entirely dead `_compile_merge_relationship_guard_source_sql`
      function has been deleted. Remaining loop-backed write families are only
      the self-loop `MERGE` relationship path (the two
      `CompiledCypherLoop` steps in
      `_compile_merge_relationship_self_loop_program`) and the fresh-endpoint
      execution wrapper in `_compile_write_helpers.py`.
      The self-loop `MERGE` relationship path has now also been converted to
      two `CompiledCypherStatement`s: the first is the guarded merge-node
      statement (via `_compile_type_aware_merge_node_sql`), and the second is
      a new `INSERT INTO {edge_table} ... SELECT {alias}.id, {alias}.id ...
      FROM {node_table} AS {alias} WHERE {node props} AND NOT EXISTS (...)`
      built by `_compile_type_aware_merge_self_loop_edge_sql`. The three
      helpers it replaced — `_compile_merge_node_guard_source_sql`,
      `_compile_merge_self_loop_node_lookup_source_sql`, and
      `_compile_merge_self_loop_edge_insert_statement` — have been deleted.
      `CompiledCypherLoop` is now only used in the fresh-endpoint execution
      wrapper in `_compile_write_helpers.py`, which remains the one
      genuinely loop-dependent write path (each matched source row requires a
      new auto-generated node id to wire into the subsequent edge insert, and
      correlating those ids set-based via a CTE-with-RETURNING approach is not
      possible in DuckDB because DuckDB does not support data-modifying CTEs
      — INSERT is not allowed inside a CTE body in DuckDB, confirmed by direct
      runtime test). The loop is therefore the permanent accepted execution
      model for the fresh-endpoint write path on all backends, not just DuckDB.
      All write families in the admitted subset now compile to DuckDB-executable
      SQL or loops where genuinely needed, and the read-only carveout is gone.
- [x] Add a full PostgreSQL lowering path from the same logical IR rather than
      treating PostgreSQL support as a mere SQLGlot rendering target, including
      a backend-specific schema/runtime contract for the admitted subset.
      Done: the shared IR/backend-binding path now includes an explicit
      PostgreSQL lowerer entrypoint instead of leaving PostgreSQL as a missing
      backend slot, and the blanket backend-level write prohibition has now
      been removed so admitted shared write families can flow through the same
      lowering path rather than being rejected before lowering. A PostgreSQL
      runtime test suite now also lives in
      `tests/test_postgresql_runtime.py`, covering MATCH/RETURN, CREATE node,
      CREATE relationship, MERGE node, MERGE relationship, MATCH+CREATE
      fresh-endpoint, node plus relationship `SET`, relationship `DELETE`,
      and schema sequence-based ID generation;
      renderer regressions now also pin backend-specific PostgreSQL write SQL
      shapes such as relationship `SET` lowering through `UPDATE ... FROM`
      plus relationship `DELETE` lowering from the shared IR path, and they
      now also pin the loop-backed fresh-endpoint and traversal-backed
      PostgreSQL write programs that rely on `RETURNING id` plus per-row
      binding flow without needing a live PostgreSQL server. The repo now also
      has a dedicated `scripts/dev/run_postgresql_runtime_docker.sh` helper
      that starts a disposable PostgreSQL container, exports
      `CYPHERGLOT_TEST_POSTGRES_DSN`, and runs the PostgreSQL runtime suite
      against that live backend instead of relying only on manually managed
      local servers;
      tests skip automatically when psycopg2 is not installed or
      `CYPHERGLOT_TEST_POSTGRES_DSN` is not set, and the Docker-backed helper
      now passes that PostgreSQL runtime suite against a live disposable
      container.
- [x] Keep splitting oversized handwritten modules as Phase 12 work touches
      them, so the IR/lowering migration does not just move complexity around
      while leaving new monoliths behind. Prefer doing both jobs at once:
      when a refactor exposes a real backend/IR/type-aware seam, split that
      seam out immediately instead of doing one pass that only rearranges the
      architecture and a later pass that only shrinks files. Do this in two
      steps so the work stays focused: first keep carving down the three core
      handwritten production modules `src/cypherglot/compile.py`,
      `src/cypherglot/normalize.py`, and `src/cypherglot/validate.py`, with
      `compile.py` as the immediate Phase 12 target; then, once that core
      lowering/backend slice settles, reassess the remaining oversized support
      and test files and split only the ones that still look structurally
      justified rather than line-count noisy. Started: the former DuckDB
      SQLGlot rewrite block was first split out of `compile.py` so the
      backend-specific cleanup could be isolated, and the later type-aware
      migration then removed that rewrite-heavy path entirely. DuckDB now uses
      the shared backend lowerer directly, with only narrow backend-specific
      branches left inside the shared type-aware compiler where SQL semantics
      genuinely differ. The shared SQL string assembly,
      schema-less predicate, and basic property/value SQL helpers have now
      also been split into `src/cypherglot/_compile_sql_utils.py`, so
      extracted lowering helpers no longer have to keep reaching back into
      `compile.py` for that utility layer. The compiled-program dataclasses
      plus the single-statement helper wrappers now also live in
      `src/cypherglot/_compiled_program.py`, so the main compiler file no
      longer owns that program model inline either. The type-aware variable-
      length module now also owns its remaining variable-length-only outer
      projection/order helper slice directly instead of importing those
      helpers from `compile.py`, and the neighboring dead projected/group-by
      helpers have been deleted instead of preserved. The shared type-aware
      binding/alias spec dataclasses plus the `WITH` naming helpers now also
      live in `src/cypherglot/_compile_type_aware_common.py`, so extracted
      type-aware code no longer has to import those basics through
      `compile.py` either. The shared type-aware predicate and field-
      expression helpers now also live in
      `src/cypherglot/_compile_type_aware_common.py`, so extracted type-aware
      code no longer has to route node/relationship predicate or field-shaping
      through `compile.py` for those shared basics either. The shared
      type-aware `WITH` binding column/expression helpers now also live there,
      so extracted type-aware lowering no longer has to reach back into
      `compile.py` for that binding-shaping slice either. The full type-aware
      `MATCH ... WITH ... RETURN` lowering slice now also lives in
      `src/cypherglot/_compile_type_aware_with.py`, so the main compiler file
      no longer carries that `WITH` source/select/predicate/order/group
      family inline while the surrounding read-shaping helpers are being split
      on real seams. The neighboring type-aware read family for single-node,
      optional-node, one-hop relationship, fixed-length chain, shared return/
      aggregate/order/group shaping, and variable-length branch expansion now
      also lives in `src/cypherglot/_compile_type_aware_reads.py`, so the main
      compiler file no longer owns that read-shaping seam inline either and
      the variable-length plus `WITH` modules now import that family directly
      instead of reaching back through `compile.py`. The old generic
      non-type-aware read helper family that previously covered chain source
      assembly, relationship source assembly, `UNWIND`, and generic
      `WITH`/`RETURN` shaping has now been retired from the active compiler
      path altogether. The one admitted slice that was still using it,
      `UNWIND`, now lives in `src/cypherglot/_compile_unwind.py`, and the
      orphaned JSON-era helper module has been deleted rather than kept around
      as dead compatibility scaffolding.
      The remaining matched/traversal relationship write helper cluster has now
      also been split out of `compile.py` into
      `src/cypherglot/_compile_write_helpers.py`, which drops the main compiler
      file itself back under the current handwritten size target and removes a
      dead one-use schema-less merge-node helper instead of preserving it. The
      oversized shared `RETURN` item parser has now also been split out of
      `src/cypherglot/_normalize_support.py` into
      `src/cypherglot/_normalize_return_items.py`, which drops the support
      module under the current handwritten size target without keeping a second
      inline copy of that parser around. The `MATCH ... WITH ... RETURN` /
      `UNWIND` normalization family has now also been split out of
      `src/cypherglot/normalize.py` into
      `src/cypherglot/_normalize_with_helpers.py`, which drops both modules
      under the current handwritten size target without leaving a second inline
      copy of that normalization stack behind. The projection-validation helper
      family now also lives in `src/cypherglot/_validate_projection.py`, and
      `src/cypherglot/validate.py` now delegates the `WITH WHERE` validation
      seam there instead of keeping every projection-related helper inline. The
      type-aware read projection and ordering helper slice now also lives in
      `src/cypherglot/_compile_type_aware_read_projections.py`, which drops
      `src/cypherglot/_compile_type_aware_reads.py` under the current
      handwritten size target instead of leaving the shared type-aware read
      seam as another Phase 12 monolith. The broader CASE / WITH / RETURN
      projection stack is still only partially extracted, so the remaining
      clear production-file monolith for this item is still
      `src/cypherglot/validate.py`. The shared plain-read projection validator
      now also delegates into `src/cypherglot/_validate_projection.py`
      alongside the existing `WITH WHERE` delegation, which drops
      `src/cypherglot/validate.py` from 4752 lines to 3164 lines while the
      broader match/with-shape validation stack continues to be extracted. The
      duplicated CASE helper stack in `src/cypherglot/validate.py` now also
      delegates into `src/cypherglot/_validate_projection.py`, which drops
      `src/cypherglot/validate.py` further to 2997 lines without changing
      admitted validation behavior. The match-pattern/vector/unwind/optional
      shape validator family now also lives in
      `src/cypherglot/_validate_shape_helpers.py`, which drops
      `src/cypherglot/validate.py` further to 2692 lines while preserving the
      same admitted subset behavior through delegating wrappers.
      The standalone-write and mixed read-write validation branches now also
      live in `src/cypherglot/_validate_write_helpers.py`, which drops
      `src/cypherglot/validate.py` further to 2572 lines while preserving the
      same admitted subset behavior through delegating wrappers.
      The remaining giant `WITH` validation branch now also lives in
      `src/cypherglot/_validate_with_helpers.py`, which drops
      `src/cypherglot/validate.py` further to 351 lines while preserving the
      same admitted subset behavior through delegating wrappers; the next
      priority is splitting `src/cypherglot/_validate_with_helpers.py` itself,
      which is now the remaining handwritten validation monolith at 2213 lines.
      The 28 individually-duplicated unary scalar function blocks inside
      `src/cypherglot/_validate_with_helpers.py` (abs, sign, round, floor,
      ceil, sqrt, exp, sin, cos, tan, asin, acos, atan, ln, log, radians,
      degrees, log10, toString, toInteger, toFloat, toBoolean, lower, upper,
      trim, ltrim, rtrim, reverse) have now been consolidated into a single
      generic regex handler, which drops `_validate_with_helpers.py` from 2213
      to 1187 lines. All handwritten production source files are now under the
      1500-line target.
      The render test monolith has also now been
      split again on a real path-family seam, with `tests/test_render_paths.py`
      reduced to 1410 lines and the later bounded-variable-length / `WITH`
      path cases moved into `tests/test_render_paths_variable.py` at 1464
      lines, so the touched render-path suites no longer sit above the current
      handwritten size target while this broader cleanup remains in progress.
      The type-aware SQLite runtime monolith has now also been split on real
      runtime seams into shared fixture support plus focused base, traversal,
      and path/relational modules, with
      `tests/test_sqlite_runtime_type_aware.py` down to 993 lines,
      `tests/test_sqlite_runtime_type_aware_traversal.py` at 631 lines, and
      `tests/test_sqlite_runtime_type_aware_paths.py` at 1231 lines instead of
      keeping one 2960-line runtime suite inline. The compile-program test
      monolith has now also been split on a real traversal/new-endpoint seam,
      with `tests/test_compile_programs.py` down to 804 lines,
      `tests/test_compile_programs_traversal.py` at 988 lines, and
      `tests/test_compile_programs_endpoints.py` at 347 lines instead of
      keeping one 2029-line compile-program suite inline. The compile test
      monolith has now also been split along direct, multi-hop, WITH, and
      optional/public-API seams, with `tests/test_compile.py` down to 863
      lines, `tests/test_compile_multihop.py` at 1371 lines,
      `tests/test_compile_with.py` at 1393 lines,
      `tests/test_compile_with_relational.py` at 814 lines, and
      `tests/test_compile_optional.py` at 338 lines instead of keeping one
      4559-line compile suite inline. The normalize test monolith has now also
      been split on base/WITH/optional-vector seams, with
      `tests/test_normalize.py` down to 1231 lines,
      `tests/test_normalize_with.py` at 1101 lines, and
      `tests/test_normalize_optional_vector.py` at 301 lines instead of
      keeping one 2617-line normalize suite inline. The validate test
      monolith has now also been split on base/WITH/rejection seams, with
      `tests/test_validate.py` down to 888 lines,
      `tests/test_validate_with.py` at 516 lines, and
      `tests/test_validate_rejections.py` at 565 lines instead of keeping one
      1953-line validate suite inline. The oversized benchmark script set has
      now also been split on shared runtime/CLI seams into
      `scripts/benchmarks/_benchmark_common.py` and
      `scripts/benchmarks/_benchmark_cli_helpers.py`, which drops
      `scripts/benchmarks/benchmark_sqlite_runtime.py` to 1454 lines,
      `scripts/benchmarks/benchmark_neo4j_runtime.py` to 1375 lines, and
      `scripts/benchmarks/benchmark_sqlite_schema_shapes.py` to 1482 lines.
- [x] Add backend-specific schema-generation and DDL support where needed so the
      backend story is complete for SQLite, PostgreSQL, and DuckDB instead of
      assuming one SQLite-first DDL contract underneath all rendered SQL.
      Done for the current source-first cut: `GraphSchema` now exposes a
      backend-aware DDL surface instead
      of only `sqlite_ddl()`, with explicit SQLite, DuckDB, and PostgreSQL
      column/type mappings and backend-specific table emission rules in the
      source layer, and the remaining source/test call sites now use that
      generic `ddl(backend)` contract instead of backend-named wrapper
      methods. That removes another SQLite-first naming assumption from the
      shared surface. DuckDB and PostgreSQL DDL now also provision explicit
      per-table id sequences so backend-native schemas can auto-generate ids
      for admitted write paths instead of relying on SQLite rowid behavior.
- [x] Add a Neo4j-like schema-definition surface so users define graph types
      in graph terms instead of authoring raw SQL table/index DDL.
      Done: CypherGlot now exposes `graph_schema_from_text(...)` for a narrow
      graph-native `CREATE NODE` / `CREATE EDGE` schema-definition surface
      above the raw `GraphSchema(...)` Python API.
- [x] Make `CREATE NODE` / `CREATE EDGE` style schema commands lower to both
      backend table DDL and the default physical support objects required by
      that graph shape.
      Done: `schema_ddl_from_text(...)` now lowers those schema commands
      through the existing backend DDL generator, including the default edge
      traversal support indexes already emitted by `GraphSchema.ddl(...)`.
- [x] Automatically provision the baseline edge endpoint indexes during
      edge-type creation instead of expecting users to author separate
      SQL-like index statements for ordinary traversal performance.
      Done: `GraphSchema.ddl(...)` now emits the default edge traversal indexes
      for every edge table across SQLite, DuckDB, and PostgreSQL, and schema
      tests explicitly assert those generated support objects.
- [x] Reserve explicit `CREATE INDEX` support for additional workload-
      specific property indexes on node or edge properties, not for the
      baseline edge endpoint indexes that the schema contract already assumes.
      Done: `CREATE INDEX <Name> ON NODE|EDGE <Type> (...)` now lowers to
      explicit property indexes via `GraphSchema.property_indexes`, while the
      baseline edge endpoint indexes remain generated automatically by
      `GraphSchema.ddl(...)`.
- [x] Document automatic default edge traversal indexes as part of the
      edge-schema contract instead of presenting them as optional manual
      tuning every user must rediscover.
      Done: the schema contract guide now treats baseline edge traversal
      indexes as generated default physical support objects, while leaving
      extra property indexes as explicit workload-specific tuning.
- [x] Keep SQLGlot as the emitted SQL AST and dialect-rendering layer, but make
      backend-specific AST shaping an explicit lowering concern instead of
      relying mainly on renderer-time tweak passes.
      Done for the current source-first slice: render helpers are now a thin
      SQLGlot emission layer over backend-selected compilation, SQLite,
      DuckDB, and PostgreSQL all enter through explicit backend lowerers, and
      the main DuckDB SQLGlot rewrite surface now hangs off the backend
      lowering path rather than the public renderer architecture.
- [x] Retain small backend-specific SQLGlot rewrite passes only where they are
      still the clearest final cleanup step after backend lowering, and do not
      let rewrite growth become the primary multi-backend architecture.
      Done: DuckDB-specific SQLGlot cleanup now runs through the DuckDB backend
      lowerer registration itself instead of a post-lowering special case in the
      top-level compiler entrypoint, so the remaining rewrite surface stays a
      narrow backend cleanup step rather than public compiler architecture.
- [x] Finish the source-first pass before doing broad reconciliation work in
      tests, examples, docs, and benchmarks; during the main architecture
      rewrite, prioritize landing the new IR and backend lowerers over keeping
      every supporting surface in sync after each source edit.
      Done: the source-first IR and backend-lowering pass landed first, and the
      follow-up repo work is now happening as explicit stabilization across
      tests, docs, examples, scripts, benchmarks, and CI instead of being mixed
      into the core architecture edits.
- [x] Add backend-parity regression layers for the admitted subset:
      normalized-Cypher-to-IR tests, IR-to-backend-SQLGlot lowering tests, and
      direct execution/runtime parity tests for SQLite, PostgreSQL, and DuckDB.
      Done: the current suite covers normalized-Cypher-to-IR checks in
      tests/test_ir.py, backend lowering/render assertions in
      tests/test_render_backends.py, and direct runtime execution coverage
      across SQLite, DuckDB, and PostgreSQL in the runtime backend test
      modules; the full pytest suite is green on the current workspace state.
- [x] After the source-first compiler pass, run a dedicated stabilization pass
      that fixes tests, examples, docs, and benchmarks against the new
      architecture rather than treating those updates as the main pacing item
      during the core source migration.
      Done: the stabilization pass has now updated backend-specific tests,
      schema and public-entrypoint docs, quickstart/homepage examples,
      scripts/dev workflow references, benchmark documentation, and CI
      PostgreSQL runtime coverage against the post-refactor architecture.
- [x] Revisit the full test suite after the source-first compiler pass so test
      coverage is explicit about which backend each case targets, including
      SQLite, DuckDB, and PostgreSQL execution paths where needed instead of
      leaving backend assumptions implicit in SQLite-first fixtures. If the
      PostgreSQL path needs containerized execution to be practical, wire that
      in rather than soft-skipping real backend coverage.
      Done: backend-specific runtime and render suites are now split by backend
      in the test tree, and CI now runs `tests/test_postgresql_runtime.py`
      against a PostgreSQL service container instead of relying only on local
      DSN-driven execution.
- [x] Revisit all repo examples after the source-first compiler pass so the
      documented and example-backed query/program flows match the new
      multi-backend architecture and current admitted subset instead of older
      SQLite-shaped assumptions.
      Done: the docs homepage, quickstart, README, and public-entrypoints guide
      now describe the backend-aware parse/validate/normalize/IR/lowering
      pipeline, current multi-backend stance, and the graph-native schema
      definition surface instead of older SQLite-shaped-only examples.
- [x] Revisit [scripts/dev](/mnt/ssd2/repos/cypherglot/scripts/dev) after the
      source-first compiler pass so regeneration and local developer workflows
      reflect the backend-neutral IR plus explicit SQLite, DuckDB, and
      PostgreSQL support story instead of pre-refactor compiler assumptions.
      Done: the checked-in dev helpers now cleanly cover Docker-backed ANTLR
      regeneration and disposable PostgreSQL runtime execution, and the README
      development workflow points directly at those current scripts.
- [x] Revisit [scripts/benchmarks](/mnt/ssd2/repos/cypherglot/scripts/benchmarks)
      after the source-first compiler pass so benchmark harnesses and reported
      metrics reflect backend-aware compile/lowering/runtime comparisons under
      the new architecture instead of the older SQLite-shaped pipeline.
      Done: the benchmark suite now clearly separates the historical
      SQLite schema-shape decision benchmark, the compiler benchmark, and the
      Neo4j runtime benchmark from the still-open SQL-backend runtime
      restructuring work, and the benchmark guide no longer presents the repo
      as a SQLite-only pipeline story.
- [ ] Update the public backend support policy and docs so CypherGlot stops
      presenting DuckDB as read-only if that parity work lands, and instead
      documents equal supported-backend expectations across SQLite,
      PostgreSQL, and DuckDB.
- [ ] Revisit benchmark coverage after the IR/lowering split lands so compile
      latency, backend-lowering cost, and backend-specific runtime parity are
      all measured against the new architecture instead of the older
      SQLite-shaped pipeline.
  - [ ] Keep
        [scripts/benchmarks/benchmark_sqlite_schema_shapes.py](/mnt/ssd2/repos/cypherglot/scripts/benchmarks/benchmark_sqlite_schema_shapes.py)
        as the closed historical schema-decision benchmark: it answered which
        relational graph schema shape to keep, and it should not expand back
        into the main runtime-performance story.
  - [ ] Keep
        [scripts/benchmarks/benchmark_compiler.py](/mnt/ssd2/repos/cypherglot/scripts/benchmarks/benchmark_compiler.py)
        as the compiler-path benchmark: it should own `p50`, `p95`, and `p99`
        for parse/validate/normalize/IR/lowering/render steps plus end-to-end
        Cypher-to-target-SQL latency for SQLite, DuckDB, and PostgreSQL.
  - [ ] Treat
        [scripts/benchmarks/benchmark_neo4j_runtime.py](/mnt/ssd2/repos/cypherglot/scripts/benchmarks/benchmark_neo4j_runtime.py)
        as the completed direct Neo4j runtime/compatibility benchmark and keep
        new SQL-backend runtime work out of that harness.
  - [ ] Replace
        [scripts/benchmarks/benchmark_sqlite_runtime.py](/mnt/ssd2/repos/cypherglot/scripts/benchmarks/benchmark_sqlite_runtime.py)
        with shared SQL-runtime benchmark infrastructure plus explicit SQLite,
        DuckDB, and PostgreSQL runtime entrypoints, or otherwise split it into
        three dedicated target-specific runtime scripts, so each backend owns
        ingest/setup/query-execution measurement instead of DuckDB and
        PostgreSQL piggybacking on a SQLite-first story.
  - [ ] Keep OLTP and OLAP as workload families inside the SQL-runtime
        benchmarks, but report them under each backend rather than making a
        SQLite-centric runtime harness the primary benchmark identity.
  - [ ] For the primary DuckDB runtime benchmark path, use DuckDB-native bulk
        ingest/loading (for example CSV/COPY-style import or similarly direct
        table-loading flow) instead of the attached-SQLite convenience path;
        keep the attached-SQLite path only as an auxiliary/debug benchmark if
        it remains useful.
  - [ ] Add RSS measurement to each major benchmark stage in the SQL-runtime
        harnesses, not just ingest/setup snapshots: connect, schema creation,
        index creation, ingest, analyze/statistics, compile, execute, and suite
        boundaries.

## Phase 13

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
