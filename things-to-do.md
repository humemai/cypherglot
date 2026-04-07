# CypherGlot Things To Do

Frontend-compiler roadmap for the separate `cypherglot` repo.

CypherGlot is the Cypher frontend compiler. Its job is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot AST
```

It should not:

- execute SQL
- execute vector search
- manage LanceDB or NumPy vector indexes
- own storage or table execution
- treat final SQL strings as its main abstraction

Its main product is a SQLGlot `Expression` tree that HumemDB can plan and execute.

## Phase 1

Establish the repo boundary and compiler contract.

Status: in progress.

- [ ] Make the compiler-only repo scope explicit in code, docs, and API shape.
- [ ] Keep raw Cypher string input as the main frontend entrypoint.
- [ ] Keep raw-Cypher parsing responsibility inside `cypherglot` instead of expecting
  HumemDB to parse Cypher first and only delegate later.
- [ ] Keep SQLGlot as a direct dependency and SQLGlot AST as the main output.
- [ ] Avoid introducing a redundant extra AST or IR layer unless a clear later need
  emerges.
- [ ] Keep the repo focused on parsing, normalization, validation, and compilation,
  not execution.
- [ ] Document the intended relationship with HumemDB clearly: `cypherglot` compiles,
  HumemDB plans and executes.
- [ ] Keep the minimal useful public contract explicit: a `to_sqlglot_ast(...)`-style
  API that accepts raw Cypher and returns SQLGlot `Expression` trees.

## Phase 2

Own the parser-generation workflow.

Status: planned.

- [ ] Choose the concrete parser-generation path and keep it Python-first.
- [ ] Vendor or generate parser-ready grammar artifacts in-repo instead of depending
  on a low-trust runtime parser package.
- [ ] Use openCypher materials as reference input, not as a runtime dependency.
- [ ] Keep parser regeneration a documented, owned development workflow.
- [ ] Prefer a Docker-first regeneration flow so contributors do not need Java on the
  host.
- [ ] Once `cypherglot` owns checked-in generated parser outputs, make those artifacts
  verifiable in CI so they cannot silently drift from the grammar inputs.
- [ ] Do not carry a placeholder generated-artifacts workflow before real generated
  outputs exist; add that CI check when parser-generation ownership becomes real in
  this repo.

## Phase 3

Build the normalization and validation boundary.

Status: planned.

- [ ] Parse raw Cypher into a stable Cypher-facing internal representation.
- [ ] Normalize parser output into repo-owned structures before lowering to SQLGlot.
- [ ] Keep syntax errors owned by the generated parser path instead of silently
  falling back to ad hoc parsing.
- [ ] Separate syntax acceptance from admitted-subset policy validation.
- [ ] Reject unsupported constructs clearly instead of pretending to support broad
  Neo4j or openCypher compatibility.

## Phase 4

Compile the admitted Cypher subset to SQLGlot AST.

Status: planned.

- [ ] Start with the practical MVP subset: `MATCH`, node labels, single-hop directed
  relationships, simple `WHERE`, `RETURN`, `ORDER BY`, and `LIMIT`.
- [ ] Keep the core mapping explicit in compiler work: node patterns lower to
  `nodes`, labeled nodes lower through `node_labels`, relationships lower through
  `edges`, and property access lowers through JSON extraction expressions or dialect
  equivalents.
- [ ] Lower node patterns to the agreed relational schema using `nodes`,
  `node_labels`, and `edges`.
- [ ] Treat labels as normalized rows and properties as JSON-backed values in the
  compiler assumptions.
- [ ] Compile property access through the appropriate JSON extraction expressions.
- [ ] Return SQLGlot AST as the compiler product instead of final SQL strings.
- [ ] Keep SQLGlot AST broad enough for this role without pretending SQLGlot is
  semantically aware of HumemDB-specific extensions; SQLGlot handles structure while
  HumemDB and `cypherglot` own the language meaning.

## Phase 5

Broaden the admitted language carefully.

Status: planned.

- [ ] Expand the admitted subset incrementally instead of trying to claim broad
  Cypher completeness.
- [ ] Use TCK-style and grammar-backed evidence to widen support clause family by
  clause family.
- [ ] Keep `OPTIONAL MATCH`, variable-length paths, named paths, broad multi-part
  flows, and subqueries out until there is clear evidence and implementation support.
- [ ] Add operationally useful breadth before broader path semantics when tradeoffs
  matter.
- [ ] Keep the language boundary documented and defended by tests.

## Phase 6

Make the frontend vector-aware without making it vector-executing.

Status: planned.

- [ ] Admit the chosen Cypher-side vector syntax under the normal parser boundary.
- [ ] Carry vector intent forward as metadata, placeholders, or compiler-recognizable
  structures rather than executing vector logic inside `cypherglot`.
- [ ] Keep the mixed-query boundary explicit: for Neo4j-like Cypher vector search,
  `cypherglot` should parse the ordinary Cypher structure and carry vector intent
  forward, while HumemDB turns that into vector search plus a conditioned query path
  for execution.
- [ ] Keep vector search execution in HumemDB.
- [ ] Keep vector-index lifecycle and build execution in HumemDB.
- [ ] Ensure the compiler can participate in the shared SQLGlot-based execution flow
  without pretending vectors are ordinary backend-native Cypher semantics.

## Phase 7

Harden CypherGlot for the first public release.

Status: planned.

- [ ] Stabilize the public compiler entrypoint and output contract.
- [ ] Make syntax and subset-policy errors clear and testable.
- [ ] Add regression coverage for the admitted Cypher subset and compiler output
  shape.
- [ ] Document the admitted subset, non-goals, and HumemDB integration boundary.
- [ ] Keep the release claim narrow: useful Cypher frontend compiler, not full Cypher
  compatibility.
- [ ] Keep the repo-level release claim explicit too: reusable Cypher frontend
  compiler to SQLGlot AST, not a standalone graph database or SQL execution engine.
