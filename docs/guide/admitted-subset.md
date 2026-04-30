# Admitted Subset

CypherGlot is intentionally narrow. It accepts a small Cypher surface and rejects
broader grammar families clearly at validation time.

## Practical Coverage

CypherGlot's current practical coverage should be understood from the admitted,
documented, and regression-tested subset in this guide rather than from parser
breadth alone.

For mainstream onboarding, the current surface is best described as a strong
single-hop, read-heavy Neo4j-style subset rather than full Cypher parity.
Neo4j is the practical reference engine for this admitted surface: queries in
the subset should ideally be valid there unchanged, while other Cypher runtimes
may require small compatibility rewrites around the same core shapes.
That includes:

- ordinary `MATCH ... RETURN` reads over one connected pattern
- narrow standalone `OPTIONAL MATCH ... RETURN`
- narrow `MATCH ... WITH ... RETURN` rebinding flows
- narrow standalone `UNWIND ... RETURN`
- grouped `count(...)`, `count(*)`, `sum(...)`, `avg(...)`, `min(...)`, and
  `max(...)` aggregation slices
- common projection families over admitted field, scalar-binding, or literal
  inputs: `size(...)`, `id(...)`, `type(...)`, searched `CASE`,
  `properties(...)`, `labels(...)`, `keys(...)`, `startNode(...)`,
  `endNode(...)`, string rewrite functions, numeric functions, conversion
  functions, and predicate outputs
- one-hop relationship-property reads across those same admitted projection and
  predicate families
- bounded read-side variable-length relationship reads, including syntactic
  relationship aliases that are not referenced downstream, for
  ordinary `MATCH ... RETURN` and narrow
  `MATCH ... WITH ... RETURN`
- optional final-`RETURN` aliases for common admitted introspection,
  aggregate, unary computed, and narrow multi-argument computed outputs

That is the current strong onboarding/read-heavy target: a practical mainstream
subset for paste-in read queries that stay within simple connected patterns and
narrow projection flows, plus the narrow write and bounded traversal families
documented below. It is not a claim of broad Neo4j parity or of coverage for
most Neo4j users overall. It is also not a claim that every runtime in the
benchmark matrix accepts the exact same raw query text unchanged; Neo4j anchors
the subset, and some other engines may need light adaptation at execution time.

## Read subset

CypherGlot currently admits:

- `MATCH ... RETURN`
- narrow standalone `OPTIONAL MATCH ... RETURN`
- narrow `MATCH ... WITH ... RETURN`
- narrow standalone `UNWIND ... RETURN`
- a single connected `MATCH` pattern
- at most one relationship hop in the matched pattern
- bounded non-negative-length variable-length relationship reads in ordinary
  `MATCH ... RETURN` and narrow `MATCH ... WITH ... RETURN`, including
  syntactic relationship aliases that are not referenced in downstream
  `RETURN`, `WITH`, or predicate surfaces
- traversal-backed `MATCH ... CREATE` and `MATCH ... MERGE` with exactly one
  reused matched node alias plus at most one fresh endpoint node, whether that
  fresh endpoint is labeled or unlabeled
- narrow `WHERE` predicates over admitted entity fields, plus admitted
  `id(alias) OP literal_or_parameter`; for relationship bindings, admitted
  `type(rel_alias) = literal_or_parameter`
- that same ordinary-read and narrow-optional `WHERE` slice also admits
  field string predicates `STARTS WITH`, `ENDS WITH`, and `CONTAINS`, plus
  field null predicates `IS NULL` and `IS NOT NULL`; in admitted one-hop
  relationship reads, those same field string/null predicates also apply to
  relationship property fields such as `r.note`
- that same ordinary-read and narrow-optional `WHERE` slice also admits narrow
  nested `size(...)` field predicates over compile-safe property inputs:
  `size(alias.field) OP literal_or_parameter`, `size(alias.field) IS NULL`, and
  `size(alias.field) IS NOT NULL`; in admitted one-hop relationship reads,
  those same nested `size(...)` filters also apply to relationship property
  fields such as `size(r.note)`
- `ORDER BY`, `SKIP`, and `LIMIT` on admitted `RETURN` queries
- optional `AS output_alias` on ordinary `MATCH` and narrow `OPTIONAL MATCH`
  field projections such as `RETURN u.name AS name`
- whole-entity pass-through returns on ordinary `MATCH` and narrow `OPTIONAL MATCH`,
  such as `RETURN u`, `RETURN u AS user`, or in one-hop `MATCH` queries
  `RETURN r AS rel`
- a narrow standalone aggregation slice on ordinary `MATCH`: `count(bound_alias)`
  or `count(*)`, each with optional `AS output_alias`,
  optionally alongside grouped field or whole-entity projections
- a first narrow scalar-expression slice on ordinary `MATCH`: scalar literals or
  named parameters with explicit aliases such as `RETURN 'tag' AS tag` or
  `RETURN $value AS value`
- a first narrow computed-expression slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `size(...)` over admitted field projections, with optional
  `AS output_alias`; scalar literal/parameter inputs still require explicit
  aliases. This same slice also admits nested `id(alias)` and `type(rel_alias)`
  outputs under `size(...)`
- a narrow unary string-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `lower(alias.field)`, `upper(alias.field)`,
  `trim(alias.field)`, `ltrim(alias.field)`, `rtrim(alias.field)`, and
  `reverse(alias.field)`, each with optional `AS output_alias`,
  plus scalar literal/parameter inputs such as `lower('tag') AS lower_tag`,
  `upper($value) AS upper_value`, `trim(' tag ') AS trimmed`,
  `ltrim(' tag') AS left_trimmed`, `rtrim('tag ') AS right_trimmed`, or
  `reverse('tag') AS reversed_tag`
- in admitted one-hop relationship reads, those same field-projection families
  also apply to relationship property fields such as `r.note`
- a narrow null-fallback slice on ordinary `MATCH` and narrow `OPTIONAL MATCH`:
  `coalesce(alias.field, literal_or_parameter)` with optional
  `AS output_alias`
- a narrow replace slice on ordinary `MATCH` and narrow `OPTIONAL MATCH`:
  `replace(admitted_input, literal_or_parameter, literal_or_parameter)` with
  optional `AS output_alias`
  over admitted field projections plus scalar literal/parameter inputs
- a narrow left/right slice on ordinary `MATCH` and narrow `OPTIONAL MATCH`:
  `left(admitted_input, literal_or_parameter)` and
  `right(admitted_input, literal_or_parameter)` with optional
  `AS output_alias`
  over admitted field projections plus scalar literal/parameter inputs
- a narrow split slice on ordinary `MATCH` and narrow `OPTIONAL MATCH`:
  `split(admitted_input, literal_or_parameter)` with optional
  `AS output_alias`
  over admitted field projections plus scalar literal/parameter inputs
- a narrow substring slice on ordinary `MATCH` and narrow `OPTIONAL MATCH`:
  `substring(admitted_input, literal_or_parameter)` and
  `substring(admitted_input, literal_or_parameter, literal_or_parameter)` with
  optional `AS output_alias`
  over admitted field projections plus scalar literal/parameter inputs
- a narrow numeric-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `abs(alias.field)` with optional `AS output_alias`, plus
  scalar literal/parameter inputs such as `abs(-3) AS magnitude`
- a narrow sign-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `sign(alias.field)` with optional `AS output_alias`, plus
  scalar literal/parameter inputs such as `sign(-3.2) AS age_sign`
- a narrow rounding-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `round(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `round(-3.2) AS value`
- a narrow ceiling-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `ceil(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `ceil(-3.2) AS value`
- a narrow floor-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `floor(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `floor(-3.2) AS value`
- a narrow square-root-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `sqrt(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `sqrt(-3.2) AS value`
- a narrow exponential-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `exp(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `exp(-3.2) AS value`
- a narrow sine-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `sin(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `sin(-3.2) AS value`
- a narrow cosine-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `cos(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `cos(-3.2) AS value`
- a narrow tangent-function slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `tan(alias.field) AS output_alias`, plus scalar
  literal/parameter inputs such as `tan(-3.2) AS value`
- a narrow string-conversion slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `toString(alias.field)` with optional `AS output_alias`,
  plus scalar literal/parameter inputs such as `toString(-3) AS text`
- a narrow integer-conversion slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `toInteger(alias.field)` with optional `AS output_alias`,
  plus scalar literal/parameter inputs such as `toInteger(-3.2) AS age_int`
- a narrow float-conversion slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `toFloat(alias.field)` with optional `AS output_alias`,
  plus scalar literal/parameter inputs such as `toFloat(-3) AS age_float`
- a narrow boolean-conversion slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `toBoolean(alias.field)` with optional `AS output_alias`,
  plus scalar literal/parameter inputs such as `toBoolean(true) AS is_active`
- in admitted one-hop relationship reads, those same numeric and conversion
  families also apply to relationship property fields such as `r.weight`,
  `r.score`, and `r.active`, including forms such as
  `sqrt(r.score) AS sqrt_score`, `exp(r.score) AS exp_score`, and
  `sin(r.score) AS sin_score`
- a first narrow predicate-return slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `alias.field OP literal_or_parameter AS output_alias`, plus
  admitted string predicates `alias.field STARTS WITH literal_or_parameter`,
  `alias.field ENDS WITH literal_or_parameter`, and
  `alias.field CONTAINS literal_or_parameter`, plus
  admitted `id(alias) OP literal_or_parameter AS output_alias` and
  `type(rel_alias) OP literal_or_parameter AS output_alias`, plus admitted
  `size(alias.field) OP literal_or_parameter AS output_alias`,
  `size(id(alias)) OP literal_or_parameter AS output_alias`, and
  `size(type(rel_alias)) OP literal_or_parameter AS output_alias`
- a narrow null-predicate slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `alias.field IS NULL AS output_alias`,
  `alias.field IS NOT NULL AS output_alias`,
  `size(alias.field) IS NULL AS output_alias`, and
  `size(alias.field) IS NOT NULL AS output_alias`
- in admitted one-hop relationship reads, those same predicate families also
  apply to relationship property fields such as `r.weight` and `r.note`,
  including nested `size(r.note) OP literal_or_parameter`
- a first narrow built-in function alias slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `id(alias)` and `type(rel_alias)`, each with optional
  `AS output_alias`
- a narrow graph-introspection projection slice on ordinary `MATCH` and narrow
  `OPTIONAL MATCH`: `properties(entity_alias)`, `labels(node_alias)`,
  `keys(entity_alias)`, `startNode(rel_alias)`, `endNode(rel_alias)`, and the
  admitted `startNode(rel_alias).field` / `endNode(rel_alias).field` forms,
  each with optional `AS output_alias`
- `ORDER BY projected_alias` for admitted ordinary field, whole-entity,
  aggregate, scalar, `size(...)`, predicate, `id(...)`, and `type(...)` outputs

The admitted `WITH` slice is currently narrow:

- one `MATCH` clause before `WITH`
- one `WITH` clause with passthrough variable items such as `WITH u` or
  `WITH u AS person`, plus simple scalar rebinding such as `WITH u.name AS name`
- an optional narrow `WHERE` after `WITH`, using `scalar_alias OP value` or
  `entity_alias.field OP value`, plus admitted `id(entity_alias) OP value` and
  `type(rel_alias) OP value`
- that same narrow `WITH WHERE` slice also admits
  `scalar_alias IS NULL`, `scalar_alias IS NOT NULL`,
  `entity_alias.field IS NULL`, and `entity_alias.field IS NOT NULL`
- `WITH WHERE` also admits narrow string predicates over compile-safe scalar and
  entity-field inputs: `scalar_alias STARTS WITH value`,
  `scalar_alias ENDS WITH value`, `scalar_alias CONTAINS value`,
  `entity_alias.field STARTS WITH value`, `entity_alias.field ENDS WITH value`,
  and `entity_alias.field CONTAINS value`; in admitted one-hop relationship
  reads, those same `WITH WHERE` string/null field predicates also apply to
  relationship bindings such as `rel.note`
- `WITH WHERE` also admits narrow nested `size(...)` predicates over
  compile-safe scalar and entity-field inputs: `size(scalar_alias) OP value`,
  `size(scalar_alias) IS NULL`, `size(scalar_alias) IS NOT NULL`,
  `size(entity_alias.field) OP value`, `size(entity_alias.field) IS NULL`, and
  `size(entity_alias.field) IS NOT NULL`; in admitted one-hop relationship
  reads, those same nested `size(...)` filters also apply to relationship
  bindings such as `size(rel.note)`
- one final `RETURN` clause after `WITH`
- final projections shaped as `RETURN alias.field` for entity bindings,
  `RETURN entity_alias` for pass-through entity bindings, or `RETURN scalar_alias`
  for scalar bindings
- optional `AS output_alias` on admitted final `RETURN` projection items
- a first aggregation slice: `count(binding_alias)` or `count(*)`, each with
  optional `AS output_alias`, optionally
  alongside grouped scalar or entity pass-through return items
- a narrow built-in function alias slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `id(entity_alias)` and `type(rel_alias)`, each
  with optional `AS output_alias`
- a narrow graph-introspection projection slice in the final `RETURN` of
  admitted `MATCH ... WITH ... RETURN`: `properties(entity_alias)`,
  `labels(node_alias)`, `keys(entity_alias)`, `startNode(rel_alias)`,
  `endNode(rel_alias)`, and the admitted `startNode(rel_alias).field` /
  `endNode(rel_alias).field` forms, each with optional `AS output_alias`
- a narrow computed-expression slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `size(entity_alias.field)`,
  `size(scalar_alias)`, `size(id(entity_alias))`, and `size(type(rel_alias))`,
  each with optional `AS output_alias`; scalar literal/parameter inputs such as
  `size('tag') AS tag_len` or `size($value) AS value_len` still require aliases
- a narrow unary string-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `lower(entity_alias.field)`,
  `upper(entity_alias.field)`, `lower(scalar_alias)`, `upper(scalar_alias)`,
  `trim(entity_alias.field)`, `trim(scalar_alias)`,
  `ltrim(entity_alias.field)`, `ltrim(scalar_alias)`,
  `rtrim(entity_alias.field)`, `rtrim(scalar_alias)`,
  `reverse(entity_alias.field)`, and `reverse(scalar_alias)`, each with
  optional `AS output_alias`, plus scalar literal/parameter inputs
  such as `lower('tag') AS lower_tag`, `upper($value) AS upper_value`,
  `trim(' tag ') AS trimmed`, `ltrim(' tag') AS left_trimmed`,
  `rtrim('tag ') AS right_trimmed`, or `reverse('tag') AS reversed_tag`
- in admitted `MATCH ... WITH ... RETURN` flows, those same entity-field
  families also apply to relationship bindings such as `rel.note`
- a narrow null-fallback slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `coalesce(entity_alias.field, literal_or_parameter)`
  and `coalesce(scalar_alias, literal_or_parameter)` with optional
  `AS output_alias`
- a narrow replace slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`:
  `replace(admitted_input, literal_or_parameter, literal_or_parameter)` with
  optional `AS output_alias`
  over admitted entity-field projections, scalar bindings, and scalar
  literal/parameter primary inputs
- a narrow left/right slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`:
  `left(admitted_input, literal_or_parameter)` and
  `right(admitted_input, literal_or_parameter)` with optional
  `AS output_alias`
  over admitted entity-field projections, scalar bindings, and scalar
  literal/parameter primary inputs
- a narrow split slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`:
  `split(admitted_input, literal_or_parameter)` with optional
  `AS output_alias`
  over admitted entity-field projections, scalar bindings, and scalar
  literal/parameter primary inputs
- a narrow substring slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`:
  `substring(admitted_input, literal_or_parameter)` and
  `substring(admitted_input, literal_or_parameter, literal_or_parameter)` with
  optional `AS output_alias`
  over admitted entity-field projections, scalar bindings, and scalar
  literal/parameter primary inputs
- a narrow numeric-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `abs(entity_alias.field) AS output_alias`
  and `abs(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `abs(-3) AS magnitude`
- a narrow sign-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `sign(entity_alias.field) AS output_alias`
  and `sign(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `sign(-3.2) AS age_sign`
- a narrow rounding-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `round(entity_alias.field) AS output_alias`
  and `round(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `round(-3.2) AS value`
- a narrow ceiling-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `ceil(entity_alias.field) AS output_alias`
  and `ceil(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `ceil(-3.2) AS value`
- a narrow floor-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `floor(entity_alias.field) AS output_alias`
  and `floor(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `floor(-3.2) AS value`
- a narrow square-root-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `sqrt(entity_alias.field) AS output_alias`
  and `sqrt(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `sqrt(-3.2) AS value`
- a narrow exponential-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `exp(entity_alias.field) AS output_alias`
  and `exp(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `exp(-3.2) AS value`
- a narrow sine-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `sin(entity_alias.field) AS output_alias`
  and `sin(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `sin(-3.2) AS value`
- a narrow cosine-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `cos(entity_alias.field) AS output_alias`
  and `cos(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `cos(-3.2) AS value`
- a narrow tangent-function slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `tan(entity_alias.field) AS output_alias`
  and `tan(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `tan(-3.2) AS value`
- a narrow string-conversion slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `toString(entity_alias.field)` and
  `toString(scalar_alias)`, each with optional `AS output_alias`, plus scalar
  literal/parameter inputs such as `toString(-3) AS text`
- a narrow integer-conversion slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `toInteger(entity_alias.field) AS output_alias`
  and `toInteger(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `toInteger(-3.2) AS age_int`
- a narrow float-conversion slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `toFloat(entity_alias.field) AS output_alias`
  and `toFloat(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `toFloat(-3) AS age_float`
- a narrow boolean-conversion slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `toBoolean(entity_alias.field) AS output_alias`
  and `toBoolean(scalar_alias) AS output_alias`, plus scalar literal/parameter
  inputs such as `toBoolean(true) AS is_active`
- in admitted `MATCH ... WITH ... RETURN` flows, those same numeric and
  conversion families also apply to relationship bindings such as
  `rel.weight`, `rel.score`, and rebound scalar aliases like `active`,
  including forms such as `sqrt(rel.score) AS sqrt_score` and
  `exp(score) AS exp_score`, and `sin(rel.score) AS sin_score`
- a narrow scalar-expression slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: scalar literals or named parameters with explicit
  aliases such as `RETURN 'tag' AS tag` or `RETURN $value AS value`
- a narrow predicate-return slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `scalar_alias OP literal_or_parameter AS output_alias`
  or `entity_alias.field OP literal_or_parameter AS output_alias`, including
  admitted string predicates `entity_alias.field STARTS WITH literal_or_parameter`,
  `entity_alias.field ENDS WITH literal_or_parameter`,
  `entity_alias.field CONTAINS literal_or_parameter`,
  `scalar_alias CONTAINS literal_or_parameter`,
  `scalar_alias ENDS WITH literal_or_parameter`, and
  plus admitted
  `id(entity_alias) OP literal_or_parameter AS output_alias` and
  `type(rel_alias) OP literal_or_parameter AS output_alias`, plus admitted
  `size(scalar_alias) OP literal_or_parameter AS output_alias`,
  `size(entity_alias.field) OP literal_or_parameter AS output_alias`,
  `size(id(entity_alias)) OP literal_or_parameter AS output_alias`, and
  `size(type(rel_alias)) OP literal_or_parameter AS output_alias`
- a narrow null-predicate slice in the final `RETURN` of admitted
  `MATCH ... WITH ... RETURN`: `scalar_alias IS NULL AS output_alias`,
  `scalar_alias IS NOT NULL AS output_alias`,
  `entity_alias.field IS NULL AS output_alias`,
  `entity_alias.field IS NOT NULL AS output_alias`,
  `size(scalar_alias) IS NULL AS output_alias`, and
  `size(entity_alias.field) IS NOT NULL AS output_alias`
- in admitted `MATCH ... WITH ... RETURN` flows, those same predicate families
  also apply to relationship bindings such as `rel.weight`, `rel.note`, and
  rebound scalar aliases like `note`, including nested
  `size(rel.note) OP literal_or_parameter` and
  `size(note) OP literal_or_parameter`
- matching `ORDER BY alias.field`, `ORDER BY scalar_alias`,
  `ORDER BY entity_alias`, and `ORDER BY aggregate_alias`
- `ORDER BY projected_alias` for admitted scalar, field, aggregate, `id(...)`,
  `type(...)`, whole-entity, `size(...)`, scalar literal/parameter outputs, and predicate outputs
  produced by the final `RETURN`

Whole-entity pass-through returns on the strict relational product path do not
compile to packed object values anymore. Where whole-entity slices remain
admitted, they are expected to expand into stable typed dotted columns such as
`user.id`, `user.name`, `rel.id`, or endpoint-field projections; list- or
object-shaped packaging is not part of the emitted-SQL contract.

The admitted `UNWIND` slice is currently narrow:

- standalone `UNWIND list_literal AS x RETURN x`
- optional `AS output_alias` on admitted scalar unwind projections
- `ORDER BY` on the unwind alias or projected scalar alias
- `SKIP` or `OFFSET`, and `LIMIT`

The admitted `OPTIONAL MATCH` slice is currently narrow:

- standalone single-node `OPTIONAL MATCH (n[:Label]) RETURN ...`
- the same shape with admitted node-property and `id(alias)` `WHERE` predicates
- ordinary node-field projection, optional `AS output_alias`, `ORDER BY`, `SKIP`
  or `OFFSET`, and `LIMIT`
- narrow scalar literal or parameter outputs with explicit aliases such as
  `RETURN 'tag' AS tag` or `RETURN $value AS value`
- the same narrow `size(...)`, predicate-return, and `id(...)` slices admitted
  in ordinary single-node reads, plus the same narrow `lower(...)`,
  `upper(...)`, `trim(...)`, `ltrim(...)`, `rtrim(...)`, `reverse(...)`, `coalesce(...)`, `replace(...)`, `left(...)`, `right(...)`, `split(...)`, `substring(...)`, `abs(...)`, `sign(...)`, `round(...)`,
  `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toInteger(...)`, `toFloat(...)`, and `toBoolean(...)` output slice
- whole-node pass-through returns such as `RETURN n` or `RETURN n AS item`
- the same narrow `count(bound_alias)` or `count(*)` aggregation slice, each
  with optional `AS output_alias`, as ordinary
  `MATCH`, including grouped field or whole-entity projections
- null-preserving lowering through a left-join-style compiled shape for unmatched rows

CypherGlot does not yet admit broader `WITH` semantics such as `WITH DISTINCT`,
broader aggregation forms beyond narrow admitted `count(binding_alias)` and `count(*)`, broader expression projections beyond narrow admitted `id(...)`, `type(...)`, `lower(...)`, `upper(...)`, `trim(...)`, `ltrim(...)`, `rtrim(...)`, `reverse(...)`, `coalesce(...)`, `replace(...)`, `left(...)`, `right(...)`, `split(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, and `toBoolean(...)` outputs in the
final `RETURN`, broader ordinary-read expression projections beyond narrow admitted literal, parameter, `size(...)`, `lower(...)`, `upper(...)`, `trim(...)`, `ltrim(...)`, `rtrim(...)`, `reverse(...)`, `coalesce(...)`, `replace(...)`, `left(...)`, `right(...)`, `split(...)`, two-arg or three-arg `substring(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, simple predicate, `id(...)`, and `type(...)` outputs, broader `WITH` final-RETURN expression projections beyond narrow admitted scalar literal/parameter, predicate, `size(...)`, `lower(...)`, `upper(...)`, `trim(...)`, `ltrim(...)`, `rtrim(...)`, `reverse(...)`, `coalesce(...)`, `replace(...)`, `left(...)`, `right(...)`, `split(...)`, two-arg or three-arg `substring(...)`, `abs(...)`, `sign(...)`, `round(...)`, `ceil(...)`, `floor(...)`, `sqrt(...)`, `exp(...)`, `sin(...)`, `cos(...)`, `tan(...)`, `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, `id(...)`, and `type(...)` outputs, or
multi-stage `WITH ... WITH ...` flows.

## Mixed read-write subset

CypherGlot currently admits:

- `MATCH ... SET`
- `MATCH ... DELETE`
- narrow `MATCH ... CREATE`
- narrow `MATCH ... MERGE`

The admitted `MATCH ... CREATE` shapes are:

- one matched node pattern before `CREATE`
- or two disconnected matched node patterns before `CREATE`
- or one matched directed relationship pattern / fixed-length directed chain
  before `CREATE`, when the `CREATE` relationship endpoints reuse already
  matched node aliases exactly, or reuse one matched node alias plus one fresh
  labeled endpoint node
- no named paths, no broader variable-length relationships beyond the bounded
  read-side subset, and no multi-hop traversals in the matched portion

The admitted `MATCH ... MERGE` shape is:

- exactly two disconnected matched node patterns before `MERGE`
- or one matched directed relationship pattern / fixed-length directed chain
  before `MERGE`, when the `MERGE` relationship endpoints reuse already matched
  node aliases exactly, or reuse one matched node alias plus one fresh labeled
  endpoint node
- one directed relationship pattern in the `MERGE` clause that reuses those two
  matched aliases exactly
- no `ON CREATE` / `ON MATCH` actions, no fresh endpoint nodes outside the
  narrow one-fresh-endpoint traversal-backed subset, no named paths,
  no broader variable-length relationships, and no multi-hop traversals in the
  matched portion

## Bounded variable-length read subset

CypherGlot now admits a narrow bounded variable-length read form for ordinary
`MATCH ... RETURN` and `MATCH ... WITH ... RETURN`:

- one directed relationship pattern such as `-[:KNOWS*1..2]->` or
  `-[:KNOWS*..2]->`
- non-negative numeric bounds with a finite upper bound
- endpoint node aliases and endpoint node predicates/projections over the
  matched endpoints
- lowering to a finite union of fixed-hop plans under the hood

This bounded subset still excludes:

- open-ended ranges such as `[:KNOWS*]` or `[:KNOWS*2..]`
- relationship aliases on variable-length patterns
- relationship properties on variable-length patterns
- broader aggregate forms beyond direct bounded variable-length returns built
  from endpoint field projections plus `count(*)`, `count(endpoint_alias)`,
  and `aggregate(endpoint.field)`
- grouped or non-count aggregate `MATCH ... RETURN` directly over a
  variable-length pattern; use `MATCH ... WITH ... RETURN` instead when
  broader aggregation is needed
- broader write-side traversal semantics

## Traversal-backed write subset

CypherGlot now admits a narrow traversal-backed write form for `MATCH ... CREATE`
and `MATCH ... MERGE`:

- the matched side may be one directed relationship pattern or one fixed-length
  directed relationship chain
- the write-side relationship must either reuse already matched node aliases
  exactly or, for narrow `MATCH ... CREATE` / `MATCH ... MERGE`, reuse one
  matched node alias plus one fresh labeled endpoint node
- no `ON CREATE` / `ON MATCH` actions

## Write subset

CypherGlot currently admits narrow `CREATE` and `MERGE` statements used by the
compiler and rendering contract.

Some admitted `CREATE` forms lower to a single SQLGlot expression. Others, plus
the admitted standalone `MERGE` forms, lower to a small compiled program with
multiple SQLGlot-backed statements.

The admitted standalone `MERGE` shapes are:

- one labeled node pattern, matched-or-created idempotently
- or one directed relationship pattern whose endpoint node patterns are both
  labeled, matched-or-created idempotently as one guarded pattern
- no `ON CREATE` / `ON MATCH` actions and no broader full-pattern `MERGE`
  semantics beyond that narrow subset

## Deferred clause families

CypherGlot currently rejects these families explicitly:

- broader `OPTIONAL MATCH` semantics beyond the narrow admitted subset
- named path patterns such as `MATCH p = (...)`
- broader variable-length relationships such as `[:KNOWS*]`, `[:KNOWS*2..]`,
  aliased variable-length relationships, and variable-length relationships with
  relationship property constraints
- multi-hop pattern chains outside the admitted fixed-length directed read and
  `MATCH ... WITH ... RETURN` subset
- broader traversal-backed write shapes beyond reusing already matched node
  aliases in narrow `MATCH ... CREATE` and `MATCH ... MERGE`, plus the narrow
  one-fresh-endpoint `MATCH ... CREATE` subset
- disconnected multi-pattern `MATCH` clauses outside the narrow `MATCH ... CREATE` subset
- broader `WITH` semantics beyond the narrow admitted subset
- broader `UNWIND` semantics beyond the narrow admitted subset
- broader `MERGE` semantics beyond the narrow admitted subset
- broader multi-part queries

## Narrow vector-aware subset

CypherGlot now admits one narrow vector-aware read shape:

- `CALL db.index.vector.queryNodes('index_name', integer_top_k, $query) YIELD node, score RETURN node.id, score`
- the same procedure call followed by one admitted `MATCH ...` candidate-filter clause
  before `RETURN`
- or the same procedure call with admitted `YIELD node, score WHERE ...` filtering
  before `RETURN`

That vector-aware shape is normalized into structured metadata plus an admitted
Cypher candidate query. The normalized handoff carries the vector index name,
query-parameter name, normalized `top_k`, one admitted normalized candidate
query, explicit `return_items`, and `order_by` items. CypherGlot does
not compile that procedure call into ordinary SQLGlot output yet. Host runtimes
such as HumemDB are still responsible for vector planning and execution.

## Why the boundary is strict

CypherGlot uses SQLGlot as the structural SQL backend, but it still owns the
Cypher-side language boundary. Keeping the unsupported families rejected in
validation makes the contract explicit and keeps later normalization and
compilation stages honest.
