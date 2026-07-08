# Diagnostics

Every query CypherGlot refuses raises a structured `CypherCompileError` that
says **what** was rejected (a named construct), **where** (line and column in
the original Cypher text), and **how to move forward** (a porting hint). This
page documents that error surface.

## `CypherCompileError`

`CypherCompileError` is exported from the package root and subclasses
`ValueError`, so existing code that catches `ValueError` keeps working
unchanged.

```python
import cypherglot

try:
    cypherglot.to_sql(
        "MATCH (n:User:Admin) RETURN n.name",
        backend="sqlite",
        schema_context=ctx,
    )
except cypherglot.CypherCompileError as error:
    print(error.construct)  # "multi_label"
    print(error.line)       # 1
    print(error.column)     # 8
    print(error.message)    # the bare message, no location suffix
    print(error.hint)       # "Model the second label as a boolean property, ..."
    print(str(error))
    # CypherGlot does not support multiple labels on one node.
    #   (line 1, column 8) Hint: Model the second label as a boolean property,
    #   or split the type.
```

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `message` | `str` | The rejection message without location or hint. |
| `line` | `int \| None` | 1-based line in the original Cypher text, when known. |
| `column` | `int \| None` | 0-based column, when known. |
| `construct` | `str \| None` | Stable identifier for the rejected family (below). |
| `hint` | `str \| None` | A porting suggestion, when one exists. |

The `str(...)` rendering is
`"{message} (line X, column Y) Hint: {hint}"`, with the location and hint
segments omitted when unknown.

## Construct identifiers

`construct` is a stable, machine-checkable identifier — group errors by it when
batch-validating a query corpus. Current identifiers:

**Named unsupported constructs** (detected early by a scan of the parse tree,
each with a porting hint — see
[Porting from Neo4j](porting-from-neo4j.md) for the rewrite table):

- `list_comprehension`, `pattern_comprehension` — comprehension expressions
- `exists_subquery` — `EXISTS { ... }`
- `quantified_predicate` — `all()`/`any()`/`none()`/`single()`
- `multi_label` — `(n:A:B)` (a design non-goal)
- `named_path` — `MATCH p = (...)`
- `shortest_path` — `shortestPath()`/`allShortestPaths()`

**Subset-boundary gates** (shapes the grammar parses but the admitted subset
does not cover yet):

- `syntax_error` — the text did not parse; `line`/`column` point at the first
  parser error
- `union` — a `UNION` branch outside the admitted per-branch subset
- `multi_clause` / `read_shape` / `statement_shape` / `non_regular_query` —
  multi-part or otherwise unadmitted statement shapes
- `optional_match` — `OPTIONAL MATCH` beyond the admitted narrow forms
- `multi_hop_chain` — pattern chains beyond the admitted hop limit
- `disconnected_pattern` — disconnected `MATCH` patterns outside the narrow
  write subset
- `variable_length` / `unbounded_variable_length` /
  `variable_length_aggregate` — variable-length forms outside the bounded
  read subset
- `call_procedure` — any procedure call other than the admitted
  `db.index.vector.queryNodes(...)` vector shape

New identifiers may be added as the subset grows; existing ones are kept
stable.

## Batch-validating a corpus

The intended workflow for auditing an existing query set:

```python
import collections
import cypherglot

by_construct = collections.defaultdict(list)
for name, text in queries.items():
    try:
        cypherglot.validate_cypher_text(text)
    except cypherglot.CypherCompileError as error:
        by_construct[error.construct].append((name, error))

for construct, failures in sorted(by_construct.items()):
    print(f"{construct}: {len(failures)} queries")
```

Everything not in `by_construct` is inside the admitted subset.

## Related guides

- [Porting from Neo4j](porting-from-neo4j.md) — the rewrite table per construct
- [Admitted Subset](admitted-subset.md) — the positive definition
- [Public Entrypoints](public-entrypoints.md) — where errors are raised from
