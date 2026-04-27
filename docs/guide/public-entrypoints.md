# Public Entrypoints

CypherGlot exposes a small compiler-facing API. These are the public surfaces a
host runtime or integration layer should usually start from:

- `parse_cypher_text(...)`
- `validate_cypher_text(...)`
- `normalize_cypher_text(...)`
- `graph_schema_from_text(...)`
- `schema_ddl_from_text(...)`
- `to_sqlglot_ast(...)`
- `to_sql(...)`
- `to_sqlglot_program(...)`
- `render_cypher_program_text(...)`

They split naturally into three layers:

- parse and validation
- normalized host-runtime handoff
- SQLGlot-backed compilation and rendering

If you want the overall boundary and backend assumptions, see the
[Compiler Contract](compiler-contract.md) and
[Schema Contract](schema-contract.md) guides. This page is narrower: it explains
what each public entrypoint is for and shows what a few real query shapes look
like as they move through the API.

## At a glance

| Entrypoint | What it returns | Use it when | Notes |
| --- | --- | --- | --- |
| `parse_cypher_text(text)` | `CypherParseResult` | You want the raw parse result and syntax errors | Parsing does not enforce the admitted subset by itself. |
| `validate_cypher_text(text)` | `None` or raises `ValueError` | You want an explicit admitted-subset check | Good boundary check before relying on compilation. |
| `normalize_cypher_text(text)` | A repo-owned normalized statement object | You need a stable host-runtime handoff shape | This is also the main public surface for vector-aware queries. |
| `graph_schema_from_text(text)` | `GraphSchema` | You want to define graph types via `CREATE NODE` / `CREATE EDGE` text instead of raw Python object construction | This is the graph-native schema-definition surface above `GraphSchema(...)`. |
| `schema_ddl_from_text(text, backend)` | `list[str]` | You want backend DDL from that schema-definition text | Reuses the checked-in type-aware schema DDL generator. |
| `to_sqlglot_ast(text)` | one SQLGlot `Expression` | The admitted query lowers to one SQL statement | Rejects multi-step shapes. |
| `to_sql(text, dialect=...)` | rendered SQL text | You want string SQL for one admitted statement | Thin rendering helper over `to_sqlglot_ast(...)`. |
| `to_sqlglot_program(text)` | `CompiledCypherProgram` | The admitted query lowers to multiple SQL-backed steps | Used for writes and other program-shaped flows. |
| `render_cypher_program_text(text, dialect=...)` | `RenderedCypherProgram` | You want SQL text for each compiled program step | Preserves program structure instead of flattening it. |

## How to choose

Use the highest-level public entrypoint that matches your actual need.

- Use `parse_cypher_text(...)` when you are inspecting syntax or surfacing parse
  errors directly.
- Use `validate_cypher_text(...)` when you need a fast yes-or-no admitted-subset
  boundary check.
- Use `normalize_cypher_text(...)` when a host runtime needs structured Cypher
  intent, especially for vector-aware queries.
- Use `graph_schema_from_text(...)` when a host wants graph-native schema input
  without constructing `GraphSchema(...)` manually.
- Use `schema_ddl_from_text(...)` when that schema-definition text should lower
  directly to backend DDL.
- Use `to_sqlglot_ast(...)` or `to_sql(...)` for admitted single-statement
  shapes.
- Use `to_sqlglot_program(...)` or `render_cypher_program_text(...)` for
  admitted multi-step shapes.

## Walkthrough: schema-definition text

CypherGlot also exposes a small graph-native schema-definition surface for the
type-aware storage contract.

```python
import cypherglot

schema_text = """
CREATE NODE User (name STRING NOT NULL, age INTEGER);
CREATE NODE Company (name STRING NOT NULL);
CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER);
CREATE INDEX user_name_idx ON NODE User(name);
"""

graph_schema = cypherglot.graph_schema_from_text(schema_text)
sqlite_ddl = cypherglot.schema_ddl_from_text(schema_text, backend="sqlite")
```

This layer is intentionally narrow.

- `CREATE NODE <Type> (...)` defines one node type and its typed properties.
- `CREATE EDGE <Type> FROM <Source> TO <Target> (...)` defines one edge type,
  endpoint contract, and typed properties.
- `CREATE INDEX <Name> ON NODE|EDGE <Type> (...)` adds workload-specific
  property indexes on declared typed properties.
- baseline edge traversal indexes still come from the generated DDL layer
  automatically
- baseline edge endpoint indexes stay implicit and generated rather than
  becoming explicit schema commands every host must repeat

## Walkthrough: single-statement read

This is the simplest mainstream path: parse, validate, normalize, then compile
to one SQLGlot expression or one SQL string.

```python
import cypherglot

query = (
    "MATCH (u:User) "
    "WHERE u.name = $name "
    "RETURN u.name "
    "ORDER BY u.name "
    "LIMIT 1"
)

parsed = cypherglot.parse_cypher_text(query)
assert not parsed.has_errors

cypherglot.validate_cypher_text(query)

normalized = cypherglot.normalize_cypher_text(query)
print(type(normalized).__name__)

expression = cypherglot.to_sqlglot_ast(query)
print(type(expression).__name__)
print(expression.to_s())

sql = cypherglot.to_sql(query)
print(sql)
```

What each step is doing:

- `parse_cypher_text(...)` gives you the raw frontend parse result.
- `validate_cypher_text(...)` confirms the query is inside the current admitted
  subset.
- `normalize_cypher_text(...)` converts the query into a repo-owned normalized
  object that downstream compiler stages understand.
- `to_sqlglot_ast(...)` lowers that admitted read into one SQLGlot expression.
- `to_sql(...)` renders that expression to SQL text.

For this query family, the rendered SQL is a single `SELECT ...` statement.
On the intended type-aware path, a representative query shape looks like this:

```sql
SELECT u.name
FROM cg_node_user AS u
WHERE u.name = :name
ORDER BY u.name
LIMIT 1
```

The generated SQLGlot AST for the product path is a `Select`
expression that hangs off ordinary typed table and column references
instead of generic property blobs. The exact aliasing and quoting details come
from the active schema context and backend renderer; the important contract is
that admitted reads lower through generated `cg_node_*` / `cg_edge_*` tables
and typed columns.

## Walkthrough: multi-step write program

Some admitted Cypher shapes are not one SQL statement. They compile into a small
program of SQL-backed steps instead.

```python
import cypherglot

query = "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"

program = cypherglot.to_sqlglot_program(query)
print(type(program).__name__)
print(len(program.steps))

rendered = cypherglot.render_cypher_program_text(query)
print(type(rendered).__name__)
print(len(rendered.steps))

loop = rendered.steps[0]
print(type(loop).__name__)
print(loop.source)
print(loop.body[0].sql)
print(loop.source.to_s())
print(loop.body[0].sql.to_s())
print(loop.body[1].sql.to_s())
print(loop.body[2].sql.to_s())
```

The distinction matters:

- `to_sqlglot_program(...)` keeps the compiled structure as SQLGlot-backed
  program objects.
- `render_cypher_program_text(...)` turns each compiled step into SQL text while
  preserving the same step boundaries.

That is why `to_sql(...)` rejects these shapes. A multi-step write cannot be
represented honestly as one flat SQL string without losing structure.

For this example, the generated program contains one loop with one source
query and a small body of write statements. On the type-aware path,
the useful mental model is:

1. read matched `x.id` rows from the generated `Begin` node table
2. insert the fresh `End` node into its generated node table and capture the
   created id
3. insert the `TYPE` relationship into its generated edge table using the
   matched source id and the created target id

The exact SQLGlot AST node spelling depends on the active schema context, but
the public contract is the step structure: `to_sqlglot_program(...)` preserves
the multi-step program shape, while `render_cypher_program_text(...)` renders
each step separately. It is not a promise that admitted writes flatten into one
SQL string.

## Walkthrough: vector-aware normalization handoff

Vector-aware `CALL db.index.vector.queryNodes(...)` queries are part of the
public normalization contract, but they are not compiled into SQLGlot-backed SQL
today.

```python
import cypherglot

query = (
    "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
    "YIELD node, score "
    "WHERE node.region = 'west' "
    "RETURN node.id, score "
    "ORDER BY score DESC"
)

cypherglot.validate_cypher_text(query)

normalized = cypherglot.normalize_cypher_text(query)
print(type(normalized).__name__)
print(normalized.index_name)
print(normalized.query_param_name)
print(normalized.top_k)
print(type(normalized.candidate_query).__name__)
print(normalized.return_items)
print(normalized.order_by)
print(repr(normalized))
```

For this family, `normalize_cypher_text(...)` is the key public surface. The
result is a `NormalizedQueryNodesVectorSearch` object that carries:

- vector index name
- query parameter name
- admitted top-k value
- a normalized candidate `MATCH` query
- admitted return items
- admitted ordering

That handoff is for host runtimes such as HumemDB. The host runtime can perform
vector search, apply any runtime-specific planning, and then join the vector
result with relational execution as needed.

The generated normalized handoff object for this example is:

```python
NormalizedQueryNodesVectorSearch(
  kind='vector_query',
  procedure_kind='queryNodes',
  index_name='user_embedding_idx',
  query_param_name='query',
  top_k=3,
  candidate_query=NormalizedMatchNode(
    kind='match',
    pattern_kind='node',
    node=NodePattern(alias='node', label=None, properties=()),
    predicates=(
      Predicate(alias='node', field='region', operator='=', value='west', disjunct_index=0),
    ),
    returns=(
      ReturnItem(
        alias='node',
        field='id',
        kind='field',
        operator=None,
        value=None,
        start_value=None,
        length_value=None,
        search_value=None,
        replace_value=None,
        delimiter_value=None,
        output_alias=None,
      ),
    ),
    order_by=(),
    limit=None,
    distinct=False,
    skip=None,
  ),
  return_items=('node.id', 'score'),
  order_by=(('score', 'desc'),),
)
```

What does not happen today:

- `to_sqlglot_ast(...)` does not compile vector-aware `CALL` queries
- `to_sql(...)` does not render them to SQL text
- `to_sqlglot_program(...)` does not turn them into a SQL-backed program

That boundary is intentional. CypherGlot carries vector intent forward, but it
does not pretend vector execution is native SQLGlot work.

## Practical guidance

- If your runtime needs syntax errors, start with `parse_cypher_text(...)`.
- If your runtime needs an explicit contract check, call
  `validate_cypher_text(...)`.
- If your runtime consumes structured Cypher intent, prefer
  `normalize_cypher_text(...)`.
- If your runtime wants one SQL statement, use `to_sqlglot_ast(...)` or
  `to_sql(...)`.
- If your runtime wants a structured write program, use `to_sqlglot_program(...)`
  or `render_cypher_program_text(...)`.
- If the query is vector-aware, stay at the normalization layer.

## Related guides

- [Quick Start](../getting-started/quickstart.md)
- [Admitted Subset](admitted-subset.md)
- [Compiler Contract](compiler-contract.md)
- [Schema Contract](schema-contract.md)
