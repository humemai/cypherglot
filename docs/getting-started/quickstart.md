# Quick Start

CypherGlot exposes a small compiler-focused API:

- parse raw Cypher
- validate the admitted subset
- normalize the accepted shape
- define the graph schema in graph-native terms when needed
- compile admitted queries into SQLGlot-backed output

CypherGlot targets a practical mainstream single-hop, read-heavy subset, not
full Cypher parity.

## Parse and validate

```python
import cypherglot

text = "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1"

result = cypherglot.parse_cypher_text(text)
assert not result.has_errors

cypherglot.validate_cypher_text(text)
```

## Normalize

```python
normalized = cypherglot.normalize_cypher_text(
    "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1"
)

print(type(normalized).__name__)
```

## Compile a single-statement read

```python
expression = cypherglot.to_sqlglot_ast(
    "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
)

print(expression.sql())
print(cypherglot.to_sql(
    "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
))
```

## Compile a multi-step write

```python
program = cypherglot.to_sqlglot_program(
    "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
)

rendered = cypherglot.render_cypher_program_text(
    "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
)

print(type(program).__name__)
print(rendered.steps[0])
```

## Define a graph schema

```python
schema_text = """
CREATE NODE User (name STRING NOT NULL, age INTEGER);
CREATE NODE Company (name STRING NOT NULL);
CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER);
CREATE INDEX user_name_idx ON NODE User(name);
"""

graph_schema = cypherglot.graph_schema_from_text(schema_text)
sqlite_ddl = cypherglot.schema_ddl_from_text(schema_text, backend="sqlite")

print(type(graph_schema).__name__)
print(sqlite_ddl[0])
```

## Public entrypoints

- `parse_cypher_text(text)` returns the raw parse tree and collected syntax errors.
- `validate_cypher_text(text)` enforces the current admitted-subset boundary.
- `normalize_cypher_text(text)` returns repo-owned normalized statement objects.
- `graph_schema_from_text(text)` returns a `GraphSchema` built from graph-native
  schema-definition text.
- `schema_ddl_from_text(text, backend)` lowers that schema text to backend DDL.
- `to_sqlglot_ast(text)` returns one SQLGlot `Expression` for admitted
  single-statement shapes.
- `to_sqlglot_program(text)` returns a SQL-backed compiled program for admitted
  multi-step shapes.
- `to_sql(text, dialect=...)` renders admitted single-statement shapes to SQL.
- `render_cypher_program_text(text, dialect=...)` renders compiled programs to SQL
  text without changing their step structure.

Vector-aware `CALL db.index.vector.queryNodes(...)` queries are part of the
normalization boundary for host runtimes, but they are not compiled into SQLGlot
output directly.

For a fuller walkthrough of when to use each surface, including single-statement,
multi-step, and vector-aware examples, see the
[Public Entrypoints](../guide/public-entrypoints.md) guide.
