# CypherGlot

CypherGlot is the Cypher frontend compiler for the HumemAI stack.

[![Docs](https://img.shields.io/badge/docs-humem.ai-0f766e)](https://docs.humem.ai/cypherglot/)
[![Test](https://github.com/humemai/cypherglot/actions/workflows/test.yml/badge.svg)](https://github.com/humemai/cypherglot/actions/workflows/test.yml)
[![Build Docs](https://github.com/humemai/cypherglot/actions/workflows/build-docs.yml/badge.svg)](https://github.com/humemai/cypherglot/actions/workflows/build-docs.yml)
[![Generated Frontend](https://github.com/humemai/cypherglot/actions/workflows/generated-cypher.yml/badge.svg)](https://github.com/humemai/cypherglot/actions/workflows/generated-cypher.yml)
[![Publish PyPI](https://github.com/humemai/cypherglot/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/humemai/cypherglot/actions/workflows/publish-pypi.yml)

---

## ✨ What CypherGlot is

It takes Neo4j-like Cypher, enforces an explicit admitted subset, normalizes the
accepted shape, and lowers it into SQLGlot-backed output that another runtime can
plan and execute.

```text
raw Cypher string
→ parse
→ validate admitted subset
→ normalize
→ compile
→ SQLGlot AST or SQL-backed program
```

CypherGlot is intentionally compiler-only.

- It parses and lowers Cypher.
- It does not execute SQL.
- It does not own storage.
- It does not execute vector search.

## 🎯 What it is for

- a reusable Cypher frontend compiler
- a stable boundary between Cypher parsing and host-runtime execution
- SQLGlot-backed output for embedded runtimes such as HumemDB

## Current SQL target

CypherGlot currently lowers admitted queries to SQL that is tested and supported
for SQLite-backed host runtimes, with a narrow DuckDB read-only rendering path
for analytical experiments over the same graph-to-table contract.

- the current supported backend target is SQLite-first, with narrow DuckDB
  read-only support for admitted analytical reads
- HumemDB is the reference host runtime for that contract
- the current runtime validation in this repo is SQLite-first, with narrow
  DuckDB read-only runtime coverage
- `dialect=...` rendering support is useful for string output experiments and host
  integration work, but it is not a portability guarantee across backends today
- DuckDB support is currently limited to read-only graph-query families; write
  programs and full backend neutrality are still out of scope

## Graph-to-table schema contract

CypherGlot’s output is schema-aware. If you want to execute its compiled SQL,
your runtime needs to provide the graph-to-table layout that the compiler
expects.

The current tested downstream SQLite contract is:

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE nodes (
  id INTEGER PRIMARY KEY,
  properties TEXT NOT NULL DEFAULT '{}',
  CHECK (json_valid(properties)),
  CHECK (json_type(properties) = 'object')
) STRICT;

CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  type TEXT NOT NULL,
  from_id INTEGER NOT NULL,
  to_id INTEGER NOT NULL,
  properties TEXT NOT NULL DEFAULT '{}',
  CHECK (json_valid(properties)),
  CHECK (json_type(properties) = 'object'),
  FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
  FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
) STRICT;

CREATE TABLE node_labels (
  node_id INTEGER NOT NULL,
  label TEXT NOT NULL,
  PRIMARY KEY (node_id, label),
  FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);
CREATE INDEX idx_edges_from_id ON edges(from_id);
CREATE INDEX idx_edges_to_id ON edges(to_id);
CREATE INDEX idx_edges_type ON edges(type);
CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
```

Recommended baseline indexes:

```sql
CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);
CREATE INDEX idx_edges_from_id ON edges(from_id);
CREATE INDEX idx_edges_to_id ON edges(to_id);
CREATE INDEX idx_edges_type ON edges(type);
CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
```

Keep `node_labels` as the canonical label store. If a runtime later adds a label
cache on `nodes` for performance, that should be treated as denormalized derived
data, not as the source of truth.

See the dedicated guide for the full schema contract, column semantics, and
indexing notes:

- [Schema Contract](https://docs.humem.ai/cypherglot/guide/schema-contract/)

## ✅ Current status

The current target is a practical mainstream single-hop, read-heavy subset for
Neo4j-style onboarding, not full Cypher parity.

Today the public surface already covers:

- parsing through the vendored openCypher grammar
- admitted-subset validation
- normalization into repo-owned statement objects
- compilation of admitted single-statement shapes into one SQLGlot `Expression`
- compilation of admitted multi-step write shapes into a small SQL-backed program
- thin SQL rendering helpers over the compiled output

The most useful admitted families today are:

- `MATCH ... RETURN`
- narrow standalone `OPTIONAL MATCH ... RETURN`
- narrow `MATCH ... WITH ... RETURN`
- narrow standalone `UNWIND ... RETURN`
- standalone `CREATE`
- `MATCH ... SET`
- `MATCH ... DELETE`
- narrow `MATCH ... CREATE` relationship writes
- grouped `count(...)`, `count(*)`, `sum(...)`, `avg(...)`, `min(...)`, and `max(...)`
- common scalar, predicate, graph-introspection, string, numeric, conversion, and
  narrow multi-argument computed projections over already admitted inputs

Vector-aware `CALL db.index.vector.queryNodes(...)` shapes are validated and
normalized for host runtimes, but they are not compiled into SQLGlot output yet.

## Public API at a glance

The stable entrypoints are:

- `parse_cypher_text(text)`
- `validate_cypher_text(text)`
- `normalize_cypher_text(text)`
- `to_sqlglot_ast(text)`
- `to_sqlglot_program(text)`
- `to_sql(text, dialect=...)`
- `render_cypher_program_text(text, dialect=...)`

Lower-level `compile_*`, `normalize_*`, and `render_compiled_*` helpers remain
available for implementation-facing use.

## 🔗 Documentation

- [CypherGlot docs](https://docs.humem.ai/cypherglot/)

The admitted language boundary is documented in the docs site and kept honest by
regression tests.

## 🧠 What is supported today

- parsing through the vendored openCypher grammar
- admitted-subset validation
- normalization into repo-owned statement objects
- compilation of admitted single-statement shapes into one SQLGlot `Expression`
- compilation of admitted multi-step write shapes into a small SQL-backed program
- thin SQL rendering helpers over the compiled output

Main admitted query families today:

- `MATCH ... RETURN`
- narrow standalone `OPTIONAL MATCH ... RETURN`
- narrow `MATCH ... WITH ... RETURN`
- narrow standalone `UNWIND ... RETURN`
- standalone `CREATE`
- `MATCH ... SET`
- `MATCH ... DELETE`
- narrow `MATCH ... CREATE` relationship writes
- grouped `count(...)`, `count(*)`, `sum(...)`, `avg(...)`, `min(...)`, and `max(...)`
- common scalar, predicate, graph-introspection, string, numeric, conversion, and
  narrow multi-argument computed projections over already admitted inputs

That is intentionally a practical mainstream single-hop subset for onboarding,
not a full Cypher compatibility claim.

## ⚡ Quick examples

Parse and validate one admitted read:

```python
import cypherglot

text = "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"

parsed = cypherglot.parse_cypher_text(text)
assert not parsed.has_errors

cypherglot.validate_cypher_text(text)
normalized = cypherglot.normalize_cypher_text(text)

print(type(normalized).__name__)
```

Compile a single-statement read to SQLGlot AST or SQL text:

```python
expression = cypherglot.to_sqlglot_ast(
    "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
)

print(expression.sql())

sql = cypherglot.to_sql(
    "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
)

print(sql)
```

Compile a multi-step write shape to a SQL-backed program:

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

## Install

CypherGlot supports Python 3.10 and newer.

Install from PyPI:

```bash
uv pip install cypherglot
```

Install from source in editable mode:

```bash
uv pip install -e .
```

## Development

Set up the local environment:

```bash
uv sync --group test --group docs
```

Run the tests:

```bash
uv run pytest
```

Check the generated frontend state:

```bash
scripts/dev/regenerate_cypher_frontend_docker.sh --check
```

Build the docs locally:

```bash
uv run mkdocs build --strict
```

## 🔗 Quick links

- Docs: [docs.humem.ai/cypherglot](https://docs.humem.ai/cypherglot/)
- Repository: [github.com/humemai/cypherglot](https://github.com/humemai/cypherglot)
- Issues: [github.com/humemai/cypherglot/issues](https://github.com/humemai/cypherglot/issues)

## 📦 Packaging

CypherGlot is a pure Python package today. It ships compiler code and generated
frontend artifacts, but it does not ship a database runtime or platform-specific
service layer.

The public package version comes from Git tags through Hatch VCS.

## 📄 License

CypherGlot is licensed under MIT. See `LICENSE`.
