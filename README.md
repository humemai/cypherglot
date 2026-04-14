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

The repo is moving from the legacy generic JSON-backed graph layout toward a
generated type-aware schema contract.

The long-term target contract is:

- one table per node type
- one table per edge type
- typed property columns instead of one catch-all JSON `properties` blob
- explicit `from_id` and `to_id` foreign keys on edge tables
- traversal-oriented indexes on generated edge tables

For a graph schema with node types `User` and `Company`, and an edge type
`WORKS_AT(User -> Company)`, the target SQLite contract looks like:

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE cg_node_user (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER
) STRICT;

CREATE TABLE cg_node_company (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
) STRICT;

CREATE TABLE cg_edge_works_at (
  id INTEGER PRIMARY KEY,
  from_id INTEGER NOT NULL,
  to_id INTEGER NOT NULL,
  since INTEGER,
  FOREIGN KEY (from_id) REFERENCES cg_node_user(id) ON DELETE CASCADE,
  FOREIGN KEY (to_id) REFERENCES cg_node_company(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);
CREATE INDEX idx_cg_edge_works_at_to_id ON cg_edge_works_at(to_id);
CREATE INDEX idx_cg_edge_works_at_from_to ON cg_edge_works_at(from_id, to_id);
CREATE INDEX idx_cg_edge_works_at_to_from ON cg_edge_works_at(to_id, from_id);
```

Recommended baseline indexes:

```sql
CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);
CREATE INDEX idx_cg_edge_works_at_to_id ON cg_edge_works_at(to_id);
CREATE INDEX idx_cg_edge_works_at_from_to ON cg_edge_works_at(from_id, to_id);
CREATE INDEX idx_cg_edge_works_at_to_from ON cg_edge_works_at(to_id, from_id);
```

The generic `nodes` / `edges` / `node_labels` layout should now be treated as
legacy compatibility rather than the intended long-term backend contract.

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

## Logging

CypherGlot uses the standard library `logging` module.

- it stays silent by default
- it does not configure the root logger
- it installs a `NullHandler` on the `cypherglot` package logger so library use
  does not emit warnings or force host logging policy

When a host runtime wants compiler diagnostics, enable `DEBUG` on the
`cypherglot` logger:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("cypherglot").setLevel(logging.DEBUG)
```

Current level semantics:

- `DEBUG`: parse, validate, normalize, compile, and render pipeline events,
  including schema-layout and dialect decisions at public entrypoints
- `INFO`: reserved for explicit high-value lifecycle events; CypherGlot does not
  currently emit routine `INFO` logs
- `WARNING`: reserved for degraded or compatibility-path behavior
- `ERROR`: reserved for internal failures rather than ordinary admitted-subset
  rejection

Ordinary validation rejection remains an exception path, not an `ERROR` log.

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
