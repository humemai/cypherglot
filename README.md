# CypherGlot

CypherGlot is the Cypher frontend compiler for the HumemAI stack.

Its intended job is:

```text
raw Cypher string
→ parse
→ Cypher AST
→ compile
→ SQLGlot AST
```

It is intentionally not a database engine. It should not execute SQL, execute
vector search, or own storage. Its main product is a SQLGlot `Expression` tree
that a host runtime such as `humemdb` can plan and execute.

## Status

The repository is currently in the compiler-foundation stage. The packaging,
testing, documentation, and release workflows are in place so the repo can be
published and documented consistently while the parser and lowering surface is
filled in.

## Install

```bash
pip install cypherglot
```

## Documentation

- Docs: <https://docs.humem.ai/cypherglot/>
- Repository: <https://github.com/humemai/cypherglot>

## Development

Run the unit tests:

```bash
python -m unittest discover -s tests -v
```

Build the docs locally:

```bash
uv sync --group docs
uv run mkdocs build --strict
```

The roadmap lives in `things-to-do.md`.
