# Documentation

CypherGlot uses MkDocs Material for its documentation site.

Build the docs locally with:

```bash
uv sync --group docs
uv run mkdocs build --strict
```

The versioned docs deployment path mirrors the HumemAI docs workflow used by the
other repositories in this workspace.

## Logging notes

CypherGlot is a library, so logging must remain host-controlled.

- use the standard library `logging` module only
- never configure the root logger in package code
- stay silent by default via a package-level `NullHandler`
- keep compiler-pipeline diagnostics at `DEBUG`
- reserve `INFO` for rare high-value lifecycle events only
- reserve `WARNING` for degraded or compatibility behavior
- reserve `ERROR` for internal failures, not ordinary subset rejection

Host runtimes that want diagnostics should raise the `cypherglot` logger level
explicitly instead of expecting the package to print by default.
