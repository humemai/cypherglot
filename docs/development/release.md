# Release Workflow

CypherGlot's public release path centers on two outputs:

- the PyPI package
- the versioned docs site

## Package release

Package versions come from Git tags that match the Hatch VCS pattern:

```text
v0.1.0
v0.1.1
v0.2.0
```

The PyPI workflow builds the package, checks metadata, verifies that the wheel
installs, and publishes through PyPI trusted publishing.

## Docs release

The docs deployment workflow:

- checks out `cypherglot`
- installs the docs dependencies
- checks out `humemai-docs`
- runs `mike deploy` with the `cypherglot` prefix
- optionally updates the `latest` alias

## Shared docs hub

The shared docs landing page at `https://docs.humem.ai/` can publish `cypherglot`
as its own section, separate from `humemdb`.
