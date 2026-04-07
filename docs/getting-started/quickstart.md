# Quick Start

CypherGlot is being set up as a reusable Cypher frontend compiler. The package
currently exposes versioned package metadata while the parser and lowering API are
being filled in.

Today, the main practical quick start is to install the package and track the
compiler contract and roadmap while the parser surface lands.

The intended long-term shape is a small API that accepts raw Cypher and returns a
SQLGlot AST that a host runtime can execute.
