# Schema Contract

CypherGlot is not just Cypher-to-generic-SQL. It lowers admitted Cypher into SQL
that assumes one concrete graph-to-table layout.

If a host runtime wants to execute CypherGlot output directly, it should expose
this physical contract or a compatibility layer that behaves the same way.

## Physical contract

CypherGlot lowers admitted Cypher against a generated type-aware schema rather
than a generic `nodes` / `edges` / `node_labels` layout.

## Target physical layout

The target SQLite contract is generated from graph schema metadata:

- one table per node type
- one table per edge type
- typed property columns instead of a single catch-all `properties` blob
- foreign keys from edge tables to their source and target node tables
- automatically generated baseline edge traversal indexes that match one-hop
  and multi-hop traversal directions

For a graph schema with node types `User` and `Company`, and an edge type
`WORKS_AT(User -> Company)`, the generated SQLite contract looks like:

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

## Column semantics

Node tables such as `cg_node_user`

- `id`: stable node identifier used in joins, predicates, and whole-entity returns
- one column per declared property, using the declared logical type

Edge tables such as `cg_edge_works_at`

- `id`: stable relationship identifier
- `from_id`: source node id for outgoing relationships
- `to_id`: target node id for outgoing relationships
- one column per declared edge property, using the declared logical type

Type identity is carried by table selection itself. There is no separate
canonical `node_labels` table in the target contract, because node type filters
resolve to the appropriate typed table directly.

## Result shape contract

The physical schema is fixed and relational, but Cypher return values can still
be shaped in different ways.

CypherGlot's type-aware target is strict relational SQL output:

- emitted SQL should return plain scalar values and typed columns
- whole entities should expand into stable dotted columns such as `user.id` and
  `user.name`
- SQL should not rely on dialect-specific object or list constructors for the
  target path

Object-shaped compatibility output is a compatibility mode. The portable SQL
contract is the strict relational path described here.

This distinction matters because the storage schema can be fully fixed and
type-aware while some return helpers still try to package values back into one
structured SQL value. For the portable target path, that packaging should happen
outside emitted SQL or be rejected when it cannot be represented as ordinary
columns.

## How CypherGlot uses the target schema

CypherGlot uses these access patterns:

- node scans read from the node table selected by the node type
- relationship scans read from the edge table selected by the relationship type
- node type filters resolve through table choice, not a `node_labels` join
- relationship traversal joins edge tables to their declared source and target
  node tables
- property access reads typed columns directly
- whole-node returns reconstruct entity objects from `id`, type identity, and
  the typed property columns
- whole-relationship returns reconstruct entity objects from `id`, edge type,
  endpoints, and the typed property columns

Helpers that naturally want list or object outputs, such as `labels(...)` and
`keys(...)`, do not map cleanly to portable SQL columns across dialects. For
the strict relational target path, they should therefore be handled by an upper
runtime layer or remain unsupported rather than forcing structured packaging
back into emitted SQL.

Examples:

```sql
SELECT u.name
FROM cg_node_user AS u
```

```sql
SELECT a.name, r.since
FROM cg_edge_works_at AS r
JOIN cg_node_user AS a ON a.id = r.from_id
JOIN cg_node_company AS b ON b.id = r.to_id
```

## Recommended indexes

CypherGlot's generated schema contract already includes the baseline traversal
indexes for every edge table. Hosts using `GraphSchema.ddl(...)` should treat
those indexes as part of the default physical contract, not as optional manual
tuning to be rediscovered later.

For `WORKS_AT(User -> Company)`, the default generated edge indexes are:

```sql
CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);
CREATE INDEX idx_cg_edge_works_at_to_id ON cg_edge_works_at(to_id);
CREATE INDEX idx_cg_edge_works_at_from_to ON cg_edge_works_at(from_id, to_id);
CREATE INDEX idx_cg_edge_works_at_to_from ON cg_edge_works_at(to_id, from_id);
```

Additional indexes over typed property columns remain workload-specific. Those
can now be declared explicitly through the graph-native schema text surface or
through `GraphSchema(property_indexes=...)`, but they still sit on top of the
generated default traversal indexes rather than replacing them.

## Logical types

CypherGlot's target type-aware contract assumes these logical value families at
the schema boundary:

- ids: integer-like values
- node type names: text carried in schema metadata and table selection
- relationship type names: text carried in schema metadata and table selection
- properties: declared scalar fields materialized as typed columns

## Contract vs implementation detail

The important target contract is:

- generated node and edge tables derived from graph schema metadata
- stable endpoint columns: `from_id`, `to_id`
- stable primary key column: `id`
- type identity resolved through table choice
- typed property columns instead of one catch-all properties blob

The exact naming scheme and extra backend-local accelerators remain
implementation choices. In this repo, the first source-level contract for that
target now lives in `cypherglot.schema`, which owns table naming, validation,
and baseline SQLite DDL generation for the type-aware layout.

CypherGlot also now exposes a small graph-native text surface above that Python
API:

```text
CREATE NODE User (name STRING NOT NULL, age INTEGER);
CREATE NODE Company (name STRING NOT NULL);
CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER);
CREATE INDEX user_name_idx ON NODE User(name);
```

Hosts can feed that text through `graph_schema_from_text(...)` to get a
`GraphSchema`, or through `schema_ddl_from_text(...)` to lower it directly to
backend DDL.

That text surface admits explicit `CREATE INDEX` only for additional typed
property indexes on node or edge tables. The default edge traversal indexes are
still part of the generated baseline schema contract and should not be modeled
as separate schema commands.
