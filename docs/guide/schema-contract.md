# Schema Contract

CypherGlot is not just Cypher-to-generic-SQL. It lowers admitted Cypher into SQL
that assumes one concrete graph-to-table layout.

If a host runtime wants to execute CypherGlot output directly, it should expose
this physical contract or a compatibility layer that behaves the same way.

## Current physical layout

CypherGlot is currently tested and supported against SQLite-backed runtimes, and
the current downstream SQLite schema contract is:

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

`properties` is physically stored as `TEXT` in SQLite, but it is logically part
of the JSON-object contract. SQLite's JSON functions operate over that stored
text.

## Column semantics

`nodes`

- `id`: stable node identifier used in joins, predicates, and whole-entity returns
- `properties`: JSON object for node properties such as `{"name": "Alice"}`

`edges`

- `id`: stable relationship identifier
- `type`: relationship type name such as `KNOWS`
- `from_id`: source node id for outgoing relationships
- `to_id`: target node id for outgoing relationships
- `properties`: JSON object for relationship properties

`node_labels`

- `node_id`: foreign-key-like reference to `nodes.id`
- `label`: one node label value

## Why labels are separate

`node_labels` is the canonical label store.

That is intentional:

- it keeps the layout multi-label capable
- it gives label filtering a clean relational shape
- it is the index-friendly layout for `MATCH (n:Label)` and label existence checks
- it avoids packing labels into one serialized column and then fighting that shape
  later with ad hoc indexing

If a runtime wants an extra cached label column on `nodes` for a hot path, that
should be treated as denormalized derived data, not as the canonical source of
truth.

## How CypherGlot uses the schema

The current compiler assumes these access patterns:

- node scans read from `nodes`
- relationship scans read from `edges`
- node label filters use joins or `EXISTS` probes against `node_labels`
- relationship traversal joins `edges.from_id` and `edges.to_id` to `nodes.id`
- property access reads `nodes.properties` or `edges.properties`
- whole-node returns reconstruct `{id, label, properties}`
- whole-relationship returns reconstruct `{id, type, properties}`

Examples:

```sql
SELECT JSON_EXTRACT(u.properties, '$.name')
FROM nodes AS u
JOIN node_labels AS u_label_0
  ON u_label_0.node_id = u.id
 AND u_label_0.label = 'User'
```

```sql
SELECT JSON_EXTRACT(a.properties, '$.name'), JSON_EXTRACT(r.properties, '$.note')
FROM edges AS r
JOIN nodes AS a ON a.id = r.from_id
JOIN nodes AS b ON b.id = r.to_id
```

## Recommended indexes

The current emitted query shapes strongly benefit from these indexes:

```sql
CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);

CREATE INDEX idx_edges_from_id ON edges(from_id);
CREATE INDEX idx_edges_to_id ON edges(to_id);
CREATE INDEX idx_edges_type ON edges(type);
CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
```

## Property indexing

CypherGlot assumes JSON-property access semantics, but it does not require one
single portable property-index strategy.

That part is backend-specific and workload-specific.

Typical options include:

- generated columns for hot properties such as `name`, `score`, or `region`
- expression indexes over JSON extraction expressions
- materialized side tables if one runtime wants stricter relational typing

Those optimizations are valid as long as they preserve the logical contract:

- `nodes.properties` behaves like a JSON object
- `edges.properties` behaves like a JSON object
- the visible label/type/id semantics remain unchanged

## Logical types

CypherGlot currently assumes these logical value families at the schema boundary:

- ids: integer-like values
- labels: text
- relationship types: text
- properties: JSON objects containing scalar values that map cleanly to the
  admitted Cypher subset

If a runtime needs stricter physical typing for indexing, it can project those
values into generated columns or indexed expressions, but the logical contract
above should stay stable.

## Contract vs implementation detail

The important contract is:

- three logical tables: nodes, edges, node_labels
- endpoint columns: `from_id`, `to_id`
- relationship type column: `type`
- JSON-like property columns: `properties`
- canonical label storage in `node_labels`

The exact DDL details around extra generated columns or additional backend-local
accelerators remain runtime choices, but the SQLite contract above is the current
tested and documented baseline.
