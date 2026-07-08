# Dialect Notes

CypherGlot compiles the same admitted Cypher to five SQL backends. The emitted
SQL is semantically equivalent across them, but each engine has a personality:
type spellings differ, some capabilities are absent, and a few Cypher functions
render to different SQL function names. This page collects those per-engine
notes in one place.

Backends are selected by name — `sqlite`, `duckdb`, `postgresql`, `turso`,
`clickhouse` — on `to_sql(...)`, `schema_ddl_from_text(...)`, and the other
compilation entrypoints.

## Capability summary

| | SQLite | Turso | DuckDB | PostgreSQL | ClickHouse |
| --- | --- | --- | --- | --- | --- |
| Reads | ✓ | ✓ | ✓ | ✓ | ✓ |
| Writes (`CREATE`/`MERGE`/`SET`/`DELETE`) | ✓ | ✓ | ✓ | ✓ | — (read/OLAP only) |
| Native `BOOLEAN` type | — (`INTEGER` + `CHECK`) | — (`INTEGER` + `CHECK`) | ✓ | ✓ | ✓ |
| `WITH RECURSIVE` variable-length lowering | ✓ | — (rejected at parse time) | ✓ | ✓ | ✓ (24.4+) |
| `collect(...)` renders as | `json_group_array` | `json_group_array` | `array_agg` | `array_agg` | `groupArray` |

## Variable-length strategy

Bounded variable-length patterns (`-[:KNOWS*1..3]->`) lower under one of two
strategies, chosen by the `variable_length_strategy` argument:

- **`unroll`** (the default) — a finite `UNION ALL` of fixed-hop plans, one per
  admitted hop count. Plain SQL; every backend executes it.
- **`recursive_cte`** — a single `WITH RECURSIVE` traversal. Supported on
  SQLite, DuckDB, PostgreSQL, and ClickHouse (24.4+). **Turso rejects
  `WITH RECURSIVE` at parse time** (as of Turso 0.6), so keep the default
  `unroll` strategy there.

Both strategies produce the same rows; they differ in plan shape and, on large
graphs, in performance characteristics.

## Per-engine notes

### SQLite

- DDL types: `string → TEXT`, `integer → INTEGER`, `float → REAL`,
  `boolean → INTEGER` with a `CHECK (col IN (0, 1))` constraint.
- Generated tables are `STRICT`, ids are `INTEGER PRIMARY KEY` (no sequence
  objects), and foreign keys require `PRAGMA foreign_keys = ON` per connection
  — the generated DDL includes the pragma.
- Boolean predicates compare against `0`/`1` rather than `TRUE`/`FALSE`.

### Turso

- SQLite-dialect compatible: identical DDL and type mapping, same `STRICT`
  tables, same boolean-as-integer convention.
- The one divergence that matters to CypherGlot: no `WITH RECURSIVE` (see the
  strategy section above). The default `unroll` lowering runs unmodified.

### DuckDB

- DDL types: `string → VARCHAR`, `integer → BIGINT`, `float → DOUBLE`,
  `boolean → BOOLEAN`.
- Generated DDL omits foreign-key constraints (id integrity is enforced by the
  compiler's write programs instead).
- Write programs use `UPDATE ... FROM` where the lowering benefits from it.
- Mixed-type numeric expressions coerce through `DOUBLE`.

### PostgreSQL

- DDL types: `string → TEXT`, `integer → BIGINT`, `float → DOUBLE PRECISION`,
  `boolean → BOOLEAN`; ids come from sequence objects.
- The richest write dialect: `UPDATE ... FROM`, `DELETE ... USING`, and
  `RETURNING` are all used by the compiled write programs where applicable.
- `toInteger(...)` on float inputs truncates explicitly (`trunc` before the
  cast) so the result matches Cypher semantics rather than PostgreSQL's
  round-half-even cast.

### ClickHouse

- **Read/OLAP only.** Point updates and deletes in ClickHouse are asynchronous,
  non-transactional `ALTER` mutations, so CypherGlot does not compile write
  statements for it; use it for the analytical read side.
- DDL: `MergeTree` engine tables; the primary key lives in the table
  `ORDER BY` clause rather than an inline `PRIMARY KEY`; nullable property
  columns wrap as `Nullable(T)`; no secondary `CREATE INDEX` statements are
  emitted (ClickHouse has no B-tree point indexes).
- Like PostgreSQL, integer conversion truncates explicitly.

## One subset, five targets

The practical guarantee: a query that passes `validate_cypher_text(...)`
compiles on every backend that supports its statement kind (writes exclude
ClickHouse). Row-level result agreement across backends is enforced by the
project's cross-engine parity and conformance test suites. What varies by
engine is captured above — type spellings, write-dialect features, and the
recursive-CTE availability — not query semantics.

## Related guides

- [Schema Contract](schema-contract.md) — how graph types become tables
- [Admitted Subset](admitted-subset.md) — what compiles at all
- [Porting from Neo4j](porting-from-neo4j.md)
