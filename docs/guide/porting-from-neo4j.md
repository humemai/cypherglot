# Porting from Neo4j

CypherGlot compiles a documented Cypher subset to SQL. If your queries come
from a Neo4j codebase, most simple read shapes paste in unchanged; this page
covers what does not, and what the compiler tells you when it refuses.

The design promise is that nothing fails silently or vaguely: every construct
outside the subset is rejected at validation time with a **named construct, a
line/column location, and a porting hint**. The table below is generated from
the same construct list the compiler's scanner uses, so the documentation and
the error messages cannot drift apart.

For the full positive definition of what is admitted, see the
[Admitted Subset](admitted-subset.md) guide. For how to catch and inspect
rejections programmatically, see [Diagnostics](diagnostics.md).

## Constructs that are rejected by name

Each row shows the `construct` identifier carried on the raised
`CypherCompileError`, an example that triggers it, and the porting rewrite the
hint suggests.

| Construct | Example | Status | Porting rewrite |
| --- | --- | --- | --- |
| `list_comprehension` | `RETURN [x IN list \| x * 2]` | Not yet supported | Compute the projection with `WITH`/`RETURN` items, or filter rows with `WHERE`. |
| `pattern_comprehension` | `RETURN [(a)-[:KNOWS]->(b) \| b.name]` | Not yet supported | Use `OPTIONAL MATCH` with an aggregate (e.g. `collect`/`count`) instead. |
| `exists_subquery` | `WHERE EXISTS { (a)-[:KNOWS]->(:User) }` | Not yet supported | Express the check as a `MATCH`/`OPTIONAL MATCH` plus a `WHERE` predicate. |
| `quantified_predicate` | `WHERE all(x IN list WHERE x > 0)` | Not yet supported | Rewrite as explicit `MATCH`/`WHERE` conditions over the pattern. |
| `call_procedure` | `CALL db.labels()` | Only `db.index.vector.queryNodes(...)` is admitted | Other procedures are engine-specific; use plain `MATCH`/`RETURN` queries. |
| `multi_label` | `MATCH (n:User:Admin)` | **Design non-goal** | Model the second label as a boolean property, or split the type. |
| `named_path` | `MATCH p = (a)-[:KNOWS]->(b)` | Not yet supported | Bind the nodes and relationships you need with aliases instead. |
| `shortest_path` | `shortestPath((a)-[:KNOWS*]->(b))` | Not supported | Bounded variable-length patterns (`[*1..k]`) cover fixed-depth reachability. |
| `unbounded_variable_length` | `-[:KNOWS*]->` or `-[:KNOWS*2..]->` | Not supported | Give the pattern an explicit upper bound, e.g. `[*1..4]`. |

Two Neo4j constructs are not in CypherGlot's grammar at all, so they surface as
**located syntax errors** (`construct="syntax_error"`) rather than named
rejections:

- `reduce(acc = init, x IN list | ...)` — the accumulator-initializer syntax
  does not parse. Rewrite as an aggregate (`sum`, `collect` + host-side fold).
- `FOREACH (x IN list | ...)` — the clause form does not parse. Issue the
  writes as individual admitted `MATCH ... SET`/`CREATE` statements instead.

`multi_label` deserves emphasis: unlike the "not yet" rows, it is a deliberate
non-goal, not a temporary gap. CypherGlot's schema stores one type per physical
node table, which is load-bearing for the columnar-friendly type-local scans
the compiler emits. See the design-non-goals section of the
[Admitted Subset](admitted-subset.md) guide.

## Parameterize with `$param`, not string interpolation

Cypher parameters compile to SQL bind parameters:

```cypher
MATCH (u:User) WHERE u.age > $min_age RETURN u.name
```

```sql
SELECT u.name FROM cg_node_user AS u WHERE u.age > :min_age
```

Passing values as `$param` instead of interpolating literals into the query
text matters twice over:

1. **CypherGlot's compile cache** is keyed on the exact query text. A
   parameterized query is byte-identical on every call and hits the cache;
   a query with interpolated literals is a new text every time and recompiles
   from scratch.
2. **The database's own plan cache** works the same way on the SQL side. The
   emitted `:min_age` bind parameter lets engines reuse prepared plans.

If you are porting code that builds Cypher strings with f-strings or `+`,
switch it to `$param` placeholders and pass the values through your database
driver's parameter mechanism. The compiled SQL carries the binding through
unchanged.

## Practical porting workflow

1. Run your existing queries through `validate_cypher_text(...)` (or just
   `to_sql(...)`) in a loop and collect the `CypherCompileError`s.
2. Group by `error.construct` — the table above gives you the rewrite per
   group, and `error.line`/`error.column` point at the exact spot.
3. Queries that validate cleanly are in the subset and compile on every
   supported backend; per-engine behavior differences are covered in
   [Dialect Notes](dialect-notes.md).

## Related guides

- [Admitted Subset](admitted-subset.md)
- [Diagnostics](diagnostics.md)
- [Dialect Notes](dialect-notes.md)
