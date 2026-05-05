# Runtime Result Summary

- Scanned JSON files: 70
- Completed runs: 70
- Skipped unreadable or non-completed runs: 0
- Grouped configurations: 26
- Grouped benchmark campaigns: 3

### Large runtime dataset

The current large runtime matrix used the `large` preset with `2000` OLTP iterations / `20` OLTP warmup and `50` OLAP iterations / `5` OLAP warmup.

That corresponds to roughly:

- `10,000,000` total nodes
- `77,790,000` total edges
- `10` node types
- `10` edge types
- `61` property fields across the schema (`38` per node, `23` per edge)
- `4` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `DuckDB`: `1.5.2`
- `PostgreSQL`: `16.13 (Debian 16.13-1.pgdg13+1)`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.2.post1`

Runtime benchmark artifacts also record these engine versions in a top-level
`database_versions` object inside each JSON payload.

For the SQL backends in this refreshed run, setup follows the more standard
bulk-load sequence: `schema -> ingest -> index -> analyze`. That means the
reported `ingest` step does not include index-maintenance cost during row
insertion, and the `index` step captures post-load index construction.

Neo4j is a direct-Cypher runner rather than a compile-plus-execute SQL
path.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
summary tables below, that engine-specific post-load work is folded into the
`Analyze` column, along with the checkpoint step, so the setup layout stays
consistent across engines.


OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DuckDB Unindexed (1) | `43.48 ms +- 0.00` | `251.93 ms +- 0.00` | `163619.23 ms +- 0.00` | `130.78 ms +- 0.00` | `0.19 ms +- 0.00` | `5.55 ms +- 0.00` | `7.12 ms +- 0.00` | `7.74 ms +- 0.00` |
| PostgreSQL Indexed (1) | `17.87 ms +- 0.00` | `756.82 ms +- 0.00` | `1594638.15 ms +- 0.00` | `184695.90 ms +- 0.00` | `28522.85 ms +- 0.00` | `1.58 ms +- 0.00` | `2.29 ms +- 0.00` | `2.86 ms +- 0.00` |
| Neo4j Indexed (1) | `79.29 ms +- 0.00` | `752.43 ms +- 0.00` | `7978649.31 ms +- 0.00` | `181514.10 ms +- 0.00` | `0.00 ms +- 0.00` | `0.23 ms +- 0.00` | `0.30 ms +- 0.00` | `0.39 ms +- 0.00` |
| ArcadeDB Unindexed (1) | `456.97 ms +- 0.00` | `719.24 ms +- 0.00` | `3718936.23 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `418.18 ms +- 0.00` | `581.00 ms +- 0.00` | `635.32 ms +- 0.00` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DuckDB Unindexed (1) | `11.98 ms +- 0.00` | `228.66 ms +- 0.00` | `161041.70 ms +- 0.00` | `135.55 ms +- 0.00` | `0.14 ms +- 0.00` | `586.50 ms +- 0.00` | `608.24 ms +- 0.00` | `631.12 ms +- 0.00` |
| PostgreSQL Indexed (1) | `8.31 ms +- 0.00` | `19606.57 ms +- 0.00` | `1537238.16 ms +- 0.00` | `186351.93 ms +- 0.00` | `42964.84 ms +- 0.00` | `6102.48 ms +- 0.00` | `6658.60 ms +- 0.00` | `6828.17 ms +- 0.00` |
| Neo4j Indexed (1) | `79.29 ms +- 0.00` | `752.43 ms +- 0.00` | `7978649.31 ms +- 0.00` | `181514.10 ms +- 0.00` | `0.00 ms +- 0.00` | `6745.80 ms +- 0.00` | `7053.37 ms +- 0.00` | `7276.03 ms +- 0.00` |
| ArcadeDB Unindexed (1) | `2.11 ms +- 0.00` | `152.18 ms +- 0.00` | `4449258.90 ms +- 0.00` | `0.00 ms +- 0.00` | `101004.86 ms +- 0.00` | `4279.56 ms +- 0.00` | `4689.30 ms +- 0.00` | `4942.41 ms +- 0.00` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| DuckDB Unindexed (1) | `88.01 MiB +- 0.00` | `92.63 MiB +- 0.00` | `5464.61 MiB +- 0.00` | `5459.88 MiB +- 0.00` | `5459.88 MiB +- 0.00` | `5666.62 MiB +- 0.00` |
| PostgreSQL Indexed (1) | `123.04 MiB +- 0.00` | `127.29 MiB +- 0.00` | `783.12 MiB +- 0.00` | `666.91 MiB +- 0.00` | `654.20 MiB +- 0.00` | `916.44 MiB +- 0.00` |
| Neo4j Indexed (1) | `725.97 MiB +- 0.00` | `776.19 MiB +- 0.00` | `2937.07 MiB +- 0.00` | `4815.07 MiB +- 0.00` | `0.00 MiB +- 0.00` | `3193.59 MiB +- 0.00` |
| ArcadeDB Unindexed (1) | `157.92 MiB +- 0.00` | `500.69 MiB +- 0.00` | `17801.07 MiB +- 0.00` | `17801.07 MiB +- 0.00` | `17801.07 MiB +- 0.00` | `19616.94 MiB +- 0.00` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| DuckDB Unindexed (1) | `391.71 MiB +- 0.00` | `391.97 MiB +- 0.00` | `5688.56 MiB +- 0.00` | `5689.20 MiB +- 0.00` | `5689.20 MiB +- 0.00` | `7133.27 MiB +- 0.00` |
| PostgreSQL Indexed (1) | `844.42 MiB +- 0.00` | `491.92 MiB +- 0.00` | `1295.95 MiB +- 0.00` | `1007.35 MiB +- 0.00` | `1024.57 MiB +- 0.00` | `1131.13 MiB +- 0.00` |
| Neo4j Indexed (1) | `725.97 MiB +- 0.00` | `776.19 MiB +- 0.00` | `2937.07 MiB +- 0.00` | `4815.07 MiB +- 0.00` | `0.00 MiB +- 0.00` | `2704.78 MiB +- 0.00` |
| ArcadeDB Unindexed (1) | `19617.13 MiB +- 0.00` | `19618.23 MiB +- 0.00` | `17275.36 MiB +- 0.00` | `17275.36 MiB +- 0.00` | `30074.79 MiB +- 0.00` | `19692.39 MiB +- 0.00` |

#### Large runtime suite comparison

This rolls the large-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/duckdb_unindexed` | `5.55 ms +- 0.00` | `7.12 ms +- 0.00` | `7.74 ms +- 0.00` |
| `olap/duckdb_unindexed` | `586.50 ms +- 0.00` | `608.24 ms +- 0.00` | `631.12 ms +- 0.00` |
| `oltp/postgresql_indexed` | `1.58 ms +- 0.00` | `2.29 ms +- 0.00` | `2.86 ms +- 0.00` |
| `olap/postgresql_indexed` | `6102.48 ms +- 0.00` | `6658.60 ms +- 0.00` | `6828.17 ms +- 0.00` |
| `oltp/neo4j_indexed` | `0.23 ms +- 0.00` | `0.30 ms +- 0.00` | `0.39 ms +- 0.00` |
| `olap/neo4j_indexed` | `6745.80 ms +- 0.00` | `7053.37 ms +- 0.00` | `7276.03 ms +- 0.00` |
| `oltp/arcadedb_embedded_unindexed` | `418.18 ms +- 0.00` | `581.00 ms +- 0.00` | `635.32 ms +- 0.00` |
| `olap/arcadedb_embedded_unindexed` | `4279.56 ms +- 0.00` | `4689.30 ms +- 0.00` | `4942.41 ms +- 0.00` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher
  execution timings, so they are not strictly comparable to the
  compile-plus-execute SQL
  paths.
- DuckDB can appear in indexed and unindexed modes here; each
  table includes whichever DuckDB runs are present in the current
  matrix.
- ArcadeDB Embedded is shown in both indexed and unindexed modes
  because the harness supports both direct-runtime paths in the
  current matrix.
- RSS values in these tables are point-in-time resident-memory snapshots
  taken at each named checkpoint, not deltas from the previous step
  and not
  peak-memory readings.
- Total RSS is the sum of benchmark-process RSS plus database-server
  RSS when
  the backend is external.

#### Large runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `oltp_create_type1_node` | `8543.99 ms +- 0.00` |
| `oltp_cross_type_lookup` | `8359.80 ms +- 0.00` |
| `oltp_delete_type1_edge` | `8631.45 ms +- 0.00` |
| `oltp_delete_type1_node` | `8140.65 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `8421.33 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `8481.71 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `8763.57 ms +- 0.00` |
| `oltp_program_create_and_link` | `8103.33 ms +- 0.00` |
| `oltp_type1_neighbors` | `9790.40 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `8288.67 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `8663.02 ms +- 0.00` |
| `oltp_update_type1_score` | `7941.78 ms +- 0.00` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `oltp_create_type1_node` | `155.89 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1377.21 ms +- 0.00` |
| `oltp_delete_type1_edge` | `1043.26 ms +- 0.00` |
| `oltp_delete_type1_node` | `1004.09 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `1605.89 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `1488.83 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `1208.21 ms +- 0.00` |
| `oltp_program_create_and_link` | `1053.13 ms +- 0.00` |
| `oltp_type1_neighbors` | `950.43 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `141.92 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1197.69 ms +- 0.00` |
| `oltp_update_type1_score` | `933.25 ms +- 0.00` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `oltp_create_type1_node` | `155.89 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1377.21 ms +- 0.00` |
| `oltp_delete_type1_edge` | `1043.26 ms +- 0.00` |
| `oltp_delete_type1_node` | `1004.09 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `1605.89 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `1488.83 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `1208.21 ms +- 0.00` |
| `oltp_program_create_and_link` | `1053.13 ms +- 0.00` |
| `oltp_type1_neighbors` | `950.43 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `141.92 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1197.69 ms +- 0.00` |
| `oltp_update_type1_score` | `933.25 ms +- 0.00` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `oltp_create_type1_node` | `0.66 ms +- 0.00` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.39 ms +- 0.00` |
| `oltp_delete_type1_node` | `0.59 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `1.14 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `0.00 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `0.00 ms +- 0.00` |
| `oltp_program_create_and_link` | `1.06 ms +- 0.00` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.41 ms +- 0.00` |
| `oltp_update_type1_score` | `0.37 ms +- 0.00` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `olap_cross_type_edge_rollup` | `9636.48 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `10243.80 ms +- 0.00` |
| `olap_relationship_function_projection` | `8881.60 ms +- 0.00` |
| `olap_three_type_path_count` | `8273.06 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `9498.02 ms +- 0.00` |
| `olap_type1_age_rollup` | `10590.34 ms +- 0.00` |
| `olap_type2_score_distribution` | `9961.80 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `9732.71 ms +- 0.00` |
| `olap_variable_length_reachability` | `9842.14 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `9477.39 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `9387.68 ms +- 0.00` |
| `olap_with_where_lower_projection` | `8709.63 ms +- 0.00` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `olap_cross_type_edge_rollup` | `26443.50 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `40660.83 ms +- 0.00` |
| `olap_relationship_function_projection` | `26043.30 ms +- 0.00` |
| `olap_three_type_path_count` | `21058.26 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `2093.76 ms +- 0.00` |
| `olap_type1_age_rollup` | `1456.88 ms +- 0.00` |
| `olap_type2_score_distribution` | `1940.37 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `1748.93 ms +- 0.00` |
| `olap_variable_length_reachability` | `1916.87 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `2848.82 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `2971.09 ms +- 0.00` |
| `olap_with_where_lower_projection` | `3021.85 ms +- 0.00` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `olap_cross_type_edge_rollup` | `26443.50 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `40660.83 ms +- 0.00` |
| `olap_relationship_function_projection` | `26043.30 ms +- 0.00` |
| `olap_three_type_path_count` | `21058.26 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `2093.76 ms +- 0.00` |
| `olap_type1_age_rollup` | `1456.88 ms +- 0.00` |
| `olap_type2_score_distribution` | `1940.37 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `1748.93 ms +- 0.00` |
| `olap_variable_length_reachability` | `1916.87 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `2848.82 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `2971.09 ms +- 0.00` |
| `olap_with_where_lower_projection` | `3021.85 ms +- 0.00` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Unindexed (1) |
| --- | --- |
| `olap_cross_type_edge_rollup` | `0.00 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `0.00 ms +- 0.00` |
| `olap_relationship_function_projection` | `0.00 ms +- 0.00` |
| `olap_three_type_path_count` | `0.00 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `0.00 ms +- 0.00` |
| `olap_type1_age_rollup` | `0.00 ms +- 0.00` |
| `olap_type2_score_distribution` | `0.00 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `0.00 ms +- 0.00` |
| `olap_variable_length_reachability` | `0.00 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `0.00 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `0.00 ms +- 0.00` |
| `olap_with_where_lower_projection` | `0.00 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p50`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `9.34 ms +- 0.00` | `1.97 ms +- 0.00` | `0.26 ms +- 0.00` | - |
| `oltp_create_type1_node` | `1.61 ms +- 0.00` | `1.13 ms +- 0.00` | `0.21 ms +- 0.00` | `0.03 ms +- 0.00` |
| `oltp_cross_type_lookup` | `6.24 ms +- 0.00` | `1.88 ms +- 0.00` | `0.25 ms +- 0.00` | `455.99 ms +- 0.00` |
| `oltp_delete_type1_edge` | `6.18 ms +- 0.00` | `1.35 ms +- 0.00` | `0.20 ms +- 0.00` | `456.11 ms +- 0.00` |
| `oltp_delete_type1_node` | `3.90 ms +- 0.00` | `0.87 ms +- 0.00` | `0.24 ms +- 0.00` | `459.05 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `10.07 ms +- 0.00` | `2.41 ms +- 0.00` | `0.28 ms +- 0.00` | `977.19 ms +- 0.00` |
| `oltp_program_create_and_link` | `6.98 ms +- 0.00` | `2.48 ms +- 0.00` | `0.21 ms +- 0.00` | `460.57 ms +- 0.00` |
| `oltp_type1_neighbors` | `5.65 ms +- 0.00` | `1.59 ms +- 0.00` | `0.25 ms +- 0.00` | `456.74 ms +- 0.00` |
| `oltp_type1_point_lookup` | `4.59 ms +- 0.00` | `1.38 ms +- 0.00` | `0.24 ms +- 0.00` | - |
| `oltp_unwind_literal_top2` | `1.35 ms +- 0.00` | `1.24 ms +- 0.00` | `0.20 ms +- 0.00` | `0.01 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `6.17 ms +- 0.00` | `1.52 ms +- 0.00` | `0.20 ms +- 0.00` | `458.62 ms +- 0.00` |
| `oltp_update_type1_score` | `4.57 ms +- 0.00` | `1.10 ms +- 0.00` | `0.20 ms +- 0.00` | `457.49 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p95`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `11.92 ms +- 0.00` | `2.74 ms +- 0.00` | `0.36 ms +- 0.00` | - |
| `oltp_create_type1_node` | `2.11 ms +- 0.00` | `1.86 ms +- 0.00` | `0.28 ms +- 0.00` | `0.08 ms +- 0.00` |
| `oltp_cross_type_lookup` | `7.78 ms +- 0.00` | `2.66 ms +- 0.00` | `0.30 ms +- 0.00` | `650.52 ms +- 0.00` |
| `oltp_delete_type1_edge` | `7.91 ms +- 0.00` | `2.14 ms +- 0.00` | `0.27 ms +- 0.00` | `662.91 ms +- 0.00` |
| `oltp_delete_type1_node` | `5.54 ms +- 0.00` | `1.39 ms +- 0.00` | `0.30 ms +- 0.00` | `649.84 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `12.93 ms +- 0.00` | `3.45 ms +- 0.00` | `0.36 ms +- 0.00` | `1237.15 ms +- 0.00` |
| `oltp_program_create_and_link` | `8.59 ms +- 0.00` | `3.52 ms +- 0.00` | `0.26 ms +- 0.00` | `643.33 ms +- 0.00` |
| `oltp_type1_neighbors` | `7.19 ms +- 0.00` | `2.20 ms +- 0.00` | `0.33 ms +- 0.00` | `652.64 ms +- 0.00` |
| `oltp_type1_point_lookup` | `5.93 ms +- 0.00` | `1.75 ms +- 0.00` | `0.40 ms +- 0.00` | - |
| `oltp_unwind_literal_top2` | `1.67 ms +- 0.00` | `1.79 ms +- 0.00` | `0.25 ms +- 0.00` | `0.02 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `7.83 ms +- 0.00` | `2.24 ms +- 0.00` | `0.27 ms +- 0.00` | `653.25 ms +- 0.00` |
| `oltp_update_type1_score` | `6.01 ms +- 0.00` | `1.70 ms +- 0.00` | `0.27 ms +- 0.00` | `660.24 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p99`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `12.59 ms +- 0.00` | `3.20 ms +- 0.00` | `0.46 ms +- 0.00` | - |
| `oltp_create_type1_node` | `2.63 ms +- 0.00` | `2.31 ms +- 0.00` | `0.35 ms +- 0.00` | `0.10 ms +- 0.00` |
| `oltp_cross_type_lookup` | `8.37 ms +- 0.00` | `3.23 ms +- 0.00` | `0.36 ms +- 0.00` | `720.13 ms +- 0.00` |
| `oltp_delete_type1_edge` | `8.43 ms +- 0.00` | `2.98 ms +- 0.00` | `0.34 ms +- 0.00` | `733.83 ms +- 0.00` |
| `oltp_delete_type1_node` | `5.81 ms +- 0.00` | `1.90 ms +- 0.00` | `0.39 ms +- 0.00` | `723.73 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `14.08 ms +- 0.00` | `4.28 ms +- 0.00` | `0.46 ms +- 0.00` | `1285.23 ms +- 0.00` |
| `oltp_program_create_and_link` | `9.27 ms +- 0.00` | `4.39 ms +- 0.00` | `0.31 ms +- 0.00` | `727.13 ms +- 0.00` |
| `oltp_type1_neighbors` | `7.85 ms +- 0.00` | `2.71 ms +- 0.00` | `0.42 ms +- 0.00` | `721.34 ms +- 0.00` |
| `oltp_type1_point_lookup` | `6.52 ms +- 0.00` | `2.08 ms +- 0.00` | `0.57 ms +- 0.00` | - |
| `oltp_unwind_literal_top2` | `2.12 ms +- 0.00` | `2.19 ms +- 0.00` | `0.32 ms +- 0.00` | `0.03 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `8.61 ms +- 0.00` | `2.83 ms +- 0.00` | `0.32 ms +- 0.00` | `724.08 ms +- 0.00` |
| `oltp_update_type1_score` | `6.61 ms +- 0.00` | `2.18 ms +- 0.00` | `0.38 ms +- 0.00` | `717.59 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p50`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `48.66 ms +- 0.00` | `2260.23 ms +- 0.00` | `9608.41 ms +- 0.00` | `11971.50 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `1434.54 ms +- 0.00` | `214.10 ms +- 0.00` | `15303.76 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `841.02 ms +- 0.00` | `4345.94 ms +- 0.00` | `44813.46 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `21.20 ms +- 0.00` | `40.34 ms +- 0.00` | `8420.21 ms +- 0.00` | `19110.95 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `7.71 ms +- 0.00` | `195.30 ms +- 0.00` | `1208.84 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `101.65 ms +- 0.00` | `4203.30 ms +- 0.00` | `9057.73 ms +- 0.00` | `12724.04 ms +- 0.00` |
| `olap_three_type_path_count` | `247.93 ms +- 0.00` | `6541.53 ms +- 0.00` | `5161.85 ms +- 0.00` | `140.74 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `11.15 ms +- 0.00` | `65.94 ms +- 0.00` | `1151.86 ms +- 0.00` | `1499.93 ms +- 0.00` |
| `olap_type1_age_rollup` | `7.27 ms +- 0.00` | `81.02 ms +- 0.00` | `1379.92 ms +- 0.00` | `925.62 ms +- 0.00` |
| `olap_type2_score_distribution` | `8.54 ms +- 0.00` | `49.04 ms +- 0.00` | `1336.07 ms +- 0.00` | `694.42 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `22.83 ms +- 0.00` | `6.10 ms +- 0.00` | `0.42 ms +- 0.00` | `638.33 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | `6082.14 ms +- 0.00` | `75491.50 ms +- 0.00` | - | - |
| `olap_variable_length_reachability` | `515.80 ms +- 0.00` | `3802.03 ms +- 0.00` | `1.71 ms +- 0.00` | `438.81 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `10.63 ms +- 0.00` | `116.89 ms +- 0.00` | `1648.88 ms +- 0.00` | `966.79 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `11.53 ms +- 0.00` | `116.79 ms +- 0.00` | `1058.31 ms +- 0.00` | `1221.95 ms +- 0.00` |
| `olap_with_where_lower_projection` | `11.43 ms +- 0.00` | `109.66 ms +- 0.00` | `1035.62 ms +- 0.00` | `1021.66 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p95`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `52.30 ms +- 0.00` | `3017.94 ms +- 0.00` | `10179.93 ms +- 0.00` | `12387.77 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `1493.88 ms +- 0.00` | `371.42 ms +- 0.00` | `16484.68 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `872.55 ms +- 0.00` | `4392.50 ms +- 0.00` | `45915.90 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `23.73 ms +- 0.00` | `42.13 ms +- 0.00` | `8552.70 ms +- 0.00` | `19627.99 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `9.58 ms +- 0.00` | `238.39 ms +- 0.00` | `1231.67 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `105.66 ms +- 0.00` | `5538.06 ms +- 0.00` | `9520.14 ms +- 0.00` | `13255.01 ms +- 0.00` |
| `olap_three_type_path_count` | `298.49 ms +- 0.00` | `7264.43 ms +- 0.00` | `5728.55 ms +- 0.00` | `275.06 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `13.13 ms +- 0.00` | `83.52 ms +- 0.00` | `1177.35 ms +- 0.00` | `2277.64 ms +- 0.00` |
| `olap_type1_age_rollup` | `8.90 ms +- 0.00` | `107.95 ms +- 0.00` | `1412.83 ms +- 0.00` | `1487.51 ms +- 0.00` |
| `olap_type2_score_distribution` | `10.36 ms +- 0.00` | `52.90 ms +- 0.00` | `1374.79 ms +- 0.00` | `946.65 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `26.90 ms +- 0.00` | `7.21 ms +- 0.00` | `0.69 ms +- 0.00` | `1108.36 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | `6236.49 ms +- 0.00` | `79967.46 ms +- 0.00` | - | - |
| `olap_variable_length_reachability` | `538.01 ms +- 0.00` | `5100.30 ms +- 0.00` | `2.24 ms +- 0.00` | `640.14 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `13.29 ms +- 0.00` | `120.70 ms +- 0.00` | `1770.86 ms +- 0.00` | `1294.73 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `14.12 ms +- 0.00` | `119.18 ms +- 0.00` | `1228.02 ms +- 0.00` | `1619.13 ms +- 0.00` |
| `olap_with_where_lower_projection` | `14.46 ms +- 0.00` | `113.46 ms +- 0.00` | `1220.17 ms +- 0.00` | `1351.59 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p99`

| Query | DuckDB Unindexed (1) | PostgreSQL Indexed (1) | Neo4j Indexed (1) | ArcadeDB Unindexed (1) |
| --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `53.19 ms +- 0.00` | `3213.39 ms +- 0.00` | `10462.13 ms +- 0.00` | `13808.91 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `1530.11 ms +- 0.00` | `568.83 ms +- 0.00` | `16705.85 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `887.85 ms +- 0.00` | `4414.95 ms +- 0.00` | `46597.37 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `26.18 ms +- 0.00` | `43.63 ms +- 0.00` | `8583.01 ms +- 0.00` | `19756.63 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `10.35 ms +- 0.00` | `254.26 ms +- 0.00` | `1242.76 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `116.94 ms +- 0.00` | `5739.13 ms +- 0.00` | `9669.57 ms +- 0.00` | `13605.38 ms +- 0.00` |
| `olap_three_type_path_count` | `304.68 ms +- 0.00` | `7378.90 ms +- 0.00` | `6096.26 ms +- 0.00` | `506.27 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `14.09 ms +- 0.00` | `89.22 ms +- 0.00` | `1194.01 ms +- 0.00` | `2512.25 ms +- 0.00` |
| `olap_type1_age_rollup` | `9.26 ms +- 0.00` | `120.48 ms +- 0.00` | `1434.30 ms +- 0.00` | `1650.45 ms +- 0.00` |
| `olap_type2_score_distribution` | `11.04 ms +- 0.00` | `56.44 ms +- 0.00` | `1407.98 ms +- 0.00` | `979.80 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `28.48 ms +- 0.00` | `7.32 ms +- 0.00` | `2.38 ms +- 0.00` | `1335.10 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | `6509.90 ms +- 0.00` | `81225.58 ms +- 0.00` | - | - |
| `olap_variable_length_reachability` | `552.45 ms +- 0.00` | `5781.73 ms +- 0.00` | `3.17 ms +- 0.00` | `701.18 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `13.70 ms +- 0.00` | `123.22 ms +- 0.00` | `3244.05 ms +- 0.00` | `1364.89 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `14.72 ms +- 0.00` | `119.25 ms +- 0.00` | `1245.19 ms +- 0.00` | `1668.55 ms +- 0.00` |
| `olap_with_where_lower_projection` | `14.94 ms +- 0.00` | `114.46 ms +- 0.00` | `1252.48 ms +- 0.00` | `1419.53 ms +- 0.00` |

### Medium runtime dataset

The current medium runtime matrix used the `medium` preset with `5000` OLTP iterations / `100` OLTP warmup and `100` OLAP iterations / `10` OLAP warmup.

That corresponds to roughly:

- `600,000` total nodes
- `6,223,200` total edges
- `6` node types
- `8` edge types
- `37` property fields across the schema (`22` per node, `15` per edge)
- `11` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.40.1`
- `DuckDB`: `1.5.2`
- `PostgreSQL`: `16.13 (Debian 16.13-1.pgdg13+1)`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.2.post1`
- `LadybugDB`: `0.16.1`

Runtime benchmark artifacts also record these engine versions in a top-level
`database_versions` object inside each JSON payload.

For the SQL backends in this refreshed run, setup follows the more standard
bulk-load sequence: `schema -> ingest -> index -> analyze`. That means the
reported `ingest` step does not include index-maintenance cost during row
insertion, and the `index` step captures post-load index construction.

Neo4j is a direct-Cypher runner rather than a compile-plus-execute SQL
path.

LadybugDB is also a direct-Cypher runner, and it currently uses a
post-load `CHECKPOINT` instead of an `ANALYZE` step. In the summary
tables below, that checkpoint time is shown in the `Analyze` column so
the setup layout stays consistent across engines.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
summary tables below, that engine-specific post-load work is folded into the
`Analyze` column, along with the checkpoint step, so the setup layout stays
consistent across engines.


OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `13.06 ms +- 0.12` | `5.59 ms +- 0.28` | `51556.46 ms +- 2614.13` | `4929.25 ms +- 242.15` | `1509.99 ms +- 27.76` | `1.23 ms +- 0.01` | `1.40 ms +- 0.13` | `1.86 ms +- 0.40` |
| SQLite Unindexed (3) | `9.81 ms +- 2.96` | `6.42 ms +- 1.44` | `52306.67 ms +- 593.73` | `1462.54 ms +- 41.58` | `269.61 ms +- 6.67` | `68.86 ms +- 0.61` | `76.50 ms +- 5.86` | `82.78 ms +- 9.05` |
| DuckDB Indexed (3) | `17.37 ms +- 3.86` | `171.60 ms +- 2.34` | `12056.21 ms +- 268.88` | `4209.23 ms +- 52.76` | `0.36 ms +- 0.04` | `2.52 ms +- 0.17` | `3.33 ms +- 0.46` | `4.34 ms +- 0.97` |
| DuckDB Unindexed (3) | `15.93 ms +- 0.10` | `168.09 ms +- 1.24` | `11714.28 ms +- 87.23` | `91.00 ms +- 5.17` | `0.13 ms +- 0.02` | `3.00 ms +- 0.07` | `3.68 ms +- 0.11` | `4.25 ms +- 0.07` |
| PostgreSQL Indexed (3) | `4.59 ms +- 0.21` | `614.78 ms +- 18.97` | `99684.32 ms +- 6547.31` | `8670.03 ms +- 4876.05` | `2572.96 ms +- 60.44` | `1.49 ms +- 0.04` | `1.86 ms +- 0.29` | `2.35 ms +- 0.40` |
| PostgreSQL Unindexed (3) | `4.90 ms +- 0.45` | `589.91 ms +- 76.02` | `104638.17 ms +- 3311.46` | `75.25 ms +- 4.24` | `4162.39 ms +- 1907.20` | `42.45 ms +- 10.64` | `45.82 ms +- 10.40` | `48.99 ms +- 9.90` |
| Neo4j Indexed (3) | `58.66 ms +- 2.55` | `468.76 ms +- 6.48` | `455055.40 ms +- 5035.10` | `12294.75 ms +- 2326.81` | `0.00 ms +- 0.00` | `0.23 ms +- 0.01` | `0.35 ms +- 0.11` | `0.53 ms +- 0.25` |
| Neo4j Unindexed (3) | `61.82 ms +- 13.84` | `460.85 ms +- 7.92` | `456545.26 ms +- 6858.25` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `77.26 ms +- 1.95` | `86.66 ms +- 4.86` | `93.78 ms +- 8.59` |
| ArcadeDB Indexed (3) | `282.69 ms +- 7.64` | `311.40 ms +- 28.16` | `178192.85 ms +- 1011.78` | `19788.30 ms +- 501.52` | `0.00 ms +- 0.00` | `0.06 ms +- 0.00` | `0.08 ms +- 0.00` | `0.11 ms +- 0.01` |
| ArcadeDB Unindexed (3) | `291.17 ms +- 26.01` | `379.44 ms +- 47.65` | `178329.49 ms +- 4634.73` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `43.85 ms +- 0.38` | `46.51 ms +- 0.73` | `48.86 ms +- 1.00` |
| LadybugDB Unindexed (3) | `80.93 ms +- 5.68` | `68.20 ms +- 0.20` | `4362388.75 ms +- 186486.05` | `0.00 ms +- 0.00` | `27.81 ms +- 8.54` | `4.25 ms +- 0.29` | `5.86 ms +- 0.44` | `6.68 ms +- 0.63` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `7.26 ms +- 0.09` | `5.64 ms +- 0.67` | `52824.45 ms +- 2429.92` | `4838.75 ms +- 207.28` | `1515.51 ms +- 30.54` | `4585.57 ms +- 39.36` | `4787.13 ms +- 135.62` | `4993.76 ms +- 259.59` |
| SQLite Unindexed (3) | `7.28 ms +- 0.10` | `5.44 ms +- 0.47` | `52140.64 ms +- 1044.44` | `1439.95 ms +- 26.45` | `253.43 ms +- 10.22` | `4464.60 ms +- 26.62` | `4665.24 ms +- 96.39` | `4757.78 ms +- 157.45` |
| DuckDB Indexed (3) | `8.68 ms +- 0.18` | `170.83 ms +- 1.15` | `12005.13 ms +- 157.49` | `4221.80 ms +- 58.72` | `0.32 ms +- 0.04` | `70.88 ms +- 1.55` | `74.59 ms +- 2.17` | `76.65 ms +- 2.56` |
| DuckDB Unindexed (3) | `8.69 ms +- 0.56` | `169.56 ms +- 2.49` | `11825.19 ms +- 118.94` | `87.57 ms +- 0.10` | `0.12 ms +- 0.02` | `69.68 ms +- 1.13` | `73.03 ms +- 1.57` | `75.28 ms +- 2.11` |
| PostgreSQL Indexed (3) | `4.54 ms +- 0.40` | `923.54 ms +- 61.98` | `102115.74 ms +- 10920.24` | `7771.72 ms +- 2329.81` | `2494.49 ms +- 43.37` | `519.79 ms +- 14.08` | `596.60 ms +- 35.24` | `620.09 ms +- 36.77` |
| PostgreSQL Unindexed (3) | `4.03 ms +- 0.13` | `790.91 ms +- 21.25` | `102531.28 ms +- 10995.31` | `75.96 ms +- 1.41` | `3427.49 ms +- 838.27` | `635.71 ms +- 22.20` | `749.51 ms +- 98.32` | `800.98 ms +- 106.20` |
| Neo4j Indexed (3) | `58.66 ms +- 2.55` | `468.76 ms +- 6.48` | `455055.40 ms +- 5035.10` | `12294.75 ms +- 2326.81` | `0.00 ms +- 0.00` | `523.54 ms +- 5.21` | `538.03 ms +- 6.55` | `557.56 ms +- 13.37` |
| Neo4j Unindexed (3) | `61.82 ms +- 13.84` | `460.85 ms +- 7.92` | `456545.26 ms +- 6858.25` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `568.31 ms +- 7.57` | `584.68 ms +- 9.89` | `612.74 ms +- 31.38` |
| ArcadeDB Indexed (3) | `1.35 ms +- 0.01` | `53.63 ms +- 0.83` | `205780.07 ms +- 2862.77` | `19133.44 ms +- 888.97` | `6281.02 ms +- 192.23` | `1266.01 ms +- 81.18` | `1368.48 ms +- 91.65` | `1407.60 ms +- 78.55` |
| ArcadeDB Unindexed (3) | `1.50 ms +- 0.16` | `75.80 ms +- 2.81` | `209640.33 ms +- 8602.30` | `0.00 ms +- 0.00` | `6265.61 ms +- 183.35` | `1243.61 ms +- 42.02` | `1334.51 ms +- 47.43` | `1374.11 ms +- 56.88` |
| LadybugDB Unindexed (3) | `80.91 ms +- 15.52` | `60.19 ms +- 30.45` | `4219583.22 ms +- 140977.44` | `0.00 ms +- 0.00` | `22.66 ms +- 0.47` | `1916.91 ms +- 34.16` | `1988.54 ms +- 47.69` | `2013.39 ms +- 63.60` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `105.36 MiB +- 0.67` | `105.40 MiB +- 0.67` | `108.70 MiB +- 0.66` | `108.70 MiB +- 0.66` | `108.70 MiB +- 0.66` | `502.35 MiB +- 1.91` |
| SQLite Unindexed (3) | `104.04 MiB +- 0.19` | `104.13 MiB +- 0.18` | `108.46 MiB +- 0.17` | `108.46 MiB +- 0.17` | `108.46 MiB +- 0.17` | `500.76 MiB +- 4.14` |
| DuckDB Indexed (3) | `101.16 MiB +- 1.13` | `104.92 MiB +- 1.06` | `908.19 MiB +- 37.29` | `998.10 MiB +- 6.73` | `998.10 MiB +- 6.73` | `899.02 MiB +- 28.76` |
| DuckDB Unindexed (3) | `101.48 MiB +- 1.03` | `105.18 MiB +- 1.02` | `910.93 MiB +- 19.33` | `911.01 MiB +- 19.33` | `911.01 MiB +- 19.33` | `926.71 MiB +- 34.22` |
| PostgreSQL Indexed (3) | `132.73 MiB +- 0.10` | `135.42 MiB +- 0.31` | `375.27 MiB +- 0.38` | `393.33 MiB +- 1.34` | `393.47 MiB +- 1.46` | `871.15 MiB +- 16.38` |
| PostgreSQL Unindexed (3) | `133.20 MiB +- 0.71` | `135.86 MiB +- 0.50` | `375.94 MiB +- 0.53` | `360.70 MiB +- 0.30` | `360.77 MiB +- 0.69` | `833.02 MiB +- 17.60` |
| Neo4j Indexed (3) | `657.81 MiB +- 9.17` | `722.59 MiB +- 9.47` | `2407.63 MiB +- 366.33` | `2986.87 MiB +- 46.70` | `0.00 MiB +- 0.00` | `2125.55 MiB +- 108.68` |
| Neo4j Unindexed (3) | `670.48 MiB +- 11.52` | `721.03 MiB +- 8.30` | `2403.85 MiB +- 423.80` | `2403.51 MiB +- 424.39` | `0.00 MiB +- 0.00` | `2162.54 MiB +- 111.57` |
| ArcadeDB Indexed (3) | `165.06 MiB +- 0.21` | `261.51 MiB +- 12.99` | `4279.30 MiB +- 18.73` | `5578.78 MiB +- 2157.05` | `5578.78 MiB +- 2157.05` | `5402.74 MiB +- 273.47` |
| ArcadeDB Unindexed (3) | `165.27 MiB +- 2.68` | `225.25 MiB +- 8.42` | `4265.96 MiB +- 42.01` | `4265.96 MiB +- 42.01` | `4265.96 MiB +- 42.01` | `5537.81 MiB +- 26.57` |
| LadybugDB Unindexed (3) | `285.08 MiB +- 0.12` | `321.62 MiB +- 0.18` | `4351.01 MiB +- 73.18` | `4351.01 MiB +- 73.18` | `4351.32 MiB +- 73.24` | `4189.95 MiB +- 80.73` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `498.99 MiB +- 2.47` | `499.03 MiB +- 2.47` | `385.16 MiB +- 8.30` | `385.16 MiB +- 8.30` | `385.16 MiB +- 8.30` | `291.80 MiB +- 5.09` |
| SQLite Unindexed (3) | `497.40 MiB +- 4.08` | `497.44 MiB +- 4.08` | `382.59 MiB +- 4.90` | `382.59 MiB +- 4.90` | `382.59 MiB +- 4.90` | `300.67 MiB +- 4.22` |
| DuckDB Indexed (3) | `595.00 MiB +- 23.26` | `594.13 MiB +- 24.91` | `1331.91 MiB +- 35.23` | `1446.54 MiB +- 26.83` | `1446.54 MiB +- 26.83` | `1438.46 MiB +- 50.82` |
| DuckDB Unindexed (3) | `599.51 MiB +- 47.77` | `598.95 MiB +- 47.24` | `1331.98 MiB +- 55.79` | `1331.65 MiB +- 55.34` | `1331.65 MiB +- 55.34` | `1407.85 MiB +- 43.29` |
| PostgreSQL Indexed (3) | `840.33 MiB +- 16.03` | `767.80 MiB +- 15.72` | `851.77 MiB +- 16.00` | `870.37 MiB +- 16.64` | `870.00 MiB +- 15.80` | `643.56 MiB +- 7.76` |
| PostgreSQL Unindexed (3) | `811.96 MiB +- 17.52` | `762.54 MiB +- 18.13` | `847.92 MiB +- 19.24` | `832.29 MiB +- 19.10` | `832.79 MiB +- 19.46` | `585.33 MiB +- 10.38` |
| Neo4j Indexed (3) | `657.81 MiB +- 9.17` | `722.59 MiB +- 9.47` | `2407.63 MiB +- 366.33` | `2986.87 MiB +- 46.70` | `0.00 MiB +- 0.00` | `8556.04 MiB +- 7456.70` |
| Neo4j Unindexed (3) | `670.48 MiB +- 11.52` | `721.03 MiB +- 8.30` | `2403.85 MiB +- 423.80` | `2403.51 MiB +- 424.39` | `0.00 MiB +- 0.00` | `5018.84 MiB +- 340.24` |
| ArcadeDB Indexed (3) | `5402.97 MiB +- 273.35` | `5403.71 MiB +- 273.49` | `4402.74 MiB +- 77.95` | `4473.13 MiB +- 75.86` | `5144.12 MiB +- 144.41` | `9097.13 MiB +- 50.69` |
| ArcadeDB Unindexed (3) | `5537.82 MiB +- 26.58` | `5538.09 MiB +- 26.57` | `4365.83 MiB +- 67.84` | `4365.83 MiB +- 67.84` | `5037.68 MiB +- 210.81` | `8599.57 MiB +- 414.93` |
| LadybugDB Unindexed (3) | `2499.70 MiB +- 14.70` | `2501.62 MiB +- 12.38` | `4476.90 MiB +- 119.48` | `4476.90 MiB +- 119.48` | `4476.90 MiB +- 119.48` | `13070.55 MiB +- 119.42` |

#### Medium runtime suite comparison

This rolls the medium-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.23 ms +- 0.01` | `1.40 ms +- 0.13` | `1.86 ms +- 0.40` |
| `olap/sqlite_indexed` | `4585.57 ms +- 39.36` | `4787.13 ms +- 135.62` | `4993.76 ms +- 259.59` |
| `oltp/sqlite_unindexed` | `68.86 ms +- 0.61` | `76.50 ms +- 5.86` | `82.78 ms +- 9.05` |
| `olap/sqlite_unindexed` | `4464.60 ms +- 26.62` | `4665.24 ms +- 96.39` | `4757.78 ms +- 157.45` |
| `oltp/duckdb_indexed` | `2.52 ms +- 0.17` | `3.33 ms +- 0.46` | `4.34 ms +- 0.97` |
| `olap/duckdb_indexed` | `70.88 ms +- 1.55` | `74.59 ms +- 2.17` | `76.65 ms +- 2.56` |
| `oltp/duckdb_unindexed` | `3.00 ms +- 0.07` | `3.68 ms +- 0.11` | `4.25 ms +- 0.07` |
| `olap/duckdb_unindexed` | `69.68 ms +- 1.13` | `73.03 ms +- 1.57` | `75.28 ms +- 2.11` |
| `oltp/postgresql_indexed` | `1.49 ms +- 0.04` | `1.86 ms +- 0.29` | `2.35 ms +- 0.40` |
| `olap/postgresql_indexed` | `519.79 ms +- 14.08` | `596.60 ms +- 35.24` | `620.09 ms +- 36.77` |
| `oltp/postgresql_unindexed` | `42.45 ms +- 10.64` | `45.82 ms +- 10.40` | `48.99 ms +- 9.90` |
| `olap/postgresql_unindexed` | `635.71 ms +- 22.20` | `749.51 ms +- 98.32` | `800.98 ms +- 106.20` |
| `oltp/neo4j_indexed` | `0.23 ms +- 0.01` | `0.35 ms +- 0.11` | `0.53 ms +- 0.25` |
| `olap/neo4j_indexed` | `523.54 ms +- 5.21` | `538.03 ms +- 6.55` | `557.56 ms +- 13.37` |
| `oltp/neo4j_unindexed` | `77.26 ms +- 1.95` | `86.66 ms +- 4.86` | `93.78 ms +- 8.59` |
| `olap/neo4j_unindexed` | `568.31 ms +- 7.57` | `584.68 ms +- 9.89` | `612.74 ms +- 31.38` |
| `oltp/arcadedb_embedded_indexed` | `0.06 ms +- 0.00` | `0.08 ms +- 0.00` | `0.11 ms +- 0.01` |
| `olap/arcadedb_embedded_indexed` | `1266.01 ms +- 81.18` | `1368.48 ms +- 91.65` | `1407.60 ms +- 78.55` |
| `oltp/arcadedb_embedded_unindexed` | `43.85 ms +- 0.38` | `46.51 ms +- 0.73` | `48.86 ms +- 1.00` |
| `olap/arcadedb_embedded_unindexed` | `1243.61 ms +- 42.02` | `1334.51 ms +- 47.43` | `1374.11 ms +- 56.88` |
| `oltp/ladybug_unindexed` | `4.25 ms +- 0.29` | `5.86 ms +- 0.44` | `6.68 ms +- 0.63` |
| `olap/ladybug_unindexed` | `1916.91 ms +- 34.16` | `1988.54 ms +- 47.69` | `2013.39 ms +- 63.60` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher
  execution timings, so they are not strictly comparable to the
  compile-plus-execute SQL
  paths.
- DuckDB can appear in indexed and unindexed modes here; each
  table includes whichever DuckDB runs are present in the current
  matrix.
- ArcadeDB Embedded is shown in both indexed and unindexed modes
  because the harness supports both direct-runtime paths in the
  current matrix.
- LadybugDB is also a single-path run here. The current harness benchmarks
  an unindexed direct-Cypher path.
- RSS values in these tables are point-in-time resident-memory snapshots
  taken at each named checkpoint, not deltas from the previous step
  and not
  peak-memory readings.
- Total RSS is the sum of benchmark-process RSS plus database-server
  RSS when
  the backend is external.

#### Medium runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `1258.56 ms +- 49.66` | `1145.60 ms +- 31.69` |
| `oltp_create_type1_node` | `1227.35 ms +- 123.36` | `1090.51 ms +- 36.10` |
| `oltp_cross_type_lookup` | `1173.89 ms +- 3.87` | `1088.89 ms +- 49.60` |
| `oltp_delete_type1_edge` | `1241.35 ms +- 175.60` | `1097.27 ms +- 64.12` |
| `oltp_delete_type1_node` | `1170.86 ms +- 61.70` | `1121.53 ms +- 26.09` |
| `oltp_merge_cross_type_edge` | `1150.99 ms +- 50.91` | `1127.59 ms +- 47.38` |
| `oltp_optional_missing_type1_lookup` | `1169.10 ms +- 31.97` | `1109.43 ms +- 34.98` |
| `oltp_optional_type1_lookup` | `1206.07 ms +- 104.37` | `1078.22 ms +- 51.65` |
| `oltp_program_create_and_link` | `1259.37 ms +- 70.68` | `1089.50 ms +- 66.19` |
| `oltp_type1_neighbors` | `1170.67 ms +- 68.48` | `1075.02 ms +- 108.95` |
| `oltp_type1_point_lookup` | `1186.94 ms +- 53.07` | `1080.89 ms +- 68.90` |
| `oltp_unwind_literal_top2` | `1162.99 ms +- 34.77` | `1105.82 ms +- 28.44` |
| `oltp_update_cross_type_edge_rank` | `1247.07 ms +- 194.90` | `1132.20 ms +- 10.46` |
| `oltp_update_type1_score` | `1139.46 ms +- 107.37` | `1080.60 ms +- 33.84` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `159.52 ms +- 3.51` | `384.31 ms +- 51.21` |
| `oltp_create_type1_node` | `135.77 ms +- 4.22` | `153.31 ms +- 29.03` |
| `oltp_cross_type_lookup` | `232.17 ms +- 22.74` | `346.24 ms +- 24.76` |
| `oltp_delete_type1_edge` | `185.85 ms +- 37.39` | `308.39 ms +- 18.68` |
| `oltp_delete_type1_node` | `143.76 ms +- 6.53` | `317.56 ms +- 33.38` |
| `oltp_merge_cross_type_edge` | `192.62 ms +- 31.25` | `405.08 ms +- 12.34` |
| `oltp_optional_missing_type1_lookup` | `362.27 ms +- 36.08` | `361.68 ms +- 31.81` |
| `oltp_optional_type1_lookup` | `378.77 ms +- 30.43` | `347.93 ms +- 43.47` |
| `oltp_program_create_and_link` | `154.56 ms +- 10.03` | `326.05 ms +- 17.66` |
| `oltp_type1_neighbors` | `209.13 ms +- 42.77` | `327.07 ms +- 16.84` |
| `oltp_type1_point_lookup` | `226.55 ms +- 54.07` | `316.71 ms +- 9.54` |
| `oltp_unwind_literal_top2` | `203.20 ms +- 23.60` | `201.69 ms +- 11.07` |
| `oltp_update_cross_type_edge_rank` | `180.02 ms +- 24.05` | `310.31 ms +- 39.45` |
| `oltp_update_type1_score` | `184.37 ms +- 35.91` | `321.64 ms +- 29.87` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `159.52 ms +- 3.51` | `384.31 ms +- 51.21` |
| `oltp_create_type1_node` | `135.77 ms +- 4.22` | `153.31 ms +- 29.03` |
| `oltp_cross_type_lookup` | `232.17 ms +- 22.74` | `346.24 ms +- 24.76` |
| `oltp_delete_type1_edge` | `185.85 ms +- 37.39` | `308.39 ms +- 18.68` |
| `oltp_delete_type1_node` | `143.76 ms +- 6.53` | `317.56 ms +- 33.38` |
| `oltp_merge_cross_type_edge` | `192.62 ms +- 31.25` | `405.08 ms +- 12.34` |
| `oltp_optional_missing_type1_lookup` | `362.27 ms +- 36.08` | `361.68 ms +- 31.81` |
| `oltp_optional_type1_lookup` | `378.77 ms +- 30.43` | `347.93 ms +- 43.47` |
| `oltp_program_create_and_link` | `154.56 ms +- 10.03` | `326.05 ms +- 17.66` |
| `oltp_type1_neighbors` | `209.13 ms +- 42.77` | `327.07 ms +- 16.84` |
| `oltp_type1_point_lookup` | `226.55 ms +- 54.07` | `316.71 ms +- 9.54` |
| `oltp_unwind_literal_top2` | `203.20 ms +- 23.60` | `201.69 ms +- 11.07` |
| `oltp_update_cross_type_edge_rank` | `180.02 ms +- 24.05` | `310.31 ms +- 39.45` |
| `oltp_update_type1_score` | `184.37 ms +- 35.91` | `321.64 ms +- 29.87` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `0.72 ms +- 0.04` | `0.55 ms +- 0.06` |
| `oltp_create_type1_node` | `0.52 ms +- 0.06` | `0.54 ms +- 0.02` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.31 ms +- 0.02` | `0.27 ms +- 0.03` |
| `oltp_delete_type1_node` | `0.33 ms +- 0.07` | `0.30 ms +- 0.07` |
| `oltp_merge_cross_type_edge` | `0.57 ms +- 0.08` | `0.52 ms +- 0.05` |
| `oltp_optional_missing_type1_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_program_create_and_link` | `0.60 ms +- 0.16` | `0.52 ms +- 0.01` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.43 ms +- 0.01` | `0.40 ms +- 0.02` |
| `oltp_update_type1_score` | `0.65 ms +- 0.29` | `0.37 ms +- 0.01` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1258.22 ms +- 66.84` | `1297.21 ms +- 348.49` |
| `olap_fixed_length_path_projection` | `1213.45 ms +- 39.89` | `1163.41 ms +- 127.18` |
| `olap_fixed_length_path_with_rebinding` | `1234.07 ms +- 72.81` | `1144.42 ms +- 15.32` |
| `olap_graph_introspection_rollup` | `1309.06 ms +- 155.98` | `1324.69 ms +- 223.23` |
| `olap_relationship_function_projection` | `1255.06 ms +- 53.01` | `1262.02 ms +- 347.18` |
| `olap_three_type_path_count` | `1214.14 ms +- 100.36` | `1363.13 ms +- 344.94` |
| `olap_type1_active_leaderboard` | `1174.02 ms +- 54.23` | `1129.35 ms +- 52.33` |
| `olap_type1_age_rollup` | `1213.96 ms +- 93.52` | `1155.41 ms +- 180.40` |
| `olap_type2_score_distribution` | `1173.51 ms +- 66.33` | `1243.61 ms +- 119.94` |
| `olap_variable_length_grouped_max_rollup` | `1262.66 ms +- 70.89` | `1229.54 ms +- 69.95` |
| `olap_variable_length_reachability` | `1237.77 ms +- 87.23` | `1311.42 ms +- 288.75` |
| `olap_with_scalar_rebinding` | `1249.53 ms +- 62.98` | `1295.51 ms +- 143.71` |
| `olap_with_size_predicate_projection` | `1208.76 ms +- 91.67` | `1316.26 ms +- 291.64` |
| `olap_with_where_lower_projection` | `1282.16 ms +- 150.16` | `1288.11 ms +- 404.71` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `2311.25 ms +- 186.69` | `2598.37 ms +- 583.32` |
| `olap_fixed_length_path_projection` | `19045.44 ms +- 858.95` | `19387.97 ms +- 2380.65` |
| `olap_fixed_length_path_with_rebinding` | `19270.03 ms +- 943.37` | `19270.42 ms +- 735.68` |
| `olap_graph_introspection_rollup` | `2855.28 ms +- 175.56` | `3334.67 ms +- 726.89` |
| `olap_relationship_function_projection` | `2479.54 ms +- 376.15` | `2554.41 ms +- 542.51` |
| `olap_three_type_path_count` | `1281.29 ms +- 77.02` | `1275.27 ms +- 362.45` |
| `olap_type1_active_leaderboard` | `553.02 ms +- 24.56` | `624.92 ms +- 77.19` |
| `olap_type1_age_rollup` | `494.73 ms +- 70.45` | `494.85 ms +- 44.98` |
| `olap_type2_score_distribution` | `527.78 ms +- 82.96` | `499.49 ms +- 48.86` |
| `olap_variable_length_grouped_max_rollup` | `212.62 ms +- 8.90` | `453.73 ms +- 78.75` |
| `olap_variable_length_reachability` | `277.19 ms +- 31.04` | `478.16 ms +- 89.91` |
| `olap_with_scalar_rebinding` | `552.06 ms +- 38.55` | `617.32 ms +- 162.30` |
| `olap_with_size_predicate_projection` | `636.53 ms +- 28.51` | `701.49 ms +- 142.36` |
| `olap_with_where_lower_projection` | `584.39 ms +- 104.86` | `715.71 ms +- 233.33` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `2311.25 ms +- 186.69` | `2598.37 ms +- 583.32` |
| `olap_fixed_length_path_projection` | `19045.44 ms +- 858.95` | `19387.97 ms +- 2380.65` |
| `olap_fixed_length_path_with_rebinding` | `19270.03 ms +- 943.37` | `19270.42 ms +- 735.68` |
| `olap_graph_introspection_rollup` | `2855.28 ms +- 175.56` | `3334.67 ms +- 726.89` |
| `olap_relationship_function_projection` | `2479.54 ms +- 376.15` | `2554.41 ms +- 542.51` |
| `olap_three_type_path_count` | `1281.29 ms +- 77.02` | `1275.27 ms +- 362.45` |
| `olap_type1_active_leaderboard` | `553.02 ms +- 24.56` | `624.92 ms +- 77.19` |
| `olap_type1_age_rollup` | `494.73 ms +- 70.45` | `494.85 ms +- 44.98` |
| `olap_type2_score_distribution` | `527.78 ms +- 82.96` | `499.49 ms +- 48.86` |
| `olap_variable_length_grouped_max_rollup` | `212.62 ms +- 8.90` | `453.73 ms +- 78.75` |
| `olap_variable_length_reachability` | `277.19 ms +- 31.04` | `478.16 ms +- 89.91` |
| `olap_with_scalar_rebinding` | `552.06 ms +- 38.55` | `617.32 ms +- 162.30` |
| `olap_with_size_predicate_projection` | `636.53 ms +- 28.51` | `701.49 ms +- 142.36` |
| `olap_with_where_lower_projection` | `584.39 ms +- 104.86` | `715.71 ms +- 233.33` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_fixed_length_path_with_rebinding` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_relationship_function_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_three_type_path_count` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type1_age_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type2_score_distribution` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_variable_length_reachability` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_where_lower_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.68 ms +- 0.01` | `17.59 ms +- 0.30` | `2.49 ms +- 0.18` | `4.52 ms +- 0.18` | `1.85 ms +- 0.04` | `19.44 ms +- 0.36` | `0.25 ms +- 0.02` | `44.14 ms +- 0.41` | `0.10 ms +- 0.01` | `89.66 ms +- 0.74` | `6.47 ms +- 0.27` |
| `oltp_create_type1_node` | `0.82 ms +- 0.01` | `0.79 ms +- 0.01` | `1.35 ms +- 0.05` | `1.28 ms +- 0.03` | `1.01 ms +- 0.04` | `0.97 ms +- 0.00` | `0.21 ms +- 0.01` | `0.20 ms +- 0.01` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.32 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.36 ms +- 0.00` | `57.71 ms +- 0.58` | `3.67 ms +- 0.28` | `3.50 ms +- 0.18` | `1.76 ms +- 0.03` | `39.84 ms +- 2.08` | `0.23 ms +- 0.01` | `470.36 ms +- 29.14` | `0.02 ms +- 0.00` | `41.28 ms +- 0.25` | `4.91 ms +- 0.40` |
| `oltp_delete_type1_edge` | `0.86 ms +- 0.01` | `57.97 ms +- 0.52` | `3.63 ms +- 0.27` | `3.34 ms +- 0.10` | `1.26 ms +- 0.04` | `86.48 ms +- 1.32` | `0.20 ms +- 0.01` | `21.09 ms +- 0.35` | `0.04 ms +- 0.00` | `44.10 ms +- 0.75` | `4.22 ms +- 0.56` |
| `oltp_delete_type1_node` | `0.88 ms +- 0.02` | `479.30 ms +- 5.74` | `0.91 ms +- 0.01` | `1.90 ms +- 0.03` | `0.83 ms +- 0.03` | `270.82 ms +- 0.00` | `0.24 ms +- 0.01` | `22.66 ms +- 0.28` | `0.07 ms +- 0.00` | `44.15 ms +- 1.01` | `3.67 ms +- 0.15` |
| `oltp_merge_cross_type_edge` | `1.92 ms +- 0.02` | `68.47 ms +- 0.90` | `3.38 ms +- 0.39` | `5.24 ms +- 0.14` | `2.27 ms +- 0.07` | `75.60 ms +- 0.96` | `0.26 ms +- 0.01` | `44.27 ms +- 0.80` | `0.10 ms +- 0.00` | `90.35 ms +- 1.32` | `12.10 ms +- 1.09` |
| `oltp_program_create_and_link` | `1.97 ms +- 0.02` | `9.94 ms +- 0.48` | `3.36 ms +- 0.21` | `4.12 ms +- 0.04` | `2.34 ms +- 0.08` | `10.86 ms +- 0.94` | `0.22 ms +- 0.01` | `22.14 ms +- 0.96` | `0.19 ms +- 0.01` | `46.99 ms +- 3.41` | `3.50 ms +- 0.17` |
| `oltp_type1_neighbors` | `1.14 ms +- 0.00` | `57.54 ms +- 0.08` | `3.36 ms +- 0.22` | `3.15 ms +- 0.07` | `1.53 ms +- 0.03` | `40.30 ms +- 1.62` | `0.25 ms +- 0.01` | `237.73 ms +- 3.04` | `0.03 ms +- 0.00` | `41.41 ms +- 0.11` | `4.75 ms +- 0.33` |
| `oltp_type1_point_lookup` | `1.11 ms +- 0.00` | `9.10 ms +- 0.02` | `1.38 ms +- 0.02` | `2.33 ms +- 0.03` | `1.34 ms +- 0.03` | `7.71 ms +- 0.87` | `0.29 ms +- 0.02` | `20.52 ms +- 1.02` | `0.02 ms +- 0.00` | `40.89 ms +- 0.07` | `3.31 ms +- 0.11` |
| `oltp_unwind_literal_top2` | `0.98 ms +- 0.00` | `0.98 ms +- 0.01` | `1.27 ms +- 0.04` | `1.24 ms +- 0.01` | `1.18 ms +- 0.04` | `1.16 ms +- 0.01` | `0.20 ms +- 0.01` | `0.20 ms +- 0.01` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.57 ms +- 0.02` |
| `oltp_update_cross_type_edge_rank` | `1.16 ms +- 0.01` | `58.25 ms +- 0.50` | `3.97 ms +- 0.48` | `3.20 ms +- 0.08` | `1.46 ms +- 0.06` | `103.79 ms +- 33.86` | `0.21 ms +- 0.01` | `21.58 ms +- 0.94` | `0.02 ms +- 0.00` | `45.32 ms +- 0.83` | `4.09 ms +- 0.38` |
| `oltp_update_type1_score` | `0.81 ms +- 0.01` | `8.62 ms +- 0.16` | `1.53 ms +- 0.06` | `2.18 ms +- 0.04` | `1.03 ms +- 0.03` | `8.61 ms +- 1.77` | `0.20 ms +- 0.01` | `22.18 ms +- 0.54` | `0.03 ms +- 0.00` | `41.97 ms +- 0.49` | `3.14 ms +- 0.13` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.96 ms +- 0.29` | `18.84 ms +- 1.02` | `3.36 ms +- 0.68` | `5.57 ms +- 0.24` | `2.22 ms +- 0.24` | `22.04 ms +- 1.22` | `0.39 ms +- 0.12` | `48.46 ms +- 2.56` | `0.15 ms +- 0.03` | `94.41 ms +- 0.76` | `8.52 ms +- 0.36` |
| `oltp_create_type1_node` | `0.91 ms +- 0.06` | `0.86 ms +- 0.03` | `1.84 ms +- 0.29` | `1.46 ms +- 0.10` | `1.28 ms +- 0.27` | `1.10 ms +- 0.06` | `0.32 ms +- 0.11` | `0.26 ms +- 0.02` | `0.04 ms +- 0.00` | `0.03 ms +- 0.00` | `0.50 ms +- 0.07` |
| `oltp_cross_type_lookup` | `1.60 ms +- 0.22` | `60.75 ms +- 2.23` | `4.86 ms +- 0.61` | `4.25 ms +- 0.28` | `2.13 ms +- 0.26` | `42.36 ms +- 2.85` | `0.33 ms +- 0.09` | `546.73 ms +- 60.06` | `0.04 ms +- 0.00` | `42.61 ms +- 0.40` | `6.62 ms +- 0.69` |
| `oltp_delete_type1_edge` | `0.95 ms +- 0.03` | `60.46 ms +- 1.36` | `4.75 ms +- 0.51` | `4.09 ms +- 0.13` | `1.56 ms +- 0.27` | `92.60 ms +- 2.88` | `0.31 ms +- 0.12` | `23.68 ms +- 0.81` | `0.07 ms +- 0.01` | `47.17 ms +- 0.91` | `5.98 ms +- 0.78` |
| `oltp_delete_type1_node` | `1.03 ms +- 0.06` | `551.71 ms +- 64.85` | `1.09 ms +- 0.10` | `2.61 ms +- 0.05` | `1.04 ms +- 0.13` | `284.64 ms +- 0.00` | `0.38 ms +- 0.18` | `25.15 ms +- 0.71` | `0.11 ms +- 0.01` | `47.46 ms +- 1.58` | `5.47 ms +- 0.34` |
| `oltp_merge_cross_type_edge` | `2.28 ms +- 0.39` | `71.79 ms +- 2.37` | `4.59 ms +- 1.00` | `6.38 ms +- 0.19` | `2.85 ms +- 0.50` | `81.67 ms +- 2.59` | `0.40 ms +- 0.12` | `47.69 ms +- 0.31` | `0.14 ms +- 0.02` | `95.33 ms +- 1.38` | `15.08 ms +- 1.68` |
| `oltp_program_create_and_link` | `2.30 ms +- 0.32` | `10.92 ms +- 0.63` | `4.47 ms +- 0.49` | `4.96 ms +- 0.12` | `3.06 ms +- 0.51` | `13.00 ms +- 0.64` | `0.31 ms +- 0.10` | `24.75 ms +- 1.33` | `0.23 ms +- 0.02` | `52.43 ms +- 5.37` | `5.22 ms +- 0.25` |
| `oltp_type1_neighbors` | `1.24 ms +- 0.03` | `60.16 ms +- 0.83` | `4.39 ms +- 0.41` | `3.87 ms +- 0.04` | `1.88 ms +- 0.28` | `42.98 ms +- 2.32` | `0.38 ms +- 0.09` | `251.09 ms +- 3.10` | `0.05 ms +- 0.01` | `42.75 ms +- 0.47` | `6.36 ms +- 0.48` |
| `oltp_type1_point_lookup` | `1.23 ms +- 0.06` | `9.90 ms +- 0.10` | `1.78 ms +- 0.37` | `2.73 ms +- 0.06` | `1.66 ms +- 0.26` | `10.82 ms +- 0.45` | `0.47 ms +- 0.13` | `22.83 ms +- 0.98` | `0.06 ms +- 0.00` | `42.44 ms +- 0.49` | `5.15 ms +- 0.15` |
| `oltp_unwind_literal_top2` | `1.12 ms +- 0.10` | `1.04 ms +- 0.03` | `1.61 ms +- 0.25` | `1.38 ms +- 0.04` | `1.45 ms +- 0.23` | `1.29 ms +- 0.10` | `0.30 ms +- 0.09` | `0.26 ms +- 0.02` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.86 ms +- 0.18` |
| `oltp_update_cross_type_edge_rank` | `1.30 ms +- 0.07` | `62.26 ms +- 1.88` | `5.21 ms +- 0.89` | `3.96 ms +- 0.14` | `1.91 ms +- 0.36` | `109.65 ms +- 35.48` | `0.31 ms +- 0.10` | `23.98 ms +- 1.27` | `0.03 ms +- 0.00` | `49.87 ms +- 1.64` | `5.62 ms +- 0.47` |
| `oltp_update_type1_score` | `0.90 ms +- 0.05` | `9.31 ms +- 0.32` | `2.06 ms +- 0.32` | `2.91 ms +- 0.09` | `1.30 ms +- 0.19` | `10.65 ms +- 1.03` | `0.33 ms +- 0.10` | `25.07 ms +- 3.22` | `0.04 ms +- 0.00` | `43.59 ms +- 0.84` | `4.97 ms +- 0.26` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.53 ms +- 0.69` | `20.45 ms +- 2.17` | `4.40 ms +- 1.22` | `6.29 ms +- 0.19` | `2.70 ms +- 0.32` | `25.19 ms +- 1.53` | `0.59 ms +- 0.26` | `53.68 ms +- 5.44` | `0.21 ms +- 0.04` | `97.82 ms +- 0.81` | `9.63 ms +- 0.77` |
| `oltp_create_type1_node` | `1.17 ms +- 0.14` | `1.09 ms +- 0.06` | `2.66 ms +- 0.78` | `1.76 ms +- 0.22` | `1.81 ms +- 0.57` | `1.43 ms +- 0.09` | `0.48 ms +- 0.20` | `0.35 ms +- 0.07` | `0.06 ms +- 0.00` | `0.05 ms +- 0.01` | `0.75 ms +- 0.34` |
| `oltp_cross_type_lookup` | `2.16 ms +- 0.76` | `65.17 ms +- 3.91` | `6.06 ms +- 1.00` | `4.79 ms +- 0.30` | `2.54 ms +- 0.39` | `46.21 ms +- 3.22` | `0.49 ms +- 0.19` | `588.60 ms +- 98.22` | `0.04 ms +- 0.01` | `44.35 ms +- 0.68` | `8.09 ms +- 1.19` |
| `oltp_delete_type1_edge` | `1.20 ms +- 0.11` | `63.48 ms +- 3.11` | `5.91 ms +- 1.12` | `4.67 ms +- 0.13` | `2.03 ms +- 0.31` | `97.28 ms +- 3.85` | `0.47 ms +- 0.27` | `27.56 ms +- 1.27` | `0.09 ms +- 0.02` | `50.16 ms +- 1.72` | `6.86 ms +- 1.03` |
| `oltp_delete_type1_node` | `1.71 ms +- 0.71` | `599.58 ms +- 96.00` | `1.54 ms +- 0.29` | `2.83 ms +- 0.08` | `1.37 ms +- 0.13` | `289.48 ms +- 0.00` | `0.55 ms +- 0.35` | `28.78 ms +- 1.14` | `0.14 ms +- 0.01` | `50.30 ms +- 2.49` | `6.12 ms +- 0.48` |
| `oltp_merge_cross_type_edge` | `2.95 ms +- 0.72` | `76.27 ms +- 4.08` | `5.94 ms +- 1.36` | `7.44 ms +- 0.06` | `3.60 ms +- 0.74` | `87.45 ms +- 3.22` | `0.60 ms +- 0.28` | `52.12 ms +- 0.71` | `0.21 ms +- 0.05` | `98.99 ms +- 1.49` | `16.96 ms +- 1.98` |
| `oltp_program_create_and_link` | `2.89 ms +- 0.56` | `12.50 ms +- 1.08` | `5.81 ms +- 1.05` | `6.22 ms +- 0.30` | `3.82 ms +- 0.75` | `15.84 ms +- 1.39` | `0.43 ms +- 0.24` | `28.64 ms +- 1.53` | `0.30 ms +- 0.02` | `56.28 ms +- 5.83` | `5.67 ms +- 0.23` |
| `oltp_type1_neighbors` | `1.49 ms +- 0.12` | `64.04 ms +- 2.30` | `5.54 ms +- 1.02` | `4.38 ms +- 0.10` | `2.37 ms +- 0.43` | `47.82 ms +- 1.79` | `0.62 ms +- 0.25` | `262.16 ms +- 2.51` | `0.07 ms +- 0.01` | `44.39 ms +- 0.89` | `7.61 ms +- 0.90` |
| `oltp_type1_point_lookup` | `1.73 ms +- 0.54` | `11.03 ms +- 0.40` | `2.58 ms +- 1.02` | `3.24 ms +- 0.09` | `1.99 ms +- 0.37` | `12.85 ms +- 1.01` | `0.76 ms +- 0.35` | `26.30 ms +- 0.17` | `0.08 ms +- 0.01` | `44.49 ms +- 0.93` | `5.47 ms +- 0.18` |
| `oltp_unwind_literal_top2` | `1.55 ms +- 0.42` | `1.27 ms +- 0.12` | `2.38 ms +- 0.82` | `1.77 ms +- 0.07` | `1.84 ms +- 0.32` | `1.67 ms +- 0.16` | `0.43 ms +- 0.20` | `0.36 ms +- 0.06` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `1.18 ms +- 0.45` |
| `oltp_update_cross_type_edge_rank` | `1.76 ms +- 0.18` | `67.98 ms +- 1.95` | `6.27 ms +- 1.47` | `4.50 ms +- 0.29` | `2.40 ms +- 0.39` | `113.77 ms +- 35.06` | `0.43 ms +- 0.23` | `28.13 ms +- 1.57` | `0.04 ms +- 0.00` | `53.67 ms +- 2.66` | `6.42 ms +- 0.50` |
| `oltp_update_type1_score` | `1.19 ms +- 0.19` | `10.44 ms +- 0.91` | `3.02 ms +- 1.16` | `3.16 ms +- 0.18` | `1.69 ms +- 0.20` | `12.72 ms +- 2.07` | `0.51 ms +- 0.18` | `28.70 ms +- 5.21` | `0.05 ms +- 0.01` | `45.84 ms +- 1.09` | `5.37 ms +- 0.21` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1107.95 ms +- 60.53` | `193.10 ms +- 2.72` | `9.20 ms +- 0.31` | `8.62 ms +- 0.12` | `104.09 ms +- 3.78` | `102.46 ms +- 1.69` | `604.30 ms +- 22.88` | `598.14 ms +- 5.83` | `994.33 ms +- 55.78` | `918.95 ms +- 18.48` | `43.89 ms +- 2.58` |
| `olap_fixed_length_path_projection` | `1.84 ms +- 0.05` | `1317.13 ms +- 7.22` | `178.87 ms +- 3.84` | `175.13 ms +- 2.52` | `20.90 ms +- 0.72` | `898.88 ms +- 50.90` | `1428.78 ms +- 13.31` | `1437.25 ms +- 16.18` | `6906.12 ms +- 669.22` | `6709.21 ms +- 319.46` | `2167.66 ms +- 16.72` |
| `olap_fixed_length_path_with_rebinding` | `1737.34 ms +- 53.83` | `1242.00 ms +- 12.83` | `105.87 ms +- 3.24` | `103.27 ms +- 2.22` | `440.02 ms +- 14.07` | `416.63 ms +- 6.13` | `3714.81 ms +- 57.69` | `3819.39 ms +- 77.19` | `6658.23 ms +- 392.54` | `6675.21 ms +- 224.04` | `1784.98 ms +- 5.15` |
| `olap_graph_introspection_rollup` | `1.81 ms +- 0.00` | `105.12 ms +- 2.42` | `5.74 ms +- 0.29` | `5.01 ms +- 0.22` | `7.15 ms +- 0.84` | `92.47 ms +- 3.48` | `527.83 ms +- 4.21` | `519.78 ms +- 12.08` | `1591.61 ms +- 70.71` | `1510.81 ms +- 12.15` | `158.66 ms +- 2.30` |
| `olap_optional_type1_aggregate` | `16.93 ms +- 0.38` | `22.80 ms +- 0.23` | `4.16 ms +- 0.16` | `3.82 ms +- 0.11` | `17.07 ms +- 1.28` | `17.12 ms +- 1.48` | `66.30 ms +- 7.95` | `59.19 ms +- 0.31` | - | - | `4.14 ms +- 0.08` |
| `olap_relationship_function_projection` | `512.35 ms +- 18.94` | `161.20 ms +- 1.88` | `15.08 ms +- 0.40` | `14.39 ms +- 0.05` | `142.09 ms +- 4.86` | `145.18 ms +- 2.90` | `579.58 ms +- 18.70` | `583.52 ms +- 23.38` | `1089.37 ms +- 60.20` | `1044.70 ms +- 10.66` | `240.39 ms +- 1.30` |
| `olap_three_type_path_count` | `1433.01 ms +- 33.58` | `872.92 ms +- 5.14` | `38.12 ms +- 1.34` | `37.39 ms +- 2.38` | `333.80 ms +- 25.54` | `334.38 ms +- 0.91` | `558.48 ms +- 5.09` | `564.77 ms +- 14.64` | `6.22 ms +- 0.06` | `6.16 ms +- 0.17` | `12.65 ms +- 0.58` |
| `olap_type1_active_leaderboard` | `1.35 ms +- 0.03` | `11.32 ms +- 0.20` | `5.76 ms +- 0.36` | `5.18 ms +- 0.09` | `10.19 ms +- 0.63` | `9.97 ms +- 0.58` | `71.85 ms +- 9.50` | `65.49 ms +- 0.54` | `88.40 ms +- 4.13` | `76.61 ms +- 3.09` | `4.94 ms +- 0.02` |
| `olap_type1_age_rollup` | `101.16 ms +- 5.91` | `24.24 ms +- 0.53` | `3.76 ms +- 0.14` | `3.78 ms +- 0.37` | `11.70 ms +- 0.85` | `10.97 ms +- 0.37` | `77.82 ms +- 9.28` | `70.63 ms +- 0.33` | `58.71 ms +- 1.04` | `58.70 ms +- 3.27` | `3.49 ms +- 0.32` |
| `olap_type2_score_distribution` | `9.01 ms +- 0.32` | `23.85 ms +- 0.43` | `4.59 ms +- 0.28` | `4.19 ms +- 0.14` | `8.32 ms +- 0.81` | `17.75 ms +- 0.47` | `64.87 ms +- 0.54` | `61.82 ms +- 0.47` | `62.30 ms +- 1.52` | `60.90 ms +- 1.44` | `3.43 ms +- 0.28` |
| `olap_variable_length_grouped_max_rollup` | `2.45 ms +- 0.01` | `698.44 ms +- 9.56` | `8.92 ms +- 1.49` | `8.25 ms +- 0.82` | `5.12 ms +- 0.18` | `184.36 ms +- 11.88` | `0.31 ms +- 0.06` | `21.80 ms +- 0.68` | `0.10 ms +- 0.02` | `47.70 ms +- 1.13` | `6.88 ms +- 0.47` |
| `olap_variable_length_grouped_rollup` | `68231.69 ms +- 705.23` | `64346.26 ms +- 440.01` | `721.30 ms +- 13.55` | `715.38 ms +- 10.13` | `7144.40 ms +- 173.91` | `7460.21 ms +- 423.65` | - | - | - | - | `26184.72 ms +- 532.16` |
| `olap_variable_length_reachability` | `2.98 ms +- 0.20` | `2355.31 ms +- 13.81` | `15.00 ms +- 0.47` | `14.26 ms +- 0.54` | `26.28 ms +- 2.85` | `414.83 ms +- 9.83` | `1.00 ms +- 0.01` | - | `0.44 ms +- 0.06` | `42.45 ms +- 0.82` | `4.88 ms +- 0.28` |
| `olap_with_scalar_rebinding` | `102.77 ms +- 6.08` | `27.27 ms +- 0.73` | `5.81 ms +- 0.41` | `5.14 ms +- 0.11` | `15.59 ms +- 0.95` | `22.49 ms +- 0.83` | `78.51 ms +- 0.09` | `76.35 ms +- 0.79` | `71.30 ms +- 1.09` | `69.98 ms +- 1.31` | `6.84 ms +- 0.02` |
| `olap_with_size_predicate_projection` | `15.87 ms +- 0.52` | `15.92 ms +- 0.58` | `5.91 ms +- 0.45` | `5.61 ms +- 0.28` | `15.17 ms +- 0.94` | `22.29 ms +- 0.67` | `40.08 ms +- 0.37` | `39.94 ms +- 0.98` | `107.02 ms +- 7.06` | `102.23 ms +- 2.06` | `34.85 ms +- 1.16` |
| `olap_with_where_lower_projection` | `90.55 ms +- 5.09` | `16.68 ms +- 0.97` | `6.05 ms +- 0.43` | `5.46 ms +- 0.18` | `14.72 ms +- 1.00` | `21.28 ms +- 1.00` | `38.63 ms +- 0.22` | `38.31 ms +- 0.39` | `90.03 ms +- 3.44` | `86.88 ms +- 1.38` | `8.16 ms +- 0.06` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1137.25 ms +- 60.86` | `199.62 ms +- 3.02` | `9.88 ms +- 0.18` | `9.51 ms +- 0.26` | `110.78 ms +- 5.33` | `110.11 ms +- 3.04` | `645.60 ms +- 67.09` | `616.44 ms +- 11.76` | `1079.69 ms +- 118.14` | `958.50 ms +- 30.21` | `48.32 ms +- 4.34` |
| `olap_fixed_length_path_projection` | `1.98 ms +- 0.13` | `1338.62 ms +- 10.32` | `188.65 ms +- 7.33` | `184.90 ms +- 3.36` | `23.52 ms +- 0.65` | `973.30 ms +- 96.55` | `1452.05 ms +- 14.53` | `1472.99 ms +- 19.14` | `7458.68 ms +- 711.46` | `7240.28 ms +- 352.00` | `2255.14 ms +- 28.85` |
| `olap_fixed_length_path_with_rebinding` | `1778.37 ms +- 58.50` | `1264.34 ms +- 24.44` | `113.92 ms +- 4.32` | `108.05 ms +- 2.94` | `457.91 ms +- 30.15` | `577.73 ms +- 148.74` | `3767.25 ms +- 57.10` | `3903.89 ms +- 102.52` | `7247.86 ms +- 471.83` | `7183.14 ms +- 263.22` | `1837.93 ms +- 13.03` |
| `olap_graph_introspection_rollup` | `1.91 ms +- 0.06` | `110.62 ms +- 2.13` | `7.01 ms +- 0.75` | `5.94 ms +- 0.56` | `8.57 ms +- 1.45` | `98.85 ms +- 4.46` | `540.20 ms +- 3.19` | `533.50 ms +- 14.95` | `1666.10 ms +- 112.36` | `1557.74 ms +- 21.21` | `169.04 ms +- 1.76` |
| `olap_optional_type1_aggregate` | `17.69 ms +- 0.56` | `24.56 ms +- 0.54` | `4.98 ms +- 0.19` | `4.58 ms +- 0.53` | `20.81 ms +- 3.83` | `18.53 ms +- 0.75` | `72.62 ms +- 7.72` | `63.99 ms +- 0.61` | - | - | `5.65 ms +- 0.20` |
| `olap_relationship_function_projection` | `529.44 ms +- 15.29` | `169.86 ms +- 12.22` | `16.55 ms +- 0.34` | `15.60 ms +- 0.44` | `150.68 ms +- 5.13` | `154.82 ms +- 3.87` | `599.10 ms +- 23.81` | `601.30 ms +- 30.68` | `1142.69 ms +- 100.54` | `1081.62 ms +- 18.09` | `250.41 ms +- 2.70` |
| `olap_three_type_path_count` | `1451.37 ms +- 33.03` | `898.58 ms +- 19.78` | `42.81 ms +- 1.58` | `43.06 ms +- 1.23` | `444.76 ms +- 86.36` | `490.68 ms +- 130.51` | `591.11 ms +- 39.58` | `589.26 ms +- 15.96` | `6.74 ms +- 0.37` | `6.55 ms +- 0.46` | `52.76 ms +- 8.13` |
| `olap_type1_active_leaderboard` | `1.57 ms +- 0.22` | `11.98 ms +- 0.44` | `6.95 ms +- 0.39` | `6.48 ms +- 0.22` | `11.66 ms +- 0.40` | `11.60 ms +- 0.94` | `77.34 ms +- 11.69` | `71.03 ms +- 0.52` | `125.77 ms +- 26.75` | `111.31 ms +- 14.86` | `6.71 ms +- 0.78` |
| `olap_type1_age_rollup` | `109.28 ms +- 5.45` | `25.20 ms +- 0.58` | `4.38 ms +- 0.10` | `4.29 ms +- 0.23` | `13.55 ms +- 1.35` | `12.89 ms +- 0.62` | `83.71 ms +- 11.67` | `76.98 ms +- 1.69` | `68.61 ms +- 3.18` | `67.97 ms +- 2.72` | `4.44 ms +- 0.63` |
| `olap_type2_score_distribution` | `9.68 ms +- 0.25` | `24.68 ms +- 0.56` | `5.42 ms +- 0.31` | `4.85 ms +- 0.32` | `9.73 ms +- 1.22` | `19.23 ms +- 0.63` | `70.94 ms +- 2.85` | `67.53 ms +- 2.32` | `67.01 ms +- 3.69` | `64.30 ms +- 2.57` | `4.36 ms +- 0.51` |
| `olap_variable_length_grouped_max_rollup` | `2.55 ms +- 0.05` | `724.90 ms +- 26.85` | `11.08 ms +- 2.38` | `12.13 ms +- 3.91` | `6.04 ms +- 0.80` | `207.58 ms +- 14.97` | `0.43 ms +- 0.16` | `23.50 ms +- 1.22` | `0.16 ms +- 0.01` | `88.42 ms +- 8.58` | `9.03 ms +- 0.88` |
| `olap_variable_length_grouped_rollup` | `71327.66 ms +- 2049.70` | `67324.23 ms +- 1540.00` | `742.80 ms +- 16.69` | `734.04 ms +- 14.96` | `8207.04 ms +- 473.24` | `8797.64 ms +- 1406.68` | - | - | - | - | `27108.16 ms +- 727.94` |
| `olap_variable_length_reachability` | `3.22 ms +- 0.29` | `2461.84 ms +- 55.36` | `17.32 ms +- 2.14` | `15.71 ms +- 0.81` | `29.37 ms +- 1.89` | `447.97 ms +- 18.14` | `1.28 ms +- 0.11` | - | `0.50 ms +- 0.07` | `44.86 ms +- 1.20` | `6.43 ms +- 0.34` |
| `olap_with_scalar_rebinding` | `109.64 ms +- 4.55` | `29.58 ms +- 1.70` | `7.62 ms +- 1.12` | `6.20 ms +- 0.67` | `17.20 ms +- 0.61` | `24.44 ms +- 0.79` | `84.68 ms +- 2.95` | `81.59 ms +- 4.39` | `78.81 ms +- 2.75` | `76.87 ms +- 6.67` | `8.26 ms +- 0.67` |
| `olap_with_size_predicate_projection` | `16.51 ms +- 0.60` | `17.02 ms +- 1.15` | `7.02 ms +- 0.54` | `6.70 ms +- 0.88` | `16.76 ms +- 0.92` | `23.88 ms +- 0.55` | `42.73 ms +- 0.88` | `41.96 ms +- 1.98` | `118.13 ms +- 12.82` | `109.18 ms +- 3.41` | `40.69 ms +- 3.35` |
| `olap_with_where_lower_projection` | `95.93 ms +- 5.19` | `18.19 ms +- 1.74` | `7.04 ms +- 0.18` | `6.42 ms +- 0.63` | `17.18 ms +- 0.24` | `22.92 ms +- 0.71` | `41.44 ms +- 1.91` | `41.51 ms +- 0.82` | `97.99 ms +- 7.01` | `92.40 ms +- 1.27` | `9.40 ms +- 0.65` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1148.40 ms +- 56.12` | `204.41 ms +- 0.62` | `10.44 ms +- 0.13` | `10.51 ms +- 0.24` | `113.84 ms +- 6.32` | `111.55 ms +- 3.92` | `662.09 ms +- 79.69` | `630.50 ms +- 14.70` | `1111.54 ms +- 128.00` | `972.84 ms +- 32.12` | `53.27 ms +- 2.15` |
| `olap_fixed_length_path_projection` | `2.23 ms +- 0.26` | `1347.81 ms +- 17.72` | `192.65 ms +- 7.54` | `188.16 ms +- 4.37` | `25.20 ms +- 0.79` | `989.96 ms +- 84.86` | `1482.31 ms +- 36.33` | `1501.39 ms +- 32.29` | `7643.46 ms +- 618.84` | `7466.81 ms +- 476.97` | `2389.05 ms +- 117.32` |
| `olap_fixed_length_path_with_rebinding` | `1797.87 ms +- 64.84` | `1280.44 ms +- 28.21` | `118.96 ms +- 8.86` | `111.36 ms +- 5.55` | `464.45 ms +- 34.45` | `635.06 ms +- 186.68` | `3876.81 ms +- 42.36` | `4167.29 ms +- 367.40` | `7479.95 ms +- 384.31` | `7401.63 ms +- 313.50` | `1873.82 ms +- 34.46` |
| `olap_graph_introspection_rollup` | `1.99 ms +- 0.11` | `116.42 ms +- 9.46` | `7.63 ms +- 1.20` | `7.36 ms +- 2.13` | `9.18 ms +- 1.83` | `103.38 ms +- 3.12` | `546.28 ms +- 4.36` | `556.21 ms +- 43.09` | `1691.57 ms +- 132.72` | `1577.47 ms +- 17.16` | `171.38 ms +- 2.99` |
| `olap_optional_type1_aggregate` | `18.49 ms +- 0.37` | `25.22 ms +- 0.88` | `5.23 ms +- 0.14` | `4.81 ms +- 0.65` | `24.28 ms +- 5.30` | `19.93 ms +- 1.21` | `77.62 ms +- 7.71` | `68.23 ms +- 3.53` | - | - | `9.61 ms +- 1.81` |
| `olap_relationship_function_projection` | `538.36 ms +- 17.94` | `176.67 ms +- 19.18` | `18.60 ms +- 1.02` | `16.35 ms +- 0.52` | `154.86 ms +- 4.91` | `159.98 ms +- 4.60` | `612.89 ms +- 35.75` | `613.78 ms +- 36.52` | `1164.32 ms +- 118.78` | `1094.59 ms +- 17.32` | `257.67 ms +- 4.68` |
| `olap_three_type_path_count` | `1455.67 ms +- 32.56` | `930.17 ms +- 68.56` | `44.47 ms +- 2.48` | `46.46 ms +- 0.16` | `532.04 ms +- 135.17` | `559.01 ms +- 183.92` | `675.32 ms +- 164.77` | `602.86 ms +- 24.54` | `8.39 ms +- 1.55` | `8.72 ms +- 2.08` | `58.00 ms +- 7.78` |
| `olap_type1_active_leaderboard` | `1.66 ms +- 0.19` | `12.67 ms +- 1.40` | `7.41 ms +- 0.08` | `7.91 ms +- 2.06` | `12.15 ms +- 0.17` | `13.49 ms +- 0.79` | `82.78 ms +- 14.46` | `77.03 ms +- 3.34` | `150.69 ms +- 33.79` | `125.21 ms +- 12.94` | `7.37 ms +- 0.61` |
| `olap_type1_age_rollup` | `112.94 ms +- 3.49` | `26.59 ms +- 0.41` | `4.83 ms +- 0.14` | `4.67 ms +- 0.40` | `15.94 ms +- 2.68` | `15.51 ms +- 2.04` | `88.89 ms +- 16.21` | `81.02 ms +- 2.93` | `75.07 ms +- 8.41` | `72.15 ms +- 6.15` | `4.88 ms +- 0.69` |
| `olap_type2_score_distribution` | `10.57 ms +- 0.58` | `26.76 ms +- 0.42` | `5.85 ms +- 0.31` | `5.72 ms +- 0.60` | `11.54 ms +- 1.88` | `21.67 ms +- 1.71` | `76.79 ms +- 2.79` | `72.00 ms +- 3.79` | `70.72 ms +- 7.16` | `65.52 ms +- 2.27` | `4.87 ms +- 0.41` |
| `olap_variable_length_grouped_max_rollup` | `3.04 ms +- 0.75` | `746.42 ms +- 47.91` | `12.98 ms +- 4.46` | `13.76 ms +- 5.63` | `6.75 ms +- 0.75` | `222.86 ms +- 15.81` | `0.59 ms +- 0.23` | `25.76 ms +- 1.04` | `0.21 ms +- 0.03` | `109.69 ms +- 10.74` | `9.87 ms +- 0.53` |
| `olap_variable_length_grouped_rollup` | `74575.26 ms +- 4023.76` | `68547.89 ms +- 2559.38` | `753.93 ms +- 16.51` | `747.45 ms +- 26.43` | `8463.57 ms +- 525.30` | `9419.98 ms +- 1431.05` | - | - | - | - | `27303.97 ms +- 916.95` |
| `olap_variable_length_reachability` | `3.45 ms +- 0.23` | `2607.56 ms +- 159.41` | `18.49 ms +- 2.92` | `18.50 ms +- 0.82` | `31.54 ms +- 0.91` | `466.67 ms +- 23.20` | `1.99 ms +- 0.92` | - | `0.54 ms +- 0.07` | `47.52 ms +- 0.53` | `7.00 ms +- 0.50` |
| `olap_with_scalar_rebinding` | `114.73 ms +- 4.07` | `38.53 ms +- 7.60` | `8.31 ms +- 1.42` | `6.62 ms +- 0.21` | `18.36 ms +- 0.97` | `25.80 ms +- 0.98` | `90.19 ms +- 1.79` | `92.84 ms +- 14.29` | `83.63 ms +- 3.05` | `83.43 ms +- 11.38` | `9.55 ms +- 1.02` |
| `olap_with_size_predicate_projection` | `17.35 ms +- 0.30` | `17.86 ms +- 1.66` | `8.63 ms +- 1.22` | `7.46 ms +- 0.82` | `18.36 ms +- 0.66` | `26.45 ms +- 1.49` | `44.60 ms +- 0.97` | `43.94 ms +- 1.93` | `122.66 ms +- 13.76` | `115.36 ms +- 3.05` | `42.47 ms +- 3.92` |
| `olap_with_where_lower_projection` | `98.13 ms +- 4.82` | `19.09 ms +- 1.68` | `8.04 ms +- 0.66` | `7.31 ms +- 1.43` | `19.39 ms +- 0.97` | `24.33 ms +- 0.87` | `44.33 ms +- 2.18` | `45.48 ms +- 0.87` | `103.63 ms +- 7.97` | `96.52 ms +- 2.14` | `11.48 ms +- 0.54` |

### Small runtime dataset

The current small runtime matrix used the `small` preset with `10000` OLTP iterations / `200` OLTP warmup and `500` OLAP iterations / `20` OLAP warmup.

That corresponds to roughly:

- `4,000` total nodes
- `12,000` total edges
- `4` node types
- `4` edge types
- `24` property fields across the schema (`14` per node, `10` per edge)
- `11` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.40.1`
- `DuckDB`: `1.5.2`
- `PostgreSQL`: `16.13 (Debian 16.13-1.pgdg13+1)`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.2.post1`
- `LadybugDB`: `0.16.1`

Runtime benchmark artifacts also record these engine versions in a top-level
`database_versions` object inside each JSON payload.

For the SQL backends in this refreshed run, setup follows the more standard
bulk-load sequence: `schema -> ingest -> index -> analyze`. That means the
reported `ingest` step does not include index-maintenance cost during row
insertion, and the `index` step captures post-load index construction.

Neo4j is a direct-Cypher runner rather than a compile-plus-execute SQL
path.

LadybugDB is also a direct-Cypher runner, and it currently uses a
post-load `CHECKPOINT` instead of an `ANALYZE` step. In the summary
tables below, that checkpoint time is shown in the `Analyze` column so
the setup layout stays consistent across engines.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
summary tables below, that engine-specific post-load work is folded into the
`Analyze` column, along with the checkpoint step, so the setup layout stays
consistent across engines.


OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `10.94 ms +- 7.81` | `6.75 ms +- 4.40` | `87.79 ms +- 0.96` | `7.45 ms +- 0.47` | `3.37 ms +- 0.04` | `1.27 ms +- 0.07` | `1.75 ms +- 0.37` | `2.31 ms +- 0.55` |
| SQLite Unindexed (3) | `8.94 ms +- 4.54` | `5.92 ms +- 3.38` | `91.47 ms +- 2.18` | `1.67 ms +- 0.19` | `0.47 ms +- 0.15` | `1.28 ms +- 0.03` | `1.44 ms +- 0.20` | `1.74 ms +- 0.38` |
| DuckDB Indexed (3) | `11.76 ms +- 2.25` | `159.48 ms +- 61.14` | `204.74 ms +- 36.44` | `120.73 ms +- 40.58` | `0.21 ms +- 0.17` | `2.19 ms +- 0.18` | `3.49 ms +- 1.14` | `4.42 ms +- 1.60` |
| DuckDB Unindexed (3) | `11.90 ms +- 1.15` | `94.63 ms +- 6.92` | `169.84 ms +- 4.94` | `44.22 ms +- 1.54` | `0.11 ms +- 0.01` | `2.08 ms +- 0.11` | `2.92 ms +- 0.64` | `3.58 ms +- 0.96` |
| PostgreSQL Indexed (3) | `3.83 ms +- 0.27` | `333.41 ms +- 33.95` | `277.71 ms +- 36.36` | `320.27 ms +- 184.10` | `80.79 ms +- 4.12` | `1.50 ms +- 0.04` | `1.97 ms +- 0.09` | `2.34 ms +- 0.09` |
| PostgreSQL Unindexed (3) | `3.79 ms +- 0.01` | `403.22 ms +- 139.87` | `277.38 ms +- 50.98` | `200.16 ms +- 330.34` | `118.04 ms +- 72.62` | `1.60 ms +- 0.03` | `1.97 ms +- 0.09` | `2.39 ms +- 0.08` |
| Neo4j Indexed (3) | `78.97 ms +- 25.22` | `372.78 ms +- 25.99` | `2323.73 ms +- 261.22` | `1008.67 ms +- 355.26` | `0.00 ms +- 0.00` | `0.24 ms +- 0.01` | `0.34 ms +- 0.02` | `0.47 ms +- 0.04` |
| Neo4j Unindexed (3) | `76.16 ms +- 13.63` | `436.81 ms +- 52.83` | `2240.38 ms +- 310.34` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.35 ms +- 0.01` | `0.54 ms +- 0.09` | `0.83 ms +- 0.29` |
| ArcadeDB Indexed (3) | `337.09 ms +- 55.15` | `313.87 ms +- 51.14` | `609.29 ms +- 108.02` | `345.57 ms +- 39.89` | `0.00 ms +- 0.00` | `0.04 ms +- 0.00` | `0.08 ms +- 0.02` | `0.12 ms +- 0.05` |
| ArcadeDB Unindexed (3) | `294.83 ms +- 5.85` | `248.65 ms +- 12.27` | `570.99 ms +- 16.32` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.43 ms +- 0.01` | `0.49 ms +- 0.01` | `0.63 ms +- 0.00` |
| LadybugDB Unindexed (3) | `97.23 ms +- 26.34` | `68.82 ms +- 25.50` | `977.43 ms +- 104.23` | `0.00 ms +- 0.00` | `28.84 ms +- 2.19` | `1.42 ms +- 0.18` | `5.38 ms +- 1.91` | `14.79 ms +- 0.87` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `9.59 ms +- 5.78` | `7.73 ms +- 6.16` | `99.19 ms +- 15.34` | `8.31 ms +- 0.35` | `4.10 ms +- 0.89` | `2.60 ms +- 0.45` | `4.20 ms +- 1.55` | `5.27 ms +- 2.06` |
| SQLite Unindexed (3) | `9.66 ms +- 4.08` | `8.93 ms +- 6.27` | `80.77 ms +- 3.00` | `1.65 ms +- 0.18` | `0.37 ms +- 0.03` | `2.53 ms +- 0.24` | `2.89 ms +- 0.77` | `3.13 ms +- 1.06` |
| DuckDB Indexed (3) | `8.30 ms +- 0.41` | `89.44 ms +- 1.47` | `162.00 ms +- 1.13` | `74.81 ms +- 1.79` | `0.13 ms +- 0.02` | `2.92 ms +- 0.11` | `3.51 ms +- 0.22` | `3.98 ms +- 0.19` |
| DuckDB Unindexed (3) | `10.70 ms +- 3.29` | `129.15 ms +- 67.61` | `171.17 ms +- 20.76` | `63.43 ms +- 35.65` | `0.21 ms +- 0.09` | `3.26 ms +- 0.65` | `4.29 ms +- 1.55` | `4.88 ms +- 1.83` |
| PostgreSQL Indexed (3) | `4.06 ms +- 0.11` | `321.25 ms +- 10.68` | `237.11 ms +- 80.23` | `220.09 ms +- 18.24` | `76.62 ms +- 0.63` | `3.07 ms +- 0.10` | `3.74 ms +- 0.17` | `4.31 ms +- 0.17` |
| PostgreSQL Unindexed (3) | `3.94 ms +- 0.17` | `323.35 ms +- 8.47` | `187.15 ms +- 12.16` | `12.11 ms +- 4.86` | `121.22 ms +- 4.50` | `2.89 ms +- 0.13` | `3.41 ms +- 0.28` | `3.87 ms +- 0.30` |
| Neo4j Indexed (3) | `78.97 ms +- 25.22` | `372.78 ms +- 25.99` | `2323.73 ms +- 261.22` | `1008.67 ms +- 355.26` | `0.00 ms +- 0.00` | `2.50 ms +- 0.30` | `3.22 ms +- 0.61` | `3.86 ms +- 0.77` |
| Neo4j Unindexed (3) | `76.16 ms +- 13.63` | `436.81 ms +- 52.83` | `2240.38 ms +- 310.34` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.29 ms +- 0.07` | `2.73 ms +- 0.12` | `3.32 ms +- 0.17` |
| ArcadeDB Indexed (3) | `1.73 ms +- 0.31` | `26.67 ms +- 2.68` | `430.52 ms +- 84.77` | `163.96 ms +- 90.56` | `296.59 ms +- 22.71` | `3.22 ms +- 0.11` | `4.32 ms +- 0.40` | `5.09 ms +- 0.43` |
| ArcadeDB Unindexed (3) | `2.09 ms +- 0.74` | `31.31 ms +- 3.01` | `355.65 ms +- 3.83` | `0.00 ms +- 0.00` | `289.74 ms +- 3.78` | `2.96 ms +- 0.07` | `3.68 ms +- 0.14` | `4.20 ms +- 0.13` |
| LadybugDB Unindexed (3) | `72.72 ms +- 5.34` | `34.43 ms +- 16.14` | `954.49 ms +- 75.17` | `0.00 ms +- 0.00` | `27.19 ms +- 9.07` | `5.07 ms +- 0.15` | `22.15 ms +- 0.98` | `26.50 ms +- 0.20` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `96.39 MiB +- 0.07` | `96.43 MiB +- 0.06` | `98.46 MiB +- 0.04` | `98.51 MiB +- 0.04` | `98.51 MiB +- 0.04` | `590.17 MiB +- 54.29` |
| SQLite Unindexed (3) | `96.30 MiB +- 0.14` | `96.33 MiB +- 0.15` | `98.36 MiB +- 0.14` | `98.40 MiB +- 0.15` | `98.40 MiB +- 0.15` | `601.73 MiB +- 7.24` |
| DuckDB Indexed (3) | `98.31 MiB +- 0.20` | `101.50 MiB +- 0.20` | `151.39 MiB +- 1.48` | `169.98 MiB +- 2.59` | `169.98 MiB +- 2.59` | `825.57 MiB +- 57.00` |
| DuckDB Unindexed (3) | `98.38 MiB +- 0.09` | `101.56 MiB +- 0.04` | `152.64 MiB +- 1.55` | `152.71 MiB +- 1.55` | `152.71 MiB +- 1.55` | `820.62 MiB +- 36.57` |
| PostgreSQL Indexed (3) | `123.29 MiB +- 0.30` | `125.21 MiB +- 0.26` | `133.18 MiB +- 0.11` | `133.62 MiB +- 0.46` | `136.06 MiB +- 0.18` | `775.53 MiB +- 10.04` |
| PostgreSQL Unindexed (3) | `123.17 MiB +- 0.48` | `125.00 MiB +- 0.22` | `133.51 MiB +- 0.52` | `133.10 MiB +- 0.96` | `135.00 MiB +- 0.23` | `789.87 MiB +- 34.86` |
| Neo4j Indexed (3) | `679.56 MiB +- 8.13` | `732.38 MiB +- 10.23` | `1782.05 MiB +- 22.57` | `912.43 MiB +- 11.82` | `0.00 MiB +- 0.00` | `1231.96 MiB +- 16.10` |
| Neo4j Unindexed (3) | `657.49 MiB +- 6.79` | `702.53 MiB +- 2.96` | `1456.01 MiB +- 536.79` | `1448.51 MiB +- 535.44` | `0.00 MiB +- 0.00` | `1204.24 MiB +- 51.22` |
| ArcadeDB Indexed (3) | `141.82 MiB +- 9.81` | `204.79 MiB +- 5.84` | `270.29 MiB +- 3.18` | `355.64 MiB +- 4.46` | `355.64 MiB +- 4.46` | `2534.56 MiB +- 229.80` |
| ArcadeDB Unindexed (3) | `147.33 MiB +- 0.20` | `204.18 MiB +- 5.70` | `270.43 MiB +- 3.16` | `270.43 MiB +- 3.16` | `270.43 MiB +- 3.16` | `2436.93 MiB +- 81.23` |
| LadybugDB Unindexed (3) | `270.70 MiB +- 0.03` | `295.99 MiB +- 0.14` | `461.85 MiB +- 5.23` | `461.85 MiB +- 5.23` | `462.10 MiB +- 5.23` | `500.30 MiB +- 2.81` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `588.83 MiB +- 53.15` | `588.54 MiB +- 52.84` | `587.56 MiB +- 51.96` | `587.56 MiB +- 51.96` | `587.56 MiB +- 51.96` | `344.23 MiB +- 2.40` |
| SQLite Unindexed (3) | `599.40 MiB +- 6.09` | `599.43 MiB +- 6.09` | `598.13 MiB +- 5.57` | `597.80 MiB +- 5.07` | `597.47 MiB +- 4.59` | `346.36 MiB +- 4.62` |
| DuckDB Indexed (3) | `752.98 MiB +- 57.02` | `751.85 MiB +- 57.53` | `777.69 MiB +- 56.22` | `789.62 MiB +- 53.92` | `789.63 MiB +- 53.92` | `514.37 MiB +- 15.76` |
| DuckDB Unindexed (3) | `795.96 MiB +- 32.04` | `793.14 MiB +- 30.63` | `822.14 MiB +- 29.48` | `821.47 MiB +- 28.62` | `820.81 MiB +- 28.07` | `499.16 MiB +- 5.13` |
| PostgreSQL Indexed (3) | `770.68 MiB +- 9.61` | `770.82 MiB +- 9.89` | `769.93 MiB +- 11.69` | `770.35 MiB +- 11.64` | `772.24 MiB +- 11.48` | `430.96 MiB +- 1.03` |
| PostgreSQL Unindexed (3) | `785.55 MiB +- 36.81` | `785.12 MiB +- 36.90` | `784.03 MiB +- 36.99` | `783.74 MiB +- 36.58` | `786.48 MiB +- 36.58` | `422.58 MiB +- 4.32` |
| Neo4j Indexed (3) | `679.56 MiB +- 8.13` | `732.38 MiB +- 10.23` | `1782.05 MiB +- 22.57` | `912.43 MiB +- 11.82` | `0.00 MiB +- 0.00` | `1216.73 MiB +- 16.77` |
| Neo4j Unindexed (3) | `657.49 MiB +- 6.79` | `702.53 MiB +- 2.96` | `1456.01 MiB +- 536.79` | `1448.51 MiB +- 535.44` | `0.00 MiB +- 0.00` | `1181.76 MiB +- 45.04` |
| ArcadeDB Indexed (3) | `2534.56 MiB +- 229.80` | `2534.62 MiB +- 229.79` | `1238.61 MiB +- 901.30` | `960.25 MiB +- 1143.34` | `460.09 MiB +- 231.70` | `995.73 MiB +- 47.10` |
| ArcadeDB Unindexed (3) | `2436.94 MiB +- 81.22` | `2436.99 MiB +- 81.23` | `2437.04 MiB +- 81.23` | `2437.04 MiB +- 81.23` | `2269.04 MiB +- 363.32` | `1023.14 MiB +- 71.73` |
| LadybugDB Unindexed (3) | `478.59 MiB +- 7.27` | `481.65 MiB +- 5.71` | `544.00 MiB +- 6.10` | `544.00 MiB +- 6.10` | `544.00 MiB +- 6.10` | `607.87 MiB +- 11.14` |

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.27 ms +- 0.07` | `1.75 ms +- 0.37` | `2.31 ms +- 0.55` |
| `olap/sqlite_indexed` | `2.60 ms +- 0.45` | `4.20 ms +- 1.55` | `5.27 ms +- 2.06` |
| `oltp/sqlite_unindexed` | `1.28 ms +- 0.03` | `1.44 ms +- 0.20` | `1.74 ms +- 0.38` |
| `olap/sqlite_unindexed` | `2.53 ms +- 0.24` | `2.89 ms +- 0.77` | `3.13 ms +- 1.06` |
| `oltp/duckdb_indexed` | `2.19 ms +- 0.18` | `3.49 ms +- 1.14` | `4.42 ms +- 1.60` |
| `olap/duckdb_indexed` | `2.92 ms +- 0.11` | `3.51 ms +- 0.22` | `3.98 ms +- 0.19` |
| `oltp/duckdb_unindexed` | `2.08 ms +- 0.11` | `2.92 ms +- 0.64` | `3.58 ms +- 0.96` |
| `olap/duckdb_unindexed` | `3.26 ms +- 0.65` | `4.29 ms +- 1.55` | `4.88 ms +- 1.83` |
| `oltp/postgresql_indexed` | `1.50 ms +- 0.04` | `1.97 ms +- 0.09` | `2.34 ms +- 0.09` |
| `olap/postgresql_indexed` | `3.07 ms +- 0.10` | `3.74 ms +- 0.17` | `4.31 ms +- 0.17` |
| `oltp/postgresql_unindexed` | `1.60 ms +- 0.03` | `1.97 ms +- 0.09` | `2.39 ms +- 0.08` |
| `olap/postgresql_unindexed` | `2.89 ms +- 0.13` | `3.41 ms +- 0.28` | `3.87 ms +- 0.30` |
| `oltp/neo4j_indexed` | `0.24 ms +- 0.01` | `0.34 ms +- 0.02` | `0.47 ms +- 0.04` |
| `olap/neo4j_indexed` | `2.50 ms +- 0.30` | `3.22 ms +- 0.61` | `3.86 ms +- 0.77` |
| `oltp/neo4j_unindexed` | `0.35 ms +- 0.01` | `0.54 ms +- 0.09` | `0.83 ms +- 0.29` |
| `olap/neo4j_unindexed` | `2.29 ms +- 0.07` | `2.73 ms +- 0.12` | `3.32 ms +- 0.17` |
| `oltp/arcadedb_embedded_indexed` | `0.04 ms +- 0.00` | `0.08 ms +- 0.02` | `0.12 ms +- 0.05` |
| `olap/arcadedb_embedded_indexed` | `3.22 ms +- 0.11` | `4.32 ms +- 0.40` | `5.09 ms +- 0.43` |
| `oltp/arcadedb_embedded_unindexed` | `0.43 ms +- 0.01` | `0.49 ms +- 0.01` | `0.63 ms +- 0.00` |
| `olap/arcadedb_embedded_unindexed` | `2.96 ms +- 0.07` | `3.68 ms +- 0.14` | `4.20 ms +- 0.13` |
| `oltp/ladybug_unindexed` | `1.42 ms +- 0.18` | `5.38 ms +- 1.91` | `14.79 ms +- 0.87` |
| `olap/ladybug_unindexed` | `5.07 ms +- 0.15` | `22.15 ms +- 0.98` | `26.50 ms +- 0.20` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher
  execution timings, so they are not strictly comparable to the
  compile-plus-execute SQL
  paths.
- DuckDB can appear in indexed and unindexed modes here; each
  table includes whichever DuckDB runs are present in the current
  matrix.
- ArcadeDB Embedded is shown in both indexed and unindexed modes
  because the harness supports both direct-runtime paths in the
  current matrix.
- LadybugDB is also a single-path run here. The current harness benchmarks
  an unindexed direct-Cypher path.
- RSS values in these tables are point-in-time resident-memory snapshots
  taken at each named checkpoint, not deltas from the previous step
  and not
  peak-memory readings.
- Total RSS is the sum of benchmark-process RSS plus database-server
  RSS when
  the backend is external.

#### Small runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `371.12 ms +- 57.94` | `290.22 ms +- 13.30` |
| `oltp_create_type1_node` | `404.45 ms +- 56.81` | `287.50 ms +- 5.00` |
| `oltp_cross_type_lookup` | `358.37 ms +- 64.54` | `291.54 ms +- 6.59` |
| `oltp_delete_type1_edge` | `355.64 ms +- 76.63` | `291.15 ms +- 10.01` |
| `oltp_delete_type1_node` | `371.28 ms +- 66.20` | `296.76 ms +- 10.77` |
| `oltp_merge_cross_type_edge` | `362.46 ms +- 63.65` | `297.60 ms +- 7.91` |
| `oltp_optional_missing_type1_lookup` | `357.26 ms +- 55.69` | `303.05 ms +- 14.32` |
| `oltp_optional_type1_lookup` | `365.82 ms +- 53.10` | `291.59 ms +- 5.51` |
| `oltp_program_create_and_link` | `367.46 ms +- 55.93` | `296.65 ms +- 7.73` |
| `oltp_type1_neighbors` | `362.57 ms +- 58.72` | `295.78 ms +- 2.78` |
| `oltp_type1_point_lookup` | `366.73 ms +- 64.27` | `295.00 ms +- 12.03` |
| `oltp_unwind_literal_top2` | `358.27 ms +- 68.65` | `303.85 ms +- 12.84` |
| `oltp_update_cross_type_edge_rank` | `363.37 ms +- 43.26` | `288.24 ms +- 15.03` |
| `oltp_update_type1_score` | `368.61 ms +- 62.28` | `296.51 ms +- 10.21` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `169.92 ms +- 22.63` | `186.48 ms +- 14.45` |
| `oltp_create_type1_node` | `162.93 ms +- 32.44` | `150.99 ms +- 3.85` |
| `oltp_cross_type_lookup` | `191.18 ms +- 34.51` | `184.60 ms +- 15.82` |
| `oltp_delete_type1_edge` | `164.21 ms +- 31.82` | `163.19 ms +- 6.71` |
| `oltp_delete_type1_node` | `166.58 ms +- 26.02` | `159.53 ms +- 2.88` |
| `oltp_merge_cross_type_edge` | `176.27 ms +- 36.77` | `174.29 ms +- 5.94` |
| `oltp_optional_missing_type1_lookup` | `191.03 ms +- 24.65` | `164.69 ms +- 2.29` |
| `oltp_optional_type1_lookup` | `211.09 ms +- 40.86` | `176.25 ms +- 22.00` |
| `oltp_program_create_and_link` | `179.21 ms +- 30.13` | `172.18 ms +- 6.02` |
| `oltp_type1_neighbors` | `192.64 ms +- 27.35` | `173.92 ms +- 17.40` |
| `oltp_type1_point_lookup` | `193.65 ms +- 21.89` | `158.04 ms +- 5.17` |
| `oltp_unwind_literal_top2` | `190.88 ms +- 18.15` | `152.15 ms +- 1.48` |
| `oltp_update_cross_type_edge_rank` | `163.90 ms +- 20.85` | `165.47 ms +- 3.17` |
| `oltp_update_type1_score` | `166.10 ms +- 27.15` | `164.10 ms +- 10.48` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `169.92 ms +- 22.63` | `186.48 ms +- 14.45` |
| `oltp_create_type1_node` | `162.93 ms +- 32.44` | `150.99 ms +- 3.85` |
| `oltp_cross_type_lookup` | `191.18 ms +- 34.51` | `184.60 ms +- 15.82` |
| `oltp_delete_type1_edge` | `164.21 ms +- 31.82` | `163.19 ms +- 6.71` |
| `oltp_delete_type1_node` | `166.58 ms +- 26.02` | `159.53 ms +- 2.88` |
| `oltp_merge_cross_type_edge` | `176.27 ms +- 36.77` | `174.29 ms +- 5.94` |
| `oltp_optional_missing_type1_lookup` | `191.03 ms +- 24.65` | `164.69 ms +- 2.29` |
| `oltp_optional_type1_lookup` | `211.09 ms +- 40.86` | `176.25 ms +- 22.00` |
| `oltp_program_create_and_link` | `179.21 ms +- 30.13` | `172.18 ms +- 6.02` |
| `oltp_type1_neighbors` | `192.64 ms +- 27.35` | `173.92 ms +- 17.40` |
| `oltp_type1_point_lookup` | `193.65 ms +- 21.89` | `158.04 ms +- 5.17` |
| `oltp_unwind_literal_top2` | `190.88 ms +- 18.15` | `152.15 ms +- 1.48` |
| `oltp_update_cross_type_edge_rank` | `163.90 ms +- 20.85` | `165.47 ms +- 3.17` |
| `oltp_update_type1_score` | `166.10 ms +- 27.15` | `164.10 ms +- 10.48` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `0.64 ms +- 0.02` | `0.66 ms +- 0.15` |
| `oltp_create_type1_node` | `0.75 ms +- 0.14` | `0.83 ms +- 0.32` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.41 ms +- 0.10` | `0.28 ms +- 0.03` |
| `oltp_delete_type1_node` | `0.39 ms +- 0.13` | `0.27 ms +- 0.06` |
| `oltp_merge_cross_type_edge` | `0.64 ms +- 0.07` | `0.75 ms +- 0.08` |
| `oltp_optional_missing_type1_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_program_create_and_link` | `0.71 ms +- 0.20` | `0.54 ms +- 0.05` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.41 ms +- 0.05` | `0.39 ms +- 0.10` |
| `oltp_update_type1_score` | `0.38 ms +- 0.01` | `0.39 ms +- 0.10` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `377.74 ms +- 76.63` | `320.21 ms +- 22.54` |
| `olap_fixed_length_path_projection` | `388.75 ms +- 77.27` | `306.79 ms +- 13.47` |
| `olap_fixed_length_path_with_rebinding` | `384.30 ms +- 72.67` | `301.37 ms +- 10.60` |
| `olap_graph_introspection_rollup` | `379.37 ms +- 75.94` | `312.09 ms +- 6.96` |
| `olap_optional_type1_aggregate` | `372.04 ms +- 69.52` | `312.37 ms +- 19.56` |
| `olap_relationship_function_projection` | `389.33 ms +- 63.64` | `308.29 ms +- 14.73` |
| `olap_three_type_path_count` | `372.78 ms +- 43.91` | `306.59 ms +- 15.68` |
| `olap_type1_active_leaderboard` | `365.48 ms +- 63.53` | `302.36 ms +- 6.71` |
| `olap_type1_age_rollup` | `374.90 ms +- 65.75` | `303.18 ms +- 10.98` |
| `olap_type2_score_distribution` | `369.75 ms +- 48.16` | `308.31 ms +- 6.97` |
| `olap_variable_length_grouped_max_rollup` | `381.40 ms +- 52.18` | `311.52 ms +- 0.81` |
| `olap_variable_length_grouped_rollup` | `396.05 ms +- 61.15` | `318.88 ms +- 10.71` |
| `olap_variable_length_reachability` | `392.40 ms +- 68.83` | `313.24 ms +- 8.32` |
| `olap_with_scalar_rebinding` | `376.18 ms +- 61.00` | `314.04 ms +- 5.55` |
| `olap_with_size_predicate_projection` | `400.09 ms +- 84.31` | `311.10 ms +- 6.85` |
| `olap_with_where_lower_projection` | `376.46 ms +- 60.02` | `307.54 ms +- 8.45` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `249.70 ms +- 56.75` | `224.96 ms +- 28.45` |
| `olap_fixed_length_path_projection` | `263.56 ms +- 63.69` | `253.77 ms +- 14.13` |
| `olap_fixed_length_path_with_rebinding` | `289.52 ms +- 77.51` | `247.10 ms +- 17.55` |
| `olap_graph_introspection_rollup` | `268.51 ms +- 58.58` | `249.77 ms +- 3.44` |
| `olap_optional_type1_aggregate` | `206.18 ms +- 30.78` | `189.26 ms +- 17.79` |
| `olap_relationship_function_projection` | `266.19 ms +- 58.38` | `237.47 ms +- 20.81` |
| `olap_three_type_path_count` | `203.29 ms +- 32.40` | `178.60 ms +- 9.32` |
| `olap_type1_active_leaderboard` | `231.13 ms +- 40.54` | `216.73 ms +- 1.46` |
| `olap_type1_age_rollup` | `211.73 ms +- 43.70` | `208.05 ms +- 30.17` |
| `olap_type2_score_distribution` | `206.88 ms +- 44.65` | `185.36 ms +- 13.39` |
| `olap_variable_length_grouped_max_rollup` | `204.66 ms +- 26.10` | `213.66 ms +- 10.01` |
| `olap_variable_length_grouped_rollup` | `309.72 ms +- 64.18` | `296.33 ms +- 9.74` |
| `olap_variable_length_reachability` | `205.62 ms +- 27.89` | `197.58 ms +- 3.22` |
| `olap_with_scalar_rebinding` | `229.91 ms +- 45.04` | `213.02 ms +- 6.04` |
| `olap_with_size_predicate_projection` | `226.50 ms +- 46.93` | `221.00 ms +- 27.92` |
| `olap_with_where_lower_projection` | `233.00 ms +- 21.04` | `222.11 ms +- 9.60` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe end-to-end`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `249.70 ms +- 56.75` | `224.96 ms +- 28.45` |
| `olap_fixed_length_path_projection` | `263.56 ms +- 63.69` | `253.77 ms +- 14.13` |
| `olap_fixed_length_path_with_rebinding` | `289.52 ms +- 77.51` | `247.10 ms +- 17.55` |
| `olap_graph_introspection_rollup` | `268.51 ms +- 58.58` | `249.77 ms +- 3.44` |
| `olap_optional_type1_aggregate` | `206.18 ms +- 30.78` | `189.26 ms +- 17.79` |
| `olap_relationship_function_projection` | `266.19 ms +- 58.38` | `237.47 ms +- 20.81` |
| `olap_three_type_path_count` | `203.29 ms +- 32.40` | `178.60 ms +- 9.32` |
| `olap_type1_active_leaderboard` | `231.13 ms +- 40.54` | `216.73 ms +- 1.46` |
| `olap_type1_age_rollup` | `211.73 ms +- 43.70` | `208.05 ms +- 30.17` |
| `olap_type2_score_distribution` | `206.88 ms +- 44.65` | `185.36 ms +- 13.39` |
| `olap_variable_length_grouped_max_rollup` | `204.66 ms +- 26.10` | `213.66 ms +- 10.01` |
| `olap_variable_length_grouped_rollup` | `309.72 ms +- 64.18` | `296.33 ms +- 9.74` |
| `olap_variable_length_reachability` | `205.62 ms +- 27.89` | `197.58 ms +- 3.22` |
| `olap_with_scalar_rebinding` | `229.91 ms +- 45.04` | `213.02 ms +- 6.04` |
| `olap_with_size_predicate_projection` | `226.50 ms +- 46.93` | `221.00 ms +- 27.92` |
| `olap_with_where_lower_projection` | `233.00 ms +- 21.04` | `222.11 ms +- 9.60` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_fixed_length_path_with_rebinding` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_relationship_function_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_three_type_path_count` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type1_age_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_type2_score_distribution` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_variable_length_reachability` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap_with_where_lower_projection` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.68 ms +- 0.08` | `1.61 ms +- 0.01` | `2.87 ms +- 0.63` | `2.65 ms +- 0.23` | `1.87 ms +- 0.07` | `1.95 ms +- 0.05` | `0.27 ms +- 0.00` | `0.50 ms +- 0.02` | `0.07 ms +- 0.01` | `0.86 ms +- 0.02` | `0.74 ms +- 0.06` |
| `oltp_create_type1_node` | `0.85 ms +- 0.03` | `0.79 ms +- 0.00` | `1.47 ms +- 0.18` | `1.35 ms +- 0.06` | `1.01 ms +- 0.04` | `0.97 ms +- 0.01` | `0.22 ms +- 0.00` | `0.20 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` | `0.24 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.48 ms +- 0.13` | `1.48 ms +- 0.01` | `2.77 ms +- 0.45` | `2.30 ms +- 0.12` | `1.79 ms +- 0.02` | `1.87 ms +- 0.00` | `0.26 ms +- 0.00` | `0.37 ms +- 0.01` | `0.02 ms +- 0.01` | `0.41 ms +- 0.00` | `2.49 ms +- 0.88` |
| `oltp_delete_type1_edge` | `0.93 ms +- 0.08` | `0.98 ms +- 0.05` | `2.13 ms +- 0.12` | `1.90 ms +- 0.11` | `1.24 ms +- 0.01` | `1.37 ms +- 0.02` | `0.22 ms +- 0.01` | `0.32 ms +- 0.01` | `0.04 ms +- 0.01` | `0.43 ms +- 0.00` | `1.72 ms +- 0.84` |
| `oltp_delete_type1_node` | `0.69 ms +- 0.04` | `1.19 ms +- 0.11` | `0.91 ms +- 0.01` | `0.90 ms +- 0.01` | `0.79 ms +- 0.01` | `1.18 ms +- 0.01` | `0.22 ms +- 0.01` | `0.34 ms +- 0.01` | `0.06 ms +- 0.01` | `0.45 ms +- 0.01` | `0.48 ms +- 0.05` |
| `oltp_merge_cross_type_edge` | `1.99 ms +- 0.13` | `1.93 ms +- 0.02` | `3.66 ms +- 0.28` | `3.46 ms +- 0.06` | `2.25 ms +- 0.01` | `2.56 ms +- 0.10` | `0.27 ms +- 0.01` | `0.50 ms +- 0.02` | `0.05 ms +- 0.01` | `0.86 ms +- 0.01` | `5.06 ms +- 0.21` |
| `oltp_program_create_and_link` | `2.29 ms +- 0.29` | `2.05 ms +- 0.20` | `3.24 ms +- 0.01` | `3.83 ms +- 0.99` | `2.33 ms +- 0.02` | `2.36 ms +- 0.06` | `0.23 ms +- 0.01` | `0.33 ms +- 0.01` | `0.05 ms +- 0.01` | `0.45 ms +- 0.00` | `0.68 ms +- 0.07` |
| `oltp_type1_neighbors` | `1.20 ms +- 0.09` | `1.26 ms +- 0.01` | `2.29 ms +- 0.32` | `2.34 ms +- 0.60` | `1.56 ms +- 0.06` | `1.62 ms +- 0.01` | `0.26 ms +- 0.01` | `0.38 ms +- 0.02` | `0.04 ms +- 0.00` | `0.41 ms +- 0.00` | `2.62 ms +- 1.23` |
| `oltp_type1_point_lookup` | `1.13 ms +- 0.02` | `1.15 ms +- 0.01` | `1.42 ms +- 0.06` | `1.46 ms +- 0.11` | `1.37 ms +- 0.05` | `1.36 ms +- 0.02` | `0.30 ms +- 0.01` | `0.42 ms +- 0.02` | `0.03 ms +- 0.00` | `0.41 ms +- 0.01` | `0.37 ms +- 0.03` |
| `oltp_unwind_literal_top2` | `1.04 ms +- 0.06` | `0.98 ms +- 0.00` | `1.35 ms +- 0.07` | `1.35 ms +- 0.08` | `1.19 ms +- 0.01` | `1.19 ms +- 0.00` | `0.23 ms +- 0.01` | `0.22 ms +- 0.02` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.53 ms +- 0.07` |
| `oltp_update_cross_type_edge_rank` | `1.14 ms +- 0.07` | `1.16 ms +- 0.05` | `2.54 ms +- 0.07` | `2.07 ms +- 0.06` | `1.42 ms +- 0.00` | `1.68 ms +- 0.07` | `0.22 ms +- 0.01` | `0.32 ms +- 0.01` | `0.03 ms +- 0.00` | `0.42 ms +- 0.00` | `1.67 ms +- 0.76` |
| `oltp_update_type1_score` | `0.86 ms +- 0.03` | `0.83 ms +- 0.01` | `1.63 ms +- 0.25` | `1.31 ms +- 0.20` | `1.03 ms +- 0.03` | `1.09 ms +- 0.02` | `0.23 ms +- 0.01` | `0.34 ms +- 0.02` | `0.02 ms +- 0.00` | `0.42 ms +- 0.01` | `0.39 ms +- 0.02` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.27 ms +- 0.46` | `1.72 ms +- 0.01` | `4.36 ms +- 2.05` | `4.13 ms +- 1.95` | `2.39 ms +- 0.24` | `2.36 ms +- 0.23` | `0.35 ms +- 0.01` | `0.78 ms +- 0.19` | `0.18 ms +- 0.04` | `0.94 ms +- 0.03` | `1.21 ms +- 0.60` |
| `oltp_create_type1_node` | `1.07 ms +- 0.23` | `0.84 ms +- 0.01` | `2.70 ms +- 1.04` | `2.10 ms +- 0.83` | `1.38 ms +- 0.15` | `1.14 ms +- 0.02` | `0.29 ms +- 0.01` | `0.31 ms +- 0.06` | `0.04 ms +- 0.02` | `0.02 ms +- 0.00` | `0.39 ms +- 0.15` |
| `oltp_cross_type_lookup` | `2.04 ms +- 0.48` | `1.59 ms +- 0.08` | `5.60 ms +- 3.17` | `2.78 ms +- 0.21` | `2.37 ms +- 0.07` | `2.40 ms +- 0.05` | `0.37 ms +- 0.04` | `0.52 ms +- 0.05` | `0.04 ms +- 0.01` | `0.45 ms +- 0.01` | `9.39 ms +- 9.82` |
| `oltp_delete_type1_edge` | `1.32 ms +- 0.30` | `1.26 ms +- 0.44` | `3.53 ms +- 1.73` | `2.40 ms +- 0.07` | `1.69 ms +- 0.09` | `1.65 ms +- 0.16` | `0.29 ms +- 0.00` | `0.52 ms +- 0.18` | `0.08 ms +- 0.03` | `0.47 ms +- 0.01` | `4.33 ms +- 2.97` |
| `oltp_delete_type1_node` | `0.95 ms +- 0.28` | `1.57 ms +- 0.68` | `1.10 ms +- 0.14` | `1.10 ms +- 0.03` | `1.06 ms +- 0.11` | `1.44 ms +- 0.13` | `0.29 ms +- 0.00` | `0.50 ms +- 0.09` | `0.11 ms +- 0.05` | `0.49 ms +- 0.01` | `0.80 ms +- 0.40` |
| `oltp_merge_cross_type_edge` | `2.77 ms +- 0.62` | `2.04 ms +- 0.03` | `5.49 ms +- 2.11` | `5.00 ms +- 1.57` | `2.92 ms +- 0.05` | `3.18 ms +- 0.28` | `0.35 ms +- 0.00` | `0.81 ms +- 0.27` | `0.16 ms +- 0.02` | `1.02 ms +- 0.02` | `29.45 ms +- 0.95` |
| `oltp_program_create_and_link` | `3.62 ms +- 1.37` | `2.49 ms +- 0.86` | `3.98 ms +- 0.17` | `5.44 ms +- 2.61` | `2.87 ms +- 0.08` | `2.77 ms +- 0.21` | `0.30 ms +- 0.01` | `0.46 ms +- 0.04` | `0.12 ms +- 0.05` | `0.49 ms +- 0.00` | `1.09 ms +- 0.53` |
| `oltp_type1_neighbors` | `1.48 ms +- 0.37` | `1.34 ms +- 0.06` | `4.73 ms +- 2.47` | `3.52 ms +- 1.83` | `2.09 ms +- 0.13` | `2.10 ms +- 0.05` | `0.38 ms +- 0.05` | `0.56 ms +- 0.13` | `0.07 ms +- 0.01` | `0.45 ms +- 0.00` | `11.61 ms +- 13.79` |
| `oltp_type1_point_lookup` | `1.34 ms +- 0.19` | `1.25 ms +- 0.04` | `2.07 ms +- 0.40` | `2.04 ms +- 0.62` | `1.83 ms +- 0.23` | `1.69 ms +- 0.13` | `0.52 ms +- 0.07` | `0.82 ms +- 0.22` | `0.06 ms +- 0.01` | `0.65 ms +- 0.01` | `0.56 ms +- 0.23` |
| `oltp_unwind_literal_top2` | `1.26 ms +- 0.28` | `1.04 ms +- 0.02` | `2.06 ms +- 0.56` | `1.83 ms +- 0.55` | `1.54 ms +- 0.09` | `1.49 ms +- 0.04` | `0.34 ms +- 0.04` | `0.32 ms +- 0.07` | `0.02 ms +- 0.01` | `0.01 ms +- 0.00` | `0.97 ms +- 0.47` |
| `oltp_update_cross_type_edge_rank` | `1.80 ms +- 0.49` | `1.28 ms +- 0.20` | `3.20 ms +- 0.24` | `2.65 ms +- 0.13` | `1.88 ms +- 0.04` | `2.09 ms +- 0.20` | `0.32 ms +- 0.06` | `0.44 ms +- 0.04` | `0.05 ms +- 0.01` | `0.45 ms +- 0.01` | `4.09 ms +- 2.89` |
| `oltp_update_type1_score` | `1.09 ms +- 0.18` | `0.89 ms +- 0.00` | `3.10 ms +- 1.37` | `2.09 ms +- 1.05` | `1.34 ms +- 0.14` | `1.37 ms +- 0.05` | `0.33 ms +- 0.06` | `0.49 ms +- 0.08` | `0.05 ms +- 0.02` | `0.45 ms +- 0.01` | `0.63 ms +- 0.27` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.84 ms +- 0.63` | `2.04 ms +- 0.07` | `5.10 ms +- 2.47` | `4.96 ms +- 2.66` | `2.80 ms +- 0.21` | `2.80 ms +- 0.19` | `0.44 ms +- 0.06` | `1.33 ms +- 0.93` | `0.27 ms +- 0.10` | `1.21 ms +- 0.06` | `1.56 ms +- 1.03` |
| `oltp_create_type1_node` | `1.41 ms +- 0.43` | `1.01 ms +- 0.07` | `3.71 ms +- 1.66` | `2.78 ms +- 1.36` | `1.70 ms +- 0.23` | `1.47 ms +- 0.02` | `0.38 ms +- 0.04` | `0.61 ms +- 0.41` | `0.07 ms +- 0.04` | `0.04 ms +- 0.00` | `0.92 ms +- 0.95` |
| `oltp_cross_type_lookup` | `2.66 ms +- 0.65` | `1.87 ms +- 0.24` | `7.03 ms +- 4.28` | `3.33 ms +- 0.30` | `2.69 ms +- 0.07` | `2.78 ms +- 0.04` | `0.50 ms +- 0.09` | `0.69 ms +- 0.19` | `0.05 ms +- 0.01` | `0.57 ms +- 0.01` | `33.36 ms +- 4.11` |
| `oltp_delete_type1_edge` | `1.80 ms +- 0.46` | `1.83 ms +- 1.03` | `4.46 ms +- 2.60` | `2.95 ms +- 0.04` | `2.05 ms +- 0.06` | `2.07 ms +- 0.15` | `0.37 ms +- 0.04` | `0.82 ms +- 0.54` | `0.14 ms +- 0.06` | `0.58 ms +- 0.02` | `33.77 ms +- 5.20` |
| `oltp_delete_type1_node` | `1.32 ms +- 0.59` | `2.05 ms +- 1.20` | `1.44 ms +- 0.14` | `1.47 ms +- 0.07` | `1.38 ms +- 0.08` | `1.81 ms +- 0.15` | `0.35 ms +- 0.04` | `0.73 ms +- 0.33` | `0.16 ms +- 0.09` | `0.60 ms +- 0.02` | `1.02 ms +- 0.60` |
| `oltp_merge_cross_type_edge` | `3.35 ms +- 0.78` | `2.33 ms +- 0.15` | `6.59 ms +- 3.22` | `6.05 ms +- 2.58` | `3.35 ms +- 0.02` | `3.71 ms +- 0.27` | `0.43 ms +- 0.04` | `1.26 ms +- 0.85` | `0.27 ms +- 0.10` | `1.57 ms +- 0.05` | `38.26 ms +- 0.83` |
| `oltp_program_create_and_link` | `4.60 ms +- 2.06` | `2.88 ms +- 1.25` | `4.57 ms +- 0.35` | `6.28 ms +- 2.92` | `3.37 ms +- 0.03` | `3.30 ms +- 0.15` | `0.37 ms +- 0.04` | `0.62 ms +- 0.09` | `0.17 ms +- 0.10` | `0.61 ms +- 0.01` | `1.32 ms +- 0.74` |
| `oltp_type1_neighbors` | `1.98 ms +- 0.61` | `1.60 ms +- 0.23` | `6.36 ms +- 3.68` | `4.14 ms +- 2.13` | `2.40 ms +- 0.14` | `2.50 ms +- 0.07` | `0.52 ms +- 0.10` | `0.76 ms +- 0.35` | `0.09 ms +- 0.01` | `0.54 ms +- 0.01` | `33.96 ms +- 4.93` |
| `oltp_type1_point_lookup` | `1.71 ms +- 0.28` | `1.56 ms +- 0.15` | `3.05 ms +- 0.99` | `2.68 ms +- 0.91` | `2.18 ms +- 0.26` | `2.05 ms +- 0.06` | `0.84 ms +- 0.23` | `1.48 ms +- 0.68` | `0.09 ms +- 0.01` | `0.76 ms +- 0.02` | `0.72 ms +- 0.42` |
| `oltp_unwind_literal_top2` | `1.65 ms +- 0.51` | `1.22 ms +- 0.01` | `2.79 ms +- 0.81` | `2.37 ms +- 0.81` | `1.88 ms +- 0.08` | `1.81 ms +- 0.04` | `0.47 ms +- 0.09` | `0.43 ms +- 0.12` | `0.03 ms +- 0.01` | `0.02 ms +- 0.00` | `1.73 ms +- 1.59` |
| `oltp_update_cross_type_edge_rank` | `2.97 ms +- 1.17` | `1.41 ms +- 0.22` | `3.70 ms +- 0.40` | `3.20 ms +- 0.34` | `2.29 ms +- 0.04` | `2.54 ms +- 0.24` | `0.44 ms +- 0.11` | `0.57 ms +- 0.10` | `0.06 ms +- 0.02` | `0.53 ms +- 0.03` | `29.66 ms +- 6.26` |
| `oltp_update_type1_score` | `1.42 ms +- 0.33` | `1.08 ms +- 0.01` | `4.25 ms +- 2.07` | `2.73 ms +- 1.51` | `1.70 ms +- 0.14` | `1.81 ms +- 0.06` | `0.47 ms +- 0.10` | `0.66 ms +- 0.19` | `0.07 ms +- 0.05` | `0.55 ms +- 0.04` | `1.23 ms +- 1.18` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `2.44 ms +- 0.28` | `1.95 ms +- 0.10` | `2.46 ms +- 0.11` | `2.86 ms +- 0.51` | `2.65 ms +- 0.11` | `2.54 ms +- 0.12` | `2.23 ms +- 0.19` | `2.09 ms +- 0.14` | `4.71 ms +- 0.96` | `3.90 ms +- 0.04` | `4.67 ms +- 0.17` |
| `olap_fixed_length_path_projection` | `2.14 ms +- 0.46` | `3.66 ms +- 0.16` | `3.95 ms +- 0.31` | `4.39 ms +- 0.95` | `5.26 ms +- 0.39` | `4.53 ms +- 0.34` | `5.46 ms +- 0.82` | `4.73 ms +- 0.19` | `5.14 ms +- 0.22` | `4.11 ms +- 0.11` | `18.73 ms +- 0.51` |
| `olap_fixed_length_path_with_rebinding` | `4.37 ms +- 0.80` | `4.17 ms +- 0.16` | `4.62 ms +- 0.50` | `5.08 ms +- 1.30` | `5.51 ms +- 0.39` | `4.67 ms +- 0.27` | `5.76 ms +- 0.85` | `5.13 ms +- 0.08` | `5.82 ms +- 0.56` | `4.77 ms +- 0.09` | `8.08 ms +- 0.52` |
| `olap_graph_introspection_rollup` | `2.09 ms +- 0.29` | `2.18 ms +- 0.24` | `2.77 ms +- 0.15` | `3.14 ms +- 0.79` | `3.01 ms +- 0.12` | `2.84 ms +- 0.05` | `3.15 ms +- 0.29` | `2.94 ms +- 0.11` | `7.02 ms +- 0.16` | `6.48 ms +- 0.16` | `3.21 ms +- 0.09` |
| `olap_optional_type1_aggregate` | `1.47 ms +- 0.14` | `1.38 ms +- 0.05` | `1.93 ms +- 0.08` | `2.04 ms +- 0.13` | `1.59 ms +- 0.03` | `1.57 ms +- 0.04` | `0.72 ms +- 0.02` | `0.71 ms +- 0.03` | `0.79 ms +- 0.14` | `0.76 ms +- 0.19` | `1.45 ms +- 0.54` |
| `olap_relationship_function_projection` | `3.23 ms +- 0.49` | `2.48 ms +- 0.39` | `2.96 ms +- 0.07` | `3.48 ms +- 0.82` | `3.71 ms +- 0.05` | `3.52 ms +- 0.16` | `2.64 ms +- 0.18` | `2.47 ms +- 0.13` | `5.01 ms +- 0.10` | `4.65 ms +- 0.12` | `3.25 ms +- 0.14` |
| `olap_three_type_path_count` | `2.61 ms +- 0.16` | `2.46 ms +- 0.11` | `2.35 ms +- 0.18` | `2.60 ms +- 0.56` | `3.18 ms +- 0.15` | `2.84 ms +- 0.14` | `1.95 ms +- 0.12` | `1.87 ms +- 0.05` | `0.10 ms +- 0.02` | `0.06 ms +- 0.01` | `5.34 ms +- 0.34` |
| `olap_type1_active_leaderboard` | `1.39 ms +- 0.11` | `1.38 ms +- 0.06` | `2.06 ms +- 0.03` | `2.10 ms +- 0.02` | `1.60 ms +- 0.03` | `1.59 ms +- 0.04` | `1.47 ms +- 0.05` | `1.36 ms +- 0.02` | `1.38 ms +- 0.31` | `1.25 ms +- 0.47` | `0.56 ms +- 0.14` |
| `olap_type1_age_rollup` | `1.69 ms +- 0.16` | `1.56 ms +- 0.06` | `1.92 ms +- 0.03` | `2.03 ms +- 0.12` | `1.74 ms +- 0.02` | `1.73 ms +- 0.02` | `0.82 ms +- 0.02` | `0.81 ms +- 0.03` | `0.61 ms +- 0.03` | `0.59 ms +- 0.01` | `1.65 ms +- 0.38` |
| `olap_type2_score_distribution` | `1.76 ms +- 0.28` | `1.70 ms +- 0.10` | `2.11 ms +- 0.11` | `2.30 ms +- 0.35` | `1.87 ms +- 0.05` | `1.83 ms +- 0.03` | `0.79 ms +- 0.04` | `0.72 ms +- 0.04` | `0.59 ms +- 0.01` | `0.59 ms +- 0.01` | `2.12 ms +- 0.19` |
| `olap_variable_length_grouped_max_rollup` | `2.59 ms +- 0.69` | `3.17 ms +- 0.65` | `4.36 ms +- 0.22` | `5.16 ms +- 1.20` | `3.13 ms +- 0.09` | `3.31 ms +- 0.18` | `0.26 ms +- 0.02` | `0.38 ms +- 0.02` | `0.04 ms +- 0.00` | `0.44 ms +- 0.00` | `4.17 ms +- 1.40` |
| `olap_variable_length_grouped_rollup` | `6.80 ms +- 1.64` | `6.06 ms +- 1.16` | `4.38 ms +- 0.20` | `4.95 ms +- 1.34` | `6.24 ms +- 0.17` | `5.70 ms +- 0.48` | `10.71 ms +- 1.89` | `9.67 ms +- 0.28` | `16.81 ms +- 0.20` | `16.15 ms +- 0.51` | `22.37 ms +- 1.34` |
| `olap_variable_length_reachability` | `2.06 ms +- 0.28` | `2.54 ms +- 0.11` | `3.78 ms +- 0.29` | `4.30 ms +- 0.64` | `2.79 ms +- 0.06` | `2.91 ms +- 0.10` | `0.41 ms +- 0.01` | `0.50 ms +- 0.01` | `0.09 ms +- 0.02` | `0.47 ms +- 0.01` | `1.84 ms +- 0.23` |
| `olap_with_scalar_rebinding` | `2.63 ms +- 0.71` | `2.11 ms +- 0.24` | `2.50 ms +- 0.09` | `2.81 ms +- 0.67` | `2.38 ms +- 0.11` | `2.33 ms +- 0.04` | `0.89 ms +- 0.07` | `0.85 ms +- 0.02` | `0.70 ms +- 0.02` | `0.68 ms +- 0.01` | `2.25 ms +- 0.05` |
| `olap_with_size_predicate_projection` | `2.15 ms +- 0.36` | `1.92 ms +- 0.18` | `2.31 ms +- 0.04` | `2.53 ms +- 0.47` | `2.32 ms +- 0.05` | `2.28 ms +- 0.08` | `1.31 ms +- 0.09` | `1.22 ms +- 0.04` | `1.30 ms +- 0.04` | `1.20 ms +- 0.07` | `0.78 ms +- 0.11` |
| `olap_with_where_lower_projection` | `2.21 ms +- 0.38` | `1.82 ms +- 0.13` | `2.18 ms +- 0.06` | `2.40 ms +- 0.54` | `2.12 ms +- 0.05` | `2.10 ms +- 0.05` | `1.36 ms +- 0.12` | `1.27 ms +- 0.03` | `1.37 ms +- 0.09` | `1.28 ms +- 0.01` | `0.70 ms +- 0.09` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.04 ms +- 1.04` | `2.19 ms +- 0.46` | `3.09 ms +- 0.24` | `3.92 ms +- 1.26` | `3.31 ms +- 0.33` | `3.07 ms +- 0.19` | `3.28 ms +- 0.67` | `2.39 ms +- 0.23` | `7.28 ms +- 1.95` | `5.17 ms +- 0.42` | `46.25 ms +- 3.04` |
| `olap_fixed_length_path_projection` | `3.46 ms +- 1.35` | `3.85 ms +- 0.42` | `4.68 ms +- 0.38` | `5.67 ms +- 1.75` | `6.22 ms +- 0.32` | `5.46 ms +- 0.55` | `6.86 ms +- 1.20` | `5.66 ms +- 0.50` | `6.99 ms +- 0.86` | `6.24 ms +- 2.43` | `35.97 ms +- 10.81` |
| `olap_fixed_length_path_with_rebinding` | `7.47 ms +- 3.39` | `4.69 ms +- 0.75` | `5.49 ms +- 0.56` | `6.63 ms +- 2.63` | `6.55 ms +- 0.31` | `5.50 ms +- 0.28` | `7.23 ms +- 1.34` | `6.00 ms +- 0.17` | `9.62 ms +- 2.47` | `6.13 ms +- 0.08` | `61.36 ms +- 2.37` |
| `olap_graph_introspection_rollup` | `3.42 ms +- 1.12` | `2.72 ms +- 1.10` | `3.21 ms +- 0.35` | `4.11 ms +- 1.71` | `3.73 ms +- 0.07` | `3.43 ms +- 0.26` | `4.32 ms +- 0.92` | `3.46 ms +- 0.24` | `9.86 ms +- 2.29` | `7.76 ms +- 0.52` | `49.71 ms +- 0.45` |
| `olap_optional_type1_aggregate` | `2.24 ms +- 0.65` | `1.48 ms +- 0.13` | `2.41 ms +- 0.12` | `2.87 ms +- 1.04` | `2.01 ms +- 0.17` | `1.79 ms +- 0.21` | `1.03 ms +- 0.13` | `0.85 ms +- 0.07` | `1.26 ms +- 0.03` | `0.92 ms +- 0.23` | `2.72 ms +- 0.22` |
| `olap_relationship_function_projection` | `5.84 ms +- 2.23` | `3.05 ms +- 1.32` | `3.51 ms +- 0.16` | `4.52 ms +- 1.94` | `4.90 ms +- 0.21` | `4.07 ms +- 0.34` | `3.43 ms +- 0.70` | `2.96 ms +- 0.40` | `6.17 ms +- 0.19` | `5.40 ms +- 0.43` | `44.04 ms +- 3.91` |
| `olap_three_type_path_count` | `4.60 ms +- 1.62` | `2.72 ms +- 0.42` | `2.89 ms +- 0.36` | `3.67 ms +- 1.63` | `4.01 ms +- 0.14` | `3.45 ms +- 0.32` | `2.81 ms +- 0.48` | `2.29 ms +- 0.03` | `0.20 ms +- 0.01` | `0.14 ms +- 0.01` | `69.19 ms +- 5.06` |
| `olap_type1_active_leaderboard` | `2.02 ms +- 0.48` | `1.53 ms +- 0.26` | `2.58 ms +- 0.20` | `2.58 ms +- 0.20` | `1.94 ms +- 0.20` | `1.98 ms +- 0.22` | `2.10 ms +- 0.31` | `1.74 ms +- 0.15` | `2.51 ms +- 0.85` | `2.26 ms +- 0.71` | `0.84 ms +- 0.10` |
| `olap_type1_age_rollup` | `2.55 ms +- 0.56` | `1.66 ms +- 0.14` | `2.37 ms +- 0.10` | `2.86 ms +- 0.95` | `2.15 ms +- 0.25` | `1.96 ms +- 0.26` | `1.24 ms +- 0.23` | `1.09 ms +- 0.16` | `1.02 ms +- 0.21` | `0.77 ms +- 0.12` | `2.75 ms +- 0.16` |
| `olap_type2_score_distribution` | `3.10 ms +- 1.22` | `1.87 ms +- 0.36` | `2.59 ms +- 0.35` | `3.11 ms +- 1.26` | `2.32 ms +- 0.29` | `2.16 ms +- 0.11` | `1.13 ms +- 0.17` | `0.85 ms +- 0.09` | `0.81 ms +- 0.17` | `0.69 ms +- 0.09` | `3.01 ms +- 0.37` |
| `olap_variable_length_grouped_max_rollup` | `4.37 ms +- 2.59` | `3.87 ms +- 1.79` | `5.11 ms +- 0.19` | `6.62 ms +- 2.34` | `3.76 ms +- 0.14` | `3.83 ms +- 0.43` | `0.36 ms +- 0.07` | `0.50 ms +- 0.04` | `0.05 ms +- 0.01` | `0.48 ms +- 0.02` | `5.57 ms +- 2.34` |
| `olap_variable_length_grouped_rollup` | `9.39 ms +- 4.43` | `6.87 ms +- 2.45` | `5.31 ms +- 0.17` | `6.36 ms +- 2.70` | `7.30 ms +- 0.22` | `6.47 ms +- 0.69` | `12.49 ms +- 2.71` | `10.90 ms +- 0.06` | `18.61 ms +- 0.33` | `18.19 ms +- 0.84` | `25.37 ms +- 2.88` |
| `olap_variable_length_reachability` | `3.21 ms +- 1.01` | `2.68 ms +- 0.29` | `4.59 ms +- 0.45` | `5.59 ms +- 1.59` | `3.46 ms +- 0.28` | `3.38 ms +- 0.22` | `0.57 ms +- 0.06` | `0.62 ms +- 0.04` | `0.14 ms +- 0.03` | `0.54 ms +- 0.05` | `2.56 ms +- 0.39` |
| `olap_with_scalar_rebinding` | `4.44 ms +- 1.95` | `2.61 ms +- 1.04` | `2.92 ms +- 0.22` | `3.56 ms +- 1.46` | `2.64 ms +- 0.21` | `2.78 ms +- 0.34` | `1.19 ms +- 0.32` | `1.22 ms +- 0.17` | `0.98 ms +- 0.01` | `0.95 ms +- 0.04` | `3.15 ms +- 0.39` |
| `olap_with_size_predicate_projection` | `3.82 ms +- 1.70` | `2.29 ms +- 0.77` | `2.75 ms +- 0.16` | `3.40 ms +- 1.29` | `2.93 ms +- 0.08` | `2.66 ms +- 0.31` | `1.75 ms +- 0.24` | `1.49 ms +- 0.14` | `1.64 ms +- 0.08` | `1.44 ms +- 0.13` | `0.94 ms +- 0.17` |
| `olap_with_where_lower_projection` | `3.22 ms +- 1.48` | `2.18 ms +- 0.69` | `2.62 ms +- 0.35` | `3.09 ms +- 1.47` | `2.63 ms +- 0.19` | `2.51 ms +- 0.22` | `1.82 ms +- 0.36` | `1.67 ms +- 0.11` | `1.96 ms +- 0.12` | `1.80 ms +- 0.02` | `0.92 ms +- 0.19` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.88 ms +- 1.33` | `2.23 ms +- 0.47` | `3.52 ms +- 0.21` | `4.48 ms +- 1.42` | `4.01 ms +- 0.91` | `3.45 ms +- 0.06` | `3.98 ms +- 0.61` | `2.95 ms +- 0.47` | `8.74 ms +- 1.62` | `5.95 ms +- 0.62` | `53.42 ms +- 2.35` |
| `olap_fixed_length_path_projection` | `4.99 ms +- 2.34` | `4.16 ms +- 0.77` | `5.33 ms +- 0.31` | `6.40 ms +- 2.22` | `7.24 ms +- 0.35` | `6.17 ms +- 0.38` | `8.03 ms +- 1.49` | `6.72 ms +- 0.66` | `8.28 ms +- 1.23` | `7.15 ms +- 1.82` | `46.85 ms +- 10.17` |
| `olap_fixed_length_path_with_rebinding` | `8.62 ms +- 4.09` | `5.06 ms +- 1.07` | `6.04 ms +- 0.50` | `7.39 ms +- 2.85` | `7.30 ms +- 0.29` | `6.13 ms +- 0.28` | `8.61 ms +- 1.12` | `7.46 ms +- 0.50` | `12.01 ms +- 2.29` | `6.98 ms +- 0.30` | `65.99 ms +- 1.88` |
| `olap_graph_introspection_rollup` | `4.44 ms +- 1.78` | `3.15 ms +- 1.71` | `3.71 ms +- 0.30` | `4.62 ms +- 1.76` | `4.42 ms +- 0.12` | `3.96 ms +- 0.41` | `5.20 ms +- 1.00` | `4.38 ms +- 0.25` | `11.03 ms +- 2.64` | `8.65 ms +- 0.61` | `57.44 ms +- 0.33` |
| `olap_optional_type1_aggregate` | `3.15 ms +- 1.58` | `1.55 ms +- 0.18` | `2.81 ms +- 0.16` | `3.45 ms +- 1.37` | `2.31 ms +- 0.14` | `2.03 ms +- 0.26` | `1.25 ms +- 0.16` | `1.14 ms +- 0.05` | `1.53 ms +- 0.23` | `1.06 ms +- 0.20` | `4.66 ms +- 2.41` |
| `olap_relationship_function_projection` | `6.88 ms +- 2.90` | `3.29 ms +- 1.71` | `4.04 ms +- 0.13` | `5.14 ms +- 2.26` | `5.55 ms +- 0.42` | `4.52 ms +- 0.41` | `4.23 ms +- 0.86` | `3.65 ms +- 0.37` | `7.04 ms +- 0.36` | `6.49 ms +- 0.38` | `55.30 ms +- 2.17` |
| `olap_three_type_path_count` | `5.76 ms +- 2.34` | `2.91 ms +- 0.61` | `3.36 ms +- 0.33` | `4.18 ms +- 1.88` | `4.67 ms +- 0.33` | `3.82 ms +- 0.37` | `3.52 ms +- 0.69` | `3.25 ms +- 0.02` | `0.29 ms +- 0.03` | `0.17 ms +- 0.03` | `75.16 ms +- 2.99` |
| `olap_type1_active_leaderboard` | `3.11 ms +- 1.23` | `1.58 ms +- 0.31` | `3.03 ms +- 0.21` | `3.02 ms +- 0.25` | `2.25 ms +- 0.22` | `2.28 ms +- 0.19` | `2.52 ms +- 0.45` | `1.98 ms +- 0.19` | `3.72 ms +- 1.15` | `2.57 ms +- 0.75` | `1.13 ms +- 0.02` |
| `olap_type1_age_rollup` | `3.54 ms +- 1.54` | `1.78 ms +- 0.30` | `2.79 ms +- 0.20` | `3.38 ms +- 1.23` | `2.54 ms +- 0.19` | `2.32 ms +- 0.22` | `1.43 ms +- 0.21` | `1.38 ms +- 0.16` | `1.47 ms +- 0.41` | `1.34 ms +- 0.77` | `4.91 ms +- 2.36` |
| `olap_type2_score_distribution` | `4.33 ms +- 1.87` | `1.96 ms +- 0.37` | `2.98 ms +- 0.35` | `3.62 ms +- 1.48` | `2.75 ms +- 0.30` | `2.55 ms +- 0.09` | `1.36 ms +- 0.24` | `1.09 ms +- 0.21` | `0.98 ms +- 0.20` | `0.92 ms +- 0.34` | `10.30 ms +- 3.50` |
| `olap_variable_length_grouped_max_rollup` | `5.03 ms +- 3.19` | `4.19 ms +- 2.26` | `5.57 ms +- 0.27` | `7.46 ms +- 2.73` | `4.32 ms +- 0.13` | `4.35 ms +- 0.42` | `0.51 ms +- 0.15` | `0.58 ms +- 0.09` | `0.06 ms +- 0.01` | `0.56 ms +- 0.07` | `8.34 ms +- 3.60` |
| `olap_variable_length_grouped_rollup` | `10.25 ms +- 4.63` | `7.30 ms +- 3.03` | `5.88 ms +- 0.24` | `7.14 ms +- 2.97` | `8.22 ms +- 0.57` | `7.35 ms +- 0.94` | `14.43 ms +- 3.81` | `12.70 ms +- 0.47` | `20.34 ms +- 0.80` | `19.63 ms +- 0.91` | `26.76 ms +- 2.91` |
| `olap_variable_length_reachability` | `4.31 ms +- 1.74` | `2.94 ms +- 0.57` | `4.90 ms +- 0.57` | `6.14 ms +- 1.86` | `4.04 ms +- 0.37` | `3.73 ms +- 0.33` | `0.73 ms +- 0.12` | `0.73 ms +- 0.06` | `0.21 ms +- 0.10` | `0.82 ms +- 0.40` | `3.55 ms +- 1.06` |
| `olap_with_scalar_rebinding` | `5.77 ms +- 2.57` | `2.95 ms +- 1.47` | `3.42 ms +- 0.20` | `4.14 ms +- 1.85` | `3.03 ms +- 0.26` | `3.28 ms +- 0.43` | `1.59 ms +- 0.23` | `1.48 ms +- 0.13` | `1.14 ms +- 0.12` | `1.13 ms +- 0.06` | `8.10 ms +- 5.01` |
| `olap_with_size_predicate_projection` | `5.22 ms +- 2.77` | `2.56 ms +- 1.17` | `3.34 ms +- 0.21` | `3.95 ms +- 1.75` | `3.25 ms +- 0.01` | `3.03 ms +- 0.46` | `2.19 ms +- 0.70` | `1.73 ms +- 0.18` | `2.11 ms +- 0.08` | `1.74 ms +- 0.21` | `1.02 ms +- 0.22` |
| `olap_with_where_lower_projection` | `4.13 ms +- 2.43` | `2.46 ms +- 1.04` | `2.99 ms +- 0.36` | `3.58 ms +- 1.89` | `3.08 ms +- 0.08` | `2.93 ms +- 0.27` | `2.15 ms +- 0.58` | `1.94 ms +- 0.20` | `2.43 ms +- 0.18` | `1.98 ms +- 0.09` | `1.13 ms +- 0.26` |
