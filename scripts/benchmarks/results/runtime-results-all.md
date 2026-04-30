# Runtime Result Summary

- Scanned JSON files: 40
- Completed runs: 34
- Skipped non-completed runs: 6
- Grouped configurations: 13
- Grouped benchmark campaigns: 2

### Medium runtime dataset

The current medium runtime matrix used the `medium` preset with `5000` OLTP iterations / `100` OLTP warmup and `100` OLAP iterations / `10` OLAP warmup.

That corresponds to roughly:

- `600,000` total nodes
- `6,223,200` total edges
- `3` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.50.4`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.2`

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
| SQLite Indexed | `13.71 ms +- 3.78` | `8.89 ms +- 0.59` | `65866.70 ms +- 136.48` | `9628.32 ms +- 1.14` | `3134.97 ms +- 18.39` | `1.07 ms +- 0.01` | `1.33 ms +- 0.02` | `1.70 ms +- 0.03` |
| Neo4j Indexed | `61.38 ms +- 0.00` | `454.47 ms +- 0.00` | `390863.75 ms +- 0.00` | `15825.63 ms +- 0.00` | `0.00 ms +- 0.00` | `0.21 ms +- 0.00` | `0.33 ms +- 0.00` | `0.49 ms +- 0.00` |
| ArcadeDB Indexed | `307.73 ms +- 0.00` | `390.16 ms +- 0.00` | `158322.12 ms +- 0.00` | `21112.36 ms +- 0.00` | `0.00 ms +- 0.00` | `6.00 ms +- 0.00` | `6.82 ms +- 0.00` | `8.13 ms +- 0.00` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `11.72 ms +- 0.31` | `6.74 ms +- 0.15` | `68031.36 ms +- 826.85` | `9744.63 ms +- 59.13` | `3168.74 ms +- 10.19` | `826.98 ms +- 6.93` | `876.72 ms +- 6.82` | `905.42 ms +- 8.07` |
| Neo4j Indexed | `61.38 ms +- 0.00` | `454.47 ms +- 0.00` | `390863.75 ms +- 0.00` | `15825.63 ms +- 0.00` | `0.00 ms +- 0.00` | `550.12 ms +- 0.00` | `590.01 ms +- 0.00` | `616.87 ms +- 0.00` |
| ArcadeDB Indexed | `2.26 ms +- 0.00` | `53.66 ms +- 0.00` | `157348.53 ms +- 0.00` | `19135.38 ms +- 0.00` | `6632.25 ms +- 0.00` | `332.61 ms +- 0.00` | `368.49 ms +- 0.00` | `388.81 ms +- 0.00` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `102.07 MiB +- 0.07` | `102.10 MiB +- 0.07` | `105.90 MiB +- 0.06` | `105.90 MiB +- 0.06` | `105.90 MiB +- 0.06` | `486.43 MiB +- 6.90` |
| Neo4j Indexed | `656.12 MiB +- 0.00` | `720.91 MiB +- 0.00` | `2702.03 MiB +- 0.00` | `4076.24 MiB +- 0.00` | `0.00 MiB +- 0.00` | `2597.76 MiB +- 0.00` |
| ArcadeDB Indexed | `166.98 MiB +- 0.00` | `293.25 MiB +- 0.00` | `4610.92 MiB +- 0.00` | `4709.53 MiB +- 0.00` | `4709.53 MiB +- 0.00` | `4704.25 MiB +- 0.00` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `483.43 MiB +- 6.90` | `483.46 MiB +- 6.90` | `395.86 MiB +- 0.06` | `395.86 MiB +- 0.06` | `395.37 MiB +- 0.64` | `308.37 MiB +- 1.45` |
| Neo4j Indexed | `656.12 MiB +- 0.00` | `720.91 MiB +- 0.00` | `2702.03 MiB +- 0.00` | `4076.24 MiB +- 0.00` | `0.00 MiB +- 0.00` | `3056.92 MiB +- 0.00` |
| ArcadeDB Indexed | `4704.69 MiB +- 0.00` | `4714.34 MiB +- 0.00` | `4873.57 MiB +- 0.00` | `4996.99 MiB +- 0.00` | `5929.62 MiB +- 0.00` | `5925.08 MiB +- 0.00` |

#### Medium runtime suite comparison

This rolls the medium-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.07 ms +- 0.01` | `1.33 ms +- 0.02` | `1.70 ms +- 0.03` |
| `olap/sqlite_indexed` | `826.98 ms +- 6.93` | `876.72 ms +- 6.82` | `905.42 ms +- 8.07` |
| `oltp/neo4j_indexed` | `0.21 ms +- 0.00` | `0.33 ms +- 0.00` | `0.49 ms +- 0.00` |
| `olap/neo4j_indexed` | `550.12 ms +- 0.00` | `590.01 ms +- 0.00` | `616.87 ms +- 0.00` |
| `oltp/arcadedb_embedded_indexed` | `6.00 ms +- 0.00` | `6.82 ms +- 0.00` | `8.13 ms +- 0.00` |
| `olap/arcadedb_embedded_indexed` | `332.61 ms +- 0.00` | `368.49 ms +- 0.00` | `388.81 ms +- 0.00` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher
  execution timings, so they are not strictly comparable to the
  compile-plus-execute SQL
  paths.
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

#### Medium runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.48 ms +- 0.00` | `0.24 ms +- 0.00` | `0.12 ms +- 0.00` |
| `oltp_create_type1_node` | `0.73 ms +- 0.00` | `0.19 ms +- 0.00` | `0.05 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.18 ms +- 0.01` | `0.21 ms +- 0.00` | `0.03 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.76 ms +- 0.00` | `0.18 ms +- 0.00` | `0.05 ms +- 0.00` |
| `oltp_delete_type1_node` | `1.01 ms +- 0.00` | `0.24 ms +- 0.00` | `0.09 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `1.69 ms +- 0.01` | `0.25 ms +- 0.00` | `0.12 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `0.91 ms +- 0.00` | `0.18 ms +- 0.00` | `41.18 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `0.91 ms +- 0.01` | `0.19 ms +- 0.00` | `42.11 ms +- 0.00` |
| `oltp_program_create_and_link` | `1.76 ms +- 0.03` | `0.20 ms +- 0.00` | `0.12 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.00 ms +- 0.01` | `0.24 ms +- 0.00` | `0.03 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.95 ms +- 0.01` | `0.28 ms +- 0.00` | `0.02 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.84 ms +- 0.00` | `0.19 ms +- 0.00` | `0.01 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.05 ms +- 0.01` | `0.19 ms +- 0.00` | `0.05 ms +- 0.00` |
| `oltp_update_type1_score` | `0.70 ms +- 0.00` | `0.20 ms +- 0.00` | `0.04 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.87 ms +- 0.02` | `0.36 ms +- 0.00` | `0.23 ms +- 0.00` |
| `oltp_create_type1_node` | `0.87 ms +- 0.01` | `0.29 ms +- 0.00` | `0.09 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.38 ms +- 0.01` | `0.31 ms +- 0.00` | `0.10 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.95 ms +- 0.03` | `0.28 ms +- 0.00` | `0.16 ms +- 0.00` |
| `oltp_delete_type1_node` | `1.55 ms +- 0.00` | `0.41 ms +- 0.00` | `0.19 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `2.05 ms +- 0.08` | `0.35 ms +- 0.00` | `0.19 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `1.06 ms +- 0.04` | `0.27 ms +- 0.00` | `47.18 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `1.06 ms +- 0.05` | `0.28 ms +- 0.00` | `46.73 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.17 ms +- 0.03` | `0.32 ms +- 0.00` | `0.22 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.18 ms +- 0.03` | `0.39 ms +- 0.00` | `0.10 ms +- 0.00` |
| `oltp_type1_point_lookup` | `1.15 ms +- 0.01` | `0.45 ms +- 0.00` | `0.07 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.98 ms +- 0.00` | `0.27 ms +- 0.00` | `0.03 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.47 ms +- 0.02` | `0.29 ms +- 0.00` | `0.13 ms +- 0.00` |
| `oltp_update_type1_score` | `0.86 ms +- 0.00` | `0.29 ms +- 0.00` | `0.09 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.29 ms +- 0.01` | `0.53 ms +- 0.00` | `0.32 ms +- 0.00` |
| `oltp_create_type1_node` | `1.09 ms +- 0.05` | `0.42 ms +- 0.00` | `0.15 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.72 ms +- 0.02` | `0.46 ms +- 0.00` | `0.15 ms +- 0.00` |
| `oltp_delete_type1_edge` | `1.25 ms +- 0.08` | `0.41 ms +- 0.00` | `0.24 ms +- 0.00` |
| `oltp_delete_type1_node` | `2.28 ms +- 0.02` | `0.67 ms +- 0.00` | `0.28 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `2.58 ms +- 0.09` | `0.54 ms +- 0.00` | `0.42 ms +- 0.00` |
| `oltp_optional_missing_type1_lookup` | `1.36 ms +- 0.08` | `0.42 ms +- 0.00` | `57.29 ms +- 0.00` |
| `oltp_optional_type1_lookup` | `1.32 ms +- 0.06` | `0.39 ms +- 0.00` | `54.02 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.62 ms +- 0.14` | `0.50 ms +- 0.00` | `0.32 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.46 ms +- 0.10` | `0.58 ms +- 0.00` | `0.16 ms +- 0.00` |
| `oltp_type1_point_lookup` | `1.43 ms +- 0.07` | `0.70 ms +- 0.00` | `0.10 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `1.19 ms +- 0.01` | `0.40 ms +- 0.00` | `0.05 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `2.00 ms +- 0.02` | `0.42 ms +- 0.00` | `0.18 ms +- 0.00` |
| `oltp_update_type1_score` | `1.17 ms +- 0.01` | `0.46 ms +- 0.00` | `0.13 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1573.38 ms +- 52.98` | `613.37 ms +- 0.00` | `965.54 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `3311.66 ms +- 6.52` | `1529.84 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `3353.66 ms +- 10.87` | `3846.73 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `1.59 ms +- 0.07` | `564.39 ms +- 0.00` | `1521.04 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `31.05 ms +- 0.31` | `64.90 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `804.51 ms +- 6.79` | `642.02 ms +- 0.00` | `1007.94 ms +- 0.00` |
| `olap_three_type_path_count` | `2825.96 ms +- 26.60` | `591.54 ms +- 0.00` | `6.69 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `1.21 ms +- 0.03` | `70.75 ms +- 0.00` | `84.79 ms +- 0.00` |
| `olap_type1_age_rollup` | `158.90 ms +- 0.74` | `77.33 ms +- 0.00` | `63.86 ms +- 0.00` |
| `olap_type2_score_distribution` | `17.80 ms +- 1.80` | `73.54 ms +- 0.00` | `66.36 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `2.32 ms +- 0.09` | `0.39 ms +- 0.00` | `0.26 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - |
| `olap_variable_length_reachability` | `2.62 ms +- 0.14` | `0.85 ms +- 0.00` | `0.85 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `156.07 ms +- 3.74` | `89.48 ms +- 0.00` | `74.44 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `28.08 ms +- 0.49` | `44.03 ms +- 0.00` | `106.62 ms +- 0.00` |
| `olap_with_where_lower_projection` | `135.85 ms +- 2.85` | `42.61 ms +- 0.00` | `92.87 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1748.15 ms +- 48.39` | `626.32 ms +- 0.00` | `1001.16 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `3403.42 ms +- 10.59` | `1673.42 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `3534.24 ms +- 9.83` | `4011.27 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `1.75 ms +- 0.12` | `648.47 ms +- 0.00` | `1575.66 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `32.74 ms +- 1.04` | `69.86 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `828.76 ms +- 10.77` | `674.33 ms +- 0.00` | `1071.12 ms +- 0.00` |
| `olap_three_type_path_count` | `3061.45 ms +- 25.16` | `693.09 ms +- 0.00` | `7.86 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `1.42 ms +- 0.02` | `76.47 ms +- 0.00` | `155.07 ms +- 0.00` |
| `olap_type1_age_rollup` | `169.51 ms +- 0.75` | `84.37 ms +- 0.00` | `113.39 ms +- 0.00` |
| `olap_type2_score_distribution` | `18.59 ms +- 2.17` | `83.40 ms +- 0.00` | `99.89 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `3.06 ms +- 0.10` | `0.69 ms +- 0.00` | `0.54 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - |
| `olap_variable_length_reachability` | `3.11 ms +- 0.08` | `1.15 ms +- 0.00` | `1.49 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `167.54 ms +- 3.98` | `111.67 ms +- 0.00` | `110.95 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `30.38 ms +- 0.33` | `48.48 ms +- 0.00` | `157.57 ms +- 0.00` |
| `olap_with_where_lower_projection` | `146.69 ms +- 3.75` | `47.15 ms +- 0.00` | `127.19 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | Neo4j Indexed | ArcadeDB Indexed |
| --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1989.59 ms +- 72.77` | `637.64 ms +- 0.00` | `1007.22 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `3444.74 ms +- 32.89` | `1700.16 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | `3608.14 ms +- 16.74` | `4070.93 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `1.83 ms +- 0.15` | `699.21 ms +- 0.00` | `1587.97 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `34.09 ms +- 1.04` | `72.76 ms +- 0.00` | - |
| `olap_relationship_function_projection` | `844.17 ms +- 15.63` | `687.89 ms +- 0.00` | `1156.09 ms +- 0.00` |
| `olap_three_type_path_count` | `3100.00 ms +- 19.29` | `860.39 ms +- 0.00` | `11.72 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `1.75 ms +- 0.33` | `79.69 ms +- 0.00` | `176.36 ms +- 0.00` |
| `olap_type1_age_rollup` | `176.37 ms +- 1.33` | `85.77 ms +- 0.00` | `137.26 ms +- 0.00` |
| `olap_type2_score_distribution` | `18.91 ms +- 2.15` | `107.86 ms +- 0.00` | `116.96 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `3.43 ms +- 0.25` | `0.94 ms +- 0.00` | `0.99 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - |
| `olap_variable_length_reachability` | `3.19 ms +- 0.05` | `1.36 ms +- 0.00` | `1.72 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `172.19 ms +- 9.60` | `147.93 ms +- 0.00` | `131.99 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `31.81 ms +- 0.16` | `50.28 ms +- 0.00` | `202.99 ms +- 0.00` |
| `olap_with_where_lower_projection` | `151.12 ms +- 7.26` | `50.25 ms +- 0.00` | `134.43 ms +- 0.00` |

### Small runtime dataset

The current small runtime matrix used the `small` preset with `10000` OLTP iterations / `200` OLTP warmup and `500` OLAP iterations / `20` OLAP warmup.

That corresponds to roughly:

- `4,000` total nodes
- `12,000` total edges
- `10` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.50.4`
- `DuckDB`: `1.5.2`
- `PostgreSQL`: `16.11 (Debian 16.11-1.pgdg13+1)`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.2`
- `LadybugDB`: `0.15.3`

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
| SQLite Indexed | `9.49 ms +- 1.48` | `5.82 ms +- 2.04` | `143.83 ms +- 30.39` | `17.69 ms +- 2.87` | `7.49 ms +- 0.95` | `1.10 ms +- 0.13` | `1.51 ms +- 0.33` | `1.99 ms +- 0.57` |
| SQLite Unindexed | `11.14 ms +- 3.21` | `6.50 ms +- 1.16` | `127.58 ms +- 25.54` | `1.72 ms +- 0.28` | `0.72 ms +- 0.34` | `1.45 ms +- 0.15` | `2.15 ms +- 0.40` | `2.72 ms +- 0.63` |
| DuckDB Unindexed | `11.82 ms +- 2.32` | `99.83 ms +- 7.95` | `187.42 ms +- 22.36` | `0.00 ms +- 0.00` | `0.20 ms +- 0.13` | `2.30 ms +- 0.56` | `3.44 ms +- 1.02` | `4.22 ms +- 1.26` |
| PostgreSQL Indexed | `4.10 ms +- 0.28` | `333.90 ms +- 38.32` | `233.20 ms +- 13.51` | `268.44 ms +- 106.86` | `82.75 ms +- 6.37` | `1.35 ms +- 0.05` | `1.90 ms +- 0.12` | `2.25 ms +- 0.17` |
| PostgreSQL Unindexed | `4.51 ms +- 0.32` | `314.75 ms +- 11.99` | `222.72 ms +- 7.83` | `12.84 ms +- 4.75` | `84.38 ms +- 3.46` | `1.78 ms +- 0.51` | `2.71 ms +- 1.10` | `3.31 ms +- 1.54` |
| Neo4j Indexed | `84.65 ms +- 31.68` | `382.36 ms +- 80.80` | `2269.46 ms +- 589.30` | `893.76 ms +- 36.04` | `0.00 ms +- 0.00` | `0.24 ms +- 0.04` | `0.41 ms +- 0.13` | `0.66 ms +- 0.31` |
| Neo4j Unindexed | `94.85 ms +- 47.64` | `376.70 ms +- 28.49` | `2236.84 ms +- 576.04` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.36 ms +- 0.01` | `0.55 ms +- 0.06` | `0.76 ms +- 0.16` |
| ArcadeDB Indexed | `357.34 ms +- 55.75` | `402.31 ms +- 72.95` | `674.79 ms +- 81.06` | `344.88 ms +- 90.70` | `0.00 ms +- 0.00` | `0.10 ms +- 0.00` | `0.23 ms +- 0.03` | `0.36 ms +- 0.07` |
| ArcadeDB Unindexed | `402.77 ms +- 156.03` | `425.25 ms +- 196.39` | `691.43 ms +- 182.76` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.46 ms +- 0.02` | `0.79 ms +- 0.08` | `1.09 ms +- 0.16` |
| LadybugDB Unindexed | `89.36 ms +- 23.28` | `48.36 ms +- 7.99` | `746.89 ms +- 46.84` | `0.00 ms +- 0.00` | `22.52 ms +- 2.02` | `1.49 ms +- 0.13` | `2.70 ms +- 0.44` | `3.67 ms +- 0.62` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `10.11 ms +- 1.93` | `6.62 ms +- 2.10` | `120.39 ms +- 25.09` | `17.24 ms +- 2.97` | `7.69 ms +- 1.07` | `3.89 ms +- 0.89` | `4.78 ms +- 1.52` | `5.41 ms +- 1.89` |
| SQLite Unindexed | `9.04 ms +- 1.39` | `5.25 ms +- 1.61` | `102.05 ms +- 14.03` | `1.92 ms +- 0.86` | `0.58 ms +- 0.20` | `4.18 ms +- 1.34` | `5.26 ms +- 2.12` | `5.90 ms +- 2.29` |
| DuckDB Unindexed | `9.48 ms +- 0.88` | `90.47 ms +- 1.33` | `173.06 ms +- 24.54` | `0.00 ms +- 0.00` | `0.14 ms +- 0.05` | `3.53 ms +- 1.32` | `4.45 ms +- 1.91` | `5.18 ms +- 2.36` |
| PostgreSQL Indexed | `3.98 ms +- 0.27` | `333.03 ms +- 23.12` | `200.37 ms +- 33.20` | `233.39 ms +- 29.89` | `83.10 ms +- 4.97` | `2.98 ms +- 0.03` | `3.74 ms +- 0.06` | `4.19 ms +- 0.10` |
| PostgreSQL Unindexed | `5.43 ms +- 1.98` | `336.65 ms +- 20.76` | `208.58 ms +- 57.37` | `11.43 ms +- 8.07` | `95.13 ms +- 18.63` | `3.54 ms +- 1.10` | `4.57 ms +- 1.63` | `5.14 ms +- 1.84` |
| Neo4j Indexed | `84.65 ms +- 31.68` | `382.36 ms +- 80.80` | `2269.46 ms +- 589.30` | `893.76 ms +- 36.04` | `0.00 ms +- 0.00` | `2.73 ms +- 0.57` | `3.83 ms +- 1.08` | `4.47 ms +- 1.20` |
| Neo4j Unindexed | `94.85 ms +- 47.64` | `376.70 ms +- 28.49` | `2236.84 ms +- 576.04` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.35 ms +- 0.12` | `2.96 ms +- 0.28` | `3.52 ms +- 0.25` |
| ArcadeDB Indexed | `2.38 ms +- 0.43` | `38.07 ms +- 6.98` | `404.88 ms +- 12.15` | `117.17 ms +- 2.86` | `301.96 ms +- 11.41` | `3.12 ms +- 0.10` | `5.44 ms +- 0.26` | `9.70 ms +- 0.27` |
| ArcadeDB Unindexed | `2.74 ms +- 0.85` | `65.93 ms +- 19.53` | `503.36 ms +- 146.82` | `0.00 ms +- 0.00` | `420.92 ms +- 107.23` | `4.47 ms +- 1.14` | `8.45 ms +- 2.82` | `18.33 ms +- 9.14` |
| LadybugDB Unindexed | `95.38 ms +- 40.22` | `29.25 ms +- 3.02` | `753.49 ms +- 123.95` | `0.00 ms +- 0.00` | `23.19 ms +- 3.35` | `5.85 ms +- 0.63` | `7.64 ms +- 0.92` | `8.91 ms +- 0.89` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `93.33 MiB +- 0.02` | `93.42 MiB +- 0.02` | `95.86 MiB +- 0.02` | `95.91 MiB +- 0.02` | `95.91 MiB +- 0.02` | `606.27 MiB +- 55.50` |
| SQLite Unindexed | `93.35 MiB +- 0.04` | `93.44 MiB +- 0.04` | `95.88 MiB +- 0.04` | `95.93 MiB +- 0.04` | `95.93 MiB +- 0.04` | `657.56 MiB +- 55.77` |
| DuckDB Unindexed | `94.46 MiB +- 0.90` | `99.36 MiB +- 0.81` | `151.64 MiB +- 0.70` | `151.64 MiB +- 0.70` | `151.64 MiB +- 0.70` | `662.50 MiB +- 35.15` |
| PostgreSQL Indexed | `118.90 MiB +- 1.28` | `120.58 MiB +- 0.72` | `129.11 MiB +- 0.58` | `129.29 MiB +- 0.43` | `131.86 MiB +- 0.82` | `797.98 MiB +- 11.47` |
| PostgreSQL Unindexed | `119.41 MiB +- 0.46` | `121.06 MiB +- 0.09` | `129.71 MiB +- 0.12` | `129.82 MiB +- 0.16` | `131.28 MiB +- 0.06` | `776.09 MiB +- 7.54` |
| Neo4j Indexed | `683.98 MiB +- 19.00` | `714.36 MiB +- 3.51` | `1473.75 MiB +- 539.90` | `908.56 MiB +- 41.78` | `0.00 MiB +- 0.00` | `1213.29 MiB +- 24.18` |
| Neo4j Unindexed | `673.35 MiB +- 16.77` | `723.40 MiB +- 21.52` | `1468.40 MiB +- 499.69` | `1467.26 MiB +- 500.78` | `0.00 MiB +- 0.00` | `1181.79 MiB +- 19.13` |
| ArcadeDB Indexed | `145.64 MiB +- 2.78` | `217.55 MiB +- 20.66` | `285.80 MiB +- 11.66` | `360.06 MiB +- 13.14` | `360.06 MiB +- 13.14` | `361.41 MiB +- 12.04` |
| ArcadeDB Unindexed | `145.99 MiB +- 0.43` | `211.04 MiB +- 2.97` | `283.78 MiB +- 2.66` | `283.78 MiB +- 2.66` | `283.78 MiB +- 2.66` | `284.21 MiB +- 2.50` |
| LadybugDB Unindexed | `265.87 MiB +- 0.00` | `292.66 MiB +- 0.00` | `517.23 MiB +- 12.98` | `517.23 MiB +- 12.98` | `517.55 MiB +- 12.98` | `568.69 MiB +- 15.34` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `603.97 MiB +- 54.17` | `603.68 MiB +- 53.93` | `602.04 MiB +- 54.26` | `602.04 MiB +- 54.26` | `602.04 MiB +- 54.26` | `338.35 MiB +- 1.71` |
| SQLite Unindexed | `656.90 MiB +- 55.87` | `656.93 MiB +- 55.87` | `653.97 MiB +- 53.93` | `653.97 MiB +- 53.93` | `653.97 MiB +- 53.93` | `346.05 MiB +- 12.95` |
| DuckDB Unindexed | `614.30 MiB +- 35.85` | `609.53 MiB +- 34.84` | `636.47 MiB +- 34.94` | `636.47 MiB +- 34.94` | `636.47 MiB +- 34.94` | `625.11 MiB +- 19.34` |
| PostgreSQL Indexed | `793.43 MiB +- 10.83` | `793.80 MiB +- 10.25` | `793.32 MiB +- 8.83` | `793.62 MiB +- 9.32` | `795.48 MiB +- 9.17` | `429.82 MiB +- 16.90` |
| PostgreSQL Unindexed | `772.60 MiB +- 7.54` | `772.67 MiB +- 8.02` | `772.88 MiB +- 8.06` | `772.63 MiB +- 8.06` | `774.99 MiB +- 7.82` | `410.71 MiB +- 13.94` |
| Neo4j Indexed | `683.98 MiB +- 19.00` | `714.36 MiB +- 3.51` | `1473.75 MiB +- 539.90` | `908.56 MiB +- 41.78` | `0.00 MiB +- 0.00` | `1212.54 MiB +- 31.11` |
| Neo4j Unindexed | `673.35 MiB +- 16.77` | `723.40 MiB +- 21.52` | `1468.40 MiB +- 499.69` | `1467.26 MiB +- 500.78` | `0.00 MiB +- 0.00` | `1474.65 MiB +- 514.21` |
| ArcadeDB Indexed | `362.06 MiB +- 12.81` | `369.13 MiB +- 21.94` | `390.97 MiB +- 16.13` | `404.99 MiB +- 39.87` | `458.76 MiB +- 42.68` | `460.05 MiB +- 41.61` |
| ArcadeDB Unindexed | `284.35 MiB +- 2.50` | `286.04 MiB +- 2.91` | `287.54 MiB +- 4.56` | `287.54 MiB +- 4.56` | `338.72 MiB +- 3.36` | `341.91 MiB +- 5.09` |
| LadybugDB Unindexed | `518.61 MiB +- 24.78` | `522.84 MiB +- 22.16` | `549.52 MiB +- 27.35` | `549.52 MiB +- 27.35` | `549.52 MiB +- 27.35` | `613.19 MiB +- 19.63` |

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.10 ms +- 0.13` | `1.51 ms +- 0.33` | `1.99 ms +- 0.57` |
| `olap/sqlite_indexed` | `3.89 ms +- 0.89` | `4.78 ms +- 1.52` | `5.41 ms +- 1.89` |
| `oltp/sqlite_unindexed` | `1.45 ms +- 0.15` | `2.15 ms +- 0.40` | `2.72 ms +- 0.63` |
| `olap/sqlite_unindexed` | `4.18 ms +- 1.34` | `5.26 ms +- 2.12` | `5.90 ms +- 2.29` |
| `oltp/duckdb` | `2.30 ms +- 0.56` | `3.44 ms +- 1.02` | `4.22 ms +- 1.26` |
| `olap/duckdb` | `3.53 ms +- 1.32` | `4.45 ms +- 1.91` | `5.18 ms +- 2.36` |
| `oltp/postgresql_indexed` | `1.35 ms +- 0.05` | `1.90 ms +- 0.12` | `2.25 ms +- 0.17` |
| `olap/postgresql_indexed` | `2.98 ms +- 0.03` | `3.74 ms +- 0.06` | `4.19 ms +- 0.10` |
| `oltp/postgresql_unindexed` | `1.78 ms +- 0.51` | `2.71 ms +- 1.10` | `3.31 ms +- 1.54` |
| `olap/postgresql_unindexed` | `3.54 ms +- 1.10` | `4.57 ms +- 1.63` | `5.14 ms +- 1.84` |
| `oltp/neo4j_indexed` | `0.24 ms +- 0.04` | `0.41 ms +- 0.13` | `0.66 ms +- 0.31` |
| `olap/neo4j_indexed` | `2.73 ms +- 0.57` | `3.83 ms +- 1.08` | `4.47 ms +- 1.20` |
| `oltp/neo4j_unindexed` | `0.36 ms +- 0.01` | `0.55 ms +- 0.06` | `0.76 ms +- 0.16` |
| `olap/neo4j_unindexed` | `2.35 ms +- 0.12` | `2.96 ms +- 0.28` | `3.52 ms +- 0.25` |
| `oltp/arcadedb_embedded_indexed` | `0.10 ms +- 0.00` | `0.23 ms +- 0.03` | `0.36 ms +- 0.07` |
| `olap/arcadedb_embedded_indexed` | `3.12 ms +- 0.10` | `5.44 ms +- 0.26` | `9.70 ms +- 0.27` |
| `oltp/arcadedb_embedded_unindexed` | `0.46 ms +- 0.02` | `0.79 ms +- 0.08` | `1.09 ms +- 0.16` |
| `olap/arcadedb_embedded_unindexed` | `4.47 ms +- 1.14` | `8.45 ms +- 2.82` | `18.33 ms +- 9.14` |
| `oltp/ladybug_unindexed` | `1.49 ms +- 0.13` | `2.70 ms +- 0.44` | `3.67 ms +- 0.62` |
| `olap/ladybug_unindexed` | `5.85 ms +- 0.63` | `7.64 ms +- 0.92` | `8.91 ms +- 0.89` |

Read these tables with a couple of caveats:

- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime
  timings through CypherGlot.
- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher
  execution timings, so they are not strictly comparable to the
  compile-plus-execute SQL
  paths.
- DuckDB is a single-path run here, and that path is intentionally
  `unindexed`.
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

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.45 ms +- 0.14` | `1.70 ms +- 0.30` | `3.22 ms +- 1.45` | `1.73 ms +- 0.13` | `2.52 ms +- 1.23` | `0.28 ms +- 0.04` | `0.52 ms +- 0.02` | `0.07 ms +- 0.01` | `0.93 ms +- 0.06` | `0.94 ms +- 0.12` |
| `oltp_create_type1_node` | `0.76 ms +- 0.08` | `0.74 ms +- 0.06` | `1.43 ms +- 0.22` | `0.91 ms +- 0.02` | `0.97 ms +- 0.12` | `0.22 ms +- 0.03` | `0.21 ms +- 0.01` | `0.03 ms +- 0.00` | `0.04 ms +- 0.01` | `0.33 ms +- 0.08` |
| `oltp_cross_type_lookup` | `1.33 ms +- 0.16` | `1.75 ms +- 0.45` | `2.76 ms +- 0.58` | `1.67 ms +- 0.10` | `2.29 ms +- 0.91` | `0.26 ms +- 0.04` | `0.38 ms +- 0.02` | `0.03 ms +- 0.00` | `0.44 ms +- 0.02` | `2.51 ms +- 0.06` |
| `oltp_delete_type1_edge` | `0.79 ms +- 0.10` | `1.22 ms +- 0.17` | `2.94 ms +- 1.68` | `1.19 ms +- 0.06` | `1.73 ms +- 0.75` | `0.23 ms +- 0.05` | `0.34 ms +- 0.01` | `0.07 ms +- 0.01` | `0.48 ms +- 0.03` | `2.03 ms +- 0.37` |
| `oltp_delete_type1_node` | `0.63 ms +- 0.07` | `2.47 ms +- 0.67` | `1.02 ms +- 0.38` | `0.72 ms +- 0.02` | `1.37 ms +- 0.41` | `0.23 ms +- 0.04` | `0.34 ms +- 0.02` | `0.07 ms +- 0.00` | `0.50 ms +- 0.04` | `0.52 ms +- 0.06` |
| `oltp_merge_cross_type_edge` | `1.66 ms +- 0.14` | `2.50 ms +- 0.49` | `5.10 ms +- 2.76` | `2.12 ms +- 0.11` | `3.02 ms +- 0.96` | `0.28 ms +- 0.05` | `0.53 ms +- 0.01` | `0.08 ms +- 0.01` | `0.96 ms +- 0.07` | `6.40 ms +- 1.03` |
| `oltp_optional_missing_type1_lookup` | `1.02 ms +- 0.14` | `1.02 ms +- 0.04` | `1.62 ms +- 0.18` | `1.24 ms +- 0.06` | `1.37 ms +- 0.25` | `0.22 ms +- 0.03` | `0.35 ms +- 0.02` | `0.44 ms +- 0.02` | `0.43 ms +- 0.00` | `0.79 ms +- 0.16` |
| `oltp_optional_type1_lookup` | `0.96 ms +- 0.05` | `1.03 ms +- 0.04` | `1.57 ms +- 0.13` | `1.21 ms +- 0.04` | `1.44 ms +- 0.35` | `0.22 ms +- 0.03` | `0.36 ms +- 0.02` | `0.44 ms +- 0.02` | `0.43 ms +- 0.01` | `0.66 ms +- 0.05` |
| `oltp_program_create_and_link` | `1.91 ms +- 0.35` | `2.14 ms +- 0.50` | `4.30 ms +- 2.31` | `2.13 ms +- 0.05` | `2.38 ms +- 0.17` | `0.24 ms +- 0.05` | `0.35 ms +- 0.01` | `0.07 ms +- 0.00` | `0.49 ms +- 0.03` | `0.78 ms +- 0.10` |
| `oltp_type1_neighbors` | `1.13 ms +- 0.16` | `1.57 ms +- 0.51` | `2.46 ms +- 0.57` | `1.42 ms +- 0.10` | `1.94 ms +- 0.56` | `0.26 ms +- 0.03` | `0.40 ms +- 0.04` | `0.03 ms +- 0.00` | `0.42 ms +- 0.02` | `2.54 ms +- 0.19` |
| `oltp_type1_point_lookup` | `1.07 ms +- 0.10` | `1.11 ms +- 0.14` | `1.35 ms +- 0.21` | `1.19 ms +- 0.08` | `1.37 ms +- 0.20` | `0.30 ms +- 0.04` | `0.44 ms +- 0.03` | `0.02 ms +- 0.00` | `0.42 ms +- 0.02` | `0.38 ms +- 0.03` |
| `oltp_unwind_literal_top2` | `0.93 ms +- 0.13` | `0.88 ms +- 0.04` | `1.22 ms +- 0.04` | `1.13 ms +- 0.01` | `1.19 ms +- 0.14` | `0.22 ms +- 0.02` | `0.21 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.55 ms +- 0.06` |
| `oltp_update_cross_type_edge_rank` | `1.01 ms +- 0.15` | `1.33 ms +- 0.24` | `2.13 ms +- 0.44` | `1.30 ms +- 0.00` | `2.13 ms +- 0.90` | `0.22 ms +- 0.02` | `0.33 ms +- 0.01` | `0.05 ms +- 0.02` | `0.47 ms +- 0.04` | `2.03 ms +- 0.40` |
| `oltp_update_type1_score` | `0.78 ms +- 0.10` | `0.81 ms +- 0.08` | `1.15 ms +- 0.02` | `0.95 ms +- 0.01` | `1.15 ms +- 0.24` | `0.22 ms +- 0.03` | `0.33 ms +- 0.01` | `0.05 ms +- 0.01` | `0.42 ms +- 0.01` | `0.47 ms +- 0.06` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.84 ms +- 0.39` | `2.33 ms +- 0.64` | `4.54 ms +- 2.69` | `2.40 ms +- 0.20` | `3.91 ms +- 2.53` | `0.44 ms +- 0.12` | `0.77 ms +- 0.06` | `0.23 ms +- 0.01` | `1.37 ms +- 0.22` | `1.97 ms +- 0.79` |
| `oltp_create_type1_node` | `1.04 ms +- 0.23` | `1.02 ms +- 0.17` | `2.18 ms +- 0.83` | `1.29 ms +- 0.05` | `1.46 ms +- 0.34` | `0.35 ms +- 0.08` | `0.31 ms +- 0.02` | `0.07 ms +- 0.01` | `0.07 ms +- 0.01` | `0.85 ms +- 0.45` |
| `oltp_cross_type_lookup` | `2.01 ms +- 0.57` | `2.47 ms +- 0.93` | `4.68 ms +- 2.33` | `2.37 ms +- 0.18` | `3.29 ms +- 1.63` | `0.43 ms +- 0.17` | `0.55 ms +- 0.06` | `0.08 ms +- 0.01` | `0.71 ms +- 0.04` | `3.82 ms +- 0.25` |
| `oltp_delete_type1_edge` | `1.00 ms +- 0.30` | `2.04 ms +- 0.66` | `4.53 ms +- 3.72` | `1.74 ms +- 0.13` | `3.00 ms +- 1.88` | `0.44 ms +- 0.27` | `0.48 ms +- 0.01` | `0.14 ms +- 0.01` | `0.98 ms +- 0.17` | `3.56 ms +- 0.93` |
| `oltp_delete_type1_node` | `0.79 ms +- 0.19` | `4.24 ms +- 1.63` | `1.89 ms +- 1.69` | `1.06 ms +- 0.07` | `2.27 ms +- 1.01` | `0.42 ms +- 0.19` | `0.50 ms +- 0.03` | `0.18 ms +- 0.01` | `1.00 ms +- 0.20` | `1.08 ms +- 0.48` |
| `oltp_merge_cross_type_edge` | `2.02 ms +- 0.29` | `4.03 ms +- 1.25` | `6.74 ms +- 4.27` | `2.85 ms +- 0.19` | `4.62 ms +- 2.18` | `0.46 ms +- 0.17` | `0.77 ms +- 0.04` | `0.25 ms +- 0.04` | `1.39 ms +- 0.23` | `9.52 ms +- 2.07` |
| `oltp_optional_missing_type1_lookup` | `1.39 ms +- 0.25` | `1.32 ms +- 0.12` | `2.42 ms +- 0.53` | `1.81 ms +- 0.19` | `2.07 ms +- 0.56` | `0.34 ms +- 0.07` | `0.49 ms +- 0.04` | `0.77 ms +- 0.09` | `0.75 ms +- 0.01` | `2.03 ms +- 0.91` |
| `oltp_optional_type1_lookup` | `1.33 ms +- 0.17` | `1.32 ms +- 0.14` | `2.35 ms +- 0.48` | `1.76 ms +- 0.12` | `2.23 ms +- 0.85` | `0.38 ms +- 0.15` | `0.54 ms +- 0.07` | `0.89 ms +- 0.23` | `0.74 ms +- 0.03` | `1.77 ms +- 0.74` |
| `oltp_program_create_and_link` | `2.38 ms +- 0.75` | `2.98 ms +- 0.90` | `6.15 ms +- 4.25` | `2.78 ms +- 0.10` | `3.22 ms +- 0.28` | `0.42 ms +- 0.18` | `0.50 ms +- 0.01` | `0.22 ms +- 0.01` | `0.96 ms +- 0.16` | `1.76 ms +- 0.69` |
| `oltp_type1_neighbors` | `1.61 ms +- 0.43` | `2.49 ms +- 1.32` | `4.22 ms +- 2.07` | `1.99 ms +- 0.23` | `2.83 ms +- 1.01` | `0.39 ms +- 0.08` | `0.60 ms +- 0.12` | `0.08 ms +- 0.01` | `0.73 ms +- 0.09` | `4.47 ms +- 1.31` |
| `oltp_type1_point_lookup` | `1.91 ms +- 0.73` | `1.59 ms +- 0.45` | `1.96 ms +- 0.48` | `1.68 ms +- 0.24` | `2.03 ms +- 0.42` | `0.63 ms +- 0.20` | `0.92 ms +- 0.33` | `0.05 ms +- 0.01` | `0.74 ms +- 0.14` | `0.88 ms +- 0.67` |
| `oltp_unwind_literal_top2` | `1.40 ms +- 0.51` | `1.15 ms +- 0.12` | `1.66 ms +- 0.22` | `1.61 ms +- 0.03` | `1.74 ms +- 0.30` | `0.32 ms +- 0.03` | `0.30 ms +- 0.01` | `0.03 ms +- 0.00` | `0.03 ms +- 0.00` | `1.47 ms +- 0.67` |
| `oltp_update_cross_type_edge_rank` | `1.31 ms +- 0.38` | `2.01 ms +- 0.64` | `3.30 ms +- 1.58` | `1.83 ms +- 0.02` | `3.21 ms +- 1.55` | `0.32 ms +- 0.04` | `0.48 ms +- 0.00` | `0.11 ms +- 0.02` | `0.84 ms +- 0.10` | `3.44 ms +- 0.77` |
| `oltp_update_type1_score` | `1.16 ms +- 0.35` | `1.13 ms +- 0.21` | `1.60 ms +- 0.11` | `1.40 ms +- 0.05` | `2.04 ms +- 0.94` | `0.34 ms +- 0.06` | `0.48 ms +- 0.02` | `0.09 ms +- 0.00` | `0.69 ms +- 0.01` | `1.15 ms +- 0.54` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.21 ms +- 0.48` | `2.79 ms +- 0.85` | `5.43 ms +- 3.33` | `2.76 ms +- 0.21` | `4.71 ms +- 3.22` | `0.71 ms +- 0.34` | `1.04 ms +- 0.18` | `0.38 ms +- 0.03` | `2.15 ms +- 0.75` | `2.80 ms +- 0.94` |
| `oltp_create_type1_node` | `1.41 ms +- 0.46` | `1.35 ms +- 0.35` | `2.79 ms +- 1.14` | `1.58 ms +- 0.08` | `1.92 ms +- 0.66` | `0.54 ms +- 0.16` | `0.44 ms +- 0.05` | `0.14 ms +- 0.01` | `0.14 ms +- 0.01` | `1.33 ms +- 0.76` |
| `oltp_cross_type_lookup` | `2.75 ms +- 1.10` | `3.10 ms +- 1.29` | `5.76 ms +- 3.04` | `2.88 ms +- 0.26` | `3.92 ms +- 2.12` | `0.68 ms +- 0.41` | `0.76 ms +- 0.19` | `0.16 ms +- 0.04` | `0.90 ms +- 0.05` | `4.74 ms +- 0.20` |
| `oltp_delete_type1_edge` | `1.36 ms +- 0.55` | `2.86 ms +- 1.19` | `5.57 ms +- 4.91` | `2.06 ms +- 0.14` | `3.79 ms +- 2.56` | `0.77 ms +- 0.62` | `0.64 ms +- 0.02` | `0.25 ms +- 0.06` | `1.41 ms +- 0.31` | `4.72 ms +- 1.26` |
| `oltp_delete_type1_node` | `1.13 ms +- 0.45` | `5.41 ms +- 2.22` | `2.55 ms +- 2.52` | `1.35 ms +- 0.06` | `2.82 ms +- 1.42` | `0.70 ms +- 0.44` | `0.62 ms +- 0.06` | `0.28 ms +- 0.02` | `1.37 ms +- 0.39` | `2.27 ms +- 1.30` |
| `oltp_merge_cross_type_edge` | `2.39 ms +- 0.32` | `4.83 ms +- 1.46` | `7.78 ms +- 4.96` | `3.22 ms +- 0.25` | `5.43 ms +- 2.77` | `0.75 ms +- 0.39` | `0.98 ms +- 0.11` | `0.42 ms +- 0.07` | `2.15 ms +- 0.32` | `11.23 ms +- 2.21` |
| `oltp_optional_missing_type1_lookup` | `1.80 ms +- 0.42` | `1.56 ms +- 0.10` | `3.06 ms +- 0.79` | `2.14 ms +- 0.26` | `2.57 ms +- 0.92` | `0.48 ms +- 0.11` | `0.62 ms +- 0.07` | `0.99 ms +- 0.11` | `0.94 ms +- 0.02` | `2.81 ms +- 1.34` |
| `oltp_optional_type1_lookup` | `1.70 ms +- 0.31` | `1.61 ms +- 0.20` | `3.05 ms +- 0.76` | `2.16 ms +- 0.25` | `2.67 ms +- 1.11` | `0.65 ms +- 0.39` | `0.77 ms +- 0.23` | `1.34 ms +- 0.69` | `0.92 ms +- 0.01` | `2.63 ms +- 1.14` |
| `oltp_program_create_and_link` | `2.79 ms +- 0.92` | `3.52 ms +- 1.12` | `7.31 ms +- 5.27` | `3.15 ms +- 0.12` | `3.70 ms +- 0.30` | `0.78 ms +- 0.52` | `0.63 ms +- 0.04` | `0.35 ms +- 0.02` | `1.34 ms +- 0.31` | `2.85 ms +- 1.19` |
| `oltp_type1_neighbors` | `2.09 ms +- 0.63` | `3.26 ms +- 1.90` | `5.12 ms +- 2.52` | `2.38 ms +- 0.39` | `3.44 ms +- 1.44` | `0.63 ms +- 0.26` | `0.90 ms +- 0.38` | `0.15 ms +- 0.02` | `0.94 ms +- 0.19` | `5.90 ms +- 1.66` |
| `oltp_type1_point_lookup` | `2.69 ms +- 1.34` | `2.21 ms +- 1.00` | `2.56 ms +- 0.79` | `2.05 ms +- 0.45` | `2.57 ms +- 0.80` | `1.08 ms +- 0.50` | `1.70 ms +- 0.99` | `0.11 ms +- 0.03` | `0.99 ms +- 0.27` | `1.73 ms +- 1.59` |
| `oltp_unwind_literal_top2` | `2.09 ms +- 1.28` | `1.48 ms +- 0.22` | `2.03 ms +- 0.29` | `1.90 ms +- 0.01` | `2.18 ms +- 0.54` | `0.45 ms +- 0.07` | `0.41 ms +- 0.03` | `0.04 ms +- 0.00` | `0.04 ms +- 0.00` | `2.08 ms +- 0.99` |
| `oltp_update_cross_type_edge_rank` | `1.72 ms +- 0.63` | `2.56 ms +- 0.93` | `4.06 ms +- 2.00` | `2.17 ms +- 0.03` | `3.85 ms +- 1.95` | `0.50 ms +- 0.13` | `0.59 ms +- 0.02` | `0.20 ms +- 0.03` | `1.17 ms +- 0.24` | `4.48 ms +- 0.86` |
| `oltp_update_type1_score` | `1.67 ms +- 0.74` | `1.53 ms +- 0.43` | `1.98 ms +- 0.12` | `1.71 ms +- 0.08` | `2.83 ms +- 1.77` | `0.52 ms +- 0.10` | `0.62 ms +- 0.04` | `0.17 ms +- 0.00` | `0.88 ms +- 0.03` | `1.80 ms +- 0.89` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `3.91 ms +- 1.27` | `3.24 ms +- 1.25` | `2.71 ms +- 0.55` | `2.61 ms +- 0.04` | `3.47 ms +- 1.51` | `2.75 ms +- 0.68` | `2.22 ms +- 0.11` | `4.35 ms +- 0.08` | `7.66 ms +- 3.04` | `5.65 ms +- 1.69` |
| `olap_fixed_length_path_projection` | `7.00 ms +- 1.36` | `8.25 ms +- 2.96` | `5.05 ms +- 2.30` | `5.41 ms +- 0.20` | `6.73 ms +- 3.43` | `5.15 ms +- 0.23` | `4.78 ms +- 0.18` | `4.04 ms +- 0.18` | `5.34 ms +- 0.69` | `18.68 ms +- 1.08` |
| `olap_fixed_length_path_with_rebinding` | `7.18 ms +- 1.92` | `9.05 ms +- 3.35` | `5.87 ms +- 2.75` | `5.37 ms +- 0.02` | `6.59 ms +- 2.61` | `6.56 ms +- 1.71` | `5.24 ms +- 0.23` | `4.96 ms +- 0.15` | `6.44 ms +- 2.12` | `8.85 ms +- 1.54` |
| `olap_graph_introspection_rollup` | `2.08 ms +- 0.50` | `3.24 ms +- 1.70` | `3.47 ms +- 1.75` | `2.87 ms +- 0.06` | `3.05 ms +- 0.47` | `3.90 ms +- 1.41` | `2.98 ms +- 0.24` | `6.60 ms +- 0.50` | `8.97 ms +- 3.72` | `3.34 ms +- 0.90` |
| `olap_optional_type1_aggregate` | `1.52 ms +- 0.31` | `1.50 ms +- 0.21` | `2.06 ms +- 0.36` | `1.43 ms +- 0.01` | `1.72 ms +- 0.43` | `0.78 ms +- 0.09` | `0.73 ms +- 0.03` | `0.81 ms +- 0.00` | `1.24 ms +- 0.37` | `2.55 ms +- 1.73` |
| `olap_relationship_function_projection` | `5.42 ms +- 1.80` | `3.37 ms +- 0.84` | `3.80 ms +- 1.75` | `3.79 ms +- 0.13` | `3.82 ms +- 0.34` | `3.54 ms +- 1.48` | `2.63 ms +- 0.23` | `4.73 ms +- 0.17` | `7.90 ms +- 2.76` | `3.72 ms +- 0.84` |
| `olap_three_type_path_count` | `5.16 ms +- 0.91` | `6.12 ms +- 2.33` | `2.42 ms +- 0.40` | `3.15 ms +- 0.05` | `4.20 ms +- 2.28` | `2.20 ms +- 0.27` | `1.93 ms +- 0.08` | `0.09 ms +- 0.02` | `0.10 ms +- 0.03` | `4.49 ms +- 0.37` |
| `olap_type1_active_leaderboard` | `1.33 ms +- 0.26` | `1.53 ms +- 0.39` | `2.04 ms +- 0.27` | `1.46 ms +- 0.04` | `1.67 ms +- 0.36` | `1.45 ms +- 0.18` | `1.36 ms +- 0.05` | `1.55 ms +- 0.02` | `2.05 ms +- 0.41` | `0.68 ms +- 0.16` |
| `olap_type1_age_rollup` | `1.95 ms +- 0.47` | `1.69 ms +- 0.22` | `2.02 ms +- 0.38` | `1.56 ms +- 0.01` | `1.89 ms +- 0.51` | `0.89 ms +- 0.12` | `0.84 ms +- 0.03` | `0.90 ms +- 0.00` | `1.16 ms +- 0.17` | `2.27 ms +- 1.42` |
| `olap_type2_score_distribution` | `1.66 ms +- 0.32` | `2.10 ms +- 0.79` | `2.51 ms +- 0.90` | `1.68 ms +- 0.06` | `2.18 ms +- 0.82` | `0.79 ms +- 0.06` | `0.76 ms +- 0.05` | `0.90 ms +- 0.05` | `1.17 ms +- 0.23` | `3.10 ms +- 1.37` |
| `olap_variable_length_grouped_max_rollup` | `2.55 ms +- 0.91` | `4.37 ms +- 1.26` | `6.25 ms +- 2.82` | `3.05 ms +- 0.16` | `4.54 ms +- 2.10` | `0.26 ms +- 0.02` | `0.36 ms +- 0.02` | `0.14 ms +- 0.02` | `0.82 ms +- 0.10` | `6.68 ms +- 1.82` |
| `olap_variable_length_grouped_rollup` | `11.99 ms +- 2.08` | `11.18 ms +- 2.08` | `5.83 ms +- 2.58` | `6.28 ms +- 0.12` | `6.28 ms +- 0.57` | `11.36 ms +- 2.69` | `10.03 ms +- 0.35` | `16.93 ms +- 0.70` | `23.23 ms +- 5.46` | `26.11 ms +- 2.53` |
| `olap_variable_length_reachability` | `1.97 ms +- 0.55` | `3.96 ms +- 1.16` | `3.79 ms +- 0.78` | `2.58 ms +- 0.04` | `3.60 ms +- 1.41` | `0.41 ms +- 0.03` | `0.51 ms +- 0.05` | `0.15 ms +- 0.02` | `0.95 ms +- 0.14` | `2.51 ms +- 1.02` |
| `olap_with_scalar_rebinding` | `3.07 ms +- 1.00` | `2.70 ms +- 1.24` | `3.34 ms +- 1.43` | `2.17 ms +- 0.09` | `2.34 ms +- 0.25` | `0.93 ms +- 0.09` | `0.89 ms +- 0.09` | `1.01 ms +- 0.16` | `1.23 ms +- 0.05` | `3.17 ms +- 1.13` |
| `olap_with_size_predicate_projection` | `2.57 ms +- 0.86` | `2.31 ms +- 0.94` | `2.76 ms +- 1.24` | `2.27 ms +- 0.05` | `2.39 ms +- 0.37` | `1.35 ms +- 0.19` | `1.19 ms +- 0.14` | `1.30 ms +- 0.02` | `1.54 ms +- 0.29` | `0.83 ms +- 0.09` |
| `olap_with_where_lower_projection` | `2.88 ms +- 1.04` | `2.24 ms +- 0.81` | `2.61 ms +- 1.15` | `1.99 ms +- 0.06` | `2.13 ms +- 0.19` | `1.34 ms +- 0.22` | `1.20 ms +- 0.10` | `1.38 ms +- 0.09` | `1.71 ms +- 0.78` | `0.93 ms +- 0.44` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.60 ms +- 1.83` | `4.38 ms +- 2.09` | `3.38 ms +- 0.87` | `3.39 ms +- 0.15` | `4.44 ms +- 2.07` | `4.26 ms +- 1.57` | `2.81 ms +- 0.27` | `7.34 ms +- 0.45` | `12.75 ms +- 4.31` | `7.84 ms +- 2.04` |
| `olap_fixed_length_path_projection` | `7.98 ms +- 1.88` | `10.18 ms +- 4.71` | `6.26 ms +- 3.05` | `6.47 ms +- 0.17` | `8.61 ms +- 4.90` | `7.00 ms +- 0.58` | `5.91 ms +- 0.75` | `9.20 ms +- 0.45` | `12.84 ms +- 2.17` | `22.50 ms +- 1.84` |
| `olap_fixed_length_path_with_rebinding` | `8.76 ms +- 3.16` | `10.82 ms +- 4.62` | `7.39 ms +- 4.19` | `6.46 ms +- 0.09` | `8.14 ms +- 3.32` | `9.13 ms +- 2.36` | `6.46 ms +- 0.43` | `12.94 ms +- 0.41` | `15.42 ms +- 6.56` | `11.43 ms +- 1.87` |
| `olap_graph_introspection_rollup` | `2.71 ms +- 0.99` | `4.21 ms +- 2.48` | `4.18 ms +- 2.45` | `3.58 ms +- 0.15` | `3.84 ms +- 0.72` | `5.31 ms +- 1.99` | `3.79 ms +- 0.69` | `12.81 ms +- 0.68` | `17.79 ms +- 7.40` | `5.10 ms +- 1.31` |
| `olap_optional_type1_aggregate` | `1.79 ms +- 0.57` | `2.16 ms +- 0.40` | `2.76 ms +- 0.83` | `1.93 ms +- 0.07` | `2.46 ms +- 0.88` | `1.42 ms +- 0.44` | `1.04 ms +- 0.09` | `1.29 ms +- 0.13` | `2.15 ms +- 0.75` | `4.16 ms +- 2.10` |
| `olap_relationship_function_projection` | `6.68 ms +- 2.82` | `4.55 ms +- 1.84` | `4.73 ms +- 2.50` | `4.75 ms +- 0.06` | `4.86 ms +- 0.99` | `5.03 ms +- 2.18` | `3.28 ms +- 0.45` | `9.69 ms +- 0.63` | `16.08 ms +- 5.76` | `5.06 ms +- 1.40` |
| `olap_three_type_path_count` | `5.87 ms +- 1.28` | `7.37 ms +- 2.97` | `2.93 ms +- 0.58` | `3.96 ms +- 0.21` | `5.44 ms +- 3.02` | `3.31 ms +- 0.89` | `2.83 ms +- 0.47` | `0.22 ms +- 0.02` | `0.24 ms +- 0.05` | `6.15 ms +- 1.00` |
| `olap_type1_active_leaderboard` | `1.71 ms +- 0.58` | `2.21 ms +- 0.81` | `2.59 ms +- 0.59` | `2.01 ms +- 0.08` | `2.47 ms +- 0.82` | `2.39 ms +- 0.95` | `1.92 ms +- 0.22` | `1.99 ms +- 0.12` | `3.79 ms +- 1.58` | `1.33 ms +- 0.59` |
| `olap_type1_age_rollup` | `2.40 ms +- 0.76` | `2.35 ms +- 0.61` | `2.67 ms +- 0.72` | `2.12 ms +- 0.06` | `2.67 ms +- 0.92` | `1.48 ms +- 0.51` | `1.15 ms +- 0.16` | `1.24 ms +- 0.12` | `1.91 ms +- 0.39` | `4.10 ms +- 1.43` |
| `olap_type2_score_distribution` | `2.00 ms +- 0.55` | `2.86 ms +- 1.33` | `3.49 ms +- 1.51` | `2.22 ms +- 0.20` | `3.02 ms +- 1.19` | `1.13 ms +- 0.17` | `1.02 ms +- 0.19` | `1.33 ms +- 0.07` | `1.93 ms +- 0.62` | `4.38 ms +- 1.78` |
| `olap_variable_length_grouped_max_rollup` | `4.24 ms +- 1.99` | `5.29 ms +- 1.67` | `7.61 ms +- 3.90` | `3.84 ms +- 0.28` | `5.80 ms +- 3.04` | `0.35 ms +- 0.02` | `0.51 ms +- 0.02` | `0.32 ms +- 0.06` | `1.68 ms +- 0.56` | `8.91 ms +- 1.82` |
| `olap_variable_length_grouped_rollup` | `14.31 ms +- 3.68` | `12.88 ms +- 3.23` | `7.29 ms +- 3.48` | `7.44 ms +- 0.09` | `7.83 ms +- 1.11` | `14.77 ms +- 5.25` | `11.67 ms +- 0.41` | `20.32 ms +- 2.01` | `36.05 ms +- 14.62` | `29.90 ms +- 3.02` |
| `olap_variable_length_reachability` | `2.67 ms +- 1.17` | `4.79 ms +- 1.62` | `4.91 ms +- 1.41` | `3.31 ms +- 0.10` | `4.65 ms +- 1.87` | `0.57 ms +- 0.05` | `0.70 ms +- 0.07` | `0.35 ms +- 0.04` | `1.66 ms +- 0.36` | `3.97 ms +- 1.23` |
| `olap_with_scalar_rebinding` | `3.90 ms +- 1.59` | `3.58 ms +- 1.93` | `4.17 ms +- 1.97` | `2.73 ms +- 0.26` | `2.96 ms +- 0.44` | `1.38 ms +- 0.24` | `1.18 ms +- 0.29` | `1.63 ms +- 0.15` | `1.80 ms +- 0.25` | `4.62 ms +- 1.66` |
| `olap_with_size_predicate_projection` | `3.36 ms +- 1.69` | `3.42 ms +- 2.06` | `3.50 ms +- 1.72` | `2.99 ms +- 0.15` | `3.07 ms +- 0.59` | `1.85 ms +- 0.28` | `1.55 ms +- 0.23` | `3.31 ms +- 0.47` | `4.54 ms +- 1.47` | `1.32 ms +- 0.73` |
| `olap_with_where_lower_projection` | `3.46 ms +- 1.54` | `3.04 ms +- 1.64` | `3.35 ms +- 1.70` | `2.61 ms +- 0.08` | `2.80 ms +- 0.29` | `1.84 ms +- 0.42` | `1.58 ms +- 0.26` | `3.03 ms +- 0.28` | `4.51 ms +- 1.63` | `1.52 ms +- 0.87` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `5.64 ms +- 2.97` | `4.99 ms +- 2.27` | `4.07 ms +- 1.23` | `3.81 ms +- 0.26` | `4.93 ms +- 2.26` | `5.23 ms +- 1.80` | `3.69 ms +- 0.24` | `9.48 ms +- 2.21` | `34.74 ms +- 17.59` | `8.98 ms +- 2.21` |
| `olap_fixed_length_path_projection` | `8.93 ms +- 2.60` | `11.19 ms +- 5.02` | `7.21 ms +- 3.59` | `7.40 ms +- 0.37` | `9.58 ms +- 5.26` | `8.26 ms +- 0.57` | `6.73 ms +- 0.94` | `17.27 ms +- 2.97` | `32.56 ms +- 15.84` | `24.65 ms +- 1.64` |
| `olap_fixed_length_path_with_rebinding` | `9.66 ms +- 3.69` | `12.02 ms +- 4.54` | `8.43 ms +- 5.39` | `6.96 ms +- 0.14` | `9.10 ms +- 3.76` | `10.15 ms +- 2.46` | `8.09 ms +- 0.16` | `18.43 ms +- 3.45` | `29.92 ms +- 20.62` | `12.39 ms +- 1.97` |
| `olap_graph_introspection_rollup` | `3.09 ms +- 1.16` | `4.82 ms +- 2.85` | `5.10 ms +- 3.48` | `3.90 ms +- 0.28` | `4.20 ms +- 0.74` | `6.04 ms +- 2.16` | `4.35 ms +- 0.86` | `16.83 ms +- 2.22` | `30.90 ms +- 18.69` | `6.41 ms +- 1.76` |
| `olap_optional_type1_aggregate` | `1.94 ms +- 0.66` | `2.47 ms +- 0.68` | `3.27 ms +- 1.01` | `2.18 ms +- 0.11` | `3.05 ms +- 1.45` | `1.89 ms +- 0.92` | `1.28 ms +- 0.03` | `9.90 ms +- 0.99` | `3.03 ms +- 0.97` | `4.94 ms +- 2.16` |
| `olap_relationship_function_projection` | `7.22 ms +- 3.30` | `5.04 ms +- 2.08` | `5.23 ms +- 2.80` | `5.24 ms +- 0.16` | `5.26 ms +- 0.90` | `5.65 ms +- 2.32` | `3.91 ms +- 0.37` | `14.01 ms +- 1.69` | `31.22 ms +- 14.39` | `5.94 ms +- 1.77` |
| `olap_three_type_path_count` | `6.49 ms +- 1.48` | `8.02 ms +- 3.11` | `3.74 ms +- 1.47` | `4.46 ms +- 0.34` | `6.06 ms +- 3.27` | `4.02 ms +- 0.83` | `3.43 ms +- 0.20` | `0.35 ms +- 0.05` | `0.52 ms +- 0.21` | `7.30 ms +- 0.77` |
| `olap_type1_active_leaderboard` | `2.02 ms +- 0.95` | `2.56 ms +- 0.95` | `3.05 ms +- 0.89` | `2.23 ms +- 0.11` | `3.13 ms +- 1.19` | `3.08 ms +- 1.29` | `2.14 ms +- 0.23` | `8.86 ms +- 5.60` | `4.57 ms +- 1.97` | `1.87 ms +- 0.93` |
| `olap_type1_age_rollup` | `2.76 ms +- 0.85` | `2.87 ms +- 0.92` | `3.13 ms +- 0.91` | `2.42 ms +- 0.08` | `3.02 ms +- 1.09` | `2.01 ms +- 1.00` | `1.50 ms +- 0.02` | `1.74 ms +- 0.33` | `15.10 ms +- 22.06` | `5.32 ms +- 1.60` |
| `olap_type2_score_distribution` | `2.21 ms +- 0.72` | `3.36 ms +- 1.64` | `4.45 ms +- 2.20` | `2.51 ms +- 0.08` | `3.56 ms +- 1.49` | `1.28 ms +- 0.11` | `1.23 ms +- 0.23` | `5.70 ms +- 4.31` | `5.78 ms +- 4.13` | `5.32 ms +- 2.31` |
| `olap_variable_length_grouped_max_rollup` | `5.28 ms +- 2.69` | `5.96 ms +- 1.79` | `8.41 ms +- 3.94` | `4.31 ms +- 0.39` | `6.45 ms +- 3.29` | `0.45 ms +- 0.03` | `0.64 ms +- 0.08` | `0.57 ms +- 0.11` | `26.52 ms +- 14.97` | `11.55 ms +- 0.90` |
| `olap_variable_length_grouped_rollup` | `15.46 ms +- 4.33` | `14.30 ms +- 3.05` | `8.08 ms +- 3.30` | `8.55 ms +- 0.25` | `8.76 ms +- 1.04` | `16.57 ms +- 4.96` | `13.39 ms +- 0.29` | `29.18 ms +- 0.61` | `47.86 ms +- 18.33` | `32.35 ms +- 1.86` |
| `olap_variable_length_reachability` | `3.24 ms +- 1.56` | `5.24 ms +- 1.61` | `5.96 ms +- 2.15` | `3.57 ms +- 0.13` | `5.19 ms +- 2.23` | `0.70 ms +- 0.03` | `0.91 ms +- 0.13` | `0.52 ms +- 0.04` | `13.03 ms +- 18.84` | `5.73 ms +- 1.63` |
| `olap_with_scalar_rebinding` | `4.21 ms +- 1.71` | `4.03 ms +- 2.15` | `5.09 ms +- 2.79` | `3.08 ms +- 0.19` | `3.30 ms +- 0.39` | `1.74 ms +- 0.42` | `1.39 ms +- 0.34` | `8.44 ms +- 5.26` | `6.31 ms +- 6.13` | `5.64 ms +- 1.84` |
| `olap_with_size_predicate_projection` | `4.74 ms +- 3.74` | `4.03 ms +- 2.61` | `3.87 ms +- 1.84` | `3.55 ms +- 0.31` | `3.43 ms +- 0.75` | `2.25 ms +- 0.40` | `1.79 ms +- 0.25` | `4.23 ms +- 0.56` | `5.52 ms +- 1.82` | `2.09 ms +- 1.52` |
| `olap_with_where_lower_projection` | `3.76 ms +- 1.71` | `3.53 ms +- 1.64` | `3.76 ms +- 1.81` | `2.94 ms +- 0.05` | `3.19 ms +- 0.41` | `2.18 ms +- 0.59` | `1.83 ms +- 0.33` | `9.75 ms +- 4.86` | `5.68 ms +- 1.88` | `2.15 ms +- 0.80` |
