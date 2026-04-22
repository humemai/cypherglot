# Runtime Result Summary

- Scanned JSON files: 30
- Completed runs: 30
- Skipped non-completed runs: 0
- Grouped configurations: 10
- Grouped benchmark campaigns: 1

### Small runtime dataset

The current small runtime matrix used the `small` preset with `20000` OLTP iterations / `200` OLTP warmup and `500` OLAP iterations / `20` OLAP warmup.

That corresponds to roughly:

- `4,000` total nodes
- `12,000` total edges
- `10` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.50.4`
- `DuckDB`: `1.5.1`
- `PostgreSQL`: `16.11 (Debian 16.11-1.pgdg13+1)`
- `Neo4j`: `5.26.24`
- `ArcadeDB Embedded`: `26.4.1.dev3`
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
| SQLite Indexed | `9.87 ms +- 1.48` | `5.84 ms +- 1.35` | `97.15 ms +- 3.90` | `13.82 ms +- 0.73` | `6.43 ms +- 0.19` | `0.96 ms +- 0.00` | `1.02 ms +- 0.01` | `1.18 ms +- 0.04` |
| SQLite Unindexed | `9.44 ms +- 1.16` | `5.64 ms +- 1.35` | `118.44 ms +- 34.08` | `1.53 ms +- 0.46` | `0.65 ms +- 0.36` | `1.20 ms +- 0.05` | `1.32 ms +- 0.10` | `1.57 ms +- 0.21` |
| DuckDB Unindexed | `9.51 ms +- 0.63` | `90.42 ms +- 1.09` | `157.03 ms +- 6.26` | `0.00 ms +- 0.00` | `0.13 ms +- 0.03` | `1.80 ms +- 0.14` | `2.40 ms +- 0.45` | `2.84 ms +- 0.58` |
| PostgreSQL Indexed | `4.45 ms +- 0.87` | `327.85 ms +- 19.66` | `234.95 ms +- 22.61` | `239.53 ms +- 44.79` | `93.77 ms +- 8.49` | `1.20 ms +- 0.04` | `1.56 ms +- 0.16` | `2.07 ms +- 0.31` |
| PostgreSQL Unindexed | `3.79 ms +- 0.28` | `312.85 ms +- 1.08` | `222.61 ms +- 6.86` | `12.53 ms +- 4.93` | `78.02 ms +- 6.01` | `1.42 ms +- 0.14` | `1.93 ms +- 0.46` | `2.40 ms +- 0.68` |
| Neo4j Indexed | `75.56 ms +- 19.36` | `371.43 ms +- 97.81` | `2103.81 ms +- 786.58` | `847.07 ms +- 90.43` | `0.00 ms +- 0.00` | `0.19 ms +- 0.03` | `0.27 ms +- 0.04` | `0.34 ms +- 0.07` |
| Neo4j Unindexed | `58.38 ms +- 2.07` | `345.54 ms +- 13.68` | `1725.81 ms +- 104.69` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.31 ms +- 0.01` | `0.43 ms +- 0.02` | `0.53 ms +- 0.03` |
| ArcadeDB Indexed | `281.57 ms +- 15.42` | `294.24 ms +- 2.84` | `530.67 ms +- 13.90` | `247.73 ms +- 48.00` | `0.00 ms +- 0.00` | `0.02 ms +- 0.00` | `0.04 ms +- 0.00` | `0.05 ms +- 0.00` |
| ArcadeDB Unindexed | `284.51 ms +- 6.49` | `273.62 ms +- 25.36` | `540.28 ms +- 14.35` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.40 ms +- 0.01` | `0.43 ms +- 0.01` | `0.51 ms +- 0.05` |
| LadybugDB Unindexed | `69.93 ms +- 2.79` | `41.37 ms +- 0.52` | `674.05 ms +- 6.66` | `0.00 ms +- 0.00` | `18.92 ms +- 0.88` | `0.89 ms +- 0.03` | `1.33 ms +- 0.02` | `1.63 ms +- 0.03` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `8.87 ms +- 1.38` | `5.06 ms +- 1.77` | `91.20 ms +- 4.17` | `14.16 ms +- 1.02` | `6.50 ms +- 0.18` | `3.17 ms +- 0.03` | `3.35 ms +- 0.03` | `3.54 ms +- 0.01` |
| SQLite Unindexed | `9.67 ms +- 1.30` | `5.26 ms +- 1.45` | `89.43 ms +- 2.93` | `1.26 ms +- 0.12` | `0.43 ms +- 0.04` | `3.27 ms +- 0.05` | `3.51 ms +- 0.07` | `3.71 ms +- 0.11` |
| DuckDB Unindexed | `8.59 ms +- 0.47` | `88.73 ms +- 1.13` | `152.97 ms +- 11.01` | `0.00 ms +- 0.00` | `0.10 ms +- 0.01` | `3.28 ms +- 1.36` | `3.99 ms +- 1.93` | `4.54 ms +- 2.08` |
| PostgreSQL Indexed | `3.79 ms +- 0.05` | `324.78 ms +- 3.96` | `182.93 ms +- 8.86` | `204.22 ms +- 6.57` | `79.25 ms +- 4.84` | `2.70 ms +- 0.17` | `3.21 ms +- 0.43` | `3.73 ms +- 0.69` |
| PostgreSQL Unindexed | `3.82 ms +- 0.21` | `328.72 ms +- 6.99` | `171.85 ms +- 15.05` | `9.59 ms +- 4.80` | `90.65 ms +- 14.90` | `2.42 ms +- 0.03` | `2.69 ms +- 0.01` | `2.94 ms +- 0.05` |
| Neo4j Indexed | `75.56 ms +- 19.36` | `371.43 ms +- 97.81` | `2103.81 ms +- 786.58` | `847.07 ms +- 90.43` | `0.00 ms +- 0.00` | `2.56 ms +- 0.49` | `3.08 ms +- 0.87` | `3.68 ms +- 1.01` |
| Neo4j Unindexed | `58.38 ms +- 2.07` | `345.54 ms +- 13.68` | `1725.81 ms +- 104.69` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.47 ms +- 0.07` | `2.87 ms +- 0.17` | `3.39 ms +- 0.16` |
| ArcadeDB Indexed | `1.47 ms +- 0.14` | `21.45 ms +- 0.60` | `292.43 ms +- 5.97` | `59.07 ms +- 3.92` | `281.89 ms +- 1.47` | `3.55 ms +- 0.50` | `4.20 ms +- 0.67` | `4.89 ms +- 0.84` |
| ArcadeDB Unindexed | `1.56 ms +- 0.13` | `23.77 ms +- 2.46` | `281.16 ms +- 10.10` | `0.00 ms +- 0.00` | `281.26 ms +- 3.90` | `3.35 ms +- 0.07` | `3.79 ms +- 0.14` | `4.24 ms +- 0.15` |
| LadybugDB Unindexed | `66.31 ms +- 1.99` | `25.34 ms +- 1.49` | `670.34 ms +- 11.24` | `0.00 ms +- 0.00` | `18.84 ms +- 0.42` | `5.86 ms +- 0.12` | `6.76 ms +- 0.16` | `7.35 ms +- 0.25` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `92.61 MiB +- 0.02` | `92.70 MiB +- 0.02` | `95.12 MiB +- 0.02` | `95.17 MiB +- 0.02` | `95.17 MiB +- 0.02` | `636.12 MiB +- 16.77` |
| SQLite Unindexed | `92.66 MiB +- 0.01` | `92.76 MiB +- 0.01` | `95.18 MiB +- 0.01` | `95.22 MiB +- 0.01` | `95.22 MiB +- 0.01` | `595.67 MiB +- 7.05` |
| DuckDB Unindexed | `94.38 MiB +- 0.04` | `99.61 MiB +- 0.35` | `154.53 MiB +- 4.61` | `154.53 MiB +- 4.61` | `154.53 MiB +- 4.61` | `814.96 MiB +- 29.93` |
| PostgreSQL Indexed | `119.71 MiB +- 0.72` | `121.47 MiB +- 0.48` | `129.39 MiB +- 0.34` | `129.51 MiB +- 0.44` | `132.12 MiB +- 0.52` | `992.65 MiB +- 70.52` |
| PostgreSQL Unindexed | `119.80 MiB +- 0.77` | `120.95 MiB +- 0.56` | `128.84 MiB +- 0.19` | `129.14 MiB +- 0.12` | `130.69 MiB +- 0.24` | `951.11 MiB +- 66.90` |
| Neo4j Indexed | `670.80 MiB +- 14.96` | `722.63 MiB +- 12.10` | `1800.19 MiB +- 6.72` | `955.82 MiB +- 15.54` | `0.00 MiB +- 0.00` | `1237.82 MiB +- 23.96` |
| Neo4j Unindexed | `668.05 MiB +- 15.92` | `729.74 MiB +- 4.86` | `1488.07 MiB +- 510.12` | `1487.02 MiB +- 510.17` | `0.00 MiB +- 0.00` | `1263.27 MiB +- 2.62` |
| ArcadeDB Indexed | `151.24 MiB +- 4.06` | `221.00 MiB +- 15.72` | `292.06 MiB +- 14.48` | `365.22 MiB +- 52.65` | `365.22 MiB +- 52.65` | `1060.29 MiB +- 40.65` |
| ArcadeDB Unindexed | `149.73 MiB +- 1.82` | `222.08 MiB +- 10.61` | `293.67 MiB +- 17.21` | `293.67 MiB +- 17.21` | `293.67 MiB +- 17.21` | `1033.34 MiB +- 22.20` |
| LadybugDB Unindexed | `267.61 MiB +- 0.00` | `294.33 MiB +- 0.06` | `500.49 MiB +- 20.89` | `500.49 MiB +- 20.89` | `500.80 MiB +- 20.89` | `521.36 MiB +- 22.64` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `630.86 MiB +- 15.57` | `630.56 MiB +- 15.28` | `626.96 MiB +- 17.28` | `626.96 MiB +- 17.28` | `626.96 MiB +- 17.28` | `369.38 MiB +- 2.95` |
| SQLite Unindexed | `588.45 MiB +- 6.88` | `586.84 MiB +- 6.85` | `583.89 MiB +- 7.07` | `583.89 MiB +- 7.07` | `583.89 MiB +- 7.07` | `373.40 MiB +- 5.11` |
| DuckDB Unindexed | `756.94 MiB +- 32.10` | `754.03 MiB +- 32.53` | `778.48 MiB +- 31.62` | `778.48 MiB +- 31.62` | `778.48 MiB +- 31.62` | `539.72 MiB +- 35.35` |
| PostgreSQL Indexed | `985.75 MiB +- 71.32` | `985.90 MiB +- 71.34` | `982.46 MiB +- 69.40` | `982.12 MiB +- 69.97` | `984.27 MiB +- 69.95` | `625.88 MiB +- 29.46` |
| PostgreSQL Unindexed | `944.11 MiB +- 65.41` | `944.77 MiB +- 65.45` | `941.64 MiB +- 67.59` | `940.06 MiB +- 66.68` | `943.43 MiB +- 66.72` | `562.64 MiB +- 25.44` |
| Neo4j Indexed | `670.80 MiB +- 14.96` | `722.63 MiB +- 12.10` | `1800.19 MiB +- 6.72` | `955.82 MiB +- 15.54` | `0.00 MiB +- 0.00` | `1241.99 MiB +- 19.30` |
| Neo4j Unindexed | `668.05 MiB +- 15.92` | `729.74 MiB +- 4.86` | `1488.07 MiB +- 510.12` | `1487.02 MiB +- 510.17` | `0.00 MiB +- 0.00` | `1551.20 MiB +- 526.86` |
| ArcadeDB Indexed | `1060.76 MiB +- 40.64` | `1061.14 MiB +- 40.81` | `1062.88 MiB +- 40.77` | `1063.42 MiB +- 40.83` | `1077.59 MiB +- 44.71` | `1113.10 MiB +- 55.71` |
| ArcadeDB Unindexed | `1033.79 MiB +- 22.18` | `1034.16 MiB +- 22.22` | `1035.89 MiB +- 21.42` | `1035.89 MiB +- 21.42` | `1057.44 MiB +- 16.61` | `1078.40 MiB +- 13.00` |
| LadybugDB Unindexed | `469.26 MiB +- 3.41` | `475.35 MiB +- 8.24` | `566.21 MiB +- 32.82` | `566.21 MiB +- 32.82` | `566.21 MiB +- 32.82` | `623.68 MiB +- 28.46` |

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `0.96 ms +- 0.00` | `1.02 ms +- 0.01` | `1.18 ms +- 0.04` |
| `olap/sqlite_indexed` | `3.17 ms +- 0.03` | `3.35 ms +- 0.03` | `3.54 ms +- 0.01` |
| `oltp/sqlite_unindexed` | `1.20 ms +- 0.05` | `1.32 ms +- 0.10` | `1.57 ms +- 0.21` |
| `olap/sqlite_unindexed` | `3.27 ms +- 0.05` | `3.51 ms +- 0.07` | `3.71 ms +- 0.11` |
| `oltp/duckdb` | `1.80 ms +- 0.14` | `2.40 ms +- 0.45` | `2.84 ms +- 0.58` |
| `olap/duckdb` | `3.28 ms +- 1.36` | `3.99 ms +- 1.93` | `4.54 ms +- 2.08` |
| `oltp/postgresql_indexed` | `1.20 ms +- 0.04` | `1.56 ms +- 0.16` | `2.07 ms +- 0.31` |
| `olap/postgresql_indexed` | `2.70 ms +- 0.17` | `3.21 ms +- 0.43` | `3.73 ms +- 0.69` |
| `oltp/postgresql_unindexed` | `1.42 ms +- 0.14` | `1.93 ms +- 0.46` | `2.40 ms +- 0.68` |
| `olap/postgresql_unindexed` | `2.42 ms +- 0.03` | `2.69 ms +- 0.01` | `2.94 ms +- 0.05` |
| `oltp/neo4j_indexed` | `0.19 ms +- 0.03` | `0.27 ms +- 0.04` | `0.34 ms +- 0.07` |
| `olap/neo4j_indexed` | `2.56 ms +- 0.49` | `3.08 ms +- 0.87` | `3.68 ms +- 1.01` |
| `oltp/neo4j_unindexed` | `0.31 ms +- 0.01` | `0.43 ms +- 0.02` | `0.53 ms +- 0.03` |
| `olap/neo4j_unindexed` | `2.47 ms +- 0.07` | `2.87 ms +- 0.17` | `3.39 ms +- 0.16` |
| `oltp/arcadedb_embedded_indexed` | `0.02 ms +- 0.00` | `0.04 ms +- 0.00` | `0.05 ms +- 0.00` |
| `olap/arcadedb_embedded_indexed` | `3.55 ms +- 0.50` | `4.20 ms +- 0.67` | `4.89 ms +- 0.84` |
| `oltp/arcadedb_embedded_unindexed` | `0.40 ms +- 0.01` | `0.43 ms +- 0.01` | `0.51 ms +- 0.05` |
| `olap/arcadedb_embedded_unindexed` | `3.35 ms +- 0.07` | `3.79 ms +- 0.14` | `4.24 ms +- 0.15` |
| `oltp/ladybug_unindexed` | `0.89 ms +- 0.03` | `1.33 ms +- 0.02` | `1.63 ms +- 0.03` |
| `olap/ladybug_unindexed` | `5.86 ms +- 0.12` | `6.76 ms +- 0.16` | `7.35 ms +- 0.25` |

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
| `oltp_create_cross_type_edge` | `1.32 ms +- 0.01` | `1.42 ms +- 0.00` | `2.19 ms +- 0.14` | `1.53 ms +- 0.03` | `1.88 ms +- 0.37` | `0.22 ms +- 0.04` | `0.46 ms +- 0.02` | `0.04 ms +- 0.00` | `0.80 ms +- 0.01` | `0.71 ms +- 0.02` |
| `oltp_create_type1_node` | `0.68 ms +- 0.00` | `0.66 ms +- 0.00` | `1.23 ms +- 0.15` | `0.82 ms +- 0.03` | `0.85 ms +- 0.07` | `0.18 ms +- 0.03` | `0.18 ms +- 0.01` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.22 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.14 ms +- 0.00` | `1.36 ms +- 0.00` | `2.63 ms +- 1.10` | `1.49 ms +- 0.09` | `1.70 ms +- 0.19` | `0.21 ms +- 0.03` | `0.33 ms +- 0.01` | `0.01 ms +- 0.00` | `0.39 ms +- 0.00` | `2.01 ms +- 0.05` |
| `oltp_delete_type1_edge` | `0.74 ms +- 0.01` | `1.01 ms +- 0.13` | `1.82 ms +- 0.08` | `1.04 ms +- 0.01` | `1.21 ms +- 0.04` | `0.17 ms +- 0.02` | `0.29 ms +- 0.01` | `0.03 ms +- 0.00` | `0.41 ms +- 0.01` | `1.13 ms +- 0.15` |
| `oltp_delete_type1_node` | `0.58 ms +- 0.00` | `1.86 ms +- 0.15` | `0.79 ms +- 0.03` | `0.63 ms +- 0.00` | `1.03 ms +- 0.04` | `0.18 ms +- 0.02` | `0.31 ms +- 0.01` | `0.05 ms +- 0.00` | `0.43 ms +- 0.01` | `0.44 ms +- 0.01` |
| `oltp_program_create_and_link` | `1.66 ms +- 0.01` | `1.66 ms +- 0.01` | `3.49 ms +- 1.23` | `1.95 ms +- 0.07` | `2.05 ms +- 0.13` | `0.18 ms +- 0.02` | `0.32 ms +- 0.00` | `0.04 ms +- 0.00` | `0.42 ms +- 0.00` | `0.63 ms +- 0.02` |
| `oltp_type1_neighbors` | `0.96 ms +- 0.00` | `1.17 ms +- 0.00` | `1.77 ms +- 0.10` | `1.36 ms +- 0.20` | `1.76 ms +- 0.38` | `0.21 ms +- 0.03` | `0.33 ms +- 0.02` | `0.02 ms +- 0.00` | `0.39 ms +- 0.01` | `1.92 ms +- 0.16` |
| `oltp_type1_point_lookup` | `0.91 ms +- 0.01` | `0.99 ms +- 0.04` | `1.14 ms +- 0.01` | `1.14 ms +- 0.12` | `1.18 ms +- 0.13` | `0.21 ms +- 0.03` | `0.33 ms +- 0.01` | `0.01 ms +- 0.00` | `0.39 ms +- 0.01` | `0.34 ms +- 0.01` |
| `oltp_update_cross_type_edge_rank` | `0.91 ms +- 0.00` | `1.16 ms +- 0.14` | `1.85 ms +- 0.18` | `1.20 ms +- 0.05` | `1.46 ms +- 0.07` | `0.18 ms +- 0.02` | `0.30 ms +- 0.01` | `0.02 ms +- 0.00` | `0.40 ms +- 0.01` | `1.16 ms +- 0.21` |
| `oltp_update_type1_score` | `0.69 ms +- 0.00` | `0.73 ms +- 0.00` | `1.06 ms +- 0.04` | `0.85 ms +- 0.03` | `1.09 ms +- 0.21` | `0.18 ms +- 0.03` | `0.29 ms +- 0.01` | `0.02 ms +- 0.00` | `0.39 ms +- 0.01` | `0.37 ms +- 0.01` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.39 ms +- 0.02` | `1.52 ms +- 0.03` | `3.72 ms +- 2.03` | `2.12 ms +- 0.51` | `3.22 ms +- 1.27` | `0.30 ms +- 0.06` | `0.62 ms +- 0.05` | `0.07 ms +- 0.00` | `0.84 ms +- 0.02` | `0.86 ms +- 0.01` |
| `oltp_create_type1_node` | `0.73 ms +- 0.03` | `0.70 ms +- 0.01` | `1.98 ms +- 1.15` | `1.03 ms +- 0.08` | `1.04 ms +- 0.17` | `0.25 ms +- 0.05` | `0.24 ms +- 0.01` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` | `0.32 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.20 ms +- 0.03` | `1.49 ms +- 0.11` | `3.34 ms +- 1.75` | `2.04 ms +- 0.40` | `2.61 ms +- 0.90` | `0.28 ms +- 0.06` | `0.43 ms +- 0.02` | `0.02 ms +- 0.00` | `0.41 ms +- 0.01` | `2.98 ms +- 0.01` |
| `oltp_delete_type1_edge` | `0.78 ms +- 0.03` | `1.08 ms +- 0.10` | `2.12 ms +- 0.14` | `1.27 ms +- 0.14` | `1.42 ms +- 0.14` | `0.23 ms +- 0.02` | `0.39 ms +- 0.02` | `0.04 ms +- 0.00` | `0.44 ms +- 0.02` | `1.96 ms +- 0.12` |
| `oltp_delete_type1_node` | `0.61 ms +- 0.01` | `1.98 ms +- 0.16` | `0.87 ms +- 0.03` | `0.77 ms +- 0.05` | `1.28 ms +- 0.12` | `0.23 ms +- 0.03` | `0.42 ms +- 0.01` | `0.07 ms +- 0.00` | `0.45 ms +- 0.01` | `0.55 ms +- 0.01` |
| `oltp_program_create_and_link` | `1.79 ms +- 0.02` | `1.81 ms +- 0.10` | `4.50 ms +- 2.10` | `2.43 ms +- 0.24` | `2.46 ms +- 0.33` | `0.24 ms +- 0.02` | `0.43 ms +- 0.01` | `0.05 ms +- 0.00` | `0.46 ms +- 0.02` | `0.80 ms +- 0.01` |
| `oltp_type1_neighbors` | `0.99 ms +- 0.01` | `1.25 ms +- 0.05` | `2.05 ms +- 0.12` | `1.68 ms +- 0.45` | `2.36 ms +- 0.84` | `0.28 ms +- 0.05` | `0.43 ms +- 0.03` | `0.03 ms +- 0.00` | `0.42 ms +- 0.02` | `2.94 ms +- 0.16` |
| `oltp_type1_point_lookup` | `0.96 ms +- 0.02` | `1.28 ms +- 0.43` | `1.20 ms +- 0.04` | `1.72 ms +- 0.52` | `1.46 ms +- 0.29` | `0.40 ms +- 0.07` | `0.51 ms +- 0.02` | `0.03 ms +- 0.01` | `0.47 ms +- 0.05` | `0.42 ms +- 0.02` |
| `oltp_update_cross_type_edge_rank` | `0.98 ms +- 0.01` | `1.26 ms +- 0.10` | `3.04 ms +- 1.82` | `1.48 ms +- 0.27` | `1.74 ms +- 0.19` | `0.24 ms +- 0.04` | `0.41 ms +- 0.01` | `0.03 ms +- 0.00` | `0.43 ms +- 0.02` | `2.04 ms +- 0.18` |
| `oltp_update_type1_score` | `0.73 ms +- 0.01` | `0.78 ms +- 0.03` | `1.19 ms +- 0.08` | `1.09 ms +- 0.10` | `1.71 ms +- 0.88` | `0.25 ms +- 0.04` | `0.39 ms +- 0.03` | `0.03 ms +- 0.02` | `0.42 ms +- 0.01` | `0.46 ms +- 0.02` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.59 ms +- 0.02` | `1.74 ms +- 0.09` | `4.64 ms +- 2.81` | `3.16 ms +- 1.69` | `4.67 ms +- 2.28` | `0.37 ms +- 0.10` | `0.75 ms +- 0.04` | `0.09 ms +- 0.00` | `0.97 ms +- 0.09` | `1.04 ms +- 0.09` |
| `oltp_create_type1_node` | `0.89 ms +- 0.08` | `0.87 ms +- 0.04` | `2.41 ms +- 1.40` | `1.24 ms +- 0.06` | `1.25 ms +- 0.26` | `0.32 ms +- 0.09` | `0.32 ms +- 0.03` | `0.03 ms +- 0.00` | `0.02 ms +- 0.00` | `0.37 ms +- 0.01` |
| `oltp_cross_type_lookup` | `1.38 ms +- 0.12` | `1.62 ms +- 0.06` | `3.94 ms +- 2.08` | `2.57 ms +- 0.69` | `3.16 ms +- 1.14` | `0.35 ms +- 0.10` | `0.51 ms +- 0.03` | `0.02 ms +- 0.00` | `0.46 ms +- 0.04` | `3.57 ms +- 0.02` |
| `oltp_delete_type1_edge` | `0.93 ms +- 0.08` | `1.26 ms +- 0.12` | `2.52 ms +- 0.13` | `1.68 ms +- 0.22` | `1.76 ms +- 0.30` | `0.27 ms +- 0.03` | `0.48 ms +- 0.03` | `0.06 ms +- 0.00` | `0.51 ms +- 0.08` | `2.41 ms +- 0.12` |
| `oltp_delete_type1_node` | `0.72 ms +- 0.06` | `2.23 ms +- 0.20` | `1.03 ms +- 0.06` | `1.04 ms +- 0.09` | `1.59 ms +- 0.21` | `0.26 ms +- 0.02` | `0.50 ms +- 0.02` | `0.08 ms +- 0.00` | `0.50 ms +- 0.07` | `0.64 ms +- 0.01` |
| `oltp_program_create_and_link` | `2.04 ms +- 0.04` | `2.07 ms +- 0.19` | `5.19 ms +- 2.48` | `2.80 ms +- 0.28` | `2.85 ms +- 0.40` | `0.27 ms +- 0.02` | `0.52 ms +- 0.01` | `0.06 ms +- 0.00` | `0.53 ms +- 0.06` | `0.94 ms +- 0.02` |
| `oltp_type1_neighbors` | `1.09 ms +- 0.07` | `1.46 ms +- 0.17` | `2.39 ms +- 0.14` | `2.17 ms +- 0.70` | `2.85 ms +- 1.10` | `0.34 ms +- 0.08` | `0.51 ms +- 0.04` | `0.05 ms +- 0.00` | `0.48 ms +- 0.05` | `3.64 ms +- 0.30` |
| `oltp_type1_point_lookup` | `1.11 ms +- 0.02` | `2.07 ms +- 1.48` | `1.35 ms +- 0.09` | `2.88 ms +- 1.47` | `1.81 ms +- 0.37` | `0.64 ms +- 0.22` | `0.71 ms +- 0.06` | `0.05 ms +- 0.01` | `0.65 ms +- 0.02` | `0.53 ms +- 0.07` |
| `oltp_update_cross_type_edge_rank` | `1.16 ms +- 0.02` | `1.47 ms +- 0.07` | `3.57 ms +- 2.21` | `1.79 ms +- 0.33` | `2.03 ms +- 0.26` | `0.29 ms +- 0.05` | `0.49 ms +- 0.03` | `0.04 ms +- 0.00` | `0.50 ms +- 0.08` | `2.56 ms +- 0.19` |
| `oltp_update_type1_score` | `0.91 ms +- 0.01` | `0.96 ms +- 0.05` | `1.39 ms +- 0.12` | `1.36 ms +- 0.12` | `2.08 ms +- 1.09` | `0.30 ms +- 0.05` | `0.47 ms +- 0.04` | `0.04 ms +- 0.03` | `0.47 ms +- 0.06` | `0.53 ms +- 0.02` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `2.81 ms +- 0.02` | `2.30 ms +- 0.11` | `2.41 ms +- 0.32` | `2.36 ms +- 0.16` | `2.16 ms +- 0.03` | `2.61 ms +- 1.17` | `2.08 ms +- 0.06` | `3.72 ms +- 0.23` | `3.59 ms +- 0.23` | `4.15 ms +- 0.39` |
| `olap_fixed_length_path_projection` | `5.68 ms +- 0.06` | `5.80 ms +- 0.13` | `4.71 ms +- 2.73` | `4.84 ms +- 0.39` | `3.82 ms +- 0.02` | `5.17 ms +- 1.48` | `4.54 ms +- 0.16` | `5.02 ms +- 0.13` | `4.77 ms +- 0.07` | `18.48 ms +- 0.33` |
| `olap_graph_introspection_rollup` | `1.51 ms +- 0.00` | `2.08 ms +- 0.01` | `3.43 ms +- 1.92` | `2.57 ms +- 0.20` | `2.36 ms +- 0.01` | `3.12 ms +- 1.04` | `2.74 ms +- 0.14` | `6.60 ms +- 0.32` | `6.46 ms +- 0.24` | `2.62 ms +- 0.11` |
| `olap_three_type_path_count` | `4.29 ms +- 0.08` | `4.17 ms +- 0.09` | `2.92 ms +- 1.46` | `3.00 ms +- 0.20` | `2.48 ms +- 0.03` | `1.93 ms +- 0.31` | `1.83 ms +- 0.05` | `0.07 ms +- 0.01` | `0.07 ms +- 0.01` | `4.61 ms +- 0.04` |
| `olap_type1_active_leaderboard` | `1.12 ms +- 0.01` | `1.23 ms +- 0.00` | `1.89 ms +- 0.12` | `1.33 ms +- 0.08` | `1.26 ms +- 0.01` | `1.22 ms +- 0.14` | `1.22 ms +- 0.04` | `0.89 ms +- 0.02` | `1.03 ms +- 0.01` | `0.59 ms +- 0.04` |
| `olap_type1_age_rollup` | `1.54 ms +- 0.03` | `1.44 ms +- 0.01` | `1.85 ms +- 0.13` | `1.44 ms +- 0.06` | `1.38 ms +- 0.03` | `0.78 ms +- 0.07` | `0.77 ms +- 0.03` | `0.57 ms +- 0.03` | `0.56 ms +- 0.01` | `1.79 ms +- 0.11` |
| `olap_type2_score_distribution` | `1.39 ms +- 0.01` | `1.52 ms +- 0.01` | `2.63 ms +- 1.26` | `1.51 ms +- 0.04` | `1.45 ms +- 0.02` | `0.69 ms +- 0.04` | `0.69 ms +- 0.02` | `0.56 ms +- 0.01` | `0.56 ms +- 0.01` | `1.91 ms +- 0.10` |
| `olap_variable_length_grouped_rollup` | `9.97 ms +- 0.27` | `9.36 ms +- 0.24` | `5.83 ms +- 2.59` | `5.66 ms +- 0.59` | `5.04 ms +- 0.11` | `8.90 ms +- 0.54` | `9.51 ms +- 0.19` | `17.34 ms +- 4.27` | `15.34 ms +- 0.18` | `20.80 ms +- 0.47` |
| `olap_variable_length_reachability` | `1.53 ms +- 0.01` | `2.99 ms +- 0.08` | `3.92 ms +- 1.47` | `2.36 ms +- 0.11` | `2.38 ms +- 0.03` | `0.35 ms +- 0.04` | `0.45 ms +- 0.02` | `0.06 ms +- 0.00` | `0.45 ms +- 0.01` | `1.65 ms +- 0.08` |
| `olap_with_scalar_rebinding` | `1.91 ms +- 0.01` | `1.83 ms +- 0.01` | `3.23 ms +- 1.62` | `1.94 ms +- 0.08` | `1.87 ms +- 0.02` | `0.82 ms +- 0.10` | `0.82 ms +- 0.01` | `0.66 ms +- 0.02` | `0.65 ms +- 0.01` | `1.97 ms +- 0.12` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `3.03 ms +- 0.03` | `2.55 ms +- 0.30` | `2.80 ms +- 0.50` | `2.80 ms +- 0.39` | `2.35 ms +- 0.13` | `3.22 ms +- 1.72` | `2.55 ms +- 0.26` | `4.73 ms +- 0.31` | `4.21 ms +- 0.18` | `5.44 ms +- 0.61` |
| `olap_fixed_length_path_projection` | `5.91 ms +- 0.19` | `6.23 ms +- 0.08` | `5.58 ms +- 3.44` | `5.70 ms +- 0.92` | `4.48 ms +- 0.22` | `6.11 ms +- 2.26` | `5.27 ms +- 0.44` | `5.66 ms +- 0.41` | `5.19 ms +- 0.28` | `20.35 ms +- 0.23` |
| `olap_graph_introspection_rollup` | `1.54 ms +- 0.02` | `2.29 ms +- 0.14` | `4.11 ms +- 2.53` | `3.00 ms +- 0.46` | `2.65 ms +- 0.12` | `3.73 ms +- 1.56` | `3.15 ms +- 0.19` | `8.16 ms +- 1.39` | `7.35 ms +- 0.40` | `3.42 ms +- 0.06` |
| `olap_three_type_path_count` | `4.60 ms +- 0.08` | `4.39 ms +- 0.13` | `3.65 ms +- 2.17` | `3.41 ms +- 0.52` | `2.78 ms +- 0.05` | `2.35 ms +- 0.83` | `2.31 ms +- 0.03` | `0.13 ms +- 0.01` | `0.13 ms +- 0.00` | `5.41 ms +- 0.16` |
| `olap_type1_active_leaderboard` | `1.26 ms +- 0.17` | `1.29 ms +- 0.01` | `2.15 ms +- 0.16` | `1.55 ms +- 0.28` | `1.38 ms +- 0.04` | `1.63 ms +- 0.39` | `1.54 ms +- 0.21` | `1.28 ms +- 0.10` | `1.43 ms +- 0.13` | `0.81 ms +- 0.05` |
| `olap_type1_age_rollup` | `1.58 ms +- 0.06` | `1.51 ms +- 0.09` | `2.15 ms +- 0.21` | `1.73 ms +- 0.20` | `1.48 ms +- 0.03` | `0.98 ms +- 0.22` | `0.96 ms +- 0.06` | `0.87 ms +- 0.02` | `0.72 ms +- 0.06` | `2.49 ms +- 0.26` |
| `olap_type2_score_distribution` | `1.44 ms +- 0.07` | `1.56 ms +- 0.02` | `3.22 ms +- 1.91` | `1.84 ms +- 0.30` | `1.53 ms +- 0.04` | `0.93 ms +- 0.15` | `0.83 ms +- 0.09` | `0.63 ms +- 0.06` | `0.59 ms +- 0.03` | `2.53 ms +- 0.11` |
| `olap_variable_length_grouped_rollup` | `10.56 ms +- 0.25` | `10.09 ms +- 0.51` | `7.09 ms +- 3.46` | `6.79 ms +- 0.74` | `5.62 ms +- 0.03` | `10.37 ms +- 1.34` | `10.43 ms +- 0.53` | `19.50 ms +- 4.50` | `16.83 ms +- 0.57` | `22.37 ms +- 0.62` |
| `olap_variable_length_reachability` | `1.65 ms +- 0.04` | `3.15 ms +- 0.21` | `5.18 ms +- 3.03` | `2.87 ms +- 0.46` | `2.72 ms +- 0.03` | `0.45 ms +- 0.06` | `0.59 ms +- 0.02` | `0.09 ms +- 0.00` | `0.50 ms +- 0.04` | `2.12 ms +- 0.18` |
| `olap_with_scalar_rebinding` | `1.95 ms +- 0.02` | `1.99 ms +- 0.16` | `3.99 ms +- 2.04` | `2.38 ms +- 0.36` | `1.91 ms +- 0.04` | `1.07 ms +- 0.30` | `1.03 ms +- 0.12` | `0.92 ms +- 0.02` | `0.91 ms +- 0.01` | `2.67 ms +- 0.32` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `3.35 ms +- 0.19` | `2.71 ms +- 0.34` | `3.03 ms +- 0.48` | `4.52 ms +- 2.82` | `2.61 ms +- 0.11` | `3.85 ms +- 1.82` | `3.26 ms +- 0.32` | `6.05 ms +- 0.25` | `5.46 ms +- 0.29` | `6.27 ms +- 0.98` |
| `olap_fixed_length_path_projection` | `5.98 ms +- 0.19` | `6.45 ms +- 0.08` | `6.22 ms +- 3.65` | `6.44 ms +- 1.27` | `4.77 ms +- 0.19` | `7.06 ms +- 2.50` | `6.06 ms +- 0.44` | `7.32 ms +- 0.61` | `6.29 ms +- 0.17` | `21.05 ms +- 0.31` |
| `olap_graph_introspection_rollup` | `1.69 ms +- 0.13` | `2.51 ms +- 0.24` | `4.71 ms +- 2.79` | `3.24 ms +- 0.39` | `2.85 ms +- 0.18` | `4.46 ms +- 1.59` | `3.84 ms +- 0.16` | `10.17 ms +- 2.57` | `7.97 ms +- 0.30` | `3.76 ms +- 0.13` |
| `olap_three_type_path_count` | `4.85 ms +- 0.13` | `4.69 ms +- 0.26` | `4.36 ms +- 2.61` | `3.70 ms +- 0.68` | `3.10 ms +- 0.15` | `3.41 ms +- 0.63` | `3.23 ms +- 0.02` | `0.15 ms +- 0.01` | `0.15 ms +- 0.00` | `6.08 ms +- 0.46` |
| `olap_type1_active_leaderboard` | `1.50 ms +- 0.26` | `1.38 ms +- 0.05` | `2.50 ms +- 0.04` | `1.75 ms +- 0.35` | `1.57 ms +- 0.17` | `1.90 ms +- 0.50` | `1.78 ms +- 0.23` | `1.58 ms +- 0.38` | `1.58 ms +- 0.25` | `1.10 ms +- 0.12` |
| `olap_type1_age_rollup` | `1.67 ms +- 0.13` | `1.56 ms +- 0.13` | `2.52 ms +- 0.02` | `1.98 ms +- 0.11` | `1.73 ms +- 0.18` | `1.24 ms +- 0.18` | `1.20 ms +- 0.06` | `0.92 ms +- 0.03` | `0.83 ms +- 0.08` | `2.87 ms +- 0.51` |
| `olap_type2_score_distribution` | `1.50 ms +- 0.16` | `1.62 ms +- 0.02` | `3.79 ms +- 2.27` | `2.08 ms +- 0.41` | `1.72 ms +- 0.20` | `1.12 ms +- 0.04` | `1.02 ms +- 0.12` | `0.77 ms +- 0.08` | `0.66 ms +- 0.08` | `2.89 ms +- 0.08` |
| `olap_variable_length_grouped_rollup` | `10.90 ms +- 0.30` | `10.76 ms +- 1.03` | `7.83 ms +- 3.57` | `7.74 ms +- 0.95` | `6.12 ms +- 0.30` | `11.90 ms +- 2.59` | `11.57 ms +- 0.84` | `20.79 ms +- 4.91` | `17.77 ms +- 0.62` | `23.67 ms +- 0.85` |
| `olap_variable_length_reachability` | `1.92 ms +- 0.01` | `3.27 ms +- 0.23` | `5.79 ms +- 3.38` | `3.20 ms +- 0.62` | `2.95 ms +- 0.08` | `0.53 ms +- 0.05` | `0.67 ms +- 0.05` | `0.12 ms +- 0.01` | `0.57 ms +- 0.06` | `2.72 ms +- 0.55` |
| `olap_with_scalar_rebinding` | `2.01 ms +- 0.04` | `2.15 ms +- 0.25` | `4.60 ms +- 2.14` | `2.64 ms +- 0.58` | `2.01 ms +- 0.13` | `1.37 ms +- 0.37` | `1.27 ms +- 0.19` | `1.07 ms +- 0.12` | `1.07 ms +- 0.06` | `3.04 ms +- 0.61` |
