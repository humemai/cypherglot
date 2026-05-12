# Runtime Result Summary

- Scanned JSON files: 98
- Completed runs: 97
- Skipped unreadable or non-completed runs: 1
- Grouped configurations: 33
- Grouped benchmark campaigns: 3

### Large runtime dataset

The current large runtime matrix used the `large` preset with `2000` OLTP iterations / `20` OLTP warmup and `50` OLAP iterations / `5` OLAP warmup.

That corresponds to roughly:

- `10,000,000` total nodes
- `77,790,000` total edges
- `10` node types
- `10` edge types
- `61` property fields across the schema (`38` per node, `23` per edge)
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
tables below, that checkpoint time is shown in the `Analyze / Checkpoint` column so
the setup layout stays consistent across engines.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
cross-engine setup tables below, that engine-specific post-load work is omitted so
only shared phases appear side by side. The ArcadeDB-specific timing tables below break out
`GAV` separately and keep `Analyze / Checkpoint` as a shared bucket.


Wall-clock run time per completed result file:

| Combo | Wall-clock run time |
| --- | --- |
| SQLite Indexed (3) | `8449.35 s +- 667.62` |
| SQLite Unindexed (3) | `16830.38 s +- 2741.54` |
| DuckDB Indexed (3) | `2133.15 s +- 61.44` |
| DuckDB Unindexed (3) | `2163.85 s +- 38.24` |
| PostgreSQL Indexed (3) | `10923.59 s +- 1230.35` |
| PostgreSQL Unindexed (3) | `17491.67 s +- 2449.72` |
| Neo4j Indexed (3) | `15044.60 s +- 829.90` |
| Neo4j Unindexed (3) | `23127.43 s +- 470.87` |
| ArcadeDB Indexed (3) | `14264.72 s +- 161.38` |
| ArcadeDB Unindexed (3) | `23534.64 s +- 1185.84` |
| LadybugDB Unindexed (1) | `137740.64 s +- 0.00` |

OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `13.43 ms +- 3.86` | `7.99 ms +- 1.42` | `1334611.87 ms +- 196662.20` | `88426.15 ms +- 20865.32` | `19674.38 ms +- 1218.43` | `1.27 ms +- 0.03` | `1.48 ms +- 0.20` | `1.79 ms +- 0.31` |
| SQLite Unindexed (3) | `15.75 ms +- 4.38` | `8.31 ms +- 1.65` | `1299306.96 ms +- 132023.39` | `52239.95 ms +- 56367.07` | `18731.07 ms +- 23106.03` | `258.63 ms +- 104.84` | `273.91 ms +- 116.60` | `288.16 ms +- 132.62` |
| DuckDB Indexed (3) | `65.05 ms +- 28.35` | `401.72 ms +- 146.13` | `158946.23 ms +- 4811.95` | `37593.17 ms +- 2184.11` | `0.37 ms +- 0.06` | `3.37 ms +- 0.23` | `4.21 ms +- 0.41` | `4.82 ms +- 0.62` |
| DuckDB Unindexed (3) | `56.04 ms +- 23.66` | `325.32 ms +- 137.48` | `162736.38 ms +- 3748.04` | `226.54 ms +- 84.79` | `0.16 ms +- 0.03` | `5.80 ms +- 0.50` | `7.35 ms +- 0.54` | `8.15 ms +- 0.75` |
| PostgreSQL Indexed (3) | `18.54 ms +- 2.85` | `783.01 ms +- 23.12` | `1638136.26 ms +- 51548.54` | `201269.63 ms +- 31733.80` | `24140.91 ms +- 8330.83` | `1.54 ms +- 0.04` | `2.13 ms +- 0.16` | `2.62 ms +- 0.24` |
| PostgreSQL Unindexed (3) | `13.76 ms +- 4.33` | `799.91 ms +- 53.86` | `1701188.93 ms +- 153813.56` | `355.51 ms +- 200.49` | `39445.75 ms +- 10292.06` | `267.32 ms +- 97.53` | `292.32 ms +- 100.07` | `311.61 ms +- 103.44` |
| Neo4j Indexed (3) | `66.28 ms +- 12.60` | `724.01 ms +- 24.61` | `8711503.66 ms +- 651103.07` | `217119.96 ms +- 44661.84` | `0.00 ms +- 0.00` | `0.25 ms +- 0.02` | `0.38 ms +- 0.07` | `0.57 ms +- 0.16` |
| Neo4j Unindexed (3) | `62.10 ms +- 1.35` | `821.42 ms +- 127.46` | `9177380.34 ms +- 694062.48` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `340.81 ms +- 6.99` | `360.65 ms +- 6.93` | `386.77 ms +- 3.00` |
| ArcadeDB Indexed (3) | `266.47 ms +- 5.49` | `483.92 ms +- 5.96` | `3230654.03 ms +- 38804.87` | `268929.00 ms +- 6846.16` | `0.00 ms +- 0.00` | `0.09 ms +- 0.00` | `0.13 ms +- 0.02` | `0.15 ms +- 0.03` |
| ArcadeDB Unindexed (3) | `279.38 ms +- 15.59` | `514.30 ms +- 33.90` | `3257976.33 ms +- 77374.56` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `434.92 ms +- 20.09` | `448.25 ms +- 11.38` | `461.15 ms +- 5.30` |
| LadybugDB Unindexed (1) | `74.25 ms +- 0.00` | `96.37 ms +- 0.00` | `66740427.21 ms +- 0.00` | `0.00 ms +- 0.00` | `88.62 ms +- 0.00` | `6.41 ms +- 0.00` | `33.66 ms +- 0.00` | `37.38 ms +- 0.00` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `20.26 ms +- 10.78` | `17.55 ms +- 11.32` | `1388977.97 ms +- 84383.18` | `86568.48 ms +- 10111.97` | `20593.93 ms +- 2185.67` | `4117.93 ms +- 177.53` | `4175.15 ms +- 175.63` | `4199.82 ms +- 184.77` |
| SQLite Unindexed (3) | `16.90 ms +- 5.82` | `8.52 ms +- 2.02` | `1341852.97 ms +- 11330.20` | `20162.89 ms +- 795.90` | `7189.11 ms +- 1480.97` | `8319.09 ms +- 307.47` | `8621.42 ms +- 501.20` | `8722.94 ms +- 559.92` |
| DuckDB Indexed (3) | `11.89 ms +- 2.72` | `402.40 ms +- 152.91` | `160004.23 ms +- 5377.48` | `37283.97 ms +- 2225.00` | `0.36 ms +- 0.12` | `566.42 ms +- 22.11` | `582.46 ms +- 28.14` | `590.98 ms +- 30.21` |
| DuckDB Unindexed (3) | `12.55 ms +- 0.52` | `410.09 ms +- 157.58` | `163131.28 ms +- 5950.90` | `227.56 ms +- 80.30` | `0.17 ms +- 0.04` | `579.79 ms +- 13.74` | `606.69 ms +- 1.85` | `620.66 ms +- 9.08` |
| PostgreSQL Indexed (3) | `8.38 ms +- 1.01` | `21124.45 ms +- 1445.29` | `1707644.06 ms +- 268797.39` | `228205.68 ms +- 76197.88` | `38787.42 ms +- 26742.54` | `6493.17 ms +- 583.99` | `7749.85 ms +- 1758.69` | `8124.87 ms +- 2131.78` |
| PostgreSQL Unindexed (3) | `11.78 ms +- 6.28` | `1566.46 ms +- 502.34` | `1657093.15 ms +- 172090.97` | `575.39 ms +- 207.68` | `39766.55 ms +- 32339.44` | `8446.08 ms +- 665.76` | `9934.45 ms +- 1444.59` | `10604.07 ms +- 1651.43` |
| Neo4j Indexed (3) | `66.28 ms +- 12.60` | `724.01 ms +- 24.61` | `8711503.66 ms +- 651103.07` | `217119.96 ms +- 44661.84` | `0.00 ms +- 0.00` | `6902.01 ms +- 199.80` | `7355.40 ms +- 321.96` | `7620.51 ms +- 435.05` |
| Neo4j Unindexed (3) | `62.10 ms +- 1.35` | `821.42 ms +- 127.46` | `9177380.34 ms +- 694062.48` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `7127.17 ms +- 211.88` | `7320.18 ms +- 279.28` | `7544.89 ms +- 307.09` |
| ArcadeDB Indexed (3) | `1.94 ms +- 0.64` | `132.17 ms +- 15.21` | `4018428.88 ms +- 150719.71` | `288256.82 ms +- 8902.22` | `0.00 ms +- 0.00` | `3259.27 ms +- 84.76` | `3485.60 ms +- 245.29` | `3645.72 ms +- 250.93` |
| ArcadeDB Unindexed (3) | `1.83 ms +- 0.61` | `134.28 ms +- 7.66` | `4123560.84 ms +- 73558.07` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2911.46 ms +- 364.57` | `3731.69 ms +- 119.49` | `4241.15 ms +- 169.22` |
| LadybugDB Unindexed (1) | `25.94 ms +- 0.00` | `60.89 ms +- 0.00` | `68746542.98 ms +- 0.00` | `0.00 ms +- 0.00` | `92.75 ms +- 0.00` | `743.83 ms +- 0.00` | `810.32 ms +- 0.00` | `834.93 ms +- 0.00` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `104.69 MiB +- 10.14` | `104.72 MiB +- 10.14` | `158.90 MiB +- 1.95` | `158.88 MiB +- 1.97` | `158.88 MiB +- 1.97` | `397.69 MiB +- 5.67` |
| SQLite Unindexed (3) | `104.60 MiB +- 8.87` | `104.63 MiB +- 8.87` | `139.56 MiB +- 31.64` | `140.56 MiB +- 29.90` | `140.58 MiB +- 29.87` | `290.79 MiB +- 45.47` |
| DuckDB Indexed (3) | `90.29 MiB +- 2.20` | `94.88 MiB +- 2.11` | `5491.39 MiB +- 11.05` | `5636.36 MiB +- 9.92` | `5636.36 MiB +- 9.91` | `5749.07 MiB +- 11.31` |
| DuckDB Unindexed (3) | `90.43 MiB +- 2.25` | `94.96 MiB +- 2.21` | `5463.75 MiB +- 1.09` | `5461.89 MiB +- 1.74` | `5461.89 MiB +- 1.74` | `5672.03 MiB +- 4.69` |
| PostgreSQL Indexed (3) | `107.70 MiB +- 26.87` | `112.10 MiB +- 26.22` | `1061.94 MiB +- 402.07` | `1039.53 MiB +- 515.07` | `1004.86 MiB +- 538.79` | `1221.21 MiB +- 546.25` |
| PostgreSQL Unindexed (3) | `138.41 MiB +- 11.23` | `142.29 MiB +- 11.06` | `779.65 MiB +- 207.32` | `719.08 MiB +- 172.20` | `827.68 MiB +- 137.32` | `599.30 MiB +- 228.50` |
| Neo4j Indexed (3) | `699.41 MiB +- 33.12` | `746.40 MiB +- 26.92` | `3069.31 MiB +- 684.61` | `5139.15 MiB +- 661.63` | `0.00 MiB +- 0.00` | `4127.95 MiB +- 1635.81` |
| Neo4j Unindexed (3) | `666.20 MiB +- 11.90` | `721.22 MiB +- 21.97` | `2797.32 MiB +- 521.41` | `2796.30 MiB +- 521.41` | `0.00 MiB +- 0.00` | `2614.52 MiB +- 574.40` |
| ArcadeDB Indexed (3) | `179.95 MiB +- 6.60` | `407.27 MiB +- 16.56` | `32539.63 MiB +- 597.15` | `42498.75 MiB +- 8012.73` | `42498.75 MiB +- 8012.73` | `40258.79 MiB +- 1669.94` |
| ArcadeDB Unindexed (3) | `182.08 MiB +- 1.78` | `366.23 MiB +- 59.20` | `32316.12 MiB +- 733.67` | `32316.12 MiB +- 733.67` | `32316.12 MiB +- 733.67` | `34392.57 MiB +- 8375.01` |
| LadybugDB Unindexed (1) | `299.00 MiB +- 0.00` | `357.84 MiB +- 0.00` | `17736.68 MiB +- 0.00` | `17736.68 MiB +- 0.00` | `17737.11 MiB +- 0.00` | `17785.57 MiB +- 0.00` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `339.57 MiB +- 5.67` | `339.57 MiB +- 5.72` | `307.44 MiB +- 122.74` | `308.33 MiB +- 121.15` | `308.33 MiB +- 121.13` | `351.25 MiB +- 94.76` |
| SQLite Unindexed (3) | `251.78 MiB +- 13.01` | `251.81 MiB +- 13.01` | `312.78 MiB +- 13.42` | `312.78 MiB +- 13.42` | `312.78 MiB +- 13.42` | `322.11 MiB +- 59.01` |
| DuckDB Indexed (3) | `397.59 MiB +- 1.18` | `395.96 MiB +- 0.75` | `5683.26 MiB +- 31.91` | `5773.01 MiB +- 105.59` | `5773.03 MiB +- 105.57` | `6872.00 MiB +- 1181.87` |
| DuckDB Unindexed (3) | `396.25 MiB +- 4.06` | `396.28 MiB +- 3.77` | `5699.49 MiB +- 10.59` | `5699.79 MiB +- 10.12` | `5699.79 MiB +- 10.12` | `7103.79 MiB +- 547.73` |
| PostgreSQL Indexed (3) | `1075.62 MiB +- 517.63` | `474.25 MiB +- 43.51` | `1191.47 MiB +- 591.66` | `1185.03 MiB +- 590.28` | `1149.81 MiB +- 640.56` | `938.55 MiB +- 587.48` |
| PostgreSQL Unindexed (3) | `521.55 MiB +- 131.09` | `430.11 MiB +- 38.91` | `1018.00 MiB +- 360.36` | `929.53 MiB +- 294.65` | `1016.31 MiB +- 267.67` | `522.04 MiB +- 332.03` |
| Neo4j Indexed (3) | `699.41 MiB +- 33.12` | `746.40 MiB +- 26.92` | `3069.31 MiB +- 684.61` | `5139.15 MiB +- 661.63` | `0.00 MiB +- 0.00` | `2063.52 MiB +- 632.64` |
| Neo4j Unindexed (3) | `666.20 MiB +- 11.90` | `721.22 MiB +- 21.97` | `2797.32 MiB +- 521.41` | `2796.30 MiB +- 521.41` | `0.00 MiB +- 0.00` | `2324.92 MiB +- 606.91` |
| ArcadeDB Indexed (3) | `40258.56 MiB +- 1670.25` | `40258.97 MiB +- 1670.11` | `31972.20 MiB +- 1473.16` | `43458.82 MiB +- 1774.69` | `51281.15 MiB +- 3546.10` | `61168.77 MiB +- 3618.98` |
| ArcadeDB Unindexed (3) | `34393.09 MiB +- 8375.26` | `34395.11 MiB +- 8377.64` | `32610.11 MiB +- 961.31` | `32610.11 MiB +- 961.31` | `42912.29 MiB +- 1196.38` | `38754.09 MiB +- 7314.48` |
| LadybugDB Unindexed (1) | `13045.62 MiB +- 0.00` | `13045.92 MiB +- 0.00` | `17985.25 MiB +- 0.00` | `17985.25 MiB +- 0.00` | `17985.25 MiB +- 0.00` | `72844.59 MiB +- 0.00` |

#### Large runtime suite comparison

This rolls the large-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.27 ms +- 0.03` | `1.48 ms +- 0.20` | `1.79 ms +- 0.31` |
| `olap/sqlite_indexed` | `4117.93 ms +- 177.53` | `4175.15 ms +- 175.63` | `4199.82 ms +- 184.77` |
| `oltp/sqlite_unindexed` | `258.63 ms +- 104.84` | `273.91 ms +- 116.60` | `288.16 ms +- 132.62` |
| `olap/sqlite_unindexed` | `8319.09 ms +- 307.47` | `8621.42 ms +- 501.20` | `8722.94 ms +- 559.92` |
| `oltp/duckdb_indexed` | `3.37 ms +- 0.23` | `4.21 ms +- 0.41` | `4.82 ms +- 0.62` |
| `olap/duckdb_indexed` | `566.42 ms +- 22.11` | `582.46 ms +- 28.14` | `590.98 ms +- 30.21` |
| `oltp/duckdb_unindexed` | `5.80 ms +- 0.50` | `7.35 ms +- 0.54` | `8.15 ms +- 0.75` |
| `olap/duckdb_unindexed` | `579.79 ms +- 13.74` | `606.69 ms +- 1.85` | `620.66 ms +- 9.08` |
| `oltp/postgresql_indexed` | `1.54 ms +- 0.04` | `2.13 ms +- 0.16` | `2.62 ms +- 0.24` |
| `olap/postgresql_indexed` | `6493.17 ms +- 583.99` | `7749.85 ms +- 1758.69` | `8124.87 ms +- 2131.78` |
| `oltp/postgresql_unindexed` | `267.32 ms +- 97.53` | `292.32 ms +- 100.07` | `311.61 ms +- 103.44` |
| `olap/postgresql_unindexed` | `8446.08 ms +- 665.76` | `9934.45 ms +- 1444.59` | `10604.07 ms +- 1651.43` |
| `oltp/neo4j_indexed` | `0.25 ms +- 0.02` | `0.38 ms +- 0.07` | `0.57 ms +- 0.16` |
| `olap/neo4j_indexed` | `6902.01 ms +- 199.80` | `7355.40 ms +- 321.96` | `7620.51 ms +- 435.05` |
| `oltp/neo4j_unindexed` | `340.81 ms +- 6.99` | `360.65 ms +- 6.93` | `386.77 ms +- 3.00` |
| `olap/neo4j_unindexed` | `7127.17 ms +- 211.88` | `7320.18 ms +- 279.28` | `7544.89 ms +- 307.09` |
| `oltp/arcadedb_embedded_indexed` | `0.09 ms +- 0.00` | `0.13 ms +- 0.02` | `0.15 ms +- 0.03` |
| `olap/arcadedb_embedded_indexed` | `3259.27 ms +- 84.76` | `3485.60 ms +- 245.29` | `3645.72 ms +- 250.93` |
| `oltp/arcadedb_embedded_unindexed` | `434.92 ms +- 20.09` | `448.25 ms +- 11.38` | `461.15 ms +- 5.30` |
| `olap/arcadedb_embedded_unindexed` | `2911.46 ms +- 364.57` | `3731.69 ms +- 119.49` | `4241.15 ms +- 169.22` |
| `oltp/ladybug_unindexed` | `6.41 ms +- 0.00` | `33.66 ms +- 0.00` | `37.38 ms +- 0.00` |
| `olap/ladybug_unindexed` | `743.83 ms +- 0.00` | `810.32 ms +- 0.00` | `834.93 ms +- 0.00` |

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

#### Large ArcadeDB-specific timings

These tables keep ArcadeDB-only timing phases out of the
cross-engine comparison tables.

ArcadeDB setup hierarchy is `connect/reset -> schema/constraints ->
ingest -> index -> GAV -> analyze/checkpoint`, with `GAV` present only
for OLAP fixtures.

ArcadeDB setup phase breakdown, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `266.47 ms +- 5.49` | `483.92 ms +- 5.96` | `3230654.03 ms +- 38804.87` | `268929.00 ms +- 6846.16` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `279.38 ms +- 15.59` | `514.30 ms +- 33.90` | `3257976.33 ms +- 77374.56` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |

ArcadeDB setup phase breakdown, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `1.94 ms +- 0.64` | `132.17 ms +- 15.21` | `4018428.88 ms +- 150719.71` | `288256.82 ms +- 8902.22` | `86496.38 ms +- 4646.23` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `1.83 ms +- 0.61` | `134.28 ms +- 7.66` | `4123560.84 ms +- 73558.07` | `0.00 ms +- 0.00` | `88428.79 ms +- 4688.45` | `0.00 ms +- 0.00` |

#### Large runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.
`open` is already reported here; worker close time is not currently
recorded in the raw benchmark payloads.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `7053.28 ms +- 1584.46` | `6304.90 ms +- 405.60` |
| `oltp_create_type1_node` | `7000.34 ms +- 1595.74` | `6575.02 ms +- 932.77` |
| `oltp_cross_type_lookup` | `6729.54 ms +- 790.46` | `6602.76 ms +- 1060.47` |
| `oltp_delete_type1_edge` | `6923.46 ms +- 1844.54` | `6495.10 ms +- 771.99` |
| `oltp_delete_type1_node` | `6866.12 ms +- 1643.42` | `6574.41 ms +- 764.95` |
| `oltp_merge_cross_type_edge` | `6808.68 ms +- 1641.11` | `5941.03 ms +- 24.37` |
| `oltp_program_create_and_link` | `6935.76 ms +- 1282.09` | `6348.18 ms +- 562.37` |
| `oltp_type1_neighbors` | `6635.18 ms +- 1045.41` | `6887.67 ms +- 1460.11` |
| `oltp_type1_point_lookup` | `8172.84 ms +- 3363.95` | `10404.70 ms +- 7457.20` |
| `oltp_unwind_literal_top2` | `6823.04 ms +- 1276.96` | `6614.80 ms +- 692.69` |
| `oltp_update_cross_type_edge_rank` | `7134.33 ms +- 1728.27` | `6623.88 ms +- 751.42` |
| `oltp_update_type1_score` | `6841.52 ms +- 1640.47` | `6767.45 ms +- 1085.83` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `185.89 ms +- 65.29` | `1495.97 ms +- 382.34` |
| `oltp_create_type1_node` | `144.32 ms +- 11.56` | `143.00 ms +- 10.71` |
| `oltp_cross_type_lookup` | `241.62 ms +- 128.34` | `955.12 ms +- 85.16` |
| `oltp_delete_type1_edge` | `157.35 ms +- 40.85` | `866.12 ms +- 238.49` |
| `oltp_delete_type1_node` | `155.63 ms +- 43.03` | `754.42 ms +- 90.94` |
| `oltp_merge_cross_type_edge` | `184.93 ms +- 69.09` | `1292.11 ms +- 20.93` |
| `oltp_program_create_and_link` | `165.71 ms +- 44.77` | `781.57 ms +- 92.90` |
| `oltp_type1_neighbors` | `160.83 ms +- 42.91` | `1046.92 ms +- 264.92` |
| `oltp_type1_point_lookup` | `397.34 ms +- 73.28` | `810.81 ms +- 265.62` |
| `oltp_unwind_literal_top2` | `266.67 ms +- 125.49` | `315.51 ms +- 166.63` |
| `oltp_update_cross_type_edge_rank` | `160.00 ms +- 45.89` | `719.22 ms +- 61.53` |
| `oltp_update_type1_score` | `156.17 ms +- 41.34` | `796.10 ms +- 152.24` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `0.52 ms +- 0.04` | `0.58 ms +- 0.08` |
| `oltp_create_type1_node` | `0.50 ms +- 0.01` | `0.69 ms +- 0.11` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.25 ms +- 0.01` | `0.28 ms +- 0.05` |
| `oltp_delete_type1_node` | `0.21 ms +- 0.02` | `0.23 ms +- 0.04` |
| `oltp_merge_cross_type_edge` | `0.52 ms +- 0.03` | `0.45 ms +- 0.03` |
| `oltp_program_create_and_link` | `0.52 ms +- 0.02` | `0.47 ms +- 0.05` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.33 ms +- 0.01` | `0.33 ms +- 0.01` |
| `oltp_update_type1_score` | `0.33 ms +- 0.04` | `0.36 ms +- 0.05` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `8306.50 ms +- 302.13` | `8588.36 ms +- 500.84` |
| `olap_fixed_length_path_projection` | `8306.50 ms +- 302.13` | `8473.26 ms +- 457.26` |
| `olap_fixed_length_path_with_rebinding` | `8070.00 ms +- 470.98` | `8739.70 ms +- 295.90` |
| `olap_graph_introspection_rollup` | `8458.52 ms +- 419.63` | `8440.62 ms +- 274.67` |
| `olap_optional_type1_aggregate` | `7473.40 ms +- 435.53` | `7689.00 ms +- 189.05` |
| `olap_relationship_function_projection` | `8458.52 ms +- 419.63` | `8639.23 ms +- 426.70` |
| `olap_three_type_path_count` | `8306.50 ms +- 302.13` | `8473.26 ms +- 457.26` |
| `olap_type1_active_leaderboard` | `7473.40 ms +- 435.53` | `7689.00 ms +- 189.05` |
| `olap_type1_age_rollup` | `7473.40 ms +- 435.53` | `7689.00 ms +- 189.05` |
| `olap_type2_score_distribution` | `8306.50 ms +- 302.13` | `8473.26 ms +- 457.26` |
| `olap_variable_length_grouped_max_rollup` | `8493.45 ms +- 432.93` | `8810.46 ms +- 364.66` |
| `olap_variable_length_grouped_rollup` | `8458.52 ms +- 419.63` | `8639.23 ms +- 426.70` |
| `olap_variable_length_reachability` | `8306.50 ms +- 302.13` | `8554.08 ms +- 451.99` |
| `olap_with_scalar_rebinding` | `8458.52 ms +- 419.63` | `8639.23 ms +- 426.70` |
| `olap_with_size_predicate_projection` | `8458.52 ms +- 419.63` | `8639.23 ms +- 426.70` |
| `olap_with_where_lower_projection` | `8458.52 ms +- 419.63` | `8639.23 ms +- 426.70` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

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

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.72 ms +- 0.04` | `237.72 ms +- 16.59` | `2.70 ms +- 0.23` | `9.58 ms +- 0.50` | `1.95 ms +- 0.03` | `230.77 ms +- 8.92` | `0.29 ms +- 0.02` | `699.71 ms +- 7.45` | `0.18 ms +- 0.01` | `910.85 ms +- 13.17` | `10.99 ms +- 0.00` |
| `oltp_create_type1_node` | `0.93 ms +- 0.02` | `0.81 ms +- 0.03` | `1.49 ms +- 0.08` | `1.65 ms +- 0.14` | `1.06 ms +- 0.07` | `1.06 ms +- 0.04` | `0.23 ms +- 0.02` | `0.22 ms +- 0.01` | `0.03 ms +- 0.00` | `0.02 ms +- 0.01` | `0.41 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.39 ms +- 0.02` | - | `5.88 ms +- 0.42` | `6.45 ms +- 0.63` | `1.82 ms +- 0.06` | `344.61 ms +- 29.01` | `0.25 ms +- 0.01` | - | `0.06 ms +- 0.01` | `443.23 ms +- 8.10` | `7.03 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.88 ms +- 0.02` | `757.51 ms +- 0.00` | `5.92 ms +- 0.47` | `6.51 ms +- 0.70` | `1.31 ms +- 0.04` | `1018.11 ms +- 0.00` | `0.22 ms +- 0.02` | `342.35 ms +- 29.00` | `0.07 ms +- 0.01` | `449.94 ms +- 6.10` | `6.15 ms +- 0.00` |
| `oltp_delete_type1_node` | `0.90 ms +- 0.02` | - | `0.91 ms +- 0.03` | `4.18 ms +- 0.39` | `0.86 ms +- 0.02` | - | `0.26 ms +- 0.02` | `371.31 ms +- 24.55` | `0.08 ms +- 0.01` | `447.25 ms +- 9.22` | `5.94 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `1.98 ms +- 0.07` | `908.46 ms +- 0.00` | `3.33 ms +- 0.28` | `10.53 ms +- 0.74` | `2.35 ms +- 0.05` | `953.25 ms +- 0.00` | `0.31 ms +- 0.02` | `714.68 ms +- 27.81` | `0.18 ms +- 0.00` | `907.58 ms +- 17.77` | `15.38 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.10 ms +- 0.07` | `125.41 ms +- 12.71` | `3.53 ms +- 0.31` | `7.32 ms +- 0.49` | `2.44 ms +- 0.04` | `64.09 ms +- 1.33` | `0.24 ms +- 0.02` | `366.51 ms +- 25.43` | `0.20 ms +- 0.01` | `445.66 ms +- 7.43` | `5.90 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.17 ms +- 0.03` | `717.85 ms +- 0.00` | `5.66 ms +- 0.38` | `6.05 ms +- 0.64` | `1.57 ms +- 0.04` | `347.41 ms +- 24.33` | `0.27 ms +- 0.02` | - | `0.06 ms +- 0.01` | `442.52 ms +- 6.69` | `6.97 ms +- 0.00` |
| `oltp_type1_point_lookup` | `1.14 ms +- 0.02` | `123.13 ms +- 15.42` | `1.39 ms +- 0.02` | `4.97 ms +- 0.78` | `1.37 ms +- 0.03` | `50.55 ms +- 3.30` | `0.27 ms +- 0.02` | `319.79 ms +- 10.29` | `0.06 ms +- 0.01` | `443.21 ms +- 7.59` | `5.88 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `1.00 ms +- 0.02` | `1.00 ms +- 0.04` | `1.27 ms +- 0.05` | `1.35 ms +- 0.05` | `1.22 ms +- 0.03` | `1.23 ms +- 0.05` | `0.22 ms +- 0.02` | `0.22 ms +- 0.01` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.56 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.20 ms +- 0.03` | `784.90 ms +- 49.47` | `6.66 ms +- 0.34` | `6.19 ms +- 0.40` | `1.50 ms +- 0.02` | `947.03 ms +- 44.54` | `0.23 ms +- 0.03` | `355.98 ms +- 16.20` | `0.05 ms +- 0.00` | `447.20 ms +- 8.91` | `6.18 ms +- 0.00` |
| `oltp_update_type1_score` | `0.84 ms +- 0.02` | `122.13 ms +- 10.95` | `1.70 ms +- 0.09` | `4.81 ms +- 0.63` | `1.08 ms +- 0.03` | `113.56 ms +- 2.79` | `0.23 ms +- 0.03` | `355.32 ms +- 15.00` | `0.08 ms +- 0.02` | `446.62 ms +- 6.86` | `5.48 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.94 ms +- 0.22` | `255.51 ms +- 18.25` | `3.39 ms +- 0.62` | `12.05 ms +- 0.36` | `2.73 ms +- 0.15` | `285.81 ms +- 32.73` | `0.44 ms +- 0.07` | `743.44 ms +- 34.20` | `0.32 ms +- 0.17` | `926.14 ms +- 15.95` | `41.55 ms +- 0.00` |
| `oltp_create_type1_node` | `1.10 ms +- 0.14` | `0.95 ms +- 0.14` | `1.86 ms +- 0.26` | `2.13 ms +- 0.31` | `1.53 ms +- 0.28` | `1.63 ms +- 0.03` | `0.35 ms +- 0.07` | `0.36 ms +- 0.05` | `0.08 ms +- 0.01` | `0.05 ms +- 0.02` | `0.61 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.66 ms +- 0.24` | - | `7.55 ms +- 0.53` | `8.08 ms +- 0.65` | `2.44 ms +- 0.22` | `379.63 ms +- 17.80` | `0.37 ms +- 0.06` | - | `0.07 ms +- 0.01` | `462.43 ms +- 27.25` | `49.51 ms +- 0.00` |
| `oltp_delete_type1_edge` | `1.03 ms +- 0.19` | `805.68 ms +- 0.00` | `7.54 ms +- 0.45` | `8.20 ms +- 0.87` | `1.95 ms +- 0.18` | `1137.52 ms +- 0.00` | `0.33 ms +- 0.06` | `362.03 ms +- 42.72` | `0.11 ms +- 0.02` | `469.23 ms +- 23.93` | `47.89 ms +- 0.00` |
| `oltp_delete_type1_node` | `1.07 ms +- 0.19` | - | `1.13 ms +- 0.24` | `5.73 ms +- 0.32` | `1.26 ms +- 0.12` | - | `0.39 ms +- 0.07` | `388.96 ms +- 35.11` | `0.11 ms +- 0.01` | `473.07 ms +- 37.40` | `28.24 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `2.35 ms +- 0.33` | `964.49 ms +- 0.00` | `4.00 ms +- 0.69` | `13.21 ms +- 0.68` | `3.17 ms +- 0.26` | `994.98 ms +- 0.00` | `0.45 ms +- 0.09` | `765.38 ms +- 23.26` | `0.21 ms +- 0.01` | `921.35 ms +- 17.01` | `45.35 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.41 ms +- 0.30` | `134.91 ms +- 14.63` | `4.21 ms +- 0.59` | `8.99 ms +- 0.68` | `3.26 ms +- 0.24` | `74.16 ms +- 4.35` | `0.35 ms +- 0.08` | `382.55 ms +- 29.51` | `0.21 ms +- 0.02` | `456.51 ms +- 10.09` | `26.38 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.43 ms +- 0.24` | `749.89 ms +- 0.00` | `7.33 ms +- 0.42` | `7.71 ms +- 0.76` | `2.06 ms +- 0.23` | `382.13 ms +- 13.56` | `0.40 ms +- 0.07` | - | `0.09 ms +- 0.01` | `452.68 ms +- 10.15` | `49.69 ms +- 0.00` |
| `oltp_type1_point_lookup` | `1.35 ms +- 0.20` | `130.44 ms +- 16.29` | `1.56 ms +- 0.16` | `6.38 ms +- 0.78` | `1.80 ms +- 0.06` | `55.34 ms +- 3.57` | `0.46 ms +- 0.07` | `336.77 ms +- 13.52` | `0.12 ms +- 0.01` | `469.33 ms +- 37.16` | `35.28 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `1.13 ms +- 0.12` | `1.13 ms +- 0.15` | `1.44 ms +- 0.24` | `1.70 ms +- 0.21` | `1.66 ms +- 0.12` | `1.68 ms +- 0.22` | `0.33 ms +- 0.07` | `0.30 ms +- 0.02` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.75 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.43 ms +- 0.25` | `824.71 ms +- 70.27` | `8.26 ms +- 0.50` | `7.80 ms +- 0.40` | `2.15 ms +- 0.08` | `986.40 ms +- 46.04` | `0.34 ms +- 0.07` | `373.03 ms +- 19.76` | `0.06 ms +- 0.01` | `455.44 ms +- 9.36` | `47.69 ms +- 0.00` |
| `oltp_update_type1_score` | `0.95 ms +- 0.08` | `137.59 ms +- 14.85` | `2.19 ms +- 0.23` | `6.28 ms +- 0.52` | `1.55 ms +- 0.14` | `135.05 ms +- 19.16` | `0.36 ms +- 0.07` | `377.69 ms +- 19.30` | `0.14 ms +- 0.05` | `454.17 ms +- 8.13` | `31.01 ms +- 0.00` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.39 ms +- 0.24` | `272.56 ms +- 29.47` | `4.14 ms +- 1.02` | `13.00 ms +- 0.71` | `3.35 ms +- 0.43` | `312.88 ms +- 53.07` | `0.64 ms +- 0.17` | `811.81 ms +- 71.52` | `0.37 ms +- 0.17` | `935.56 ms +- 19.39` | `44.61 ms +- 0.00` |
| `oltp_create_type1_node` | `1.38 ms +- 0.13` | `1.18 ms +- 0.30` | `2.23 ms +- 0.46` | `2.72 ms +- 0.31` | `1.91 ms +- 0.35` | `2.07 ms +- 0.17` | `0.52 ms +- 0.16` | `0.54 ms +- 0.12` | `0.09 ms +- 0.01` | `0.07 ms +- 0.03` | `0.67 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.95 ms +- 0.41` | - | `8.41 ms +- 0.94` | `9.01 ms +- 0.91` | `2.93 ms +- 0.31` | `400.81 ms +- 6.85` | `0.51 ms +- 0.12` | - | `0.10 ms +- 0.03` | `507.78 ms +- 93.87` | `53.42 ms +- 0.00` |
| `oltp_delete_type1_edge` | `1.22 ms +- 0.29` | `831.51 ms +- 0.00` | `8.35 ms +- 0.69` | `9.04 ms +- 1.29` | `2.56 ms +- 0.38` | `1236.03 ms +- 0.00` | `0.49 ms +- 0.13` | `428.52 ms +- 130.34` | `0.14 ms +- 0.02` | `488.18 ms +- 46.33` | `53.19 ms +- 0.00` |
| `oltp_delete_type1_node` | `1.35 ms +- 0.32` | - | `1.43 ms +- 0.31` | `6.11 ms +- 0.53` | `1.70 ms +- 0.21` | - | `0.55 ms +- 0.15` | `405.32 ms +- 38.56` | `0.13 ms +- 0.00` | `484.81 ms +- 45.88` | `32.69 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | `2.74 ms +- 0.40` | `1014.09 ms +- 0.00` | `4.68 ms +- 0.98` | `14.45 ms +- 0.90` | `3.81 ms +- 0.41` | `1035.29 ms +- 0.00` | `0.71 ms +- 0.24` | `817.79 ms +- 38.11` | `0.24 ms +- 0.02` | `928.40 ms +- 15.68` | `48.62 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.82 ms +- 0.43` | `141.00 ms +- 16.52` | `4.85 ms +- 0.71` | `10.05 ms +- 1.08` | `3.89 ms +- 0.44` | `86.05 ms +- 2.71` | `0.49 ms +- 0.16` | `395.98 ms +- 31.98` | `0.24 ms +- 0.03` | `473.66 ms +- 24.85` | `31.89 ms +- 0.00` |
| `oltp_type1_neighbors` | `1.72 ms +- 0.39` | `768.88 ms +- 0.00` | `8.02 ms +- 0.53` | `8.55 ms +- 1.06` | `2.50 ms +- 0.30` | `403.83 ms +- 4.23` | `0.58 ms +- 0.14` | - | `0.13 ms +- 0.01` | `462.48 ms +- 13.09` | `54.03 ms +- 0.00` |
| `oltp_type1_point_lookup` | `1.63 ms +- 0.33` | `135.44 ms +- 17.20` | `1.91 ms +- 0.37` | `7.05 ms +- 0.91` | `2.14 ms +- 0.09` | `61.99 ms +- 3.09` | `0.88 ms +- 0.42` | `350.68 ms +- 11.66` | `0.15 ms +- 0.01` | `483.66 ms +- 46.41` | `40.94 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `1.36 ms +- 0.29` | `1.32 ms +- 0.21` | `1.78 ms +- 0.40` | `2.17 ms +- 0.26` | `2.00 ms +- 0.19` | `2.06 ms +- 0.17` | `0.48 ms +- 0.14` | `0.41 ms +- 0.06` | `0.03 ms +- 0.00` | `0.03 ms +- 0.00` | `0.86 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.72 ms +- 0.43` | `877.27 ms +- 138.15` | `9.29 ms +- 1.00` | `8.70 ms +- 0.39` | `2.70 ms +- 0.12` | `1031.49 ms +- 56.42` | `0.49 ms +- 0.15` | `387.61 ms +- 23.11` | `0.07 ms +- 0.01` | `461.10 ms +- 7.41` | `51.58 ms +- 0.00` |
| `oltp_update_type1_score` | `1.16 ms +- 0.19` | `157.36 ms +- 37.47` | `2.69 ms +- 0.31` | `6.99 ms +- 0.75` | `1.94 ms +- 0.21` | `148.79 ms +- 29.04` | `0.52 ms +- 0.13` | `398.18 ms +- 9.77` | `0.17 ms +- 0.07` | `462.83 ms +- 10.24` | `36.00 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `14334.80 ms +- 938.99` | `2413.09 ms +- 53.82` | `48.21 ms +- 3.37` | `49.04 ms +- 2.29` | `3023.05 ms +- 1706.98` | `2583.15 ms +- 152.11` | `10483.79 ms +- 769.30` | `10640.09 ms +- 721.49` | `9531.07 ms +- 391.64` | `9682.50 ms +- 300.68` | `112.25 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `1.89 ms +- 0.08` | `14530.51 ms +- 605.28` | `1417.11 ms +- 55.85` | `1446.14 ms +- 42.14` | `221.54 ms +- 13.46` | `12837.97 ms +- 5073.98` | `15969.32 ms +- 605.74` | `15467.77 ms +- 330.01` | - | - | `5299.89 ms +- 0.00` |
| `olap_fixed_length_path_with_rebinding` | `20202.04 ms +- 654.37` | `15847.30 ms +- 394.77` | `813.09 ms +- 29.67` | `836.34 ms +- 13.35` | `4757.14 ms +- 725.35` | `7394.72 ms +- 1807.54` | `45516.75 ms +- 1300.42` | `43424.43 ms +- 894.65` | - | - | `4538.13 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `1.86 ms +- 0.08` | `1335.42 ms +- 28.78` | `21.94 ms +- 1.69` | `21.23 ms +- 1.91` | `42.67 ms +- 3.95` | `2676.83 ms +- 1186.90` | `8300.53 ms +- 258.32` | `7834.34 ms +- 328.59` | `14511.29 ms +- 285.70` | `14441.44 ms +- 66.92` | `404.58 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `158.27 ms +- 1.70` | `261.70 ms +- 10.60` | `7.80 ms +- 0.70` | `8.31 ms +- 0.53` | `193.71 ms +- 7.44` | `186.16 ms +- 10.42` | `1304.47 ms +- 86.19` | `1317.72 ms +- 64.13` | - | - | `7.99 ms +- 0.00` |
| `olap_relationship_function_projection` | `6466.58 ms +- 343.54` | `1951.10 ms +- 52.92` | `100.23 ms +- 3.90` | `101.91 ms +- 2.52` | `5669.61 ms +- 1639.42` | `5321.90 ms +- 723.60` | `8440.24 ms +- 565.78` | `7621.06 ms +- 485.02` | `9797.34 ms +- 181.48` | `10015.44 ms +- 176.43` | `595.62 ms +- 0.00` |
| `olap_three_type_path_count` | `15783.96 ms +- 510.37` | `9988.94 ms +- 411.81` | `246.33 ms +- 8.16` | `257.47 ms +- 8.53` | `7343.18 ms +- 980.16` | `6916.59 ms +- 2329.22` | `5349.66 ms +- 171.86` | `5525.85 ms +- 218.36` | `122.93 ms +- 31.75` | `138.29 ms +- 34.21` | `67.54 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `3.68 ms +- 0.46` | `141.95 ms +- 9.76` | `11.03 ms +- 0.49` | `11.52 ms +- 0.42` | `64.95 ms +- 2.97` | `62.95 ms +- 1.16` | `1235.02 ms +- 72.93` | `1275.40 ms +- 80.32` | `1151.49 ms +- 39.97` | `1035.54 ms +- 46.28` | `6.75 ms +- 0.00` |
| `olap_type1_age_rollup` | `1615.47 ms +- 90.30` | `269.22 ms +- 10.29` | `7.05 ms +- 0.68` | `7.40 ms +- 0.34` | `80.64 ms +- 5.47` | `78.47 ms +- 3.40` | `1492.97 ms +- 108.47` | `1505.03 ms +- 76.74` | `631.96 ms +- 84.00` | `702.59 ms +- 56.78` | `4.98 ms +- 0.00` |
| `olap_type2_score_distribution` | `77.83 ms +- 2.01` | `276.04 ms +- 9.11` | `8.52 ms +- 0.68` | `8.77 ms +- 0.28` | `50.05 ms +- 2.77` | `80.46 ms +- 2.89` | `1510.93 ms +- 230.56` | `1366.99 ms +- 89.77` | `647.48 ms +- 47.87` | `621.39 ms +- 7.13` | `4.89 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `2.67 ms +- 0.38` | `8231.43 ms +- 243.78` | `18.53 ms +- 1.34` | `22.06 ms +- 0.93` | `5.78 ms +- 0.30` | `2169.81 ms +- 110.58` | `0.50 ms +- 0.07` | `346.47 ms +- 18.36` | `0.15 ms +- 0.01` | `541.61 ms +- 36.55` | `9.95 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | `5842.29 ms +- 208.22` | `5953.15 ms +- 143.43` | `77266.29 ms +- 2426.86` | `80116.14 ms +- 2190.15` | - | - | - | - | - |
| `olap_variable_length_reachability` | `6.32 ms +- 1.61` | `68825.50 ms +- 2865.57` | `486.41 ms +- 39.33` | `518.37 ms +- 43.74` | `4816.13 ms +- 1954.01` | `14382.55 ms +- 950.12` | `1.69 ms +- 0.04` | - | `1.74 ms +- 0.03` | `466.45 ms +- 86.66` | `8.63 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `1590.61 ms +- 99.93` | `331.04 ms +- 6.36` | `10.33 ms +- 0.41` | `10.73 ms +- 0.56` | `121.55 ms +- 7.98` | `109.56 ms +- 2.27` | `1759.12 ms +- 234.93` | `1540.62 ms +- 86.04` | `833.17 ms +- 35.54` | `916.30 ms +- 164.83` | `10.40 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `186.00 ms +- 4.68` | `188.03 ms +- 5.03` | `11.98 ms +- 0.52` | `11.92 ms +- 0.52` | `120.51 ms +- 8.05` | `116.57 ms +- 10.42` | `1094.37 ms +- 110.89` | `966.01 ms +- 32.83` | `1009.91 ms +- 57.84` | `1020.48 ms +- 27.57` | `72.22 ms +- 0.00` |
| `olap_with_where_lower_projection` | `1336.91 ms +- 69.98` | `195.17 ms +- 5.08` | `11.85 ms +- 0.65` | `12.21 ms +- 0.87` | `114.01 ms +- 10.35` | `103.51 ms +- 2.29` | `1070.85 ms +- 115.46` | `948.60 ms +- 39.02` | `872.68 ms +- 47.66` | `874.97 ms +- 19.76` | `13.67 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `14638.37 ms +- 729.89` | `2556.93 ms +- 175.58` | `53.60 ms +- 5.19` | `54.08 ms +- 2.08` | `4709.48 ms +- 2613.02` | `3576.36 ms +- 119.71` | `10921.58 ms +- 663.62` | `10891.21 ms +- 884.15` | `9738.48 ms +- 380.43` | `10108.24 ms +- 489.67` | `162.31 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `2.13 ms +- 0.18` | `14850.74 ms +- 704.91` | `1444.76 ms +- 67.39` | `1487.95 ms +- 56.12` | `655.70 ms +- 553.13` | `16476.97 ms +- 6249.69` | `16660.27 ms +- 263.74` | `15905.57 ms +- 570.05` | - | - | `5626.00 ms +- 0.00` |
| `olap_fixed_length_path_with_rebinding` | `20349.62 ms +- 724.59` | `16072.40 ms +- 423.11` | `844.38 ms +- 37.84` | `860.77 ms +- 20.71` | `11096.31 ms +- 11560.25` | `10703.34 ms +- 2876.08` | `49936.07 ms +- 5168.86` | `44291.07 ms +- 1026.95` | - | - | `4826.53 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `1.89 ms +- 0.10` | `1354.78 ms +- 27.51` | `24.82 ms +- 2.95` | `24.60 ms +- 2.93` | `55.56 ms +- 21.72` | `4062.81 ms +- 2152.47` | `8414.02 ms +- 287.15` | `7928.40 ms +- 362.15` | `14717.31 ms +- 365.96` | `14621.39 ms +- 92.17` | `439.93 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `164.36 ms +- 1.42` | `267.61 ms +- 13.46` | `9.67 ms +- 0.76` | `10.40 ms +- 0.71` | `221.43 ms +- 23.52` | `197.03 ms +- 18.13` | `1346.04 ms +- 108.14` | `1342.44 ms +- 71.25` | - | - | `64.21 ms +- 0.00` |
| `olap_relationship_function_projection` | `6541.25 ms +- 386.75` | `1989.36 ms +- 59.98` | `108.10 ms +- 7.80` | `105.95 ms +- 4.04` | `7169.38 ms +- 2258.29` | `7551.87 ms +- 2625.83` | `8724.73 ms +- 703.76` | `7956.95 ms +- 705.88` | `9981.97 ms +- 312.51` | `10419.46 ms +- 407.76` | `632.28 ms +- 0.00` |
| `olap_three_type_path_count` | `15994.56 ms +- 590.26` | `10274.07 ms +- 586.69` | `277.38 ms +- 32.77` | `308.16 ms +- 11.01` | `8784.38 ms +- 2141.59` | `9692.59 ms +- 3510.83` | `5815.87 ms +- 123.91` | `6043.29 ms +- 203.58` | `135.66 ms +- 38.80` | `4387.03 ms +- 4617.94` | `87.43 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `4.44 ms +- 0.93` | `147.85 ms +- 11.37` | `13.34 ms +- 0.74` | `14.13 ms +- 0.97` | `78.65 ms +- 11.07` | `71.37 ms +- 8.08` | `1264.50 ms +- 75.95` | `1306.66 ms +- 89.70` | `2849.15 ms +- 2208.09` | `3739.72 ms +- 1257.16` | `40.15 ms +- 0.00` |
| `olap_type1_age_rollup` | `1660.44 ms +- 79.23` | `276.49 ms +- 12.84` | `8.84 ms +- 0.80` | `8.88 ms +- 0.25` | `180.39 ms +- 152.07` | `87.65 ms +- 10.02` | `1556.64 ms +- 157.85` | `1528.20 ms +- 83.92` | `864.26 ms +- 84.78` | `954.22 ms +- 71.15` | `20.02 ms +- 0.00` |
| `olap_type2_score_distribution` | `80.28 ms +- 3.00` | `287.23 ms +- 19.09` | `9.52 ms +- 1.15` | `10.37 ms +- 0.02` | `53.84 ms +- 4.09` | `89.38 ms +- 9.89` | `1545.51 ms +- 227.30` | `1390.74 ms +- 97.09` | `685.62 ms +- 88.92` | `683.03 ms +- 30.13` | `18.52 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `2.91 ms +- 0.51` | `8507.29 ms +- 424.53` | `20.62 ms +- 2.15` | `24.59 ms +- 2.08` | `7.02 ms +- 1.04` | `2359.25 ms +- 156.13` | `0.92 ms +- 0.22` | `359.81 ms +- 24.86` | `0.20 ms +- 0.02` | `783.44 ms +- 131.46` | `32.96 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | `5943.48 ms +- 254.61` | `6199.21 ms +- 128.53` | `83765.46 ms +- 6595.98` | `86857.11 ms +- 4267.28` | - | - | - | - | - |
| `olap_variable_length_reachability` | `7.06 ms +- 2.23` | `72000.38 ms +- 5126.20` | `520.05 ms +- 39.84` | `554.93 ms +- 55.16` | `6825.51 ms +- 2233.91` | `16869.31 ms +- 2080.08` | `2.53 ms +- 0.42` | - | `1.90 ms +- 0.07` | `540.65 ms +- 186.18` | `26.68 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `1621.20 ms +- 119.04` | `342.53 ms +- 8.17` | `12.68 ms +- 0.28` | `13.82 ms +- 0.47` | `135.42 ms +- 21.89` | `117.47 ms +- 1.43` | `1819.64 ms +- 221.09` | `1578.76 ms +- 87.21` | `867.84 ms +- 24.73` | `1040.86 ms +- 309.55` | `43.82 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `192.90 ms +- 5.83` | `193.21 ms +- 8.37` | `13.69 ms +- 1.52` | `14.31 ms +- 1.31` | `134.49 ms +- 22.92` | `123.91 ms +- 12.44` | `1172.80 ms +- 110.22` | `988.23 ms +- 35.64` | `1070.02 ms +- 54.42` | `1104.82 ms +- 23.55` | `83.45 ms +- 0.00` |
| `olap_with_where_lower_projection` | `1365.83 ms +- 72.81` | `200.37 ms +- 8.36` | `14.41 ms +- 1.87` | `14.87 ms +- 0.63` | `124.62 ms +- 21.41` | `114.85 ms +- 12.59` | `1149.89 ms +- 128.41` | `971.18 ms +- 45.55` | `914.74 ms +- 40.79` | `1034.86 ms +- 191.51` | `50.50 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `14728.60 ms +- 721.31` | `2602.90 ms +- 230.35` | `56.10 ms +- 7.41` | `62.79 ms +- 10.32` | `5523.20 ms +- 3730.47` | `3736.60 ms +- 90.34` | `11266.70 ms +- 754.64` | `10969.18 ms +- 877.06` | `10320.61 ms +- 193.00` | `10840.34 ms +- 675.14` | `166.18 ms +- 0.00` |
| `olap_fixed_length_path_projection` | `2.21 ms +- 0.26` | `15078.91 ms +- 862.36` | `1456.18 ms +- 69.83` | `1508.81 ms +- 67.34` | `944.52 ms +- 646.51` | `17761.73 ms +- 8013.22` | `16825.14 ms +- 189.48` | `16430.67 ms +- 893.95` | - | - | `5747.57 ms +- 0.00` |
| `olap_fixed_length_path_with_rebinding` | `20400.74 ms +- 785.26` | `16160.57 ms +- 463.68` | `853.71 ms +- 35.76` | `871.04 ms +- 20.46` | `12256.42 ms +- 13469.30` | `11637.28 ms +- 4019.27` | `51690.85 ms +- 7045.64` | `45109.11 ms +- 1966.18` | - | - | `4969.89 ms +- 0.00` |
| `olap_graph_introspection_rollup` | `1.92 ms +- 0.14` | `1373.23 ms +- 26.30` | `28.75 ms +- 5.98` | `27.14 ms +- 3.25` | `956.92 ms +- 1579.66` | `4407.59 ms +- 2014.75` | `8558.15 ms +- 219.21` | `8009.92 ms +- 419.41` | `14861.02 ms +- 418.74` | `14835.53 ms +- 274.05` | `462.96 ms +- 0.00` |
| `olap_optional_type1_aggregate` | `199.79 ms +- 55.83` | `284.59 ms +- 25.49` | `9.99 ms +- 0.63` | `11.77 ms +- 1.92` | `243.94 ms +- 41.35` | `200.95 ms +- 18.49` | `1547.60 ms +- 416.12` | `1838.10 ms +- 745.86` | - | - | `66.50 ms +- 0.00` |
| `olap_relationship_function_projection` | `6589.09 ms +- 433.20` | `2008.52 ms +- 74.46` | `118.15 ms +- 16.36` | `115.76 ms +- 7.35` | `7610.63 ms +- 2676.90` | `8502.51 ms +- 2873.78` | `8799.31 ms +- 770.95` | `8066.11 ms +- 754.12` | `10001.03 ms +- 333.15` | `10658.62 ms +- 734.65` | `668.37 ms +- 0.00` |
| `olap_three_type_path_count` | `16038.47 ms +- 572.94` | `10329.12 ms +- 612.17` | `283.51 ms +- 32.91` | `313.65 ms +- 11.96` | `9407.91 ms +- 2882.66` | `10843.66 ms +- 4322.00` | `6182.19 ms +- 338.84` | `6239.90 ms +- 128.52` | `143.24 ms +- 43.96` | `6192.88 ms +- 5682.59` | `92.27 ms +- 0.00` |
| `olap_type1_active_leaderboard` | `4.83 ms +- 1.23` | `152.42 ms +- 12.21` | `14.19 ms +- 0.99` | `15.25 ms +- 1.42` | `90.83 ms +- 21.76` | `82.94 ms +- 20.44` | `1381.49 ms +- 214.00` | `1769.45 ms +- 704.92` | `3848.75 ms +- 2638.04` | `6097.21 ms +- 2417.34` | `41.91 ms +- 0.00` |
| `olap_type1_age_rollup` | `1710.03 ms +- 105.47` | `289.76 ms +- 21.69` | `9.20 ms +- 1.31` | `9.40 ms +- 0.42` | `211.86 ms +- 192.74` | `111.44 ms +- 42.46` | `1685.00 ms +- 350.22` | `1538.72 ms +- 85.60` | `941.73 ms +- 73.59` | `1080.66 ms +- 172.50` | `29.03 ms +- 0.00` |
| `olap_type2_score_distribution` | `82.48 ms +- 3.38` | `291.87 ms +- 22.41` | `10.21 ms +- 0.96` | `10.91 ms +- 0.15` | `56.74 ms +- 2.43` | `92.82 ms +- 9.69` | `1625.27 ms +- 227.86` | `1467.22 ms +- 72.80` | `712.90 ms +- 70.58` | `708.62 ms +- 31.23` | `27.18 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `2.97 ms +- 0.54` | `8656.28 ms +- 568.35` | `22.23 ms +- 2.70` | `26.13 ms +- 2.17` | `10.33 ms +- 5.82` | `2402.62 ms +- 179.48` | `2.67 ms +- 0.28` | `372.17 ms +- 28.77` | `0.28 ms +- 0.08` | `918.31 ms +- 230.64` | `35.52 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | `6003.40 ms +- 299.21` | `6325.47 ms +- 208.20` | `84756.93 ms +- 6566.56` | `91866.96 ms +- 2237.40` | - | - | - | - | - |
| `olap_variable_length_reachability` | `9.19 ms +- 4.37` | `72863.20 ms +- 5559.52` | `546.98 ms +- 25.54` | `587.62 ms +- 58.49` | `7509.05 ms +- 2289.25` | `17643.09 ms +- 3023.30` | `4.19 ms +- 1.32` | - | `1.98 ms +- 0.09` | `606.78 ms +- 270.37` | `29.53 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `1650.29 ms +- 158.72` | `349.71 ms +- 11.60` | `13.48 ms +- 0.35` | `14.36 ms +- 0.67` | `146.96 ms +- 38.21` | `124.02 ms +- 3.00` | `2326.93 ms +- 820.98` | `1638.61 ms +- 141.44` | `899.66 ms +- 35.47` | `1082.27 ms +- 324.46` | `46.65 ms +- 0.00` |
| `olap_with_size_predicate_projection` | `197.92 ms +- 9.20` | `199.41 ms +- 12.67` | `14.10 ms +- 1.65` | `14.97 ms +- 1.29` | `140.21 ms +- 27.21` | `126.26 ms +- 13.67` | `1246.15 ms +- 172.93` | `1093.66 ms +- 68.85` | `1084.19 ms +- 67.04` | `1132.51 ms +- 27.80` | `87.08 ms +- 0.00` |
| `olap_with_where_lower_projection` | `1378.79 ms +- 66.43` | `203.64 ms +- 10.85` | `15.49 ms +- 2.08` | `15.50 ms +- 0.74` | `131.46 ms +- 31.60` | `124.70 ms +- 23.38` | `1166.05 ms +- 135.40` | `1085.63 ms +- 64.25` | `933.29 ms +- 51.13` | `1166.55 ms +- 398.34` | `53.32 ms +- 0.00` |

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
tables below, that checkpoint time is shown in the `Analyze / Checkpoint` column so
the setup layout stays consistent across engines.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
cross-engine setup tables below, that engine-specific post-load work is omitted so
only shared phases appear side by side. The ArcadeDB-specific timing tables below break out
`GAV` separately and keep `Analyze / Checkpoint` as a shared bucket.


Wall-clock run time per completed result file:

| Combo | Wall-clock run time |
| --- | --- |
| SQLite Indexed (3) | `8387.61 s +- 66.26` |
| SQLite Unindexed (3) | `12443.87 s +- 93.21` |
| DuckDB Indexed (3) | `435.13 s +- 17.47` |
| DuckDB Unindexed (3) | `460.26 s +- 8.89` |
| PostgreSQL Indexed (3) | `1381.77 s +- 62.41` |
| PostgreSQL Unindexed (3) | `4213.88 s +- 543.44` |
| Neo4j Indexed (3) | `1503.51 s +- 16.80` |
| Neo4j Unindexed (3) | `6557.23 s +- 114.36` |
| ArcadeDB Indexed (3) | `3162.86 s +- 106.31` |
| ArcadeDB Unindexed (3) | `5696.20 s +- 7.91` |
| LadybugDB Unindexed (3) | `12346.62 s +- 386.97` |

OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `13.06 ms +- 0.12` | `5.59 ms +- 0.28` | `51556.46 ms +- 2614.13` | `4929.25 ms +- 242.15` | `1509.99 ms +- 27.76` | `1.23 ms +- 0.01` | `1.40 ms +- 0.13` | `1.86 ms +- 0.40` |
| SQLite Unindexed (3) | `9.81 ms +- 2.96` | `6.42 ms +- 1.44` | `52306.67 ms +- 593.73` | `1462.54 ms +- 41.58` | `269.61 ms +- 6.67` | `68.86 ms +- 0.61` | `76.50 ms +- 5.86` | `82.78 ms +- 9.05` |
| DuckDB Indexed (3) | `17.37 ms +- 3.86` | `171.60 ms +- 2.34` | `12056.21 ms +- 268.88` | `4209.23 ms +- 52.76` | `0.36 ms +- 0.04` | `2.52 ms +- 0.17` | `3.33 ms +- 0.46` | `4.34 ms +- 0.97` |
| DuckDB Unindexed (3) | `15.93 ms +- 0.10` | `168.09 ms +- 1.24` | `11714.28 ms +- 87.23` | `91.00 ms +- 5.17` | `0.13 ms +- 0.02` | `3.00 ms +- 0.07` | `3.68 ms +- 0.11` | `4.25 ms +- 0.07` |
| PostgreSQL Indexed (3) | `4.59 ms +- 0.21` | `614.78 ms +- 18.97` | `99684.32 ms +- 6547.31` | `8670.03 ms +- 4876.05` | `2572.96 ms +- 60.44` | `1.49 ms +- 0.04` | `1.86 ms +- 0.29` | `2.35 ms +- 0.40` |
| PostgreSQL Unindexed (3) | `4.90 ms +- 0.45` | `589.91 ms +- 76.02` | `104638.17 ms +- 3311.46` | `75.25 ms +- 4.24` | `4162.39 ms +- 1907.20` | `42.45 ms +- 10.64` | `45.82 ms +- 10.40` | `48.99 ms +- 9.90` |
| Neo4j Indexed (3) | `58.66 ms +- 2.55` | `468.76 ms +- 6.48` | `455055.40 ms +- 5035.10` | `12294.75 ms +- 2326.81` | `0.00 ms +- 0.00` | `0.23 ms +- 0.01` | `0.35 ms +- 0.11` | `0.53 ms +- 0.25` |
| Neo4j Unindexed (3) | `61.82 ms +- 13.84` | `460.85 ms +- 7.92` | `456545.26 ms +- 6858.25` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `77.26 ms +- 1.95` | `86.66 ms +- 4.86` | `93.78 ms +- 8.59` |
| ArcadeDB Indexed (3) | `309.33 ms +- 69.03` | `320.11 ms +- 43.47` | `176167.15 ms +- 5094.71` | `18563.33 ms +- 703.81` | `0.00 ms +- 0.00` | `0.05 ms +- 0.00` | `0.07 ms +- 0.00` | `0.09 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `284.85 ms +- 10.36` | `326.22 ms +- 21.85` | `178029.18 ms +- 4255.84` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `43.08 ms +- 0.40` | `44.59 ms +- 0.29` | `46.71 ms +- 0.26` |
| LadybugDB Unindexed (3) | `80.93 ms +- 5.68` | `68.20 ms +- 0.20` | `4362388.75 ms +- 186486.05` | `0.00 ms +- 0.00` | `27.81 ms +- 8.54` | `4.25 ms +- 0.29` | `5.86 ms +- 0.44` | `6.68 ms +- 0.63` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `7.26 ms +- 0.09` | `5.64 ms +- 0.67` | `52824.45 ms +- 2429.92` | `4838.75 ms +- 207.28` | `1515.51 ms +- 30.54` | `4585.57 ms +- 39.36` | `4787.13 ms +- 135.62` | `4993.76 ms +- 259.59` |
| SQLite Unindexed (3) | `7.28 ms +- 0.10` | `5.44 ms +- 0.47` | `52140.64 ms +- 1044.44` | `1439.95 ms +- 26.45` | `253.43 ms +- 10.22` | `4464.60 ms +- 26.62` | `4665.24 ms +- 96.39` | `4757.78 ms +- 157.45` |
| DuckDB Indexed (3) | `8.68 ms +- 0.18` | `170.83 ms +- 1.15` | `12005.13 ms +- 157.49` | `4221.80 ms +- 58.72` | `0.32 ms +- 0.04` | `70.88 ms +- 1.55` | `74.59 ms +- 2.17` | `76.65 ms +- 2.56` |
| DuckDB Unindexed (3) | `8.69 ms +- 0.56` | `169.56 ms +- 2.49` | `11825.19 ms +- 118.94` | `87.57 ms +- 0.10` | `0.12 ms +- 0.02` | `69.68 ms +- 1.13` | `73.03 ms +- 1.57` | `75.28 ms +- 2.11` |
| PostgreSQL Indexed (3) | `4.54 ms +- 0.40` | `923.54 ms +- 61.98` | `102115.74 ms +- 10920.24` | `7771.72 ms +- 2329.81` | `2494.49 ms +- 43.37` | `519.79 ms +- 14.08` | `596.60 ms +- 35.24` | `620.09 ms +- 36.77` |
| PostgreSQL Unindexed (3) | `4.03 ms +- 0.13` | `790.91 ms +- 21.25` | `102531.28 ms +- 10995.31` | `75.96 ms +- 1.41` | `3427.49 ms +- 838.27` | `635.71 ms +- 22.20` | `749.51 ms +- 98.32` | `800.98 ms +- 106.20` |
| Neo4j Indexed (3) | `58.66 ms +- 2.55` | `468.76 ms +- 6.48` | `455055.40 ms +- 5035.10` | `12294.75 ms +- 2326.81` | `0.00 ms +- 0.00` | `523.54 ms +- 5.21` | `538.03 ms +- 6.55` | `557.56 ms +- 13.37` |
| Neo4j Unindexed (3) | `61.82 ms +- 13.84` | `460.85 ms +- 7.92` | `456545.26 ms +- 6858.25` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `568.31 ms +- 7.57` | `584.68 ms +- 9.89` | `612.74 ms +- 31.38` |
| ArcadeDB Indexed (3) | `1.48 ms +- 0.28` | `51.56 ms +- 3.13` | `197399.88 ms +- 1663.66` | `17326.49 ms +- 110.85` | `0.00 ms +- 0.00` | `1247.19 ms +- 55.42` | `1324.24 ms +- 68.69` | `1368.06 ms +- 67.16` |
| ArcadeDB Unindexed (3) | `1.35 ms +- 0.05` | `62.17 ms +- 14.58` | `200963.54 ms +- 1423.51` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `1199.06 ms +- 19.04` | `1279.35 ms +- 19.11` | `1315.58 ms +- 14.72` |
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
| ArcadeDB Indexed (3) | `160.68 MiB +- 16.74` | `259.60 MiB +- 17.86` | `4271.99 MiB +- 29.87` | `4321.29 MiB +- 25.02` | `4321.36 MiB +- 25.00` | `5187.91 MiB +- 143.92` |
| ArcadeDB Unindexed (3) | `164.96 MiB +- 2.23` | `261.67 MiB +- 18.00` | `4294.50 MiB +- 23.35` | `4294.50 MiB +- 23.35` | `4294.50 MiB +- 23.35` | `4965.90 MiB +- 503.84` |
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
| ArcadeDB Indexed (3) | `5188.46 MiB +- 143.46` | `5189.15 MiB +- 143.48` | `4322.99 MiB +- 31.79` | `4445.46 MiB +- 7.90` | `5057.88 MiB +- 93.99` | `9171.70 MiB +- 162.26` |
| ArcadeDB Unindexed (3) | `4966.04 MiB +- 503.88` | `4966.35 MiB +- 503.85` | `4320.79 MiB +- 42.46` | `4320.79 MiB +- 42.46` | `5095.12 MiB +- 171.59` | `8633.63 MiB +- 186.93` |
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
| `oltp/arcadedb_embedded_indexed` | `0.05 ms +- 0.00` | `0.07 ms +- 0.00` | `0.09 ms +- 0.00` |
| `olap/arcadedb_embedded_indexed` | `1247.19 ms +- 55.42` | `1324.24 ms +- 68.69` | `1368.06 ms +- 67.16` |
| `oltp/arcadedb_embedded_unindexed` | `43.08 ms +- 0.40` | `44.59 ms +- 0.29` | `46.71 ms +- 0.26` |
| `olap/arcadedb_embedded_unindexed` | `1199.06 ms +- 19.04` | `1279.35 ms +- 19.11` | `1315.58 ms +- 14.72` |
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

#### Medium ArcadeDB-specific timings

These tables keep ArcadeDB-only timing phases out of the
cross-engine comparison tables.

ArcadeDB setup hierarchy is `connect/reset -> schema/constraints ->
ingest -> index -> GAV -> analyze/checkpoint`, with `GAV` present only
for OLAP fixtures.

ArcadeDB setup phase breakdown, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `309.33 ms +- 69.03` | `320.11 ms +- 43.47` | `176167.15 ms +- 5094.71` | `18563.33 ms +- 703.81` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `284.85 ms +- 10.36` | `326.22 ms +- 21.85` | `178029.18 ms +- 4255.84` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |

ArcadeDB setup phase breakdown, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `1.48 ms +- 0.28` | `51.56 ms +- 3.13` | `197399.88 ms +- 1663.66` | `17326.49 ms +- 110.85` | `6001.10 ms +- 268.08` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `1.35 ms +- 0.05` | `62.17 ms +- 14.58` | `200963.54 ms +- 1423.51` | `0.00 ms +- 0.00` | `6208.19 ms +- 427.16` | `0.00 ms +- 0.00` |

#### Medium runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.
`open` is already reported here; worker close time is not currently
recorded in the raw benchmark payloads.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `1155.75 ms +- 18.70` | `1118.28 ms +- 62.97` |
| `oltp_create_type1_node` | `1155.51 ms +- 40.66` | `1102.39 ms +- 75.24` |
| `oltp_cross_type_lookup` | `1186.41 ms +- 74.71` | `1150.09 ms +- 67.98` |
| `oltp_delete_type1_edge` | `1157.16 ms +- 47.00` | `1123.09 ms +- 45.74` |
| `oltp_delete_type1_node` | `1144.05 ms +- 23.05` | `1086.74 ms +- 43.69` |
| `oltp_merge_cross_type_edge` | `1191.36 ms +- 88.46` | `1140.02 ms +- 54.97` |
| `oltp_program_create_and_link` | `1335.41 ms +- 303.67` | `1074.97 ms +- 55.10` |
| `oltp_type1_neighbors` | `1163.14 ms +- 89.00` | `1106.14 ms +- 19.38` |
| `oltp_type1_point_lookup` | `1330.81 ms +- 297.97` | `1126.17 ms +- 37.48` |
| `oltp_unwind_literal_top2` | `1135.37 ms +- 28.85` | `1130.14 ms +- 28.23` |
| `oltp_update_cross_type_edge_rank` | `1375.18 ms +- 392.81` | `1098.33 ms +- 75.94` |
| `oltp_update_type1_score` | `1153.83 ms +- 22.80` | `1035.19 ms +- 38.44` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `154.48 ms +- 8.08` | `403.81 ms +- 58.34` |
| `oltp_create_type1_node` | `141.88 ms +- 15.69` | `137.66 ms +- 4.88` |
| `oltp_cross_type_lookup` | `199.10 ms +- 16.83` | `336.66 ms +- 25.27` |
| `oltp_delete_type1_edge` | `140.43 ms +- 10.83` | `325.91 ms +- 25.74` |
| `oltp_delete_type1_node` | `141.78 ms +- 7.03` | `324.45 ms +- 15.29` |
| `oltp_merge_cross_type_edge` | `153.18 ms +- 9.96` | `386.89 ms +- 1.01` |
| `oltp_program_create_and_link` | `160.99 ms +- 25.75` | `342.94 ms +- 38.01` |
| `oltp_type1_neighbors` | `195.24 ms +- 43.21` | `381.59 ms +- 35.07` |
| `oltp_type1_point_lookup` | `196.65 ms +- 26.56` | `374.41 ms +- 72.87` |
| `oltp_unwind_literal_top2` | `255.70 ms +- 39.39` | `187.54 ms +- 41.80` |
| `oltp_update_cross_type_edge_rank` | `143.27 ms +- 7.41` | `328.02 ms +- 35.44` |
| `oltp_update_type1_score` | `142.57 ms +- 7.00` | `348.55 ms +- 15.37` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `0.51 ms +- 0.05` | `0.57 ms +- 0.03` |
| `oltp_create_type1_node` | `0.52 ms +- 0.08` | `0.50 ms +- 0.06` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.32 ms +- 0.04` | `0.26 ms +- 0.03` |
| `oltp_delete_type1_node` | `0.30 ms +- 0.04` | `0.27 ms +- 0.01` |
| `oltp_merge_cross_type_edge` | `0.60 ms +- 0.11` | `0.56 ms +- 0.09` |
| `oltp_program_create_and_link` | `0.60 ms +- 0.11` | `0.52 ms +- 0.05` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.38 ms +- 0.06` | `0.36 ms +- 0.02` |
| `oltp_update_type1_score` | `0.36 ms +- 0.03` | `0.35 ms +- 0.05` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_fixed_length_path_projection` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_fixed_length_path_with_rebinding` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_graph_introspection_rollup` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_optional_type1_aggregate` | `1420.08 ms +- 161.39` | `1420.45 ms +- 208.66` |
| `olap_relationship_function_projection` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_three_type_path_count` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_type1_active_leaderboard` | `1420.08 ms +- 161.39` | `1420.45 ms +- 208.66` |
| `olap_type1_age_rollup` | `1420.08 ms +- 161.39` | `1420.45 ms +- 208.66` |
| `olap_type2_score_distribution` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_variable_length_grouped_max_rollup` | `1470.48 ms +- 189.72` | `1454.05 ms +- 230.44` |
| `olap_variable_length_grouped_rollup` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_variable_length_reachability` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_with_scalar_rebinding` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_with_size_predicate_projection` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |
| `olap_with_where_lower_projection` | `1370.53 ms +- 127.38` | `1420.17 ms +- 126.18` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

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
| `oltp_create_cross_type_edge` | `1.68 ms +- 0.01` | `17.59 ms +- 0.30` | `2.49 ms +- 0.18` | `4.52 ms +- 0.18` | `1.85 ms +- 0.04` | `19.44 ms +- 0.36` | `0.25 ms +- 0.02` | `44.14 ms +- 0.41` | `0.10 ms +- 0.01` | `88.45 ms +- 0.85` | `6.47 ms +- 0.27` |
| `oltp_create_type1_node` | `0.82 ms +- 0.01` | `0.79 ms +- 0.01` | `1.35 ms +- 0.05` | `1.28 ms +- 0.03` | `1.01 ms +- 0.04` | `0.97 ms +- 0.00` | `0.21 ms +- 0.01` | `0.20 ms +- 0.01` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.32 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.36 ms +- 0.00` | `57.71 ms +- 0.58` | `3.67 ms +- 0.28` | `3.50 ms +- 0.18` | `1.76 ms +- 0.03` | `39.84 ms +- 2.08` | `0.23 ms +- 0.01` | `470.36 ms +- 29.14` | `0.02 ms +- 0.00` | `41.00 ms +- 0.48` | `4.91 ms +- 0.40` |
| `oltp_delete_type1_edge` | `0.86 ms +- 0.01` | `57.97 ms +- 0.52` | `3.63 ms +- 0.27` | `3.34 ms +- 0.10` | `1.26 ms +- 0.04` | `86.48 ms +- 1.32` | `0.20 ms +- 0.01` | `21.09 ms +- 0.35` | `0.04 ms +- 0.00` | `43.37 ms +- 0.48` | `4.22 ms +- 0.56` |
| `oltp_delete_type1_node` | `0.88 ms +- 0.02` | `479.30 ms +- 5.74` | `0.91 ms +- 0.01` | `1.90 ms +- 0.03` | `0.83 ms +- 0.03` | `270.82 ms +- 0.00` | `0.24 ms +- 0.01` | `22.66 ms +- 0.28` | `0.07 ms +- 0.01` | `43.45 ms +- 0.55` | `3.67 ms +- 0.15` |
| `oltp_merge_cross_type_edge` | `1.92 ms +- 0.02` | `68.47 ms +- 0.90` | `3.38 ms +- 0.39` | `5.24 ms +- 0.14` | `2.27 ms +- 0.07` | `75.60 ms +- 0.96` | `0.26 ms +- 0.01` | `44.27 ms +- 0.80` | `0.09 ms +- 0.00` | `88.87 ms +- 0.81` | `12.10 ms +- 1.09` |
| `oltp_program_create_and_link` | `1.97 ms +- 0.02` | `9.94 ms +- 0.48` | `3.36 ms +- 0.21` | `4.12 ms +- 0.04` | `2.34 ms +- 0.08` | `10.86 ms +- 0.94` | `0.22 ms +- 0.01` | `22.14 ms +- 0.96` | `0.18 ms +- 0.00` | `44.20 ms +- 0.51` | `3.50 ms +- 0.17` |
| `oltp_type1_neighbors` | `1.14 ms +- 0.00` | `57.54 ms +- 0.08` | `3.36 ms +- 0.22` | `3.15 ms +- 0.07` | `1.53 ms +- 0.03` | `40.30 ms +- 1.62` | `0.25 ms +- 0.01` | `237.73 ms +- 3.04` | `0.03 ms +- 0.00` | `41.16 ms +- 0.32` | `4.75 ms +- 0.33` |
| `oltp_type1_point_lookup` | `1.11 ms +- 0.00` | `9.10 ms +- 0.02` | `1.38 ms +- 0.02` | `2.33 ms +- 0.03` | `1.34 ms +- 0.03` | `7.71 ms +- 0.87` | `0.29 ms +- 0.02` | `20.52 ms +- 1.02` | `0.02 ms +- 0.00` | `41.09 ms +- 0.20` | `3.31 ms +- 0.11` |
| `oltp_unwind_literal_top2` | `0.98 ms +- 0.00` | `0.98 ms +- 0.01` | `1.27 ms +- 0.04` | `1.24 ms +- 0.01` | `1.18 ms +- 0.04` | `1.16 ms +- 0.01` | `0.20 ms +- 0.01` | `0.20 ms +- 0.01` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.57 ms +- 0.02` |
| `oltp_update_cross_type_edge_rank` | `1.16 ms +- 0.01` | `58.25 ms +- 0.50` | `3.97 ms +- 0.48` | `3.20 ms +- 0.08` | `1.46 ms +- 0.06` | `103.79 ms +- 33.86` | `0.21 ms +- 0.01` | `21.58 ms +- 0.94` | `0.03 ms +- 0.00` | `43.87 ms +- 0.46` | `4.09 ms +- 0.38` |
| `oltp_update_type1_score` | `0.81 ms +- 0.01` | `8.62 ms +- 0.16` | `1.53 ms +- 0.06` | `2.18 ms +- 0.04` | `1.03 ms +- 0.03` | `8.61 ms +- 1.77` | `0.20 ms +- 0.01` | `22.18 ms +- 0.54` | `0.03 ms +- 0.00` | `41.42 ms +- 0.55` | `3.14 ms +- 0.13` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.96 ms +- 0.29` | `18.84 ms +- 1.02` | `3.36 ms +- 0.68` | `5.57 ms +- 0.24` | `2.22 ms +- 0.24` | `22.04 ms +- 1.22` | `0.39 ms +- 0.12` | `48.46 ms +- 2.56` | `0.12 ms +- 0.01` | `92.01 ms +- 0.90` | `8.52 ms +- 0.36` |
| `oltp_create_type1_node` | `0.91 ms +- 0.06` | `0.86 ms +- 0.03` | `1.84 ms +- 0.29` | `1.46 ms +- 0.10` | `1.28 ms +- 0.27` | `1.10 ms +- 0.06` | `0.32 ms +- 0.11` | `0.26 ms +- 0.02` | `0.04 ms +- 0.00` | `0.03 ms +- 0.00` | `0.50 ms +- 0.07` |
| `oltp_cross_type_lookup` | `1.60 ms +- 0.22` | `60.75 ms +- 2.23` | `4.86 ms +- 0.61` | `4.25 ms +- 0.28` | `2.13 ms +- 0.26` | `42.36 ms +- 2.85` | `0.33 ms +- 0.09` | `546.73 ms +- 60.06` | `0.04 ms +- 0.00` | `42.04 ms +- 0.38` | `6.62 ms +- 0.69` |
| `oltp_delete_type1_edge` | `0.95 ms +- 0.03` | `60.46 ms +- 1.36` | `4.75 ms +- 0.51` | `4.09 ms +- 0.13` | `1.56 ms +- 0.27` | `92.60 ms +- 2.88` | `0.31 ms +- 0.12` | `23.68 ms +- 0.81` | `0.06 ms +- 0.00` | `44.66 ms +- 0.46` | `5.98 ms +- 0.78` |
| `oltp_delete_type1_node` | `1.03 ms +- 0.06` | `551.71 ms +- 64.85` | `1.09 ms +- 0.10` | `2.61 ms +- 0.05` | `1.04 ms +- 0.13` | `284.64 ms +- 0.00` | `0.38 ms +- 0.18` | `25.15 ms +- 0.71` | `0.10 ms +- 0.01` | `44.63 ms +- 0.54` | `5.47 ms +- 0.34` |
| `oltp_merge_cross_type_edge` | `2.28 ms +- 0.39` | `71.79 ms +- 2.37` | `4.59 ms +- 1.00` | `6.38 ms +- 0.19` | `2.85 ms +- 0.50` | `81.67 ms +- 2.59` | `0.40 ms +- 0.12` | `47.69 ms +- 0.31` | `0.11 ms +- 0.01` | `91.82 ms +- 0.86` | `15.08 ms +- 1.68` |
| `oltp_program_create_and_link` | `2.30 ms +- 0.32` | `10.92 ms +- 0.63` | `4.47 ms +- 0.49` | `4.96 ms +- 0.12` | `3.06 ms +- 0.51` | `13.00 ms +- 0.64` | `0.31 ms +- 0.10` | `24.75 ms +- 1.33` | `0.21 ms +- 0.01` | `46.70 ms +- 0.32` | `5.22 ms +- 0.25` |
| `oltp_type1_neighbors` | `1.24 ms +- 0.03` | `60.16 ms +- 0.83` | `4.39 ms +- 0.41` | `3.87 ms +- 0.04` | `1.88 ms +- 0.28` | `42.98 ms +- 2.32` | `0.38 ms +- 0.09` | `251.09 ms +- 3.10` | `0.05 ms +- 0.00` | `42.46 ms +- 0.05` | `6.36 ms +- 0.48` |
| `oltp_type1_point_lookup` | `1.23 ms +- 0.06` | `9.90 ms +- 0.10` | `1.78 ms +- 0.37` | `2.73 ms +- 0.06` | `1.66 ms +- 0.26` | `10.82 ms +- 0.45` | `0.47 ms +- 0.13` | `22.83 ms +- 0.98` | `0.05 ms +- 0.00` | `42.70 ms +- 0.57` | `5.15 ms +- 0.15` |
| `oltp_unwind_literal_top2` | `1.12 ms +- 0.10` | `1.04 ms +- 0.03` | `1.61 ms +- 0.25` | `1.38 ms +- 0.04` | `1.45 ms +- 0.23` | `1.29 ms +- 0.10` | `0.30 ms +- 0.09` | `0.26 ms +- 0.02` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.86 ms +- 0.18` |
| `oltp_update_cross_type_edge_rank` | `1.30 ms +- 0.07` | `62.26 ms +- 1.88` | `5.21 ms +- 0.89` | `3.96 ms +- 0.14` | `1.91 ms +- 0.36` | `109.65 ms +- 35.48` | `0.31 ms +- 0.10` | `23.98 ms +- 1.27` | `0.03 ms +- 0.01` | `45.44 ms +- 0.39` | `5.62 ms +- 0.47` |
| `oltp_update_type1_score` | `0.90 ms +- 0.05` | `9.31 ms +- 0.32` | `2.06 ms +- 0.32` | `2.91 ms +- 0.09` | `1.30 ms +- 0.19` | `10.65 ms +- 1.03` | `0.33 ms +- 0.10` | `25.07 ms +- 3.22` | `0.04 ms +- 0.00` | `42.55 ms +- 0.77` | `4.97 ms +- 0.26` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.53 ms +- 0.69` | `20.45 ms +- 2.17` | `4.40 ms +- 1.22` | `6.29 ms +- 0.19` | `2.70 ms +- 0.32` | `25.19 ms +- 1.53` | `0.59 ms +- 0.26` | `53.68 ms +- 5.44` | `0.15 ms +- 0.01` | `95.43 ms +- 1.10` | `9.63 ms +- 0.77` |
| `oltp_create_type1_node` | `1.17 ms +- 0.14` | `1.09 ms +- 0.06` | `2.66 ms +- 0.78` | `1.76 ms +- 0.22` | `1.81 ms +- 0.57` | `1.43 ms +- 0.09` | `0.48 ms +- 0.20` | `0.35 ms +- 0.07` | `0.06 ms +- 0.01` | `0.05 ms +- 0.00` | `0.75 ms +- 0.34` |
| `oltp_cross_type_lookup` | `2.16 ms +- 0.76` | `65.17 ms +- 3.91` | `6.06 ms +- 1.00` | `4.79 ms +- 0.30` | `2.54 ms +- 0.39` | `46.21 ms +- 3.22` | `0.49 ms +- 0.19` | `588.60 ms +- 98.22` | `0.04 ms +- 0.00` | `44.06 ms +- 0.72` | `8.09 ms +- 1.19` |
| `oltp_delete_type1_edge` | `1.20 ms +- 0.11` | `63.48 ms +- 3.11` | `5.91 ms +- 1.12` | `4.67 ms +- 0.13` | `2.03 ms +- 0.31` | `97.28 ms +- 3.85` | `0.47 ms +- 0.27` | `27.56 ms +- 1.27` | `0.07 ms +- 0.01` | `46.75 ms +- 0.55` | `6.86 ms +- 1.03` |
| `oltp_delete_type1_node` | `1.71 ms +- 0.71` | `599.58 ms +- 96.00` | `1.54 ms +- 0.29` | `2.83 ms +- 0.08` | `1.37 ms +- 0.13` | `289.48 ms +- 0.00` | `0.55 ms +- 0.35` | `28.78 ms +- 1.14` | `0.12 ms +- 0.01` | `46.91 ms +- 0.48` | `6.12 ms +- 0.48` |
| `oltp_merge_cross_type_edge` | `2.95 ms +- 0.72` | `76.27 ms +- 4.08` | `5.94 ms +- 1.36` | `7.44 ms +- 0.06` | `3.60 ms +- 0.74` | `87.45 ms +- 3.22` | `0.60 ms +- 0.28` | `52.12 ms +- 0.71` | `0.14 ms +- 0.01` | `95.22 ms +- 0.63` | `16.96 ms +- 1.98` |
| `oltp_program_create_and_link` | `2.89 ms +- 0.56` | `12.50 ms +- 1.08` | `5.81 ms +- 1.05` | `6.22 ms +- 0.30` | `3.82 ms +- 0.75` | `15.84 ms +- 1.39` | `0.43 ms +- 0.24` | `28.64 ms +- 1.53` | `0.25 ms +- 0.02` | `49.00 ms +- 0.46` | `5.67 ms +- 0.23` |
| `oltp_type1_neighbors` | `1.49 ms +- 0.12` | `64.04 ms +- 2.30` | `5.54 ms +- 1.02` | `4.38 ms +- 0.10` | `2.37 ms +- 0.43` | `47.82 ms +- 1.79` | `0.62 ms +- 0.25` | `262.16 ms +- 2.51` | `0.06 ms +- 0.00` | `44.78 ms +- 0.20` | `7.61 ms +- 0.90` |
| `oltp_type1_point_lookup` | `1.73 ms +- 0.54` | `11.03 ms +- 0.40` | `2.58 ms +- 1.02` | `3.24 ms +- 0.09` | `1.99 ms +- 0.37` | `12.85 ms +- 1.01` | `0.76 ms +- 0.35` | `26.30 ms +- 0.17` | `0.07 ms +- 0.00` | `45.97 ms +- 1.50` | `5.47 ms +- 0.18` |
| `oltp_unwind_literal_top2` | `1.55 ms +- 0.42` | `1.27 ms +- 0.12` | `2.38 ms +- 0.82` | `1.77 ms +- 0.07` | `1.84 ms +- 0.32` | `1.67 ms +- 0.16` | `0.43 ms +- 0.20` | `0.36 ms +- 0.06` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `1.18 ms +- 0.45` |
| `oltp_update_cross_type_edge_rank` | `1.76 ms +- 0.18` | `67.98 ms +- 1.95` | `6.27 ms +- 1.47` | `4.50 ms +- 0.29` | `2.40 ms +- 0.39` | `113.77 ms +- 35.06` | `0.43 ms +- 0.23` | `28.13 ms +- 1.57` | `0.04 ms +- 0.01` | `47.51 ms +- 0.54` | `6.42 ms +- 0.50` |
| `oltp_update_type1_score` | `1.19 ms +- 0.19` | `10.44 ms +- 0.91` | `3.02 ms +- 1.16` | `3.16 ms +- 0.18` | `1.69 ms +- 0.20` | `12.72 ms +- 2.07` | `0.51 ms +- 0.18` | `28.70 ms +- 5.21` | `0.05 ms +- 0.00` | `44.89 ms +- 0.59` | `5.37 ms +- 0.21` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1107.95 ms +- 60.53` | `193.10 ms +- 2.72` | `9.20 ms +- 0.31` | `8.62 ms +- 0.12` | `104.09 ms +- 3.78` | `102.46 ms +- 1.69` | `604.30 ms +- 22.88` | `598.14 ms +- 5.83` | `980.27 ms +- 52.82` | `892.43 ms +- 11.94` | `43.89 ms +- 2.58` |
| `olap_fixed_length_path_projection` | `1.84 ms +- 0.05` | `1317.13 ms +- 7.22` | `178.87 ms +- 3.84` | `175.13 ms +- 2.52` | `20.90 ms +- 0.72` | `898.88 ms +- 50.90` | `1428.78 ms +- 13.31` | `1437.25 ms +- 16.18` | `6698.76 ms +- 421.00` | `6391.22 ms +- 249.50` | `2167.66 ms +- 16.72` |
| `olap_fixed_length_path_with_rebinding` | `1737.34 ms +- 53.83` | `1242.00 ms +- 12.83` | `105.87 ms +- 3.24` | `103.27 ms +- 2.22` | `440.02 ms +- 14.07` | `416.63 ms +- 6.13` | `3714.81 ms +- 57.69` | `3819.39 ms +- 77.19` | `6641.14 ms +- 342.63` | `6461.54 ms +- 27.91` | `1784.98 ms +- 5.15` |
| `olap_graph_introspection_rollup` | `1.81 ms +- 0.00` | `105.12 ms +- 2.42` | `5.74 ms +- 0.29` | `5.01 ms +- 0.22` | `7.15 ms +- 0.84` | `92.47 ms +- 3.48` | `527.83 ms +- 4.21` | `519.78 ms +- 12.08` | `1569.37 ms +- 24.74` | `1471.94 ms +- 16.05` | `158.66 ms +- 2.30` |
| `olap_optional_type1_aggregate` | `16.93 ms +- 0.38` | `22.80 ms +- 0.23` | `4.16 ms +- 0.16` | `3.82 ms +- 0.11` | `17.07 ms +- 1.28` | `17.12 ms +- 1.48` | `66.30 ms +- 7.95` | `59.19 ms +- 0.31` | - | - | `4.14 ms +- 0.08` |
| `olap_relationship_function_projection` | `512.35 ms +- 18.94` | `161.20 ms +- 1.88` | `15.08 ms +- 0.40` | `14.39 ms +- 0.05` | `142.09 ms +- 4.86` | `145.18 ms +- 2.90` | `579.58 ms +- 18.70` | `583.52 ms +- 23.38` | `1097.57 ms +- 38.56` | `1024.97 ms +- 8.55` | `240.39 ms +- 1.30` |
| `olap_three_type_path_count` | `1433.01 ms +- 33.58` | `872.92 ms +- 5.14` | `38.12 ms +- 1.34` | `37.39 ms +- 2.38` | `333.80 ms +- 25.54` | `334.38 ms +- 0.91` | `558.48 ms +- 5.09` | `564.77 ms +- 14.64` | `5.96 ms +- 0.15` | `6.13 ms +- 0.10` | `12.65 ms +- 0.58` |
| `olap_type1_active_leaderboard` | `1.35 ms +- 0.03` | `11.32 ms +- 0.20` | `5.76 ms +- 0.36` | `5.18 ms +- 0.09` | `10.19 ms +- 0.63` | `9.97 ms +- 0.58` | `71.85 ms +- 9.50` | `65.49 ms +- 0.54` | `84.28 ms +- 3.04` | `75.59 ms +- 1.75` | `4.94 ms +- 0.02` |
| `olap_type1_age_rollup` | `101.16 ms +- 5.91` | `24.24 ms +- 0.53` | `3.76 ms +- 0.14` | `3.78 ms +- 0.37` | `11.70 ms +- 0.85` | `10.97 ms +- 0.37` | `77.82 ms +- 9.28` | `70.63 ms +- 0.33` | `57.47 ms +- 0.52` | `56.19 ms +- 0.43` | `3.49 ms +- 0.32` |
| `olap_type2_score_distribution` | `9.01 ms +- 0.32` | `23.85 ms +- 0.43` | `4.59 ms +- 0.28` | `4.19 ms +- 0.14` | `8.32 ms +- 0.81` | `17.75 ms +- 0.47` | `64.87 ms +- 0.54` | `61.82 ms +- 0.47` | `59.94 ms +- 0.45` | `59.55 ms +- 0.74` | `3.43 ms +- 0.28` |
| `olap_variable_length_grouped_max_rollup` | `2.45 ms +- 0.01` | `698.44 ms +- 9.56` | `8.92 ms +- 1.49` | `8.25 ms +- 0.82` | `5.12 ms +- 0.18` | `184.36 ms +- 11.88` | `0.31 ms +- 0.06` | `21.80 ms +- 0.68` | `0.09 ms +- 0.05` | `48.00 ms +- 1.76` | `6.88 ms +- 0.47` |
| `olap_variable_length_grouped_rollup` | `68231.69 ms +- 705.23` | `64346.26 ms +- 440.01` | `721.30 ms +- 13.55` | `715.38 ms +- 10.13` | `7144.40 ms +- 173.91` | `7460.21 ms +- 423.65` | - | - | - | - | `26184.72 ms +- 532.16` |
| `olap_variable_length_reachability` | `2.98 ms +- 0.20` | `2355.31 ms +- 13.81` | `15.00 ms +- 0.47` | `14.26 ms +- 0.54` | `26.28 ms +- 2.85` | `414.83 ms +- 9.83` | `1.00 ms +- 0.01` | - | `0.40 ms +- 0.02` | `41.52 ms +- 0.45` | `4.88 ms +- 0.28` |
| `olap_with_scalar_rebinding` | `102.77 ms +- 6.08` | `27.27 ms +- 0.73` | `5.81 ms +- 0.41` | `5.14 ms +- 0.11` | `15.59 ms +- 0.95` | `22.49 ms +- 0.83` | `78.51 ms +- 0.09` | `76.35 ms +- 0.79` | `69.16 ms +- 1.64` | `67.92 ms +- 1.18` | `6.84 ms +- 0.02` |
| `olap_with_size_predicate_projection` | `15.87 ms +- 0.52` | `15.92 ms +- 0.58` | `5.91 ms +- 0.45` | `5.61 ms +- 0.28` | `15.17 ms +- 0.94` | `22.29 ms +- 0.67` | `40.08 ms +- 0.37` | `39.94 ms +- 0.98` | `107.08 ms +- 4.11` | `102.37 ms +- 0.22` | `34.85 ms +- 1.16` |
| `olap_with_where_lower_projection` | `90.55 ms +- 5.09` | `16.68 ms +- 0.97` | `6.05 ms +- 0.43` | `5.46 ms +- 0.18` | `14.72 ms +- 1.00` | `21.28 ms +- 1.00` | `38.63 ms +- 0.22` | `38.31 ms +- 0.39` | `89.13 ms +- 2.22` | `87.48 ms +- 1.22` | `8.16 ms +- 0.06` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1137.25 ms +- 60.86` | `199.62 ms +- 3.02` | `9.88 ms +- 0.18` | `9.51 ms +- 0.26` | `110.78 ms +- 5.33` | `110.11 ms +- 3.04` | `645.60 ms +- 67.09` | `616.44 ms +- 11.76` | `1039.37 ms +- 87.05` | `925.60 ms +- 16.79` | `48.32 ms +- 4.34` |
| `olap_fixed_length_path_projection` | `1.98 ms +- 0.13` | `1338.62 ms +- 10.32` | `188.65 ms +- 7.33` | `184.90 ms +- 3.36` | `23.52 ms +- 0.65` | `973.30 ms +- 96.55` | `1452.05 ms +- 14.53` | `1472.99 ms +- 19.14` | `7137.57 ms +- 512.61` | `6873.54 ms +- 273.61` | `2255.14 ms +- 28.85` |
| `olap_fixed_length_path_with_rebinding` | `1778.37 ms +- 58.50` | `1264.34 ms +- 24.44` | `113.92 ms +- 4.32` | `108.05 ms +- 2.94` | `457.91 ms +- 30.15` | `577.73 ms +- 148.74` | `3767.25 ms +- 57.10` | `3903.89 ms +- 102.52` | `7110.22 ms +- 389.39` | `6903.04 ms +- 77.64` | `1837.93 ms +- 13.03` |
| `olap_graph_introspection_rollup` | `1.91 ms +- 0.06` | `110.62 ms +- 2.13` | `7.01 ms +- 0.75` | `5.94 ms +- 0.56` | `8.57 ms +- 1.45` | `98.85 ms +- 4.46` | `540.20 ms +- 3.19` | `533.50 ms +- 14.95` | `1599.71 ms +- 19.86` | `1502.26 ms +- 14.79` | `169.04 ms +- 1.76` |
| `olap_optional_type1_aggregate` | `17.69 ms +- 0.56` | `24.56 ms +- 0.54` | `4.98 ms +- 0.19` | `4.58 ms +- 0.53` | `20.81 ms +- 3.83` | `18.53 ms +- 0.75` | `72.62 ms +- 7.72` | `63.99 ms +- 0.61` | - | - | `5.65 ms +- 0.20` |
| `olap_relationship_function_projection` | `529.44 ms +- 15.29` | `169.86 ms +- 12.22` | `16.55 ms +- 0.34` | `15.60 ms +- 0.44` | `150.68 ms +- 5.13` | `154.82 ms +- 3.87` | `599.10 ms +- 23.81` | `601.30 ms +- 30.68` | `1120.75 ms +- 33.83` | `1097.27 ms +- 92.62` | `250.41 ms +- 2.70` |
| `olap_three_type_path_count` | `1451.37 ms +- 33.03` | `898.58 ms +- 19.78` | `42.81 ms +- 1.58` | `43.06 ms +- 1.23` | `444.76 ms +- 86.36` | `490.68 ms +- 130.51` | `591.11 ms +- 39.58` | `589.26 ms +- 15.96` | `6.56 ms +- 0.20` | `6.43 ms +- 0.03` | `52.76 ms +- 8.13` |
| `olap_type1_active_leaderboard` | `1.57 ms +- 0.22` | `11.98 ms +- 0.44` | `6.95 ms +- 0.39` | `6.48 ms +- 0.22` | `11.66 ms +- 0.40` | `11.60 ms +- 0.94` | `77.34 ms +- 11.69` | `71.03 ms +- 0.52` | `115.39 ms +- 21.43` | `99.80 ms +- 0.71` | `6.71 ms +- 0.78` |
| `olap_type1_age_rollup` | `109.28 ms +- 5.45` | `25.20 ms +- 0.58` | `4.38 ms +- 0.10` | `4.29 ms +- 0.23` | `13.55 ms +- 1.35` | `12.89 ms +- 0.62` | `83.71 ms +- 11.67` | `76.98 ms +- 1.69` | `68.93 ms +- 10.43` | `59.16 ms +- 0.62` | `4.44 ms +- 0.63` |
| `olap_type2_score_distribution` | `9.68 ms +- 0.25` | `24.68 ms +- 0.56` | `5.42 ms +- 0.31` | `4.85 ms +- 0.32` | `9.73 ms +- 1.22` | `19.23 ms +- 0.63` | `70.94 ms +- 2.85` | `67.53 ms +- 2.32` | `61.56 ms +- 0.59` | `61.16 ms +- 1.22` | `4.36 ms +- 0.51` |
| `olap_variable_length_grouped_max_rollup` | `2.55 ms +- 0.05` | `724.90 ms +- 26.85` | `11.08 ms +- 2.38` | `12.13 ms +- 3.91` | `6.04 ms +- 0.80` | `207.58 ms +- 14.97` | `0.43 ms +- 0.16` | `23.50 ms +- 1.22` | `0.13 ms +- 0.05` | `68.85 ms +- 14.14` | `9.03 ms +- 0.88` |
| `olap_variable_length_grouped_rollup` | `71327.66 ms +- 2049.70` | `67324.23 ms +- 1540.00` | `742.80 ms +- 16.69` | `734.04 ms +- 14.96` | `8207.04 ms +- 473.24` | `8797.64 ms +- 1406.68` | - | - | - | - | `27108.16 ms +- 727.94` |
| `olap_variable_length_reachability` | `3.22 ms +- 0.29` | `2461.84 ms +- 55.36` | `17.32 ms +- 2.14` | `15.71 ms +- 0.81` | `29.37 ms +- 1.89` | `447.97 ms +- 18.14` | `1.28 ms +- 0.11` | - | `0.53 ms +- 0.03` | `42.95 ms +- 0.52` | `6.43 ms +- 0.34` |
| `olap_with_scalar_rebinding` | `109.64 ms +- 4.55` | `29.58 ms +- 1.70` | `7.62 ms +- 1.12` | `6.20 ms +- 0.67` | `17.20 ms +- 0.61` | `24.44 ms +- 0.79` | `84.68 ms +- 2.95` | `81.59 ms +- 4.39` | `71.03 ms +- 1.92` | `70.89 ms +- 0.75` | `8.26 ms +- 0.67` |
| `olap_with_size_predicate_projection` | `16.51 ms +- 0.60` | `17.02 ms +- 1.15` | `7.02 ms +- 0.54` | `6.70 ms +- 0.88` | `16.76 ms +- 0.92` | `23.88 ms +- 0.55` | `42.73 ms +- 0.88` | `41.96 ms +- 1.98` | `112.61 ms +- 4.13` | `108.35 ms +- 1.59` | `40.69 ms +- 3.35` |
| `olap_with_where_lower_projection` | `95.93 ms +- 5.19` | `18.19 ms +- 1.74` | `7.04 ms +- 0.18` | `6.42 ms +- 0.63` | `17.18 ms +- 0.24` | `22.92 ms +- 0.71` | `41.44 ms +- 1.91` | `41.51 ms +- 0.82` | `94.98 ms +- 1.61` | `91.62 ms +- 1.48` | `9.40 ms +- 0.65` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1148.40 ms +- 56.12` | `204.41 ms +- 0.62` | `10.44 ms +- 0.13` | `10.51 ms +- 0.24` | `113.84 ms +- 6.32` | `111.55 ms +- 3.92` | `662.09 ms +- 79.69` | `630.50 ms +- 14.70` | `1082.18 ms +- 120.93` | `962.28 ms +- 11.02` | `53.27 ms +- 2.15` |
| `olap_fixed_length_path_projection` | `2.23 ms +- 0.26` | `1347.81 ms +- 17.72` | `192.65 ms +- 7.54` | `188.16 ms +- 4.37` | `25.20 ms +- 0.79` | `989.96 ms +- 84.86` | `1482.31 ms +- 36.33` | `1501.39 ms +- 32.29` | `7378.57 ms +- 526.17` | `7038.49 ms +- 285.88` | `2389.05 ms +- 117.32` |
| `olap_fixed_length_path_with_rebinding` | `1797.87 ms +- 64.84` | `1280.44 ms +- 28.21` | `118.96 ms +- 8.86` | `111.36 ms +- 5.55` | `464.45 ms +- 34.45` | `635.06 ms +- 186.68` | `3876.81 ms +- 42.36` | `4167.29 ms +- 367.40` | `7338.42 ms +- 310.20` | `7102.56 ms +- 72.29` | `1873.82 ms +- 34.46` |
| `olap_graph_introspection_rollup` | `1.99 ms +- 0.11` | `116.42 ms +- 9.46` | `7.63 ms +- 1.20` | `7.36 ms +- 2.13` | `9.18 ms +- 1.83` | `103.38 ms +- 3.12` | `546.28 ms +- 4.36` | `556.21 ms +- 43.09` | `1628.13 ms +- 19.52` | `1514.61 ms +- 13.16` | `171.38 ms +- 2.99` |
| `olap_optional_type1_aggregate` | `18.49 ms +- 0.37` | `25.22 ms +- 0.88` | `5.23 ms +- 0.14` | `4.81 ms +- 0.65` | `24.28 ms +- 5.30` | `19.93 ms +- 1.21` | `77.62 ms +- 7.71` | `68.23 ms +- 3.53` | - | - | `9.61 ms +- 1.81` |
| `olap_relationship_function_projection` | `538.36 ms +- 17.94` | `176.67 ms +- 19.18` | `18.60 ms +- 1.02` | `16.35 ms +- 0.52` | `154.86 ms +- 4.91` | `159.98 ms +- 4.60` | `612.89 ms +- 35.75` | `613.78 ms +- 36.52` | `1136.41 ms +- 38.04` | `1115.34 ms +- 102.34` | `257.67 ms +- 4.68` |
| `olap_three_type_path_count` | `1455.67 ms +- 32.56` | `930.17 ms +- 68.56` | `44.47 ms +- 2.48` | `46.46 ms +- 0.16` | `532.04 ms +- 135.17` | `559.01 ms +- 183.92` | `675.32 ms +- 164.77` | `602.86 ms +- 24.54` | `7.99 ms +- 1.61` | `7.14 ms +- 0.23` | `58.00 ms +- 7.78` |
| `olap_type1_active_leaderboard` | `1.66 ms +- 0.19` | `12.67 ms +- 1.40` | `7.41 ms +- 0.08` | `7.91 ms +- 2.06` | `12.15 ms +- 0.17` | `13.49 ms +- 0.79` | `82.78 ms +- 14.46` | `77.03 ms +- 3.34` | `136.30 ms +- 38.68` | `116.24 ms +- 7.24` | `7.37 ms +- 0.61` |
| `olap_type1_age_rollup` | `112.94 ms +- 3.49` | `26.59 ms +- 0.41` | `4.83 ms +- 0.14` | `4.67 ms +- 0.40` | `15.94 ms +- 2.68` | `15.51 ms +- 2.04` | `88.89 ms +- 16.21` | `81.02 ms +- 2.93` | `84.46 ms +- 22.38` | `64.61 ms +- 4.44` | `4.88 ms +- 0.69` |
| `olap_type2_score_distribution` | `10.57 ms +- 0.58` | `26.76 ms +- 0.42` | `5.85 ms +- 0.31` | `5.72 ms +- 0.60` | `11.54 ms +- 1.88` | `21.67 ms +- 1.71` | `76.79 ms +- 2.79` | `72.00 ms +- 3.79` | `64.56 ms +- 3.64` | `62.34 ms +- 1.14` | `4.87 ms +- 0.41` |
| `olap_variable_length_grouped_max_rollup` | `3.04 ms +- 0.75` | `746.42 ms +- 47.91` | `12.98 ms +- 4.46` | `13.76 ms +- 5.63` | `6.75 ms +- 0.75` | `222.86 ms +- 15.81` | `0.59 ms +- 0.23` | `25.76 ms +- 1.04` | `0.17 ms +- 0.05` | `103.34 ms +- 11.24` | `9.87 ms +- 0.53` |
| `olap_variable_length_grouped_rollup` | `74575.26 ms +- 4023.76` | `68547.89 ms +- 2559.38` | `753.93 ms +- 16.51` | `747.45 ms +- 26.43` | `8463.57 ms +- 525.30` | `9419.98 ms +- 1431.05` | - | - | - | - | `27303.97 ms +- 916.95` |
| `olap_variable_length_reachability` | `3.45 ms +- 0.23` | `2607.56 ms +- 159.41` | `18.49 ms +- 2.92` | `18.50 ms +- 0.82` | `31.54 ms +- 0.91` | `466.67 ms +- 23.20` | `1.99 ms +- 0.92` | - | `0.60 ms +- 0.03` | `44.39 ms +- 1.20` | `7.00 ms +- 0.50` |
| `olap_with_scalar_rebinding` | `114.73 ms +- 4.07` | `38.53 ms +- 7.60` | `8.31 ms +- 1.42` | `6.62 ms +- 0.21` | `18.36 ms +- 0.97` | `25.80 ms +- 0.98` | `90.19 ms +- 1.79` | `92.84 ms +- 14.29` | `74.88 ms +- 1.71` | `73.03 ms +- 0.61` | `9.55 ms +- 1.02` |
| `olap_with_size_predicate_projection` | `17.35 ms +- 0.30` | `17.86 ms +- 1.66` | `8.63 ms +- 1.22` | `7.46 ms +- 0.82` | `18.36 ms +- 0.66` | `26.45 ms +- 1.49` | `44.60 ms +- 0.97` | `43.94 ms +- 1.93` | `118.99 ms +- 3.50` | `117.13 ms +- 6.00` | `42.47 ms +- 3.92` |
| `olap_with_where_lower_projection` | `98.13 ms +- 4.82` | `19.09 ms +- 1.68` | `8.04 ms +- 0.66` | `7.31 ms +- 1.43` | `19.39 ms +- 0.97` | `24.33 ms +- 0.87` | `44.33 ms +- 2.18` | `45.48 ms +- 0.87` | `101.16 ms +- 2.86` | `96.63 ms +- 2.43` | `11.48 ms +- 0.54` |

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
tables below, that checkpoint time is shown in the `Analyze / Checkpoint` column so
the setup layout stays consistent across engines.

ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The
indexed and unindexed rows below measure ArcadeDB Embedded directly rather
than a CypherGlot compile-plus-execute SQL path.
ArcadeDB also records graph analytical view build time as `gav_ms`; in the
cross-engine setup tables below, that engine-specific post-load work is omitted so
only shared phases appear side by side. The ArcadeDB-specific timing tables below break out
`GAV` separately and keep `Analyze / Checkpoint` as a shared bucket.


Wall-clock run time per completed result file:

| Combo | Wall-clock run time |
| --- | --- |
| SQLite Indexed (3) | `282.20 s +- 31.75` |
| SQLite Unindexed (3) | `260.45 s +- 13.88` |
| DuckDB Indexed (3) | `451.38 s +- 61.71` |
| DuckDB Unindexed (3) | `417.10 s +- 36.58` |
| PostgreSQL Indexed (3) | `340.45 s +- 7.22` |
| PostgreSQL Unindexed (3) | `349.12 s +- 4.41` |
| Neo4j Indexed (3) | `142.12 s +- 7.77` |
| Neo4j Unindexed (3) | `152.37 s +- 8.48` |
| ArcadeDB Indexed (3) | `56.15 s +- 0.37` |
| ArcadeDB Unindexed (3) | `103.25 s +- 1.12` |
| LadybugDB Unindexed (3) | `337.08 s +- 36.79` |

OLTP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `10.94 ms +- 7.81` | `6.75 ms +- 4.40` | `87.79 ms +- 0.96` | `7.45 ms +- 0.47` | `3.37 ms +- 0.04` | `1.27 ms +- 0.07` | `1.75 ms +- 0.37` | `2.31 ms +- 0.55` |
| SQLite Unindexed (3) | `8.94 ms +- 4.54` | `5.92 ms +- 3.38` | `91.47 ms +- 2.18` | `1.67 ms +- 0.19` | `0.47 ms +- 0.15` | `1.28 ms +- 0.03` | `1.44 ms +- 0.20` | `1.74 ms +- 0.38` |
| DuckDB Indexed (3) | `11.76 ms +- 2.25` | `159.48 ms +- 61.14` | `204.74 ms +- 36.44` | `120.73 ms +- 40.58` | `0.21 ms +- 0.17` | `2.19 ms +- 0.18` | `3.49 ms +- 1.14` | `4.42 ms +- 1.60` |
| DuckDB Unindexed (3) | `11.90 ms +- 1.15` | `94.63 ms +- 6.92` | `169.84 ms +- 4.94` | `44.22 ms +- 1.54` | `0.11 ms +- 0.01` | `2.08 ms +- 0.11` | `2.92 ms +- 0.64` | `3.58 ms +- 0.96` |
| PostgreSQL Indexed (3) | `3.83 ms +- 0.27` | `333.41 ms +- 33.95` | `277.71 ms +- 36.36` | `320.27 ms +- 184.10` | `80.79 ms +- 4.12` | `1.50 ms +- 0.04` | `1.97 ms +- 0.09` | `2.34 ms +- 0.09` |
| PostgreSQL Unindexed (3) | `3.79 ms +- 0.01` | `403.22 ms +- 139.87` | `277.38 ms +- 50.98` | `200.16 ms +- 330.34` | `118.04 ms +- 72.62` | `1.60 ms +- 0.03` | `1.97 ms +- 0.09` | `2.39 ms +- 0.08` |
| Neo4j Indexed (3) | `78.97 ms +- 25.22` | `372.78 ms +- 25.99` | `2323.73 ms +- 261.22` | `1008.67 ms +- 355.26` | `0.00 ms +- 0.00` | `0.24 ms +- 0.01` | `0.34 ms +- 0.02` | `0.47 ms +- 0.04` |
| Neo4j Unindexed (3) | `76.16 ms +- 13.63` | `436.81 ms +- 52.83` | `2240.38 ms +- 310.34` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.35 ms +- 0.01` | `0.54 ms +- 0.09` | `0.83 ms +- 0.29` |
| ArcadeDB Indexed (3) | `287.30 ms +- 4.98` | `238.02 ms +- 4.05` | `551.49 ms +- 11.80` | `310.65 ms +- 16.36` | `0.00 ms +- 0.00` | `0.03 ms +- 0.00` | `0.06 ms +- 0.00` | `0.08 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `294.25 ms +- 7.77` | `242.23 ms +- 10.75` | `550.99 ms +- 17.42` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.42 ms +- 0.01` | `0.47 ms +- 0.01` | `0.58 ms +- 0.00` |
| LadybugDB Unindexed (3) | `97.23 ms +- 26.34` | `68.82 ms +- 25.50` | `977.43 ms +- 104.23` | `0.00 ms +- 0.00` | `28.84 ms +- 2.19` | `1.42 ms +- 0.18` | `5.38 ms +- 1.91` | `14.79 ms +- 0.87` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze / Checkpoint | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed (3) | `9.59 ms +- 5.78` | `7.73 ms +- 6.16` | `99.19 ms +- 15.34` | `8.31 ms +- 0.35` | `4.10 ms +- 0.89` | `2.60 ms +- 0.45` | `4.20 ms +- 1.55` | `5.27 ms +- 2.06` |
| SQLite Unindexed (3) | `9.66 ms +- 4.08` | `8.93 ms +- 6.27` | `80.77 ms +- 3.00` | `1.65 ms +- 0.18` | `0.37 ms +- 0.03` | `2.53 ms +- 0.24` | `2.89 ms +- 0.77` | `3.13 ms +- 1.06` |
| DuckDB Indexed (3) | `8.30 ms +- 0.41` | `89.44 ms +- 1.47` | `162.00 ms +- 1.13` | `74.81 ms +- 1.79` | `0.13 ms +- 0.02` | `2.92 ms +- 0.11` | `3.51 ms +- 0.22` | `3.98 ms +- 0.19` |
| DuckDB Unindexed (3) | `10.70 ms +- 3.29` | `129.15 ms +- 67.61` | `171.17 ms +- 20.76` | `63.43 ms +- 35.65` | `0.21 ms +- 0.09` | `3.26 ms +- 0.65` | `4.29 ms +- 1.55` | `4.88 ms +- 1.83` |
| PostgreSQL Indexed (3) | `4.06 ms +- 0.11` | `321.25 ms +- 10.68` | `237.11 ms +- 80.23` | `220.09 ms +- 18.24` | `76.62 ms +- 0.63` | `3.07 ms +- 0.10` | `3.74 ms +- 0.17` | `4.31 ms +- 0.17` |
| PostgreSQL Unindexed (3) | `3.94 ms +- 0.17` | `323.35 ms +- 8.47` | `187.15 ms +- 12.16` | `12.11 ms +- 4.86` | `121.22 ms +- 4.50` | `2.89 ms +- 0.13` | `3.41 ms +- 0.28` | `3.87 ms +- 0.30` |
| Neo4j Indexed (3) | `78.97 ms +- 25.22` | `372.78 ms +- 25.99` | `2323.73 ms +- 261.22` | `1008.67 ms +- 355.26` | `0.00 ms +- 0.00` | `2.50 ms +- 0.30` | `3.22 ms +- 0.61` | `3.86 ms +- 0.77` |
| Neo4j Unindexed (3) | `76.16 ms +- 13.63` | `436.81 ms +- 52.83` | `2240.38 ms +- 310.34` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.29 ms +- 0.07` | `2.73 ms +- 0.12` | `3.32 ms +- 0.17` |
| ArcadeDB Indexed (3) | `1.38 ms +- 0.04` | `24.65 ms +- 1.67` | `370.36 ms +- 23.36` | `89.09 ms +- 31.16` | `0.00 ms +- 0.00` | `2.92 ms +- 0.10` | `3.45 ms +- 0.12` | `3.90 ms +- 0.02` |
| ArcadeDB Unindexed (3) | `1.58 ms +- 0.12` | `32.21 ms +- 1.59` | `368.69 ms +- 26.10` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.90 ms +- 0.05` | `3.47 ms +- 0.15` | `3.97 ms +- 0.17` |
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
| ArcadeDB Indexed (3) | `148.96 MiB +- 1.65` | `209.83 MiB +- 1.05` | `275.54 MiB +- 5.07` | `362.07 MiB +- 5.66` | `362.07 MiB +- 5.67` | `2487.60 MiB +- 136.06` |
| ArcadeDB Unindexed (3) | `149.44 MiB +- 2.89` | `211.43 MiB +- 1.69` | `273.57 MiB +- 1.80` | `273.57 MiB +- 1.80` | `273.57 MiB +- 1.80` | `2456.38 MiB +- 59.27` |
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
| ArcadeDB Indexed (3) | `2487.61 MiB +- 136.06` | `2487.67 MiB +- 136.06` | `1847.78 MiB +- 1217.65` | `1805.25 MiB +- 1291.30` | `1814.26 MiB +- 1284.57` | `1175.38 MiB +- 218.49` |
| ArcadeDB Unindexed (3) | `2456.38 MiB +- 59.27` | `2456.48 MiB +- 59.22` | `1415.84 MiB +- 883.42` | `1415.84 MiB +- 883.42` | `1017.52 MiB +- 1213.74` | `972.96 MiB +- 28.58` |
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
| `oltp/arcadedb_embedded_indexed` | `0.03 ms +- 0.00` | `0.06 ms +- 0.00` | `0.08 ms +- 0.00` |
| `olap/arcadedb_embedded_indexed` | `2.92 ms +- 0.10` | `3.45 ms +- 0.12` | `3.90 ms +- 0.02` |
| `oltp/arcadedb_embedded_unindexed` | `0.42 ms +- 0.01` | `0.47 ms +- 0.01` | `0.58 ms +- 0.00` |
| `olap/arcadedb_embedded_unindexed` | `2.90 ms +- 0.05` | `3.47 ms +- 0.15` | `3.97 ms +- 0.17` |
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

#### Small ArcadeDB-specific timings

These tables keep ArcadeDB-only timing phases out of the
cross-engine comparison tables.

ArcadeDB setup hierarchy is `connect/reset -> schema/constraints ->
ingest -> index -> GAV -> analyze/checkpoint`, with `GAV` present only
for OLAP fixtures.

ArcadeDB setup phase breakdown, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `287.30 ms +- 4.98` | `238.02 ms +- 4.05` | `551.49 ms +- 11.80` | `310.65 ms +- 16.36` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `294.25 ms +- 7.77` | `242.23 ms +- 10.75` | `550.99 ms +- 17.42` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |

ArcadeDB setup phase breakdown, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | GAV | Analyze / Checkpoint |
| --- | --- | --- | --- | --- | --- | --- |
| ArcadeDB Indexed (3) | `1.38 ms +- 0.04` | `24.65 ms +- 1.67` | `370.36 ms +- 23.36` | `89.09 ms +- 31.16` | `279.68 ms +- 4.00` | `0.00 ms +- 0.00` |
| ArcadeDB Unindexed (3) | `1.58 ms +- 0.12` | `32.21 ms +- 1.59` | `368.69 ms +- 26.10` | `0.00 ms +- 0.00` | `294.72 ms +- 5.12` | `0.00 ms +- 0.00` |

#### Small runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

These ArcadeDB-only tables also show worker startup timing
separately from query execution, using the worker-side
`worker_startup` metrics recorded in the raw JSON.
`open` is already reported here; worker close time is not currently
recorded in the raw benchmark payloads.

##### OLTP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `322.13 ms +- 4.52` | `296.42 ms +- 6.11` |
| `oltp_create_type1_node` | `320.05 ms +- 8.16` | `291.78 ms +- 2.82` |
| `oltp_cross_type_lookup` | `318.54 ms +- 5.19` | `294.89 ms +- 5.25` |
| `oltp_delete_type1_edge` | `308.84 ms +- 6.98` | `284.69 ms +- 8.66` |
| `oltp_delete_type1_node` | `319.12 ms +- 3.57` | `291.34 ms +- 6.62` |
| `oltp_merge_cross_type_edge` | `320.22 ms +- 4.72` | `288.41 ms +- 4.63` |
| `oltp_program_create_and_link` | `324.78 ms +- 10.53` | `298.02 ms +- 8.76` |
| `oltp_type1_neighbors` | `321.49 ms +- 6.50` | `288.70 ms +- 2.53` |
| `oltp_type1_point_lookup` | `316.92 ms +- 5.28` | `280.89 ms +- 5.19` |
| `oltp_unwind_literal_top2` | `322.71 ms +- 7.94` | `285.84 ms +- 6.30` |
| `oltp_update_cross_type_edge_rank` | `313.86 ms +- 7.24` | `293.40 ms +- 4.63` |
| `oltp_update_type1_score` | `314.54 ms +- 3.92` | `295.40 ms +- 3.89` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe execute`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `153.14 ms +- 1.01` | `172.12 ms +- 4.03` |
| `oltp_create_type1_node` | `146.84 ms +- 3.88` | `147.06 ms +- 4.36` |
| `oltp_cross_type_lookup` | `171.90 ms +- 4.29` | `164.42 ms +- 0.32` |
| `oltp_delete_type1_edge` | `145.13 ms +- 2.06` | `164.21 ms +- 4.66` |
| `oltp_delete_type1_node` | `144.94 ms +- 3.06` | `162.07 ms +- 5.51` |
| `oltp_merge_cross_type_edge` | `158.31 ms +- 5.08` | `169.88 ms +- 4.49` |
| `oltp_program_create_and_link` | `157.65 ms +- 1.63` | `163.48 ms +- 0.68` |
| `oltp_type1_neighbors` | `175.02 ms +- 5.70` | `162.93 ms +- 1.15` |
| `oltp_type1_point_lookup` | `161.55 ms +- 8.21` | `165.71 ms +- 7.72` |
| `oltp_unwind_literal_top2` | `175.62 ms +- 2.11` | `149.26 ms +- 4.06` |
| `oltp_update_cross_type_edge_rank` | `154.15 ms +- 3.41` | `161.99 ms +- 3.00` |
| `oltp_update_type1_score` | `146.77 ms +- 1.60` | `159.92 ms +- 6.22` |

##### OLTP ArcadeDB worker startup breakdown, `startup probe reset`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `oltp_create_cross_type_edge` | `0.60 ms +- 0.06` | `0.66 ms +- 0.08` |
| `oltp_create_type1_node` | `0.56 ms +- 0.07` | `0.55 ms +- 0.04` |
| `oltp_cross_type_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_delete_type1_edge` | `0.33 ms +- 0.01` | `0.33 ms +- 0.10` |
| `oltp_delete_type1_node` | `0.28 ms +- 0.02` | `0.23 ms +- 0.03` |
| `oltp_merge_cross_type_edge` | `0.63 ms +- 0.02` | `0.69 ms +- 0.17` |
| `oltp_program_create_and_link` | `0.60 ms +- 0.03` | `0.59 ms +- 0.03` |
| `oltp_type1_neighbors` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_type1_point_lookup` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_unwind_literal_top2` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `0.43 ms +- 0.05` | `0.38 ms +- 0.05` |
| `oltp_update_type1_score` | `0.37 ms +- 0.06` | `0.35 ms +- 0.04` |

##### OLAP ArcadeDB worker startup breakdown, `open`

| Query | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) |
| --- | --- | --- |
| `olap_cross_type_edge_rollup` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_fixed_length_path_projection` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_fixed_length_path_with_rebinding` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_graph_introspection_rollup` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_optional_type1_aggregate` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_relationship_function_projection` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_three_type_path_count` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_type1_active_leaderboard` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_type1_age_rollup` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_type2_score_distribution` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_variable_length_grouped_max_rollup` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_variable_length_grouped_rollup` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_variable_length_reachability` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_with_scalar_rebinding` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_with_size_predicate_projection` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |
| `olap_with_where_lower_projection` | `517.97 ms +- 28.97` | `501.24 ms +- 0.05` |

##### OLAP ArcadeDB worker startup breakdown, `startup probe execute`

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
| `oltp_create_cross_type_edge` | `1.68 ms +- 0.08` | `1.61 ms +- 0.01` | `2.87 ms +- 0.63` | `2.65 ms +- 0.23` | `1.87 ms +- 0.07` | `1.95 ms +- 0.05` | `0.27 ms +- 0.00` | `0.50 ms +- 0.02` | `0.06 ms +- 0.01` | `0.84 ms +- 0.02` | `0.74 ms +- 0.06` |
| `oltp_create_type1_node` | `0.85 ms +- 0.03` | `0.79 ms +- 0.00` | `1.47 ms +- 0.18` | `1.35 ms +- 0.06` | `1.01 ms +- 0.04` | `0.97 ms +- 0.01` | `0.22 ms +- 0.00` | `0.20 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` | `0.24 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.48 ms +- 0.13` | `1.48 ms +- 0.01` | `2.77 ms +- 0.45` | `2.30 ms +- 0.12` | `1.79 ms +- 0.02` | `1.87 ms +- 0.00` | `0.26 ms +- 0.00` | `0.37 ms +- 0.01` | `0.02 ms +- 0.01` | `0.41 ms +- 0.01` | `2.49 ms +- 0.88` |
| `oltp_delete_type1_edge` | `0.93 ms +- 0.08` | `0.98 ms +- 0.05` | `2.13 ms +- 0.12` | `1.90 ms +- 0.11` | `1.24 ms +- 0.01` | `1.37 ms +- 0.02` | `0.22 ms +- 0.01` | `0.32 ms +- 0.01` | `0.04 ms +- 0.00` | `0.42 ms +- 0.00` | `1.72 ms +- 0.84` |
| `oltp_delete_type1_node` | `0.69 ms +- 0.04` | `1.19 ms +- 0.11` | `0.91 ms +- 0.01` | `0.90 ms +- 0.01` | `0.79 ms +- 0.01` | `1.18 ms +- 0.01` | `0.22 ms +- 0.01` | `0.34 ms +- 0.01` | `0.06 ms +- 0.00` | `0.44 ms +- 0.00` | `0.48 ms +- 0.05` |
| `oltp_merge_cross_type_edge` | `1.99 ms +- 0.13` | `1.93 ms +- 0.02` | `3.66 ms +- 0.28` | `3.46 ms +- 0.06` | `2.25 ms +- 0.01` | `2.56 ms +- 0.10` | `0.27 ms +- 0.01` | `0.50 ms +- 0.02` | `0.05 ms +- 0.00` | `0.84 ms +- 0.01` | `5.06 ms +- 0.21` |
| `oltp_program_create_and_link` | `2.29 ms +- 0.29` | `2.05 ms +- 0.20` | `3.24 ms +- 0.01` | `3.83 ms +- 0.99` | `2.33 ms +- 0.02` | `2.36 ms +- 0.06` | `0.23 ms +- 0.01` | `0.33 ms +- 0.01` | `0.05 ms +- 0.00` | `0.44 ms +- 0.01` | `0.68 ms +- 0.07` |
| `oltp_type1_neighbors` | `1.20 ms +- 0.09` | `1.26 ms +- 0.01` | `2.29 ms +- 0.32` | `2.34 ms +- 0.60` | `1.56 ms +- 0.06` | `1.62 ms +- 0.01` | `0.26 ms +- 0.01` | `0.38 ms +- 0.02` | `0.04 ms +- 0.01` | `0.41 ms +- 0.01` | `2.62 ms +- 1.23` |
| `oltp_type1_point_lookup` | `1.13 ms +- 0.02` | `1.15 ms +- 0.01` | `1.42 ms +- 0.06` | `1.46 ms +- 0.11` | `1.37 ms +- 0.05` | `1.36 ms +- 0.02` | `0.30 ms +- 0.01` | `0.42 ms +- 0.02` | `0.03 ms +- 0.00` | `0.41 ms +- 0.01` | `0.37 ms +- 0.03` |
| `oltp_unwind_literal_top2` | `1.04 ms +- 0.06` | `0.98 ms +- 0.00` | `1.35 ms +- 0.07` | `1.35 ms +- 0.08` | `1.19 ms +- 0.01` | `1.19 ms +- 0.00` | `0.23 ms +- 0.01` | `0.22 ms +- 0.02` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.53 ms +- 0.07` |
| `oltp_update_cross_type_edge_rank` | `1.14 ms +- 0.07` | `1.16 ms +- 0.05` | `2.54 ms +- 0.07` | `2.07 ms +- 0.06` | `1.42 ms +- 0.00` | `1.68 ms +- 0.07` | `0.22 ms +- 0.01` | `0.32 ms +- 0.01` | `0.02 ms +- 0.00` | `0.41 ms +- 0.00` | `1.67 ms +- 0.76` |
| `oltp_update_type1_score` | `0.86 ms +- 0.03` | `0.83 ms +- 0.01` | `1.63 ms +- 0.25` | `1.31 ms +- 0.20` | `1.03 ms +- 0.03` | `1.09 ms +- 0.02` | `0.23 ms +- 0.01` | `0.34 ms +- 0.02` | `0.02 ms +- 0.00` | `0.41 ms +- 0.01` | `0.39 ms +- 0.02` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.27 ms +- 0.46` | `1.72 ms +- 0.01` | `4.36 ms +- 2.05` | `4.13 ms +- 1.95` | `2.39 ms +- 0.24` | `2.36 ms +- 0.23` | `0.35 ms +- 0.01` | `0.78 ms +- 0.19` | `0.15 ms +- 0.01` | `0.88 ms +- 0.02` | `1.21 ms +- 0.60` |
| `oltp_create_type1_node` | `1.07 ms +- 0.23` | `0.84 ms +- 0.01` | `2.70 ms +- 1.04` | `2.10 ms +- 0.83` | `1.38 ms +- 0.15` | `1.14 ms +- 0.02` | `0.29 ms +- 0.01` | `0.31 ms +- 0.06` | `0.03 ms +- 0.00` | `0.02 ms +- 0.00` | `0.39 ms +- 0.15` |
| `oltp_cross_type_lookup` | `2.04 ms +- 0.48` | `1.59 ms +- 0.08` | `5.60 ms +- 3.17` | `2.78 ms +- 0.21` | `2.37 ms +- 0.07` | `2.40 ms +- 0.05` | `0.37 ms +- 0.04` | `0.52 ms +- 0.05` | `0.04 ms +- 0.01` | `0.43 ms +- 0.00` | `9.39 ms +- 9.82` |
| `oltp_delete_type1_edge` | `1.32 ms +- 0.30` | `1.26 ms +- 0.44` | `3.53 ms +- 1.73` | `2.40 ms +- 0.07` | `1.69 ms +- 0.09` | `1.65 ms +- 0.16` | `0.29 ms +- 0.00` | `0.52 ms +- 0.18` | `0.06 ms +- 0.01` | `0.45 ms +- 0.00` | `4.33 ms +- 2.97` |
| `oltp_delete_type1_node` | `0.95 ms +- 0.28` | `1.57 ms +- 0.68` | `1.10 ms +- 0.14` | `1.10 ms +- 0.03` | `1.06 ms +- 0.11` | `1.44 ms +- 0.13` | `0.29 ms +- 0.00` | `0.50 ms +- 0.09` | `0.08 ms +- 0.00` | `0.46 ms +- 0.01` | `0.80 ms +- 0.40` |
| `oltp_merge_cross_type_edge` | `2.77 ms +- 0.62` | `2.04 ms +- 0.03` | `5.49 ms +- 2.11` | `5.00 ms +- 1.57` | `2.92 ms +- 0.05` | `3.18 ms +- 0.28` | `0.35 ms +- 0.00` | `0.81 ms +- 0.27` | `0.14 ms +- 0.01` | `0.94 ms +- 0.03` | `29.45 ms +- 0.95` |
| `oltp_program_create_and_link` | `3.62 ms +- 1.37` | `2.49 ms +- 0.86` | `3.98 ms +- 0.17` | `5.44 ms +- 2.61` | `2.87 ms +- 0.08` | `2.77 ms +- 0.21` | `0.30 ms +- 0.01` | `0.46 ms +- 0.04` | `0.08 ms +- 0.04` | `0.46 ms +- 0.01` | `1.09 ms +- 0.53` |
| `oltp_type1_neighbors` | `1.48 ms +- 0.37` | `1.34 ms +- 0.06` | `4.73 ms +- 2.47` | `3.52 ms +- 1.83` | `2.09 ms +- 0.13` | `2.10 ms +- 0.05` | `0.38 ms +- 0.05` | `0.56 ms +- 0.13` | `0.06 ms +- 0.00` | `0.44 ms +- 0.01` | `11.61 ms +- 13.79` |
| `oltp_type1_point_lookup` | `1.34 ms +- 0.19` | `1.25 ms +- 0.04` | `2.07 ms +- 0.40` | `2.04 ms +- 0.62` | `1.83 ms +- 0.23` | `1.69 ms +- 0.13` | `0.52 ms +- 0.07` | `0.82 ms +- 0.22` | `0.05 ms +- 0.00` | `0.65 ms +- 0.01` | `0.56 ms +- 0.23` |
| `oltp_unwind_literal_top2` | `1.26 ms +- 0.28` | `1.04 ms +- 0.02` | `2.06 ms +- 0.56` | `1.83 ms +- 0.55` | `1.54 ms +- 0.09` | `1.49 ms +- 0.04` | `0.34 ms +- 0.04` | `0.32 ms +- 0.07` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.97 ms +- 0.47` |
| `oltp_update_cross_type_edge_rank` | `1.80 ms +- 0.49` | `1.28 ms +- 0.20` | `3.20 ms +- 0.24` | `2.65 ms +- 0.13` | `1.88 ms +- 0.04` | `2.09 ms +- 0.20` | `0.32 ms +- 0.06` | `0.44 ms +- 0.04` | `0.03 ms +- 0.00` | `0.42 ms +- 0.01` | `4.09 ms +- 2.89` |
| `oltp_update_type1_score` | `1.09 ms +- 0.18` | `0.89 ms +- 0.00` | `3.10 ms +- 1.37` | `2.09 ms +- 1.05` | `1.34 ms +- 0.14` | `1.37 ms +- 0.05` | `0.33 ms +- 0.06` | `0.49 ms +- 0.08` | `0.04 ms +- 0.00` | `0.43 ms +- 0.00` | `0.63 ms +- 0.27` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.84 ms +- 0.63` | `2.04 ms +- 0.07` | `5.10 ms +- 2.47` | `4.96 ms +- 2.66` | `2.80 ms +- 0.21` | `2.80 ms +- 0.19` | `0.44 ms +- 0.06` | `1.33 ms +- 0.93` | `0.18 ms +- 0.02` | `1.06 ms +- 0.05` | `1.56 ms +- 1.03` |
| `oltp_create_type1_node` | `1.41 ms +- 0.43` | `1.01 ms +- 0.07` | `3.71 ms +- 1.66` | `2.78 ms +- 1.36` | `1.70 ms +- 0.23` | `1.47 ms +- 0.02` | `0.38 ms +- 0.04` | `0.61 ms +- 0.41` | `0.04 ms +- 0.00` | `0.04 ms +- 0.01` | `0.92 ms +- 0.95` |
| `oltp_cross_type_lookup` | `2.66 ms +- 0.65` | `1.87 ms +- 0.24` | `7.03 ms +- 4.28` | `3.33 ms +- 0.30` | `2.69 ms +- 0.07` | `2.78 ms +- 0.04` | `0.50 ms +- 0.09` | `0.69 ms +- 0.19` | `0.05 ms +- 0.01` | `0.51 ms +- 0.05` | `33.36 ms +- 4.11` |
| `oltp_delete_type1_edge` | `1.80 ms +- 0.46` | `1.83 ms +- 1.03` | `4.46 ms +- 2.60` | `2.95 ms +- 0.04` | `2.05 ms +- 0.06` | `2.07 ms +- 0.15` | `0.37 ms +- 0.04` | `0.82 ms +- 0.54` | `0.07 ms +- 0.00` | `0.58 ms +- 0.02` | `33.77 ms +- 5.20` |
| `oltp_delete_type1_node` | `1.32 ms +- 0.59` | `2.05 ms +- 1.20` | `1.44 ms +- 0.14` | `1.47 ms +- 0.07` | `1.38 ms +- 0.08` | `1.81 ms +- 0.15` | `0.35 ms +- 0.04` | `0.73 ms +- 0.33` | `0.09 ms +- 0.00` | `0.51 ms +- 0.01` | `1.02 ms +- 0.60` |
| `oltp_merge_cross_type_edge` | `3.35 ms +- 0.78` | `2.33 ms +- 0.15` | `6.59 ms +- 3.22` | `6.05 ms +- 2.58` | `3.35 ms +- 0.02` | `3.71 ms +- 0.27` | `0.43 ms +- 0.04` | `1.26 ms +- 0.85` | `0.18 ms +- 0.02` | `1.46 ms +- 0.01` | `38.26 ms +- 0.83` |
| `oltp_program_create_and_link` | `4.60 ms +- 2.06` | `2.88 ms +- 1.25` | `4.57 ms +- 0.35` | `6.28 ms +- 2.92` | `3.37 ms +- 0.03` | `3.30 ms +- 0.15` | `0.37 ms +- 0.04` | `0.62 ms +- 0.09` | `0.10 ms +- 0.05` | `0.57 ms +- 0.03` | `1.32 ms +- 0.74` |
| `oltp_type1_neighbors` | `1.98 ms +- 0.61` | `1.60 ms +- 0.23` | `6.36 ms +- 3.68` | `4.14 ms +- 2.13` | `2.40 ms +- 0.14` | `2.50 ms +- 0.07` | `0.52 ms +- 0.10` | `0.76 ms +- 0.35` | `0.07 ms +- 0.00` | `0.52 ms +- 0.06` | `33.96 ms +- 4.93` |
| `oltp_type1_point_lookup` | `1.71 ms +- 0.28` | `1.56 ms +- 0.15` | `3.05 ms +- 0.99` | `2.68 ms +- 0.91` | `2.18 ms +- 0.26` | `2.05 ms +- 0.06` | `0.84 ms +- 0.23` | `1.48 ms +- 0.68` | `0.07 ms +- 0.00` | `0.73 ms +- 0.02` | `0.72 ms +- 0.42` |
| `oltp_unwind_literal_top2` | `1.65 ms +- 0.51` | `1.22 ms +- 0.01` | `2.79 ms +- 0.81` | `2.37 ms +- 0.81` | `1.88 ms +- 0.08` | `1.81 ms +- 0.04` | `0.47 ms +- 0.09` | `0.43 ms +- 0.12` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `1.73 ms +- 1.59` |
| `oltp_update_cross_type_edge_rank` | `2.97 ms +- 1.17` | `1.41 ms +- 0.22` | `3.70 ms +- 0.40` | `3.20 ms +- 0.34` | `2.29 ms +- 0.04` | `2.54 ms +- 0.24` | `0.44 ms +- 0.11` | `0.57 ms +- 0.10` | `0.04 ms +- 0.00` | `0.47 ms +- 0.02` | `29.66 ms +- 6.26` |
| `oltp_update_type1_score` | `1.42 ms +- 0.33` | `1.08 ms +- 0.01` | `4.25 ms +- 2.07` | `2.73 ms +- 1.51` | `1.70 ms +- 0.14` | `1.81 ms +- 0.06` | `0.47 ms +- 0.10` | `0.66 ms +- 0.19` | `0.04 ms +- 0.00` | `0.50 ms +- 0.02` | `1.23 ms +- 1.18` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `2.44 ms +- 0.28` | `1.95 ms +- 0.10` | `2.46 ms +- 0.11` | `2.86 ms +- 0.51` | `2.65 ms +- 0.11` | `2.54 ms +- 0.12` | `2.23 ms +- 0.19` | `2.09 ms +- 0.14` | `3.67 ms +- 0.10` | `3.70 ms +- 0.04` | `4.67 ms +- 0.17` |
| `olap_fixed_length_path_projection` | `2.14 ms +- 0.46` | `3.66 ms +- 0.16` | `3.95 ms +- 0.31` | `4.39 ms +- 0.95` | `5.26 ms +- 0.39` | `4.53 ms +- 0.34` | `5.46 ms +- 0.82` | `4.73 ms +- 0.19` | `4.62 ms +- 0.53` | `4.02 ms +- 0.07` | `18.73 ms +- 0.51` |
| `olap_fixed_length_path_with_rebinding` | `4.37 ms +- 0.80` | `4.17 ms +- 0.16` | `4.62 ms +- 0.50` | `5.08 ms +- 1.30` | `5.51 ms +- 0.39` | `4.67 ms +- 0.27` | `5.76 ms +- 0.85` | `5.13 ms +- 0.08` | `5.06 ms +- 0.26` | `4.68 ms +- 0.02` | `8.08 ms +- 0.52` |
| `olap_graph_introspection_rollup` | `2.09 ms +- 0.29` | `2.18 ms +- 0.24` | `2.77 ms +- 0.15` | `3.14 ms +- 0.79` | `3.01 ms +- 0.12` | `2.84 ms +- 0.05` | `3.15 ms +- 0.29` | `2.94 ms +- 0.11` | `6.47 ms +- 0.23` | `6.20 ms +- 0.13` | `3.21 ms +- 0.09` |
| `olap_optional_type1_aggregate` | `1.47 ms +- 0.14` | `1.38 ms +- 0.05` | `1.93 ms +- 0.08` | `2.04 ms +- 0.13` | `1.59 ms +- 0.03` | `1.57 ms +- 0.04` | `0.72 ms +- 0.02` | `0.71 ms +- 0.03` | `0.61 ms +- 0.01` | `0.60 ms +- 0.01` | `1.45 ms +- 0.54` |
| `olap_relationship_function_projection` | `3.23 ms +- 0.49` | `2.48 ms +- 0.39` | `2.96 ms +- 0.07` | `3.48 ms +- 0.82` | `3.71 ms +- 0.05` | `3.52 ms +- 0.16` | `2.64 ms +- 0.18` | `2.47 ms +- 0.13` | `4.78 ms +- 0.22` | `4.51 ms +- 0.07` | `3.25 ms +- 0.14` |
| `olap_three_type_path_count` | `2.61 ms +- 0.16` | `2.46 ms +- 0.11` | `2.35 ms +- 0.18` | `2.60 ms +- 0.56` | `3.18 ms +- 0.15` | `2.84 ms +- 0.14` | `1.95 ms +- 0.12` | `1.87 ms +- 0.05` | `0.06 ms +- 0.01` | `0.06 ms +- 0.00` | `5.34 ms +- 0.34` |
| `olap_type1_active_leaderboard` | `1.39 ms +- 0.11` | `1.38 ms +- 0.06` | `2.06 ms +- 0.03` | `2.10 ms +- 0.02` | `1.60 ms +- 0.03` | `1.59 ms +- 0.04` | `1.47 ms +- 0.05` | `1.36 ms +- 0.02` | `0.92 ms +- 0.02` | `1.53 ms +- 0.46` | `0.56 ms +- 0.14` |
| `olap_type1_age_rollup` | `1.69 ms +- 0.16` | `1.56 ms +- 0.06` | `1.92 ms +- 0.03` | `2.03 ms +- 0.12` | `1.74 ms +- 0.02` | `1.73 ms +- 0.02` | `0.82 ms +- 0.02` | `0.81 ms +- 0.03` | `0.57 ms +- 0.00` | `0.58 ms +- 0.01` | `1.65 ms +- 0.38` |
| `olap_type2_score_distribution` | `1.76 ms +- 0.28` | `1.70 ms +- 0.10` | `2.11 ms +- 0.11` | `2.30 ms +- 0.35` | `1.87 ms +- 0.05` | `1.83 ms +- 0.03` | `0.79 ms +- 0.04` | `0.72 ms +- 0.04` | `0.59 ms +- 0.01` | `0.58 ms +- 0.00` | `2.12 ms +- 0.19` |
| `olap_variable_length_grouped_max_rollup` | `2.59 ms +- 0.69` | `3.17 ms +- 0.65` | `4.36 ms +- 0.22` | `5.16 ms +- 1.20` | `3.13 ms +- 0.09` | `3.31 ms +- 0.18` | `0.26 ms +- 0.02` | `0.38 ms +- 0.02` | `0.03 ms +- 0.01` | `0.43 ms +- 0.00` | `4.17 ms +- 1.40` |
| `olap_variable_length_grouped_rollup` | `6.80 ms +- 1.64` | `6.06 ms +- 1.16` | `4.38 ms +- 0.20` | `4.95 ms +- 1.34` | `6.24 ms +- 0.17` | `5.70 ms +- 0.48` | `10.71 ms +- 1.89` | `9.67 ms +- 0.28` | `15.96 ms +- 0.48` | `16.03 ms +- 0.33` | `22.37 ms +- 1.34` |
| `olap_variable_length_reachability` | `2.06 ms +- 0.28` | `2.54 ms +- 0.11` | `3.78 ms +- 0.29` | `4.30 ms +- 0.64` | `2.79 ms +- 0.06` | `2.91 ms +- 0.10` | `0.41 ms +- 0.01` | `0.50 ms +- 0.01` | `0.07 ms +- 0.00` | `0.46 ms +- 0.01` | `1.84 ms +- 0.23` |
| `olap_with_scalar_rebinding` | `2.63 ms +- 0.71` | `2.11 ms +- 0.24` | `2.50 ms +- 0.09` | `2.81 ms +- 0.67` | `2.38 ms +- 0.11` | `2.33 ms +- 0.04` | `0.89 ms +- 0.07` | `0.85 ms +- 0.02` | `0.68 ms +- 0.01` | `0.68 ms +- 0.02` | `2.25 ms +- 0.05` |
| `olap_with_size_predicate_projection` | `2.15 ms +- 0.36` | `1.92 ms +- 0.18` | `2.31 ms +- 0.04` | `2.53 ms +- 0.47` | `2.32 ms +- 0.05` | `2.28 ms +- 0.08` | `1.31 ms +- 0.09` | `1.22 ms +- 0.04` | `1.25 ms +- 0.07` | `1.15 ms +- 0.02` | `0.78 ms +- 0.11` |
| `olap_with_where_lower_projection` | `2.21 ms +- 0.38` | `1.82 ms +- 0.13` | `2.18 ms +- 0.06` | `2.40 ms +- 0.54` | `2.12 ms +- 0.05` | `2.10 ms +- 0.05` | `1.36 ms +- 0.12` | `1.27 ms +- 0.03` | `1.34 ms +- 0.05` | `1.26 ms +- 0.03` | `0.70 ms +- 0.09` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.04 ms +- 1.04` | `2.19 ms +- 0.46` | `3.09 ms +- 0.24` | `3.92 ms +- 1.26` | `3.31 ms +- 0.33` | `3.07 ms +- 0.19` | `3.28 ms +- 0.67` | `2.39 ms +- 0.23` | `6.01 ms +- 1.38` | `4.78 ms +- 0.54` | `46.25 ms +- 3.04` |
| `olap_fixed_length_path_projection` | `3.46 ms +- 1.35` | `3.85 ms +- 0.42` | `4.68 ms +- 0.38` | `5.67 ms +- 1.75` | `6.22 ms +- 0.32` | `5.46 ms +- 0.55` | `6.86 ms +- 1.20` | `5.66 ms +- 0.50` | `4.86 ms +- 0.50` | `6.04 ms +- 2.89` | `35.97 ms +- 10.81` |
| `olap_fixed_length_path_with_rebinding` | `7.47 ms +- 3.39` | `4.69 ms +- 0.75` | `5.49 ms +- 0.56` | `6.63 ms +- 2.63` | `6.55 ms +- 0.31` | `5.50 ms +- 0.28` | `7.23 ms +- 1.34` | `6.00 ms +- 0.17` | `6.04 ms +- 0.31` | `5.80 ms +- 0.02` | `61.36 ms +- 2.37` |
| `olap_graph_introspection_rollup` | `3.42 ms +- 1.12` | `2.72 ms +- 1.10` | `3.21 ms +- 0.35` | `4.11 ms +- 1.71` | `3.73 ms +- 0.07` | `3.43 ms +- 0.26` | `4.32 ms +- 0.92` | `3.46 ms +- 0.24` | `7.30 ms +- 0.53` | `6.80 ms +- 0.31` | `49.71 ms +- 0.45` |
| `olap_optional_type1_aggregate` | `2.24 ms +- 0.65` | `1.48 ms +- 0.13` | `2.41 ms +- 0.12` | `2.87 ms +- 1.04` | `2.01 ms +- 0.17` | `1.79 ms +- 0.21` | `1.03 ms +- 0.13` | `0.85 ms +- 0.07` | `0.88 ms +- 0.12` | `0.83 ms +- 0.10` | `2.72 ms +- 0.22` |
| `olap_relationship_function_projection` | `5.84 ms +- 2.23` | `3.05 ms +- 1.32` | `3.51 ms +- 0.16` | `4.52 ms +- 1.94` | `4.90 ms +- 0.21` | `4.07 ms +- 0.34` | `3.43 ms +- 0.70` | `2.96 ms +- 0.40` | `5.24 ms +- 0.11` | `4.82 ms +- 0.12` | `44.04 ms +- 3.91` |
| `olap_three_type_path_count` | `4.60 ms +- 1.62` | `2.72 ms +- 0.42` | `2.89 ms +- 0.36` | `3.67 ms +- 1.63` | `4.01 ms +- 0.14` | `3.45 ms +- 0.32` | `2.81 ms +- 0.48` | `2.29 ms +- 0.03` | `0.13 ms +- 0.00` | `0.12 ms +- 0.01` | `69.19 ms +- 5.06` |
| `olap_type1_active_leaderboard` | `2.02 ms +- 0.48` | `1.53 ms +- 0.26` | `2.58 ms +- 0.20` | `2.58 ms +- 0.20` | `1.94 ms +- 0.20` | `1.98 ms +- 0.22` | `2.10 ms +- 0.31` | `1.74 ms +- 0.15` | `1.58 ms +- 0.61` | `2.77 ms +- 0.52` | `0.84 ms +- 0.10` |
| `olap_type1_age_rollup` | `2.55 ms +- 0.56` | `1.66 ms +- 0.14` | `2.37 ms +- 0.10` | `2.86 ms +- 0.95` | `2.15 ms +- 0.25` | `1.96 ms +- 0.26` | `1.24 ms +- 0.23` | `1.09 ms +- 0.16` | `0.82 ms +- 0.06` | `0.71 ms +- 0.02` | `2.75 ms +- 0.16` |
| `olap_type2_score_distribution` | `3.10 ms +- 1.22` | `1.87 ms +- 0.36` | `2.59 ms +- 0.35` | `3.11 ms +- 1.26` | `2.32 ms +- 0.29` | `2.16 ms +- 0.11` | `1.13 ms +- 0.17` | `0.85 ms +- 0.09` | `0.60 ms +- 0.01` | `0.64 ms +- 0.06` | `3.01 ms +- 0.37` |
| `olap_variable_length_grouped_max_rollup` | `4.37 ms +- 2.59` | `3.87 ms +- 1.79` | `5.11 ms +- 0.19` | `6.62 ms +- 2.34` | `3.76 ms +- 0.14` | `3.83 ms +- 0.43` | `0.36 ms +- 0.07` | `0.50 ms +- 0.04` | `0.05 ms +- 0.01` | `0.47 ms +- 0.04` | `5.57 ms +- 2.34` |
| `olap_variable_length_grouped_rollup` | `9.39 ms +- 4.43` | `6.87 ms +- 2.45` | `5.31 ms +- 0.17` | `6.36 ms +- 2.70` | `7.30 ms +- 0.22` | `6.47 ms +- 0.69` | `12.49 ms +- 2.71` | `10.90 ms +- 0.06` | `17.29 ms +- 0.47` | `17.45 ms +- 0.38` | `25.37 ms +- 2.88` |
| `olap_variable_length_reachability` | `3.21 ms +- 1.01` | `2.68 ms +- 0.29` | `4.59 ms +- 0.45` | `5.59 ms +- 1.59` | `3.46 ms +- 0.28` | `3.38 ms +- 0.22` | `0.57 ms +- 0.06` | `0.62 ms +- 0.04` | `0.10 ms +- 0.00` | `0.56 ms +- 0.09` | `2.56 ms +- 0.39` |
| `olap_with_scalar_rebinding` | `4.44 ms +- 1.95` | `2.61 ms +- 1.04` | `2.92 ms +- 0.22` | `3.56 ms +- 1.46` | `2.64 ms +- 0.21` | `2.78 ms +- 0.34` | `1.19 ms +- 0.32` | `1.22 ms +- 0.17` | `1.01 ms +- 0.11` | `0.89 ms +- 0.13` | `3.15 ms +- 0.39` |
| `olap_with_size_predicate_projection` | `3.82 ms +- 1.70` | `2.29 ms +- 0.77` | `2.75 ms +- 0.16` | `3.40 ms +- 1.29` | `2.93 ms +- 0.08` | `2.66 ms +- 0.31` | `1.75 ms +- 0.24` | `1.49 ms +- 0.14` | `1.45 ms +- 0.03` | `1.24 ms +- 0.06` | `0.94 ms +- 0.17` |
| `olap_with_where_lower_projection` | `3.22 ms +- 1.48` | `2.18 ms +- 0.69` | `2.62 ms +- 0.35` | `3.09 ms +- 1.47` | `2.63 ms +- 0.19` | `2.51 ms +- 0.22` | `1.82 ms +- 0.36` | `1.67 ms +- 0.11` | `1.80 ms +- 0.09` | `1.57 ms +- 0.24` | `0.92 ms +- 0.19` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed (3) | SQLite Unindexed (3) | DuckDB Indexed (3) | DuckDB Unindexed (3) | PostgreSQL Indexed (3) | PostgreSQL Unindexed (3) | Neo4j Indexed (3) | Neo4j Unindexed (3) | ArcadeDB Indexed (3) | ArcadeDB Unindexed (3) | LadybugDB Unindexed (3) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.88 ms +- 1.33` | `2.23 ms +- 0.47` | `3.52 ms +- 0.21` | `4.48 ms +- 1.42` | `4.01 ms +- 0.91` | `3.45 ms +- 0.06` | `3.98 ms +- 0.61` | `2.95 ms +- 0.47` | `6.36 ms +- 1.50` | `6.52 ms +- 1.10` | `53.42 ms +- 2.35` |
| `olap_fixed_length_path_projection` | `4.99 ms +- 2.34` | `4.16 ms +- 0.77` | `5.33 ms +- 0.31` | `6.40 ms +- 2.22` | `7.24 ms +- 0.35` | `6.17 ms +- 0.38` | `8.03 ms +- 1.49` | `6.72 ms +- 0.66` | `6.18 ms +- 0.26` | `7.05 ms +- 2.28` | `46.85 ms +- 10.17` |
| `olap_fixed_length_path_with_rebinding` | `8.62 ms +- 4.09` | `5.06 ms +- 1.07` | `6.04 ms +- 0.50` | `7.39 ms +- 2.85` | `7.30 ms +- 0.29` | `6.13 ms +- 0.28` | `8.61 ms +- 1.12` | `7.46 ms +- 0.50` | `6.62 ms +- 0.19` | `6.52 ms +- 0.26` | `65.99 ms +- 1.88` |
| `olap_graph_introspection_rollup` | `4.44 ms +- 1.78` | `3.15 ms +- 1.71` | `3.71 ms +- 0.30` | `4.62 ms +- 1.76` | `4.42 ms +- 0.12` | `3.96 ms +- 0.41` | `5.20 ms +- 1.00` | `4.38 ms +- 0.25` | `8.06 ms +- 0.17` | `7.54 ms +- 0.46` | `57.44 ms +- 0.33` |
| `olap_optional_type1_aggregate` | `3.15 ms +- 1.58` | `1.55 ms +- 0.18` | `2.81 ms +- 0.16` | `3.45 ms +- 1.37` | `2.31 ms +- 0.14` | `2.03 ms +- 0.26` | `1.25 ms +- 0.16` | `1.14 ms +- 0.05` | `1.26 ms +- 0.29` | `0.91 ms +- 0.14` | `4.66 ms +- 2.41` |
| `olap_relationship_function_projection` | `6.88 ms +- 2.90` | `3.29 ms +- 1.71` | `4.04 ms +- 0.13` | `5.14 ms +- 2.26` | `5.55 ms +- 0.42` | `4.52 ms +- 0.41` | `4.23 ms +- 0.86` | `3.65 ms +- 0.37` | `6.13 ms +- 0.42` | `5.82 ms +- 0.27` | `55.30 ms +- 2.17` |
| `olap_three_type_path_count` | `5.76 ms +- 2.34` | `2.91 ms +- 0.61` | `3.36 ms +- 0.33` | `4.18 ms +- 1.88` | `4.67 ms +- 0.33` | `3.82 ms +- 0.37` | `3.52 ms +- 0.69` | `3.25 ms +- 0.02` | `0.14 ms +- 0.00` | `0.14 ms +- 0.00` | `75.16 ms +- 2.99` |
| `olap_type1_active_leaderboard` | `3.11 ms +- 1.23` | `1.58 ms +- 0.31` | `3.03 ms +- 0.21` | `3.02 ms +- 0.25` | `2.25 ms +- 0.22` | `2.28 ms +- 0.19` | `2.52 ms +- 0.45` | `1.98 ms +- 0.19` | `2.31 ms +- 0.90` | `2.95 ms +- 0.62` | `1.13 ms +- 0.02` |
| `olap_type1_age_rollup` | `3.54 ms +- 1.54` | `1.78 ms +- 0.30` | `2.79 ms +- 0.20` | `3.38 ms +- 1.23` | `2.54 ms +- 0.19` | `2.32 ms +- 0.22` | `1.43 ms +- 0.21` | `1.38 ms +- 0.16` | `1.28 ms +- 0.36` | `0.90 ms +- 0.11` | `4.91 ms +- 2.36` |
| `olap_type2_score_distribution` | `4.33 ms +- 1.87` | `1.96 ms +- 0.37` | `2.98 ms +- 0.35` | `3.62 ms +- 1.48` | `2.75 ms +- 0.30` | `2.55 ms +- 0.09` | `1.36 ms +- 0.24` | `1.09 ms +- 0.21` | `0.69 ms +- 0.15` | `0.84 ms +- 0.37` | `10.30 ms +- 3.50` |
| `olap_variable_length_grouped_max_rollup` | `5.03 ms +- 3.19` | `4.19 ms +- 2.26` | `5.57 ms +- 0.27` | `7.46 ms +- 2.73` | `4.32 ms +- 0.13` | `4.35 ms +- 0.42` | `0.51 ms +- 0.15` | `0.58 ms +- 0.09` | `0.05 ms +- 0.00` | `0.60 ms +- 0.14` | `8.34 ms +- 3.60` |
| `olap_variable_length_grouped_rollup` | `10.25 ms +- 4.63` | `7.30 ms +- 3.03` | `5.88 ms +- 0.24` | `7.14 ms +- 2.97` | `8.22 ms +- 0.57` | `7.35 ms +- 0.94` | `14.43 ms +- 3.81` | `12.70 ms +- 0.47` | `18.57 ms +- 0.71` | `18.65 ms +- 0.62` | `26.76 ms +- 2.91` |
| `olap_variable_length_reachability` | `4.31 ms +- 1.74` | `2.94 ms +- 0.57` | `4.90 ms +- 0.57` | `6.14 ms +- 1.86` | `4.04 ms +- 0.37` | `3.73 ms +- 0.33` | `0.73 ms +- 0.12` | `0.73 ms +- 0.06` | `0.11 ms +- 0.00` | `0.79 ms +- 0.37` | `3.55 ms +- 1.06` |
| `olap_with_scalar_rebinding` | `5.77 ms +- 2.57` | `2.95 ms +- 1.47` | `3.42 ms +- 0.20` | `4.14 ms +- 1.85` | `3.03 ms +- 0.26` | `3.28 ms +- 0.43` | `1.59 ms +- 0.23` | `1.48 ms +- 0.13` | `1.07 ms +- 0.10` | `1.07 ms +- 0.28` | `8.10 ms +- 5.01` |
| `olap_with_size_predicate_projection` | `5.22 ms +- 2.77` | `2.56 ms +- 1.17` | `3.34 ms +- 0.21` | `3.95 ms +- 1.75` | `3.25 ms +- 0.01` | `3.03 ms +- 0.46` | `2.19 ms +- 0.70` | `1.73 ms +- 0.18` | `1.67 ms +- 0.06` | `1.40 ms +- 0.06` | `1.02 ms +- 0.22` |
| `olap_with_where_lower_projection` | `4.13 ms +- 2.43` | `2.46 ms +- 1.04` | `2.99 ms +- 0.36` | `3.58 ms +- 1.89` | `3.08 ms +- 0.08` | `2.93 ms +- 0.27` | `2.15 ms +- 0.58` | `1.94 ms +- 0.20` | `1.91 ms +- 0.18` | `1.77 ms +- 0.02` | `1.13 ms +- 0.26` |
