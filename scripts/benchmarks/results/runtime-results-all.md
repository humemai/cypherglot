# Runtime Result Summary

- Scanned JSON files: 59
- Completed runs: 53
- Skipped non-completed runs: 6
- Grouped configurations: 19
- Grouped benchmark campaigns: 2

### Medium runtime dataset

The current medium runtime matrix used the `medium` preset with `5000` OLTP iterations / `100` OLTP warmup and `75` OLAP iterations / `5` OLAP warmup.

That corresponds to roughly:

- `600,000` total nodes
- `6,223,200` total edges
- `9` backend/index combinations across SQLite, DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB

Runtime result artifacts for this run now live under
`scripts/benchmarks/results/runtime`.

Versions used for this summarized run:

- `SQLite`: `3.50.4`
- `DuckDB`: `1.5.2`
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
| SQLite Indexed | `14.35 ms +- 5.49` | `7.96 ms +- 2.51` | `67873.79 ms +- 2544.69` | `9849.83 ms +- 347.95` | `3163.97 ms +- 60.92` | `1.06 ms +- 0.01` | `1.25 ms +- 0.03` | `1.62 ms +- 0.05` |
| SQLite Unindexed | `15.33 ms +- 0.34` | `5.79 ms +- 0.26` | `67041.86 ms +- 1151.86` | `282.80 ms +- 11.47` | `327.15 ms +- 1.65` | `134.91 ms +- 2.20` | `141.27 ms +- 1.45` | `145.87 ms +- 1.41` |
| DuckDB Unindexed | `15.56 ms +- 2.43` | `183.76 ms +- 15.13` | `12065.24 ms +- 197.79` | `0.00 ms +- 0.00` | `0.34 ms +- 0.01` | `3.15 ms +- 0.15` | `3.98 ms +- 0.19` | `4.71 ms +- 0.25` |
| PostgreSQL Indexed | `4.91 ms +- 0.52` | `580.10 ms +- 15.95` | `89809.92 ms +- 984.49` | `5413.47 ms +- 101.26` | `5893.93 ms +- 4969.36` | `1.23 ms +- 0.01` | `1.68 ms +- 0.04` | `2.20 ms +- 0.07` |
| PostgreSQL Unindexed | `4.96 ms +- 0.10` | `578.75 ms +- 10.33` | `88771.08 ms +- 978.12` | `91.89 ms +- 16.72` | `5154.95 ms +- 399.55` | `72.37 ms +- 5.40` | `76.48 ms +- 5.40` | `80.48 ms +- 6.29` |
| Neo4j Indexed | `65.72 ms +- 6.22` | `499.94 ms +- 32.21` | `436885.57 ms +- 37328.22` | `13745.39 ms +- 1583.30` | `0.00 ms +- 0.00` | `0.23 ms +- 0.00` | `0.36 ms +- 0.01` | `0.56 ms +- 0.05` |
| ArcadeDB Indexed | `286.34 ms +- 16.90` | `455.80 ms +- 52.74` | `156451.37 ms +- 899.75` | `21121.11 ms +- 782.03` | `0.00 ms +- 0.00` | `0.05 ms +- 0.00` | `0.09 ms +- 0.01` | `0.12 ms +- 0.01` |
| ArcadeDB Unindexed | `301.37 ms +- 20.39` | `457.19 ms +- 60.17` | `156763.93 ms +- 24.19` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `42.73 ms +- 0.29` | `51.94 ms +- 8.16` | `62.07 ms +- 18.06` |
| LadybugDB Unindexed | `77.56 ms +- 7.88` | `68.37 ms +- 1.74` | `3726041.34 ms +- 23467.45` | `0.00 ms +- 0.00` | `19.51 ms +- 0.19` | `4.18 ms +- 0.22` | `5.42 ms +- 0.17` | `6.17 ms +- 0.13` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `8.90 ms +- 0.21` | `6.98 ms +- 1.13` | `69163.67 ms +- 1070.50` | `9798.20 ms +- 316.46` | `3149.91 ms +- 31.66` | `14638.17 ms +- 162.07` | `14937.93 ms +- 157.31` | `15659.80 ms +- 546.86` |
| SQLite Unindexed | `9.02 ms +- 0.07` | `6.08 ms +- 0.05` | `66958.83 ms +- 4390.52` | `288.26 ms +- 25.50` | `341.88 ms +- 67.65` | `14271.77 ms +- 84.67` | `14429.21 ms +- 59.07` | `14469.15 ms +- 89.01` |
| DuckDB Unindexed | `9.62 ms +- 0.12` | `172.97 ms +- 3.06` | `11997.44 ms +- 122.62` | `0.00 ms +- 0.00` | `0.35 ms +- 0.03` | `59.99 ms +- 0.31` | `63.75 ms +- 0.69` | `65.74 ms +- 1.17` |
| PostgreSQL Indexed | `5.65 ms +- 0.99` | `1612.39 ms +- 1194.02` | `89376.88 ms +- 1453.15` | `7371.86 ms +- 3211.34` | `3105.61 ms +- 81.58` | `766.16 ms +- 13.06` | `817.08 ms +- 4.08` | `855.72 ms +- 15.74` |
| PostgreSQL Unindexed | `4.92 ms +- 0.03` | `811.85 ms +- 23.87` | `86875.26 ms +- 169.77` | `92.98 ms +- 3.53` | `7965.89 ms +- 4754.02` | `924.28 ms +- 3.11` | `997.89 ms +- 11.92` | `1055.29 ms +- 8.29` |
| Neo4j Indexed | `65.72 ms +- 6.22` | `499.94 ms +- 32.21` | `436885.57 ms +- 37328.22` | `13745.39 ms +- 1583.30` | `0.00 ms +- 0.00` | `15503.68 ms +- 400.71` | `16243.19 ms +- 598.54` | `16817.64 ms +- 653.82` |
| ArcadeDB Indexed | `2.42 ms +- 1.07` | `52.71 ms +- 2.27` | `181230.24 ms +- 3494.45` | `21198.68 ms +- 1294.61` | `7238.90 ms +- 141.89` | `27240.59 ms +- 270.96` | `29251.49 ms +- 1201.45` | `30290.07 ms +- 1741.19` |
| ArcadeDB Unindexed | `2.13 ms +- 0.34` | `60.14 ms +- 11.53` | `181069.89 ms +- 1768.73` | `0.00 ms +- 0.00` | `6660.51 ms +- 383.98` | `26947.72 ms +- 203.72` | `29469.65 ms +- 207.37` | `30161.39 ms +- 606.60` |
| LadybugDB Unindexed | `80.44 ms +- 2.83` | `49.27 ms +- 7.23` | `3733756.96 ms +- 37987.28` | `0.00 ms +- 0.00` | `21.42 ms +- 2.25` | `2493.06 ms +- 28.03` | `2547.45 ms +- 49.23` | `2561.24 ms +- 49.44` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `102.81 MiB +- 0.04` | `102.84 MiB +- 0.04` | `106.70 MiB +- 0.03` | `106.70 MiB +- 0.03` | `106.70 MiB +- 0.03` | `444.31 MiB +- 24.14` |
| SQLite Unindexed | `102.53 MiB +- 0.69` | `102.56 MiB +- 0.69` | `106.43 MiB +- 0.67` | `106.43 MiB +- 0.67` | `106.43 MiB +- 0.67` | `453.09 MiB +- 11.18` |
| DuckDB Unindexed | `97.98 MiB +- 0.12` | `104.04 MiB +- 0.28` | `1292.39 MiB +- 78.75` | `1292.39 MiB +- 78.75` | `1292.39 MiB +- 78.75` | `1665.32 MiB +- 78.79` |
| PostgreSQL Indexed | `131.00 MiB +- 2.31` | `134.67 MiB +- 3.09` | `375.97 MiB +- 1.47` | `395.24 MiB +- 2.62` | `394.88 MiB +- 1.86` | `842.77 MiB +- 34.08` |
| PostgreSQL Unindexed | `130.78 MiB +- 2.94` | `134.89 MiB +- 4.71` | `375.55 MiB +- 2.90` | `360.15 MiB +- 3.89` | `360.75 MiB +- 3.04` | `816.71 MiB +- 35.77` |
| Neo4j Indexed | `675.99 MiB +- 25.46` | `711.55 MiB +- 14.42` | `2400.38 MiB +- 187.40` | `2975.87 MiB +- 41.57` | `0.00 MiB +- 0.00` | `2475.27 MiB +- 611.09` |
| ArcadeDB Indexed | `166.03 MiB +- 3.14` | `258.93 MiB +- 1.98` | `4234.37 MiB +- 58.60` | `4404.15 MiB +- 71.27` | `4404.15 MiB +- 71.27` | `4449.43 MiB +- 63.39` |
| ArcadeDB Unindexed | `164.47 MiB +- 2.68` | `256.12 MiB +- 14.62` | `4308.23 MiB +- 20.19` | `4308.23 MiB +- 20.19` | `4308.23 MiB +- 20.19` | `4428.81 MiB +- 40.91` |
| LadybugDB Unindexed | `280.67 MiB +- 0.33` | `317.21 MiB +- 0.47` | `4161.99 MiB +- 176.52` | `4161.99 MiB +- 176.52` | `4162.30 MiB +- 176.52` | `4172.28 MiB +- 178.82` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `439.66 MiB +- 24.44` | `439.69 MiB +- 24.44` | `441.05 MiB +- 23.85` | `440.72 MiB +- 23.57` | `440.72 MiB +- 23.57` | `294.02 MiB +- 8.02` |
| SQLite Unindexed | `447.12 MiB +- 11.17` | `447.15 MiB +- 11.17` | `449.16 MiB +- 12.57` | `449.16 MiB +- 12.57` | `449.16 MiB +- 12.57` | `306.28 MiB +- 7.72` |
| DuckDB Unindexed | `958.20 MiB +- 67.22` | `958.56 MiB +- 69.33` | `1844.93 MiB +- 113.36` | `1844.93 MiB +- 113.36` | `1844.93 MiB +- 113.36` | `2504.71 MiB +- 72.77` |
| PostgreSQL Indexed | `811.81 MiB +- 32.83` | `738.05 MiB +- 34.38` | `822.45 MiB +- 34.14` | `841.21 MiB +- 33.66` | `840.95 MiB +- 34.06` | `694.94 MiB +- 56.54` |
| PostgreSQL Unindexed | `794.62 MiB +- 35.50` | `745.67 MiB +- 35.57` | `829.86 MiB +- 34.96` | `814.26 MiB +- 34.96` | `815.06 MiB +- 34.81` | `664.41 MiB +- 24.71` |
| Neo4j Indexed | `675.99 MiB +- 25.46` | `711.55 MiB +- 14.42` | `2400.38 MiB +- 187.40` | `2975.87 MiB +- 41.57` | `0.00 MiB +- 0.00` | `4269.41 MiB +- 1366.84` |
| ArcadeDB Indexed | `4448.15 MiB +- 63.35` | `4448.84 MiB +- 63.29` | `4357.93 MiB +- 193.43` | `4572.21 MiB +- 218.93` | `5249.41 MiB +- 204.81` | `7203.15 MiB +- 63.56` |
| ArcadeDB Unindexed | `4430.01 MiB +- 43.41` | `4432.98 MiB +- 43.07` | `4358.85 MiB +- 53.70` | `4358.85 MiB +- 53.70` | `5061.67 MiB +- 36.29` | `8972.06 MiB +- 40.30` |
| LadybugDB Unindexed | `2660.44 MiB +- 91.67` | `2604.00 MiB +- 171.89` | `4993.96 MiB +- 203.44` | `4993.96 MiB +- 203.44` | `4993.96 MiB +- 203.44` | `13561.99 MiB +- 204.12` |

#### Medium runtime suite comparison

This rolls the medium-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.06 ms +- 0.01` | `1.25 ms +- 0.03` | `1.62 ms +- 0.05` |
| `olap/sqlite_indexed` | `14638.17 ms +- 162.07` | `14937.93 ms +- 157.31` | `15659.80 ms +- 546.86` |
| `oltp/sqlite_unindexed` | `134.91 ms +- 2.20` | `141.27 ms +- 1.45` | `145.87 ms +- 1.41` |
| `olap/sqlite_unindexed` | `14271.77 ms +- 84.67` | `14429.21 ms +- 59.07` | `14469.15 ms +- 89.01` |
| `oltp/duckdb` | `3.15 ms +- 0.15` | `3.98 ms +- 0.19` | `4.71 ms +- 0.25` |
| `olap/duckdb` | `59.99 ms +- 0.31` | `63.75 ms +- 0.69` | `65.74 ms +- 1.17` |
| `oltp/postgresql_indexed` | `1.23 ms +- 0.01` | `1.68 ms +- 0.04` | `2.20 ms +- 0.07` |
| `olap/postgresql_indexed` | `766.16 ms +- 13.06` | `817.08 ms +- 4.08` | `855.72 ms +- 15.74` |
| `oltp/postgresql_unindexed` | `72.37 ms +- 5.40` | `76.48 ms +- 5.40` | `80.48 ms +- 6.29` |
| `olap/postgresql_unindexed` | `924.28 ms +- 3.11` | `997.89 ms +- 11.92` | `1055.29 ms +- 8.29` |
| `oltp/neo4j_indexed` | `0.23 ms +- 0.00` | `0.36 ms +- 0.01` | `0.56 ms +- 0.05` |
| `olap/neo4j_indexed` | `15503.68 ms +- 400.71` | `16243.19 ms +- 598.54` | `16817.64 ms +- 653.82` |
| `oltp/arcadedb_embedded_indexed` | `0.05 ms +- 0.00` | `0.09 ms +- 0.01` | `0.12 ms +- 0.01` |
| `olap/arcadedb_embedded_indexed` | `27240.59 ms +- 270.96` | `29251.49 ms +- 1201.45` | `30290.07 ms +- 1741.19` |
| `oltp/arcadedb_embedded_unindexed` | `42.73 ms +- 0.29` | `51.94 ms +- 8.16` | `62.07 ms +- 18.06` |
| `olap/arcadedb_embedded_unindexed` | `26947.72 ms +- 203.72` | `29469.65 ms +- 207.37` | `30161.39 ms +- 606.60` |
| `oltp/ladybug_unindexed` | `4.18 ms +- 0.22` | `5.42 ms +- 0.17` | `6.17 ms +- 0.13` |
| `olap/ladybug_unindexed` | `2493.06 ms +- 28.03` | `2547.45 ms +- 49.23` | `2561.24 ms +- 49.44` |

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

#### Medium runtime query breakdowns

These tables show per-query end-to-end percentiles for the same
runtime matrix, aggregated as mean and standard deviation across
repeated runs.

##### OLTP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.47 ms +- 0.02` | `30.44 ms +- 0.10` | `4.97 ms +- 0.32` | `1.59 ms +- 0.02` | `26.13 ms +- 1.42` | `0.25 ms +- 0.00` | `0.10 ms +- 0.00` | `86.46 ms +- 1.34` | `6.87 ms +- 0.03` |
| `oltp_create_type1_node` | `0.74 ms +- 0.01` | `0.70 ms +- 0.00` | `1.35 ms +- 0.03` | `0.84 ms +- 0.01` | `0.83 ms +- 0.01` | `0.21 ms +- 0.00` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.35 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.19 ms +- 0.00` | `100.39 ms +- 1.21` | `3.71 ms +- 0.18` | `1.52 ms +- 0.01` | `49.05 ms +- 3.32` | `0.22 ms +- 0.01` | `0.02 ms +- 0.00` | `43.24 ms +- 0.91` | `5.55 ms +- 0.59` |
| `oltp_delete_type1_edge` | `0.76 ms +- 0.01` | `102.49 ms +- 0.84` | `3.87 ms +- 0.26` | `1.10 ms +- 0.02` | `109.43 ms +- 5.08` | `0.20 ms +- 0.00` | `0.04 ms +- 0.00` | `42.52 ms +- 0.65` | `4.49 ms +- 0.45` |
| `oltp_delete_type1_node` | `0.99 ms +- 0.00` | `864.69 ms +- 16.41` | `1.90 ms +- 0.03` | `0.71 ms +- 0.01` | `339.27 ms +- 28.63` | `0.25 ms +- 0.01` | `0.08 ms +- 0.00` | `42.54 ms +- 0.62` | `3.86 ms +- 0.05` |
| `oltp_program_create_and_link` | `1.74 ms +- 0.01` | `16.69 ms +- 0.80` | `4.51 ms +- 0.27` | `2.03 ms +- 0.02` | `15.28 ms +- 1.51` | `0.21 ms +- 0.00` | `0.19 ms +- 0.01` | `42.61 ms +- 0.39` | `3.79 ms +- 0.05` |
| `oltp_type1_neighbors` | `1.00 ms +- 0.00` | `99.83 ms +- 0.19` | `3.36 ms +- 0.15` | `1.32 ms +- 0.02` | `49.14 ms +- 3.98` | `0.25 ms +- 0.01` | `0.03 ms +- 0.01` | `42.72 ms +- 0.23` | `5.46 ms +- 0.58` |
| `oltp_type1_point_lookup` | `0.94 ms +- 0.01` | `15.39 ms +- 0.55` | `2.24 ms +- 0.03` | `1.10 ms +- 0.02` | `12.35 ms +- 2.30` | `0.29 ms +- 0.00` | `0.02 ms +- 0.00` | `42.19 ms +- 0.67` | `3.49 ms +- 0.03` |
| `oltp_update_cross_type_edge_rank` | `1.04 ms +- 0.01` | `102.88 ms +- 1.74` | `3.40 ms +- 0.16` | `1.25 ms +- 0.01` | `109.99 ms +- 5.75` | `0.20 ms +- 0.01` | `0.02 ms +- 0.00` | `42.27 ms +- 0.46` | `4.55 ms +- 0.40` |
| `oltp_update_type1_score` | `0.70 ms +- 0.01` | `15.65 ms +- 0.34` | `2.18 ms +- 0.07` | `0.87 ms +- 0.01` | `12.24 ms +- 2.02` | `0.20 ms +- 0.00` | `0.03 ms +- 0.00` | `42.77 ms +- 0.08` | `3.36 ms +- 0.02` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.68 ms +- 0.04` | `32.60 ms +- 0.21` | `6.06 ms +- 0.34` | `2.15 ms +- 0.14` | `28.34 ms +- 1.38` | `0.37 ms +- 0.01` | `0.16 ms +- 0.01` | `91.79 ms +- 1.38` | `8.62 ms +- 0.07` |
| `oltp_create_type1_node` | `0.90 ms +- 0.01` | `0.81 ms +- 0.00` | `1.75 ms +- 0.06` | `1.12 ms +- 0.02` | `1.05 ms +- 0.07` | `0.32 ms +- 0.01` | `0.05 ms +- 0.01` | `0.04 ms +- 0.00` | `0.52 ms +- 0.04` |
| `oltp_cross_type_lookup` | `1.39 ms +- 0.05` | `105.82 ms +- 0.44` | `4.64 ms +- 0.20` | `2.06 ms +- 0.10` | `52.59 ms +- 4.00` | `0.34 ms +- 0.02` | `0.03 ms +- 0.00` | `72.74 ms +- 38.34` | `6.87 ms +- 0.53` |
| `oltp_delete_type1_edge` | `0.93 ms +- 0.04` | `108.48 ms +- 0.37` | `4.89 ms +- 0.33` | `1.59 ms +- 0.11` | `114.21 ms +- 5.74` | `0.31 ms +- 0.01` | `0.07 ms +- 0.01` | `45.21 ms +- 1.09` | `5.74 ms +- 0.42` |
| `oltp_delete_type1_node` | `1.18 ms +- 0.06` | `897.74 ms +- 15.18` | `2.36 ms +- 0.07` | `1.03 ms +- 0.06` | `354.46 ms +- 28.42` | `0.38 ms +- 0.02` | `0.13 ms +- 0.01` | `45.49 ms +- 1.24` | `5.22 ms +- 0.00` |
| `oltp_program_create_and_link` | `2.01 ms +- 0.05` | `18.45 ms +- 0.37` | `5.55 ms +- 0.37` | `2.68 ms +- 0.02` | `17.28 ms +- 1.12` | `0.31 ms +- 0.01` | `0.25 ms +- 0.05` | `45.38 ms +- 0.80` | `5.13 ms +- 0.01` |
| `oltp_type1_neighbors` | `1.17 ms +- 0.07` | `105.15 ms +- 1.28` | `4.33 ms +- 0.19` | `1.77 ms +- 0.04` | `52.79 ms +- 4.15` | `0.40 ms +- 0.03` | `0.05 ms +- 0.01` | `58.22 ms +- 17.03` | `6.72 ms +- 0.54` |
| `oltp_type1_point_lookup` | `1.11 ms +- 0.02` | `16.73 ms +- 0.42` | `2.98 ms +- 0.04` | `1.51 ms +- 0.04` | `14.66 ms +- 1.70` | `0.49 ms +- 0.03` | `0.05 ms +- 0.00` | `46.54 ms +- 0.09` | `4.88 ms +- 0.06` |
| `oltp_update_cross_type_edge_rank` | `1.25 ms +- 0.04` | `109.79 ms +- 0.05` | `4.30 ms +- 0.22` | `1.72 ms +- 0.02` | `114.82 ms +- 6.43` | `0.31 ms +- 0.01` | `0.05 ms +- 0.00` | `45.19 ms +- 1.11` | `5.81 ms +- 0.35` |
| `oltp_update_type1_score` | `0.85 ms +- 0.03` | `17.13 ms +- 0.14` | `2.97 ms +- 0.12` | `1.16 ms +- 0.06` | `14.58 ms +- 0.97` | `0.32 ms +- 0.01` | `0.04 ms +- 0.01` | `68.76 ms +- 31.81` | `4.71 ms +- 0.04` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.13 ms +- 0.10` | `35.32 ms +- 0.45` | `7.11 ms +- 0.40` | `2.74 ms +- 0.16` | `31.82 ms +- 2.30` | `0.55 ms +- 0.04` | `0.21 ms +- 0.02` | `96.19 ms +- 0.85` | `9.64 ms +- 0.11` |
| `oltp_create_type1_node` | `1.18 ms +- 0.08` | `1.01 ms +- 0.01` | `2.28 ms +- 0.10` | `1.53 ms +- 0.07` | `1.40 ms +- 0.12` | `0.52 ms +- 0.06` | `0.08 ms +- 0.03` | `0.05 ms +- 0.00` | `0.63 ms +- 0.03` |
| `oltp_cross_type_lookup` | `1.76 ms +- 0.07` | `112.56 ms +- 0.35` | `5.46 ms +- 0.32` | `2.55 ms +- 0.12` | `57.74 ms +- 5.80` | `0.54 ms +- 0.11` | `0.04 ms +- 0.00` | `101.67 ms +- 75.03` | `7.81 ms +- 0.56` |
| `oltp_delete_type1_edge` | `1.28 ms +- 0.14` | `114.21 ms +- 1.24` | `5.70 ms +- 0.45` | `2.20 ms +- 0.14` | `119.64 ms +- 6.43` | `0.48 ms +- 0.03` | `0.09 ms +- 0.02` | `48.54 ms +- 1.13` | `6.50 ms +- 0.42` |
| `oltp_delete_type1_node` | `1.58 ms +- 0.15` | `910.19 ms +- 12.88` | `2.93 ms +- 0.06` | `1.53 ms +- 0.09` | `362.29 ms +- 29.40` | `0.54 ms +- 0.01` | `0.17 ms +- 0.01` | `48.96 ms +- 1.13` | `6.01 ms +- 0.14` |
| `oltp_program_create_and_link` | `2.56 ms +- 0.09` | `20.76 ms +- 0.47` | `6.49 ms +- 0.56` | `3.22 ms +- 0.03` | `19.91 ms +- 1.76` | `0.49 ms +- 0.01` | `0.31 ms +- 0.06` | `48.65 ms +- 0.62` | `5.92 ms +- 0.12` |
| `oltp_type1_neighbors` | `1.44 ms +- 0.17` | `111.73 ms +- 0.47` | `5.13 ms +- 0.27` | `2.27 ms +- 0.03` | `57.21 ms +- 5.64` | `0.68 ms +- 0.11` | `0.08 ms +- 0.02` | `85.39 ms +- 50.27` | `7.65 ms +- 0.57` |
| `oltp_type1_point_lookup` | `1.42 ms +- 0.07` | `18.28 ms +- 0.54` | `3.44 ms +- 0.06` | `1.94 ms +- 0.04` | `17.34 ms +- 2.29` | `0.79 ms +- 0.15` | `0.09 ms +- 0.00` | `52.56 ms +- 3.89` | `5.56 ms +- 0.18` |
| `oltp_update_cross_type_edge_rank` | `1.67 ms +- 0.05` | `116.00 ms +- 0.18` | `5.10 ms +- 0.25` | `2.36 ms +- 0.10` | `120.21 ms +- 7.34` | `0.46 ms +- 0.04` | `0.06 ms +- 0.01` | `48.69 ms +- 1.01` | `6.61 ms +- 0.24` |
| `oltp_update_type1_score` | `1.17 ms +- 0.01` | `18.69 ms +- 0.08` | `3.48 ms +- 0.17` | `1.63 ms +- 0.05` | `17.29 ms +- 1.87` | `0.51 ms +- 0.04` | `0.06 ms +- 0.01` | `89.95 ms +- 56.11` | `5.40 ms +- 0.15` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1641.91 ms +- 136.24` | `382.75 ms +- 9.13` | `8.93 ms +- 0.49` | `119.28 ms +- 3.50` | `114.87 ms +- 0.87` | `670.65 ms +- 57.43` | `1156.04 ms +- 54.71` | `1115.64 ms +- 102.54` | `50.59 ms +- 0.82` |
| `olap_fixed_length_path_projection` | `3530.53 ms +- 204.91` | `3187.33 ms +- 125.17` | `114.13 ms +- 0.61` | `23.03 ms +- 0.48` | `924.96 ms +- 8.35` | `1677.46 ms +- 80.50` | `6713.33 ms +- 992.37` | `6143.26 ms +- 504.97` | `2228.07 ms +- 18.79` |
| `olap_graph_introspection_rollup` | `1.64 ms +- 0.02` | `199.85 ms +- 4.22` | `6.59 ms +- 0.32` | `7.25 ms +- 0.57` | `104.37 ms +- 0.51` | `593.15 ms +- 9.14` | `1724.31 ms +- 41.17` | `1671.77 ms +- 134.21` | `171.19 ms +- 0.51` |
| `olap_three_type_path_count` | `2958.06 ms +- 110.17` | `2085.43 ms +- 57.66` | `39.14 ms +- 0.40` | `361.48 ms +- 2.61` | `356.62 ms +- 3.72` | `630.63 ms +- 42.04` | `9.86 ms +- 2.50` | `8.75 ms +- 2.88` | `13.22 ms +- 0.28` |
| `olap_type1_active_leaderboard` | `1.21 ms +- 0.01` | `19.48 ms +- 0.82` | `5.88 ms +- 0.13` | `12.37 ms +- 0.29` | `10.97 ms +- 0.23` | `77.01 ms +- 8.83` | `94.06 ms +- 5.61` | `73.66 ms +- 4.13` | `5.65 ms +- 0.11` |
| `olap_type1_age_rollup` | `149.89 ms +- 0.34` | `48.81 ms +- 0.80` | `3.80 ms +- 0.16` | `13.49 ms +- 0.54` | `12.59 ms +- 0.06` | `82.82 ms +- 8.06` | `61.74 ms +- 2.02` | `58.81 ms +- 0.68` | `4.20 ms +- 0.32` |
| `olap_type2_score_distribution` | `17.49 ms +- 0.37` | `46.85 ms +- 2.01` | `4.35 ms +- 0.14` | `10.25 ms +- 2.70` | `22.02 ms +- 0.04` | `80.58 ms +- 3.36` | `62.75 ms +- 1.08` | `60.22 ms +- 0.37` | `4.31 ms +- 0.44` |
| `olap_variable_length_grouped_rollup` | `137916.84 ms +- 1540.31` | `131880.56 ms +- 916.83` | `396.05 ms +- 1.35` | `7066.60 ms +- 124.02` | `7149.93 ms +- 8.19` | `151130.35 ms +- 3954.09` | `262508.11 ms +- 3666.78` | `260228.00 ms +- 1283.38` | `22439.62 ms +- 259.91` |
| `olap_variable_length_reachability` | `2.61 ms +- 0.12` | `4814.13 ms +- 99.22` | `15.31 ms +- 0.72` | `31.05 ms +- 0.50` | `519.21 ms +- 9.08` | `0.94 ms +- 0.01` | `0.41 ms +- 0.02` | `43.06 ms +- 0.50` | `6.11 ms +- 0.07` |
| `olap_with_scalar_rebinding` | `161.51 ms +- 15.28` | `52.51 ms +- 0.52` | `5.68 ms +- 0.36` | `16.86 ms +- 0.41` | `27.22 ms +- 0.08` | `93.16 ms +- 5.92` | `75.27 ms +- 4.88` | `73.98 ms +- 3.58` | `7.67 ms +- 0.06` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1675.21 ms +- 138.12` | `395.24 ms +- 12.04` | `10.34 ms +- 0.62` | `126.83 ms +- 3.68` | `122.93 ms +- 0.90` | `684.69 ms +- 59.68` | `1216.85 ms +- 50.88` | `1138.07 ms +- 103.45` | `58.64 ms +- 0.30` |
| `olap_fixed_length_path_projection` | `3658.31 ms +- 380.09` | `3277.58 ms +- 225.59` | `121.30 ms +- 1.41` | `26.06 ms +- 1.13` | `944.21 ms +- 11.25` | `1987.87 ms +- 531.74` | `7299.14 ms +- 1214.89` | `6918.12 ms +- 898.05` | `2302.81 ms +- 12.95` |
| `olap_graph_introspection_rollup` | `1.74 ms +- 0.07` | `206.15 ms +- 5.72` | `8.28 ms +- 0.86` | `9.44 ms +- 0.65` | `111.79 ms +- 0.70` | `861.71 ms +- 429.67` | `1799.02 ms +- 16.85` | `1724.24 ms +- 149.74` | `179.78 ms +- 0.86` |
| `olap_three_type_path_count` | `3246.07 ms +- 559.40` | `2245.65 ms +- 259.79` | `45.00 ms +- 0.67` | `377.25 ms +- 3.38` | `371.00 ms +- 5.89` | `870.73 ms +- 220.39` | `16.51 ms +- 10.75` | `10.84 ms +- 5.44` | `14.38 ms +- 0.12` |
| `olap_type1_active_leaderboard` | `1.50 ms +- 0.06` | `20.48 ms +- 0.77` | `7.77 ms +- 0.53` | `14.06 ms +- 0.25` | `12.64 ms +- 0.17` | `84.39 ms +- 9.14` | `115.85 ms +- 12.02` | `82.94 ms +- 4.31` | `7.01 ms +- 0.64` |
| `olap_type1_age_rollup` | `162.74 ms +- 2.59` | `51.20 ms +- 1.18` | `5.20 ms +- 0.57` | `15.93 ms +- 1.02` | `14.22 ms +- 0.00` | `90.36 ms +- 10.49` | `68.71 ms +- 4.97` | `64.16 ms +- 1.24` | `5.40 ms +- 0.61` |
| `olap_type2_score_distribution` | `19.34 ms +- 2.28` | `58.24 ms +- 15.02` | `5.41 ms +- 0.22` | `12.52 ms +- 2.39` | `24.08 ms +- 0.43` | `134.49 ms +- 72.27` | `88.42 ms +- 29.87` | `68.51 ms +- 7.93` | `5.62 ms +- 0.92` |
| `olap_variable_length_grouped_rollup` | `140355.54 ms +- 2062.73` | `132894.00 ms +- 126.33` | `409.49 ms +- 2.48` | `7532.32 ms +- 36.52` | `7784.81 ms +- 104.01` | `157572.75 ms +- 6790.97` | `281830.38 ms +- 10800.88` | `284565.06 ms +- 897.98` | `22883.39 ms +- 478.08` |
| `olap_variable_length_reachability` | `2.85 ms +- 0.17` | `5087.24 ms +- 237.94` | `17.87 ms +- 1.99` | `36.81 ms +- 0.76` | `564.14 ms +- 3.45` | `1.35 ms +- 0.09` | `0.56 ms +- 0.09` | `45.00 ms +- 1.22` | `7.80 ms +- 0.44` |
| `olap_with_scalar_rebinding` | `256.05 ms +- 152.68` | `56.34 ms +- 1.63` | `6.89 ms +- 0.38` | `19.56 ms +- 0.89` | `29.05 ms +- 0.17` | `143.55 ms +- 65.01` | `79.50 ms +- 5.78` | `79.60 ms +- 4.34` | `9.72 ms +- 0.12` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `1689.92 ms +- 130.45` | `398.19 ms +- 13.90` | `10.77 ms +- 0.59` | `131.31 ms +- 2.49` | `127.48 ms +- 1.60` | `694.36 ms +- 61.45` | `1303.52 ms +- 134.41` | `1141.68 ms +- 103.37` | `65.20 ms +- 0.38` |
| `olap_fixed_length_path_projection` | `3696.79 ms +- 429.00` | `3302.70 ms +- 254.29` | `125.68 ms +- 3.26` | `28.70 ms +- 3.22` | `986.35 ms +- 60.29` | `2096.66 ms +- 676.04` | `7651.73 ms +- 1308.25` | `7513.42 ms +- 890.49` | `2361.38 ms +- 53.68` |
| `olap_graph_introspection_rollup` | `1.77 ms +- 0.05` | `209.88 ms +- 9.88` | `10.16 ms +- 1.92` | `10.98 ms +- 0.69` | `116.87 ms +- 1.07` | `896.26 ms +- 464.85` | `1823.24 ms +- 34.16` | `1758.55 ms +- 170.71` | `182.30 ms +- 0.39` |
| `olap_three_type_path_count` | `3413.63 ms +- 816.54` | `2304.96 ms +- 336.61` | `46.62 ms +- 1.58` | `402.13 ms +- 20.31` | `399.71 ms +- 6.63` | `938.87 ms +- 181.99` | `31.31 ms +- 27.91` | `12.63 ms +- 4.12` | `14.56 ms +- 0.12` |
| `olap_type1_active_leaderboard` | `1.63 ms +- 0.07` | `21.03 ms +- 1.14` | `9.39 ms +- 1.07` | `14.92 ms +- 0.65` | `14.57 ms +- 0.41` | `91.53 ms +- 8.88` | `143.74 ms +- 42.95` | `92.13 ms +- 8.14` | `8.01 ms +- 1.26` |
| `olap_type1_age_rollup` | `171.20 ms +- 4.24` | `54.50 ms +- 0.52` | `6.23 ms +- 1.34` | `17.73 ms +- 2.88` | `14.97 ms +- 0.34` | `94.21 ms +- 8.57` | `78.98 ms +- 15.05` | `67.80 ms +- 0.77` | `6.11 ms +- 0.72` |
| `olap_type2_score_distribution` | `20.79 ms +- 3.51` | `65.89 ms +- 18.55` | `5.92 ms +- 0.35` | `13.28 ms +- 2.17` | `27.31 ms +- 0.71` | `155.94 ms +- 88.16` | `129.21 ms +- 89.94` | `70.87 ms +- 6.52` | `6.06 ms +- 0.89` |
| `olap_variable_length_grouped_rollup` | `147316.72 ms +- 5280.83` | `133088.03 ms +- 45.75` | `414.88 ms +- 3.53` | `7876.60 ms +- 139.48` | `8254.49 ms +- 13.62` | `163033.56 ms +- 6719.48` | `291650.93 ms +- 16021.09` | `290822.36 ms +- 7259.29` | `22947.89 ms +- 437.71` |
| `olap_variable_length_reachability` | `3.04 ms +- 0.33` | `5183.70 ms +- 360.44` | `20.20 ms +- 4.10` | `40.44 ms +- 2.45` | `581.51 ms +- 1.01` | `2.29 ms +- 0.27` | `0.64 ms +- 0.10` | `48.11 ms +- 4.10` | `8.52 ms +- 0.17` |
| `olap_with_scalar_rebinding` | `282.49 ms +- 192.40` | `62.61 ms +- 8.58` | `7.51 ms +- 0.46` | `21.11 ms +- 1.09` | `29.60 ms +- 0.06` | `172.70 ms +- 109.06` | `87.43 ms +- 3.91` | `86.31 ms +- 5.08` | `12.34 ms +- 0.15` |

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
- `DuckDB`: `1.5.2`
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
| SQLite Indexed | `9.09 ms +- 1.46` | `5.45 ms +- 1.56` | `111.13 ms +- 3.53` | `15.20 ms +- 1.30` | `6.77 ms +- 0.23` | `1.09 ms +- 0.07` | `1.44 ms +- 0.14` | `1.82 ms +- 0.21` |
| SQLite Unindexed | `11.56 ms +- 2.77` | `5.39 ms +- 1.92` | `131.80 ms +- 37.81` | `1.51 ms +- 0.46` | `0.63 ms +- 0.28` | `1.54 ms +- 0.10` | `2.29 ms +- 0.29` | `2.87 ms +- 0.42` |
| DuckDB Unindexed | `10.06 ms +- 0.21` | `113.75 ms +- 19.43` | `191.12 ms +- 36.62` | `0.00 ms +- 0.00` | `0.12 ms +- 0.01` | `2.53 ms +- 0.15` | `4.05 ms +- 0.43` | `4.85 ms +- 0.54` |
| PostgreSQL Indexed | `4.30 ms +- 0.24` | `315.89 ms +- 2.49` | `238.62 ms +- 11.03` | `214.21 ms +- 11.40` | `87.01 ms +- 0.44` | `1.48 ms +- 0.12` | `2.22 ms +- 0.23` | `2.72 ms +- 0.39` |
| PostgreSQL Unindexed | `4.21 ms +- 0.21` | `314.79 ms +- 8.00` | `235.49 ms +- 7.19` | `14.10 ms +- 6.26` | `87.44 ms +- 8.57` | `1.71 ms +- 0.29` | `2.58 ms +- 0.68` | `3.16 ms +- 0.84` |
| Neo4j Indexed | `106.42 ms +- 61.09` | `422.39 ms +- 95.45` | `2643.22 ms +- 763.90` | `849.53 ms +- 91.98` | `0.00 ms +- 0.00` | `0.27 ms +- 0.02` | `0.47 ms +- 0.04` | `0.81 ms +- 0.08` |
| Neo4j Unindexed | `103.86 ms +- 37.82` | `419.47 ms +- 98.19` | `2112.08 ms +- 156.81` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.42 ms +- 0.05` | `0.65 ms +- 0.11` | `0.93 ms +- 0.21` |
| ArcadeDB Indexed | `431.88 ms +- 154.54` | `456.32 ms +- 122.69` | `739.66 ms +- 135.82` | `426.94 ms +- 59.23` | `0.00 ms +- 0.00` | `0.03 ms +- 0.01` | `0.07 ms +- 0.03` | `0.10 ms +- 0.05` |
| ArcadeDB Unindexed | `436.90 ms +- 88.68` | `412.66 ms +- 36.82` | `700.79 ms +- 46.77` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.47 ms +- 0.04` | `0.70 ms +- 0.12` | `0.90 ms +- 0.20` |
| LadybugDB Unindexed | `77.95 ms +- 3.85` | `51.97 ms +- 16.33` | `682.95 ms +- 17.00` | `0.00 ms +- 0.00` | `19.95 ms +- 0.59` | `1.18 ms +- 0.06` | `1.76 ms +- 0.11` | `2.44 ms +- 0.23` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `9.12 ms +- 1.62` | `5.98 ms +- 1.36` | `100.99 ms +- 5.27` | `14.84 ms +- 0.57` | `7.72 ms +- 1.34` | `3.66 ms +- 0.28` | `4.30 ms +- 0.56` | `4.70 ms +- 0.58` |
| SQLite Unindexed | `11.86 ms +- 2.14` | `6.11 ms +- 1.43` | `96.06 ms +- 3.06` | `1.31 ms +- 0.07` | `0.59 ms +- 0.05` | `3.66 ms +- 0.04` | `4.36 ms +- 0.01` | `4.94 ms +- 0.13` |
| DuckDB Unindexed | `10.63 ms +- 2.39` | `107.30 ms +- 26.91` | `176.60 ms +- 33.40` | `0.00 ms +- 0.00` | `0.17 ms +- 0.08` | `4.03 ms +- 1.76` | `5.22 ms +- 2.45` | `6.06 ms +- 2.74` |
| PostgreSQL Indexed | `4.24 ms +- 0.13` | `348.12 ms +- 19.37` | `220.63 ms +- 43.97` | `215.86 ms +- 19.04` | `98.34 ms +- 27.16` | `4.22 ms +- 1.64` | `5.49 ms +- 2.14` | `6.22 ms +- 2.46` |
| PostgreSQL Unindexed | `4.58 ms +- 1.11` | `377.65 ms +- 94.73` | `183.51 ms +- 0.98` | `15.73 ms +- 0.84` | `77.79 ms +- 2.53` | `2.90 ms +- 0.29` | `3.64 ms +- 0.61` | `4.16 ms +- 0.82` |
| Neo4j Indexed | `106.42 ms +- 61.09` | `422.39 ms +- 95.45` | `2643.22 ms +- 763.90` | `849.53 ms +- 91.98` | `0.00 ms +- 0.00` | `3.27 ms +- 0.97` | `4.49 ms +- 1.31` | `5.21 ms +- 1.41` |
| Neo4j Unindexed | `103.86 ms +- 37.82` | `419.47 ms +- 98.19` | `2112.08 ms +- 156.81` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `3.12 ms +- 0.42` | `4.53 ms +- 0.88` | `5.27 ms +- 1.16` |
| ArcadeDB Indexed | `2.61 ms +- 1.14` | `30.10 ms +- 5.95` | `383.90 ms +- 77.96` | `114.20 ms +- 65.60` | `296.75 ms +- 12.66` | `5.19 ms +- 1.19` | `6.91 ms +- 2.10` | `8.05 ms +- 2.38` |
| ArcadeDB Unindexed | `2.81 ms +- 1.06` | `32.63 ms +- 7.38` | `392.28 ms +- 66.42` | `0.00 ms +- 0.00` | `310.17 ms +- 15.64` | `4.74 ms +- 0.81` | `6.19 ms +- 1.36` | `7.22 ms +- 1.63` |
| LadybugDB Unindexed | `75.41 ms +- 3.07` | `26.77 ms +- 0.39` | `678.91 ms +- 36.95` | `0.00 ms +- 0.00` | `24.47 ms +- 5.64` | `6.11 ms +- 0.18` | `7.75 ms +- 0.25` | `9.02 ms +- 0.16` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `93.01 MiB +- 0.07` | `93.11 MiB +- 0.07` | `95.55 MiB +- 0.07` | `95.60 MiB +- 0.07` | `95.60 MiB +- 0.07` | `652.79 MiB +- 49.11` |
| SQLite Unindexed | `92.40 MiB +- 0.94` | `92.50 MiB +- 0.94` | `94.94 MiB +- 0.94` | `94.99 MiB +- 0.94` | `94.99 MiB +- 0.94` | `678.56 MiB +- 34.26` |
| DuckDB Unindexed | `94.05 MiB +- 1.00` | `98.61 MiB +- 0.72` | `153.37 MiB +- 4.47` | `153.37 MiB +- 4.47` | `153.37 MiB +- 4.47` | `829.93 MiB +- 43.05` |
| PostgreSQL Indexed | `129.73 MiB +- 16.52` | `130.77 MiB +- 16.92` | `139.26 MiB +- 16.96` | `139.53 MiB +- 16.81` | `142.01 MiB +- 17.08` | `994.46 MiB +- 91.68` |
| PostgreSQL Unindexed | `119.12 MiB +- 0.31` | `120.77 MiB +- 0.12` | `129.10 MiB +- 0.11` | `129.48 MiB +- 0.06` | `131.06 MiB +- 0.12` | `997.60 MiB +- 19.02` |
| Neo4j Indexed | `694.13 MiB +- 37.51` | `738.35 MiB +- 22.91` | `1166.74 MiB +- 563.50` | `916.16 MiB +- 76.18` | `0.00 MiB +- 0.00` | `1257.89 MiB +- 96.41` |
| Neo4j Unindexed | `686.46 MiB +- 18.25` | `729.49 MiB +- 19.35` | `1474.01 MiB +- 506.80` | `1471.05 MiB +- 505.74` | `0.00 MiB +- 0.00` | `1241.64 MiB +- 22.32` |
| ArcadeDB Indexed | `135.16 MiB +- 5.98` | `204.78 MiB +- 4.10` | `293.89 MiB +- 19.98` | `367.13 MiB +- 43.11` | `367.13 MiB +- 43.11` | `1130.21 MiB +- 12.62` |
| ArcadeDB Unindexed | `139.37 MiB +- 6.41` | `206.97 MiB +- 2.15` | `292.69 MiB +- 25.56` | `292.69 MiB +- 25.56` | `292.69 MiB +- 25.56` | `1041.29 MiB +- 27.84` |
| LadybugDB Unindexed | `265.64 MiB +- 0.36` | `292.25 MiB +- 0.59` | `514.28 MiB +- 38.47` | `514.28 MiB +- 38.47` | `514.56 MiB +- 38.45` | `532.24 MiB +- 37.29` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `647.21 MiB +- 49.35` | `647.24 MiB +- 49.35` | `642.32 MiB +- 51.52` | `642.32 MiB +- 51.52` | `642.32 MiB +- 51.52` | `380.17 MiB +- 0.61` |
| SQLite Unindexed | `673.96 MiB +- 32.04` | `673.01 MiB +- 31.07` | `669.73 MiB +- 29.38` | `669.40 MiB +- 29.91` | `669.40 MiB +- 29.91` | `378.20 MiB +- 10.24` |
| DuckDB Unindexed | `770.03 MiB +- 43.82` | `767.21 MiB +- 41.94` | `790.45 MiB +- 38.22` | `790.45 MiB +- 38.22` | `790.45 MiB +- 38.22` | `550.43 MiB +- 17.66` |
| PostgreSQL Indexed | `988.07 MiB +- 92.22` | `988.79 MiB +- 91.91` | `987.57 MiB +- 92.27` | `987.44 MiB +- 92.25` | `989.66 MiB +- 92.21` | `621.55 MiB +- 52.92` |
| PostgreSQL Unindexed | `988.16 MiB +- 18.23` | `987.53 MiB +- 19.08` | `983.44 MiB +- 21.08` | `982.73 MiB +- 20.01` | `984.58 MiB +- 19.95` | `623.44 MiB +- 32.22` |
| Neo4j Indexed | `694.13 MiB +- 37.51` | `738.35 MiB +- 22.91` | `1166.74 MiB +- 563.50` | `916.16 MiB +- 76.18` | `0.00 MiB +- 0.00` | `1867.07 MiB +- 445.84` |
| Neo4j Unindexed | `686.46 MiB +- 18.25` | `729.49 MiB +- 19.35` | `1474.01 MiB +- 506.80` | `1471.05 MiB +- 505.74` | `0.00 MiB +- 0.00` | `1230.75 MiB +- 25.94` |
| ArcadeDB Indexed | `1130.69 MiB +- 12.65` | `1131.18 MiB +- 12.78` | `1133.19 MiB +- 10.96` | `1134.14 MiB +- 10.59` | `1148.21 MiB +- 12.14` | `1202.77 MiB +- 13.84` |
| ArcadeDB Unindexed | `1041.71 MiB +- 27.80` | `1042.08 MiB +- 27.85` | `1042.41 MiB +- 27.85` | `1042.41 MiB +- 27.85` | `1051.84 MiB +- 23.69` | `1080.24 MiB +- 22.30` |
| LadybugDB Unindexed | `471.66 MiB +- 37.82` | `478.32 MiB +- 37.80` | `576.81 MiB +- 8.16` | `576.81 MiB +- 8.16` | `576.81 MiB +- 8.16` | `630.26 MiB +- 10.91` |

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `1.09 ms +- 0.07` | `1.44 ms +- 0.14` | `1.82 ms +- 0.21` |
| `olap/sqlite_indexed` | `3.66 ms +- 0.28` | `4.30 ms +- 0.56` | `4.70 ms +- 0.58` |
| `oltp/sqlite_unindexed` | `1.54 ms +- 0.10` | `2.29 ms +- 0.29` | `2.87 ms +- 0.42` |
| `olap/sqlite_unindexed` | `3.66 ms +- 0.04` | `4.36 ms +- 0.01` | `4.94 ms +- 0.13` |
| `oltp/duckdb` | `2.53 ms +- 0.15` | `4.05 ms +- 0.43` | `4.85 ms +- 0.54` |
| `olap/duckdb` | `4.03 ms +- 1.76` | `5.22 ms +- 2.45` | `6.06 ms +- 2.74` |
| `oltp/postgresql_indexed` | `1.48 ms +- 0.12` | `2.22 ms +- 0.23` | `2.72 ms +- 0.39` |
| `olap/postgresql_indexed` | `4.22 ms +- 1.64` | `5.49 ms +- 2.14` | `6.22 ms +- 2.46` |
| `oltp/postgresql_unindexed` | `1.71 ms +- 0.29` | `2.58 ms +- 0.68` | `3.16 ms +- 0.84` |
| `olap/postgresql_unindexed` | `2.90 ms +- 0.29` | `3.64 ms +- 0.61` | `4.16 ms +- 0.82` |
| `oltp/neo4j_indexed` | `0.27 ms +- 0.02` | `0.47 ms +- 0.04` | `0.81 ms +- 0.08` |
| `olap/neo4j_indexed` | `3.27 ms +- 0.97` | `4.49 ms +- 1.31` | `5.21 ms +- 1.41` |
| `oltp/neo4j_unindexed` | `0.42 ms +- 0.05` | `0.65 ms +- 0.11` | `0.93 ms +- 0.21` |
| `olap/neo4j_unindexed` | `3.12 ms +- 0.42` | `4.53 ms +- 0.88` | `5.27 ms +- 1.16` |
| `oltp/arcadedb_embedded_indexed` | `0.03 ms +- 0.01` | `0.07 ms +- 0.03` | `0.10 ms +- 0.05` |
| `olap/arcadedb_embedded_indexed` | `5.19 ms +- 1.19` | `6.91 ms +- 2.10` | `8.05 ms +- 2.38` |
| `oltp/arcadedb_embedded_unindexed` | `0.47 ms +- 0.04` | `0.70 ms +- 0.12` | `0.90 ms +- 0.20` |
| `olap/arcadedb_embedded_unindexed` | `4.74 ms +- 0.81` | `6.19 ms +- 1.36` | `7.22 ms +- 1.63` |
| `oltp/ladybug_unindexed` | `1.18 ms +- 0.06` | `1.76 ms +- 0.11` | `2.44 ms +- 0.23` |
| `olap/ladybug_unindexed` | `6.11 ms +- 0.18` | `7.75 ms +- 0.25` | `9.02 ms +- 0.16` |

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
| `oltp_create_cross_type_edge` | `1.47 ms +- 0.10` | `1.95 ms +- 0.33` | `3.07 ms +- 0.37` | `1.93 ms +- 0.33` | `2.20 ms +- 0.30` | `0.30 ms +- 0.03` | `0.64 ms +- 0.12` | `0.06 ms +- 0.01` | `0.95 ms +- 0.09` | `0.78 ms +- 0.01` |
| `oltp_create_type1_node` | `0.72 ms +- 0.01` | `0.85 ms +- 0.12` | `1.87 ms +- 0.42` | `0.97 ms +- 0.14` | `0.94 ms +- 0.12` | `0.26 ms +- 0.02` | `0.24 ms +- 0.03` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.26 ms +- 0.00` |
| `oltp_cross_type_lookup` | `1.26 ms +- 0.03` | `1.82 ms +- 0.24` | `4.82 ms +- 0.82` | `1.86 ms +- 0.34` | `2.24 ms +- 0.82` | `0.32 ms +- 0.00` | `0.40 ms +- 0.03` | `0.02 ms +- 0.00` | `0.45 ms +- 0.04` | `2.61 ms +- 0.28` |
| `oltp_delete_type1_edge` | `0.85 ms +- 0.13` | `1.26 ms +- 0.33` | `2.36 ms +- 0.12` | `1.37 ms +- 0.32` | `1.90 ms +- 0.85` | `0.25 ms +- 0.03` | `0.39 ms +- 0.07` | `0.04 ms +- 0.01` | `0.48 ms +- 0.05` | `1.78 ms +- 0.05` |
| `oltp_delete_type1_node` | `0.66 ms +- 0.08` | `2.25 ms +- 0.47` | `0.91 ms +- 0.03` | `0.73 ms +- 0.04` | `1.24 ms +- 0.33` | `0.25 ms +- 0.04` | `0.42 ms +- 0.07` | `0.07 ms +- 0.02` | `0.51 ms +- 0.06` | `0.45 ms +- 0.00` |
| `oltp_program_create_and_link` | `1.94 ms +- 0.24` | `1.89 ms +- 0.04` | `3.53 ms +- 0.21` | `2.60 ms +- 0.54` | `2.56 ms +- 0.75` | `0.25 ms +- 0.03` | `0.44 ms +- 0.07` | `0.05 ms +- 0.01` | `0.51 ms +- 0.06` | `0.69 ms +- 0.02` |
| `oltp_type1_neighbors` | `1.16 ms +- 0.21` | `2.02 ms +- 0.61` | `3.09 ms +- 1.33` | `1.59 ms +- 0.29` | `1.57 ms +- 0.05` | `0.31 ms +- 0.01` | `0.41 ms +- 0.04` | `0.03 ms +- 0.01` | `0.45 ms +- 0.04` | `2.58 ms +- 0.25` |
| `oltp_type1_point_lookup` | `1.02 ms +- 0.07` | `1.24 ms +- 0.16` | `1.56 ms +- 0.47` | `1.40 ms +- 0.35` | `1.23 ms +- 0.02` | `0.30 ms +- 0.02` | `0.43 ms +- 0.07` | `0.01 ms +- 0.00` | `0.42 ms +- 0.02` | `0.37 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | `1.08 ms +- 0.23` | `1.19 ms +- 0.02` | `2.49 ms +- 0.55` | `1.38 ms +- 0.09` | `2.17 ms +- 0.82` | `0.24 ms +- 0.01` | `0.42 ms +- 0.06` | `0.03 ms +- 0.01` | `0.47 ms +- 0.05` | `1.86 ms +- 0.08` |
| `oltp_update_type1_score` | `0.73 ms +- 0.01` | `0.97 ms +- 0.16` | `1.60 ms +- 0.06` | `1.00 ms +- 0.14` | `1.09 ms +- 0.15` | `0.26 ms +- 0.01` | `0.38 ms +- 0.05` | `0.02 ms +- 0.00` | `0.45 ms +- 0.04` | `0.39 ms +- 0.01` |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `1.98 ms +- 0.34` | `2.71 ms +- 0.69` | `5.94 ms +- 2.10` | `2.73 ms +- 0.55` | `3.52 ms +- 1.30` | `0.52 ms +- 0.09` | `1.02 ms +- 0.24` | `0.12 ms +- 0.06` | `1.38 ms +- 0.24` | `1.14 ms +- 0.10` |
| `oltp_create_type1_node` | `0.93 ms +- 0.03` | `1.19 ms +- 0.23` | `3.24 ms +- 1.16` | `1.42 ms +- 0.18` | `1.32 ms +- 0.22` | `0.43 ms +- 0.10` | `0.39 ms +- 0.09` | `0.04 ms +- 0.01` | `0.04 ms +- 0.02` | `0.35 ms +- 0.02` |
| `oltp_cross_type_lookup` | `1.81 ms +- 0.32` | `3.23 ms +- 0.53` | `7.28 ms +- 0.67` | `2.98 ms +- 1.19` | `3.03 ms +- 1.16` | `0.55 ms +- 0.02` | `0.55 ms +- 0.01` | `0.04 ms +- 0.00` | `0.65 ms +- 0.10` | `3.96 ms +- 0.47` |
| `oltp_delete_type1_edge` | `1.13 ms +- 0.26` | `2.18 ms +- 0.72` | `3.13 ms +- 0.20` | `2.14 ms +- 0.63` | `3.05 ms +- 1.36` | `0.44 ms +- 0.07` | `0.58 ms +- 0.15` | `0.08 ms +- 0.03` | `0.68 ms +- 0.12` | `2.79 ms +- 0.11` |
| `oltp_delete_type1_node` | `0.86 ms +- 0.15` | `3.12 ms +- 1.20` | `1.21 ms +- 0.08` | `1.10 ms +- 0.11` | `2.01 ms +- 0.95` | `0.39 ms +- 0.10` | `0.65 ms +- 0.21` | `0.13 ms +- 0.07` | `0.76 ms +- 0.17` | `0.62 ms +- 0.04` |
| `oltp_program_create_and_link` | `2.48 ms +- 0.49` | `2.60 ms +- 0.40` | `5.51 ms +- 1.77` | `3.81 ms +- 1.14` | `3.68 ms +- 1.81` | `0.40 ms +- 0.13` | `0.73 ms +- 0.19` | `0.10 ms +- 0.06` | `0.75 ms +- 0.16` | `0.91 ms +- 0.05` |
| `oltp_type1_neighbors` | `1.55 ms +- 0.36` | `2.98 ms +- 1.06` | `4.47 ms +- 2.40` | `2.39 ms +- 0.70` | `2.26 ms +- 0.26` | `0.56 ms +- 0.08` | `0.57 ms +- 0.04` | `0.04 ms +- 0.01` | `0.68 ms +- 0.13` | `3.86 ms +- 0.46` |
| `oltp_type1_point_lookup` | `1.34 ms +- 0.09` | `1.84 ms +- 0.44` | `2.42 ms +- 1.15` | `2.06 ms +- 0.61` | `1.75 ms +- 0.01` | `0.60 ms +- 0.15` | `0.79 ms +- 0.27` | `0.04 ms +- 0.00` | `0.64 ms +- 0.01` | `0.51 ms +- 0.06` |
| `oltp_update_cross_type_edge_rank` | `1.42 ms +- 0.44` | `1.58 ms +- 0.04` | `3.85 ms +- 1.76` | `2.02 ms +- 0.12` | `3.55 ms +- 1.95` | `0.40 ms +- 0.11` | `0.66 ms +- 0.15` | `0.05 ms +- 0.02` | `0.69 ms +- 0.16` | `2.91 ms +- 0.18` |
| `oltp_update_type1_score` | `0.94 ms +- 0.04` | `1.42 ms +- 0.34` | `3.43 ms +- 0.52` | `1.52 ms +- 0.21` | `1.61 ms +- 0.20` | `0.42 ms +- 0.08` | `0.52 ms +- 0.04` | `0.04 ms +- 0.01` | `0.69 ms +- 0.13` | `0.53 ms +- 0.02` |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | `2.32 ms +- 0.40` | `3.17 ms +- 0.85` | `7.00 ms +- 2.60` | `3.25 ms +- 0.84` | `4.24 ms +- 1.75` | `0.99 ms +- 0.34` | `1.45 ms +- 0.43` | `0.18 ms +- 0.10` | `1.70 ms +- 0.35` | `2.08 ms +- 0.56` |
| `oltp_create_type1_node` | `1.16 ms +- 0.03` | `1.64 ms +- 0.44` | `3.93 ms +- 1.46` | `1.86 ms +- 0.49` | `1.73 ms +- 0.41` | `0.78 ms +- 0.33` | `0.63 ms +- 0.19` | `0.06 ms +- 0.03` | `0.07 ms +- 0.03` | `0.54 ms +- 0.16` |
| `oltp_cross_type_lookup` | `2.49 ms +- 1.02` | `4.08 ms +- 0.30` | `8.42 ms +- 0.74` | `3.69 ms +- 1.84` | `3.54 ms +- 1.46` | `0.89 ms +- 0.05` | `0.67 ms +- 0.05` | `0.06 ms +- 0.01` | `0.84 ms +- 0.17` | `5.01 ms +- 0.57` |
| `oltp_delete_type1_edge` | `1.55 ms +- 0.49` | `3.06 ms +- 1.24` | `3.67 ms +- 0.25` | `2.69 ms +- 1.02` | `4.02 ms +- 1.79` | `0.75 ms +- 0.12` | `0.84 ms +- 0.35` | `0.12 ms +- 0.05` | `0.89 ms +- 0.22` | `3.66 ms +- 0.21` |
| `oltp_delete_type1_node` | `1.23 ms +- 0.33` | `3.76 ms +- 1.64` | `1.53 ms +- 0.09` | `1.39 ms +- 0.20` | `2.56 ms +- 1.34` | `0.59 ms +- 0.23` | `0.92 ms +- 0.48` | `0.18 ms +- 0.12` | `1.00 ms +- 0.31` | `1.05 ms +- 0.19` |
| `oltp_program_create_and_link` | `2.90 ms +- 0.60` | `3.35 ms +- 1.12` | `6.53 ms +- 2.15` | `4.61 ms +- 1.70` | `4.38 ms +- 2.21` | `0.60 ms +- 0.28` | `1.08 ms +- 0.36` | `0.14 ms +- 0.11` | `1.02 ms +- 0.31` | `1.46 ms +- 0.11` |
| `oltp_type1_neighbors` | `1.96 ms +- 0.53` | `3.54 ms +- 1.29` | `5.25 ms +- 2.83` | `2.85 ms +- 0.94` | `2.73 ms +- 0.51` | `0.94 ms +- 0.22` | `0.79 ms +- 0.16` | `0.07 ms +- 0.03` | `0.88 ms +- 0.21` | `4.91 ms +- 0.59` |
| `oltp_type1_point_lookup` | `1.55 ms +- 0.04` | `2.30 ms +- 0.69` | `3.01 ms +- 1.53` | `2.47 ms +- 0.86` | `2.07 ms +- 0.08` | `1.11 ms +- 0.49` | `1.26 ms +- 0.45` | `0.06 ms +- 0.01` | `0.84 ms +- 0.03` | `0.94 ms +- 0.51` |
| `oltp_update_cross_type_edge_rank` | `1.86 ms +- 0.70` | `1.87 ms +- 0.04` | `4.57 ms +- 2.10` | `2.39 ms +- 0.11` | `4.36 ms +- 2.37` | `0.77 ms +- 0.51` | `0.96 ms +- 0.30` | `0.07 ms +- 0.04` | `0.90 ms +- 0.24` | `3.85 ms +- 0.21` |
| `oltp_update_type1_score` | `1.21 ms +- 0.04` | `1.93 ms +- 0.54` | `4.55 ms +- 0.70` | `2.02 ms +- 0.58` | `1.98 ms +- 0.27` | `0.69 ms +- 0.24` | `0.67 ms +- 0.07` | `0.07 ms +- 0.03` | `0.87 ms +- 0.22` | `0.86 ms +- 0.16` |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `3.45 ms +- 0.37` | `2.55 ms +- 0.04` | `3.78 ms +- 1.83` | `3.86 ms +- 1.74` | `2.47 ms +- 0.27` | `3.36 ms +- 1.44` | `3.25 ms +- 0.74` | `7.11 ms +- 2.32` | `7.14 ms +- 2.16` | `4.11 ms +- 0.52` |
| `olap_fixed_length_path_projection` | `6.61 ms +- 0.40` | `6.62 ms +- 0.12` | `5.98 ms +- 3.04` | `7.89 ms +- 3.24` | `5.07 ms +- 0.36` | `6.45 ms +- 1.98` | `5.64 ms +- 0.73` | `6.76 ms +- 1.23` | `6.55 ms +- 1.29` | `18.31 ms +- 0.75` |
| `olap_graph_introspection_rollup` | `1.77 ms +- 0.18` | `2.38 ms +- 0.06` | `4.08 ms +- 2.02` | `4.27 ms +- 1.38` | `2.82 ms +- 0.40` | `4.00 ms +- 1.59` | `3.30 ms +- 0.24` | `11.67 ms +- 3.34` | `11.18 ms +- 3.29` | `2.90 ms +- 0.28` |
| `olap_three_type_path_count` | `5.03 ms +- 0.15` | `4.73 ms +- 0.11` | `3.66 ms +- 1.88` | `4.66 ms +- 2.15` | `3.00 ms +- 0.36` | `2.43 ms +- 0.72` | `2.42 ms +- 0.66` | `0.10 ms +- 0.03` | `0.11 ms +- 0.02` | `4.15 ms +- 0.42` |
| `olap_type1_active_leaderboard` | `1.33 ms +- 0.16` | `1.37 ms +- 0.03` | `2.97 ms +- 1.49` | `1.97 ms +- 0.76` | `1.42 ms +- 0.07` | `1.73 ms +- 0.50` | `1.66 ms +- 0.35` | `1.28 ms +- 0.39` | `1.42 ms +- 0.23` | `0.52 ms +- 0.12` |
| `olap_type1_age_rollup` | `1.81 ms +- 0.19` | `1.62 ms +- 0.03` | `2.79 ms +- 1.16` | `2.17 ms +- 0.96` | `1.54 ms +- 0.12` | `1.07 ms +- 0.26` | `0.97 ms +- 0.10` | `0.68 ms +- 0.10` | `0.74 ms +- 0.09` | `1.30 ms +- 0.25` |
| `olap_type2_score_distribution` | `1.57 ms +- 0.13` | `1.70 ms +- 0.06` | `3.20 ms +- 1.66` | `2.28 ms +- 1.01` | `1.65 ms +- 0.14` | `0.92 ms +- 0.21` | `0.94 ms +- 0.16` | `0.68 ms +- 0.06` | `0.67 ms +- 0.09` | `2.50 ms +- 0.23` |
| `olap_variable_length_grouped_rollup` | `10.93 ms +- 0.78` | `10.21 ms +- 0.12` | `4.94 ms +- 0.63` | `8.42 ms +- 2.75` | `6.01 ms +- 0.76` | `11.23 ms +- 2.69` | `11.45 ms +- 1.22` | `22.65 ms +- 4.55` | `18.29 ms +- 1.00` | `22.94 ms +- 0.33` |
| `olap_variable_length_reachability` | `1.82 ms +- 0.22` | `3.35 ms +- 0.06` | `5.08 ms +- 2.24` | `3.58 ms +- 1.59` | `2.81 ms +- 0.38` | `0.43 ms +- 0.04` | `0.58 ms +- 0.07` | `0.08 ms +- 0.02` | `0.52 ms +- 0.05` | `1.84 ms +- 0.23` |
| `olap_with_scalar_rebinding` | `2.28 ms +- 0.26` | `2.04 ms +- 0.03` | `3.83 ms +- 1.89` | `3.13 ms +- 1.02` | `2.17 ms +- 0.24` | `1.07 ms +- 0.28` | `1.01 ms +- 0.10` | `0.86 ms +- 0.15` | `0.78 ms +- 0.11` | `2.55 ms +- 0.16` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.25 ms +- 0.64` | `3.20 ms +- 0.06` | `4.99 ms +- 2.78` | `4.87 ms +- 2.18` | `3.04 ms +- 0.43` | `4.92 ms +- 1.65` | `4.61 ms +- 1.00` | `9.10 ms +- 3.22` | `9.46 ms +- 3.10` | `6.15 ms +- 0.37` |
| `olap_fixed_length_path_projection` | `7.48 ms +- 0.69` | `7.48 ms +- 0.22` | `7.54 ms +- 3.78` | `10.10 ms +- 4.10` | `6.19 ms +- 0.62` | `8.59 ms +- 2.26` | `8.23 ms +- 1.67` | `8.81 ms +- 2.07` | `8.61 ms +- 2.18` | `20.83 ms +- 0.76` |
| `olap_graph_introspection_rollup` | `2.14 ms +- 0.26` | `2.99 ms +- 0.12` | `5.22 ms +- 2.64` | `5.95 ms +- 2.37` | `3.45 ms +- 0.63` | `5.32 ms +- 1.75` | `5.59 ms +- 1.48` | `15.04 ms +- 4.57` | `14.91 ms +- 4.87` | `4.14 ms +- 0.67` |
| `olap_three_type_path_count` | `5.63 ms +- 0.46` | `5.59 ms +- 0.20` | `4.68 ms +- 2.74` | `5.93 ms +- 3.00` | `4.02 ms +- 0.86` | `3.88 ms +- 1.06` | `3.92 ms +- 1.33` | `0.20 ms +- 0.05` | `0.19 ms +- 0.01` | `5.77 ms +- 0.44` |
| `olap_type1_active_leaderboard` | `1.68 ms +- 0.32` | `1.88 ms +- 0.12` | `3.99 ms +- 2.20` | `2.69 ms +- 0.95` | `1.86 ms +- 0.25` | `2.63 ms +- 0.87` | `2.23 ms +- 0.46` | `2.07 ms +- 0.88` | `2.41 ms +- 0.76` | `0.94 ms +- 0.13` |
| `olap_type1_age_rollup` | `2.22 ms +- 0.41` | `1.96 ms +- 0.01` | `3.66 ms +- 1.79` | `2.80 ms +- 1.19` | `2.03 ms +- 0.36` | `1.72 ms +- 0.47` | `1.51 ms +- 0.15` | `1.16 ms +- 0.24` | `1.44 ms +- 0.50` | `3.12 ms +- 0.39` |
| `olap_type2_score_distribution` | `1.96 ms +- 0.28` | `2.15 ms +- 0.19` | `4.31 ms +- 2.16` | `3.03 ms +- 1.38` | `2.10 ms +- 0.42` | `1.51 ms +- 0.41` | `1.42 ms +- 0.29` | `0.95 ms +- 0.18` | `1.01 ms +- 0.36` | `3.67 ms +- 0.18` |
| `olap_variable_length_grouped_rollup` | `12.63 ms +- 2.00` | `11.82 ms +- 0.26` | `6.16 ms +- 0.90` | `10.84 ms +- 3.61` | `7.59 ms +- 1.58` | `14.06 ms +- 4.16` | `15.25 ms +- 2.05` | `29.72 ms +- 9.69` | `21.64 ms +- 1.62` | `26.60 ms +- 0.75` |
| `olap_variable_length_reachability` | `2.28 ms +- 0.45` | `3.94 ms +- 0.05` | `6.49 ms +- 3.18` | `4.51 ms +- 1.90` | `3.51 ms +- 0.65` | `0.66 ms +- 0.09` | `0.77 ms +- 0.07` | `0.20 ms +- 0.08` | `0.78 ms +- 0.08` | `2.70 ms +- 0.29` |
| `olap_with_scalar_rebinding` | `2.69 ms +- 0.49` | `2.57 ms +- 0.13` | `5.11 ms +- 2.72` | `4.21 ms +- 1.23` | `2.67 ms +- 0.47` | `1.65 ms +- 0.35` | `1.79 ms +- 0.56` | `1.86 ms +- 0.75` | `1.45 ms +- 0.63` | `3.61 ms +- 0.17` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | `4.80 ms +- 0.58` | `3.66 ms +- 0.16` | `5.75 ms +- 3.07` | `5.58 ms +- 2.76` | `3.34 ms +- 0.53` | `5.76 ms +- 2.03` | `5.04 ms +- 1.29` | `10.60 ms +- 3.13` | `11.35 ms +- 3.88` | `7.30 ms +- 0.61` |
| `olap_fixed_length_path_projection` | `8.17 ms +- 0.62` | `8.45 ms +- 0.42` | `8.36 ms +- 4.15` | `11.20 ms +- 4.34` | `7.63 ms +- 1.94` | `9.56 ms +- 2.19` | `9.09 ms +- 1.80` | `11.16 ms +- 2.42` | `10.02 ms +- 2.76` | `22.21 ms +- 0.20` |
| `olap_graph_introspection_rollup` | `2.39 ms +- 0.17` | `3.33 ms +- 0.17` | `5.92 ms +- 3.10` | `6.88 ms +- 2.70` | `3.78 ms +- 0.80` | `6.14 ms +- 2.11` | `6.20 ms +- 1.70` | `16.47 ms +- 4.66` | `16.77 ms +- 5.12` | `5.02 ms +- 0.51` |
| `olap_three_type_path_count` | `6.14 ms +- 0.64` | `6.36 ms +- 0.07` | `5.63 ms +- 3.59` | `6.78 ms +- 3.31` | `4.57 ms +- 0.99` | `4.52 ms +- 1.14` | `4.88 ms +- 1.86` | `0.28 ms +- 0.11` | `0.24 ms +- 0.04` | `7.01 ms +- 0.51` |
| `olap_type1_active_leaderboard` | `1.95 ms +- 0.40` | `2.09 ms +- 0.10` | `4.62 ms +- 2.38` | `3.11 ms +- 1.09` | `2.08 ms +- 0.34` | `3.15 ms +- 1.07` | `2.61 ms +- 0.52` | `2.71 ms +- 1.37` | `3.31 ms +- 1.30` | `1.53 ms +- 0.18` |
| `olap_type1_age_rollup` | `2.47 ms +- 0.43` | `2.27 ms +- 0.02` | `4.45 ms +- 1.95` | `3.11 ms +- 1.24` | `2.36 ms +- 0.46` | `2.07 ms +- 0.58` | `2.27 ms +- 1.16` | `1.40 ms +- 0.39` | `1.81 ms +- 0.55` | `4.49 ms +- 0.69` |
| `olap_type2_score_distribution` | `2.21 ms +- 0.31` | `2.38 ms +- 0.15` | `5.42 ms +- 2.50` | `3.59 ms +- 1.92` | `2.41 ms +- 0.59` | `1.79 ms +- 0.44` | `1.83 ms +- 0.77` | `1.20 ms +- 0.32` | `1.33 ms +- 0.43` | `4.63 ms +- 0.04` |
| `olap_variable_length_grouped_rollup` | `13.29 ms +- 2.01` | `13.25 ms +- 0.40` | `7.02 ms +- 0.90` | `12.08 ms +- 3.91` | `8.38 ms +- 1.67` | `16.25 ms +- 3.81` | `17.56 ms +- 1.55` | `34.06 ms +- 11.70` | `24.48 ms +- 1.66` | `29.62 ms +- 1.96` |
| `olap_variable_length_reachability` | `2.59 ms +- 0.62` | `4.57 ms +- 0.34` | `7.35 ms +- 3.50` | `5.20 ms +- 2.51` | `4.10 ms +- 0.74` | `0.95 ms +- 0.27` | `0.93 ms +- 0.13` | `0.27 ms +- 0.09` | `0.92 ms +- 0.11` | `3.71 ms +- 0.50` |
| `olap_with_scalar_rebinding` | `2.95 ms +- 0.49` | `3.05 ms +- 0.27` | `6.06 ms +- 2.99` | `4.71 ms +- 1.41` | `2.96 ms +- 0.49` | `1.88 ms +- 0.53` | `2.27 ms +- 1.07` | `2.33 ms +- 1.10` | `1.97 ms +- 1.23` | `4.66 ms +- 0.46` |
