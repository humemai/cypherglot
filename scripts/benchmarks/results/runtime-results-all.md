# Runtime Result Summary

- Scanned JSON files: 10
- Completed runs: 10
- Skipped non-completed runs: 0
- Grouped configurations: 10
- Grouped benchmark campaigns: 1

### Small runtime dataset

The current small runtime matrix used the `small` preset with `100` OLTP iterations / `5` OLTP warmup and `100` OLAP iterations / `5` OLAP warmup.

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
| SQLite Indexed | `15.33 ms +- 0.00` | `8.78 ms +- 0.00` | `130.93 ms +- 0.00` | `16.24 ms +- 0.00` | `6.90 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| SQLite Unindexed | `16.38 ms +- 0.00` | `8.78 ms +- 0.00` | `126.73 ms +- 0.00` | `1.57 ms +- 0.00` | `0.61 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| DuckDB Unindexed | `10.94 ms +- 0.00` | `128.98 ms +- 0.00` | `216.08 ms +- 0.00` | `0.00 ms +- 0.00` | `0.12 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| PostgreSQL Indexed | `5.93 ms +- 0.00` | `363.86 ms +- 0.00` | `226.87 ms +- 0.00` | `320.15 ms +- 0.00` | `86.77 ms +- 0.00` | `1.57 ms +- 0.00` | `2.24 ms +- 0.00` | `2.77 ms +- 0.00` |
| PostgreSQL Unindexed | `4.68 ms +- 0.00` | `321.90 ms +- 0.00` | `252.56 ms +- 0.00` | `11.20 ms +- 0.00` | `89.29 ms +- 0.00` | `1.95 ms +- 0.00` | `2.96 ms +- 0.00` | `3.82 ms +- 0.00` |
| Neo4j Indexed | `73.01 ms +- 0.00` | `356.31 ms +- 0.00` | `2282.51 ms +- 0.00` | `913.95 ms +- 0.00` | `0.00 ms +- 0.00` | `0.73 ms +- 0.00` | `1.28 ms +- 0.00` | `1.91 ms +- 0.00` |
| Neo4j Unindexed | `68.43 ms +- 0.00` | `334.76 ms +- 0.00` | `2202.45 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.87 ms +- 0.00` | `1.81 ms +- 0.00` | `3.68 ms +- 0.00` |
| ArcadeDB Indexed | `369.93 ms +- 0.00` | `520.97 ms +- 0.00` | `846.91 ms +- 0.00` | `395.81 ms +- 0.00` | `0.00 ms +- 0.00` | `0.32 ms +- 0.00` | `0.92 ms +- 0.00` | `2.37 ms +- 0.00` |
| ArcadeDB Unindexed | `366.69 ms +- 0.00` | `521.60 ms +- 0.00` | `1036.23 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.91 ms +- 0.00` | `2.67 ms +- 0.00` | `11.58 ms +- 0.00` |
| LadybugDB Unindexed | `76.35 ms +- 0.00` | `57.36 ms +- 0.00` | `729.49 ms +- 0.00` | `0.00 ms +- 0.00` | `23.12 ms +- 0.00` | `1.15 ms +- 0.00` | `2.41 ms +- 0.00` | `3.02 ms +- 0.00` |

OLAP summary:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `12.64 ms +- 0.00` | `4.30 ms +- 0.00` | `118.02 ms +- 0.00` | `16.53 ms +- 0.00` | `7.28 ms +- 0.00` | `2.91 ms +- 0.00` | `4.24 ms +- 0.00` | `4.62 ms +- 0.00` |
| SQLite Unindexed | `20.69 ms +- 0.00` | `11.57 ms +- 0.00` | `115.44 ms +- 0.00` | `2.29 ms +- 0.00` | `0.68 ms +- 0.00` | `3.30 ms +- 0.00` | `4.61 ms +- 0.00` | `5.11 ms +- 0.00` |
| DuckDB Unindexed | `12.62 ms +- 0.00` | `118.87 ms +- 0.00` | `207.48 ms +- 0.00` | `0.00 ms +- 0.00` | `0.21 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| PostgreSQL Indexed | `5.57 ms +- 0.00` | `549.07 ms +- 0.00` | `187.00 ms +- 0.00` | `226.84 ms +- 0.00` | `81.99 ms +- 0.00` | `3.61 ms +- 0.00` | `5.09 ms +- 0.00` | `5.97 ms +- 0.00` |
| PostgreSQL Unindexed | `5.24 ms +- 0.00` | `332.84 ms +- 0.00` | `210.58 ms +- 0.00` | `17.18 ms +- 0.00` | `82.89 ms +- 0.00` | `3.21 ms +- 0.00` | `4.45 ms +- 0.00` | `5.71 ms +- 0.00` |
| Neo4j Indexed | `73.01 ms +- 0.00` | `356.31 ms +- 0.00` | `2282.51 ms +- 0.00` | `913.95 ms +- 0.00` | `0.00 ms +- 0.00` | `2.91 ms +- 0.00` | `4.35 ms +- 0.00` | `5.74 ms +- 0.00` |
| Neo4j Unindexed | `68.43 ms +- 0.00` | `334.76 ms +- 0.00` | `2202.45 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `2.97 ms +- 0.00` | `4.50 ms +- 0.00` | `6.73 ms +- 0.00` |
| ArcadeDB Indexed | `1.69 ms +- 0.00` | `24.22 ms +- 0.00` | `358.20 ms +- 0.00` | `75.76 ms +- 0.00` | `289.68 ms +- 0.00` | `4.38 ms +- 0.00` | `7.69 ms +- 0.00` | `15.91 ms +- 0.00` |
| ArcadeDB Unindexed | `3.04 ms +- 0.00` | `59.02 ms +- 0.00` | `336.65 ms +- 0.00` | `0.00 ms +- 0.00` | `382.61 ms +- 0.00` | `4.66 ms +- 0.00` | `8.62 ms +- 0.00` | `15.81 ms +- 0.00` |
| LadybugDB Unindexed | `76.94 ms +- 0.00` | `27.69 ms +- 0.00` | `675.44 ms +- 0.00` | `0.00 ms +- 0.00` | `18.87 ms +- 0.00` | `2.44 ms +- 0.00` | `3.95 ms +- 0.00` | `4.95 ms +- 0.00` |

The tables below sum all process memory involved in the benchmark at each checkpoint:
embedded backends contribute only the benchmark process, while PostgreSQL and Neo4j add
the server-side RSS snapshot to the client process snapshot.

Total RSS checkpoints, OLTP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `93.39 MiB +- 0.00` | `93.50 MiB +- 0.00` | `95.92 MiB +- 0.00` | `95.98 MiB +- 0.00` | `95.98 MiB +- 0.00` | `96.36 MiB +- 0.00` |
| SQLite Unindexed | `93.25 MiB +- 0.00` | `93.35 MiB +- 0.00` | `95.80 MiB +- 0.00` | `95.85 MiB +- 0.00` | `95.85 MiB +- 0.00` | `96.18 MiB +- 0.00` |
| DuckDB Unindexed | `94.77 MiB +- 0.00` | `99.48 MiB +- 0.00` | `157.60 MiB +- 0.00` | `157.60 MiB +- 0.00` | `157.60 MiB +- 0.00` | `147.90 MiB +- 0.00` |
| PostgreSQL Indexed | `118.89 MiB +- 0.00` | `120.84 MiB +- 0.00` | `129.19 MiB +- 0.00` | `129.33 MiB +- 0.00` | `132.08 MiB +- 0.00` | `152.10 MiB +- 0.00` |
| PostgreSQL Unindexed | `118.94 MiB +- 0.00` | `120.88 MiB +- 0.00` | `129.24 MiB +- 0.00` | `129.61 MiB +- 0.00` | `131.28 MiB +- 0.00` | `148.79 MiB +- 0.00` |
| Neo4j Indexed | `675.22 MiB +- 0.00` | `732.39 MiB +- 0.00` | `1809.65 MiB +- 0.00` | `954.29 MiB +- 0.00` | `0.00 MiB +- 0.00` | `1013.07 MiB +- 0.00` |
| Neo4j Unindexed | `655.25 MiB +- 0.00` | `700.83 MiB +- 0.00` | `1760.54 MiB +- 0.00` | `1759.51 MiB +- 0.00` | `0.00 MiB +- 0.00` | `990.31 MiB +- 0.00` |
| ArcadeDB Indexed | `133.77 MiB +- 0.00` | `203.79 MiB +- 0.00` | `319.09 MiB +- 0.00` | `362.20 MiB +- 0.00` | `362.20 MiB +- 0.00` | `362.65 MiB +- 0.00` |
| ArcadeDB Unindexed | `134.25 MiB +- 0.00` | `212.11 MiB +- 0.00` | `297.45 MiB +- 0.00` | `297.45 MiB +- 0.00` | `297.45 MiB +- 0.00` | `297.86 MiB +- 0.00` |
| LadybugDB Unindexed | `265.80 MiB +- 0.00` | `292.53 MiB +- 0.00` | `512.99 MiB +- 0.00` | `512.99 MiB +- 0.00` | `513.30 MiB +- 0.00` | `531.82 MiB +- 0.00` |

Total RSS checkpoints, OLAP:

| Combo | Connect / Reset | Schema / Constraints | Ingest | Index | Analyze | Suite complete |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite Indexed | `96.36 MiB +- 0.00` | `96.39 MiB +- 0.00` | `97.46 MiB +- 0.00` | `97.50 MiB +- 0.00` | `97.50 MiB +- 0.00` | `121.98 MiB +- 0.00` |
| SQLite Unindexed | `96.18 MiB +- 0.00` | `96.21 MiB +- 0.00` | `97.25 MiB +- 0.00` | `97.25 MiB +- 0.00` | `97.26 MiB +- 0.00` | `112.37 MiB +- 0.00` |
| DuckDB Unindexed | `154.03 MiB +- 0.00` | `154.87 MiB +- 0.00` | `174.09 MiB +- 0.00` | `174.09 MiB +- 0.00` | `174.09 MiB +- 0.00` | `177.62 MiB +- 0.00` |
| PostgreSQL Indexed | `149.47 MiB +- 0.00` | `151.10 MiB +- 0.00` | `150.91 MiB +- 0.00` | `151.02 MiB +- 0.00` | `152.81 MiB +- 0.00` | `162.25 MiB +- 0.00` |
| PostgreSQL Unindexed | `146.04 MiB +- 0.00` | `146.56 MiB +- 0.00` | `148.38 MiB +- 0.00` | `148.40 MiB +- 0.00` | `149.93 MiB +- 0.00` | `160.84 MiB +- 0.00` |
| Neo4j Indexed | `675.22 MiB +- 0.00` | `732.39 MiB +- 0.00` | `1809.65 MiB +- 0.00` | `954.29 MiB +- 0.00` | `0.00 MiB +- 0.00` | `1908.36 MiB +- 0.00` |
| Neo4j Unindexed | `655.25 MiB +- 0.00` | `700.83 MiB +- 0.00` | `1760.54 MiB +- 0.00` | `1759.51 MiB +- 0.00` | `0.00 MiB +- 0.00` | `1015.84 MiB +- 0.00` |
| ArcadeDB Indexed | `362.79 MiB +- 0.00` | `362.81 MiB +- 0.00` | `387.16 MiB +- 0.00` | `387.52 MiB +- 0.00` | `428.55 MiB +- 0.00` | `430.33 MiB +- 0.00` |
| ArcadeDB Unindexed | `298.01 MiB +- 0.00` | `299.97 MiB +- 0.00` | `302.16 MiB +- 0.00` | `302.16 MiB +- 0.00` | `341.04 MiB +- 0.00` | `340.87 MiB +- 0.00` |
| LadybugDB Unindexed | `452.32 MiB +- 0.00` | `460.24 MiB +- 0.00` | `576.55 MiB +- 0.00` | `576.55 MiB +- 0.00` | `576.55 MiB +- 0.00` | `616.04 MiB +- 0.00` |

#### Small runtime suite comparison

This rolls the small-runtime matrix up to suite-level end-to-end percentiles for each workload/backend combination.

| Suite | p50 | p95 | p99 |
| --- | --- | --- | --- |
| `oltp/sqlite_indexed` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap/sqlite_indexed` | `2.91 ms +- 0.00` | `4.24 ms +- 0.00` | `4.62 ms +- 0.00` |
| `oltp/sqlite_unindexed` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap/sqlite_unindexed` | `3.30 ms +- 0.00` | `4.61 ms +- 0.00` | `5.11 ms +- 0.00` |
| `oltp/duckdb` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `olap/duckdb` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `oltp/postgresql_indexed` | `1.57 ms +- 0.00` | `2.24 ms +- 0.00` | `2.77 ms +- 0.00` |
| `olap/postgresql_indexed` | `3.61 ms +- 0.00` | `5.09 ms +- 0.00` | `5.97 ms +- 0.00` |
| `oltp/postgresql_unindexed` | `1.95 ms +- 0.00` | `2.96 ms +- 0.00` | `3.82 ms +- 0.00` |
| `olap/postgresql_unindexed` | `3.21 ms +- 0.00` | `4.45 ms +- 0.00` | `5.71 ms +- 0.00` |
| `oltp/neo4j_indexed` | `0.73 ms +- 0.00` | `1.28 ms +- 0.00` | `1.91 ms +- 0.00` |
| `olap/neo4j_indexed` | `2.91 ms +- 0.00` | `4.35 ms +- 0.00` | `5.74 ms +- 0.00` |
| `oltp/neo4j_unindexed` | `0.87 ms +- 0.00` | `1.81 ms +- 0.00` | `3.68 ms +- 0.00` |
| `olap/neo4j_unindexed` | `2.97 ms +- 0.00` | `4.50 ms +- 0.00` | `6.73 ms +- 0.00` |
| `oltp/arcadedb_embedded_indexed` | `0.32 ms +- 0.00` | `0.92 ms +- 0.00` | `2.37 ms +- 0.00` |
| `olap/arcadedb_embedded_indexed` | `4.38 ms +- 0.00` | `7.69 ms +- 0.00` | `15.91 ms +- 0.00` |
| `oltp/arcadedb_embedded_unindexed` | `0.91 ms +- 0.00` | `2.67 ms +- 0.00` | `11.58 ms +- 0.00` |
| `olap/arcadedb_embedded_unindexed` | `4.66 ms +- 0.00` | `8.62 ms +- 0.00` | `15.81 ms +- 0.00` |
| `oltp/ladybug_unindexed` | `1.15 ms +- 0.00` | `2.41 ms +- 0.00` | `3.02 ms +- 0.00` |
| `olap/ladybug_unindexed` | `2.44 ms +- 0.00` | `3.95 ms +- 0.00` | `4.95 ms +- 0.00` |

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
| `oltp_create_cross_type_edge` | - | - | - | `1.92 ms +- 0.00` | `3.18 ms +- 0.00` | `0.57 ms +- 0.00` | `1.28 ms +- 0.00` | `0.50 ms +- 0.00` | `1.67 ms +- 0.00` | `0.91 ms +- 0.00` |
| `oltp_create_type1_node` | - | - | - | `1.08 ms +- 0.00` | `1.00 ms +- 0.00` | `0.50 ms +- 0.00` | `0.51 ms +- 0.00` | `0.18 ms +- 0.00` | `0.14 ms +- 0.00` | `0.41 ms +- 0.00` |
| `oltp_cross_type_lookup` | - | - | - | `1.79 ms +- 0.00` | `2.24 ms +- 0.00` | `1.02 ms +- 0.00` | `0.79 ms +- 0.00` | `0.22 ms +- 0.00` | - | `3.15 ms +- 0.00` |
| `oltp_delete_type1_edge` | - | - | - | `1.31 ms +- 0.00` | `2.87 ms +- 0.00` | `0.55 ms +- 0.00` | `0.65 ms +- 0.00` | `0.32 ms +- 0.00` | `0.78 ms +- 0.00` | `1.90 ms +- 0.00` |
| `oltp_delete_type1_node` | - | - | - | `0.83 ms +- 0.00` | `2.17 ms +- 0.00` | `0.50 ms +- 0.00` | `0.68 ms +- 0.00` | `0.33 ms +- 0.00` | `0.89 ms +- 0.00` | `0.52 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | - | - | - | `2.46 ms +- 0.00` | - | `0.60 ms +- 0.00` | `1.28 ms +- 0.00` | `0.39 ms +- 0.00` | `1.69 ms +- 0.00` | - |
| `oltp_optional_missing_type1_lookup` | - | - | - | `1.21 ms +- 0.00` | `1.41 ms +- 0.00` | `0.60 ms +- 0.00` | `0.91 ms +- 0.00` | `0.87 ms +- 0.00` | `0.88 ms +- 0.00` | `1.06 ms +- 0.00` |
| `oltp_optional_type1_lookup` | - | - | - | `1.28 ms +- 0.00` | `1.45 ms +- 0.00` | `0.73 ms +- 0.00` | `1.15 ms +- 0.00` | - | - | `1.04 ms +- 0.00` |
| `oltp_program_create_and_link` | - | - | - | `2.49 ms +- 0.00` | `2.87 ms +- 0.00` | `0.58 ms +- 0.00` | `0.57 ms +- 0.00` | `0.49 ms +- 0.00` | `1.05 ms +- 0.00` | `0.76 ms +- 0.00` |
| `oltp_type1_neighbors` | - | - | - | `1.54 ms +- 0.00` | `1.97 ms +- 0.00` | `1.00 ms +- 0.00` | `1.07 ms +- 0.00` | `0.19 ms +- 0.00` | - | - |
| `oltp_type1_point_lookup` | - | - | - | `1.21 ms +- 0.00` | `1.22 ms +- 0.00` | `2.06 ms +- 0.00` | `1.51 ms +- 0.00` | `0.16 ms +- 0.00` | `0.90 ms +- 0.00` | `0.46 ms +- 0.00` |
| `oltp_unwind_literal_top2` | - | - | - | `1.09 ms +- 0.00` | `1.21 ms +- 0.00` | `0.56 ms +- 0.00` | `0.60 ms +- 0.00` | `0.05 ms +- 0.00` | `0.06 ms +- 0.00` | `0.60 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | - | - | - | `2.77 ms +- 0.00` | `2.62 ms +- 0.00` | `0.44 ms +- 0.00` | `0.55 ms +- 0.00` | `0.28 ms +- 0.00` | `1.00 ms +- 0.00` | `1.79 ms +- 0.00` |
| `oltp_update_type1_score` | - | - | - | `0.97 ms +- 0.00` | `1.20 ms +- 0.00` | `0.55 ms +- 0.00` | `0.62 ms +- 0.00` | `0.21 ms +- 0.00` | `0.96 ms +- 0.00` | - |

##### OLTP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | - | - | - | `2.84 ms +- 0.00` | `4.58 ms +- 0.00` | `0.85 ms +- 0.00` | `2.56 ms +- 0.00` | `1.03 ms +- 0.00` | `4.29 ms +- 0.00` | `2.93 ms +- 0.00` |
| `oltp_create_type1_node` | - | - | - | `1.37 ms +- 0.00` | `1.59 ms +- 0.00` | `0.86 ms +- 0.00` | `0.88 ms +- 0.00` | `0.35 ms +- 0.00` | `0.29 ms +- 0.00` | `2.01 ms +- 0.00` |
| `oltp_cross_type_lookup` | - | - | - | `2.57 ms +- 0.00` | `3.17 ms +- 0.00` | `1.38 ms +- 0.00` | `1.35 ms +- 0.00` | `0.41 ms +- 0.00` | - | `5.09 ms +- 0.00` |
| `oltp_delete_type1_edge` | - | - | - | `2.06 ms +- 0.00` | `3.86 ms +- 0.00` | `0.79 ms +- 0.00` | `1.35 ms +- 0.00` | `0.68 ms +- 0.00` | `2.21 ms +- 0.00` | `3.17 ms +- 0.00` |
| `oltp_delete_type1_node` | - | - | - | `1.24 ms +- 0.00` | `3.85 ms +- 0.00` | `1.08 ms +- 0.00` | `1.35 ms +- 0.00` | `1.20 ms +- 0.00` | `3.26 ms +- 0.00` | `0.74 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | - | - | - | `3.28 ms +- 0.00` | - | `0.97 ms +- 0.00` | `2.00 ms +- 0.00` | `0.90 ms +- 0.00` | `3.87 ms +- 0.00` | - |
| `oltp_optional_missing_type1_lookup` | - | - | - | `1.99 ms +- 0.00` | `2.13 ms +- 0.00` | `0.91 ms +- 0.00` | `1.57 ms +- 0.00` | `3.66 ms +- 0.00` | `4.21 ms +- 0.00` | `2.81 ms +- 0.00` |
| `oltp_optional_type1_lookup` | - | - | - | `1.81 ms +- 0.00` | `2.02 ms +- 0.00` | `1.32 ms +- 0.00` | `2.12 ms +- 0.00` | - | - | `1.98 ms +- 0.00` |
| `oltp_program_create_and_link` | - | - | - | `3.72 ms +- 0.00` | `4.03 ms +- 0.00` | `0.93 ms +- 0.00` | `0.95 ms +- 0.00` | `1.08 ms +- 0.00` | `3.44 ms +- 0.00` | `1.24 ms +- 0.00` |
| `oltp_type1_neighbors` | - | - | - | `2.25 ms +- 0.00` | `2.69 ms +- 0.00` | `1.98 ms +- 0.00` | `2.03 ms +- 0.00` | `0.38 ms +- 0.00` | - | - |
| `oltp_type1_point_lookup` | - | - | - | `1.86 ms +- 0.00` | `1.87 ms +- 0.00` | `3.68 ms +- 0.00` | `5.36 ms +- 0.00` | `1.15 ms +- 0.00` | `2.87 ms +- 0.00` | `0.79 ms +- 0.00` |
| `oltp_unwind_literal_top2` | - | - | - | `1.44 ms +- 0.00` | `1.79 ms +- 0.00` | `1.44 ms +- 0.00` | `1.22 ms +- 0.00` | `0.09 ms +- 0.00` | `0.10 ms +- 0.00` | `2.76 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | - | - | - | `3.60 ms +- 0.00` | `5.25 ms +- 0.00` | `0.78 ms +- 0.00` | `0.83 ms +- 0.00` | `0.57 ms +- 0.00` | `2.34 ms +- 0.00` | `3.00 ms +- 0.00` |
| `oltp_update_type1_score` | - | - | - | `1.35 ms +- 0.00` | `1.69 ms +- 0.00` | `1.02 ms +- 0.00` | `1.82 ms +- 0.00` | `0.41 ms +- 0.00` | `2.47 ms +- 0.00` | - |

##### OLTP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_create_cross_type_edge` | - | - | - | `3.96 ms +- 0.00` | `5.94 ms +- 0.00` | `1.04 ms +- 0.00` | `4.60 ms +- 0.00` | `1.48 ms +- 0.00` | `14.65 ms +- 0.00` | `3.86 ms +- 0.00` |
| `oltp_create_type1_node` | - | - | - | `2.41 ms +- 0.00` | `1.84 ms +- 0.00` | `1.09 ms +- 0.00` | `1.76 ms +- 0.00` | `0.39 ms +- 0.00` | `0.74 ms +- 0.00` | `2.96 ms +- 0.00` |
| `oltp_cross_type_lookup` | - | - | - | `2.69 ms +- 0.00` | `3.33 ms +- 0.00` | `1.87 ms +- 0.00` | `1.58 ms +- 0.00` | `0.49 ms +- 0.00` | - | `5.91 ms +- 0.00` |
| `oltp_delete_type1_edge` | - | - | - | `2.50 ms +- 0.00` | `4.70 ms +- 0.00` | `0.96 ms +- 0.00` | `1.93 ms +- 0.00` | `1.28 ms +- 0.00` | `11.91 ms +- 0.00` | `3.41 ms +- 0.00` |
| `oltp_delete_type1_node` | - | - | - | `1.57 ms +- 0.00` | `4.23 ms +- 0.00` | `1.55 ms +- 0.00` | `2.08 ms +- 0.00` | `1.87 ms +- 0.00` | `12.34 ms +- 0.00` | `1.15 ms +- 0.00` |
| `oltp_merge_cross_type_edge` | - | - | - | `3.80 ms +- 0.00` | - | `1.21 ms +- 0.00` | `5.93 ms +- 0.00` | `1.07 ms +- 0.00` | `13.67 ms +- 0.00` | - |
| `oltp_optional_missing_type1_lookup` | - | - | - | `3.92 ms +- 0.00` | `2.30 ms +- 0.00` | `2.06 ms +- 0.00` | `3.17 ms +- 0.00` | `13.07 ms +- 0.00` | `11.49 ms +- 0.00` | `3.33 ms +- 0.00` |
| `oltp_optional_type1_lookup` | - | - | - | `2.15 ms +- 0.00` | `2.20 ms +- 0.00` | `1.80 ms +- 0.00` | `5.04 ms +- 0.00` | - | - | `2.64 ms +- 0.00` |
| `oltp_program_create_and_link` | - | - | - | `4.11 ms +- 0.00` | `4.13 ms +- 0.00` | `1.25 ms +- 0.00` | `2.63 ms +- 0.00` | `2.82 ms +- 0.00` | `12.89 ms +- 0.00` | `2.04 ms +- 0.00` |
| `oltp_type1_neighbors` | - | - | - | `2.42 ms +- 0.00` | `2.87 ms +- 0.00` | `3.02 ms +- 0.00` | `2.53 ms +- 0.00` | `0.76 ms +- 0.00` | - | - |
| `oltp_type1_point_lookup` | - | - | - | `2.04 ms +- 0.00` | `2.05 ms +- 0.00` | `5.53 ms +- 0.00` | `12.46 ms +- 0.00` | `5.92 ms +- 0.00` | `17.69 ms +- 0.00` | `1.37 ms +- 0.00` |
| `oltp_unwind_literal_top2` | - | - | - | `1.55 ms +- 0.00` | `2.03 ms +- 0.00` | `2.01 ms +- 0.00` | `4.10 ms +- 0.00` | `0.14 ms +- 0.00` | `0.13 ms +- 0.00` | `3.10 ms +- 0.00` |
| `oltp_update_cross_type_edge_rank` | - | - | - | `4.08 ms +- 0.00` | `12.26 ms +- 0.00` | `1.71 ms +- 0.00` | `1.00 ms +- 0.00` | `0.97 ms +- 0.00` | `12.79 ms +- 0.00` | `3.47 ms +- 0.00` |
| `oltp_update_type1_score` | - | - | - | `1.52 ms +- 0.00` | `1.78 ms +- 0.00` | `1.69 ms +- 0.00` | `2.75 ms +- 0.00` | `0.51 ms +- 0.00` | `19.08 ms +- 0.00` | - |

##### OLAP query breakdown, end-to-end `p50`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | - | - | - | `2.86 ms +- 0.00` | `3.60 ms +- 0.00` | `2.90 ms +- 0.00` | `3.08 ms +- 0.00` | `7.88 ms +- 0.00` | `9.44 ms +- 0.00` | `4.61 ms +- 0.00` |
| `olap_fixed_length_path_projection` | - | - | - | `6.76 ms +- 0.00` | - | `5.72 ms +- 0.00` | `6.02 ms +- 0.00` | `4.86 ms +- 0.00` | `3.91 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | - | - | - | `6.61 ms +- 0.00` | `6.12 ms +- 0.00` | `5.94 ms +- 0.00` | `5.97 ms +- 0.00` | `4.97 ms +- 0.00` | `5.72 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `2.16 ms +- 0.00` | `3.14 ms +- 0.00` | - | `3.28 ms +- 0.00` | `3.13 ms +- 0.00` | `3.59 ms +- 0.00` | `3.74 ms +- 0.00` | `11.47 ms +- 0.00` | `12.29 ms +- 0.00` | `2.54 ms +- 0.00` |
| `olap_optional_type1_aggregate` | - | - | - | `2.38 ms +- 0.00` | `2.12 ms +- 0.00` | `1.37 ms +- 0.00` | `1.23 ms +- 0.00` | `0.99 ms +- 0.00` | `1.34 ms +- 0.00` | `1.63 ms +- 0.00` |
| `olap_relationship_function_projection` | - | `3.94 ms +- 0.00` | - | `3.99 ms +- 0.00` | `3.94 ms +- 0.00` | `3.42 ms +- 0.00` | `3.53 ms +- 0.00` | `7.99 ms +- 0.00` | `9.92 ms +- 0.00` | `3.20 ms +- 0.00` |
| `olap_three_type_path_count` | - | - | - | `4.78 ms +- 0.00` | `3.39 ms +- 0.00` | `2.88 ms +- 0.00` | `2.83 ms +- 0.00` | `0.13 ms +- 0.00` | `0.16 ms +- 0.00` | `3.84 ms +- 0.00` |
| `olap_type1_active_leaderboard` | - | - | - | `2.20 ms +- 0.00` | `1.69 ms +- 0.00` | `2.05 ms +- 0.00` | `2.15 ms +- 0.00` | `1.98 ms +- 0.00` | `2.15 ms +- 0.00` | `0.62 ms +- 0.00` |
| `olap_type1_age_rollup` | - | - | - | `2.70 ms +- 0.00` | `1.86 ms +- 0.00` | `1.89 ms +- 0.00` | `1.68 ms +- 0.00` | `0.90 ms +- 0.00` | `0.94 ms +- 0.00` | `2.12 ms +- 0.00` |
| `olap_type2_score_distribution` | - | - | - | `1.93 ms +- 0.00` | `1.80 ms +- 0.00` | `1.51 ms +- 0.00` | `1.56 ms +- 0.00` | `0.92 ms +- 0.00` | `0.94 ms +- 0.00` | `2.18 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `2.50 ms +- 0.00` | - | - | `3.01 ms +- 0.00` | `3.02 ms +- 0.00` | `0.35 ms +- 0.00` | `0.51 ms +- 0.00` | `0.25 ms +- 0.00` | `1.18 ms +- 0.00` | `4.87 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - | `6.26 ms +- 0.00` | `6.12 ms +- 0.00` | `10.47 ms +- 0.00` | `10.60 ms +- 0.00` | `20.72 ms +- 0.00` | `19.31 ms +- 0.00` | - |
| `olap_variable_length_reachability` | - | - | - | `3.50 ms +- 0.00` | `3.44 ms +- 0.00` | `0.62 ms +- 0.00` | `0.71 ms +- 0.00` | `0.37 ms +- 0.00` | `1.03 ms +- 0.00` | `2.07 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `4.29 ms +- 0.00` | `4.17 ms +- 0.00` | - | `2.46 ms +- 0.00` | `2.54 ms +- 0.00` | `1.09 ms +- 0.00` | `1.12 ms +- 0.00` | `1.41 ms +- 0.00` | `1.18 ms +- 0.00` | - |
| `olap_with_size_predicate_projection` | `2.65 ms +- 0.00` | `2.67 ms +- 0.00` | - | `2.96 ms +- 0.00` | `2.58 ms +- 0.00` | `1.39 ms +- 0.00` | `1.51 ms +- 0.00` | `2.44 ms +- 0.00` | `2.40 ms +- 0.00` | `0.85 ms +- 0.00` |
| `olap_with_where_lower_projection` | `2.96 ms +- 0.00` | `2.57 ms +- 0.00` | - | `2.16 ms +- 0.00` | `2.74 ms +- 0.00` | `1.43 ms +- 0.00` | `1.35 ms +- 0.00` | `2.72 ms +- 0.00` | `2.63 ms +- 0.00` | `0.81 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p95`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | - | - | - | `4.11 ms +- 0.00` | `5.70 ms +- 0.00` | `4.54 ms +- 0.00` | `5.71 ms +- 0.00` | `11.41 ms +- 0.00` | `16.10 ms +- 0.00` | `6.39 ms +- 0.00` |
| `olap_fixed_length_path_projection` | - | - | - | `8.25 ms +- 0.00` | - | `8.55 ms +- 0.00` | `10.56 ms +- 0.00` | `9.61 ms +- 0.00` | `9.76 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | - | - | - | `9.95 ms +- 0.00` | `8.19 ms +- 0.00` | `9.18 ms +- 0.00` | `8.05 ms +- 0.00` | `12.62 ms +- 0.00` | `14.07 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `3.00 ms +- 0.00` | `4.26 ms +- 0.00` | - | `4.02 ms +- 0.00` | `3.86 ms +- 0.00` | `6.00 ms +- 0.00` | `6.27 ms +- 0.00` | `21.91 ms +- 0.00` | `17.75 ms +- 0.00` | `4.10 ms +- 0.00` |
| `olap_optional_type1_aggregate` | - | - | - | `3.53 ms +- 0.00` | `3.09 ms +- 0.00` | `2.02 ms +- 0.00` | `1.81 ms +- 0.00` | `2.07 ms +- 0.00` | `2.09 ms +- 0.00` | `3.44 ms +- 0.00` |
| `olap_relationship_function_projection` | - | `6.94 ms +- 0.00` | - | `4.76 ms +- 0.00` | `4.76 ms +- 0.00` | `4.61 ms +- 0.00` | `5.21 ms +- 0.00` | `12.57 ms +- 0.00` | `17.46 ms +- 0.00` | `4.55 ms +- 0.00` |
| `olap_three_type_path_count` | - | - | - | `6.67 ms +- 0.00` | `4.15 ms +- 0.00` | `4.65 ms +- 0.00` | `4.61 ms +- 0.00` | `0.41 ms +- 0.00` | `0.33 ms +- 0.00` | `5.22 ms +- 0.00` |
| `olap_type1_active_leaderboard` | - | - | - | `3.50 ms +- 0.00` | `2.40 ms +- 0.00` | `4.74 ms +- 0.00` | `3.80 ms +- 0.00` | `3.06 ms +- 0.00` | `2.87 ms +- 0.00` | `1.54 ms +- 0.00` |
| `olap_type1_age_rollup` | - | - | - | `4.16 ms +- 0.00` | `2.56 ms +- 0.00` | `2.79 ms +- 0.00` | `2.63 ms +- 0.00` | `1.83 ms +- 0.00` | `2.70 ms +- 0.00` | `3.65 ms +- 0.00` |
| `olap_type2_score_distribution` | - | - | - | `4.89 ms +- 0.00` | `2.52 ms +- 0.00` | `2.10 ms +- 0.00` | `2.17 ms +- 0.00` | `1.62 ms +- 0.00` | `2.18 ms +- 0.00` | `3.63 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `4.91 ms +- 0.00` | - | - | `3.95 ms +- 0.00` | `3.81 ms +- 0.00` | `0.52 ms +- 0.00` | `0.71 ms +- 0.00` | `0.48 ms +- 0.00` | `2.38 ms +- 0.00` | `6.93 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - | `8.00 ms +- 0.00` | `8.30 ms +- 0.00` | `13.71 ms +- 0.00` | `13.47 ms +- 0.00` | `33.87 ms +- 0.00` | `38.24 ms +- 0.00` | - |
| `olap_variable_length_reachability` | - | - | - | `5.21 ms +- 0.00` | `6.24 ms +- 0.00` | `0.91 ms +- 0.00` | `1.01 ms +- 0.00` | `1.12 ms +- 0.00` | `1.96 ms +- 0.00` | `4.07 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `5.34 ms +- 0.00` | `5.05 ms +- 0.00` | - | `3.15 ms +- 0.00` | `3.25 ms +- 0.00` | `1.64 ms +- 0.00` | `1.84 ms +- 0.00` | `2.41 ms +- 0.00` | `3.14 ms +- 0.00` | - |
| `olap_with_size_predicate_projection` | `3.97 ms +- 0.00` | `3.34 ms +- 0.00` | - | `4.35 ms +- 0.00` | `4.22 ms +- 0.00` | `1.81 ms +- 0.00` | `2.37 ms +- 0.00` | `3.65 ms +- 0.00` | `3.30 ms +- 0.00` | `1.69 ms +- 0.00` |
| `olap_with_where_lower_projection` | `3.99 ms +- 0.00` | `3.46 ms +- 0.00` | - | `2.97 ms +- 0.00` | `3.68 ms +- 0.00` | `1.87 ms +- 0.00` | `1.83 ms +- 0.00` | `4.35 ms +- 0.00` | `3.63 ms +- 0.00` | `2.24 ms +- 0.00` |

##### OLAP query breakdown, end-to-end `p99`

| Query | SQLite Indexed | SQLite Unindexed | DuckDB Unindexed | PostgreSQL Indexed | PostgreSQL Unindexed | Neo4j Indexed | Neo4j Unindexed | ArcadeDB Indexed | ArcadeDB Unindexed | LadybugDB Unindexed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `olap_cross_type_edge_rollup` | - | - | - | `4.75 ms +- 0.00` | `7.07 ms +- 0.00` | `6.13 ms +- 0.00` | `6.87 ms +- 0.00` | `21.62 ms +- 0.00` | `35.03 ms +- 0.00` | `7.27 ms +- 0.00` |
| `olap_fixed_length_path_projection` | - | - | - | `8.72 ms +- 0.00` | - | `12.18 ms +- 0.00` | `15.94 ms +- 0.00` | `18.95 ms +- 0.00` | `10.76 ms +- 0.00` | - |
| `olap_fixed_length_path_with_rebinding` | - | - | - | `14.77 ms +- 0.00` | `9.41 ms +- 0.00` | `13.56 ms +- 0.00` | `14.55 ms +- 0.00` | `14.97 ms +- 0.00` | `15.28 ms +- 0.00` | - |
| `olap_graph_introspection_rollup` | `3.40 ms +- 0.00` | `5.02 ms +- 0.00` | - | `4.23 ms +- 0.00` | `4.04 ms +- 0.00` | `8.61 ms +- 0.00` | `7.26 ms +- 0.00` | `28.50 ms +- 0.00` | `22.09 ms +- 0.00` | `4.40 ms +- 0.00` |
| `olap_optional_type1_aggregate` | - | - | - | `3.69 ms +- 0.00` | `3.30 ms +- 0.00` | `2.98 ms +- 0.00` | `2.36 ms +- 0.00` | `28.30 ms +- 0.00` | `2.39 ms +- 0.00` | `4.82 ms +- 0.00` |
| `olap_relationship_function_projection` | - | `7.49 ms +- 0.00` | - | `5.25 ms +- 0.00` | `5.37 ms +- 0.00` | `6.18 ms +- 0.00` | `5.72 ms +- 0.00` | `17.12 ms +- 0.00` | `31.95 ms +- 0.00` | `5.03 ms +- 0.00` |
| `olap_three_type_path_count` | - | - | - | `7.46 ms +- 0.00` | `4.59 ms +- 0.00` | `6.11 ms +- 0.00` | `5.89 ms +- 0.00` | `0.82 ms +- 0.00` | `0.57 ms +- 0.00` | `6.90 ms +- 0.00` |
| `olap_type1_active_leaderboard` | - | - | - | `5.01 ms +- 0.00` | `2.85 ms +- 0.00` | `5.50 ms +- 0.00` | `5.74 ms +- 0.00` | `12.68 ms +- 0.00` | `3.02 ms +- 0.00` | `1.93 ms +- 0.00` |
| `olap_type1_age_rollup` | - | - | - | `4.51 ms +- 0.00` | `3.01 ms +- 0.00` | `4.05 ms +- 0.00` | `3.41 ms +- 0.00` | `10.40 ms +- 0.00` | `12.91 ms +- 0.00` | `5.20 ms +- 0.00` |
| `olap_type2_score_distribution` | - | - | - | `6.60 ms +- 0.00` | `2.63 ms +- 0.00` | `2.29 ms +- 0.00` | `3.58 ms +- 0.00` | `9.89 ms +- 0.00` | `22.48 ms +- 0.00` | `4.57 ms +- 0.00` |
| `olap_variable_length_grouped_max_rollup` | `5.33 ms +- 0.00` | - | - | `4.20 ms +- 0.00` | `4.23 ms +- 0.00` | `1.02 ms +- 0.00` | `1.02 ms +- 0.00` | `0.60 ms +- 0.00` | `2.46 ms +- 0.00` | `7.79 ms +- 0.00` |
| `olap_variable_length_grouped_rollup` | - | - | - | `8.74 ms +- 0.00` | `9.40 ms +- 0.00` | `15.46 ms +- 0.00` | `15.39 ms +- 0.00` | `36.93 ms +- 0.00` | `59.94 ms +- 0.00` | - |
| `olap_variable_length_reachability` | - | - | - | `6.04 ms +- 0.00` | `15.82 ms +- 0.00` | `1.25 ms +- 0.00` | `2.28 ms +- 0.00` | `3.45 ms +- 0.00` | `2.18 ms +- 0.00` | `5.73 ms +- 0.00` |
| `olap_with_scalar_rebinding` | `5.44 ms +- 0.00` | `5.74 ms +- 0.00` | - | `3.39 ms +- 0.00` | `3.45 ms +- 0.00` | `1.86 ms +- 0.00` | `2.15 ms +- 0.00` | `17.35 ms +- 0.00` | `15.36 ms +- 0.00` | - |
| `olap_with_size_predicate_projection` | `4.38 ms +- 0.00` | `3.62 ms +- 0.00` | - | `4.67 ms +- 0.00` | `5.57 ms +- 0.00` | `2.30 ms +- 0.00` | `11.43 ms +- 0.00` | `11.00 ms +- 0.00` | `12.72 ms +- 0.00` | `2.17 ms +- 0.00` |
| `olap_with_where_lower_projection` | `4.55 ms +- 0.00` | `3.70 ms +- 0.00` | - | `3.53 ms +- 0.00` | `4.97 ms +- 0.00` | `2.36 ms +- 0.00` | `4.06 ms +- 0.00` | `22.00 ms +- 0.00` | `3.74 ms +- 0.00` | `3.54 ms +- 0.00` |
