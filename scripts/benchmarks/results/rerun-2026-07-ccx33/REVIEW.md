# CypherGlot re-run campaign — consolidated review package (final)

**Generated 2026-07-15** (supersedes the 2026-07-14 draft). Compiler:
`v0.1.1rc2` (73a0771; sqlglot 30.12.0). Engines at their latest pinned
versions — arcadedb-embedded 26.7.2, ladybug 0.18.1, duckdb 1.5.4,
pyturso 0.6.1, psycopg2 2.9.12, clickhouse-connect 1.4.1, neo4j image
5.26.28 — and each engine is reported at that single version (no
version-comparison narratives; superseded-version measurements ship
artifact-only). Fleet: Hetzner CCX33, one repeat per box (5 primary boxes
+ single-mission top-up boxes to complete n=5), local NVMe, docker images
digest-pinned, solo 1-worker regime at large. Statistic: **median
[min–max] over repeats** (Raasveldt et al. DBTest'18; Hoefler & Belli
SC'15); mean±std in the appendix file. Cells with a sample count other
than 5 are annotated `(n=k)`.

Files here:
- `pooled_tables_rerun.md` — all five analysis views, median [min–max]
- `pooled_tables_rerun_mean_appendix.md` — same views, mean±std
- `pooled_raw_rerun.csv` — every pooled sample with repeat identity
- Raw JSONs: `~/w3-rerun/box{1..5},topup{1..6}` + `mini:/mnt/hdd8tb/cypherglot/rerun-campaign/`

## Coverage: n=5 (or better) everywhere except the SF10 capacity tier

| Block | Status |
|---|---|
| small/synthetic-small (9 engines × modes) | ✅ n=5 |
| medium/synthetic-medium + medium/snb-sf1 (8 engines × modes) | ✅ n=5 |
| **large/synthetic-large (6 engines × modes)** | ✅ **n=5** (sqlite n=6) — completed by the top-up fleet |
| vendor-sizing (pg/neo4j/age × modes × 2 datasets) | ✅ n=5 |
| ladybug 0.18.1 (small/medium/snb-sf1) | ✅ n=5 |
| large/snb-sf10 | clickhouse-indexed **n=5**; neo4j-unindexed n=2 full + 1 timeout-partial; all other engines infeasible (capacity findings) |
| Compiler microbenchmark (fleet silicon, cache-off) | ✅ box1, fixed harness |
| Schema microbenchmark | ✅ 3 repeats × 2 boxes |

## Residuals & capacity findings (all documented, none silent)

- **SNB SF10 is a feasibility story**: turso >165 GB partial and still
  ingesting (both modes); postgres/sqlite/age fail on query-phase temp
  spill or ingest pressure; ladybug OOM at ingest; neo4j completed on
  2 of several attempts (plus one 6-hour-job-timeout partial with 8/16
  OLAP queries measured). Report as the outcome-accounting table.
- **postgresql-indexed @ synthetic-large is capacity-borderline**: its
  OLAP temp spill exceeded a 225G box in 2 of 7 attempts even solo;
  the 5 reported samples are clean completions.
- **Query-universe caveat (table footnote required)**: pooled suite-level
  p50 mixes per-run query coverage when runs differ in which slow queries
  completed. Known cells: pg-indexed@large OLAP (one sample ran all 19
  OLAP queries incl. a 239 s VL rollup; four samples recorded 14 —
  the VL-family gap needs a harness investigation note), pg-unindexed@
  large OLAP (same shape), duckdb medium/snb-sf1 OLAP, sqlite medium
  OLAP indexed, clickhouse vendor snb OLAP. Medians are robust to this;
  min–max exposes it honestly. RECOMMENDATION for the paper: use
  per-query tables for these cells, or pool only queries present in all
  samples of a cell (decide at writing time).
- db-size is unavailable for server engines (in-container volumes);
  scope the size column to embedded engines or report ingest time only.
- Setup rows pool indexed+unindexed ingest samples (n=10/20 annotations).

## Headline numbers

- **Crossover**: embedded compile targets (sqlite/duckdb) dominate OLTP at
  every scale; column stores take OLAP as scale grows; the compiled-
  Cypher-on-relational approach brackets native neo4j from both sides in
  most cells (full tables in pooled_tables_rerun.md).
- **Index-removal sensitivity**: OLTP ratios 1.2×–6500×; OLAP ≈1.0×
  everywhere (scan-shaped corpus OLAP).
- **VL lowering ablation**: rcte wins up to ~50× on reachability shapes,
  loses up to ~9× on grouped-rollup shapes — both strategies needed;
  tight ranges at n=5.
- **Vendor-sizing block**: vendor memory config makes neo4j OLAP ~1.6×
  faster on SNB-SF1 but only ~7% on synthetic-medium; postgres unchanged;
  age mixed. Defaults cost some engines real performance,
  workload-dependently (appendix figure + Discussion paragraph).
- **Ladybug (0.18.1)**: medium-scale engine; small/medium/snb-sf1 rows at
  n=5; large-scale ingest OOM on a 30G host (documented limit; upstream
  report candidate — user files).

## Upstream candidates (user files)

1. ladybug 0.18.1 ingest OOM at ~10M nodes/78M edges on 30G host (log:
   `box1/ladybug-0181/synthetic-large/session/20260712T170237Z/logs/`).
2. turso @ SNB SF10 storage blowup (>165 GB vs sqlite's ~60–100 GB).
3. (context, likely config) postgres SNB SF10 query temp spill >45 GB.

## Harness backlog (post-deadline)

- SNB fixture loader requires the raw `initial_snapshot/dynamic` dir even
  on cache hit (topology_ldbc_snb.py:444) — check cache first.
- Job kill paths can leak the engine container (matrix worker should
  docker-rm on kill).
- Investigate the pg-indexed VL-query recording gap (14 vs 19 OLAP
  queries per run, no timeout/fail recorded for the absent five).

## Excluded from paper tables (ship artifact-only)

- Fallback campaign (July 8–12, pre-rc2 compiler): `~/w3-fleet-fallback/`.
- Ladybug 0.18.0 main-matrix block (superseded version).
- Guard-killed partial attempts superseded by clean retries (quarantined
  under `out-excluded/`, e.g. topup1 pg-indexed).
- Snapshot-inherited fallback JSONs in the raw topup5 archive (the clean
  root `topup5-clean` holds its one genuine result).

## Cost

Campaign total ≈ €345 (fallback + re-run + supplements + top-ups). All
servers deleted 2026-07-15 ~12:00 UTC; snapshot 406523883 (€0.60/mo)
retained until paper data verified.
