# CypherGlot re-run campaign — consolidated review package

**Generated 2026-07-14.** Compiler: `v0.1.1rc2` (73a0771; sqlglot 30.12.0,
arcadedb-embedded 26.7.2 stable, ladybug 0.18.0 in the main matrix /
0.18.1 in the supplement). Fleet: 5× Hetzner CCX33 (cg-rerun-1..5), one
repeat per box, local NVMe, docker images digest-pinned. Statistic:
**median [min–max] over the 5 box repeats** (primary; Le Boudec/Hoefler),
mean±std in the appendix file. Cells with fewer than 5 samples are
annotated `(n=k)` in every table.

Files here:
- `pooled_tables_rerun.md` — all five analysis views, median [min–max]
- `pooled_tables_rerun_mean_appendix.md` — same views, mean±std (appendix/artifact)
- `pooled_raw_rerun.csv` — every pooled sample with repeat (box) identity
- Raw JSONs: `~/w3-rerun/box{1..5}/` and `mini:/mnt/hdd8tb/cypherglot/rerun-campaign/`

## Coverage: what has full n=5

| Block | Status |
|---|---|
| small/synthetic-small (9 engines × modes) | ✅ n=5 |
| medium/synthetic-medium (8 engines × modes) | ✅ n=5 |
| medium/snb-sf1 (8 engines × modes) | ✅ n=5 |
| vendor-sizing (pg/neo4j/age × modes × 2 datasets) | ✅ n=5 |
| ladybug 0.18.1 supplement (small/medium/snb-sf1) | ✅ n=5 |
| ladybug 0.18.0 main-matrix medium synthetic | ✅ n=5 |
| Compiler microbenchmark (fleet silicon, cache-off) | ✅ 1 box (box1), fixed harness |
| Schema microbenchmark | ✅ boxes 1+3, 3 repeats each |

## Coverage residuals (all documented, none silent)

- **large/synthetic-large**: per-cell n=2–5. Cause: disk-capacity guard
  kills during the co-run first passes; retry passes + wave-2 (raw-CSV
  deletion) recovered most cells. Worst cells: postgresql-indexed n=2,
  neo4j-unindexed n=2. sqlite-indexed n=3–4 (box4's final attempt died at
  query 18/19 on temp-spill; partial per-query data retained).
- **large/snb-sf10**: thin by design reality — clickhouse-indexed n=3,
  neo4j-unindexed n=1; **turso (both modes) infeasible on 240G boxes**
  (>165 GB partial DB and still ingesting = capacity finding, reported as
  such); postgres/sqlite/age SNB-SF10 cells failed on query-phase temp
  spill or ingest OOM across boxes. The SF10 tier is reported as a
  capacity/outcome story (Table: outcome accounting), not a latency table.
- **ladybug-medium/ladybug-snb-sf1 (0.18.0 pin): n=4** — box5's sf1 stage
  was hit by the provisioning error; its patch chain re-ran the vendor
  variants but not this cell. The 0.18.1 supplement has full n=5 on the
  same cell.
- **ladybug large**: 0.18.0 = ingest too slow to attempt (pre-fix);
  0.18.1 probe = ingest OOM-killed at 10M/78M edges on the 30G host
  (n=1 probe, disclosed as a finding; NOT a latency row).

## Headline numbers (from pooled_tables_rerun.md)

**Ladybug #645 fix verified, with a twist (0.18.0 → 0.18.1, medium, n=5):**
- Ingest: synthetic 4415s → 522s (**8.5× faster**); snb-sf1 4312s → 644s (6.7×)
- On-disk: synthetic 967 MiB → 1747 MiB (**1.8× larger**); snb 2460 → 8810 MiB (**3.6× larger**)
- Query p50 (snb-sf1): OLTP 33 → 59 ms, OLAP 261 → 342 ms (**~1.3–1.8× slower**)
- Interpretation candidate: the ALP import fix trades compression/layout
  for ingest speed; larger footprint also explains the new large-scale
  ingest OOM. Strong Discussion material + second upstream report.

**Vendor-sizing block (defaults vs vendor-recommended memory, n=5):**
- neo4j OLAP @ snb-sf1: 3063 → 1925 ms indexed (**1.6× faster**), 2774 → 1780 unindexed
- neo4j OLAP @ synthetic-medium: only ~7% faster (13715 → 12789)
- postgresql: ≈ unchanged everywhere (defaults already adequate for this workload/host)
- age: mixed — snb OLAP unindexed 3300 → 2649 (1.25× faster), synthetic OLAP ~14% *slower*
- Paper angle: "archaic defaults" cost neo4j real performance on the
  SNB-shaped workload but the effect is workload-dependent, not uniform.

**Index-removal sensitivity** (unchanged story, now with ranges): OLTP
ratios span 1.2× (small) to 6532× (sqlite @ large); OLAP is flat (≈1.0×)
almost everywhere — the corpus's OLAP queries are scan-shaped.

**VL lowering ablation**: rcte wins up to 50× on reachability-shaped
queries (postgres 0.02×, duckdb 0.06×, sqlite 0.03–0.09×) but loses up to
8.9× on grouped-rollup shapes — the both-strategies-needed story holds
with tight ranges at n=5.

## Data-quality caveats for table footnotes

1. Pooled suite p50 across repeats can look bimodal when one box lost its
   slowest queries to timeouts (pooled p50 then covers fewer, faster
   queries). Visible at: duckdb medium/snb-sf1 OLAP (max 14–16s vs median
   ~95ms), sqlite medium OLAP indexed (min 5.8s vs median 18.3s),
   clickhouse vendor snb OLAP (min 376ms). Cross-reference the outcome
   table; consider per-query tables for these cells in the paper.
2. db-size is 0.00/missing for server engines (postgres/clickhouse
   in-container volumes; age/neo4j not sampled) — report ingest time
   only, or scope the size column to embedded engines.
3. Setup rows pool indexed+unindexed ingest samples (hence n=10/20);
   split if the paper wants per-mode ingest.

## Findings bank (upstream candidates, user files)

1. **ladybug 0.18.1 ingest OOM** at ~10M nodes/78M edges on 30G host
   (kill -9 during ingest; log archived at
   `box1/ladybug-0181/synthetic-large/session/20260712T170237Z/logs/`),
   plus the storage-blowup observation above.
2. **turso @ SNB SF10 storage blowup**: >165 GB partial and unfinished vs
   sqlite ≈60–100 GB for identical rows (both index modes) — banked.
3. postgres SNB query-phase temp spill >45 GB at SF10 (context for the
   capacity table, likely config-tunable, disclose rather than file).

## Fallback campaign data

The July 8–12 fallback campaign (531 JSONs, pre-rc2 compiler) stays OUT of
paper tables (different compiler version); ships labeled in the artifact
(`~/w3-fleet-fallback/`, mirrored on mini).

## Cost

Campaign total ≈ €330 (fallback + re-run + supplements). All servers
deleted 2026-07-14 02:25 UTC; snapshot 406523883 (€0.60/mo) retained
until paper data verified.
