# Re-run campaign results (July 2026, Hetzner CCX33 fleet)

Consolidated results of the paper's measurement campaign: 5 identical
Hetzner CCX33 boxes (cg-rerun-1..5), one repeat per box, local NVMe,
docker images digest-pinned, compiler frozen at `v0.1.1rc2` (73a0771).
Primary statistic: median [min–max] over the 5 repeats; cells with fewer
samples are annotated `(n=k)`.

| File | Contents |
|---|---|
| `REVIEW.md` | Coverage, residuals, headline numbers, data-quality caveats |
| `pooled_tables_rerun.md` | All analysis views, median [min–max] (paper tables) |
| `pooled_tables_rerun_mean_appendix.md` | Same views, mean±std (appendix) |
| `pooled_raw_rerun.csv` | Every pooled sample with repeat (box) identity |
| `micro/compiler_benchmark_ccx33_box1.json` | Compiler microbenchmark, solo box, cache off |
| `micro/schema/` | Schema DDL microbenchmark, 3 repeats × 2 boxes |

Regenerate the tables from the raw archives (box directories holding each
box's `/root/w3` tree; archived on the maintainer's laptop and mini):

```
python -m scripts.benchmarks.runtime.analyze_w3 BOX1 BOX2 BOX3 BOX4 BOX5 \
    --repeat-roots --expected-n 5 --csv pooled_raw_rerun.csv
```

`--repeat-roots` treats each box as one repeat, labels cells by stage path
(e.g. `medium/snb-sf1`, `vendor-sizing/synthetic-medium`), skips
smoke/micro trees, and pools only the newest attempt per cell (retry
passes supersede earlier attempts of the same variant).

The July 8–12 fallback-campaign data (pre-rc2 compiler) is excluded from
these tables by policy and ships separately in the paper artifact.

The earlier `*-Ryzen-9-7950X.md` files in the parent directory are stale
April laptop baselines kept for provenance; paper numbers come from this
directory only (see the measurement-host policy in REVIEW.md).
