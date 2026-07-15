"""Aggregate W3 matrix results into the paper's analysis views.

Reads every runtime-benchmark JSON under one or more result directories and
prints Markdown tables:

1. Engine x workload latency (pooled execute p50/mean over repeats), per
   index mode -- the crossover view (F2/T5).
2. Index-removal sensitivity: unindexed/indexed ratio per engine per workload
   (F3/T6).
3. Variable-length lowering ablation: unroll vs recursive CTE per engine (F6).
4. Outcome accounting: pass/timeout/fail counts per engine (timeouts are
   data).
5. Setup costs: ingest time and on-disk size per engine (T7).

Repeats are aggregated as median [min-max] over the per-run pooled values
(the primary statistic; pass --stat mean for the appendix's mean +/- std
view). Native engines report execute only; compile targets also carry the
compile stage (reported separately, per the steady-state methodology).

Usage:
  python -m scripts.benchmarks.runtime.analyze_w3 RESULTS_DIR [RESULTS_DIR...]
      [--label-from-parent] [--csv OUT.csv]
  python -m scripts.benchmarks.runtime.analyze_w3 BOX_DIR [BOX_DIR...]
      --repeat-roots [--stat median|mean] [--csv OUT.csv]

With --repeat-roots each root is one repeat (a fleet box archive): files are
labelled by their stage path under the box (e.g. medium/synthetic-medium,
vendor-sizing/snb-sf1), smoke/micro/tmp trees are skipped, and when a cell
was attempted multiple times on one box (retry passes) only the newest
attempt is pooled — earlier attempts of the same variant are superseded,
not extra samples.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

_EXCLUDED_STAGE_PARTS = {"smoke", "micro", "tmp", "rootfiles", "all"}
_TIMESTAMP_SUFFIX = re.compile(r"-\d{8}T\d{6}Z$")


def _iter_result_files(roots: list[Path]) -> Iterator[Path]:
    for root in roots:
        if root.is_file() and root.suffix == ".json":
            yield root
            continue
        yield from sorted(root.rglob("*.json"))


def _iter_repeat_files(roots: list[Path]) -> Iterator[tuple[Path, str, str]]:
    """Yield (path, repeat_id, stage_label) with per-cell retry dedup.

    A cell is (repeat, stage_label, variant prefix); among multiple attempt
    files (timestamp-suffixed) only the newest is yielded.
    """
    for root in roots:
        repeat = root.name
        newest: dict[tuple[str, str], Path] = {}
        for path in sorted(root.rglob("*.json")):
            rel = path.relative_to(root)
            parts = rel.parts
            # results live under <stage...>/out/<file>.json
            if len(parts) < 3 or parts[-2] != "out":
                continue
            stage_parts = parts[:-2]
            if _EXCLUDED_STAGE_PARTS.intersection(stage_parts):
                continue
            label = "/".join(stage_parts)
            prefix = _TIMESTAMP_SUFFIX.sub("", path.stem)
            key = (label, prefix)
            held = newest.get(key)
            if held is None or path.name > held.name:
                newest[key] = path
        for (label, _prefix), path in sorted(newest.items()):
            yield path, repeat, label


def _pooled(stage: dict[str, Any] | None, key: str) -> float | None:
    if not isinstance(stage, dict):
        return None
    return stage.get(key) or stage.get(f"mean_of_{key}")


def _mean_std(values: list[float]) -> tuple[float, float]:
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return mean, math.sqrt(var)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


# Set by main() from --stat; median is the paper's primary statistic.
_STAT_MODE = "median"
_EXPECTED_N: int | None = None


def _center(values: list[float]) -> float:
    return _median(values) if _STAT_MODE == "median" else _mean_std(values)[0]


def _fmt(value: float | None, std: float | None = None) -> str:
    if value is None:
        return "-"
    if std is not None and std > 0:
        return f"{value:.2f}±{std:.2f}"
    return f"{value:.2f}"


def _fmt_agg(values: list[float] | None) -> str:
    """Primary cell format: median [min-max]; mean±std in --stat mean mode.

    Cells whose sample count differs from --expected-n are annotated (n=k)
    so partial coverage is visible in the table itself.
    """
    if not values:
        return "-"
    if _STAT_MODE == "mean":
        cell = _fmt(*_mean_std(values))
    elif len(values) == 1:
        cell = f"{values[0]:.2f}"
    else:
        cell = f"{_median(values):.2f} [{min(values):.2f}-{max(values):.2f}]"
    if _EXPECTED_N is not None and len(values) != _EXPECTED_N:
        cell += f" (n={len(values)})"
    return cell


class W3Aggregate:
    """Collects per-(label, engine, index_mode, workload) samples."""

    def __init__(self) -> None:
        # samples[(label, engine, mode, workload)] = list of pooled p50 ms
        self.execute: dict[tuple, list[float]] = defaultdict(list)
        self.compile: dict[tuple, list[float]] = defaultdict(list)
        self.counts: dict[tuple, dict[str, int]] = defaultdict(
            lambda: {"pass": 0, "timeout": 0, "fail": 0}
        )
        self.ingest_s: dict[tuple, list[float]] = defaultdict(list)
        self.db_mib: dict[tuple, list[float]] = defaultdict(list)
        # per-query samples for the ablation: (label, engine, mode, query) -> ms
        self.query_execute: dict[tuple, list[float]] = defaultdict(list)
        # raw rows for the CSV: (label, engine, mode, workload, repeat, p50)
        self.samples: list[tuple[str, str, str, str, str, float]] = []
        # per-query rows: (label, engine, mode, workload, query, repeat, p50)
        self.query_samples: list[
            tuple[str, str, str, str, str, str, float]] = []

    def add_file(self, path: Path, label: str, repeat: str = "-") -> None:
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        results = payload.get("results")
        if not isinstance(results, dict):
            return
        workloads = results.get("workloads")
        if not isinstance(workloads, dict):
            return
        for workload, suites in workloads.items():
            if not isinstance(suites, dict):
                continue
            for suite_name, suite in suites.items():
                if suite_name == "description" or not isinstance(suite, dict):
                    continue
                backend = suite.get("backend") or suite_name.rsplit("_", 1)[0]
                mode = suite.get("index_mode") or "unindexed"
                key = (label, backend, mode, workload)
                p50 = _pooled(suite.get("execute"), "p50_ms")
                if p50 is not None:
                    self.execute[key].append(p50)
                    self.samples.append(
                        (label, backend, mode, workload, repeat, p50)
                    )
                c50 = _pooled(suite.get("compile"), "p50_ms")
                if c50 is not None:
                    self.compile[key].append(c50)
                counts = self.counts[key]
                counts["pass"] += int(suite.get("pass_count") or 0)
                counts["timeout"] += int(suite.get("timeout_count") or 0)
                counts["fail"] += int(suite.get("fail_count") or 0)
                setup = suite.get("setup") or {}
                if isinstance(setup.get("ingest_ms"), (int, float)):
                    self.ingest_s[(label, backend)].append(
                        setup["ingest_ms"] / 1000.0
                    )
                storage = suite.get("storage") or {}
                if isinstance(storage.get("db_size_mib"), (int, float)):
                    self.db_mib[(label, backend)].append(storage["db_size_mib"])
                for query in suite.get("queries") or []:
                    if not isinstance(query, dict):
                        continue
                    q50 = _pooled(query.get("execute"), "p50_ms")
                    if q50 is not None:
                        self.query_execute[
                            (label, backend, mode, query.get("name"))
                        ].append(q50)
                        self.query_samples.append(
                            (label, backend, mode, workload,
                             query.get("name"), repeat, q50)
                        )


def _print_crossover(agg: W3Aggregate, labels: list[str]) -> None:
    stat_desc = (
        "median [min-max] over repeats"
        if _STAT_MODE == "median"
        else "mean±std over repeats"
    )
    print(f"\n## Engine x workload (pooled execute p50 ms, {stat_desc})\n")
    for label in labels:
        engines = sorted(
            {k[1] for k in agg.execute if k[0] == label},
            key=lambda e: _center(
                agg.execute.get(
                    (label, e, "indexed", "olap"),
                    agg.execute.get((label, e, "unindexed", "olap"), [9e9]),
                )
            ),
        )
        print(f"### {label}\n")
        print("| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |")
        print("|---|---|---|---|---|")
        for engine in engines:
            cells = []
            for workload in ("oltp", "olap"):
                for mode in ("indexed", "unindexed"):
                    values = agg.execute.get((label, engine, mode, workload))
                    cells.append(_fmt_agg(values))
            print(f"| {engine} | {cells[0]} | {cells[1]} | {cells[2]} | {cells[3]} |")
        print()


def _print_index_sensitivity(agg: W3Aggregate, labels: list[str]) -> None:
    print("\n## Index-removal sensitivity (unindexed / indexed, execute p50)\n")
    print("| dataset | engine | OLTP ratio | OLAP ratio |")
    print("|---|---|---|---|")
    for label in labels:
        engines = sorted({k[1] for k in agg.execute if k[0] == label})
        for engine in engines:
            row = [label, engine]
            for workload in ("oltp", "olap"):
                idx = agg.execute.get((label, engine, "indexed", workload))
                unidx = agg.execute.get((label, engine, "unindexed", workload))
                if idx and unidx and _center(idx) > 0:
                    row.append(f"{_center(unidx) / _center(idx):.1f}x")
                else:
                    row.append("-")
            if row[2] != "-" or row[3] != "-":
                print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} |")
    print()


def _print_ablation(agg: W3Aggregate, labels: list[str]) -> None:
    print("\n## Variable-length lowering: unroll vs recursive CTE (execute p50 ms)\n")
    print("| dataset | engine | mode | query | unroll | rcte | rcte/unroll |")
    print("|---|---|---|---|---|---|---|")
    base_names = sorted({
        k[3][: -len("+rcte")]
        for k in agg.query_execute
        if k[3] and k[3].endswith("+rcte")
    })
    for label in labels:
        for (lbl, engine, mode, qname), rcte_vals in sorted(agg.query_execute.items()):
            if lbl != label or not qname or not qname.endswith("+rcte"):
                continue
            base = qname[: -len("+rcte")]
            unroll_vals = agg.query_execute.get((lbl, engine, mode, base))
            if not unroll_vals:
                continue
            u = _center(unroll_vals)
            r = _center(rcte_vals)
            ratio = f"{r / u:.2f}x" if u > 0 else "-"
            print(f"| {label} | {engine} | {mode} | {base} "
                  f"| {_fmt_agg(unroll_vals)} | {_fmt_agg(rcte_vals)} | {ratio} |")
    if not base_names:
        print("| (no rcte twins found) | | | | | | |")
    print()


def _print_outcomes(agg: W3Aggregate, labels: list[str]) -> None:
    print("\n## Outcome accounting (timeouts are data)\n")
    print("| dataset | engine | mode | workload | pass | timeout | fail |")
    print("|---|---|---|---|---|---|---|")
    for key in sorted(agg.counts):
        label, engine, mode, workload = key
        c = agg.counts[key]
        if c["timeout"] or c["fail"]:
            print(f"| {label} | {engine} | {mode} | {workload} "
                  f"| {c['pass']} | {c['timeout']} | {c['fail']} |")
    print()


def _print_setup(agg: W3Aggregate, labels: list[str]) -> None:
    stat_desc = "median [min-max]" if _STAT_MODE == "median" else "mean±std"
    print(f"\n## Ingest time and on-disk size ({stat_desc} over runs)\n")
    print("| dataset | engine | ingest s | db MiB |")
    print("|---|---|---|---|")
    for label in labels:
        engines = sorted({k[1] for k in agg.ingest_s if k[0] == label})
        for engine in engines:
            ing = agg.ingest_s.get((label, engine))
            size = agg.db_mib.get((label, engine))
            print(f"| {label} | {engine} "
                  f"| {_fmt_agg(ing)} "
                  f"| {_fmt_agg(size)} |")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument(
        "--label-from-parent",
        action="store_true",
        help="Label each file by its result dir's parent name (e.g. the stage "
        "label like synthetic-medium / snb-sf1) instead of one shared label.",
    )
    parser.add_argument("--csv", type=Path, help="Also dump raw samples as CSV.")
    parser.add_argument(
        "--per-query-csv", type=Path,
        help="Dump per-query per-repeat p50 samples as CSV (the input for "
        "median-of-per-query-medians aggregation).")
    parser.add_argument(
        "--repeat-roots",
        action="store_true",
        help="Treat each root as one repeat (a fleet box archive): label by "
        "stage path, skip smoke/micro/tmp, and pool only the newest attempt "
        "per cell (retries supersede earlier attempts).",
    )
    parser.add_argument(
        "--stat",
        choices=("median", "mean"),
        default="median",
        help="Aggregation across repeats: median [min-max] (primary, "
        "default) or mean±std (appendix view).",
    )
    parser.add_argument(
        "--expected-n",
        type=int,
        default=None,
        help="Annotate cells whose sample count differs from this (e.g. 5).",
    )
    args = parser.parse_args()

    global _STAT_MODE, _EXPECTED_N
    _STAT_MODE = args.stat
    _EXPECTED_N = args.expected_n

    agg = W3Aggregate()
    if args.repeat_roots:
        for path, repeat, label in _iter_repeat_files(args.roots):
            agg.add_file(path, label, repeat)
    else:
        for path in _iter_result_files(args.roots):
            label = path.parent.parent.name if args.label_from_parent else "all"
            agg.add_file(path, label)
    labels = sorted({k[0] for k in agg.execute}) or ["all"]

    if not agg.execute:
        print("No parseable runtime result JSONs found.", file=sys.stderr)
        return 1

    _print_crossover(agg, labels)
    _print_index_sensitivity(agg, labels)
    _print_ablation(agg, labels)
    _print_outcomes(agg, labels)
    _print_setup(agg, labels)

    if args.csv:
        with args.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                ["label", "engine", "index_mode", "workload", "repeat", "p50_ms"]
            )
            for row in sorted(agg.samples):
                writer.writerow(row)
        print(f"raw samples -> {args.csv}", file=sys.stderr)
    if args.per_query_csv:
        with args.per_query_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["label", "engine", "index_mode", "workload",
                             "query", "repeat", "p50_ms"])
            for row in sorted(agg.query_samples):
                writer.writerow(row)
        print(f"per-query samples -> {args.per_query_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
