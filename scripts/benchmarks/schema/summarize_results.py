"""Summarize repeated schema benchmark JSON results as Markdown tables."""

from __future__ import annotations

import argparse
import json
import os
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parents[1] / "results" / "schema"

SCHEMA_DISPLAY_NAMES = {
    "json": "generic JSON",
    "typed": "typed-property",
    "typeaware": "type-aware",
}

SCHEMA_ORDER = {"json": 0, "typed": 1, "typeaware": 2}

QUERY_WORKLOAD_GROUP = {
    "point-read": "OLTP-leaning",
    "ordered-top-k": "OLTP-leaning",
    "adjacency-read": "OLTP-leaning",
    "multi-hop-read": "OLAP-leaning",
    "relationship-aggregate": "OLAP-leaning",
    "relationship-projection": "OLAP-leaning",
}

QUERY_WORKLOAD_ORDER = {"OLTP-leaning": 0, "OLAP-leaning": 1}

SCALE_SIGNATURES = {
    "small": {
        "node_type_count": 4,
        "edge_type_count": 4,
        "nodes_per_type": 1_000,
        "edges_per_source": 3,
        "multi_hop_length": 4,
        "node_numeric_property_count": 10,
        "node_text_property_count": 2,
        "node_boolean_property_count": 2,
        "edge_numeric_property_count": 6,
        "edge_text_property_count": 2,
        "edge_boolean_property_count": 1,
    },
    "medium": {
        "node_type_count": 6,
        "edge_type_count": 8,
        "nodes_per_type": 5_000,
        "edges_per_source": 4,
        "multi_hop_length": 5,
        "node_numeric_property_count": 10,
        "node_text_property_count": 2,
        "node_boolean_property_count": 2,
        "edge_numeric_property_count": 6,
        "edge_text_property_count": 2,
        "edge_boolean_property_count": 1,
    },
    "large": {
        "node_type_count": 10,
        "edge_type_count": 10,
        "nodes_per_type": 100_000,
        "edges_per_source": 4,
        "multi_hop_length": 5,
        "node_numeric_property_count": 10,
        "node_text_property_count": 2,
        "node_boolean_property_count": 2,
        "edge_numeric_property_count": 6,
        "edge_text_property_count": 2,
        "edge_boolean_property_count": 1,
    },
}

SCALE_ORDER = {"small": 0, "medium": 1, "large": 2, "custom": 3}


@dataclass(frozen=True, slots=True)
class RunRecord:
    path: Path
    payload: dict[str, Any]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan schema benchmark JSON results, group repeated runs by config, "
            "and emit Markdown summary tables with mean and standard deviation."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        default=[DEFAULT_RESULTS_DIR],
        help="JSON files or directories to scan. Defaults to results/schema.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional Markdown output path. Defaults to stdout.",
    )
    parser.add_argument(
        "--include-queries",
        action="store_true",
        default=True,
        help=(
            "Include per-query mean tables in addition to setup tables "
            "(default: enabled)."
        ),
    )
    parser.add_argument(
        "--no-queries",
        dest="include_queries",
        action="store_false",
        help="Skip per-query tables and emit only setup summaries.",
    )
    return parser.parse_args()


def _discover_json_files(inputs: list[Path]) -> list[Path]:
    discovered: set[Path] = set()
    for input_path in inputs:
        if input_path.is_dir():
            discovered.update(
                path for path in input_path.rglob("*.json") if path.is_file()
            )
        elif input_path.is_file() and input_path.suffix == ".json":
            discovered.add(input_path)
    return sorted(discovered)


def _load_completed_runs(paths: list[Path]) -> tuple[list[RunRecord], list[Path]]:
    completed: list[RunRecord] = []
    skipped: list[Path] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("run_status", "completed") != "completed":
            skipped.append(path)
            continue
        completed.append(RunRecord(path=path, payload=payload))
    return completed, skipped


def _normalize_for_key(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(
            sorted((key, _normalize_for_key(item)) for key, item in value.items())
        )
    if isinstance(value, list):
        return tuple(_normalize_for_key(item) for item in value)
    return value


def _config_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload.get("benchmark_entrypoint"),
        _normalize_for_key(payload.get("scale", {})),
        _normalize_for_key(payload.get("controls", {})),
        tuple(sorted(payload.get("schemas", {}).keys())),
        payload.get("environment", {}).get("sqlite"),
    )


def _group_runs(records: list[RunRecord]) -> list[list[RunRecord]]:
    grouped: dict[tuple[Any, ...], list[RunRecord]] = defaultdict(list)
    for record in records:
        grouped[_config_key(record.payload)].append(record)
    return sorted(
        (sorted(group, key=lambda item: item.path.name) for group in grouped.values()),
        key=lambda group: (
            SCALE_ORDER.get(_infer_scale_name(group[0].payload.get("scale", {})), 99),
            group[0].path.name,
        ),
    )


def _infer_scale_name(scale: dict[str, Any]) -> str:
    for name, signature in SCALE_SIGNATURES.items():
        if all(scale.get(key) == value for key, value in signature.items()):
            return name
    return "custom"


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def _format_stat(values: list[float], *, unit: str) -> str:
    mean_value, std_value = _mean_std(values)
    return f"{mean_value:.2f} {unit} +- {std_value:.2f}"


def _stat_cell(values: list[float], *, unit: str) -> str:
    return f"`{_format_stat(values, unit=unit)}`"


def _results_dir_label(paths: list[Path]) -> str:
    common_path = Path(os.path.commonpath([str(path) for path in paths]))
    try:
        return str(common_path.relative_to(REPO_ROOT))
    except ValueError:
        return str(common_path)


def _sorted_schema_names(group: list[RunRecord]) -> list[str]:
    schema_names = list(group[0].payload.get("schemas", {}).keys())
    return sorted(schema_names, key=lambda name: SCHEMA_ORDER.get(name, 99))


def _pooled_mean_ms(suite: dict[str, Any]) -> float:
    pooled = suite["pooled_execute"]
    if "mean_ms" in pooled:
        return float(pooled["mean_ms"])
    return float(pooled["mean_ns"]) / 1_000_000.0


def _pooled_metric_ms(suite: dict[str, Any], metric: str) -> float:
    pooled = suite["pooled_execute"]
    return float(pooled[f"{metric}_ms"])


def _query_metric_ms(query_result: dict[str, Any], metric: str) -> float:
    return float(query_result["execute"][f"{metric}_ms"])


def _query_records(group: list[RunRecord]) -> list[dict[str, str]]:
    schema_names = _sorted_schema_names(group)
    first_schema_queries = group[0].payload["schemas"][schema_names[0]]["queries"]
    records = [
        {
            "name": str(query_result["name"]),
            "category": str(query_result["category"]),
            "workload": QUERY_WORKLOAD_GROUP.get(
                str(query_result["category"]),
                "Other",
            ),
        }
        for query_result in first_schema_queries
    ]
    return sorted(
        records,
        key=lambda item: (
            QUERY_WORKLOAD_ORDER.get(item["workload"], 99),
            item["name"],
        ),
    )


def _setup_table(group: list[RunRecord]) -> list[str]:
    header_cells = [
        "Schema",
        "Connect",
        "DDL",
        "Ingest",
        "Index",
        "Analyze",
        "RSS Connect",
        "RSS DDL",
        "RSS Ingest",
        "RSS Index",
        "RSS Analyze",
        "Size",
        "Pooled Mean",
        "Pooled p50",
        "Pooled p95",
        "Pooled p99",
    ]
    separator_cells = ["---"] + (["---:"] * (len(header_cells) - 1))
    lines = [
        "| " + " | ".join(header_cells) + " |",
        "| " + " | ".join(separator_cells) + " |",
    ]
    for schema_name in _sorted_schema_names(group):
        schema_runs = [record.payload["schemas"][schema_name] for record in group]
        connect_ms = [suite["setup"]["connect_ms"] for suite in schema_runs]
        schema_ms = [suite["setup"]["schema_ms"] for suite in schema_runs]
        ingest_ms = [suite["setup"]["ingest_ms"] for suite in schema_runs]
        index_ms = [suite["setup"]["index_ms"] for suite in schema_runs]
        analyze_ms = [suite["setup"]["analyze_ms"] for suite in schema_runs]
        rss_connect = [suite["setup"]["rss_mib"]["connect"] for suite in schema_runs]
        rss_schema = [suite["setup"]["rss_mib"]["schema"] for suite in schema_runs]
        rss_ingest = [suite["setup"]["rss_mib"]["ingest"] for suite in schema_runs]
        rss_index = [suite["setup"]["rss_mib"]["index"] for suite in schema_runs]
        rss_analyze = [suite["setup"]["rss_mib"]["analyze"] for suite in schema_runs]
        size_mb = [suite["setup"]["database_size_mb"] for suite in schema_runs]
        pooled_mean_ms = [_pooled_mean_ms(suite) for suite in schema_runs]
        pooled_p50_ms = [_pooled_metric_ms(suite, "p50") for suite in schema_runs]
        pooled_p95_ms = [_pooled_metric_ms(suite, "p95") for suite in schema_runs]
        pooled_p99_ms = [_pooled_metric_ms(suite, "p99") for suite in schema_runs]
        lines.append(
            "| "
            + " | ".join(
                [
                    SCHEMA_DISPLAY_NAMES.get(schema_name, schema_name),
                    _stat_cell(connect_ms, unit="ms"),
                    _stat_cell(schema_ms, unit="ms"),
                    _stat_cell(ingest_ms, unit="ms"),
                    _stat_cell(index_ms, unit="ms"),
                    _stat_cell(analyze_ms, unit="ms"),
                    _stat_cell(rss_connect, unit="MiB"),
                    _stat_cell(rss_schema, unit="MiB"),
                    _stat_cell(rss_ingest, unit="MiB"),
                    _stat_cell(rss_index, unit="MiB"),
                    _stat_cell(rss_analyze, unit="MiB"),
                    _stat_cell(size_mb, unit="MiB"),
                    _stat_cell(pooled_mean_ms, unit="ms"),
                    _stat_cell(pooled_p50_ms, unit="ms"),
                    _stat_cell(pooled_p95_ms, unit="ms"),
                    _stat_cell(pooled_p99_ms, unit="ms"),
                ]
            )
            + " |"
        )
    return lines


def _query_table(group: list[RunRecord], *, metric: str, workload: str) -> list[str]:
    schema_names = _sorted_schema_names(group)
    query_records = [
        query_record
        for query_record in _query_records(group)
        if query_record["workload"] == workload
    ]
    separator_cells = ["---", "---"] + (["---:"] * len(schema_names))
    header = [
        "Query",
        "Category",
        *[SCHEMA_DISPLAY_NAMES.get(name, name) for name in schema_names],
    ]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(separator_cells) + " |",
    ]
    for query_record in query_records:
        row = [f"`{query_record['name']}`", f"`{query_record['category']}`"]
        for schema_name in schema_names:
            values: list[float] = []
            for record in group:
                for query_result in record.payload["schemas"][schema_name]["queries"]:
                    if query_result["name"] == query_record["name"]:
                        values.append(_query_metric_ms(query_result, metric))
                        break
            row.append(_stat_cell(values, unit="ms"))
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _render_group(group: list[RunRecord], *, include_queries: bool) -> list[str]:
    payload = group[0].payload
    controls = payload.get("controls", {})
    scale_name = _infer_scale_name(payload.get("scale", {}))
    selected_schemas = controls.get("schemas") or _sorted_schema_names(group)
    lines = [
        f"## {scale_name.title()} schema dataset",
        "",
        f"Runs: `{len(group)}`",
        "",
        "Controls:",
        f"- iterations: `{controls.get('iterations', 'unknown')}`",
        f"- warmup: `{controls.get('warmup', 'unknown')}`",
        f"- batch size: `{controls.get('batch_size', 'unknown')}`",
        f"- schemas: `{'`, `'.join(selected_schemas)}`",
        "",
        "Files:",
    ]
    lines.extend(f"- `{path.name}`" for path in (record.path for record in group))
    lines.extend(["", "### Setup summary", ""])
    lines.extend(_setup_table(group))
    if include_queries:
        for metric in ("mean", "p50", "p95", "p99"):
            lines.extend(["", f"### Query {metric} summary", ""])
            for workload in ("OLTP-leaning", "OLAP-leaning"):
                lines.extend(["", f"#### {workload} query {metric}", ""])
                lines.extend(_query_table(group, metric=metric, workload=workload))
    return lines


def _render_markdown(
    groups: list[list[RunRecord]],
    *,
    include_queries: bool,
    results_label: str,
    skipped: list[Path],
) -> str:
    lines = [
        "# Schema benchmark repeated-run summary",
        "",
        f"Inputs: `{results_label}`",
    ]
    if skipped:
        lines.append(f"Skipped incomplete files: `{len(skipped)}`")
    lines.append("")
    if not groups:
        lines.append("No completed schema benchmark JSON files found.")
        return "\n".join(lines) + "\n"
    for index, group in enumerate(groups):
        if index:
            lines.append("")
        lines.extend(_render_group(group, include_queries=include_queries))
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    paths = _discover_json_files(args.inputs)
    completed, skipped = _load_completed_runs(paths)
    groups = _group_runs(completed)
    results_label = _results_dir_label(paths) if paths else str(DEFAULT_RESULTS_DIR)
    markdown = _render_markdown(
        groups,
        include_queries=args.include_queries,
        results_label=results_label,
        skipped=skipped,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
