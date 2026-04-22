"""Summarize repeated runtime benchmark JSON results as Markdown tables."""

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
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parents[1] / "results" / "runtime"

DISPLAY_ORDER = {
    "sqlite_indexed": 0,
    "sqlite_unindexed": 1,
    "duckdb": 2,
    "postgresql_indexed": 3,
    "postgresql_unindexed": 4,
    "neo4j_indexed": 5,
    "neo4j_unindexed": 6,
    "arcadedb_embedded_indexed": 7,
    "arcadedb_embedded_unindexed": 8,
    "ladybug_unindexed": 9,
}

WORKLOAD_ORDER = {"oltp": 0, "olap": 1}


@dataclass(frozen=True, slots=True)
class RunRecord:
    path: Path
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ConfigSummary:
    suite_name: str
    display_name: str
    scale_name: str
    graph_scale: dict[str, Any]
    controls: dict[str, Any]
    database_versions: dict[str, str]
    corpus_path: str | None
    files: tuple[Path, ...]
    repeat_count: int
    workload_summaries: dict[str, dict[str, Any]]


def _results_dir_label(paths: list[Path]) -> str:
    common_path = Path(os.path.commonpath([str(path) for path in paths]))
    try:
        return str(common_path.relative_to(REPO_ROOT))
    except ValueError:
        return str(common_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan runtime benchmark JSON results, group repeated runs by config, "
            "and emit Markdown summary tables with mean and standard deviation."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        default=[DEFAULT_RESULTS_DIR],
        help="JSON files or directories to scan. Defaults to results/runtime.",
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
            "Include per-query percentile tables in addition to suite tables "
            "(default: enabled)."
        ),
    )
    parser.add_argument(
        "--no-queries",
        dest="include_queries",
        action="store_false",
        help="Skip per-query percentile tables and emit only suite-level tables.",
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
        if payload.get("run_status") != "completed":
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


def _suite_names(payload: dict[str, Any]) -> tuple[str, ...]:
    workloads = payload.get("results", {}).get("workloads", {})
    suite_names: list[str] = []
    for workload_name in sorted(workloads):
        workload = workloads[workload_name]
        if not isinstance(workload, dict):
            continue
        for suite_name in sorted(workload):
            if suite_name == "description":
                continue
            suite_names.append(f"{workload_name}:{suite_name}")
    return tuple(suite_names)


def _config_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload.get("benchmark_entrypoint"),
        _normalize_for_key(payload.get("enabled_backends", [])),
        payload.get("corpus_path"),
        _normalize_for_key(payload.get("graph_scale", {})),
        _normalize_for_key(payload.get("workload_controls", {})),
        payload.get("index_mode"),
        payload.get("cypherglot_version"),
        _normalize_for_key(payload.get("database_versions", {})),
        _suite_names(payload),
    )


def _group_runs(records: list[RunRecord]) -> list[list[RunRecord]]:
    grouped: dict[tuple[Any, ...], list[RunRecord]] = defaultdict(list)
    for record in records:
        grouped[_config_key(record.payload)].append(record)
    return sorted(
        (sorted(group, key=lambda item: item.path.name) for group in grouped.values()),
        key=lambda group: group[0].path.name,
    )


def _infer_scale_name(graph_scale: dict[str, Any]) -> str:
    signature = {
        key: graph_scale.get(key)
        for key in (
            "node_type_count",
            "edge_type_count",
            "nodes_per_type",
            "edges_per_source",
            "edge_degree_profile",
            "node_extra_text_property_count",
            "node_extra_numeric_property_count",
            "node_extra_boolean_property_count",
            "edge_extra_text_property_count",
            "edge_extra_numeric_property_count",
            "edge_extra_boolean_property_count",
            "variable_hop_max",
            "ingest_batch_size",
        )
    }
    presets = {
        "small": {
            "node_type_count": 4,
            "edge_type_count": 4,
            "nodes_per_type": 1000,
            "edges_per_source": 3,
            "edge_degree_profile": "uniform",
            "node_extra_text_property_count": 2,
            "node_extra_numeric_property_count": 6,
            "node_extra_boolean_property_count": 2,
            "edge_extra_text_property_count": 1,
            "edge_extra_numeric_property_count": 3,
            "edge_extra_boolean_property_count": 1,
            "variable_hop_max": 2,
            "ingest_batch_size": 1000,
        },
        "medium": {
            "node_type_count": 6,
            "edge_type_count": 8,
            "nodes_per_type": 100000,
            "edges_per_source": 4,
            "edge_degree_profile": "skewed",
            "node_extra_text_property_count": 4,
            "node_extra_numeric_property_count": 10,
            "node_extra_boolean_property_count": 4,
            "edge_extra_text_property_count": 2,
            "edge_extra_numeric_property_count": 6,
            "edge_extra_boolean_property_count": 2,
            "variable_hop_max": 5,
            "ingest_batch_size": 5000,
        },
        "large": {
            "node_type_count": 10,
            "edge_type_count": 10,
            "nodes_per_type": 1000000,
            "edges_per_source": 8,
            "edge_degree_profile": "skewed",
            "node_extra_text_property_count": 8,
            "node_extra_numeric_property_count": 18,
            "node_extra_boolean_property_count": 8,
            "edge_extra_text_property_count": 4,
            "edge_extra_numeric_property_count": 10,
            "edge_extra_boolean_property_count": 4,
            "variable_hop_max": 8,
            "ingest_batch_size": 10000,
        },
    }
    for name, preset in presets.items():
        if signature == preset:
            return name
    return "custom"


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def _format_stat(values: list[float], *, unit: str = "ms") -> str:
    mean_value, std_value = _mean_std(values)
    return f"{mean_value:.2f} {unit} +- {std_value:.2f}"


def _stat_cell(values: list[float], *, unit: str = "ms") -> str:
    return f"`{_format_stat(values, unit=unit)}`"


def _suite_rows(group: list[RunRecord]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in group:
        workloads = record.payload.get("results", {}).get("workloads", {})
        for workload_name, workload in workloads.items():
            if not isinstance(workload, dict):
                continue
            for suite_name, suite in workload.items():
                if suite_name == "description":
                    continue
                rows[(workload_name, suite_name)].append(suite)
    return rows


def _query_rows(
    group: list[RunRecord],
) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    rows: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in group:
        workloads = record.payload.get("results", {}).get("workloads", {})
        for workload_name, workload in workloads.items():
            if not isinstance(workload, dict):
                continue
            for suite_name, suite in workload.items():
                if suite_name == "description":
                    continue
                for query in suite.get("queries", []):
                    rows[(workload_name, suite_name, query["name"])].append(query)
    return rows


def _table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return ""
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join("---" for _ in headers) + " |"
    body_rows = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_row, separator_row, *body_rows])


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _display_suite_name(suite_name: str) -> str:
    if suite_name.startswith("sqlite_"):
        return f"SQLite {suite_name.split('_', 1)[1].capitalize()}"
    if suite_name == "duckdb":
        return "DuckDB Unindexed"
    if suite_name.startswith("postgresql_"):
        suffix = suite_name.split("_", 1)[1]
        return f"PostgreSQL {suffix.capitalize()}"
    if suite_name.startswith("neo4j_"):
        suffix = suite_name.split("_", 1)[1]
        return f"Neo4j {suffix.capitalize()}"
    if suite_name.startswith("arcadedb_embedded_"):
        suffix = suite_name.removeprefix("arcadedb_embedded_")
        return "ArcadeDB Indexed" if suffix == "indexed" else "ArcadeDB Unindexed"
    if suite_name == "ladybug_unindexed":
        return "LadybugDB Unindexed"
    return suite_name.replace("_", " ")


def _summary_sort_key(summary: ConfigSummary) -> tuple[int, str]:
    return (DISPLAY_ORDER.get(summary.suite_name, 999), summary.display_name)


def _combined_rss(snapshot: dict[str, Any] | None) -> float | None:
    if not snapshot:
        return None
    combined = snapshot.get("combined_mib")
    if combined is not None:
        return float(combined)
    client = snapshot.get("client_mib")
    server = snapshot.get("server_mib")
    if client is None and server is None:
        return None
    return _to_float(client) + _to_float(server)


def _normalized_setup(
    payload: dict[str, Any],
    suite: dict[str, Any],
) -> dict[str, float]:
    setup = suite.get("setup", {})
    top_setup = payload.get("setup", {})
    return {
        "connect_reset_ms": _to_float(
            setup.get("connect_ms", top_setup.get("connect_ms", setup.get("reset_ms")))
        ),
        "schema_constraints_ms": _to_float(
            setup.get("schema_ms", setup.get("seed_constraints_ms"))
        ),
        "ingest_ms": _to_float(setup.get("ingest_ms")),
        "index_ms": _to_float(setup.get("index_ms")),
        "analyze_ms": _to_float(setup.get("analyze_ms"))
        + _to_float(setup.get("gav_ms"))
        + _to_float(setup.get("checkpoint_ms")),
    }


def _normalized_rss(payload: dict[str, Any], suite: dict[str, Any]) -> dict[str, float]:
    snapshots = suite.get("rss_snapshots_mib", {})
    top_connect = payload.get("setup", {}).get("connect_rss_mib")
    return {
        "connect_reset_mib": _to_float(
            _combined_rss(snapshots.get("after_connect"))
            or _combined_rss(top_connect)
            or _combined_rss(snapshots.get("after_reset"))
        ),
        "schema_constraints_mib": _to_float(
            _combined_rss(snapshots.get("after_schema"))
            or _combined_rss(snapshots.get("after_seed_constraints"))
        ),
        "ingest_mib": _to_float(_combined_rss(snapshots.get("after_ingest"))),
        "index_mib": _to_float(_combined_rss(snapshots.get("after_index"))),
        "analyze_mib": _to_float(
            _combined_rss(snapshots.get("after_analyze"))
            or _combined_rss(snapshots.get("after_gav"))
            or _combined_rss(snapshots.get("after_checkpoint"))
        ),
        "suite_complete_mib": _to_float(
            _combined_rss(snapshots.get("suite_complete"))
        ),
    }


def _query_metric_lists(
    suites: list[dict[str, Any]],
) -> dict[str, dict[str, list[float]]]:
    metrics: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"p50_ms": [], "p95_ms": [], "p99_ms": []}
    )
    for suite in suites:
        for query in suite.get("queries", []):
            metrics[query["name"]]["p50_ms"].append(query["end_to_end"]["p50_ms"])
            metrics[query["name"]]["p95_ms"].append(query["end_to_end"]["p95_ms"])
            metrics[query["name"]]["p99_ms"].append(query["end_to_end"]["p99_ms"])
    return metrics


def _build_config_summary(group: list[RunRecord]) -> ConfigSummary:
    payload = group[0].payload
    grouped_suites = _suite_rows(group)
    suite_names = sorted({suite_name for _, suite_name in grouped_suites})
    if len(suite_names) != 1:
        raise ValueError(
            f"Expected one suite name per config group, got {suite_names}."
        )
    suite_name = suite_names[0]
    workload_summaries: dict[str, dict[str, Any]] = {}
    for workload_name, grouped_suite_name in sorted(grouped_suites):
        suites = grouped_suites[(workload_name, grouped_suite_name)]
        setup_rows = [
            _normalized_setup(record.payload, suite)
            for record, suite in zip(group, suites, strict=False)
        ]
        rss_rows = [
            _normalized_rss(record.payload, suite)
            for record, suite in zip(group, suites, strict=False)
        ]
        workload_summaries[workload_name] = {
            "end_to_end": {
                "p50_ms": [suite["end_to_end"]["mean_of_p50_ms"] for suite in suites],
                "p95_ms": [suite["end_to_end"]["mean_of_p95_ms"] for suite in suites],
                "p99_ms": [suite["end_to_end"]["mean_of_p99_ms"] for suite in suites],
            },
            "setup": {
                key: [row[key] for row in setup_rows]
                for key in (
                    "connect_reset_ms",
                    "schema_constraints_ms",
                    "ingest_ms",
                    "index_ms",
                    "analyze_ms",
                )
            },
            "rss": {
                key: [row[key] for row in rss_rows]
                for key in (
                    "connect_reset_mib",
                    "schema_constraints_mib",
                    "ingest_mib",
                    "index_mib",
                    "analyze_mib",
                    "suite_complete_mib",
                )
            },
            "queries": _query_metric_lists(suites),
        }
    return ConfigSummary(
        suite_name=suite_name,
        display_name=_display_suite_name(suite_name),
        scale_name=_infer_scale_name(payload.get("graph_scale", {})),
        graph_scale=payload.get("graph_scale", {}),
        controls=payload.get("workload_controls", {}),
        database_versions=dict(payload.get("database_versions", {})),
        corpus_path=payload.get("corpus_path"),
        files=tuple(record.path for record in group),
        repeat_count=len(group),
        workload_summaries=workload_summaries,
    )


def _campaign_key(summary: ConfigSummary) -> tuple[Any, ...]:
    return (
        summary.scale_name,
        _normalize_for_key(summary.graph_scale),
        _normalize_for_key(summary.controls),
        summary.corpus_path,
    )


def _campaigns(groups: list[list[RunRecord]]) -> list[list[ConfigSummary]]:
    by_campaign: dict[tuple[Any, ...], list[ConfigSummary]] = defaultdict(list)
    for group in groups:
        summary = _build_config_summary(group)
        by_campaign[_campaign_key(summary)].append(summary)
    return sorted(
        (
            sorted(items, key=_summary_sort_key)
            for items in by_campaign.values()
        ),
        key=lambda items: (items[0].scale_name, _summary_sort_key(items[0])),
    )


def _render_database_versions(summaries: list[ConfigSummary]) -> list[str]:
    merged: dict[str, str] = {}
    for summary in summaries:
        merged.update(summary.database_versions)
    label_map = {
        "sqlite": "SQLite",
        "arcadedb-embedded": "ArcadeDB Embedded",
        "duckdb": "DuckDB",
        "postgresql": "PostgreSQL",
        "neo4j": "Neo4j",
        "ladybug": "LadybugDB",
    }
    version_order = {
        "sqlite": 0,
        "duckdb": 1,
        "postgresql": 2,
        "neo4j": 3,
        "arcadedb-embedded": 4,
        "ladybug": 5,
    }
    return [
        f"- `{label_map.get(name, name)}`: `{version}`"
        for name, version in sorted(
            merged.items(),
            key=lambda item: (
                version_order.get(item[0], 999),
                label_map.get(item[0], item[0]),
            ),
        )
    ]


def _render_workload_summary_table(
    summaries: list[ConfigSummary],
    *,
    workload_name: str,
) -> str:
    rows: list[list[str]] = []
    for summary in summaries:
        workload = summary.workload_summaries.get(workload_name)
        if workload is None:
            continue
        rows.append(
            [
                summary.display_name,
                _stat_cell(workload["setup"]["connect_reset_ms"]),
                _stat_cell(workload["setup"]["schema_constraints_ms"]),
                _stat_cell(workload["setup"]["ingest_ms"]),
                _stat_cell(workload["setup"]["index_ms"]),
                _stat_cell(workload["setup"]["analyze_ms"]),
                _stat_cell(workload["end_to_end"]["p50_ms"]),
                _stat_cell(workload["end_to_end"]["p95_ms"]),
                _stat_cell(workload["end_to_end"]["p99_ms"]),
            ]
        )
    return _table(
        [
            "Combo",
            "Connect / Reset",
            "Schema / Constraints",
            "Ingest",
            "Index",
            "Analyze",
            "End-to-end p50",
            "End-to-end p95",
            "End-to-end p99",
        ],
        rows,
    )


def _render_rss_table(
    summaries: list[ConfigSummary],
    *,
    workload_name: str,
) -> str:
    rows: list[list[str]] = []
    for summary in summaries:
        workload = summary.workload_summaries.get(workload_name)
        if workload is None:
            continue
        rows.append(
            [
                summary.display_name,
                _stat_cell(workload["rss"]["connect_reset_mib"], unit="MiB"),
                _stat_cell(
                    workload["rss"]["schema_constraints_mib"],
                    unit="MiB",
                ),
                _stat_cell(workload["rss"]["ingest_mib"], unit="MiB"),
                _stat_cell(workload["rss"]["index_mib"], unit="MiB"),
                _stat_cell(workload["rss"]["analyze_mib"], unit="MiB"),
                _stat_cell(
                    workload["rss"]["suite_complete_mib"],
                    unit="MiB",
                ),
            ]
        )
    return _table(
        [
            "Combo",
            "Connect / Reset",
            "Schema / Constraints",
            "Ingest",
            "Index",
            "Analyze",
            "Suite complete",
        ],
        rows,
    )


def _render_suite_comparison_table(summaries: list[ConfigSummary]) -> str:
    rows: list[list[str]] = []
    for summary in summaries:
        for workload_name in sorted(
            summary.workload_summaries,
            key=lambda name: WORKLOAD_ORDER.get(name, 999),
        ):
            workload = summary.workload_summaries[workload_name]
            rows.append(
                [
                    f"`{workload_name}/{summary.suite_name}`",
                    _stat_cell(workload["end_to_end"]["p50_ms"]),
                    _stat_cell(workload["end_to_end"]["p95_ms"]),
                    _stat_cell(workload["end_to_end"]["p99_ms"]),
                ]
            )
    return _table(["Suite", "p50", "p95", "p99"], rows)


def _render_query_breakdown_table(
    summaries: list[ConfigSummary],
    *,
    workload_name: str,
    percentile_key: str,
) -> str:
    query_names = sorted(
        {
            query_name
            for summary in summaries
            for query_name in summary.workload_summaries.get(workload_name, {}).get(
                "queries",
                {},
            )
        }
    )
    rows: list[list[str]] = []
    for query_name in query_names:
        row = [f"`{query_name}`"]
        for summary in summaries:
            workload = summary.workload_summaries.get(workload_name)
            if workload is None:
                row.append("-")
                continue
            values = workload["queries"].get(query_name, {}).get(percentile_key)
            row.append(_stat_cell(values) if values else "-")
        rows.append(row)
    return _table(
        ["Query", *[summary.display_name for summary in summaries]],
        rows,
    )


def _sql_setup_note(summaries: list[ConfigSummary]) -> list[str]:
    sql_suites = {
        summary.suite_name
        for summary in summaries
        if summary.suite_name.startswith("sqlite_")
        or summary.suite_name.startswith("postgresql_")
        or summary.suite_name == "duckdb"
    }
    if not sql_suites:
        return []
    return [
        "For the SQL backends in this refreshed run, setup follows the more standard",
        "bulk-load sequence: `schema -> ingest -> index -> analyze`. That means the",
        "reported `ingest` step does not include index-maintenance cost during row",
        "insertion, and the `index` step captures post-load index construction.",
    ]


def _direct_runner_notes(summaries: list[ConfigSummary]) -> list[str]:
    suite_names = {summary.suite_name for summary in summaries}
    lines: list[str] = []
    if "neo4j_indexed" in suite_names or "neo4j_unindexed" in suite_names:
        lines.extend(
            [
                (
                    "Neo4j is a direct-Cypher runner rather than a "
                    "compile-plus-execute SQL"
                ),
                "path.",
                "",
            ]
        )
    if "ladybug_unindexed" in suite_names:
        lines.extend(
            [
                "LadybugDB is also a direct-Cypher runner, and it currently uses a",
                "post-load `CHECKPOINT` instead of an `ANALYZE` step. In the summary",
                (
                    "tables below, that checkpoint time is shown in the `Analyze` "
                    "column so"
                ),
                "the setup layout stays consistent across engines.",
                "",
            ]
        )
    if (
        "arcadedb_embedded_indexed" in suite_names
        or "arcadedb_embedded_unindexed" in suite_names
    ):
        lines.extend(
            [
                "ArcadeDB Embedded is also a direct-Cypher runner in this matrix. The",
                (
                    "indexed and unindexed rows below measure ArcadeDB Embedded "
                    "directly rather"
                ),
                "than a CypherGlot compile-plus-execute SQL path.",
                (
                    "ArcadeDB also records graph analytical view build time as "
                    "`gav_ms`; in the"
                ),
                (
                    "summary tables below, that engine-specific post-load work is "
                    "folded into the"
                ),
                (
                    "`Analyze` column, along with the checkpoint step, so the "
                    "setup layout stays"
                ),
                "consistent across engines.",
                "",
            ]
        )
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _caveats(summaries: list[ConfigSummary]) -> list[str]:
    suite_names = {summary.suite_name for summary in summaries}
    lines = [
        "Read these tables with a couple of caveats:",
        "",
        "- SQLite, DuckDB, and PostgreSQL numbers are compile-plus-execute runtime",
        "  timings through CypherGlot.",
        "- Neo4j, ArcadeDB Embedded, and LadybugDB numbers are direct Cypher",
        "  execution timings, so they are not strictly comparable to the",
        "  compile-plus-execute SQL",
        "  paths.",
    ]
    if "duckdb" in suite_names:
        lines.extend(
            [
                "- DuckDB is a single-path run here, and that path is intentionally",
                "  `unindexed`.",
            ]
        )
    if (
        "arcadedb_embedded_indexed" in suite_names
        or "arcadedb_embedded_unindexed" in suite_names
    ):
        lines.extend(
            [
                "- ArcadeDB Embedded is shown in both indexed and unindexed modes",
                "  because the harness supports both direct-runtime paths in the",
                "  current matrix.",
            ]
        )
    if "ladybug_unindexed" in suite_names:
        lines.extend(
            [
                (
                    "- LadybugDB is also a single-path run here. The current "
                    "harness benchmarks"
                ),
                "  an unindexed direct-Cypher path.",
            ]
        )
    lines.extend(
        [
            "- RSS values in these tables are point-in-time resident-memory snapshots",
            "  taken at each named checkpoint, not deltas from the previous step",
            "  and not",
            "  peak-memory readings.",
            "- Total RSS is the sum of benchmark-process RSS plus database-server",
            "  RSS when",
            "  the backend is external.",
        ]
    )
    return lines


def _render_campaign(
    summaries: list[ConfigSummary],
    *,
    include_queries: bool,
) -> str:
    lead = summaries[0]
    graph_scale = lead.graph_scale
    controls = lead.controls
    results_dir_label = _results_dir_label(
        [path for summary in summaries for path in summary.files]
    )
    sql_setup_note = _sql_setup_note(summaries)
    direct_runner_notes = _direct_runner_notes(summaries)

    lines = [
        f"### {lead.scale_name.capitalize()} runtime dataset",
        "",
        (
            f"The current {lead.scale_name} runtime matrix used the "
            f"`{lead.scale_name}` preset with "
            f"`{controls.get('oltp_iterations')}` OLTP iterations / "
            f"`{controls.get('oltp_warmup')}` OLTP warmup and "
            f"`{controls.get('olap_iterations')}` OLAP iterations / "
            f"`{controls.get('olap_warmup')}` OLAP warmup."
        ),
        "",
        "That corresponds to roughly:",
        "",
        f"- `{graph_scale.get('total_nodes'):,}` total nodes",
        f"- `{graph_scale.get('total_edges'):,}` total edges",
        (
            f"- `{len(summaries)}` backend/index combinations across SQLite, "
            "DuckDB, PostgreSQL, Neo4j, ArcadeDB, and LadybugDB"
        ),
        "",
        "Runtime result artifacts for this run now live under",
        f"`{results_dir_label}`.",
        "",
        "Versions used for this summarized run:",
        "",
        *_render_database_versions(summaries),
        "",
        "Runtime benchmark artifacts also record these engine versions in a top-level",
        "`database_versions` object inside each JSON payload.",
    ]
    if sql_setup_note:
        lines.extend(["", *sql_setup_note])
    if direct_runner_notes:
        lines.extend(["", *direct_runner_notes])
    if lines[-1] != "":
        lines.append("")
    lines.extend(
        [
            "",
            "OLTP summary:",
            "",
            _render_workload_summary_table(summaries, workload_name="oltp"),
            "",
            "OLAP summary:",
            "",
            _render_workload_summary_table(summaries, workload_name="olap"),
            "",
            (
                "The tables below sum all process memory involved in the benchmark "
                "at each checkpoint:"
            ),
            (
                "embedded backends contribute only the benchmark process, while "
                "PostgreSQL and Neo4j add"
            ),
            "the server-side RSS snapshot to the client process snapshot.",
            "",
            "Total RSS checkpoints, OLTP:",
            "",
            _render_rss_table(summaries, workload_name="oltp"),
            "",
            "Total RSS checkpoints, OLAP:",
            "",
            _render_rss_table(summaries, workload_name="olap"),
            "",
            f"#### {lead.scale_name.capitalize()} runtime suite comparison",
            "",
            (
                f"This rolls the {lead.scale_name}-runtime matrix up to suite-level "
                "end-to-end percentiles for each workload/backend combination."
            ),
            "",
            _render_suite_comparison_table(summaries),
            "",
            *_caveats(summaries),
        ]
    )
    if include_queries:
        workload_label = {"oltp": "OLTP", "olap": "OLAP"}
        percentile_label = {
            "p50_ms": "p50",
            "p95_ms": "p95",
            "p99_ms": "p99",
        }
        lines.extend(
            [
                "",
                f"#### {lead.scale_name.capitalize()} runtime query breakdowns",
                "",
                "These tables show per-query end-to-end percentiles for the same",
                "runtime matrix, aggregated as mean and standard deviation across",
                "repeated runs.",
            ]
        )
        for workload_name in ("oltp", "olap"):
            for percentile_key in ("p50_ms", "p95_ms", "p99_ms"):
                lines.extend(
                    [
                        "",
                        (
                            f"##### {workload_label[workload_name]} query "
                            "breakdown, end-to-end "
                            f"`{percentile_label[percentile_key]}`"
                        ),
                        "",
                        _render_query_breakdown_table(
                            summaries,
                            workload_name=workload_name,
                            percentile_key=percentile_key,
                        ),
                    ]
                )
    return "\n".join(lines)


def render_summary(
    records: list[RunRecord],
    *,
    skipped: list[Path],
    include_queries: bool,
) -> str:
    if not records:
        return (
            "# Runtime Result Summary\n\n"
            "No completed runtime result JSON files were found.\n"
        )

    groups = _group_runs(records)
    campaigns = _campaigns(groups)
    lines = [
        "# Runtime Result Summary",
        "",
        f"- Scanned JSON files: {len(records) + len(skipped)}",
        f"- Completed runs: {len(records)}",
        f"- Skipped non-completed runs: {len(skipped)}",
        f"- Grouped configurations: {len(groups)}",
        f"- Grouped benchmark campaigns: {len(campaigns)}",
    ]
    for campaign in campaigns:
        lines.extend(["", _render_campaign(campaign, include_queries=include_queries)])
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    json_paths = _discover_json_files(args.inputs)
    completed, skipped = _load_completed_runs(json_paths)
    markdown = render_summary(
        completed,
        skipped=skipped,
        include_queries=args.include_queries,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
