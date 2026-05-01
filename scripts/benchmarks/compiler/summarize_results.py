"""Render compiler benchmark JSON results as Markdown."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "results" / "compiler_benchmark.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render compiler benchmark JSON results as a Markdown report."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        default=[DEFAULT_INPUT_PATH],
        help=(
            "JSON files or directories to scan. Defaults to "
            "results/compiler_benchmark.json."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional Markdown output path. Defaults to stdout.",
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


def _relative_label(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _format_us(value: Any) -> str:
    return f"`{float(value):.2f} us`"


def _metric_columns(summary: dict[str, Any]) -> list[str]:
    return [
        _format_us(summary["mean_us"]),
        _format_us(summary["p50_us"]),
        _format_us(summary["p95_us"]),
        _format_us(summary["p99_us"]),
    ]


def _stage_metric_columns(summary: dict[str, Any]) -> list[str]:
    return [
        _format_us(summary["p50_us"]),
        _format_us(summary["p95_us"]),
    ]


def _render_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _shared_entrypoint_rows(payload: dict[str, Any]) -> list[list[str]]:
    return [
        [
            str(result["entrypoint"]),
            str(result["query_count"]),
            str(result["iterations"]),
            str(result["warmup"]),
            *_metric_columns(result["overall"]),
        ]
        for result in payload.get("shared_entrypoint_results", [])
    ]


def _backend_entrypoint_rows(payload: dict[str, Any]) -> list[list[str]]:
    return [
        [
            str(result["backend"]),
            str(result["entrypoint"]),
            str(result["query_count"]),
            str(result["iterations"]),
            str(result["warmup"]),
            *_metric_columns(result["overall"]),
        ]
        for result in payload.get("backend_entrypoint_results", [])
    ]


def _backend_lowering_rows(payload: dict[str, Any]) -> list[list[str]]:
    rows: list[list[str]] = []
    for result in payload.get("backend_lowering_results", []):
        overall = result["overall"]
        rows.append(
            [
                str(result["backend"]),
                str(result["query_count"]),
                _format_us(overall["build_ir"]["p50_us"]),
                _format_us(overall["bind_backend"]["p50_us"]),
                _format_us(overall["lower_backend"]["p50_us"]),
                _format_us(overall["render_program"]["p50_us"]),
                _format_us(overall["end_to_end"]["p50_us"]),
                _format_us(overall["end_to_end"]["p95_us"]),
            ]
        )
    return rows


def _sqlglot_rows(payload: dict[str, Any]) -> list[list[str]]:
    rows: list[list[str]] = []
    for suite in payload.get("sqlglot_suites", []):
        dialect_pair = suite.get("dialect_pair", {})
        for result in suite.get("results", []):
            rows.append(
                [
                    str(suite["implementation"]),
                    str(result["method"]),
                    f"{dialect_pair.get('read', '?')} -> {dialect_pair.get('write', '?')}",
                    str(result["query_count"]),
                    *_metric_columns(result["overall"]),
                ]
            )
    return rows


def _per_query_rows(results: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for query_result in results:
        rows.append(
            [
                str(query_result["name"]),
                str(query_result["category"]),
                *_metric_columns(query_result["summary"]),
            ]
        )
    return rows


def _backend_lowering_query_rows(results: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for query_result in results:
        rows.append(
            [
                str(query_result["name"]),
                str(query_result["category"]),
                *_stage_metric_columns(query_result["build_ir"]),
                *_stage_metric_columns(query_result["bind_backend"]),
                *_stage_metric_columns(query_result["lower_backend"]),
                *_stage_metric_columns(query_result["render_program"]),
                *_stage_metric_columns(query_result["end_to_end"]),
            ]
        )
    return rows


def _render_benchmark_sections(payload: dict[str, Any]) -> list[str]:
    lines = ["## Benchmark Sections", ""]
    benchmark_sections = payload.get("benchmark_sections", {})
    for name, section in benchmark_sections.items():
        lines.append(f"### `{name}`")
        lines.append("")
        lines.append(f"- Purpose: `{section.get('purpose', 'unknown')}`")
        lines.append(
            f"- Comparison axis: `{section.get('comparison_axis', 'unknown')}`"
        )
        if "entrypoints" in section:
            lines.append(
                f"- Entrypoints: `{', '.join(section.get('entrypoints', []))}`"
            )
        if "backends" in section:
            lines.append(
                f"- Backends: `{', '.join(section.get('backends', []))}`"
            )
        lines.append("")
    return lines


def _render_shared_query_sections(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for result in payload.get("shared_entrypoint_results", []):
        lines.extend(
            [
                f"### `{result['entrypoint']}` per-query",
                "",
            ]
        )
        lines.extend(
            _render_table(
                ["Query", "Category", "Mean", "P50", "P95", "P99"],
                _per_query_rows(result.get("queries", [])),
            )
        )
        lines.append("")
    return lines


def _render_backend_entrypoint_query_sections(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for result in payload.get("backend_entrypoint_results", []):
        lines.extend(
            [
                f"### `{result['backend']}` / `{result['entrypoint']}` per-query",
                "",
            ]
        )
        lines.extend(
            _render_table(
                ["Query", "Category", "Mean", "P50", "P95", "P99"],
                _per_query_rows(result.get("queries", [])),
            )
        )
        lines.append("")
    return lines


def _render_backend_lowering_query_sections(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for result in payload.get("backend_lowering_results", []):
        lines.extend(
            [
                f"### `{result['backend']}` backend lowering per-query",
                "",
            ]
        )
        lines.extend(
            _render_table(
                [
                    "Query",
                    "Category",
                    "Build IR P50",
                    "Build IR P95",
                    "Bind Backend P50",
                    "Bind Backend P95",
                    "Lower Backend P50",
                    "Lower Backend P95",
                    "Render Program P50",
                    "Render Program P95",
                    "End-to-End P50",
                    "End-to-End P95",
                ],
                _backend_lowering_query_rows(result.get("queries", [])),
            )
        )
        lines.append("")
    return lines


def _render_sqlglot_suite_details(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for suite in payload.get("sqlglot_suites", []):
        lines.extend(
            [
                f"### SQLGlot `{suite['implementation']}` details",
                "",
                f"- Version: `{suite.get('sqlglot_version', 'unknown')}`",
            ]
        )
        module_files = suite.get("module_files", {})
        if module_files:
            lines.append("- Module files:")
            for module_name, module_path in module_files.items():
                lines.append(f"  - `{module_name}`: `{module_path}`")
        lines.append("")
    return lines


def _render_sqlglot_query_sections(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for suite in payload.get("sqlglot_suites", []):
        dialect_pair = suite.get("dialect_pair", {})
        dialect_label = (
            f"{dialect_pair.get('read', '?')} -> {dialect_pair.get('write', '?')}"
        )
        for result in suite.get("results", []):
            lines.extend(
                [
                    (
                        "### SQLGlot "
                        f"`{suite['implementation']}` / `{result['method']}` per-query"
                    ),
                    "",
                    f"- Dialects: `{dialect_label}`",
                    "",
                ]
            )
            lines.extend(
                _render_table(
                    ["Query", "Category", "Mean", "P50", "P95", "P99"],
                    _per_query_rows(result.get("queries", [])),
                )
            )
            lines.append("")
    return lines


def _render_payload(payload: dict[str, Any], *, source_path: Path) -> str:
    schema_contract = payload.get("schema_contract", {})
    benchmark_schema = payload.get("benchmark_schema", {})
    lines = [
        "# Compiler Benchmark Summary",
        "",
        f"Source: `{_relative_label(source_path)}`  ",
        f"Generated: `{payload.get('generated_at', 'unknown')}`  ",
        f"CypherGlot: `{payload.get('cypherglot_version', 'unknown')}`  ",
        f"Python: `{payload.get('python_version', 'unknown')}`  ",
        f"Platform: `{payload.get('platform', 'unknown')}`",
        "",
        "## Overview",
        "",
        f"- Corpus: `{payload.get('corpus_path', 'unknown')}`",
        f"- Query count: `{payload.get('query_count', 0)}`",
        f"- Schema layout: `{schema_contract.get('layout', 'unknown')}`",
        f"- SQL emission: `{schema_contract.get('emitted_sql', 'unknown')}`",
        f"- Release subset: `{schema_contract.get('release_subset', 'unknown')}`",
        f"- Node types: `{', '.join(benchmark_schema.get('node_types', []))}`",
        f"- Edge types: `{', '.join(benchmark_schema.get('edge_types', []))}`",
        f"- SQL corpus: `{payload.get('sql_corpus_path', 'unknown')}`",
        f"- SQL query count: `{payload.get('sql_query_count', 0)}`",
        f"- SQLGlot mode: `{payload.get('sqlglot_mode', 'unknown')}`",
        "",
    ]
    lines.extend(_render_benchmark_sections(payload))
    lines.extend([
        "## Shared Entrypoints",
        "",
    ])
    lines.extend(
        _render_table(
            ["Entrypoint", "Queries", "Iterations", "Warmup", "Mean", "P50", "P95", "P99"],
            _shared_entrypoint_rows(payload),
        )
    )
    lines.extend(["", "## Shared Entrypoints Per Query", ""])
    lines.extend(_render_shared_query_sections(payload))
    lines.extend(
        [
            "## Backend Entrypoints",
            "",
        ]
    )
    lines.extend(
        _render_table(
            ["Backend", "Entrypoint", "Queries", "Iterations", "Warmup", "Mean", "P50", "P95", "P99"],
            _backend_entrypoint_rows(payload),
        )
    )
    lines.extend(["", "## Backend Entrypoints Per Query", ""])
    lines.extend(_render_backend_entrypoint_query_sections(payload))
    lines.extend(
        [
            "## Backend Lowering",
            "",
        ]
    )
    lines.extend(
        _render_table(
            [
                "Backend",
                "Queries",
                "Build IR P50",
                "Bind Backend P50",
                "Lower Backend P50",
                "Render Program P50",
                "End-to-End P50",
                "End-to-End P95",
            ],
            _backend_lowering_rows(payload),
        )
    )
    lines.extend(["", "## Backend Lowering Per Query", ""])
    lines.extend(_render_backend_lowering_query_sections(payload))

    sqlglot_rows = _sqlglot_rows(payload)
    if sqlglot_rows:
        lines.extend(
            [
                "",
                "## SQLGlot Suites",
                "",
            ]
        )
        lines.extend(
            _render_table(
                ["Implementation", "Method", "Dialects", "Queries", "Mean", "P50", "P95", "P99"],
                sqlglot_rows,
            )
        )
        lines.extend(["", "## SQLGlot Per Query", ""])
        lines.extend(_render_sqlglot_query_sections(payload))
        lines.extend(["", "## SQLGlot Details", ""])
        lines.extend(_render_sqlglot_suite_details(payload))
    lines.append("")
    return "\n".join(lines)


def _render_report(paths: list[Path]) -> str:
    sections = [
        _render_payload(json.loads(path.read_text(encoding="utf-8")), source_path=path)
        for path in paths
    ]
    return "\n\n".join(sections)


def main() -> int:
    args = _parse_args()
    paths = _discover_json_files(args.inputs)
    if not paths:
        raise ValueError("No compiler benchmark JSON files found.")

    report = _render_report(paths)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())