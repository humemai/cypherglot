"""Shared data structures for the SQL runtime benchmark modules."""

from __future__ import annotations

import csv
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cypherglot

from _benchmark_common import (
    EdgeTypePlan,
    RuntimeScale,
    _edge_out_degree,
    _measure_ns,
    _node_id,
    _node_name,
    _node_type_name,
    _progress,
    _rss_mib,
)


DuckDBConnection = Any
PostgreSQLConnection = Any


@dataclass(slots=True)
class ManagedDirectory:
    path: Path
    temp_dir: tempfile.TemporaryDirectory[str] | None = None

    def close(self) -> None:
        if self.temp_dir is not None:
            self.temp_dir.cleanup()


@dataclass(frozen=True, slots=True)
class PreparedArtifact:
    mode: str
    compiled: str | cypherglot.RenderedCypherProgram


@dataclass(slots=True)
class SharedSQLiteFixture:
    work_dir: ManagedDirectory
    db_path: Path
    setup_metrics: dict[str, int]
    row_counts: dict[str, int]
    rss_snapshots_mib: dict[str, dict[str, float | None]]
    db_size_mib: float
    wal_size_mib: float
    index_mode: str

    def close(self) -> None:
        self.work_dir.close()


@dataclass(slots=True)
class GeneratedGraphFixture:
    work_dir: ManagedDirectory
    csv_dir: Path
    manifest_path: Path
    table_csv_paths: dict[str, Path]
    table_columns: dict[str, list[str]]
    row_counts: dict[str, int]
    rss_snapshots_mib: dict[str, dict[str, float | None]]
    index_mode: str
    setup_metrics: dict[str, int] | None = None
    db_path: Path | None = None
    db_size_mib: float = 0.0
    wal_size_mib: float = 0.0

    def close(self) -> None:
        self.work_dir.close()


def _create_managed_directory(
    *,
    root_dir: Path | None,
    prefix: str,
    name: str | None = None,
) -> ManagedDirectory:
    if root_dir is None:
        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix=prefix
        )
        return ManagedDirectory(path=Path(temp_dir.name), temp_dir=temp_dir)

    directory_name = name if name is not None else prefix.rstrip("-")
    path = root_dir / directory_name
    path.mkdir(parents=True, exist_ok=False)
    return ManagedDirectory(path=path)


def _node_row(
    scale: RuntimeScale,
    type_index: int,
    local_index: int,
) -> tuple[Any, ...]:
    row: list[Any] = [
        _node_id(scale, type_index, local_index),
        _node_name(type_index, local_index),
        18 + ((type_index * 5 + local_index) % 47),
        round(1.0 + ((type_index * 17 + local_index * 7) % 500) / 100.0, 2),
        int((type_index + local_index) % 3 != 0),
    ]
    row.extend(
        (
            f"{_node_type_name(type_index).lower()}-"
            f"text-{property_index:02d}-{local_index:06d}"
        )
        for property_index in range(1, scale.node_extra_text_property_count + 1)
    )
    row.extend(
        round(
            property_index
            + ((type_index * 31 + local_index * (property_index + 9)) % 10_000)
            / 100.0,
            2,
        )
        for property_index in range(1, scale.node_extra_numeric_property_count + 1)
    )
    row.extend(
        int((type_index + local_index + property_index) % 2 == 0)
        for property_index in range(1, scale.node_extra_boolean_property_count + 1)
    )
    return tuple(row)


def _edge_row(
    scale: RuntimeScale,
    plan: EdgeTypePlan,
    source_local_index: int,
    edge_ordinal: int,
    edge_id: int,
) -> tuple[Any, ...]:
    from_node_id = _node_id(scale, plan.source_type_index, source_local_index)
    target_local_index = (
        (source_local_index - 1 + plan.type_index + edge_ordinal)
        % scale.nodes_per_type
    ) + 1
    to_node_id = _node_id(scale, plan.target_type_index, target_local_index)
    row: list[Any] = [
        edge_id,
        from_node_id,
        to_node_id,
        f"{plan.name.lower()}-note-{edge_ordinal:02d}-{source_local_index:06d}",
        round(
            0.5 + ((plan.type_index + source_local_index + edge_ordinal) % 11) * 0.35,
            2,
        ),
        round(
            1.0
            + ((plan.type_index * 7 + source_local_index + edge_ordinal) % 17) * 0.4,
            2,
        ),
        int((plan.type_index + source_local_index + edge_ordinal) % 2 == 0),
        1 + ((plan.type_index + source_local_index + edge_ordinal) % 100),
    ]
    row.extend(
        f"{plan.name.lower()}-text-{property_index:02d}-{source_local_index:06d}"
        for property_index in range(1, scale.edge_extra_text_property_count + 1)
    )
    row.extend(
        round(
            property_index
            + (
                (
                    plan.type_index * 19
                    + source_local_index * (property_index + 5)
                    + edge_ordinal
                )
                % 5_000
            )
            / 100.0,
            2,
        )
        for property_index in range(1, scale.edge_extra_numeric_property_count + 1)
    )
    row.extend(
        int(
            (
                plan.type_index
                + source_local_index
                + edge_ordinal
                + property_index
            )
            % 2
            == 0
        )
        for property_index in range(1, scale.edge_extra_boolean_property_count + 1)
    )
    return tuple(row)


def _graph_fixture_table_columns(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, list[str]]:
    columns: dict[str, list[str]] = {}
    for node_type in graph_schema.node_types:
        columns[node_type.table_name] = [
            "id",
            *(property_schema.name for property_schema in node_type.properties),
        ]
    for edge_type in graph_schema.edge_types:
        columns[edge_type.table_name] = [
            "id",
            "from_id",
            "to_id",
            *(property_schema.name for property_schema in edge_type.properties),
        ]
    return columns


def _write_graph_fixture_csv(
    csv_path: Path,
    *,
    column_names: list[str],
    rows: list[tuple[Any, ...]],
) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(column_names)
        writer.writerows(rows)


def _prepare_generated_graph_fixture(
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    index_mode: str,
    db_root_dir: Path | None = None,
) -> GeneratedGraphFixture:
    progress_label = f"generated-fixture/{index_mode}"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix=f"cypherglot-runtime-generated-{index_mode}-",
        name=f"generated-{index_mode}",
    )
    csv_dir = work_dir.path / "csv"
    csv_dir.mkdir(parents=True, exist_ok=False)
    manifest_path = work_dir.path / "manifest.json"
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}
    table_columns = _graph_fixture_table_columns(graph_schema)

    _progress(
        f"{progress_label}: creating dataset "
        f"({scale.total_nodes} nodes, {scale.total_edges} edges)"
    )
    rss_snapshots_mib["before_generate"] = _capture_rss_snapshot(backend="fixture")

    def generate() -> dict[str, Path]:
        table_csv_paths: dict[str, Path] = {}

        for type_index, node_type in enumerate(graph_schema.node_types, start=1):
            _progress(
                f"{progress_label}: node type {type_index}/{scale.node_type_count} "
                f"({node_type.name})"
            )
            rows = [
                _node_row(scale, type_index, local_index)
                for local_index in range(1, scale.nodes_per_type + 1)
            ]
            csv_path = csv_dir / f"{node_type.table_name}.csv"
            _write_graph_fixture_csv(
                csv_path,
                column_names=table_columns[node_type.table_name],
                rows=rows,
            )
            table_csv_paths[node_type.table_name] = csv_path

        edge_id = 1
        for edge_type_index, plan in enumerate(edge_plans, start=1):
            _progress(
                f"{progress_label}: edge type "
                f"{edge_type_index}/{scale.edge_type_count} "
                f"({plan.name})"
            )
            rows: list[tuple[Any, ...]] = []
            for source_local_index in range(1, scale.nodes_per_type + 1):
                edge_count_for_source = _edge_out_degree(scale, source_local_index)
                for edge_ordinal in range(1, edge_count_for_source + 1):
                    rows.append(
                        _edge_row(
                            scale,
                            plan,
                            source_local_index,
                            edge_ordinal,
                            edge_id,
                        )
                    )
                    edge_id += 1
            table_name = graph_schema.edge_types[plan.type_index - 1].table_name
            csv_path = csv_dir / f"{table_name}.csv"
            _write_graph_fixture_csv(
                csv_path,
                column_names=table_columns[table_name],
                rows=rows,
            )
            table_csv_paths[table_name] = csv_path

        return table_csv_paths

    table_csv_paths, generate_ns = _measure_ns(generate)
    rss_snapshots_mib["after_generate"] = _capture_rss_snapshot(backend="fixture")

    row_counts = {
        "node_count": scale.total_nodes,
        "edge_count": scale.total_edges,
        "node_type_count": scale.node_type_count,
        "edge_type_count": scale.edge_type_count,
    }
    manifest_path.write_text(
        json.dumps(
            {
                "index_mode": index_mode,
                "row_counts": row_counts,
                "table_columns": table_columns,
                "table_csv_paths": {
                    table_name: str(path)
                    for table_name, path in table_csv_paths.items()
                },
                "generate_ns": generate_ns,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _progress(f"{progress_label}: dataset ready")
    return GeneratedGraphFixture(
        work_dir=work_dir,
        csv_dir=csv_dir,
        manifest_path=manifest_path,
        table_csv_paths=table_csv_paths,
        table_columns=table_columns,
        row_counts=row_counts,
        rss_snapshots_mib=rss_snapshots_mib,
        index_mode=index_mode,
    )


def _query_index_statements(graph_schema: cypherglot.GraphSchema) -> list[str]:
    statements: list[str] = []
    for node_type in graph_schema.node_types:
        statements.append(
            f"CREATE INDEX idx_{node_type.table_name}_name "
            f"ON {node_type.table_name}(name)"
        )
        statements.append(
            f"CREATE INDEX idx_{node_type.table_name}_active_score "
            f"ON {node_type.table_name}(active, score DESC)"
        )
        statements.append(
            f"CREATE INDEX idx_{node_type.table_name}_age "
            f"ON {node_type.table_name}(age)"
        )

    for edge_type in graph_schema.edge_types:
        statements.append(
            f"CREATE INDEX idx_{edge_type.table_name}_rank "
            f"ON {edge_type.table_name}(rank)"
        )
        statements.append(
            f"CREATE INDEX idx_{edge_type.table_name}_active_score "
            f"ON {edge_type.table_name}(active, score DESC)"
        )
    return statements


def _capture_rss_snapshot(
    *,
    backend: str,
    server_mib: float | None = None,
) -> dict[str, float | None]:
    client_mib = _rss_mib()
    combined_mib: float | None
    if backend == "postgresql" and server_mib is None:
        combined_mib = None
    else:
        combined_mib = client_mib + (server_mib or 0.0)

    return {
        "client_mib": client_mib,
        "server_mib": server_mib,
        "combined_mib": combined_mib,
    }


def _summarize_rss_samples(
    samples: list[dict[str, float | None]],
) -> dict[str, float | None]:
    def summarize_metric(metric_name: str) -> tuple[float | None, float | None]:
        values = [
            sample[metric_name]
            for sample in samples
            if sample[metric_name] is not None
        ]
        if not values:
            return None, None
        return sum(values) / len(values), max(values)

    client_mean, client_peak = summarize_metric("client_mib")
    server_mean, server_peak = summarize_metric("server_mib")
    combined_mean, combined_peak = summarize_metric("combined_mib")
    return {
        "client_mean_mib": client_mean,
        "client_peak_mib": client_peak,
        "server_mean_mib": server_mean,
        "server_peak_mib": server_peak,
        "combined_mean_mib": combined_mean,
        "combined_peak_mib": combined_peak,
    }
