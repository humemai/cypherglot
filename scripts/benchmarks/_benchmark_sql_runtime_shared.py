"""Shared data structures for the SQL runtime benchmark modules."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cypherglot

from _benchmark_common import _rss_mib


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
