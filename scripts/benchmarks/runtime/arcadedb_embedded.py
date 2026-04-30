"""Validate and benchmark the runtime corpus directly on ArcadeDB Embedded.

This script seeds the same synthetic graph shape used by the SQL runtime
benchmarks into an on-disk ArcadeDB Embedded database, then executes the
runtime corpus directly as OpenCypher. ArcadeDB Embedded supports both indexed
and unindexed modes for the query-facing node and edge properties used by the
benchmark corpus.
"""

from __future__ import annotations

import argparse
import csv
import gc
import importlib.metadata
import json
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import cypherglot

from scripts.benchmarks.common.shared import (
    BenchmarkQueryTimeoutError,
    CorpusQuery,
    RuntimeScale,
    _average_edges_per_source,
    _build_graph_schema,
    _call_with_timeout,
    _measure_ns,
    _progress,
    _progress_iteration,
    _render_corpus_queries,
    _select_queries,
    _summarize,
    _token_map,
    _write_json_atomic,
)
from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    ManagedDirectory,
    _capture_rss_snapshot,
    _create_managed_directory,
    _prepare_generated_graph_fixture,
)

try:
    import arcadedb_embedded as arcadedb
except ImportError:  # pragma: no cover - optional dependency
    arcadedb = None

if arcadedb is None:
    ARCADEDB_QUERY_EXCEPTIONS: tuple[type[BaseException], ...] = (
        RuntimeError,
        ValueError,
    )
else:
    ARCADEDB_QUERY_EXCEPTIONS = (
        arcadedb.ArcadeDBError,
        RuntimeError,
        ValueError,
    )


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
DEFAULT_RUNTIME_RESULTS_DIR = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "runtime"
)
DEFAULT_OUTPUT_PATH = (
    DEFAULT_RUNTIME_RESULTS_DIR / "arcadedb-embedded_runtime_benchmark.json"
)
ARCADEDB_BACKEND_NAME = "arcadedb-embedded"
ARCADEDB_SUITE_PREFIX = "arcadedb_embedded"
ARCADEDB_GAV_NAME = "cypherglot_olap"
ARCADEDB_GAV_NODE_PROPERTIES = ("id", "name", "age", "active", "score")
ARCADEDB_GAV_EDGE_PROPERTIES = ("rank", "active", "score")
INDEX_MODE_CHOICES = ("indexed", "unindexed", "both")
ARCADEDB_WORKER_STARTUP_TIMEOUT_S = 5.0
ARCADEDB_WORKER_SHUTDOWN_TIMEOUT_S = 1.0
RuntimeProgressCallback = Callable[[dict[str, object], int], None]


@dataclass(slots=True)
class ArcadeDBFixture:
    work_dir: ManagedDirectory
    db_path: Path
    database: Any
    setup_metrics: dict[str, int]
    row_counts: dict[str, int]
    rss_snapshots_mib: dict[str, dict[str, float | None]]
    db_size_mib: float
    wal_size_mib: float
    index_mode: str

    def close(self) -> None:
        self.database.close()
        self.work_dir.close()


def _arcadedb_available() -> bool:
    return arcadedb is not None


def _arcadedb_version() -> str | None:
    if arcadedb is None:
        return None

    try:
        return importlib.metadata.version("arcadedb-embedded")
    except importlib.metadata.PackageNotFoundError:
        version = getattr(arcadedb, "__version__", None)
        if version is None:
            return None
        return str(version)


def _open_arcadedb(db_path: Path) -> Any:
    if arcadedb is None:
        raise ValueError(
            "arcadedb-embedded is not installed. Install it with "
            "`uv pip install arcadedb-embedded` or a dev build such as "
            "`uv pip install arcadedb-embedded==26.4.1.dev3`."
        )
    db_path_str = str(db_path)
    if arcadedb.database_exists(db_path_str):
        return arcadedb.open_database(db_path_str)
    return arcadedb.create_database(db_path_str)


def _recursive_file_size_mib(path: Path) -> float:
    total_bytes = 0
    if path.is_file():
        total_bytes += path.stat().st_size
    elif path.exists():
        for child in path.rglob("*"):
            if child.is_file():
                total_bytes += child.stat().st_size
    return total_bytes / (1024.0 * 1024.0)


def _arcadedb_storage_sizes(db_path: Path) -> tuple[float, float]:
    wal_bytes = 0
    if db_path.exists():
        for child in db_path.rglob("*"):
            if child.is_file() and "wal" in child.name.lower():
                wal_bytes += child.stat().st_size
    return _recursive_file_size_mib(db_path), wal_bytes / (1024.0 * 1024.0)


def _arcadedb_type_name(type_name: str) -> str:
    if type_name == "boolean":
        return "BOOLEAN"
    if type_name == "integer":
        return "LONG"
    if type_name == "float":
        return "DOUBLE"
    return "STRING"


def _create_arcadedb_schema(
    db: Any,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for node_type in graph_schema.node_types:
        db.command("sql", f"CREATE VERTEX TYPE {node_type.name}")
        db.command("sql", f"CREATE PROPERTY {node_type.name}.id LONG")
        for property_schema in node_type.properties:
            db.command(
                "sql",
                (
                    f"CREATE PROPERTY {node_type.name}.{property_schema.name} "
                    f"{_arcadedb_type_name(property_schema.logical_type)}"
                ),
            )

    for edge_type in graph_schema.edge_types:
        db.command("sql", f"CREATE EDGE TYPE {edge_type.name}")
        for property_schema in edge_type.properties:
            db.command(
                "sql",
                (
                    f"CREATE PROPERTY {edge_type.name}.{property_schema.name} "
                    f"{_arcadedb_type_name(property_schema.logical_type)}"
                ),
            )


def _coerce_arcadedb_value(value: object, logical_type: str) -> object:
    if value is None:
        return None
    if logical_type == "boolean":
        if isinstance(value, str):
            return value not in {"", "0", "false", "False"}
        return bool(value)
    if logical_type == "integer":
        return int(value)
    if logical_type == "float":
        return float(value)
    return str(value)


def _node_property_type_map(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, dict[str, str]]:
    type_map: dict[str, dict[str, str]] = {}
    for node_type in graph_schema.node_types:
        property_types = {"id": "integer"}
        for property_schema in node_type.properties:
            property_types[property_schema.name] = property_schema.logical_type
        type_map[node_type.table_name] = property_types
    return type_map


def _edge_property_type_map(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, dict[str, str]]:
    type_map: dict[str, dict[str, str]] = {}
    for edge_type in graph_schema.edge_types:
        property_types = {"from_id": "integer", "to_id": "integer"}
        for property_schema in edge_type.properties:
            property_types[property_schema.name] = property_schema.logical_type
        type_map[edge_type.table_name] = property_types
    return type_map


def _seed_arcadedb_from_fixture(
    db: Any,
    *,
    sqlite_source: GeneratedGraphFixture,
    graph_schema: cypherglot.GraphSchema,
    ingest_batch_size: int,
) -> dict[str, int]:
    node_property_types = _node_property_type_map(graph_schema)
    edge_property_types = _edge_property_type_map(graph_schema)
    rid_lookup: dict[int, str] = {}
    batch_size = max(1, ingest_batch_size)

    with db.graph_batch(
        batch_size=batch_size,
        expected_edge_count=sqlite_source.row_counts["edge_count"],
        bidirectional=False,
        commit_every=batch_size,
        use_wal=False,
        parallel_flush=False,
    ) as batch:
        for node_type in graph_schema.node_types:
            table_name = node_type.table_name
            property_types = node_property_types[table_name]
            with sqlite_source.table_csv_paths[table_name].open(
                "r",
                encoding="utf-8",
                newline="",
            ) as handle:
                reader = csv.DictReader(handle)
                payload_rows: list[dict[str, object]] = []
                node_ids: list[int] = []
                for row in reader:
                    payload = {
                        column_name: _coerce_arcadedb_value(
                            row[column_name],
                            property_types[column_name],
                        )
                        for column_name in reader.fieldnames or []
                    }
                    node_ids.append(int(payload["id"]))
                    payload_rows.append(payload)
                    if len(payload_rows) < batch_size:
                        continue
                    created_rids = batch.create_vertices(node_type.name, payload_rows)
                    for node_id, rid in zip(node_ids, created_rids, strict=True):
                        rid_lookup[node_id] = rid
                    payload_rows.clear()
                    node_ids.clear()
                if payload_rows:
                    created_rids = batch.create_vertices(node_type.name, payload_rows)
                    for node_id, rid in zip(node_ids, created_rids, strict=True):
                        rid_lookup[node_id] = rid

        for edge_type in graph_schema.edge_types:
            table_name = edge_type.table_name
            property_types = edge_property_types[table_name]
            with sqlite_source.table_csv_paths[table_name].open(
                "r",
                encoding="utf-8",
                newline="",
            ) as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    payload = {
                        column_name: _coerce_arcadedb_value(
                            row[column_name],
                            property_types[column_name],
                        )
                        for column_name in (reader.fieldnames or [])
                        if column_name != "id"
                    }
                    from_id = int(payload.pop("from_id"))
                    to_id = int(payload.pop("to_id"))
                    from_rid = rid_lookup.get(from_id)
                    to_rid = rid_lookup.get(to_id)
                    if from_rid is None or to_rid is None:
                        raise RuntimeError(
                            "Missing RID endpoint during ArcadeDB ingest: "
                            f"from_id={from_id}, to_id={to_id}"
                        )
                    batch.new_edge(from_rid, edge_type.name, to_rid, **payload)

    return {
        "node_count": sqlite_source.row_counts["node_count"],
        "edge_count": sqlite_source.row_counts["edge_count"],
        "node_type_count": len(graph_schema.node_types),
        "edge_type_count": len(graph_schema.edge_types),
    }


def _arcadedb_query_index_statements(
    graph_schema: cypherglot.GraphSchema,
) -> list[str]:
    statements: list[str] = []
    for node_type in graph_schema.node_types:
        statements.extend(
            [
                f"CREATE INDEX ON {node_type.name} (id) UNIQUE_HASH",
                f"CREATE INDEX ON {node_type.name} (name) NOTUNIQUE",
                f"CREATE INDEX ON {node_type.name} (active) NOTUNIQUE",
                f"CREATE INDEX ON {node_type.name} (score) NOTUNIQUE",
                f"CREATE INDEX ON {node_type.name} (age) NOTUNIQUE",
            ]
        )

    for edge_type in graph_schema.edge_types:
        statements.extend(
            [
                f"CREATE INDEX ON {edge_type.name} (rank) NOTUNIQUE",
                f"CREATE INDEX ON {edge_type.name} (active) NOTUNIQUE",
                f"CREATE INDEX ON {edge_type.name} (score) NOTUNIQUE",
            ]
        )
    return statements


def _create_arcadedb_query_indexes(
    db: Any,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _arcadedb_query_index_statements(graph_schema):
        db.command("sql", statement)


def _arcadedb_gav_statement(graph_schema: cypherglot.GraphSchema) -> str:
    vertex_types = ", ".join(node_type.name for node_type in graph_schema.node_types)
    edge_types = ", ".join(edge_type.name for edge_type in graph_schema.edge_types)
    node_properties = ", ".join(ARCADEDB_GAV_NODE_PROPERTIES)
    edge_properties = ", ".join(ARCADEDB_GAV_EDGE_PROPERTIES)
    return (
        f"CREATE GRAPH ANALYTICAL VIEW {ARCADEDB_GAV_NAME} "
        f"VERTEX TYPES ({vertex_types}) "
        f"EDGE TYPES ({edge_types}) "
        f"PROPERTIES ({node_properties}) "
        f"EDGE PROPERTIES ({edge_properties}) "
        "UPDATE MODE OFF"
    )


def _create_arcadedb_gav(
    db: Any,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    try:
        db.command("sql", _arcadedb_gav_statement(graph_schema))
    except ARCADEDB_QUERY_EXCEPTIONS as exc:
        raise RuntimeError(
            "ArcadeDB Graph Analytical View SQL support is required for the "
            f"OLAP benchmark path: {exc}"
        ) from exc


def _fetch_arcadedb_gav_metadata(db: Any, name: str) -> dict[str, Any] | None:
    row = db.query(
        "sql",
        "SELECT FROM schema:graphAnalyticalViews WHERE name = ?",
        name,
    ).first()
    if row is None:
        return None

    return {
        "name": row.get("name"),
        "status": row.get("status"),
        "updateMode": row.get("updateMode"),
        "nodeCount": row.get("nodeCount"),
        "edgeCount": row.get("edgeCount"),
        "buildDurationMs": row.get("buildDurationMs"),
    }


def _wait_for_arcadedb_gav_status(
    db: Any,
    name: str,
    expected_statuses: set[str],
    timeout_sec: float = 180.0,
) -> dict[str, Any]:
    start = time.perf_counter()
    last_metadata = None
    while True:
        metadata = _fetch_arcadedb_gav_metadata(db, name)
        if metadata is not None:
            last_metadata = metadata
            if metadata["status"] in expected_statuses:
                return metadata

        if time.perf_counter() - start > timeout_sec:
            raise RuntimeError(
                f"Timed out waiting for GAV {name} to reach "
                f"{sorted(expected_statuses)}. Last metadata: {last_metadata}"
            )
        time.sleep(0.25)


def _prepare_arcadedb_olap_gav(
    db: Any,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, Any]:
    _create_arcadedb_gav(db, graph_schema)
    return _wait_for_arcadedb_gav_status(db, ARCADEDB_GAV_NAME, {"READY"})


def _rollback_arcadedb_transaction(db: Any) -> None:
    try:
        db.rollback()
    except ARCADEDB_QUERY_EXCEPTIONS as exc:
        message = str(exc).lower()
        if "not begun" not in message and "no active transaction" not in message:
            raise


def _rewrite_arcadedb_query(
    fixture: ArcadeDBFixture,
    query: CorpusQuery,
) -> str:
    base_id = fixture.row_counts["node_count"] + 1
    if query.name == "oltp_create_type1_node":
        return query.query.replace(
            "{name:",
            f"{{id: {base_id}, name:",
            1,
        )
    if query.name == "oltp_program_create_and_link":
        prefix, suffix = query.query.rsplit("{name:", 1)
        return prefix + f"{{id: {base_id + 1}, name:" + suffix
    return query.query


def _suite_key(index_mode: str) -> str:
    return f"{ARCADEDB_SUITE_PREFIX}_{index_mode}"


def _prepare_arcadedb_fixture(
    *,
    workload: str,
    index_mode: str,
    graph_schema: cypherglot.GraphSchema,
    sqlite_source: GeneratedGraphFixture,
    ingest_batch_size: int,
    db_root_dir: Path | None,
) -> ArcadeDBFixture:
    suite_name = f"arcadedb-embedded-{workload}-{index_mode}-suite"
    progress_label = f"{ARCADEDB_BACKEND_NAME}/{index_mode}"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix="cypherglot-runtime-arcadedb-embedded-suite-",
        name=suite_name,
    )
    db_path = work_dir.path / "runtime.arcadedb"
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}

    _progress(
        f"{progress_label}: creating fixture "
        f"({sqlite_source.row_counts['node_count']} nodes, "
        f"{sqlite_source.row_counts['edge_count']} edges)"
    )
    database, connect_ns = _measure_ns(lambda: _open_arcadedb(db_path))
    rss_snapshots_mib["after_connect"] = _capture_rss_snapshot(
        backend=ARCADEDB_BACKEND_NAME
    )
    try:
        _progress(f"{progress_label}: creating schema")
        _, schema_ns = _measure_ns(
            lambda: _create_arcadedb_schema(database, graph_schema)
        )
        rss_snapshots_mib["after_schema"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )

        _progress(
            f"{progress_label}: ingesting from generated fixture "
            f"({sqlite_source.csv_dir})"
        )
        row_counts, ingest_ns = _measure_ns(
            lambda: _seed_arcadedb_from_fixture(
                database,
                sqlite_source=sqlite_source,
                graph_schema=graph_schema,
                ingest_batch_size=ingest_batch_size,
            )
        )
        rss_snapshots_mib["after_ingest"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )

        if index_mode == "indexed":
            _progress(f"{progress_label}: creating query indexes")
            _, index_ns = _measure_ns(
                lambda: _create_arcadedb_query_indexes(database, graph_schema)
            )
        else:
            index_ns = 0
        rss_snapshots_mib["after_index"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )

        if workload == "olap":
            _progress(f"{progress_label}: creating graph analytical view")
            _, gav_ns = _measure_ns(
                lambda: _prepare_arcadedb_olap_gav(database, graph_schema)
            )
        else:
            gav_ns = 0
        rss_snapshots_mib["after_gav"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )

        _, checkpoint_ns = _measure_ns(lambda: None)
        rss_snapshots_mib["after_checkpoint"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )
    except Exception:
        database.close()
        work_dir.close()
        raise

    db_size_mib, wal_size_mib = _arcadedb_storage_sizes(db_path)
    _progress(
        f"{progress_label}: fixture ready "
        f"(ingest={ingest_ns / 1_000_000_000.0:.2f}s, db={db_size_mib:.2f} MiB)"
    )
    return ArcadeDBFixture(
        work_dir=work_dir,
        db_path=db_path,
        database=database,
        setup_metrics={
            "connect_ns": connect_ns,
            "schema_ns": schema_ns,
            "ingest_ns": ingest_ns,
            "index_ns": index_ns,
            "gav_ns": gav_ns,
            "checkpoint_ns": checkpoint_ns,
        },
        row_counts=row_counts,
        rss_snapshots_mib=rss_snapshots_mib,
        db_size_mib=db_size_mib,
        wal_size_mib=wal_size_mib,
        index_mode=index_mode,
    )


def _pool_summaries(
    query_results: list[dict[str, object]],
    key: str,
) -> dict[str, float]:
    successful = [result for result in query_results if result["status"] == "passed"]
    if not successful:
        return {
            "mean_of_mean_ms": 0.0,
            "mean_of_p50_ms": 0.0,
            "mean_of_p95_ms": 0.0,
            "mean_of_p99_ms": 0.0,
        }
    return {
        "mean_of_mean_ms": sum(result[key]["mean_ms"] for result in successful)
        / len(successful),
        "mean_of_p50_ms": sum(result[key]["p50_ms"] for result in successful)
        / len(successful),
        "mean_of_p95_ms": sum(result[key]["p95_ms"] for result in successful)
        / len(successful),
        "mean_of_p99_ms": sum(result[key]["p99_ms"] for result in successful)
        / len(successful),
    }


def _load_corpus(path: Path) -> list[CorpusQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("Runtime benchmark corpus must be a non-empty JSON list.")

    queries: list[CorpusQuery] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Runtime corpus item {index} must be a JSON object.")
        try:
            name = item["name"]
            workload = item["workload"]
            category = item["category"]
            query = item["query"]
            backends = item["backends"]
        except KeyError as exc:
            raise ValueError(
                f"Runtime corpus item {index} is missing required key {exc.args[0]!r}."
            ) from exc
        mode = item.get("mode", "statement")
        mutation = item.get("mutation", False)

        if not isinstance(name, str) or not name:
            raise ValueError(f"Runtime corpus item {index} has invalid 'name'.")
        if workload not in {"oltp", "olap"}:
            raise ValueError(f"Runtime corpus item {index} has invalid 'workload'.")
        if not isinstance(category, str) or not category:
            raise ValueError(f"Runtime corpus item {index} has invalid 'category'.")
        if not isinstance(query, str) or not query:
            raise ValueError(f"Runtime corpus item {index} has invalid 'query'.")
        if mode not in {"statement", "program"}:
            raise ValueError(f"Runtime corpus item {index} has invalid 'mode'.")
        if not isinstance(mutation, bool):
            raise ValueError(f"Runtime corpus item {index} has invalid 'mutation'.")
        if not isinstance(backends, list) or not backends:
            raise ValueError(f"Runtime corpus item {index} has invalid 'backends'.")

        queries.append(
            CorpusQuery(
                name=name,
                workload=workload,
                category=category,
                query=query,
                backends=tuple(backends),
                mode=mode,
                mutation=mutation,
            )
        )
    return queries


def _consume_result(result: object) -> None:
    if result is None:
        return
    list(result)


def _corpus_query_payload(query: CorpusQuery) -> dict[str, object]:
    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "query": query.query,
        "backends": list(query.backends),
        "mode": query.mode,
        "mutation": query.mutation,
    }


def _corpus_query_from_payload(payload: dict[str, object]) -> CorpusQuery:
    return CorpusQuery(
        name=str(payload["name"]),
        workload=str(payload["workload"]),
        category=str(payload["category"]),
        query=str(payload["query"]),
        backends=tuple(str(backend) for backend in payload["backends"]),
        mode=str(payload["mode"]),
        mutation=bool(payload["mutation"]),
    )


def _arcadedb_worker_fixture(
    *,
    db_path: Path,
    row_counts: dict[str, int],
    index_mode: str,
) -> ArcadeDBFixture:
    return ArcadeDBFixture(
        work_dir=ManagedDirectory(path=db_path.parent),
        db_path=db_path,
        database=_open_arcadedb(db_path),
        setup_metrics={},
        row_counts=row_counts,
        rss_snapshots_mib={},
        db_size_mib=0.0,
        wal_size_mib=0.0,
        index_mode=index_mode,
    )


def _append_arcadedb_worker_progress(
    progress_path: Path,
    *,
    phase: str,
    iteration: int,
) -> None:
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "phase": phase,
                    "iteration": iteration,
                }
            )
        )
        handle.write("\n")


def _next_arcadedb_worker_step(
    last_progress_event: dict[str, object] | None,
    *,
    warmup: int,
) -> tuple[str, int]:
    if last_progress_event is None:
        return "warmup", 1
    phase = str(last_progress_event["phase"])
    iteration = int(last_progress_event["iteration"])
    if phase == "ready":
        return "warmup", 1
    if phase == "warmup":
        if iteration < warmup:
            return "warmup", iteration + 1
        return "iteration", 1
    return "iteration", iteration + 1


def _arcadedb_timeout_result(
    *,
    query: CorpusQuery,
    index_mode: str,
    timeout_ms: float,
    last_progress_event: dict[str, object] | None,
    warmup: int,
) -> dict[str, object]:
    phase, iteration = _next_arcadedb_worker_step(
        last_progress_event,
        warmup=warmup,
    )
    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": ARCADEDB_BACKEND_NAME,
        "index_mode": index_mode,
        "mode": query.mode,
        "mutation": query.mutation,
        "status": "timed_out",
        "query_timeout": {
            "phase": phase,
            "timeout_ms": timeout_ms,
            "iteration": iteration,
        },
    }


def _read_arcadedb_worker_progress(
    progress_path: Path,
    *,
    offset: int,
) -> tuple[list[dict[str, object]], int]:
    if not progress_path.exists():
        return [], offset
    with progress_path.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        chunk = handle.read()
        new_offset = handle.tell()
    events = [
        json.loads(line)
        for line in chunk.splitlines()
        if line.strip()
    ]
    return events, new_offset


def _run_arcadedb_query_worker(spec_path: Path) -> int:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    query = _corpus_query_from_payload(spec["query"])
    warmup = int(spec["warmup"])
    iterations = int(spec["iterations"])
    progress_path = Path(spec["progress_path"])
    result_path = Path(spec["result_path"])
    index_mode = str(spec["index_mode"])

    fixture = _arcadedb_worker_fixture(
        db_path=Path(spec["db_path"]),
        row_counts={
            str(key): int(value) for key, value in dict(spec["row_counts"]).items()
        },
        index_mode=index_mode,
    )
    try:
        if warmup > 0 or iterations > 0:
            _run_query_once(fixture, query=query)
        _append_arcadedb_worker_progress(
            progress_path,
            phase="ready",
            iteration=0,
        )
        for warmup_index in range(1, warmup + 1):
            _run_query_once(fixture, query=query)
            _append_arcadedb_worker_progress(
                progress_path,
                phase="warmup",
                iteration=warmup_index,
            )

        execute_latencies: list[int] = []
        end_to_end_latencies: list[int] = []
        reset_latencies: list[int] = []
        for iteration_index in range(1, iterations + 1):
            metrics = _run_query_once(fixture, query=query)
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
            _append_arcadedb_worker_progress(
                progress_path,
                phase="iteration",
                iteration=iteration_index,
            )

        result_path.write_text(
            json.dumps(
                {
                    "name": query.name,
                    "workload": query.workload,
                    "category": query.category,
                    "backend": ARCADEDB_BACKEND_NAME,
                    "index_mode": index_mode,
                    "mode": query.mode,
                    "mutation": query.mutation,
                    "status": "passed",
                    "execute": _summarize(execute_latencies),
                    "end_to_end": _summarize(end_to_end_latencies),
                    "reset": _summarize(reset_latencies),
                }
            ),
            encoding="utf-8",
        )
        return 0
    except ARCADEDB_QUERY_EXCEPTIONS as exc:
        result_path.write_text(
            json.dumps(
                {
                    "name": query.name,
                    "workload": query.workload,
                    "category": query.category,
                    "backend": ARCADEDB_BACKEND_NAME,
                    "index_mode": index_mode,
                    "mode": query.mode,
                    "mutation": query.mutation,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            ),
            encoding="utf-8",
        )
        return 0
    finally:
        fixture.database.close()


def _measure_query_in_arcadedb_worker(
    fixture: ArcadeDBFixture,
    *,
    query: CorpusQuery,
    iterations: int,
    warmup: int,
    timeout_ms: float,
) -> dict[str, object]:
    query_slug = query.name.replace("/", "_")
    spec_path = fixture.work_dir.path / f"{query_slug}.worker-spec.json"
    progress_path = fixture.work_dir.path / f"{query_slug}.worker-progress.jsonl"
    result_path = fixture.work_dir.path / f"{query_slug}.worker-result.json"
    spec = {
        "db_path": str(fixture.db_path),
        "row_counts": fixture.row_counts,
        "index_mode": fixture.index_mode,
        "query": _corpus_query_payload(query),
        "warmup": warmup,
        "iterations": iterations,
        "progress_path": str(progress_path),
        "result_path": str(result_path),
    }
    spec_path.write_text(json.dumps(spec), encoding="utf-8")

    command = [
        sys.executable,
        "-m",
        "scripts.benchmarks.runtime.arcadedb_embedded",
        "--query-worker-spec",
        str(spec_path),
    ]
    process = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    timeout_s = timeout_ms / 1_000.0
    started_at = time.monotonic()
    last_progress_at = started_at
    last_progress_event: dict[str, object] | None = None
    progress_offset = 0
    stderr_output = ""
    worker_ready = False

    try:
        while True:
            events, progress_offset = _read_arcadedb_worker_progress(
                progress_path,
                offset=progress_offset,
            )
            if events:
                last_progress_event = events[-1]
                last_progress_at = time.monotonic()
                if any(str(event["phase"]) == "ready" for event in events):
                    worker_ready = True

            if result_path.exists():
                result = json.loads(result_path.read_text(encoding="utf-8"))
                try:
                    process.wait(timeout=ARCADEDB_WORKER_SHUTDOWN_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
                return result

            return_code = process.poll()
            if return_code is not None:
                _, stderr_output = process.communicate()
                break

            now = time.monotonic()
            if not worker_ready:
                if now - started_at > ARCADEDB_WORKER_STARTUP_TIMEOUT_S:
                    process.kill()
                    _, stderr_output = process.communicate()
                    return {
                        "name": query.name,
                        "workload": query.workload,
                        "category": query.category,
                        "backend": ARCADEDB_BACKEND_NAME,
                        "index_mode": fixture.index_mode,
                        "mode": query.mode,
                        "mutation": query.mutation,
                        "status": "failed",
                        "error_type": "WorkerStartupTimeout",
                        "error_message": (
                            "ArcadeDB worker did not become ready within "
                            f"{ARCADEDB_WORKER_STARTUP_TIMEOUT_S:.1f}s."
                        ),
                    }
                time.sleep(0.05)
                continue

            if now - last_progress_at > timeout_s:
                process.kill()
                _, stderr_output = process.communicate()
                return _arcadedb_timeout_result(
                    query=query,
                    index_mode=fixture.index_mode,
                    timeout_ms=timeout_ms,
                    last_progress_event=last_progress_event,
                    warmup=warmup,
                )

            time.sleep(0.05)

        events, progress_offset = _read_arcadedb_worker_progress(
            progress_path,
            offset=progress_offset,
        )
        if events:
            last_progress_event = events[-1]

        if result_path.exists():
            return json.loads(result_path.read_text(encoding="utf-8"))

        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": fixture.index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "failed",
            "error_type": "WorkerProcessError",
            "error_message": (
                stderr_output.strip()
                or f"ArcadeDB worker exited with code {process.returncode}."
            ),
        }
    finally:
        for path in (spec_path, progress_path, result_path):
            path.unlink(missing_ok=True)


def _run_query_once(
    fixture: ArcadeDBFixture,
    *,
    query: CorpusQuery,
) -> dict[str, int]:
    reset_ns = 0
    transaction_started = False
    query_text = _rewrite_arcadedb_query(fixture, query)

    try:
        if query.mutation:
            fixture.database.begin()
            transaction_started = True
            _, execute_ns = _measure_ns(
                lambda: _consume_result(
                    fixture.database.command("opencypher", query_text)
                )
            )
        else:
            _, execute_ns = _measure_ns(
                lambda: _consume_result(
                    fixture.database.query("opencypher", query_text)
                )
            )
        end_to_end_ns = execute_ns
    except Exception:
        if transaction_started:
            _rollback_arcadedb_transaction(fixture.database)
        raise

    if transaction_started:
        _, reset_ns = _measure_ns(
            lambda: _rollback_arcadedb_transaction(fixture.database)
        )

    return {
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
    }


def _measure_query(
    fixture: ArcadeDBFixture,
    *,
    query: CorpusQuery,
    iterations: int,
    warmup: int,
    progress_label: str,
    iteration_progress: bool,
    timeout_ms: float | None = None,
) -> dict[str, object]:
    if timeout_ms is not None:
        return _measure_query_in_arcadedb_worker(
            fixture,
            query=query,
            iterations=iterations,
            warmup=warmup,
            timeout_ms=timeout_ms,
        )

    warmup_iteration = 0
    try:
        for warmup_index in range(1, warmup + 1):
            warmup_iteration = warmup_index
            if iteration_progress:
                _progress_iteration(
                    progress_label,
                    phase="warmup",
                    current=warmup_index,
                    total=warmup,
                )
            _call_with_timeout(
                lambda: _run_query_once(fixture, query=query),
                timeout_ms=timeout_ms,
                operation=f"{ARCADEDB_BACKEND_NAME}:{query.name}:warmup",
            )
    except BenchmarkQueryTimeoutError:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": fixture.index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "timed_out",
            "query_timeout": {
                "phase": "warmup",
                "timeout_ms": timeout_ms,
                "iteration": warmup_iteration,
            },
        }
    except ARCADEDB_QUERY_EXCEPTIONS as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": fixture.index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

    execute_latencies: list[int] = []
    end_to_end_latencies: list[int] = []
    reset_latencies: list[int] = []

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for iteration_index in range(1, iterations + 1):
            if iteration_progress:
                _progress_iteration(
                    progress_label,
                    phase="iteration",
                    current=iteration_index,
                    total=iterations,
                )
            metrics = _call_with_timeout(
                lambda: _run_query_once(fixture, query=query),
                timeout_ms=timeout_ms,
                operation=f"{ARCADEDB_BACKEND_NAME}:{query.name}:iteration",
            )
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
    except BenchmarkQueryTimeoutError:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": fixture.index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "timed_out",
            "query_timeout": {
                "phase": "iteration",
                "timeout_ms": timeout_ms,
                "iteration": len(execute_latencies) + 1,
            },
        }
    except ARCADEDB_QUERY_EXCEPTIONS as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": fixture.index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": ARCADEDB_BACKEND_NAME,
        "index_mode": fixture.index_mode,
        "mode": query.mode,
        "mutation": query.mutation,
        "status": "passed",
        "execute": _summarize(execute_latencies),
        "end_to_end": _summarize(end_to_end_latencies),
        "reset": _summarize(reset_latencies),
    }


def _run_workload_suite(
    *,
    workload: str,
    index_mode: str,
    queries: list[CorpusQuery],
    iterations: int,
    warmup: int,
    graph_schema: cypherglot.GraphSchema,
    sqlite_source: GeneratedGraphFixture,
    ingest_batch_size: int,
    db_root_dir: Path | None,
    iteration_progress: bool,
    timeout_ms: float | None = None,
) -> dict[str, object]:
    suite_name = f"{workload}/{_suite_key(index_mode)}"
    fixture = _prepare_arcadedb_fixture(
        workload=workload,
        index_mode=index_mode,
        graph_schema=graph_schema,
        sqlite_source=sqlite_source,
        ingest_batch_size=ingest_batch_size,
        db_root_dir=db_root_dir,
    )
    try:
        rss_snapshots_mib = {
            key: dict(value)
            for key, value in fixture.rss_snapshots_mib.items()
        }
        if timeout_ms is not None:
            fixture.database.close()
        rss_snapshots_mib["suite_start"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )
        _progress(
            f"{suite_name}: starting suite with {len(queries)} queries "
            f"({iterations} iterations, {warmup} warmup)"
        )
        query_results = []
        for query_index, query in enumerate(queries, start=1):
            query_progress_label = (
                f"{suite_name}: query {query_index}/{len(queries)} {query.name}"
            )
            _progress(query_progress_label)
            query_results.append(
                _measure_query(
                    fixture,
                    query=query,
                    iterations=iterations,
                    warmup=warmup,
                    progress_label=query_progress_label,
                    iteration_progress=iteration_progress,
                    timeout_ms=timeout_ms,
                )
            )
        rss_snapshots_mib["suite_complete"] = _capture_rss_snapshot(
            backend=ARCADEDB_BACKEND_NAME
        )
        _progress(f"{suite_name}: suite complete")

        failures = [result for result in query_results if result["status"] == "failed"]
        timed_out = [result for result in query_results if result["status"] == "timed_out"]
        return {
            "backend": ARCADEDB_BACKEND_NAME,
            "index_mode": index_mode,
            "iterations": iterations,
            "warmup": warmup,
            "query_count": len(queries),
            "pass_count": len(query_results) - len(failures) - len(timed_out),
            "timeout_count": len(timed_out),
            "fail_count": len(failures),
            "setup": {
                "connect_ms": fixture.setup_metrics["connect_ns"] / 1_000_000.0,
                "schema_ms": fixture.setup_metrics["schema_ns"] / 1_000_000.0,
                "ingest_ms": fixture.setup_metrics["ingest_ns"] / 1_000_000.0,
                "index_ms": fixture.setup_metrics["index_ns"] / 1_000_000.0,
                "gav_ms": fixture.setup_metrics["gav_ns"] / 1_000_000.0,
                "checkpoint_ms": fixture.setup_metrics["checkpoint_ns"] / 1_000_000.0,
            },
            "row_counts": fixture.row_counts,
            "rss_snapshots_mib": rss_snapshots_mib,
            "storage": {
                "db_size_mib": fixture.db_size_mib,
                "wal_size_mib": fixture.wal_size_mib,
            },
            "db_path": str(fixture.db_path),
            "execute": _pool_summaries(query_results, "execute"),
            "end_to_end": _pool_summaries(query_results, "end_to_end"),
            "reset": _pool_summaries(query_results, "reset"),
            "queries": query_results,
        }
    finally:
        fixture.close()


def _index_modes(index_mode: str) -> tuple[str, ...]:
    if index_mode == "both":
        return ("indexed", "unindexed")
    return (index_mode,)


def _benchmark_result(
    *,
    queries: list[CorpusQuery],
    iterations: int,
    warmup: int,
    oltp_iterations: int | None,
    oltp_warmup: int | None,
    olap_iterations: int | None,
    olap_warmup: int | None,
    scale: RuntimeScale,
    index_mode: str,
    db_root_dir: Path | None,
    iteration_progress: bool,
    oltp_timeout_ms: float | None = None,
    olap_timeout_ms: float | None = None,
    progress_callback: RuntimeProgressCallback | None = None,
) -> tuple[dict[str, object], int]:
    graph_schema, edge_plans = _build_graph_schema(scale)
    token_map = _token_map(scale, graph_schema, edge_plans)
    rendered_queries = _render_corpus_queries(queries, token_map)

    oltp_queries = [query for query in rendered_queries if query.workload == "oltp"]
    olap_queries = [query for query in rendered_queries if query.workload == "olap"]
    oltp_iterations_value = iterations if oltp_iterations is None else oltp_iterations
    oltp_warmup_value = warmup if oltp_warmup is None else oltp_warmup
    olap_iterations_value = iterations if olap_iterations is None else olap_iterations
    olap_warmup_value = warmup if olap_warmup is None else olap_warmup

    def suite_kwargs(timeout_ms: float | None) -> dict[str, object]:
        kwargs: dict[str, object] = {"iteration_progress": iteration_progress}
        if timeout_ms is not None:
            kwargs["timeout_ms"] = timeout_ms
        return kwargs

    workloads: dict[str, object] = {}
    failure_count = 0
    if progress_callback is not None:
        progress_callback(
            {"workloads": workloads, "token_map": token_map},
            failure_count,
        )

    sqlite_source = _prepare_generated_graph_fixture(
        scale=scale,
        graph_schema=graph_schema,
        edge_plans=edge_plans,
        index_mode="unindexed",
        db_root_dir=db_root_dir,
    )
    try:
        for current_index_mode in _index_modes(index_mode):
            if oltp_queries:
                workloads.setdefault(
                    "oltp",
                    {
                        "description": (
                            "Transactional-style ArcadeDB Embedded execution "
                            "over the generated graph using the runtime corpus "
                            "directly as OpenCypher."
                        )
                    },
                )
                suite = _run_workload_suite(
                    workload="oltp",
                    index_mode=current_index_mode,
                    queries=oltp_queries,
                    iterations=oltp_iterations_value,
                    warmup=oltp_warmup_value,
                    graph_schema=graph_schema,
                    sqlite_source=sqlite_source,
                    ingest_batch_size=scale.ingest_batch_size,
                    db_root_dir=db_root_dir,
                    **suite_kwargs(oltp_timeout_ms),
                )
                workloads["oltp"][_suite_key(current_index_mode)] = suite
                failure_count += int(suite["fail_count"])
                failure_count += int(suite.get("timeout_count", 0))
                if progress_callback is not None:
                    progress_callback(
                        {"workloads": workloads, "token_map": token_map},
                        failure_count,
                    )

            if olap_queries:
                workloads.setdefault(
                    "olap",
                    {
                        "description": (
                            "Analytical-style ArcadeDB Embedded execution over "
                            "the generated graph using the runtime corpus "
                            "directly as OpenCypher, with a Graph Analytical "
                            "View prepared before the OLAP suite."
                        )
                    },
                )
                suite = _run_workload_suite(
                    workload="olap",
                    index_mode=current_index_mode,
                    queries=olap_queries,
                    iterations=olap_iterations_value,
                    warmup=olap_warmup_value,
                    graph_schema=graph_schema,
                    sqlite_source=sqlite_source,
                    ingest_batch_size=scale.ingest_batch_size,
                    db_root_dir=db_root_dir,
                    **suite_kwargs(olap_timeout_ms),
                )
                workloads["olap"][_suite_key(current_index_mode)] = suite
                failure_count += int(suite["fail_count"])
                failure_count += int(suite.get("timeout_count", 0))
                if progress_callback is not None:
                    progress_callback(
                        {"workloads": workloads, "token_map": token_map},
                        failure_count,
                    )
    finally:
        sqlite_source.close()

    return {
        "workloads": workloads,
        "token_map": token_map,
    }, failure_count


def _build_payload(
    *,
    started_at: datetime,
    database_versions: dict[str, str],
    corpus_path: Path,
    queries: list[CorpusQuery],
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    index_mode: str,
    default_iterations: int,
    default_warmup: int,
    oltp_iterations: int,
    oltp_warmup: int,
    oltp_timeout_ms: float | None = None,
    olap_iterations: int,
    olap_warmup: int,
    olap_timeout_ms: float | None = None,
    db_root_dir: Path | None,
    result: dict[str, object],
    failure_count: int,
    status: str,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "benchmark_entrypoint": ARCADEDB_BACKEND_NAME,
        "enabled_backends": [ARCADEDB_BACKEND_NAME],
        "generated_at": started_at.isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "run_status": status,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
        "database_versions": database_versions,
        "corpus_path": str(corpus_path),
        "workload_counts": {
            "oltp": len([query for query in queries if query.workload == "oltp"]),
            "olap": len([query for query in queries if query.workload == "olap"]),
        },
        "graph_scale": {
            "node_type_count": scale.node_type_count,
            "edge_type_count": scale.edge_type_count,
            "nodes_per_type": scale.nodes_per_type,
            "edges_per_source": scale.edges_per_source,
            "edge_degree_profile": scale.edge_degree_profile,
            "average_edges_per_source": _average_edges_per_source(scale),
            "total_nodes": scale.total_nodes,
            "total_edges": scale.total_edges,
            "node_extra_text_property_count": scale.node_extra_text_property_count,
            "node_extra_numeric_property_count": (
                scale.node_extra_numeric_property_count
            ),
            "node_extra_boolean_property_count": (
                scale.node_extra_boolean_property_count
            ),
            "edge_extra_text_property_count": scale.edge_extra_text_property_count,
            "edge_extra_numeric_property_count": (
                scale.edge_extra_numeric_property_count
            ),
            "edge_extra_boolean_property_count": (
                scale.edge_extra_boolean_property_count
            ),
            "ingest_batch_size": scale.ingest_batch_size,
            "variable_hop_max": scale.variable_hop_max,
        },
        "schema_contract": {
            "layout": "property-graph",
            "node_labels": [node_type.name for node_type in graph_schema.node_types],
            "relationship_types": [
                edge_type.name for edge_type in graph_schema.edge_types
            ],
        },
        "index_mode": index_mode,
        "workload_controls": {
            "default_iterations": default_iterations,
            "default_warmup": default_warmup,
            "oltp_iterations": oltp_iterations,
            "oltp_warmup": oltp_warmup,
            "oltp_timeout_ms": oltp_timeout_ms,
            "olap_iterations": olap_iterations,
            "olap_warmup": olap_warmup,
            "olap_timeout_ms": olap_timeout_ms,
        },
        "db_root_dir": str(db_root_dir) if db_root_dir is not None else None,
        "results": result,
        "failure_count": failure_count,
    }
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    return payload


def _print_suite(name: str, suite: dict[str, object]) -> None:
    print(name)
    print(
        "  setup: "
        f"connect={suite['setup']['connect_ms']:.2f} ms, "
        f"schema={suite['setup']['schema_ms']:.2f} ms, "
        f"ingest={suite['setup']['ingest_ms']:.2f} ms, "
        f"index={suite['setup']['index_ms']:.2f} ms, "
        f"checkpoint={suite['setup']['checkpoint_ms']:.2f} ms"
    )
    print(
        "  storage: "
        f"db={suite['storage']['db_size_mib']:.2f} MiB, "
        f"wal={suite['storage']['wal_size_mib']:.2f} MiB"
    )
    print(
        "  status: "
        f"passed={suite['pass_count']}, "
        f"timed_out={suite.get('timeout_count', 0)}, failed={suite['fail_count']}"
    )
    if suite["pass_count"]:
        print(
            "  pooled execute: "
            f"mean={suite['execute']['mean_of_mean_ms']:.2f} ms, "
            f"p50={suite['execute']['mean_of_p50_ms']:.2f} ms, "
            f"p95={suite['execute']['mean_of_p95_ms']:.2f} ms"
        )
        print(
            "  pooled end-to-end: "
            f"mean={suite['end_to_end']['mean_of_mean_ms']:.2f} ms, "
            f"p50={suite['end_to_end']['mean_of_p50_ms']:.2f} ms, "
            f"p95={suite['end_to_end']['mean_of_p95_ms']:.2f} ms"
        )
    for query_result in suite["queries"]:
        if query_result["status"] == "passed":
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                f"execute_p50={query_result['execute']['p50_ms']:.2f} ms, "
                f"execute_p95={query_result['execute']['p95_ms']:.2f} ms, "
                f"end_to_end_p50={query_result['end_to_end']['p50_ms']:.2f} ms, "
                f"end_to_end_p95={query_result['end_to_end']['p95_ms']:.2f} ms"
            )
            continue
        if query_result["status"] == "timed_out":
            timeout = query_result["query_timeout"]
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                "TIMED OUT "
                f"(phase={timeout['phase']}, iteration={timeout['iteration']}, "
                f"timeout={timeout['timeout_ms']:.2f} ms)"
            )
            continue
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"FAILED {query_result['error_type']}: {query_result['error_message']}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and benchmark the runtime Cypher corpus directly on "
            "ArcadeDB Embedded using the same synthetic graph shape as the "
            "SQL runtime harnesses."
        )
    )
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--oltp-iterations", type=int)
    parser.add_argument("--oltp-warmup", type=int)
    parser.add_argument("--olap-iterations", type=int)
    parser.add_argument("--olap-warmup", type=int)
    parser.add_argument("--oltp-timeout-ms", type=float)
    parser.add_argument("--olap-timeout-ms", type=float)
    parser.add_argument(
        "--index-mode",
        choices=INDEX_MODE_CHOICES,
        default="both",
        help="Run the ArcadeDB benchmark with indexed, unindexed, or both modes.",
    )
    parser.add_argument(
        "--iteration-progress",
        action="store_true",
        help="Print warmup and measured iteration counters for each query.",
    )
    parser.add_argument("--query-name", action="append", dest="query_names")
    parser.add_argument("--node-type-count", type=int, default=4)
    parser.add_argument("--edge-type-count", type=int, default=4)
    parser.add_argument("--nodes-per-type", type=int, default=25_000)
    parser.add_argument("--edges-per-source", type=int, default=3)
    parser.add_argument(
        "--edge-degree-profile",
        choices=("uniform", "skewed"),
        default="uniform",
    )
    parser.add_argument("--node-extra-text-property-count", type=int, default=2)
    parser.add_argument("--node-extra-numeric-property-count", type=int, default=6)
    parser.add_argument("--node-extra-boolean-property-count", type=int, default=2)
    parser.add_argument("--edge-extra-text-property-count", type=int, default=1)
    parser.add_argument("--edge-extra-numeric-property-count", type=int, default=3)
    parser.add_argument("--edge-extra-boolean-property-count", type=int, default=1)
    parser.add_argument("--variable-hop-max", type=int, default=2)
    parser.add_argument("--ingest-batch-size", type=int, default=5_000)
    parser.add_argument(
        "--db-root-dir",
        type=Path,
        help=(
            "Optional directory where intermediate SQLite and ArcadeDB Embedded "
            "database artifacts should be created instead of using temporary "
            "directories."
        ),
    )
    parser.add_argument(
        "--query-worker-spec",
        type=Path,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.query_worker_spec is not None:
        return _run_arcadedb_query_worker(args.query_worker_spec)

    started_at = datetime.now(UTC)
    args_dict = vars(args)
    oltp_timeout_ms = args_dict.get("oltp_timeout_ms", None)
    olap_timeout_ms = args_dict.get("olap_timeout_ms", None)
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be non-negative.")
    if args.oltp_iterations is not None and args.oltp_iterations <= 0:
        raise ValueError("--oltp-iterations must be positive.")
    if args.oltp_warmup is not None and args.oltp_warmup < 0:
        raise ValueError("--oltp-warmup must be non-negative.")
    if args.olap_iterations is not None and args.olap_iterations <= 0:
        raise ValueError("--olap-iterations must be positive.")
    if args.olap_warmup is not None and args.olap_warmup < 0:
        raise ValueError("--olap-warmup must be non-negative.")
    if oltp_timeout_ms is not None and oltp_timeout_ms <= 0:
        raise ValueError("--oltp-timeout-ms must be positive.")
    if olap_timeout_ms is not None and olap_timeout_ms <= 0:
        raise ValueError("--olap-timeout-ms must be positive.")
    if args.node_type_count <= 0:
        raise ValueError("--node-type-count must be positive.")
    if args.edge_type_count <= 0:
        raise ValueError("--edge-type-count must be positive.")
    if args.nodes_per_type <= 0:
        raise ValueError("--nodes-per-type must be positive.")
    if args.edges_per_source <= 0:
        raise ValueError("--edges-per-source must be positive.")
    if args.variable_hop_max <= 0:
        raise ValueError("--variable-hop-max must be positive.")
    if args.ingest_batch_size <= 0:
        raise ValueError("--ingest-batch-size must be positive.")
    if args.db_root_dir is not None:
        args.db_root_dir.mkdir(parents=True, exist_ok=True)
    if not _arcadedb_available():
        raise ValueError(
            "arcadedb-embedded is not installed. Install it with "
            "`uv pip install arcadedb-embedded` or a dev build such as "
            "`uv pip install arcadedb-embedded==26.4.1.dev3`."
        )

    scale = RuntimeScale(
        node_type_count=args.node_type_count,
        edge_type_count=args.edge_type_count,
        nodes_per_type=args.nodes_per_type,
        edges_per_source=args.edges_per_source,
        edge_degree_profile=args.edge_degree_profile,
        node_extra_text_property_count=args.node_extra_text_property_count,
        node_extra_numeric_property_count=args.node_extra_numeric_property_count,
        node_extra_boolean_property_count=args.node_extra_boolean_property_count,
        edge_extra_text_property_count=args.edge_extra_text_property_count,
        edge_extra_numeric_property_count=args.edge_extra_numeric_property_count,
        edge_extra_boolean_property_count=args.edge_extra_boolean_property_count,
        ingest_batch_size=args.ingest_batch_size,
        variable_hop_max=args.variable_hop_max,
    )
    graph_schema, _ = _build_graph_schema(scale)
    queries = _select_queries(_load_corpus(args.corpus), args.query_names)
    database_versions: dict[str, str] = {}
    version = _arcadedb_version()
    if version is not None:
        database_versions[ARCADEDB_BACKEND_NAME] = version

    _progress(
        f"{ARCADEDB_BACKEND_NAME} runtime benchmark: starting "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, index_mode={args.index_mode})"
    )

    def write_checkpoint(
        result: dict[str, object],
        *,
        failure_count: int,
        status: str,
    ) -> None:
        payload = _build_payload(
            started_at=started_at,
            database_versions=database_versions,
            corpus_path=args.corpus,
            queries=queries,
            scale=scale,
            graph_schema=graph_schema,
            index_mode=args.index_mode,
            default_iterations=args.iterations,
            default_warmup=args.warmup,
            oltp_iterations=(
                args.oltp_iterations
                if args.oltp_iterations is not None
                else args.iterations
            ),
            oltp_warmup=(
                args.oltp_warmup if args.oltp_warmup is not None else args.warmup
            ),
            oltp_timeout_ms=oltp_timeout_ms,
            olap_iterations=(
                args.olap_iterations
                if args.olap_iterations is not None
                else args.iterations
            ),
            olap_warmup=(
                args.olap_warmup if args.olap_warmup is not None else args.warmup
            ),
            olap_timeout_ms=olap_timeout_ms,
            db_root_dir=args.db_root_dir,
            result=result,
            failure_count=failure_count,
            status=status,
            completed_at=datetime.now(UTC) if status == "completed" else None,
        )
        _write_json_atomic(args.output, payload)

    write_checkpoint(
        {"workloads": {}, "token_map": {}},
        failure_count=0,
        status="running",
    )

    result, failure_count = _benchmark_result(
        queries=queries,
        iterations=args.iterations,
        warmup=args.warmup,
        oltp_iterations=args.oltp_iterations,
        oltp_warmup=args.oltp_warmup,
        olap_iterations=args.olap_iterations,
        olap_warmup=args.olap_warmup,
        scale=scale,
        index_mode=args.index_mode,
        db_root_dir=args.db_root_dir,
        iteration_progress=args.iteration_progress,
        oltp_timeout_ms=oltp_timeout_ms,
        olap_timeout_ms=olap_timeout_ms,
        progress_callback=(
            lambda partial_result, partial_failure_count: write_checkpoint(
                partial_result,
                failure_count=partial_failure_count,
                status="running",
            )
        ),
    )

    write_checkpoint(
        result,
        failure_count=failure_count,
        status="completed",
    )

    _progress(
        f"{ARCADEDB_BACKEND_NAME} runtime benchmark: wrote baseline to {args.output}"
    )
    print(f"Wrote ArcadeDB Embedded runtime benchmark baseline to {args.output}")
    workloads = result["workloads"]
    if "oltp" in workloads:
        for suite_name, suite in workloads["oltp"].items():
            if suite_name == "description":
                continue
            _print_suite(f"oltp/{suite_name}", suite)
    if "olap" in workloads:
        for suite_name, suite in workloads["olap"].items():
            if suite_name == "description":
                continue
            _print_suite(f"olap/{suite_name}", suite)
    return 1 if failure_count else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
