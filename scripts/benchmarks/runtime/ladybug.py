"""Validate and benchmark the runtime corpus directly on Ladybug.

This script seeds the same synthetic graph shape used by the SQL runtime
benchmarks into an on-disk Ladybug database, then executes the runtime corpus
directly as Cypher. Ladybug is currently benchmarked in an unindexed query mode:
primary-key behavior comes from the database itself, but the benchmark does not
attempt to create any additional query indexes because Ladybug's public Python
surface does not currently expose the same secondary-index DDL used elsewhere in
this suite.
"""

from __future__ import annotations

import argparse
import csv
import gc
import json
import platform
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import cypherglot

from scripts.benchmarks.common.shared import (
    CorpusQuery,
    RuntimeScale,
    _average_edges_per_source,
    _build_graph_schema,
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
    import ladybug
except ImportError:  # pragma: no cover - optional dependency
    ladybug = None


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
DEFAULT_OUTPUT_PATH = DEFAULT_RUNTIME_RESULTS_DIR / "ladybug_runtime_benchmark.json"
LADYBUG_INDEX_MODE = "unindexed"
RuntimeProgressCallback = Callable[[dict[str, object], int], None]


@dataclass(slots=True)
class LadybugFixture:
    work_dir: ManagedDirectory
    db_path: Path
    database: Any
    connection: Any
    setup_metrics: dict[str, int]
    row_counts: dict[str, int]
    rss_snapshots_mib: dict[str, dict[str, float | None]]
    db_size_mib: float
    wal_size_mib: float

    def close(self) -> None:
        self.connection.close()
        self.database.close()
        self.work_dir.close()


def _ladybug_available() -> bool:
    return ladybug is not None


def _ladybug_version() -> str | None:
    if ladybug is None:
        return None
    version = getattr(ladybug, "__version__", None)
    if version is None:
        return None
    return str(version)


def _open_ladybug(db_path: Path) -> tuple[Any, Any]:
    if ladybug is None:
        raise ValueError(
            "ladybug is not installed. Install it with `uv pip install ladybug`."
        )
    database = ladybug.Database(str(db_path))
    connection = ladybug.Connection(database)
    return database, connection


def _ladybug_file_size_mib(path: Path) -> float:
    if not path.exists():
        return 0.0
    return path.stat().st_size / (1024.0 * 1024.0)


def _ladybug_storage_sizes(db_path: Path) -> tuple[float, float]:
    wal_path = Path(f"{db_path}.wal")
    return _ladybug_file_size_mib(db_path), _ladybug_file_size_mib(wal_path)


def _ladybug_type_name(type_name: str) -> str:
    if type_name == "boolean":
        return "BOOLEAN"
    if type_name == "integer":
        return "INT64"
    if type_name == "float":
        return "DOUBLE"
    return "STRING"


def _create_ladybug_schema(
    conn: Any,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for node_type in graph_schema.node_types:
        columns = ["id INT64 PRIMARY KEY"]
        columns.extend(
            f"{property_schema.name} "
            f"{_ladybug_type_name(property_schema.logical_type)}"
            for property_schema in node_type.properties
        )
        conn.execute(f"CREATE NODE TABLE {node_type.name}({', '.join(columns)})")

    for edge_type in graph_schema.edge_types:
        columns = [f"FROM {edge_type.source_type} TO {edge_type.target_type}"]
        columns.extend(
            f"{property_schema.name} "
            f"{_ladybug_type_name(property_schema.logical_type)}"
            for property_schema in edge_type.properties
        )
        conn.execute(f"CREATE REL TABLE {edge_type.name}({', '.join(columns)})")


def _rewrite_fixture_edge_csv(
    fixture: GeneratedGraphFixture,
    *,
    table_name: str,
    csv_path: Path,
) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        with fixture.table_csv_paths[table_name].open(
            "r",
            encoding="utf-8",
            newline="",
        ) as source_handle:
            reader = csv.DictReader(source_handle)
            fieldnames = [
                fieldname
                for fieldname in reader.fieldnames or []
                if fieldname != "id"
            ]
            output_header = _ladybug_copy_column_names(fieldnames)
            writer = csv.DictWriter(handle, fieldnames=output_header)
            writer.writeheader()
            for row in reader:
                writer.writerow(
                    {
                        output_name: row[input_name]
                        for input_name, output_name in zip(
                            fieldnames,
                            output_header,
                            strict=True,
                        )
                    }
                )


def _ladybug_copy_column_names(column_names: list[str]) -> list[str]:
    return [
        (
            "from"
            if column_name == "from_id"
            else "to" if column_name == "to_id" else column_name
        )
        for column_name in column_names
    ]


def _seed_ladybug_from_fixture(
    conn: Any,
    *,
    sqlite_source: GeneratedGraphFixture,
    graph_schema: cypherglot.GraphSchema,
    output_dir: Path,
) -> dict[str, int]:
    for node_type in graph_schema.node_types:
        table_name = node_type.table_name
        csv_path = sqlite_source.table_csv_paths[table_name]
        conn.execute(f'COPY {node_type.name} FROM "{csv_path}" (header=true)')

    for edge_type in graph_schema.edge_types:
        table_name = edge_type.table_name
        csv_path = output_dir / f"{table_name}.csv"
        _rewrite_fixture_edge_csv(
            sqlite_source,
            table_name=table_name,
            csv_path=csv_path,
        )
        conn.execute(f'COPY {edge_type.name} FROM "{csv_path}" (header=true)')

    return dict(sqlite_source.row_counts)


def _checkpoint_ladybug(conn: Any) -> None:
    conn.execute("CHECKPOINT")


def _rollback_ladybug_transaction(conn: Any) -> None:
    try:
        conn.execute("ROLLBACK")
    except RuntimeError as exc:
        if "No active transaction" not in str(exc):
            raise


def _rewrite_ladybug_query(
    fixture: LadybugFixture,
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
    if query.name == "olap_graph_introspection_rollup":
        return query.query.replace(
            (
                "type(r) AS rel_type, startNode(r).id AS start_id, "
                "endNode(r).id AS end_id"
            ),
            "'EdgeType02' AS rel_type, a.id AS start_id, b.id AS end_id",
            1,
        )
    return query.query


def _prepare_ladybug_fixture(
    *,
    workload: str,
    graph_schema: cypherglot.GraphSchema,
    sqlite_source: GeneratedGraphFixture,
    db_root_dir: Path | None,
) -> LadybugFixture:
    suite_name = f"ladybug-{workload}-{LADYBUG_INDEX_MODE}-suite"
    progress_label = f"{workload}/ladybug_{LADYBUG_INDEX_MODE}"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix="cypherglot-runtime-ladybug-suite-",
        name=suite_name,
    )
    db_path = work_dir.path / "runtime.lbug"
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}

    _progress(
        f"{progress_label}: creating fixture "
        f"({sqlite_source.row_counts['node_count']} nodes, "
        f"{sqlite_source.row_counts['edge_count']} edges)"
    )
    (database, connection), connect_ns = _measure_ns(lambda: _open_ladybug(db_path))
    rss_snapshots_mib["after_connect"] = _capture_rss_snapshot(backend="ladybug")
    try:
        _progress(f"{progress_label}: creating schema")
        _, schema_ns = _measure_ns(
            lambda: _create_ladybug_schema(connection, graph_schema)
        )
        rss_snapshots_mib["after_schema"] = _capture_rss_snapshot(backend="ladybug")

        _progress(
            f"{progress_label}: ingesting from generated fixture "
            f"({sqlite_source.csv_dir})"
        )
        row_counts, ingest_ns = _measure_ns(
            lambda: _seed_ladybug_from_fixture(
                connection,
                sqlite_source=sqlite_source,
                graph_schema=graph_schema,
                output_dir=work_dir.path,
            )
        )
        rss_snapshots_mib["after_ingest"] = _capture_rss_snapshot(backend="ladybug")

        index_ns = 0
        rss_snapshots_mib["after_index"] = _capture_rss_snapshot(backend="ladybug")

        _progress(f"{progress_label}: checkpointing post-load state")
        _, checkpoint_ns = _measure_ns(lambda: _checkpoint_ladybug(connection))
        rss_snapshots_mib["after_checkpoint"] = _capture_rss_snapshot(
            backend="ladybug"
        )
    except Exception:
        connection.close()
        database.close()
        work_dir.close()
        raise

    db_size_mib, wal_size_mib = _ladybug_storage_sizes(db_path)
    _progress(
        f"{progress_label}: fixture ready "
        f"(ingest={ingest_ns / 1_000_000_000.0:.2f}s, db={db_size_mib:.2f} MiB)"
    )
    return LadybugFixture(
        work_dir=work_dir,
        db_path=db_path,
        database=database,
        connection=connection,
        setup_metrics={
            "connect_ns": connect_ns,
            "schema_ns": schema_ns,
            "ingest_ns": ingest_ns,
            "index_ns": index_ns,
            "checkpoint_ns": checkpoint_ns,
        },
        row_counts=row_counts,
        rss_snapshots_mib=rss_snapshots_mib,
        db_size_mib=db_size_mib,
        wal_size_mib=wal_size_mib,
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


def _run_query_once(
    fixture: LadybugFixture,
    *,
    query: CorpusQuery,
) -> dict[str, int]:
    reset_ns = 0
    transaction_started = False
    try:
        if query.mutation:
            fixture.connection.execute("BEGIN TRANSACTION")
            transaction_started = True

        query_text = _rewrite_ladybug_query(fixture, query)
        _, execute_ns = _measure_ns(
            lambda: list(fixture.connection.execute(query_text))
        )
        end_to_end_ns = execute_ns
    except Exception:
        if transaction_started:
            _rollback_ladybug_transaction(fixture.connection)
        raise

    if transaction_started:
        _, reset_ns = _measure_ns(
            lambda: _rollback_ladybug_transaction(fixture.connection)
        )

    return {
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
    }


def _measure_query(
    fixture: LadybugFixture,
    *,
    query: CorpusQuery,
    iterations: int,
    warmup: int,
    progress_label: str,
    iteration_progress: bool,
) -> dict[str, object]:
    try:
        for warmup_index in range(1, warmup + 1):
            if iteration_progress:
                _progress_iteration(
                    progress_label,
                    phase="warmup",
                    current=warmup_index,
                    total=warmup,
                )
            _run_query_once(fixture, query=query)
    except (RuntimeError, ValueError) as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": "ladybug",
            "index_mode": LADYBUG_INDEX_MODE,
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
            metrics = _run_query_once(fixture, query=query)
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
    except (RuntimeError, ValueError) as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": "ladybug",
            "index_mode": LADYBUG_INDEX_MODE,
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
        "backend": "ladybug",
        "index_mode": LADYBUG_INDEX_MODE,
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
    queries: list[CorpusQuery],
    iterations: int,
    warmup: int,
    graph_schema: cypherglot.GraphSchema,
    sqlite_source: GeneratedGraphFixture,
    db_root_dir: Path | None,
    iteration_progress: bool,
) -> dict[str, object]:
    suite_name = f"{workload}/ladybug_{LADYBUG_INDEX_MODE}"
    fixture = _prepare_ladybug_fixture(
        workload=workload,
        graph_schema=graph_schema,
        sqlite_source=sqlite_source,
        db_root_dir=db_root_dir,
    )
    try:
        rss_snapshots_mib = {
            key: dict(value)
            for key, value in fixture.rss_snapshots_mib.items()
        }
        rss_snapshots_mib["suite_start"] = _capture_rss_snapshot(backend="ladybug")
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
                )
            )
        rss_snapshots_mib["suite_complete"] = _capture_rss_snapshot(backend="ladybug")
        _progress(f"{suite_name}: suite complete")

        failures = [result for result in query_results if result["status"] == "failed"]
        return {
            "backend": "ladybug",
            "index_mode": LADYBUG_INDEX_MODE,
            "iterations": iterations,
            "warmup": warmup,
            "query_count": len(queries),
            "pass_count": len(query_results) - len(failures),
            "fail_count": len(failures),
            "setup": {
                "connect_ms": fixture.setup_metrics["connect_ns"] / 1_000_000.0,
                "schema_ms": fixture.setup_metrics["schema_ns"] / 1_000_000.0,
                "ingest_ms": fixture.setup_metrics["ingest_ns"] / 1_000_000.0,
                "index_ms": fixture.setup_metrics["index_ns"] / 1_000_000.0,
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
    db_root_dir: Path | None,
    iteration_progress: bool,
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
        index_mode=LADYBUG_INDEX_MODE,
        db_root_dir=db_root_dir,
    )
    try:
        if oltp_queries:
            workloads.setdefault(
                "oltp",
                {
                    "description": (
                        "Transactional-style Ladybug execution over the generated "
                        "graph using the runtime corpus directly as Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                workload="oltp",
                queries=oltp_queries,
                iterations=oltp_iterations_value,
                warmup=oltp_warmup_value,
                graph_schema=graph_schema,
                sqlite_source=sqlite_source,
                db_root_dir=db_root_dir,
                iteration_progress=iteration_progress,
            )
            workloads["oltp"]["ladybug_unindexed"] = suite
            failure_count += int(suite["fail_count"])
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
                        "Analytical-style Ladybug execution over the generated "
                        "graph using the runtime corpus directly as Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                workload="olap",
                queries=olap_queries,
                iterations=olap_iterations_value,
                warmup=olap_warmup_value,
                graph_schema=graph_schema,
                sqlite_source=sqlite_source,
                db_root_dir=db_root_dir,
                iteration_progress=iteration_progress,
            )
            workloads["olap"]["ladybug_unindexed"] = suite
            failure_count += int(suite["fail_count"])
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
    default_iterations: int,
    default_warmup: int,
    oltp_iterations: int,
    oltp_warmup: int,
    olap_iterations: int,
    olap_warmup: int,
    db_root_dir: Path | None,
    result: dict[str, object],
    failure_count: int,
    status: str,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "benchmark_entrypoint": "ladybug",
        "enabled_backends": ["ladybug"],
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
        "index_mode": LADYBUG_INDEX_MODE,
        "workload_controls": {
            "default_iterations": default_iterations,
            "default_warmup": default_warmup,
            "oltp_iterations": oltp_iterations,
            "oltp_warmup": oltp_warmup,
            "olap_iterations": olap_iterations,
            "olap_warmup": olap_warmup,
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
        f"passed={suite['pass_count']}, failed={suite['fail_count']}"
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and benchmark the runtime Cypher corpus directly on Ladybug "
            "using the same synthetic graph shape as the SQL runtime harnesses."
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
            "Optional directory where intermediate SQLite and Ladybug database "
            "artifacts should be created instead of using temporary directories."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    started_at = datetime.now(UTC)
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
    if not _ladybug_available():
        raise ValueError(
            "ladybug is not installed. Install it with `uv pip install ladybug`."
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
    version = _ladybug_version()
    if version is not None:
        database_versions["ladybug"] = version

    _progress(
        "ladybug runtime benchmark: starting "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, index_mode={LADYBUG_INDEX_MODE})"
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
            olap_iterations=(
                args.olap_iterations
                if args.olap_iterations is not None
                else args.iterations
            ),
            olap_warmup=(
                args.olap_warmup if args.olap_warmup is not None else args.warmup
            ),
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
        db_root_dir=args.db_root_dir,
        iteration_progress=args.iteration_progress,
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

    _progress(f"ladybug runtime benchmark: wrote baseline to {args.output}")
    print(f"Wrote Ladybug runtime benchmark baseline to {args.output}")
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
