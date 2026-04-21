"""Shared orchestration core for the SQL runtime benchmark entrypoints."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import cypherglot

from _benchmark_cli_helpers import parse_sql_runtime_args
from _benchmark_common import (
    CorpusQuery,
    RuntimeScale,
    RuntimeProgressCallback,
    _average_edges_per_source,
    _build_graph_schema,
    _edge_out_degree as _shared_edge_out_degree,
    _measure_ns,
    _progress,
    _progress_iteration,
    _render_corpus_queries,
    _select_queries,
    _summarize,
    _token_map,
    _write_json_atomic,
)
from _postgres_runtime_support import (
    acquire_postgresql_benchmark_dsn,
    postgresql_benchmark_server_rss_mib,
    release_postgresql_benchmark_dsn,
)
from _benchmark_sql_runtime_duckdb_backend import (
    _analyze_duckdb,
    _create_duckdb_connection,
    _create_duckdb_schema,
    _duckdb_file_size_mib,
    _duckdb_available,
    _duckdb_version,
    _execute_duckdb_program,
    _seed_duckdb_from_fixture,
)
from _benchmark_sql_runtime_postgresql_backend import (
    _analyze_postgresql,
    _configure_postgresql_indexes,
    _create_postgresql_connection,
    _create_postgresql_schema,
    _execute_bound_postgresql_sql,
    _execute_postgresql_program,
    _postgresql_available,
    _postgresql_server_version,
    _reset_postgresql_schema,
    _seed_postgresql_from_fixture,
)
from _benchmark_sql_runtime_shared import (
    DuckDBConnection,
    GeneratedGraphFixture,
    ManagedDirectory,
    PostgreSQLConnection,
    PreparedArtifact,
    _capture_rss_snapshot,
    _create_managed_directory,
    _prepare_generated_graph_fixture,
    _summarize_rss_samples,
)
from _benchmark_sql_runtime_sqlite_backend import (
    _analyze_sqlite,
    _configure_sqlite_indexes,
    _create_sqlite_connection,
    _create_sqlite_schema,
    _execute_sqlite_program,
    _rollback_sqlite_iteration,
    _seed_sqlite_from_generated_fixture,
    _sqlite_file_size_mib,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
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
    DEFAULT_RUNTIME_RESULTS_DIR
    / "sqlite_runtime_benchmark_baseline.json"
)
DEFAULT_DUCKDB_OUTPUT_PATH = (
    DEFAULT_RUNTIME_RESULTS_DIR
    / "duckdb_runtime_benchmark_baseline.json"
)
DEFAULT_POSTGRESQL_OUTPUT_PATH = (
    DEFAULT_RUNTIME_RESULTS_DIR
    / "postgresql_runtime_benchmark_baseline.json"
)
SQLITE_SAVEPOINT = "benchmark_iteration"


@dataclass(frozen=True, slots=True)
class SQLRuntimeBenchmarkEntrypoint:
    name: str
    description: str
    default_output_path: Path
    enabled_backends: tuple[str, ...]
    default_index_mode: str = "both"
    index_mode_choices: tuple[str, ...] = ("indexed", "unindexed", "both")


SQLITE_ENTRYPOINT = SQLRuntimeBenchmarkEntrypoint(
    name="sqlite",
    description=(
        "Benchmark SQLite-backed OLTP and OLAP runtime over a generated multi-type "
        "type-aware graph."
    ),
    default_output_path=DEFAULT_OUTPUT_PATH,
    enabled_backends=("sqlite",),
)

DUCKDB_ENTRYPOINT = SQLRuntimeBenchmarkEntrypoint(
    name="duckdb",
    description=(
        "Benchmark DuckDB-backed OLTP and OLAP runtime over the generated "
        "type-aware graph."
    ),
    default_output_path=DEFAULT_DUCKDB_OUTPUT_PATH,
    enabled_backends=("duckdb",),
    default_index_mode="unindexed",
    index_mode_choices=("unindexed",),
)

POSTGRESQL_ENTRYPOINT = SQLRuntimeBenchmarkEntrypoint(
    name="postgresql",
    description=(
        "Benchmark PostgreSQL-backed OLTP and OLAP runtime over a generated "
        "multi-type type-aware graph."
    ),
    default_output_path=DEFAULT_POSTGRESQL_OUTPUT_PATH,
    enabled_backends=("postgresql",),
)


_edge_out_degree = _shared_edge_out_degree


class _BackendRunner:
    def __init__(
        self,
        backend: str,
        work_dir: ManagedDirectory,
        *,
        graph_schema: cypherglot.GraphSchema,
        schema_context: cypherglot.CompilerSchemaContext,
        sqlite_source: GeneratedGraphFixture,
        postgres_dsn: str | None = None,
    ) -> None:
        self.backend = backend
        self.work_dir = work_dir
        self.graph_schema = graph_schema
        self.schema_context = schema_context
        self.sqlite_source = sqlite_source
        self.postgres_dsn = postgres_dsn
        self.setup_metrics: dict[str, int] = {}
        self.row_counts: dict[str, int] = {}
        self.rss_snapshots_mib: dict[str, dict[str, float | None]] = {}
        self.db_size_mib = 0.0
        self.wal_size_mib = 0.0
        self.index_mode = sqlite_source.index_mode
        self.artifact_path: Path | None = None
        self.connection: sqlite3.Connection | DuckDBConnection | PostgreSQLConnection
        self._initialize()

    def _initialize(self) -> None:
        if self.backend == "sqlite":
            db_path = self.work_dir.path / "runtime.sqlite3"
            self.connection, connect_ns = _measure_ns(
                lambda: _create_sqlite_connection(db_path)
            )
            self.setup_metrics["connect_ns"] = connect_ns
            self.rss_snapshots_mib["after_connect"] = self.capture_rss_snapshot()
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _create_sqlite_schema(self.sqlite, self.graph_schema)
            )
            self.rss_snapshots_mib["after_schema"] = self.capture_rss_snapshot()
            _progress(
                f"sqlite/{self.index_mode}: ingesting from generated fixture "
                f"({self.sqlite_source.csv_dir})"
            )
            self.row_counts, self.setup_metrics["ingest_ns"] = _measure_ns(
                lambda: _seed_sqlite_from_generated_fixture(
                    self.sqlite,
                    graph_schema=self.graph_schema,
                    generated_fixture=self.sqlite_source,
                    ingest_batch_size=5_000,
                    progress_label=f"sqlite/{self.index_mode}",
                )
            )
            self.rss_snapshots_mib["after_ingest"] = self.capture_rss_snapshot()
            _, self.setup_metrics["index_ns"] = _measure_ns(
                lambda: _configure_sqlite_indexes(
                    self.sqlite,
                    self.graph_schema,
                    index_mode=self.index_mode,
                )
            )
            self.rss_snapshots_mib["after_index"] = self.capture_rss_snapshot()
            _, self.setup_metrics["analyze_ns"] = _measure_ns(
                lambda: _analyze_sqlite(self.sqlite)
            )
            self.rss_snapshots_mib["after_analyze"] = self.capture_rss_snapshot()
            self.db_size_mib, self.wal_size_mib = _sqlite_file_size_mib(db_path)
            self.rss_snapshots_mib["suite_start"] = self.capture_rss_snapshot()
            self.artifact_path = db_path
            return

        if self.backend == "duckdb":
            db_path = self.work_dir.path / "runtime.duckdb"
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_duckdb_connection(db_path)
            )
            self.rss_snapshots_mib["after_connect"] = self.capture_rss_snapshot()
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _create_duckdb_schema(self.duck, self.graph_schema)
            )
            self.rss_snapshots_mib["after_schema"] = self.capture_rss_snapshot()
            _progress(
                f"duckdb/{self.index_mode}: ingesting from generated fixture "
                f"({self.sqlite_source.csv_dir})"
            )
            self.row_counts, self.setup_metrics["ingest_ns"] = _measure_ns(
                lambda: _seed_duckdb_from_fixture(
                    self.duck,
                    generated_fixture=self.sqlite_source,
                    graph_schema=self.graph_schema,
                )
            )
            self.rss_snapshots_mib["after_ingest"] = self.capture_rss_snapshot()
            self.setup_metrics["index_ns"] = 0
            self.rss_snapshots_mib["after_index"] = self.capture_rss_snapshot()
            _, self.setup_metrics["analyze_ns"] = _measure_ns(
                lambda: _analyze_duckdb(self.duck)
            )
            self.rss_snapshots_mib["after_analyze"] = self.capture_rss_snapshot()
            self.db_size_mib = _duckdb_file_size_mib(db_path)
            self.wal_size_mib = 0.0
            self.artifact_path = db_path
            self.rss_snapshots_mib["suite_start"] = self.capture_rss_snapshot()
            return

        if self.backend == "postgresql":
            if not self.postgres_dsn:
                raise ValueError("PostgreSQL backend requires a DSN.")
            self.db_size_mib = 0.0
            self.wal_size_mib = 0.0
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_postgresql_connection(self.postgres_dsn)
            )
            self.rss_snapshots_mib["after_connect"] = self.capture_rss_snapshot()
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _reset_postgresql_schema(self.postgresql, self.graph_schema)
            )
            _, schema_create_ns = _measure_ns(
                lambda: _create_postgresql_schema(self.postgresql, self.graph_schema)
            )
            self.setup_metrics["schema_ns"] += schema_create_ns
            self.rss_snapshots_mib["after_schema"] = self.capture_rss_snapshot()
            _progress(
                f"postgresql/{self.index_mode}: ingesting from generated fixture "
                f"({self.sqlite_source.csv_dir})"
            )
            self.row_counts, self.setup_metrics["ingest_ns"] = _measure_ns(
                lambda: _seed_postgresql_from_fixture(
                    self.postgresql,
                    generated_fixture=self.sqlite_source,
                    graph_schema=self.graph_schema,
                )
            )
            self.rss_snapshots_mib["after_ingest"] = self.capture_rss_snapshot()
            _, self.setup_metrics["index_ns"] = _measure_ns(
                lambda: _configure_postgresql_indexes(
                    self.postgresql,
                    self.graph_schema,
                    index_mode=self.index_mode,
                )
            )
            self.rss_snapshots_mib["after_index"] = self.capture_rss_snapshot()
            _, self.setup_metrics["analyze_ns"] = _measure_ns(
                lambda: _analyze_postgresql(self.postgresql)
            )
            self.rss_snapshots_mib["after_analyze"] = self.capture_rss_snapshot()
            self.rss_snapshots_mib["suite_start"] = self.capture_rss_snapshot()
            self.artifact_path = None
            return

        raise ValueError(f"Unsupported backend {self.backend!r}.")

    @property
    def sqlite(self) -> sqlite3.Connection:
        assert isinstance(self.connection, sqlite3.Connection)
        return self.connection

    @property
    def duck(self) -> DuckDBConnection:
        return self.connection

    @property
    def postgresql(self) -> PostgreSQLConnection:
        return self.connection

    def close(self) -> None:
        self.connection.close()
        self.work_dir.close()

    def server_rss_mib(self) -> float | None:
        if self.backend != "postgresql":
            return None
        return postgresql_benchmark_server_rss_mib(self.postgres_dsn)

    def capture_rss_snapshot(self) -> dict[str, float | None]:
        return _capture_rss_snapshot(
            backend=self.backend,
            server_mib=self.server_rss_mib(),
        )

    def capture_lightweight_rss_snapshot(self) -> dict[str, float | None]:
        return _capture_rss_snapshot(backend=self.backend)

    def compile_query(self, query: CorpusQuery) -> PreparedArtifact:
        if query.mode == "statement":
            if self.backend == "duckdb":
                return PreparedArtifact(
                    mode="statement",
                    compiled=cypherglot.to_sql(
                        query.query,
                        dialect="duckdb",
                        schema_context=self.schema_context,
                    ),
                )
            if self.backend == "postgresql":
                return PreparedArtifact(
                    mode="statement",
                    compiled=cypherglot.to_sql(
                        query.query,
                        dialect="postgres",
                        backend="postgresql",
                        schema_context=self.schema_context,
                    ),
                )
            return PreparedArtifact(
                mode="statement",
                compiled=cypherglot.to_sql(
                    query.query,
                    backend="sqlite",
                    schema_context=self.schema_context,
                ),
            )
        if self.backend == "postgresql":
            return PreparedArtifact(
                mode="program",
                compiled=cypherglot.render_cypher_program_text(
                    query.query,
                    dialect="postgres",
                    backend="postgresql",
                    schema_context=self.schema_context,
                ),
            )
        if self.backend == "duckdb":
            return PreparedArtifact(
                mode="program",
                compiled=cypherglot.render_cypher_program_text(
                    query.query,
                    dialect="duckdb",
                    backend="duckdb",
                    schema_context=self.schema_context,
                ),
            )
        if self.backend != "sqlite":
            raise ValueError(
                "Rendered program execution is only supported on SQLite, DuckDB, "
                "and PostgreSQL."
            )
        return PreparedArtifact(
            mode="program",
            compiled=cypherglot.render_cypher_program_text(
                query.query,
                backend="sqlite",
                schema_context=self.schema_context,
            ),
        )

    def execute_query(self, artifact: PreparedArtifact) -> None:
        if artifact.mode == "statement":
            if self.backend == "sqlite":
                cursor = self.sqlite.execute(artifact.compiled)
                if cursor.description is not None:
                    cursor.fetchall()
                return
            if self.backend == "postgresql":
                with self.postgresql.cursor() as cur:
                    _execute_bound_postgresql_sql(cur, artifact.compiled, {})
                    if cur.description is not None:
                        cur.fetchall()
                return
            cursor = self.duck.execute(artifact.compiled)
            if cursor.description is not None:
                cursor.fetchall()
            return
        if self.backend == "postgresql":
            _execute_postgresql_program(self.postgresql, artifact.compiled)
            return
        if self.backend == "duckdb":
            _execute_duckdb_program(self.duck, artifact.compiled)
            return
        if self.backend != "sqlite":
            raise ValueError(
                "Rendered program execution is only supported on SQLite, DuckDB, "
                "and PostgreSQL."
            )
        _execute_sqlite_program(self.sqlite, artifact.compiled, commit=False)


def _run_iteration(runner: _BackendRunner, query: CorpusQuery) -> dict[str, int]:
    if runner.backend == "postgresql":
        return _run_postgresql_iteration(runner, query)

    reset_ns = 0
    rss_stages_mib = {"before_compile": runner.capture_rss_snapshot()}
    if query.mutation and runner.backend == "sqlite":
        runner.sqlite.execute(f"SAVEPOINT {SQLITE_SAVEPOINT}")
    if query.mutation and runner.backend == "duckdb":
        runner.duck.execute("BEGIN TRANSACTION")
    try:
        artifact, compile_ns = _measure_ns(lambda: runner.compile_query(query))
        rss_stages_mib["after_compile"] = runner.capture_rss_snapshot()
        _, execute_ns = _measure_ns(lambda: runner.execute_query(artifact))
        end_to_end_ns = compile_ns + execute_ns
        rss_stages_mib["after_execute"] = runner.capture_rss_snapshot()
    finally:
        if query.mutation and runner.backend == "sqlite":
            _, reset_ns = _measure_ns(
                lambda: _rollback_sqlite_iteration(runner.sqlite, SQLITE_SAVEPOINT)
            )
        if query.mutation and runner.backend == "duckdb":
            _, reset_ns = _measure_ns(lambda: runner.duck.execute("ROLLBACK"))
        rss_stages_mib["after_reset"] = runner.capture_rss_snapshot()
    return {
        "compile_ns": compile_ns,
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
        "rss_stages_mib": rss_stages_mib,
    }


def _run_postgresql_iteration(
    runner: _BackendRunner,
    query: CorpusQuery,
) -> dict[str, int]:
    reset_ns = 0
    capture_iteration_rss = getattr(
        runner,
        "capture_lightweight_rss_snapshot",
        runner.capture_rss_snapshot,
    )
    rss_stages_mib = {"before_compile": capture_iteration_rss()}
    try:
        artifact, compile_ns = _measure_ns(lambda: runner.compile_query(query))
        rss_stages_mib["after_compile"] = capture_iteration_rss()
        _, execute_ns = _measure_ns(lambda: runner.execute_query(artifact))
        end_to_end_ns = compile_ns + execute_ns
        rss_stages_mib["after_execute"] = capture_iteration_rss()
        if query.mutation:
            _, reset_ns = _measure_ns(runner.postgresql.rollback)
        else:
            runner.postgresql.rollback()
    except Exception:
        runner.postgresql.rollback()
        raise
    rss_stages_mib["after_reset"] = capture_iteration_rss()
    return {
        "compile_ns": compile_ns,
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
        "rss_stages_mib": rss_stages_mib,
    }


def _measure_query(
    runner: _BackendRunner,
    query: CorpusQuery,
    *,
    iterations: int,
    warmup: int,
    progress_label: str = "",
    iteration_progress: bool = False,
) -> dict[str, object]:
    for warmup_index in range(1, warmup + 1):
        if iteration_progress:
            _progress_iteration(
                progress_label,
                phase="warmup",
                current=warmup_index,
                total=warmup,
            )
        _run_iteration(runner, query)

    compile_latencies: list[int] = []
    execute_latencies: list[int] = []
    end_to_end_latencies: list[int] = []
    reset_latencies: list[int] = []
    rss_samples_by_stage = {
        "before_compile": [],
        "after_compile": [],
        "after_execute": [],
        "after_reset": [],
    }

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
            metrics = _run_iteration(runner, query)
            compile_latencies.append(metrics["compile_ns"])
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
            for stage_name, stage_snapshot in metrics["rss_stages_mib"].items():
                rss_samples_by_stage[stage_name].append(stage_snapshot)
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": runner.backend,
        "index_mode": runner.index_mode,
        "mode": query.mode,
        "mutation": query.mutation,
        "compile": _summarize(compile_latencies),
        "execute": _summarize(execute_latencies),
        "end_to_end": _summarize(end_to_end_latencies),
        "reset": _summarize(reset_latencies),
        "rss_stages_mib": {
            stage_name: _summarize_rss_samples(stage_samples)
            for stage_name, stage_samples in rss_samples_by_stage.items()
        },
    }


def _pool_summaries(
    query_results: list[dict[str, object]],
    key: str,
) -> dict[str, float]:
    if not query_results:
        return {
            "mean_of_mean_ms": 0.0,
            "mean_of_p50_ms": 0.0,
            "mean_of_p95_ms": 0.0,
            "mean_of_p99_ms": 0.0,
        }
    return {
        "mean_of_mean_ms": sum(result[key]["mean_ms"] for result in query_results)
        / len(query_results),
        "mean_of_p50_ms": sum(result[key]["p50_ms"] for result in query_results)
        / len(query_results),
        "mean_of_p95_ms": sum(result[key]["p95_ms"] for result in query_results)
        / len(query_results),
        "mean_of_p99_ms": sum(result[key]["p99_ms"] for result in query_results)
        / len(query_results),
    }


def _run_backend_suite(
    backend: str,
    queries: list[CorpusQuery],
    *,
    workload: str,
    iterations: int,
    warmup: int,
    graph_schema: cypherglot.GraphSchema,
    schema_context: cypherglot.CompilerSchemaContext,
    sqlite_source: GeneratedGraphFixture,
    postgres_dsn: str | None = None,
    db_root_dir: Path | None = None,
    iteration_progress: bool = False,
) -> dict[str, object]:
    backend_index_mode = sqlite_source.index_mode
    suite_progress_name = f"{workload}/{backend}_{backend_index_mode}"
    suite_name = f"{workload}-{backend}-{backend_index_mode}-suite"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix=f"cypherglot-runtime-{backend}-suite-",
        name=suite_name,
    )
    runner = _BackendRunner(
        backend,
        work_dir,
        graph_schema=graph_schema,
        schema_context=schema_context,
        sqlite_source=sqlite_source,
        postgres_dsn=postgres_dsn,
    )
    try:
        _progress(
            f"{suite_progress_name}: starting suite with {len(queries)} queries "
            f"({iterations} iterations, {warmup} warmup)"
        )
        query_results = []
        for query_index, query in enumerate(queries, start=1):
            query_progress_label = (
                f"{suite_progress_name}: query {query_index}/{len(queries)} "
                f"{query.name}"
            )
            _progress(query_progress_label)
            query_results.append(
                _measure_query(
                    runner,
                    query,
                    iterations=iterations,
                    warmup=warmup,
                    progress_label=query_progress_label,
                    iteration_progress=iteration_progress,
                )
            )
        runner.rss_snapshots_mib["suite_complete"] = runner.capture_rss_snapshot()
        _progress(f"{suite_progress_name}: suite complete")
        return {
            "backend": backend,
            "index_mode": runner.index_mode,
            "iterations": iterations,
            "warmup": warmup,
            "query_count": len(queries),
            "setup": {
                f"{metric[:-3]}_ms": value / 1_000_000.0
                for metric, value in runner.setup_metrics.items()
            },
            "row_counts": runner.row_counts,
            "rss_snapshots_mib": runner.rss_snapshots_mib,
            "storage": {
                "db_size_mib": runner.db_size_mib,
                "wal_size_mib": runner.wal_size_mib,
            },
            "db_path": (
                str(runner.artifact_path) if runner.artifact_path is not None else None
            ),
            "compile": _pool_summaries(query_results, "compile"),
            "execute": _pool_summaries(query_results, "execute"),
            "end_to_end": _pool_summaries(query_results, "end_to_end"),
            "reset": _pool_summaries(query_results, "reset"),
            "queries": query_results,
        }
    finally:
        runner.close()


def _build_payload(
    *,
    entrypoint: SQLRuntimeBenchmarkEntrypoint,
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
    olap_iterations: int,
    olap_warmup: int,
    db_root_dir: Path | None,
    result: dict[str, object],
    status: str,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "benchmark_entrypoint": entrypoint.name,
        "enabled_backends": list(entrypoint.enabled_backends),
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
            "layout": "type-aware",
            "node_tables": [
                node_type.table_name for node_type in graph_schema.node_types
            ],
            "edge_tables": [
                edge_type.table_name for edge_type in graph_schema.edge_types
            ],
        },
        "index_mode": (
            index_mode
        ),
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
    }
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    return payload


def _detect_database_versions(
    entrypoint: SQLRuntimeBenchmarkEntrypoint,
    postgres_dsn: str | None,
) -> dict[str, str]:
    versions: dict[str, str] = {}
    for backend in entrypoint.enabled_backends:
        if backend == "sqlite":
            versions[backend] = sqlite3.sqlite_version
            continue
        if backend == "duckdb":
            version = _duckdb_version()
            if version is None:
                raise ValueError("duckdb is not installed.")
            versions[backend] = version
            continue
        if backend == "postgresql":
            if not postgres_dsn:
                raise ValueError(
                    "PostgreSQL DSN is required to determine the server version."
                )
            versions[backend] = _postgresql_server_version(postgres_dsn)
            continue
        raise ValueError(f"Unsupported backend {backend!r}.")
    return versions


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

        normalized_backends: list[str] = []
        for backend in backends:
            if backend not in {"sqlite", "duckdb", "postgresql"}:
                raise ValueError(
                    f"Runtime corpus item {index} has unsupported backend {backend!r}."
                )
            normalized_backends.append(backend)
        queries.append(
            CorpusQuery(
                name=name,
                workload=workload,
                category=category,
                query=query,
                backends=tuple(normalized_backends),
                mode=mode,
                mutation=mutation,
            )
        )
    return queries


def _filter_duckdb_queries(queries: list[CorpusQuery]) -> list[CorpusQuery]:
    return [query for query in queries if "duckdb" in query.backends]


def _filter_postgresql_queries(queries: list[CorpusQuery]) -> list[CorpusQuery]:
    return [query for query in queries if "postgresql" in query.backends]


def _resolve_postgresql_runtime_dsn(
    entrypoint: SQLRuntimeBenchmarkEntrypoint,
    configured_dsn: str,
) -> tuple[str | None, bool]:
    if "postgresql" not in entrypoint.enabled_backends:
        return None, False
    if configured_dsn:
        return configured_dsn, False
    return acquire_postgresql_benchmark_dsn(), True


def _benchmark_result(
    queries: list[CorpusQuery],
    *,
    iterations: int,
    warmup: int,
    oltp_iterations: int | None = None,
    oltp_warmup: int | None = None,
    olap_iterations: int | None = None,
    olap_warmup: int | None = None,
    entrypoint: SQLRuntimeBenchmarkEntrypoint,
    postgres_dsn: str | None = None,
    scale: RuntimeScale,
    index_mode: str,
    db_root_dir: Path | None = None,
    iteration_progress: bool = False,
    progress_callback: RuntimeProgressCallback | None = None,
) -> dict[str, object]:
    graph_schema, edge_plans = _build_graph_schema(scale)
    schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)

    token_map = _token_map(scale, graph_schema, edge_plans)
    rendered_queries = _render_corpus_queries(queries, token_map)

    oltp_queries = [query for query in rendered_queries if query.workload == "oltp"]
    olap_queries = [query for query in rendered_queries if query.workload == "olap"]
    oltp_iterations_value = iterations if oltp_iterations is None else oltp_iterations
    oltp_warmup_value = warmup if oltp_warmup is None else oltp_warmup
    olap_iterations_value = iterations if olap_iterations is None else olap_iterations
    olap_warmup_value = warmup if olap_warmup is None else olap_warmup
    enabled_backends = set(entrypoint.enabled_backends)

    index_modes = [index_mode] if index_mode != "both" else ["indexed", "unindexed"]

    workloads: dict[str, object] = {}
    if progress_callback is not None:
        progress_callback({"workloads": workloads, "token_map": token_map})
    generated_fixtures = {
        mode: _prepare_generated_graph_fixture(
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode=mode,
            db_root_dir=db_root_dir,
        )
        for mode in index_modes
    }
    try:
        if oltp_queries:
            workloads["oltp"] = {
                "description": (
                    "Transactional-style reads and mutations over the generated "
                    "multi-type type-aware graph for the selected backend set."
                )
            }
            sqlite_oltp_queries = [
                query for query in oltp_queries if "sqlite" in query.backends
            ]
            duckdb_oltp_queries = _filter_duckdb_queries(oltp_queries)
            postgresql_oltp_queries = _filter_postgresql_queries(oltp_queries)
            for mode, fixture in generated_fixtures.items():
                if "sqlite" in enabled_backends:
                    workloads["oltp"][f"sqlite_{mode}"] = _run_backend_suite(
                        "sqlite",
                        sqlite_oltp_queries,
                        workload="oltp",
                        iterations=oltp_iterations_value,
                        warmup=oltp_warmup_value,
                        graph_schema=graph_schema,
                        schema_context=schema_context,
                        sqlite_source=fixture,
                        db_root_dir=db_root_dir,
                        iteration_progress=iteration_progress,
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {"workloads": workloads, "token_map": token_map}
                        )
                if "postgresql" in enabled_backends and postgres_dsn:
                    workloads["oltp"][f"postgresql_{mode}"] = _run_backend_suite(
                        "postgresql",
                        postgresql_oltp_queries,
                        workload="oltp",
                        iterations=oltp_iterations_value,
                        warmup=oltp_warmup_value,
                        graph_schema=graph_schema,
                        schema_context=schema_context,
                        sqlite_source=fixture,
                        postgres_dsn=postgres_dsn,
                        db_root_dir=db_root_dir,
                        iteration_progress=iteration_progress,
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {"workloads": workloads, "token_map": token_map}
                        )
            if "duckdb" in enabled_backends and duckdb_oltp_queries:
                duckdb_source = generated_fixtures.get("unindexed")
                if duckdb_source is None:
                    duckdb_source = next(iter(generated_fixtures.values()))
                workloads["oltp"]["duckdb"] = _run_backend_suite(
                    "duckdb",
                    duckdb_oltp_queries,
                    workload="oltp",
                    iterations=oltp_iterations_value,
                    warmup=oltp_warmup_value,
                    graph_schema=graph_schema,
                    schema_context=schema_context,
                    sqlite_source=duckdb_source,
                    db_root_dir=db_root_dir,
                    iteration_progress=iteration_progress,
                )
                if progress_callback is not None:
                    progress_callback(
                        {"workloads": workloads, "token_map": token_map}
                    )

        if olap_queries:
            workloads["olap"] = {
                "description": (
                    "Analytical read queries measured against the generated graph "
                    "for the selected SQL runtime backends."
                ),
            }
            sqlite_olap_queries = [
                query for query in olap_queries if "sqlite" in query.backends
            ]
            postgresql_olap_queries = _filter_postgresql_queries(olap_queries)
            duckdb_olap_queries = _filter_duckdb_queries(olap_queries)
            for mode, fixture in generated_fixtures.items():
                if "sqlite" in enabled_backends:
                    workloads["olap"][f"sqlite_{mode}"] = _run_backend_suite(
                        "sqlite",
                        sqlite_olap_queries,
                        workload="olap",
                        iterations=olap_iterations_value,
                        warmup=olap_warmup_value,
                        graph_schema=graph_schema,
                        schema_context=schema_context,
                        sqlite_source=fixture,
                        db_root_dir=db_root_dir,
                        iteration_progress=iteration_progress,
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {"workloads": workloads, "token_map": token_map}
                        )
                if "postgresql" in enabled_backends and postgres_dsn:
                    workloads["olap"][f"postgresql_{mode}"] = _run_backend_suite(
                        "postgresql",
                        postgresql_olap_queries,
                        workload="olap",
                        iterations=olap_iterations_value,
                        warmup=olap_warmup_value,
                        graph_schema=graph_schema,
                        schema_context=schema_context,
                        sqlite_source=fixture,
                        postgres_dsn=postgres_dsn,
                        db_root_dir=db_root_dir,
                        iteration_progress=iteration_progress,
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {"workloads": workloads, "token_map": token_map}
                        )
            if "duckdb" in enabled_backends and duckdb_olap_queries:
                duckdb_source = generated_fixtures.get("unindexed")
                if duckdb_source is None:
                    duckdb_source = next(iter(generated_fixtures.values()))
                workloads["olap"]["duckdb"] = _run_backend_suite(
                    "duckdb",
                    duckdb_olap_queries,
                    workload="olap",
                    iterations=olap_iterations_value,
                    warmup=olap_warmup_value,
                    graph_schema=graph_schema,
                    schema_context=schema_context,
                    sqlite_source=duckdb_source,
                    db_root_dir=db_root_dir,
                    iteration_progress=iteration_progress,
                )
                if progress_callback is not None:
                    progress_callback({"workloads": workloads, "token_map": token_map})
    finally:
        for fixture in generated_fixtures.values():
            fixture.close()

    return {
        "workloads": workloads,
        "token_map": token_map,
    }


def _print_suite(name: str, suite: dict[str, object]) -> None:
    def format_rss_snapshot(snapshot: dict[str, float | None]) -> str:
        parts = [f"client={snapshot['client_mib']:.2f} MiB"]
        if snapshot["server_mib"] is not None:
            parts.append(f"server={snapshot['server_mib']:.2f} MiB")
        if snapshot["combined_mib"] is not None:
            parts.append(f"combined={snapshot['combined_mib']:.2f} MiB")
        return ", ".join(parts)

    print(name)
    setup_parts = [
        f"connect={suite['setup']['connect_ms']:.2f} ms",
        f"schema={suite['setup']['schema_ms']:.2f} ms",
        f"index={suite['setup']['index_ms']:.2f} ms",
        f"ingest={suite['setup']['ingest_ms']:.2f} ms",
    ]
    if "analyze_ms" in suite["setup"]:
        setup_parts.append(f"analyze={suite['setup']['analyze_ms']:.2f} ms")
    print("  setup: " + ", ".join(setup_parts))
    print(
        "  rss: "
        + ", ".join(
            f"{key}({format_rss_snapshot(value)})"
            for key, value in suite["rss_snapshots_mib"].items()
        )
    )
    print(
        "  storage: "
        f"db={suite['storage']['db_size_mib']:.2f} MiB, "
        f"wal={suite['storage']['wal_size_mib']:.2f} MiB"
    )
    print(
        "  pooled compile: "
        f"mean={suite['compile']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['compile']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['compile']['mean_of_p95_ms']:.2f} ms, "
        f"p99={suite['compile']['mean_of_p99_ms']:.2f} ms"
    )
    print(
        "  pooled execute: "
        f"mean={suite['execute']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['execute']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['execute']['mean_of_p95_ms']:.2f} ms, "
        f"p99={suite['execute']['mean_of_p99_ms']:.2f} ms"
    )
    print(
        "  pooled reset: "
        f"mean={suite['reset']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['reset']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['reset']['mean_of_p95_ms']:.2f} ms, "
        f"p99={suite['reset']['mean_of_p99_ms']:.2f} ms"
    )
    print(
        "  pooled end-to-end: "
        f"mean={suite['end_to_end']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['end_to_end']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['end_to_end']['mean_of_p95_ms']:.2f} ms, "
        f"p99={suite['end_to_end']['mean_of_p99_ms']:.2f} ms"
    )
    for query_result in suite["queries"]:
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"compile_mean={query_result['compile']['mean_ms']:.2f} ms, "
            f"compile_p50={query_result['compile']['p50_ms']:.2f} ms, "
            f"compile_p95={query_result['compile']['p95_ms']:.2f} ms, "
            f"compile_p99={query_result['compile']['p99_ms']:.2f} ms, "
            f"execute_mean={query_result['execute']['mean_ms']:.2f} ms, "
            f"execute_p50={query_result['execute']['p50_ms']:.2f} ms, "
            f"execute_p95={query_result['execute']['p95_ms']:.2f} ms, "
            f"execute_p99={query_result['execute']['p99_ms']:.2f} ms, "
            f"reset_mean={query_result['reset']['mean_ms']:.2f} ms, "
            f"reset_p50={query_result['reset']['p50_ms']:.2f} ms, "
            f"reset_p95={query_result['reset']['p95_ms']:.2f} ms, "
            f"reset_p99={query_result['reset']['p99_ms']:.2f} ms, "
            f"end_to_end_mean={query_result['end_to_end']['mean_ms']:.2f} ms, "
            f"end_to_end_p50={query_result['end_to_end']['p50_ms']:.2f} ms, "
            f"end_to_end_p95={query_result['end_to_end']['p95_ms']:.2f} ms, "
            f"end_to_end_p99={query_result['end_to_end']['p99_ms']:.2f} ms"
        )


def _parse_args(entrypoint: SQLRuntimeBenchmarkEntrypoint) -> argparse.Namespace:
    return parse_sql_runtime_args(
        description=entrypoint.description,
        default_corpus_path=DEFAULT_CORPUS_PATH,
        default_output_path=entrypoint.default_output_path,
        enabled_backends=entrypoint.enabled_backends,
        default_index_mode=entrypoint.default_index_mode,
        index_mode_choices=entrypoint.index_mode_choices,
    )


def main(entrypoint: SQLRuntimeBenchmarkEntrypoint = SQLITE_ENTRYPOINT) -> int:
    args = _parse_args(entrypoint)
    started_at = datetime.now(UTC)
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")
    if args.node_type_count < 3:
        raise ValueError("--node-type-count must be at least 3 for the runtime corpus.")
    if args.edge_type_count < 3:
        raise ValueError("--edge-type-count must be at least 3 for the runtime corpus.")
    if args.nodes_per_type <= 0:
        raise ValueError("--nodes-per-type must be positive.")
    if args.edges_per_source <= 0:
        raise ValueError("--edges-per-source must be positive.")
    if args.variable_hop_max <= 0:
        raise ValueError("--variable-hop-max must be positive.")
    if args.ingest_batch_size <= 0:
        raise ValueError("--ingest-batch-size must be positive.")
    if args.oltp_iterations is not None and args.oltp_iterations <= 0:
        raise ValueError("--oltp-iterations must be positive.")
    if args.oltp_warmup is not None and args.oltp_warmup < 0:
        raise ValueError("--oltp-warmup must be non-negative.")
    if args.olap_iterations is not None and args.olap_iterations <= 0:
        raise ValueError("--olap-iterations must be positive.")
    if args.olap_warmup is not None and args.olap_warmup < 0:
        raise ValueError("--olap-warmup must be non-negative.")

    include_duckdb = "duckdb" in entrypoint.enabled_backends
    if include_duckdb and not _duckdb_available():
        raise ValueError("duckdb is not installed.")
    postgres_dsn, acquired_postgresql_runtime = _resolve_postgresql_runtime_dsn(
        entrypoint,
        getattr(args, "postgres_dsn", "").strip(),
    )
    if postgres_dsn and not _postgresql_available():
        raise ValueError(
            "psycopg2 is not installed. Install it or omit --postgres-dsn."
        )
    database_versions = _detect_database_versions(entrypoint, postgres_dsn or None)

    db_root_dir = None
    if args.db_root_dir is not None:
        run_name = (
            f"benchmark-{entrypoint.name}-runtime-"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        )
        db_root_dir = args.db_root_dir / run_name
        db_root_dir.mkdir(parents=True, exist_ok=False)

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

    queries = _select_queries(_load_corpus(args.corpus), args.query_names)
    graph_schema, _ = _build_graph_schema(scale)

    def write_checkpoint(result: dict[str, object], *, status: str) -> None:
        payload = _build_payload(
            entrypoint=entrypoint,
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
            olap_iterations=(
                args.olap_iterations
                if args.olap_iterations is not None
                else args.iterations
            ),
            olap_warmup=(
                args.olap_warmup if args.olap_warmup is not None else args.warmup
            ),
            db_root_dir=db_root_dir,
            result=result,
            status=status,
            completed_at=datetime.now(UTC) if status == "completed" else None,
        )
        _write_json_atomic(args.output, payload)

    write_checkpoint({"workloads": {}, "token_map": {}}, status="running")
    _progress(
        "runtime benchmark: starting "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, index_mode={args.index_mode})"
    )
    if args.oltp_iterations is not None or args.oltp_warmup is not None:
        oltp_warmup_value = (
            args.oltp_warmup if args.oltp_warmup is not None else args.warmup
        )
        _progress(
            "runtime benchmark: OLTP overrides "
            f"(iterations={args.oltp_iterations or args.iterations}, "
            f"warmup={oltp_warmup_value})"
        )
    if args.olap_iterations is not None or args.olap_warmup is not None:
        olap_warmup_value = (
            args.olap_warmup if args.olap_warmup is not None else args.warmup
        )
        _progress(
            "runtime benchmark: OLAP overrides "
            f"(iterations={args.olap_iterations or args.iterations}, "
            f"warmup={olap_warmup_value})"
        )
    if db_root_dir is not None:
        _progress(f"runtime benchmark: persisting databases under {db_root_dir}")
    try:
        result = _benchmark_result(
            queries,
            iterations=args.iterations,
            warmup=args.warmup,
            oltp_iterations=args.oltp_iterations,
            oltp_warmup=args.oltp_warmup,
            olap_iterations=args.olap_iterations,
            olap_warmup=args.olap_warmup,
            entrypoint=entrypoint,
            postgres_dsn=postgres_dsn or None,
            scale=scale,
            index_mode=args.index_mode,
            db_root_dir=db_root_dir,
            iteration_progress=args.iteration_progress,
            progress_callback=lambda partial_result: write_checkpoint(
                partial_result,
                status="running",
            ),
        )
        write_checkpoint(result, status="completed")
        _progress(f"runtime benchmark: wrote baseline to {args.output}")

        print(f"Wrote runtime benchmark baseline to {args.output}")
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
        return 0
    finally:
        if acquired_postgresql_runtime:
            release_postgresql_benchmark_dsn()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
