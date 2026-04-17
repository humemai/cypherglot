"""Benchmark end-to-end runtime over generated multi-type graph workloads.

This script generates a synthetic type-aware graph with configurable node and
edge type counts, property counts, and edge fanout. It then benchmarks a
corpus of OLTP and OLAP Cypher queries, records compile, execute, end-to-end,
and reset timing splits, and captures setup cost, RSS checkpoints, and on-disk
SQLite size. OLAP queries can also be executed through DuckDB against the same
SQLite-ingested fixture.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import re
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import cypherglot

from _benchmark_cli_helpers import parse_sqlite_runtime_args
from _benchmark_common import (
    CorpusQuery,
    EdgeTypePlan,
    RuntimeScale,
    _average_edges_per_source,
    _build_graph_schema,
    _edge_out_degree,
    _measure_ns,
    _node_id,
    _node_name,
    _node_type_name,
    _progress,
    _progress_iteration,
    _render_corpus_queries,
    RuntimeProgressCallback,
    _rss_mib,
    _select_queries,
    _summarize,
    _token_map,
    _write_json_atomic,
)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None


DuckDBConnection = Any
PostgreSQLConnection = Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "sqlite_runtime_benchmark_baseline.json"
)
SQLITE_SAVEPOINT = "benchmark_iteration"
DUCKDB_OLAP_QUERY_SKIP_NAMES = frozenset({"olap_variable_length_reachability"})


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
    rss_snapshots_mib: dict[str, float]
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


def _sqlite_file_size_mib(db_path: Path) -> tuple[float, float]:
    db_size = db_path.stat().st_size / (1024.0 * 1024.0) if db_path.exists() else 0.0
    wal_path = db_path.with_name(db_path.name + "-wal")
    wal_size = (
        wal_path.stat().st_size / (1024.0 * 1024.0) if wal_path.exists() else 0.0
    )
    return db_size, wal_size


def _create_sqlite_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.create_function("REVERSE", 1, _sqlite_reverse)
    conn.create_function("LEFT", 2, _sqlite_left)
    conn.create_function("RIGHT", 2, _sqlite_right)
    conn.create_function("SPLIT", 2, _sqlite_split)
    conn.create_function("STR_POSITION", 2, _sqlite_str_position)
    return conn


def _create_sqlite_schema(
    conn: sqlite3.Connection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    conn.executescript("\n".join(graph_schema.ddl("sqlite")))


def _create_postgresql_connection(dsn: str) -> PostgreSQLConnection:
    if psycopg2 is None:
        raise ValueError("psycopg2 is not installed.")
    return psycopg2.connect(dsn)


def _reset_postgresql_schema(
    conn: PostgreSQLConnection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    table_names = [
        *(edge_type.table_name for edge_type in graph_schema.edge_types),
        *(node_type.table_name for node_type in graph_schema.node_types),
    ]
    with conn.cursor() as cur:
        for table_name in table_names:
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        for table_name in table_names:
            cur.execute(f"DROP SEQUENCE IF EXISTS {table_name}_id_seq CASCADE")
    conn.commit()


def _create_postgresql_schema(
    conn: PostgreSQLConnection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    with conn.cursor() as cur:
        for statement in graph_schema.ddl("postgresql"):
            cur.execute(statement)
    conn.commit()


def _drop_all_sqlite_indexes(conn: sqlite3.Connection) -> None:
    index_names = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    for index_name in index_names:
        conn.execute(f'DROP INDEX "{index_name}"')


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


def _create_query_indexes(
    conn: sqlite3.Connection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _query_index_statements(graph_schema):
        conn.execute(statement)


def _create_postgresql_query_indexes(
    conn: PostgreSQLConnection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    with conn.cursor() as cur:
        for statement in _query_index_statements(graph_schema):
            cur.execute(statement)
    conn.commit()


def _drop_all_postgresql_indexes(conn: PostgreSQLConnection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = current_schema() "
            "AND indexname NOT LIKE %s",
            ("%_pkey",),
        )
        index_names = [row[0] for row in cur.fetchall()]
        for index_name in index_names:
            cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')
    conn.commit()


def _configure_sqlite_indexes(
    conn: sqlite3.Connection,
    graph_schema: cypherglot.GraphSchema,
    *,
    index_mode: str,
) -> None:
    if index_mode == "indexed":
        _create_query_indexes(conn, graph_schema)
        return
    if index_mode == "unindexed":
        _drop_all_sqlite_indexes(conn)
        return
    raise ValueError(f"Unsupported index mode {index_mode!r}.")


def _configure_postgresql_indexes(
    conn: PostgreSQLConnection,
    graph_schema: cypherglot.GraphSchema,
    *,
    index_mode: str,
) -> None:
    if index_mode == "indexed":
        _create_postgresql_query_indexes(conn, graph_schema)
        return
    if index_mode == "unindexed":
        _drop_all_postgresql_indexes(conn)
        return
    raise ValueError(f"Unsupported index mode {index_mode!r}.")


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
            (
                1.0
                + ((plan.type_index * 7 + source_local_index + edge_ordinal) % 17)
                * 0.4
            ),
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


def _seed_sqlite(
    conn: sqlite3.Connection,
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    progress_label: str | None = None,
) -> dict[str, int]:
    node_row_count = 0
    edge_row_count = 0

    for type_index, node_type in enumerate(graph_schema.node_types, start=1):
        if progress_label is not None:
            _progress(
                f"{progress_label}: node type {type_index}/{scale.node_type_count} "
                f"({node_type.name})"
            )
        sample_row = _node_row(scale, type_index, 1)
        placeholders = ", ".join("?" for _ in sample_row)
        batch: list[tuple[Any, ...]] = []
        for local_index in range(1, scale.nodes_per_type + 1):
            batch.append(_node_row(scale, type_index, local_index))
            if len(batch) < scale.ingest_batch_size:
                continue
            conn.executemany(
                f"INSERT INTO {node_type.table_name} VALUES ({placeholders})",
                batch,
            )
            batch.clear()
        if batch:
            conn.executemany(
                f"INSERT INTO {node_type.table_name} VALUES ({placeholders})",
                batch,
            )
        node_row_count += scale.nodes_per_type

    edge_id = 1
    for edge_type_index, plan in enumerate(edge_plans, start=1):
        if progress_label is not None:
            _progress(
                f"{progress_label}: edge type "
                f"{edge_type_index}/{scale.edge_type_count} "
                f"({plan.name})"
            )
        table_name = graph_schema.edge_types[plan.type_index - 1].table_name
        sample_row = _edge_row(scale, plan, 1, 1, edge_id)
        placeholders = ", ".join("?" for _ in sample_row)
        batch = []
        for source_local_index in range(1, scale.nodes_per_type + 1):
            edge_count_for_source = _edge_out_degree(scale, source_local_index)
            for edge_ordinal in range(1, edge_count_for_source + 1):
                batch.append(
                    _edge_row(scale, plan, source_local_index, edge_ordinal, edge_id)
                )
                edge_id += 1
                edge_row_count += 1
                if len(batch) < scale.ingest_batch_size:
                    continue
                conn.executemany(
                    f"INSERT INTO {table_name} VALUES ({placeholders})",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                f"INSERT INTO {table_name} VALUES ({placeholders})",
                batch,
            )

    conn.commit()
    if progress_label is not None:
        _progress(
            f"{progress_label}: ingest committed "
            f"({node_row_count} nodes, {edge_row_count} edges)"
        )
    return {
        "node_count": node_row_count,
        "edge_count": edge_row_count,
        "node_type_count": scale.node_type_count,
        "edge_type_count": scale.edge_type_count,
    }


def _analyze_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("ANALYZE")


def _prepare_shared_sqlite_fixture(
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    index_mode: str,
    db_root_dir: Path | None = None,
) -> SharedSQLiteFixture:
    progress_label = f"sqlite/{index_mode}"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix=f"cypherglot-runtime-{index_mode}-",
        name=f"sqlite-{index_mode}",
    )
    db_path = work_dir.path / "runtime.sqlite3"
    rss_snapshots_mib: dict[str, float] = {}

    _progress(
        f"{progress_label}: creating fixture "
        f"({scale.total_nodes} nodes, {scale.total_edges} edges)"
    )
    conn, connect_ns = _measure_ns(lambda: _create_sqlite_connection(db_path))
    rss_snapshots_mib["after_connect"] = _rss_mib()
    try:
        _progress(f"{progress_label}: creating schema")
        _, schema_ns = _measure_ns(lambda: _create_sqlite_schema(conn, graph_schema))
        rss_snapshots_mib["after_schema"] = _rss_mib()
        _progress(f"{progress_label}: configuring indexes")
        _, index_ns = _measure_ns(
            lambda: _configure_sqlite_indexes(
                conn,
                graph_schema,
                index_mode=index_mode,
            )
        )
        rss_snapshots_mib["after_index"] = _rss_mib()
        _progress(f"{progress_label}: ingesting synthetic graph")
        row_counts, ingest_ns = _measure_ns(
            lambda: _seed_sqlite(
                conn,
                scale=scale,
                graph_schema=graph_schema,
                edge_plans=edge_plans,
                progress_label=progress_label,
            )
        )
        rss_snapshots_mib["after_ingest"] = _rss_mib()
        _progress(f"{progress_label}: analyzing SQLite statistics")
        _, analyze_ns = _measure_ns(lambda: _analyze_sqlite(conn))
        rss_snapshots_mib["after_analyze"] = _rss_mib()
    finally:
        conn.close()

    db_size_mib, wal_size_mib = _sqlite_file_size_mib(db_path)
    _progress(
        f"{progress_label}: fixture ready "
        f"(ingest={ingest_ns / 1_000_000_000.0:.2f}s, db={db_size_mib:.2f} MiB)"
    )
    return SharedSQLiteFixture(
        work_dir=work_dir,
        db_path=db_path,
        setup_metrics={
            "connect_ns": connect_ns,
            "schema_ns": schema_ns,
            "index_ns": index_ns,
            "ingest_ns": ingest_ns,
            "analyze_ns": analyze_ns,
        },
        row_counts=row_counts,
        rss_snapshots_mib=rss_snapshots_mib,
        db_size_mib=db_size_mib,
        wal_size_mib=wal_size_mib,
        index_mode=index_mode,
    )


def _postgres_placeholder_list(width: int) -> str:
    return ", ".join("%s" for _ in range(width))


def _set_postgresql_sequence(
    conn: PostgreSQLConnection,
    table_name: str,
    max_id: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT setval('{table_name}_id_seq', %s, true)", (max_id,))


def _seed_postgresql_from_sqlite(
    conn: PostgreSQLConnection,
    *,
    sqlite_source: SharedSQLiteFixture,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, int]:
    source = sqlite3.connect(sqlite_source.db_path)
    try:
        with conn.cursor() as cur:
            table_names = [
                *(node_type.table_name for node_type in graph_schema.node_types),
                *(edge_type.table_name for edge_type in graph_schema.edge_types),
            ]
            for table_name in table_names:
                source_cursor = source.execute(f"SELECT * FROM {table_name}")
                first_row = source_cursor.fetchone()
                if first_row is None:
                    continue
                placeholders = _postgres_placeholder_list(len(first_row))
                cur.execute(
                    f"INSERT INTO {table_name} VALUES ({placeholders})",
                    first_row,
                )
                while True:
                    rows = source_cursor.fetchmany(5_000)
                    if not rows:
                        break
                    cur.executemany(
                        f"INSERT INTO {table_name} VALUES ({placeholders})",
                        rows,
                    )
                max_id_row = source.execute(
                    f"SELECT COALESCE(MAX(id), 0) FROM {table_name}"
                ).fetchone()
                max_id = int(max_id_row[0]) if max_id_row is not None else 0
                if max_id > 0:
                    _set_postgresql_sequence(conn, table_name, max_id)
        conn.commit()
    finally:
        source.close()
    return dict(sqlite_source.row_counts)


def _analyze_postgresql(conn: PostgreSQLConnection) -> None:
    with conn.cursor() as cur:
        cur.execute("ANALYZE")
    conn.commit()


def _create_duckdb_connection(db_path: Path) -> DuckDBConnection:
    if duckdb is None:
        raise ValueError("duckdb is not installed.")
    return duckdb.connect(str(db_path))


def _configure_duckdb_from_sqlite(
    conn: DuckDBConnection,
    sqlite_path: Path,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    sqlite_path_sql = str(sqlite_path).replace("'", "''")
    conn.execute("INSTALL sqlite")
    conn.execute("LOAD sqlite")
    conn.execute(f"ATTACH '{sqlite_path_sql}' AS sqlite_db (TYPE sqlite)")
    for node_type in graph_schema.node_types:
        conn.execute(
            f"CREATE VIEW {node_type.table_name} AS "
            f"SELECT * FROM sqlite_db.{node_type.table_name}"
        )
    for edge_type in graph_schema.edge_types:
        conn.execute(
            f"CREATE VIEW {edge_type.table_name} AS "
            f"SELECT * FROM sqlite_db.{edge_type.table_name}"
        )


def _sqlite_reverse(value: object) -> object:
    if value is None:
        return None
    return str(value)[::-1]


def _sqlite_left(value: object, count: object) -> object:
    if value is None or count is None:
        return None
    return str(value)[: int(count)]


def _sqlite_right(value: object, count: object) -> object:
    if value is None or count is None:
        return None
    count_int = int(count)
    if count_int <= 0:
        return ""
    return str(value)[-count_int:]


def _sqlite_split(value: object, delimiter: object) -> object:
    if value is None or delimiter is None:
        return None
    return json.dumps(str(value).split(str(delimiter)))


def _sqlite_str_position(haystack: object, needle: object) -> object:
    if haystack is None or needle is None:
        return None
    return str(haystack).find(str(needle)) + 1


class _BackendRunner:
    def __init__(
        self,
        backend: str,
        work_dir: ManagedDirectory,
        *,
        graph_schema: cypherglot.GraphSchema,
        schema_context: cypherglot.CompilerSchemaContext,
        sqlite_source: SharedSQLiteFixture,
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
        self.rss_snapshots_mib: dict[str, float] = {}
        self.db_size_mib = sqlite_source.db_size_mib
        self.wal_size_mib = sqlite_source.wal_size_mib
        self.index_mode = sqlite_source.index_mode
        self.connection: sqlite3.Connection | DuckDBConnection | PostgreSQLConnection
        self._initialize()

    def _initialize(self) -> None:
        if self.backend == "sqlite":
            self.connection, connect_ns = _measure_ns(
                lambda: _create_sqlite_connection(self.sqlite_source.db_path)
            )
            self.setup_metrics = {
                "connect_ns": connect_ns,
                "schema_ns": self.sqlite_source.setup_metrics["schema_ns"],
                "index_ns": self.sqlite_source.setup_metrics["index_ns"],
                "ingest_ns": self.sqlite_source.setup_metrics["ingest_ns"],
                "analyze_ns": self.sqlite_source.setup_metrics["analyze_ns"],
            }
            self.row_counts = dict(self.sqlite_source.row_counts)
            self.rss_snapshots_mib = dict(self.sqlite_source.rss_snapshots_mib)
            self.rss_snapshots_mib["suite_connect"] = _rss_mib()
            return

        if self.backend == "duckdb":
            db_path = self.work_dir.path / "runtime.duckdb"
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_duckdb_connection(db_path)
            )
            self.rss_snapshots_mib["after_connect"] = _rss_mib()
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _configure_duckdb_from_sqlite(
                    self.duck,
                    self.sqlite_source.db_path,
                    self.graph_schema,
                )
            )
            self.rss_snapshots_mib["after_schema"] = _rss_mib()
            self.setup_metrics["index_ns"] = 0
            self.setup_metrics["ingest_ns"] = 0
            self.setup_metrics["analyze_ns"] = 0
            self.row_counts = dict(self.sqlite_source.row_counts)
            return

        if self.backend == "postgresql":
            if not self.postgres_dsn:
                raise ValueError("PostgreSQL backend requires a DSN.")
            self.db_size_mib = 0.0
            self.wal_size_mib = 0.0
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_postgresql_connection(self.postgres_dsn)
            )
            self.rss_snapshots_mib["after_connect"] = _rss_mib()
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _reset_postgresql_schema(self.postgresql, self.graph_schema)
            )
            _, schema_create_ns = _measure_ns(
                lambda: _create_postgresql_schema(self.postgresql, self.graph_schema)
            )
            self.setup_metrics["schema_ns"] += schema_create_ns
            self.rss_snapshots_mib["after_schema"] = _rss_mib()
            _, self.setup_metrics["index_ns"] = _measure_ns(
                lambda: _configure_postgresql_indexes(
                    self.postgresql,
                    self.graph_schema,
                    index_mode=self.index_mode,
                )
            )
            self.rss_snapshots_mib["after_index"] = _rss_mib()
            self.row_counts, self.setup_metrics["ingest_ns"] = _measure_ns(
                lambda: _seed_postgresql_from_sqlite(
                    self.postgresql,
                    sqlite_source=self.sqlite_source,
                    graph_schema=self.graph_schema,
                )
            )
            self.rss_snapshots_mib["after_ingest"] = _rss_mib()
            _, self.setup_metrics["analyze_ns"] = _measure_ns(
                lambda: _analyze_postgresql(self.postgresql)
            )
            self.rss_snapshots_mib["after_analyze"] = _rss_mib()
            return

        raise ValueError(f"Unsupported backend {self.backend!r}.")

    @property
    def sqlite(self) -> sqlite3.Connection:
        assert isinstance(self.connection, sqlite3.Connection)
        return self.connection

    @property
    def duck(self) -> DuckDBConnection:
        if duckdb is None:
            raise ValueError("duckdb is not installed.")
        assert isinstance(self.connection, duckdb.DuckDBPyConnection)
        return self.connection

    @property
    def postgresql(self) -> PostgreSQLConnection:
        if psycopg2 is None:
            raise ValueError("psycopg2 is not installed.")
        return self.connection

    def close(self) -> None:
        self.connection.close()
        self.work_dir.close()

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
        if self.backend != "sqlite":
            raise ValueError(
                "Rendered program execution is only supported on SQLite and PostgreSQL."
            )
        return PreparedArtifact(
            mode="program",
            compiled=cypherglot.render_cypher_program_text(
                query.query,
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
        if self.backend != "sqlite":
            raise ValueError(
                "Rendered program execution is only supported on SQLite and PostgreSQL."
            )
        _execute_sqlite_program(self.sqlite, artifact.compiled, commit=False)


def _execute_bound_postgresql_sql(
    cur: Any,
    sql: str,
    bindings: dict[str, object],
) -> None:
    bound_sql = sql
    for name in bindings:
        pyformat_token = f"%({name})s"
        bound_sql = bound_sql.replace(f"${name}", pyformat_token)
        bound_sql = bound_sql.replace(f":{name}", pyformat_token)

    if re.search(r"%\([A-Za-z_][A-Za-z0-9_]*\)s", bound_sql):
        cur.execute(bound_sql, bindings)
        return

    cur.execute(bound_sql)


def _execute_postgresql_program(
    conn: PostgreSQLConnection,
    program: cypherglot.RenderedCypherProgram,
) -> None:
    bindings: dict[str, object] = {}
    with conn.cursor() as cur:
        for step in program.steps:
            if isinstance(step, cypherglot.RenderedCypherLoop):
                _execute_bound_postgresql_sql(cur, step.source, bindings)
                rows = cur.fetchall()
                for row in rows:
                    loop_bindings = bindings | dict(
                        zip(step.row_bindings, row, strict=True)
                    )
                    for statement in step.body:
                        _execute_bound_postgresql_sql(
                            cur,
                            statement.sql,
                            loop_bindings,
                        )
                        if statement.bind_columns:
                            returned = cur.fetchone()
                            if returned is None:
                                raise ValueError(
                                    "Expected bound columns from program step."
                                )
                            loop_bindings |= dict(
                                zip(statement.bind_columns, returned, strict=True)
                            )
                continue

            _execute_bound_postgresql_sql(cur, step.sql, bindings)
            if step.bind_columns:
                returned = cur.fetchone()
                if returned is None:
                    raise ValueError("Expected bound columns from benchmark statement.")
                bindings |= dict(zip(step.bind_columns, returned, strict=True))


def _execute_sqlite_program(
    conn: sqlite3.Connection,
    program: cypherglot.RenderedCypherProgram,
    *,
    commit: bool,
) -> None:
    bindings: dict[str, object] = {}
    for step in program.steps:
        if isinstance(step, cypherglot.RenderedCypherLoop):
            rows = conn.execute(step.source, bindings).fetchall()
            for row in rows:
                loop_bindings = bindings | dict(
                    zip(step.row_bindings, row, strict=True)
                )
                for statement in step.body:
                    cursor = conn.execute(statement.sql, loop_bindings)
                    if statement.bind_columns:
                        returned = cursor.fetchone()
                        if returned is None:
                            raise ValueError(
                                "Expected bound columns from program step."
                            )
                        loop_bindings |= dict(
                            zip(statement.bind_columns, returned, strict=True)
                        )
            continue

        cursor = conn.execute(step.sql, bindings)
        if step.bind_columns:
            returned = cursor.fetchone()
            if returned is None:
                raise ValueError("Expected bound columns from benchmark statement.")
            bindings |= dict(zip(step.bind_columns, returned, strict=True))
    if commit:
        conn.commit()


def _rollback_sqlite_iteration(conn: sqlite3.Connection) -> None:
    conn.execute(f"ROLLBACK TO {SQLITE_SAVEPOINT}")
    conn.execute(f"RELEASE {SQLITE_SAVEPOINT}")


def _run_iteration(runner: _BackendRunner, query: CorpusQuery) -> dict[str, int]:
    if runner.backend == "postgresql":
        return _run_postgresql_iteration(runner, query)

    reset_ns = 0
    if query.mutation and runner.backend == "sqlite":
        runner.sqlite.execute(f"SAVEPOINT {SQLITE_SAVEPOINT}")
    try:
        total_started_ns = time.perf_counter_ns()
        artifact, compile_ns = _measure_ns(lambda: runner.compile_query(query))
        _, execute_ns = _measure_ns(lambda: runner.execute_query(artifact))
        end_to_end_ns = time.perf_counter_ns() - total_started_ns
    finally:
        if query.mutation and runner.backend == "sqlite":
            _, reset_ns = _measure_ns(
                lambda: _rollback_sqlite_iteration(runner.sqlite)
            )
    return {
        "compile_ns": compile_ns,
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
    }


def _run_postgresql_iteration(
    runner: _BackendRunner,
    query: CorpusQuery,
) -> dict[str, int]:
    reset_ns = 0
    try:
        total_started_ns = time.perf_counter_ns()
        artifact, compile_ns = _measure_ns(lambda: runner.compile_query(query))
        _, execute_ns = _measure_ns(lambda: runner.execute_query(artifact))
        end_to_end_ns = time.perf_counter_ns() - total_started_ns
        if query.mutation:
            _, reset_ns = _measure_ns(runner.postgresql.rollback)
        else:
            runner.postgresql.rollback()
    except Exception:
        runner.postgresql.rollback()
        raise
    return {
        "compile_ns": compile_ns,
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
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
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": runner.backend,
        "index_mode": runner.index_mode if runner.backend == "sqlite" else "n/a",
        "mode": query.mode,
        "mutation": query.mutation,
        "compile": _summarize(compile_latencies),
        "execute": _summarize(execute_latencies),
        "end_to_end": _summarize(end_to_end_latencies),
        "reset": _summarize(reset_latencies),
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
    sqlite_source: SharedSQLiteFixture,
    postgres_dsn: str | None = None,
    db_root_dir: Path | None = None,
    iteration_progress: bool = False,
) -> dict[str, object]:
    suite_progress_name = (
        f"{workload}/{backend}_{sqlite_source.index_mode}"
        if backend == "sqlite"
        else f"{workload}/{backend}"
    )
    suite_name = (
        f"{workload}-{backend}-{sqlite_source.index_mode}-suite"
        if backend == "sqlite"
        else f"{workload}-{backend}-suite"
    )
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
        _progress(f"{suite_progress_name}: suite complete")
        return {
            "backend": backend,
            "index_mode": runner.index_mode if backend == "sqlite" else "n/a",
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
            "db_path": str(runner.sqlite_source.db_path)
            if backend == "sqlite"
            else str(runner.work_dir.path / "runtime.duckdb"),
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
    started_at: datetime,
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
        "generated_at": started_at.isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "run_status": status,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
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
        "index_mode": index_mode,
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
            if backend not in {"sqlite", "duckdb"}:
                raise ValueError(
                    f"Runtime corpus item {index} has unsupported backend {backend!r}."
                )
            normalized_backends.append(backend)
        if mode == "program" and "duckdb" in normalized_backends:
            raise ValueError(
                f"Runtime corpus item {index} cannot run program mode on duckdb."
            )

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


def _filter_duckdb_olap_queries(queries: list[CorpusQuery]) -> list[CorpusQuery]:
    return [
        query
        for query in queries
        if "duckdb" in query.backends and query.name not in DUCKDB_OLAP_QUERY_SKIP_NAMES
    ]


def _filter_postgresql_queries(queries: list[CorpusQuery]) -> list[CorpusQuery]:
    return [query for query in queries if "sqlite" in query.backends]


def _benchmark_result(
    queries: list[CorpusQuery],
    *,
    iterations: int,
    warmup: int,
    oltp_iterations: int | None = None,
    oltp_warmup: int | None = None,
    olap_iterations: int | None = None,
    olap_warmup: int | None = None,
    include_duckdb: bool,
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

    index_modes = [index_mode] if index_mode != "both" else ["indexed", "unindexed"]

    workloads: dict[str, object] = {}
    if progress_callback is not None:
        progress_callback({"workloads": workloads, "token_map": token_map})
    sqlite_fixtures = {
        mode: _prepare_shared_sqlite_fixture(
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
                    "Transactional-style reads and mutations over a generated "
                    "multi-type type-aware SQLite graph."
                )
            }
            sqlite_oltp_queries = [
                query for query in oltp_queries if "sqlite" in query.backends
            ]
            postgresql_oltp_queries = _filter_postgresql_queries(oltp_queries)
            for mode, fixture in sqlite_fixtures.items():
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
                    progress_callback({"workloads": workloads, "token_map": token_map})
                if postgres_dsn:
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

        if olap_queries:
            workloads["olap"] = {
                "description": (
                    "Analytical read queries measured on indexed and unindexed "
                    "SQLite and PostgreSQL fixtures plus DuckDB over the same "
                    "generated graph."
                ),
                "duckdb_skipped_queries": sorted(DUCKDB_OLAP_QUERY_SKIP_NAMES),
            }
            sqlite_olap_queries = [
                query for query in olap_queries if "sqlite" in query.backends
            ]
            postgresql_olap_queries = _filter_postgresql_queries(olap_queries)
            duckdb_olap_queries = _filter_duckdb_olap_queries(olap_queries)
            for mode, fixture in sqlite_fixtures.items():
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
                    progress_callback({"workloads": workloads, "token_map": token_map})
                if postgres_dsn:
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
            if include_duckdb and duckdb_olap_queries:
                duckdb_source = sqlite_fixtures.get("indexed")
                if duckdb_source is None:
                    duckdb_source = next(iter(sqlite_fixtures.values()))
                skipped_names = sorted(
                    query.name
                    for query in olap_queries
                    if "duckdb" in query.backends
                    and query.name in DUCKDB_OLAP_QUERY_SKIP_NAMES
                )
                if skipped_names:
                    _progress(
                        "olap/duckdb: skipping query/queries not admitted for the "
                        "attached-SQLite DuckDB benchmark path: "
                        + ", ".join(skipped_names)
                    )
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
        for fixture in sqlite_fixtures.values():
            fixture.close()

    return {
        "workloads": workloads,
        "token_map": token_map,
    }


def _print_suite(name: str, suite: dict[str, object]) -> None:
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
            f"{key}={value:.2f} MiB"
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


def _parse_args() -> argparse.Namespace:
    return parse_sqlite_runtime_args(
        default_corpus_path=DEFAULT_CORPUS_PATH,
        default_output_path=DEFAULT_OUTPUT_PATH,
    )


def main() -> int:
    args = _parse_args()
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

    include_duckdb = not args.skip_duckdb
    if include_duckdb and duckdb is None:
        raise ValueError("duckdb is not installed. Install it or pass --skip-duckdb.")
    postgres_dsn = args.postgres_dsn.strip()
    if postgres_dsn and psycopg2 is None:
        raise ValueError(
            "psycopg2 is not installed. Install it or omit --postgres-dsn."
        )

    db_root_dir = None
    if args.db_root_dir is not None:
        run_name = (
            "benchmark-sqlite-runtime-"
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
            started_at=started_at,
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
    result = _benchmark_result(
        queries,
        iterations=args.iterations,
        warmup=args.warmup,
        oltp_iterations=args.oltp_iterations,
        oltp_warmup=args.oltp_warmup,
        olap_iterations=args.olap_iterations,
        olap_warmup=args.olap_warmup,
        include_duckdb=include_duckdb,
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
            if suite_name == "duckdb_skipped_queries":
                continue
            _print_suite(f"olap/{suite_name}", suite)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
