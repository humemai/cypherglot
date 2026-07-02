"""ClickHouse helpers for SQL runtime benchmarks.

ClickHouse is a columnar OLAP server. CypherGlot lowers Cypher to ClickHouse-
dialect SQL (via SQLGlot) and runs it here over a MergeTree-backed copy of the
generated graph. It is scoped to reads/OLAP: point updates/deletes are async,
non-transactional ALTER mutations, so the benchmark only exercises read queries.

Unlike the embedded backends, this talks to a server over HTTP via the
``clickhouse-connect`` driver, so it mirrors the PostgreSQL backend's
server-style shape (connection params instead of a file path).
"""

from __future__ import annotations

import csv
import threading
import uuid
from dataclasses import dataclass

import cypherglot

from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
)

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover - optional dependency
    clickhouse_connect = None


@dataclass(frozen=True, slots=True)
class ClickHouseConnectionParams:
    host: str
    port: int
    username: str
    password: str
    database: str


def _clickhouse_available() -> bool:
    return clickhouse_connect is not None


def _create_clickhouse_client(params: ClickHouseConnectionParams):
    if clickhouse_connect is None:
        raise ValueError("clickhouse-connect is not installed.")
    # Sessionless: an abandoned server-side query must not lock a session and
    # cascade SESSION_IS_LOCKED over every later query in the suite.
    clickhouse_connect.common.set_setting("autogenerate_session_id", False)
    return clickhouse_connect.get_client(
        host=params.host,
        port=params.port,
        username=params.username,
        password=params.password,
        database=params.database,
        # Outer socket net well above any query budget; real enforcement is
        # max_execution_time plus the KILL QUERY watchdog below.
        send_receive_timeout=910,
    )


def _clickhouse_server_version(params: ClickHouseConnectionParams) -> str | None:
    if clickhouse_connect is None:
        return None
    client = _create_clickhouse_client(params)
    try:
        return str(client.server_version)
    finally:
        client.close()


def _reset_clickhouse_schema(
    client,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    table_names = [
        *(edge_type.table_name for edge_type in graph_schema.edge_types),
        *(node_type.table_name for node_type in graph_schema.node_types),
    ]
    for table_name in table_names:
        client.command(f"DROP TABLE IF EXISTS {table_name}")


def _create_clickhouse_schema(
    client,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in graph_schema.ddl("clickhouse"):
        client.command(statement.rstrip(";"))


def _clickhouse_literal(value: str, logical_type: str) -> object:
    if value == "":
        return None
    if logical_type == "integer":
        return int(value)
    if logical_type == "float":
        return float(value)
    if logical_type == "boolean":
        return int(value)
    return value


def _clickhouse_table_logical_types(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, list[str]]:
    logical_types: dict[str, list[str]] = {}
    for node_type in graph_schema.node_types:
        logical_types[node_type.table_name] = [
            "integer",
            *(property_schema.logical_type for property_schema in node_type.properties),
        ]
    for edge_type in graph_schema.edge_types:
        logical_types[edge_type.table_name] = [
            "integer",
            "integer",
            "integer",
            *(property_schema.logical_type for property_schema in edge_type.properties),
        ]
    return logical_types


def _seed_clickhouse_from_generated_fixture(
    client,
    *,
    graph_schema: cypherglot.GraphSchema,
    generated_fixture: GeneratedGraphFixture,
    ingest_batch_size: int,
    progress_label: str | None = None,
) -> dict[str, int]:
    from scripts.benchmarks.common.shared import _progress

    logical_types = _clickhouse_table_logical_types(graph_schema)
    table_names = [
        *(node_type.table_name for node_type in graph_schema.node_types),
        *(edge_type.table_name for edge_type in graph_schema.edge_types),
    ]
    for table_index, table_name in enumerate(table_names, start=1):
        if progress_label is not None:
            _progress(
                f"{progress_label}: table {table_index}/{len(table_names)} "
                f"({table_name})"
            )
        column_names = list(generated_fixture.table_columns[table_name])
        typed_columns = logical_types[table_name]
        batch: list[list[object]] = []
        with generated_fixture.table_csv_paths[table_name].open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            reader = csv.reader(handle)
            next(reader, None)
            for row in reader:
                batch.append(
                    [
                        _clickhouse_literal(value, logical_type)
                        for value, logical_type in zip(row, typed_columns, strict=True)
                    ]
                )
                if len(batch) < ingest_batch_size:
                    continue
                client.insert(table_name, batch, column_names=column_names)
                batch.clear()
        if batch:
            client.insert(table_name, batch, column_names=column_names)
    return dict(generated_fixture.row_counts)


def _optimize_clickhouse(client, graph_schema: cypherglot.GraphSchema) -> None:
    # Merge parts so reads hit a stable layout (ClickHouse's analogue of ANALYZE
    # for benchmarking; the MergeTree ORDER BY key is the primary index).
    table_names = [
        *(node_type.table_name for node_type in graph_schema.node_types),
        *(edge_type.table_name for edge_type in graph_schema.edge_types),
    ]
    for table_name in table_names:
        client.command(f"OPTIMIZE TABLE {table_name} FINAL")


def _execute_clickhouse_statement(
    client,
    sql: str,
    timeout_ms: float | None = None,
    kill_client=None,
) -> None:
    if timeout_ms is None:
        client.query(sql)
        return
    # Two enforcement layers. max_execution_time is checked at progress
    # points, which heavy join builds can stall past (observed: a deep-hop
    # union join ran to the HTTP read timeout with the timer armed). The
    # watchdog thread issues KILL QUERY at the budget via a second client, so
    # cancellation lands regardless of the server's check granularity.
    query_id = uuid.uuid4().hex
    watchdog: threading.Timer | None = None
    if kill_client is not None:
        def _kill() -> None:
            try:
                kill_client.command(
                    f"KILL QUERY WHERE query_id = '{query_id}' ASYNC"
                )
            except Exception:
                pass

        watchdog = threading.Timer(timeout_ms / 1000.0 + 2.0, _kill)
        watchdog.daemon = True
        watchdog.start()
    try:
        client.query(
            sql,
            settings={
                "max_execution_time": max(1, int(timeout_ms / 1000.0)),
                # query_id rides the settings dict onto the HTTP request, so
                # the watchdog can target this exact query with KILL QUERY.
                "query_id": query_id,
            },
        )
    finally:
        if watchdog is not None:
            watchdog.cancel()
