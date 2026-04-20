"""SQLite fixture and execution helpers for SQL runtime benchmarks."""

from __future__ import annotations

import json
import sqlite3
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
)
from _benchmark_sql_runtime_shared import (
    SharedSQLiteFixture,
    _capture_rss_snapshot,
    _create_managed_directory,
    _query_index_statements,
)


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


def _create_query_indexes(
    conn: sqlite3.Connection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _query_index_statements(graph_schema):
        conn.execute(statement)


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
        batch: list[tuple[Any, ...]] = []
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
    progress_label = f"shared-fixture/{index_mode}"
    work_dir = _create_managed_directory(
        root_dir=db_root_dir,
        prefix=f"cypherglot-runtime-{index_mode}-",
        name=f"sqlite-{index_mode}",
    )
    db_path = work_dir.path / "runtime.sqlite3"
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}

    _progress(
        f"{progress_label}: creating fixture "
        f"({scale.total_nodes} nodes, {scale.total_edges} edges)"
    )
    conn, connect_ns = _measure_ns(lambda: _create_sqlite_connection(db_path))
    rss_snapshots_mib["after_connect"] = _capture_rss_snapshot(backend="sqlite")
    try:
        _progress(f"{progress_label}: creating schema")
        _, schema_ns = _measure_ns(lambda: _create_sqlite_schema(conn, graph_schema))
        rss_snapshots_mib["after_schema"] = _capture_rss_snapshot(backend="sqlite")
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
        rss_snapshots_mib["after_ingest"] = _capture_rss_snapshot(backend="sqlite")
        _progress(f"{progress_label}: configuring indexes")
        _, index_ns = _measure_ns(
            lambda: _configure_sqlite_indexes(
                conn,
                graph_schema,
                index_mode=index_mode,
            )
        )
        rss_snapshots_mib["after_index"] = _capture_rss_snapshot(backend="sqlite")
        _progress(f"{progress_label}: analyzing source-fixture statistics")
        _, analyze_ns = _measure_ns(lambda: _analyze_sqlite(conn))
        rss_snapshots_mib["after_analyze"] = _capture_rss_snapshot(backend="sqlite")
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


def _rollback_sqlite_iteration(conn: sqlite3.Connection, savepoint: str) -> None:
    conn.execute(f"ROLLBACK TO {savepoint}")
    conn.execute(f"RELEASE {savepoint}")


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
