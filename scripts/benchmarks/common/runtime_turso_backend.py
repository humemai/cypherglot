"""Turso Database fixture and execution helpers for SQL runtime benchmarks.

Turso (the Rust SQLite rewrite, driver ``pyturso``) speaks SQLite's SQL dialect,
so this mirrors ``runtime_sqlite_backend.py``. Two differences from SQLite:
- pyturso's Connection has no ``create_function``, so the SQLite custom scalar
  functions (REVERSE/LEFT/RIGHT/SPLIT/STR_POSITION) cannot be registered; queries
  that need them will raise at execution and must be recorded as unsupported.
- DDL is executed statement-by-statement (no ``executescript``).
"""

from __future__ import annotations

import csv
from pathlib import Path

import cypherglot

from scripts.benchmarks.common.shared import (
    _progress,
)
from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    _query_index_statements,
)

try:
    import turso
except ImportError:  # pragma: no cover - optional dependency
    turso = None


def _turso_available() -> bool:
    return turso is not None


def _turso_version() -> str | None:
    if turso is None:
        return None
    return getattr(turso, "__version__", "unknown")


def _turso_file_size_mib(db_path: Path) -> tuple[float, float]:
    db_size = db_path.stat().st_size / (1024.0 * 1024.0) if db_path.exists() else 0.0
    wal_path = db_path.with_name(db_path.name + "-wal")
    wal_size = (
        wal_path.stat().st_size / (1024.0 * 1024.0) if wal_path.exists() else 0.0
    )
    return db_size, wal_size


def _create_turso_connection(db_path: Path):
    if turso is None:
        raise ValueError("turso (pyturso) is not installed.")
    conn = turso.connect(str(db_path))
    for pragma in (
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA foreign_keys = ON",
    ):
        try:
            conn.execute(pragma)
        except Exception:  # pragma: no cover - tolerate pre-1.0 pragma gaps
            pass
    # NB: pyturso has no create_function; the SQLite custom string functions
    # (REVERSE/LEFT/RIGHT/SPLIT/STR_POSITION) are unavailable here.
    return conn


def _create_turso_schema(
    conn,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in graph_schema.ddl("turso"):
        conn.execute(statement)


def _drop_all_turso_indexes(conn) -> None:
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
    conn,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _query_index_statements(graph_schema):
        conn.execute(statement)


def _configure_turso_indexes(
    conn,
    graph_schema: cypherglot.GraphSchema,
    *,
    index_mode: str,
) -> None:
    if index_mode == "indexed":
        _create_query_indexes(conn, graph_schema)
        return
    if index_mode == "unindexed":
        _drop_all_turso_indexes(conn)
        return
    raise ValueError(f"Unsupported index mode {index_mode!r}.")


def _turso_literal(value: str, logical_type: str) -> object:
    if value == "":
        return None
    if logical_type == "integer":
        return int(value)
    if logical_type == "float":
        return float(value)
    if logical_type == "boolean":
        return int(value)
    return value


def _turso_table_logical_types(
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


def _seed_turso_from_generated_fixture(
    conn,
    *,
    graph_schema: cypherglot.GraphSchema,
    generated_fixture: GeneratedGraphFixture,
    ingest_batch_size: int,
    progress_label: str | None = None,
) -> dict[str, int]:
    logical_types = _turso_table_logical_types(graph_schema)
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
        column_names = generated_fixture.table_columns[table_name]
        placeholders = ", ".join("?" for _ in column_names)
        typed_columns = logical_types[table_name]
        batch: list[tuple[object, ...]] = []
        with generated_fixture.table_csv_paths[table_name].open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            reader = csv.reader(handle)
            next(reader, None)
            for row in reader:
                batch.append(
                    tuple(
                        _turso_literal(value, logical_type)
                        for value, logical_type in zip(row, typed_columns, strict=True)
                    )
                )
                if len(batch) < ingest_batch_size:
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
    return dict(generated_fixture.row_counts)


def _analyze_turso(conn) -> None:
    conn.execute("ANALYZE")


def _execute_turso_program(
    conn,
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


def _rollback_turso_iteration(conn, savepoint: str) -> None:
    conn.execute(f"ROLLBACK TO {savepoint}")
    conn.execute(f"RELEASE {savepoint}")
