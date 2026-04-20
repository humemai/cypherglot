"""PostgreSQL helpers for SQL runtime benchmarks."""

from __future__ import annotations

from pathlib import Path
import re
import sqlite3
from typing import Any

import cypherglot

from _benchmark_sql_runtime_shared import (
    PostgreSQLConnection,
    SharedSQLiteFixture,
    _query_index_statements,
)

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None


def _postgresql_available() -> bool:
    return psycopg2 is not None


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


def _set_postgresql_sequence(
    conn: PostgreSQLConnection,
    table_name: str,
    max_id: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT setval('{table_name}_id_seq', %s, true)", (max_id,))


def _postgresql_boolean_indexes(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, tuple[int, ...]]:
    boolean_indexes: dict[str, tuple[int, ...]] = {}

    for node_type in graph_schema.node_types:
        indexes = tuple(
            column_index
            for column_index, property_schema in enumerate(
                node_type.properties,
                start=1,
            )
            if property_schema.name == "active"
            or property_schema.name.startswith("flag_")
        )
        boolean_indexes[node_type.table_name] = indexes

    for edge_type in graph_schema.edge_types:
        indexes = tuple(
            column_index
            for column_index, property_schema in enumerate(
                edge_type.properties,
                start=3,
            )
            if property_schema.name == "active"
            or property_schema.name.startswith("flag_")
        )
        boolean_indexes[edge_type.table_name] = indexes

    return boolean_indexes


def _coerce_postgresql_row(
    row: tuple[object, ...],
    boolean_indexes: tuple[int, ...],
) -> tuple[object, ...]:
    if not boolean_indexes:
        return row

    coerced = list(row)
    for index in boolean_indexes:
        coerced[index] = bool(coerced[index]) if coerced[index] is not None else None
    return tuple(coerced)


def _postgresql_copy_value(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "t" if value else "f"

    text = str(value)
    return (
        text.replace("\\", r"\\")
        .replace("\t", r"\t")
        .replace("\n", r"\n")
        .replace("\r", r"\r")
    )


def _write_sqlite_table_postgresql_copy_data(
    source: sqlite3.Connection,
    *,
    table_name: str,
    copy_path: Path,
    boolean_indexes: tuple[int, ...],
) -> None:
    cursor = source.execute(f"SELECT * FROM {table_name} ORDER BY id")
    with copy_path.open("w", encoding="utf-8", newline="") as handle:
        while True:
            rows = cursor.fetchmany(5_000)
            if not rows:
                break
            for row in rows:
                coerced_row = _coerce_postgresql_row(row, boolean_indexes)
                handle.write(
                    "\t".join(
                        _postgresql_copy_value(value) for value in coerced_row
                    )
                )
                handle.write("\n")


def _copy_postgresql_table_from_file(
    cur: Any,
    *,
    table_name: str,
    copy_path: Path,
) -> None:
    with copy_path.open("r", encoding="utf-8", newline="") as handle:
        cur.copy_expert(
            (
                f"COPY {table_name} FROM STDIN WITH ("
                r"FORMAT text, DELIMITER E'\t', NULL '\N'"
                ")"
            ),
            handle,
        )


def _seed_postgresql_from_sqlite(
    conn: PostgreSQLConnection,
    *,
    sqlite_source: SharedSQLiteFixture,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, int]:
    source = sqlite3.connect(sqlite_source.db_path)
    boolean_indexes = _postgresql_boolean_indexes(graph_schema)
    try:
        with conn.cursor() as cur:
            table_names = [
                *(node_type.table_name for node_type in graph_schema.node_types),
                *(edge_type.table_name for edge_type in graph_schema.edge_types),
            ]
            for table_name in table_names:
                table_boolean_indexes = boolean_indexes[table_name]
                copy_path = sqlite_source.work_dir.path / f"{table_name}.postgres.copy"
                _write_sqlite_table_postgresql_copy_data(
                    source,
                    table_name=table_name,
                    copy_path=copy_path,
                    boolean_indexes=table_boolean_indexes,
                )
                _copy_postgresql_table_from_file(
                    cur,
                    table_name=table_name,
                    copy_path=copy_path,
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
