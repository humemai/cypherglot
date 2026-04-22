"""PostgreSQL helpers for SQL runtime benchmarks."""

from __future__ import annotations

import csv
from pathlib import Path
import re
from typing import Any

import cypherglot

from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    PostgreSQLConnection,
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


def _postgresql_server_version(dsn: str) -> str:
    conn = _create_postgresql_connection(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None or not row or not isinstance(row[0], str):
        raise ValueError("Unable to determine PostgreSQL server version.")
    return row[0]


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
        value = coerced[index]
        if value is None:
            coerced[index] = None
        elif isinstance(value, str):
            coerced[index] = value not in {"", "0", "false", "False"}
        else:
            coerced[index] = bool(value)
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


def _write_fixture_table_postgresql_copy_data(
    fixture: GeneratedGraphFixture,
    *,
    table_name: str,
    copy_path: Path,
    boolean_indexes: tuple[int, ...],
) -> int:
    max_id = 0
    with copy_path.open("w", encoding="utf-8", newline="") as handle:
        with fixture.table_csv_paths[table_name].open(
            "r",
            encoding="utf-8",
            newline="",
        ) as csv_handle:
            reader = csv.reader(csv_handle)
            next(reader, None)
            for row in reader:
                if row:
                    max_id = max(max_id, int(row[0]))
                coerced_row = _coerce_postgresql_row(tuple(row), boolean_indexes)
                handle.write(
                    "\t".join(
                        _postgresql_copy_value(value) for value in coerced_row
                    )
                )
                handle.write("\n")
    return max_id


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


def _seed_postgresql_from_fixture(
    conn: PostgreSQLConnection,
    *,
    generated_fixture: GeneratedGraphFixture,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, int]:
    boolean_indexes = _postgresql_boolean_indexes(graph_schema)
    with conn.cursor() as cur:
        table_names = [
            *(node_type.table_name for node_type in graph_schema.node_types),
            *(edge_type.table_name for edge_type in graph_schema.edge_types),
        ]
        for table_name in table_names:
            table_boolean_indexes = boolean_indexes[table_name]
            copy_path = generated_fixture.work_dir.path / f"{table_name}.postgres.copy"
            max_id = _write_fixture_table_postgresql_copy_data(
                generated_fixture,
                table_name=table_name,
                copy_path=copy_path,
                boolean_indexes=table_boolean_indexes,
            )
            _copy_postgresql_table_from_file(
                cur,
                table_name=table_name,
                copy_path=copy_path,
            )
            if max_id > 0:
                _set_postgresql_sequence(conn, table_name, max_id)
    conn.commit()
    return dict(generated_fixture.row_counts)


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
