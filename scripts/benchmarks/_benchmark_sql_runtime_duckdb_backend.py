"""DuckDB helpers for SQL runtime benchmarks."""

from __future__ import annotations

import csv
from pathlib import Path
import sqlite3

import cypherglot

from _benchmark_sql_runtime_shared import (
    DuckDBConnection,
    SharedSQLiteFixture,
    _query_index_statements,
)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None


def _duckdb_available() -> bool:
    return duckdb is not None


def _create_duckdb_connection(db_path: Path) -> DuckDBConnection:
    if duckdb is None:
        raise ValueError("duckdb is not installed.")
    return duckdb.connect(str(db_path))


def _create_duckdb_schema(
    conn: DuckDBConnection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in graph_schema.ddl("duckdb"):
        conn.execute(statement)


def _create_duckdb_query_indexes(
    conn: DuckDBConnection,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _query_index_statements(graph_schema):
        conn.execute(statement)


def _duckdb_csv_type_name(type_name: str) -> str:
    if type_name == "boolean":
        return "BOOLEAN"
    if type_name == "integer":
        return "BIGINT"
    if type_name == "float":
        return "DOUBLE"
    return "VARCHAR"


def _duckdb_table_column_types(
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, list[str]]:
    types: dict[str, list[str]] = {}

    for node_type in graph_schema.node_types:
        types[node_type.table_name] = [
            *(
                _duckdb_csv_type_name(property_schema.logical_type)
                for property_schema in node_type.properties
            ),
        ]

    for edge_type in graph_schema.edge_types:
        types[edge_type.table_name] = [
            "BIGINT",
            "BIGINT",
            *(
                _duckdb_csv_type_name(property_schema.logical_type)
                for property_schema in edge_type.properties
            ),
        ]

    return types


def _write_sqlite_table_csv(
    source: sqlite3.Connection,
    *,
    table_name: str,
    column_names: list[str],
    csv_path: Path,
) -> None:
    projection = ", ".join(column_names)
    cursor = source.execute(f"SELECT {projection} FROM {table_name} ORDER BY id")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        description = cursor.description
        if description is None:
            return
        writer.writerow([column[0] for column in description])
        while True:
            rows = cursor.fetchmany(5_000)
            if not rows:
                break
            writer.writerows(rows)


def _seed_duckdb_from_sqlite(
    conn: DuckDBConnection,
    *,
    sqlite_source: SharedSQLiteFixture,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, int]:
    source = sqlite3.connect(sqlite_source.db_path)
    try:
        table_names = [
            *(node_type.table_name for node_type in graph_schema.node_types),
            *(edge_type.table_name for edge_type in graph_schema.edge_types),
        ]
        column_types = _duckdb_table_column_types(graph_schema)
        for table_name in table_names:
            column_names = [
                column[1]
                for column in source.execute(f"PRAGMA table_info({table_name})")
                if column[1] != "id"
            ]
            csv_path = sqlite_source.work_dir.path / f"{table_name}.csv"
            _write_sqlite_table_csv(
                source,
                table_name=table_name,
                column_names=column_names,
                csv_path=csv_path,
            )
            columns_spec = ", ".join(
                f"'{column_name}': '{column_type}'"
                for column_name, column_type in zip(
                    column_names,
                    column_types[table_name],
                    strict=True,
                )
            )
            conn.execute(
                f"INSERT INTO {table_name} ({', '.join(column_names)}) "
                "SELECT * FROM read_csv(?, header=true, columns={"
                + columns_spec
                + "})",
                [str(csv_path)],
            )
    finally:
        source.close()

    return dict(sqlite_source.row_counts)


def _analyze_duckdb(conn: DuckDBConnection) -> None:
    conn.execute("ANALYZE")


def _execute_duckdb_program(
    conn: DuckDBConnection,
    program: cypherglot.RenderedCypherProgram,
) -> None:
    bindings: dict[str, object] = {}
    for step in program.steps:
        if isinstance(step, cypherglot.RenderedCypherLoop):
            loop_source = step.source
            for name, value in bindings.items():
                loop_source = loop_source.replace(f"${name}", str(value))
                loop_source = loop_source.replace(f":{name}", str(value))

            rows = conn.execute(loop_source).fetchall()
            for row in rows:
                loop_bindings = bindings | dict(
                    zip(step.row_bindings, row, strict=True)
                )
                for statement in step.body:
                    sql = statement.sql
                    for name, value in loop_bindings.items():
                        sql = sql.replace(f"${name}", str(value))
                        sql = sql.replace(f":{name}", str(value))

                    cursor = conn.execute(sql)
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

        sql = step.sql
        for name, value in bindings.items():
            sql = sql.replace(f"${name}", str(value))
            sql = sql.replace(f":{name}", str(value))

        cursor = conn.execute(sql)
        if step.bind_columns:
            returned = cursor.fetchone()
            if returned is None:
                raise ValueError("Expected bound columns from benchmark statement.")
            bindings |= dict(zip(step.bind_columns, returned, strict=True))


def _duckdb_file_size_mib(db_path: Path) -> float:
    if not db_path.exists():
        return 0.0
    return db_path.stat().st_size / (1024.0 * 1024.0)
