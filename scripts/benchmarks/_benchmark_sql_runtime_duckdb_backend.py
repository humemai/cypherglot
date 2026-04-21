"""DuckDB helpers for SQL runtime benchmarks."""

from __future__ import annotations

from pathlib import Path

import cypherglot

from _benchmark_sql_runtime_shared import (
    DuckDBConnection,
    GeneratedGraphFixture,
    _query_index_statements,
)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None


def _duckdb_available() -> bool:
    return duckdb is not None


def _duckdb_version() -> str | None:
    if duckdb is None:
        return None
    return str(duckdb.__version__)


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
            "BIGINT",
            *(
                _duckdb_csv_type_name(property_schema.logical_type)
                for property_schema in node_type.properties
            ),
        ]

    for edge_type in graph_schema.edge_types:
        types[edge_type.table_name] = [
            "BIGINT",
            "BIGINT",
            "BIGINT",
            *(
                _duckdb_csv_type_name(property_schema.logical_type)
                for property_schema in edge_type.properties
            ),
        ]

    return types


def _seed_duckdb_from_fixture(
    conn: DuckDBConnection,
    *,
    generated_fixture: GeneratedGraphFixture,
    graph_schema: cypherglot.GraphSchema,
) -> dict[str, int]:
    table_names = [
        *(node_type.table_name for node_type in graph_schema.node_types),
        *(edge_type.table_name for edge_type in graph_schema.edge_types),
    ]
    column_types = _duckdb_table_column_types(graph_schema)
    for table_name in table_names:
        column_names = generated_fixture.table_columns[table_name]
        csv_path = generated_fixture.table_csv_paths[table_name]
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
        _advance_duckdb_sequence(conn, table_name)

    return dict(generated_fixture.row_counts)


def _advance_duckdb_sequence(
    conn: DuckDBConnection,
    table_name: str,
) -> None:
    max_id_row = conn.execute(
        f"SELECT COALESCE(MAX(id), 0) FROM {table_name}"
    ).fetchone()
    if max_id_row is None:
        return

    max_id = int(max_id_row[0])
    if max_id <= 0:
        return

    sequence_name = f"{table_name}_id_seq"
    # DuckDB does not reliably advance the sequence here unless the query result
    # is fully consumed. Using an aggregate forces evaluation without materializing
    # every nextval row in Python.
    conn.execute(
        f"SELECT MAX(nextval('{sequence_name}')) FROM range(?)",
        [max_id],
    ).fetchone()


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
