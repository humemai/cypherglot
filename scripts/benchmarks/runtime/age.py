"""Apache AGE runtime helpers — a native-Cypher baseline on PostgreSQL.

Apache AGE is a PostgreSQL extension that runs openCypher natively via
``cypher('graph', $$ <query> $$) AS (col agtype, ...)``. Unlike the SQL compile
targets, CypherGlot does NOT lower for AGE: AGE runs the Cypher directly, so it
is a *baseline* (like Neo4j/ArcadeDB/Ladybug). Its unique value is that AGE runs
on PostgreSQL and CypherGlot also targets PostgreSQL, giving the cleanest
head-to-head: native graph extension vs. lowered SQL on the same engine.

This module is deliberately self-contained (psycopg2 + sqlglot) so it can drive
both the conformance comparison and a performance benchmark. It reuses
CypherGlot's own frontend to discover RETURN column names, so the mandatory
``AS (...)`` column list is built automatically for any admitted-subset query.
"""

from __future__ import annotations

import json
from typing import Any

import sqlglot

import cypherglot

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None  # type: ignore[assignment]


# AGE lacks a few constructs CypherGlot admits for the SQL backends. Queries that
# use them are recorded as skipped rather than failed (the set itself is a
# "native AGE vs lowered SQL" finding).
#   - UNION / UNION ALL: AGE's cypher() wrapper returns a single set; stacking
#     two cypher() calls needs UNION at the SQL layer, not inside one call.
AGE_UNSUPPORTED_QUERIES: frozenset[str] = frozenset(
    {
        "union_distinct",
        "union_dedup",
        "union_all_keeps",
    }
)


def _age_available() -> bool:
    return psycopg2 is not None


def _age_uses_union(query: str) -> bool:
    return " UNION " in f" {query.upper()} "


def setup_age(conn, graph_name: str) -> None:
    """Load the AGE extension and (re)create an empty graph on the connection."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS age")
        cur.execute("LOAD 'age'")
        cur.execute('SET search_path = ag_catalog, "$user", public')
        cur.execute(
            "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = %s",
            (graph_name,),
        )
        (exists,) = cur.fetchone()
        if exists:
            cur.execute("SELECT drop_graph(%s, true)", (graph_name,))
        cur.execute("SELECT create_graph(%s)", (graph_name,))
    conn.commit()


def _prepare_age_cursor(conn) -> Any:
    cur = conn.cursor()
    cur.execute("LOAD 'age'")
    cur.execute('SET search_path = ag_catalog, "$user", public')
    return cur


def create_age_labels(
    conn,
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    """Declare a vertex label per node type and an edge label per edge type."""
    with _prepare_age_cursor(conn) as cur:
        for node_type in graph_schema.node_types:
            cur.execute("SELECT create_vlabel(%s, %s)", (graph_name, node_type.name))
        for edge_type in graph_schema.edge_types:
            cur.execute("SELECT create_elabel(%s, %s)", (graph_name, edge_type.name))
    conn.commit()


def _agtype_property_literal(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _agtype_property_map(properties: dict[str, object]) -> str:
    return "{" + ", ".join(
        f"{key}: {_agtype_property_literal(value)}"
        for key, value in properties.items()
    ) + "}"


def run_age_cypher(
    cur,
    graph_name: str,
    cypher: str,
    *,
    column_clause: str = "result agtype",
) -> list[tuple[object, ...]]:
    """Execute a Cypher statement through AGE and return raw agtype rows."""
    cur.execute(
        f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS ({column_clause})"
    )
    if cur.description is None:
        return []
    return cur.fetchall()


def seed_age_node(
    cur,
    graph_name: str,
    label: str,
    properties: dict[str, object],
) -> None:
    run_age_cypher(
        cur,
        graph_name,
        f"CREATE (:{label} {_agtype_property_map(properties)})",
    )


def seed_age_edge(
    cur,
    graph_name: str,
    *,
    edge_label: str,
    source_label: str,
    source_id: int,
    target_label: str,
    target_id: int,
    properties: dict[str, object] | None = None,
) -> None:
    prop_map = _agtype_property_map(properties) if properties else ""
    run_age_cypher(
        cur,
        graph_name,
        (
            f"MATCH (a:{source_label} {{id: {source_id}}}), "
            f"(b:{target_label} {{id: {target_id}}}) "
            f"CREATE (a)-[:{edge_label} {prop_map}]->(b)"
        ),
    )


def age_return_columns(
    query: str,
    schema_context: cypherglot.CompilerSchemaContext,
) -> list[str]:
    """Discover RETURN column names for a query by compiling it to SQL with
    CypherGlot's own frontend and reading the projected output names. This
    builds AGE's mandatory ``AS (c1 agtype, ...)`` list automatically."""
    sql = cypherglot.to_sql(query, backend="sqlite", schema_context=schema_context)
    expression = sqlglot.parse_one(sql, read="sqlite")
    selects = getattr(expression, "selects", None)
    if not selects:
        raise ValueError(f"Unable to determine output columns for query: {query}")
    return [select.alias_or_name for select in selects]


def age_column_clause(columns: list[str]) -> str:
    return ", ".join(f'"{name}" agtype' for name in columns)


def parse_agtype(value: object) -> object:
    """AGE returns agtype as text via psycopg2 (e.g. ``"Alice"``, ``30``,
    ``["Bob", "Carol"]``, ``true``). Decode it to a native Python value."""
    if value is None:
        return None
    if isinstance(value, (int, float, bool, list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def execute_age_query(
    cur,
    graph_name: str,
    query: str,
    schema_context: cypherglot.CompilerSchemaContext,
) -> tuple[list[str], list[tuple[object, ...]]]:
    """Run an admitted-subset Cypher query natively on AGE and return
    ``(column_names, rows)`` with each cell decoded from agtype."""
    columns = age_return_columns(query, schema_context)
    rows = run_age_cypher(
        cur,
        graph_name,
        query,
        column_clause=age_column_clause(columns),
    )
    decoded = [tuple(parse_agtype(value) for value in row) for row in rows]
    return columns, decoded
