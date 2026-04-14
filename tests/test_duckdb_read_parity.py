from __future__ import annotations

import json
import sqlite3
import unittest

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional test dependency
    duckdb = None


@unittest.skipIf(duckdb is None, "duckdb is not installed")
class DuckDBReadParityTests(unittest.TestCase):
    def setUp(self) -> None:
        assert duckdb is not None
        self.graph_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(
                        PropertyField("note", "string"),
                        PropertyField("weight", "float"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
            ),
        )
        self.schema_context = CompilerSchemaContext.type_aware(self.graph_schema)

        self.sqlite = sqlite3.connect(":memory:")
        self.sqlite.executescript("\n".join(self.graph_schema.sqlite_ddl()))

        self.duckdb = duckdb.connect()
        self.duckdb.execute(
            "CREATE TABLE cg_node_user ("
            "id BIGINT, name VARCHAR, age BIGINT, score DOUBLE, active BOOLEAN)"
        )
        self.duckdb.execute("CREATE TABLE cg_node_company (id BIGINT, name VARCHAR)")
        self.duckdb.execute(
            "CREATE TABLE cg_edge_knows ("
            "id BIGINT, from_id BIGINT, to_id BIGINT, note VARCHAR, "
            "weight DOUBLE, score DOUBLE, active BOOLEAN)"
        )
        self.duckdb.execute(
            "CREATE TABLE cg_edge_works_at ("
            "id BIGINT, from_id BIGINT, to_id BIGINT, since BIGINT)"
        )
        self._seed_graphs()

    def tearDown(self) -> None:
        self.sqlite.close()
        self.duckdb.close()

    def test_curated_admitted_reads_match_sqlite_results(self) -> None:
        queries = (
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            "MATCH (u:User) RETURN u AS user ORDER BY u.age, u.name",
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' "
            "RETURN u.name AS name ORDER BY name",
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Zed' RETURN u.name AS name",
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
            "MATCH (u:User) RETURN count(*) AS total",
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean "
            "ORDER BY mean DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "RETURN id(a) AS uid, type(r) AS rel_type, "
            "startNode(r).id AS start_id, endNode(r).id AS end_id "
            "ORDER BY uid, end_id",
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'Alice' "
            "RETURN b.name AS friend ORDER BY friend",
        )

        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(
                    self._execute_sqlite(query),
                    self._execute_duckdb(query),
                )

    def _seed_graphs(self) -> None:
        user_rows = [
            (1, "Alice", 30, 1.2, 1),
            (2, "Bob", 25, 2.8, 0),
            (3, "Alice", 22, 3.0, 1),
            (4, "Cara", 4, 4.4, 0),
        ]
        company_rows = [(5, "Acme")]
        knows_rows = [
            (10, 1, 2, "Alice met", 1.5, 2.2, 1),
            (11, 2, 4, "coworker", 0.5, 3.7, 0),
            (12, 3, 2, "friend", 2.0, 1.1, 1),
        ]
        works_at_rows = [(20, 2, 5, 2020)]

        self.sqlite.executemany(
            "INSERT INTO cg_node_user (id, name, age, score, active) "
            "VALUES (?, ?, ?, ?, ?)",
            user_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            company_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO cg_edge_knows (id, from_id, to_id, note, weight, "
            "score, active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            knows_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
            "VALUES (?, ?, ?, ?)",
            works_at_rows,
        )
        self.sqlite.commit()

        for row in user_rows:
            self.duckdb.execute("INSERT INTO cg_node_user VALUES (?, ?, ?, ?, ?)", row)
        for row in company_rows:
            self.duckdb.execute("INSERT INTO cg_node_company VALUES (?, ?)", row)
        for row in knows_rows:
            self.duckdb.execute(
                "INSERT INTO cg_edge_knows VALUES (?, ?, ?, ?, ?, ?, ?)",
                row,
            )
        for row in works_at_rows:
            self.duckdb.execute("INSERT INTO cg_edge_works_at VALUES (?, ?, ?, ?)", row)

    def _execute_sqlite(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(query, schema_context=self.schema_context)
        return self._stabilize_rows(
            self._normalize_rows(self.sqlite.execute(sql).fetchall())
        )

    def _execute_duckdb(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query,
            dialect="duckdb",
            schema_context=self.schema_context,
        )
        return self._stabilize_rows(
            self._normalize_rows(self.duckdb.execute(sql).fetchall())
        )

    def _stabilize_rows(
        self,
        rows: list[tuple[object, ...]],
    ) -> list[tuple[object, ...]]:
        return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))

    def _normalize_rows(
        self,
        rows: list[tuple[object, ...]] | tuple[tuple[object, ...], ...],
    ) -> list[tuple[object, ...]]:
        return [tuple(self._normalize_value(value) for value in row) for row in rows]

    def _normalize_value(self, value: object) -> object:
        if isinstance(value, dict):
            return {key: self._normalize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize_value(item) for item in value]
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                return self._normalize_value(json.loads(stripped))
            if stripped.startswith('"') and stripped.endswith('"'):
                return self._normalize_value(json.loads(stripped))
            if stripped in {"true", "false", "null"}:
                return self._normalize_value(json.loads(stripped))
            if stripped and stripped[0] in "-0123456789":
                try:
                    return self._normalize_value(json.loads(stripped))
                except json.JSONDecodeError:
                    pass
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, float):
            return round(value, 12)
        return value
