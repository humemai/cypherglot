from __future__ import annotations

import json
import sqlite3
import unittest

import cypherglot
from tests._clickhouse_runtime_support import (
    ClickHouseConnectionParams,
    acquire_clickhouse_test_params,
    release_clickhouse_test_params,
)
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover - optional test dependency
    clickhouse_connect = None  # type: ignore[assignment]


class ClickHouseReadParityTests(unittest.TestCase):
    """The SAME Cypher, lowered to ClickHouse-dialect SQL, must return the same
    results as the SQLite reference on read queries. ClickHouse is a columnar
    OLAP engine scoped to reads; writes are out of scope (async, non-
    transactional mutations), so this suite only exercises the read corpus."""

    _params: ClickHouseConnectionParams

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._params = acquire_clickhouse_test_params()

    @classmethod
    def tearDownClass(cls) -> None:
        release_clickhouse_test_params()
        super().tearDownClass()

    def setUp(self) -> None:
        assert clickhouse_connect is not None
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
        self.sqlite.executescript("\n".join(self.graph_schema.ddl("sqlite")))

        self.clickhouse = clickhouse_connect.get_client(
            host=self._params.host,
            port=self._params.port,
            username=self._params.username,
            password=self._params.password,
            database=self._params.database,
        )
        self._reset_clickhouse_schema()
        for statement in self.graph_schema.ddl("clickhouse"):
            self.clickhouse.command(statement.rstrip(";"))
        self._seed_graphs()

    def tearDown(self) -> None:
        self.sqlite.close()
        clickhouse = getattr(self, "clickhouse", None)
        if clickhouse is None:
            return
        try:
            self._reset_clickhouse_schema()
        finally:
            clickhouse.close()

    def test_curated_admitted_reads_match_sqlite_results(self) -> None:
        queries = (
            "MATCH (u:User) RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered "
                "RETURN lowered ORDER BY lowered"
            ),
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered, "
                "u.score AS score RETURN lowered, avg(score) AS mean "
                "ORDER BY mean DESC, lowered"
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WHERE a.name = 'Alice' RETURN b.name AS friend "
                "ORDER BY friend"
            ),
            "MATCH (u:User) WHERE u.age IN [25, 30] "
            "RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WHERE u.name IN ['Alice', 'Cara'] "
            "RETURN u.name AS name, u.age AS age ORDER BY name, age",
            "MATCH (u:User) WHERE u.age IN [] "
            "RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) RETURN DISTINCT u.name AS name ORDER BY name",
            "MATCH (u:User) WITH DISTINCT u.name AS name RETURN name ORDER BY name",
            "MATCH (u:User) RETURN u.active AS active, collect(u.name) AS names "
            "ORDER BY active",
            "MATCH (a:User) OPTIONAL MATCH (a)-[:KNOWS]->(b:User) "
            "RETURN a.name AS name, b.name AS friend ORDER BY name, friend",
            "MATCH (u:User) WHERE u.age < 18 OR u.age > 28 "
            "RETURN u.name AS name, u.age AS age ORDER BY name, age",
            "MATCH (u:User) WHERE u.age > 18 AND (u.name = 'Alice' OR u.name = 'Cara') "
            "RETURN u.name AS name, u.age AS age ORDER BY name, age",
            "MATCH (u:User) WHERE NOT u.age IN [25, 30] "
            "RETURN u.name AS name, u.age AS age ORDER BY name, age",
            "MATCH (u:User) RETURN u.active AS active, count(*) AS c ORDER BY active",
            "MATCH (u:User) RETURN min(u.age) AS mn, max(u.age) AS mx",
            "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name AS s, b.name AS t "
            "ORDER BY s, t",
            "MATCH (u:User)-[:WORKS_AT]->(c:Company) "
            "RETURN u.name AS person, c.name AS company ORDER BY person",
        )

        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(
                    self._execute_sqlite(query),
                    self._execute_clickhouse(query),
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
            "INSERT INTO cg_edge_knows "
            "(id, from_id, to_id, note, weight, score, active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            knows_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
            "VALUES (?, ?, ?, ?)",
            works_at_rows,
        )
        self.sqlite.commit()

        self.clickhouse.insert(
            "cg_node_user",
            user_rows,
            column_names=["id", "name", "age", "score", "active"],
        )
        self.clickhouse.insert(
            "cg_node_company",
            company_rows,
            column_names=["id", "name"],
        )
        self.clickhouse.insert(
            "cg_edge_knows",
            knows_rows,
            column_names=["id", "from_id", "to_id", "note", "weight", "score", "active"],
        )
        self.clickhouse.insert(
            "cg_edge_works_at",
            works_at_rows,
            column_names=["id", "from_id", "to_id", "since"],
        )

    def _execute_sqlite(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query,
            backend="sqlite",
            schema_context=self.schema_context,
        )
        rows = self.sqlite.execute(sql).fetchall()
        return self._stabilize_rows(self._normalize_rows(rows))

    def _execute_clickhouse(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query,
            backend="clickhouse",
            schema_context=self.schema_context,
        )
        rows = self.clickhouse.query(sql).result_rows
        return self._stabilize_rows(self._normalize_rows(rows))

    def _stabilize_rows(
        self,
        rows: list[tuple[object, ...]],
    ) -> list[tuple[object, ...]]:
        return sorted(
            rows,
            key=lambda row: json.dumps(row, sort_keys=True, default=str),
        )

    def _normalize_rows(
        self,
        rows: list[tuple[object, ...]] | tuple[tuple[object, ...], ...],
    ) -> list[tuple[object, ...]]:
        return [tuple(self._normalize_value(value) for value in row) for row in rows]

    def _normalize_value(self, value: object) -> object:
        if isinstance(value, (list, tuple)):
            normalized = [self._normalize_value(item) for item in value]
            return sorted(
                normalized,
                key=lambda item: json.dumps(item, sort_keys=True, default=str),
            )
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    return self._normalize_value(json.loads(stripped))
                except json.JSONDecodeError:
                    return value
            return value
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, float):
            return round(value, 9)
        return value

    def _reset_clickhouse_schema(self) -> None:
        for table_name in (
            "cg_edge_works_at",
            "cg_edge_knows",
            "cg_node_company",
            "cg_node_user",
        ):
            self.clickhouse.command(f"DROP TABLE IF EXISTS {table_name}")


if __name__ == "__main__":
    unittest.main()
