from __future__ import annotations

import json
import sqlite3
import unittest

import cypherglot
from tests._postgres_runtime_support import (
    acquire_postgresql_test_dsn,
    release_postgresql_test_dsn,
)
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional test dependency
    psycopg2 = None  # type: ignore[assignment]


class PostgreSQLReadParityTests(unittest.TestCase):
    _postgres_dsn: str

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._postgres_dsn = acquire_postgresql_test_dsn()

    @classmethod
    def tearDownClass(cls) -> None:
        release_postgresql_test_dsn()
        super().tearDownClass()

    def setUp(self) -> None:
        assert psycopg2 is not None
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

        self.postgres = psycopg2.connect(self._postgres_dsn)
        self.postgres.autocommit = False
        with self.postgres.cursor() as cur:
            for statement in self.graph_schema.ddl("postgresql"):
                cur.execute(statement)
        self.postgres.commit()
        self._seed_graphs()

    def tearDown(self) -> None:
        self.sqlite.close()
        postgres = getattr(self, "postgres", None)
        if postgres is None or postgres.closed:
            return
        try:
            self._reset_postgresql_schema()
        finally:
            postgres.close()

    def test_curated_admitted_reads_match_sqlite_results(self) -> None:
        queries = (
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered "
                "RETURN lowered ORDER BY lowered"
            ),
            (
                "MATCH (u:User) WITH toInteger(u.score) AS score_int "
                "RETURN score_int >= 2 AS ge_two ORDER BY ge_two"
            ),
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered, "
                "u.score AS score RETURN lowered, avg(score) AS mean "
                "ORDER BY mean DESC, lowered"
            ),
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WITH startNode(r).name AS start_name, endNode(r).id AS end_id "
                "RETURN start_name, end_id ORDER BY end_id, start_name"
            ),
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH startNode(s).name AS employee, endNode(s).name AS employer "
                "RETURN employee, employer ORDER BY employer, employee"
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WITH lower(b.name) AS lowered RETURN lowered ORDER BY lowered"
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WITH toInteger(b.score) AS score_int "
                "RETURN score_int ORDER BY score_int"
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WHERE a.name = 'Alice' RETURN b.name AS friend "
                "ORDER BY friend"
            ),
        )

        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(
                    self._execute_sqlite(query),
                    self._execute_postgresql(query),
                )

    def _seed_graphs(self) -> None:
        user_rows = [
            (1, "Alice", 30, 1.2, True),
            (2, "Bob", 25, 2.8, False),
            (3, "Alice", 22, 3.0, True),
            (4, "Cara", 4, 4.4, False),
        ]
        company_rows = [(5, "Acme")]
        knows_rows = [
            (10, 1, 2, "Alice met", 1.5, 2.2, True),
            (11, 2, 4, "coworker", 0.5, 3.7, False),
            (12, 3, 2, "friend", 2.0, 1.1, True),
        ]
        works_at_rows = [(20, 2, 5, 2020)]

        self.sqlite.executemany(
            (
                "INSERT INTO cg_node_user (id, name, age, score, active) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            user_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            company_rows,
        )
        self.sqlite.executemany(
            (
                "INSERT INTO cg_edge_knows "
                "(id, from_id, to_id, note, weight, score, active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            knows_rows,
        )
        self.sqlite.executemany(
            (
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)"
            ),
            works_at_rows,
        )
        self.sqlite.commit()

        with self.postgres.cursor() as cur:
            cur.executemany(
                (
                    "INSERT INTO cg_node_user (id, name, age, score, active) "
                    "VALUES (%s, %s, %s, %s, %s)"
                ),
                user_rows,
            )
            cur.executemany(
                "INSERT INTO cg_node_company (id, name) VALUES (%s, %s)",
                company_rows,
            )
            cur.executemany(
                (
                    "INSERT INTO cg_edge_knows "
                    "(id, from_id, to_id, note, weight, score, active) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                ),
                knows_rows,
            )
            cur.executemany(
                (
                    "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                works_at_rows,
            )
        self.postgres.commit()

    def _execute_sqlite(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query,
            backend="sqlite",
            schema_context=self.schema_context,
        )
        rows = self.sqlite.execute(sql).fetchall()
        return self._stabilize_rows(self._normalize_rows(rows))

    def _execute_postgresql(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query,
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )
        with self.postgres.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return self._stabilize_rows(self._normalize_rows(rows))

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

    def _reset_postgresql_schema(self) -> None:
        with self.postgres.cursor() as cur:
            for statement in (
                "DROP TABLE IF EXISTS cg_edge_works_at",
                "DROP TABLE IF EXISTS cg_edge_knows",
                "DROP TABLE IF EXISTS cg_node_company",
                "DROP TABLE IF EXISTS cg_node_user",
                "DROP SEQUENCE IF EXISTS cg_edge_works_at_id_seq",
                "DROP SEQUENCE IF EXISTS cg_edge_knows_id_seq",
                "DROP SEQUENCE IF EXISTS cg_node_company_id_seq",
                "DROP SEQUENCE IF EXISTS cg_node_user_id_seq",
            ):
                cur.execute(statement)
        self.postgres.commit()
