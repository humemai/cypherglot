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
    import turso
except ImportError:  # pragma: no cover - optional test dependency
    turso = None


@unittest.skipIf(turso is None, "turso (pyturso) is not installed")
class TursoReadParityTests(unittest.TestCase):
    """Turso speaks SQLite's SQL dialect, so the SAME generated SQL must return
    the SAME results on SQLite and Turso. (Queries needing the SQLite custom
    string functions REVERSE/LEFT/RIGHT/SPLIT are excluded — pyturso cannot
    register them.)"""

    def setUp(self) -> None:
        assert turso is not None
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
                    properties=(PropertyField("weight", "float"),),
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

        self.turso = turso.connect(":memory:")
        for statement in self.graph_schema.ddl("turso"):
            self.turso.execute(statement)

        self._seed()

    def tearDown(self) -> None:
        self.sqlite.close()
        self.turso.close()

    def _seed(self) -> None:
        users = [
            (1, "Alice", 30, 1.2, 1),
            (2, "Bob", 25, 2.8, 0),
            (3, "Cara", 22, 3.0, 1),
            (4, "Dan", 40, 4.4, 0),
        ]
        companies = [(10, "Acme"), (11, "Globex")]
        knows = [(20, 1, 2, 1.5), (21, 1, 3, 2.2), (22, 2, 3, 0.5)]
        works_at = [(30, 1, 10, 2020), (31, 2, 11, 2019)]
        for conn in (self.sqlite, self.turso):
            conn.executemany(
                "INSERT INTO cg_node_user (id, name, age, score, active) "
                "VALUES (?, ?, ?, ?, ?)",
                users,
            )
            conn.executemany(
                "INSERT INTO cg_node_company (id, name) VALUES (?, ?)", companies
            )
            conn.executemany(
                "INSERT INTO cg_edge_knows (id, from_id, to_id, weight) "
                "VALUES (?, ?, ?, ?)",
                knows,
            )
            conn.executemany(
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)",
                works_at,
            )
            conn.commit()

    def test_curated_reads_match_sqlite(self) -> None:
        queries = (
            "MATCH (u:User) RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WHERE u.age > 24 RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WHERE u.name IN ['Alice', 'Dan'] "
            "RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WHERE u.age < 24 OR u.age > 35 "
            "RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) WHERE NOT u.age = 30 RETURN u.name AS name ORDER BY name",
            "MATCH (u:User) RETURN DISTINCT u.active AS active ORDER BY active",
            "MATCH (u:User) RETURN count(*) AS total",
            "MATCH (u:User) RETURN u.active AS active, count(*) AS c ORDER BY active",
            "MATCH (u:User) RETURN min(u.age) AS mn, max(u.age) AS mx",
            "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name AS s, b.name AS t "
            "ORDER BY s, t",
            "MATCH (a:User)-[:KNOWS]->(b) WHERE a.name = 'Alice' "
            "RETURN b.name AS friend ORDER BY friend",
            "MATCH (u:User)-[:WORKS_AT]->(c:Company) "
            "RETURN u.name AS person, c.name AS company ORDER BY person",
            "MATCH (a:User) OPTIONAL MATCH (a)-[:KNOWS]->(b:User) "
            "RETURN a.name AS name, b.name AS friend ORDER BY name, friend",
            "MATCH (a:User)-[:KNOWS]->(b:User) "
            "RETURN a.name AS name, collect(b.name) AS friends ORDER BY name",
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'Alice' "
            "RETURN b.name AS reachable ORDER BY reachable",
            "MATCH (u:User) RETURN u.name AS name, "
            "CASE WHEN u.age >= 30 THEN 'senior' ELSE 'junior' END AS bucket "
            "ORDER BY name",
        )
        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(
                    self._run(self.sqlite, query),
                    self._run(self.turso, query),
                )

    def _run(self, conn, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query, backend="turso", schema_context=self.schema_context
        )
        return self._stabilize(
            [tuple(self._canon(v) for v in row) for row in conn.execute(sql).fetchall()]
        )

    def _stabilize(self, rows):
        return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, default=str))

    def _canon(self, value: object) -> object:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    return sorted(
                        (self._canon(v) for v in json.loads(stripped)),
                        key=lambda v: json.dumps(v, sort_keys=True, default=str),
                    )
                except json.JSONDecodeError:
                    return value
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else round(value, 9)
        return value


if __name__ == "__main__":
    unittest.main()
