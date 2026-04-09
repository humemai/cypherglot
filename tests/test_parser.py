from __future__ import annotations

import unittest

import cypherglot


class TestParser(unittest.TestCase):
    def test_parse_cypher_text_parses_current_create_shape(self) -> None:
        result = cypherglot.parse_cypher_text(
            "CREATE (u:User {name: 'Alice', age: 30})"
        )

        self.assertFalse(result.has_errors)
        self.assertEqual(type(result.tree).__name__, "OC_CypherContext")
        self.assertEqual(
            result.source_text,
            "CREATE (u:User {name: 'Alice', age: 30})",
        )
        self.assertIsNotNone(result.token_stream)

    def test_parse_cypher_text_parses_current_match_where_shape(self) -> None:
        result = cypherglot.parse_cypher_text(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertFalse(result.has_errors)
        self.assertEqual(type(result.tree).__name__, "OC_CypherContext")

    def test_parse_cypher_text_reports_syntax_errors(self) -> None:
        result = cypherglot.parse_cypher_text("MATCH (u RETURN u")

        self.assertTrue(result.has_errors)
        self.assertGreaterEqual(len(result.syntax_errors), 1)
