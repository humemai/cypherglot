from __future__ import annotations

import logging
import unittest

import cypherglot


class LoggingTests(unittest.TestCase):
    def _schema_context(self) -> cypherglot.CompilerSchemaContext:
        return cypherglot.CompilerSchemaContext.type_aware(
            cypherglot.GraphSchema(
                node_types=(
                    cypherglot.NodeTypeSpec(
                        name="User",
                        properties=(cypherglot.PropertyField("name", "string"),),
                    ),
                ),
                edge_types=(),
            )
        )

    def test_package_logger_has_null_handler(self) -> None:
        logger = logging.getLogger("cypherglot")

        self.assertTrue(
            any(isinstance(handler, logging.NullHandler) for handler in logger.handlers)
        )

    def test_to_sql_emits_debug_logs_when_enabled(self) -> None:
        with self.assertLogs("cypherglot", level="DEBUG") as captured:
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u.name",
                schema_context=self._schema_context(),
            )

        joined = "\n".join(captured.output)
        self.assertIn("Rendering Cypher text to SQL", joined)
        self.assertIn("Compiling Cypher text", joined)
        self.assertIn("Parsing Cypher text", joined)
        self.assertIn("Validating parsed Cypher result", joined)

    def test_validation_failures_are_logged_at_debug(self) -> None:
        with self.assertLogs("cypherglot", level="DEBUG") as captured:
            with self.assertRaises(ValueError):
                cypherglot.validate_cypher_text("MATCH (u RETURN u")

        self.assertIn("Validation failed", "\n".join(captured.output))