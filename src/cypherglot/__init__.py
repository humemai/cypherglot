from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from ._compiled_program import CompiledCypherLoop
from .compile import (
    CompiledCypherProgram,
    CompiledCypherStatement,
    compile_cypher_program_text,
    compile_cypher_text,
    compile_normalized_cypher_program,
    compile_normalized_cypher_statement,
)
from .ir import SQLBackend
from .render import (
    RenderedCypherLoop,
    RenderedCypherProgram,
    RenderedCypherStatement,
    render_compiled_cypher_program,
    render_cypher_program_text,
    to_sql,
    to_sqlglot_ast,
    to_sqlglot_program,
)
from .normalize import (
    NormalizedCypherStatement,
    NormalizedMatchWithReturn,
    NormalizedQueryNodesVectorSearch,
    WithBinding,
    normalize_cypher_parse_result,
    normalize_cypher_text,
)
from .parser import CypherParseResult, CypherSyntaxError, parse_cypher_text
from .schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyIndexSpec,
    PropertyField,
    SchemaContractError,
    edge_table_name,
    node_table_name,
    property_column_name,
)
from .schema_commands import graph_schema_from_text, schema_ddl_from_text
from .validate import validate_cypher_parse_result, validate_cypher_text


def _detect_version() -> str:
    try:
        return version("cypherglot")
    except PackageNotFoundError:
        return "0.0.0"


__version__ = _detect_version()

__all__ = [
    "CypherParseResult",
    "CypherSyntaxError",
    "CompiledCypherLoop",
    "CompiledCypherProgram",
    "CompiledCypherStatement",
    "RenderedCypherLoop",
    "RenderedCypherProgram",
    "RenderedCypherStatement",
    "NormalizedCypherStatement",
    "NormalizedMatchWithReturn",
    "NormalizedQueryNodesVectorSearch",
    "WithBinding",
    "__version__",
    "compile_cypher_program_text",
    "compile_cypher_text",
    "compile_normalized_cypher_program",
    "compile_normalized_cypher_statement",
    "CompilerSchemaContext",
    "edge_table_name",
    "graph_schema_from_text",
    "normalize_cypher_parse_result",
    "normalize_cypher_text",
    "EdgeTypeSpec",
    "GraphSchema",
    "NodeTypeSpec",
    "parse_cypher_text",
    "PropertyIndexSpec",
    "property_column_name",
    "PropertyField",
    "render_compiled_cypher_program",
    "render_cypher_program_text",
    "SchemaContractError",
    "schema_ddl_from_text",
    "SQLBackend",
    "to_sql",
    "to_sqlglot_ast",
    "to_sqlglot_program",
    "validate_cypher_parse_result",
    "validate_cypher_text",
    "node_table_name",
]
