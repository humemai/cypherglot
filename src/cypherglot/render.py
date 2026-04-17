"""Thin public rendering helpers built on top of CypherGlot compilation."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from ._logging import get_logger
from ._compiled_program import CompiledCypherLoop
from .compile import (
    CompiledCypherProgram,
    CompiledCypherStatement,
    compile_cypher_program_text,
    compile_cypher_text,
)
from .ir import SQLBackend
from .schema import CompilerSchemaContext


logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RenderedCypherStatement:
    sql: str
    bind_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RenderedCypherLoop:
    source: str
    row_bindings: tuple[str, ...]
    body: tuple[RenderedCypherStatement, ...]


RenderedCypherProgramStep = RenderedCypherStatement | RenderedCypherLoop


@dataclass(frozen=True, slots=True)
class RenderedCypherProgram:
    steps: tuple[RenderedCypherProgramStep, ...]


def to_sqlglot_ast(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str = SQLBackend.SQLITE,
) -> exp.Expression:
    """Compile one single-statement Cypher shape into SQLGlot AST."""

    logger.debug("Rendering helper requested SQLGlot AST")
    return compile_cypher_text(
        text,
        schema_context=schema_context,
        backend=backend,
    )


def to_sqlglot_program(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str = SQLBackend.SQLITE,
) -> CompiledCypherProgram:
    """Compile one Cypher shape into SQLGlot-backed compiled output."""

    logger.debug("Rendering helper requested SQLGlot program")
    return compile_cypher_program_text(
        text,
        schema_context=schema_context,
        backend=backend,
    )


def to_sql(
    text: str,
    *,
    dialect: str | None = None,
    pretty: bool = False,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> str:
    """Render one single-statement Cypher shape to SQL using SQLGlot.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    logger.debug(
        "Rendering Cypher text to SQL",
        extra={
            "dialect": dialect or "sqlite",
            "backend": _resolve_render_backend(backend, dialect).value,
            "pretty": pretty,
        },
    )
    resolved_backend = _resolve_render_backend(backend, dialect)
    sql = _render_expression_sql(
        compile_cypher_text(
            text,
            schema_context=schema_context,
            backend=resolved_backend,
        ),
        dialect=dialect,
        backend=resolved_backend,
        pretty=pretty,
    )
    logger.debug("Rendered Cypher text to SQL")
    return sql


def render_cypher_program_text(
    text: str,
    *,
    dialect: str | None = None,
    pretty: bool = False,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    logger.debug(
        "Rendering compiled Cypher program text",
        extra={
            "dialect": dialect or "sqlite",
            "backend": _resolve_render_backend(backend, dialect).value,
            "pretty": pretty,
        },
    )
    resolved_backend = _resolve_render_backend(backend, dialect)
    program = render_compiled_cypher_program(
        compile_cypher_program_text(
            text,
            schema_context=schema_context,
            backend=resolved_backend,
        ),
        dialect=dialect,
        backend=resolved_backend,
        pretty=pretty,
    )
    logger.debug("Rendered compiled Cypher program text")
    return program


def render_compiled_cypher_program(
    program: CompiledCypherProgram,
    *,
    dialect: str | None = None,
    backend: SQLBackend | str = SQLBackend.SQLITE,
    pretty: bool = False,
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    resolved_backend = _resolve_render_backend(backend, dialect)

    return RenderedCypherProgram(
        steps=tuple(
            _render_program_step(
                step,
                dialect=dialect,
                backend=resolved_backend,
                pretty=pretty,
            )
            for step in program.steps
        )
    )


def _render_program_step(
    step: CompiledCypherStatement | CompiledCypherLoop,
    *,
    dialect: str | None,
    backend: SQLBackend,
    pretty: bool,
) -> RenderedCypherProgramStep:
    if isinstance(step, CompiledCypherStatement):
        return RenderedCypherStatement(
            sql=_render_expression_sql(
                step.sql,
                dialect=dialect,
                backend=backend,
                pretty=pretty,
            ),
            bind_columns=step.bind_columns,
        )

    return RenderedCypherLoop(
        source=_render_expression_sql(
            step.source,
            dialect=dialect,
            backend=backend,
            pretty=pretty,
        ),
        row_bindings=step.row_bindings,
        body=tuple(
            RenderedCypherStatement(
                sql=_render_expression_sql(
                    statement.sql,
                    dialect=dialect,
                    backend=backend,
                    pretty=pretty,
                ),
                bind_columns=statement.bind_columns,
            )
            for statement in step.body
        ),
    )


def _render_expression_sql(
    expression: exp.Expression,
    *,
    dialect: str | None,
    backend: SQLBackend,
    pretty: bool,
) -> str:
    if dialect is not None:
        return expression.sql(dialect=dialect, pretty=pretty)

    if backend != SQLBackend.SQLITE:
        return expression.sql(dialect=backend.value, pretty=pretty)

    return expression.sql(pretty=pretty)

def _resolve_render_backend(
    backend: SQLBackend | str | None,
    dialect: str | None,
) -> SQLBackend:
    if backend is not None:
        if isinstance(backend, SQLBackend):
            return backend
        return SQLBackend(backend)
    if dialect in {backend.value for backend in SQLBackend}:
        return SQLBackend(dialect)
    return SQLBackend.SQLITE


