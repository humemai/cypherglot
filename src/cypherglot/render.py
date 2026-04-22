"""Thin public rendering helpers built on top of CypherGlot compilation."""

from __future__ import annotations

from dataclasses import dataclass
from threading import local
from typing import Any, Callable

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from ._logging import get_logger
from ._compiled_program import CompiledCypherLoop
from .compile import (
    CompiledCypherProgram,
    CompiledCypherStatement,
    compile_cypher_program_text,
    compile_cypher_text,
)
from .ir import BACKEND_CAPABILITIES, SQLBackend
from .schema import CompilerSchemaContext


logger = get_logger(__name__)
_BACKEND_BY_NAME = {backend.value: backend for backend in SQLBackend}
_RENDER_GENERATORS = local()


@dataclass(frozen=True, slots=True)
class RenderedCypherStatement:
    """One rendered SQL statement plus any columns it binds for later steps."""

    sql: str
    bind_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RenderedCypherLoop:
    """A rendered loop step with a source query and per-row statement body."""

    source: str
    row_bindings: tuple[str, ...]
    body: tuple[RenderedCypherStatement, ...]


RenderedCypherProgramStep = RenderedCypherStatement | RenderedCypherLoop


@dataclass(frozen=True, slots=True)
class RenderedCypherProgram:
    """A rendered multi-step Cypher program ready for runtime execution."""

    steps: tuple[RenderedCypherProgramStep, ...]


def to_sqlglot_ast(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> exp.Expression:
    """Compile one single-statement Cypher shape into SQLGlot AST.

    Callers must provide an explicit SQL backend.
    """

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
    backend: SQLBackend | str | None = None,
) -> CompiledCypherProgram:
    """Compile one Cypher shape into SQLGlot-backed compiled output.

    Callers must provide an explicit SQL backend.
    """

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

    Callers must provide either an explicit SQL dialect or backend.
    """

    resolved_backend = _resolve_render_backend(backend, dialect)
    resolved_dialect = _resolve_render_dialect(resolved_backend, dialect)

    logger.debug(
        "Rendering Cypher text to SQL",
        extra={
            "dialect": dialect or resolved_backend.value,
            "backend": resolved_backend.value,
            "pretty": pretty,
        },
    )
    sql = _render_expression_sql(
        compile_cypher_text(
            text,
            schema_context=schema_context,
            backend=resolved_backend,
        ),
        sql_kwargs=_build_render_sql_kwargs(
            dialect=resolved_dialect,
            pretty=pretty,
        ),
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

    Callers must provide either an explicit SQL dialect or backend.
    """

    resolved_backend = _resolve_render_backend(backend, dialect)
    resolved_dialect = _resolve_render_dialect(resolved_backend, dialect)

    logger.debug(
        "Rendering compiled Cypher program text",
        extra={
            "dialect": dialect or resolved_backend.value,
            "backend": resolved_backend.value,
            "pretty": pretty,
        },
    )
    program = _render_compiled_cypher_program(
        compile_cypher_program_text(
            text,
            schema_context=schema_context,
            backend=resolved_backend,
        ),
        dialect=resolved_dialect,
        pretty=pretty,
    )
    logger.debug("Rendered compiled Cypher program text")
    return program


def render_compiled_cypher_program(
    program: CompiledCypherProgram,
    *,
    dialect: str | None = None,
    backend: SQLBackend | str | None = None,
    pretty: bool = False,
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    Callers must provide either an explicit SQL dialect or backend.
    """

    resolved_backend = _resolve_render_backend(backend, dialect)
    resolved_dialect = _resolve_render_dialect(resolved_backend, dialect)
    return _render_compiled_cypher_program(
        program,
        dialect=resolved_dialect,
        pretty=pretty,
    )


def _render_compiled_cypher_program(
    program: CompiledCypherProgram,
    *,
    dialect: str | None,
    pretty: bool,
) -> RenderedCypherProgram:
    render_sql = _build_expression_renderer(
        dialect=dialect,
        pretty=pretty,
    )

    return RenderedCypherProgram(
        steps=tuple(
            _render_program_step(
                step,
                render_sql=render_sql,
            )
            for step in program.steps
        )
    )


def _render_program_step(
    step: CompiledCypherStatement | CompiledCypherLoop,
    *,
    render_sql,
) -> RenderedCypherProgramStep:
    if isinstance(step, CompiledCypherStatement):
        return RenderedCypherStatement(
            sql=render_sql(step.sql),
            bind_columns=step.bind_columns,
        )

    return RenderedCypherLoop(
        source=render_sql(step.source),
        row_bindings=step.row_bindings,
        body=tuple(
            RenderedCypherStatement(
                sql=render_sql(statement.sql),
                bind_columns=statement.bind_columns,
            )
            for statement in step.body
        ),
    )


def _build_expression_renderer(
    *,
    dialect: str | None,
    pretty: bool,
) -> Callable[[exp.Expression], str]:
    sql_kwargs = _build_render_sql_kwargs(dialect=dialect, pretty=pretty)
    return lambda expression: _render_expression_sql(
        expression,
        sql_kwargs=sql_kwargs,
    )


def _build_render_sql_kwargs(
    *,
    dialect: str | None,
    pretty: bool,
) -> dict[str, str | bool]:
    sql_kwargs: dict[str, str | bool] = {"pretty": pretty}
    if dialect is not None:
        sql_kwargs["dialect"] = dialect
    return sql_kwargs


def _render_expression_sql(
    expression: exp.Expression,
    *,
    sql_kwargs: dict[str, str | bool],
) -> str:
    dialect = sql_kwargs.get("dialect")
    pretty = bool(sql_kwargs.get("pretty", False))
    if not isinstance(dialect, str):
        return expression.sql(**sql_kwargs)

    generator = _get_cached_generator(dialect=dialect, pretty=pretty)
    return generator.generate(expression)


def _get_cached_generator(*, dialect: str, pretty: bool) -> Any:
    cache = getattr(_RENDER_GENERATORS, "cache", None)
    if cache is None:
        cache = {}
        _RENDER_GENERATORS.cache = cache

    key = (dialect, pretty)
    generator = cache.get(key)
    if generator is None:
        generator = Dialect.get_or_raise(dialect).generator(pretty=pretty)
        cache[key] = generator
    return generator


def _resolve_render_dialect(
    backend: SQLBackend,
    dialect: str | None,
) -> str | None:
    if dialect is not None:
        return dialect
    return BACKEND_CAPABILITIES[backend].render_dialect


def _resolve_render_backend(
    backend: SQLBackend | str | None,
    dialect: str | None,
) -> SQLBackend:
    if backend is not None:
        if isinstance(backend, SQLBackend):
            return backend
        return SQLBackend(backend)
    if dialect in _BACKEND_BY_NAME:
        return _BACKEND_BY_NAME[dialect]
    raise ValueError(
        "CypherGlot rendering requires an explicit SQL dialect or backend."
    )
