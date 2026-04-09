"""Thin public rendering helpers built on top of CypherGlot compilation."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from .compile import (
    CompiledCypherLoop,
    CompiledCypherProgram,
    CompiledCypherStatement,
    compile_cypher_program_text,
    compile_cypher_text,
)


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


def to_sqlglot_ast(text: str) -> exp.Expression:
    """Compile one single-statement Cypher shape into SQLGlot AST."""

    return compile_cypher_text(text)


def to_sqlglot_program(text: str) -> CompiledCypherProgram:
    """Compile one Cypher shape into SQLGlot-backed compiled output."""

    return compile_cypher_program_text(text)


def to_sql(text: str, *, dialect: str | None = None, pretty: bool = False) -> str:
    """Render one single-statement Cypher shape to SQL using SQLGlot.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    return _render_expression_sql(
        compile_cypher_text(text),
        dialect=dialect,
        pretty=pretty,
    )


def render_cypher_program_text(
    text: str,
    *,
    dialect: str | None = None,
    pretty: bool = False,
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    return render_compiled_cypher_program(
        compile_cypher_program_text(text),
        dialect=dialect,
        pretty=pretty,
    )


def render_compiled_cypher_program(
    program: CompiledCypherProgram,
    *,
    dialect: str | None = None,
    pretty: bool = False,
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    return RenderedCypherProgram(
        steps=tuple(
            _render_program_step(step, dialect=dialect, pretty=pretty)
            for step in program.steps
        )
    )


def _render_program_step(
    step: CompiledCypherStatement | CompiledCypherLoop,
    *,
    dialect: str | None,
    pretty: bool,
) -> RenderedCypherProgramStep:
    if isinstance(step, CompiledCypherStatement):
        return RenderedCypherStatement(
            sql=_render_expression_sql(step.sql, dialect=dialect, pretty=pretty),
            bind_columns=step.bind_columns,
        )

    return RenderedCypherLoop(
        source=_render_expression_sql(step.source, dialect=dialect, pretty=pretty),
        row_bindings=step.row_bindings,
        body=tuple(
            RenderedCypherStatement(
                sql=_render_expression_sql(
                    statement.sql,
                    dialect=dialect,
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
    pretty: bool,
) -> str:
    if dialect is not None and dialect != "sqlite":
        return expression.sql(dialect=dialect, pretty=pretty)

    return _rewrite_sqlite_json_object_pairs(expression.sql(pretty=pretty))


def _rewrite_sqlite_json_object_pairs(sql: str) -> str:
    result: list[str] = []
    index = 0
    marker = "JSON_OBJECT("

    while True:
        start = sql.find(marker, index)
        if start < 0:
            result.append(sql[index:])
            return "".join(result)

        result.append(sql[index : start + len(marker)])
        args_start = start + len(marker)
        args_end = _find_matching_parenthesis(sql, args_start - 1)
        result.append(_rewrite_top_level_json_object_arguments(sql[args_start:args_end]))
        result.append(")")
        index = args_end + 1


def _rewrite_top_level_json_object_arguments(arguments_sql: str) -> str:
    result: list[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = 0

    while index < len(arguments_sql):
        char = arguments_sql[index]

        if in_single_quote:
            result.append(char)
            if char == "'":
                if index + 1 < len(arguments_sql) and arguments_sql[index + 1] == "'":
                    result.append(arguments_sql[index + 1])
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            result.append(char)
            if char == '"':
                if index + 1 < len(arguments_sql) and arguments_sql[index + 1] == '"':
                    result.append(arguments_sql[index + 1])
                    index += 2
                    continue
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
            result.append(char)
            index += 1
            continue

        if char == '"':
            in_double_quote = True
            result.append(char)
            index += 1
            continue

        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == ":" and depth == 0:
            result.append(",")
            index += 1
            continue

        result.append(char)
        index += 1

    return "".join(result)


def _find_matching_parenthesis(sql: str, open_index: int) -> int:
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = open_index

    while index < len(sql):
        char = sql[index]

        if in_single_quote:
            if char == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            if char == '"':
                if index + 1 < len(sql) and sql[index + 1] == '"':
                    index += 2
                    continue
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
            index += 1
            continue

        if char == '"':
            in_double_quote = True
            index += 1
            continue

        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index

        index += 1

    raise ValueError("Unmatched JSON_OBJECT parenthesis while rendering SQLite SQL.")
