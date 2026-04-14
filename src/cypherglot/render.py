"""Thin public rendering helpers built on top of CypherGlot compilation."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from ._logging import get_logger
from .compile import (
    CompiledCypherLoop,
    CompiledCypherProgram,
    CompiledCypherStatement,
    compile_cypher_program_text,
    compile_cypher_text,
)
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
) -> exp.Expression:
    """Compile one single-statement Cypher shape into SQLGlot AST."""

    logger.debug("Rendering helper requested SQLGlot AST")
    return compile_cypher_text(text, schema_context=schema_context)


def to_sqlglot_program(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
) -> CompiledCypherProgram:
    """Compile one Cypher shape into SQLGlot-backed compiled output."""

    logger.debug("Rendering helper requested SQLGlot program")
    return compile_cypher_program_text(text, schema_context=schema_context)


def to_sql(
    text: str,
    *,
    dialect: str | None = None,
    pretty: bool = False,
    schema_context: CompilerSchemaContext | None = None,
) -> str:
    """Render one single-statement Cypher shape to SQL using SQLGlot.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    logger.debug(
        "Rendering Cypher text to SQL",
        extra={"dialect": dialect or "sqlite", "pretty": pretty},
    )
    sql = _render_expression_sql(
        compile_cypher_text(text, schema_context=schema_context),
        dialect=dialect,
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
) -> RenderedCypherProgram:
    """Render one compiled Cypher program into SQL strings.

    When no dialect is provided, CypherGlot currently renders SQL for SQLite.
    """

    logger.debug(
        "Rendering compiled Cypher program text",
        extra={"dialect": dialect or "sqlite", "pretty": pretty},
    )
    program = render_compiled_cypher_program(
        compile_cypher_program_text(text, schema_context=schema_context),
        dialect=dialect,
        pretty=pretty,
    )
    logger.debug("Rendered compiled Cypher program text")
    return program


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
    if dialect == "duckdb":
        return _render_duckdb_expression_sql(expression, pretty=pretty)

    if dialect is not None and dialect != "sqlite":
        return expression.sql(dialect=dialect, pretty=pretty)

    return expression.sql(pretty=pretty)


def _render_duckdb_expression_sql(
    expression: exp.Expression,
    *,
    pretty: bool,
) -> str:
    transformed = expression.copy().transform(_rewrite_duckdb_json_extract)
    transformed = transformed.transform(_rewrite_duckdb_integer_casts)
    transformed = transformed.transform(_rewrite_duckdb_numeric_functions)
    transformed = transformed.transform(_rewrite_duckdb_length_functions)
    transformed = transformed.transform(_rewrite_duckdb_numeric_comparisons)
    transformed = transformed.transform(_rewrite_duckdb_min_max)
    _rewrite_duckdb_order_clauses(transformed)
    return transformed.sql(dialect="duckdb", pretty=pretty)


def _rewrite_duckdb_json_extract(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, exp.JSONExtract):
        return node

    scalar_extract = exp.func(
        "JSON_EXTRACT_STRING",
        node.this.copy(),
        node.expression.copy(),
    )

    if _duckdb_json_extract_requires_numeric_cast(node):
        return exp.TryCast(this=scalar_extract, to=exp.DataType.build("DOUBLE"))

    return scalar_extract


def _is_duckdb_json_extract_string(node: exp.Expression) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_EXTRACT_STRING"


def _is_duckdb_numeric_json_order_key(node: exp.Expression) -> bool:
    return isinstance(node, exp.TryCast) and _is_duckdb_json_extract_string(node.this)


def _duckdb_numeric_cast(expression: exp.Expression) -> exp.TryCast:
    return exp.TryCast(this=expression.copy(), to=exp.DataType.build("DOUBLE"))


def _is_duckdb_numeric_function(node: exp.Expression) -> bool:
    return isinstance(
        node,
        (
            exp.Sum,
            exp.Avg,
            exp.Abs,
            exp.Sign,
            exp.Round,
            exp.Ceil,
            exp.Floor,
            exp.Sqrt,
            exp.Exp,
            exp.Sin,
            exp.Cos,
            exp.Tan,
            exp.Asin,
            exp.Acos,
            exp.Atan,
            exp.Ln,
            exp.Log,
            exp.Degrees,
            exp.Radians,
        ),
    )


def _should_cast_duckdb_numeric_operand(node: exp.Expression) -> bool:
    return isinstance(node, (exp.Column, exp.Identifier, exp.Placeholder))


def _duckdb_json_extract_requires_numeric_cast(node: exp.JSONExtract) -> bool:
    parent = node.parent
    if parent is None:
        return False

    if isinstance(
        parent,
        (
            exp.Sum,
            exp.Avg,
            exp.Abs,
            exp.Sign,
            exp.Round,
            exp.Ceil,
            exp.Floor,
            exp.Sqrt,
            exp.Exp,
            exp.Sin,
            exp.Cos,
            exp.Tan,
            exp.Asin,
            exp.Acos,
            exp.Atan,
            exp.Ln,
            exp.Log,
            exp.Degrees,
            exp.Radians,
        ),
    ):
        return True

    if isinstance(parent, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        other = parent.right if parent.left is node else parent.left
        return isinstance(other, exp.Literal) and not other.is_string

    return False


def _rewrite_duckdb_numeric_functions(node: exp.Expression) -> exp.Expression:
    if not _is_duckdb_numeric_function(node):
        return node

    operand = node.this
    if operand is None or isinstance(operand, exp.TryCast):
        return node

    if _should_cast_duckdb_numeric_operand(operand):
        node.set("this", _duckdb_numeric_cast(operand))

    return node


def _rewrite_duckdb_length_functions(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, exp.Length):
        return node

    operand = node.this
    if operand is None or isinstance(operand, exp.Cast):
        return node

    if isinstance(
        operand,
        (exp.Column, exp.Identifier, exp.Placeholder, exp.TryCast, exp.Anonymous),
    ):
        node.set("this", exp.Cast(this=operand.copy(), to=exp.DataType.build("TEXT")))

    return node


def _is_duckdb_integer_type(data_type: exp.DataType | None) -> bool:
    if data_type is None:
        return False

    return data_type.sql(dialect="duckdb").upper() in {
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
    }


def _rewrite_duckdb_integer_casts(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, exp.Cast) or not _is_duckdb_integer_type(node.to):
        return node

    source = node.this
    if source is None:
        return node

    if isinstance(source, exp.Literal) and source.is_string:
        return node

    return exp.Cast(
        this=exp.func("TRUNC", _duckdb_numeric_cast(source)),
        to=node.to.copy(),
    )


def _rewrite_duckdb_numeric_comparisons(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return node

    left = node.left
    right = node.right
    if left is None or right is None:
        return node

    if isinstance(right, exp.Literal) and not right.is_string and _should_cast_duckdb_numeric_operand(left):
        node.set("this", _duckdb_numeric_cast(left))
        return node

    if isinstance(left, exp.Literal) and not left.is_string and _should_cast_duckdb_numeric_operand(right):
        node.set("expression", _duckdb_numeric_cast(right))

    return node


def _rewrite_duckdb_min_max(node: exp.Expression) -> exp.Expression:
    if isinstance(node, (exp.Min, exp.Max)) and _is_duckdb_json_extract_string(node.this):
        value = node.this.copy()
        descending = isinstance(node, exp.Max)
        return exp.First(
            this=exp.Order(
                this=value.copy(),
                expressions=[
                    exp.Ordered(
                        this=exp.TryCast(
                            this=value.copy(),
                            to=exp.DataType.build("DOUBLE"),
                        ),
                        desc=descending,
                        nulls_first=False,
                    ),
                    exp.Ordered(
                        this=value,
                        desc=descending,
                        nulls_first=False,
                    ),
                ],
            )
        )

    return node


def _rewrite_duckdb_order_clauses(expression: exp.Expression) -> None:
    for order in expression.find_all(exp.Order):
        if any(_is_duckdb_numeric_json_order_key(ordered.this) for ordered in order.expressions):
            continue

        rewritten: list[exp.Ordered] = []
        for ordered in order.expressions:
            if isinstance(ordered.this, (exp.Literal, exp.Placeholder)):
                continue

            if _is_duckdb_json_extract_string(ordered.this):
                rewritten.append(
                    exp.Ordered(
                        this=exp.TryCast(
                            this=ordered.this.copy(),
                            to=exp.DataType.build("DOUBLE"),
                        ),
                        desc=ordered.args.get("desc") is True,
                        nulls_first=False,
                    )
                )
                rewritten.append(
                    exp.Ordered(
                        this=ordered.this.copy(),
                        desc=ordered.args.get("desc") is True,
                        nulls_first=ordered.args.get("nulls_first"),
                    )
                )
                continue

            rewritten.append(ordered)

        if not rewritten:
            if order.parent is not None and order.arg_key is not None:
                order.parent.set(order.arg_key, None)
            continue

        order.set("expressions", rewritten)


