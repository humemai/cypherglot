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
    if dialect == "duckdb":
        return _render_duckdb_expression_sql(expression, pretty=pretty)

    if dialect is not None and dialect != "sqlite":
        return expression.sql(dialect=dialect, pretty=pretty)

    return _rewrite_sqlite_json_object_pairs(expression.sql(pretty=pretty))


def _render_duckdb_expression_sql(
    expression: exp.Expression,
    *,
    pretty: bool,
) -> str:
    transformed = expression.copy().transform(_rewrite_duckdb_json_extract)
    transformed = transformed.transform(_rewrite_duckdb_integer_casts)
    transformed = transformed.transform(_rewrite_duckdb_numeric_functions)
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
