from __future__ import annotations

from sqlglot import exp

from ._compiled_program import (
    CompiledCypherLoop,
    CompiledCypherProgram,
    CompiledCypherProgramStep,
    CompiledCypherStatement,
)
from .ir import GraphRelationalBackendIR


def _compile_duckdb_backend_program(
    backend_ir: GraphRelationalBackendIR,
) -> CompiledCypherProgram:
    from .compile import _compile_graph_relational_backend_program

    return _rewrite_duckdb_compiled_program(
        _compile_graph_relational_backend_program(backend_ir)
    )


def _rewrite_duckdb_compiled_program(
    program: CompiledCypherProgram,
) -> CompiledCypherProgram:
    return CompiledCypherProgram(
        steps=tuple(_rewrite_duckdb_program_step(step) for step in program.steps)
    )


def _rewrite_duckdb_program_step(
    step: CompiledCypherProgramStep,
) -> CompiledCypherProgramStep:
    if isinstance(step, CompiledCypherStatement):
        return CompiledCypherStatement(
            sql=_rewrite_duckdb_expression(step.sql),
            bind_columns=step.bind_columns,
        )

    return CompiledCypherLoop(
        source=_rewrite_duckdb_expression(step.source),
        row_bindings=step.row_bindings,
        body=tuple(
            CompiledCypherStatement(
                sql=_rewrite_duckdb_expression(statement.sql),
                bind_columns=statement.bind_columns,
            )
            for statement in step.body
        ),
    )


def _rewrite_duckdb_expression(expression: exp.Expression) -> exp.Expression:
    transformed = expression.copy().transform(_rewrite_duckdb_json_extract)
    transformed = transformed.transform(_rewrite_duckdb_integer_casts)
    transformed = transformed.transform(_rewrite_duckdb_numeric_functions)
    transformed = transformed.transform(_rewrite_duckdb_length_functions)
    transformed = transformed.transform(_rewrite_duckdb_numeric_comparisons)
    transformed = transformed.transform(_rewrite_duckdb_min_max)
    _rewrite_duckdb_order_clauses(transformed)
    return transformed


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

    if isinstance(node.parent, exp.Update) and node.arg_key == "expressions":
        return node

    left = node.left
    right = node.right
    if left is None or right is None:
        return node

    if (
        isinstance(right, exp.Literal)
        and not right.is_string
        and _should_cast_duckdb_numeric_operand(left)
    ):
        node.set("this", _duckdb_numeric_cast(left))
        return node

    if (
        isinstance(left, exp.Literal)
        and not left.is_string
        and _should_cast_duckdb_numeric_operand(right)
    ):
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
        if any(
            _is_duckdb_numeric_json_order_key(ordered.this)
            for ordered in order.expressions
        ):
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