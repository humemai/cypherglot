from __future__ import annotations

from typing import Literal

from sqlglot import exp

from ._normalize_support import _ParameterRef, CypherValue


_AGGREGATE_SQL_NAMES: dict[str, str] = {
    "count": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
}


def _compile_stream_predicate(
    expression: str,
    type_expression: str | None,
    operator: Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ],
    value: CypherValue,
) -> str:
    if operator == "IS NULL":
        if type_expression is None:
            return f"{expression} IS NULL"
        return f"({type_expression} IS NULL OR {type_expression} = 'null')"
    if operator == "IS NOT NULL":
        if type_expression is None:
            return f"{expression} IS NOT NULL"
        return f"({type_expression} IS NOT NULL AND {type_expression} != 'null')"

    value_sql = _sql_value(value)
    if operator == "=":
        if value is None:
            if type_expression is None:
                return f"{expression} IS NULL"
            return f"({type_expression} IS NULL OR {type_expression} = 'null')"
        return f"{expression} = {value_sql}"
    if operator in {"<", "<=", ">", ">="}:
        return f"{expression} {operator} {value_sql}"
    if operator == "STARTS WITH":
        return f"substr({expression}, 1, length({value_sql})) = {value_sql}"
    if operator == "ENDS WITH":
        return (
            f"length({expression}) >= length({value_sql}) AND "
            "substr("
            f"{expression}, length({expression}) - length({value_sql}) + 1"
            f") = {value_sql}"
        )
    if operator == "CONTAINS":
        return f"instr({expression}, {value_sql}) > 0"
    raise ValueError(f"Unsupported predicate operator: {operator!r}")


def _assemble_select_sql(
    *,
    select_sql: str,
    distinct: bool,
    from_sql: str,
    joins: list[str],
    where_parts: list[str],
    group_sql: str | None = None,
    order_sql: str | None,
    limit: int | None,
    skip: int | None,
) -> str:
    parts = [f"SELECT {'DISTINCT ' if distinct else ''}{select_sql}", from_sql]
    parts.extend(joins)
    if where_parts:
        parts.append(f"WHERE {' AND '.join(where_parts)}")
    if group_sql is not None:
        parts.append(f"GROUP BY {group_sql}")
    if order_sql is not None:
        parts.append(f"ORDER BY {order_sql}")
    if limit is not None:
        parts.append(f"LIMIT {limit}")
    if skip is not None:
        parts.append(f"OFFSET {skip}")
    return " ".join(parts)


def _assemble_update_sql(
    *,
    target_sql: str,
    assignments_sql: str,
    from_sql: str | None,
    where_parts: list[str],
    assignment_prefix: str | None = "properties = ",
) -> str:
    if assignment_prefix is None:
        set_sql = f"SET {assignments_sql}"
    else:
        set_sql = f"SET {assignment_prefix}{assignments_sql}"
    parts = [target_sql, set_sql]
    if from_sql is not None:
        parts.append(from_sql)
    if where_parts:
        parts.append(f"WHERE {' AND '.join(where_parts)}")
    return " ".join(parts)


def _assemble_delete_sql(
    *,
    target_sql: str,
    using_sql: str | None,
    where_parts: list[str],
) -> str:
    parts = [target_sql]
    if using_sql is not None:
        parts.append(using_sql)
    if where_parts:
        parts.append(f"WHERE {' AND '.join(where_parts)}")
    return " ".join(parts)


def _assemble_insert_select_sql(
    *,
    target_sql: str,
    select_sql: str,
    from_sql: str,
    joins: list[str] | None = None,
    where_parts: list[str],
) -> str:
    parts = [target_sql, select_sql, from_sql]
    if joins:
        parts.extend(joins)
    if where_parts:
        parts.append(f"WHERE {' AND '.join(where_parts)}")
    return " ".join(parts)


def _edge_endpoint_column(
    direction: Literal["out", "in"],
    side: Literal["left", "right"],
) -> str:
    if direction == "out":
        return "from_id" if side == "left" else "to_id"
    return "to_id" if side == "left" else "from_id"


def _sql_value(value: CypherValue) -> str:
    if isinstance(value, _ParameterRef):
        return f":{value.name}"
    if isinstance(value, tuple):
        raise ValueError(
            "CypherGlot MVP compilation does not yet support vector values "
            "in SQL lowering."
        )
    return _sql_literal(value)


def _sql_literal(value: str | int | float | bool | None) -> str:
    return getattr(exp, "convert")(value).sql()
