from __future__ import annotations

from ._compile_sql_utils import _assemble_select_sql, _sql_value
from ._compile_type_aware_common import _with_scalar_prefix
from .ir import GraphRelationalReadIR
from .normalize import WithOrderItem, WithReturnItem


def _compile_unwind_sql(statement: GraphRelationalReadIR) -> str:
    alias = statement.unwind_alias
    if alias is None:
        raise ValueError("UNWIND lowering requires an unwind alias.")

    select_sql = _compile_unwind_select_list(statement.returns, alias)
    order_sql = _compile_unwind_order_by(statement.order_by, alias)
    inner_sql = _compile_unwind_source_sql(statement)
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=False,
        from_sql=f"FROM ({inner_sql}) AS with_q",
        joins=[],
        where_parts=[],
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_unwind_select_list(
    returns: tuple[WithReturnItem, ...],
    alias: str,
) -> str:
    scalar_column = _with_scalar_prefix(alias)
    parts: list[str] = []
    for item in returns:
        if item.kind != "scalar" or item.alias != alias:
            raise ValueError(
                "UNWIND lowering currently supports only scalar RETURN items on "
                "the unwind alias."
            )
        parts.append(f'with_q."{scalar_column}" AS "{item.column_name}"')
    return ", ".join(parts)


def _compile_unwind_order_by(
    order_by: tuple[WithOrderItem, ...],
    alias: str,
) -> str | None:
    if not order_by:
        return None

    scalar_column = _with_scalar_prefix(alias)
    return ", ".join(
        f'with_q."{scalar_column}" {item.direction.upper()}' for item in order_by
    )


def _compile_unwind_source_sql(statement: GraphRelationalReadIR) -> str:
    alias = statement.unwind_alias
    source_kind = statement.unwind_source_kind
    source_items = statement.unwind_source_items
    if alias is None or source_kind is None:
        raise ValueError("UNWIND lowering requires an admitted source description.")
    if source_kind != "literal":
        raise ValueError("UNWIND lowering currently supports only literal list sources.")

    return _compile_unwind_literal_source(alias=alias, items=source_items)


def _compile_unwind_literal_source(
    *,
    alias: str,
    items: tuple[object, ...],
) -> str:
    if not items:
        return f'SELECT NULL AS "{_with_scalar_prefix(alias)}" WHERE 1 = 0'

    column_sql = f'"{_with_scalar_prefix(alias)}"'
    return " UNION ALL ".join(
        f"SELECT {_sql_value(item)} AS {column_sql}" for item in items
    )
