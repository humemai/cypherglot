from __future__ import annotations

from typing import Callable

from ._compile_sql_utils import _AGGREGATE_SQL_NAMES, _sql_literal, _sql_value
from ._compile_type_aware_common import (
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_node_field_expression,
)
from ._normalize_support import OrderItem, ReturnItem


_TYPE_AWARE_RELATIONAL_JSON_DEPENDENT_KINDS = {
    "entity",
    "properties",
    "labels",
    "keys",
}


def _compile_type_aware_match_node_select_expression(
    alias: str,
    node_type: object,
    item: ReturnItem,
) -> str:
    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_type_aware_match_node_aggregate_return_expression(
            alias,
            node_type,
            item,
        )
    return _compile_type_aware_return_expression(
        alias,
        node_type,
        item,
    )


def _compile_type_aware_match_node_select_expressions(
    alias: str,
    node_type: object,
    item: ReturnItem,
) -> list[tuple[str, str]]:
    if item.kind == "entity":
        output_name = item.column_name
        expressions = [
            (f"{alias}.id", f"{output_name}.id"),
            (_sql_literal(node_type.name), f"{output_name}.label"),
        ]
        expressions.extend(
            (
                f"{alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in node_type.properties
        )
        return expressions
    if item.kind == "properties":
        if not node_type.properties:
            raise ValueError(
                "Type-aware relational output mode does not yet support "
                "properties(...) for entity types without declared properties."
            )
        output_name = item.column_name
        return [
            (
                f"{alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in node_type.properties
        ]

    return [
        (
            _compile_type_aware_match_node_select_expression(
                alias,
                node_type,
                item,
            ),
            item.column_name,
        )
    ]


def _compile_type_aware_match_node_aggregate_return_expression(
    alias: str,
    node_type: object,
    item: ReturnItem,
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        if item.alias == "*":
            return "COUNT(*)"
        return f"{function_name}({alias}.id)"
    inner = _compile_type_aware_return_expression(
        alias,
        node_type,
        ReturnItem(alias=item.alias, field=item.field, kind="field"),
    )
    return f"{function_name}({inner})"


def _compile_type_aware_match_node_group_by(
    alias: str,
    node_type: object,
    returns: tuple[ReturnItem, ...],
) -> str | None:
    if not any(item.kind in _AGGREGATE_SQL_NAMES for item in returns):
        return None
    group_items: list[str] = []
    for item in returns:
        if item.kind in _AGGREGATE_SQL_NAMES:
            continue
        if item.kind in {"entity", "properties"}:
            group_items.extend(
                expression
                for expression, _ in _compile_type_aware_match_node_select_expressions(
                    alias,
                    node_type,
                    item,
                )
            )
            continue
        group_items.append(
            _compile_type_aware_return_expression(
                alias,
                node_type,
                item,
            )
        )
    if not group_items:
        return None
    return ", ".join(group_items)


def _compile_type_aware_return_expression(
    alias: str,
    node_type: object,
    item: ReturnItem,
) -> str:
    scalar_expression = _compile_type_aware_scalar_return_expression(
        item,
        field_expression_resolver=(
            lambda field: _compile_type_aware_node_field_expression(
                alias,
                node_type,
                field,
            )
        ),
    )
    if scalar_expression is not None:
        return scalar_expression
    _require_type_aware_relational_support(
        item.kind,
        field=item.field,
    )
    if item.kind == "field":
        assert item.field is not None
        return _compile_type_aware_node_field_expression(alias, node_type, item.field)
    if item.kind == "id":
        return f"{alias}.id"
    if item.kind == "scalar":
        assert item.value is not None
        return _sql_value(item.value)
    raise ValueError(
        "Type-aware lowering currently supports entity, properties, labels, "
        "keys, field, id, scalar, and the first scalar-function slice for "
        "single-node MATCH reads."
    )


def _compile_type_aware_scalar_return_expression(
    item: ReturnItem,
    *,
    field_expression_resolver: Callable[[str], str],
) -> str | None:
    if item.kind == "field":
        assert item.field is not None
        return field_expression_resolver(item.field)
    if item.kind == "id":
        return field_expression_resolver("id")
    if item.kind == "scalar":
        assert item.value is not None
        return _sql_value(item.value)
    if item.kind == "size":
        if item.field is not None:
            return f"LENGTH({field_expression_resolver(item.field)})"
        assert item.value is not None
        return f"LENGTH({_sql_value(item.value)})"
    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"{item.kind.upper()}({inner})"
    if item.kind == "coalesce":
        assert item.field is not None
        assert item.value is not None
        field_sql = field_expression_resolver(item.field)
        value_sql = _sql_value(item.value)
        return f"COALESCE({field_sql}, {value_sql})"
    if item.kind == "replace":
        assert item.search_value is not None
        assert item.replace_value is not None
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return (
            f"REPLACE({inner}, {_sql_value(item.search_value)}, "
            f"{_sql_value(item.replace_value)})"
        )
    if item.kind in {"left", "right"}:
        assert item.length_value is not None
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"{item.kind.upper()}({inner}, {_sql_value(item.length_value)})"
    if item.kind == "split":
        assert item.delimiter_value is not None
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"SPLIT({inner}, {_sql_value(item.delimiter_value)})"
    if item.kind in {
        "abs",
        "sign",
        "round",
        "ceil",
        "floor",
        "sqrt",
        "exp",
        "sin",
        "cos",
        "tan",
        "asin",
        "acos",
        "atan",
        "ln",
        "log",
        "log10",
        "radians",
        "degrees",
    }:
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"{item.kind.upper()}({inner})"
    if item.kind == "to_string":
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"CAST({inner} AS TEXT)"
    if item.kind == "to_integer":
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"CAST({inner} AS INTEGER)"
    if item.kind == "to_float":
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"CAST({inner} AS REAL)"
    if item.kind == "to_boolean":
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        return f"CAST({inner} AS BOOLEAN)"
    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        if item.field is not None:
            inner = field_expression_resolver(item.field)
        else:
            assert item.value is not None
            inner = _sql_value(item.value)
        if item.length_value is None:
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        return (
            f"SUBSTRING({inner}, ({start_sql} + 1), "
            f"{_sql_value(item.length_value)})"
        )
    return None


def _compile_type_aware_order_by(
    alias: str,
    node_type: object,
    order_by: tuple[OrderItem, ...],
    returns: tuple[ReturnItem, ...],
) -> str | None:
    if not order_by:
        return None
    parts: list[str] = []
    for item in order_by:
        if item.field == "__value__":
            matched_return = next(
                (
                    return_item
                    for return_item in returns
                    if return_item.output_alias == item.alias
                ),
                None,
            )
            if matched_return is not None:
                if matched_return.kind in _AGGREGATE_SQL_NAMES:
                    parts.append(
                        f'"{matched_return.column_name}" '
                        f"{item.direction.upper()}"
                    )
                    continue
                if matched_return.kind in {
                    "entity",
                    "properties",
                }:
                    parts.extend(
                        f"{expression} {item.direction.upper()}"
                        for expression, _ in _compile_type_aware_match_node_select_expressions(
                            alias,
                            node_type,
                            matched_return,
                        )
                    )
                    continue
                expression = _compile_type_aware_return_expression(
                    alias,
                    node_type,
                    matched_return,
                )
                parts.append(f"{expression} {item.direction.upper()}")
                continue

        expression = _compile_type_aware_node_field_expression(
            alias,
            node_type,
            item.field,
        )
        parts.append(f"{expression} {item.direction.upper()}")
    return ", ".join(parts)


def _compile_type_aware_relationship_return_expression(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    item: ReturnItem,
) -> str:
    if item.kind == "start_node":
        if item.field is not None:
            return _compile_type_aware_node_field_expression(
                left_alias,
                left_type,
                item.field,
            )
        _require_type_aware_relational_support(
            item.kind,
            field=item.field,
        )
        raise AssertionError("unreachable")
    if item.kind == "end_node":
        if item.field is not None:
            return _compile_type_aware_node_field_expression(
                right_alias,
                right_type,
                item.field,
            )
        _require_type_aware_relational_support(
            item.kind,
            field=item.field,
        )
        raise AssertionError("unreachable")
    if item.alias == left_alias:
        return _compile_type_aware_return_expression(
            left_alias,
            left_type,
            item,
        )
    if item.alias == right_alias:
        return _compile_type_aware_return_expression(
            right_alias,
            right_type,
            item,
        )
    if item.alias == relationship_alias:
        scalar_expression = _compile_type_aware_scalar_return_expression(
            item,
            field_expression_resolver=(
                lambda field: _compile_type_aware_edge_field_expression(
                    relationship_alias,
                    edge_type,
                    field,
                )
            ),
        )
        if scalar_expression is not None:
            return scalar_expression
        _require_type_aware_relational_support(
            item.kind,
            field=item.field,
        )
        if item.kind == "field":
            assert item.field is not None
            return _compile_type_aware_edge_field_expression(
                relationship_alias,
                edge_type,
                item.field,
            )
        if item.kind == "id":
            return f"{relationship_alias}.id"
        if item.kind == "type":
            return _sql_literal(edge_type.name)
        if item.kind == "scalar":
            assert item.value is not None
            return _sql_value(item.value)
        raise ValueError(
            "Type-aware lowering currently supports entity, properties, keys, "
            "field, id, type, scalar, and the first scalar-function slice for "
            "one-hop MATCH reads."
        )
    raise ValueError(f"Unknown return alias {item.alias!r} for one-hop MATCH.")


def _compile_type_aware_match_relationship_select_expression(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    item: ReturnItem,
) -> str:
    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_type_aware_match_relationship_aggregate_return_expression(
            left_alias,
            left_type,
            relationship_alias,
            edge_type,
            right_alias,
            right_type,
            item,
        )
    return _compile_type_aware_relationship_return_expression(
        left_alias,
        left_type,
        relationship_alias,
        edge_type,
        right_alias,
        right_type,
        item,
    )


def _compile_type_aware_match_relationship_select_expressions(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    item: ReturnItem,
) -> list[tuple[str, str]]:
    if item.kind == "start_node" and item.field is None:
        output_name = item.column_name
        expressions = [
            (f"{left_alias}.id", f"{output_name}.id"),
            (_sql_literal(left_type.name), f"{output_name}.label"),
        ]
        expressions.extend(
            (
                f"{left_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in left_type.properties
        )
        return expressions
    if item.kind == "end_node" and item.field is None:
        output_name = item.column_name
        expressions = [
            (f"{right_alias}.id", f"{output_name}.id"),
            (_sql_literal(right_type.name), f"{output_name}.label"),
        ]
        expressions.extend(
            (
                f"{right_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in right_type.properties
        )
        return expressions
    if item.alias == relationship_alias and item.kind == "entity":
        output_name = item.column_name
        expressions = [
            (f"{relationship_alias}.id", f"{output_name}.id"),
            (_sql_literal(edge_type.name), f"{output_name}.type"),
            (f"{relationship_alias}.from_id", f"{output_name}.from_id"),
            (f"{relationship_alias}.to_id", f"{output_name}.to_id"),
        ]
        expressions.extend(
            (
                f"{relationship_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in edge_type.properties
        )
        return expressions
    if item.alias == relationship_alias and item.kind == "properties":
        if not edge_type.properties:
            raise ValueError(
                "Type-aware relational output mode does not yet support "
                "properties(...) for entity types without declared properties."
            )
        output_name = item.column_name
        return [
            (
                f"{relationship_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in edge_type.properties
        ]

    return [
        (
            _compile_type_aware_match_relationship_select_expression(
                left_alias,
                left_type,
                relationship_alias,
                edge_type,
                right_alias,
                right_type,
                item,
            ),
            item.column_name,
        )
    ]


def _compile_type_aware_match_relationship_aggregate_return_expression(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    item: ReturnItem,
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        if item.alias == "*":
            return "COUNT(*)"
        if item.alias == relationship_alias:
            return f"{function_name}({relationship_alias}.id)"
        if item.alias == left_alias:
            return f"{function_name}({left_alias}.id)"
        if item.alias == right_alias:
            return f"{function_name}({right_alias}.id)"
        raise ValueError(
            f"Unknown aggregate alias {item.alias!r} for one-hop MATCH."
        )
    inner = _compile_type_aware_relationship_return_expression(
        left_alias,
        left_type,
        relationship_alias,
        edge_type,
        right_alias,
        right_type,
        ReturnItem(alias=item.alias, field=item.field, kind="field"),
    )
    return f"{function_name}({inner})"


def _compile_type_aware_match_relationship_group_by(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    returns: tuple[ReturnItem, ...],
) -> str | None:
    if not any(item.kind in _AGGREGATE_SQL_NAMES for item in returns):
        return None
    group_items: list[str] = []
    for item in returns:
        if item.kind in _AGGREGATE_SQL_NAMES:
            continue
        if item.kind in {
            "entity",
            "properties",
            "start_node",
            "end_node",
        }:
            group_items.extend(
                expression
                for expression, _ in _compile_type_aware_match_relationship_select_expressions(
                    left_alias,
                    left_type,
                    relationship_alias,
                    edge_type,
                    right_alias,
                    right_type,
                    item,
                )
            )
            continue
        group_items.append(
            _compile_type_aware_relationship_return_expression(
                left_alias,
                left_type,
                relationship_alias,
                edge_type,
                right_alias,
                right_type,
                item,
            )
        )
    if not group_items:
        return None
    return ", ".join(group_items)


def _compile_type_aware_relationship_order_by(
    left_alias: str,
    left_type: object,
    relationship_alias: str,
    edge_type: object,
    right_alias: str,
    right_type: object,
    order_by: tuple[OrderItem, ...],
    returns: tuple[ReturnItem, ...],
) -> str | None:
    if not order_by:
        return None

    parts: list[str] = []
    for item in order_by:
        if item.field == "__value__":
            matched_return = next(
                (
                    return_item
                    for return_item in returns
                    if return_item.output_alias == item.alias
                ),
                None,
            )
            if matched_return is not None:
                if matched_return.kind in _AGGREGATE_SQL_NAMES:
                    parts.append(
                        f'"{matched_return.column_name}" '
                        f"{item.direction.upper()}"
                    )
                    continue
                if matched_return.kind in {
                    "entity",
                    "properties",
                    "start_node",
                    "end_node",
                }:
                    parts.extend(
                        f"{expression} {item.direction.upper()}"
                        for expression, _ in _compile_type_aware_match_relationship_select_expressions(
                            left_alias,
                            left_type,
                            relationship_alias,
                            edge_type,
                            right_alias,
                            right_type,
                            matched_return,
                        )
                    )
                    continue
                expression = _compile_type_aware_relationship_return_expression(
                    left_alias,
                    left_type,
                    relationship_alias,
                    edge_type,
                    right_alias,
                    right_type,
                    matched_return,
                )
                parts.append(f"{expression} {item.direction.upper()}")
                continue

        if item.alias == left_alias:
            expression = _compile_type_aware_node_field_expression(
                left_alias,
                left_type,
                item.field,
            )
        elif item.alias == right_alias:
            expression = _compile_type_aware_node_field_expression(
                right_alias,
                right_type,
                item.field,
            )
        elif item.alias == relationship_alias:
            expression = _compile_type_aware_edge_field_expression(
                relationship_alias,
                edge_type,
                item.field,
            )
        else:
            raise ValueError(
                f"Unknown ORDER BY alias {item.alias!r} for one-hop MATCH."
            )
        parts.append(f"{expression} {item.direction.upper()}")
    return ", ".join(parts)


def _require_type_aware_relational_support(
    kind: str,
    *,
    field: str | None,
) -> None:
    if kind in _TYPE_AWARE_RELATIONAL_JSON_DEPENDENT_KINDS:
        raise ValueError(
            "Type-aware relational output mode does not yet support whole-entity "
            "or introspection returns that require SQL JSON constructors."
        )
    if kind in {"start_node", "end_node"} and field is None:
        raise ValueError(
            "Type-aware relational output mode does not yet support whole-entity "
            "or introspection returns that require SQL JSON constructors."
        )
