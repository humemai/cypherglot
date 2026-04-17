from __future__ import annotations

from typing import Literal

from sqlglot import exp

from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    _ParameterRef,
    CypherValue,
    Predicate,
    RelationshipPattern,
)


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
            f"substr({expression}, length({expression}) - length({value_sql}) + 1) = {value_sql}"
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


def _append_node_label_join(
    *,
    joins: list[str],
    node_alias: str,
    label: str | None,
    join_alias: str,
) -> None:
    if label is None:
        return
    joins.append(
        f"JOIN node_labels AS {join_alias} "
        f"ON {join_alias}.node_id = {node_alias}.id "
        f"AND {join_alias}.label = {_sql_literal(label)}"
    )


def _append_node_label_filter(
    *,
    where_parts: list[str],
    node_alias: str,
    label: str | None,
    filter_alias: str,
) -> None:
    if label is None:
        return
    where_parts.append(
        "EXISTS ("
        f"SELECT 1 FROM node_labels AS {filter_alias} "
        f"WHERE {filter_alias}.node_id = {node_alias}.id "
        f"AND {filter_alias}.label = {_sql_literal(label)}"
        ")"
    )


def _append_relationship_type_filter(
    where_parts: list[str],
    relationship: RelationshipPattern,
) -> None:
    if relationship.type_name is None:
        return
    rel_alias = relationship.alias or "edge"
    _append_relationship_type_filter_for_alias(where_parts, relationship, rel_alias)


def _append_relationship_type_filter_for_alias(
    where_parts: list[str],
    relationship: RelationshipPattern,
    rel_alias: str,
) -> None:
    if relationship.type_name is None:
        return
    type_names = relationship.type_name.split("|")
    if len(type_names) == 1:
        where_parts.append(f"{rel_alias}.type = {_sql_literal(type_names[0])}")
        return
    options = ", ".join(_sql_literal(type_name) for type_name in type_names)
    where_parts.append(f"{rel_alias}.type IN ({options})")


def _extend_pattern_property_filters(
    *,
    where_parts: list[str],
    alias: str,
    alias_kind: Literal["node", "relationship"],
    properties: tuple[tuple[str, CypherValue], ...],
) -> None:
    for field, value in properties:
        where_parts.append(
            _compile_property_predicate(
                alias=alias,
                alias_kind=alias_kind,
                field=field,
                operator="=",
                value=value,
            )
        )


def _append_predicate_filters(
    *,
    where_parts: list[str],
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    predicates: tuple[Predicate, ...],
) -> None:
    if not predicates:
        return

    disjuncts: dict[int, list[str]] = {}
    disjunct_order: list[int] = []
    for predicate in predicates:
        if predicate.disjunct_index not in disjuncts:
            disjuncts[predicate.disjunct_index] = []
            disjunct_order.append(predicate.disjunct_index)
        disjuncts[predicate.disjunct_index].append(
            _compile_predicate(
                predicate=predicate,
                table_alias=alias_map[predicate.alias],
                alias_kind=alias_kinds[predicate.alias],
            )
        )

    if len(disjunct_order) == 1:
        where_parts.extend(disjuncts[disjunct_order[0]])
        return

    where_parts.append(
        "(" + " OR ".join(
            "(" + " AND ".join(disjuncts[index]) + ")"
            for index in disjunct_order
        ) + ")"
    )


def _compile_predicate(
    *,
    predicate: Predicate,
    table_alias: str,
    alias_kind: Literal["node", "relationship"],
) -> str:
    if predicate.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = predicate.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        expression = f"LENGTH({_property_expression(table_alias, alias_kind, inner_field)})"
        return _compile_stream_predicate(
            expression,
            None,
            predicate.operator,
            predicate.value,
        )

    if predicate.field == "id":
        if predicate.operator in {"IS NULL", "IS NOT NULL"}:
            raise ValueError(
                "CypherGlot MVP compilation does not support null predicates on id."
            )
        return f"{table_alias}.id {predicate.operator} {_sql_value(predicate.value)}"

    if alias_kind == "node" and predicate.field == "label":
        if predicate.operator != "=":
            raise ValueError(
                "CypherGlot MVP compilation supports only equality predicates "
                "on node label."
            )
        return (
            "EXISTS ("
            f"SELECT 1 FROM node_labels AS {table_alias}_label_filter "
            f"WHERE {table_alias}_label_filter.node_id = {table_alias}.id "
            f"AND {table_alias}_label_filter.label = {_sql_value(predicate.value)}"
            ")"
        )

    if alias_kind == "relationship" and predicate.field == "type":
        if predicate.operator != "=":
            raise ValueError(
                "CypherGlot MVP compilation supports only equality predicates "
                "on relationship type."
            )
        return f"{table_alias}.type = {_sql_value(predicate.value)}"

    return _compile_property_predicate(
        alias=table_alias,
        alias_kind=alias_kind,
        field=predicate.field,
        operator=predicate.operator,
        value=predicate.value,
    )


def _compile_property_predicate(
    *,
    alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
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
    expression = _property_expression(alias, alias_kind, field)
    type_expression = _property_type_expression(alias, alias_kind, field)

    if operator == "IS NULL":
        return f"({type_expression} IS NULL OR {type_expression} = 'null')"
    if operator == "IS NOT NULL":
        return f"({type_expression} IS NOT NULL AND {type_expression} != 'null')"

    value_sql = _sql_value(value)

    if operator == "=":
        if value is None:
            return f"({type_expression} IS NULL OR {type_expression} = 'null')"
        return f"{expression} = {value_sql}"
    if operator in {"<", "<=", ">", ">="}:
        return f"{expression} {operator} {value_sql}"
    if operator == "STARTS WITH":
        return f"substr({expression}, 1, length({value_sql})) = {value_sql}"
    if operator == "ENDS WITH":
        return (
            f"length({expression}) >= length({value_sql}) AND "
            f"substr({expression}, length({expression}) - length({value_sql}) + 1) "
            f"= {value_sql}"
        )
    if operator == "CONTAINS":
        return f"instr({expression}, {value_sql}) > 0"

    raise ValueError(f"Unsupported predicate operator: {operator!r}")


def _property_expression(
    alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
) -> str:
    return f"JSON_EXTRACT({_properties_column(alias, alias_kind)}, '$.{field}')"


def _property_type_expression(
    alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
) -> str:
    return f"JSON_TYPE({_properties_column(alias, alias_kind)}, '$.{field}')"


def _properties_column(
    alias: str,
    alias_kind: Literal["node", "relationship"],
) -> str:
    return f"{alias}.properties"


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
    return exp.convert(value).sql()