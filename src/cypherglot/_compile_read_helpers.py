from __future__ import annotations

from typing import Literal

from ._compile_sql_utils import (
    _AGGREGATE_SQL_NAMES,
    _append_node_label_join,
    _append_predicate_filters,
    _append_relationship_type_filter_for_alias,
    _assemble_select_sql,
    _compile_predicate,
    _compile_stream_predicate,
    _edge_endpoint_column,
    _extend_pattern_property_filters,
    _properties_column,
    _property_expression,
    _sql_literal,
    _sql_value,
)
from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    CaseSpec,
    CypherValue,
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
    ReturnItem,
)
from .ir import GraphRelationalReadIR
from .normalize import (
    WithBinding,
    WithCaseSpec,
    WithOrderItem,
    WithPredicate,
    WithReturnItem,
)


def _compile_match_relationship_source_components(
    statement: object,
) -> tuple[
    str,
    list[str],
    list[str],
    dict[str, str],
    dict[str, Literal["node", "relationship"]],
]:
    relationship_alias = statement.relationship.alias or "edge"
    left_alias = statement.left.alias
    right_alias = statement.right.alias
    distinct_endpoints = _create_relationship_uses_distinct_nodes(
        statement.left,
        statement.right,
    )
    if distinct_endpoints:
        joins = [
            (
                f"JOIN nodes AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}."
                f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
            ),
            (
                f"JOIN nodes AS {right_alias} "
                f"ON {right_alias}.id = {relationship_alias}."
                f"{_edge_endpoint_column(statement.relationship.direction, 'right')}"
            ),
        ]
    else:
        joins = [
            (
                f"JOIN nodes AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}."
                f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
            )
        ]
    where_parts: list[str] = []

    _append_node_label_join(
        joins=joins,
        node_alias=left_alias,
        label=statement.left.label,
        join_alias=f"{left_alias}_label_0",
    )
    if distinct_endpoints:
        _append_node_label_join(
            joins=joins,
            node_alias=right_alias,
            label=statement.right.label,
            join_alias=f"{right_alias}_label_1",
        )
    _append_relationship_type_filter_for_alias(
        where_parts,
        statement.relationship,
        relationship_alias,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=left_alias,
        alias_kind="node",
        properties=statement.left.properties,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=relationship_alias,
        alias_kind="relationship",
        properties=statement.relationship.properties,
    )
    if distinct_endpoints:
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=right_alias,
            alias_kind="node",
            properties=statement.right.properties,
        )
    else:
        where_parts.append(
            f"{relationship_alias}.{_edge_endpoint_column(statement.relationship.direction, 'left')} = "
            f"{relationship_alias}.{_edge_endpoint_column(statement.relationship.direction, 'right')}"
        )

    alias_map = {
        left_alias: left_alias,
    }
    alias_kinds: dict[str, Literal["node", "relationship"]] = {left_alias: "node"}
    if distinct_endpoints:
        alias_map[right_alias] = right_alias
        alias_kinds[right_alias] = "node"
    if statement.relationship.alias is not None:
        alias_map[statement.relationship.alias] = relationship_alias
        alias_kinds[statement.relationship.alias] = "relationship"

    _append_predicate_filters(
        where_parts=where_parts,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        predicates=statement.predicates,
    )

    return (
        f"FROM edges AS {relationship_alias}",
        joins,
        where_parts,
        alias_map,
        alias_kinds,
    )


def _compile_unwind_sql(statement: GraphRelationalReadIR) -> str:
    alias = statement.unwind_alias
    if alias is None:
        raise ValueError("UNWIND lowering requires an unwind alias.")
    binding = WithBinding(
        source_alias=alias,
        output_alias=alias,
        binding_kind="scalar",
    )
    inner_sql = _compile_unwind_source_sql(statement)
    select_sql = _compile_with_select_list(statement.returns, (binding,))
    order_sql = _compile_with_order_by(statement.order_by, (binding,))
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


def _compile_unwind_source_sql(statement: GraphRelationalReadIR) -> str:
    alias = statement.unwind_alias
    source_kind = statement.unwind_source_kind
    source_param_name = statement.unwind_source_param_name
    source_items = statement.unwind_source_items
    if alias is None or source_kind is None:
        raise ValueError("UNWIND lowering requires an admitted source description.")
    if source_kind == "parameter":
        return (
            f"SELECT unwind_q.value AS \"{_with_scalar_prefix(alias)}\" "
            f"FROM JSON_EACH(:{source_param_name}) AS unwind_q"
        )

    return _compile_unwind_literal_source(
        alias=alias,
        items=source_items,
    )


def _compile_unwind_literal_source(
    *,
    alias: str,
    items: tuple[CypherValue, ...],
) -> str:
    if not items:
        return f'SELECT NULL AS "{_with_scalar_prefix(alias)}" WHERE 1 = 0'

    column_sql = f'"{_with_scalar_prefix(alias)}"'
    return " UNION ALL ".join(
        f"SELECT {_sql_value(item)} AS {column_sql}" for item in items
    )


def _compile_chain_source_components(
    *,
    nodes: tuple[NodePattern, ...],
    relationships: tuple[RelationshipPattern, ...],
    predicates: tuple[Predicate, ...],
) -> tuple[
    str,
    list[str],
    list[str],
    dict[str, str],
    dict[str, Literal["node", "relationship"]],
]:
    edge_aliases = [
        relationship.alias or f"__cg_edge_{index}"
        for index, relationship in enumerate(relationships)
    ]
    joins: list[str] = []
    where_parts: list[str] = []
    alias_map = {node.alias: node.alias for node in nodes}
    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        node.alias: "node" for node in nodes
    }

    for index, relationship in enumerate(relationships):
        edge_alias = edge_aliases[index]
        left_alias = nodes[index].alias
        right_alias = nodes[index + 1].alias
        if index == 0:
            joins.append(
                f"JOIN nodes AS {left_alias} ON {left_alias}.id = {edge_alias}."
                f"{_edge_endpoint_column(relationship.direction, 'left')}"
            )
        else:
            joins.append(
                f"JOIN edges AS {edge_alias} ON {nodes[index].alias}.id = {edge_alias}."
                f"{_edge_endpoint_column(relationship.direction, 'left')}"
            )
        joins.append(
            f"JOIN nodes AS {right_alias} ON {right_alias}.id = {edge_alias}."
            f"{_edge_endpoint_column(relationship.direction, 'right')}"
        )
        _append_relationship_type_filter_for_alias(
            where_parts,
            relationship,
            edge_alias,
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=edge_alias,
            alias_kind="relationship",
            properties=relationship.properties,
        )
        if relationship.alias is not None:
            alias_map[relationship.alias] = edge_alias
            alias_kinds[relationship.alias] = "relationship"

    for index, node in enumerate(nodes):
        _append_node_label_join(
            joins=joins,
            node_alias=node.alias,
            label=node.label,
            join_alias=f"{node.alias}_label_{index}",
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=node.alias,
            alias_kind="node",
            properties=node.properties,
        )

    _append_predicate_filters(
        where_parts=where_parts,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        predicates=predicates,
    )

    return (
        f"FROM edges AS {edge_aliases[0]}",
        joins,
        where_parts,
        alias_map,
        alias_kinds,
    )


def _compile_with_binding_columns(
    binding: WithBinding,
    *,
    relationship_alias: str | None = None,
    left_alias: str | None = None,
    right_alias: str | None = None,
    table_alias_map: dict[str, str] | None = None,
) -> str:
    source_table_alias = (
        table_alias_map.get(binding.source_alias, binding.source_alias)
        if table_alias_map is not None
        else binding.source_alias
    )
    if (
        table_alias_map is None
        and relationship_alias is not None
        and binding.alias_kind == "relationship"
    ):
        source_table_alias = relationship_alias
    elif (
        table_alias_map is None
        and relationship_alias is not None
        and binding.source_alias == left_alias
    ):
        source_table_alias = left_alias or binding.source_alias
    elif (
        table_alias_map is None
        and relationship_alias is not None
        and binding.source_alias == right_alias
    ):
        source_table_alias = right_alias or binding.source_alias

    if binding.binding_kind == "scalar":
        return (
            f"{_compile_with_binding_expression(binding, source_table_alias)} AS "
            f'"{_with_scalar_prefix(binding.output_alias)}"'
        )

    prefix = _with_entity_prefix(binding.output_alias)
    columns = [
        f"{source_table_alias}.id AS \"{prefix}_id\"",
    ]
    if binding.alias_kind == "relationship":
        columns.append(f"{source_table_alias}.type AS \"{prefix}_type\"")
    columns.append(
        f"{_properties_column(source_table_alias, binding.alias_kind)} "
        f"AS \"{prefix}_properties\""
    )
    return ", ".join(columns)


def _compile_with_select_list(
    returns: tuple[WithReturnItem, ...],
    bindings: tuple[WithBinding, ...],
) -> str:
    binding_map = {binding.output_alias: binding for binding in bindings}
    return ", ".join(
        f"{_compile_with_return_expression(item, binding_map)} AS \"{item.column_name}\""
        for item in returns
    )


def _compile_with_non_aggregate_expression(
    item: WithReturnItem,
    binding_map: dict[str, WithBinding],
) -> str:
    if item.kind == "case":
        assert isinstance(item.value, WithCaseSpec)
        return _compile_with_case_return_expression(item.value, binding_map)

    if item.kind == "scalar_value":
        assert item.value is not None
        return _sql_value(item.value)

    if item.kind == "size" and item.value is not None:
        return f"LENGTH({_sql_value(item.value)})"

    if (
        item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}
        and item.value is not None
    ):
        return f"{item.kind.upper()}({_sql_value(item.value)})"

    if item.kind in {"left", "right"} and item.value is not None:
        assert item.length_value is not None
        return (
            f"{item.kind.upper()}({_sql_value(item.value)}, "
            f"{_sql_value(item.length_value)})"
        )

    if item.kind == "split" and item.value is not None:
        assert item.delimiter_value is not None
        return f"SPLIT({_sql_value(item.value)}, {_sql_value(item.delimiter_value)})"

    if item.kind == "abs" and item.value is not None:
        return f"ABS({_sql_value(item.value)})"

    if item.kind == "sign" and item.value is not None:
        return f"SIGN({_sql_value(item.value)})"

    if item.kind == "round" and item.value is not None:
        return f"ROUND({_sql_value(item.value)})"

    if item.kind == "ceil" and item.value is not None:
        return f"CEIL({_sql_value(item.value)})"

    if item.kind == "floor" and item.value is not None:
        return f"FLOOR({_sql_value(item.value)})"

    if item.kind == "sqrt" and item.value is not None:
        return f"SQRT({_sql_value(item.value)})"

    if item.kind == "exp" and item.value is not None:
        return f"EXP({_sql_value(item.value)})"

    if item.kind == "sin" and item.value is not None:
        return f"SIN({_sql_value(item.value)})"

    if item.kind == "cos" and item.value is not None:
        return f"COS({_sql_value(item.value)})"

    if item.kind == "tan" and item.value is not None:
        return f"TAN({_sql_value(item.value)})"

    if item.kind == "asin" and item.value is not None:
        return f"ASIN({_sql_value(item.value)})"

    if item.kind == "acos" and item.value is not None:
        return f"ACOS({_sql_value(item.value)})"

    if item.kind == "atan" and item.value is not None:
        return f"ATAN({_sql_value(item.value)})"

    if item.kind == "ln" and item.value is not None:
        return f"LN({_sql_value(item.value)})"

    if item.kind == "log" and item.value is not None:
        return f"LOG({_sql_value(item.value)})"

    if item.kind == "log10" and item.value is not None:
        return f"LOG10({_sql_value(item.value)})"

    if item.kind == "radians" and item.value is not None:
        return f"RADIANS({_sql_value(item.value)})"

    if item.kind == "degrees" and item.value is not None:
        return f"DEGREES({_sql_value(item.value)})"

    if item.kind == "to_string" and item.value is not None:
        return f"CAST({_sql_value(item.value)} AS TEXT)"

    if item.kind == "to_integer" and item.value is not None:
        return f"CAST({_sql_value(item.value)} AS INTEGER)"

    if item.kind == "to_float" and item.value is not None:
        return f"CAST({_sql_value(item.value)} AS REAL)"

    if item.kind == "to_boolean" and item.value is not None:
        return f"CAST({_sql_value(item.value)} AS BOOLEAN)"

    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        if item.value is not None:
            if item.length_value is None:
                return f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1))"
            length_sql = _sql_value(item.length_value)
            return (
                f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1), "
                f"{length_sql})"
            )

    if item.kind == "coalesce" and item.value is not None:
        binding = binding_map[item.alias]
        if binding.binding_kind == "scalar" and item.field is None:
            inner = f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
        else:
            inner = _compile_with_non_aggregate_expression(
                WithReturnItem(kind="field", alias=item.alias, field=item.field),
                binding_map,
            )
        return f"COALESCE({inner}, {_sql_value(item.value)})"

    if item.kind == "replace":
        assert item.search_value is not None
        assert item.replace_value is not None
        if item.value is not None:
            return (
                f"REPLACE({_sql_value(item.value)}, {_sql_value(item.search_value)}, "
                f"{_sql_value(item.replace_value)})"
            )
        binding = binding_map[item.alias]
        if binding.binding_kind == "scalar" and item.field is None:
            inner = f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
        else:
            inner = _compile_with_non_aggregate_expression(
                WithReturnItem(kind="field", alias=item.alias, field=item.field),
                binding_map,
            )
        return (
            f"REPLACE({inner}, {_sql_value(item.search_value)}, "
            f"{_sql_value(item.replace_value)})"
        )

    if item.kind in {"left", "right"}:
        assert item.length_value is not None
        function_name = item.kind.upper()
        binding = binding_map[item.alias]
        if binding.binding_kind == "scalar" and item.field is None:
            return (
                f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}", '
                f'{_sql_value(item.length_value)})'
            )
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"{function_name}({inner}, {_sql_value(item.length_value)})"

    if item.kind == "split":
        assert item.delimiter_value is not None
        binding = binding_map[item.alias]
        if item.value is not None:
            return f"SPLIT({_sql_value(item.value)}, {_sql_value(item.delimiter_value)})"
        if binding.binding_kind == "scalar" and item.field is None:
            return (
                f'SPLIT(with_q."{_with_scalar_prefix(binding.output_alias)}", '
                f'{_sql_value(item.delimiter_value)})'
            )
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"SPLIT({inner}, {_sql_value(item.delimiter_value)})"

    binding = binding_map[item.alias]
    if item.kind == "scalar":
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'

    if item.kind == "entity":
        return _compile_with_entity_object_expression(binding)

    if item.kind == "id":
        return f'with_q."{_with_entity_prefix(binding.output_alias)}_id"'

    if item.kind == "type":
        return f'with_q."{_with_entity_prefix(binding.output_alias)}_type"'

    if item.kind == "properties":
        return (
            f"COALESCE(with_q.\"{_with_entity_prefix(binding.output_alias)}_properties\", '{{}}')"
        )

    if item.kind == "labels":
        prefix = _with_entity_prefix(binding.output_alias)
        label_alias = f"{binding.output_alias}_label_values"
        return (
            "COALESCE((SELECT JSON_GROUP_ARRAY("
            f"{label_alias}.label) FROM node_labels AS {label_alias} "
            f"WHERE {label_alias}.node_id = with_q.\"{prefix}_id\"), '[]')"
        )

    if item.kind == "keys":
        prefix = _with_entity_prefix(binding.output_alias)
        key_alias = f"{binding.output_alias}_property_keys"
        return (
            "COALESCE((SELECT JSON_GROUP_ARRAY("
            f"{key_alias}.key) FROM JSON_EACH(COALESCE(with_q.\"{prefix}_properties\", '{{}}')) AS {key_alias}), '[]')"
        )

    if item.kind == "start_node":
        prefix = _with_entity_prefix(binding.output_alias)
        edge_alias = f"{binding.output_alias}_start_edge"
        node_alias = f"{binding.output_alias}_start_node"
        node_id_expression = (
            f'(SELECT {edge_alias}.from_id FROM edges AS {edge_alias} '
            f'WHERE {edge_alias}.id = with_q."{prefix}_id")'
        )
        if item.field is not None:
            return _compile_node_field_from_id_expression(
                entity_alias=f"{binding.output_alias}_start",
                node_alias=node_alias,
                node_id_expression=node_id_expression,
                field=item.field,
            )
        return _raise_whole_node_return_removed()

    if item.kind == "end_node":
        prefix = _with_entity_prefix(binding.output_alias)
        edge_alias = f"{binding.output_alias}_end_edge"
        node_alias = f"{binding.output_alias}_end_node"
        node_id_expression = (
            f'(SELECT {edge_alias}.to_id FROM edges AS {edge_alias} '
            f'WHERE {edge_alias}.id = with_q."{prefix}_id")'
        )
        if item.field is not None:
            return _compile_node_field_from_id_expression(
                entity_alias=f"{binding.output_alias}_end",
                node_alias=node_alias,
                node_id_expression=node_id_expression,
                field=item.field,
            )
        return _raise_whole_node_return_removed()

    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        function_name = item.kind.upper()
        if binding.binding_kind == "scalar" and item.field is None:
            return f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"{function_name}({inner})"

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
        function_name = item.kind.upper()
        if binding.binding_kind == "scalar" and item.field is None:
            return f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"{function_name}({inner})"

    if item.kind in {"to_string", "to_integer", "to_float", "to_boolean"}:
        cast_type = {
            "to_string": "TEXT",
            "to_integer": "INTEGER",
            "to_float": "REAL",
            "to_boolean": "BOOLEAN",
        }[item.kind]
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CAST(with_q."{_with_scalar_prefix(binding.output_alias)}" AS {cast_type})'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CAST({inner} AS {cast_type})"

    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        if item.length_value is None:
            if binding.binding_kind == "scalar" and item.field is None:
                return (
                    f'SUBSTRING(with_q."{_with_scalar_prefix(binding.output_alias)}", '
                    f'({start_sql} + 1))'
                )
            inner = _compile_with_non_aggregate_expression(
                WithReturnItem(kind="field", alias=item.alias, field=item.field),
                binding_map,
            )
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        length_sql = _sql_value(item.length_value)
        if binding.binding_kind == "scalar" and item.field is None:
            return (
                f'SUBSTRING(with_q."{_with_scalar_prefix(binding.output_alias)}", '
                f'({start_sql} + 1), {length_sql})'
            )
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"SUBSTRING({inner}, ({start_sql} + 1), {length_sql})"

    if item.kind == "size":
        if binding.binding_kind == "scalar":
            return f'LENGTH(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"LENGTH({inner})"

    if item.kind == "predicate":
        assert item.operator is not None
        if item.field is not None and item.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
            inner_field = item.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
            if inner_field == "__value__":
                expression = f'LENGTH(with_q."{_with_scalar_prefix(binding.output_alias)}")'
            else:
                expression = _compile_with_non_aggregate_expression(
                    WithReturnItem(kind="size", alias=item.alias, field=inner_field),
                    binding_map,
                )
            return _compile_stream_predicate(expression, None, item.operator, item.value)
        return _compile_with_predicate(
            WithPredicate(
                kind="scalar" if item.field is None else "field",
                alias=item.alias,
                field=item.field,
                operator=item.operator,
                value=item.value,
            ),
            binding_map,
        )

    prefix = _with_entity_prefix(binding.output_alias)
    if item.field == "id":
        return f'with_q."{prefix}_id"'
    if binding.alias_kind == "node" and item.field == "label":
        return (
            f'(SELECT {item.alias}_label_return.label FROM node_labels AS {item.alias}_label_return '
            f'WHERE {item.alias}_label_return.node_id = with_q."{prefix}_id" LIMIT 1)'
        )
    if binding.alias_kind == "relationship" and item.field == "type":
        return f'with_q."{prefix}_type"'
    return (
        f'JSON_EXTRACT(with_q."{prefix}_properties", '
        f'{_sql_literal("$." + item.field)})'
    )


def _compile_with_return_expression(
    item: WithReturnItem,
    binding_map: dict[str, WithBinding],
) -> str:
    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_with_aggregate_return_expression(item, binding_map)
    return _compile_with_non_aggregate_expression(item, binding_map)


def _compile_with_order_by(
    order_by: tuple[WithOrderItem, ...],
    bindings: tuple[WithBinding, ...],
) -> str | None:
    if not order_by:
        return None
    binding_map = {binding.output_alias: binding for binding in bindings}
    return ", ".join(
        f"{_compile_with_order_expression(item, binding_map)} {item.direction.upper()}"
        for item in order_by
    )


def _compile_with_order_expression(
    item: WithOrderItem,
    binding_map: dict[str, WithBinding],
) -> str:
    if item.kind == "aggregate":
        return f'"{item.alias}"'
    return _compile_with_return_expression(
        WithReturnItem(
            kind=item.kind,
            alias=item.alias,
            field=item.field,
            operator=item.operator,
            value=item.value,
            start_value=item.start_value,
            length_value=item.length_value,
            search_value=item.search_value,
            replace_value=item.replace_value,
            delimiter_value=item.delimiter_value,
        ),
        binding_map,
    )


def _compile_with_group_by(
    returns: tuple[WithReturnItem, ...],
    bindings: tuple[WithBinding, ...],
) -> str | None:
    if not any(item.kind in _AGGREGATE_SQL_NAMES for item in returns):
        return None
    binding_map = {binding.output_alias: binding for binding in bindings}
    group_items = [
        _compile_with_return_expression(item, binding_map)
        for item in returns
        if item.kind not in _AGGREGATE_SQL_NAMES
    ]
    if not group_items:
        return None
    return ", ".join(group_items)


def _compile_with_entity_object_expression(binding: WithBinding) -> str:
    _ = binding
    raise ValueError(
        "CypherGlot relational output no longer supports whole-entity WITH returns."
    )


def _compile_with_binding_expression(binding: WithBinding, table_alias: str) -> str:
    if binding.alias_kind is None or binding.source_field is None:
        raise ValueError("Scalar WITH bindings require a source alias kind and field.")
    field = binding.source_field
    if field == "id":
        return f"{table_alias}.id"
    if binding.alias_kind == "node" and field == "label":
        return (
            f"(SELECT {binding.output_alias}_label_return.label "
            f"FROM node_labels AS {binding.output_alias}_label_return "
            f"WHERE {binding.output_alias}_label_return.node_id = {table_alias}.id "
            "LIMIT 1)"
        )
    if binding.alias_kind == "relationship" and field == "type":
        return f"{table_alias}.type"
    return _property_expression(table_alias, binding.alias_kind, field)


def _compile_with_count_argument(binding: WithBinding) -> str:
    if binding.binding_kind == "scalar":
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
    return f'with_q."{_with_entity_prefix(binding.output_alias)}_id"'


def _compile_with_predicates(
    predicates: tuple[WithPredicate, ...],
    bindings: tuple[WithBinding, ...],
) -> list[str]:
    if not predicates:
        return []

    binding_map = {binding.output_alias: binding for binding in bindings}
    disjuncts: dict[int, list[str]] = {}
    disjunct_order: list[int] = []
    for predicate in predicates:
        if predicate.disjunct_index not in disjuncts:
            disjuncts[predicate.disjunct_index] = []
            disjunct_order.append(predicate.disjunct_index)
        disjuncts[predicate.disjunct_index].append(
            _compile_with_predicate(predicate, binding_map)
        )

    if len(disjunct_order) == 1:
        return disjuncts[disjunct_order[0]]

    return [
        "(" + " OR ".join(
            "(" + " AND ".join(disjuncts[index]) + ")"
            for index in disjunct_order
        ) + ")"
    ]


def _compile_with_predicate(
    predicate: WithPredicate,
    binding_map: dict[str, WithBinding],
) -> str:
    binding = binding_map[predicate.alias]
    if predicate.kind == "scalar":
        expression = f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
        return _compile_stream_predicate(
            expression,
            None,
            predicate.operator,
            predicate.value,
        )

    assert predicate.field is not None
    return _compile_with_field_predicate(
        binding,
        predicate.field,
        predicate.operator,
        predicate.value,
    )


def _compile_with_field_predicate(
    binding: WithBinding,
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
    if field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        if binding.binding_kind == "scalar":
            if inner_field != "__value__":
                raise ValueError(
                    "CypherGlot MVP compilation supports WITH size predicates on scalar bindings only as size(scalar_alias)."
                )
            expression = f'LENGTH(with_q."{_with_scalar_prefix(binding.output_alias)}")'
            return _compile_stream_predicate(expression, None, operator, value)

        prefix = _with_entity_prefix(binding.output_alias)
        expression = (
            f'LENGTH(JSON_EXTRACT(with_q."{prefix}_properties", '
            f'{_sql_literal("$." + inner_field)}))'
        )
        return _compile_stream_predicate(expression, None, operator, value)

    prefix = _with_entity_prefix(binding.output_alias)
    if field == "id":
        expression = f'with_q."{prefix}_id"'
        return _compile_stream_predicate(expression, None, operator, value)
    if binding.alias_kind == "node" and field == "label":
        if operator != "=":
            raise ValueError(
                "CypherGlot MVP compilation supports only equality predicates on node label."
            )
        expression = (
            f"(SELECT {binding.output_alias}_label_filter.label "
            f"FROM node_labels AS {binding.output_alias}_label_filter "
            f"WHERE {binding.output_alias}_label_filter.node_id = with_q.\"{prefix}_id\" "
            "LIMIT 1)"
        )
        return f"{expression} = {_sql_value(value)}"
    if binding.alias_kind == "relationship" and field == "type":
        if operator != "=":
            raise ValueError(
                "CypherGlot MVP compilation supports only equality predicates on relationship type."
            )
        expression = f'with_q."{prefix}_type"'
        return f"{expression} = {_sql_value(value)}"

    expression = (
        f'JSON_EXTRACT(with_q."{prefix}_properties", '
        f'{_sql_literal("$." + field)})'
    )
    type_expression = (
        f'JSON_TYPE(with_q."{prefix}_properties", '
        f'{_sql_literal("$." + field)})'
    )
    return _compile_stream_predicate(expression, type_expression, operator, value)


def _compile_select_list(
    *,
    returns: tuple[ReturnItem, ...],
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    return ", ".join(
        f"{_compile_return_expression(item, alias_map, alias_kinds)} "
        f"AS \"{item.column_name}\""
        for item in returns
    )


def _compile_return_expression(
    item: ReturnItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    if item.kind == "id":
        return f"{alias_map[item.alias]}.id"
    if item.kind == "type":
        return f"{alias_map[item.alias]}.type"
    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        function_name = item.kind.upper()
        if item.value is not None:
            return f"{function_name}({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"{function_name}({inner})"
    if item.kind == "to_string":
        if item.value is not None:
            return f"CAST({_sql_value(item.value)} AS TEXT)"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"CAST({inner} AS TEXT)"
    if item.kind == "to_integer":
        if item.value is not None:
            return f"CAST({_sql_value(item.value)} AS INTEGER)"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"CAST({inner} AS INTEGER)"
    if item.kind == "to_float":
        if item.value is not None:
            return f"CAST({_sql_value(item.value)} AS REAL)"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"CAST({inner} AS REAL)"
    if item.kind == "to_boolean":
        if item.value is not None:
            return f"CAST({_sql_value(item.value)} AS BOOLEAN)"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"CAST({inner} AS BOOLEAN)"
    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        if item.value is not None:
            if item.length_value is None:
                return f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1))"
            length_sql = _sql_value(item.length_value)
            return (
                f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1), "
                f"{length_sql})"
            )
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        if item.length_value is None:
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        length_sql = _sql_value(item.length_value)
        return f"SUBSTRING({inner}, ({start_sql} + 1), {length_sql})"
    if item.kind in {"round", "ceil", "floor", "abs", "sign", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees"}:
        function_name = item.kind.upper()
        if item.value is not None:
            return f"{function_name}({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"{function_name}({inner})"
    if item.kind == "coalesce":
        assert item.value is not None
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"COALESCE({inner}, {_sql_value(item.value)})"

    if item.kind == "replace":
        assert item.search_value is not None
        assert item.replace_value is not None
        if item.value is not None:
            return (
                f"REPLACE({_sql_value(item.value)}, {_sql_value(item.search_value)}, "
                f"{_sql_value(item.replace_value)})"
            )
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return (
            f"REPLACE({inner}, {_sql_value(item.search_value)}, "
            f"{_sql_value(item.replace_value)})"
        )
    if item.kind in {"left", "right"}:
        assert item.length_value is not None
        function_name = item.kind.upper()
        if item.value is not None:
            return (
                f"{function_name}({_sql_value(item.value)}, "
                f"{_sql_value(item.length_value)})"
            )
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"{function_name}({inner}, {_sql_value(item.length_value)})"
    if item.kind == "split":
        assert item.delimiter_value is not None
        if item.value is not None:
            return f"SPLIT({_sql_value(item.value)}, {_sql_value(item.delimiter_value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"SPLIT({inner}, {_sql_value(item.delimiter_value)})"
    if item.kind == "predicate":
        return _compile_return_predicate_expression(item, alias_map, alias_kinds)
    if item.kind == "case":
        assert isinstance(item.value, CaseSpec)
        return _compile_case_return_expression(item.value, alias_map, alias_kinds)
    if item.kind == "size":
        return _compile_size_expression(item, alias_map, alias_kinds)
    if item.kind == "scalar":
        assert item.value is not None
        return _sql_value(item.value)
    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_aggregate_return_expression(item, alias_map, alias_kinds)

    table_alias = alias_map[item.alias]
    alias_kind = alias_kinds[item.alias]

    if item.kind == "properties":
        return f"COALESCE({table_alias}.properties, '{{}}')"

    if item.kind == "labels":
        label_alias = f"{item.alias}_label_values"
        return (
            "COALESCE((SELECT JSON_GROUP_ARRAY("
            f"{label_alias}.label) FROM node_labels AS {label_alias} "
            f"WHERE {label_alias}.node_id = {table_alias}.id), '[]')"
        )

    if item.kind == "keys":
        key_alias = f"{item.alias}_property_keys"
        return (
            "COALESCE((SELECT JSON_GROUP_ARRAY("
            f"{key_alias}.key) FROM JSON_EACH(COALESCE({table_alias}.properties, '{{}}')) AS {key_alias}), '[]')"
        )

    if item.kind == "start_node":
        start_node_alias = f"{item.alias}_start_node"
        if item.field is not None:
            return _compile_node_field_from_id_expression(
                entity_alias=f"{item.alias}_start",
                node_alias=start_node_alias,
                node_id_expression=f"{table_alias}.from_id",
                field=item.field,
            )
        return _raise_whole_node_return_removed()

    if item.kind == "end_node":
        end_node_alias = f"{item.alias}_end_node"
        if item.field is not None:
            return _compile_node_field_from_id_expression(
                entity_alias=f"{item.alias}_end",
                node_alias=end_node_alias,
                node_id_expression=f"{table_alias}.to_id",
                field=item.field,
            )
        return _raise_whole_node_return_removed()

    if item.field is None:
        return _raise_whole_entity_return_removed()

    if item.field == "id":
        return f"{table_alias}.id"
    if alias_kind == "node" and item.field == "label":
        return (
            f"(SELECT {item.alias}_label_return.label "
            f"FROM node_labels AS {item.alias}_label_return "
            f"WHERE {item.alias}_label_return.node_id = {table_alias}.id LIMIT 1)"
        )
    if alias_kind == "relationship" and item.field == "type":
        return f"{table_alias}.type"
    return _property_expression(table_alias, alias_kind, item.field)


def _compile_order_by(
    *,
    order_by: tuple[OrderItem, ...],
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    returns: tuple[ReturnItem, ...] = (),
) -> str | None:
    if not order_by:
        return None
    return ", ".join(
        f"{_compile_order_expression(item, alias_map, alias_kinds, returns)} "
        f"{item.direction.upper()}"
        for item in order_by
    )


def _compile_order_expression(
    item: OrderItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    returns: tuple[ReturnItem, ...] = (),
) -> str:
    if item.field == "__value__":
        for return_item in returns:
            if return_item.output_alias == item.alias:
                if return_item.kind in _AGGREGATE_SQL_NAMES:
                    return f'"{return_item.column_name}"'
                return _compile_return_expression(return_item, alias_map, alias_kinds)

    table_alias = alias_map[item.alias]
    alias_kind = alias_kinds[item.alias]

    if item.field == "id":
        return f"{table_alias}.id"
    if item.field == "__value__":
        raise ValueError(f"Unknown ORDER BY alias: {item.alias}")
    if alias_kind == "node" and item.field == "label":
        return (
            f"(SELECT {item.alias}_label_order.label "
            f"FROM node_labels AS {item.alias}_label_order "
            f"WHERE {item.alias}_label_order.node_id = {table_alias}.id LIMIT 1)"
        )
    if alias_kind == "relationship" and item.field == "type":
        return f"{table_alias}.type"
    return _property_expression(table_alias, alias_kind, item.field)


def _raise_whole_entity_return_removed() -> str:
    raise ValueError(
        "CypherGlot relational output no longer supports whole-entity returns."
    )


def _raise_whole_node_return_removed() -> str:
    raise ValueError(
        "CypherGlot relational output no longer supports whole-node helper returns."
    )


def _compile_node_field_from_id_expression(
    *,
    entity_alias: str,
    node_alias: str,
    node_id_expression: str,
    field: str,
) -> str:
    if field == "id":
        return node_id_expression
    if field == "label":
        return (
            f"(SELECT {entity_alias}_label_return.label FROM node_labels AS {entity_alias}_label_return "
            f"WHERE {entity_alias}_label_return.node_id = {node_id_expression} LIMIT 1)"
        )
    return (
        f"JSON_EXTRACT((SELECT {node_alias}.properties FROM nodes AS {node_alias} "
        f"WHERE {node_alias}.id = {node_id_expression}), '$.{field}')"
    )


def _compile_size_expression(
    item: ReturnItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    if item.field is not None:
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"LENGTH({inner})"
    assert item.value is not None
    return f"LENGTH({_sql_value(item.value)})"


def _compile_return_predicate_expression(
    item: ReturnItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    assert item.field is not None
    assert item.operator is not None
    if item.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = item.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        return _compile_stream_predicate(
            _compile_size_expression(
                ReturnItem(alias=item.alias, field=inner_field, kind="size"),
                alias_map,
                alias_kinds,
            ),
            None,
            item.operator,
            item.value,
        )
    return _compile_predicate(
        predicate=Predicate(
            alias=item.alias,
            field=item.field,
            operator=item.operator,
            value=item.value,
        ),
        table_alias=alias_map[item.alias],
        alias_kind=alias_kinds[item.alias],
    )


def _compile_count_argument(
    item: ReturnItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    if item.alias == "*":
        return "*"
    table_alias = alias_map[item.alias]
    alias_kind = alias_kinds[item.alias]
    if alias_kind == "relationship":
        return f"{table_alias}.id"
    return f"{table_alias}.id"


def _compile_case_return_expression(
    spec: CaseSpec,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    when_sql = " ".join(
        f"WHEN {_compile_return_predicate_expression(arm.condition, alias_map, alias_kinds)} "
        f"THEN {_compile_return_expression(arm.result, alias_map, alias_kinds)}"
        for arm in spec.when_items
    )
    else_sql = _compile_return_expression(spec.else_item, alias_map, alias_kinds)
    return f"CASE {when_sql} ELSE {else_sql} END"


def _compile_aggregate_return_expression(
    item: ReturnItem,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        return (
            f"{function_name}("
            f"{_compile_count_argument(item, alias_map, alias_kinds)}"
            ")"
        )
    inner = _compile_return_expression(
        ReturnItem(alias=item.alias, field=item.field, kind="field"),
        alias_map,
        alias_kinds,
    )
    return f"{function_name}({inner})"


def _compile_with_aggregate_return_expression(
    item: WithReturnItem,
    binding_map: dict[str, WithBinding],
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        if item.alias == "*":
            return "COUNT(*)"
        binding = binding_map[item.alias]
        return f"COUNT({_compile_with_count_argument(binding)})"
    binding = binding_map[item.alias]
    if binding.binding_kind != "scalar" or item.field is not None:
        raise ValueError(
            f"CypherGlot aggregate compilation currently expects scalar WITH bindings for {item.kind}(...)"
        )
    return f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}")'


def _compile_with_case_return_expression(
    spec: WithCaseSpec,
    binding_map: dict[str, WithBinding],
) -> str:
    when_sql = " ".join(
        f"WHEN {_compile_with_return_expression(arm.condition, binding_map)} "
        f"THEN {_compile_with_return_expression(arm.result, binding_map)}"
        for arm in spec.when_items
    )
    else_sql = _compile_with_return_expression(spec.else_item, binding_map)
    return f"CASE {when_sql} ELSE {else_sql} END"


def _compile_group_by(
    *,
    returns: tuple[ReturnItem, ...],
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str | None:
    if not any(item.kind in _AGGREGATE_SQL_NAMES for item in returns):
        return None
    group_items = [
        _compile_return_expression(item, alias_map, alias_kinds)
        for item in returns
        if item.kind not in _AGGREGATE_SQL_NAMES
    ]
    if not group_items:
        return None
    return ", ".join(group_items)


def _create_relationship_uses_distinct_nodes(
    left: NodePattern,
    right: NodePattern,
) -> bool:
    if left.alias != right.alias:
        return True
    if left.label != right.label:
        raise ValueError(
            "CypherGlot CREATE self-loop patterns require the repeated node alias "
            "to use the same label on both sides."
        )
    if left.properties != right.properties:
        raise ValueError(
            "CypherGlot CREATE self-loop patterns require the repeated node alias "
            "to use the same inline properties on both sides."
        )
    return False


def _with_entity_prefix(alias: str) -> str:
    return f"cg_with_{alias}"


def _with_scalar_prefix(alias: str) -> str:
    return f"cg_with_scalar_{alias}"
