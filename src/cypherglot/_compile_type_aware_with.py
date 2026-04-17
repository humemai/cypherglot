from __future__ import annotations

from typing import Literal

from ._compile_sql_utils import (
    _AGGREGATE_SQL_NAMES,
    _assemble_select_sql,
    _compile_stream_predicate,
    _sql_literal,
    _sql_value,
)
from ._compile_type_aware_common import (
    _TypeAwareWithBindingSpec,
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_match_relationship_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
    _compile_type_aware_with_binding_columns,
    _type_aware_with_property_column,
    _with_entity_prefix,
    _with_scalar_prefix,
)
from ._compile_type_aware_read_projections import _require_type_aware_relational_support
from ._compile_type_aware_reads import (
    _compile_type_aware_chain_source_components,
    _is_variable_length_relationship,
)
from ._normalize_support import _SIZE_PREDICATE_FIELD_PREFIX, CypherValue
from .ir import GraphRelationalReadIR
from .normalize import (
    WithCaseSpec,
    WithCaseWhen,
    WithOrderItem,
    WithPredicate,
    WithReturnItem,
)
from .schema import GraphSchema


def _compile_type_aware_match_with_return_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> str:
    inner_sql, binding_specs = _compile_type_aware_with_source_sql(
        statement,
        graph_schema,
    )
    select_sql = _compile_type_aware_with_select_list(
        statement.returns,
        binding_specs,
    )
    order_sql = _compile_type_aware_with_order_by(
        statement.order_by,
        binding_specs,
    )
    group_sql = _compile_type_aware_with_group_by(
        statement.returns,
        binding_specs,
    )
    where_parts = _compile_type_aware_with_predicates(
        statement.predicates,
        binding_specs,
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM ({inner_sql}) AS with_q",
        joins=[],
        where_parts=where_parts,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_with_source_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> tuple[str, dict[str, _TypeAwareWithBindingSpec]]:
    source = statement.source
    if source is None:
        raise ValueError(
            "Type-aware lowering requires a source read IR for MATCH ... WITH ... RETURN."
        )

    if source.source_kind == "relationship-chain":
        from_sql, joins, where_parts, alias_specs = _compile_type_aware_chain_source_components(
            nodes=source.nodes,
            relationships=source.relationships,
            predicates=source.predicates,
            graph_schema=graph_schema,
        )

        select_parts: list[str] = []
        binding_specs: dict[str, _TypeAwareWithBindingSpec] = {}
        output_alias_by_source_alias = {
            binding.source_alias: binding.output_alias
            for binding in statement.bindings
            if binding.binding_kind == "entity"
        }
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware fixed-length multi-hop source."
                )
            binding_specs[binding.output_alias] = _TypeAwareWithBindingSpec(
                binding=binding,
                entity_type=alias_spec.entity_type,
                start_binding_output_alias=(
                    output_alias_by_source_alias.get(alias_spec.start_node_alias)
                    if alias_spec.alias_kind == "relationship"
                    else None
                ),
                end_binding_output_alias=(
                    output_alias_by_source_alias.get(alias_spec.end_node_alias)
                    if alias_spec.alias_kind == "relationship"
                    else None
                ),
            )
            select_parts.extend(
                _compile_type_aware_with_binding_columns(
                    binding,
                    table_alias=alias_spec.table_alias,
                    entity_type=alias_spec.entity_type,
                )
            )

        return (
            _assemble_select_sql(
                select_sql=", ".join(select_parts),
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            ),
            binding_specs,
        )

    if source.source_kind == "node":
        node = source.node
        if node.label is None:
            raise ValueError(
                "Type-aware lowering currently requires an explicit node label in "
                "MATCH ... WITH ... RETURN node sources."
            )

        node_type = graph_schema.node_type(node.label)
        where_parts: list[str] = []
        for field, value in node.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        node.alias,
                        node_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
            )
        for predicate in source.predicates:
            if predicate.alias != node.alias:
                raise ValueError(
                    "Type-aware lowering currently supports only node-source "
                    "predicates on the matched node alias in MATCH ... WITH ... RETURN."
                )
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    node.alias,
                    node_type,
                    predicate,
                )
            )

        select_parts: list[str] = []
        binding_specs: dict[str, _TypeAwareWithBindingSpec] = {}
        for binding in statement.bindings:
            if binding.source_alias != node.alias:
                raise ValueError(
                    "Type-aware lowering currently supports MATCH ... WITH ... "
                    "RETURN node bindings only from the matched node alias."
                )
            binding_specs[binding.output_alias] = _TypeAwareWithBindingSpec(
                binding=binding,
                entity_type=node_type,
            )
            select_parts.extend(
                _compile_type_aware_with_binding_columns(
                    binding,
                    table_alias=node.alias,
                    entity_type=node_type,
                )
            )

        return (
            _assemble_select_sql(
                select_sql=", ".join(select_parts),
                distinct=False,
                from_sql=f"FROM {node_type.table_name} AS {node.alias}",
                joins=[],
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            ),
            binding_specs,
        )

    if source.source_kind == "relationship":
        if _is_variable_length_relationship(source.relationship):
            from ._compile_type_aware_variable_length import (
                compile_type_aware_variable_length_with_source_sql,
            )

            return compile_type_aware_variable_length_with_source_sql(
                statement,
                graph_schema,
            )

        relationship = source.relationship
        if relationship.type_name is None or "|" in relationship.type_name:
            raise ValueError(
                "Type-aware lowering currently requires exactly one relationship "
                "type in MATCH ... WITH ... RETURN relationship sources."
            )
        if source.left.label is None or source.right.label is None:
            raise ValueError(
                "Type-aware lowering currently requires explicit endpoint labels "
                "in MATCH ... WITH ... RETURN relationship sources."
            )
        if relationship.direction != "out":
            raise ValueError(
                "Type-aware lowering currently supports only outgoing one-hop "
                "MATCH ... WITH ... RETURN relationship sources."
            )

        edge_type = graph_schema.edge_type(relationship.type_name)
        left_type = graph_schema.node_type(source.left.label)
        right_type = graph_schema.node_type(source.right.label)
        if source.left.label != edge_type.source_type:
            raise ValueError(
                "Type-aware lowering currently requires the matched left node "
                "label to match the relationship source type in MATCH ... "
                "WITH ... RETURN."
            )
        if source.right.label != edge_type.target_type:
            raise ValueError(
                "Type-aware lowering currently requires the matched right node "
                "label to match the relationship target type in MATCH ... "
                "WITH ... RETURN."
            )

        relationship_alias = relationship.alias or "edge"
        where_parts: list[str] = []
        for field, value in source.left.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        source.left.alias,
                        left_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
            )
        for field, value in relationship.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_edge_field_expression(
                        relationship_alias,
                        edge_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
            )
        for field, value in source.right.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        source.right.alias,
                        right_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
            )

        for predicate in source.predicates:
            if predicate.alias == source.left.alias:
                where_parts.append(
                    _compile_type_aware_match_node_predicate(
                        source.left.alias,
                        left_type,
                        predicate,
                    )
                )
                continue
            if predicate.alias == source.right.alias:
                where_parts.append(
                    _compile_type_aware_match_node_predicate(
                        source.right.alias,
                        right_type,
                        predicate,
                    )
                )
                continue
            if predicate.alias == relationship_alias:
                where_parts.append(
                    _compile_type_aware_match_relationship_predicate(
                        relationship_alias,
                        edge_type,
                        predicate,
                    )
                )
                continue
            raise ValueError(
                "Type-aware lowering currently supports MATCH ... WITH ... "
                "RETURN relationship-source predicates only on the matched node "
                "and relationship aliases."
            )

        alias_map = {
            source.left.alias: (source.left.alias, left_type),
            source.right.alias: (source.right.alias, right_type),
            relationship_alias: (relationship_alias, edge_type),
        }
        output_alias_by_source_alias = {
            binding.source_alias: binding.output_alias
            for binding in statement.bindings
            if binding.binding_kind == "entity"
        }
        select_parts: list[str] = []
        binding_specs = {}
        for binding in statement.bindings:
            table_alias, entity_type = alias_map.get(binding.source_alias, (None, None))
            if table_alias is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware relationship source."
                )
            binding_specs[binding.output_alias] = _TypeAwareWithBindingSpec(
                binding=binding,
                entity_type=entity_type,
                start_binding_output_alias=(
                    output_alias_by_source_alias.get(source.left.alias)
                    if binding.source_alias == relationship_alias
                    else None
                ),
                end_binding_output_alias=(
                    output_alias_by_source_alias.get(source.right.alias)
                    if binding.source_alias == relationship_alias
                    else None
                ),
            )
            select_parts.extend(
                _compile_type_aware_with_binding_columns(
                    binding,
                    table_alias=table_alias,
                    entity_type=entity_type,
                )
            )

        return (
            _assemble_select_sql(
                select_sql=", ".join(select_parts),
                distinct=False,
                from_sql=f"FROM {edge_type.table_name} AS {relationship_alias}",
                joins=[
                    f"JOIN {left_type.table_name} AS {source.left.alias} "
                    f"ON {source.left.alias}.id = {relationship_alias}.from_id",
                    f"JOIN {right_type.table_name} AS {source.right.alias} "
                    f"ON {source.right.alias}.id = {relationship_alias}.to_id",
                ],
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            ),
            binding_specs,
        )

    raise ValueError(
        "Type-aware lowering currently supports MATCH ... WITH ... RETURN only "
        "for single-node and one-hop relationship sources."
    )


def _compile_type_aware_with_select_list(
    returns: tuple[WithReturnItem, ...],
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    select_parts: list[str] = []
    for item in returns:
        for expression, output_name in _compile_type_aware_with_select_expressions(
            item,
            binding_specs,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    return ", ".join(select_parts)


def _compile_type_aware_with_select_expressions(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> list[tuple[str, str]]:
    if item.kind in {"start_node", "end_node"} and item.field is not None:
        endpoint_kind: Literal["start", "end"] = (
            "start" if item.kind == "start_node" else "end"
        )
        return [
            (
                _compile_type_aware_with_endpoint_expression(
                    binding_specs,
                    item,
                    endpoint_kind=endpoint_kind,
                ),
                item.column_name,
            )
        ]

    if item.kind in {"entity", "properties", "start_node", "end_node"}:
        binding_spec = binding_specs[item.alias]
        binding = binding_spec.binding
        if item.kind in {"start_node", "end_node"} and item.field is None:
            if binding.alias_kind != "relationship":
                raise ValueError(
                    "Type-aware lowering currently supports startNode(...) and "
                    "endNode(...) only for relationship WITH bindings."
                )
            endpoint_alias = (
                binding_spec.start_binding_output_alias
                if item.kind == "start_node"
                else binding_spec.end_binding_output_alias
            )
            if endpoint_alias is None:
                raise ValueError(
                    "Type-aware lowering currently requires explicit rebound endpoint "
                    "node bindings to compile startNode(...) and endNode(...) after WITH."
                )
            endpoint_binding_spec = binding_specs[endpoint_alias]
            endpoint_binding = endpoint_binding_spec.binding
            assert endpoint_binding_spec.entity_type is not None
            output_name = item.column_name
            prefix = _with_entity_prefix(endpoint_binding.output_alias)
            expressions = [
                (f'with_q."{prefix}_id"', f"{output_name}.id"),
                (_sql_literal(endpoint_binding_spec.entity_type.name), f"{output_name}.label"),
            ]
            expressions.extend(
                (
                    f'with_q."{_type_aware_with_property_column(endpoint_binding.output_alias, property_field.name)}"',
                    f"{output_name}.{property_field.name}",
                )
                for property_field in endpoint_binding_spec.entity_type.properties
            )
            return expressions
        if binding.binding_kind == "entity":
            output_name = item.column_name
            prefix = _with_entity_prefix(binding.output_alias)
            if item.kind == "entity":
                expressions = [(f'with_q."{prefix}_id"', f"{output_name}.id")]
                if binding.alias_kind == "node":
                    assert binding_spec.entity_type is not None
                    expressions.append(
                        (_sql_literal(binding_spec.entity_type.name), f"{output_name}.label")
                    )
                elif binding.alias_kind == "relationship":
                    assert binding_spec.entity_type is not None
                    expressions.append(
                        (_sql_literal(binding_spec.entity_type.name), f"{output_name}.type")
                    )
                    expressions.append((f'with_q."{prefix}_from_id"', f"{output_name}.from_id"))
                    expressions.append((f'with_q."{prefix}_to_id"', f"{output_name}.to_id"))
                assert binding_spec.entity_type is not None
                expressions.extend(
                    (
                        f'with_q."{_type_aware_with_property_column(binding.output_alias, property_field.name)}"',
                        f"{output_name}.{property_field.name}",
                    )
                    for property_field in binding_spec.entity_type.properties
                )
                return expressions
            assert binding_spec.entity_type is not None
            if not binding_spec.entity_type.properties:
                raise ValueError(
                    "Type-aware relational output mode does not yet support "
                    "properties(...) for entity types without declared properties."
                )
            return [
                (
                    f'with_q."{_type_aware_with_property_column(binding.output_alias, property_field.name)}"',
                    f"{output_name}.{property_field.name}",
                )
                for property_field in binding_spec.entity_type.properties
            ]

    return [
        (
            _compile_type_aware_with_return_expression(
                item,
                binding_specs,
            ),
            item.column_name,
        )
    ]


def _compile_type_aware_with_return_expression(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    if item.kind == "case":
        assert isinstance(item.value, WithCaseSpec)
        return _compile_type_aware_with_case_return_expression(
            item.value,
            binding_specs,
        )

    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_type_aware_with_aggregate_return_expression(
            item,
            binding_specs,
        )

    if item.kind == "scalar_value":
        assert item.value is not None
        return _sql_value(item.value)

    scalar_expression = _compile_type_aware_with_scalar_return_expression(
        item,
        binding_specs,
    )
    if scalar_expression is not None:
        return scalar_expression

    binding_spec = binding_specs[item.alias]
    binding = binding_spec.binding
    if item.kind == "scalar":
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
    if item.kind == "field":
        if binding.binding_kind == "scalar":
            return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
        assert item.field is not None
        return _compile_type_aware_with_entity_field_expression(
            binding_spec,
            item.field,
        )
    if item.kind == "id":
        return f'with_q."{_with_entity_prefix(binding.output_alias)}_id"'
    if item.kind == "type":
        if binding.alias_kind != "relationship" or binding_spec.entity_type is None:
            raise ValueError(
                "Type-aware lowering currently supports type(...) only for "
                "relationship WITH bindings."
            )
        return _sql_literal(binding_spec.entity_type.name)
    _require_type_aware_relational_support(
        item.kind,
        field=item.field,
    )
    if item.kind == "start_node":
        return _compile_type_aware_with_endpoint_expression(
            binding_specs,
            item,
            endpoint_kind="start",
        )
    if item.kind == "end_node":
        return _compile_type_aware_with_endpoint_expression(
            binding_specs,
            item,
            endpoint_kind="end",
        )
    raise ValueError(
        "Type-aware lowering currently supports only scalar literals, scalar "
        "bindings, entity-field access, the first scalar-function slice, "
        "id(...), type(...), properties(...), labels(...), keys(...), "
        "startNode(...), endNode(...), and direct entity returns in "
        "MATCH ... WITH ... RETURN."
    )


def _compile_type_aware_with_scalar_return_expression(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str | None:
    if item.kind == "coalesce" and item.value is not None:
        inner = _compile_type_aware_with_binding_input_expression(
            item,
            binding_specs,
        )
        return f"COALESCE({inner}, {_sql_value(item.value)})"

    if item.kind == "replace":
        assert item.search_value is not None
        assert item.replace_value is not None
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return (
            f"REPLACE({inner}, {_sql_value(item.search_value)}, "
            f"{_sql_value(item.replace_value)})"
        )

    if item.kind in {"left", "right"}:
        assert item.length_value is not None
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"{item.kind.upper()}({inner}, {_sql_value(item.length_value)})"

    if item.kind == "split":
        assert item.delimiter_value is not None
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"SPLIT({inner}, {_sql_value(item.delimiter_value)})"

    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        if item.length_value is None:
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        return (
            f"SUBSTRING({inner}, ({start_sql} + 1), "
            f"{_sql_value(item.length_value)})"
        )

    if item.kind == "size":
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"LENGTH({inner})"

    if item.kind == "predicate":
        assert item.operator is not None
        if (
            item.field is not None
            and item.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX)
        ):
            inner_field = item.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
            if inner_field == "__value__":
                binding_spec = binding_specs[item.alias]
                binding = binding_spec.binding
                scalar_column = _with_scalar_prefix(binding.output_alias)
                expression = f'LENGTH(with_q."{scalar_column}")'
            else:
                expression = _compile_type_aware_with_return_expression(
                    WithReturnItem(
                        kind="size",
                        alias=item.alias,
                        field=inner_field,
                    ),
                    binding_specs,
                )
            return _compile_stream_predicate(
                expression,
                None,
                item.operator,
                item.value,
            )
        return _compile_type_aware_with_predicate(
            WithPredicate(
                kind="scalar" if item.field is None else "field",
                alias=item.alias,
                field=item.field,
                operator=item.operator,
                value=item.value,
            ),
            binding_specs,
        )

    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"{item.kind.upper()}({inner})"

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
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"{item.kind.upper()}({inner})"

    if item.kind == "to_string":
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"CAST({inner} AS TEXT)"

    if item.kind == "to_integer":
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"CAST({inner} AS INTEGER)"

    if item.kind == "to_float":
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"CAST({inner} AS REAL)"

    if item.kind == "to_boolean":
        inner = _compile_type_aware_with_function_input_expression(
            item,
            binding_specs,
        )
        return f"CAST({inner} AS BOOLEAN)"

    return None


def _compile_type_aware_with_function_input_expression(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    if item.value is not None:
        return _sql_value(item.value)

    binding_spec = binding_specs[item.alias]
    binding = binding_spec.binding
    if binding.binding_kind == "scalar" and item.field is None:
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'

    if item.field is None:
        raise ValueError(
            "Type-aware lowering currently expects an explicit field when "
            "applying scalar functions to entity WITH bindings."
        )

    return _compile_type_aware_with_entity_field_expression(binding_spec, item.field)


def _compile_type_aware_with_binding_input_expression(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    binding_spec = binding_specs[item.alias]
    binding = binding_spec.binding
    if binding.binding_kind == "scalar" and item.field is None:
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'

    if item.field is None:
        raise ValueError(
            "Type-aware lowering currently expects an explicit field when "
            "applying scalar functions to entity WITH bindings."
        )

    return _compile_type_aware_with_entity_field_expression(binding_spec, item.field)


def _compile_type_aware_with_order_by(
    order_by: tuple[WithOrderItem, ...],
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str | None:
    if not order_by:
        return None

    parts: list[str] = []
    for item in order_by:
        if item.kind == "aggregate":
            parts.append(f'"{item.alias}" {item.direction.upper()}')
            continue
        if item.kind in {"entity", "properties", "start_node", "end_node"}:
            parts.extend(
                f"{expression} {item.direction.upper()}"
                for expression, _ in _compile_type_aware_with_select_expressions(
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
                    binding_specs,
                )
            )
            continue
        expression = _compile_type_aware_with_return_expression(
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
            binding_specs,
        )
        parts.append(f"{expression} {item.direction.upper()}")
    return ", ".join(parts)


def _compile_type_aware_with_group_by(
    returns: tuple[WithReturnItem, ...],
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str | None:
    if not any(item.kind in _AGGREGATE_SQL_NAMES for item in returns):
        return None
    group_items: list[str] = []
    for item in returns:
        if item.kind in _AGGREGATE_SQL_NAMES:
            continue
        if item.kind in {"entity", "properties", "start_node", "end_node"}:
            group_items.extend(
                expression
                for expression, _ in _compile_type_aware_with_select_expressions(
                    item,
                    binding_specs,
                )
            )
            continue
        group_items.append(
            _compile_type_aware_with_return_expression(
                item,
                binding_specs,
            )
        )
    if not group_items:
        return None
    return ", ".join(group_items)


def _compile_type_aware_with_aggregate_return_expression(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        if item.alias == "*":
            return "COUNT(*)"
        binding_spec = binding_specs[item.alias]
        binding = binding_spec.binding
        if binding.binding_kind == "scalar":
            return f'COUNT(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        return f'COUNT(with_q."{_with_entity_prefix(binding.output_alias)}_id")'

    binding_spec = binding_specs[item.alias]
    binding = binding_spec.binding
    if binding.binding_kind == "scalar":
        if item.field is not None:
            raise ValueError(
                "Type-aware aggregate lowering does not accept field-qualified "
                f"scalar WITH bindings for {item.kind}(...)."
            )
        return f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}")'

    if item.field is None:
        raise ValueError(
            "Type-aware aggregate lowering currently expects an explicit entity "
            f"field for {item.kind}(... ) over WITH entity bindings."
        )

    inner = _compile_type_aware_with_entity_field_expression(binding_spec, item.field)
    return f"{function_name}({inner})"


def _compile_type_aware_with_case_return_expression(
    spec: WithCaseSpec,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    when_sql = " ".join(
        _compile_type_aware_with_case_arm(
            arm,
            binding_specs,
        )
        for arm in spec.when_items
    )
    else_sql = _compile_type_aware_with_return_expression(
        spec.else_item,
        binding_specs,
    )
    return f"CASE {when_sql} ELSE {else_sql} END"


def _compile_type_aware_with_case_arm(
    arm: WithCaseWhen,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    condition_sql = _compile_type_aware_with_return_expression(
        arm.condition,
        binding_specs,
    )
    result_sql = _compile_type_aware_with_return_expression(
        arm.result,
        binding_specs,
    )
    return f"WHEN {condition_sql} THEN {result_sql}"


def _compile_type_aware_with_predicates(
    predicates: tuple[WithPredicate, ...],
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> list[str]:
    if not predicates:
        return []

    disjuncts: dict[int, list[str]] = {}
    disjunct_order: list[int] = []
    for predicate in predicates:
        if predicate.disjunct_index not in disjuncts:
            disjuncts[predicate.disjunct_index] = []
            disjunct_order.append(predicate.disjunct_index)
        disjuncts[predicate.disjunct_index].append(
            _compile_type_aware_with_predicate(predicate, binding_specs)
        )

    if len(disjunct_order) == 1:
        return disjuncts[disjunct_order[0]]

    return [
        "(" + " OR ".join(
            "(" + " AND ".join(disjuncts[index]) + ")"
            for index in disjunct_order
        ) + ")"
    ]


def _compile_type_aware_with_predicate(
    predicate: WithPredicate,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str:
    binding_spec = binding_specs[predicate.alias]
    binding = binding_spec.binding
    if predicate.kind == "scalar":
        expression = f'with_q."{_with_scalar_prefix(binding.output_alias)}"'
        return _compile_stream_predicate(
            expression,
            None,
            predicate.operator,
            predicate.value,
        )

    assert predicate.field is not None
    return _compile_type_aware_with_field_predicate(
        binding_spec,
        predicate.field,
        predicate.operator,
        predicate.value,
    )


def _compile_type_aware_with_field_predicate(
    binding_spec: _TypeAwareWithBindingSpec,
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
    binding = binding_spec.binding
    if field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        if binding.binding_kind == "scalar":
            if inner_field != "__value__":
                raise ValueError(
                    "Type-aware lowering supports WITH size predicates on scalar "
                    "bindings only as size(scalar_alias)."
                )
            expression = f'LENGTH(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        else:
            expression = (
                "LENGTH("
                + _compile_type_aware_with_entity_field_expression(
                    binding_spec,
                    inner_field,
                )
                + ")"
            )
        return _compile_stream_predicate(expression, None, operator, value)

    expression = _compile_type_aware_with_entity_field_expression(binding_spec, field)
    return _compile_type_aware_predicate(
        field_expression=expression,
        operator=operator,
        value=value,
    )


def _compile_type_aware_with_entity_field_expression(
    binding_spec: _TypeAwareWithBindingSpec,
    field: str,
) -> str:
    binding = binding_spec.binding
    if binding.binding_kind == "scalar":
        return f'with_q."{_with_scalar_prefix(binding.output_alias)}"'

    prefix = _with_entity_prefix(binding.output_alias)
    if field == "id":
        return f'with_q."{prefix}_id"'
    if (
        binding.alias_kind == "node"
        and binding_spec.entity_type is not None
        and field == "label"
    ):
        return _sql_literal(binding_spec.entity_type.name)
    if (
        binding.alias_kind == "relationship"
        and binding_spec.entity_type is not None
        and field == "type"
    ):
        return _sql_literal(binding_spec.entity_type.name)
    return f'with_q."{_type_aware_with_property_column(binding.output_alias, field)}"'


def _compile_type_aware_with_endpoint_expression(
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
    item: WithReturnItem,
    *,
    endpoint_kind: Literal["start", "end"],
) -> str:
    binding_spec = binding_specs[item.alias]
    binding = binding_spec.binding
    if binding.alias_kind != "relationship":
        raise ValueError(
            "Type-aware lowering currently supports startNode(...) and "
            "endNode(...) only for relationship WITH bindings."
        )

    endpoint_alias = (
        binding_spec.start_binding_output_alias
        if endpoint_kind == "start"
        else binding_spec.end_binding_output_alias
    )
    if endpoint_alias is None:
        raise ValueError(
            "Type-aware lowering currently requires explicit rebound endpoint "
            "node bindings to compile startNode(...) and endNode(...) after WITH."
        )

    endpoint_binding_spec = binding_specs[endpoint_alias]
    if item.field is not None:
        return _compile_type_aware_with_entity_field_expression(
            endpoint_binding_spec,
            item.field,
        )
    _require_type_aware_relational_support(
        item.kind,
        field=item.field,
    )
    raise AssertionError("unreachable")
