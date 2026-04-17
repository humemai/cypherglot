from __future__ import annotations

from ._compile_sql_utils import (
    _AGGREGATE_SQL_NAMES,
    _assemble_select_sql,
    _sql_literal,
)
from ._compile_type_aware_common import (
    _TypeAwareAliasSpec,
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_match_relationship_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
)
from ._compile_type_aware_read_projections import (
    _compile_type_aware_match_node_group_by,
    _compile_type_aware_match_node_select_expressions,
    _compile_type_aware_match_relationship_group_by,
    _compile_type_aware_match_relationship_select_expressions,
    _compile_type_aware_order_by,
    _compile_type_aware_relationship_order_by,
    _compile_type_aware_return_expression,
    _compile_type_aware_scalar_return_expression,
    _require_type_aware_relational_support,
)
from ._normalize_support import (
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
    ReturnItem,
)
from .ir import GraphRelationalReadIR
from .normalize import NormalizedMatchChain, NormalizedMatchRelationship
from .schema import GraphSchema


def _compile_type_aware_match_node_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> str:
    node = statement.node
    if node.label is None:
        raise ValueError(
            "Type-aware lowering currently requires an explicit node label in "
            "single-node MATCH reads."
        )

    node_type = graph_schema.node_type(node.label)
    alias = node.alias
    where_parts: list[str] = []

    for field, value in node.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    alias,
                    node_type,
                    field,
                ),
                operator="=",
                value=value,
            )
        )

    for predicate in statement.predicates:
        if predicate.alias != alias:
            raise ValueError(
                "Type-aware lowering currently supports only single-node "
                "predicates on the matched node alias."
            )
        where_parts.append(
            _compile_type_aware_match_node_predicate(alias, node_type, predicate)
        )

    select_parts: list[str] = []
    for item in statement.returns:
        for expression, output_name in _compile_type_aware_match_node_select_expressions(
            alias,
            node_type,
            item,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    select_sql = ", ".join(select_parts)
    order_sql = _compile_type_aware_order_by(
        alias,
        node_type,
        statement.order_by,
        statement.returns,
    )
    group_sql = _compile_type_aware_match_node_group_by(
        alias,
        node_type,
        statement.returns,
    )

    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM {node_type.table_name} AS {alias}",
        joins=[],
        where_parts=where_parts,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_match_relationship_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> str:
    if _is_variable_length_relationship(statement.relationship):
        from ._compile_type_aware_variable_length import (
            compile_type_aware_variable_length_match_relationship_sql,
        )

        return compile_type_aware_variable_length_match_relationship_sql(
            statement,
            graph_schema,
        )

    if (
        statement.left.label is None
        or statement.right.label is None
        or statement.relationship.type_name is None
    ):
        raise ValueError(
            "Type-aware lowering currently requires explicit endpoint labels and "
            "a relationship type for one-hop MATCH reads."
        )

    left_alias = statement.left.alias
    right_alias = statement.right.alias
    relationship_alias = statement.relationship.alias or "edge"
    left_type = graph_schema.node_type(statement.left.label)
    right_type = graph_schema.node_type(statement.right.label)
    edge_type = graph_schema.edge_type(statement.relationship.type_name)

    source_label = statement.left.label
    target_label = statement.right.label
    if statement.relationship.direction == "in":
        source_label, target_label = target_label, source_label
    if source_label != edge_type.source_type or target_label != edge_type.target_type:
        raise ValueError(
            "Type-aware lowering currently requires one-hop relationship endpoint "
            "labels to match the schema contract."
        )

    distinct_endpoints = _create_relationship_uses_distinct_nodes(
        statement.left,
        statement.right,
    )
    where_parts: list[str] = []

    for field, value in statement.left.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    left_alias,
                    left_type,
                    field,
                ),
                operator="=",
                value=value,
            )
        )
    for field, value in statement.relationship.properties:
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
    for field, value in statement.right.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    right_alias,
                    right_type,
                    field,
                ),
                operator="=",
                value=value,
            )
        )

    for predicate in statement.predicates:
        if predicate.alias == left_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    left_alias,
                    left_type,
                    predicate,
                )
            )
            continue
        if predicate.alias == right_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    right_alias,
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
            "Type-aware lowering currently supports only one-hop predicates on "
            "the matched node and relationship aliases."
        )

    if not distinct_endpoints:
        where_parts.append(f"{relationship_alias}.from_id = {relationship_alias}.to_id")

    select_parts: list[str] = []
    for item in statement.returns:
        for expression, output_name in _compile_type_aware_match_relationship_select_expressions(
            left_alias,
            left_type,
            relationship_alias,
            edge_type,
            right_alias,
            right_type,
            item,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    select_sql = ", ".join(select_parts)
    order_sql = _compile_type_aware_relationship_order_by(
        left_alias,
        left_type,
        relationship_alias,
        edge_type,
        right_alias,
        right_type,
        statement.order_by,
        statement.returns,
    )
    group_sql = _compile_type_aware_match_relationship_group_by(
        left_alias,
        left_type,
        relationship_alias,
        edge_type,
        right_alias,
        right_type,
        statement.returns,
    )

    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM {edge_type.table_name} AS {relationship_alias}",
        joins=(
            [
                f"JOIN {left_type.table_name} AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}.from_id",
                f"JOIN {right_type.table_name} AS {right_alias} "
                f"ON {right_alias}.id = {relationship_alias}.to_id",
            ]
            if distinct_endpoints
            else [
                f"JOIN {left_type.table_name} AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}.from_id"
            ]
        ),
        where_parts=where_parts,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_optional_match_node_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> str:
    node = statement.node
    if node.label is None:
        raise ValueError(
            "Type-aware lowering currently requires an explicit node label in "
            "single-node OPTIONAL MATCH reads."
        )

    node_type = graph_schema.node_type(node.label)
    alias = node.alias
    on_parts = ["1 = 1"]

    for field, value in node.properties:
        on_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    alias,
                    node_type,
                    field,
                ),
                operator="=",
                value=value,
            )
        )

    for predicate in statement.predicates:
        if predicate.alias != alias:
            raise ValueError(
                "Type-aware lowering currently supports only single-node "
                "predicates on the matched node alias in OPTIONAL MATCH."
            )
        on_parts.append(
            _compile_type_aware_match_node_predicate(alias, node_type, predicate)
        )

    select_parts: list[str] = []
    for item in statement.returns:
        for expression, output_name in _compile_type_aware_match_node_select_expressions(
            alias,
            node_type,
            item,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    select_sql = ", ".join(select_parts)
    order_sql = _compile_type_aware_order_by(
        alias,
        node_type,
        statement.order_by,
        statement.returns,
    )
    group_sql = _compile_type_aware_match_node_group_by(
        alias,
        node_type,
        statement.returns,
    )

    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql="FROM (SELECT 1 AS __cg_seed) AS seed",
        joins=[f"LEFT JOIN {node_type.table_name} AS {alias} ON {' AND '.join(on_parts)}"],
        where_parts=[],
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_match_chain_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> str:
    from_sql, joins, where_parts, alias_specs = _compile_type_aware_chain_source_components(
        nodes=statement.nodes,
        relationships=statement.relationships,
        predicates=statement.predicates,
        graph_schema=graph_schema,
    )
    select_parts: list[str] = []
    for item in statement.returns:
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    select_sql = ", ".join(select_parts)
    order_sql = _compile_type_aware_chain_order_by(
        statement.order_by,
        statement.returns,
        alias_specs,
    )
    group_sql = _compile_type_aware_chain_group_by(
        statement.returns,
        alias_specs,
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_chain_source_components(
    *,
    nodes: tuple[NodePattern, ...],
    relationships: tuple[RelationshipPattern, ...],
    predicates: tuple[Predicate, ...],
    graph_schema: GraphSchema,
) -> tuple[str, list[str], list[str], dict[str, _TypeAwareAliasSpec]]:
    edge_aliases = [
        relationship.alias or f"__cg_edge_{index}"
        for index, relationship in enumerate(relationships)
    ]
    joins: list[str] = []
    where_parts: list[str] = []
    alias_specs: dict[str, _TypeAwareAliasSpec] = {}

    for node in nodes:
        if node.label is None:
            raise ValueError(
                "Type-aware lowering currently requires explicit node labels in "
                "fixed-length multi-hop MATCH reads."
            )
        node_type = graph_schema.node_type(node.label)
        alias_specs[node.alias] = _TypeAwareAliasSpec(
            table_alias=node.alias,
            alias_kind="node",
            entity_type=node_type,
        )

    for index, relationship in enumerate(relationships):
        if _is_variable_length_relationship(relationship):
            raise ValueError(
                "Type-aware lowering does not support variable-length multi-hop "
                "relationship reads yet."
            )
        if relationship.type_name is None or "|" in relationship.type_name:
            raise ValueError(
                "Type-aware lowering currently requires exactly one relationship "
                "type per hop in fixed-length multi-hop MATCH reads."
            )
        if relationship.direction != "out":
            raise ValueError(
                "Type-aware lowering currently supports only outgoing fixed-length "
                "multi-hop MATCH reads."
            )

        edge_alias = edge_aliases[index]
        left_node = nodes[index]
        right_node = nodes[index + 1]
        left_type = graph_schema.node_type(left_node.label or "")
        right_type = graph_schema.node_type(right_node.label or "")
        edge_type = graph_schema.edge_type(relationship.type_name)

        if left_type.name != edge_type.source_type:
            raise ValueError(
                "Type-aware lowering currently requires each hop's left node "
                "label to match the relationship source type."
            )
        if right_type.name != edge_type.target_type:
            raise ValueError(
                "Type-aware lowering currently requires each hop's right node "
                "label to match the relationship target type."
            )

        if index == 0:
            joins.append(
                f"JOIN {left_type.table_name} AS {left_node.alias} "
                f"ON {left_node.alias}.id = {edge_alias}.from_id"
            )
        else:
            joins.append(
                f"JOIN {edge_type.table_name} AS {edge_alias} "
                f"ON {left_node.alias}.id = {edge_alias}.from_id"
            )
        joins.append(
            f"JOIN {right_type.table_name} AS {right_node.alias} "
            f"ON {right_node.alias}.id = {edge_alias}.to_id"
        )

        for field, value in relationship.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_edge_field_expression(
                        edge_alias,
                        edge_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
            )

        if relationship.alias is not None:
            alias_specs[relationship.alias] = _TypeAwareAliasSpec(
                table_alias=edge_alias,
                alias_kind="relationship",
                entity_type=edge_type,
                start_node_alias=left_node.alias,
                end_node_alias=right_node.alias,
            )

    for node in nodes:
        node_type = graph_schema.node_type(node.label or "")
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

    for predicate in predicates:
        alias_spec = alias_specs.get(predicate.alias)
        if alias_spec is None:
            raise ValueError(
                "Type-aware lowering currently supports fixed-length multi-hop "
                "predicates only on matched node and relationship aliases."
            )
        if alias_spec.alias_kind == "node":
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    alias_spec.table_alias,
                    alias_spec.entity_type,
                    predicate,
                )
            )
            continue
        where_parts.append(
            _compile_type_aware_match_relationship_predicate(
                alias_spec.table_alias,
                alias_spec.entity_type,
                predicate,
            )
        )

    first_edge_type = graph_schema.edge_type(relationships[0].type_name or "")
    return (
        f"FROM {first_edge_type.table_name} AS {edge_aliases[0]}",
        joins,
        where_parts,
        alias_specs,
    )


def _compile_type_aware_chain_select_expression(
    item: ReturnItem,
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    if item.kind in _AGGREGATE_SQL_NAMES:
        return _compile_type_aware_chain_aggregate_return_expression(item, alias_specs)
    return _compile_type_aware_chain_return_expression(
        item,
        alias_specs,
    )


def _compile_type_aware_chain_select_expressions(
    item: ReturnItem,
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> list[tuple[str, str]]:
    if item.kind in _AGGREGATE_SQL_NAMES:
        return [
            (
                _compile_type_aware_chain_aggregate_return_expression(
                    item,
                    alias_specs,
                ),
                item.column_name,
            )
        ]
    alias_spec = alias_specs.get(item.alias)
    if alias_spec is None:
        raise ValueError(
            f"Unknown return alias {item.alias!r} for fixed-length multi-hop MATCH."
        )
    if item.kind in {"start_node", "end_node"} and item.field is None:
        endpoint_alias = (
            alias_spec.start_node_alias
            if item.kind == "start_node"
            else alias_spec.end_node_alias
        )
        if endpoint_alias is None:
            raise ValueError(
                "Type-aware fixed-length multi-hop MATCH lowering requires "
                "endpoint node aliases for relational endpoint expansion."
            )
        endpoint_spec = alias_specs[endpoint_alias]
        output_name = item.column_name
        expressions = [
            (f"{endpoint_spec.table_alias}.id", f"{output_name}.id"),
            (
                _sql_literal(endpoint_spec.entity_type.name),
                f"{output_name}.label",
            ),
        ]
        expressions.extend(
            (
                f"{endpoint_spec.table_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in endpoint_spec.entity_type.properties
        )
        return expressions
    if alias_spec.alias_kind == "node" and item.kind == "entity":
        output_name = item.column_name
        expressions = [
            (f"{alias_spec.table_alias}.id", f"{output_name}.id"),
            (_sql_literal(alias_spec.entity_type.name), f"{output_name}.label"),
        ]
        expressions.extend(
            (
                f"{alias_spec.table_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in alias_spec.entity_type.properties
        )
        return expressions
    if alias_spec.alias_kind == "node" and item.kind == "properties":
        if not alias_spec.entity_type.properties:
            raise ValueError(
                "Type-aware relational output mode does not yet support "
                "properties(...) for entity types without declared properties."
            )
        output_name = item.column_name
        return [
            (
                f"{alias_spec.table_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in alias_spec.entity_type.properties
        ]
    if alias_spec.alias_kind == "relationship" and item.kind == "entity":
        output_name = item.column_name
        expressions = [
            (f"{alias_spec.table_alias}.id", f"{output_name}.id"),
            (_sql_literal(alias_spec.entity_type.name), f"{output_name}.type"),
            (f"{alias_spec.table_alias}.from_id", f"{output_name}.from_id"),
            (f"{alias_spec.table_alias}.to_id", f"{output_name}.to_id"),
        ]
        expressions.extend(
            (
                f"{alias_spec.table_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in alias_spec.entity_type.properties
        )
        return expressions
    if alias_spec.alias_kind == "relationship" and item.kind == "properties":
        if not alias_spec.entity_type.properties:
            raise ValueError(
                "Type-aware relational output mode does not yet support "
                "properties(...) for entity types without declared properties."
            )
        output_name = item.column_name
        return [
            (
                f"{alias_spec.table_alias}.{property_field.column_name}",
                f"{output_name}.{property_field.name}",
            )
            for property_field in alias_spec.entity_type.properties
        ]

    return [
        (
            _compile_type_aware_chain_select_expression(
                item,
                alias_specs,
            ),
            item.column_name,
        )
    ]


def _compile_type_aware_chain_return_expression(
    item: ReturnItem,
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    alias_spec = alias_specs.get(item.alias)
    if alias_spec is None:
        raise ValueError(
            f"Unknown return alias {item.alias!r} for fixed-length multi-hop MATCH."
        )

    scalar_expression = _compile_type_aware_scalar_return_expression(
        item,
        field_expression_resolver=(
            lambda field: _compile_type_aware_alias_field_expression(alias_spec, field)
        ),
    )
    if scalar_expression is not None:
        return scalar_expression

    if alias_spec.alias_kind == "node":
        return _compile_type_aware_return_expression(
            alias_spec.table_alias,
            alias_spec.entity_type,
            item,
        )

    if item.kind in {"start_node", "end_node"}:
        endpoint_alias = (
            alias_spec.start_node_alias
            if item.kind == "start_node"
            else alias_spec.end_node_alias
        )
        if endpoint_alias is None:
            raise ValueError(
                "Type-aware fixed-length multi-hop MATCH lowering requires "
                "endpoint node aliases for hop introspection returns."
            )
        endpoint_spec = alias_specs[endpoint_alias]
        return _compile_type_aware_return_expression(
            endpoint_spec.table_alias,
            endpoint_spec.entity_type,
            ReturnItem(
                alias=endpoint_alias,
                field=item.field,
                kind="field" if item.field is not None else "entity",
            ),
        )

    _require_type_aware_relational_support(
        item.kind,
        field=item.field,
    )
    if item.kind == "type":
        return _sql_literal(alias_spec.entity_type.name)
    raise ValueError(
        "Type-aware fixed-length multi-hop MATCH lowering currently supports "
        "field, scalar, id, aggregate, and the first direct entity/introspection "
        "slices over matched aliases."
    )


def _compile_type_aware_chain_aggregate_return_expression(
    item: ReturnItem,
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    function_name = _AGGREGATE_SQL_NAMES[item.kind]
    if item.kind == "count":
        if item.alias == "*":
            return "COUNT(*)"
        alias_spec = alias_specs.get(item.alias)
        if alias_spec is None:
            raise ValueError(
                "Unknown aggregate alias "
                f"{item.alias!r} for fixed-length multi-hop MATCH."
            )
        return f"{function_name}({alias_spec.table_alias}.id)"
    if item.field is None:
        raise ValueError(
            "Type-aware fixed-length multi-hop aggregate lowering currently "
            "expects an explicit field for non-count aggregates."
        )
    alias_spec = alias_specs.get(item.alias)
    if alias_spec is None:
        raise ValueError(
            f"Unknown aggregate alias {item.alias!r} for fixed-length multi-hop MATCH."
        )
    inner = _compile_type_aware_alias_field_expression(alias_spec, item.field)
    return f"{function_name}({inner})"


def _compile_type_aware_alias_field_expression(
    alias_spec: _TypeAwareAliasSpec,
    field: str,
) -> str:
    if alias_spec.alias_kind == "node":
        return _compile_type_aware_node_field_expression(
            alias_spec.table_alias,
            alias_spec.entity_type,
            field,
        )
    return _compile_type_aware_edge_field_expression(
        alias_spec.table_alias,
        alias_spec.entity_type,
        field,
    )


def _compile_type_aware_chain_order_by(
    order_by: tuple[OrderItem, ...],
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
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
                        f'"{matched_return.column_name}" {item.direction.upper()}'
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
                        for expression, _ in _compile_type_aware_chain_select_expressions(
                            matched_return,
                            alias_specs,
                        )
                    )
                    continue
                parts.append(
                    f"{_compile_type_aware_chain_return_expression(
                        matched_return,
                        alias_specs,
                    )} "
                    f"{item.direction.upper()}"
                )
                continue

        alias_spec = alias_specs.get(item.alias)
        if alias_spec is None:
            raise ValueError(
                "Unknown ORDER BY alias "
                f"{item.alias!r} for fixed-length multi-hop MATCH."
            )
        expression = _compile_type_aware_alias_field_expression(alias_spec, item.field)
        parts.append(f"{expression} {item.direction.upper()}")
    return ", ".join(parts)


def _compile_type_aware_chain_group_by(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
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
                for expression, _ in _compile_type_aware_chain_select_expressions(
                    item,
                    alias_specs,
                )
            )
            continue
        group_items.append(
            _compile_type_aware_chain_return_expression(
                item,
                alias_specs,
            )
        )
    if not group_items:
        return None
    return ", ".join(group_items)


def _expand_type_aware_variable_length_relationship_branches(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
    *,
    returns: tuple[ReturnItem, ...] | None = None,
) -> tuple[NormalizedMatchChain, ...]:
    relationship = statement.relationship
    if relationship.type_name is None or "|" in relationship.type_name:
        raise ValueError(
            "Type-aware variable-length lowering currently requires explicit "
            "endpoint labels."
        )
    if relationship.direction != "out":
        raise ValueError(
            "Type-aware variable-length lowering currently supports only "
            "outgoing paths."
        )

    edge_type = graph_schema.edge_type(relationship.type_name)
    if statement.left.label != edge_type.source_type:
        raise ValueError(
            "Type-aware variable-length lowering currently requires the left "
            "node label to match the relationship source type."
        )
    if statement.right.label != edge_type.target_type:
        raise ValueError(
            "Type-aware variable-length lowering currently requires the right "
            "node label to match the relationship target type."
        )
    if relationship.max_hops is None:
        raise ValueError(
            "Type-aware variable-length lowering requires a finite max_hops."
        )
    if relationship.min_hops < 0:
        raise ValueError(
            "Type-aware variable-length lowering requires min_hops >= 0."
        )
    if relationship.max_hops > 1 and edge_type.source_type != edge_type.target_type:
        raise ValueError(
            "Type-aware variable-length lowering currently requires repeated "
            "relationship hops to connect the same node type."
        )

    branches: list[NormalizedMatchChain] = []
    base_relationship = RelationshipPattern(
        alias=None,
        type_name=relationship.type_name,
        direction=relationship.direction,
        properties=relationship.properties,
    )
    branch_returns = statement.returns if returns is None else returns

    for hop_count in range(max(1, relationship.min_hops), relationship.max_hops + 1):
        nodes = [statement.left]
        for index in range(1, hop_count):
            nodes.append(
                NodePattern(
                    alias=f"__cg_variable_hop_{hop_count}_node_{index}",
                    label=edge_type.target_type,
                )
            )
        nodes.append(statement.right)
        branches.append(
            NormalizedMatchChain(
                kind="match",
                pattern_kind="relationship_chain",
                nodes=tuple(nodes),
                relationships=tuple(base_relationship for _ in range(hop_count)),
                predicates=statement.predicates,
                returns=branch_returns,
                distinct=statement.distinct,
                order_by=statement.order_by,
                limit=statement.limit,
                skip=statement.skip,
            )
        )

    return tuple(branches)


def _supports_type_aware_zero_hop_variable_length_branch(
    statement: NormalizedMatchRelationship,
) -> bool:
    return (
        statement.relationship.min_hops == 0
        and statement.left.label is not None
        and statement.left.label == statement.right.label
    )


def _is_variable_length_relationship(relationship: RelationshipPattern) -> bool:
    return relationship.max_hops != 1 or relationship.min_hops != 1


def _create_relationship_uses_distinct_nodes(
    left: NodePattern,
    right: NodePattern,
) -> bool:
    return left.alias != right.alias or left.label != right.label