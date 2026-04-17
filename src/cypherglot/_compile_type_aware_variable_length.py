from __future__ import annotations

from ._compile_sql_utils import _AGGREGATE_SQL_NAMES, _assemble_select_sql
from ._compile_type_aware_common import (
    _TypeAwareAliasSpec,
    _TypeAwareWithBindingSpec,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
    _compile_type_aware_with_binding_columns,
    _with_scalar_prefix,
)
from ._compile_type_aware_reads import (
    _compile_type_aware_chain_return_expression,
    _compile_type_aware_chain_select_expressions,
    _compile_type_aware_chain_source_components,
    _expand_type_aware_variable_length_relationship_branches,
    _supports_type_aware_zero_hop_variable_length_branch,
)
from ._normalize_support import OrderItem, ReturnItem
from .ir import GraphRelationalReadIR
from .normalize import NormalizedMatchChain, NormalizedMatchRelationship
from .schema import GraphSchema


def _supports_direct_variable_length_aggregate_return(
    returns: tuple[ReturnItem, ...],
) -> bool:
    return any(item.kind in _AGGREGATE_SQL_NAMES for item in returns) and all(
        item.kind in _AGGREGATE_SQL_NAMES
        or item.kind not in {"type", "start_node", "end_node"}
        for item in returns
    )


def _compile_variable_length_outer_projection(
    item: ReturnItem,
    index: int,
) -> str:
    if item.kind not in _AGGREGATE_SQL_NAMES:
        return f'variable_length_q."{item.column_name}" AS "{item.column_name}"'
    aggregate_column = _variable_length_aggregate_hidden_column(index)
    aggregate_sql = (
        "COUNT(*)"
        if item.kind == "count" and item.alias == "*"
        else f'{_AGGREGATE_SQL_NAMES[item.kind]}(variable_length_q."{aggregate_column}")'
    )
    return f'{aggregate_sql} AS "{item.column_name}"'


def _variable_length_aggregate_hidden_column(index: int) -> str:
    return f"__cg_aggregate_{index}"


def _projected_order_column_name(
    item: OrderItem,
    returns: tuple[ReturnItem, ...],
) -> str:
    for return_item in returns:
        if item.field == "__value__" and return_item.output_alias == item.alias:
            return return_item.column_name
        if (
            return_item.alias == item.alias
            and return_item.field == item.field
            and return_item.kind == "field"
        ):
            return return_item.column_name

    raise ValueError(
        "Unknown projected ORDER BY item for variable-length relationship read: "
        f"{item.alias}.{item.field}"
    )


def _compile_type_aware_zero_hop_variable_length_source_components(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
) -> tuple[str, list[str], list[str], dict[str, _TypeAwareAliasSpec]]:
    assert statement.left.label is not None
    node_type = graph_schema.node_type(statement.left.label)
    node_alias = "__cg_zero_hop_node"
    where_parts: list[str] = []

    for field, value in statement.left.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    node_alias,
                    node_type,
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
                    node_alias,
                    node_type,
                    field,
                ),
                operator="=",
                value=value,
            )
        )

    for predicate in statement.predicates:
        if predicate.alias not in {statement.left.alias, statement.right.alias}:
            raise ValueError(
                "Type-aware variable-length zero-hop lowering currently supports "
                "predicates only on the matched endpoint aliases."
            )
        where_parts.append(
            _compile_type_aware_match_node_predicate(
                node_alias,
                node_type,
                predicate,
            )
        )

    alias_specs = {
        statement.left.alias: _TypeAwareAliasSpec(
            table_alias=node_alias,
            alias_kind="node",
            entity_type=node_type,
        ),
        statement.right.alias: _TypeAwareAliasSpec(
            table_alias=node_alias,
            alias_kind="node",
            entity_type=node_type,
        ),
    }
    return f"FROM {node_type.table_name} AS {node_alias}", [], where_parts, alias_specs


def compile_type_aware_variable_length_match_relationship_sql(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
) -> str:
    if any(
        item.kind in {"type", "start_node", "end_node"}
        for item in statement.returns
    ):
        raise ValueError(
            "Type-aware variable-length relationship reads currently support "
            "node/entity/helper returns plus scalar field and aggregate returns, "
            "but not relationship-type or endpoint introspection returns."
        )
    if _supports_direct_variable_length_aggregate_return(statement.returns):
        return _compile_type_aware_variable_length_aggregate_match_relationship_sql(
            statement,
            graph_schema,
        )

    branches = _expand_type_aware_variable_length_relationship_branches(
        statement,
        graph_schema,
    )
    branch_sql: list[str] = []
    representative_alias_specs: dict[str, _TypeAwareAliasSpec] | None = None
    if _supports_type_aware_zero_hop_variable_length_branch(statement):
        _, _, _, representative_alias_specs = (
            _compile_type_aware_zero_hop_variable_length_source_components(
                statement,
                graph_schema,
            )
        )
        branch_sql.append(
            _compile_type_aware_zero_hop_variable_length_branch_sql(
                statement,
                graph_schema,
            )
        )
    branch_sql.extend(
        _compile_type_aware_variable_length_branch_sql(
            branch,
            graph_schema,
        )
        for branch in branches
    )
    if representative_alias_specs is None and branches:
        _, _, _, representative_alias_specs = _compile_type_aware_chain_source_components(
            nodes=branches[0].nodes,
            relationships=branches[0].relationships,
            predicates=branches[0].predicates,
            graph_schema=graph_schema,
        )
    order_sql = _compile_type_aware_variable_length_order_by(
        order_by=statement.order_by,
        returns=statement.returns,
        alias_specs=representative_alias_specs or {},
        table_alias="variable_length_q",
    )
    return _assemble_select_sql(
        select_sql="*",
        distinct=statement.distinct,
        from_sql=f"FROM ({' UNION ALL '.join(branch_sql)}) AS variable_length_q",
        joins=[],
        where_parts=[],
        group_sql=None,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_variable_length_branch_sql(
    branch: NormalizedMatchChain,
    graph_schema: GraphSchema,
) -> str:
    from_sql, joins, where_parts, alias_specs = _compile_type_aware_chain_source_components(
        nodes=branch.nodes,
        relationships=branch.relationships,
        predicates=branch.predicates,
        graph_schema=graph_schema,
    )
    select_sql = ", ".join(
        f'{expression} AS "{output_name}"'
        for item in branch.returns
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
        )
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )


def _compile_type_aware_zero_hop_variable_length_branch_sql(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
) -> str:
    from_sql, joins, where_parts, alias_specs = (
        _compile_type_aware_zero_hop_variable_length_source_components(
            statement,
            graph_schema,
        )
    )
    select_sql = ", ".join(
        f'{expression} AS "{output_name}"'
        for item in statement.returns
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
        )
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )


def _compile_type_aware_variable_length_aggregate_match_relationship_sql(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
) -> str:
    branch_sql: list[str] = []
    representative_alias_specs: dict[str, _TypeAwareAliasSpec] | None = None
    if _supports_type_aware_zero_hop_variable_length_branch(statement):
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_zero_hop_variable_length_source_components(
                statement,
                graph_schema,
            )
        )
        representative_alias_specs = alias_specs
        branch_sql.append(
            _assemble_select_sql(
                select_sql=_compile_type_aware_variable_length_aggregate_branch_select_list(
                    statement.returns,
                    alias_specs,
                ),
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )
    for branch in _expand_type_aware_variable_length_relationship_branches(
        statement,
        graph_schema,
        returns=(),
    ):
        from_sql, joins, where_parts, alias_specs = _compile_type_aware_chain_source_components(
            nodes=branch.nodes,
            relationships=branch.relationships,
            predicates=branch.predicates,
            graph_schema=graph_schema,
        )
        if representative_alias_specs is None:
            representative_alias_specs = alias_specs
        branch_sql.append(
            _assemble_select_sql(
                select_sql=_compile_type_aware_variable_length_aggregate_branch_select_list(
                    statement.returns,
                    alias_specs,
                ),
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )

    select_sql = ", ".join(
        _compile_type_aware_variable_length_outer_projections(
            statement.returns,
            representative_alias_specs or {},
        )
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM ({' UNION ALL '.join(branch_sql)}) AS variable_length_q",
        joins=[],
        where_parts=[],
        group_sql=_compile_type_aware_variable_length_outer_group_by(
            statement.returns,
            representative_alias_specs or {},
        ),
        order_sql=_compile_type_aware_variable_length_order_by(
            order_by=statement.order_by,
            returns=statement.returns,
            alias_specs=representative_alias_specs or {},
            table_alias=None,
        ),
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_variable_length_aggregate_branch_select_list(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    return ", ".join(
        _compile_type_aware_variable_length_branch_projection(
            item,
            index,
            alias_specs,
        )
        for index, item in enumerate(returns)
    )


def _compile_type_aware_variable_length_branch_projection(
    item: ReturnItem,
    index: int,
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    if item.kind not in _AGGREGATE_SQL_NAMES:
        return ", ".join(
            f'{expression} AS "{output_name}"'
            for expression, output_name in _compile_type_aware_chain_select_expressions(
                item,
                alias_specs,
            )
        )
    hidden_column = _variable_length_aggregate_hidden_column(index)
    if item.kind == "count":
        if item.alias == "*":
            return f'1 AS "{hidden_column}"'
        alias_spec = alias_specs.get(item.alias)
        if alias_spec is None:
            raise ValueError(
                f"Unknown aggregate alias {item.alias!r} for type-aware variable-length MATCH."
            )
        return f'{alias_spec.table_alias}.id AS "{hidden_column}"'
    return (
        f'{_compile_type_aware_chain_return_expression(ReturnItem(alias=item.alias, field=item.field, kind="field"), alias_specs)} '
        f'AS "{hidden_column}"'
    )


def _compile_type_aware_variable_length_outer_projections(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> list[str]:
    projections: list[str] = []
    for index, item in enumerate(returns):
        if item.kind in _AGGREGATE_SQL_NAMES:
            projections.append(_compile_variable_length_outer_projection(item, index))
            continue
        projections.extend(
            f'variable_length_q."{output_name}" AS "{output_name}"'
            for _, output_name in _compile_type_aware_chain_select_expressions(
                item,
                alias_specs,
            )
        )
    return projections


def _compile_type_aware_variable_length_outer_group_by(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str | None:
    group_items: list[str] = []
    for item in returns:
        if item.kind in _AGGREGATE_SQL_NAMES:
            continue
        group_items.extend(
            f'variable_length_q."{output_name}"'
            for _, output_name in _compile_type_aware_chain_select_expressions(
                item,
                alias_specs,
            )
        )
    if not group_items:
        return None
    return ", ".join(group_items)


def _compile_type_aware_variable_length_order_by(
    *,
    order_by: tuple[OrderItem, ...],
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
    table_alias: str | None = None,
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
                parts.extend(
                    f'variable_length_q."{output_name}" {item.direction.upper()}'
                    for _, output_name in _compile_type_aware_chain_select_expressions(
                        matched_return,
                        alias_specs,
                    )
                )
                continue

        projected_column = _projected_order_column_name(item, returns)
        qualified_column = (
            f'{table_alias}."{projected_column}"'
            if table_alias is not None
            else f'"{projected_column}"'
        )
        parts.append(f"{qualified_column} {item.direction.upper()}")
    return ", ".join(parts)


def compile_type_aware_variable_length_with_source_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
) -> tuple[str, dict[str, _TypeAwareWithBindingSpec]]:
    source = statement.source
    assert source is not None
    assert source.source_kind == "relationship"

    branch_sql: list[str] = []
    binding_specs: dict[str, _TypeAwareWithBindingSpec] = {}
    output_alias_by_source_alias = {
        binding.source_alias: binding.output_alias
        for binding in statement.bindings
        if binding.binding_kind == "entity"
    }

    if _supports_type_aware_zero_hop_variable_length_branch(source):
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_zero_hop_variable_length_source_components(
                source,
                graph_schema,
            )
        )
        select_parts: list[str] = []
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware variable-length source."
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

        branch_sql.append(
            _assemble_select_sql(
                select_sql=", ".join(select_parts),
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )

    for branch in _expand_type_aware_variable_length_relationship_branches(
        source,
        graph_schema,
        returns=(),
    ):
        from_sql, joins, where_parts, alias_specs = _compile_type_aware_chain_source_components(
            nodes=branch.nodes,
            relationships=branch.relationships,
            predicates=branch.predicates,
            graph_schema=graph_schema,
        )
        select_parts: list[str] = []
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware variable-length source."
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

        branch_sql.append(
            _assemble_select_sql(
                select_sql=", ".join(select_parts),
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )

    return " UNION ALL ".join(branch_sql), binding_specs