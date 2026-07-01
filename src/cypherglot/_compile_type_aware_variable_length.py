from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterator

from ._compile_sql_utils import (
    _AGGREGATE_SQL_NAMES,
    _assemble_select_sql,
    _group_disjunct_predicates,
)
from ._compile_type_aware_common import (
    _TypeAwareAliasSpec,
    _TypeAwareWithBindingSpec,
    _build_type_aware_with_binding_spec,
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
    _is_type_aware_entity_field_numeric,
)
from ._compile_type_aware_read_projections import _is_type_aware_constant_projection
from ._compile_type_aware_reads import (
    _compile_type_aware_chain_group_by,
    _compile_type_aware_chain_order_by,
    _compile_type_aware_chain_return_expression,
    _compile_type_aware_chain_select_expressions,
    _compile_type_aware_chain_source_components,
    _expand_type_aware_variable_length_relationship_branches,
    _supports_type_aware_zero_hop_variable_length_branch,
)
from ._normalize_support import OrderItem, Predicate, ReturnItem
from .ir import GraphRelationalReadIR, SQLBackend
from .normalize import NormalizedMatchChain, NormalizedMatchRelationship, WithBinding
from .schema import GraphSchema


# Selects how ``(a)-[:E*lo..hi]->(b)`` is lowered. ``"unroll"`` (the default)
# expands one fixed-length chain per hop count and ``UNION ALL``s them.
# ``"recursive_cte"`` emits a single ``WITH RECURSIVE`` traversal. Both lowerings
# realise the same admitted *walk* semantics (nodes/edges may repeat); they are
# two encodings of one result multiset, toggled for paper ablations.
_VARIABLE_LENGTH_STRATEGY: contextvars.ContextVar[str] = contextvars.ContextVar(
    "cypherglot_variable_length_strategy",
    default="unroll",
)


@contextlib.contextmanager
def variable_length_strategy_scope(strategy: str) -> Iterator[None]:
    """Bind the active variable-length lowering strategy for the enclosed call."""
    if strategy not in {"unroll", "recursive_cte"}:
        raise ValueError(
            "variable_length_strategy must be 'unroll' or 'recursive_cte', "
            f"got {strategy!r}."
        )
    token = _VARIABLE_LENGTH_STRATEGY.set(strategy)
    try:
        yield
    finally:
        _VARIABLE_LENGTH_STRATEGY.reset(token)


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
    alias_specs: dict[str, _TypeAwareAliasSpec],
    backend: SQLBackend,
) -> str:
    if item.kind not in _AGGREGATE_SQL_NAMES:
        return f'variable_length_q."{item.column_name}" AS "{item.column_name}"'
    aggregate_column = _variable_length_aggregate_hidden_column(index)
    if item.kind == "count" and item.alias == "*":
        aggregate_sql = "COUNT(*)"
    else:
        from ._compile_type_aware_read_projections import (
            _compile_type_aware_aggregate_expression,
        )

        alias_spec = alias_specs.get(item.alias) if item.field is not None else None

        aggregate_sql = _compile_type_aware_aggregate_expression(
            item.kind,
            f'variable_length_q."{aggregate_column}"',
            backend,
            cast_operand=not (
                alias_spec is not None
                and item.field is not None
                and _is_type_aware_entity_field_numeric(
                    alias_spec.entity_type,
                    item.field,
                )
            ),
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
    backend: SQLBackend,
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
                backend=backend,
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
                backend=backend,
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
                backend=backend,
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
    backend: SQLBackend,
) -> str:
    if _VARIABLE_LENGTH_STRATEGY.get() == "recursive_cte":
        return _compile_type_aware_variable_length_recursive_cte_sql(
            statement,
            graph_schema,
            backend=backend,
        )
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
            backend=backend,
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
                backend=backend,
            )
        )
        branch_sql.append(
            _compile_type_aware_zero_hop_variable_length_branch_sql(
                statement,
                graph_schema,
                backend=backend,
            )
        )
    branch_sql.extend(
        _compile_type_aware_variable_length_branch_sql(
            branch,
            graph_schema,
            backend=backend,
        )
        for branch in branches
    )
    if representative_alias_specs is None and branches:
        _, _, _, representative_alias_specs = (
            _compile_type_aware_chain_source_components(
                nodes=branches[0].nodes,
                relationships=branches[0].relationships,
                predicates=branches[0].predicates,
                graph_schema=graph_schema,
                backend=backend,
            )
        )
    order_sql = _compile_type_aware_variable_length_order_by(
        order_by=statement.order_by,
        returns=statement.returns,
        alias_specs=representative_alias_specs or {},
        table_alias="variable_length_q",
        backend=backend,
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
    backend: SQLBackend,
) -> str:
    from_sql, joins, where_parts, alias_specs = (
        _compile_type_aware_chain_source_components(
            nodes=branch.nodes,
            relationships=branch.relationships,
            predicates=branch.predicates,
            graph_schema=graph_schema,
            backend=backend,
        )
    )
    select_sql = ", ".join(
        f'{expression} AS "{output_name}"'
        for item in branch.returns
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
            backend=backend,
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
    backend: SQLBackend,
) -> str:
    from_sql, joins, where_parts, alias_specs = (
        _compile_type_aware_zero_hop_variable_length_source_components(
            statement,
            graph_schema,
            backend=backend,
        )
    )
    select_sql = ", ".join(
        f'{expression} AS "{output_name}"'
        for item in statement.returns
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
            backend=backend,
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
    backend: SQLBackend,
) -> str:
    branch_sql: list[str] = []
    representative_alias_specs: dict[str, _TypeAwareAliasSpec] | None = None
    if _supports_type_aware_zero_hop_variable_length_branch(statement):
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_zero_hop_variable_length_source_components(
                statement,
                graph_schema,
                backend=backend,
            )
        )
        representative_alias_specs = alias_specs
        branch_sql.append(
            _assemble_select_sql(
                select_sql=(
                    _compile_type_aware_variable_length_aggregate_branch_select_list(
                        statement.returns,
                        alias_specs,
                        backend=backend,
                    )
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
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_chain_source_components(
                nodes=branch.nodes,
                relationships=branch.relationships,
                predicates=branch.predicates,
                graph_schema=graph_schema,
                backend=backend,
            )
        )
        if representative_alias_specs is None:
            representative_alias_specs = alias_specs
        branch_sql.append(
            _assemble_select_sql(
                select_sql=(
                    _compile_type_aware_variable_length_aggregate_branch_select_list(
                        statement.returns,
                        alias_specs,
                        backend=backend,
                    )
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
            backend=backend,
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
            backend=backend,
        ),
        order_sql=_compile_type_aware_variable_length_order_by(
            order_by=statement.order_by,
            returns=statement.returns,
            alias_specs=representative_alias_specs or {},
            table_alias=None,
            backend=backend,
        ),
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_type_aware_variable_length_aggregate_branch_select_list(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
    backend: SQLBackend,
) -> str:
    return ", ".join(
        _compile_type_aware_variable_length_branch_projection(
            item,
            index,
            alias_specs,
            backend=backend,
        )
        for index, item in enumerate(returns)
    )


def _compile_type_aware_variable_length_branch_projection(
    item: ReturnItem,
    index: int,
    alias_specs: dict[str, _TypeAwareAliasSpec],
    backend: SQLBackend,
) -> str:
    if item.kind not in _AGGREGATE_SQL_NAMES:
        return ", ".join(
            f'{expression} AS "{output_name}"'
            for expression, output_name in _compile_type_aware_chain_select_expressions(
                item,
                alias_specs,
                backend=backend,
            )
        )
    hidden_column = _variable_length_aggregate_hidden_column(index)
    if item.kind == "count":
        if item.alias == "*":
            return f'1 AS "{hidden_column}"'
        alias_spec = alias_specs.get(item.alias)
        if alias_spec is None:
            raise ValueError(
                "Unknown aggregate alias "
                f"{item.alias!r} for type-aware variable-length MATCH."
            )
        return f'{alias_spec.table_alias}.id AS "{hidden_column}"'
    field_expression = _compile_type_aware_chain_return_expression(
        ReturnItem(alias=item.alias, field=item.field, kind="field"),
        alias_specs,
        backend=backend,
    )
    return f'{field_expression} AS "{hidden_column}"'


def _compile_type_aware_variable_length_outer_projections(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
    backend: SQLBackend,
) -> list[str]:
    projections: list[str] = []
    for index, item in enumerate(returns):
        if item.kind in _AGGREGATE_SQL_NAMES:
            projections.append(
                _compile_variable_length_outer_projection(
                    item,
                    index,
                    alias_specs,
                    backend=backend,
                )
            )
            continue
        projections.extend(
            f'variable_length_q."{output_name}" AS "{output_name}"'
            for _, output_name in _compile_type_aware_chain_select_expressions(
                item,
                alias_specs,
                backend=backend,
            )
        )
    return projections


def _compile_type_aware_variable_length_outer_group_by(
    returns: tuple[ReturnItem, ...],
    alias_specs: dict[str, _TypeAwareAliasSpec],
    backend: SQLBackend,
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
                backend=backend,
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
    backend: SQLBackend,
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
                if _is_type_aware_constant_projection(matched_return):
                    continue
                parts.extend(
                    f'variable_length_q."{output_name}" {item.direction.upper()}'
                    for _, output_name in _compile_type_aware_chain_select_expressions(
                        matched_return,
                        alias_specs,
                        backend=backend,
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
    return ", ".join(parts) or None


def compile_type_aware_variable_length_with_source_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> tuple[str, dict[str, _TypeAwareWithBindingSpec]]:
    from ._compile_type_aware_with_source import (
        _compile_type_aware_source_binding_columns,
    )

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
                backend=backend,
            )
        )
        select_parts: list[str] = []
        source_binding_specs = {
            alias: _build_type_aware_with_binding_spec(
                binding=WithBinding(
                    source_alias=alias,
                    output_alias=alias,
                    binding_kind="entity",
                    alias_kind=alias_spec.alias_kind,
                ),
                entity_type=alias_spec.entity_type,
                start_binding_output_alias=alias_spec.start_node_alias,
                end_binding_output_alias=alias_spec.end_node_alias,
            )
            for alias, alias_spec in alias_specs.items()
        }
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware variable-length source."
                )
            binding_columns, scalar_logical_type = (
                _compile_type_aware_source_binding_columns(
                    binding,
                    table_alias=alias_spec.table_alias,
                    entity_type=alias_spec.entity_type,
                    source_alias_specs=alias_specs,
                    source_binding_specs=source_binding_specs,
                    backend=backend,
                )
            )
            binding_specs[binding.output_alias] = _build_type_aware_with_binding_spec(
                binding=binding,
                entity_type=alias_spec.entity_type,
                scalar_logical_type=scalar_logical_type,
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
            select_parts.extend(binding_columns)

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
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_chain_source_components(
                nodes=branch.nodes,
                relationships=branch.relationships,
                predicates=branch.predicates,
                graph_schema=graph_schema,
                backend=backend,
            )
        )
        select_parts: list[str] = []
        source_binding_specs = {
            alias: _build_type_aware_with_binding_spec(
                binding=WithBinding(
                    source_alias=alias,
                    output_alias=alias,
                    binding_kind="entity",
                    alias_kind=alias_spec.alias_kind,
                ),
                entity_type=alias_spec.entity_type,
                start_binding_output_alias=alias_spec.start_node_alias,
                end_binding_output_alias=alias_spec.end_node_alias,
            )
            for alias, alias_spec in alias_specs.items()
        }
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware variable-length source."
                )
            binding_columns, scalar_logical_type = (
                _compile_type_aware_source_binding_columns(
                    binding,
                    table_alias=alias_spec.table_alias,
                    entity_type=alias_spec.entity_type,
                    source_alias_specs=alias_specs,
                    source_binding_specs=source_binding_specs,
                    backend=backend,
                )
            )
            binding_specs[binding.output_alias] = _build_type_aware_with_binding_spec(
                binding=binding,
                entity_type=alias_spec.entity_type,
                scalar_logical_type=scalar_logical_type,
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
            select_parts.extend(binding_columns)

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


def _compile_type_aware_variable_length_recursive_cte_sql(
    statement: NormalizedMatchRelationship,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    """Lower ``(a)-[:E*lo..hi]->(b)`` to a single ``WITH RECURSIVE`` traversal.

    The generated CTE carries only ``(end_id, depth)`` for each *walk* (nodes and
    edges may repeat). Seeding at ``depth = 0`` with the start node(s) and
    extending by one edge per recursion step reproduces exactly the walk multiset
    that the branch-unroll strategy enumerates via one fixed-length chain per hop
    count; the final ``UNION ALL`` (not ``UNION``) preserves multiplicity so a
    node reachable by two length-2 walks appears twice.
    """
    relationship = statement.relationship
    if relationship.type_name is None or "|" in relationship.type_name:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires exactly one "
            "relationship type."
        )
    if relationship.direction != "out":
        raise NotImplementedError(
            "recursive_cte variable-length lowering currently supports only "
            "outgoing paths."
        )
    if relationship.max_hops is None:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires a finite max_hops."
        )
    if relationship.min_hops < 0:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires min_hops >= 0."
        )
    if any(
        item.kind in {"type", "start_node", "end_node"}
        for item in statement.returns
    ):
        raise NotImplementedError(
            "recursive_cte variable-length lowering does not support "
            "relationship-type or endpoint-introspection returns."
        )
    if statement.left.label is None or statement.right.label is None:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires explicit endpoint "
            "labels."
        )

    edge_type = graph_schema.edge_type(relationship.type_name)
    if statement.left.label != edge_type.source_type:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires the left node label "
            "to match the relationship source type."
        )
    if statement.right.label != edge_type.target_type:
        raise NotImplementedError(
            "recursive_cte variable-length lowering requires the right node label "
            "to match the relationship target type."
        )
    if edge_type.source_type != edge_type.target_type:
        raise NotImplementedError(
            "recursive_cte variable-length lowering currently requires the "
            "relationship to connect a single node type."
        )

    left_alias = statement.left.alias
    right_alias = statement.right.alias

    # The CTE only carries the walk endpoint, so the start node is not
    # projectable. Any return/order that reaches back to the start alias (when it
    # is distinct from the end alias) is out of scope for this lowering.
    if left_alias != right_alias:
        for item in statement.returns:
            if item.kind != "count" and item.alias == left_alias:
                raise NotImplementedError(
                    "recursive_cte variable-length lowering cannot project the "
                    "start-node alias; only the reached (right) alias is "
                    "available."
                )
        for order_item in statement.order_by:
            if order_item.field != "__value__" and order_item.alias == left_alias:
                raise NotImplementedError(
                    "recursive_cte variable-length lowering cannot ORDER BY the "
                    "start-node alias."
                )

    # Predicates may only constrain the endpoint aliases; splitting a single OR
    # disjunct across the base (start) term and the outer (end) filter would
    # change its meaning, so reject that shape rather than mis-lower it.
    left_predicates: list[Predicate] = []
    right_predicates: list[Predicate] = []
    for predicate in statement.predicates:
        if predicate.alias == left_alias:
            left_predicates.append(predicate)
        elif predicate.alias == right_alias:
            right_predicates.append(predicate)
        else:
            raise NotImplementedError(
                "recursive_cte variable-length lowering supports predicates only "
                "on the matched endpoint aliases."
            )
    if left_alias != right_alias:
        shared_disjuncts = {p.disjunct_index for p in left_predicates} & {
            p.disjunct_index for p in right_predicates
        }
        if shared_disjuncts:
            raise NotImplementedError(
                "recursive_cte variable-length lowering cannot split an OR "
                "predicate across the start and end aliases."
            )

    source_type = graph_schema.node_type(statement.left.label)
    lo = relationship.min_hops
    hi = relationship.max_hops

    cte_name = "__cg_vl"
    recur_alias = "__cg_vl_r"
    start_alias = "__cg_vl_start"
    edge_alias = "__cg_vl_e"
    end_alias = right_alias

    # --- base term: start node(s) at depth 0 -------------------------------
    # For a single-source pattern the start predicates/properties select one (or
    # a few) start rows; for the all-pairs pattern (no start filter) this
    # enumerates every start node, exactly like the branch-unroll zero-hop base.
    base_where = _compile_type_aware_variable_length_endpoint_where(
        node_alias=start_alias,
        node_type=source_type,
        properties=statement.left.properties,
        predicates=left_predicates,
        backend=backend,
    )
    base_sql = _assemble_select_sql(
        select_sql=f"{start_alias}.id AS end_id, 0 AS depth",
        distinct=False,
        from_sql=f"FROM {source_type.table_name} AS {start_alias}",
        joins=[],
        where_parts=base_where,
        order_sql=None,
        limit=None,
        skip=None,
    )

    # --- recursive term: extend every walk by one edge, capped at hi -------
    recursive_where = [f"{recur_alias}.depth < {hi}"]
    for field, value in relationship.properties:
        recursive_where.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_edge_field_expression(
                    edge_alias,
                    edge_type,
                    field,
                ),
                operator="=",
                value=value,
                backend=backend,
            )
        )
    recursive_sql = _assemble_select_sql(
        select_sql=(
            f"{edge_alias}.to_id AS end_id, {recur_alias}.depth + 1 AS depth"
        ),
        distinct=False,
        from_sql=f"FROM {cte_name} AS {recur_alias}",
        joins=[
            f"JOIN {edge_type.table_name} AS {edge_alias} "
            f"ON {edge_alias}.from_id = {recur_alias}.end_id"
        ],
        where_parts=recursive_where,
        order_sql=None,
        limit=None,
        skip=None,
    )

    cte_sql = (
        f"WITH RECURSIVE {cte_name} AS "
        f"({base_sql} UNION ALL {recursive_sql})"
    )

    # --- outer query over reached endpoints --------------------------------
    alias_specs = {
        end_alias: _TypeAwareAliasSpec(
            table_alias=end_alias,
            alias_kind="node",
            entity_type=source_type,
        )
    }
    if left_alias != right_alias:
        # Zero-hop rows (depth 0) bind ``b = a``; because the outer join uses the
        # walk endpoint, referencing the start alias resolves to the same row.
        alias_specs[left_alias] = alias_specs[end_alias]

    outer_where = [f"{recur_alias}.depth >= {lo}"]
    outer_where.extend(
        _compile_type_aware_variable_length_endpoint_where(
            node_alias=end_alias,
            node_type=source_type,
            properties=statement.right.properties,
            predicates=right_predicates,
            backend=backend,
        )
    )

    select_parts: list[str] = []
    for item in statement.returns:
        for expression, output_name in _compile_type_aware_chain_select_expressions(
            item,
            alias_specs,
            backend=backend,
        ):
            select_parts.append(f'{expression} AS "{output_name}"')
    select_sql = ", ".join(select_parts)

    order_sql = _compile_type_aware_chain_order_by(
        statement.order_by,
        statement.returns,
        alias_specs,
        backend=backend,
    )
    group_sql = _compile_type_aware_chain_group_by(
        statement.returns,
        alias_specs,
        backend=backend,
    )

    outer_sql = _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM {cte_name} AS {recur_alias}",
        joins=[
            f"JOIN {source_type.table_name} AS {end_alias} "
            f"ON {end_alias}.id = {recur_alias}.end_id"
        ],
        where_parts=outer_where,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )
    return f"{cte_sql} {outer_sql}"


def _compile_type_aware_variable_length_endpoint_where(
    *,
    node_alias: str,
    node_type: object,
    properties: tuple[tuple[str, object], ...],
    predicates: list[Predicate],
    backend: SQLBackend,
) -> list[str]:
    where_parts: list[str] = [
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                node_alias,
                node_type,
                field,
            ),
            operator="=",
            value=value,
            backend=backend,
        )
        for field, value in properties
    ]
    predicate_parts: list[tuple[int, str]] = [
        (
            predicate.disjunct_index,
            _compile_type_aware_match_node_predicate(
                node_alias,
                node_type,
                predicate,
                backend=backend,
            ),
        )
        for predicate in predicates
    ]
    where_parts.extend(_group_disjunct_predicates(predicate_parts))
    return where_parts
