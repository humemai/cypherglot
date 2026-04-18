from __future__ import annotations

from ._compile_sql_utils import _assemble_select_sql, _sql_literal, _sql_value
from ._compile_type_aware_common import (
    _TypeAwareAliasSpec,
    _TypeAwareWithBindingSpec,
    _build_type_aware_with_binding_spec,
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_match_relationship_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
    _compile_type_aware_size_expression,
    _compile_type_aware_with_binding_columns,
    _is_type_aware_binding_numeric,
    _type_aware_with_expression_logical_type,
    _with_scalar_prefix,
)
from ._compile_type_aware_read_projections import (
    _compile_type_aware_integer_cast_expression,
)
from ._compile_type_aware_reads import (
    _compile_type_aware_chain_source_components,
    _is_variable_length_relationship,
)
from .ir import GraphRelationalReadIR, SQLBackend
from .normalize import WithBinding, WithCaseSpec, WithReturnItem
from .schema import GraphSchema


def _compile_type_aware_source_binding_columns(
    binding: WithBinding,
    *,
    table_alias: str,
    entity_type: object,
    source_alias_specs: dict[str, _TypeAwareAliasSpec],
    source_binding_specs: dict[str, _TypeAwareWithBindingSpec],
    backend: SQLBackend,
) -> tuple[list[str], str | None]:
    binding_expression = binding.expression
    if binding.binding_kind != "scalar" or binding_expression is None:
        return (
            _compile_type_aware_with_binding_columns(
                binding,
                table_alias=table_alias,
                entity_type=entity_type,
            ),
            None,
        )
    expression_sql = _compile_type_aware_source_binding_expression(
        binding_expression,
        source_alias_specs,
        source_binding_specs,
        backend=backend,
    )
    return [f'{expression_sql} AS "{_with_scalar_prefix(binding.output_alias)}"'], (
        _type_aware_source_binding_expression_logical_type(
            binding_expression,
            source_alias_specs,
            source_binding_specs,
        )
    )


def _type_aware_source_binding_expression_logical_type(
    item: WithReturnItem,
    source_alias_specs: dict[str, _TypeAwareAliasSpec],
    source_binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str | None:
    if item.kind in {"start_node", "end_node"} and item.field is not None:
        alias_spec = source_alias_specs[item.alias]
        if alias_spec.alias_kind != "relationship":
            return None
        endpoint_alias = (
            alias_spec.start_node_alias
            if item.kind == "start_node"
            else alias_spec.end_node_alias
        )
        if endpoint_alias is None:
            return None
        return _type_aware_with_expression_logical_type(
            WithReturnItem(kind="field", alias=endpoint_alias, field=item.field),
            source_binding_specs,
        )
    return _type_aware_with_expression_logical_type(item, source_binding_specs)


def _compile_type_aware_source_binding_expression(
    item: WithReturnItem,
    source_alias_specs: dict[str, _TypeAwareAliasSpec],
    source_binding_specs: dict[str, _TypeAwareWithBindingSpec],
    *,
    backend: SQLBackend,
) -> str:
    if item.kind == "case":
        assert isinstance(item.value, WithCaseSpec)
        when_sql = " ".join(
            "WHEN "
            + _compile_type_aware_source_binding_expression(
                arm.condition,
                source_alias_specs,
                source_binding_specs,
                backend=backend,
            )
            + " THEN "
            + _compile_type_aware_source_binding_expression(
                arm.result,
                source_alias_specs,
                source_binding_specs,
                backend=backend,
            )
            for arm in item.value.when_items
        )
        else_sql = _compile_type_aware_source_binding_expression(
            item.value.else_item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"CASE {when_sql} ELSE {else_sql} END"

    if item.kind == "scalar_value":
        return _sql_value(item.value)

    if item.kind == "scalar":
        binding_spec = source_binding_specs[item.alias]
        if binding_spec.binding.binding_kind == "scalar":
            return _compile_type_aware_source_binding_expression(
                binding_spec.binding.expression,
                source_alias_specs,
                source_binding_specs,
                backend=backend,
            )
        raise ValueError(
            "Source WITH binding expressions currently require scalar aliases to "
            "come from scalar source bindings."
        )

    if item.kind == "field":
        assert item.field is not None
        return _compile_type_aware_source_alias_field_expression(
            item.alias,
            item.field,
            source_alias_specs,
        )

    if item.kind == "id":
        return f'{source_alias_specs[item.alias].table_alias}.id'

    if item.kind == "type":
        return _sql_literal(source_alias_specs[item.alias].entity_type.name)

    if item.kind in {"start_node", "end_node"}:
        if item.field is None:
            raise ValueError(
                "Source WITH binding expressions currently require endpoint-derived "
                "bindings to project a scalar field."
            )
        relationship_alias_spec = source_alias_specs[item.alias]
        if relationship_alias_spec.alias_kind != "relationship":
            raise ValueError(
                "Source WITH binding expressions currently support startNode(...) "
                "and endNode(...) only over relationship source aliases."
            )
        endpoint_alias = (
            relationship_alias_spec.start_node_alias
            if item.kind == "start_node"
            else relationship_alias_spec.end_node_alias
        )
        if endpoint_alias is None:
            raise ValueError(
                "Source WITH binding expressions currently require explicit endpoint "
                "aliases to compile startNode(...) and endNode(...)."
            )
        return _compile_type_aware_source_alias_field_expression(
            endpoint_alias,
            item.field,
            source_alias_specs,
        )

    if item.kind == "size":
        if item.field is None:
            inner = _compile_type_aware_source_binding_expression(
                WithReturnItem(kind="scalar", alias=item.alias),
                source_alias_specs,
                source_binding_specs,
                backend=backend,
            )
        else:
            inner = _compile_type_aware_source_alias_field_expression(
                item.alias,
                item.field,
                source_alias_specs,
            )
        return _compile_type_aware_size_expression(inner)

    if item.kind == "predicate":
        assert item.operator is not None
        if item.field is None:
            expression = _compile_type_aware_source_binding_expression(
                WithReturnItem(kind="scalar", alias=item.alias),
                source_alias_specs,
                source_binding_specs,
                backend=backend,
            )
            return _compile_type_aware_predicate(
                field_expression=expression,
                operator=item.operator,
                value=item.value,
                backend=backend,
                is_statically_numeric=_is_type_aware_binding_numeric(
                    source_binding_specs[item.alias]
                ),
            )
        return _compile_type_aware_predicate(
            field_expression=_compile_type_aware_source_alias_field_expression(
                item.alias,
                item.field,
                source_alias_specs,
            ),
            operator=item.operator,
            value=item.value,
            backend=backend,
            is_statically_numeric=_is_type_aware_binding_numeric(
                source_binding_specs[item.alias],
                item.field,
            ),
        )

    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
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
        from ._compile_type_aware_read_projections import (
            _compile_type_aware_numeric_function_expression,
        )

        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return _compile_type_aware_numeric_function_expression(
            item.kind,
            inner,
            backend,
            cast_operand=item.value is None
            and not _is_type_aware_binding_numeric(
                source_binding_specs[item.alias],
                item.field,
            ),
        )

    if item.kind == "to_string":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"CAST({inner} AS TEXT)"

    if item.kind == "to_integer":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return _compile_type_aware_integer_cast_expression(
            inner,
            backend,
            source_value=item.value,
            is_statically_numeric=item.value is None
            and _is_type_aware_binding_numeric(
                source_binding_specs[item.alias],
                item.field,
            ),
        )

    if item.kind == "to_float":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"CAST({inner} AS REAL)"

    if item.kind == "to_boolean":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"CAST({inner} AS BOOLEAN)"

    if item.kind == "coalesce" and item.value is not None:
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"COALESCE({inner}, {_sql_value(item.value)})"

    if item.kind == "replace":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return (
            f"REPLACE({inner}, {_sql_value(item.search_value)}, "
            f"{_sql_value(item.replace_value)})"
        )

    if item.kind in {"left", "right"}:
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"{item.kind.upper()}({inner}, {_sql_value(item.length_value)})"

    if item.kind == "split":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        return f"SPLIT({inner}, {_sql_value(item.delimiter_value)})"

    if item.kind == "substring":
        inner = _compile_type_aware_source_binding_input_expression(
            item,
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
        start_sql = _sql_value(item.start_value)
        if item.length_value is None:
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        return f"SUBSTRING({inner}, ({start_sql} + 1), {_sql_value(item.length_value)})"

    raise ValueError(
        f"Unsupported derived WITH binding expression kind: {item.kind!r}."
    )


def _compile_type_aware_source_binding_input_expression(
    item: WithReturnItem,
    source_alias_specs: dict[str, _TypeAwareAliasSpec],
    source_binding_specs: dict[str, _TypeAwareWithBindingSpec],
    *,
    backend: SQLBackend,
) -> str:
    if item.value is not None:
        return _sql_value(item.value)
    if item.field is None:
        return _compile_type_aware_source_binding_expression(
            WithReturnItem(kind="scalar", alias=item.alias),
            source_alias_specs,
            source_binding_specs,
            backend=backend,
        )
    return _compile_type_aware_source_alias_field_expression(
        item.alias,
        item.field,
        source_alias_specs,
    )


def _compile_type_aware_source_alias_field_expression(
    alias: str,
    field: str,
    source_alias_specs: dict[str, _TypeAwareAliasSpec],
) -> str:
    alias_spec = source_alias_specs[alias]
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


def _compile_type_aware_with_source_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> tuple[str, dict[str, _TypeAwareWithBindingSpec]]:
    source = statement.source
    if source is None:
        raise ValueError(
            "Type-aware lowering requires a source read IR for MATCH ... WITH "
            "... RETURN."
        )

    if source.source_kind == "relationship-chain":
        from_sql, joins, where_parts, alias_specs = (
            _compile_type_aware_chain_source_components(
                nodes=source.nodes,
                relationships=source.relationships,
                predicates=source.predicates,
                graph_schema=graph_schema,
                backend=backend,
            )
        )

        select_parts: list[str] = []
        binding_specs: dict[str, _TypeAwareWithBindingSpec] = {}
        output_alias_by_source_alias = {
            binding.source_alias: binding.output_alias
            for binding in statement.bindings
            if binding.binding_kind == "entity"
        }
        source_binding_specs = {
            alias: _build_type_aware_with_binding_spec(
                binding=WithBinding(
                    source_alias=alias,
                    output_alias=alias,
                    binding_kind="entity",
                    alias_kind=alias_spec.alias_kind,
                ),
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
            for alias, alias_spec in alias_specs.items()
        }
        for binding in statement.bindings:
            alias_spec = alias_specs.get(binding.source_alias)
            if alias_spec is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware fixed-length multi-hop source."
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
                    backend=backend,
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
                    backend=backend,
                )
            )

        select_parts: list[str] = []
        binding_specs: dict[str, _TypeAwareWithBindingSpec] = {}
        source_binding_specs = {
            node.alias: _build_type_aware_with_binding_spec(
                binding=WithBinding(
                    source_alias=node.alias,
                    output_alias=node.alias,
                    binding_kind="entity",
                    alias_kind="node",
                ),
                entity_type=node_type,
            )
        }
        for binding in statement.bindings:
            if binding.source_alias != node.alias:
                raise ValueError(
                    "Type-aware lowering currently supports MATCH ... WITH ... "
                    "RETURN node bindings only from the matched node alias."
                )
            binding_columns, scalar_logical_type = (
                _compile_type_aware_source_binding_columns(
                    binding,
                    table_alias=node.alias,
                    entity_type=node_type,
                    source_alias_specs={
                        node.alias: _TypeAwareAliasSpec(
                            table_alias=node.alias,
                            alias_kind="node",
                            entity_type=node_type,
                        )
                    },
                    source_binding_specs=source_binding_specs,
                    backend=backend,
                )
            )
            binding_specs[binding.output_alias] = _build_type_aware_with_binding_spec(
                binding=binding,
                entity_type=node_type,
                scalar_logical_type=scalar_logical_type,
            )
            select_parts.extend(binding_columns)

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
                backend=backend,
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
                    backend=backend,
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
                    backend=backend,
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
                    backend=backend,
                )
            )

        for predicate in source.predicates:
            if predicate.alias == source.left.alias:
                where_parts.append(
                    _compile_type_aware_match_node_predicate(
                        source.left.alias,
                        left_type,
                        predicate,
                        backend=backend,
                    )
                )
                continue
            if predicate.alias == source.right.alias:
                where_parts.append(
                    _compile_type_aware_match_node_predicate(
                        source.right.alias,
                        right_type,
                        predicate,
                        backend=backend,
                    )
                )
                continue
            if predicate.alias == relationship_alias:
                where_parts.append(
                    _compile_type_aware_match_relationship_predicate(
                        relationship_alias,
                        edge_type,
                        predicate,
                        backend=backend,
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
        source_binding_specs = {
            alias: _build_type_aware_with_binding_spec(
                binding=WithBinding(
                    source_alias=alias,
                    output_alias=alias,
                    binding_kind="entity",
                    alias_kind=(
                        "relationship"
                        if alias == relationship_alias
                        else "node"
                    ),
                ),
                entity_type=entity_type,
                start_binding_output_alias=(
                    output_alias_by_source_alias.get(source.left.alias)
                    if alias == relationship_alias
                    else None
                ),
                end_binding_output_alias=(
                    output_alias_by_source_alias.get(source.right.alias)
                    if alias == relationship_alias
                    else None
                ),
            )
            for alias, (_, entity_type) in alias_map.items()
        }
        source_alias_specs = {
            source.left.alias: _TypeAwareAliasSpec(
                table_alias=source.left.alias,
                alias_kind="node",
                entity_type=left_type,
            ),
            source.right.alias: _TypeAwareAliasSpec(
                table_alias=source.right.alias,
                alias_kind="node",
                entity_type=right_type,
            ),
            relationship_alias: _TypeAwareAliasSpec(
                table_alias=relationship_alias,
                alias_kind="relationship",
                entity_type=edge_type,
                start_node_alias=source.left.alias,
                end_node_alias=source.right.alias,
            ),
        }
        for binding in statement.bindings:
            table_alias, entity_type = alias_map.get(binding.source_alias, (None, None))
            if table_alias is None:
                raise ValueError(
                    f"Unknown WITH binding source alias {binding.source_alias!r} "
                    "for type-aware relationship source."
                )
            binding_columns, scalar_logical_type = (
                _compile_type_aware_source_binding_columns(
                    binding,
                    table_alias=table_alias,
                    entity_type=entity_type,
                    source_alias_specs=source_alias_specs,
                    source_binding_specs=source_binding_specs,
                    backend=backend,
                )
            )
            binding_specs[binding.output_alias] = _build_type_aware_with_binding_spec(
                binding=binding,
                entity_type=entity_type,
                scalar_logical_type=scalar_logical_type,
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
            select_parts.extend(binding_columns)

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
