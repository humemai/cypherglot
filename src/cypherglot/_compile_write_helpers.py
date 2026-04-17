from __future__ import annotations

from sqlglot import parse_one

from ._compiled_program import (
    CompiledCypherLoop,
    CompiledCypherProgram,
    _single_statement_program,
)
from ._compile_read_helpers import (
    _compile_chain_source_components,
    _compile_match_relationship_source_components,
)
from ._compile_sql_utils import (
    _append_node_label_filter,
    _append_predicate_filters,
    _append_relationship_type_filter_for_alias,
    _assemble_insert_select_sql,
    _assemble_select_sql,
    _extend_pattern_property_filters,
    _sql_literal,
    _sql_value,
)
from ._compile_type_aware_common import (
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
)
from ._normalize_support import NodePattern, Predicate, RelationshipPattern
from .ir import GraphRelationalReadIR
from .schema import GraphSchema


def _relationship_write_action(merge: bool) -> str:
    return "MERGE" if merge else "CREATE"


def _direct_matched_node_relationship_write_messages(
    *,
    merge: bool,
) -> tuple[str, str, str, str, str]:
    action = _relationship_write_action(merge)
    unsupported_message = (
        "CypherGlot MATCH ... MERGE compilation currently supports one "
        "matched-node merge form only for self-loop relationship merges "
        "reusing that alias."
        if merge
        else "CypherGlot MATCH ... CREATE compilation currently supports one "
        "matched-node create form only for self-loop relationship creates "
        "reusing that alias."
    )
    return (
        f"Type-aware MATCH ... {action} lowering requires an explicit matched node label.",
        f"Type-aware MATCH ... {action} lowering currently supports predicates only on the matched node alias.",
        unsupported_message,
        f"Type-aware MATCH ... {action} lowering requires explicit endpoint labels.",
        f"Type-aware MATCH ... {action} lowering requires endpoint labels to match the schema contract.",
    )


def _direct_matched_node_pair_relationship_write_messages(
    *,
    merge: bool,
) -> tuple[str, str, str, str, str]:
    action = _relationship_write_action(merge)
    unsupported_message = (
        "CypherGlot MATCH ... MERGE compilation currently supports only "
        "relationship merges between already matched node aliases."
        if merge
        else "CypherGlot MATCH ... CREATE compilation currently supports only "
        "relationship creation between already matched node aliases."
    )
    return (
        unsupported_message,
        f"Type-aware MATCH ... {action} lowering requires explicit matched endpoint labels.",
        f"Type-aware MATCH ... {action} lowering currently supports predicates only on the matched node aliases.",
        f"Type-aware MATCH ... {action} lowering requires explicit endpoint labels.",
        f"Type-aware MATCH ... {action} lowering requires endpoint labels to match the schema contract.",
    )


def _traversal_relationship_write_messages(
    *,
    merge: bool,
) -> tuple[str, str]:
    action = _relationship_write_action(merge)
    return (
        f"Type-aware MATCH ... {action} lowering requires explicit endpoint labels.",
        f"Type-aware MATCH ... {action} lowering requires endpoint labels to match the schema contract.",
    )


def _direct_fresh_endpoint_relationship_messages(
    *,
    merge: bool,
) -> tuple[str, str, str, str, str, str, str]:
    action = _relationship_write_action(merge)
    return (
        f"Type-aware MATCH ... {action} lowering requires an explicit matched node label.",
        f"Type-aware MATCH ... {action} lowering currently supports predicates only on the matched node alias.",
        (
            "CypherGlot MATCH ... MERGE compilation currently supports one "
            "matched-node merge form only for self-loop relationship merges "
            "reusing that alias."
            if merge
            else "CypherGlot MATCH ... CREATE compilation currently supports one "
            "matched-node create form only for self-loop relationship creates "
            "reusing that alias."
        ),
        f"Type-aware MATCH ... {action} lowering requires explicit endpoint labels.",
        f"Type-aware MATCH ... {action} lowering requires endpoint labels to match the schema contract.",
        (
            "CypherGlot MATCH ... MERGE compilation currently supports only one "
            "fresh endpoint beside the matched node alias."
            if merge
            else "CypherGlot MATCH ... CREATE compilation currently supports only one "
            "fresh endpoint beside the matched node alias."
        ),
        f"CypherGlot MATCH ... {action} compilation currently requires an explicit label on the fresh endpoint node.",
    )


def _traversal_fresh_endpoint_relationship_messages(
    *,
    merge: bool,
) -> tuple[str, str, str, str]:
    action = _relationship_write_action(merge)
    return (
        f"Type-aware MATCH ... {action} lowering requires explicit endpoint labels.",
        f"Type-aware MATCH ... {action} lowering requires endpoint labels to match the schema contract.",
        (
            "CypherGlot MATCH ... MERGE compilation currently supports only one "
            "fresh endpoint beside the traversal-backed matched aliases."
            if merge
            else "CypherGlot MATCH ... CREATE compilation currently supports only one "
            "fresh endpoint beside the traversal-backed matched aliases."
        ),
        f"CypherGlot MATCH ... {action} compilation currently requires an explicit label on the fresh endpoint node.",
    )


def _compile_direct_matched_node_relationship_write_sql(
    *,
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
    predicates: tuple[Predicate, ...],
    relationship: RelationshipPattern,
    graph_schema: GraphSchema | None,
    merge: bool,
) -> str:
    (
        require_label_message,
        predicate_message,
        unsupported_message,
        endpoint_label_message,
        mismatch_message,
    ) = _direct_matched_node_relationship_write_messages(merge=merge)
    if left.alias != match_node.alias or right.alias != match_node.alias:
        raise ValueError(unsupported_message)

    _alias, from_sql, where_parts, node_type, left_endpoint, right_endpoint = (
        _compile_direct_matched_node_relationship_source_parts(
            match_node=match_node,
            left=left,
            right=right,
            predicates=predicates,
            graph_schema=graph_schema,
            require_label_message=require_label_message,
            predicate_message=predicate_message,
        )
    )
    assert node_type is not None or graph_schema is None
    return _compile_resolved_match_relationship_write_from_aliases(
        relationship=relationship,
        left_alias=left.alias,
        right_alias=right.alias,
        from_sql=from_sql,
        joins=[],
        where_parts=where_parts,
        graph_schema=graph_schema,
        left_label=left_endpoint.label,
        right_label=right_endpoint.label,
        require_label_message=endpoint_label_message,
        mismatch_message=mismatch_message,
        merge=merge,
    )


def _compile_direct_matched_node_pair_relationship_write_sql(
    *,
    left_match: NodePattern,
    right_match: NodePattern,
    left: NodePattern,
    right: NodePattern,
    predicates: tuple[Predicate, ...],
    relationship: RelationshipPattern,
    graph_schema: GraphSchema | None,
    merge: bool,
) -> str:
    (
        unsupported_message,
        require_label_message,
        predicate_message,
        endpoint_label_message,
        mismatch_message,
    ) = _direct_matched_node_pair_relationship_write_messages(merge=merge)
    matched_aliases = {left_match.alias, right_match.alias}
    if left.alias not in matched_aliases or right.alias not in matched_aliases:
        raise ValueError(unsupported_message)

    (
        _left_alias,
        _right_alias,
        from_sql,
        where_parts,
        left_type,
        right_type,
        left_endpoint,
        right_endpoint,
    ) = _compile_direct_matched_node_pair_relationship_source_parts(
        left_match=left_match,
        right_match=right_match,
        left=left,
        right=right,
        predicates=predicates,
        graph_schema=graph_schema,
        require_label_message=require_label_message,
        predicate_message=predicate_message,
    )
    assert left_type is not None or graph_schema is None
    assert right_type is not None or graph_schema is None
    return _compile_resolved_match_relationship_write_from_aliases(
        relationship=relationship,
        left_alias=left.alias,
        right_alias=right.alias,
        from_sql=from_sql,
        joins=[],
        where_parts=where_parts,
        graph_schema=graph_schema,
        left_label=left_endpoint.label,
        right_label=right_endpoint.label,
        require_label_message=endpoint_label_message,
        mismatch_message=mismatch_message,
        merge=merge,
    )


def _compile_direct_matched_node_relationship_source_parts(
    *,
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
    predicates: tuple[Predicate, ...],
    graph_schema: GraphSchema | None,
    require_label_message: str,
    predicate_message: str,
) -> tuple[str, str, list[str], object | None, NodePattern, NodePattern]:
    from .compile import _resolve_write_endpoint_node_pattern_from_candidates

    alias = match_node.alias
    from_sql, where_parts, node_type = _compile_node_source_parts(
        node=match_node,
        predicates=predicates,
        graph_schema=graph_schema,
        require_label_message=require_label_message,
        predicate_message=predicate_message,
    )
    return (
        alias,
        from_sql,
        where_parts,
        node_type,
        _resolve_write_endpoint_node_pattern_from_candidates(left, (match_node,)),
        _resolve_write_endpoint_node_pattern_from_candidates(right, (match_node,)),
    )


def _compile_direct_matched_node_pair_relationship_source_parts(
    *,
    left_match: NodePattern,
    right_match: NodePattern,
    left: NodePattern,
    right: NodePattern,
    predicates: tuple[Predicate, ...],
    graph_schema: GraphSchema | None,
    require_label_message: str,
    predicate_message: str,
) -> tuple[
    str,
    str,
    str,
    list[str],
    object | None,
    object | None,
    NodePattern,
    NodePattern,
]:
    from .compile import _resolve_write_endpoint_node_pattern_from_candidates

    left_alias = left_match.alias
    right_alias = right_match.alias
    from_sql, where_parts, left_type, right_type = _compile_node_pair_source_parts(
        left_node=left_match,
        right_node=right_match,
        predicates=predicates,
        graph_schema=graph_schema,
        require_label_message=require_label_message,
        predicate_message=predicate_message,
    )
    return (
        left_alias,
        right_alias,
        from_sql,
        where_parts,
        left_type,
        right_type,
        _resolve_write_endpoint_node_pattern_from_candidates(
            left,
            (left_match, right_match),
        ),
        _resolve_write_endpoint_node_pattern_from_candidates(
            right,
            (left_match, right_match),
        ),
    )


def _compile_node_source_parts(
    *,
    node: NodePattern,
    predicates: tuple[Predicate, ...],
    graph_schema: GraphSchema | None,
    require_label_message: str,
    predicate_message: str,
) -> tuple[str, list[str], object | None]:
    alias = node.alias
    if graph_schema is None:
        where_parts: list[str] = []
        _append_node_label_filter(
            where_parts=where_parts,
            node_alias=alias,
            label=node.label,
            filter_alias=f"{alias}_label_filter_0",
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=alias,
            alias_kind="node",
            properties=node.properties,
        )
        _append_predicate_filters(
            where_parts=where_parts,
            alias_map={alias: alias},
            alias_kinds={alias: "node"},
            predicates=predicates,
        )
        return f"FROM nodes AS {alias}", where_parts, None

    if node.label is None:
        raise ValueError(require_label_message)

    node_type = graph_schema.node_type(node.label)
    where_parts = [
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                alias,
                node_type,
                field,
            ),
            operator="=",
            value=value,
        )
        for field, value in node.properties
    ]
    for predicate in predicates:
        if predicate.alias != alias:
            raise ValueError(predicate_message)
        where_parts.append(
            _compile_type_aware_match_node_predicate(alias, node_type, predicate)
        )
    return f"FROM {node_type.table_name} AS {alias}", where_parts, node_type


def _compile_node_pair_source_parts(
    *,
    left_node: NodePattern,
    right_node: NodePattern,
    predicates: tuple[Predicate, ...],
    graph_schema: GraphSchema | None,
    require_label_message: str,
    predicate_message: str,
) -> tuple[str, list[str], object | None, object | None]:
    left_alias = left_node.alias
    right_alias = right_node.alias
    if graph_schema is None:
        where_parts: list[str] = []
        _append_node_label_filter(
            where_parts=where_parts,
            node_alias=left_alias,
            label=left_node.label,
            filter_alias=f"{left_alias}_label_filter_0",
        )
        _append_node_label_filter(
            where_parts=where_parts,
            node_alias=right_alias,
            label=right_node.label,
            filter_alias=f"{right_alias}_label_filter_1",
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=left_alias,
            alias_kind="node",
            properties=left_node.properties,
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=right_alias,
            alias_kind="node",
            properties=right_node.properties,
        )
        _append_predicate_filters(
            where_parts=where_parts,
            alias_map={left_alias: left_alias, right_alias: right_alias},
            alias_kinds={left_alias: "node", right_alias: "node"},
            predicates=predicates,
        )
        return (
            f"FROM nodes AS {left_alias}, nodes AS {right_alias}",
            where_parts,
            None,
            None,
        )

    if left_node.label is None or right_node.label is None:
        raise ValueError(require_label_message)

    left_type = graph_schema.node_type(left_node.label)
    right_type = graph_schema.node_type(right_node.label)
    where_parts = [
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                left_alias,
                left_type,
                field,
            ),
            operator="=",
            value=value,
        )
        for field, value in left_node.properties
    ]
    where_parts.extend(
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                right_alias,
                right_type,
                field,
            ),
            operator="=",
            value=value,
        )
        for field, value in right_node.properties
    )
    for predicate in predicates:
        if predicate.alias == left_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(left_alias, left_type, predicate)
            )
            continue
        if predicate.alias == right_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(right_alias, right_type, predicate)
            )
            continue
        raise ValueError(predicate_message)
    return (
        f"FROM {left_type.table_name} AS {left_alias}, "
        f"{right_type.table_name} AS {right_alias}",
        where_parts,
        left_type,
        right_type,
    )


def _validate_type_aware_edge_contract(
    *,
    relationship: RelationshipPattern,
    edge_type: object,
    left_label: str | None,
    right_label: str | None,
    mismatch_message: str,
) -> None:
    source_label = left_label
    target_label = right_label
    if relationship.direction == "in":
        source_label, target_label = target_label, source_label
    if source_label != edge_type.source_type or target_label != edge_type.target_type:
        raise ValueError(mismatch_message)


def _compile_resolved_match_relationship_write_sql(
    *,
    relationship: RelationshipPattern,
    from_id_sql: str,
    to_id_sql: str,
    from_sql: str,
    joins: list[str],
    where_parts: list[str],
    graph_schema: GraphSchema | None,
    left_label: str | None,
    right_label: str | None,
    require_label_message: str,
    mismatch_message: str,
    merge: bool,
) -> str:
    from .compile import (
        _removed_schema_less_write_sql,
        _require_single_relationship_type,
        _resolve_type_aware_property_column,
    )

    guarded_where_parts = where_parts
    if merge:
        guarded_where_parts = list(where_parts)
        if graph_schema is None:
            exists_where = [
                f"existing_merge_edge.from_id = {from_id_sql}",
                f"existing_merge_edge.to_id = {to_id_sql}",
            ]
            _append_relationship_type_filter_for_alias(
                exists_where,
                relationship,
                "existing_merge_edge",
            )
            _extend_pattern_property_filters(
                where_parts=exists_where,
                alias="existing_merge_edge",
                alias_kind="relationship",
                properties=relationship.properties,
            )
            guarded_where_parts.append(
                "NOT EXISTS ("
                "SELECT 1 FROM edges AS existing_merge_edge "
                f"WHERE {' AND '.join(exists_where)}"
                ")"
            )
        else:
            if left_label is None or right_label is None:
                raise ValueError(require_label_message)

            edge_type = graph_schema.edge_type(
                _require_single_relationship_type(relationship)
            )
            _validate_type_aware_edge_contract(
                relationship=relationship,
                edge_type=edge_type,
                left_label=left_label,
                right_label=right_label,
                mismatch_message=mismatch_message,
            )
            exists_where = [
                f"existing_merge_edge.from_id = {from_id_sql}",
                f"existing_merge_edge.to_id = {to_id_sql}",
            ]
            for field, value in relationship.properties:
                exists_where.append(
                    _compile_type_aware_predicate(
                        field_expression=_compile_type_aware_edge_field_expression(
                            "existing_merge_edge",
                            edge_type,
                            field,
                        ),
                        operator="=",
                        value=value,
                    )
                )
            guarded_where_parts.append(
                "NOT EXISTS ("
                + _assemble_select_sql(
                    select_sql="1",
                    distinct=False,
                    from_sql=f"FROM {edge_type.table_name} AS existing_merge_edge",
                    joins=[],
                    where_parts=exists_where,
                    order_sql=None,
                    limit=None,
                    skip=None,
                )
                + ")"
            )

    if graph_schema is None:
        return _assemble_insert_select_sql(
            target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
            select_sql=(
                f"SELECT {_sql_literal(_require_single_relationship_type(relationship))}, "
                f"{from_id_sql}, {to_id_sql}, {_removed_schema_less_write_sql()}"
            ),
            from_sql=from_sql,
            joins=joins,
            where_parts=guarded_where_parts,
        )

    if left_label is None or right_label is None:
        raise ValueError(require_label_message)

    edge_type = graph_schema.edge_type(_require_single_relationship_type(relationship))
    _validate_type_aware_edge_contract(
        relationship=relationship,
        edge_type=edge_type,
        left_label=left_label,
        right_label=right_label,
        mismatch_message=mismatch_message,
    )
    columns = ["from_id", "to_id"]
    values = [from_id_sql, to_id_sql]
    for field, value in relationship.properties:
        columns.append(_resolve_type_aware_property_column(edge_type, field))
        values.append(_sql_value(value))
    return _assemble_insert_select_sql(
        target_sql=f"INSERT INTO {edge_type.table_name} ({', '.join(columns)})",
        select_sql=f"SELECT {', '.join(values)}",
        from_sql=from_sql,
        joins=joins,
        where_parts=guarded_where_parts,
    )


def _compile_resolved_match_relationship_write_from_aliases(
    *,
    relationship: RelationshipPattern,
    left_alias: str,
    right_alias: str,
    from_sql: str,
    joins: list[str],
    where_parts: list[str],
    graph_schema: GraphSchema | None,
    left_label: str | None,
    right_label: str | None,
    require_label_message: str,
    mismatch_message: str,
    merge: bool,
) -> str:
    from .compile import _match_create_endpoint_id_sql

    return _compile_resolved_match_relationship_write_sql(
        relationship=relationship,
        from_id_sql=_match_create_endpoint_id_sql(
            relationship.direction,
            "from",
            left_alias,
            right_alias,
        ),
        to_id_sql=_match_create_endpoint_id_sql(
            relationship.direction,
            "to",
            left_alias,
            right_alias,
        ),
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        graph_schema=graph_schema,
        left_label=left_label,
        right_label=right_label,
        require_label_message=require_label_message,
        mismatch_message=mismatch_message,
        merge=merge,
    )


def _compile_traversal_relationship_write_sql(
    *,
    source: GraphRelationalReadIR,
    left: NodePattern,
    right: NodePattern,
    relationship: RelationshipPattern,
    graph_schema: GraphSchema | None,
    merge: bool,
) -> str:
    endpoint_label_message, mismatch_message = (
        _traversal_relationship_write_messages(merge=merge)
    )
    from_sql, joins, where_parts, alias_map, left_node, right_node = (
        _compile_traversal_relationship_source_parts(
            source=source,
            left=left,
            right=right,
            graph_schema=graph_schema,
        )
    )
    return _compile_resolved_match_relationship_write_from_aliases(
        relationship=relationship,
        left_alias=alias_map[left.alias],
        right_alias=alias_map[right.alias],
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        graph_schema=graph_schema,
        left_label=left_node.label,
        right_label=right_node.label,
        require_label_message=endpoint_label_message,
        mismatch_message=mismatch_message,
        merge=merge,
    )


def _compile_direct_fresh_endpoint_relationship_program(
    *,
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
    predicates: tuple[Predicate, ...],
    relationship: RelationshipPattern,
    graph_schema: GraphSchema | None,
    merge: bool,
) -> CompiledCypherProgram:
    (
        source_require_label_message,
        source_predicate_message,
        _all_matched_unsupported_message,
        _endpoint_label_message,
        _mismatch_message,
        unsupported_message,
        label_message,
    ) = _direct_fresh_endpoint_relationship_messages(merge=merge)
    alias, from_sql, where_parts, _node_type, left_endpoint, right_endpoint = (
        _compile_direct_matched_node_relationship_source_parts(
            match_node=match_node,
            left=left,
            right=right,
            predicates=predicates,
            graph_schema=graph_schema,
            require_label_message=source_require_label_message,
            predicate_message=source_predicate_message,
        )
    )
    return _compile_match_fresh_endpoint_relationship_program(
        left=left,
        right=right,
        relationship=relationship,
        matched_aliases={alias},
        all_matched_sql=(
            _compile_direct_matched_node_relationship_write_sql(
                match_node=match_node,
                left=left,
                right=right,
                predicates=predicates,
                relationship=relationship,
                graph_schema=graph_schema,
                merge=merge,
            )
            if left.alias == alias and right.alias == alias
            else None
        ),
        source_from_sql=from_sql,
        source_joins=[],
        source_where_parts=where_parts,
        matched_node_id_sql_by_alias={alias: f"{alias}.id"},
        guard_relationship_merge=merge,
        left_endpoint=left_endpoint,
        right_endpoint=right_endpoint,
        graph_schema=graph_schema,
        unsupported_message=unsupported_message,
        label_message=label_message,
    )


def _compile_traversal_fresh_endpoint_relationship_program(
    *,
    source: GraphRelationalReadIR,
    left: NodePattern,
    right: NodePattern,
    relationship: RelationshipPattern,
    graph_schema: GraphSchema | None,
    merge: bool,
) -> CompiledCypherProgram:
    (
        _endpoint_label_message,
        _mismatch_message,
        unsupported_message,
        label_message,
    ) = _traversal_fresh_endpoint_relationship_messages(merge=merge)
    from_sql, joins, where_parts, alias_map, left_endpoint, right_endpoint = (
        _compile_traversal_relationship_source_parts(
            source=source,
            left=left,
            right=right,
            graph_schema=graph_schema,
        )
    )
    return _compile_match_fresh_endpoint_relationship_program(
        left=left,
        right=right,
        relationship=relationship,
        matched_aliases=set(alias_map),
        all_matched_sql=(
            _compile_traversal_relationship_write_sql(
                source=source,
                left=left,
                right=right,
                relationship=relationship,
                graph_schema=graph_schema,
                merge=merge,
            )
            if left.alias in alias_map and right.alias in alias_map
            else None
        ),
        source_from_sql=from_sql,
        source_joins=joins,
        source_where_parts=where_parts,
        matched_node_id_sql_by_alias={
            alias: f"{source_alias}.id" for alias, source_alias in alias_map.items()
        },
        guard_relationship_merge=merge,
        left_endpoint=left_endpoint,
        right_endpoint=right_endpoint,
        graph_schema=graph_schema,
        unsupported_message=unsupported_message,
        label_message=label_message,
    )


def _compile_traversal_relationship_source_parts(
    *,
    source: GraphRelationalReadIR,
    left: NodePattern,
    right: NodePattern,
    graph_schema: GraphSchema | None,
) -> tuple[str, list[str], list[str], dict[str, str], NodePattern, NodePattern]:
    from .compile import (
        _compile_type_aware_traversal_write_source_components,
        _resolve_write_endpoint_node_pattern_from_traversal_source,
    )

    if graph_schema is not None:
        from_sql, joins, where_parts, alias_map = (
            _compile_type_aware_traversal_write_source_components(
                source,
                graph_schema,
            )
        )
    elif source.source_kind == "relationship-chain":
        from_sql, joins, where_parts, alias_map, _alias_kinds = (
            _compile_chain_source_components(
                nodes=source.nodes,
                relationships=source.relationships,
                predicates=source.predicates,
            )
        )
    elif source.source_kind == "relationship":
        from_sql, joins, where_parts, alias_map, _alias_kinds = (
            _compile_match_relationship_source_components(source)
        )
    else:
        raise ValueError(
            "Traversal-backed relationship writes require a relationship MATCH source."
        )

    return (
        from_sql,
        joins,
        where_parts,
        alias_map,
        _resolve_write_endpoint_node_pattern_from_traversal_source(left, source),
        _resolve_write_endpoint_node_pattern_from_traversal_source(right, source),
    )


def _compile_match_fresh_endpoint_relationship_program(
    *,
    left: NodePattern,
    right: NodePattern,
    relationship: RelationshipPattern,
    matched_aliases: set[str],
    all_matched_sql: str | None,
    source_from_sql: str,
    source_joins: list[str],
    source_where_parts: list[str],
    matched_node_id_sql_by_alias: dict[str, str],
    guard_relationship_merge: bool,
    left_endpoint: NodePattern,
    right_endpoint: NodePattern,
    graph_schema: GraphSchema | None,
    unsupported_message: str,
    label_message: str,
) -> CompiledCypherProgram:
    from .compile import (
        _compile_create_node_steps,
        _compile_edge_insert_statement,
        _require_single_relationship_type,
    )

    if left.alias in matched_aliases and right.alias in matched_aliases:
        if all_matched_sql is None:
            raise ValueError(
                "Expected concrete all-matched SQL for a fully matched relationship write."
            )
        return _single_statement_program(all_matched_sql)

    new_endpoint = left
    matched_endpoint = right
    new_endpoint_is_right = False
    if right.alias not in matched_aliases:
        new_endpoint = right
        matched_endpoint = left
        new_endpoint_is_right = True
    if matched_endpoint.alias not in matched_aliases:
        raise ValueError(unsupported_message)
    if new_endpoint.label is None:
        raise ValueError(label_message)
    matched_node_id_sql = matched_node_id_sql_by_alias[matched_endpoint.alias]
    source_where_parts_for_loop = source_where_parts
    if guard_relationship_merge:
        if relationship.direction == "out":
            matched_column = "from_id" if new_endpoint_is_right else "to_id"
            new_column = "to_id" if new_endpoint_is_right else "from_id"
        else:
            matched_column = "to_id" if new_endpoint_is_right else "from_id"
            new_column = "from_id" if new_endpoint_is_right else "to_id"

        existing_edge_alias = "existing_merge_edge"
        existing_node_alias = "existing_merge_new_node"
        exists_where = [
            f"{existing_edge_alias}.{matched_column} = {matched_node_id_sql}"
        ]
        if graph_schema is None:
            exists_from_sql = f"FROM edges AS {existing_edge_alias}"
            existing_node_table_sql = "nodes"
        else:
            edge_type = graph_schema.edge_type(
                _require_single_relationship_type(relationship)
            )
            new_node_type = graph_schema.node_type(new_endpoint.label)
            exists_from_sql = f"FROM {edge_type.table_name} AS {existing_edge_alias}"
            existing_node_table_sql = new_node_type.table_name
        exists_joins = [
            f"JOIN {existing_node_table_sql} AS {existing_node_alias} ON {existing_node_alias}.id = {existing_edge_alias}.{new_column}"
        ]

        if graph_schema is None:
            if new_endpoint.label is not None:
                exists_joins.append(
                    f"JOIN node_labels AS {existing_node_alias}_label_0 ON {existing_node_alias}_label_0.node_id = {existing_node_alias}.id AND {existing_node_alias}_label_0.label = {_sql_literal(new_endpoint.label)}"
                )
            _append_relationship_type_filter_for_alias(
                exists_where,
                relationship,
                existing_edge_alias,
            )
            _extend_pattern_property_filters(
                where_parts=exists_where,
                alias=existing_edge_alias,
                alias_kind="relationship",
                properties=relationship.properties,
            )
            _extend_pattern_property_filters(
                where_parts=exists_where,
                alias=existing_node_alias,
                alias_kind="node",
                properties=new_endpoint.properties,
            )
        else:
            for field, value in relationship.properties:
                exists_where.append(
                    _compile_type_aware_predicate(
                        field_expression=_compile_type_aware_edge_field_expression(
                            existing_edge_alias,
                            edge_type,
                            field,
                        ),
                        operator="=",
                        value=value,
                    )
                )
            for field, value in new_endpoint.properties:
                exists_where.append(
                    _compile_type_aware_predicate(
                        field_expression=_compile_type_aware_node_field_expression(
                            existing_node_alias,
                            new_node_type,
                            field,
                        ),
                        operator="=",
                        value=value,
                    )
                )
        exists_sql = _assemble_select_sql(
            select_sql="1",
            distinct=False,
            from_sql=exists_from_sql,
            joins=exists_joins,
            where_parts=exists_where,
            order_sql=None,
            limit=1,
            skip=None,
        )
        source_where_parts_for_loop = [
            *source_where_parts,
            f"NOT EXISTS ({exists_sql})",
        ]

    source_sql = _assemble_select_sql(
        select_sql=f"{matched_node_id_sql} AS match_node_id",
        distinct=False,
        from_sql=source_from_sql,
        joins=source_joins,
        where_parts=source_where_parts_for_loop,
        order_sql=None,
        limit=None,
        skip=None,
    )

    left_binding = (
        ":created_node_id"
        if left_endpoint.alias == new_endpoint.alias
        else ":match_node_id"
    )
    right_binding = (
        ":created_node_id"
        if right_endpoint.alias == new_endpoint.alias
        else ":match_node_id"
    )
    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(source_sql),
                row_bindings=("match_node_id",),
                body=_compile_create_node_steps(
                    new_endpoint,
                    "created_node_id",
                    graph_schema=graph_schema,
                )
                + (
                    _compile_edge_insert_statement(
                        relationship=relationship,
                        from_value=(
                            left_binding
                            if relationship.direction == "out"
                            else right_binding
                        ),
                        to_value=(
                            right_binding
                            if relationship.direction == "out"
                            else left_binding
                        ),
                        graph_schema=graph_schema,
                        left_node=left_endpoint,
                        right_node=right_endpoint,
                    ),
                ),
            ),
        ),
    )