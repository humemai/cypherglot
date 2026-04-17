from __future__ import annotations

from sqlglot import parse_one

from ._compiled_program import (
    CompiledCypherProgram,
    CompiledCypherStatement,
    _single_statement_program,
)
from ._normalize_support import NodePattern, RelationshipPattern
from .ir import (
    GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR,
    GraphRelationalCreateRelationshipWriteIR,
    GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR,
    GraphRelationalMatchMergeRelationshipWriteIR,
    GraphRelationalMergeNodeWriteIR,
    GraphRelationalMergeRelationshipWriteIR,
)
from .schema import GraphSchema


def _compile_create_relationship_program(
    statement: GraphRelationalCreateRelationshipWriteIR,
    graph_schema: GraphSchema,
) -> CompiledCypherProgram:
    from .compile import (
        _compile_create_node_steps,
        _compile_edge_insert_statement,
        _create_relationship_uses_distinct_nodes,
    )

    left_steps = _compile_create_node_steps(
        statement.left,
        "left_node_id",
        graph_schema=graph_schema,
    )
    right_steps: tuple[CompiledCypherStatement, ...] = ()
    right_binding = ":left_node_id"

    if _create_relationship_uses_distinct_nodes(statement.left, statement.right):
        right_steps = _compile_create_node_steps(
            statement.right,
            "right_node_id",
            graph_schema=graph_schema,
        )
        right_binding = ":right_node_id"

    edge_statement = _compile_edge_insert_statement(
        relationship=statement.relationship,
        from_value=(
            ":left_node_id"
            if statement.relationship.direction == "out"
            else right_binding
        ),
        to_value=(
            right_binding
            if statement.relationship.direction == "out"
            else ":left_node_id"
        ),
        graph_schema=graph_schema,
        left_node=statement.left,
        right_node=statement.right,
    )

    return CompiledCypherProgram(steps=left_steps + right_steps + (edge_statement,))


def _compile_create_relationship_from_separate_patterns_program(
    statement: GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR,
    graph_schema: GraphSchema,
) -> CompiledCypherProgram:
    from .compile import _compile_create_node_steps, _compile_edge_insert_statement

    first_steps = _compile_create_node_steps(
        statement.first_node,
        "first_node_id",
        graph_schema=graph_schema,
    )
    second_steps = _compile_create_node_steps(
        statement.second_node,
        "second_node_id",
        graph_schema=graph_schema,
    )
    alias_bindings = {
        statement.first_node.alias: ":first_node_id",
        statement.second_node.alias: ":second_node_id",
    }
    left_binding = alias_bindings[statement.left.alias]
    right_binding = alias_bindings[statement.right.alias]

    edge_statement = _compile_edge_insert_statement(
        relationship=statement.relationship,
        from_value=(
            left_binding if statement.relationship.direction == "out" else right_binding
        ),
        to_value=(
            right_binding if statement.relationship.direction == "out" else left_binding
        ),
        graph_schema=graph_schema,
        left_node=statement.left,
        right_node=statement.right,
    )

    return CompiledCypherProgram(
        steps=first_steps + second_steps + (edge_statement,)
    )


def _compile_merge_node_program(
    statement: GraphRelationalMergeNodeWriteIR,
    graph_schema: GraphSchema,
) -> CompiledCypherProgram:
    from .compile import _compile_type_aware_merge_node_sql

    return _single_statement_program(
        _compile_type_aware_merge_node_sql(statement.node, graph_schema)
    )


def _compile_merge_relationship_program(
    statement: GraphRelationalMergeRelationshipWriteIR,
    graph_schema: GraphSchema,
) -> CompiledCypherProgram:
    from ._compile_write_helpers import _compile_resolved_match_relationship_write_sql
    from .compile import (
        _compile_type_aware_merge_node_sql,
        _compile_type_aware_node_field_expression,
        _compile_type_aware_predicate,
        _create_relationship_uses_distinct_nodes,
    )

    if not _create_relationship_uses_distinct_nodes(statement.left, statement.right):
        return _compile_merge_relationship_self_loop_program(
            statement,
            graph_schema=graph_schema,
        )

    if statement.left.label is None or statement.right.label is None:
        raise ValueError(
            "Type-aware MERGE relationship lowering requires explicit endpoint labels."
        )

    left_type = graph_schema.node_type(statement.left.label)
    right_type = graph_schema.node_type(statement.right.label)
    relationship_sql = _compile_resolved_match_relationship_write_sql(
        relationship=statement.relationship,
        from_id_sql=(
            f"{statement.left.alias}.id"
            if statement.relationship.direction == "out"
            else f"{statement.right.alias}.id"
        ),
        to_id_sql=(
            f"{statement.right.alias}.id"
            if statement.relationship.direction == "out"
            else f"{statement.left.alias}.id"
        ),
        from_sql=(
            f"FROM {left_type.table_name} AS {statement.left.alias}, "
            f"{right_type.table_name} AS {statement.right.alias}"
        ),
        joins=[],
        where_parts=[
            *[
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        statement.left.alias,
                        left_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
                for field, value in statement.left.properties
            ],
            *[
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        statement.right.alias,
                        right_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                )
                for field, value in statement.right.properties
            ],
        ],
        graph_schema=graph_schema,
        left_label=statement.left.label,
        right_label=statement.right.label,
        require_label_message=(
            "Type-aware MERGE relationship lowering requires explicit endpoint labels."
        ),
        mismatch_message=(
            "Type-aware MERGE relationship lowering requires endpoint labels to match the schema contract."
        ),
        merge=True,
    )
    return CompiledCypherProgram(
        steps=(
            CompiledCypherStatement(
                sql=parse_one(
                    _compile_type_aware_merge_node_sql(
                        statement.left,
                        graph_schema,
                    )
                )
            ),
            CompiledCypherStatement(
                sql=parse_one(
                    _compile_type_aware_merge_node_sql(
                        statement.right,
                        graph_schema,
                    )
                )
            ),
            CompiledCypherStatement(sql=parse_one(relationship_sql)),
        )
    )


def _compile_merge_relationship_self_loop_program(
    statement: GraphRelationalMergeRelationshipWriteIR,
    graph_schema: GraphSchema,
) -> CompiledCypherProgram:
    from .compile import _compile_type_aware_merge_node_sql

    return CompiledCypherProgram(
        steps=(
            CompiledCypherStatement(
                sql=parse_one(
                    _compile_type_aware_merge_node_sql(
                        statement.left,
                        graph_schema,
                    )
                )
            ),
            CompiledCypherStatement(
                sql=parse_one(
                    _compile_type_aware_merge_self_loop_edge_sql(
                        statement.relationship,
                        statement.left,
                        graph_schema,
                    )
                )
            ),
        )
    )


def _compile_type_aware_merge_self_loop_edge_sql(
    relationship: RelationshipPattern,
    node: NodePattern,
    graph_schema: GraphSchema,
) -> str:
    from .compile import (
        _assemble_select_sql,
        _assemble_insert_select_sql,
        _compile_type_aware_edge_field_expression,
        _compile_type_aware_node_field_expression,
        _compile_type_aware_predicate,
        _require_single_relationship_type,
        _resolve_type_aware_property_column,
        _sql_value,
    )

    alias = node.alias
    if node.label is None:
        raise ValueError(
            "Type-aware MERGE relationship lowering requires an explicit endpoint label."
        )
    node_type = graph_schema.node_type(node.label)
    edge_type = graph_schema.edge_type(_require_single_relationship_type(relationship))
    if node_type.name != edge_type.source_type or node_type.name != edge_type.target_type:
        raise ValueError(
            "Type-aware MERGE relationship self-loop lowering requires the "
            "edge schema to use the same source and target node type."
        )

    node_id_sql = f"{alias}.id"
    node_where_parts = [
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
    exists_where = [
        f"existing_merge_edge.from_id = {node_id_sql}",
        f"existing_merge_edge.to_id = {node_id_sql}",
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
    node_where_parts.append(
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
    columns = ["from_id", "to_id"]
    values = [node_id_sql, node_id_sql]
    for field, value in relationship.properties:
        columns.append(_resolve_type_aware_property_column(edge_type, field))
        values.append(_sql_value(value))
    return _assemble_insert_select_sql(
        target_sql=f"INSERT INTO {edge_type.table_name} ({', '.join(columns)})",
        select_sql=f"SELECT {', '.join(values)}",
        from_sql=f"FROM {node_type.table_name} AS {alias}",
        where_parts=node_where_parts,
    )


def _compile_match_merge_relationship_sql(
    statement: GraphRelationalMatchMergeRelationshipWriteIR,
    graph_schema: GraphSchema,
) -> str:
    from .compile import (
        _assemble_select_sql,
        _assemble_insert_select_sql,
        _compile_type_aware_edge_field_expression,
        _compile_type_aware_match_node_predicate,
        _compile_type_aware_node_field_expression,
        _compile_type_aware_predicate,
        _match_create_endpoint_id_sql,
        _require_single_relationship_type,
        _resolve_type_aware_property_column,
        _resolve_write_endpoint_node_pattern,
        _sql_value,
    )

    matched_aliases = {statement.left_match.alias, statement.right_match.alias}
    if (
        statement.left.alias not in matched_aliases
        or statement.right.alias not in matched_aliases
    ):
        raise ValueError(
            "CypherGlot MATCH ... MERGE compilation currently supports only "
            "relationship merges between already matched node aliases."
        )

    left_alias = statement.left_match.alias
    right_alias = statement.right_match.alias

    if statement.left_match.label is None or statement.right_match.label is None:
        raise ValueError(
            "Type-aware MATCH ... MERGE lowering requires explicit node labels for matched endpoints."
        )

    left_endpoint = _resolve_write_endpoint_node_pattern(
        statement.left,
        statement.left_match,
    )
    right_endpoint = _resolve_write_endpoint_node_pattern(
        statement.right,
        statement.right_match,
    )
    left_type = graph_schema.node_type(statement.left_match.label)
    right_type = graph_schema.node_type(statement.right_match.label)
    relationship_type = _require_single_relationship_type(statement.relationship)
    edge_type = graph_schema.edge_type(relationship_type)
    source_label = left_endpoint.label
    target_label = right_endpoint.label
    if statement.relationship.direction == "in":
        source_label, target_label = target_label, source_label
    if source_label != edge_type.source_type or target_label != edge_type.target_type:
        raise ValueError(
            "Type-aware MATCH ... MERGE lowering requires endpoint labels to match the schema contract."
        )

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
        for field, value in statement.left_match.properties
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
        for field, value in statement.right_match.properties
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
        raise ValueError(
            "Type-aware MATCH ... MERGE lowering currently supports predicates only on the matched node aliases."
        )

    exists_where = [
        f"existing_merge_edge.from_id = {_match_create_endpoint_id_sql(statement.relationship.direction, 'from', statement.left.alias, statement.right.alias)}",
        f"existing_merge_edge.to_id = {_match_create_endpoint_id_sql(statement.relationship.direction, 'to', statement.left.alias, statement.right.alias)}",
    ]
    for field, value in statement.relationship.properties:
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
    where_parts.append(
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

    target_columns = ["from_id", "to_id"]
    select_values = [
        _match_create_endpoint_id_sql(
            statement.relationship.direction,
            "from",
            statement.left.alias,
            statement.right.alias,
        ),
        _match_create_endpoint_id_sql(
            statement.relationship.direction,
            "to",
            statement.left.alias,
            statement.right.alias,
        ),
    ]
    for field, value in statement.relationship.properties:
        target_columns.append(_resolve_type_aware_property_column(edge_type, field))
        select_values.append(_sql_value(value))

    return _assemble_insert_select_sql(
        target_sql=(
            f"INSERT INTO {edge_type.table_name} ({', '.join(target_columns)})"
        ),
        select_sql=f"SELECT {', '.join(select_values)}",
        from_sql=(
            f"FROM {left_type.table_name} AS {left_alias}, "
            f"{right_type.table_name} AS {right_alias}"
        ),
        where_parts=where_parts,
    )




def _compile_match_create_relationship_between_nodes_sql(
    statement: GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR,
    graph_schema: GraphSchema,
) -> str:
    from .compile import (
        _assemble_insert_select_sql,
        _compile_type_aware_match_node_predicate,
        _compile_type_aware_node_field_expression,
        _compile_type_aware_predicate,
        _match_create_endpoint_id_sql,
        _require_single_relationship_type,
        _resolve_type_aware_property_column,
        _resolve_write_endpoint_node_pattern,
        _sql_value,
    )

    matched_aliases = {statement.left_match.alias, statement.right_match.alias}
    if (
        statement.left.alias not in matched_aliases
        or statement.right.alias not in matched_aliases
    ):
        raise ValueError(
            "CypherGlot MATCH ... CREATE compilation currently supports only "
            "relationship creation between already matched node aliases."
        )

    left_alias = statement.left_match.alias
    right_alias = statement.right_match.alias

    if statement.left_match.label is None or statement.right_match.label is None:
        raise ValueError(
            "Type-aware MATCH ... CREATE lowering requires explicit node labels for matched endpoints."
        )

    left_endpoint = _resolve_write_endpoint_node_pattern(
        statement.left,
        statement.left_match,
    )
    right_endpoint = _resolve_write_endpoint_node_pattern(
        statement.right,
        statement.right_match,
    )
    left_type = graph_schema.node_type(statement.left_match.label)
    right_type = graph_schema.node_type(statement.right_match.label)
    relationship_type = _require_single_relationship_type(statement.relationship)
    edge_type = graph_schema.edge_type(relationship_type)
    source_label = left_endpoint.label
    target_label = right_endpoint.label
    if statement.relationship.direction == "in":
        source_label, target_label = target_label, source_label
    if source_label != edge_type.source_type or target_label != edge_type.target_type:
        raise ValueError(
            "Type-aware MATCH ... CREATE lowering requires endpoint labels to match the schema contract."
        )

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
        for field, value in statement.left_match.properties
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
        for field, value in statement.right_match.properties
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
        raise ValueError(
            "Type-aware MATCH ... CREATE lowering currently supports predicates only on the matched node aliases."
        )

    target_columns = ["from_id", "to_id"]
    select_values = [
        _match_create_endpoint_id_sql(
            statement.relationship.direction,
            "from",
            statement.left.alias,
            statement.right.alias,
        ),
        _match_create_endpoint_id_sql(
            statement.relationship.direction,
            "to",
            statement.left.alias,
            statement.right.alias,
        ),
    ]
    for field, value in statement.relationship.properties:
        target_columns.append(_resolve_type_aware_property_column(edge_type, field))
        select_values.append(_sql_value(value))

    return _assemble_insert_select_sql(
        target_sql=(
            f"INSERT INTO {edge_type.table_name} ({', '.join(target_columns)})"
        ),
        select_sql=f"SELECT {', '.join(select_values)}",
        from_sql=(
            f"FROM {left_type.table_name} AS {left_alias}, "
            f"{right_type.table_name} AS {right_alias}"
        ),
        where_parts=where_parts,
    )