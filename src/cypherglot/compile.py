"""Compile normalized CypherGlot statements into SQLGlot AST."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from sqlglot import exp, parse_one

from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    _ParameterRef,
    CaseSpec,
    CypherValue,
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
    ReturnItem,
    SetItem,
)
from .normalize import (
    NormalizedCreateNode,
    NormalizedCreateRelationship,
    NormalizedCreateRelationshipFromSeparatePatterns,
    NormalizedCypherStatement,
    NormalizedDeleteNode,
    NormalizedDeleteRelationship,
    NormalizedMergeNode,
    NormalizedMergeRelationship,
    NormalizedMatchCreateRelationship,
    NormalizedMatchCreateRelationshipFromTraversal,
    NormalizedMatchCreateRelationshipBetweenNodes,
    NormalizedMatchChain,
    NormalizedMatchMergeRelationship,
    NormalizedMatchMergeRelationshipFromTraversal,
    NormalizedMatchNode,
    NormalizedOptionalMatchNode,
    NormalizedMatchRelationship,
    NormalizedMatchWithReturn,
    NormalizedUnwind,
    NormalizedQueryNodesVectorSearch,
    NormalizedSetNode,
    NormalizedSetRelationship,
    WithOrderItem,
    WithPredicate,
    WithReturnItem,
    WithBinding,
    WithCaseSpec,
    normalize_cypher_parse_result,
)
from .parser import CypherParseResult, parse_cypher_text


@dataclass(frozen=True, slots=True)
class CompiledCypherStatement:
    sql: exp.Expression
    bind_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompiledCypherLoop:
    source: exp.Expression
    row_bindings: tuple[str, ...]
    body: tuple[CompiledCypherStatement, ...]


CompiledCypherProgramStep = CompiledCypherStatement | CompiledCypherLoop


@dataclass(frozen=True, slots=True)
class CompiledCypherProgram:
    steps: tuple[CompiledCypherProgramStep, ...]


_AGGREGATE_SQL_NAMES: dict[str, str] = {
    "count": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
}


def compile_cypher_text(text: str) -> exp.Expression:
    """Parse, normalize, and compile one admitted Cypher statement."""

    return _require_single_statement_program(compile_cypher_program_text(text))


def compile_cypher_parse_result(result: CypherParseResult) -> exp.Expression:
    """Compile one parse result into a SQLGlot expression tree."""

    return _require_single_statement_program(
        compile_cypher_program_parse_result(result)
    )


def compile_cypher_program_text(text: str) -> CompiledCypherProgram:
    """Parse, normalize, and compile one Cypher statement into a SQL program."""

    return compile_cypher_program_parse_result(parse_cypher_text(text))


def compile_cypher_program_parse_result(
    result: CypherParseResult,
) -> CompiledCypherProgram:
    """Compile one parse result into a SQLGlot-backed program."""

    return compile_normalized_cypher_program(normalize_cypher_parse_result(result))


def compile_normalized_cypher_statement(
    statement: NormalizedCypherStatement,
) -> exp.Expression:
    """Compile one normalized statement into a SQLGlot expression tree."""

    return _require_single_statement_program(
        compile_normalized_cypher_program(statement)
    )


def compile_normalized_cypher_program(
    statement: NormalizedCypherStatement,
) -> CompiledCypherProgram:
    """Compile one normalized statement into a SQLGlot-backed program."""

    if isinstance(statement, NormalizedCreateNode):
        return CompiledCypherProgram(
            steps=_compile_create_node_steps(statement.node, "created_node_id")
        )

    if isinstance(statement, NormalizedCreateRelationship):
        return _compile_create_relationship_program(statement)

    if isinstance(statement, NormalizedCreateRelationshipFromSeparatePatterns):
        return _compile_create_relationship_from_separate_patterns_program(statement)

    if isinstance(statement, NormalizedMergeNode):
        return _compile_merge_node_program(statement)

    if isinstance(statement, NormalizedMergeRelationship):
        return _compile_merge_relationship_program(statement)

    if isinstance(statement, NormalizedMatchNode):
        return _single_statement_program(_compile_match_node_sql(statement))

    if isinstance(statement, NormalizedOptionalMatchNode):
        return _single_statement_program(_compile_optional_match_node_sql(statement))

    if isinstance(statement, NormalizedMatchRelationship):
        return _single_statement_program(_compile_match_relationship_sql(statement))

    if isinstance(statement, NormalizedMatchChain):
        return _single_statement_program(_compile_match_chain_sql(statement))

    if isinstance(statement, NormalizedMatchWithReturn):
        return _single_statement_program(_compile_match_with_return_sql(statement))

    if isinstance(statement, NormalizedUnwind):
        return _single_statement_program(_compile_unwind_sql(statement))

    if isinstance(statement, NormalizedQueryNodesVectorSearch):
        raise ValueError(
            "CypherGlot vector-aware queryNodes normalization carries vector intent "
            "forward for host runtimes, but it does not yet compile vector-aware "
            "CALL queries into SQLGlot output."
        )

    if isinstance(statement, NormalizedSetNode):
        return _single_statement_program(_compile_set_node_sql(statement))

    if isinstance(statement, NormalizedSetRelationship):
        return _single_statement_program(_compile_set_relationship_sql(statement))

    if isinstance(statement, NormalizedDeleteNode):
        return _single_statement_program(_compile_delete_node_sql(statement))

    if isinstance(statement, NormalizedDeleteRelationship):
        return _single_statement_program(_compile_delete_relationship_sql(statement))

    if isinstance(statement, NormalizedMatchMergeRelationship):
        return _single_statement_program(
            _compile_match_merge_relationship_sql(statement)
        )

    if isinstance(statement, NormalizedMatchMergeRelationshipFromTraversal):
        return _compile_match_merge_relationship_from_traversal_program(statement)

    if isinstance(statement, NormalizedMatchCreateRelationship):
        return _compile_match_create_relationship_program(statement)

    if isinstance(statement, NormalizedMatchCreateRelationshipFromTraversal):
        return _compile_match_create_relationship_from_traversal_program(statement)

    if isinstance(statement, NormalizedMatchCreateRelationshipBetweenNodes):
        return _single_statement_program(
            _compile_match_create_relationship_between_nodes_sql(statement)
        )

    raise ValueError(
        "CypherGlot MVP compilation currently supports admitted standalone CREATE, "
        "MATCH ... RETURN, narrow UNWIND ... RETURN, MATCH ... SET, MATCH ... DELETE, and a narrow MATCH ... "
        "CREATE or MATCH ... MERGE relationship subset over one node pattern, one directed "
        "relationship pattern, or one fixed-length directed relationship chain."
    )


def _single_statement_program(sql: str) -> CompiledCypherProgram:
    return CompiledCypherProgram(steps=(CompiledCypherStatement(parse_one(sql)),))


def _require_single_statement_program(program: CompiledCypherProgram) -> exp.Expression:
    if len(program.steps) != 1:
        raise ValueError(
            "This Cypher shape compiles to a multi-step SQL program; use "
            "compile_cypher_program_text(...) instead."
        )

    step = program.steps[0]
    if not isinstance(step, CompiledCypherStatement) or step.bind_columns:
        raise ValueError(
            "This Cypher shape compiles to a multi-step SQL program; use "
            "compile_cypher_program_text(...) instead."
        )
    return step.sql


def _compile_match_node_sql(statement: NormalizedMatchNode) -> str:
    alias = statement.node.alias
    joins: list[str] = []
    where_parts: list[str] = []

    _append_node_label_join(
        joins=joins,
        node_alias=alias,
        label=statement.node.label,
        join_alias=f"{alias}_label_0",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=alias,
        alias_kind="node",
        properties=statement.node.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=statement.predicates,
    )

    select_sql = _compile_select_list(
        returns=statement.returns,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
    )
    order_sql = _compile_order_by(
        order_by=statement.order_by,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        returns=statement.returns,
    )
    group_sql = _compile_group_by(returns=statement.returns, alias_map={alias: alias}, alias_kinds={alias: "node"})

    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM nodes AS {alias}",
        joins=joins,
        where_parts=where_parts,
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_match_relationship_sql(statement: NormalizedMatchRelationship) -> str:
    if _is_variable_length_relationship(statement.relationship):
        return _compile_variable_length_match_relationship_sql(statement)

    from_sql, joins, where_parts, alias_map, alias_kinds = (
        _compile_match_relationship_source_components(statement)
    )
    select_sql = _compile_select_list(
        returns=statement.returns,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
    )
    order_sql = _compile_order_by(
        order_by=statement.order_by,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        returns=statement.returns,
    )
    group_sql = _compile_group_by(
        returns=statement.returns,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
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


def _compile_match_relationship_source_components(
    statement: NormalizedMatchRelationship,
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
    where_parts: list[str] = []

    _append_node_label_join(
        joins=joins,
        node_alias=left_alias,
        label=statement.left.label,
        join_alias=f"{left_alias}_label_0",
    )
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
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=right_alias,
        alias_kind="node",
        properties=statement.right.properties,
    )

    alias_map = {
        left_alias: left_alias,
        right_alias: right_alias,
    }
    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        left_alias: "node",
        right_alias: "node",
    }
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


def _compile_match_chain_sql(statement: NormalizedMatchChain) -> str:
    from_sql, joins, where_parts, alias_map, alias_kinds = _compile_chain_source_components(
        nodes=statement.nodes,
        relationships=statement.relationships,
        predicates=statement.predicates,
    )
    select_sql = _compile_select_list(
        returns=statement.returns,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
    )
    order_sql = _compile_order_by(
        order_by=statement.order_by,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        returns=statement.returns,
    )
    group_sql = _compile_group_by(
        returns=statement.returns,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
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


def _compile_optional_match_node_sql(statement: NormalizedOptionalMatchNode) -> str:
    alias = statement.node.alias
    on_parts = ["1 = 1"]

    if statement.node.label is not None:
        on_parts.append(
            "EXISTS ("
            f"SELECT 1 FROM node_labels AS {alias}_label_filter "
            f"WHERE {alias}_label_filter.node_id = {alias}.id "
            f"AND {alias}_label_filter.label = {_sql_literal(statement.node.label)}"
            ")"
        )

    _extend_pattern_property_filters(
        where_parts=on_parts,
        alias=alias,
        alias_kind="node",
        properties=statement.node.properties,
    )
    _append_predicate_filters(
        where_parts=on_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=statement.predicates,
    )

    select_sql = _compile_select_list(
        returns=statement.returns,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
    )
    order_sql = _compile_order_by(
        order_by=statement.order_by,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        returns=statement.returns,
    )
    group_sql = _compile_group_by(
        returns=statement.returns,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
    )

    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql="FROM (SELECT 1 AS __cg_seed) AS seed",
        joins=[f"LEFT JOIN nodes AS {alias} ON {' AND '.join(on_parts)}"],
        where_parts=[],
        group_sql=group_sql,
        order_sql=order_sql,
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_set_node_sql(statement: NormalizedSetNode) -> str:
    alias = statement.node.alias
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=alias,
        label=statement.node.label,
        filter_alias=f"{alias}_label_filter_0",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=alias,
        alias_kind="node",
        properties=statement.node.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=statement.predicates,
    )

    assignments_sql = _compile_json_set_assignments(
        target_alias=alias,
        alias_kind="node",
        assignments=statement.assignments,
    )

    return _assemble_update_sql(
        target_sql=f"UPDATE nodes AS {alias}",
        assignments_sql=assignments_sql,
        from_sql=None,
        where_parts=where_parts,
    )


def _compile_match_with_return_sql(statement: NormalizedMatchWithReturn) -> str:
    inner_sql = _compile_with_source_sql(statement)
    select_sql = _compile_with_select_list(statement.returns, statement.bindings)
    order_sql = _compile_with_order_by(statement.order_by, statement.bindings)
    group_sql = _compile_with_group_by(statement.returns, statement.bindings)
    where_parts = _compile_with_predicates(statement.predicates, statement.bindings)
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


def _compile_unwind_sql(statement: NormalizedUnwind) -> str:
    binding = WithBinding(
        source_alias=statement.alias,
        output_alias=statement.alias,
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


def _compile_unwind_source_sql(statement: NormalizedUnwind) -> str:
    source_sql = (
        f":{statement.source_param_name}"
        if statement.source_kind == "parameter"
        else _compile_unwind_literal_source(statement.source_items)
    )
    return (
        f"SELECT unwind_q.value AS \"{_with_scalar_prefix(statement.alias)}\" "
        f"FROM JSON_EACH({source_sql}) AS unwind_q"
    )


def _compile_unwind_literal_source(items: tuple[CypherValue, ...]) -> str:
    if not items:
        return "JSON_ARRAY()"
    return f"JSON_ARRAY({', '.join(_sql_value(item) for item in items)})"


def _compile_with_source_sql(statement: NormalizedMatchWithReturn) -> str:
    source = statement.source
    if isinstance(source, NormalizedMatchNode):
        alias = source.node.alias
        joins: list[str] = []
        where_parts: list[str] = []
        _append_node_label_join(
            joins=joins,
            node_alias=alias,
            label=source.node.label,
            join_alias=f"{alias}_label_0",
        )
        _extend_pattern_property_filters(
            where_parts=where_parts,
            alias=alias,
            alias_kind="node",
            properties=source.node.properties,
        )
        _append_predicate_filters(
            where_parts=where_parts,
            alias_map={alias: alias},
            alias_kinds={alias: "node"},
            predicates=source.predicates,
        )
        select_sql = ", ".join(_compile_with_binding_columns(binding) for binding in statement.bindings)
        return _assemble_select_sql(
            select_sql=select_sql,
            distinct=False,
            from_sql=f"FROM nodes AS {alias}",
            joins=joins,
            where_parts=where_parts,
            order_sql=None,
            limit=None,
            skip=None,
        )

    if isinstance(source, NormalizedMatchChain):
        from_sql, joins, where_parts, alias_map, _ = _compile_chain_source_components(
            nodes=source.nodes,
            relationships=source.relationships,
            predicates=source.predicates,
        )
        select_sql = ", ".join(
            _compile_with_binding_columns(binding, table_alias_map=alias_map)
            for binding in statement.bindings
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

    if _is_variable_length_relationship(source.relationship):
        return _compile_variable_length_with_source_sql(source, statement.bindings)

    if isinstance(source, NormalizedMatchRelationship):
        from_sql, joins, where_parts, alias_map, _ = (
            _compile_match_relationship_source_components(source)
        )
        select_sql = ", ".join(
            _compile_with_binding_columns(binding, table_alias_map=alias_map)
            for binding in statement.bindings
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
    edge_aliases = [relationship.alias or f"__cg_edge_{index}" for index, relationship in enumerate(relationships)]
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
                f"JOIN nodes AS {left_alias} ON {left_alias}.id = {edge_alias}.{_edge_endpoint_column(relationship.direction, 'left')}"
            )
        else:
            joins.append(
                f"JOIN edges AS {edge_alias} ON {nodes[index].alias}.id = {edge_alias}.{_edge_endpoint_column(relationship.direction, 'left')}"
            )
        joins.append(
            f"JOIN nodes AS {right_alias} ON {right_alias}.id = {edge_alias}.{_edge_endpoint_column(relationship.direction, 'right')}"
        )
        _append_relationship_type_filter_for_alias(where_parts, relationship, edge_alias)
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

    return f"FROM edges AS {edge_aliases[0]}", joins, where_parts, alias_map, alias_kinds


def _is_variable_length_relationship(relationship: RelationshipPattern) -> bool:
    return relationship.min_hops != 1 or relationship.max_hops != 1


def _compile_variable_length_match_relationship_sql(
    statement: NormalizedMatchRelationship,
) -> str:
    if _supports_direct_variable_length_aggregate_return(statement.returns):
        return _compile_variable_length_aggregate_match_relationship_sql(statement)

    branch_sql: list[str] = []
    if statement.relationship.min_hops == 0:
        branch_sql.append(
            _compile_zero_hop_variable_length_branch_sql(
                statement=statement,
                returns=statement.returns,
            )
        )
    branch_sql.extend(
        _compile_match_chain_sql(branch)
        for branch in _expand_variable_length_relationship_branches(statement)
    )
    order_sql = _compile_projected_order_by(
        order_by=statement.order_by,
        returns=statement.returns,
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


def _compile_variable_length_with_source_sql(
    source: NormalizedMatchRelationship,
    bindings: tuple[WithBinding, ...],
) -> str:
    branch_sql: list[str] = []
    if source.relationship.min_hops == 0:
        from_sql, joins, where_parts, alias_map, _ = (
            _compile_zero_hop_variable_length_source_components(source)
        )
        select_sql = ", ".join(
            _compile_with_binding_columns(binding, table_alias_map=alias_map)
            for binding in bindings
        )
        branch_sql.append(
            _assemble_select_sql(
                select_sql=select_sql,
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )
    for branch in _expand_variable_length_relationship_branches(source, returns=()):
        from_sql, joins, where_parts, alias_map, _ = _compile_chain_source_components(
            nodes=branch.nodes,
            relationships=branch.relationships,
            predicates=branch.predicates,
        )
        select_sql = ", ".join(
            _compile_with_binding_columns(binding, table_alias_map=alias_map)
            for binding in bindings
        )
        branch_sql.append(
            _assemble_select_sql(
                select_sql=select_sql,
                distinct=False,
                from_sql=from_sql,
                joins=joins,
                where_parts=where_parts,
                order_sql=None,
                limit=None,
                skip=None,
            )
        )

    return " UNION ALL ".join(branch_sql)


def _supports_direct_variable_length_aggregate_return(
    returns: tuple[ReturnItem, ...],
) -> bool:
    return any(item.kind in _AGGREGATE_SQL_NAMES for item in returns) and all(
        item.kind in _AGGREGATE_SQL_NAMES or item.kind == "field"
        for item in returns
    )


def _compile_variable_length_aggregate_match_relationship_sql(
    statement: NormalizedMatchRelationship,
) -> str:
    branch_sql: list[str] = []
    if statement.relationship.min_hops == 0:
        from_sql, joins, where_parts, alias_map, alias_kinds = (
            _compile_zero_hop_variable_length_source_components(statement)
        )
        branch_sql.append(
            _assemble_select_sql(
                select_sql=_compile_variable_length_aggregate_branch_select_list(
                    statement.returns,
                    alias_map,
                    alias_kinds,
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

    for branch in _expand_variable_length_relationship_branches(statement, returns=()):
        from_sql, joins, where_parts, alias_map, alias_kinds = _compile_chain_source_components(
            nodes=branch.nodes,
            relationships=branch.relationships,
            predicates=branch.predicates,
        )
        branch_sql.append(
            _assemble_select_sql(
                select_sql=_compile_variable_length_aggregate_branch_select_list(
                    statement.returns,
                    alias_map,
                    alias_kinds,
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
        _compile_variable_length_outer_projection(item, index)
        for index, item in enumerate(statement.returns)
    )
    return _assemble_select_sql(
        select_sql=select_sql,
        distinct=statement.distinct,
        from_sql=f"FROM ({' UNION ALL '.join(branch_sql)}) AS variable_length_q",
        joins=[],
        where_parts=[],
        group_sql=_compile_variable_length_outer_group_by(statement.returns),
        order_sql=_compile_projected_order_by(
            order_by=statement.order_by,
            returns=statement.returns,
            table_alias=None,
        ),
        limit=statement.limit,
        skip=statement.skip,
    )


def _compile_variable_length_aggregate_branch_select_list(
    returns: tuple[ReturnItem, ...],
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    return ", ".join(
        _compile_variable_length_branch_projection(item, index, alias_map, alias_kinds)
        for index, item in enumerate(returns)
    )


def _compile_variable_length_branch_projection(
    item: ReturnItem,
    index: int,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> str:
    if item.kind == "field":
        return (
            f'{_compile_return_expression(item, alias_map, alias_kinds)} '
            f'AS "{item.column_name}"'
        )
    if item.kind == "count":
        if item.alias == "*":
            return f'1 AS "{_variable_length_aggregate_hidden_column(index)}"'
        return f'{alias_map[item.alias]}.id AS "{_variable_length_aggregate_hidden_column(index)}"'
    return (
        f'{_compile_return_expression(ReturnItem(alias=item.alias, field=item.field, kind="field"), alias_map, alias_kinds)} '
        f'AS "{_variable_length_aggregate_hidden_column(index)}"'
    )


def _compile_variable_length_outer_projection(
    item: ReturnItem,
    index: int,
) -> str:
    if item.kind == "field":
        return f'variable_length_q."{item.column_name}" AS "{item.column_name}"'
    aggregate_column = _variable_length_aggregate_hidden_column(index)
    aggregate_sql = (
        "COUNT(*)"
        if item.kind == "count" and item.alias == "*"
        else f'{_AGGREGATE_SQL_NAMES[item.kind]}(variable_length_q."{aggregate_column}")'
    )
    return f'{aggregate_sql} AS "{item.column_name}"'


def _compile_variable_length_outer_group_by(
    returns: tuple[ReturnItem, ...],
) -> str | None:
    group_items = [
        f'variable_length_q."{item.column_name}"'
        for item in returns
        if item.kind == "field"
    ]
    if not group_items:
        return None
    return ", ".join(group_items)


def _variable_length_aggregate_hidden_column(index: int) -> str:
    return f"__cg_aggregate_{index}"


def _expand_variable_length_relationship_branches(
    statement: NormalizedMatchRelationship,
    *,
    returns: tuple[ReturnItem, ...] | None = None,
) -> tuple[NormalizedMatchChain, ...]:
    max_hops = statement.relationship.max_hops
    assert max_hops is not None

    branches: list[NormalizedMatchChain] = []
    base_relationship = RelationshipPattern(
        alias=None,
        type_name=statement.relationship.type_name,
        direction=statement.relationship.direction,
        properties=statement.relationship.properties,
    )
    branch_returns = statement.returns if returns is None else returns

    for hop_count in range(max(1, statement.relationship.min_hops), max_hops + 1):
        nodes = [statement.left]
        for index in range(1, hop_count):
            nodes.append(
                NodePattern(
                    alias=f"__cg_variable_hop_{hop_count}_node_{index}",
                    label=None,
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
            )
        )

    return tuple(branches)


def _compile_zero_hop_variable_length_source_components(
    statement: NormalizedMatchRelationship,
) -> tuple[
    str,
    list[str],
    list[str],
    dict[str, str],
    dict[str, Literal["node", "relationship"]],
]:
    node_alias = "__cg_zero_hop_node"
    joins: list[str] = []
    where_parts: list[str] = []

    _append_node_label_join(
        joins=joins,
        node_alias=node_alias,
        label=statement.left.label,
        join_alias=f"{node_alias}_left_label_0",
    )
    _append_node_label_join(
        joins=joins,
        node_alias=node_alias,
        label=statement.right.label,
        join_alias=f"{node_alias}_right_label_1",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=node_alias,
        alias_kind="node",
        properties=statement.left.properties,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=node_alias,
        alias_kind="node",
        properties=statement.right.properties,
    )

    alias_map = {
        statement.left.alias: node_alias,
        statement.right.alias: node_alias,
    }
    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        statement.left.alias: "node",
        statement.right.alias: "node",
    }
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        predicates=statement.predicates,
    )

    return f"FROM nodes AS {node_alias}", joins, where_parts, alias_map, alias_kinds


def _compile_zero_hop_variable_length_branch_sql(
    *,
    statement: NormalizedMatchRelationship,
    returns: tuple[ReturnItem, ...],
) -> str:
    from_sql, joins, where_parts, alias_map, alias_kinds = (
        _compile_zero_hop_variable_length_source_components(statement)
    )
    return _assemble_select_sql(
        select_sql=_compile_select_list(
            returns=returns,
            alias_map=alias_map,
            alias_kinds=alias_kinds,
        ),
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        group_sql=_compile_group_by(
            returns=returns,
            alias_map=alias_map,
            alias_kinds=alias_kinds,
        ),
        order_sql=None,
        limit=None,
        skip=None,
    )


def _compile_projected_order_by(
    *,
    order_by: tuple[OrderItem, ...],
    returns: tuple[ReturnItem, ...],
    table_alias: str | None,
) -> str | None:
    if not order_by:
        return None

    return ", ".join(
        (
            f'{table_alias}."{_projected_order_column_name(item, returns)}" {item.direction.upper()}'
            if table_alias is not None
            else f'"{_projected_order_column_name(item, returns)}" {item.direction.upper()}'
        )
        for item in order_by
    )


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
        f"Unknown projected ORDER BY item for variable-length relationship read: {item.alias}.{item.field}"
    )


def _compile_with_binding_columns(
    binding: WithBinding,
    *,
    relationship_alias: str | None = None,
    left_alias: str | None = None,
    right_alias: str | None = None,
    table_alias_map: dict[str, str] | None = None,
) -> str:
    source_table_alias = table_alias_map.get(binding.source_alias, binding.source_alias) if table_alias_map is not None else binding.source_alias
    if table_alias_map is None and relationship_alias is not None and binding.alias_kind == "relationship":
        source_table_alias = relationship_alias
    elif table_alias_map is None and relationship_alias is not None and binding.source_alias == left_alias:
        source_table_alias = left_alias or binding.source_alias
    elif table_alias_map is None and relationship_alias is not None and binding.source_alias == right_alias:
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

    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"} and item.value is not None:
        return f"{item.kind.upper()}({_sql_value(item.value)})"

    if item.kind in {"left", "right"} and item.value is not None:
        assert item.length_value is not None
        return f"{item.kind.upper()}({_sql_value(item.value)}, {_sql_value(item.length_value)})"

    if item.kind == "split" and item.value is not None:
        assert item.delimiter_value is not None
        return f"SPLIT({_sql_value(item.value)}, {_sql_value(item.delimiter_value)})"

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
            return f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1), {length_sql})"

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
        properties_expression = (
            f'(SELECT {node_alias}.properties FROM nodes AS {node_alias} '
            f'WHERE {node_alias}.id = {node_id_expression})'
        )
        return _compile_node_json_object_from_id_expression(
            entity_alias=f"{binding.output_alias}_start",
            node_id_expression=node_id_expression,
            properties_expression=properties_expression,
        )

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
        properties_expression = (
            f'(SELECT {node_alias}.properties FROM nodes AS {node_alias} '
            f'WHERE {node_alias}.id = {node_id_expression})'
        )
        return _compile_node_json_object_from_id_expression(
            entity_alias=f"{binding.output_alias}_end",
            node_id_expression=node_id_expression,
            properties_expression=properties_expression,
        )

    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
        function_name = item.kind.upper()
        if binding.binding_kind == "scalar" and item.field is None:
            return f'{function_name}(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"{function_name}({inner})"

    if item.kind == "abs":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'ABS(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"ABS({inner})"

    if item.kind == "sign":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'SIGN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"SIGN({inner})"

    if item.kind == "round":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'ROUND(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"ROUND({inner})"

    if item.kind == "ceil":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CEIL(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CEIL({inner})"

    if item.kind == "floor":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'FLOOR(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"FLOOR({inner})"

    if item.kind == "sqrt":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'SQRT(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"SQRT({inner})"

    if item.kind == "exp":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'EXP(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"EXP({inner})"

    if item.kind == "sin":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'SIN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"SIN({inner})"

    if item.kind == "cos":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'COS(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"COS({inner})"

    if item.kind == "tan":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'TAN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"TAN({inner})"

    if item.kind == "asin":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'ASIN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"ASIN({inner})"

    if item.kind == "acos":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'ACOS(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"ACOS({inner})"

    if item.kind == "atan":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'ATAN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"ATAN({inner})"

    if item.kind == "ln":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'LN(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"LN({inner})"

    if item.kind == "log":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'LOG(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"LOG({inner})"

    if item.kind == "log10":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'LOG10(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"LOG10({inner})"

    if item.kind == "radians":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'RADIANS(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"RADIANS({inner})"

    if item.kind == "degrees":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'DEGREES(with_q."{_with_scalar_prefix(binding.output_alias)}")'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"DEGREES({inner})"

    if item.kind == "to_string":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CAST(with_q."{_with_scalar_prefix(binding.output_alias)}" AS TEXT)'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CAST({inner} AS TEXT)"

    if item.kind == "to_integer":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CAST(with_q."{_with_scalar_prefix(binding.output_alias)}" AS INTEGER)'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CAST({inner} AS INTEGER)"

    if item.kind == "to_float":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CAST(with_q."{_with_scalar_prefix(binding.output_alias)}" AS REAL)'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CAST({inner} AS REAL)"

    if item.kind == "to_boolean":
        if binding.binding_kind == "scalar" and item.field is None:
            return f'CAST(with_q."{_with_scalar_prefix(binding.output_alias)}" AS BOOLEAN)'
        inner = _compile_with_non_aggregate_expression(
            WithReturnItem(kind="field", alias=item.alias, field=item.field),
            binding_map,
        )
        return f"CAST({inner} AS BOOLEAN)"

    if item.kind == "substring":
        assert item.start_value is not None
        start_sql = _sql_value(item.start_value)
        if item.length_value is None:
            if binding.binding_kind == "scalar" and item.field is None:
                return f'SUBSTRING(with_q."{_with_scalar_prefix(binding.output_alias)}", ({start_sql} + 1))'
            inner = _compile_with_non_aggregate_expression(
                WithReturnItem(kind="field", alias=item.alias, field=item.field),
                binding_map,
            )
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        length_sql = _sql_value(item.length_value)
        if binding.binding_kind == "scalar" and item.field is None:
            return f'SUBSTRING(with_q."{_with_scalar_prefix(binding.output_alias)}", ({start_sql} + 1), {length_sql})'
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
                binding = binding_map[item.alias]
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
    return f'JSON_EXTRACT(with_q."{prefix}_properties", { _sql_literal("$." + item.field) })'


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


def _with_entity_prefix(alias: str) -> str:
    return f"__cg_with_{alias}"


def _with_scalar_prefix(alias: str) -> str:
    return f"__cg_with_scalar_{alias}"


def _compile_with_entity_object_expression(binding: WithBinding) -> str:
    prefix = _with_entity_prefix(binding.output_alias)
    arguments = [
        f"'id', with_q.\"{prefix}_id\"",
    ]
    if binding.alias_kind == "node":
        arguments.append(
            "'label', "
            f"(SELECT {binding.output_alias}_label_return.label "
            f"FROM node_labels AS {binding.output_alias}_label_return "
            f"WHERE {binding.output_alias}_label_return.node_id = with_q.\"{prefix}_id\" "
            "LIMIT 1)"
        )
    elif binding.alias_kind == "relationship":
        arguments.append(f"'type', with_q.\"{prefix}_type\"")
    arguments.append(
        f"'properties', JSON(COALESCE(with_q.\"{prefix}_properties\", '{{}}'))"
    )
    return f"JSON_OBJECT({', '.join(arguments)})"


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
            f"LIMIT 1)"
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
        return _compile_stream_predicate(expression, None, predicate.operator, predicate.value)

    assert predicate.field is not None
    return _compile_with_field_predicate(binding, predicate.field, predicate.operator, predicate.value)


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

    expression = f'JSON_EXTRACT(with_q."{prefix}_properties", { _sql_literal("$." + field) })'
    type_expression = f'JSON_TYPE(with_q."{prefix}_properties", { _sql_literal("$." + field) })'
    return _compile_stream_predicate(expression, type_expression, operator, value)


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
            return f"{expression} IS NULL" if type_expression is None else f"({type_expression} IS NULL OR {type_expression} = 'null')"
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


def _compile_set_relationship_sql(statement: NormalizedSetRelationship) -> str:
    relationship_alias = statement.relationship.alias or "edge"
    left_alias = statement.left.alias
    right_alias = statement.right.alias
    where_parts = [
        (
            f"{left_alias}.id = {relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
        ),
        (
            f"{right_alias}.id = {relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'right')}"
        ),
    ]

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=left_alias,
        label=statement.left.label,
        filter_alias=f"{left_alias}_label_filter_0",
    )
    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=right_alias,
        label=statement.right.label,
        filter_alias=f"{right_alias}_label_filter_1",
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
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=right_alias,
        alias_kind="node",
        properties=statement.right.properties,
    )

    alias_map = {
        left_alias: left_alias,
        right_alias: right_alias,
    }
    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        left_alias: "node",
        right_alias: "node",
    }
    if statement.relationship.alias is not None:
        alias_map[statement.relationship.alias] = relationship_alias
        alias_kinds[statement.relationship.alias] = "relationship"

    _append_predicate_filters(
        where_parts=where_parts,
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        predicates=statement.predicates,
    )

    assignments_sql = _compile_json_set_assignments(
        target_alias=relationship_alias,
        alias_kind="relationship",
        assignments=statement.assignments,
    )

    return _assemble_update_sql(
        target_sql=f"UPDATE edges AS {relationship_alias}",
        assignments_sql=assignments_sql,
        from_sql=f"FROM nodes AS {left_alias}, nodes AS {right_alias}",
        where_parts=where_parts,
    )


def _compile_delete_node_sql(statement: NormalizedDeleteNode) -> str:
    alias = statement.node.alias
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=alias,
        label=statement.node.label,
        filter_alias=f"{alias}_label_filter_0",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=alias,
        alias_kind="node",
        properties=statement.node.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=statement.predicates,
    )

    return _assemble_delete_sql(
        target_sql=f"DELETE FROM nodes AS {alias}",
        using_sql=None,
        where_parts=where_parts,
    )


def _compile_delete_relationship_sql(statement: NormalizedDeleteRelationship) -> str:
    relationship_alias = statement.relationship.alias or "edge"
    from_sql, joins, where_parts, _alias_map, _alias_kinds = (
        _compile_match_relationship_source_components(statement)
    )
    matching_edge_ids_sql = _assemble_select_sql(
        select_sql=f"{relationship_alias}.id",
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )

    return _assemble_delete_sql(
        target_sql="DELETE FROM edges",
        using_sql=None,
        where_parts=[f"id IN ({matching_edge_ids_sql})"],
    )


def _compile_match_create_relationship_sql(
    statement: NormalizedMatchCreateRelationship,
) -> str:
    matched_aliases = {statement.match_node.alias}
    if (
        statement.left.alias not in matched_aliases
        or statement.right.alias not in matched_aliases
    ):
        raise ValueError(
            "CypherGlot MATCH ... CREATE compilation currently supports only "
            "relationship creation between already matched node aliases."
        )

    alias = statement.match_node.alias
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=alias,
        label=statement.match_node.label,
        filter_alias=f"{alias}_label_filter_0",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=alias,
        alias_kind="node",
        properties=statement.match_node.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=statement.predicates,
    )

    return _assemble_insert_select_sql(
        target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
        select_sql=(
            f"SELECT {_sql_literal(statement.relationship.type_name)}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'from',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'to',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_compile_json_object(statement.relationship.properties)}"
        ),
        from_sql=f"FROM nodes AS {alias}",
        where_parts=where_parts,
    )


def _compile_match_merge_relationship_sql(
    statement: NormalizedMatchMergeRelationship,
) -> str:
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
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=left_alias,
        label=statement.left_match.label,
        filter_alias=f"{left_alias}_label_filter_0",
    )
    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=right_alias,
        label=statement.right_match.label,
        filter_alias=f"{right_alias}_label_filter_1",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=left_alias,
        alias_kind="node",
        properties=statement.left_match.properties,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=right_alias,
        alias_kind="node",
        properties=statement.right_match.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={left_alias: left_alias, right_alias: right_alias},
        alias_kinds={left_alias: "node", right_alias: "node"},
        predicates=statement.predicates,
    )
    where_parts.append(
        _compile_relationship_absence_predicate(
            relationship=statement.relationship,
            from_id_sql=_match_create_endpoint_id_sql(
                statement.relationship.direction,
                "from",
                statement.left.alias,
                statement.right.alias,
            ),
            to_id_sql=_match_create_endpoint_id_sql(
                statement.relationship.direction,
                "to",
                statement.left.alias,
                statement.right.alias,
            ),
            existing_alias="existing_merge_edge",
        )
    )

    return _assemble_insert_select_sql(
        target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
        select_sql=(
            f"SELECT {_sql_literal(statement.relationship.type_name)}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'from',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'to',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_compile_json_object(statement.relationship.properties)}"
        ),
        from_sql=f"FROM nodes AS {left_alias}, nodes AS {right_alias}",
        where_parts=where_parts,
    )


def _compile_match_create_relationship_program(
    statement: NormalizedMatchCreateRelationship,
) -> CompiledCypherProgram:
    matched_aliases = {statement.match_node.alias}
    if (
        statement.left.alias in matched_aliases
        and statement.right.alias in matched_aliases
    ):
        return _single_statement_program(
            _compile_match_create_relationship_sql(statement)
        )

    new_endpoint = statement.left
    matched_endpoint = statement.right
    from_value = ":created_node_id"
    to_value = ":match_node_id"

    if statement.right.alias not in matched_aliases:
        new_endpoint = statement.right
        matched_endpoint = statement.left
        from_value = ":match_node_id"
        to_value = ":created_node_id"

    if matched_endpoint.alias != statement.match_node.alias:
        raise ValueError(
            "CypherGlot MATCH ... CREATE compilation currently supports only "
            "one matched node alias plus at most one fresh endpoint node."
        )

    if new_endpoint.label is None:
        raise ValueError(
            "CypherGlot MATCH ... CREATE compilation requires a label for any "
            "fresh endpoint node."
        )

    if statement.relationship.direction == "in":
        from_value, to_value = to_value, from_value

    source_sql = _compile_match_node_id_source_sql(
        node=statement.match_node,
        predicates=statement.predicates,
        binding_name="match_node_id",
    )
    create_node_sql = parse_one(
        "INSERT INTO nodes (properties) VALUES "
        f"({_compile_json_object(new_endpoint.properties)}) RETURNING id"
    )
    insert_label_sql = parse_one(
        "INSERT INTO node_labels (node_id, label) VALUES "
        f"(:created_node_id, {_sql_literal(new_endpoint.label)})"
    )
    insert_edge_sql = parse_one(
        "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
        f"({_sql_literal(statement.relationship.type_name)}, {from_value}, {to_value}, "
        f"{_compile_json_object(statement.relationship.properties)})"
    )

    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(source_sql),
                row_bindings=("match_node_id",),
                body=(
                    CompiledCypherStatement(
                        sql=create_node_sql,
                        bind_columns=("created_node_id",),
                    ),
                    CompiledCypherStatement(sql=insert_label_sql),
                    CompiledCypherStatement(sql=insert_edge_sql),
                ),
            ),
        )
    )


def _compile_create_relationship_program(
    statement: NormalizedCreateRelationship,
) -> CompiledCypherProgram:
    left_steps = _compile_create_node_steps(statement.left, "left_node_id")
    right_steps: tuple[CompiledCypherStatement, ...] = ()
    right_binding = ":left_node_id"

    if _create_relationship_uses_distinct_nodes(statement.left, statement.right):
        right_steps = _compile_create_node_steps(statement.right, "right_node_id")
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
    )

    return CompiledCypherProgram(steps=left_steps + right_steps + (edge_statement,))


def _compile_merge_node_program(
    statement: NormalizedMergeNode,
) -> CompiledCypherProgram:
    source_sql = _compile_merge_node_guard_source_sql(statement.node)
    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(source_sql),
                row_bindings=("merge_guard",),
                body=_compile_create_node_steps(statement.node, "merged_node_id"),
            ),
        )
    )


def _compile_merge_relationship_program(
    statement: NormalizedMergeRelationship,
) -> CompiledCypherProgram:
    if not _create_relationship_uses_distinct_nodes(statement.left, statement.right):
        raise ValueError(
            "CypherGlot MERGE relationship compilation currently supports only distinct endpoint aliases."
        )

    create_program = _compile_create_relationship_program(
        NormalizedCreateRelationship(
            kind="create",
            pattern_kind="relationship",
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    )
    body = tuple(
        step
        for step in create_program.steps
        if isinstance(step, CompiledCypherStatement)
    )
    if len(body) != len(create_program.steps):
        raise ValueError(
            "CypherGlot MERGE relationship compilation currently supports only direct create-step bodies."
        )

    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(_compile_merge_relationship_guard_source_sql(statement)),
                row_bindings=("merge_guard",),
                body=body,
            ),
        )
    )


def _compile_create_relationship_from_separate_patterns_program(
    statement: NormalizedCreateRelationshipFromSeparatePatterns,
) -> CompiledCypherProgram:
    first_steps = _compile_create_node_steps(statement.first_node, "first_node_id")
    second_steps = _compile_create_node_steps(statement.second_node, "second_node_id")
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
    )

    return CompiledCypherProgram(
        steps=first_steps + second_steps + (edge_statement,)
    )


def _compile_merge_node_guard_source_sql(node: NodePattern) -> str:
    alias = node.alias
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

    exists_sql = _assemble_select_sql(
        select_sql="1",
        distinct=False,
        from_sql=f"FROM nodes AS {alias}",
        joins=[],
        where_parts=where_parts,
        order_sql=None,
        limit=1,
        skip=None,
    )
    return f"SELECT 1 AS merge_guard WHERE NOT EXISTS ({exists_sql})"


def _compile_merge_relationship_guard_source_sql(
    statement: NormalizedMergeRelationship,
) -> str:
    relationship_alias = statement.relationship.alias or "merge_edge"
    left_alias = statement.left.alias
    right_alias = statement.right.alias
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
    where_parts: list[str] = []

    _append_node_label_join(
        joins=joins,
        node_alias=left_alias,
        label=statement.left.label,
        join_alias=f"{left_alias}_label_0",
    )
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
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=right_alias,
        alias_kind="node",
        properties=statement.right.properties,
    )

    exists_sql = _assemble_select_sql(
        select_sql="1",
        distinct=False,
        from_sql=f"FROM edges AS {relationship_alias}",
        joins=joins,
        where_parts=where_parts,
        order_sql=None,
        limit=1,
        skip=None,
    )
    return f"SELECT 1 AS merge_guard WHERE NOT EXISTS ({exists_sql})"


def _compile_match_create_relationship_between_nodes_sql(
    statement: NormalizedMatchCreateRelationshipBetweenNodes,
) -> str:
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
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=left_alias,
        label=statement.left_match.label,
        filter_alias=f"{left_alias}_label_filter_0",
    )
    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=right_alias,
        label=statement.right_match.label,
        filter_alias=f"{right_alias}_label_filter_1",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=left_alias,
        alias_kind="node",
        properties=statement.left_match.properties,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=right_alias,
        alias_kind="node",
        properties=statement.right_match.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={left_alias: left_alias, right_alias: right_alias},
        alias_kinds={left_alias: "node", right_alias: "node"},
        predicates=statement.predicates,
    )

    return _assemble_insert_select_sql(
        target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
        select_sql=(
            f"SELECT {_sql_literal(statement.relationship.type_name)}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'from',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'to',
                statement.left.alias,
                statement.right.alias,
            )}, "
            f"{_compile_json_object(statement.relationship.properties)}"
        ),
        from_sql=f"FROM nodes AS {left_alias}, nodes AS {right_alias}",
        where_parts=where_parts,
    )


def _compile_match_create_relationship_from_traversal_sql(
    statement: NormalizedMatchCreateRelationshipFromTraversal,
) -> str:
    from_sql, joins, where_parts, alias_map = _compile_traversal_write_source_components(
        statement.source
    )
    return _assemble_insert_select_sql(
        target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
        select_sql=(
            f"SELECT {_sql_literal(statement.relationship.type_name)}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'from',
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            )}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'to',
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            )}, "
            f"{_compile_json_object(statement.relationship.properties)}"
        ),
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
    )


def _compile_match_create_relationship_from_traversal_program(
    statement: NormalizedMatchCreateRelationshipFromTraversal,
) -> CompiledCypherProgram:
    _from_sql, _joins, _where_parts, alias_map = _compile_traversal_write_source_components(
        statement.source
    )
    if statement.left.alias in alias_map and statement.right.alias in alias_map:
        return _single_statement_program(
            _compile_match_create_relationship_from_traversal_sql(statement)
        )

    new_endpoint = statement.left
    matched_endpoint = statement.right
    from_value = ":created_node_id"
    to_value = ":match_node_id"

    if statement.right.alias not in alias_map:
        new_endpoint = statement.right
        matched_endpoint = statement.left
        from_value = ":match_node_id"
        to_value = ":created_node_id"

    if matched_endpoint.alias not in alias_map:
        raise ValueError(
            "CypherGlot MATCH ... CREATE compilation currently supports traversal-backed creates only with one reused matched node alias plus at most one fresh endpoint node."
        )

    if statement.relationship.direction == "in":
        from_value, to_value = to_value, from_value

    source_sql = _compile_traversal_write_node_id_source_sql(
        source=statement.source,
        source_alias=matched_endpoint.alias,
        binding_name="match_node_id",
    )
    create_node_sql = parse_one(
        "INSERT INTO nodes (properties) VALUES "
        f"({_compile_json_object(new_endpoint.properties)}) RETURNING id"
    )
    insert_edge_sql = parse_one(
        "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
        f"({_sql_literal(statement.relationship.type_name)}, {from_value}, {to_value}, "
        f"{_compile_json_object(statement.relationship.properties)})"
    )

    body: list[CompiledCypherStatement] = [
        CompiledCypherStatement(
            sql=create_node_sql,
            bind_columns=("created_node_id",),
        )
    ]
    if new_endpoint.label is not None:
        body.append(
            CompiledCypherStatement(
                sql=parse_one(
                    "INSERT INTO node_labels (node_id, label) VALUES "
                    f"(:created_node_id, {_sql_literal(new_endpoint.label)})"
                )
            )
        )
    body.append(CompiledCypherStatement(sql=insert_edge_sql))

    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(source_sql),
                row_bindings=("match_node_id",),
                body=tuple(body),
            ),
        )
    )


def _compile_match_merge_relationship_from_traversal_sql(
    statement: NormalizedMatchMergeRelationshipFromTraversal,
) -> str:
    from_sql, joins, where_parts, alias_map = _compile_traversal_write_source_components(
        statement.source
    )
    where_parts.append(
        _compile_relationship_absence_predicate(
            relationship=statement.relationship,
            from_id_sql=_match_create_endpoint_id_sql(
                statement.relationship.direction,
                "from",
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            ),
            to_id_sql=_match_create_endpoint_id_sql(
                statement.relationship.direction,
                "to",
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            ),
            existing_alias="existing_merge_edge",
        )
    )
    return _assemble_insert_select_sql(
        target_sql="INSERT INTO edges (type, from_id, to_id, properties)",
        select_sql=(
            f"SELECT {_sql_literal(statement.relationship.type_name)}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'from',
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            )}, "
            f"{_match_create_endpoint_id_sql(
                statement.relationship.direction,
                'to',
                alias_map[statement.left.alias],
                alias_map[statement.right.alias],
            )}, "
            f"{_compile_json_object(statement.relationship.properties)}"
        ),
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
    )


def _compile_match_merge_relationship_from_traversal_program(
    statement: NormalizedMatchMergeRelationshipFromTraversal,
) -> CompiledCypherProgram:
    _from_sql, _joins, _where_parts, alias_map = _compile_traversal_write_source_components(
        statement.source
    )
    if statement.left.alias in alias_map and statement.right.alias in alias_map:
        return _single_statement_program(
            _compile_match_merge_relationship_from_traversal_sql(statement)
        )

    new_endpoint = statement.left
    matched_endpoint = statement.right
    from_value = ":created_node_id"
    to_value = ":match_node_id"

    if statement.right.alias not in alias_map:
        new_endpoint = statement.right
        matched_endpoint = statement.left
        from_value = ":match_node_id"
        to_value = ":created_node_id"

    if matched_endpoint.alias not in alias_map:
        raise ValueError(
            "CypherGlot MATCH ... MERGE compilation currently supports traversal-backed merges only with one reused matched node alias plus at most one fresh endpoint node."
        )

    if statement.relationship.direction == "in":
        from_value, to_value = to_value, from_value

    source_sql = _compile_traversal_match_merge_node_id_source_sql(
        source=statement.source,
        matched_endpoint_alias=matched_endpoint.alias,
        new_endpoint=new_endpoint,
        relationship=statement.relationship,
        binding_name="match_node_id",
    )
    create_node_sql = parse_one(
        "INSERT INTO nodes (properties) VALUES "
        f"({_compile_json_object(new_endpoint.properties)}) RETURNING id"
    )
    insert_edge_sql = parse_one(
        "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
        f"({_sql_literal(statement.relationship.type_name)}, {from_value}, {to_value}, "
        f"{_compile_json_object(statement.relationship.properties)})"
    )

    body: list[CompiledCypherStatement] = [
        CompiledCypherStatement(
            sql=create_node_sql,
            bind_columns=("created_node_id",),
        )
    ]
    if new_endpoint.label is not None:
        body.append(
            CompiledCypherStatement(
                sql=parse_one(
                    "INSERT INTO node_labels (node_id, label) VALUES "
                    f"(:created_node_id, {_sql_literal(new_endpoint.label)})"
                )
            )
        )
    body.append(CompiledCypherStatement(sql=insert_edge_sql))

    return CompiledCypherProgram(
        steps=(
            CompiledCypherLoop(
                source=parse_one(source_sql),
                row_bindings=("match_node_id",),
                body=tuple(body),
            ),
        )
    )


def _compile_traversal_write_source_components(
    source: NormalizedMatchRelationship | NormalizedMatchChain,
) -> tuple[str, list[str], list[str], dict[str, str]]:
    if isinstance(source, NormalizedMatchChain):
        from_sql, joins, where_parts, alias_map, _alias_kinds = _compile_chain_source_components(
            nodes=source.nodes,
            relationships=source.relationships,
            predicates=source.predicates,
        )
        return from_sql, joins, where_parts, alias_map

    from_sql, joins, where_parts, alias_map, _alias_kinds = (
        _compile_match_relationship_source_components(source)
    )
    return from_sql, joins, where_parts, alias_map


def _compile_traversal_write_node_id_source_sql(
    *,
    source: NormalizedMatchRelationship | NormalizedMatchChain,
    source_alias: str,
    binding_name: str,
) -> str:
    from_sql, joins, where_parts, alias_map = _compile_traversal_write_source_components(
        source
    )
    return _assemble_select_sql(
        select_sql=f"{alias_map[source_alias]}.id AS {binding_name}",
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )


def _compile_traversal_match_merge_node_id_source_sql(
    *,
    source: NormalizedMatchRelationship | NormalizedMatchChain,
    matched_endpoint_alias: str,
    new_endpoint: NodePattern,
    relationship: RelationshipPattern,
    binding_name: str,
) -> str:
    from_sql, joins, where_parts, alias_map = _compile_traversal_write_source_components(
        source
    )
    existing_node_alias = "existing_merge_new_node"
    exists_joins = [f"JOIN nodes AS {existing_node_alias} ON {existing_node_alias}.id = existing_merge_edge.{_merge_new_endpoint_edge_column(relationship.direction, new_endpoint_is_right=(new_endpoint.alias != matched_endpoint_alias))}"]
    if new_endpoint.label is not None:
        exists_joins.append(
            f"JOIN node_labels AS {existing_node_alias}_label_0 ON {existing_node_alias}_label_0.node_id = {existing_node_alias}.id AND {existing_node_alias}_label_0.label = {_sql_literal(new_endpoint.label)}"
        )
    exists_where = [
        f"existing_merge_edge.{_merge_matched_endpoint_edge_column(relationship.direction, new_endpoint_is_right=(new_endpoint.alias != matched_endpoint_alias))} = {alias_map[matched_endpoint_alias]}.id"
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
    _extend_pattern_property_filters(
        where_parts=exists_where,
        alias=existing_node_alias,
        alias_kind="node",
        properties=new_endpoint.properties,
    )
    exists_sql = _assemble_select_sql(
        select_sql="1",
        distinct=False,
        from_sql="FROM edges AS existing_merge_edge",
        joins=exists_joins,
        where_parts=exists_where,
        order_sql=None,
        limit=1,
        skip=None,
    )
    guarded_where_parts = [*where_parts, f"NOT EXISTS ({exists_sql})"]
    return _assemble_select_sql(
        select_sql=f"{alias_map[matched_endpoint_alias]}.id AS {binding_name}",
        distinct=False,
        from_sql=from_sql,
        joins=joins,
        where_parts=guarded_where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )


def _merge_matched_endpoint_edge_column(
    direction: Literal["out", "in"],
    *,
    new_endpoint_is_right: bool,
) -> str:
    if direction == "out":
        return "from_id" if new_endpoint_is_right else "to_id"
    return "to_id" if new_endpoint_is_right else "from_id"


def _merge_new_endpoint_edge_column(
    direction: Literal["out", "in"],
    *,
    new_endpoint_is_right: bool,
) -> str:
    if direction == "out":
        return "to_id" if new_endpoint_is_right else "from_id"
    return "from_id" if new_endpoint_is_right else "to_id"


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
) -> str:
    parts = [target_sql, f"SET properties = {assignments_sql}"]
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


def _compile_relationship_absence_predicate(
    *,
    relationship: RelationshipPattern,
    from_id_sql: str,
    to_id_sql: str,
    existing_alias: str,
) -> str:
    where_parts = [
        f"{existing_alias}.from_id = {from_id_sql}",
        f"{existing_alias}.to_id = {to_id_sql}",
    ]
    _append_relationship_type_filter_for_alias(
        where_parts,
        relationship,
        existing_alias,
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=existing_alias,
        alias_kind="relationship",
        properties=relationship.properties,
    )
    return (
        "NOT EXISTS ("
        f"SELECT 1 FROM edges AS {existing_alias} "
        f"WHERE {' AND '.join(where_parts)}"
        ")"
    )


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
        return _compile_stream_predicate(expression, None, predicate.operator, predicate.value)

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
            return f"SUBSTRING({_sql_value(item.value)}, ({start_sql} + 1), {length_sql})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        if item.length_value is None:
            return f"SUBSTRING({inner}, ({start_sql} + 1))"
        length_sql = _sql_value(item.length_value)
        return f"SUBSTRING({inner}, ({start_sql} + 1), {length_sql})"
    if item.kind == "round":
        if item.value is not None:
            return f"ROUND({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"ROUND({inner})"
    if item.kind == "ceil":
        if item.value is not None:
            return f"CEIL({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"CEIL({inner})"
    if item.kind == "floor":
        if item.value is not None:
            return f"FLOOR({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"FLOOR({inner})"
    if item.kind == "abs":
        if item.value is not None:
            return f"ABS({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"ABS({inner})"
    if item.kind == "sign":
        if item.value is not None:
            return f"SIGN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"SIGN({inner})"
    if item.kind == "sqrt":
        if item.value is not None:
            return f"SQRT({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"SQRT({inner})"
    if item.kind == "exp":
        if item.value is not None:
            return f"EXP({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"EXP({inner})"
    if item.kind == "sin":
        if item.value is not None:
            return f"SIN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"SIN({inner})"
    if item.kind == "cos":
        if item.value is not None:
            return f"COS({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"COS({inner})"
    if item.kind == "tan":
        if item.value is not None:
            return f"TAN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"TAN({inner})"
    if item.kind == "asin":
        if item.value is not None:
            return f"ASIN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"ASIN({inner})"
    if item.kind == "acos":
        if item.value is not None:
            return f"ACOS({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"ACOS({inner})"
    if item.kind == "atan":
        if item.value is not None:
            return f"ATAN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"ATAN({inner})"
    if item.kind == "ln":
        if item.value is not None:
            return f"LN({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"LN({inner})"
    if item.kind == "log":
        if item.value is not None:
            return f"LOG({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"LOG({inner})"
    if item.kind == "log10":
        if item.value is not None:
            return f"LOG10({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"LOG10({inner})"
    if item.kind == "radians":
        if item.value is not None:
            return f"RADIANS({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"RADIANS({inner})"
    if item.kind == "degrees":
        if item.value is not None:
            return f"DEGREES({_sql_value(item.value)})"
        inner = _compile_return_expression(
            ReturnItem(alias=item.alias, field=item.field, kind="field"),
            alias_map,
            alias_kinds,
        )
        return f"DEGREES({inner})"
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
            return f"{function_name}({_sql_value(item.value)}, {_sql_value(item.length_value)})"
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
        return _compile_node_json_object_from_id_expression(
            entity_alias=f"{item.alias}_start",
            node_id_expression=f"{table_alias}.from_id",
            properties_expression=(
                f"(SELECT {start_node_alias}.properties FROM nodes AS {start_node_alias} "
                f"WHERE {start_node_alias}.id = {table_alias}.from_id)"
            ),
        )

    if item.kind == "end_node":
        end_node_alias = f"{item.alias}_end_node"
        if item.field is not None:
            return _compile_node_field_from_id_expression(
                entity_alias=f"{item.alias}_end",
                node_alias=end_node_alias,
                node_id_expression=f"{table_alias}.to_id",
                field=item.field,
            )
        return _compile_node_json_object_from_id_expression(
            entity_alias=f"{item.alias}_end",
            node_id_expression=f"{table_alias}.to_id",
            properties_expression=(
                f"(SELECT {end_node_alias}.properties FROM nodes AS {end_node_alias} "
                f"WHERE {end_node_alias}.id = {table_alias}.to_id)"
            ),
        )

    if item.field is None:
        return _compile_entity_json_object(
            entity_alias=item.alias,
            table_alias=table_alias,
            alias_kind=alias_kind,
        )

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


def _compile_json_set_assignments(
    *,
    target_alias: str,
    alias_kind: Literal["node", "relationship"],
    assignments: tuple[SetItem, ...],
) -> str:
    if not assignments:
        raise ValueError("CypherGlot MATCH ... SET compilation requires assignments.")

    arguments = [
        f"COALESCE({_properties_column(target_alias, alias_kind)}, '{{}}')"
    ]
    for assignment in assignments:
        arguments.append(_sql_literal(f"$.{assignment.field}"))
        arguments.append(_sql_value(assignment.value))
    return f"JSON_SET({', '.join(arguments)})"


def _compile_json_object(properties: tuple[tuple[str, CypherValue], ...]) -> str:
    if not properties:
        return _sql_literal("{}")

    arguments: list[str] = []
    for field, value in properties:
        arguments.append(_sql_literal(field))
        arguments.append(_sql_value(value))
    return f"JSON_OBJECT({', '.join(arguments)})"


def _match_create_endpoint_id_sql(
    direction: Literal["out", "in"],
    side: Literal["from", "to"],
    left_alias: str,
    right_alias: str,
) -> str:
    if direction == "out":
        return f"{left_alias}.id" if side == "from" else f"{right_alias}.id"
    return f"{right_alias}.id" if side == "from" else f"{left_alias}.id"


def _compile_match_node_id_source_sql(
    *,
    node: object,
    predicates: tuple[Predicate, ...],
    binding_name: str,
) -> str:
    match_node = node
    assert hasattr(match_node, "alias")
    alias = match_node.alias
    where_parts: list[str] = []

    _append_node_label_filter(
        where_parts=where_parts,
        node_alias=alias,
        label=match_node.label,
        filter_alias=f"{alias}_label_filter_0",
    )
    _extend_pattern_property_filters(
        where_parts=where_parts,
        alias=alias,
        alias_kind="node",
        properties=match_node.properties,
    )
    _append_predicate_filters(
        where_parts=where_parts,
        alias_map={alias: alias},
        alias_kinds={alias: "node"},
        predicates=predicates,
    )

    return _assemble_select_sql(
        select_sql=f"{alias}.id AS {binding_name}",
        distinct=False,
        from_sql=f"FROM nodes AS {alias}",
        joins=[],
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )


def _compile_create_node_steps(
    node: NodePattern,
    binding_name: str,
) -> tuple[CompiledCypherStatement, ...]:
    if node.label is None:
        raise ValueError("CypherGlot CREATE compilation requires labeled nodes.")

    return (
        CompiledCypherStatement(
            sql=parse_one(
                "INSERT INTO nodes (properties) VALUES "
                f"({_compile_json_object(node.properties)}) RETURNING id"
            ),
            bind_columns=(binding_name,),
        ),
        CompiledCypherStatement(
            sql=parse_one(
                "INSERT INTO node_labels (node_id, label) VALUES "
                f"(:{binding_name}, {_sql_literal(node.label)})"
            )
        ),
    )


def _compile_edge_insert_statement(
    *,
    relationship: RelationshipPattern,
    from_value: str,
    to_value: str,
) -> CompiledCypherStatement:
    return CompiledCypherStatement(
        sql=parse_one(
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            f"({_sql_literal(_require_single_relationship_type(relationship))}, "
            f"{from_value}, {to_value}, "
            f"{_compile_json_object(relationship.properties)})"
        )
    )


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


def _require_single_relationship_type(relationship: RelationshipPattern) -> str:
    if relationship.type_name is None or "|" in relationship.type_name:
        raise ValueError(
            "CypherGlot CREATE relationship compilation requires exactly one "
            "relationship type."
        )
    return relationship.type_name


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
                if return_item.kind in {"scalar", "size", "id", "type", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "to_string", "to_integer", "to_float", "to_boolean", "substring"}:
                    return _compile_return_expression(return_item, alias_map, alias_kinds)
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


def _property_expression(
    alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
) -> str:
    return f"JSON_EXTRACT({_properties_column(alias, alias_kind)}, '$.{field}')"


def _compile_entity_json_object(
    *,
    entity_alias: str,
    table_alias: str,
    alias_kind: Literal["node", "relationship"],
) -> str:
    if alias_kind == "node":
        return _compile_node_json_object_from_id_expression(
            entity_alias=entity_alias,
            node_id_expression=f"{table_alias}.id",
            properties_expression=f"{table_alias}.properties",
        )
    return (
        f"JSON_OBJECT('id', {table_alias}.id, 'type', {table_alias}.type, "
        f"'properties', JSON(COALESCE({table_alias}.properties, '{{}}')))"
    )


def _compile_node_json_object_from_id_expression(
    *,
    entity_alias: str,
    node_id_expression: str,
    properties_expression: str,
) -> str:
    return (
        f"JSON_OBJECT('id', {node_id_expression}, 'label', "
        f"(SELECT {entity_alias}_label_return.label FROM node_labels AS {entity_alias}_label_return "
        f"WHERE {entity_alias}_label_return.node_id = {node_id_expression} LIMIT 1), "
        f"'properties', JSON(COALESCE({properties_expression}, '{{}}')))"
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
        return f"{function_name}({_compile_count_argument(item, alias_map, alias_kinds)})"
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
