"""Compile normalized CypherGlot statements into backend-aware SQLGlot AST."""

from __future__ import annotations

from typing import Literal

from sqlglot import exp, parse_one

from ._compiled_program import (
    CompiledCypherProgram,
    CompiledCypherStatement,
    _require_single_statement_program,
    _single_statement_program,
)
from ._compile_unwind import _compile_unwind_sql
from ._compile_sql_utils import (
    _assemble_delete_sql,
    _assemble_insert_select_sql,
    _assemble_select_sql,
    _assemble_update_sql,
    _edge_endpoint_column,
    _sql_value,
)
from ._compile_type_aware_common import (
    _compile_type_aware_edge_field_expression,
    _compile_type_aware_match_node_predicate,
    _compile_type_aware_match_relationship_predicate,
    _compile_type_aware_node_field_expression,
    _compile_type_aware_predicate,
)
from ._compile_type_aware_reads import (
    _create_relationship_uses_distinct_nodes,
    _compile_type_aware_match_chain_sql,
    _compile_type_aware_match_node_sql,
    _compile_type_aware_match_relationship_sql,
    _compile_type_aware_optional_match_node_sql,
    _is_variable_length_relationship,
)
from ._compile_type_aware_with import _compile_type_aware_match_with_return_sql
from ._compile_write_helpers import (
    _compile_direct_fresh_endpoint_relationship_program,
    _compile_traversal_fresh_endpoint_relationship_program,
    _validate_type_aware_edge_contract,
)
from ._compile_write_programs import (
    _compile_create_relationship_from_separate_patterns_program,
    _compile_create_relationship_program,
    _compile_match_create_relationship_between_nodes_sql,
    _compile_match_merge_relationship_sql,
    _compile_merge_node_program,
    _compile_merge_relationship_program,
)
from ._logging import get_logger
from ._normalize_support import (
    CypherValue,
    NodePattern,
    RelationshipPattern,
    SetItem,
)
from .ir import (
    GraphRelationalCreateNodeWriteIR,
    GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR,
    GraphRelationalCreateRelationshipWriteIR,
    GraphRelationalDeleteNodeWriteIR,
    GraphRelationalDeleteRelationshipWriteIR,
    GraphRelationalMatchCreateRelationshipFromTraversalWriteIR,
    GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR,
    GraphRelationalMatchCreateRelationshipOnNodeWriteIR,
    GraphRelationalMergeNodeWriteIR,
    GraphRelationalMergeRelationshipWriteIR,
    GraphRelationalMatchMergeRelationshipWriteIR,
    GraphRelationalMatchMergeRelationshipFromTraversalWriteIR,
    GraphRelationalMatchMergeRelationshipOnNodeWriteIR,
    GraphRelationalBackendIR,
    GraphRelationalReadIR,
    GraphRelationalSetNodeWriteIR,
    GraphRelationalSetRelationshipWriteIR,
    SQLBackend,
    build_graph_relational_ir,
    lower_graph_relational_ir,
)
from .normalize import (
    NormalizedCypherStatement,
    NormalizedQueryNodesVectorSearch,
    normalize_cypher_parse_result,
)
from .parser import parse_cypher_text
from .schema import CompilerSchemaContext, GraphSchema


logger = get_logger(__name__)


def compile_cypher_text(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> exp.Expression:
    logger.debug("Compiling Cypher text")
    try:
        expression = _require_single_statement_program(
            compile_cypher_program_text(
                text,
                schema_context=schema_context,
                backend=backend,
            )
        )
    except Exception:
        logger.debug("Compilation failed", exc_info=True)
        raise
    logger.debug("Compiled Cypher text")
    return expression


def compile_cypher_program_text(
    text: str,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> CompiledCypherProgram:
    logger.debug("Compiling Cypher program text")
    program = compile_normalized_cypher_program(
        normalize_cypher_parse_result(parse_cypher_text(text)),
        schema_context=schema_context,
        backend=backend,
    )
    logger.debug(
        "Compiled Cypher program text",
        extra={"step_count": len(program.steps)},
    )
    return program


def compile_normalized_cypher_statement(
    statement: NormalizedCypherStatement,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> exp.Expression:
    logger.debug(
        "Compiling normalized Cypher statement",
        extra={"statement_kind": type(statement).__name__},
    )
    expression = _require_single_statement_program(
        compile_normalized_cypher_program(
            statement,
            schema_context=schema_context,
            backend=backend,
        )
    )
    logger.debug("Compiled normalized Cypher statement")
    return expression


def compile_normalized_cypher_program(
    statement: NormalizedCypherStatement,
    *,
    schema_context: CompilerSchemaContext | None = None,
    backend: SQLBackend | str | None = None,
) -> CompiledCypherProgram:
    resolved_schema_context = _require_supported_schema_context(schema_context)
    resolved_backend = _resolve_sql_backend(backend)
    logger.debug(
        "Compiling normalized Cypher program",
        extra={
            "statement_kind": type(statement).__name__,
            "schema_layout": resolved_schema_context.layout,
            "backend": resolved_backend.value,
        },
    )
    program_ir = build_graph_relational_ir(
        statement,
        schema_context=resolved_schema_context,
    )
    program = lower_graph_relational_ir(
        program_ir,
        backend=resolved_backend,
        lowerers={
            SQLBackend.SQLITE: _compile_graph_relational_backend_program,
            SQLBackend.DUCKDB: _compile_graph_relational_backend_program,
            SQLBackend.POSTGRESQL: _compile_graph_relational_backend_program,
        },
    )
    return program


def _resolve_sql_backend(backend: SQLBackend | str | None) -> SQLBackend:
    if backend is None:
        raise ValueError(
            "CypherGlot compilation requires an explicit SQL backend."
        )
    if isinstance(backend, SQLBackend):
        return backend
    return SQLBackend(backend)


def _require_supported_schema_context(
    schema_context: CompilerSchemaContext | None,
) -> CompilerSchemaContext:
    if schema_context is None:
        raise ValueError(
            "CypherGlot compilation now requires an explicit type-aware "
            "CompilerSchemaContext."
        )
    resolved = schema_context
    resolved.validate()
    return resolved


def _compile_graph_relational_backend_program(
    backend_ir: GraphRelationalBackendIR,
) -> CompiledCypherProgram:
    statement = backend_ir.program.statement.normalized_statement
    family = backend_ir.program.statement.family
    read_query = backend_ir.program.statement.read_query
    write_query = backend_ir.program.statement.write_query
    graph_schema = backend_ir.program.schema_context.graph_schema
    assert graph_schema is not None

    if family == "create-node":
        assert isinstance(write_query, GraphRelationalCreateNodeWriteIR)
        return CompiledCypherProgram(
            steps=_compile_create_node_steps(
                write_query.node,
                "created_node_id",
                graph_schema=graph_schema,
            )
        )

    if family == "create-relationship":
        assert isinstance(write_query, GraphRelationalCreateRelationshipWriteIR)
        return _compile_create_relationship_program(
            write_query,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
        )

    if family == "create-relationship-from-separate-patterns":
        assert isinstance(
            write_query,
            GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR,
        )
        return _compile_create_relationship_from_separate_patterns_program(
            write_query,
            graph_schema=graph_schema,
        )

    if family == "merge-node":
        assert isinstance(write_query, GraphRelationalMergeNodeWriteIR)
        return _compile_merge_node_program(
            write_query,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
        )

    if family == "merge-relationship":
        assert isinstance(write_query, GraphRelationalMergeRelationshipWriteIR)
        return _compile_merge_relationship_program(
            write_query,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
        )

    if family == "match-node":
        assert read_query is not None
        return _single_statement_program(
            _compile_type_aware_match_node_sql(
                read_query,
                graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "optional-match-node":
        assert read_query is not None
        return _single_statement_program(
            _compile_type_aware_optional_match_node_sql(
                read_query,
                graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "match-relationship":
        assert read_query is not None
        return _single_statement_program(
            _compile_type_aware_match_relationship_sql(
                read_query,
                graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "match-chain":
        assert read_query is not None
        return _single_statement_program(
            _compile_type_aware_match_chain_sql(
                read_query,
                graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "match-with-return":
        assert read_query is not None
        return _single_statement_program(
            _compile_type_aware_match_with_return_sql(
                read_query,
                graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "unwind":
        assert read_query is not None
        return _single_statement_program(_compile_unwind_sql(read_query))

    if isinstance(statement, NormalizedQueryNodesVectorSearch):
        raise ValueError(
            "CypherGlot vector-aware queryNodes normalization carries vector "
            "intent forward for host runtimes, but it does not yet compile "
            "vector-aware CALL queries into SQLGlot output."
        )

    if family == "set-node":
        assert isinstance(write_query, GraphRelationalSetNodeWriteIR)
        return _single_statement_program(
                _compile_set_node_sql(
                    write_query,
                    graph_schema=graph_schema,
                    backend=backend_ir.backend,
                )
        )

    if family == "set-relationship":
        assert isinstance(write_query, GraphRelationalSetRelationshipWriteIR)
        return _single_statement_program(
                _compile_set_relationship_sql(
                    write_query,
                    graph_schema=graph_schema,
                    backend=backend_ir.backend,
                )
        )

    if family == "delete-node":
        assert isinstance(write_query, GraphRelationalDeleteNodeWriteIR)
        return _single_statement_program(
                _compile_delete_node_sql(
                    write_query,
                    graph_schema=graph_schema,
                    backend=backend_ir.backend,
                )
        )

    if family == "delete-relationship":
        assert isinstance(write_query, GraphRelationalDeleteRelationshipWriteIR)
        return _single_statement_program(
                _compile_delete_relationship_sql(
                    write_query,
                    graph_schema=graph_schema,
                    backend=backend_ir.backend,
                )
        )

    if family == "match-merge-relationship":
        assert isinstance(
            write_query,
            GraphRelationalMatchMergeRelationshipWriteIR,
        )
        return _single_statement_program(
            _compile_match_merge_relationship_sql(
                write_query,
                graph_schema=graph_schema,
                backend=backend_ir.backend,
            )
        )

    if family == "match-merge-relationship-on-node":
        assert isinstance(
            write_query,
            GraphRelationalMatchMergeRelationshipOnNodeWriteIR,
        )
        return _compile_direct_fresh_endpoint_relationship_program(
            match_node=write_query.match_node,
            left=write_query.left,
            right=write_query.right,
            predicates=write_query.predicates,
            relationship=write_query.relationship,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
            merge=True,
        )

    if family == "match-merge-relationship-from-traversal":
        assert isinstance(
            write_query,
            GraphRelationalMatchMergeRelationshipFromTraversalWriteIR,
        )
        return _compile_traversal_fresh_endpoint_relationship_program(
            source=write_query.source,
            left=write_query.left,
            right=write_query.right,
            relationship=write_query.relationship,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
            merge=True,
        )

    if family == "match-create-relationship":
        assert isinstance(
            write_query,
            GraphRelationalMatchCreateRelationshipOnNodeWriteIR,
        )
        return _compile_direct_fresh_endpoint_relationship_program(
            match_node=write_query.match_node,
            left=write_query.left,
            right=write_query.right,
            predicates=write_query.predicates,
            relationship=write_query.relationship,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
            merge=False,
        )

    if family == "match-create-relationship-from-traversal":
        assert isinstance(
            write_query,
            GraphRelationalMatchCreateRelationshipFromTraversalWriteIR,
        )
        return _compile_traversal_fresh_endpoint_relationship_program(
            source=write_query.source,
            left=write_query.left,
            right=write_query.right,
            relationship=write_query.relationship,
            graph_schema=graph_schema,
            backend=backend_ir.backend,
            merge=False,
        )

    if family == "match-create-relationship-between-nodes":
        assert isinstance(
            write_query,
            GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR,
        )
        return _single_statement_program(
            _compile_match_create_relationship_between_nodes_sql(
                write_query,
                graph_schema=graph_schema,
                backend=backend_ir.backend,
            )
        )

    raise ValueError(
        "CypherGlot MVP compilation currently supports admitted standalone "
        "CREATE, MATCH ... RETURN, narrow UNWIND ... RETURN, MATCH ... SET, "
        "MATCH ... DELETE, and a narrow MATCH ... CREATE or MATCH ... MERGE "
        "relationship subset over one node pattern, one directed relationship "
        "pattern, or one fixed-length directed relationship chain."
    )


def _compile_set_node_sql(
    statement: GraphRelationalSetNodeWriteIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    alias = statement.node.alias
    if statement.node.label is None:
        raise ValueError(
            "Type-aware MATCH ... SET lowering requires an explicit node label."
        )

    node_type = graph_schema.node_type(statement.node.label)
    where_parts = [
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                alias,
                node_type,
                field,
            ),
            operator="=",
            value=value,
            backend=backend,
        )
        for field, value in statement.node.properties
    ]
    for predicate in statement.predicates:
        if predicate.alias != alias:
            raise ValueError(
                "Type-aware MATCH ... SET lowering currently supports "
                "predicates only on the matched node alias."
            )
        where_parts.append(
            _compile_type_aware_match_node_predicate(
                alias,
                node_type,
                predicate,
                backend=backend,
            )
        )

    assignments_sql = _compile_type_aware_set_assignments(
        entity_type=node_type,
        assignments=statement.assignments,
    )
    return _assemble_update_sql(
        target_sql=f"UPDATE {node_type.table_name} AS {alias}",
        assignments_sql=assignments_sql,
        from_sql=None,
        where_parts=where_parts,
        assignment_prefix=None,
    )


def _compile_set_relationship_sql(
    statement: GraphRelationalSetRelationshipWriteIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    relationship_alias = statement.relationship.alias or "edge"
    left_alias = statement.left.alias
    right_alias = statement.right.alias
    distinct_endpoints = _create_relationship_uses_distinct_nodes(
        statement.left,
        statement.right,
    )

    if statement.left.label is None or statement.right.label is None:
        raise ValueError(
            "Type-aware MATCH ... SET lowering requires explicit endpoint labels."
        )

    left_type = graph_schema.node_type(statement.left.label)
    right_type = graph_schema.node_type(statement.right.label)
    relationship_type = _require_single_relationship_type(statement.relationship)
    edge_type = graph_schema.edge_type(relationship_type)
    if (
        statement.left.label != edge_type.source_type
        or statement.right.label != edge_type.target_type
    ):
        raise ValueError(
            "Type-aware MATCH ... SET lowering requires endpoint labels "
            "to match the schema contract."
        )

    where_parts = [
        (
            f"{left_alias}.id = {relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
        )
    ]
    if distinct_endpoints:
        where_parts.append(
            f"{right_alias}.id = {relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'right')}"
        )
    else:
        where_parts.append(
            f"{relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'left')} = "
            f"{relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'right')}"
        )
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
                backend=backend,
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
                backend=backend,
            )
        )
    if distinct_endpoints:
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
                    backend=backend,
                )
            )

    for predicate in statement.predicates:
        if predicate.alias == left_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    left_alias,
                    left_type,
                    predicate,
                    backend=backend,
                )
            )
            continue
        if distinct_endpoints and predicate.alias == right_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    right_alias,
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
            "Type-aware MATCH ... SET lowering currently supports "
            "predicates only on admitted one-hop aliases."
        )

    assignments_sql = _compile_type_aware_set_assignments(
        entity_type=edge_type,
        assignments=statement.assignments,
    )
    return _assemble_update_sql(
        target_sql=f"UPDATE {edge_type.table_name} AS {relationship_alias}",
        assignments_sql=assignments_sql,
        from_sql=(
            f"FROM {left_type.table_name} AS {left_alias}, "
            f"{right_type.table_name} AS {right_alias}"
            if distinct_endpoints
            else f"FROM {left_type.table_name} AS {left_alias}"
        ),
        where_parts=where_parts,
        assignment_prefix=None,
    )


def _compile_delete_node_sql(
    statement: GraphRelationalDeleteNodeWriteIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    alias = statement.node.alias
    if statement.node.label is None:
        raise ValueError(
            "Type-aware MATCH ... DELETE lowering requires an explicit node label."
        )

    node_type = graph_schema.node_type(statement.node.label)
    where_parts = [
        _compile_type_aware_predicate(
            field_expression=_compile_type_aware_node_field_expression(
                alias,
                node_type,
                field,
            ),
            operator="=",
            value=value,
            backend=backend,
        )
        for field, value in statement.node.properties
    ]
    for predicate in statement.predicates:
        if predicate.alias != alias:
            raise ValueError(
                "Type-aware MATCH ... DELETE lowering currently supports "
                "predicates only on the matched node alias."
            )
        where_parts.append(
              _compile_type_aware_match_node_predicate(
                 alias,
                 node_type,
                 predicate,
                 backend=backend,
              )
        )

    return _assemble_delete_sql(
        target_sql=f"DELETE FROM {node_type.table_name} AS {alias}",
        using_sql=None,
        where_parts=where_parts,
    )


def _compile_delete_relationship_sql(
    statement: GraphRelationalDeleteRelationshipWriteIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    relationship_alias = statement.relationship.alias or "edge"
    distinct_endpoints = _create_relationship_uses_distinct_nodes(
        statement.left,
        statement.right,
    )

    if statement.left.label is None or statement.right.label is None:
        raise ValueError(
            "Type-aware MATCH ... DELETE lowering requires explicit endpoint labels."
        )

    left_type = graph_schema.node_type(statement.left.label)
    right_type = graph_schema.node_type(statement.right.label)
    relationship_type = _require_single_relationship_type(statement.relationship)
    edge_type = graph_schema.edge_type(relationship_type)
    if (
        statement.left.label != edge_type.source_type
        or statement.right.label != edge_type.target_type
    ):
        raise ValueError(
            "Type-aware MATCH ... DELETE lowering requires endpoint labels "
            "to match the schema contract."
        )

    where_parts: list[str] = []
    for field, value in statement.left.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    statement.left.alias,
                    left_type,
                    field,
                ),
                operator="=",
                value=value,
                backend=backend,
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
                backend=backend,
            )
        )
    if distinct_endpoints:
        for field, value in statement.right.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        statement.right.alias,
                        right_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                    backend=backend,
                )
            )
    else:
        where_parts.append(
            f"{relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'left')} = "
            f"{relationship_alias}."
            f"{_edge_endpoint_column(statement.relationship.direction, 'right')}"
        )

    for predicate in statement.predicates:
        if predicate.alias == statement.left.alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    statement.left.alias,
                    left_type,
                    predicate,
                    backend=backend,
                )
            )
            continue
        if distinct_endpoints and predicate.alias == statement.right.alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    statement.right.alias,
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
            "Type-aware MATCH ... DELETE lowering currently supports "
            "predicates only on admitted one-hop aliases."
        )

    matching_edge_ids_sql = _assemble_select_sql(
        select_sql=f"{relationship_alias}.id",
        distinct=False,
        from_sql=f"FROM {edge_type.table_name} AS {relationship_alias}",
        joins=(
            [
                (
                    f"JOIN {left_type.table_name} AS {statement.left.alias} ON "
                    f"{statement.left.alias}.id = {relationship_alias}."
                    f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
                ),
                (
                    f"JOIN {right_type.table_name} AS {statement.right.alias} ON "
                    f"{statement.right.alias}.id = {relationship_alias}."
                    f"{_edge_endpoint_column(
                        statement.relationship.direction,
                        'right',
                    )}"
                ),
            ]
            if distinct_endpoints
            else [
                (
                    f"JOIN {left_type.table_name} AS {statement.left.alias} ON "
                    f"{statement.left.alias}.id = {relationship_alias}."
                    f"{_edge_endpoint_column(statement.relationship.direction, 'left')}"
                )
            ]
        ),
        where_parts=where_parts,
        order_sql=None,
        limit=None,
        skip=None,
    )

    return _assemble_delete_sql(
        target_sql=f"DELETE FROM {edge_type.table_name}",
        using_sql=None,
        where_parts=[f"id IN ({matching_edge_ids_sql})"],
    )


def _compile_type_aware_merge_node_sql(
    node: NodePattern,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    if node.label is None:
        raise ValueError(
            "Type-aware MERGE node lowering requires an explicit node label."
        )

    node_type = graph_schema.node_type(node.label)
    where_parts = [
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
        for field, value in node.properties
    ]
    exists_sql = _assemble_select_sql(
        select_sql="1",
        distinct=False,
        from_sql=f"FROM {node_type.table_name} AS {node.alias}",
        joins=[],
        where_parts=where_parts,
        order_sql=None,
        limit=1,
        skip=None,
    )
    insert_columns = ", ".join(
        _resolve_type_aware_property_column(node_type, field)
        for field, _ in node.properties
    )
    insert_values = ", ".join(_sql_value(value) for _, value in node.properties)
    return _compile_guarded_insert_select_sql(
        target_sql=f"INSERT INTO {node_type.table_name} ({insert_columns})",
        select_sql=f"SELECT {insert_values}",
        from_sql="FROM (SELECT 1) AS merge_guard",
        exists_sql=exists_sql,
    )


def _compile_guarded_insert_select_sql(
    *,
    target_sql: str,
    select_sql: str,
    from_sql: str,
    exists_sql: str,
    joins: list[str] | None = None,
    where_parts: list[str] | None = None,
) -> str:
    guard_where_parts = list(where_parts or ())
    guard_where_parts.append(f"NOT EXISTS ({exists_sql})")
    return _assemble_insert_select_sql(
        target_sql=target_sql,
        select_sql=select_sql,
        from_sql=from_sql,
        joins=joins,
        where_parts=guard_where_parts,
    )


def _compile_type_aware_set_assignments(
    *,
    entity_type: object,
    assignments: tuple[SetItem, ...],
) -> str:
    if not assignments:
        raise ValueError("CypherGlot MATCH ... SET compilation requires assignments.")

    return ", ".join(
        (
            f"{_resolve_type_aware_property_column(entity_type, assignment.field)} "
            f"= {_sql_value(assignment.value)}"
        )
        for assignment in assignments
    )


def _match_create_endpoint_id_sql(
    direction: Literal["out", "in"],
    side: Literal["from", "to"],
    left_alias: str,
    right_alias: str,
) -> str:
    if direction == "out":
        return f"{left_alias}.id" if side == "from" else f"{right_alias}.id"
    return f"{right_alias}.id" if side == "from" else f"{left_alias}.id"


def _compile_create_node_steps(
    node: NodePattern,
    binding_name: str,
    graph_schema: GraphSchema,
) -> tuple[CompiledCypherStatement, ...]:
    if node.label is None:
        raise ValueError("CypherGlot CREATE compilation requires labeled nodes.")

    node_type = graph_schema.node_type(node.label)
    return (
        CompiledCypherStatement(
            sql=_compile_type_aware_insert_statement(
                table_name=node_type.table_name,
                entity_type=node_type,
                properties=node.properties,
                returning_id=True,
            ),
            bind_columns=(binding_name,),
        ),
    )


def _compile_edge_insert_statement(
    *,
    relationship: RelationshipPattern,
    from_value: str,
    to_value: str,
    graph_schema: GraphSchema,
    left_node: NodePattern | None = None,
    right_node: NodePattern | None = None,
) -> CompiledCypherStatement:
    relationship_type = _require_single_relationship_type(relationship)
    if left_node is None or right_node is None:
        raise ValueError(
            "Type-aware CREATE relationship lowering requires explicit "
            "endpoint patterns."
        )
    if left_node.label is None or right_node.label is None:
        raise ValueError(
            "Type-aware CREATE relationship lowering requires explicit endpoint labels."
        )

    edge_type = graph_schema.edge_type(relationship_type)
    _validate_type_aware_edge_contract(
        relationship=relationship,
        edge_type=edge_type,
        left_label=left_node.label,
        right_label=right_node.label,
        mismatch_message=(
            "Type-aware CREATE relationship lowering requires endpoint labels "
            "to match the schema contract."
        ),
    )

    return CompiledCypherStatement(
        sql=_compile_type_aware_insert_statement(
            table_name=edge_type.table_name,
            entity_type=edge_type,
            properties=relationship.properties,
            fixed_columns=("from_id", "to_id"),
            fixed_values=(from_value, to_value),
        )
    )


def _compile_type_aware_insert_statement(
    *,
    table_name: str,
    entity_type: object,
    properties: tuple[tuple[str, CypherValue], ...],
    fixed_columns: tuple[str, ...] = (),
    fixed_values: tuple[str, ...] = (),
    returning_id: bool = False,
) -> exp.Expression:
    columns = list(fixed_columns)
    values = list(fixed_values)

    for field, value in properties:
        columns.append(_resolve_type_aware_property_column(entity_type, field))
        values.append(_sql_value(value))

    if columns:
        sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
            f"({', '.join(values)})"
        )
    else:
        sql = f"INSERT INTO {table_name} DEFAULT VALUES"

    if returning_id:
        sql += " RETURNING id"

    return parse_one(sql)


def _resolve_type_aware_property_column(entity_type: object, field: str) -> str:
    for property_field in entity_type.properties:
        if property_field.name == field:
            return property_field.column_name

    raise ValueError(
        "Type-aware CREATE lowering requires write properties to exist in the "
        "schema contract."
    )


def _resolve_write_endpoint_node_pattern_from_candidates(
    node: NodePattern,
    candidates: tuple[NodePattern, ...],
) -> NodePattern:
    if node.label is not None:
        return node
    for candidate in candidates:
        if node.alias == candidate.alias:
            return NodePattern(
                alias=node.alias,
                label=candidate.label,
                properties=node.properties,
            )
    return node


def _resolve_write_endpoint_node_pattern(
    node: NodePattern,
    matched_node: NodePattern,
) -> NodePattern:
    return _resolve_write_endpoint_node_pattern_from_candidates(
        node,
        (matched_node,),
    )


def _resolve_write_endpoint_node_pattern_from_traversal_source(
    node: NodePattern,
    source: GraphRelationalReadIR,
) -> NodePattern:
    if source.source_kind == "relationship":
        return _resolve_write_endpoint_node_pattern_from_candidates(
            node,
            (source.left, source.right),
        )

    if source.source_kind != "relationship-chain":
        raise ValueError(
            "Traversal-backed write endpoint resolution requires a relationship source."
        )

    return _resolve_write_endpoint_node_pattern_from_candidates(
        node,
        tuple(source.nodes),
    )


def _compile_type_aware_traversal_write_source_components(
    source: GraphRelationalReadIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> tuple[str, list[str], list[str], dict[str, str]]:
    if source.source_kind == "relationship-chain":
        raise ValueError(
            "Type-aware traversal-backed writes currently support only one-hop "
            "MATCH sources."
        )
    if source.source_kind != "relationship":
        raise ValueError(
            "Type-aware traversal-backed writes require a relationship MATCH source."
        )

    relationship = source.relationship
    if _is_variable_length_relationship(relationship):
        raise ValueError(
            "Type-aware traversal-backed writes currently support only "
            "fixed-length one-hop MATCH sources."
        )
    if relationship.type_name is None or "|" in relationship.type_name:
        raise ValueError(
            "Type-aware traversal-backed writes currently require exactly one "
            "relationship type in the MATCH source."
        )
    if relationship.direction != "out":
        raise ValueError(
            "Type-aware traversal-backed writes currently support only "
            "outgoing one-hop MATCH sources."
        )
    if source.left.label is None or source.right.label is None:
        raise ValueError(
            "Type-aware traversal-backed writes currently require explicit "
            "endpoint labels in the MATCH source."
        )

    edge_type = graph_schema.edge_type(relationship.type_name)
    left_type = graph_schema.node_type(source.left.label)
    right_type = graph_schema.node_type(source.right.label)
    if (
        left_type.name != edge_type.source_type
        or right_type.name != edge_type.target_type
    ):
        raise ValueError(
            "Type-aware traversal-backed writes currently require the MATCH "
            "source labels to match the relationship schema contract."
        )

    relationship_alias = relationship.alias or "edge"
    left_alias = source.left.alias
    right_alias = source.right.alias
    distinct_endpoints = _create_relationship_uses_distinct_nodes(
        source.left,
        source.right,
    )
    if distinct_endpoints:
        joins = [
            (
                f"JOIN {left_type.table_name} AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}.from_id"
            ),
            (
                f"JOIN {right_type.table_name} AS {right_alias} "
                f"ON {right_alias}.id = {relationship_alias}.to_id"
            ),
        ]
    else:
        joins = [
            (
                f"JOIN {left_type.table_name} AS {left_alias} "
                f"ON {left_alias}.id = {relationship_alias}.from_id"
            )
        ]
    where_parts: list[str] = []

    for field, value in source.left.properties:
        where_parts.append(
            _compile_type_aware_predicate(
                field_expression=_compile_type_aware_node_field_expression(
                    left_alias,
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
    if distinct_endpoints:
        for field, value in source.right.properties:
            where_parts.append(
                _compile_type_aware_predicate(
                    field_expression=_compile_type_aware_node_field_expression(
                        right_alias,
                        right_type,
                        field,
                    ),
                    operator="=",
                    value=value,
                    backend=backend,
                )
            )
    else:
        where_parts.append(f"{relationship_alias}.from_id = {relationship_alias}.to_id")

    for predicate in source.predicates:
        if predicate.alias == left_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    left_alias,
                    left_type,
                    predicate,
                    backend=backend,
                )
            )
            continue
        if predicate.alias == right_alias:
            where_parts.append(
                _compile_type_aware_match_node_predicate(
                    right_alias,
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
            "Type-aware traversal-backed writes currently support predicates "
            "only on the one-hop MATCH source aliases."
        )

    alias_map = {
        left_alias: left_alias,
        right_alias: right_alias,
    }
    if relationship.alias is not None:
        alias_map[relationship.alias] = relationship_alias

    return (
        f"FROM {edge_type.table_name} AS {relationship_alias}",
        joins,
        where_parts,
        alias_map,
    )


def _require_single_relationship_type(relationship: RelationshipPattern) -> str:
    if relationship.type_name is None or "|" in relationship.type_name:
        raise ValueError(
            "CypherGlot CREATE relationship compilation requires exactly one "
            "relationship type."
        )
    return relationship.type_name
