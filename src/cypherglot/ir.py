"""Backend-neutral graph-relational IR scaffolding for CypherGlot.

This module is the first Phase 13 source-first slice. The initial IR is kept
small and practical: it captures normalized statement intent and backend
capability metadata without trying to replace all existing lowering logic at
once.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Literal, TypeVar

from ._normalize_support import (
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
    NormalizedMatchCreateRelationshipBetweenNodes,
    NormalizedMatchCreateRelationshipFromTraversal,
    NormalizedMatchChain,
    NormalizedMatchMergeRelationship,
    NormalizedMatchMergeRelationshipFromTraversal,
    NormalizedMatchMergeRelationshipOnNode,
    NormalizedMatchNode,
    NormalizedMatchRelationship,
    NormalizedMatchWithReturn,
    NormalizedOptionalMatchNode,
    NormalizedSetNode,
    NormalizedSetRelationship,
    NormalizedUnwind,
    WithBinding,
    WithOrderItem,
    WithPredicate,
    WithReturnItem,
)
from .schema import CompilerSchemaContext


class SQLBackend(StrEnum):
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    POSTGRESQL = "postgresql"


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    backend: SQLBackend
    supports_reads: bool = True
    supports_writes: bool = True
    supports_recursive_cte: bool = True
    render_dialect: str | None = None
    supports_returning: bool = False
    supports_update_from: bool = False
    supports_delete_using: bool = False
    supports_native_boolean: bool = True
    numeric_coercion_sql_type: str | None = None
    numeric_coercion_is_tolerant: bool = False
    integer_cast_requires_truncation: bool = False


ReadPredicate = Predicate | WithPredicate
ReadReturnItem = ReturnItem | WithReturnItem
ReadOrderItem = OrderItem | WithOrderItem


@dataclass(frozen=True, slots=True)
class GraphRelationalCreateNodeWriteIR:
    node: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalCreateRelationshipWriteIR:
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR:
    first_node: NodePattern
    second_node: NodePattern
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMergeNodeWriteIR:
    node: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMergeRelationshipWriteIR:
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalSetNodeWriteIR:
    node: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class GraphRelationalSetRelationshipWriteIR:
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class GraphRelationalDeleteNodeWriteIR:
    node: NodePattern
    predicates: tuple[Predicate, ...]
    detach: bool


@dataclass(frozen=True, slots=True)
class GraphRelationalDeleteRelationshipWriteIR:
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchMergeRelationshipOnNodeWriteIR:
    match_node: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchMergeRelationshipFromTraversalWriteIR:
    source: GraphRelationalReadIR
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchCreateRelationshipOnNodeWriteIR:
    match_node: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchCreateRelationshipFromTraversalWriteIR:
    source: GraphRelationalReadIR
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchMergeRelationshipWriteIR:
    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR:
    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


GraphRelationalWriteIR = (
    GraphRelationalCreateNodeWriteIR
    | GraphRelationalCreateRelationshipWriteIR
    | GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR
    | GraphRelationalMergeNodeWriteIR
    | GraphRelationalMergeRelationshipWriteIR
    |
    GraphRelationalSetNodeWriteIR
    | GraphRelationalSetRelationshipWriteIR
    | GraphRelationalDeleteNodeWriteIR
    | GraphRelationalDeleteRelationshipWriteIR
    | GraphRelationalMatchMergeRelationshipOnNodeWriteIR
    | GraphRelationalMatchMergeRelationshipFromTraversalWriteIR
    | GraphRelationalMatchCreateRelationshipOnNodeWriteIR
    | GraphRelationalMatchCreateRelationshipFromTraversalWriteIR
    | GraphRelationalMatchMergeRelationshipWriteIR
    | GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR
)


@dataclass(frozen=True, slots=True)
class GraphRelationalReadIR:
    match_kind: Literal["match", "optional-match", "with", "unwind"]
    source_kind: Literal["node", "relationship", "relationship-chain", "unwind"]
    nodes: tuple[NodePattern, ...] = ()
    relationships: tuple[RelationshipPattern, ...] = ()
    predicates: tuple[ReadPredicate, ...] = ()
    returns: tuple[ReadReturnItem, ...] = ()
    order_by: tuple[ReadOrderItem, ...] = ()
    bindings: tuple[WithBinding, ...] = ()
    distinct: bool = False
    limit: int | None = None
    skip: int | None = None
    source: GraphRelationalReadIR | None = None
    unwind_alias: str | None = None
    unwind_source_kind: Literal["literal"] | None = None
    unwind_source_items: tuple[CypherValue, ...] = ()

    @property
    def node(self) -> NodePattern:
        if self.source_kind != "node" or len(self.nodes) != 1:
            raise ValueError("Read IR does not describe a single-node source.")
        return self.nodes[0]

    @property
    def left(self) -> NodePattern:
        if self.source_kind != "relationship" or len(self.nodes) != 2:
            raise ValueError("Read IR does not describe a one-hop relationship source.")
        return self.nodes[0]

    @property
    def right(self) -> NodePattern:
        if self.source_kind != "relationship" or len(self.nodes) != 2:
            raise ValueError("Read IR does not describe a one-hop relationship source.")
        return self.nodes[1]

    @property
    def relationship(self) -> RelationshipPattern:
        if self.source_kind != "relationship" or len(self.relationships) != 1:
            raise ValueError("Read IR does not describe a one-hop relationship source.")
        return self.relationships[0]


@dataclass(frozen=True, slots=True)
class GraphRelationalStatementIR:
    normalized_statement: NormalizedCypherStatement
    family: str
    is_write: bool
    uses_variable_length: bool
    read_query: GraphRelationalReadIR | None = None
    write_query: GraphRelationalWriteIR | None = None


@dataclass(frozen=True, slots=True)
class GraphRelationalProgramIR:
    statement: GraphRelationalStatementIR
    schema_context: CompilerSchemaContext
    backend_capabilities: dict[SQLBackend, BackendCapabilities]


@dataclass(frozen=True, slots=True)
class GraphRelationalBackendIR:
    program: GraphRelationalProgramIR
    backend: SQLBackend
    capabilities: BackendCapabilities


LoweredT = TypeVar("LoweredT")
GraphRelationalLowerer = Callable[[GraphRelationalBackendIR], LoweredT]


BACKEND_CAPABILITIES: dict[SQLBackend, BackendCapabilities] = {
    SQLBackend.SQLITE: BackendCapabilities(
        backend=SQLBackend.SQLITE,
        supports_returning=True,
        supports_update_from=False,
        supports_delete_using=False,
        supports_native_boolean=False,
    ),
    SQLBackend.DUCKDB: BackendCapabilities(
        backend=SQLBackend.DUCKDB,
        supports_writes=True,
        supports_returning=True,
        supports_update_from=True,
        supports_delete_using=False,
        supports_native_boolean=True,
        render_dialect="duckdb",
        numeric_coercion_sql_type="DOUBLE",
        numeric_coercion_is_tolerant=True,
        integer_cast_requires_truncation=True,
    ),
    SQLBackend.POSTGRESQL: BackendCapabilities(
        backend=SQLBackend.POSTGRESQL,
        supports_writes=True,
        supports_returning=True,
        supports_update_from=True,
        supports_delete_using=True,
        supports_native_boolean=True,
        render_dialect="postgres",
        integer_cast_requires_truncation=True,
    ),
}


def build_graph_relational_ir(
    statement: NormalizedCypherStatement,
    *,
    schema_context: CompilerSchemaContext,
) -> GraphRelationalProgramIR:
    return GraphRelationalProgramIR(
        statement=GraphRelationalStatementIR(
            normalized_statement=statement,
            family=_statement_family(statement),
            is_write=_statement_is_write(statement),
            uses_variable_length=_statement_uses_variable_length(statement),
            read_query=_build_read_ir(statement),
            write_query=_build_write_ir(statement),
        ),
        schema_context=schema_context,
        backend_capabilities=BACKEND_CAPABILITIES,
    )


def bind_graph_relational_backend(
    program: GraphRelationalProgramIR,
    *,
    backend: SQLBackend,
) -> GraphRelationalBackendIR:
    try:
        capabilities = program.backend_capabilities[backend]
    except KeyError as exc:
        raise ValueError(f"Unsupported SQL backend: {backend}") from exc

    statement = program.statement
    if statement.is_write and not capabilities.supports_writes:
        raise ValueError(
            f"Backend {backend.value} does not support CypherGlot write lowering yet."
        )
    if not statement.is_write and not capabilities.supports_reads:
        raise ValueError(
            f"Backend {backend.value} does not support CypherGlot read lowering yet."
        )
    if statement.uses_variable_length and not capabilities.supports_recursive_cte:
        raise ValueError(
            "Backend "
            f"{backend.value} does not support variable-length traversal lowering."
        )

    return GraphRelationalBackendIR(
        program=program,
        backend=backend,
        capabilities=capabilities,
    )


def lower_graph_relational_ir(
    program: GraphRelationalProgramIR,
    *,
    backend: SQLBackend,
    lowerers: dict[SQLBackend, GraphRelationalLowerer[LoweredT]],
) -> LoweredT:
    backend_program = bind_graph_relational_backend(program, backend=backend)
    try:
        lowerer = lowerers[backend]
    except KeyError as exc:
        raise ValueError(
            f"No graph-relational lowerer is registered for backend {backend.value}."
        ) from exc
    return lowerer(backend_program)


def _statement_family(statement: NormalizedCypherStatement) -> str:
    family_by_type = {
        NormalizedMatchNode: "match-node",
        NormalizedOptionalMatchNode: "optional-match-node",
        NormalizedMatchRelationship: "match-relationship",
        NormalizedMatchChain: "match-chain",
        NormalizedMatchWithReturn: "match-with-return",
        NormalizedUnwind: "unwind",
        NormalizedCreateNode: "create-node",
        NormalizedCreateRelationship: "create-relationship",
        NormalizedCreateRelationshipFromSeparatePatterns: (
            "create-relationship-from-separate-patterns"
        ),
        NormalizedMergeNode: "merge-node",
        NormalizedMergeRelationship: "merge-relationship",
        NormalizedMatchCreateRelationship: "match-create-relationship",
        NormalizedMatchCreateRelationshipBetweenNodes: (
            "match-create-relationship-between-nodes"
        ),
        NormalizedMatchCreateRelationshipFromTraversal: (
            "match-create-relationship-from-traversal"
        ),
        NormalizedMatchMergeRelationship: "match-merge-relationship",
        NormalizedMatchMergeRelationshipOnNode: (
            "match-merge-relationship-on-node"
        ),
        NormalizedMatchMergeRelationshipFromTraversal: (
            "match-merge-relationship-from-traversal"
        ),
        NormalizedSetNode: "set-node",
        NormalizedSetRelationship: "set-relationship",
        NormalizedDeleteNode: "delete-node",
        NormalizedDeleteRelationship: "delete-relationship",
    }
    for statement_type, family in family_by_type.items():
        if isinstance(statement, statement_type):
            return family
    return type(statement).__name__


def _statement_is_write(statement: NormalizedCypherStatement) -> bool:
    return isinstance(
        statement,
        (
            NormalizedCreateNode,
            NormalizedCreateRelationship,
            NormalizedCreateRelationshipFromSeparatePatterns,
            NormalizedMergeNode,
            NormalizedMergeRelationship,
            NormalizedMatchCreateRelationship,
            NormalizedMatchCreateRelationshipBetweenNodes,
            NormalizedMatchCreateRelationshipFromTraversal,
            NormalizedMatchMergeRelationship,
            NormalizedMatchMergeRelationshipOnNode,
            NormalizedMatchMergeRelationshipFromTraversal,
            NormalizedSetNode,
            NormalizedSetRelationship,
            NormalizedDeleteNode,
            NormalizedDeleteRelationship,
        ),
    )


def _statement_uses_variable_length(statement: NormalizedCypherStatement) -> bool:
    relationship = None
    if isinstance(statement, NormalizedMatchRelationship):
        relationship = statement.relationship
    elif isinstance(statement, NormalizedMatchWithReturn):
        relationship = getattr(statement.source, "relationship", None)
    elif isinstance(statement, NormalizedMatchCreateRelationshipFromTraversal):
        relationship = getattr(statement.source, "relationship", None)
    elif isinstance(statement, NormalizedMatchMergeRelationshipFromTraversal):
        relationship = getattr(statement.source, "relationship", None)

    if relationship is None:
        return False
    return relationship.min_hops != 1 or relationship.max_hops != 1


def _build_read_ir(
    statement: NormalizedCypherStatement,
) -> GraphRelationalReadIR | None:
    if isinstance(statement, NormalizedMatchNode):
        return GraphRelationalReadIR(
            match_kind="match",
            source_kind="node",
            nodes=(statement.node,),
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            distinct=statement.distinct,
            limit=statement.limit,
            skip=statement.skip,
        )
    if isinstance(statement, NormalizedOptionalMatchNode):
        return GraphRelationalReadIR(
            match_kind="optional-match",
            source_kind="node",
            nodes=(statement.node,),
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            distinct=statement.distinct,
            limit=statement.limit,
            skip=statement.skip,
        )
    if isinstance(statement, NormalizedMatchRelationship):
        return GraphRelationalReadIR(
            match_kind="match",
            source_kind="relationship",
            nodes=(statement.left, statement.right),
            relationships=(statement.relationship,),
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            distinct=statement.distinct,
            limit=statement.limit,
            skip=statement.skip,
        )
    if isinstance(statement, NormalizedMatchChain):
        return GraphRelationalReadIR(
            match_kind="match",
            source_kind="relationship-chain",
            nodes=statement.nodes,
            relationships=statement.relationships,
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            distinct=statement.distinct,
            limit=statement.limit,
            skip=statement.skip,
        )
    if isinstance(statement, NormalizedMatchWithReturn):
        source = _build_read_ir(statement.source)
        if source is None:
            return None
        return GraphRelationalReadIR(
            match_kind="with",
            source_kind=source.source_kind,
            nodes=source.nodes,
            relationships=source.relationships,
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            bindings=statement.bindings,
            distinct=statement.distinct,
            limit=statement.limit,
            skip=statement.skip,
            source=source,
        )
    if isinstance(statement, NormalizedUnwind):
        return GraphRelationalReadIR(
            match_kind="unwind",
            source_kind="unwind",
            returns=statement.returns,
            order_by=statement.order_by,
            limit=statement.limit,
            skip=statement.skip,
            unwind_alias=statement.alias,
            unwind_source_kind=statement.source_kind,
            unwind_source_items=statement.source_items,
        )
    return None


def _build_write_ir(
    statement: NormalizedCypherStatement,
) -> GraphRelationalWriteIR | None:
    if isinstance(statement, NormalizedCreateNode):
        return GraphRelationalCreateNodeWriteIR(node=statement.node)
    if isinstance(statement, NormalizedCreateRelationship):
        return GraphRelationalCreateRelationshipWriteIR(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedCreateRelationshipFromSeparatePatterns):
        return GraphRelationalCreateRelationshipFromSeparatePatternsWriteIR(
            first_node=statement.first_node,
            second_node=statement.second_node,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMergeNode):
        return GraphRelationalMergeNodeWriteIR(node=statement.node)
    if isinstance(statement, NormalizedMergeRelationship):
        return GraphRelationalMergeRelationshipWriteIR(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedSetNode):
        return GraphRelationalSetNodeWriteIR(
            node=statement.node,
            predicates=statement.predicates,
            assignments=statement.assignments,
        )
    if isinstance(statement, NormalizedSetRelationship):
        return GraphRelationalSetRelationshipWriteIR(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
            predicates=statement.predicates,
            assignments=statement.assignments,
        )
    if isinstance(statement, NormalizedDeleteNode):
        return GraphRelationalDeleteNodeWriteIR(
            node=statement.node,
            predicates=statement.predicates,
            detach=statement.detach,
        )
    if isinstance(statement, NormalizedDeleteRelationship):
        return GraphRelationalDeleteRelationshipWriteIR(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
            predicates=statement.predicates,
        )
    if isinstance(statement, NormalizedMatchMergeRelationshipOnNode):
        return GraphRelationalMatchMergeRelationshipOnNodeWriteIR(
            match_node=statement.match_node,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMatchMergeRelationship):
        return GraphRelationalMatchMergeRelationshipWriteIR(
            left_match=statement.left_match,
            right_match=statement.right_match,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMatchMergeRelationshipFromTraversal):
        source = _build_read_ir(statement.source)
        if source is None:
            return None
        return GraphRelationalMatchMergeRelationshipFromTraversalWriteIR(
            source=source,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMatchCreateRelationship):
        return GraphRelationalMatchCreateRelationshipOnNodeWriteIR(
            match_node=statement.match_node,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMatchCreateRelationshipFromTraversal):
        source = _build_read_ir(statement.source)
        if source is None:
            return None
        return GraphRelationalMatchCreateRelationshipFromTraversalWriteIR(
            source=source,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    if isinstance(statement, NormalizedMatchCreateRelationshipBetweenNodes):
        return GraphRelationalMatchCreateRelationshipBetweenNodesWriteIR(
            left_match=statement.left_match,
            right_match=statement.right_match,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )
    return None
