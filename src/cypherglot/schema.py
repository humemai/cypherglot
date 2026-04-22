from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Literal


SchemaBackend = Literal["sqlite", "duckdb", "postgresql"]


@dataclass(frozen=True)
class SchemaBackendCapabilities:
    logical_type_sql: dict[str, str]
    boolean_check_constraint: bool = False
    needs_foreign_key_pragma: bool = False
    needs_sequence_objects: bool = True
    reference_id_sql_type: str = "BIGINT"
    include_foreign_keys: bool = True
    strict_tables: bool = False


_IDENTIFIER_PART_RE = re.compile(r"[^a-z0-9]+")
_SCHEMA_BACKEND_CAPABILITIES: dict[SchemaBackend, SchemaBackendCapabilities] = {
    "sqlite": SchemaBackendCapabilities(
        logical_type_sql={
            "string": "TEXT",
            "integer": "INTEGER",
            "float": "REAL",
            "boolean": "INTEGER",
        },
        boolean_check_constraint=True,
        needs_foreign_key_pragma=True,
        needs_sequence_objects=False,
        reference_id_sql_type="INTEGER",
        strict_tables=True,
    ),
    "duckdb": SchemaBackendCapabilities(
        logical_type_sql={
            "string": "VARCHAR",
            "integer": "BIGINT",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
        },
        include_foreign_keys=False,
    ),
    "postgresql": SchemaBackendCapabilities(
        logical_type_sql={
            "string": "TEXT",
            "integer": "BIGINT",
            "float": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
        },
    ),
}


class SchemaContractError(ValueError):
    """Raised when a declared graph schema or schema context is invalid."""


def _validated_backend(backend: SchemaBackend) -> SchemaBackend:
    if backend not in _SCHEMA_BACKEND_CAPABILITIES:
        supported = ", ".join(sorted(_SCHEMA_BACKEND_CAPABILITIES))
        raise SchemaContractError(
            f"Unsupported schema backend {backend!r}. Supported: {supported}."
        )
    return backend


def _schema_backend_capabilities(backend: SchemaBackend) -> SchemaBackendCapabilities:
    return _SCHEMA_BACKEND_CAPABILITIES[_validated_backend(backend)]


def _identifier_suffix(name: str) -> str:
    cleaned = _IDENTIFIER_PART_RE.sub("_", name.strip().lower()).strip("_")
    if not cleaned:
        raise SchemaContractError("Schema names must contain letters or digits.")
    return cleaned


def node_table_name(node_type_name: str) -> str:
    """Return the normalized relational table name for a node type."""

    return f"cg_node_{_identifier_suffix(node_type_name)}"


def edge_table_name(edge_type_name: str) -> str:
    """Return the normalized relational table name for an edge type."""

    return f"cg_edge_{_identifier_suffix(edge_type_name)}"


def property_column_name(property_name: str) -> str:
    """Return the normalized relational column name for a property."""

    return _identifier_suffix(property_name)


def _id_sequence_name(table_name: str) -> str:
    return f"{table_name}_id_seq"


@dataclass(frozen=True)
class PropertyField:
    """A logical property definition shared by node and edge type specs."""

    name: str
    logical_type: str
    nullable: bool = True

    @property
    def column_name(self) -> str:
        return property_column_name(self.name)

    def column_sql(self, backend: SchemaBackend) -> str:
        backend = _validated_backend(backend)
        capabilities = _schema_backend_capabilities(backend)
        sql_type = capabilities.logical_type_sql.get(self.logical_type)
        if sql_type is None:
            supported = ", ".join(sorted(capabilities.logical_type_sql))
            raise SchemaContractError(
                "Unsupported logical type "
                f"{self.logical_type!r}. Supported: {supported}."
            )

        column_name = self.column_name
        constraints = [f"{column_name} {sql_type}"]
        if not self.nullable:
            constraints.append("NOT NULL")
        if capabilities.boolean_check_constraint and self.logical_type == "boolean":
            constraints.append(f"CHECK ({column_name} IN (0, 1))")
        return " ".join(constraints)


@dataclass(frozen=True)
class NodeTypeSpec:
    """A declared node type and its typed property columns."""

    name: str
    properties: tuple[PropertyField, ...] = field(default_factory=tuple)

    @property
    def table_name(self) -> str:
        return node_table_name(self.name)


@dataclass(frozen=True)
class EdgeTypeSpec:
    """A declared edge type between source and target node types."""

    name: str
    source_type: str
    target_type: str
    properties: tuple[PropertyField, ...] = field(default_factory=tuple)

    @property
    def table_name(self) -> str:
        return edge_table_name(self.name)


@dataclass(frozen=True)
class PropertyIndexSpec:
    """A secondary index declaration over node or edge property columns."""

    name: str
    target_kind: Literal["node", "edge"]
    target_type: str
    property_names: tuple[str, ...]

    @property
    def index_name(self) -> str:
        return _identifier_suffix(self.name)


@dataclass(frozen=True)
class GraphSchema:
    """A type-aware graph schema that can validate itself and emit backend DDL."""

    node_types: tuple[NodeTypeSpec, ...]
    edge_types: tuple[EdgeTypeSpec, ...]
    property_indexes: tuple[PropertyIndexSpec, ...] = field(default_factory=tuple)

    def node_type(self, name: str) -> NodeTypeSpec:
        """Return the declared node type with the given product-level name."""

        for node_type in self.node_types:
            if node_type.name == name:
                return node_type
        raise SchemaContractError(f"Unknown node type {name!r}.")

    def edge_type(self, name: str) -> EdgeTypeSpec:
        """Return the declared edge type with the given product-level name."""

        for edge_type in self.edge_types:
            if edge_type.name == name:
                return edge_type
        raise SchemaContractError(f"Unknown edge type {name!r}.")

    def validate(self) -> None:
        """Validate schema identity, references, and declared property indexes."""

        node_names = [node_type.name for node_type in self.node_types]
        if len(set(node_names)) != len(node_names):
            raise SchemaContractError("Node type names must be unique.")

        edge_names = [edge_type.name for edge_type in self.edge_types]
        if len(set(edge_names)) != len(edge_names):
            raise SchemaContractError("Edge type names must be unique.")

        table_names = [node_type.table_name for node_type in self.node_types]
        table_names.extend(edge_type.table_name for edge_type in self.edge_types)
        if len(set(table_names)) != len(table_names):
            raise SchemaContractError(
                "Schema names collide after identifier normalization."
            )

        defined_node_types = set(node_names)
        for edge_type in self.edge_types:
            if edge_type.source_type not in defined_node_types:
                raise SchemaContractError(
                    f"Edge type {edge_type.name!r} references unknown source "
                    f"node type {edge_type.source_type!r}."
                )
            if edge_type.target_type not in defined_node_types:
                raise SchemaContractError(
                    f"Edge type {edge_type.name!r} references unknown target "
                    f"node type {edge_type.target_type!r}."
                )

        normalized_index_names = [
            property_index.index_name for property_index in self.property_indexes
        ]
        if len(set(normalized_index_names)) != len(normalized_index_names):
            raise SchemaContractError(
                "Property index names must be unique after identifier normalization."
            )

        node_types_by_name = {
            node_type.name: node_type for node_type in self.node_types
        }
        edge_types_by_name = {
            edge_type.name: edge_type for edge_type in self.edge_types
        }
        for property_index in self.property_indexes:
            if not property_index.property_names:
                raise SchemaContractError(
                    f"Property index {property_index.name!r} must target at least "
                    "one property column."
                )

            if property_index.target_kind == "node":
                target_spec = node_types_by_name.get(property_index.target_type)
                if target_spec is None:
                    raise SchemaContractError(
                        f"Property index {property_index.name!r} references "
                        f"unknown node type {property_index.target_type!r}."
                    )
            else:
                target_spec = edge_types_by_name.get(property_index.target_type)
                if target_spec is None:
                    raise SchemaContractError(
                        f"Property index {property_index.name!r} references "
                        f"unknown edge type {property_index.target_type!r}."
                    )

            defined_property_names = {
                property_field.name for property_field in target_spec.properties
            }
            for property_name in property_index.property_names:
                if property_name not in defined_property_names:
                    raise SchemaContractError(
                        f"Property index {property_index.name!r} references "
                        f"unknown {property_index.target_kind} property "
                        f"{property_name!r} on {property_index.target_type!r}."
                    )

    def ddl(self, backend: SchemaBackend) -> list[str]:
        """Render backend-specific DDL for the validated graph schema."""

        self.validate()
        backend = _validated_backend(backend)
        capabilities = _schema_backend_capabilities(backend)

        ddl: list[str] = []
        if capabilities.needs_foreign_key_pragma:
            ddl.append("PRAGMA foreign_keys = ON;")
        node_table_by_name = {
            node_type.name: node_type.table_name for node_type in self.node_types
        }

        for node_type in self.node_types:
            ddl.extend(self._pre_table_ddl(node_type.table_name, backend))
            column_lines = [self._id_column_sql(node_type.table_name, backend)]
            column_lines.extend(
                property_field.column_sql(backend)
                for property_field in node_type.properties
            )
            ddl.append(
                self._create_table_sql(
                    node_type.table_name,
                    column_lines,
                    backend,
                )
            )
            ddl.extend(self._property_index_ddl(node_type))

        for edge_type in self.edge_types:
            ddl.extend(self._pre_table_ddl(edge_type.table_name, backend))
            column_lines = [
                self._id_column_sql(edge_type.table_name, backend),
                self._reference_id_column_sql(backend, "from_id"),
                self._reference_id_column_sql(backend, "to_id"),
            ]
            column_lines.extend(
                property_field.column_sql(backend)
                for property_field in edge_type.properties
            )
            column_lines.extend(
                self._foreign_key_sql(
                    backend,
                    column_name="from_id",
                    referenced_table=node_table_by_name[edge_type.source_type],
                )
            )
            column_lines.extend(
                self._foreign_key_sql(
                    backend,
                    column_name="to_id",
                    referenced_table=node_table_by_name[edge_type.target_type],
                )
            )
            ddl.append(
                self._create_table_sql(
                    edge_type.table_name,
                    column_lines,
                    backend,
                )
            )
            ddl.append(
                f"CREATE INDEX idx_{edge_type.table_name}_from_id "
                f"ON {edge_type.table_name}(from_id);"
            )
            ddl.append(
                f"CREATE INDEX idx_{edge_type.table_name}_to_id "
                f"ON {edge_type.table_name}(to_id);"
            )
            ddl.append(
                f"CREATE INDEX idx_{edge_type.table_name}_from_to "
                f"ON {edge_type.table_name}(from_id, to_id);"
            )
            ddl.append(
                f"CREATE INDEX idx_{edge_type.table_name}_to_from "
                f"ON {edge_type.table_name}(to_id, from_id);"
            )
            ddl.extend(self._property_index_ddl(edge_type))

        return ddl

    def _property_index_ddl(
        self,
        graph_type: NodeTypeSpec | EdgeTypeSpec,
    ) -> list[str]:
        target_kind = "node" if isinstance(graph_type, NodeTypeSpec) else "edge"
        statements: list[str] = []
        for property_index in self.property_indexes:
            if property_index.target_kind != target_kind:
                continue
            if property_index.target_type != graph_type.name:
                continue

            column_list = ", ".join(
                property_column_name(property_name)
                for property_name in property_index.property_names
            )
            statements.append(
                f"CREATE INDEX {property_index.index_name} "
                f"ON {graph_type.table_name}({column_list});"
            )

        return statements

    def _pre_table_ddl(self, table_name: str, backend: SchemaBackend) -> list[str]:
        capabilities = _schema_backend_capabilities(backend)
        if not capabilities.needs_sequence_objects:
            return []
        return [f"CREATE SEQUENCE {_id_sequence_name(table_name)} START 1;"]

    def _id_column_sql(self, table_name: str, backend: SchemaBackend) -> str:
        capabilities = _schema_backend_capabilities(backend)
        if not capabilities.needs_sequence_objects:
            return "id INTEGER PRIMARY KEY"
        return (
            "id BIGINT PRIMARY KEY DEFAULT "
            f"nextval('{_id_sequence_name(table_name)}')"
        )

    def _reference_id_column_sql(
        self,
        backend: SchemaBackend,
        column_name: str,
    ) -> str:
        capabilities = _schema_backend_capabilities(backend)
        return f"{column_name} {capabilities.reference_id_sql_type} NOT NULL"

    def _foreign_key_sql(
        self,
        backend: SchemaBackend,
        column_name: str,
        referenced_table: str,
    ) -> list[str]:
        if not _schema_backend_capabilities(backend).include_foreign_keys:
            return []
        return [
            "FOREIGN KEY "
            f"({column_name}) REFERENCES {referenced_table}(id) ON DELETE CASCADE"
        ]

    def _create_table_sql(
        self,
        table_name: str,
        column_lines: list[str],
        backend: SchemaBackend,
    ) -> str:
        table_suffix = (
            " STRICT" if _schema_backend_capabilities(backend).strict_tables else ""
        )
        return (
            "CREATE TABLE "
            f"{table_name} (\n  "
            + ",\n  ".join(column_lines)
            + f"\n){table_suffix};"
        )


@dataclass(frozen=True)
class CompilerSchemaContext:
    """Schema information needed by the compiler for backend-aware lowering."""

    layout: Literal["type-aware"] = "type-aware"
    graph_schema: GraphSchema | None = None

    def validate(self) -> None:
        """Validate that the context contains the schema required by its layout."""

        if self.graph_schema is None:
            raise SchemaContractError(
                "Type-aware schema context requires an explicit GraphSchema."
            )
        self.graph_schema.validate()

    @classmethod
    def type_aware(
        cls,
        graph_schema: GraphSchema,
    ) -> CompilerSchemaContext:
        """Build and validate the standard type-aware compiler schema context."""

        context = cls(
            layout="type-aware",
            graph_schema=graph_schema,
        )
        context.validate()
        return context
