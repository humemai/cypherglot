from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Literal


_IDENTIFIER_PART_RE = re.compile(r"[^a-z0-9]+")
_SQLITE_TYPE_BY_LOGICAL_TYPE = {
    "string": "TEXT",
    "integer": "INTEGER",
    "float": "REAL",
    "boolean": "INTEGER",
    "json": "TEXT",
}


class SchemaContractError(ValueError):
    pass


def _identifier_suffix(name: str) -> str:
    cleaned = _IDENTIFIER_PART_RE.sub("_", name.strip().lower()).strip("_")
    if not cleaned:
        raise SchemaContractError("Schema names must contain letters or digits.")
    return cleaned


def node_table_name(node_type_name: str) -> str:
    return f"cg_node_{_identifier_suffix(node_type_name)}"


def edge_table_name(edge_type_name: str) -> str:
    return f"cg_edge_{_identifier_suffix(edge_type_name)}"


def property_column_name(property_name: str) -> str:
    return _identifier_suffix(property_name)


@dataclass(frozen=True)
class PropertyField:
    name: str
    logical_type: str
    nullable: bool = True

    @property
    def column_name(self) -> str:
        return property_column_name(self.name)

    def sqlite_column_sql(self) -> str:
        sqlite_type = _SQLITE_TYPE_BY_LOGICAL_TYPE.get(self.logical_type)
        if sqlite_type is None:
            supported = ", ".join(sorted(_SQLITE_TYPE_BY_LOGICAL_TYPE))
            raise SchemaContractError(
                "Unsupported logical type "
                f"{self.logical_type!r}. Supported: {supported}."
            )

        column_name = self.column_name
        constraints = [f"{column_name} {sqlite_type}"]
        if not self.nullable:
            constraints.append("NOT NULL")
        if self.logical_type == "boolean":
            constraints.append(f"CHECK ({column_name} IN (0, 1))")
        if self.logical_type == "json":
            constraints.append(f"CHECK (json_valid({column_name}))")
        return " ".join(constraints)


@dataclass(frozen=True)
class NodeTypeSpec:
    name: str
    properties: tuple[PropertyField, ...] = field(default_factory=tuple)

    @property
    def table_name(self) -> str:
        return node_table_name(self.name)


@dataclass(frozen=True)
class EdgeTypeSpec:
    name: str
    source_type: str
    target_type: str
    properties: tuple[PropertyField, ...] = field(default_factory=tuple)

    @property
    def table_name(self) -> str:
        return edge_table_name(self.name)


@dataclass(frozen=True)
class GraphSchema:
    node_types: tuple[NodeTypeSpec, ...]
    edge_types: tuple[EdgeTypeSpec, ...]

    def node_type(self, name: str) -> NodeTypeSpec:
        for node_type in self.node_types:
            if node_type.name == name:
                return node_type
        raise SchemaContractError(f"Unknown node type {name!r}.")

    def edge_type(self, name: str) -> EdgeTypeSpec:
        for edge_type in self.edge_types:
            if edge_type.name == name:
                return edge_type
        raise SchemaContractError(f"Unknown edge type {name!r}.")

    def validate(self) -> None:
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

    def sqlite_ddl(self) -> list[str]:
        self.validate()

        ddl: list[str] = ["PRAGMA foreign_keys = ON;"]
        node_table_by_name = {
            node_type.name: node_type.table_name for node_type in self.node_types
        }

        for node_type in self.node_types:
            column_lines = ["id INTEGER PRIMARY KEY"]
            column_lines.extend(
                property_field.sqlite_column_sql()
                for property_field in node_type.properties
            )
            ddl.append(
                "CREATE TABLE "
                f"{node_type.table_name} (\n  "
                + ",\n  ".join(column_lines)
                + "\n) STRICT;"
            )

        for edge_type in self.edge_types:
            column_lines = [
                "id INTEGER PRIMARY KEY",
                "from_id INTEGER NOT NULL",
                "to_id INTEGER NOT NULL",
            ]
            column_lines.extend(
                property_field.sqlite_column_sql()
                for property_field in edge_type.properties
            )
            column_lines.append(
                "FOREIGN KEY (from_id) REFERENCES "
                f"{node_table_by_name[edge_type.source_type]}(id) ON DELETE CASCADE"
            )
            column_lines.append(
                "FOREIGN KEY (to_id) REFERENCES "
                f"{node_table_by_name[edge_type.target_type]}(id) ON DELETE CASCADE"
            )
            ddl.append(
                "CREATE TABLE "
                f"{edge_type.table_name} (\n  "
                + ",\n  ".join(column_lines)
                + "\n) STRICT;"
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

        return ddl


@dataclass(frozen=True)
class CompilerSchemaContext:
    layout: Literal["type-aware"] = "type-aware"
    graph_schema: GraphSchema | None = None

    def validate(self) -> None:
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
        context = cls(
            layout="type-aware",
            graph_schema=graph_schema,
        )
        context.validate()
        return context
