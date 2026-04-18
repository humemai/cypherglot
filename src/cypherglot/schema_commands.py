from __future__ import annotations

import re

from .schema import (
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyIndexSpec,
    PropertyField,
    SchemaBackend,
    SchemaContractError,
)


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_NODE_COMMAND_RE = re.compile(
    r"^CREATE\s+NODE\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)"
    r"(?:\s*\((?P<properties>.*)\))?$",
    re.IGNORECASE | re.DOTALL,
)
_EDGE_COMMAND_RE = re.compile(
    r"^CREATE\s+EDGE\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\s+"
    r"FROM\s+(?P<source>[A-Za-z][A-Za-z0-9_]*)\s+"
    r"TO\s+(?P<target>[A-Za-z][A-Za-z0-9_]*)"
    r"(?:\s*\((?P<properties>.*)\))?$",
    re.IGNORECASE | re.DOTALL,
)
_INDEX_COMMAND_RE = re.compile(
    r"^CREATE\s+INDEX\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\s+ON\s+"
    r"(?P<target_kind>NODE|EDGE)\s+"
    r"(?P<target>[A-Za-z][A-Za-z0-9_]*)\s*"
    r"\((?P<properties>[^)]*)\)$",
    re.IGNORECASE | re.DOTALL,
)
_PROPERTY_RE = re.compile(
    r"^(?P<name>[A-Za-z][A-Za-z0-9_]*)\s+"
    r"(?P<logical_type>STRING|INTEGER|FLOAT|BOOLEAN)"
    r"(?:\s+(?P<nullability>NOT\s+NULL|NULL))?$",
    re.IGNORECASE,
)


def graph_schema_from_text(text: str) -> GraphSchema:
    node_types: list[NodeTypeSpec] = []
    edge_types: list[EdgeTypeSpec] = []
    property_indexes: list[PropertyIndexSpec] = []

    for statement in _split_schema_statements(text):
        node_match = _NODE_COMMAND_RE.fullmatch(statement)
        if node_match is not None:
            node_types.append(
                NodeTypeSpec(
                    name=_parse_schema_name(node_match.group("name")),
                    properties=_parse_property_fields(
                        node_match.group("properties")
                    ),
                )
            )
            continue

        edge_match = _EDGE_COMMAND_RE.fullmatch(statement)
        if edge_match is not None:
            edge_types.append(
                EdgeTypeSpec(
                    name=_parse_schema_name(edge_match.group("name")),
                    source_type=_parse_schema_name(edge_match.group("source")),
                    target_type=_parse_schema_name(edge_match.group("target")),
                    properties=_parse_property_fields(
                        edge_match.group("properties")
                    ),
                )
            )
            continue

        index_match = _INDEX_COMMAND_RE.fullmatch(statement)
        if index_match is not None:
            property_indexes.append(
                PropertyIndexSpec(
                    name=_parse_schema_name(index_match.group("name")),
                    target_kind=index_match.group("target_kind").lower(),
                    target_type=_parse_schema_name(index_match.group("target")),
                    property_names=_parse_index_property_names(
                        index_match.group("properties")
                    ),
                )
            )
            continue

        raise SchemaContractError(
            "Unsupported schema command. Use CREATE NODE <Type> (...), "
            "CREATE EDGE <Type> FROM <Source> TO <Target> (...), or "
            "CREATE INDEX <Name> ON NODE|EDGE <Type> (...)."
        )

    schema = GraphSchema(
        node_types=tuple(node_types),
        edge_types=tuple(edge_types),
        property_indexes=tuple(property_indexes),
    )
    schema.validate()
    return schema


def schema_ddl_from_text(text: str, backend: SchemaBackend) -> list[str]:
    return graph_schema_from_text(text).ddl(backend)


def _split_schema_statements(text: str) -> tuple[str, ...]:
    stripped = text.strip()
    if not stripped:
        raise SchemaContractError("Schema definition text must not be empty.")

    statements = [part.strip() for part in stripped.split(";") if part.strip()]
    if not statements:
        raise SchemaContractError("Schema definition text must not be empty.")
    return tuple(statements)


def _parse_schema_name(name: str) -> str:
    candidate = name.strip()
    if _SCHEMA_NAME_RE.fullmatch(candidate) is None:
        raise SchemaContractError(
            f"Invalid schema identifier {name!r}. Use letters, digits, and _."
        )
    return candidate


def _parse_property_fields(properties_text: str | None) -> tuple[PropertyField, ...]:
    if properties_text is None:
        return ()

    stripped = properties_text.strip()
    if not stripped:
        return ()

    return tuple(
        _parse_property_field(property_text)
        for property_text in stripped.split(",")
        if property_text.strip()
    )


def _parse_property_field(property_text: str) -> PropertyField:
    match = _PROPERTY_RE.fullmatch(property_text.strip())
    if match is None:
        raise SchemaContractError(
            "Invalid property declaration. Use <name> <type> with optional "
            "NOT NULL, for example: name STRING NOT NULL."
        )

    nullability = match.group("nullability")
    return PropertyField(
        name=match.group("name"),
        logical_type=match.group("logical_type").lower(),
        nullable=nullability is None or nullability.upper() == "NULL",
    )


def _parse_index_property_names(properties_text: str) -> tuple[str, ...]:
    property_names = tuple(
        _parse_schema_name(property_text)
        for property_text in properties_text.split(",")
        if property_text.strip()
    )
    if not property_names:
        raise SchemaContractError(
            "CREATE INDEX requires at least one property name."
        )
    return property_names
