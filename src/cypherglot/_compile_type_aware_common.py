from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ._compile_sql_utils import _compile_stream_predicate, _sql_literal, _sql_value
from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    CypherValue,
    Predicate,
)
from .normalize import WithBinding
from .schema import property_column_name


@dataclass(frozen=True, slots=True)
class _TypeAwareWithBindingSpec:
    binding: WithBinding
    entity_type: object | None = None
    start_binding_output_alias: str | None = None
    end_binding_output_alias: str | None = None


@dataclass(frozen=True, slots=True)
class _TypeAwareAliasSpec:
    table_alias: str
    alias_kind: Literal["node", "relationship"]
    entity_type: object
    start_node_alias: str | None = None
    end_node_alias: str | None = None


def _with_entity_prefix(alias: str) -> str:
    return f"__cg_with_{alias}"


def _with_scalar_prefix(alias: str) -> str:
    return f"__cg_with_scalar_{alias}"


def _type_aware_with_property_column(output_alias: str, field: str) -> str:
    return f"{_with_entity_prefix(output_alias)}_prop_{property_column_name(field)}"


def _compile_type_aware_match_node_predicate(
    alias: str,
    node_type: object,
    predicate: Predicate,
) -> str:
    if predicate.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = predicate.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        expression = _compile_type_aware_node_field_expression(
            alias,
            node_type,
            inner_field,
        )
        return _compile_stream_predicate(
            f"LENGTH({expression})",
            None,
            predicate.operator,
            predicate.value,
        )

    if predicate.field == "id":
        if predicate.operator in {"IS NULL", "IS NOT NULL"}:
            raise ValueError(
                "Type-aware lowering does not support null predicates on id."
            )
        return f"{alias}.id {predicate.operator} {_sql_value(predicate.value)}"

    if predicate.field == "label":
        if predicate.operator != "=":
            raise ValueError(
                "Type-aware lowering supports only equality predicates on label."
            )
        return f"{_sql_literal(node_type.name)} = {_sql_value(predicate.value)}"

    return _compile_type_aware_predicate(
        field_expression=_compile_type_aware_node_field_expression(
            alias,
            node_type,
            predicate.field,
        ),
        operator=predicate.operator,
        value=predicate.value,
    )


def _compile_type_aware_match_relationship_predicate(
    alias: str,
    edge_type: object,
    predicate: Predicate,
) -> str:
    if predicate.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = predicate.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        expression = (
            "LENGTH("
            f"{_compile_type_aware_edge_field_expression(alias, edge_type, inner_field)}"
            ")"
        )
        return _compile_stream_predicate(
            expression,
            None,
            predicate.operator,
            predicate.value,
        )

    if predicate.field == "id":
        if predicate.operator in {"IS NULL", "IS NOT NULL"}:
            raise ValueError(
                "Type-aware lowering does not support null predicates on id."
            )
        return f"{alias}.id {predicate.operator} {_sql_value(predicate.value)}"

    if predicate.field == "type":
        if predicate.operator != "=":
            raise ValueError(
                "Type-aware lowering supports only equality predicates on type."
            )
        return f"{_sql_literal(edge_type.name)} = {_sql_value(predicate.value)}"

    return _compile_type_aware_predicate(
        field_expression=_compile_type_aware_edge_field_expression(
            alias,
            edge_type,
            predicate.field,
        ),
        operator=predicate.operator,
        value=predicate.value,
    )


def _compile_type_aware_predicate(
    *,
    field_expression: str,
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
        return f"{field_expression} IS NULL"
    if operator == "IS NOT NULL":
        return f"{field_expression} IS NOT NULL"

    value_sql = _sql_value(value)
    if operator == "=":
        if value is None:
            return f"{field_expression} IS NULL"
        return f"{field_expression} = {value_sql}"
    if operator in {"<", "<=", ">", ">="}:
        return f"{field_expression} {operator} {value_sql}"
    if operator == "STARTS WITH":
        return f"substr({field_expression}, 1, length({value_sql})) = {value_sql}"
    if operator == "ENDS WITH":
        return (
            f"length({field_expression}) >= length({value_sql}) AND "
            f"substr({field_expression}, length({field_expression}) - "
            f"length({value_sql}) + 1) = {value_sql}"
        )
    if operator == "CONTAINS":
        return f"instr({field_expression}, {value_sql}) > 0"
    raise ValueError(f"Unsupported predicate operator: {operator!r}")


def _compile_type_aware_node_field_expression(
    alias: str,
    node_type: object,
    field: str,
) -> str:
    if field == "id":
        return f"{alias}.id"
    if field == "label":
        return _sql_literal(node_type.name)

    for property_field in node_type.properties:
        if property_field.name == field:
            return f"{alias}.{property_field.column_name}"

    return f"{alias}.{property_column_name(field)}"


def _compile_type_aware_edge_field_expression(
    alias: str,
    edge_type: object,
    field: str,
) -> str:
    if field == "id":
        return f"{alias}.id"
    if field == "type":
        return _sql_literal(edge_type.name)

    for property_field in edge_type.properties:
        if property_field.name == field:
            return f"{alias}.{property_field.column_name}"

    return f"{alias}.{property_column_name(field)}"


def _compile_type_aware_with_binding_columns(
    binding: WithBinding,
    *,
    table_alias: str,
    entity_type: object,
) -> list[str]:
    if binding.binding_kind == "scalar":
        expression = _compile_type_aware_with_binding_expression(
            binding,
            table_alias,
            entity_type,
        )
        return [f'{expression} AS "{_with_scalar_prefix(binding.output_alias)}"']

    prefix = _with_entity_prefix(binding.output_alias)
    columns = [f'{table_alias}.id AS "{prefix}_id"']
    if binding.alias_kind == "relationship":
        columns.append(f'{table_alias}.from_id AS "{prefix}_from_id"')
        columns.append(f'{table_alias}.to_id AS "{prefix}_to_id"')
    for property_field in entity_type.properties:
        property_column = _type_aware_with_property_column(
            binding.output_alias,
            property_field.name,
        )
        columns.append(
            f'{table_alias}.{property_field.column_name} AS '
            f'"{property_column}"'
        )
    return columns


def _compile_type_aware_with_binding_expression(
    binding: WithBinding,
    table_alias: str,
    entity_type: object,
) -> str:
    if binding.alias_kind is None or binding.source_field is None:
        raise ValueError("Scalar WITH bindings require a source alias kind and field.")
    if binding.alias_kind == "node":
        return _compile_type_aware_node_field_expression(
            table_alias,
            entity_type,
            binding.source_field,
        )
    return _compile_type_aware_edge_field_expression(
        table_alias,
        entity_type,
        binding.source_field,
    )