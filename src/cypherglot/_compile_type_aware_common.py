from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ._compile_sql_utils import _compile_stream_predicate, _sql_literal, _sql_value
from .ir import BACKEND_CAPABILITIES, BackendCapabilities, SQLBackend
from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    CypherValue,
    Predicate,
)
from .normalize import WithBinding, WithCaseSpec, WithReturnItem
from .schema import property_column_name


def _compile_type_aware_size_expression(expression: str) -> str:
    return f"LENGTH(CAST({expression} AS TEXT))"


@dataclass(frozen=True, slots=True)
class _TypeAwareWithBindingSpec:
    binding: WithBinding
    entity_type: object | None = None
    scalar_logical_type: str | None = None
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
    backend: SQLBackend,
) -> str:
    if predicate.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = predicate.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        expression = _compile_type_aware_node_field_expression(
            alias,
            node_type,
            inner_field,
        )
        return _compile_stream_predicate(
            _compile_type_aware_size_expression(expression),
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
        backend=backend,
        is_statically_numeric=_is_type_aware_entity_field_numeric(
            node_type,
            predicate.field,
        ),
    )


def _compile_type_aware_match_relationship_predicate(
    alias: str,
    edge_type: object,
    predicate: Predicate,
    backend: SQLBackend,
) -> str:
    if predicate.field.startswith(_SIZE_PREDICATE_FIELD_PREFIX):
        inner_field = predicate.field.removeprefix(_SIZE_PREDICATE_FIELD_PREFIX)
        expression = _compile_type_aware_size_expression(
            _compile_type_aware_edge_field_expression(alias, edge_type, inner_field)
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
        backend=backend,
        is_statically_numeric=_is_type_aware_entity_field_numeric(
            edge_type,
            predicate.field,
        ),
    )


def _is_type_aware_numeric_literal(value: CypherValue) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_type_aware_numeric_logical_type(logical_type: object | None) -> bool:
    return logical_type in {"integer", "float"}


def _type_aware_property_logical_type(
    entity_type: object | None,
    field: str,
) -> str | None:
    if entity_type is None:
        return None
    for property_field in entity_type.properties:
        if property_field.name == field:
            return property_field.logical_type
    return None


def _type_aware_entity_field_logical_type(
    entity_type: object | None,
    field: str,
) -> str | None:
    if field == "id":
        return "integer"
    if field in {"label", "type"}:
        return "string"
    return _type_aware_property_logical_type(entity_type, field)


def _is_type_aware_entity_field_numeric(
    entity_type: object | None,
    field: str,
) -> bool:
    logical_type = _type_aware_entity_field_logical_type(entity_type, field)
    return _is_type_aware_numeric_logical_type(logical_type)


def _type_aware_binding_logical_type(
    binding_spec: _TypeAwareWithBindingSpec,
    field: str | None = None,
) -> str | None:
    binding = binding_spec.binding
    if binding.binding_kind == "scalar":
        if field is not None:
            return None
        return binding_spec.scalar_logical_type
    if field is None:
        return None
    return _type_aware_entity_field_logical_type(binding_spec.entity_type, field)


def _is_type_aware_binding_numeric(
    binding_spec: _TypeAwareWithBindingSpec,
    field: str | None = None,
) -> bool:
    logical_type = _type_aware_binding_logical_type(binding_spec, field)
    return _is_type_aware_numeric_logical_type(logical_type)


def _build_type_aware_with_binding_spec(
    binding: WithBinding,
    *,
    entity_type: object | None,
    scalar_logical_type: str | None = None,
    start_binding_output_alias: str | None = None,
    end_binding_output_alias: str | None = None,
) -> _TypeAwareWithBindingSpec:
    resolved_scalar_logical_type = scalar_logical_type
    if binding.binding_kind == "scalar" and resolved_scalar_logical_type is None:
        source_field = binding.source_field
        if source_field is not None:
            resolved_scalar_logical_type = _type_aware_entity_field_logical_type(
                entity_type,
                source_field,
            )
    return _TypeAwareWithBindingSpec(
        binding=binding,
        entity_type=entity_type,
        scalar_logical_type=resolved_scalar_logical_type,
        start_binding_output_alias=start_binding_output_alias,
        end_binding_output_alias=end_binding_output_alias,
    )


def _type_aware_scalar_literal_logical_type(value: object | None) -> str | None:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return None


def _type_aware_with_expression_logical_type(
    item: WithReturnItem,
    binding_specs: dict[str, _TypeAwareWithBindingSpec],
) -> str | None:
    if item.kind == "case":
        assert isinstance(item.value, WithCaseSpec)
        branch_types = [
            _type_aware_with_expression_logical_type(arm.result, binding_specs)
            for arm in item.value.when_items
        ]
        branch_types.append(
            _type_aware_with_expression_logical_type(item.value.else_item, binding_specs)
        )
        distinct_types = {logical_type for logical_type in branch_types if logical_type is not None}
        if len(distinct_types) == 1:
            return next(iter(distinct_types))
        return None
    if item.kind == "scalar_value":
        return _type_aware_scalar_literal_logical_type(item.value)
    if item.kind == "scalar":
        return _type_aware_binding_logical_type(binding_specs[item.alias], item.field)
    if item.kind == "field":
        return _type_aware_binding_logical_type(binding_specs[item.alias], item.field)
    if item.kind == "id":
        return "integer"
    if item.kind == "type":
        return "string"
    if item.kind == "size":
        return "integer"
    if item.kind == "predicate":
        return "boolean"
    if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse", "replace", "left", "right", "substring", "to_string"}:
        return "string"
    if item.kind == "to_integer":
        return "integer"
    if item.kind == "to_float":
        return "float"
    if item.kind == "to_boolean":
        return "boolean"
    if item.kind == "coalesce":
        input_type = _type_aware_binding_logical_type(binding_specs[item.alias], item.field)
        if input_type is not None:
            return input_type
        return _type_aware_scalar_literal_logical_type(item.value)
    if item.kind in {"abs", "sign", "round", "ceil", "floor"}:
        input_type = _type_aware_binding_logical_type(binding_specs[item.alias], item.field)
        if input_type in {"integer", "float"}:
            return input_type
        return None
    if item.kind in {"sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees"}:
        return "float"
    return None


def _type_aware_backend_capabilities(backend: SQLBackend) -> BackendCapabilities:
    return BACKEND_CAPABILITIES[backend]


def _type_aware_backend_requires_numeric_coercion(backend: SQLBackend) -> bool:
    return (
        _type_aware_backend_capabilities(backend).numeric_coercion_sql_type
        is not None
    )


def _type_aware_backend_requires_truncating_integer_cast(
    backend: SQLBackend,
) -> bool:
    return _type_aware_backend_capabilities(backend).integer_cast_requires_truncation


def _compile_type_aware_numeric_coercion_expression(
    expression: str,
    backend: SQLBackend,
) -> str:
    capabilities = _type_aware_backend_capabilities(backend)
    target_type = capabilities.numeric_coercion_sql_type
    if target_type is None:
        return expression
    cast_name = "TRY_CAST" if capabilities.numeric_coercion_is_tolerant else "CAST"
    return f"{cast_name}({expression} AS {target_type})"


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
    backend: SQLBackend,
    is_statically_numeric: bool = False,
) -> str:
    if operator == "IS NULL":
        return f"{field_expression} IS NULL"
    if operator == "IS NOT NULL":
        return f"{field_expression} IS NOT NULL"

    value_sql = _sql_value(value)
    predicate_expression = field_expression
    if (
        _type_aware_backend_requires_numeric_coercion(backend)
        and operator in {"=", "<", "<=", ">", ">="}
        and value is not None
        and _is_type_aware_numeric_literal(value)
        and not is_statically_numeric
    ):
        predicate_expression = _compile_type_aware_numeric_coercion_expression(
            field_expression,
            backend,
        )
    if operator == "=":
        if value is None:
            return f"{field_expression} IS NULL"
        return f"{predicate_expression} = {value_sql}"
    if operator in {"<", "<=", ">", ">="}:
        return f"{predicate_expression} {operator} {value_sql}"
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
