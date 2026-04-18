from __future__ import annotations

from ._compile_sql_utils import _assemble_select_sql
from ._compile_type_aware_with_projection import (
    _compile_type_aware_with_group_by,
    _compile_type_aware_with_order_by,
    _compile_type_aware_with_predicates,
    _compile_type_aware_with_select_list,
)
from ._compile_type_aware_with_source import _compile_type_aware_with_source_sql
from .ir import GraphRelationalReadIR, SQLBackend
from .schema import GraphSchema


def _compile_type_aware_match_with_return_sql(
    statement: GraphRelationalReadIR,
    graph_schema: GraphSchema,
    backend: SQLBackend,
) -> str:
    inner_sql, binding_specs = _compile_type_aware_with_source_sql(
        statement,
        graph_schema,
        backend=backend,
    )
    select_sql = _compile_type_aware_with_select_list(
        statement.returns,
        binding_specs,
        backend=backend,
    )
    order_sql = _compile_type_aware_with_order_by(
        statement.order_by,
        binding_specs,
        backend=backend,
    )
    group_sql = _compile_type_aware_with_group_by(
        statement.returns,
        binding_specs,
        backend=backend,
    )
    where_parts = _compile_type_aware_with_predicates(
        statement.predicates,
        binding_specs,
        backend=backend,
    )
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
