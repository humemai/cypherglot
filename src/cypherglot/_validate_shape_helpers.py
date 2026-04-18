"""Shared shape-validation helpers extracted from validate.py."""

from __future__ import annotations

import re

from ._normalize_support import (
	_parse_node_pattern,
	_parse_query_nodes_limit_ref,
	_parse_query_nodes_order_items,
	_parse_query_nodes_return_items,
	_parse_relationship_chain_segment,
	_split_comma_separated,
	_split_query_nodes_return_and_order,
	_split_return_clause,
	_unwrap_node_pattern,
)
from .parser import CypherParseResult


def _context_text(result: CypherParseResult, ctx: object) -> str:
	start_index = ctx.start.tokenIndex
	stop_index = ctx.stop.tokenIndex
	return result.token_stream.getText(start=start_index, stop=stop_index)


def _validate_match_pattern_shape(
	result: CypherParseResult,
	match_ctx,
	*,
	allow_two_node_disconnected: bool,
	allow_optional: bool = False,
	allow_multi_hop: bool = False,
	allow_variable_length: bool = False,
) -> None:
	if match_ctx.OPTIONAL() is not None and not allow_optional:
		raise ValueError(
			"CypherGlot currently does not admit OPTIONAL MATCH in the supported subset."
		)

	pattern_ctx = match_ctx.oC_Pattern()
	pattern_parts = pattern_ctx.oC_PatternPart()
	chain_counts: list[int] = []

	for pattern_part in pattern_parts:
		if pattern_part.oC_Variable() is not None:
			raise ValueError(
				"CypherGlot currently does not admit named path patterns in the "
				"supported subset."
			)

		anonymous_part = pattern_part.oC_AnonymousPatternPart()
		pattern_element = anonymous_part.oC_PatternElement()
		chains = pattern_element.oC_PatternElementChain()
		if len(chains) > 1 and not allow_multi_hop:
			raise ValueError(
				"CypherGlot currently does not admit multi-hop pattern chains in "
				"the supported subset."
			)

		for chain in chains:
			relationship_detail = chain.oC_RelationshipPattern().oC_RelationshipDetail()
			if (
				relationship_detail is not None
				and relationship_detail.oC_RangeLiteral() is not None
			):
				if not allow_variable_length:
					raise ValueError(
						"CypherGlot currently does not admit variable-length "
						"relationship patterns in the supported subset."
					)

				if len(pattern_parts) != 1 or len(chains) != 1:
					raise ValueError(
						"CypherGlot currently admits bounded variable-length relationship patterns only as one directed MATCH relationship read."
					)

				relationship = _parse_relationship_chain_segment(
					_context_text(result, chain.oC_RelationshipPattern())
				)
				if relationship.min_hops < 0 or relationship.max_hops is None:
					raise ValueError(
						"CypherGlot currently admits only bounded non-negative-length variable-length relationship patterns in the supported subset."
					)
				if relationship.max_hops < relationship.min_hops:
					raise ValueError(
						"CypherGlot currently admits only bounded non-negative-length variable-length relationship patterns in the supported subset."
					)

		chain_counts.append(len(chains))

	if len(pattern_parts) <= 1:
		return

	if not allow_two_node_disconnected:
		raise ValueError(
			"CypherGlot currently does not admit disconnected multi-pattern MATCH "
			"clauses in the supported subset."
		)

	if len(pattern_parts) != 2 or any(chain_count != 0 for chain_count in chain_counts):
		raise ValueError(
			"CypherGlot currently admits at most two disconnected node patterns "
			"before narrow MATCH ... CREATE statements."
		)


def _extract_single_match_source_nodes(
	result: CypherParseResult,
	match_ctx,
) -> dict[str, tuple[str | None, tuple[tuple[str, object], ...]]]:
	pattern_element = (
		match_ctx.oC_Pattern().oC_PatternPart()[0].oC_AnonymousPatternPart().oC_PatternElement()
	)
	nodes = [
		_parse_node_pattern(
			_unwrap_node_pattern(_context_text(result, pattern_element.oC_NodePattern())),
			default_alias="__humem_validate_match_node_0",
		)
	]
	for index, chain in enumerate(pattern_element.oC_PatternElementChain(), start=1):
		nodes.append(
			_parse_node_pattern(
				_unwrap_node_pattern(_context_text(result, chain.oC_NodePattern())),
				default_alias=f"__humem_validate_match_node_{index}",
			)
		)
	return {node.alias: (node.label, node.properties) for node in nodes}


def _validate_traversal_write_endpoints(
	source_nodes: dict[str, tuple[str | None, tuple[tuple[str, object], ...]]],
	left: object,
	right: object,
	*,
	allow_one_new_endpoint: bool = False,
) -> None:
	reused_count = 0
	new_count = 0
	for endpoint in (left, right):
		source_node = source_nodes.get(endpoint.alias)
		if source_node is None:
			new_count += 1
			if not allow_one_new_endpoint:
				raise ValueError(
					"CypherGlot currently validates traversal-backed MATCH write clauses only when the relationship endpoints reuse matched node aliases exactly."
				)
			continue

		reused_count += 1
		source_label, _source_properties = source_node
		if endpoint.properties:
			raise ValueError(
				"CypherGlot currently validates traversal-backed MATCH write clauses only without inline properties on reused endpoint aliases."
			)
		if endpoint.label is not None and endpoint.label != source_label:
			raise ValueError(
				"CypherGlot currently validates traversal-backed MATCH write clauses only when reused endpoint labels match the matched source aliases."
			)

	if allow_one_new_endpoint and (reused_count == 0 or new_count > 1):
		raise ValueError(
			"CypherGlot currently validates traversal-backed MATCH ... CREATE only with exactly one reused matched node alias plus at most one fresh endpoint node."
		)


def _validate_query_nodes_vector_shape(
	result: CypherParseResult,
	single_part_query_ctx,
) -> None:
	updating_clauses = single_part_query_ctx.oC_UpdatingClause()
	reading_clauses = single_part_query_ctx.oC_ReadingClause()
	return_ctx = single_part_query_ctx.oC_Return()

	if updating_clauses or return_ctx is None:
		raise ValueError(
			"CypherGlot currently admits vector-aware CALL queries only in the "
			"narrow read subset."
		)

	if len(reading_clauses) not in {1, 2}:
		raise ValueError(
			"CypherGlot currently admits only CALL db.index.vector.queryNodes(...) "
			"YIELD node, score [MATCH ...] RETURN ... in the vector-aware subset."
		)

	call_ctx = reading_clauses[0].oC_InQueryCall()
	if call_ctx is None:
		raise ValueError(
			"CypherGlot currently admits only the db.index.vector.queryNodes(...) "
			"procedure in the vector-aware subset."
		)

	procedure_ctx = call_ctx.oC_ExplicitProcedureInvocation()
	if procedure_ctx is None:
		raise ValueError(
			"CypherGlot currently admits only explicit db.index.vector.queryNodes(...) "
			"procedure calls in the vector-aware subset."
		)

	procedure_name = _context_text(result, procedure_ctx.oC_ProcedureName()).casefold()
	if procedure_name != "db.index.vector.querynodes":
		raise ValueError(
			"CypherGlot currently admits only the db.index.vector.queryNodes(...) "
			"procedure in the vector-aware subset."
		)

	procedure_args = procedure_ctx.oC_Expression()
	if len(procedure_args) != 3:
		raise ValueError(
			"CypherGlot currently admits only db.index.vector.queryNodes(index, k, "
			"$query) in the vector-aware subset."
		)

	index_text = _context_text(result, procedure_args[0]).strip()
	if re.fullmatch(r"'([^'\\]|\\.)*'", index_text) is None:
		raise ValueError(
			"CypherGlot currently requires vector procedure index names to be "
			"single-quoted string literals."
		)

	_parse_query_nodes_limit_ref(_context_text(result, procedure_args[1]).strip())

	query_text = _context_text(result, procedure_args[2]).strip()
	if re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*", query_text) is None:
		raise ValueError(
			"CypherGlot currently requires vector procedure query embeddings to "
			"come from a named parameter."
		)

	yield_items = call_ctx.oC_YieldItems()
	if yield_items is None:
		raise ValueError(
			"CypherGlot currently requires vector procedure queries to include "
			"YIELD node, score."
		)

	yielded = tuple(
		_context_text(result, item_ctx).strip() for item_ctx in yield_items.oC_YieldItem()
	)
	if yielded != ("node", "score"):
		raise ValueError(
			"CypherGlot currently requires vector procedure queries to yield "
			"node, score in that order."
		)

	yield_where = yield_items.oC_Where()
	if yield_where is not None and len(reading_clauses) == 2:
		raise ValueError(
			"CypherGlot currently admits either YIELD ... WHERE ... or YIELD ... "
			"MATCH ..., but not both in the same vector-aware query."
		)

	if len(reading_clauses) == 2:
		match_ctx = reading_clauses[1].oC_Match()
		if match_ctx is None:
			raise ValueError(
				"CypherGlot currently admits only an optional MATCH clause after "
				"the vector procedure call."
			)
		_validate_match_pattern_shape(
			result,
			match_ctx,
			allow_two_node_disconnected=False,
		)

	projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
	return_text, order_text = _split_query_nodes_return_and_order(projection_text)
	_parse_query_nodes_return_items(return_text)
	_parse_query_nodes_order_items(order_text)


def _validate_unwind_shape(result: CypherParseResult, single_part_query_ctx) -> None:
	reading_clauses = single_part_query_ctx.oC_ReadingClause()
	updating_clauses = single_part_query_ctx.oC_UpdatingClause()
	return_ctx = single_part_query_ctx.oC_Return()

	if len(reading_clauses) != 1 or updating_clauses or return_ctx is None:
		raise ValueError(
			"CypherGlot currently admits only standalone UNWIND ... RETURN statements in the narrow UNWIND subset."
		)

	unwind_ctx = reading_clauses[0].oC_Unwind()
	if unwind_ctx is None:
		raise ValueError(
			"CypherGlot currently admits only standalone UNWIND ... RETURN statements in the narrow UNWIND subset."
		)

	source_text = _context_text(result, unwind_ctx.oC_Expression()).strip()
	if not (
		(source_text.startswith("[") and source_text.endswith("]"))
	):
		raise ValueError(
			"CypherGlot currently requires UNWIND sources to be list literals."
		)

	alias = _context_text(result, unwind_ctx.oC_Variable()).strip()
	projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
	return_text, order_items, _limit, distinct, _skip = _split_return_clause(projection_text)
	if distinct:
		raise ValueError(
			"CypherGlot currently does not admit RETURN DISTINCT in the narrow UNWIND subset."
		)

	return_items = [item.strip() for item in _split_comma_separated(return_text)]
	if not return_items:
		raise ValueError(
			"CypherGlot currently requires at least one RETURN item in the narrow UNWIND subset."
		)

	projected_aliases: set[str] = set()
	for item_text in return_items:
		alias_match = re.fullmatch(
			r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
			item_text,
			flags=re.IGNORECASE,
		)
		output_alias = alias_match.group("output") if alias_match is not None else None
		expression_text = alias_match.group("expr").strip() if alias_match is not None else item_text
		if expression_text != alias:
			raise ValueError(
				"CypherGlot currently supports only RETURN unwind_alias or RETURN unwind_alias AS output_alias in the narrow UNWIND subset."
			)
		output_name = output_alias or alias
		if output_name in projected_aliases:
			raise ValueError(
				f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the narrow UNWIND subset."
			)
		projected_aliases.add(output_name)

	for item in order_items:
		if item.field == "__value__" and item.alias in projected_aliases:
			continue
		raise ValueError(
			"CypherGlot currently supports ORDER BY unwind_alias or ORDER BY projected_alias in the narrow UNWIND subset."
		)


def _validate_optional_match_shape(
	result: CypherParseResult,
	single_part_query_ctx,
	*,
	validate_plain_read_projection_shape,
) -> None:
	updating_clauses = single_part_query_ctx.oC_UpdatingClause()
	reading_clauses = single_part_query_ctx.oC_ReadingClause()
	return_ctx = single_part_query_ctx.oC_Return()

	if len(reading_clauses) != 1 or updating_clauses or return_ctx is None:
		raise ValueError(
			"CypherGlot currently admits only standalone OPTIONAL MATCH ... RETURN statements in the narrow OPTIONAL MATCH subset."
		)

	match_ctx = reading_clauses[0].oC_Match()
	if match_ctx is None or match_ctx.OPTIONAL() is None:
		raise ValueError(
			"CypherGlot currently admits only standalone OPTIONAL MATCH ... RETURN statements in the narrow OPTIONAL MATCH subset."
		)

	_validate_match_pattern_shape(
		result,
		match_ctx,
		allow_two_node_disconnected=False,
		allow_optional=True,
	)

	pattern_parts = match_ctx.oC_Pattern().oC_PatternPart()
	if len(pattern_parts) != 1:
		raise ValueError(
			"CypherGlot currently admits only one node pattern in the narrow OPTIONAL MATCH subset."
		)
	pattern_element = pattern_parts[0].oC_AnonymousPatternPart().oC_PatternElement()
	if pattern_element.oC_PatternElementChain():
		raise ValueError(
			"CypherGlot currently admits only single-node OPTIONAL MATCH patterns in the narrow OPTIONAL MATCH subset."
		)

	projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
	validate_plain_read_projection_shape(
		projection_text,
		allowed_aliases={
			alias
			for alias in re.findall(
				r"\(([A-Za-z_][A-Za-z0-9_]*)",
				_context_text(result, match_ctx.oC_Pattern()),
			)
		},
		allowed_relationship_aliases=set(),
	)