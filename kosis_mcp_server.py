"""
KOSIS MCP — 조회 + 분석 + 시각화 통합 서버
==========================================

3계층 설계:
  L1 Quick    — 사용자 질의 90% 처리 (큐레이션 + 폴백)
  L2 Analysis — 회귀/상관/분포/예측/이상치 (scipy)
  L3 Viz      — 실제 SVG 차트 (MCP image content 반환)
  Chain       — L1+L2+L3 종합 워크플로우

설치:
    pip install "mcp[cli]" httpx scipy numpy

환경변수:
    KOSIS_API_KEY=발급받은_인증키
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional

import httpx
import numpy as np
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent
from scipy import stats as scipy_stats

from kosis_analysis.answering import AnswerPlan, AnswerPlanner, AnswerStat
from kosis_analysis.charts import (
    _chart_bar_svg,
    _chart_line_svg,
    _chart_scatter_svg,
    _svg_to_image,
)
from kosis_analysis.client import (
    ERROR_MAP,
    _fetch_classifications,
    _fetch_meta,
    _fetch_period_range,
    _fetch_table_name,
    _kosis_call,
    _resolve_key,
)
from kosis_analysis.metadata import (
    MetadataCompatibilityScorer,
    TableMetadataProfile,
    _aggregate_rows_sum_by_group,
    _axis_matches_dimension,
    _build_axis_codebook,
    _compact_text,
    _concept_match_score,
    _fanout_coverage_report,
    _fanout_filter_sets,
    _infer_required_dimensions_from_query,
    _normalize_query_table_rows,
    _normalize_required_dimensions,
    _query_table_params,
    _suggest_axis_codes,
    _validate_query_table_filters,
)
from kosis_analysis.periods import (
    _api_period_de,
    _api_period_type,
    _current_quarter,
    _detect_half_year_request,
    _detect_precision_downgrade,
    _extract_open_start_year,
    _extract_year_range,
    _format_period_label,
    _is_latest_period_text,
    _is_yearly_period_type,
    _latest_count_for_years,
    _normalize_period_bound,
    _parse_month_token,
    _parse_quarter_token,
    _parse_year_token,
    _period_bounds,
    _period_range_looks_yearly,
    _period_type,
    _periods_per_year,
    _pick_finest_period,
    _pick_query_table_period_row,
    _query_table_data_nature,
    _relative_year,
    _validate_query_period_range,
)
from kosis_analysis.indicators import (
    OPERATIONS as INDICATOR_OPERATIONS,
    OPERATION_ALIASES_KO,
    operation_catalog as _indicator_operation_catalog,
)
from kosis_analysis.planner import QueryWorkflowPlanner
from kosis_analysis.quick import (
    _attach_ignored_params,
    _extract_single_region_from_query,
    _extract_single_year_from_query,
    _quick_stat_unsupported_dimensions,
    _unsupported_quick_stat_response,
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ============================================================================
# 상수
# ============================================================================

STATUS_EXECUTED = "EXECUTED"
STATUS_NEEDS_TABLE_SELECTION = "NEEDS_TABLE_SELECTION"
STATUS_STAT_NOT_FOUND = "STAT_NOT_FOUND"
STATUS_PERIOD_NOT_FOUND = "PERIOD_NOT_FOUND"
STATUS_PERIOD_RANGE_REQUESTED = "PERIOD_RANGE_REQUESTED"
STATUS_UNVERIFIED_FORMULA = "UNVERIFIED_FORMULA"
STATUS_INVALID_FILTER_CODE = "INVALID_FILTER_CODE"
STATUS_INVALID_PERIOD_RANGE = "INVALID_PERIOD_RANGE"
STATUS_DENOMINATOR_REQUIRED = "DENOMINATOR_REQUIRED"
STATUS_RUNTIME_ERROR = "RUNTIME_ERROR"
STATUS_MISSING_API_KEY = "MISSING_KEY"
STATUS_FANOUT_LIMIT_EXCEEDED = "FANOUT_LIMIT_EXCEEDED"


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.1) -> float:
    try:
        return max(minimum, float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


QUERY_TABLE_MAX_FANOUT = _env_int("KOSIS_MCP_QUERY_TABLE_MAX_FANOUT", 80)
QUERY_TABLE_CONCURRENCY = _env_int("KOSIS_MCP_QUERY_TABLE_CONCURRENCY", 8)
QUERY_TABLE_CALL_TIMEOUT = _env_float("KOSIS_MCP_QUERY_TABLE_CALL_TIMEOUT", 15.0)
NABO_API_BASE = "https://www.nabostats.go.kr/openapi"
NABO_MAX_PAGE_SIZE = 1000
NABO_DEFAULT_MAX_ROWS = 5000
NABO_DTACYCLE_ALIASES = {
    "YY": "YY",
    "Y": "YY",
    "YEAR": "YY",
    "YEARLY": "YY",
    "연": "YY",
    "년": "YY",
    "연간": "YY",
    "QY": "QY",
    "Q": "QY",
    "QUARTER": "QY",
    "QUARTERLY": "QY",
    "분기": "QY",
    "MM": "MM",
    "M": "MM",
    "MONTH": "MM",
    "MONTHLY": "MM",
    "월": "MM",
    "월간": "MM",
}
NABO_DTACYCLE_FROM_NAME = {
    "년": "YY",
    "연": "YY",
    "분기": "QY",
    "월": "MM",
}
NABO_DTACYCLE_GUIDANCE = {
    "YY": {
        "label": "annual",
        "accepted_period_examples": ["2024", ["2010", "2024"], "2010:2024", "2010-2024"],
    },
    "QY": {
        "label": "quarterly",
        "accepted_period_examples": ["2024Q1", ["2023Q1", "2024Q4"], "2023Q1:2024Q4"],
    },
    "MM": {
        "label": "monthly",
        "accepted_period_examples": ["202401", "2024-01", ["202401", "202412"], "202401:202412"],
    },
}
NABO_ITEM_META_CACHE: dict[str, dict[str, dict[str, Any]]] = {}
NABO_TABLE_CATALOG_CACHE: dict[str, list[dict[str, Any]]] = {}

ANSWER_QUERY_CONVENIENCE_NOTE = {
    "tool": "answer_query",
    "tool_mode": "convenience",
    "reason": (
        "Natural-language convenience entry point. Use the stepwise tools when the caller "
        "needs to inspect table selection, concept resolution, raw rows, or calculations."
    ),
}


def _attach_gemma_deprecation_warning(payload: dict[str, Any]) -> dict[str, Any]:
    """Compatibility wrapper: annotate answer_query as a convenience tool, not deprecated."""
    result = dict(payload)
    result.setdefault("tool_mode", "convenience")
    result.setdefault("convenience_note", ANSWER_QUERY_CONVENIENCE_NOTE)
    existing_contract = result.get("mcp_output_contract") if isinstance(result.get("mcp_output_contract"), dict) else {}
    existing_signals = existing_contract.get("current_signals") if isinstance(existing_contract, dict) else {}
    if not isinstance(existing_signals, dict):
        existing_signals = {}
    markers = list(existing_signals.get("markers_present") or [])
    markers.append("convenience_response")
    dropped = result.get("dropped_dimensions") or result.get("누락_차원") or []
    fulfillment_status = result.get("status") or result.get("이행_상태")
    gap_reason = result.get("부분충족_사유") or result.get("fulfillment_gap_reason")
    if fulfillment_status == "partial" or dropped:
        markers.append("partial_fulfillment")
    if dropped:
        markers.append("dropped_dimensions")
    if result.get("status") == "failed":
        markers.append("runtime_error")
    result["mcp_output_contract"] = _mcp_tool_output_contract(
        role="convenience_tool",
        final_answer_expected=True,
        markers=markers,
        explanation="answer_query is a natural-language convenience response; inspect status and caveats before using it.",
        extra_signals={
            "fulfillment_status": fulfillment_status,
            "dropped_dimensions": dropped,
            "fulfillment_gap_reason": gap_reason,
            "tool_mode": "convenience",
        },
    )
    return result


def _attach_shortcut_contract(payload: Any, *, tool: str, ignored_params: Optional[list[str]] = None) -> Any:
    if not isinstance(payload, dict):
        return payload
    result = dict(payload)
    existing_contract = result.get("mcp_output_contract") if isinstance(result.get("mcp_output_contract"), dict) else {}
    existing_signals = existing_contract.get("current_signals") if isinstance(existing_contract, dict) else {}
    markers = list(existing_signals.get("markers_present") or []) if isinstance(existing_signals, dict) else []
    markers.append("shortcut_response")
    if ignored_params:
        markers.append("dropped_dimensions")
        markers.append("partial_fulfillment")
    if result.get("오류") or result.get("status") in {"failed", "unsupported"}:
        markers.append("unsupported")
    result["mcp_output_contract"] = _mcp_tool_output_contract(
        role="shortcut_tool",
        final_answer_expected=True,
        markers=markers,
        explanation=f"{tool} is a narrow shortcut tool; check dropped_dimensions before treating the result as complete.",
        extra_signals={
            "tool": tool,
            "ignored_params": ignored_params or [],
        },
    )
    return result


def _compact_answer_query_response(payload: dict[str, Any], *, query: str, region: str) -> dict[str, Any]:
    """Return a slim answer_query shape for chatbot clients that do their own reasoning."""
    metadata_keys = {
        "query": query,
        "region": payload.get("region") or payload.get("지역") or region,
        "answer_type": payload.get("answer_type") or payload.get("답변유형"),
        "stat_name": payload.get("stat_name") or payload.get("통계명"),
        "unit": payload.get("unit") or payload.get("단위"),
        "period": payload.get("period") or payload.get("시점") or payload.get("used_period"),
        "source": payload.get("source") or payload.get("출처"),
        "table_id": payload.get("table_id") or payload.get("tbl_id") or payload.get("통계표ID"),
        "period_age_years": payload.get("period_age_years"),
    }
    metadata = {k: v for k, v in metadata_keys.items() if v not in (None, "", [], {})}

    notes: list[Any] = []
    for key in (
        "notes",
        "warnings",
        "validation_warnings",
        "검증_주의",
        "data_quality_note",
        "fulfillment_gap_reason",
        "부분충족_사유",
    ):
        value = payload.get(key)
        if not value:
            continue
        if isinstance(value, list):
            notes.extend(value)
        else:
            notes.append(value)

    data = None
    for key in ("data", "rows", "results", "result", "표", "시계열", "결과", "예측", "이상치"):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            data = value
            break

    contract = payload.get("mcp_output_contract") if isinstance(payload.get("mcp_output_contract"), dict) else {}
    signals = contract.get("current_signals") if isinstance(contract, dict) else {}
    markers = signals.get("markers_present") if isinstance(signals, dict) else []
    diagnostics = {
        "tool_mode": payload.get("tool_mode", "convenience"),
        "markers_present": markers or [],
        "fulfillment_status": payload.get("fulfillment_status") or payload.get("status") or payload.get("상태"),
        "dropped_dimensions": payload.get("dropped_dimensions") or payload.get("누락_차원") or [],
    }
    diagnostics = {k: v for k, v in diagnostics.items() if v not in (None, "", [], {})}

    compact = {
        "status": payload.get("status") or payload.get("상태") or "unknown",
        "code": payload.get("code") or payload.get("코드"),
        "answer": payload.get("answer") or payload.get("답변"),
        "error": payload.get("error") or payload.get("오류"),
        "data": data,
        "metadata": metadata,
        "notes": notes,
        "diagnostics": diagnostics,
    }
    return {k: v for k, v in compact.items() if v not in (None, "", [], {})}


def _mcp_tool_output_contract(
    *,
    role: str,
    final_answer_expected: bool,
    markers: Optional[list[str]] = None,
    explanation: Optional[str] = None,
    extra_signals: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Machine-readable contract shared by non-planner MCP tool outputs."""
    clean_markers = list(dict.fromkeys(markers or []))
    marker_guidance_catalog = {
        "unsupported": "Do not synthesize an answer; correct the request or choose another tool.",
        "invalid_input": "Fix the caller-provided arguments before retrying.",
        "missing_api_key": "Configure KOSIS_API_KEY or pass api_key; do not fabricate data.",
        "missing_denominator": "Provide denominator_rows from verified raw data before calculating ratios.",
        "not_matched": "Treat the result as no verified match; search or ask for clarification.",
        "search_empty": "Report that no search candidates were found for this source; do not invent a table.",
        "concept_unresolved": "Use matches_by_concept/ambiguities or ask for a more specific concept.",
        "missing_metrics": "Ask for or infer a statistical metric before selecting a table.",
        "validation_errors": "Inspect validation_errors and retry with corrected structured arguments.",
        "partial_fanout_coverage": "Disclose missing filter sets and avoid treating returned rows as complete coverage.",
        "truncated_by_max_rows": "Returned rows were cut by max_rows; do not treat latest_period_in_returned_rows as the dataset latest period.",
        "complete_fanout_miss": "Report that verified calls returned no rows; do not backfill from model knowledge.",
        "max_fanout_exceeded": "Reduce filters or split the query into smaller calls.",
        "fanout_call_failed": "Inspect fanout.call_details errors/timeouts before using any partial rows.",
        "period_not_found": "Use available_period_range or suggested_period_range before retrying.",
        "period_type_mismatch": "Convert the requested period to the table cadence shown in period_format_examples.",
        "dtacycle_mismatch": "Use the table's dtacycle_cd_suggestions or dtacycle_cd='auto' before querying NABO rows.",
        "invalid_period_format": "Normalize natural-language time to KOSIS period codes before retrying query_table.",
        "period_request_normalization_error": "Inspect period_request.errors and retry with latest, a single period, or a supported range form.",
        "duplicate_denominator_key": "Resolve duplicate denominator rows; do not choose one silently.",
        "non_numeric_aggregation_input": "Disclose dropped rows and avoid presenting aggregation as complete.",
        "aggregation_dropped_rows": "Inspect aggregation_report before using summed values.",
        "period_metadata_missing": "Treat period validation as unavailable and disclose the limitation.",
        "projection_data": "Label values as projection/forecast when answering.",
        "missing_values": "Some returned rows have no numeric value; inspect missing_value_examples before quoting values.",
        "all_values_missing": "Rows were matched but every returned value is empty; report unavailable data instead of a numeric answer.",
        "unit_caller_resolution_required": "Resolve the computed unit from unit_transformation before quoting.",
        "unit_mismatch": "Do not add or share values with different units until the caller explicitly converts them.",
        "unit_conversion_required": "Convert units outside the MCP or re-query comparable rows before using this result.",
        "negative_base_growth_rate": "Percent change used a negative base; prefer absolute change/yoy_diff for balances, losses, or deficits.",
        "unsupported_source_system": "This tool cannot route the requested source; use the provider-specific raw query tool or pass input_rows to a source-agnostic tool.",
        "share_total_from_input_rows": "Disclose that the denominator was derived from input_rows, not an external total.",
        "share_total_grouped_by_period": "Interpret share results within each period group, not across all periods.",
        "convenience_response": "Use compact metadata/notes first; request verbose=True only for diagnostics.",
        "shortcut_response": "Check ignored_params/dropped_dimensions before treating shortcut output as complete.",
        "deprecation": "Prefer the recommended replacement workflow/tool.",
        "formula_advisory_only": "Treat formula guidance as advisory; verify numerator/denominator from KOSIS metadata/raw rows.",
        "heuristic_extraction": "Treat extracted concepts as candidates only; verify with KOSIS metadata before use.",
        "not_matched_table": "The provided table identifier was not verified in provider metadata; search again before retrying.",
        "period_filter_empty": "Rows existed before period filtering, but none matched the requested period range.",
        "filter_no_match": "Rows existed before caller filters, but none matched the structured filters.",
        "partial_filter_match": "Some requested filter values did not match returned metadata; inspect filter_coverage before answering.",
        "item_metadata_joined": "Use ITEM.full_label when label is ambiguous or repeated.",
        "metadata_partial": "Some metadata could not be joined; avoid over-interpreting labels without full paths.",
        "source_preference_nabo": "Use NABO tools for follow-up unless the caller explicitly changes source.",
        "nabo_routing": "NABO source preference changed the workflow away from KOSIS table selection.",
        "nabo_metadata_candidate": "NABO metadata supplied candidate metrics; verify table/items before values.",
        "nabo_metadata_partial": "NABO metadata lookup was partial; treat candidates as unverified.",
        "catalog_fallback_search": "NABO API table-name search was empty; candidates came from local catalog substring/metadata scoring.",
    }
    marker_guidance = {
        marker: marker_guidance_catalog[marker]
        for marker in clean_markers
        if marker in marker_guidance_catalog
    }
    has_failures = any(marker in {
        "unsupported",
        "invalid_input",
        "missing_api_key",
        "missing_denominator",
        "not_matched",
        "not_matched_table",
        "search_empty",
        "concept_unresolved",
        "missing_metrics",
        "validation_errors",
        "partial_fanout_coverage",
        "truncated_by_max_rows",
        "complete_fanout_miss",
        "period_filter_empty",
        "filter_no_match",
        "partial_filter_match",
        "max_fanout_exceeded",
        "fanout_call_failed",
        "period_not_found",
        "period_type_mismatch",
        "dtacycle_mismatch",
        "invalid_period_format",
        "period_request_normalization_error",
        "duplicate_denominator_key",
        "non_numeric_aggregation_input",
        "runtime_error",
        "metadata_failed",
        "empty_rows",
        "partial_computation",
        "partial_fulfillment",
        "all_values_missing",
        "unit_mismatch",
        "unit_conversion_required",
        "unsupported_source_system",
    } for marker in clean_markers)
    current_signals = {
        "has_failures": has_failures,
        "has_caveats": bool(clean_markers),
        "markers_present": clean_markers,
        "explanation": explanation or (
            "Tool output contains caveats; inspect markers before synthesizing."
            if clean_markers else "No immediate tool-output caveats detected."
        ),
    }
    if extra_signals:
        current_signals.update(extra_signals)
    if marker_guidance:
        current_signals["marker_guidance"] = marker_guidance
    return {
        "role": role,
        "final_answer_expected": final_answer_expected,
        "machine_readable_status": True,
        "current_signals": current_signals,
        "llm_rules": [
            "Do not hide unsupported, not_matched, empty_rows, validation_errors, or partial_fanout_coverage markers.",
            "Follow current_signals.marker_guidance for every marker before synthesizing a user-facing answer.",
            "If coverage_ratio is below 1.0, disclose partial coverage before using the returned rows.",
            "Do not fill missing rows from model knowledge.",
            "If unit_caller_should_label is null, resolve the unit before quoting; do not reuse unit_raw as the computed result unit.",
        ],
        "failure_markers": [
            "unsupported",
            "invalid_input",
            "missing_api_key",
            "missing_denominator",
            "not_matched",
            "not_matched_table",
            "search_empty",
            "validation_errors",
            "partial_fanout_coverage",
            "truncated_by_max_rows",
            "complete_fanout_miss",
            "period_filter_empty",
            "filter_no_match",
            "partial_filter_match",
            "max_fanout_exceeded",
            "fanout_call_failed",
            "period_not_found",
            "period_type_mismatch",
            "dtacycle_mismatch",
            "invalid_period_format",
            "duplicate_denominator_key",
            "non_numeric_aggregation_input",
            "empty_rows",
            "all_values_missing",
            "partial_fulfillment",
            "coverage_ratio",
        ],
    }


def _missing_api_key_response(tool: str, **context: Any) -> dict[str, Any]:
    """Return a structured MCP payload instead of surfacing a ToolError."""
    clean_context = {k: v for k, v in context.items() if v not in (None, "")}
    return {
        "상태": "failed",
        "status": "failed",
        "코드": STATUS_MISSING_API_KEY,
        "code": STATUS_MISSING_API_KEY,
        "tool": tool,
        "error": "KOSIS_API_KEY is required.",
        "오류": "KOSIS_API_KEY 설정 필요",
        "권고": "서버 환경변수 KOSIS_API_KEY를 설정하거나 도구 호출에 api_key를 전달하세요.",
        **clean_context,
        "mcp_output_contract": _mcp_tool_output_contract(
            role="configuration_error",
            final_answer_expected=False,
            markers=["missing_api_key"],
            explanation="The tool cannot call KOSIS without an API key.",
            extra_signals={"tool": tool},
        ),
    }


def _resolve_nabo_key(api_key: Optional[str] = None) -> str:
    key = api_key or os.environ.get("NABO_API_KEY")
    if not key:
        raise RuntimeError("NABO_API_KEY is required.")
    return key


def _missing_nabo_key_response(tool: str, **context: Any) -> dict[str, Any]:
    clean_context = {k: v for k, v in context.items() if v not in (None, "")}
    return {
        "상태": "failed",
        "status": "failed",
        "코드": STATUS_MISSING_API_KEY,
        "code": STATUS_MISSING_API_KEY,
        "tool": tool,
        "source_system": "NABO",
        "error": "NABO_API_KEY is required.",
        "오류": "NABO_API_KEY 설정 필요",
        "권고": "서버 환경변수 NABO_API_KEY를 설정하거나 도구 호출에 api_key를 전달하세요.",
        **clean_context,
        "mcp_output_contract": _mcp_tool_output_contract(
            role="configuration_error",
            final_answer_expected=False,
            markers=["missing_api_key"],
            explanation="The tool cannot call NABO OpenAPI without an API key.",
            extra_signals={"tool": tool, "source_system": "NABO"},
        ),
    }


def _is_missing_nabo_key_error(exc: Exception) -> bool:
    return "NABO_API_KEY" in str(exc)


def _is_missing_key_error(exc: Exception) -> bool:
    return "KOSIS_API_KEY" in str(exc)


def _period_format_examples(period_type: Optional[str]) -> dict[str, Any]:
    api_type = _api_period_type(period_type)
    examples: dict[str, Any] = {
        "table_cadence": period_type,
        "api_period_type": api_type,
    }
    if api_type == "M":
        examples.update({
            "accepted_period_codes": ["202401", "202402"],
            "year_range_example": {"input": "2024", "period_range": ["202401", "202412"]},
            "quarter_range_example": {"input": "2024-Q1", "period_range": ["202401", "202403"]},
        })
    elif api_type == "Q":
        examples.update({
            "accepted_period_codes": ["20241", "20242"],
            "display_aliases": ["2024Q1", "2024-Q1"],
        })
    elif api_type == "Y":
        examples.update({
            "accepted_period_codes": ["2024", "2025"],
        })
    elif api_type == "H":
        examples.update({
            "accepted_period_codes": ["20241", "20242"],
            "display_aliases": ["2024H1", "2024H2"],
        })
    else:
        examples.update({"accepted_period_codes": []})
    return examples


def _quarter_month_bounds(value: Any) -> Optional[tuple[str, str]]:
    text = re.sub(r"\s+", "", str(value or "")).upper()
    match = re.search(r"(19\d{2}|20\d{2})[-_/]?Q([1-4])", text)
    if match:
        year, quarter_text = match.group(1), match.group(2)
    else:
        quarter = _parse_quarter_token(str(value or ""))
        if not quarter:
            return None
        year, quarter_text = quarter[:4], quarter[4:]
    quarter_no = int(quarter_text)
    start_month = (quarter_no - 1) * 3 + 1
    return f"{year}{start_month:02d}", f"{year}{start_month + 2:02d}"


def _suggest_period_range_for_cadence(
    period_range: Optional[list[str]],
    period_type: Optional[str],
) -> Optional[list[str]]:
    if not period_range:
        return None
    api_type = _api_period_type(period_type)
    bounds = [str(p).strip() for p in period_range if str(p or "").strip()]
    if not bounds:
        return None
    if len(bounds) == 1:
        bounds = [bounds[0], bounds[0]]
    if api_type == "M":
        start_quarter = _quarter_month_bounds(bounds[0])
        end_quarter = _quarter_month_bounds(bounds[1])
        if start_quarter or end_quarter:
            start = (start_quarter or end_quarter)[0]
            end = (end_quarter or start_quarter)[1]
            return [start, end]
        start_year = re.fullmatch(r"(19\d{2}|20\d{2})", bounds[0])
        end_year = re.fullmatch(r"(19\d{2}|20\d{2})", bounds[1])
        if start_year and end_year:
            return [f"{start_year.group(1)}01", f"{end_year.group(1)}12"]
    if api_type == "Q":
        start_quarter = _parse_quarter_token(bounds[0])
        end_quarter = _parse_quarter_token(bounds[1])
        if start_quarter or end_quarter:
            return [start_quarter or end_quarter, end_quarter or start_quarter]
    return None


def _period_error_markers(period_error: dict[str, Any], selected_period: Optional[dict[str, Any]]) -> list[str]:
    markers = ["invalid_input"]
    if period_error.get("code") == STATUS_PERIOD_NOT_FOUND:
        markers.append("period_not_found")
    if period_error.get("code") == STATUS_INVALID_PERIOD_RANGE:
        markers.append("validation_errors")
    requested = " ".join(str(v) for v in period_error.get("period_range_received") or [])
    api_type = _api_period_type(_period_type(selected_period))
    if api_type == "M" and _quarter_month_bounds(requested):
        markers.append("period_type_mismatch")
    return markers


def _period_error_guidance(
    period_error: dict[str, Any],
    selected_period: Optional[dict[str, Any]],
    effective_period_range: Optional[list[str]],
) -> dict[str, Any]:
    examples = _period_format_examples(_period_type(selected_period))
    suggested = period_error.get("suggested_period_range") or _suggest_period_range_for_cadence(
        effective_period_range,
        _period_type(selected_period),
    )
    return {
        "suggested_period_range": suggested,
        "period_format_examples": examples,
        "period_type_check": {
            "table_supports": [_api_period_type(_period_type(selected_period))] if selected_period else [],
            "requested_period_range": effective_period_range,
            "compatible": False,
            "auto_aggregation_not_supported": True,
        },
    }


def _period_range_format_error(
    period_range: Optional[list[str]],
    selected_period: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not period_range:
        return None
    bounds = [str(p).strip() for p in period_range if str(p or "").strip()]
    if not bounds:
        return None
    if len(bounds) == 1:
        bounds = [bounds[0], bounds[0]]
    invalid_bounds = [
        bound for bound in bounds[:2]
        if not _is_latest_period_text(bound) and _normalize_period_bound(bound) is None
    ]
    if not invalid_bounds:
        return None
    guidance = _period_error_guidance(
        {
            "code": "INVALID_PERIOD_FORMAT",
            "period_range_received": bounds[:2],
        },
        selected_period,
        period_range,
    )
    latest = selected_period.get("END_PRD_DE") if selected_period else None
    if latest and all(_is_latest_period_text(bound) for bound in bounds[:2]):
        guidance["suggested_period_range"] = [str(latest), str(latest)]
    return {
        "code": "INVALID_PERIOD_FORMAT",
        "error": "period_range contains values that are not KOSIS period codes.",
        "오류": "period_range에 KOSIS 시점 코드로 해석할 수 없는 값이 포함되어 있습니다.",
        "period_range_received": bounds[:2],
        **guidance,
    }

FORMULA_DEPENDENCIES: dict[str, dict[str, Any]] = {
    "share_ratio": {
        "canonical": "비중/구성비",
        "aliases": ["비중", "비율", "구성비", "차지하는 비중"],
        "formula": "부분 / 전체 * 100",
        "required_stats": ["분자 지표", "분모 지표"],
        "checks": ["분모 정의", "동일 기준시점", "동일 모집단", "단위 정합성"],
        "caution": "서울 소상공인 비중처럼 분모가 전국인지 서울 전체 사업체인지 반드시 확인해야 합니다.",
    },
    "growth_rate": {
        "canonical": "증가율/변화율",
        "aliases": ["증가율", "감소율", "변화율", "전년 대비", "전월 대비"],
        "formula": "(현재값 - 비교시점값) / 비교시점값 * 100",
        "required_stats": ["현재 시점 값", "비교 시점 값"],
        "checks": ["비교 시점", "0 또는 음수 기준값", "연/월/분기 주기"],
        "caution": "증감은 절대 차이이고 증가율은 기준값 대비 비율입니다.",
    },
    "average_workers_per_business": {
        "canonical": "사업체당 평균 종사자 수",
        "aliases": ["사업체당 평균 종사자", "기업당 평균 고용", "평균 고용 인원"],
        "formula": "종사자 수 / 사업체 수",
        "required_stats": ["종사자 수", "사업체 수"],
        "checks": ["기업체 기준과 사업체 기준 혼합 여부", "동일 기준시점", "동일 대상 범위"],
        "caution": "사업체 수와 기업 수는 집계 단위가 다르므로 평균 산식에 섞으면 안 됩니다.",
    },
    "unemployment_rate": {
        "canonical": "실업률",
        "aliases": ["실업률", "청년 실업률", "고령층 실업률", "여성 실업률", "남성 실업률"],
        "formula": "실업자 수 / 경제활동인구 * 100",
        "required_stats": ["실업자 수", "경제활동인구"],
        "checks": ["대상군(연령·성별) 일치", "경제활동인구 분모", "동일 기준시점"],
        "caution": "청년·여성 등 부분군 실업률은 분자와 분모를 같은 대상군으로 제한해야 합니다.",
    },
    "closure_rate": {
        "canonical": "폐업률",
        "aliases": ["폐업률", "폐업 비율", "망한 가게 비율"],
        "formula": "폐업 수 / 기준 사업체 수 * 100",
        "required_stats": ["폐업 수", "기준 사업체 수"],
        "checks": ["작성기관 산식", "분모 기준", "기간 기준"],
        "caution": "실제 폐업률 산식은 작성기관 기준을 우선해야 합니다.",
    },
    "startup_rate": {
        "canonical": "창업률",
        "aliases": ["창업률", "창업 비율", "개업률"],
        "formula": "창업 수 / 기준 사업체 수 * 100",
        "required_stats": ["창업 수", "기준 사업체 수"],
        "checks": ["분모 기준", "신설/창업 정의", "기간 기준"],
        "caution": "신설 법인, 창업기업, 사업자등록 신규 등 출처별 정의가 다릅니다.",
    },
    "survival_rate": {
        "canonical": "생존율",
        "aliases": ["생존율", "살아남은 비율", "3년 생존율", "5년 생존율"],
        "formula": "생존 기업 수 / 창업 기업 수 * 100",
        "required_stats": ["창업 코호트", "생존 기업 수"],
        "checks": ["코호트 기준", "1년/3년/5년 기간", "폐업 정의"],
        "caution": "생존율은 특정 창업연도 코호트 기준인지 확인해야 합니다.",
    },
    "loan_to_sales": {
        "canonical": "매출 대비 대출 비중",
        "aliases": ["매출 대비 대출", "대출 부담", "금융 부담"],
        "formula": "대출 잔액 / 매출액 * 100",
        "required_stats": ["대출 잔액", "매출액"],
        "checks": ["잔액/신규대출 구분", "명목 금액 단위", "업종·지역 기준 일치"],
        "caution": "대출은 금융권·정책자금·보증 실적 등 출처별 포괄범위가 다릅니다.",
    },
    "net_startup": {
        "canonical": "순창업/순증가",
        "aliases": ["순창업", "순증가", "창업 폐업 차이"],
        "formula": "창업 수 - 폐업 수",
        "required_stats": ["창업 수", "폐업 수"],
        "checks": ["기간 일치", "업종·지역 기준 일치", "창업/폐업 정의"],
        "caution": "순창업은 생존율이나 실제 고용 증가를 직접 의미하지 않습니다.",
    },
}

# ============================================================================
# 큐레이션 데이터 — kosis_curation 모듈에서 로드
# ============================================================================

from kosis_curation import (
    QuickStatParam,
    REGION_COMPOSITES,
    REGION_DEMOGRAPHIC,
    TIER_A_STATS,
    TOPICS,
    canonical_region as _canonical_region,
    lookup as _curation_lookup,
    route_query as _route_query,
    routing_hints as _routing_hints,
    topic_hints as _topic_hints,
    stats_summary as _curation_stats_summary,
)

# Phase 2 추가 차트 4종 (별도 모듈)
from kosis_charts_extra import (
    chart_heatmap_svg,
    chart_distribution_svg,
    chart_dual_axis_svg,
    chart_dashboard_svg,
)




# ============================================================================
# 헬퍼
# ============================================================================

def _lookup_quick(query: str) -> Optional[QuickStatParam]:
    """큐레이션 모듈에 위임 (Tier A 정밀 매핑 + 동의어 + 부분 일치)."""
    return _curation_lookup(query)


def _resolve_classification_term(
    term: str,
    classifications: list[dict],
    obj_id: Optional[str] = None,
) -> Optional[dict]:
    """Find the KOSIS classification entry whose ITM_NM matches `term`.

    Returns the full row (including ITM_ID for use as objL*/itmId in
    quick_stat) or None when no match is found. Pass obj_id to narrow
    to a specific classification axis (e.g. an industry axis vs a region
    axis on the same table). Prefer exact normalized labels after removing
    KOSIS code/range adornments (e.g. "C.제조업(10~34)" -> "제조업")
    before falling back to substring matches."""
    if not term:
        return None
    normalized = re.sub(r"\s+", "", term).lower()

    def normalize_label(value: Any) -> str:
        return re.sub(r"\s+", "", str(value or "")).lower()

    def canonical_label(value: Any) -> str:
        label = normalize_label(value)
        label = re.sub(r"^[a-z]\.", "", label)
        label = re.sub(r"^\d+[.)]?", "", label)
        label = re.sub(r"\([^)]*\)", "", label)
        return label.strip()

    def is_parent(row: dict) -> bool:
        return not str(row.get("UP_ITM_ID") or "").strip()

    def score(row: dict) -> Optional[tuple[int, int, int, str]]:
        itm_id = normalize_label(row.get("ITM_ID"))
        label = normalize_label(row.get("ITM_NM"))
        canonical = canonical_label(row.get("ITM_NM"))
        if not label and not itm_id:
            return None

        parent_bonus = 0 if is_parent(row) else 1
        if itm_id == normalized:
            rank = 0
        elif canonical == normalized:
            rank = 1
        elif label == normalized:
            rank = 2
        elif canonical.startswith(normalized):
            rank = 3
        elif label.startswith(normalized):
            rank = 4
        elif normalized in canonical:
            rank = 5
        elif normalized in label:
            rank = 6
        else:
            return None
        return (rank, parent_bonus, len(canonical or label), label)

    candidates = classifications
    if obj_id:
        candidates = [c for c in classifications if str(c.get("OBJ_ID") or "") == str(obj_id)]
    matches: list[tuple[tuple[int, int, int, str], dict]] = []
    for row in candidates:
        row_score = score(row)
        if row_score is not None:
            matches.append((row_score, row))
    if not matches:
        return None
    matches.sort(key=lambda pair: pair[0])
    return matches[0][1]


def _kosis_view_url(org_id: str, tbl_id: str) -> str:
    return f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}"


def _format_number(v: Any) -> str:
    try:
        n = float(v)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.3f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        return str(v)


def _format_display_number(v: Any, decimals: Optional[int] = None) -> str:
    if decimals is None:
        return _format_number(v)
    try:
        return f"{float(v):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(v)




def _default_period_type(param: QuickStatParam) -> str:
    periods = tuple(getattr(param, "supported_periods", ()) or ("Y",))
    if "Y" in periods:
        return "Y"
    return periods[0]


def _format_aggregated_dt(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:.10f}".rstrip("0").rstrip(".")


async def _fetch_series(
    client: httpx.AsyncClient, key: str, param: QuickStatParam,
    region_code: Optional[str], period_type: str = "Y",
    start_year: Optional[str] = None, end_year: Optional[str] = None,
    latest_n: Optional[int] = None,
) -> list[dict]:
    p = {
        "method": "getList", "apiKey": key,
        "orgId": param.org_id, "tblId": param.tbl_id,
        "objL1": param.obj_l1, "itmId": param.item_id,
        "prdSe": period_type, "format": "json", "jsonVD": "Y",
    }
    if param.obj_l2:
        p["objL2"] = param.obj_l2
    if getattr(param, "obj_l3", None):
        p["objL3"] = param.obj_l3
    if region_code:
        region_obj = getattr(param, "region_obj", "obj_l1")
        if region_obj == "obj_l1":
            p["objL1"] = region_code
        elif region_obj == "obj_l2":
            p["objL2"] = region_code
        elif region_obj == "obj_l3":
            p["objL3"] = region_code
    if start_year and end_year:
        p["startPrdDe"] = start_year
        p["endPrdDe"] = end_year
    elif latest_n:
        p["newEstPrdCnt"] = str(latest_n)

    obj_l2_list = getattr(param, "obj_l2_list", ()) or ()
    if obj_l2_list and getattr(param, "aggregation", None) == "sum":
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        base_rows: dict[str, dict] = {}

        for obj_l2 in obj_l2_list:
            component_params = dict(p)
            component_params["objL2"] = obj_l2
            rows = await _kosis_call(client, "Param/statisticsParameterData.do", component_params)
            for row in rows:
                period = row.get("PRD_DE") or row.get("시점")
                raw_value = row.get("DT") or row.get("값")
                if not period or raw_value in (None, ""):
                    continue
                try:
                    value = float(str(raw_value).replace(",", ""))
                except ValueError:
                    continue
                sums[period] = sums.get(period, 0.0) + value
                counts[period] = counts.get(period, 0) + 1
                base_rows.setdefault(period, dict(row))

        aggregated: list[dict] = []
        for period in sorted(sums):
            if counts.get(period) != len(obj_l2_list):
                continue
            row = base_rows[period]
            row["DT"] = _format_aggregated_dt(sums[period])
            row["UNIT_NM"] = param.unit
            row["ITM_NM"] = param.description
            row["C2"] = ",".join(obj_l2_list)
            row["C2_NM"] = "합산"
            row["_AGGREGATION"] = "sum"
            row["_COMPONENT_OBJ_L2"] = list(obj_l2_list)
            aggregated.append(row)
        return aggregated

    return await _kosis_call(client, "Param/statisticsParameterData.do", p)


def _values_from_series(series: list[dict]) -> tuple[list[str], list[float]]:
    pairs = []
    for r in series:
        try:
            t = r.get("시점") or r.get("PRD_DE")
            v = float(r.get("값") or r.get("DT"))
            if t:
                pairs.append((t, v))
        except (ValueError, TypeError):
            continue
    pairs.sort(key=lambda p: p[0])
    return [p[0] for p in pairs], [p[1] for p in pairs]


def _numeric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text in {"-", ".", "..", "...", "NA", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _row_unit_generic(row: dict[str, Any]) -> Optional[str]:
    if row.get("unit"):
        return str(row.get("unit"))
    dims = row.get("dimensions") if isinstance(row.get("dimensions"), dict) else {}
    item = dims.get("ITEM") if isinstance(dims.get("ITEM"), dict) else {}
    if item.get("unit"):
        return str(item.get("unit"))
    if row.get("UNIT_NM"):
        return str(row.get("UNIT_NM"))
    return None


def _input_rows_series_materials(input_rows: Optional[list[dict[str, Any]]]) -> dict[str, Any]:
    rows = input_rows if isinstance(input_rows, list) else []
    pairs: list[tuple[str, float, dict[str, Any]]] = []
    units: set[str] = set()
    source_systems: set[str] = set()
    table_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        period = str(row.get("period") or row.get("PRD_DE") or "")
        raw_value = row.get("value") if "value" in row else row.get("DT")
        value = _numeric_value(raw_value)
        if not period or value is None:
            continue
        pairs.append((period, value, row))
        if unit := _row_unit_generic(row):
            units.add(unit)
        for key in ("source_system", "provider"):
            if row.get(key):
                source_systems.add(str(row.get(key)))
        for key in ("tbl_id", "table_id", "statbl_id"):
            if row.get(key):
                table_ids.add(str(row.get(key)))
    pairs.sort(key=lambda item: item[0])
    return {
        "times": [period for period, _, _ in pairs],
        "values": [value for _, value, _ in pairs],
        "unit": sorted(units)[0] if len(units) == 1 else None,
        "units": sorted(units),
        "source_systems": sorted(source_systems),
        "table_ids": sorted(table_ids),
        "row_count": len(rows),
        "numeric_row_count": len(pairs),
    }


def _resolve_tool_source_system(query: str, source_system: Optional[str] = None) -> Optional[str]:
    explicit = str(source_system or "").strip().upper()
    if explicit:
        return explicit
    return _detect_explicit_source_preference(query, {})


def _unsupported_source_response(tool: str, query: str, source_system: Optional[str]) -> dict[str, Any]:
    provider = source_system or _resolve_tool_source_system(query) or "unknown"
    return {
        "status": "unsupported",
        "error": f"{tool} cannot fetch {provider} data from a natural-language query.",
        "query": query,
        "source_system": provider,
        "recommended_flow": [
            "Use search_nabo_tables/explore_nabo_table/query_nabo_table for NABO raw rows.",
            "Pass those rows to compute_indicator or an input_rows-capable analysis/chart tool.",
        ],
        "mcp_output_contract": _mcp_tool_output_contract(
            role="analysis_or_visualization",
            final_answer_expected=False,
            markers=["unsupported", "unsupported_source_system"],
            explanation=f"{tool} will not silently fall back to KOSIS for {provider} requests.",
            extra_signals={"tool": tool, "query": query, "source_system": provider},
        ),
    }


def _analysis_input_materials(times: list[str], values: list[float]) -> dict[str, Any]:
    """Expose reproducible arrays so callers can re-check or replace MCP calculations."""
    x = list(range(len(values)))
    y = [float(v) for v in values]
    return {
        "data": [{"period": t, "value": y[i]} for i, t in enumerate(times)],
        "x": x,
        "x_periods": list(times),
        "y": y,
    }


def _series_characteristics(times: list[str], values: list[float], *, unit: Optional[str] = None) -> dict[str, Any]:
    if not values:
        return {"n_points": 0}
    y = np.array(values, dtype=float)
    diffs = np.diff(y)
    nonzero_diffs = diffs[np.abs(diffs) > 1e-12]
    return {
        "n_points": len(values),
        "period_range": [times[0], times[-1]] if times else None,
        "observed_min": round(float(np.min(y)), 8),
        "observed_max": round(float(np.max(y)), 8),
        "observed_mean": round(float(np.mean(y)), 8),
        "observed_std": round(float(np.std(y)), 8),
        "is_monotonic_increasing": bool(len(diffs) and np.all(diffs >= 0)),
        "is_monotonic_decreasing": bool(len(diffs) and np.all(diffs <= 0)),
        "direction_changes": int(np.sum(np.sign(nonzero_diffs[1:]) != np.sign(nonzero_diffs[:-1]))) if len(nonzero_diffs) > 1 else 0,
        "has_negative_values": bool(np.any(y < 0)),
        "has_zero_values": bool(np.any(y == 0)),
        "likely_index": bool(unit and "지수" in str(unit)),
    }


def _analysis_must_know(
    series_result: dict[str, Any],
    times: list[str],
    values: list[float],
    *,
    extra_limitations: Optional[list[str]] = None,
) -> dict[str, Any]:
    unit = series_result.get("단위") or series_result.get("unit")
    latest_period = times[-1] if times else series_result.get("used_period")
    age = series_result.get("period_age_years")
    if age is None and latest_period:
        age = NaturalLanguageAnswerEngine._period_age_years(str(latest_period))
    limitations = [
        "MCP returns data and reproducible calculation materials; caller chooses interpretation and final wording.",
    ]
    if extra_limitations:
        limitations.extend(extra_limitations)
    if age is not None and age >= 1.0:
        limitations.append(f"Latest period is {latest_period} (~{age:.1f} years old).")
    return {
        "unit": unit,
        "latest_period": latest_period,
        "period_range": [times[0], times[-1]] if times else None,
        "data_age_years": age,
        "source": "KOSIS",
        "is_index": bool(unit and "지수" in str(unit)),
        "limitations": limitations,
    }


def _analysis_common_pitfalls(series_result: dict[str, Any], values: list[float]) -> list[dict[str, Any]]:
    unit = series_result.get("단위") or series_result.get("unit") or ""
    pitfalls: list[dict[str, Any]] = []
    if "지수" in str(unit):
        pitfalls.append({
            "pitfall": "This series is an index, not an absolute amount.",
            "example_wrong": "Treating 105 as an average price or count.",
            "example_right": "Treating 105 as 5% above a base of 100 when the base definition supports it.",
        })
    if len(values) < 8:
        pitfalls.append({
            "pitfall": "Small sample size can make regression, correlation, and outlier scores unstable.",
            "example_wrong": "Presenting p-values or forecasts as robust conclusions.",
            "example_right": "Describe them as exploratory calculations based on limited observations.",
        })
    return pitfalls


def _nabo_dtacycle_code(value: Optional[str]) -> str:
    raw = str(value or "auto").strip()
    if raw.upper() == "AUTO":
        return "AUTO"
    return NABO_DTACYCLE_ALIASES.get(raw.upper()) or NABO_DTACYCLE_ALIASES.get(raw) or raw.upper()


def _nabo_dtacycle_from_name(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for token, code in NABO_DTACYCLE_FROM_NAME.items():
        if token in raw:
            return code
    return None


def _nabo_dtacycle_suggestions(primary: Optional[str] = None, *, include_generic: bool = False) -> list[str]:
    values = [primary]
    if include_generic:
        values.extend(["YY", "QY", "MM"])
    return list(dict.fromkeys(c for c in values if c))


def _nabo_dtacycle_guidance(codes: list[str]) -> dict[str, Any]:
    return {code: NABO_DTACYCLE_GUIDANCE.get(code, {}) for code in codes}


def _nabo_normalize_period_token(value: Any, cycle: str) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lower() == "latest":
        return "latest"
    if cycle == "MM":
        month_match = re.fullmatch(r"(\d{4})[-./년\s]?(\d{1,2})월?", raw)
        if month_match:
            month = int(month_match.group(2))
            if 1 <= month <= 12:
                return f"{month_match.group(1)}{month:02d}"
    return raw


def _nabo_period_sort_key(value: Any) -> tuple[int, int, str]:
    raw = str(value or "").strip()
    digits = "".join(re.findall(r"\d+", raw))
    if len(digits) >= 6:
        return (int(digits[:4]), int(digits[4:6]), raw)
    if len(digits) >= 5:
        return (int(digits[:4]), int(digits[4:]), raw)
    if len(digits) >= 4:
        return (int(digits[:4]), 0, raw)
    return (-1, -1, raw)


def _nabo_parse_period_request(
    period: Any = None,
    period_range: Optional[list[str]] = None,
    *,
    cycle: str,
) -> dict[str, Any]:
    source = "period_range" if period_range not in (None, []) else "period"
    raw_input = period_range if source == "period_range" else period
    result: dict[str, Any] = {
        "source": source,
        "raw_input": raw_input,
        "mode": "all",
        "api_filter_period": None,
        "normalized_period": None,
        "normalized_range": None,
        "errors": [],
        "accepted_forms": {
            "latest": "latest",
            "single": "2024",
            "range_array": ["2010", "2024"],
            "range_colon": "2010:2024",
            "range_hyphen": "2010-2024",
            "range_object": {"start": "2010", "end": "2024"},
        },
    }
    if raw_input in (None, ""):
        return result
    if isinstance(raw_input, str) and raw_input.strip().lower() in {"null", "undefined", "none"}:
        result["errors"].append({
            "code": "NULLISH_PERIOD_INPUT",
            "message": "period was a null-like string; omit period for all rows or use 'latest'/a valid period code.",
            "received": raw_input,
        })
        return result

    if isinstance(raw_input, dict):
        start = raw_input.get("start") or raw_input.get("from") or raw_input.get("begin")
        end = raw_input.get("end") or raw_input.get("to") or raw_input.get("until")
        raw_bounds = [start, end]
    elif isinstance(raw_input, (list, tuple)):
        values = [
            v for v in raw_input
            if v not in (None, "")
            and not (isinstance(v, str) and v.strip().lower() in {"null", "undefined", "none"})
        ]
        if len(values) == 1:
            raw_bounds = [values[0]]
        else:
            raw_bounds = values[:2]
    else:
        raw_text = str(raw_input).strip()
        if raw_text.lower() == "latest":
            result.update({"mode": "latest", "normalized_period": "latest"})
            return result
        colon_parts = re.split(r"\s*(?::|~|–|—)\s*", raw_text, maxsplit=1)
        hyphen_match = re.fullmatch(r"\s*(\d{4,6})\s*-\s*(\d{4,6})\s*", raw_text)
        if len(colon_parts) == 2:
            raw_bounds = colon_parts
        elif hyphen_match:
            raw_bounds = [hyphen_match.group(1), hyphen_match.group(2)]
        else:
            raw_bounds = [raw_text]

    normalized = [_nabo_normalize_period_token(v, cycle) for v in raw_bounds]
    normalized = [v for v in normalized if v]
    if not normalized:
        result["errors"].append({
            "code": "INVALID_PERIOD_INPUT",
            "message": "period did not include a usable NABO period code.",
        })
        return result
    if len(normalized) == 1:
        if normalized[0] == "latest":
            result.update({"mode": "latest", "normalized_period": "latest"})
        else:
            result.update({
                "mode": "single",
                "normalized_period": normalized[0],
                "api_filter_period": normalized[0],
            })
        return result

    start, end = normalized[0], normalized[1]
    start_key = _nabo_period_sort_key(start)
    end_key = _nabo_period_sort_key(end)
    if start_key[0] < 0 or end_key[0] < 0:
        result["errors"].append({
            "code": "INVALID_PERIOD_RANGE",
            "message": "period range bounds could not be interpreted as ordered period codes.",
            "bounds": [start, end],
        })
        return result
    if start_key > end_key:
        start, end = end, start
    result.update({
        "mode": "range",
        "normalized_range": [start, end],
        "range_filter_applied_by_mcp": True,
    })
    return result


def _nabo_apply_period_request(rows: list[dict[str, Any]], period_request: dict[str, Any]) -> list[dict[str, Any]]:
    mode = period_request.get("mode")
    if mode == "latest" and rows:
        latest_period = max(str(row.get("WRTTIME_IDTFR_ID") or "") for row in rows)
        return [row for row in rows if str(row.get("WRTTIME_IDTFR_ID") or "") == latest_period]
    if mode == "range":
        start, end = period_request.get("normalized_range") or [None, None]
        start_key = _nabo_period_sort_key(start)
        end_key = _nabo_period_sort_key(end)
        selected = []
        for row in rows:
            period_key = _nabo_period_sort_key(row.get("WRTTIME_IDTFR_ID"))
            if start_key <= period_key <= end_key:
                selected.append(row)
        return selected
    return rows


def _nabo_service_root(service: str) -> str:
    roots = {
        "Sttsapitbl": "Sttsapitbl",
        "Sttsapitblitm": "Sttsapitblitm",
        "Sttsapitbldata": "Sttsapitbldata",
        "DicApiList": "DicApiList",
    }
    return roots.get(service, service)


def _nabo_parse_response(payload: dict[str, Any], service: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "status": "failed",
            "code": "INVALID_RESPONSE",
            "message": "NABO response was not a JSON object.",
            "total_count": 0,
            "rows": [],
        }
    if isinstance(payload.get("RESULT"), dict):
        result = payload["RESULT"]
        code = str(result.get("CODE") or "")
        return {
            "status": "executed" if code.endswith("000") else "failed",
            "code": code,
            "message": result.get("MESSAGE"),
            "total_count": 0,
            "rows": [],
        }

    root = _nabo_service_root(service)
    blocks = payload.get(root) or payload.get(root.lower()) or payload.get(root.upper())
    if not isinstance(blocks, list):
        return {
            "status": "failed",
            "code": "MISSING_SERVICE_ROOT",
            "message": f"NABO response did not include {root}.",
            "total_count": 0,
            "rows": [],
        }

    total_count = 0
    result_code = None
    result_message = None
    rows: list[dict[str, Any]] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        head = block.get("head")
        if isinstance(head, list):
            for item in head:
                if not isinstance(item, dict):
                    continue
                if "list_total_count" in item:
                    try:
                        total_count = int(item.get("list_total_count") or 0)
                    except (TypeError, ValueError):
                        total_count = 0
                result = item.get("RESULT")
                if isinstance(result, dict):
                    result_code = result.get("CODE")
                    result_message = result.get("MESSAGE")
        row = block.get("row")
        if isinstance(row, list):
            rows.extend([r for r in row if isinstance(r, dict)])
        elif isinstance(row, dict):
            rows.append(row)

    code = str(result_code or "")
    return {
        "status": "executed" if not code or code.endswith("000") else "failed",
        "code": code or None,
        "message": result_message,
        "total_count": total_count or len(rows),
        "rows": rows,
    }


async def _nabo_call(
    client: httpx.AsyncClient,
    service: str,
    key: str,
    params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    query = {
        "Key": key,
        "Type": "json",
        "pIndex": 1,
        "pSize": 100,
    }
    query.update({k: v for k, v in (params or {}).items() if v not in (None, "", [])})
    response = await client.get(f"{NABO_API_BASE}/{service}.do", params=query, timeout=20.0)
    response.raise_for_status()
    return _nabo_parse_response(response.json(), service)


async def _nabo_fetch_rows(
    service: str,
    key: str,
    params: Optional[dict[str, Any]] = None,
    *,
    max_rows: int = NABO_DEFAULT_MAX_ROWS,
) -> dict[str, Any]:
    clean_max = max(1, min(max_rows, 50000))
    all_rows: list[dict[str, Any]] = []
    total_count = 0
    last: dict[str, Any] = {}
    async with httpx.AsyncClient() as client:
        page = 1
        while len(all_rows) < clean_max:
            page_size = min(NABO_MAX_PAGE_SIZE, clean_max - len(all_rows))
            call_params = dict(params or {})
            call_params.update({"pIndex": page, "pSize": page_size})
            parsed = await _nabo_call(client, service, key, call_params)
            last = parsed
            total_count = int(parsed.get("total_count") or total_count or 0)
            rows = parsed.get("rows") or []
            if parsed.get("status") != "executed":
                break
            if not rows:
                break
            all_rows.extend(rows)
            if total_count and len(all_rows) >= total_count:
                break
            if len(rows) < page_size:
                break
            page += 1
    return {
        **last,
        "rows": all_rows[:clean_max],
        "returned_count": min(len(all_rows), clean_max),
        "total_count": total_count or len(all_rows),
        "truncated": bool(total_count and len(all_rows) < total_count),
        "max_rows": clean_max,
    }


def _nabo_table_record(row: dict[str, Any]) -> dict[str, Any]:
    cadence_name = row.get("DTACYCLE_NM")
    return {
        "source_system": "NABO",
        "provider": "국회예산정책처 재정경제통계시스템",
        "table_id": str(row.get("STATBL_ID") or ""),
        "table_name": row.get("STATBL_NM"),
        "category": row.get("CATE_FULLNM"),
        "cadence_name": cadence_name,
        "dtacycle_cd_suggestion": _nabo_dtacycle_from_name(cadence_name),
        "original_source": row.get("TOP_ORG_NM"),
        "department": row.get("ORG_NM"),
        "manager": row.get("USR_NM"),
        "load_date": row.get("LOAD_DATE"),
        "open_date": row.get("OPEN_DATE"),
        "data_start_year": row.get("DATA_START_YY"),
        "data_end_year": row.get("DATA_END_YY"),
        "table_comment": row.get("STATBL_CMMT"),
        "service_url": row.get("SRV_URL"),
        "raw": row,
    }


def _nabo_catalog_match_score(query: Any, table: dict[str, Any]) -> int:
    q = _compact_text(str(query or ""))
    if len(q) < 2:
        return 0
    name = _compact_text(str(table.get("table_name") or ""))
    category = _compact_text(str(table.get("category") or ""))
    comment = _compact_text(str(table.get("table_comment") or ""))
    if not name:
        return 0
    if name == q:
        return 100
    if len(name) >= 2 and name in q:
        return 92
    if len(q) >= 3 and q in name:
        return 88
    if len(q) >= 3 and (q in category or q in comment):
        return 62
    return 0


def _nabo_table_candidates_support_query(query: Any, tables: list[dict[str, Any]]) -> bool:
    return any(_nabo_catalog_match_score(query, table) >= 80 for table in tables if isinstance(table, dict))


async def _nabo_table_catalog(key: str) -> list[dict[str, Any]]:
    cache_key = "all"
    if cache_key in NABO_TABLE_CATALOG_CACHE:
        return NABO_TABLE_CATALOG_CACHE[cache_key]
    payload = await _nabo_fetch_rows("Sttsapitbl", key, {"pSize": NABO_MAX_PAGE_SIZE}, max_rows=50000)
    tables = [_nabo_table_record(row) for row in payload.get("rows", [])]
    NABO_TABLE_CATALOG_CACHE[cache_key] = tables
    return tables


async def _nabo_catalog_fallback_search(query: str, key: str, limit: int) -> dict[str, Any]:
    catalog = await _nabo_table_catalog(key)
    scored = [
        {**table, "catalog_match_score": score, "search_match_source": "nabo_table_catalog"}
        for table in catalog
        for score in [_nabo_catalog_match_score(query, table)]
        if score > 0
    ]
    scored.sort(key=lambda table: (-int(table.get("catalog_match_score") or 0), str(table.get("table_name") or ""), str(table.get("table_id") or "")))
    return {
        "tables": scored[:limit],
        "total_count": len(scored),
        "catalog_size": len(catalog),
    }


def _nabo_item_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_system": "NABO",
        "table_id": str(row.get("STATBL_ID") or ""),
        "item_tag": row.get("ITM_TAG"),
        "item_id": str(row.get("ITM_ID") or ""),
        "parent_item_id": None if row.get("PAR_ITM_ID") in (None, "", 0, "0") else str(row.get("PAR_ITM_ID")),
        "item_name": row.get("ITM_NM"),
        "item_full_name": row.get("ITM_FULLNM"),
        "unit": row.get("UI_NM"),
        "comment_id": row.get("ITM_CMMT_IDTFR"),
        "comment": row.get("ITM_CMMT_CONT"),
        "order": row.get("V_ORDER"),
        "raw": row,
    }


def _nabo_data_record(row: dict[str, Any], item_meta: Optional[dict[str, dict[str, Any]]] = None) -> dict[str, Any]:
    raw_value = row.get("DTA_VAL")
    try:
        value = float(str(raw_value).replace(",", "")) if raw_value not in (None, "") else None
    except (TypeError, ValueError):
        value = None
    unit = row.get("UI_NM")
    item_code = None if row.get("ITM_ID") in (None, "") else str(row.get("ITM_ID"))
    meta = (item_meta or {}).get(item_code or "") or {}
    item_full_name = meta.get("item_full_name") or row.get("ITM_FULLNM")
    return {
        "source_system": "NABO",
        "provider": "국회예산정책처 재정경제통계시스템",
        "table_id": str(row.get("STATBL_ID") or ""),
        "dtacycle_cd": row.get("DTACYCLE_CD"),
        "period": str(row.get("WRTTIME_IDTFR_ID") or ""),
        "value": value,
        "value_raw": raw_value,
        "unit": unit,
        "symbol": row.get("DTA_SVAL"),
        "item_full_name": item_full_name,
        "dimensions": {
            "GROUP": {
                "code": None if row.get("GRP_ID") in (None, "") else str(row.get("GRP_ID")),
                "label": row.get("GRP_NM"),
                "unit": None,
            },
            "CLASS": {
                "code": None if row.get("CLS_ID") in (None, "") else str(row.get("CLS_ID")),
                "label": row.get("CLS_NM"),
                "unit": None,
            },
            "ITEM": {
                "code": item_code,
                "label": row.get("ITM_NM"),
                "full_label": item_full_name,
                "parent_item_id": meta.get("parent_item_id"),
                "comment": meta.get("comment"),
                "unit": unit,
            },
        },
        "raw": row,
    }


NABO_FILTER_KEY_MAP = {
    "ITEM": ("ITM_ID", "ITM_NM"),
    "ITM_ID": ("ITM_ID",),
    "item_id": ("ITM_ID",),
    "ITEM_NM": ("ITM_NM",),
    "item_name": ("ITM_NM",),
    "CLASS": ("CLS_ID", "CLS_NM"),
    "CLS_ID": ("CLS_ID",),
    "class_id": ("CLS_ID",),
    "CLS_NM": ("CLS_NM",),
    "class_name": ("CLS_NM",),
    "GROUP": ("GRP_ID", "GRP_NM"),
    "GRP_ID": ("GRP_ID",),
    "group_id": ("GRP_ID",),
    "GRP_NM": ("GRP_NM",),
    "group_name": ("GRP_NM",),
    "period": ("WRTTIME_IDTFR_ID",),
    "WRTTIME_IDTFR_ID": ("WRTTIME_IDTFR_ID",),
}


def _as_string_set(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(v) for v in value if v not in (None, "")}
    if value in (None, ""):
        return set()
    return {str(value)}


def _nabo_filter_data_rows(rows: list[dict[str, Any]], filters: Optional[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not filters:
        return rows, []
    errors: list[dict[str, Any]] = []
    predicates: list[tuple[str, tuple[str, ...], set[str]]] = []
    for key, value in filters.items():
        fields = NABO_FILTER_KEY_MAP.get(str(key))
        if not fields:
            errors.append({
                "code": "UNKNOWN_FILTER_KEY",
                "filter_key": key,
                "valid_filter_keys": sorted(NABO_FILTER_KEY_MAP),
            })
            continue
        values = _as_string_set(value)
        if values:
            predicates.append((str(key), fields, values))
    if errors:
        return [], errors

    selected = []
    for row in rows:
        matched = True
        for _, fields, values in predicates:
            row_values = {str(row.get(field)) for field in fields if row.get(field) not in (None, "")}
            if not row_values & values:
                matched = False
                break
        if matched:
            selected.append(row)
    return selected, []


def _nabo_filter_coverage(rows: list[dict[str, Any]], filters: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not filters:
        return {
            "requested_value_count": 0,
            "matched_value_count": 0,
            "missing_filter_values": [],
            "coverage_ratio": 1.0,
        }
    missing: list[dict[str, Any]] = []
    requested_total = 0
    matched_total = 0
    for key, value in filters.items():
        fields = NABO_FILTER_KEY_MAP.get(str(key))
        if not fields:
            continue
        requested_values = _as_string_set(value)
        if not requested_values:
            continue
        observed = {
            str(row.get(field))
            for row in rows
            for field in fields
            if row.get(field) not in (None, "")
        }
        matched = sorted(requested_values & observed)
        missing_values = sorted(requested_values - observed)
        requested_total += len(requested_values)
        matched_total += len(matched)
        if missing_values:
            missing.append({
                "filter_key": key,
                "requested_values": sorted(requested_values),
                "matched_values": matched,
                "missing_values": missing_values,
                "matched_fields": list(fields),
            })
    ratio = 1.0 if requested_total == 0 else matched_total / requested_total
    return {
        "requested_value_count": requested_total,
        "matched_value_count": matched_total,
        "missing_filter_values": missing,
        "coverage_ratio": ratio,
    }


async def _nabo_item_meta_map(statbl_id: str, key: str) -> dict[str, dict[str, Any]]:
    cache_key = str(statbl_id or "")
    if cache_key in NABO_ITEM_META_CACHE:
        return NABO_ITEM_META_CACHE[cache_key]
    payload = await _nabo_fetch_rows(
        "Sttsapitblitm",
        key,
        {"STATBL_ID": statbl_id, "pSize": NABO_MAX_PAGE_SIZE},
        max_rows=NABO_MAX_PAGE_SIZE,
    )
    mapping = {
        item["item_id"]: item
        for item in (_nabo_item_record(row) for row in payload.get("rows", []))
        if item.get("item_id")
    }
    NABO_ITEM_META_CACHE[cache_key] = mapping
    return mapping


def _region_field_names(param: QuickStatParam) -> tuple[str, str]:
    region_obj = getattr(param, "region_obj", "obj_l1")
    if region_obj == "obj_l2":
        return "C2", "C2_NM"
    if region_obj == "obj_l3":
        return "C3", "C3_NM"
    return "C1", "C1_NM"

class NaturalLanguageAnswerEngine:
    """자연어 질문을 실제 실행 가능한 답변 또는 안전한 후보 답변으로 변환."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @staticmethod
    def _norm(text: str) -> str:
        return _compact_text(text)

    @staticmethod
    def _to_float(value: Any) -> float:
        return float(str(value).replace(",", ""))

    async def _latest_stat(self, key: str, label: Optional[str] = None, region: str = "전국") -> AnswerStat:
        result = await quick_stat(key, region, "latest", self.api_key)
        if "오류" in result or "값" not in result:
            raise RuntimeError(str(result.get("오류") or result))
        param = TIER_A_STATS.get(key)
        return AnswerStat(
            key=key,
            label=label or (param.description if param else key),
            value=self._to_float(result["값"]),
            formatted=_format_number(result["값"]),
            unit=result.get("단위", param.unit if param else ""),
            period=str(result.get("시점", "")),
            table=result.get("통계표", param.tbl_nm if param else ""),
            region=region,
        )

    @staticmethod
    def _route_payload(query: str) -> dict[str, Any]:
        return _route_query(query).to_agent_payload()

    @staticmethod
    def _effective_region(route_payload: dict[str, Any], region: str) -> str:
        """Use an explicit tool argument first, then a region parsed from the query."""
        if region and region != "전국":
            return region
        slot_region = route_payload.get("slots", {}).get("region")
        if isinstance(slot_region, str) and slot_region:
            return slot_region
        return region or "전국"

    @staticmethod
    def _same_period(stats: list[AnswerStat]) -> bool:
        return len({s.period for s in stats}) == 1

    @staticmethod
    def _validation_notes(route_payload: dict[str, Any]) -> list[str]:
        warnings = route_payload.get("validation", {}).get("warnings", [])
        notes = list(warnings)
        if "기업 수·사업체 수·자영업자 수는 모집단 기준이 다르므로 혼동 금지." not in notes:
            notes.append("기업 수·사업체 수·자영업자 수는 모집단 기준이 다르므로 혼동 금지.")
        return notes

    @staticmethod
    def _period_argument(query: str, route_payload: dict[str, Any]) -> str:
        slots = route_payload.get("slots", {})
        time_slot = slots.get("time") if isinstance(slots, dict) else None
        if _parse_month_token(query) or _parse_quarter_token(query):
            return query
        if isinstance(time_slot, dict):
            if time_slot.get("type") in {"year", "month", "quarter"}:
                return str(time_slot.get("value") or query)
            if time_slot.get("type") == "latest":
                return "latest"
        if any(term in query for term in ("재작년", "작년", "지난해", "전년", "올해", "금년")):
            return query
        return "latest"

    def _is_growth_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        if "STAT_GROWTH_RATE" in route_payload.get("intents", []):
            return True
        q = self._norm(query)
        return any(term in q for term in ("전년대비", "전월대비", "증가율", "감소율", "변화율", "얼마나늘", "얼마나줄"))

    @staticmethod
    def _growth_period_count(query: str) -> int:
        m = re.search(r"최근\s*(\d+)\s*(?:년|개월)", query)
        if m:
            return max(2, int(m.group(1)))
        return 2

    def _is_sme_smallbiz_count_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and "소상공인" in q
            and any(term in q for term in ("수", "사업체", "기업"))
            and not any(term in q for term in ("업종별", "지역별", "폐업률", "창업률", "매출"))
        )

    def _is_sme_employee_average_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and any(term in q for term in ("종사자", "고용"))
            and any(term in q for term in ("사업체당", "기업당", "평균", "함께", "비교"))
        )

    def _is_region_compare_question(self, query: str) -> bool:
        q = self._norm(query)
        return any(term in q for term in ("시도별", "지역별", "광역시별", "17개시도", "지역순위"))

    @staticmethod
    def _extract_top_n(query: str) -> Optional[int]:
        compact = re.sub(r"\s+", "", str(query))
        patterns = [
            r"top(\d+)",
            r"상위(\d+)",
            r"하위(\d+)",
            r"가장많은(\d+)[곳개]",
            r"가장적은(\d+)[곳개]",
            r"가장많은(?:시도|지역)(\d+)[곳개]",
            r"가장적은(?:시도|지역)(\d+)[곳개]",
            r"많은(\d+)[곳개]",
            r"적은(\d+)[곳개]",
            r"많은(?:시도|지역)(\d+)[곳개]",
            r"적은(?:시도|지역)(\d+)[곳개]",
            r"(?:시도|지역)(\d+)[곳개]",
            r"(\d+)개시도",
            r"(\d+)개지역",
            r"(\d+)[곳개]",
            r"(\d+)위까지",
            r"1위부터(\d+)위",
            r"순위(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, compact, re.IGNORECASE)
            if m:
                try:
                    n = int(m.group(1))
                    if 1 <= n <= 17:
                        return n
                except ValueError:
                    continue
        return None

    def _is_top_n_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        if "STAT_RANKING" in route_payload.get("intents", []):
            return True
        if self._extract_top_n(query) is not None:
            return True
        q = self._norm(query)
        return any(term in q for term in ("가장많", "가장적", "상위", "하위", "topn"))

    def _is_share_ratio_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        # Survival/closure rate is a time-based dynamic ratio, not a
        # part/whole 구성비 — refuse to dispatch share_ratio for those
        # verbal patterns (#27).
        if self._is_dynamic_ratio_question(query):
            return False
        slots = route_payload.get("slots") or {}
        calc = slots.get("calculation") if isinstance(slots, dict) else None
        if isinstance(calc, list) and "share_ratio" in calc:
            return True
        if "STAT_SHARE_RATIO" in route_payload.get("intents", []):
            return True
        q = self._norm(query)
        return any(term in q for term in ("비중", "비율", "차지", "구성비"))

    @classmethod
    def _is_dynamic_ratio_question(cls, query: str) -> bool:
        """Detect 'time-cohort ratio' verbal cues that the share_ratio
        keyword scanner mistakes for 부분/전체 비중."""
        q = re.sub(r"\s+", "", str(query))
        return any(term in q for term in (
            "살아남", "생존율", "잔존율", "폐업률", "창업률", "유지율",
        ))

    @staticmethod
    def _is_aggregation_question(query: str) -> bool:
        q = re.sub(r"\s+", "", str(query))
        return any(term in q for term in ("합계", "합산", "총합", "더하"))

    @staticmethod
    def _extract_extra_regions(query: str, primary_region: str, route_payload: dict[str, Any]) -> list[str]:
        slots = route_payload.get("slots") or {}
        candidates: list[str] = []
        comp = slots.get("comparison_target") if isinstance(slots, dict) else None
        if isinstance(comp, list):
            candidates.extend(comp)
        q_compact = re.sub(r"\s+", "", str(query))
        known_regions = [
            "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
            "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
        ]
        for region in known_regions:
            if region in q_compact and region != primary_region and region not in candidates:
                candidates.append(region)
        return [r for r in candidates if r in known_regions]

    @staticmethod
    def _extract_composite_regions(query: str) -> list[str]:
        """Return the list of composite-region labels (수도권, 비수도권, …)
        that appear in the raw query. Longer labels win when one is a
        substring of another (비수도권 contains 수도권) so the more
        specific composite is selected."""
        q_compact = re.sub(r"\s+", "", str(query))
        candidates = sorted(
            (name for name in REGION_COMPOSITES if name in q_compact),
            key=len,
            reverse=True,
        )
        deduped: list[str] = []
        for name in candidates:
            if not any(name != existing and name in existing for existing in deduped):
                deduped.append(name)
        return deduped

    @classmethod
    def _expand_composite_to_components(cls, composite: str) -> list[str]:
        return list(REGION_COMPOSITES.get(composite, []))

    _COMPOSITE_PER_CALL_TIMEOUT = 12.0
    _COMPOSITE_TOTAL_BUDGET = 60.0

    async def _answer_composite_aggregate(
        self,
        query: str,
        direct_key: str,
        composite: str,
        operation: str = "sum",
    ) -> dict[str, Any]:
        """Handle composite-region queries (수도권 사업체수, 영남권 비중 등).

        operation="sum" returns tier_a_region_sum for the components.
        operation="share" computes (sum of components) / 전국 * 100.

        All component calls run in parallel with a per-call timeout
        and an overall budget — sequential iteration of 14 regions
        (비수도권) used to chain 14 × 30 s timeouts on a slow KOSIS
        response, which presented to callers as a multi-minute hang
        and starved every other in-flight request (#30)."""
        components = self._expand_composite_to_components(composite)
        if not components:
            return await self._answer_search_fallback(query)
        components = components[:17]
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        async def fetch_one(region: str) -> tuple[str, dict[str, Any]]:
            try:
                return region, await asyncio.wait_for(
                    _quick_stat_core(direct_key, region, period, self.api_key),
                    timeout=self._COMPOSITE_PER_CALL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return region, {"오류": "호출 타임아웃"}
            except Exception as exc:  # network/protocol failures shouldn't poison the whole batch
                return region, {"오류": f"{type(exc).__name__}: {exc}"}

        try:
            gathered = await asyncio.wait_for(
                asyncio.gather(*(fetch_one(r) for r in components)),
                timeout=self._COMPOSITE_TOTAL_BUDGET,
            )
        except asyncio.TimeoutError:
            return {
                "상태": "failed",
                "코드": STATUS_RUNTIME_ERROR,
                "답변유형": "tier_a_composite_timeout",
                "질문": query,
                "answer": (
                    f"{composite}({'+'.join(components)}) 합산이 전체 예산({self._COMPOSITE_TOTAL_BUDGET:.0f}초)을 "
                    "초과해 중단했습니다. KOSIS 응답 지연 가능성 — 잠시 후 재시도하거나 시도별 단일 호출로 분리하세요."
                ),
                "구성_지역": components,
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }

        rows: list[dict[str, Any]] = []
        subtotal = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region, stat in gathered:
            if not isinstance(stat, dict) or "오류" in stat or "값" not in stat:
                missing.append(region)
                continue
            try:
                value = self._to_float(stat["값"])
            except (KeyError, ValueError):
                missing.append(region)
                continue
            subtotal += value
            unit = unit or stat.get("단위", "")
            used_period = used_period or str(stat.get("시점", ""))
            table = table or stat.get("통계표", "")
            rows.append({
                "지역": region,
                "값": _format_number(value),
                "원본값": value,
                "단위": stat.get("단위", unit),
                "시점": stat.get("시점", ""),
                "통계표": stat.get("통계표"),
            })

        if not rows:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        if missing:
            notes.append(f"{composite} 합산에서 제외된 지역: {', '.join(missing)} (데이터 없음)")
        distinct_periods = {row["시점"] for row in rows}
        if len(distinct_periods) > 1:
            notes.append(f"{composite} 구성 지역 간 시점 불일치: {sorted(distinct_periods)}")

        base_payload: dict[str, Any] = {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "질문": query,
            "표": rows,
            "구성_지역": components,
            "추천_시각화": ["bar_chart"],
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        if operation == "share":
            whole = await quick_stat(direct_key, "전국", period, self.api_key)
            if "오류" in whole or "값" not in whole:
                return await self._answer_search_fallback(query, route_payload)
            try:
                whole_value = self._to_float(whole["값"])
            except (KeyError, ValueError):
                return await self._answer_search_fallback(query, route_payload)
            if not whole_value:
                return await self._answer_search_fallback(query, route_payload)
            share = subtotal / whole_value * 100
            whole_period = str(whole.get("시점", ""))
            if used_period and whole_period and used_period != whole_period:
                notes.append(f"분자 시점 {used_period} ↔ 분모 시점 {whole_period} 불일치")
            return {
                **base_payload,
                "답변유형": "tier_a_composite_share_ratio",
                "answer": (
                    f"{used_period} 기준 {composite}({'+'.join(components)})의 "
                    f"{stat_label}은(는) {_format_number(subtotal)}{unit}로, "
                    f"전국({_format_number(whole_value)}{unit}) 대비 약 {share:.2f}%입니다."
                ),
                "계산": {
                    "분자": _format_number(subtotal),
                    "분모": _format_number(whole_value),
                    "비중_퍼센트": round(share, 2),
                    "산식": f"({' + '.join(components)}) / 전국 * 100",
                    "동일시점_여부": used_period == whole_period,
                },
                "검증_주의": notes,
            }

        return {
            **base_payload,
            "답변유형": "tier_a_region_sum",
            "answer": (
                f"{used_period} 기준 {composite}({'+'.join(components)})의 "
                f"{stat_label} 합계는 {_format_number(subtotal)}{unit}입니다."
            ),
            "계산": {
                "합계": _format_number(subtotal),
                "포함_지역": [r["지역"] for r in rows],
                "제외_지역": missing,
                "산식": " + ".join(r["지역"] for r in rows),
                "합성지역": composite,
            },
            "검증_주의": notes,
        }

    def _is_sme_large_sales_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and any(term in q for term in ("대기업", "중소기업외"))
            and any(term in q for term in ("매출", "매출액"))
            and any(term in q for term in ("비교", "비중", "차이", "전체매출"))
        )

    def _mixed_self_employed_business_key(self, query: str) -> Optional[tuple[str, str]]:
        q = self._norm(query)
        has_self_employed = any(term in q for term in ("자영업자", "자영업", "개인사업자"))
        has_business_count = any(term in q for term in ("사업체", "사업체수", "기업수", "업체수", "업체"))
        asks_jointly = any(term in q for term in ("비교", "차이", "비중", "대비", "같이", "함께", "와", "과"))
        if not (has_self_employed and has_business_count and asks_jointly):
            return None
        if "중소기업" in q:
            return "중소기업_사업체수", "중소기업 사업체 수"
        if "소상공인" in q:
            return "소상공인_사업체수", "소상공인 사업체 수"
        return "전체사업체수", "총 사업체 수"

    def _is_self_employed_sme_population_question(self, query: str) -> bool:
        return self._mixed_self_employed_business_key(query) is not None

    @staticmethod
    def _needs_advanced_analysis_plan(route_payload: dict[str, Any]) -> bool:
        advanced = {"STAT_CORRELATION", "STAT_REGRESSION", "POLICY_EFFECT_ANALYSIS"}
        return any(intent in advanced for intent in route_payload.get("intents", []))

    def _infer_direct_stat_key(self, query: str, route_payload: dict[str, Any]) -> Optional[str]:
        direct_key = route_payload["route"].get("direct_stat_key")
        if direct_key:
            return direct_key
        q = self._norm(query)
        if "중소기업" in q and any(term in q for term in ("매출", "매출액")):
            return "중소기업_매출액"
        if "대기업" in q and any(term in q for term in ("매출", "매출액")):
            return "대기업_매출액"
        if "중소기업" in q and any(term in q for term in ("종사자", "고용")):
            return "중소기업_종사자수"
        if "중소기업" in q and any(term in q for term in ("사업체", "기업수", "업체수", "중소기업수")):
            return "중소기업_사업체수"
        if "소상공인" in q and any(term in q for term in ("사업체", "업체수", "소상공인수")):
            return "소상공인_사업체수"
        return None

    async def _answer_sme_smallbiz_counts(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        sme = await self._latest_stat("중소기업_사업체수", "중소기업 사업체 수", region)
        smallbiz = await self._latest_stat("소상공인_사업체수", "소상공인 사업체 수", region)
        stats = [sme, smallbiz]

        diff = sme.value - smallbiz.value
        smallbiz_share = (smallbiz.value / sme.value * 100) if sme.value else None
        comparison = {
            "차이_중소기업-소상공인": _format_number(diff),
            "소상공인_대비_중소기업_비중": round(smallbiz_share, 2) if smallbiz_share is not None else None,
            "동일시점_여부": self._same_period(stats),
        }
        answer = (
            f"{sme.period}년 기준 {region} 중소기업 사업체 수는 {sme.formatted}{sme.unit}, "
            f"소상공인 사업체 수는 {smallbiz.formatted}{smallbiz.unit}입니다. "
            f"소상공인은 중소기업 중 더 작은 규모 요건을 만족하는 하위 집단에 가까우므로, "
            f"두 지표는 포함 범위와 작성 기준 차이 때문에 값이 다릅니다."
        )
        if smallbiz_share is not None:
            answer += f" 이 기준에서는 소상공인 사업체 수가 중소기업 사업체 수의 약 {smallbiz_share:.2f}%입니다."

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite",
            "질문": query,
            "answer": answer,
            "표": [s.to_row() for s in stats],
            "계산": {
                **comparison,
                "산식": "차이 = 중소기업 사업체 수 - 소상공인 사업체 수; 비중 = 소상공인 사업체 수 / 중소기업 사업체 수 * 100",
            },
            "해석": [
                "중소기업은 더 넓은 기업 규모 범주이고, 소상공인은 상시근로자 수 등 요건이 더 좁은 집단입니다.",
                "사업체 기준 통계이므로 법인·경영 단위의 기업체 수와 직접 동일시하면 안 됩니다.",
            ],
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_self_employed_sme_population_warning(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        business_key, business_label = self._mixed_self_employed_business_key(query) or (
            "중소기업_사업체수",
            "중소기업 사업체 수",
        )
        try:
            business = await self._latest_stat(business_key, business_label, region)
            self_employed = await self._latest_stat("자영업자수", "자영업자 수", region)
        except RuntimeError:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        notes.append(
            f"이 질의는 자영업자(종사상지위별 취업자)와 {business_label}(사업체 단위)를 함께 언급합니다. "
            "두 모집단은 단위와 작성 기준이 달라 비율·차이를 자동 계산하지 않았습니다."
        )
        if not self._same_period([business, self_employed]):
            notes.append(f"시점 불일치: {business_label} {business.period}, 자영업자 수 {self_employed.period}")

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_population_mixed_comparison",
            "질문": query,
            "answer": (
                f"{region} 기준 {business_label}와 자영업자 수를 각각 조회했습니다. "
                "두 지표는 모집단이 달라 직접 비율이나 차이로 계산하지 않았습니다."
            ),
            "표": [business.to_row(), self_employed.to_row()],
            "모집단_주의": {
                business_key: "사업체 단위 사업체 수",
                "자영업자수": "종사상지위별 취업자 중 자영업자",
                "자동계산": "차이·비율 계산 안 함",
            },
            "추천_후속": [
                "자영업자 수만 조회하려면 quick_stat('자영업자수', region='전국', period='latest')",
                "사업체 단위 비교가 필요하면 같은 사업체조사 계열 통계표를 선택",
            ],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_sme_employee_average(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        businesses = await self._latest_stat("중소기업_사업체수", "중소기업 사업체 수", region)
        workers = await self._latest_stat("중소기업_종사자수", "중소기업 종사자 수", region)
        avg = workers.value / businesses.value if businesses.value else 0.0
        answer = (
            f"{businesses.period}년 기준 {region} 중소기업 사업체 수는 "
            f"{businesses.formatted}{businesses.unit}, 종사자 수는 {workers.formatted}{workers.unit}입니다. "
            f"단순 계산한 사업체당 평균 종사자 수는 약 {avg:.2f}명입니다."
        )

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite_calculation",
            "질문": query,
            "answer": answer,
            "표": [businesses.to_row(), workers.to_row()],
            "계산": {
                "사업체당_평균_종사자수": round(avg, 2),
                "산식": "중소기업 종사자 수 / 중소기업 사업체 수",
                "동일시점_여부": self._same_period([businesses, workers]),
            },
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_sme_large_sales(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        sme = await self._latest_stat("중소기업_매출액", "중소기업 매출액", region)
        large = await self._latest_stat("대기업_매출액", "대기업 매출액", region)
        total = sme.value + large.value
        diff = large.value - sme.value
        sme_share = (sme.value / total * 100) if total else None
        large_share = (large.value / total * 100) if total else None

        answer = (
            f"{sme.period}년 기준 {region} 중소기업 매출액은 {sme.formatted}{sme.unit}, "
            f"대기업 매출액은 {large.formatted}{large.unit}입니다. "
            f"두 값을 합산한 전체 기업 매출 대비 중소기업 비중은 약 {sme_share:.2f}%입니다."
        )

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite_comparison",
            "질문": query,
            "answer": answer,
            "표": [sme.to_row(), large.to_row()],
            "계산": {
                "전체_매출액_합산": _format_number(total),
                "대기업-중소기업_차이": _format_number(diff),
                "중소기업_비중": round(sme_share, 2) if sme_share is not None else None,
                "대기업_비중": round(large_share, 2) if large_share is not None else None,
                "동일시점_여부": self._same_period([sme, large]),
                "산식": "중소기업 비중 = 중소기업 매출액 / (중소기업 매출액 + 대기업 매출액) * 100",
            },
            "해석": [
                "KOSIS 원표의 대기업 축은 '중소기업 외' 코드입니다. 법령상 대기업만의 범위와 다를 수 있어 출처 기준을 함께 표시해야 합니다.",
                "금액 단위는 억원이며, 지역·산업·기업규모 기준이 동일한 값끼리 비교했습니다.",
            ],
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_direct(
        self,
        query: str,
        region: str,
        direct_key: Optional[str] = None,
        route_payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        route_payload = route_payload or self._route_payload(query)
        direct_key = direct_key or self._infer_direct_stat_key(query, route_payload) or query
        route_payload["route"]["direct_stat_key"] = direct_key
        q = self._norm(query)

        if self._is_growth_question(query, route_payload):
            period_count = self._growth_period_count(query)
            start_p, end_p = _extract_year_range(query)
            if start_p and end_p:
                span = max(int(end_p) - int(start_p) + 1, period_count)
            else:
                span = period_count
            comparison = await stat_time_compare(direct_key, region, start_p, end_p, span, self.api_key)
            if comparison.get("상태") != "executed":
                comparison["답변유형"] = "tier_a_growth_rate_failed"
                comparison["route"] = route_payload["route"]
                comparison["검증_주의"] = self._validation_notes(route_payload)
                return comparison
            notes = self._validation_notes(route_payload)
            used = comparison.get("비교") or {}
            used_start = (used.get("시작") or {}).get("시점")
            used_end = (used.get("종료") or {}).get("시점")
            if start_p and used_start and not str(used_start).startswith(start_p):
                notes.append(f"요청 시작 시점 {start_p} → 사용 시점 {used_start}로 변경됨")
            if end_p and used_end and not str(used_end).startswith(end_p):
                notes.append(f"요청 종료 시점 {end_p} → 사용 시점 {used_end}로 변경됨")
            return {
                "상태": "executed",
                "코드": STATUS_EXECUTED,
                "답변유형": "tier_a_growth_rate",
                "질문": query,
                "answer": comparison.get("answer"),
                "표": comparison.get("표", []),
                "비교": comparison.get("비교"),
                "요청_시작": start_p,
                "요청_종료": end_p,
                "지역": region,
                "통계표": (comparison.get("표") or [{}])[0].get("통계표"),
                "추천_시각화": ["line_chart"],
                "검증_주의": notes,
                "route": route_payload["route"],
                "출처": comparison.get("출처", "통계청 KOSIS"),
            }

        if any(term in q for term in ("추이", "최근", "시계열", "그래프", "선그래프", "분석")):
            years_match = re.search(r"최근\s*(\d+)\s*년", query)
            years = int(years_match.group(1)) if years_match else 5
            open_start_year = _extract_open_start_year(query)
            trend = await _quick_trend_core(
                direct_key,
                region,
                years,
                self.api_key,
                start_year=open_start_year,
            )
            if "오류" in trend:
                return await self._answer_search_fallback(query, route_payload)
            if open_start_year:
                answer = (
                    f"{region}의 {trend.get('통계명', direct_key)} {open_start_year}년부터 최신까지 "
                    f"{len(trend.get('시계열', []))}개 시점 자료를 조회했습니다."
                )
            else:
                answer = (
                    f"{region}의 {trend.get('통계명', direct_key)} 최근 {len(trend.get('시계열', []))}개 시점 "
                    f"자료를 조회했습니다."
                )
            notes = list(route_payload["validation"].get("warnings", []))
            period_interpretation = trend.get("⚠️ 기간_해석")
            if period_interpretation:
                notes.append(period_interpretation)
            result: dict[str, Any] = {
                "상태": "executed",
                "코드": STATUS_EXECUTED,
                "답변유형": "tier_a_trend",
                "질문": query,
                "answer": answer,
                "표": trend.get("시계열", []),
                "단위": trend.get("단위"),
                "지역": region,
                "데이터수": len(trend.get("시계열", [])),
                "통계표": trend.get("통계표"),
                "추천_시각화": ["line_chart"],
                "검증_주의": notes,
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }
            if "분석" in q:
                result["분석"] = await analyze_trend(direct_key, region, max(years, 5), self.api_key)
            return result

        open_start_year = _extract_open_start_year(query)
        if open_start_year:
            trend = await _quick_trend_core(
                direct_key,
                region,
                5,
                self.api_key,
                start_year=open_start_year,
            )
            if "오류" not in trend:
                return {
                    "상태": "executed",
                    "코드": STATUS_EXECUTED,
                    "답변유형": "tier_a_trend",
                    "질문": query,
                    "answer": (
                        f"{region}의 {trend.get('통계명', direct_key)} {open_start_year}년부터 최신까지 "
                        f"{len(trend.get('시계열', []))}개 시점 자료를 조회했습니다."
                    ),
                    "표": trend.get("시계열", []),
                    "단위": trend.get("단위"),
                    "지역": region,
                    "데이터수": len(trend.get("시계열", [])),
                    "통계표": trend.get("통계표"),
                    "추천_시각화": ["line_chart"],
                    "검증_주의": route_payload["validation"].get("warnings", []),
                    "route": route_payload["route"],
                    "출처": "통계청 KOSIS",
                }

        period = self._period_argument(query, route_payload)
        stat = await quick_stat(direct_key, region, period, self.api_key)
        if "오류" in stat:
            return await self._answer_search_fallback(query, route_payload)
        if "값" not in stat:
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "답변유형": "tier_a_value_failed",
                "질문": query,
                "answer": "요청한 조건의 KOSIS 데이터가 없습니다. 최신값으로 대체하지 않고 중단했습니다.",
                "상세": stat,
                "지역": region,
                "요청_기간": period,
                "검증_주의": self._validation_notes(route_payload),
                "route": route_payload["route"],
                "출처": stat.get("출처", "통계청 KOSIS"),
            }
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_value",
            "질문": query,
            "answer": stat.get("answer"),
            "표": [{
                "지표": direct_key,
                "값": _format_number(stat.get("값")),
                "단위": stat.get("단위"),
                "시점": stat.get("시점"),
                "지역": stat.get("지역"),
                "통계표": stat.get("통계표"),
            }],
            "검증_주의": route_payload["validation"].get("warnings", []),
            "route": route_payload["route"],
            "출처": stat.get("출처", "통계청 KOSIS"),
        }

    async def _answer_region_compare(self, query: str, direct_key: Optional[str] = None) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        direct_key = direct_key or self._infer_direct_stat_key(query, route_payload) or query
        comparison = await quick_region_compare(direct_key, api_key=self.api_key)
        if "오류" in comparison:
            return await self._answer_search_fallback(query, route_payload)
        route_payload["route"]["direct_stat_key"] = direct_key

        rows = comparison.get("표", [])
        top = rows[:3]
        bottom = rows[-3:] if len(rows) >= 3 else rows
        unit = comparison.get("단위", "")
        answer = (
            f"{comparison.get('시점')}년 기준 {comparison.get('통계명', direct_key)} 시도별 비교 결과, "
            f"가장 많은 지역은 {top[0]['지역']}({top[0]['값']}{unit})입니다."
            if top else
            "시도별 비교 데이터를 조회했습니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_region_comparison",
            "질문": query,
            "answer": answer,
            "표": rows,
            "상위": top,
            "하위": bottom,
            "추천_시각화": ["bar_chart"],
            "검증_주의": route_payload["validation"].get("warnings", []),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_top_n(
        self,
        query: str,
        direct_key: str,
        top_n: int,
        include_share_ratio: bool = False,
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        comparison = await quick_region_compare(direct_key, api_key=self.api_key)
        if "오류" in comparison:
            return await self._answer_search_fallback(query, route_payload)
        route_payload["route"]["direct_stat_key"] = direct_key

        rows = comparison.get("표", [])
        q = self._norm(query)
        descending = not any(term in q for term in ("가장적", "적은", "하위", "낮은"))
        ordered = rows if descending else list(reversed(rows))
        selected = ordered[:top_n]
        unit = comparison.get("단위", "")
        period = comparison.get("시점")
        direction = "많은" if descending else "적은"
        rank_label = "상위" if descending else "하위"
        if selected:
            leader = selected[0]
            answer = (
                f"{period} 기준 {comparison.get('통계명', direct_key)} {rank_label} {len(selected)}개 지역 중 "
                f"가장 {direction} 지역은 {leader['지역']}({leader['값']}{unit})입니다."
            )
        else:
            answer = "상위/하위 비교 데이터를 조회하지 못했습니다."

        notes = self._validation_notes(route_payload)
        payload: dict[str, Any] = {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_top_n",
            "질문": query,
            "answer": answer,
            "표": selected,
            "전체_지역수": len(rows),
            "요청_top_n": top_n,
            "정렬": "내림차순" if descending else "오름차순",
            "추천_시각화": ["bar_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }
        if not include_share_ratio or not selected:
            return payload

        try:
            subtotal = sum(
                self._to_float(row.get("원값", row.get("값")))
                for row in selected
            )
        except (TypeError, ValueError):
            return payload

        whole = await quick_stat(direct_key, "전국", str(period or "latest"), self.api_key)
        if "오류" in whole or "값" not in whole:
            return payload
        try:
            whole_value = self._to_float(whole["값"])
        except (KeyError, ValueError):
            return payload
        if not whole_value:
            return payload

        whole_period = str(whole.get("시점", ""))
        if period and whole_period and str(period) != whole_period:
            notes.append(f"분자 시점 {period} ↔ 분모 시점 {whole_period} 불일치")
        share = subtotal / whole_value * 100
        stat_label = comparison.get("통계명", direct_key)
        payload.update({
            "답변유형": "tier_a_top_n_share_ratio",
            "answer": (
                f"{period} 기준 {stat_label} {rank_label} {len(selected)}개 지역"
                f"({'+'.join(row['지역'] for row in selected)}) 합계는 "
                f"{_format_number(subtotal)}{unit}로, 전국({_format_number(whole_value)}{unit}) 대비 "
                f"약 {share:.2f}%입니다."
            ),
            "계산": {
                "분자": _format_number(subtotal),
                "분모": _format_number(whole_value),
                "비중_퍼센트": round(share, 2),
                "포함_지역": [row["지역"] for row in selected],
                "산식": f"({' + '.join(row['지역'] for row in selected)}) / 전국 * 100",
                "동일시점_여부": str(period or "") == whole_period,
            },
            "추천_시각화": ["bar_chart", "pie_chart"],
            "검증_주의": notes,
        })
        return payload

    async def _answer_share_ratio(
        self,
        query: str,
        region: str,
        direct_key: str,
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        part = await quick_stat(direct_key, region, period, self.api_key)
        whole = await quick_stat(direct_key, "전국", period, self.api_key)
        if "오류" in part or "오류" in whole or "값" not in part or "값" not in whole:
            return await self._answer_search_fallback(query, route_payload)
        try:
            part_value = self._to_float(part["값"])
            whole_value = self._to_float(whole["값"])
        except (KeyError, ValueError):
            return await self._answer_search_fallback(query, route_payload)
        if not whole_value:
            return await self._answer_search_fallback(query, route_payload)

        part_period = str(part.get("시점", ""))
        whole_period = str(whole.get("시점", ""))
        unit = part.get("단위", "")
        table = part.get("통계표")
        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        share = part_value / whole_value * 100
        notes = self._validation_notes(route_payload)
        if part_period != whole_period:
            notes.append(f"분자 시점 {part_period} ↔ 분모 시점 {whole_period} 불일치 — 동일 시점 확인 필요")

        answer = (
            f"{part_period} 기준 {region} {stat_label}은(는) "
            f"{_format_number(part_value)}{unit}로, "
            f"전국({_format_number(whole_value)}{unit}) 대비 약 {share:.2f}% 입니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_share_ratio",
            "질문": query,
            "answer": answer,
            "표": [
                {"지표": stat_label, "분자": _format_number(part_value), "단위": unit, "시점": part_period, "지역": region, "통계표": table},
                {"지표": stat_label, "분모": _format_number(whole_value), "단위": unit, "시점": whole_period, "지역": "전국", "통계표": table},
            ],
            "계산": {
                "분자": _format_number(part_value),
                "분모": _format_number(whole_value),
                "비중_퍼센트": round(share, 2),
                "산식": "지역값 / 전국값 * 100",
                "동일시점_여부": part_period == whole_period,
            },
            "추천_시각화": ["bar_chart", "pie_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_region_sum(
        self,
        query: str,
        direct_key: str,
        regions: list[str],
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        regions = regions[:17]

        async def fetch_one(region: str) -> tuple[str, dict[str, Any]]:
            try:
                return region, await asyncio.wait_for(
                    _quick_stat_core(direct_key, region, period, self.api_key),
                    timeout=self._COMPOSITE_PER_CALL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return region, {"오류": "호출 타임아웃"}
            except Exception as exc:
                return region, {"오류": f"{type(exc).__name__}: {exc}"}

        try:
            gathered = await asyncio.wait_for(
                asyncio.gather(*(fetch_one(r) for r in regions)),
                timeout=self._COMPOSITE_TOTAL_BUDGET,
            )
        except asyncio.TimeoutError:
            return {
                "상태": "failed",
                "코드": STATUS_RUNTIME_ERROR,
                "답변유형": "tier_a_region_sum_timeout",
                "질문": query,
                "answer": (
                    f"{', '.join(regions)} 합산이 전체 예산({self._COMPOSITE_TOTAL_BUDGET:.0f}초)을 초과해 "
                    "중단했습니다. KOSIS 응답 지연 가능성 — 잠시 후 재시도하거나 단일 호출로 분리하세요."
                ),
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }

        rows: list[dict[str, Any]] = []
        total = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region, stat in gathered:
            if not isinstance(stat, dict) or "오류" in stat or "값" not in stat:
                missing.append(region)
                continue
            try:
                value = self._to_float(stat["값"])
            except (KeyError, ValueError):
                missing.append(region)
                continue
            total += value
            unit = unit or stat.get("단위", "")
            used_period = used_period or str(stat.get("시점", ""))
            table = table or stat.get("통계표", "")
            rows.append({
                "지역": region,
                "값": _format_number(value),
                "원본값": value,
                "단위": stat.get("단위", unit),
                "시점": stat.get("시점", ""),
                "통계표": stat.get("통계표"),
            })

        if not rows:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        if missing:
            notes.append(f"합계에서 제외된 지역: {', '.join(missing)} (데이터 없음)")
        distinct_periods = {row["시점"] for row in rows}
        if len(distinct_periods) > 1:
            notes.append(f"지역별 시점 불일치: {sorted(distinct_periods)}")

        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        answer = (
            f"{used_period} 기준 {', '.join(r['지역'] for r in rows)} {stat_label} 합계는 "
            f"{_format_number(total)}{unit} 입니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_region_sum",
            "질문": query,
            "answer": answer,
            "표": rows,
            "계산": {
                "합계": _format_number(total),
                "포함_지역": [r["지역"] for r in rows],
                "제외_지역": missing,
                "산식": " + ".join(r["지역"] for r in rows),
            },
            "추천_시각화": ["bar_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_dynamic_ratio_advisory(
        self,
        query: str,
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Route survival/closure/cohort-ratio queries to
        indicator_dependency_map instead of misclassifying them as
        share_ratio (#27).

        'X 살아남는 비율' is a time-cohort survival rate. The keyword
        scanner used to pick "비율" alone and dispatch share_ratio,
        which produced single-point part/whole answers that did not
        address the user's question. We surface the right formula
        spec and search candidates so the caller can step into the
        cohort-aware analysis."""
        q = re.sub(r"\s+", "", str(query))
        if any(t in q for t in ("폐업", "폐업률")):
            indicator = "폐업률"
        elif any(t in q for t in ("창업", "창업률")):
            indicator = "창업률"
        else:
            indicator = "생존율"
        formula_spec = await indicator_dependency_map(indicator)
        dynamic_terms = [
            f"{indicator} {query}",
            f"{indicator} 업종별",
            f"{indicator} 산업별",
            f"기업생멸행정통계 {indicator}",
            f"신생기업 {indicator}",
        ]
        search = await _search_kosis_keywords(
            f"{indicator} {query}",
            dynamic_terms,
            8,
            self.api_key,
            used_routing=True,
        )
        return {
            "상태": "needs_table_selection",
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "답변유형": "dynamic_ratio_advisory",
            "질문": query,
            "answer": (
                f"이 질문은 '{indicator}'에 해당하는 동태 지표입니다. "
                "정태 비중(share_ratio)이 아니라 시간-코호트 기반 산식이 필요합니다. "
                "indicator_dependency_map의 산식과 search_kosis 후보 표를 함께 검토하세요."
            ),
            "지표": indicator,
            "산식_사양": formula_spec,
            "검색결과": search.get("결과", []),
            "사용된_검색어": search.get("사용된_검색어", []),
            "추천_도구_호출": [
                f"indicator_dependency_map('{indicator}') — 산식·필요 통계·검증 포인트 확인",
                f"search_kosis('{indicator} {query[:30]}') — KOSIS 통계표 후보 조회",
            ],
            "route": route_payload.get("route", {}),
        }

    async def _answer_search_fallback(self, query: str, route_payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        route_payload = route_payload or self._route_payload(query)
        # Fold route terms and slot-parsed industry/scale into the
        # actual KOSIS search keywords so route.search_terms and
        # 사용된_검색어 stay consistent (#28).
        slots = route_payload.get("slots") or {}
        slot_terms: list[str] = []
        query_enrichment: list[str] = []
        if isinstance(slots, dict):
            for slot_key in ("industry", "scale", "target"):
                value = slots.get(slot_key)
                if isinstance(value, str) and value:
                    slot_terms.append(value)
                    if value not in query:
                        query_enrichment.append(value)
        enriched_query = (query + " " + " ".join(query_enrichment)).strip() if query_enrichment else query

        route = route_payload.get("route") or {}
        route_terms = [
            str(term).strip()
            for term in (route.get("search_terms") or [])
            if str(term).strip()
        ]
        search_keywords: list[str] = []
        for term in route_terms:
            missing_slot_terms = [slot_term for slot_term in slot_terms if slot_term not in term]
            if missing_slot_terms:
                search_keywords.append((" ".join([*missing_slot_terms, term])).strip())
            search_keywords.append(term)
        if not search_keywords:
            search_keywords.append(enriched_query)

        deduped_keywords = list(dict.fromkeys(search_keywords))[:6]
        search = await _search_kosis_keywords(
            enriched_query,
            deduped_keywords,
            8,
            self.api_key,
            used_routing=bool(route_terms),
        )
        slot_enrichment = None
        if slot_terms or route_terms:
            slot_enrichment = {
                "slot_terms": slot_terms,
                "route_search_terms": route_terms,
                "최종검색어": search.get("사용된_검색어", []),
            }
        intents = route_payload.get("intents") or []
        tool_hints = self._tool_routing_hints(query, intents)
        next_steps = [
            "후보 통계표의 기준시점, 단위, 분류코드, 분모를 확인합니다.",
            "동일 기준으로 시계열·비교·비중·증가율 계산을 수행합니다.",
            "표/그래프/해석/유의사항을 함께 응답합니다.",
        ]
        if tool_hints:
            next_steps = [
                "이 의도는 전용 도구로 바로 처리 가능 — 아래 추천_도구_호출 참고",
                *next_steps,
            ]
        return {
            "상태": "needs_table_selection",
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "답변유형": "search_and_plan",
            "질문": query,
            "answer": (
                "이 질문은 여러 통계표·분류코드·산식이 필요한 복합 질의입니다. "
                "아래 후보 통계표 중 적합한 표를 선택한 뒤 실제 수치 계산을 진행해야 합니다."
            ),
            "의도": intents,
            "슬롯": route_payload["slots"],
            "실행계획": route_payload["analysis_plan"],
            "검증": route_payload["validation"],
            "검색결과": search.get("결과", []),
            "사용된_검색어": search.get("사용된_검색어", []),
            "검색어_슬롯보강": slot_enrichment,
            "추천_도구_호출": tool_hints,
            "다음단계": next_steps,
            "route": route_payload["route"],
        }

    def _extract_correlation_pair(self, query: str) -> tuple[Optional[str], Optional[str]]:
        """Identify exactly two Tier-A stat keys mentioned in the query.

        Returns (None, None) when fewer than two distinct, verified
        keys are found — that ambiguous case stays on the safe
        hint-only path instead of guessing which two stats to feed to
        correlate_stats.

        Longer descriptions are matched first so '고령인구' wins over
        '인구', then the matched substring is removed so the second
        scan does not re-count overlapping tokens."""
        if not query:
            return (None, None)
        q_norm = self._norm(query)
        remaining = q_norm
        candidates: list[tuple[str, str]] = []
        for key, param in TIER_A_STATS.items():
            if param.verification_status not in {"verified", "needs_check"}:
                continue
            description = self._norm(param.description)
            display_key = self._norm(key.replace("_", " "))
            candidates.append((key, max(description, display_key, key=len)))
        candidates.sort(key=lambda kv: -len(kv[1]))

        found: list[str] = []
        for key, marker in candidates:
            if not marker:
                continue
            if marker in remaining and key not in found:
                found.append(key)
                remaining = remaining.replace(marker, " ", 1)
                if len(found) >= 2:
                    break
        if len(found) < 2:
            return (None, None)
        return (found[0], found[1])

    async def _answer_auto_correlate(
        self,
        query: str,
        stat_x: str,
        stat_y: str,
        region: str,
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        years_match = re.search(r"최근\s*(\d+)\s*년", query)
        years = int(years_match.group(1)) if years_match else 10
        result = await correlate_stats(stat_x, stat_y, region, years, self.api_key)
        if not isinstance(result, dict) or "오류" in result:
            return await self._answer_search_fallback(query, route_payload)
        notes = list(result.get("주의") or [])
        notes.append("상관계수는 인과관계를 의미하지 않습니다.")
        notes.append(f"자동 위임: answer_query → correlate_stats('{stat_x}', '{stat_y}', region='{region}', years={years})")
        used_period = (
            result.get("종료시점")
            or result.get("end_period")
            or result.get("기간_종료")
            or str(datetime.now().year)
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_auto_correlation",
            "질문": query,
            "answer": result.get("answer") or result.get("해석") or (
                f"{stat_x}과 {stat_y}의 최근 {years}년 상관관계 분석을 수행했습니다."
            ),
            "변수": [stat_x, stat_y],
            "지역": region,
            "기간": years,
            "used_period": str(used_period),
            "결과": result,
            "추천_시각화": ["chart_correlation"],
            "검증_주의": notes,
            "route": {
                **route_payload.get("route", {}),
                "delegated_to": "correlate_stats",
            },
            "출처": "통계청 KOSIS",
        }

    def _tool_routing_hints(self, query: str, intents: list[str]) -> list[str]:
        """Cross-tool routing — when the search fallback fires for an
        intent that has a dedicated MCP tool, return concrete call
        suggestions instead of just listing candidate tables."""
        hints: list[str] = []
        if "STAT_CORRELATION" in intents:
            hints.append(
                "correlate_stats(stat_x='<지표A>', stat_y='<지표B>', region='전국', years=10) — "
                "Pearson/Spearman 자동 계산. 상관관계 의도는 이 도구가 정답이며, "
                "answer_query는 후보 표 선택 단계에서 멈추는 안전 폴백입니다."
            )
        if "STAT_REGRESSION" in intents:
            hints.append(
                "correlate_stats 결과의 상관계수와 함께, stat_time_compare로 시점 변화를 보조 분석. "
                "회귀계수 자체는 별도 분석(Python statsmodels 등) 필요."
            )
        if "STAT_OUTLIER_DETECTION" in intents:
            hints.append(
                "detect_outliers(query='<지표>', region='<지역>', years=12) — Z-score 기반 이상치 탐지."
            )
        if "STAT_FORECAST" in intents:
            hints.append(
                "forecast_stat(query='<지표>', region='<지역>', years=10, horizon=3) — 단순 외삽 예측."
            )
        if "POLICY_EFFECT_ANALYSIS" in intents:
            hints.append(
                "정책효과는 단일 호출로 답할 수 없음. correlate_stats + stat_time_compare로 "
                "before/after 구간을 따로 비교하되 인과관계 단정은 금물."
            )
        return hints

    async def answer(self, query: str, region: str = "전국") -> dict[str, Any]:
        route_payload = self._route_payload(query)
        result = await self._dispatch(query, region, precomputed_route=route_payload)
        return self._finalize_response(result, query=query, route_payload=route_payload)

    async def _dispatch(
        self,
        query: str,
        region: str,
        precomputed_route: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        route_payload = precomputed_route or self._route_payload(query)
        plan = AnswerPlanner(self).build(query, region, route_payload)
        return await self._execute_plan(query, route_payload, plan)

    async def _execute_plan(
        self,
        query: str,
        route_payload: dict[str, Any],
        plan: AnswerPlan,
    ) -> dict[str, Any]:
        params = plan.params
        direct_key = plan.direct_key

        if plan.action == "mixed_population":
            return await self._answer_self_employed_sme_population_warning(query, plan.region)
        if plan.action == "sme_large_sales":
            return await self._answer_sme_large_sales(query, plan.region)
        if plan.action == "sme_smallbiz_counts":
            return await self._answer_sme_smallbiz_counts(query, plan.region)
        if plan.action == "sme_employee_average":
            return await self._answer_sme_employee_average(query, plan.region)
        if plan.action == "dynamic_ratio":
            return await self._answer_dynamic_ratio_advisory(query, route_payload)
        if plan.action == "auto_correlate":
            return await self._answer_auto_correlate(
                query,
                params["stat_x"],
                params["stat_y"],
                plan.region,
                route_payload,
            )
        if plan.action == "composite_aggregate" and direct_key:
            return await self._answer_composite_aggregate(
                query,
                direct_key,
                params["composite"],
                operation=params["operation"],
            )
        if plan.action == "region_sum" and direct_key:
            return await self._answer_region_sum(query, direct_key, params["regions"])
        if plan.action == "top_n" and direct_key:
            return await self._answer_top_n(
                query,
                direct_key,
                params["top_n"],
                include_share_ratio=params["include_share_ratio"],
            )
        if plan.action == "region_compare" and direct_key:
            return await self._answer_region_compare(query, direct_key)
        if plan.action == "share_ratio" and direct_key:
            return await self._answer_share_ratio(query, plan.region, direct_key)
        if plan.action == "direct" and direct_key:
            if not route_payload["route"].get("direct_stat_key"):
                route_payload["route"]["direct_stat_key"] = direct_key
            return await self._answer_direct(query, plan.region, direct_key, route_payload)
        return await self._answer_search_fallback(query, route_payload)

    @staticmethod
    def _period_age_years(period: str) -> Optional[float]:
        if not period:
            return None
        text = str(period)
        m = re.match(r"^(\d{4})(\d{2})?", text)
        if not m:
            return None
        now = datetime.now()
        year = int(m.group(1))
        if m.group(2):
            month = int(m.group(2))
            age = (now.year - year) + (now.month - month) / 12.0
        else:
            age = float(now.year - year)
        return round(age, 2)

    @classmethod
    def _extract_used_period(cls, result: dict[str, Any]) -> Optional[str]:
        comparison = result.get("비교") or {}
        end = (comparison.get("종료") or {}).get("시점")
        if end:
            return str(end)
        rows = result.get("표") or []
        for row in rows:
            if isinstance(row, dict):
                period = row.get("시점")
                if period:
                    return str(period)
        period = result.get("시점")
        if period:
            return str(period)
        return None

    _INTENT_FULFILLMENT: dict[str, frozenset[str]] = {
        "STAT_RANKING": frozenset({
            "tier_a_top_n", "tier_a_top_n_share_ratio",
            "tier_a_region_comparison", "tier_a_region_sum",
        }),
        "STAT_SHARE_RATIO": frozenset({
            "tier_a_share_ratio", "tier_a_composite_comparison",
            "tier_a_composite", "tier_a_composite_share_ratio",
            "tier_a_top_n_share_ratio",
        }),
        "STAT_GROWTH_RATE": frozenset({
            "tier_a_growth_rate", "tier_a_trend",
        }),
        "STAT_TIME_SERIES": frozenset({
            "tier_a_trend", "tier_a_growth_rate",
        }),
        "STAT_AVERAGE": frozenset({
            "tier_a_composite_calculation", "tier_a_composite",
        }),
        "STAT_COMPARISON": frozenset({
            "tier_a_region_comparison", "tier_a_composite_comparison",
            "tier_a_composite_share_ratio", "tier_a_top_n",
            "tier_a_top_n_share_ratio", "tier_a_region_sum",
        }),
    }

    _YEAR_MISMATCH_ANSWER_TYPES: frozenset[str] = frozenset({
        "tier_a_value", "tier_a_growth_rate", "tier_a_share_ratio",
    })

    _INTENT_DROPPED_DIMENSIONS: dict[str, tuple[str, ...]] = {
        "STAT_RANKING": ("ranking",),
        "STAT_SHARE_RATIO": ("share_ratio",),
        "STAT_GROWTH_RATE": ("growth_rate",),
        "STAT_TIME_SERIES": ("time_series",),
        "STAT_AVERAGE": ("average",),
        "STAT_COMPARISON": ("comparison",),
    }

    @classmethod
    def _intent_execution_warnings(
        cls,
        result: dict[str, Any],
        query: str,
        route_payload: Optional[dict[str, Any]],
    ) -> list[str]:
        if not route_payload:
            return []
        warnings: list[str] = []
        answer_type = str(result.get("답변유형") or "")
        intents = route_payload.get("intents") or []
        slots = route_payload.get("slots") or {}

        for intent_label, fulfilling_types in cls._INTENT_FULFILLMENT.items():
            if intent_label in intents and answer_type not in fulfilling_types:
                warnings.append(
                    f"의도 {intent_label} 감지됐으나 응답 유형은 {answer_type or '미지정'} — "
                    "단일값/요약에 그쳤을 수 있으니 시도별 비교나 비중 계산을 별도 요청 필요"
                )

        if cls._is_aggregation_question(query) and answer_type not in {
            "tier_a_region_sum", "tier_a_composite_share_ratio", "tier_a_top_n_share_ratio",
        }:
            warnings.append(
                f"합계/합산 키워드 감지됐으나 응답 유형은 {answer_type or '미지정'} — "
                "두 번째 지역을 인식하지 못해 단일 지역 값으로 폴백했을 수 있음"
            )

        comparison_targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if isinstance(comparison_targets, list) and comparison_targets and answer_type in {
            "tier_a_value", "tier_a_trend", "tier_a_growth_rate"
        }:
            warnings.append(
                f"비교 대상 {comparison_targets} 이(가) 감지됐으나 응답은 단일 대상 — "
                "비교/합산이 누락됐을 수 있음"
            )

        years_in_query = [y for y in re.findall(r"(19\d{2}|20\d{2})", query)]
        used_period = result.get("used_period")
        if (
            years_in_query
            and used_period
            and answer_type in cls._YEAR_MISMATCH_ANSWER_TYPES
        ):
            used_str = str(used_period)
            if not any(used_str.startswith(y) for y in years_in_query):
                warnings.append(
                    f"질문에 명시된 연도 {years_in_query} 와 사용 시점 {used_str} 불일치 — "
                    "요청 기간을 만족하지 못했을 수 있음"
                )

        direct_key = str((result.get("route") or {}).get("direct_stat_key") or "")
        q_compact = re.sub(r"\s+", "", query)
        asks_enterprises = bool(re.search(r"기업\s*수", query)) and "사업체" not in q_compact
        if asks_enterprises and "사업체수" in direct_key:
            warnings.append(
                "쿼리 어휘 '기업 수' → 매핑 통계 '사업체 수' (모집단 다름) — "
                "법인 단위 기업체 수와 사업체 수는 통계 작성 기준이 다릅니다"
            )
        asks_establishments = "사업체수" in q_compact or "사업체 수" in query
        if asks_establishments and "_기업수" in direct_key:
            warnings.append(
                "쿼리 어휘 '사업체 수' → 매핑 통계 '기업 수' (모집단 다름)"
            )
        return warnings

    @classmethod
    def _fulfillment_gap(
        cls,
        result: dict[str, Any],
        query: str,
        route_payload: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not route_payload:
            return None
        answer_type = str(result.get("답변유형") or "")
        intents = route_payload.get("intents") or []
        slots = route_payload.get("slots") or {}
        q_compact = re.sub(r"\s+", "", query)

        dropped: list[str] = []
        reasons: list[str] = []
        for intent_label, fulfilling_types in cls._INTENT_FULFILLMENT.items():
            if intent_label not in intents or answer_type in fulfilling_types:
                continue
            for dim in cls._INTENT_DROPPED_DIMENSIONS.get(intent_label, ()):
                dropped.append(dim)
            reasons.append(f"{intent_label} 의도가 {answer_type or '미지정'} 응답으로 완전히 충족되지 않음")

        comparison_targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if comparison_targets and answer_type in {"tier_a_value", "tier_a_trend", "tier_a_growth_rate", "tier_a_composite"}:
            dropped.append("comparison")
            reasons.append(f"비교 대상 {comparison_targets} 이(가) 단일 응답으로 축소됨")

        age_requested = (
            bool(re.search(r"\d+\s*[-~]\s*\d+\s*세", query))
            or any(term in q_compact for term in ("청년", "연령별", "연령", "나이"))
        )
        if age_requested and answer_type not in {"search_and_plan"}:
            dropped.append("age")
            reasons.append("연령/청년 조건을 만족하는 분류축 호출이 실행되지 않음")

        if cls._is_aggregation_question(query) and answer_type not in {
            "tier_a_region_sum", "tier_a_composite_share_ratio", "tier_a_top_n_share_ratio",
        }:
            dropped.append("aggregation")
            reasons.append("합계/합산 의도가 단일값 또는 부분 집계로 축소됨")

        dropped = list(dict.fromkeys(dropped))
        if not dropped:
            return None
        return {
            "이행_상태": "partial",
            "status": "partial",
            "누락_차원": dropped,
            "dropped_dimensions": dropped,
            "부분충족_사유": list(dict.fromkeys(reasons)),
        }

    @staticmethod
    def _pick_josa_eun_neun(word: str) -> str:
        """Return 은 or 는 based on final-syllable batchim. Falls back to
        는 for non-Hangul tails (English, digits, parentheses, …)."""
        if not word:
            return "는"
        last = word[-1]
        codepoint = ord(last)
        if 0xAC00 <= codepoint <= 0xD7A3:
            return "은" if (codepoint - 0xAC00) % 28 != 0 else "는"
        return "는"

    @classmethod
    def _polish_answer_text(cls, text: Optional[str]) -> Optional[str]:
        """Smooth common surface-level fragments in answer strings:
        - X은(는) → X은 or X는 by Korean batchim
        - YYYY.MM (raw monthly period) → YYYY년 M월
        - YYYY.QQ patterns left alone (quarter labels already humane)
        - Collapses 년년 / 월월 artifacts the substitutions can leave.
        - Appends human-readable unit conversion in parentheses after
          KOSIS canonical units (천명, 명, 개, 대, 건, 억원, 십억원, 천달러)."""
        if not text:
            return text
        def fix_josa(m: re.Match) -> str:
            word = m.group(1)
            return f"{word}{cls._pick_josa_eun_neun(word)}"
        text = re.sub(r"(\S+?)은\(는\)", fix_josa, text)
        text = re.sub(
            r"(?<!\d)(19\d{2}|20\d{2})\.(0[1-9]|1[0-2]|[1-9])(?!\d)",
            lambda m: f"{m.group(1)}년 {int(m.group(2))}월",
            text,
        )
        text = re.sub(r"년년", "년", text)
        text = re.sub(r"월월", "월", text)
        text = re.sub(
            r"(\d[\d,]*(?:\.\d+)?)\s*(천명|십억원|천달러|억원|명|개|대|건)(?!\s*\()",
            cls._append_humanized_unit,
            text,
        )
        return text

    @staticmethod
    def _humanize_value(value: float, unit: str) -> Optional[str]:
        """KOSIS canonical units → reader-friendly Korean expression.

        Returns None when no humanization is warranted (e.g. a 천명
        value below 10 thousand people just becomes more confusing in
        a different form)."""
        if unit == "천명":
            people = value * 1000
            if people >= 1e8:
                return f"약 {people / 1e8:.2f}억 명"
            if people >= 1e4:
                return f"약 {round(people / 1e4):,}만 명"
            return None
        if unit in {"명", "개", "대", "건"}:
            if value >= 1e8:
                return f"약 {value / 1e8:.2f}억 {unit}"
            if value >= 1e5:
                return f"약 {round(value / 1e4):,}만 {unit}"
            return None
        if unit == "억원":
            if value >= 1e4:
                jo = value / 1e4
                return f"약 {jo:,.2f}조원"
            return None
        if unit == "십억원":
            eok = value * 10
            if eok >= 1e4:
                return f"약 {eok / 1e4:,.2f}조원"
            if eok >= 1:
                return f"약 {eok:,.0f}억원"
            return None
        if unit == "천달러":
            usd = value * 1000
            if usd >= 1e12:
                return f"약 {usd / 1e12:.2f}조 달러"
            if usd >= 1e8:
                return f"약 {usd / 1e8:,.0f}억 달러"
            return None
        return None

    @classmethod
    def _append_humanized_unit(cls, m: re.Match) -> str:
        raw_value = m.group(1)
        unit = m.group(2)
        original = m.group(0)
        try:
            value = float(raw_value.replace(",", ""))
        except ValueError:
            return original
        humanized = cls._humanize_value(value, unit)
        if not humanized:
            return original
        return f"{original} ({humanized})"

    @classmethod
    def _finalize_response(
        cls,
        result: dict[str, Any],
        query: Optional[str] = None,
        route_payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(result, dict):
            return result

        if isinstance(result.get("answer"), str):
            result["answer"] = cls._polish_answer_text(result["answer"])

        if result.get("상태") != "executed":
            return result
        used = result.get("used_period") or cls._extract_used_period(result)
        notes = list(result.get("검증_주의") or [])
        if used:
            result["used_period"] = str(used)
            age = cls._period_age_years(used)
            if age is not None:
                result["period_age_years"] = age
                if age >= 1.0:
                    warn = f"사용 시점 {used} (약 {age:.1f}년 경과) — 최신 데이터가 아닐 수 있음"
                    if warn not in notes:
                        notes.append(warn)

        if query is not None:
            mismatch_warnings = cls._intent_execution_warnings(result, query, route_payload)
            for w in mismatch_warnings:
                if w not in notes:
                    notes.append(w)
            gap = cls._fulfillment_gap(result, query, route_payload)
            if gap:
                result.update(gap)

        if notes:
            result["검증_주의"] = notes
        return result


# ============================================================================
# SVG 차트 생성 (외부 라이브러리 없이)
# ============================================================================











# ============================================================================
# MCP 서버 — 도구 정의
# ============================================================================

mcp = FastMCP("kosis-analysis")


# ---- L1: Quick Layer ----

_QUICK_STAT_SUPPORTED_PARAMS = frozenset({"query", "region", "period", "api_key"})














@mcp.tool()
async def quick_stat(
    query: str, region: str = "전국", period: str = "latest",
    api_key: Optional[str] = None,
    extra_params: Optional[dict[str, Any]] = None,
    source_system: Optional[str] = None,
) -> dict:
    """[⚡] 자연어로 통계 단일값 즉시 조회.

    동작 순서:
      1. Tier A 정밀 매핑 발견 시 → 즉시 호출
      2. Tier B 라우팅 힌트 발견 시 → 추천 검색어로 KOSIS 검색
      3. 폴백: 원본 query로 KOSIS 검색

    Args:
        query: 통계 키워드 ("인구", "실업률", "중소기업 사업체수")
        region: 17개 시도명 (기본 "전국"). 영문·풀네임도 자동 정규화
                (Seoul, 서울특별시, 서울시 → 서울).
        period: "latest" 또는 "2023", "2023.03", "작년", "올해"

    extra_params: 빠른 단일값 도구가 직접 지원하지 않는 보조 조건.
        예: {"industry": "제조업"}. 실제 슬라이싱에는 사용하지 않고,
        응답의 `⚠️ 무시된_파라미터` 필드에 노출합니다.
    """
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return _unsupported_source_response("quick_stat", query, resolved_source)
    ignored_params = sorted((extra_params or {}).keys())
    result = await _quick_stat_core(query, region, period, api_key)
    result = _attach_ignored_params(result, ignored_params, "quick_stat")
    return _attach_shortcut_contract(result, tool="quick_stat", ignored_params=ignored_params)


async def _quick_stat_core(
    query: str, region: str = "전국", period: str = "latest",
    api_key: Optional[str] = None,
) -> dict:
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response("quick_stat", query=query, region=region, period=period)
        raise
    param = _lookup_quick(query)
    query_region = _extract_single_region_from_query(query)
    if region == "전국" and query_region:
        region = query_region
    query_year = _extract_single_year_from_query(query)
    if _is_latest_period_text(period) and query_year:
        period = query_year
    canonical = _canonical_region(region) or region

    # === Tier A 히트: 즉시 호출 ===
    if param:
        unsupported_dimensions = _quick_stat_unsupported_dimensions(query)
        if unsupported_dimensions:
            return _unsupported_quick_stat_response(
                query,
                param,
                unsupported_dimensions,
                canonical,
                period,
            )

        # broken 상태는 호출 시도조차 안 함 — 사용자에게 명확히 알리고 폴백
        if param.verification_status == "broken":
            hints = _routing_hints(query)
            return {
                "결과": f'⚠️ Tier A 매핑 "{query}"는 KOSIS에서 호출 실패 상태로 표시됨',
                "사유": param.note or "검증 실패",
                "권고": "Tier B 라우팅 결과로 폴백합니다.",
                "추천_검색어": hints[:5] if hints else [],
                "사용자_조치": (
                    f'KOSIS 사이트(kosis.kr)에서 "{param.tbl_nm}"을 직접 검색해 '
                    f'올바른 통계표 ID·항목 ID를 찾은 후 kosis_curation.py 수정 필요'
                ),
            }

        # 미검증 통계표면 경고 메시지 첨부 (broken 제외, needs_check만)
        verification_warning = None
        if param.verification_status != "verified":
            verification_warning = (
                f"⚠️ 이 통계표는 검증되지 않았거나 파라미터 보정이 필요합니다 "
                f"(status: {param.verification_status}). 사유: {param.note or '미상'}"
            )

        region_code = None
        if param.region_scheme:
            region_code = param.region_scheme.get(canonical)
            if not region_code:
                return {
                    "오류": f'지역 "{region}" 이 통계에서 미지원',
                    "정규화_지역": canonical if canonical != region else None,
                    "지원_지역": list(param.region_scheme.keys()),
                    "통계표": param.tbl_nm,
                }
        elif canonical != "전국":
            return {
                "오류": f'이 통계는 지역별 조회가 검증되지 않았습니다: "{region}"',
                "지원_지역": ["전국"],
                "통계표": param.tbl_nm,
                "권고": "지역별 값으로 포장하지 않도록 차단했습니다. search_kosis로 지역 분류가 있는 통계표를 먼저 확인하세요.",
            }
        region = canonical

        period_type = _default_period_type(param)
        effective_period = "latest" if _is_latest_period_text(period) else period
        latest_alias = str(period) if str(period) != "latest" and effective_period == "latest" else None
        range_start = _extract_open_start_year(effective_period)
        if effective_period != "latest" and range_start:
            years_hint = max(1, datetime.now().year - int(range_start) + 1)
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_RANGE_REQUESTED,
                "오류": f'기간 "{period}"은 단일 시점이 아니라 열린 범위 요청입니다.',
                "answer": (
                    f"'{period}'은 {range_start}년부터 최신까지의 시계열 요청으로 해석됩니다. "
                    "quick_stat은 단일값 도구이므로 최신값으로 대체하지 않고 중단했습니다."
                ),
                "통계표": param.tbl_nm,
                "요청_기간": period,
                "해석된_시작시점": range_start,
                "추천_도구_호출": [
                    f"quick_trend('{query}', region='{region}', years={years_hint})",
                    f"answer_query('{range_start}년부터 {query} 추이')",
                ],
            }
        start_period, end_period = _period_bounds(effective_period, period_type)
        precision_downgrade = _detect_precision_downgrade(effective_period, period_type)
        half_year_advisory = _detect_half_year_request(effective_period)
        if effective_period != "latest" and not start_period:
            return {
                "오류": f'기간 "{period}"을(를) 해석할 수 없습니다.',
                "통계표": param.tbl_nm,
                "지원_기간유형": period_type,
            }
        async with httpx.AsyncClient() as client:
            try:
                data = await _fetch_series(
                    client, key, param, region_code,
                    period_type=period_type,
                    start_year=start_period, end_year=end_period,
                    latest_n=1 if not start_period else None,
                )
            except RuntimeError as e:
                return {"오류": str(e), "통계표": param.tbl_nm, "권고": verification_warning}

        if not data:
            relative_hint = None
            normalized = re.sub(r"\s+", "", str(period or ""))
            if any(term in normalized for term in ("작년", "지난해", "전년", "재작년", "올해", "금년")):
                resolved_year = _parse_year_token(period)
                if resolved_year:
                    relative_hint = (
                        f"'{period}' → {resolved_year}년으로 해석했지만 이 통계표의 수록 시점에 "
                        f"포함되지 않음. 최신값을 원하면 period='latest'를 사용하세요."
                    )
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "결과": "데이터 없음",
                "answer": (
                    f"요청한 기간 '{period}'의 {param.description} 데이터가 KOSIS 통계표에 없습니다. "
                    "최신값으로 대체하지 않고 중단했습니다."
                ),
                "통계표": param.tbl_nm,
                "요청_기간": period,
                "해석된_기간": [start_period, end_period],
                "권고": verification_warning,
                "⚠️ 정밀도_다운그레이드": precision_downgrade,
                "⚠️ 기간_해석": relative_hint,
            }

        data.sort(key=lambda r: str(r.get("PRD_DE") or ""))
        row = data[-1]
        period_label = _format_period_label(row.get("PRD_DE"), period_type)
        used_period = str(row.get("PRD_DE") or "")
        age = NaturalLanguageAnswerEngine._period_age_years(used_period)
        answer_text = NaturalLanguageAnswerEngine._polish_answer_text(
            f"{period_label} {region}의 {param.description}은(는) "
            f"{_format_display_number(row.get('DT'), param.display_decimals)} {param.unit}입니다."
        )
        result = {
            "answer": answer_text,
            "값": row.get("DT"), "단위": param.unit,
            "시점": row.get("PRD_DE"),
            "used_period": used_period,
            "period_age_years": age,
            "지역": region, "통계표": param.tbl_nm,
            "출처": "통계청 KOSIS",
        }
        if age is not None and age >= 1.0:
            result["⚠️ 데이터_신선도"] = (
                f"사용 시점 {used_period} (약 {age:.1f}년 경과) — 최신 데이터가 아닐 수 있음. "
                f"수록기간 메타는 explore_table('{param.org_id}', '{param.tbl_id}')로 확인하세요."
            )
        if verification_warning:
            result["⚠️ 검증_상태"] = verification_warning
        if precision_downgrade:
            result["⚠️ 정밀도_다운그레이드"] = precision_downgrade
        if half_year_advisory:
            result["⚠️ 상하반기"] = half_year_advisory
        if latest_alias:
            result["⚠️ 기간_해석"] = f"'{latest_alias}' → latest로 해석"
        # Population-mismatch warning previously fired only through
        # answer_query's _finalize_response. Direct quick_stat callers
        # were getting silent label substitution — close that gap so
        # the trail is consistent across both entry points.
        q_compact = re.sub(r"\s+", "", str(query))
        asks_enterprises = bool(re.search(r"기업\s*수", query)) and "사업체" not in q_compact
        param_describes_sites = "사업체" in (param.description or "")
        if asks_enterprises and param_describes_sites:
            result["⚠️ 모집단_불일치"] = (
                "쿼리 어휘 '기업 수' → 매핑 통계 '사업체 수' (모집단 다름) — "
                "법인 단위 기업체 수와 사업체 수는 통계 작성 기준이 다릅니다"
            )
        return result

    # === Tier B 라우팅: 추천 검색어로 폴백 ===
    hints = _routing_hints(query)
    search_keywords = hints[:3] if hints else [query]

    async with httpx.AsyncClient() as client:
        all_results = []
        for keyword in search_keywords:
            try:
                r = await _kosis_call(client, "statisticsSearch.do", {
                    "method": "getList", "apiKey": key,
                    "searchNm": keyword, "format": "json", "jsonVD": "Y", "resultCount": 3,
                })
                for item in r:
                    item["_검색어"] = keyword
                all_results.extend(r)
            except RuntimeError:
                continue

    # 중복 제거 (통계표 ID 기준)
    seen = set()
    unique = []
    for item in all_results:
        tid = item.get("TBL_ID")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(item)

    return {
        "결과": "Tier A 정밀 매핑 없음. 검색 결과 반환.",
        "사용된_검색어": search_keywords,
        "검색_후보": [
            {
                "통계표": r.get("TBL_NM"),
                "통계표ID": r.get("TBL_ID"),
                "기관ID": r.get("ORG_ID"),
                "검색어": r.get("_검색어"),
                "URL": r.get("LINK_URL") or r.get("TBL_VIEW_URL"),
            }
            for r in unique[:8]
        ],
        "안내": (
            "후보 중 적합한 통계표를 골라서 KOSIS 사이트에서 확인하거나, "
            "더 구체적인 키워드로 다시 시도하세요."
        ),
    }


@mcp.tool()
async def quick_trend(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
    extra_params: Optional[dict[str, Any]] = None,
    source_system: Optional[str] = None,
) -> dict:
    """[⚡] 시계열 데이터 조회 (분석/시각화 입력으로 사용).

    Args:
        query: 통계 키워드
        region: 지역 (영문·풀네임 자동 정규화)
        years: 최근 N년 (기본 10)

    extra_params는 quick_stat과 동일하게 실제 슬라이싱에 사용하지 않고
    응답의 `⚠️ 무시된_파라미터` 필드에 노출합니다.
    """
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return _unsupported_source_response("quick_trend", query, resolved_source)
    ignored_params = sorted((extra_params or {}).keys())
    result = await _quick_trend_core(query, region, years, api_key)
    return _attach_ignored_params(result, ignored_params, "quick_trend")


async def _quick_trend_core(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
    start_year: Optional[str] = None,
    end_year: Optional[str] = None,
) -> dict:
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response("quick_trend", query=query, region=region, years=years)
        raise
    param = _lookup_quick(query)
    if not param:
        return {"오류": f'"{query}" 사전 매핑 없음'}
    canonical = _canonical_region(region) or region

    region_code = None
    if param.region_scheme:
        region_code = param.region_scheme.get(canonical)
        if not region_code:
            return {"오류": f'지역 "{region}" 미지원', "지원_지역": list(param.region_scheme.keys())}
    elif canonical != "전국":
        return {"오류": f'"{query}"는 지역별 시계열 조회가 검증되지 않았습니다.', "지원_지역": ["전국"]}
    region = canonical

    period_type = _default_period_type(param)
    latest_count = _latest_count_for_years(years, period_type)
    start_period = end_period = None
    if start_year:
        start_period, _ = _period_bounds(start_year, period_type)
        _, end_period = _period_bounds(end_year or str(datetime.now().year), period_type)

    async with httpx.AsyncClient() as client:
        data = await _fetch_series(
            client,
            key,
            param,
            region_code,
            period_type=period_type,
            start_year=start_period,
            end_year=end_period,
            latest_n=None if start_period else latest_count,
        )

    data.sort(key=lambda r: str(r.get("PRD_DE") or ""))
    series = [{"시점": r.get("PRD_DE"), "값": r.get("DT")} for r in data]
    used_period = str(series[-1]["시점"]) if series else ""
    age = NaturalLanguageAnswerEngine._period_age_years(used_period)
    result = {
        "통계명": param.description, "지역": region, "단위": param.unit,
        "시계열": series,
        "데이터수": len(data), "통계표": param.tbl_nm,
        "used_period": used_period,
        "period_age_years": age,
        "수록주기": period_type,
        "요청_기간_년": years,
        "요청_시점수": latest_count if not start_period else None,
    }
    if start_period:
        result["요청_시작시점"] = start_period
        result["요청_종료시점"] = end_period
    elif period_type in {"M", "Q"}:
        unit_label = "개월" if period_type == "M" else "분기"
        result["⚠️ 기간_해석"] = (
            f"years={years} → {period_type} 주기 통계이므로 최근 {latest_count}개 {unit_label} 시점 조회"
        )
    if age is not None and age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"시계열 최신 시점 {used_period} (약 {age:.1f}년 경과) — "
            "최신 데이터가 아닐 수 있음"
        )
    return result


@mcp.tool()
async def quick_region_compare(
    query: str,
    period: str = "latest",
    sort: str = "desc",
    api_key: Optional[str] = None,
    extra_params: Optional[dict[str, Any]] = None,
) -> dict:
    """[⚡] 지역/시도별 값을 한 번에 비교.

    지역 분류가 검증된 Tier A 통계만 지원합니다. 예:
    "중소기업 사업체수", "소상공인 사업체수", "실업률".
    """
    ignored_params = sorted((extra_params or {}).keys())
    result = await _quick_region_compare_core(query, period, sort, api_key)
    return _attach_ignored_params(result, ignored_params, "quick_region_compare")


async def _quick_region_compare_core(
    query: str, period: str = "latest", sort: str = "desc",
    api_key: Optional[str] = None,
) -> dict:
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response("quick_region_compare", query=query, period=period, sort=sort)
        raise
    param = _lookup_quick(query)
    if not param:
        return {"오류": f'"{query}" 사전 매핑 없음'}
    if not param.region_scheme:
        return {
            "오류": f'"{query}"는 지역별 분류가 검증되지 않았습니다.',
            "통계표": param.tbl_nm,
            "권고": "search_kosis로 지역 분류가 있는 통계표를 먼저 확인하세요.",
        }

    period_type = _default_period_type(param)
    effective_period = "latest" if _is_latest_period_text(period) else period
    start_period, end_period = _period_bounds(effective_period, period_type)
    if effective_period != "latest" and not start_period:
        return {
            "오류": f'기간 "{period}"을(를) 해석할 수 없습니다.',
            "통계표": param.tbl_nm,
            "지원_기간유형": period_type,
        }
    async with httpx.AsyncClient() as client:
        try:
            data = await _fetch_series(
                client,
                key,
                param,
                "ALL",
                period_type=period_type,
                start_year=start_period,
                end_year=end_period,
                latest_n=1 if not start_period else None,
            )
        except RuntimeError as e:
            return {"오류": str(e), "통계표": param.tbl_nm}

    if data:
        latest_period = max(str(row.get("PRD_DE") or "") for row in data)
        data = [row for row in data if str(row.get("PRD_DE") or "") == latest_period]

    code_field, name_field = _region_field_names(param)
    regions_by_code = {code: name for name, code in param.region_scheme.items()}
    allowed_codes = set(regions_by_code)
    rows = []
    for row in data:
        code = row.get(code_field)
        if code not in allowed_codes:
            continue
        region_name = regions_by_code.get(code) or row.get(name_field)
        if not region_name or region_name == "전국":
            continue
        try:
            value = float(str(row.get("DT")).replace(",", ""))
        except (TypeError, ValueError):
            continue
        rows.append({
            "지역": region_name,
            "값": _format_number(value),
            "원값": value,
            "단위": param.unit,
            "시점": row.get("PRD_DE"),
            "통계표": param.tbl_nm,
        })

    reverse = sort != "asc"
    rows.sort(key=lambda r: r["원값"], reverse=reverse)
    latest_period = rows[0]["시점"] if rows else None
    used_period = str(latest_period or "")
    age = NaturalLanguageAnswerEngine._period_age_years(used_period)
    result = {
        "통계명": param.description,
        "시점": latest_period,
        "used_period": used_period,
        "period_age_years": age,
        "단위": param.unit,
        "정렬": "내림차순" if reverse else "오름차순",
        "지역수": len(rows),
        "표": rows,
        "출처": "통계청 KOSIS",
    }
    if age is not None and age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"비교 기준 시점 {used_period} (약 {age:.1f}년 경과) — "
            "최신 데이터가 아닐 수 있음"
        )
    return result


@mcp.tool()
async def daily_term_lookup(daily_term: str) -> dict:
    """[📖] 일상용어/도메인 키워드를 통계 검색어로 변환.

    Tier B 라우팅 사전 (100+ 항목): 중소기업, 소상공인, 업종, 일상어 등.

    예:
      "월세" → ["주택 임대료", "전월세전환율", "주거비"]
      "치킨집" → ["음식점업", "분식 및 김밥 전문점"]
      "BSI" → ["중소기업 경기실사지수", "기업경기실사지수"]
    """
    daily_term = daily_term.strip()

    # 먼저 Tier A 직접 매핑 있는지 확인
    tier_a = _curation_lookup(daily_term)
    if tier_a:
        return {
            "입력": daily_term,
            "정밀_매핑": {
                "통계표": tier_a.tbl_nm,
                "설명": tier_a.description,
                "검증상태": tier_a.verification_status,
            },
            "안내": "Tier A 직접 매핑 발견. quick_stat 또는 quick_trend로 바로 호출 가능.",
        }

    # Tier B 라우팅
    hints = _routing_hints(daily_term)
    return {
        "입력": daily_term,
        "추천_검색어": hints or [daily_term],
        "매핑여부": bool(hints),
        "안내": (
            f'"{daily_term}"에 대한 {len(hints)}개 검색어 제안. '
            f'각 검색어를 search_kosis 또는 quick_stat에 넘겨 시도하세요.'
        ) if hints else f'"{daily_term}" 매핑 없음. KOSIS 통합검색으로 폴백 권장.',
    }


@mcp.tool()
async def browse_topic(topic: Optional[str] = None) -> dict:
    """[📖] 주제별 대표 통계 둘러보기.

    13개 주제: 인구·가구, 고용·노동, 물가·소비, 주거·부동산, 경제·성장,
    중소기업·소상공인, 업종·산업, 금융·재정, 복지·소득, 교육, 보건·의료,
    환경·기후, 지역.
    """
    if not topic:
        return {
            "전체_주제": list(TOPICS.keys()),
            "안내": "각 주제명을 다시 넘겨서 대표 통계 목록을 받으세요.",
        }
    hints = _topic_hints(topic)
    if not hints:
        return {"오류": f'주제 "{topic}" 없음', "가능_주제": list(TOPICS.keys())}
    return {
        "주제": topic,
        "대표_통계": hints,
        "안내": "각 통계명을 search_kosis 또는 quick_stat에 넘겨 호출하세요.",
    }


# ---- L2: Analysis Layer ----

@mcp.tool()
async def analyze_trend(
    query: str, region: str = "전국", years: int = 20,
    api_key: Optional[str] = None,
    method: str = "linear",
    include_interpretation: bool = False,
    input_rows: Optional[list[dict[str, Any]]] = None,
    source_system: Optional[str] = None,
) -> dict:
    """[📊] 통계적 추세 재료와 재현 가능한 계산값을 반환.

    제공:
      - 원자료 배열(data/x/y)
      - 선형회귀 계수, fitted_values, residuals, formula
      - 극값, 최근 변화
    """
    method_requested = str(method or "linear").lower().strip()
    valid_methods = ["linear"]
    if method_requested not in valid_methods:
        return {
            "status": "invalid_input",
            "오류": f"지원하지 않는 trend method: {method}",
            "method_requested": method,
            "valid_methods": valid_methods,
        }
    if input_rows is not None:
        row_materials = _input_rows_series_materials(input_rows)
        times = row_materials["times"]
        values = row_materials["values"]
        series_result = {
            "status": "executed",
            "?듦퀎紐?": query,
            "?⑥쐞": row_materials.get("unit"),
            "source_systems": row_materials.get("source_systems"),
            "table_ids": row_materials.get("table_ids"),
        }
    else:
        resolved_source = _resolve_tool_source_system(query, source_system)
        if resolved_source and resolved_source != "KOSIS":
            return _unsupported_source_response("analyze_trend", query, resolved_source)
        series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result.get("시계열", []))
    if input_rows is not None:
        times = row_materials["times"]
        values = row_materials["values"]
    if len(values) < 3:
        return {"오류": "분석에 충분한 데이터 없음 (3개 미만)"}

    x = np.arange(len(values))
    y = np.array(values)
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, y)
    r2 = r_value ** 2
    fitted = slope * x + intercept
    residuals = y - fitted

    changes = np.diff(y) / np.abs(y[:-1]) * 100
    avg_growth = float(np.mean(changes))
    volatility = float(np.std(changes))

    if p_value < 0.05:
        trend_label = "유의한 증가 추세" if slope > 0 else "유의한 감소 추세"
    elif volatility > 20:
        trend_label = "변동성 큼 (뚜렷한 추세 없음)"
    else:
        trend_label = "안정 (유의한 추세 없음)"

    max_idx = int(np.argmax(y))
    min_idx = int(np.argmin(y))
    recent_change = (
        float((y[-1] - y[-2]) / abs(y[-2]) * 100) if len(y) >= 2 and y[-2] != 0 else 0.0
    )

    result = {
        "status": "executed",
        "method": method_requested,
        "통계명": series_result.get("통계명"), "지역": region,
        "기간": f"{times[0]} ~ {times[-1]}",
        "데이터수": len(values),
        "must_know": _analysis_must_know(series_result, times, values),
        "input": _analysis_input_materials(times, values),
        "data_characteristics": _series_characteristics(times, values, unit=series_result.get("단위")),
        "common_pitfalls": _analysis_common_pitfalls(series_result, values),
        "analysis_materials": {
            "available_methods": valid_methods,
            "interpretation_included": include_interpretation,
            "caller_can_recompute": True,
            "input_rows_supported": True,
        },
        "input_row_profile": row_materials if input_rows is not None else None,
        "선형회귀": {
            "기울기_연간": round(slope, 4), "R제곱": round(r2, 4),
            "p_value": round(p_value, 4), "유의": p_value < 0.05,
            "slope": round(float(slope), 8),
            "intercept": round(float(intercept), 8),
            "std_err": round(float(std_err), 8),
        },
        "model": "linear_regression",
        "model_parameters": {
            "slope": round(float(slope), 8),
            "intercept": round(float(intercept), 8),
            "r_value": round(float(r_value), 8),
            "r_squared": round(float(r2), 8),
            "p_value": round(float(p_value), 8),
            "std_err": round(float(std_err), 8),
        },
        "formula": f"y = {float(slope):.12g} * x + {float(intercept):.12g}",
        "fitted_values": [round(float(v), 8) for v in fitted],
        "residuals": [round(float(v), 8) for v in residuals],
        "변화율": {
            "평균_퍼센트": round(avg_growth, 2),
            "변동성_퍼센트": round(volatility, 2),
            "최근_퍼센트": round(recent_change, 2),
            "최근_구간": {
                "시작": times[-2],
                "끝": times[-1],
            },
        },
        "극값": {
            "최댓값": {"시점": times[max_idx], "값": values[max_idx]},
            "최솟값": {"시점": times[min_idx], "값": values[min_idx]},
        },
        "단위": series_result.get("단위"),
    }
    if include_interpretation:
        result["추세_라벨"] = trend_label
        result["해석"] = (
            f"{years}년간 {trend_label}. "
            f"평균 연 {avg_growth:+.2f}% 변화, "
            f"회귀 R²={r2:.2f} (p={p_value:.3f})."
        )
    return result


@mcp.tool()
async def correlate_stats(
    query_a: str, query_b: str,
    region: str = "전국", years: int = 15,
    api_key: Optional[str] = None,
    include_interpretation: bool = False,
) -> dict:
    """[📊] 두 통계의 상관계수와 재현 가능한 정합 데이터를 반환."""
    a = await quick_trend(query_a, region, years, api_key)
    b = await quick_trend(query_b, region, years, api_key)
    if "오류" in a or "오류" in b:
        return {"오류": "데이터 수집 실패"}

    ta, va = _values_from_series(a["시계열"])
    tb, vb = _values_from_series(b["시계열"])
    common = sorted(set(ta) & set(tb))
    if len(common) < 4:
        return {"오류": f"공통 시점 부족 ({len(common)}개)"}

    da = {t: v for t, v in zip(ta, va)}
    db = {t: v for t, v in zip(tb, vb)}
    aa = [da[t] for t in common]
    bb = [db[t] for t in common]

    pr, pp = scipy_stats.pearsonr(aa, bb)
    sr, sp = scipy_stats.spearmanr(aa, bb)

    kr, kp = scipy_stats.kendalltau(aa, bb)

    def interpret(r: float) -> str:
        absr = abs(r)
        if absr < 0.2:
            s = "거의 무관"
        elif absr < 0.4:
            s = "약한 상관"
        elif absr < 0.7:
            s = "중간 상관"
        else:
            s = "강한 상관"
        return f"{s} ({'양의' if r > 0 else '음의'})"

    result = {
        "status": "executed",
        "통계_A": a.get("통계명"), "통계_B": b.get("통계명"),
        "지역": region, "공통_시점수": len(common),
        "기간": f"{common[0]} ~ {common[-1]}",
        "must_know": {
            "source": "KOSIS",
            "period_range": [common[0], common[-1]],
            "common_period_count": len(common),
            "correlation_is_not_causation": True,
            "limitations": [
                "Correlation coefficients are descriptive materials, not causal evidence.",
                "Definitions, units, and collection methods should be checked before comparing indicators.",
            ],
        },
        "input": {
            "common_periods": common,
            "series_a": [{"period": t, "value": da[t]} for t in common],
            "series_b": [{"period": t, "value": db[t]} for t in common],
            "x": aa,
            "y": bb,
        },
        "analysis_materials": {
            "available_methods": ["pearson", "spearman", "kendall", "lag_correlation_from_input"],
            "caller_can_recompute": True,
            "interpretation_included": include_interpretation,
        },
        "correlations": {
            "pearson": {"coefficient": round(float(pr), 8), "p_value": round(float(pp), 8)},
            "spearman": {"coefficient": round(float(sr), 8), "p_value": round(float(sp), 8)},
            "kendall": {"coefficient": round(float(kr), 8), "p_value": round(float(kp), 8)},
        },
        "Pearson": {
            "상관계수": round(float(pr), 4), "p_value": round(float(pp), 4),
        },
        "Spearman": {
            "상관계수": round(float(sr), 4), "p_value": round(float(sp), 4),
        },
        "Kendall": {
            "상관계수": round(float(kr), 4), "p_value": round(float(kp), 4),
        },
        "정합데이터": list(zip(common, aa, bb)),
        "common_pitfalls": [
            {
                "pitfall": "Correlation is not causation.",
                "example_wrong": "Saying indicator A caused indicator B because r is high.",
                "example_right": "Saying the two series moved together over the overlapping periods.",
            }
        ],
    }
    if include_interpretation:
        result["Pearson"]["해석"] = interpret(pr)
        result["Spearman"]["해석"] = interpret(sr)
        result["Kendall"]["해석"] = interpret(kr)
    return result


@mcp.tool()
async def forecast_stat(
    query: str, region: str = "전국",
    history_years: int = 15, horizon: int = 5,
    api_key: Optional[str] = None,
    model: str = "linear",
    include_legacy_forecast: bool = False,
) -> dict:
    """[📊] 예측에 필요한 원자료, 모델 옵션, 재현 가능한 계산 예시를 반환.

    Args:
        history_years: 과거 데이터 기간
        horizon: 미래 예측 기간 (년)
        model: 현재 계산 예시는 linear만 지원. 다른 모델은 option metadata로 제공.
    """
    model_requested = str(model or "linear").lower().strip()
    valid_models = ["linear", "materials_only"]
    if model_requested not in valid_models:
        return {
            "status": "invalid_input",
            "오류": f"지원하지 않는 forecast model: {model}",
            "model_requested": model,
            "valid_models": valid_models,
        }
    series_result = await quick_trend(query, region, history_years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result["시계열"])
    if len(values) < 4:
        return {"오류": "예측에 데이터 부족"}

    x = np.arange(len(values))
    y = np.array(values)
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, y)
    residuals = y - (slope * x + intercept)
    rmse = float(np.sqrt(np.mean(residuals ** 2)))

    last_year = int(times[-1][:4])
    future_x = np.arange(len(values), len(values) + horizon)
    future_y = slope * future_x + intercept
    ci = 1.96 * rmse

    forecast_path = []
    for i, yr in enumerate(range(last_year + 1, last_year + 1 + horizon)):
        forecast_path.append({
            "period": str(yr),
            "value": round(float(future_y[i]), 8),
            "lower": round(float(future_y[i] - ci), 8),
            "upper": round(float(future_y[i] + ci), 8),
            "interval_method": "linear_example_rmse_1.96",
        })

    characteristics = _series_characteristics(times, values, unit=series_result.get("단위"))
    model_options = [
        {
            "model": "linear",
            "computed": model_requested == "linear",
            "suitable_for": "Short exploratory extrapolation when the observed trend is approximately linear.",
            "caveats": ["Can produce impossible values outside the observed range, especially over long horizons."],
        },
        {
            "model": "logistic_or_bounded_curve",
            "computed": False,
            "suitable_for": "Series with a natural lower or upper bound.",
            "caveats": ["Requires caller-chosen bound/asymptote; MCP does not infer domain bounds."],
        },
        {
            "model": "exponential_smoothing_or_arima",
            "computed": False,
            "suitable_for": "Longer or seasonal time series.",
            "caveats": ["Needs enough observations and cadence-specific validation."],
        },
    ]

    result = {
        "status": "materials_ready",
        "통계명": series_result.get("통계명"), "지역": region,
        "과거_기간": f"{times[0]} ~ {times[-1]}",
        "must_know": _analysis_must_know(
            series_result,
            times,
            values,
            extra_limitations=[
                "Forecast paths are computed examples, not final predictions.",
                "Policy shocks, definition changes, and external variables are not modeled.",
            ],
        ),
        "input": _analysis_input_materials(times, values),
        "data_characteristics": characteristics,
        "common_pitfalls": _analysis_common_pitfalls(series_result, values) + [
            {
                "pitfall": "Linear extrapolation may cross impossible natural bounds.",
                "example_wrong": "Treating a negative long-run forecast as meaningful for a non-negative indicator.",
                "example_right": "Use the linear example as a short-run reference or choose a bounded model.",
            }
        ],
        "model_options": model_options,
        "computed_examples": {
            "linear": {
                "formula": f"y = {float(slope):.12g} * x + {float(intercept):.12g}",
                "parameters": {
                    "slope": round(float(slope), 8),
                    "intercept": round(float(intercept), 8),
                    "r_value": round(float(r_value), 8),
                    "r_squared": round(float(r_value ** 2), 8),
                    "p_value": round(float(p_value), 8),
                    "std_err": round(float(std_err), 8),
                    "rmse": round(rmse, 8),
                },
                "residuals": [round(float(v), 8) for v in residuals],
                "forecast_path": forecast_path if model_requested == "linear" else [],
            }
        },
        "analysis_materials": {
            "caller_can_recompute": True,
            "available_models": valid_models,
            "requested_model": model_requested,
            "legacy_forecast_included": include_legacy_forecast,
        },
        "단위": series_result.get("단위"),
    }
    if include_legacy_forecast:
        result["예측"] = [
            {
                "시점": row["period"],
                "예측값": round(row["value"], 2),
                "하한": round(row["lower"], 2),
                "상한": round(row["upper"], 2),
            }
            for row in forecast_path
        ]
        result["모델"] = "linear_regression_example"
        result["RMSE"] = round(rmse, 4)
        result["R제곱"] = round(r_value ** 2, 4)
    return result


@mcp.tool()
async def detect_outliers(
    query: str, region: str = "전국", years: int = 20,
    api_key: Optional[str] = None,
    method: str = "detrended_zscore",
    threshold: float = 2.5,
    iqr_multiplier: float = 1.5,
) -> dict:
    """[📊] 시계열 이상치 탐지.

    method:
    - detrended_zscore (default): 선형 추세 제거 후 잔차 Z-score
    - zscore: 원값 Z-score
    - iqr: 원값 IQR fence
    - stl: statsmodels가 설치된 경우 STL residual Z-score
    - all: 지원 method를 함께 실행
    """
    series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result["시계열"])
    if len(values) < 5:
        return {
            "status": "invalid_input",
            "오류": "탐지에 데이터 부족",
            "error": "At least 5 observations are required for outlier detection.",
            "observation_count": len(values),
            "valid_methods": ["detrended_zscore", "zscore", "iqr", "stl", "all"],
        }

    y = np.array(values, dtype=float)
    method_requested = str(method or "detrended_zscore").lower().strip()
    valid_methods = ["detrended_zscore", "zscore", "iqr", "stl", "all"]
    if method_requested not in valid_methods:
        return {
            "status": "invalid_input",
            "오류": f"알 수 없는 이상치 탐지 method: {method}",
            "error": f"Unknown outlier detection method: {method}",
            "method_requested": method,
            "valid_methods": valid_methods,
        }

    def _zscore_result(
        *,
        method_name: str,
        score_values: np.ndarray,
        baseline: Optional[np.ndarray] = None,
        notes: Optional[list[str]] = None,
        extra: Optional[dict[str, Any]] = None,
        scale_method: str = "std",
    ) -> dict[str, Any]:
        center = float(np.mean(score_values))
        scale = float(np.std(score_values))
        scale_method_used = "std"
        if scale_method == "mad":
            robust_center = float(np.median(score_values))
            mad = float(np.median(np.abs(score_values - robust_center)))
            robust_scale = 1.4826 * mad
            if robust_scale > 0:
                center = robust_center
                scale = robust_scale
                scale_method_used = "mad"
        if scale == 0:
            return {
                "status": "executed",
                "method": method_name,
                "outliers": [],
                "이상치": [],
                "center": round(center, 6),
                "scale": round(scale, 6),
                "scale_method": scale_method_used,
                "threshold": threshold,
                "notes": (notes or []) + ["No variation in the scoring series."],
                **(extra or {}),
            }
        z_scores = (score_values - center) / scale
        outliers = []
        for i, z in enumerate(z_scores):
            if abs(float(z)) <= threshold:
                continue
            row = {
                "period": times[i],
                "시점": times[i],
                "value": values[i],
                "값": values[i],
                "z_score": round(float(z), 4),
                "threshold": threshold,
            }
            if baseline is not None:
                row["expected_value"] = round(float(baseline[i]), 6)
                row["residual"] = round(float(score_values[i]), 6)
            outliers.append(row)
        return {
            "status": "executed",
            "method": method_name,
            "outliers": outliers,
            "이상치": outliers,
            "center": round(center, 6),
            "scale": round(scale, 6),
            "scale_method": scale_method_used,
            "threshold": threshold,
            "notes": notes or [],
            **(extra or {}),
        }

    def _raw_zscore() -> dict[str, Any]:
        return _zscore_result(
            method_name="zscore",
            score_values=y,
            notes=["Uses raw values. For strong trends, detrended_zscore is usually safer."],
        )

    def _detrended_zscore() -> dict[str, Any]:
        x = np.arange(len(y), dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        trend = slope * x + intercept
        residuals = y - trend
        return _zscore_result(
            method_name="detrended_zscore",
            score_values=residuals,
            baseline=trend,
            notes=["Uses residuals after removing a fitted linear trend; scores use robust MAD scaling when possible."],
            extra={
                "trend_model": "linear",
                "trend_slope_per_period": round(float(slope), 6),
                "trend_intercept": round(float(intercept), 6),
            },
            scale_method="mad",
        )

    def _iqr() -> dict[str, Any]:
        q1, q3 = np.percentile(y, [25, 75])
        iqr = float(q3 - q1)
        lower = float(q1 - iqr_multiplier * iqr)
        upper = float(q3 + iqr_multiplier * iqr)
        if iqr == 0:
            outliers: list[dict[str, Any]] = []
        else:
            outliers = [
                {
                    "period": times[i],
                    "시점": times[i],
                    "value": values[i],
                    "값": values[i],
                    "lower_fence": round(lower, 6),
                    "upper_fence": round(upper, 6),
                }
                for i, value in enumerate(values)
                if float(value) < lower or float(value) > upper
            ]
        return {
            "status": "executed",
            "method": "iqr",
            "outliers": outliers,
            "이상치": outliers,
            "q1": round(float(q1), 6),
            "q3": round(float(q3), 6),
            "iqr": round(iqr, 6),
            "iqr_multiplier": iqr_multiplier,
            "lower_fence": round(lower, 6),
            "upper_fence": round(upper, 6),
            "notes": ["Uses raw value IQR fences; trend removal is not applied."],
        }

    def _stl() -> dict[str, Any]:
        try:
            from statsmodels.tsa.seasonal import STL  # type: ignore
        except Exception:
            return {
                "status": "unsupported",
                "method": "stl",
                "outliers": [],
                "이상치": [],
                "notes": ["STL requires optional dependency statsmodels; use detrended_zscore or iqr."],
            }
        period = 12 if len(y) >= 24 else max(2, min(7, len(y) // 2))
        fitted = STL(y, period=period, robust=True).fit()
        residuals = np.asarray(fitted.resid, dtype=float)
        baseline = y - residuals
        return _zscore_result(
            method_name="stl",
            score_values=residuals,
            baseline=baseline,
            notes=[f"Uses STL residuals with period={period}."],
            extra={"stl_period": period},
        )

    method_map = {
        "detrended_zscore": _detrended_zscore,
        "zscore": _raw_zscore,
        "iqr": _iqr,
        "stl": _stl,
    }
    if method_requested == "all":
        method_results = {name: fn() for name, fn in method_map.items()}
        primary = method_results["detrended_zscore"]
        return {
            **primary,
            "method": "all",
            "primary_method": "detrended_zscore",
            "method_results": method_results,
            "통계명": series_result.get("통계명"),
            "stat_name": series_result.get("통계명"),
            "region": region,
            "must_know": _analysis_must_know(series_result, times, values),
            "input": _analysis_input_materials(times, values),
            "data_characteristics": _series_characteristics(times, values, unit=series_result.get("단위")),
            "common_pitfalls": _analysis_common_pitfalls(series_result, values),
            "observation_count": len(values),
            "period_range": [times[0], times[-1]],
            "valid_methods": valid_methods,
        }

    result = method_map[method_requested]()
    return {
        **result,
        "통계명": series_result.get("통계명"),
        "stat_name": series_result.get("통계명"),
        "region": region,
        "must_know": _analysis_must_know(series_result, times, values),
        "input": _analysis_input_materials(times, values),
        "data_characteristics": _series_characteristics(times, values, unit=series_result.get("단위")),
        "common_pitfalls": _analysis_common_pitfalls(series_result, values),
        "observation_count": len(values),
        "period_range": [times[0], times[-1]],
        "valid_methods": valid_methods,
        "method_requested": method,
    }


# ---- L3: Viz Layer ----

@mcp.tool()
async def chart_line(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
    input_rows: Optional[list[dict[str, Any]]] = None,
) -> list:
    """[🎨] 시계열 라인 차트 SVG (챗봇에 인라인 렌더링)."""
    if input_rows is not None:
        materials = _input_rows_series_materials(input_rows)
        times = materials["times"]
        values = materials["values"]
        if not times:
            return [TextContent(type="text", text=str({
                "status": "invalid_input",
                "error": "input_rows contained no numeric period/value pairs.",
                "input_row_profile": materials,
            }))]
        svg = _chart_line_svg(
            list(zip(times, values)),
            title=f"{query} ({region})",
            ylabel=materials.get("unit") or "",
            source=" · ".join(materials.get("source_systems") or ["input_rows"]),
            note=f"returned rows: {materials.get('numeric_row_count')}",
        )
        return [
            _svg_to_image(svg),
            TextContent(type="text", text=str({
                "status": "executed",
                "source": "input_rows",
                "input_row_profile": materials,
            })),
        ]
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chart_line", query, resolved_source)))]
    s = await quick_trend(query, region, years, api_key)
    if "오류" in s:
        return [TextContent(type="text", text=str(s))]
    times, values = _values_from_series(s["시계열"])
    if not times:
        return [TextContent(type="text", text="데이터 없음")]
    svg = _chart_line_svg(
        list(zip(times, values)),
        title=f"{s.get('통계명')} ({region})",
        ylabel=s.get("단위", ""),
        source=f"KOSIS · {s.get('통계표')}",
        note=f"최근: {times[-1]}",
    )
    return [
        _svg_to_image(svg),
        TextContent(type="text", text=f"{s.get('통계명')} 시계열 — {region}, {len(times)}개 시점"),
    ]


@mcp.tool()
async def chart_compare_regions(
    query: str, regions: list[str], period: str = "latest",
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
) -> list:
    """[🎨] 지역별 막대 비교 차트.

    Args:
        regions: ["서울", "부산", "대구"] 같은 지역 리스트
    """
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chart_compare_regions", query, resolved_source)))]
    items = []
    for r in regions:
        stat = await quick_stat(query, r, period, api_key)
        if "값" in stat:
            try:
                items.append((r, float(stat["값"])))
            except (ValueError, TypeError):
                continue

    if not items:
        return [TextContent(type="text", text="비교 가능한 데이터 없음")]

    svg = _chart_bar_svg(items, title=f"{query} — 지역 비교", source="KOSIS")
    return [
        _svg_to_image(svg),
        TextContent(type="text", text=f"{query} 지역 비교 ({len(items)}개 지역)"),
    ]


@mcp.tool()
async def chart_correlation(
    query_a: str, query_b: str,
    region: str = "전국", years: int = 15,
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
    input_rows: Optional[list[dict[str, Any]]] = None,
) -> list:
    """[🎨] 두 통계 산점도 + 회귀선."""
    resolved_source = _resolve_tool_source_system(f"{query_a} {query_b}", source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chart_correlation", f"{query_a} {query_b}", resolved_source)))]
    corr = await correlate_stats(query_a, query_b, region, years, api_key)
    if "오류" in corr:
        return [TextContent(type="text", text=str(corr))]
    aligned = corr.get("정합데이터", [])
    if len(aligned) < 3:
        return [TextContent(type="text", text="데이터 부족")]

    points = [(p[1], p[2]) for p in aligned]
    svg = _chart_scatter_svg(
        points,
        title=f"{corr['통계_A']} vs {corr['통계_B']}",
        xlabel=corr["통계_A"], ylabel=corr["통계_B"],
        source="KOSIS", r_value=corr["Pearson"]["상관계수"],
    )
    summary = (
        f"Pearson r={corr['Pearson']['상관계수']}, "
        f"p={corr['Pearson']['p_value']}"
    )
    return [_svg_to_image(svg), TextContent(type="text", text=summary)]


# ---- Phase 2: 추가 차트 4종 ----

@mcp.tool()
async def chart_heatmap(
    query: str,
    regions: Optional[list[str]] = None,
    years: int = 10,
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
) -> list:
    """[🎨] 지역 × 시점 매트릭스 히트맵.

    각 지역의 시계열을 한 차트에 색상으로 표시. 17개 시도 변화 비교에 유용.

    Args:
        query: 통계 키워드 (Tier A 매핑 필요. 예: "출산율", "실업률")
        regions: 비교할 지역. None이면 17개 시도 전체.
        years: 최근 N년 (기본 10)
    """
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chart_heatmap", query, resolved_source)))]
    param = _curation_lookup(query)
    if not param:
        return [TextContent(type="text", text=f'"{query}" Tier A 매핑 없음. chart_heatmap은 정밀 매핑된 통계만 지원.')]
    if not param.region_scheme:
        return [TextContent(type="text", text=f'"{query}"는 지역 분류가 없어 히트맵 불가.')]

    # 기본 지역 = 전체 시도 (전국 제외)
    if regions is None:
        regions = [r for r in param.region_scheme.keys() if r != "전국"]

    # 각 지역의 시계열 수집
    all_years: set[str] = set()
    region_data: dict[str, dict[str, float]] = {}
    for r in regions:
        result = await quick_trend(query, r, years, api_key)
        if "오류" in result:
            continue
        times, values = _values_from_series(result.get("시계열", []))
        region_data[r] = dict(zip(times, values))
        all_years.update(times)

    if not region_data or not all_years:
        return [TextContent(type="text", text="히트맵 생성에 충분한 데이터 없음")]

    sorted_years = sorted(all_years)
    # 매트릭스: rows=regions, cols=years
    matrix: list[list[Optional[float]]] = []
    valid_rows: list[str] = []
    for r in regions:
        if r in region_data:
            row = [region_data[r].get(y) for y in sorted_years]
            matrix.append(row)
            valid_rows.append(r)

    svg = chart_heatmap_svg(
        matrix, valid_rows, sorted_years,
        title=f"{param.description} — 지역 × 시점",
        source=f"KOSIS · {param.tbl_nm}",
        unit=param.unit,
    )
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=f"{param.description} 히트맵 — {len(valid_rows)}개 지역 × {len(sorted_years)}년",
        ),
    ]


@mcp.tool()
async def chart_distribution(
    query: str,
    period: str = "latest",
    highlight_regions: Optional[list[str]] = None,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 시도별 값 분포 (히스토그램 + 박스플롯).

    한 시점에서 17개 시도 값이 어떻게 퍼져 있는지. 중앙값/사분위/평균 표시.

    Args:
        query: 통계 키워드
        period: 비교 시점 (기본 "latest")
        highlight_regions: 분포선상에 강조 표시할 지역 (예: ["서울", "부산"])
    """
    param = _curation_lookup(query)
    if not param or not param.region_scheme:
        return [TextContent(type="text", text=f'"{query}" 지역별 분류 통계가 아님')]

    regions = [r for r in param.region_scheme.keys() if r != "전국"]
    values: list[float] = []
    annotations: list[tuple[str, float]] = []
    for r in regions:
        stat = await quick_stat(query, r, period, api_key)
        if "값" in stat:
            try:
                v = float(stat["값"])
                values.append(v)
                if highlight_regions and r in highlight_regions:
                    annotations.append((r, v))
            except (ValueError, TypeError):
                continue

    if len(values) < 5:
        return [TextContent(type="text", text=f"분포 그리기에 데이터 부족 ({len(values)}개)")]

    svg = chart_distribution_svg(
        values,
        title=f"{param.description} — 시도별 분포",
        bins=min(12, len(values)),
        unit=param.unit,
        source=f"KOSIS · {param.tbl_nm}",
        annotation_labels=annotations if annotations else None,
    )

    # 간단한 통계 요약
    mean = sum(values) / len(values)
    sorted_v = sorted(values)
    median = sorted_v[len(sorted_v) // 2]
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"{param.description} 분포 — 시도 {len(values)}개, "
                f"평균 {mean:.2f}, 중앙값 {median:.2f}, "
                f"범위 [{min(values):.2f}, {max(values):.2f}]"
            ),
        ),
    ]


@mcp.tool()
async def chart_dual_axis(
    query_a: str, query_b: str,
    region: str = "전국",
    years: int = 10,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 두 통계를 단위 다른 축으로 한 차트에 (이중 Y축).

    단위가 다른 두 통계의 시간적 관계를 한눈에. 예: 출산율(명) vs 집값(지수).

    Args:
        query_a: 왼쪽 축 통계 (파란 실선)
        query_b: 오른쪽 축 통계 (빨간 점선)
        region: 같은 지역으로 정합
    """
    a = await quick_trend(query_a, region, years, api_key)
    b = await quick_trend(query_b, region, years, api_key)
    if "오류" in a or "오류" in b:
        return [TextContent(type="text", text=f"데이터 수집 실패: A={a.get('오류','OK')}, B={b.get('오류','OK')}")]

    ta, va = _values_from_series(a["시계열"])
    tb, vb = _values_from_series(b["시계열"])
    series_a = list(zip(ta, va))
    series_b = list(zip(tb, vb))

    if not series_a or not series_b:
        return [TextContent(type="text", text="시계열 데이터 부족")]

    svg = chart_dual_axis_svg(
        series_a, series_b,
        label_a=a.get("통계명", query_a),
        label_b=b.get("통계명", query_b),
        title=f"{a.get('통계명')} vs {b.get('통계명')} ({region})",
        unit_a=a.get("단위", ""),
        unit_b=b.get("단위", ""),
        source="KOSIS",
    )

    common = set(ta) & set(tb)
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"이중축 비교: {a.get('통계명')} (왼쪽) vs {b.get('통계명')} (오른쪽). "
                f"공통 시점 {len(common)}개. 상관관계는 correlate_stats로 확인."
            ),
        ),
    ]


@mcp.tool()
async def chart_dashboard(
    query: str, region: str = "전국",
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
    input_rows: Optional[list[dict[str, Any]]] = None,
) -> list:
    """[🎨] 4분할 종합 대시보드 한 장.

    한 통계를 다각도로: 시계열+예측 / 지역비교 / 핵심지표 / 인사이트.
    `chain_full_analysis`의 데이터를 그래픽으로 정리.

    Args:
        query: 통계 키워드
        region: 시계열의 기준 지역
    """
    if input_rows is not None:
        return await chart_line(query, region=region, api_key=api_key, source_system=source_system, input_rows=input_rows)
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chart_dashboard", query, resolved_source)))]
    param = _curation_lookup(query)
    if not param:
        return [TextContent(type="text", text=f'"{query}" Tier A 매핑 없음')]

    # 시계열
    series_result = await quick_trend(query, region, 15, api_key)
    times, values = _values_from_series(series_result.get("시계열", []))
    timeseries = list(zip(times, values))

    # 추세 분석
    trend = await analyze_trend(query, region, 15, api_key)

    # 예측 (시계열이 충분하면)
    forecast_pts: list[tuple[str, float, float, float]] = []
    if len(values) >= 4:
        forecast = await forecast_stat(query, region, 15, 5, api_key)
        forecast_path = (
            ((forecast.get("computed_examples") or {}).get("linear") or {}).get("forecast_path")
            or forecast.get("예측")
            or []
        )
        for f in forecast_path:
            if "period" in f:
                forecast_pts.append(
                    (f["period"], f["value"], f["lower"], f["upper"])
                )
            else:
                forecast_pts.append(
                    (f["시점"], f["예측값"], f["하한"], f["상한"])
                )

    # 지역 비교 (top N)
    items: list[tuple[str, float]] = []
    if param.region_scheme:
        for r in list(param.region_scheme.keys())[:8]:
            if r == "전국":
                continue
            stat = await quick_stat(query, r, "latest", api_key)
            if "값" in stat:
                try:
                    items.append((r, float(stat["값"])))
                except (ValueError, TypeError):
                    continue

    # 핵심 지표 요약
    summary: dict = {}
    if "선형회귀" in trend:
        lr = trend["선형회귀"]
        summary["기울기/년"] = lr.get("기울기_연간")
        summary["R²"] = lr.get("R제곱")
    if "변화율" in trend:
        analysis_period = str(trend.get("기간") or "")
        summary["분석기간"] = f"{analysis_period} · {trend.get('데이터수')}개 시점"
        summary["평균 변화율"] = f"{trend['변화율']['평균_퍼센트']:+.2f}%"
        recent_window = trend["변화율"].get("최근_구간") or {}
        recent_start = str(recent_window.get("시작") or "")
        recent_end = str(recent_window.get("끝") or "")
        recent_label = (
            "최근 변화"
            if recent_start and recent_end else "최근 변화"
        )
        recent_value = f"{trend['변화율']['최근_퍼센트']:+.2f}%"
        if recent_start and recent_end:
            recent_value = f"{recent_start}→{recent_end}: {recent_value}"
        summary[recent_label] = recent_value
    if "극값" in trend:
        max_point = trend["극값"]["최댓값"]
        summary["분석기간 내 최댓값"] = f"{max_point['시점']}: {max_point['값']}"
    if trend.get("해석"):
        summary["해석"] = trend.get("해석", "")

    svg = chart_dashboard_svg(
        title=f"{param.description} ({region}) — 종합",
        timeseries=timeseries,
        items=items,
        summary=summary,
        forecast=forecast_pts if forecast_pts else None,
        unit=param.unit,
        source=f"KOSIS · {param.tbl_nm}",
    )

    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"{param.description} 대시보드 — 시계열 {len(timeseries)}개 + "
                f"예측 {len(forecast_pts)}년 + 지역 비교 {len(items)}개. "
                f"추세·요약 분석기간: {trend.get('기간', '확인 불가')}"
            ),
        ),
    ]


# ---- Chain Layer ----

@mcp.tool()
async def chain_full_analysis(
    query: str, region: str = "전국",
    api_key: Optional[str] = None,
    source_system: Optional[str] = None,
) -> list:
    """[⛓] 종합 분석 재료 번들: 통계+추세+예측재료+이상치+차트.

    LLM이 필요한 부분만 골라 해석하도록 계산 재료를 묶어 반환한다.
    """
    resolved_source = _resolve_tool_source_system(query, source_system)
    if resolved_source and resolved_source != "KOSIS":
        return [TextContent(type="text", text=str(_unsupported_source_response("chain_full_analysis", query, resolved_source)))]
    latest = await quick_stat(query, region, "latest", api_key)
    if "오류" in latest:
        return [TextContent(type="text", text=str(latest))]

    trend = await analyze_trend(query, region, 20, api_key)
    forecast = await forecast_stat(query, region, 15, 5, api_key)
    outliers = await detect_outliers(query, region, 20, api_key)

    series_result = await quick_trend(query, region, 20, api_key)
    times, values = _values_from_series(series_result.get("시계열", []))
    chart_svg = ""
    if times:
        chart_svg = _chart_line_svg(
            list(zip(times, values)),
            title=f"{trend.get('통계명')} ({region})",
            ylabel=trend.get("단위", ""), source="KOSIS",
        )

    summary = {
        "주제": query, "지역": region,
        "최신값": latest.get("answer"),
        "trend_materials": {
            "model_parameters": trend.get("model_parameters"),
            "formula": trend.get("formula"),
            "period": trend.get("기간"),
        },
        "forecast_materials": {
            "model_options": forecast.get("model_options"),
            "linear_example": ((forecast.get("computed_examples") or {}).get("linear") or {}),
        },
        "이상치": outliers.get("이상치", [])[:3],
        "must_know": trend.get("must_know") or forecast.get("must_know"),
        "출처": "통계청 KOSIS",
    }

    result = [TextContent(type="text", text=str(summary))]
    if chart_svg:
        result.insert(0, _svg_to_image(chart_svg))
    return result


@mcp.tool()
async def plan_query(query: str, api_key: Optional[str] = None, nabo_api_key: Optional[str] = None) -> dict:
    """[🧭] Gemma용 절차형 KOSIS 분석 계획을 만든다.

    질문을 차원과 작업 단계로 분해합니다.

    MUST NOT:
    - 통계표 ID 확정 (select_table_for_query의 책임)
    - 코드 매핑 (resolve_concepts의 책임)
    - 실제 값 반환 (query_table의 책임)
    - 산술/산식 (compute_indicator의 책임)

    역할은 의도 추출과 워크플로우 제안까지만입니다.
    """
    planner = QueryWorkflowPlanner()
    plan = planner.build(query)
    return await _enrich_plan_with_metadata(plan, query, api_key=api_key, nabo_api_key=nabo_api_key)


@mcp.tool()
async def answer_query(
    query: str,
    region: str = "전국",
    api_key: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """[🤖] 자연어 질문을 실제 답변 또는 안전한 분석계획으로 생성.

    검증된 Tier A 질문은 KOSIS API를 호출해 수치·표·계산 재료를 반환하고,
    복합/상위어 질문은 실제 KOSIS 검색 후보와 분석계획을 반환한다.
    verbose=False를 지정하면 data/metadata/notes 중심의 슬림 응답을 반환한다.
    """
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            result = _attach_gemma_deprecation_warning(
                _missing_api_key_response("answer_query", query=query, region=region)
            )
            return result if verbose else _compact_answer_query_response(result, query=query, region=region)
        raise
    engine = NaturalLanguageAnswerEngine(key)
    try:
        result = await engine.answer(query, region)
        result = _attach_gemma_deprecation_warning(result)
        return result if verbose else _compact_answer_query_response(result, query=query, region=region)
    except RuntimeError as e:
        result = _attach_gemma_deprecation_warning({
            "상태": "failed",
            "status": "failed",
            "코드": STATUS_RUNTIME_ERROR,
            "code": STATUS_RUNTIME_ERROR,
            "오류": str(e),
            "질문": query,
        })
        return result if verbose else _compact_answer_query_response(result, query=query, region=region)


@mcp.tool()
async def verify_stat_claims(answer_payload: dict[str, Any]) -> dict:
    """[✅] answer_query 결과의 수치·출처·산식 검증 상태를 점검.

    실제 원자료를 재호출하지 않고, 챗봇 응답 payload가 최소한의 통계 응답
    요건(값, 단위, 기준시점, 통계표, 출처, 산식)을 갖췄는지 확인한다.
    """
    if not isinstance(answer_payload, dict):
        return {
            "verified": False,
            "코드": STATUS_RUNTIME_ERROR,
            "issues": ["입력은 answer_query가 반환한 dict payload여야 합니다."],
        }

    status_code = answer_payload.get("코드")
    status = answer_payload.get("상태")
    if status_code == STATUS_NEEDS_TABLE_SELECTION or status == "needs_table_selection":
        return {
            "verified": False,
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "검증_결과": "통계표 선택 필요",
            "issues": ["아직 수치 claim이 아니라 후보 통계표와 분석계획 단계입니다."],
            "next_steps": answer_payload.get("다음단계", []),
        }

    rows = answer_payload.get("표") or []
    issues: list[str] = []
    warnings: list[str] = []
    claims: list[dict[str, Any]] = []

    if not rows:
        issues.append("표 또는 근거 행이 없어 수치 claim을 검증할 수 없습니다.")
    if isinstance(rows, list):
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                issues.append(f"{idx}번째 표 행이 dict 형식이 아닙니다.")
                continue
            missing = [
                field for field in ("값", "단위", "시점", "통계표")
                if row.get(field) in (None, "")
            ]
            if missing:
                issues.append(f"{idx}번째 표 행 누락 필드: {', '.join(missing)}")
            else:
                claims.append({
                    "지표": row.get("지표"),
                    "값": row.get("값"),
                    "단위": row.get("단위"),
                    "시점": row.get("시점"),
                    "통계표": row.get("통계표"),
                })
    else:
        issues.append("표 필드는 list 형식이어야 합니다.")

    calculation = answer_payload.get("계산")
    if calculation:
        if not isinstance(calculation, dict):
            issues.append("계산 필드는 dict 형식이어야 합니다.")
        elif "산식" not in calculation:
            issues.append("계산 결과에 산식이 없습니다.")
        elif calculation.get("동일시점_여부") is False:
            warnings.append("계산에 사용한 지표들의 기준시점이 서로 다릅니다.")

    if not answer_payload.get("출처") and not any(isinstance(row, dict) and row.get("출처") for row in rows):
        issues.append("출처 정보가 없습니다.")
    if "검증_주의" not in answer_payload and "검증" not in answer_payload:
        warnings.append("검증 주의사항 또는 validation profile이 없습니다.")

    verified = not issues and bool(claims)
    return {
        "verified": verified,
        "코드": STATUS_EXECUTED if verified else STATUS_UNVERIFIED_FORMULA,
        "검증_결과": "통과" if verified else "보완 필요",
        "claims": claims,
        "issues": issues,
        "warnings": warnings,
    }


@mcp.tool()
async def stat_time_compare(
    query: str,
    region: str = "전국",
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    years: int = 5,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] 한 통계의 시작·종료 시점 차이와 변화율 계산.

    start_period/end_period를 생략하면 최근 N개 시계열의 첫 시점과 마지막
    시점을 비교한다.
    """
    explicit_periods = bool(start_period and end_period)
    if explicit_periods:
        try:
            key = _resolve_key(api_key)
        except RuntimeError as exc:
            if _is_missing_key_error(exc):
                return _missing_api_key_response(
                    "stat_time_compare",
                    query=query,
                    region=region,
                    start_period=start_period,
                    end_period=end_period,
                )
            raise
        param = _lookup_quick(query)
        if not param:
            return {
                "상태": "failed",
                "코드": STATUS_STAT_NOT_FOUND,
                "오류": f'"{query}" 사전 매핑 없음',
                "질문": query,
            }

        canonical = _canonical_region(region) or region
        region_code = None
        if param.region_scheme:
            region_code = param.region_scheme.get(canonical)
            if not region_code:
                return {
                    "상태": "failed",
                    "코드": STATUS_PERIOD_NOT_FOUND,
                    "오류": f'지역 "{region}" 미지원',
                    "지원_지역": list(param.region_scheme.keys()),
                    "질문": query,
                }
        elif canonical != "전국":
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "오류": f'"{query}"는 지역별 시계열 조회가 검증되지 않았습니다.',
                "지원_지역": ["전국"],
                "질문": query,
            }

        period_type = _default_period_type(param)
        start_bounds = _period_bounds(str(start_period or ""), period_type)
        end_bounds = _period_bounds(str(end_period or ""), period_type)
        fetch_start = start_bounds[0] or str(start_period or "")
        fetch_end = end_bounds[1] or str(end_period or "")
        fetch_low, fetch_high = sorted([fetch_start, fetch_end])

        async with httpx.AsyncClient() as client:
            data = await _fetch_series(
                client,
                key,
                param,
                region_code,
                period_type=period_type,
                start_year=fetch_low,
                end_year=fetch_high,
            )
        if not data:
            fallback = await _quick_trend_core(query, canonical, max(years, 5), api_key)
            data = [
                {"PRD_DE": row.get("시점"), "DT": row.get("값")}
                for row in (fallback.get("시계열") or [])
            ]
        series_result = {
            "통계명": param.description,
            "지역": canonical,
            "단위": param.unit,
            "시계열": [{"시점": r.get("PRD_DE"), "값": r.get("DT")} for r in data],
            "통계표": param.tbl_nm,
        }
        region = canonical
    else:
        series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return {
            "상태": "failed",
            "코드": STATUS_STAT_NOT_FOUND,
            "오류": series_result.get("오류"),
            "질문": query,
        }

    times, values = _values_from_series(series_result.get("시계열", []))
    if len(values) < 2:
        return {
            "상태": "failed",
            "코드": STATUS_PERIOD_NOT_FOUND,
            "오류": "비교 가능한 시점이 2개 미만입니다.",
            "질문": query,
        }

    points = dict(zip(times, values))

    def pick(period: Optional[str], default_period: str) -> tuple[str, float] | None:
        if not period:
            return default_period, points[default_period]
        period = str(period)
        for candidate in times:
            if candidate == period or candidate.startswith(period):
                return candidate, points[candidate]
        return None

    start = pick(start_period, times[0])
    end = pick(end_period, times[-1])
    if start is None or end is None:
        return {
            "상태": "failed",
            "코드": STATUS_PERIOD_NOT_FOUND,
            "오류": "요청한 비교 시점을 시계열에서 찾지 못했습니다.",
            "요청_시작": start_period,
            "요청_종료": end_period,
            "가능_시점": times,
            "질문": query,
        }

    start_t, start_v = start
    end_t, end_v = end
    diff = end_v - start_v
    rate = (diff / abs(start_v) * 100) if start_v else None
    direction = "증가" if diff > 0 else "감소" if diff < 0 else "변화 없음"
    unit = series_result.get("단위", "")
    answer = (
        f"{region} {series_result.get('통계명', query)}은 {start_t} {start_v:,.3f}{unit}에서 "
        f"{end_t} {end_v:,.3f}{unit}로 {direction}했습니다."
    )
    if rate is not None:
        answer += f" 변화율은 {rate:+.2f}%입니다."

    return {
        "상태": "executed",
        "코드": STATUS_EXECUTED,
        "질문": query,
        "answer": answer,
        "비교": {
            "시작": {"시점": start_t, "값": start_v},
            "종료": {"시점": end_t, "값": end_v},
            "증감": round(diff, 4),
            "변화율_퍼센트": round(rate, 2) if rate is not None else None,
            "방향": direction,
            "산식": "(종료값 - 시작값) / 시작값 * 100",
        },
        "표": [
            {"시점": t, "값": v, "단위": unit, "통계표": series_result.get("통계표")}
            for t, v in zip(times, values)
        ],
        "출처": "통계청 KOSIS",
        "검증_주의": ["변화율과 증감은 구분해서 해석해야 합니다."],
    }


@mcp.tool()
async def indicator_dependency_map(indicator: str) -> dict:
    """[🧭] 비중·증가율·폐업률 등 산식형 지표의 필요 통계와 검증 포인트 안내."""
    q = _compact_text(indicator)
    subgroup_terms = [
        ("청년", "청년"),
        ("청소년", "청소년"),
        ("고령", "고령층"),
        ("노인", "고령층"),
        ("여성", "여성"),
        ("남성", "남성"),
    ]
    for key, spec in FORMULA_DEPENDENCIES.items():
        aliases = [spec["canonical"], *spec.get("aliases", [])]
        if any(_compact_text(alias) in q or q in _compact_text(alias) for alias in aliases):
            result = {
                "상태": "mapped",
                "코드": STATUS_EXECUTED,
                "입력": indicator,
                "dependency_key": key,
                "지표": spec["canonical"],
                "산식": spec["formula"],
                "필요_통계": spec["required_stats"],
                "검증_포인트": spec["checks"],
                "주의": spec["caution"],
                "advisory_scope": "formula_dependency_only",
                "caller_must_verify": [
                    "KOSIS metadata contains the numerator and denominator concepts",
                    "query_table raw rows share compatible period, unit, and population scope",
                    "caller has selected the correct formula operation before compute_indicator",
                ],
            }
            subgroup = next((label for term, label in subgroup_terms if term in indicator), None)
            if subgroup and key == "unemployment_rate":
                result["대상군"] = subgroup
                result["부분군_적용"] = (
                    f"{subgroup} 실업률은 같은 산식을 쓰되, 분자=실업자 수와 "
                    f"분모=경제활동인구를 모두 {subgroup} 대상군으로 제한해야 합니다."
                )
                result["추천_검색어"] = [
                    f"{subgroup} 실업률",
                    f"{subgroup} 실업자",
                    f"{subgroup} 경제활동인구",
                ]
            result["mcp_output_contract"] = _mcp_tool_output_contract(
                role="dependency_advisory",
                final_answer_expected=False,
                markers=["formula_advisory_only"],
                explanation="indicator_dependency_map only suggests formula dependencies; it does not verify a table, concept code, or raw value.",
                extra_signals={
                    "dependency_key": key,
                    "caller_must_verify": result["caller_must_verify"],
                },
            )
            return result

    route_payload = _route_query(indicator).to_agent_payload()
    return {
        "상태": "needs_definition",
        "코드": STATUS_DENOMINATOR_REQUIRED,
        "입력": indicator,
        "answer": "해당 표현은 고정 산식형 지표로 확정하지 못했습니다. 지표·분모·비교시점을 먼저 정해야 합니다.",
        "추천_검색어": route_payload["route"].get("search_terms", []),
        "의도": route_payload.get("intents", []),
        "검증": route_payload.get("validation", {}),
        "mcp_output_contract": _mcp_tool_output_contract(
            role="dependency_advisory",
            final_answer_expected=False,
            markers=["missing_denominator", "formula_advisory_only"],
            explanation="The formula dependency could not be determined from advisory patterns; caller must define numerator/denominator explicitly.",
            extra_signals={
                "recommended_next_step": "Use plan_query/select_table_for_query/resolve_concepts to verify concepts from KOSIS metadata.",
            },
        ),
    }


# ---- Utility ----

async def _search_kosis_keywords(
    query: str,
    keywords: list[str],
    limit: int,
    api_key: Optional[str] = None,
    used_routing: bool = False,
) -> dict:
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response("search_kosis", query=query, limit=limit)
        raise
    keywords = [
        keyword.strip()
        for keyword in keywords
        if isinstance(keyword, str) and keyword.strip()
    ]
    if not keywords:
        keywords = [query]
    keywords = list(dict.fromkeys(keywords))

    async with httpx.AsyncClient() as client:
        all_results = []
        for kw in keywords:
            try:
                r = await _kosis_call(client, "statisticsSearch.do", {
                    "method": "getList", "apiKey": key,
                    "searchNm": kw, "format": "json", "jsonVD": "Y", "resultCount": limit,
                })
                for item in r:
                    item["_검색어"] = kw
                all_results.extend(r)
            except RuntimeError:
                continue

    # 중복 제거
    seen = set()
    unique = []
    for item in all_results:
        tid = item.get("TBL_ID")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(item)

    # Surface any Tier A direct mapping that covers this query — search
    # alone could miss the canonical table because KOSIS' search index
    # ranks weakly-related tables higher than the verified one (#23).
    tier_a_match = _lookup_quick(query)
    tier_a_hint: Optional[dict[str, Any]] = None
    if tier_a_match is not None:
        tier_a_hint = {
            "지표": tier_a_match.description,
            "통계표": tier_a_match.tbl_nm,
            "통계표ID": tier_a_match.tbl_id,
            "기관ID": tier_a_match.org_id,
            "단위": tier_a_match.unit,
            "주기": list(tier_a_match.supported_periods),
            "검증상태": tier_a_match.verification_status,
            "권고_호출": (
                f"quick_stat('{query}', region='전국', period='latest') 으로 바로 호출 가능. "
                "search 결과를 다시 매핑할 필요 없음."
            ),
        }

    search_markers: list[str] = []
    if not unique:
        search_markers.append("search_empty")
    if used_routing:
        search_markers.append("routing_expanded")
    if tier_a_hint is not None:
        search_markers.append("tier_a_available")
    search_explanation = (
        "Search returned no candidate tables; LLM should broaden the query or rely on the planner workflow."
        if not unique else
        "Tier A direct mapping is available; prefer it over re-ranking the search list."
        if tier_a_hint is not None else
        "Search returned candidate tables; verify each candidate via select_table_for_query before use."
    )
    return {
        "입력": query,
        "라우팅_사용": used_routing,
        "사용된_검색어": keywords,
        "search_terms_used": keywords,
        "original_query_preserved": bool(keywords and keywords[0] == query),
        "결과수": len(unique),
        "Tier_A_직접_매핑": tier_a_hint,
        "결과": [
            {
                "통계표명": r.get("TBL_NM"),
                "통계표ID": r.get("TBL_ID"),
                "기관ID": r.get("ORG_ID"),
                "수록기간": f"{r.get('STRT_PRD_DE')} ~ {r.get('END_PRD_DE')}",
                "검색어": r.get("_검색어"),
                "URL": r.get("LINK_URL") or r.get("TBL_VIEW_URL"),
            }
            for r in unique[:limit]
        ],
        "mcp_output_contract": _mcp_tool_output_contract(
            role="table_search",
            final_answer_expected=False,
            markers=search_markers,
            explanation=search_explanation,
            extra_signals={
                "result_count": len(unique),
                "routing_used": used_routing,
                "tier_a_match": tier_a_hint is not None,
                "original_query_preserved": bool(keywords and keywords[0] == query),
            },
        ),
    }


def _candidate_latest_period_year(candidate: dict[str, Any]) -> int:
    years: list[int] = []
    for period in candidate.get("periods") or []:
        text = str(period.get("latest_period") or "")
        match = re.match(r"^(\d{4})", text)
        if match:
            years.append(int(match.group(1)))
    return max(years) if years else 0


def _candidate_cadence_diversity(candidate: dict[str, Any]) -> int:
    return len({
        str(period.get("cadence") or "")
        for period in candidate.get("periods") or []
        if period.get("cadence")
    })


def _candidate_item_count(candidate: dict[str, Any]) -> int:
    profile = candidate.get("metadata_profile") or {}
    try:
        return int(profile.get("item_count") or 0)
    except (TypeError, ValueError):
        return 0


def _candidate_indicator_score(candidate: dict[str, Any]) -> int:
    compatibility = candidate.get("compatibility") or {}
    try:
        return int(compatibility.get("indicator_score") or 0)
    except (TypeError, ValueError):
        return 0


def _annotate_table_candidate_ranking(candidates: list[dict[str, Any]]) -> None:
    for candidate in candidates:
        indicator_score = _candidate_indicator_score(candidate)
        features = {
            "indicator_score": indicator_score,
            "indicator_score_band": indicator_score // 3,
            "latest_period_year": _candidate_latest_period_year(candidate),
            "cadence_diversity": _candidate_cadence_diversity(candidate),
            "item_count": _candidate_item_count(candidate),
        }
        candidate["ranking_features"] = features


def _table_candidate_sort_key(candidate: dict[str, Any]) -> tuple:
    features = candidate.get("ranking_features") or {}
    return (
        candidate.get("status") != "selected",
        -int(features.get("indicator_score_band") or 0),
        -int(features.get("latest_period_year") or 0),
        -int(features.get("indicator_score") or 0),
        -int(features.get("cadence_diversity") or 0),
        -int(features.get("item_count") or 0),
        -int(candidate.get("score") or 0),
        str(candidate.get("tbl_id") or ""),
    )


def _table_selection_reasons(selected: dict[str, Any], selected_candidates: list[dict[str, Any]]) -> list[str]:
    features = selected.get("ranking_features") or {}
    indicator_score = int(features.get("indicator_score") or 0)
    indicator_score_band = int(features.get("indicator_score_band") or 0)
    latest_year = int(features.get("latest_period_year") or 0)
    cadence_count = int(features.get("cadence_diversity") or 0)
    item_count = int(features.get("item_count") or 0)
    tied_indicator = [
        candidate for candidate in selected_candidates
        if int((candidate.get("ranking_features") or {}).get("indicator_score") or 0) == indicator_score
    ]
    near_tied_indicator = [
        candidate for candidate in selected_candidates
        if int((candidate.get("ranking_features") or {}).get("indicator_score_band") or 0) == indicator_score_band
    ]
    freshest_tied = max(
        (int((candidate.get("ranking_features") or {}).get("latest_period_year") or 0) for candidate in near_tied_indicator),
        default=0,
    )
    reasons = [
        f"indicator_score={indicator_score} (exact-tied with {max(0, len(tied_indicator) - 1)} others)",
        f"indicator_score_band={indicator_score_band} (near-tied with {max(0, len(near_tied_indicator) - 1)} others)",
    ]
    if latest_year:
        suffix = "freshest among near-tied candidates" if latest_year >= freshest_tied else "not freshest among near-tied candidates"
        reasons.append(f"latest_period_year={latest_year} ({suffix})")
    if cadence_count:
        reasons.append(f"cadence_diversity={cadence_count}")
    if item_count:
        reasons.append(f"item_count={item_count}")
    return reasons


def _search_result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("결과")
    if not isinstance(rows, list):
        rows = payload.get("寃곌낵")
    return rows if isinstance(rows, list) else []


def _row_first(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _indicator_meta_match_score(raw: str, label: Any, field_name: str) -> int:
    target = _compact_text(raw)
    text = _compact_text(str(label or ""))
    if not target or not text:
        return 0
    raw_text = str(raw or "").strip()
    label_text = str(label or "")
    if re.fullmatch(r"[A-Za-z]{2,6}", raw_text):
        acronym = re.escape(raw_text)
        if re.search(rf"\(\s*{acronym}\s*\)", label_text, re.IGNORECASE):
            return 98 if field_name == "ITM_NM" else 78
        if re.search(rf"(?<![A-Za-z가-힣]){acronym}(?![A-Za-z가-힣])", label_text, re.IGNORECASE):
            return 96 if field_name == "ITM_NM" else 76
    if target == text:
        return 100
    if target in text:
        return 90 if field_name == "ITM_NM" else 70
    if text in target and len(text) >= 2:
        return 80 if field_name == "ITM_NM" else 60
    return 0


async def _normalize_indicator_from_kosis_meta(
    user_input: Any,
    api_key: Optional[str] = None,
    *,
    search_queries: Optional[list[str]] = None,
    search_limit: int = 6,
    table_limit: int = 4,
) -> dict[str, Any]:
    raw = str(user_input or "").strip()
    base = {
        "raw_input": raw,
        "normalized": raw,
        "source": "passthrough",
        "match_evidence": None,
        "alternatives": [],
    }
    if not raw:
        return base
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return {
                **base,
                "source": "metadata_unavailable",
                "status": "skipped",
                "reason": "missing_api_key",
            }
        raise

    query_inputs = [
        term.strip()
        for term in [raw, *(search_queries or [])]
        if isinstance(term, str) and term.strip()
    ]
    query_inputs = list(dict.fromkeys(query_inputs))[:4]
    rows: list[dict[str, Any]] = []
    search_failures: list[dict[str, Any]] = []
    for query_input in query_inputs:
        try:
            search_payload = await search_kosis(query_input, limit=search_limit, use_routing=False, api_key=key)
            for result_rank, result_row in enumerate(_search_result_rows(search_payload)[:table_limit]):
                rows.append({
                    **result_row,
                    "_normalization_query": query_input,
                    "_normalization_query_rank": query_inputs.index(query_input),
                    "_normalization_result_rank": result_rank,
                })
        except Exception as exc:
            search_failures.append({"query": query_input, "error": str(exc)})
    if not rows and search_failures:
        return {
            **base,
            "source": "metadata_unavailable",
            "status": "search_failed",
            "reason": search_failures[0]["error"],
            "search_failures": search_failures,
        }
    matches: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=8.0) as client:
        for row in rows:
            org_id = _row_first(row, ["기관ID", "湲곌?ID", "ORG_ID", "org_id"])
            tbl_id = _row_first(row, ["통계표ID", "?듦퀎?쏧D", "TBL_ID", "tbl_id"])
            table_name = _row_first(row, ["통계표명", "?듦퀎?쒕챸", "TBL_NM", "table_name"])
            query_used = str(row.get("_normalization_query") or raw)
            if not org_id or not tbl_id:
                continue
            try:
                item_rows = await _fetch_meta(client, key, str(org_id), str(tbl_id), "ITM")
            except Exception:
                continue
            if not isinstance(item_rows, list):
                continue
            for item in item_rows:
                label = item.get("ITM_NM") or item.get("label")
                label_en = item.get("ITM_NM_ENG") or item.get("label_en")
                scored = [
                    ("ITM_NM", label, max(
                        _indicator_meta_match_score(raw, label, "ITM_NM"),
                        _indicator_meta_match_score(query_used, label, "ITM_NM"),
                    )),
                    ("ITM_NM_ENG", label_en, max(
                        _indicator_meta_match_score(raw, label_en, "ITM_NM_ENG"),
                        _indicator_meta_match_score(query_used, label_en, "ITM_NM_ENG"),
                    )),
                ]
                field_name, matched_value, score = max(scored, key=lambda part: part[2])
                if score <= 0:
                    continue
                table_name_score = max(
                    _indicator_meta_match_score(raw, table_name, "TBL_NM"),
                    _indicator_meta_match_score(query_used, table_name, "TBL_NM"),
                    _indicator_meta_match_score(str(label or ""), table_name, "TBL_NM"),
                )
                adjusted_score = score + min(12, table_name_score // 8)
                if score < 100 and table_name_score <= 0 and len(_compact_text(raw)) <= 4:
                    adjusted_score -= 8
                matches.append({
                    "canonical_name": str(label or matched_value),
                    "score": adjusted_score,
                    "raw_match_score": score,
                    "table_name_relevance_score": table_name_score,
                    "matched_field": field_name,
                    "matched_value": matched_value,
                    "match_evidence": f"{field_name} contains {query_used!r}",
                    "normalization_query": query_used,
                    "normalization_query_rank": row.get("_normalization_query_rank"),
                    "normalization_result_rank": row.get("_normalization_result_rank"),
                    "org_id": str(org_id),
                    "tbl_id": str(tbl_id),
                    "table_name": table_name,
                    "itm_id": item.get("ITM_ID"),
                    "unit": item.get("UNIT_NM"),
                })

    if not matches:
        return {
            **base,
            "candidate_count": 0,
            "search_result_count": len(rows),
            "search_queries_used": query_inputs,
        }
    matches.sort(key=lambda item: (
        -int(item["score"]),
        0 if re.search(rf"\(\s*{re.escape(raw)}\s*\)", str(item["canonical_name"]), re.IGNORECASE) else 1,
        int(item.get("normalization_query_rank") or 0),
        int(item.get("normalization_result_rank") or 0),
        len(_compact_text(item["canonical_name"])),
        item["canonical_name"],
    ))
    best = matches[0]
    alternatives = [
        {
            "name": item["canonical_name"],
            "match_evidence": item["match_evidence"],
            "score": item["score"],
            "table_name_relevance_score": item.get("table_name_relevance_score"),
            "normalization_query": item.get("normalization_query"),
            "tbl_id": item["tbl_id"],
            "itm_id": item.get("itm_id"),
        }
        for item in matches[1:5]
    ]
    return {
        "raw_input": raw,
        "normalized": best["canonical_name"],
        "source": "kosis_meta_match",
        "match_evidence": best["match_evidence"],
        "matched_field": best["matched_field"],
        "matched_value": best["matched_value"],
        "table_name_relevance_score": best.get("table_name_relevance_score"),
        "normalization_query": best.get("normalization_query"),
        "alternatives": alternatives,
        "org_id": best["org_id"],
        "tbl_id": best["tbl_id"],
        "table_name": best["table_name"],
        "itm_id": best.get("itm_id"),
        "unit": best.get("unit"),
        "candidate_count": len(matches),
        "search_result_count": len(rows),
        "search_queries_used": query_inputs,
    }


def _metric_name_key(value: Any) -> str:
    return re.sub(r"[\s_]+", "", _compact_text(str(value or "")))


def _replace_metric_references(plan: dict[str, Any], raw_value: Any, normalized_value: Any) -> None:
    raw_key = _metric_name_key(raw_value)
    normalized = str(normalized_value or "")
    if not raw_key or not normalized:
        return
    scalar_keys = {
        "indicator",
        "metric",
        "kept_metric",
        "primary",
        "used",
    }
    list_keys = {
        "metrics",
        "concepts",
    }

    def visit(node: Any, parent_key: Optional[str] = None) -> Any:
        if isinstance(node, dict):
            for key, value in list(node.items()):
                if key in scalar_keys and isinstance(value, str) and _metric_name_key(value) == raw_key:
                    node[key] = normalized
                else:
                    node[key] = visit(value, key)
            return node
        if isinstance(node, list):
            updated = []
            for value in node:
                if parent_key in list_keys and isinstance(value, str) and _metric_name_key(value) == raw_key:
                    updated.append(normalized)
                else:
                    updated.append(visit(value, parent_key))
            return updated
        return node

    visit(plan)


def _dedupe_metric_dicts(metrics: list[Any]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        key = _metric_name_key(metric.get("name"))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(metric)
    return deduped


def _plan_contract_add_markers(plan: dict[str, Any], markers: list[str]) -> None:
    contract = plan.get("mcp_output_contract")
    if not isinstance(contract, dict):
        return
    signals = contract.setdefault("current_signals", {})
    present = list(signals.get("markers_present") or [])
    for marker in markers:
        if marker not in present:
            present.append(marker)
    signals["markers_present"] = present
    signals["has_caveats"] = bool(present)


def _change_implication_evidence(query: str, route_payload: dict[str, Any]) -> Optional[str]:
    intents = route_payload.get("intents") or []
    slots = route_payload.get("slots") or {}
    if "STAT_GROWTH_RATE" in intents:
        return "route intent STAT_GROWTH_RATE"
    calculations = slots.get("calculation") if isinstance(slots, dict) else None
    if isinstance(calculations, list) and any(item in calculations for item in ("growth_rate", "change")):
        return "router_slots.calculation implies change"
    match = re.search(r"(올랐|올라|오르|내렸|내려|내리|늘었|늘어|줄었|줄어|증가|감소|상승|하락|변화|증감)", query)
    if match:
        return f"query contains change cue {match.group(1)!r}"
    return None


def _detect_explicit_source_preference(query: str, plan: dict[str, Any]) -> Optional[str]:
    slots = plan.get("router_slots") if isinstance(plan.get("router_slots"), dict) else {}
    raw = str((slots or {}).get("source_preference") or "").strip()
    if raw:
        raw_norm = _compact_text(raw).lower()
        if raw_norm in {"nabo", "nabostats"} or "국회예산정책처" in raw or "재정경제통계시스템" in raw:
            return "NABO"
        if raw_norm in {"kosis"} or "통계청" in raw:
            return "KOSIS"
    q_norm = _compact_text(query).lower()
    if any(term in q_norm for term in ("nabo", "nabostats", "국회예산정책처", "재정경제통계시스템")):
        return "NABO"
    if "kosis" in q_norm or "통계청" in q_norm:
        return "KOSIS"
    return None


def _strip_korean_particle(value: str) -> str:
    text = str(value or "").strip(" \t\r\n,.;:()[]{}\"'")
    for suffix in ("으로", "로", "에서", "에게", "부터", "까지", "보다", "처럼", "라는", "란", "와", "과", "및", "은", "는", "이", "가", "을", "를", "의"):
        if len(text) > len(suffix) + 1 and text.endswith(suffix):
            return text[: -len(suffix)]
    return text


def _nabo_plan_candidate_phrases(query: str, plan: dict[str, Any]) -> list[str]:
    candidates: list[str] = []

    def add(value: Any) -> None:
        text = _strip_korean_particle(str(value or ""))
        if not text:
            return
        compact = _compact_text(text).lower()
        if compact in {
            "nabo", "nabostats", "기준", "최근", "최신", "추이", "시계열",
            "비율", "비중", "구성비", "대비", "증가율", "감소율", "변화율",
            "관리", "국회예산정책처", "재정경제통계시스템",
        }:
            return
        if re.fullmatch(r"\d+년?", compact):
            return
        if len(compact) <= 1:
            return
        if text not in candidates:
            candidates.append(text)

    dimensions = plan.get("intended_dimensions") if isinstance(plan.get("intended_dimensions"), dict) else {}
    add(dimensions.get("indicator"))
    for metric in plan.get("metrics") or []:
        if isinstance(metric, dict):
            add(metric.get("name"))
    for concept in plan.get("concepts") or []:
        add(concept)
    route = plan.get("route") if isinstance(plan.get("route"), dict) else {}
    for term in route.get("search_terms") or []:
        add(term)

    normalized_query = re.sub(r"[,;/|·\n]+", " ", str(query or ""))
    parts = re.split(r"\s+(?:및|그리고|또는|또|이랑|랑|하고)\s+|와\s+|과\s+", normalized_query)
    for part in parts:
        part = re.sub(r"\b(?:NABO|NABOSTATS)\b", " ", part, flags=re.IGNORECASE)
        part = re.sub(r"(국회예산정책처|재정경제통계시스템|기준|최근\s*\d+\s*년|최근|최신|추이|시계열)", " ", part)
        part = re.sub(r"\s+", " ", part).strip()
        add(part)

    tokens = [
        _strip_korean_particle(token)
        for token in re.split(r"\s+", normalized_query)
        if token.strip()
    ]
    for token in tokens:
        add(token)
    for window in (4, 3, 2):
        for idx in range(0, max(0, len(tokens) - window + 1)):
            add(" ".join(tokens[idx: idx + window]))
    return candidates[:12]


def _nabo_metric_from_search_query(query: str) -> Optional[str]:
    text = str(query or "").strip()
    if not text:
        return None
    text = re.sub(r"\b(?:GDP|GNI|GRDP)\s*(?:대비|비율|ratio)\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"(대비\s*)?(비율|비중|구성비|추이|시계열)$", "", text).strip()
    text = _strip_korean_particle(text)
    return text or None


async def _collect_nabo_plan_matches(query: str, plan: dict[str, Any], nabo_api_key: Optional[str]) -> dict[str, Any]:
    candidates = _nabo_plan_candidate_phrases(query, plan)
    metrics: list[dict[str, Any]] = []
    seen_metrics: set[str] = set()
    term_matches: list[dict[str, Any]] = []
    table_matches: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def add_metric(name: Any, *, source: str, evidence: dict[str, Any]) -> None:
        metric_name = _nabo_metric_from_search_query(str(name or ""))
        if not metric_name:
            return
        key = _metric_name_key(metric_name)
        if not key or key in seen_metrics:
            return
        seen_metrics.add(key)
        metrics.append({
            "name": metric_name,
            "role": "primary" if not metrics else "comparison",
            "source": source,
            "availability": "unknown",
            "extraction_method": "nabo_metadata_or_query_candidate",
            "caller_must_verify_with_nabo_meta": True,
            "source_system": "NABO",
            "nabo_evidence": evidence,
        })

    for candidate in candidates:
        try:
            terms_payload = await search_nabo_terms(candidate, limit=3, api_key=nabo_api_key)
            if terms_payload.get("source_system") == "NABO" and terms_payload.get("status") == "executed":
                for term in terms_payload.get("terms") or []:
                    if not isinstance(term, dict):
                        continue
                    title = term.get("title")
                    if title:
                        term_matches.append({
                            "query": candidate,
                            "title": title,
                            "term_id": term.get("term_id"),
                        })
                        add_metric(title, source="nabo_term_dictionary", evidence={"query": candidate, "term_id": term.get("term_id")})
            elif terms_payload.get("code") == STATUS_MISSING_API_KEY:
                errors.append({"tool": "search_nabo_terms", "query": candidate, "code": STATUS_MISSING_API_KEY})
                break
        except Exception as exc:
            errors.append({"tool": "search_nabo_terms", "query": candidate, "error": str(exc)})

        try:
            tables_payload = await search_nabo_tables(candidate, limit=3, api_key=nabo_api_key)
            if tables_payload.get("source_system") == "NABO" and tables_payload.get("status") in {"executed", "needs_table_selection"}:
                tables = tables_payload.get("tables") or []
                if tables:
                    table_matches.extend([
                        {
                            "query": candidate,
                            "table_id": table.get("table_id"),
                            "table_name": table.get("table_name"),
                            "dtacycle_cd_suggestion": table.get("dtacycle_cd_suggestion"),
                        }
                        for table in tables[:3]
                        if isinstance(table, dict)
                    ])
                    if _nabo_table_candidates_support_query(candidate, tables):
                        add_metric(candidate, source="nabo_table_search", evidence={"query": candidate, "result_count": len(tables)})
            elif tables_payload.get("code") == STATUS_MISSING_API_KEY:
                errors.append({"tool": "search_nabo_tables", "query": candidate, "code": STATUS_MISSING_API_KEY})
                break
        except Exception as exc:
            errors.append({"tool": "search_nabo_tables", "query": candidate, "error": str(exc)})

    if not metrics:
        for candidate in candidates:
            add_metric(candidate, source="nabo_query_candidate", evidence={"query": candidate, "metadata_status": "unverified"})

    return {
        "source": "nabo_meta_match" if term_matches or table_matches else "nabo_query_candidate",
        "candidate_phrases": candidates,
        "metrics": metrics,
        "term_matches": term_matches[:10],
        "table_matches": table_matches[:10],
        "errors": errors[:5],
    }


def _sync_analysis_tasks_to_metrics(plan: dict[str, Any]) -> None:
    metric_names = [
        metric.get("name")
        for metric in plan.get("metrics") or []
        if isinstance(metric, dict) and metric.get("name")
    ]
    if not metric_names:
        return
    for task in plan.get("analysis_tasks") or []:
        if isinstance(task, dict) and "metrics" in task:
            task["metrics"] = list(metric_names)


def _nabo_period_range_from_time_request(time_request: Any) -> Any:
    if not isinstance(time_request, dict):
        return None
    if time_request.get("type") in {"year_range", "range"} and time_request.get("start") and time_request.get("end"):
        return [str(time_request["start"]), str(time_request["end"])]
    if time_request.get("type") == "relative_period" and time_request.get("years"):
        years = int(time_request.get("years") or 1)
        return [f"<latest_available_period_minus_{max(0, years - 1)}>", "<latest_available_period>"]
    if time_request.get("type") == "year" and time_request.get("value"):
        return [str(time_request["value"]), str(time_request["value"])]
    return None


def _nabo_evidence_workflow(plan: dict[str, Any]) -> list[dict[str, Any]]:
    period_range = _nabo_period_range_from_time_request(plan.get("time_request"))
    return [
        {
            "step": 1,
            "operation": "for_each_metric",
            "tool": "search_nabo_tables",
            "args_template": {"query": "<metric.name>", "limit": 8},
            "fills": "nabo_table_candidates",
        },
        {
            "step": 2,
            "operation": "for_each_candidate_table",
            "tool": "explore_nabo_table",
            "args_template": {"statbl_id": "<nabo_table_candidates[].table_id>"},
            "fills": "items, dtacycle_cd_suggestions, query_template",
        },
        {
            "step": 3,
            "operation": "for_each_verified_nabo_table",
            "tool": "query_nabo_table",
            "args_template": {
                "statbl_id": "<selected_nabo_table.table_id>",
                "dtacycle_cd": "auto",
                "period_range": period_range or "<time_request or latest>",
                "filters": {"ITEM": ["<ITM_ID from explore_nabo_table.items>"]},
            },
        },
        {
            "step": 4,
            "operation": "apply_analysis_tasks",
            "tasks": plan.get("analysis_tasks") or [],
            "performed_by": "chatbot LLM using NABO raw rows; compute_indicator only for explicit arithmetic",
        },
    ]


def _nabo_suggested_workflow(plan: dict[str, Any], query: str) -> list[dict[str, Any]]:
    period_range = _nabo_period_range_from_time_request(plan.get("time_request"))
    return [
        {
            "step": 1,
            "tool": "search_nabo_tables",
            "purpose": "NABO 통계표 후보를 찾습니다.",
            "args": {"query": query, "limit": 8},
            "available_now": True,
        },
        {
            "step": 2,
            "tool": "explore_nabo_table",
            "purpose": "선택된 NABO 표의 항목 코드와 주기를 확인합니다.",
            "args": {"statbl_id": "<selected.table_id>"},
            "available_now": True,
        },
        {
            "step": 3,
            "tool": "query_nabo_table",
            "purpose": "NABO 원자료를 조회합니다.",
            "args": {
                "statbl_id": "<selected.table_id>",
                "dtacycle_cd": "auto",
                "period_range": period_range or "<period_range or latest>",
                "filters": "<explore_nabo_table items/classes>",
            },
            "available_now": True,
        },
    ]


async def _apply_nabo_source_routing(plan: dict[str, Any], query: str, nabo_api_key: Optional[str]) -> list[str]:
    dimensions = plan.setdefault("intended_dimensions", {})
    if not isinstance(dimensions, dict):
        dimensions = {}
        plan["intended_dimensions"] = dimensions
    slots = plan.setdefault("router_slots", {})
    if not isinstance(slots, dict):
        slots = {}
        plan["router_slots"] = slots
    slots["source_preference"] = "NABO"
    plan["source_preference"] = "NABO"
    dimensions["source_system"] = "NABO"
    dimensions["source_preference"] = "NABO"

    match_info = await _collect_nabo_plan_matches(query, plan, nabo_api_key)
    if match_info.get("metrics"):
        plan["metrics"] = match_info["metrics"]
        plan["raw_metric_candidates"] = match_info["metrics"]
    plan["nabo_indicator_normalization"] = {
        "source": match_info.get("source"),
        "candidate_phrases": match_info.get("candidate_phrases"),
        "term_matches": match_info.get("term_matches"),
        "table_matches": match_info.get("table_matches"),
        "errors": match_info.get("errors"),
    }
    metric_names = [metric.get("name") for metric in plan.get("metrics") or [] if isinstance(metric, dict) and metric.get("name")]
    if metric_names:
        dimensions["indicator"] = metric_names[0]
        dimensions["indicator_candidates"] = [
            {
                "name": metric_name,
                "role": "primary" if idx == 0 else "comparison",
                "source": "nabo_indicator_normalization",
                "extraction_method": "nabo_metadata_or_query_candidate",
            }
            for idx, metric_name in enumerate(metric_names)
        ]
        plan["concepts"] = list(dict.fromkeys([*metric_names, *(plan.get("concepts") or [])]))
    _sync_analysis_tasks_to_metrics(plan)
    workflow = _nabo_suggested_workflow(plan, query)
    plan["suggested_workflow"] = workflow
    plan["next_call"] = workflow[0] if workflow else None
    plan["evidence_workflow"] = _nabo_evidence_workflow(plan)
    policy = plan.setdefault("metric_availability_policy", {})
    if isinstance(policy, dict):
        policy["resolved_by"] = "search_nabo_tables -> explore_nabo_table"
        policy["note"] = "Explicit NABO source preference detected; verify availability against NABO metadata, not KOSIS metadata."
    markers = ["source_preference_nabo", "nabo_routing"]
    if match_info.get("term_matches") or match_info.get("table_matches"):
        markers.append("nabo_metadata_candidate")
    if match_info.get("errors"):
        markers.append("nabo_metadata_partial")
    return markers


async def _enrich_plan_with_metadata(
    plan: dict[str, Any],
    query: str,
    api_key: Optional[str] = None,
    nabo_api_key: Optional[str] = None,
) -> dict[str, Any]:
    dimensions = plan.setdefault("intended_dimensions", {})
    if not isinstance(dimensions, dict):
        return plan
    metrics = plan.setdefault("metrics", [])
    if not isinstance(metrics, list):
        metrics = []
        plan["metrics"] = metrics
    concepts = plan.setdefault("concepts", [])
    if not isinstance(concepts, list):
        concepts = []
        plan["concepts"] = concepts

    markers: list[str] = []
    source_preference = _detect_explicit_source_preference(query, plan)
    if source_preference == "NABO":
        markers.extend(await _apply_nabo_source_routing(plan, query, nabo_api_key))
        if markers:
            _plan_contract_add_markers(plan, markers)
        return plan
    if source_preference:
        plan["source_preference"] = source_preference
        dimensions["source_preference"] = source_preference
    promotion_log: list[dict[str, Any]] = []
    inference_log: list[dict[str, Any]] = []
    normalized_by_raw: dict[str, dict[str, Any]] = {}

    async def normalize_once(raw_value: Any) -> dict[str, Any]:
        key = str(raw_value or "").strip()
        if key not in normalized_by_raw:
            normalized_by_raw[key] = await _normalize_indicator_from_kosis_meta(key, api_key=api_key)
        return normalized_by_raw[key]

    raw_indicator = dimensions.get("indicator")
    if isinstance(raw_indicator, str) and raw_indicator.strip():
        normalization = await normalize_once(raw_indicator)
        dimensions["indicator_raw_input"] = raw_indicator
        dimensions["indicator_normalization"] = normalization
        if normalization.get("source") == "kosis_meta_match":
            normalized_name = normalization.get("normalized") or raw_indicator
            if normalized_name != raw_indicator:
                dimensions["indicator"] = normalized_name
                markers.append("indicator_normalized")
                _replace_metric_references(plan, raw_indicator, normalized_name)
                for idx, concept in enumerate(list(concepts)):
                    if _metric_name_key(concept) == _metric_name_key(raw_indicator):
                        concepts[idx] = normalized_name
                if normalized_name not in concepts:
                    concepts.insert(0, normalized_name)
                for metric in metrics:
                    if _metric_name_key(metric.get("name")) == _metric_name_key(raw_indicator):
                        metric["name"] = normalized_name
                        metric["indicator_raw_input"] = raw_indicator
                        metric["indicator_normalization"] = normalization

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        raw_metric = metric.get("name")
        if not isinstance(raw_metric, str) or not raw_metric.strip():
            continue
        normalization = await normalize_once(raw_metric)
        metric["indicator_raw_input"] = metric.get("indicator_raw_input") or raw_metric
        metric["indicator_normalization"] = normalization
        if normalization.get("source") == "kosis_meta_match":
            normalized_name = normalization.get("normalized") or raw_metric
            if normalized_name != raw_metric:
                metric["name"] = normalized_name
                _replace_metric_references(plan, raw_metric, normalized_name)
                markers.append("indicator_normalized")
                if normalized_name not in concepts:
                    concepts.append(normalized_name)
    plan["metrics"] = _dedupe_metric_dicts(metrics)
    metrics = plan["metrics"]

    route = plan.get("route") or {}
    route_concepts = route.get("matched_concepts") or []
    route_search_terms = [
        term for term in (route.get("search_terms") or [])
        if isinstance(term, str) and term.strip()
    ]
    initial_metric_count = len(metrics)
    blocked_metric_keys = {
        _metric_name_key(item.get("name"))
        for item in (plan.get("quarantined_metrics") or [])
        if isinstance(item, dict)
    }
    for concept in route_concepts:
        if not isinstance(concept, str) or not concept.strip():
            continue
        if route_search_terms:
            normalization = await _normalize_indicator_from_kosis_meta(
                concept,
                api_key=api_key,
                search_queries=route_search_terms,
            )
        else:
            normalization = await normalize_once(concept)
        metric_name = normalization.get("normalized") if normalization.get("source") == "kosis_meta_match" else concept
        if not metric_name:
            continue
        metric_key = _metric_name_key(metric_name)
        should_promote = (
            normalization.get("source") == "kosis_meta_match"
            or initial_metric_count == 0
        )
        if metric_key in blocked_metric_keys:
            should_promote = False
        if should_promote and not any(_metric_name_key(metric.get("name")) == metric_key for metric in metrics):
            metrics.append({
                "name": metric_name,
                "role": "primary" if not metrics else "mentioned",
                "source": "route_promotion",
                "availability": "unknown",
            })
            markers.append("route_metric_promoted")
        promotion_log.append({
            "source_field": "route.matched_concepts",
            "raw_value": concept,
            "promoted_to_metric": metric_name,
            "normalization_source": normalization.get("source"),
            "match_evidence": normalization.get("match_evidence"),
            "promoted": should_promote,
        })

    route_payload = {
        "route": route,
        "intents": plan.get("route_intents") or plan.get("intents") or [],
        "slots": plan.get("router_slots") or {},
    }
    evidence = _change_implication_evidence(query, route_payload)
    analysis_tasks = plan.setdefault("analysis_tasks", [])
    if not isinstance(analysis_tasks, list):
        analysis_tasks = []
        plan["analysis_tasks"] = analysis_tasks
    if metrics and evidence and not any(task.get("type") in {"growth_rate", "change_compare", "yoy_pct_or_growth_rate"} for task in analysis_tasks if isinstance(task, dict)):
        analysis_tasks.append({
            "type": "yoy_pct_or_growth_rate",
            "auto_inferred": True,
            "inference_evidence": evidence,
        })
        inference_log.append({
            "source_field": "query_text",
            "inferred_task": "yoy_pct_or_growth_rate",
            "inference_evidence": evidence,
        })
        markers.append("analysis_task_inferred")
        calculations = plan.setdefault("calculations", [])
        if isinstance(calculations, list) and "growth_rate" not in calculations:
            calculations.append("growth_rate")
        if plan.get("intent") == "single_value":
            plan["intent"] = "growth_rate"

    if promotion_log:
        plan["metrics_promotion_log"] = promotion_log
    if inference_log:
        plan["analysis_tasks_inference_log"] = inference_log
    if markers:
        _plan_contract_add_markers(plan, markers)
    return plan


@mcp.tool()
async def search_kosis(
    query: str, limit: int = 10,
    use_routing: bool = True,
    api_key: Optional[str] = None,
) -> dict:
    """[🔍] KOSIS 통합검색. Tier B 라우팅 사전으로 검색어를 자동 보강.

    동작:
      1. use_routing=True (기본)일 때 Tier B 사전에서 추천 검색어 추출
      2. 추천어가 있으면 각각으로 검색해서 결과 통합
      3. 추천어 없으면 원본 query로 검색

    예: "치킨집" → Tier B가 "음식점업, 분식 및 김밥 전문점" 추천 → 두 키워드로 검색

    Args:
        query: 검색어 (자연어 가능)
        limit: 최대 반환 결과 수
        use_routing: Tier B 라우팅 사용 여부 (기본 True)
    """
    keywords = [query]
    used_routing = False
    if use_routing:
        hints = _routing_hints(query)
        if hints:
            keywords = [query, *hints[:3]]
            used_routing = True

    return await _search_kosis_keywords(
        query,
        keywords,
        limit,
        api_key,
        used_routing=used_routing,
    )


@mcp.tool()
async def search_nabo_tables(
    query: str,
    limit: int = 10,
    api_key: Optional[str] = None,
) -> dict:
    """[🔍] NABOSTATS 재정경제통계시스템 통계표 검색."""
    try:
        key = _resolve_nabo_key(api_key)
    except RuntimeError as exc:
        if _is_missing_nabo_key_error(exc):
            return _missing_nabo_key_response("search_nabo_tables", query=query, limit=limit)
        raise

    safe_limit = max(1, min(int(limit or 10), NABO_MAX_PAGE_SIZE))
    params = {"pIndex": 1, "pSize": safe_limit}
    if query:
        params["STATBL_NM"] = query
    try:
        payload = await _nabo_fetch_rows("Sttsapitbl", key, params, max_rows=safe_limit)
    except Exception as exc:
        return {
            "status": "failed",
            "source_system": "NABO",
            "error": str(exc),
            "query": query,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="table_search",
                final_answer_expected=False,
                markers=["fanout_call_failed"],
                explanation="NABO table search failed during OpenAPI access.",
                extra_signals={"source_system": "NABO"},
            ),
        }

    api_tables = [_nabo_table_record(row) for row in payload.get("rows", [])]
    tables = list(api_tables)
    catalog_fallback: dict[str, Any] = {}
    if not tables and query:
        try:
            catalog_fallback = await _nabo_catalog_fallback_search(query, key, safe_limit)
            tables = catalog_fallback.get("tables") or []
        except Exception as exc:
            catalog_fallback = {"error": str(exc)}
    markers = ["search_empty"] if not tables else []
    if catalog_fallback.get("tables"):
        markers.append("catalog_fallback_search")
    search_diagnostics = {
        "api_table_name_result_count": len(api_tables),
        "catalog_fallback_used": bool(catalog_fallback),
        "catalog_fallback_result_count": len(catalog_fallback.get("tables") or []),
        "catalog_size": catalog_fallback.get("catalog_size"),
        "catalog_fallback_error": catalog_fallback.get("error"),
    }
    return {
        "status": "executed" if tables else "needs_table_selection",
        "source_system": "NABO",
        "provider": "국회예산정책처 재정경제통계시스템",
        "query": query,
        "result_count": len(tables),
        "total_count": catalog_fallback.get("total_count", payload.get("total_count", len(tables))),
        "truncated": payload.get("truncated", False),
        "search_diagnostics": search_diagnostics,
        "tables": tables,
        "results": tables,
        "must_know": {
            "source_system": "NABO",
            "provider": "국회예산정책처 재정경제통계시스템",
            "do_not_mix_with_kosis_without_source_label": True,
        },
        "mcp_output_contract": _mcp_tool_output_contract(
            role="table_search",
            final_answer_expected=False,
            markers=markers,
            explanation="NABO table search returns table candidates only, not statistical values.",
            extra_signals={
                "source_system": "NABO",
                "result_count": len(tables),
                "total_count": catalog_fallback.get("total_count", payload.get("total_count", len(tables))),
                "search_diagnostics": search_diagnostics,
            },
        ),
    }


@mcp.tool()
async def explore_nabo_table(
    statbl_id: str,
    api_key: Optional[str] = None,
) -> dict:
    """[🧭] NABO 통계표 메타와 항목 코드를 조회한다."""
    if not statbl_id:
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "error": "statbl_id is required.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="metadata_inspection",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="explore_nabo_table requires a NABO STATBL_ID.",
                extra_signals={"source_system": "NABO"},
            ),
        }
    try:
        key = _resolve_nabo_key(api_key)
    except RuntimeError as exc:
        if _is_missing_nabo_key_error(exc):
            return _missing_nabo_key_response("explore_nabo_table", statbl_id=statbl_id)
        raise

    try:
        table_payload, item_payload = await asyncio.gather(
            _nabo_fetch_rows("Sttsapitbl", key, {"STATBL_ID": statbl_id, "pSize": 1}, max_rows=1),
            _nabo_fetch_rows("Sttsapitblitm", key, {"STATBL_ID": statbl_id, "pSize": NABO_MAX_PAGE_SIZE}, max_rows=NABO_MAX_PAGE_SIZE),
        )
    except Exception as exc:
        return {
            "status": "failed",
            "source_system": "NABO",
            "table_id": statbl_id,
            "error": str(exc),
            "mcp_output_contract": _mcp_tool_output_contract(
                role="metadata_inspection",
                final_answer_expected=False,
                markers=["metadata_failed"],
                explanation="NABO metadata lookup failed during OpenAPI access.",
                extra_signals={"source_system": "NABO", "table_id": statbl_id},
            ),
        }

    table_rows = table_payload.get("rows", [])
    item_rows = item_payload.get("rows", [])
    table = _nabo_table_record(table_rows[0]) if table_rows else None
    items = [_nabo_item_record(row) for row in item_rows]
    cadence_suggestion = (table or {}).get("dtacycle_cd_suggestion")
    dtacycle_suggestions = _nabo_dtacycle_suggestions(cadence_suggestion)
    markers = []
    if not table:
        markers.append("not_matched")
    if not items:
        markers.append("metadata_failed")
    return {
        "status": "executed" if table or items else "failed",
        "source_system": "NABO",
        "provider": "국회예산정책처 재정경제통계시스템",
        "table_id": statbl_id,
        "table": table,
        "items": items,
        "item_count": len(items),
        "dtacycle_cd_suggestions": dtacycle_suggestions,
        "dtacycle_supported_values": ["YY", "QY", "MM"],
        "dtacycle_guidance": _nabo_dtacycle_guidance(dtacycle_suggestions),
        "query_template": {
            "tool": "query_nabo_table",
            "args": {
                "statbl_id": statbl_id,
                "dtacycle_cd": "auto",
                "period": "latest",
                "filters": {"ITEM": ["<ITM_ID from items>"]},
            },
        },
        "must_know": {
            "source_system": "NABO",
            "item_codes_valid_only_for_table": statbl_id,
            "data_endpoint_requires_dtacycle_cd": True,
            "dtacycle_cd_auto_supported": True,
            "period_range_forms_supported": ["period_range", "2010:2024", "2010-2024", {"start": "2010", "end": "2024"}],
        },
        "mcp_output_contract": _mcp_tool_output_contract(
            role="metadata_inspection",
            final_answer_expected=False,
            markers=markers,
            explanation="NABO metadata describes table/items only; call query_nabo_table for values.",
            extra_signals={
                "source_system": "NABO",
                "table_id": statbl_id,
                "item_count": len(items),
            },
        ),
    }


@mcp.tool()
async def query_nabo_table(
    statbl_id: str,
    dtacycle_cd: str = "auto",
    period: Optional[Any] = None,
    period_range: Optional[list[str]] = None,
    filters: Optional[dict[str, Any]] = None,
    max_rows: int = NABO_DEFAULT_MAX_ROWS,
    api_key: Optional[str] = None,
) -> dict:
    """[📥] NABO 통계표 원자료를 조회하고 공통 row 포맷으로 정규화한다."""
    if not statbl_id:
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "error": "statbl_id is required.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="query_nabo_table requires a NABO STATBL_ID.",
                extra_signals={"source_system": "NABO"},
            ),
        }
    if filters is not None and not isinstance(filters, dict):
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "table_id": statbl_id,
            "validation_errors": [
                {
                    "code": "INVALID_FILTERS_TYPE",
                    "message": "filters must be an object/dict; use null or omit it for no filters.",
                    "received_type": type(filters).__name__,
                }
            ],
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input", "validation_errors"],
                explanation="query_nabo_table received filters in an unsupported shape.",
                extra_signals={"source_system": "NABO", "table_id": statbl_id},
            ),
        }
    unknown_filter_keys = [
        key for key in (filters or {})
        if str(key) not in NABO_FILTER_KEY_MAP
    ]
    if unknown_filter_keys:
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "table_id": statbl_id,
            "validation_errors": [
                {
                    "code": "UNKNOWN_FILTER_KEY",
                    "filter_key": key,
                    "valid_filter_keys": sorted(NABO_FILTER_KEY_MAP),
                }
                for key in unknown_filter_keys
            ],
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input", "validation_errors"],
                explanation="query_nabo_table received unsupported filter keys.",
                extra_signals={"source_system": "NABO", "table_id": statbl_id},
            ),
        }
    try:
        key = _resolve_nabo_key(api_key)
    except RuntimeError as exc:
        if _is_missing_nabo_key_error(exc):
            return _missing_nabo_key_response("query_nabo_table", statbl_id=statbl_id, dtacycle_cd=dtacycle_cd)
        raise

    requested_cycle = dtacycle_cd
    cycle = _nabo_dtacycle_code(dtacycle_cd)
    table_exists: Optional[bool] = None
    table_cadence_name = None
    table_dtacycle_cd = None
    cycle_resolution: dict[str, Any] = {
        "requested": requested_cycle,
        "resolved": cycle,
        "source": "caller",
    }
    dtacycle_suggestions = _nabo_dtacycle_suggestions(None if cycle == "AUTO" else cycle)
    try:
        table_payload = await _nabo_fetch_rows("Sttsapitbl", key, {"STATBL_ID": statbl_id, "pSize": 1}, max_rows=1)
        table_rows = table_payload.get("rows", [])
        table_exists = bool(table_rows)
        table = _nabo_table_record(table_rows[0]) if table_rows else {}
        table_cadence_name = table.get("cadence_name")
        table_dtacycle_cd = table.get("dtacycle_cd_suggestion")
        if table_dtacycle_cd:
            dtacycle_suggestions = _nabo_dtacycle_suggestions(table_dtacycle_cd)
        if table_exists is False:
            return {
                "status": "not_matched",
                "source_system": "NABO",
                "table_id": statbl_id,
                "dtacycle_cd": None if cycle == "AUTO" else cycle,
                "dtacycle_cd_requested": requested_cycle,
                "dtacycle_resolution": {
                    "requested": requested_cycle,
                    "resolved": None,
                    "source": "table_metadata",
                    "table_found": False,
                },
                "row_count": 0,
                "rows": [],
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_extraction",
                    final_answer_expected=False,
                    markers=["not_matched", "not_matched_table"],
                    explanation="NABO table id was not found in table metadata.",
                    extra_signals={"source_system": "NABO", "table_id": statbl_id},
                ),
            }
        if cycle == "AUTO":
            suggested_cycle = table_dtacycle_cd
            cycle = suggested_cycle or "YY"
            cycle_resolution = {
                "requested": requested_cycle,
                "resolved": cycle,
                "source": "table_metadata" if suggested_cycle else "fallback_default",
                "table_cadence_name": table_cadence_name,
            }
            dtacycle_suggestions = _nabo_dtacycle_suggestions(suggested_cycle)
        elif table_dtacycle_cd and cycle != table_dtacycle_cd:
            return {
                "status": "period_type_incompatible",
                "source_system": "NABO",
                "table_id": statbl_id,
                "dtacycle_cd": cycle,
                "dtacycle_cd_requested": requested_cycle,
                "dtacycle_resolution": {
                    **cycle_resolution,
                    "table_cadence_name": table_cadence_name,
                    "table_dtacycle_cd": table_dtacycle_cd,
                    "compatible": False,
                },
                "dtacycle_cd_suggestions": _nabo_dtacycle_suggestions(table_dtacycle_cd),
                "dtacycle_supported_values": ["YY", "QY", "MM"],
                "period_type_check": {
                    "table_supports": [table_dtacycle_cd],
                    "requested": cycle,
                    "compatible": False,
                    "fallback_suggestion": f"use dtacycle_cd='{table_dtacycle_cd}' or dtacycle_cd='auto'",
                },
                "row_count": 0,
                "rows": [],
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_extraction",
                    final_answer_expected=False,
                    markers=["period_type_mismatch", "dtacycle_mismatch", "invalid_input"],
                    explanation="Requested NABO dtacycle_cd is not compatible with this table's metadata cadence.",
                    extra_signals={
                        "source_system": "NABO",
                        "table_id": statbl_id,
                        "table_dtacycle_cd": table_dtacycle_cd,
                        "requested_dtacycle_cd": cycle,
                    },
                ),
            }
    except Exception as exc:
        if cycle == "AUTO":
            cycle = "YY"
            cycle_resolution = {
                "requested": requested_cycle,
                "resolved": cycle,
                "source": "fallback_default_after_metadata_error",
                "metadata_error": str(exc),
            }
            dtacycle_suggestions = _nabo_dtacycle_suggestions(cycle)
        else:
            cycle_resolution["metadata_error"] = str(exc)

    try:
        period_request = _nabo_parse_period_request(period, period_range, cycle=cycle)
    except Exception as exc:
        period_request = {
            "source": "period_range" if period_range not in (None, []) else "period",
            "raw_input": period_range if period_range not in (None, []) else period,
            "mode": "invalid",
            "api_filter_period": None,
            "normalized_period": None,
            "normalized_range": None,
            "errors": [
                {
                    "code": "PERIOD_REQUEST_NORMALIZATION_ERROR",
                    "message": str(exc),
                }
            ],
            "accepted_forms": {
                "latest": "latest",
                "single": "2024",
                "range_array": ["2010", "2024"],
                "range_colon": "2010:2024",
                "range_hyphen": "2010-2024",
                "range_object": {"start": "2010", "end": "2024"},
            },
        }
    if period_request.get("errors"):
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "table_id": statbl_id,
            "dtacycle_cd": cycle,
            "dtacycle_cd_requested": requested_cycle,
            "dtacycle_resolution": cycle_resolution,
            "period_requested": period,
            "period_range_requested": period_range,
            "period_request": period_request,
            "validation_errors": period_request.get("errors"),
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input", "period_request_normalization_error", "validation_errors"],
                explanation="query_nabo_table received a period request that could not be normalized.",
                extra_signals={
                    "source_system": "NABO",
                    "table_id": statbl_id,
                    "period_request": period_request,
                },
            ),
        }
    params: dict[str, Any] = {
        "STATBL_ID": statbl_id,
        "DTACYCLE_CD": cycle,
    }
    if period_request.get("api_filter_period"):
        params["WRTTIME_IDTFR_ID"] = period_request["api_filter_period"]
    try:
        payload = await _nabo_fetch_rows("Sttsapitbldata", key, params, max_rows=max_rows)
    except Exception as exc:
        return {
            "status": "failed",
            "source_system": "NABO",
            "table_id": statbl_id,
            "dtacycle_cd": cycle,
            "error": str(exc),
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["fanout_call_failed"],
                explanation="NABO data lookup failed during OpenAPI access.",
                extra_signals={
                    "source_system": "NABO",
                    "table_id": statbl_id,
                    "dtacycle_resolution": cycle_resolution,
                    "period_request": period_request,
                },
            ),
        }

    api_rows = payload.get("rows", [])
    try:
        period_rows = _nabo_apply_period_request(api_rows, period_request)
    except Exception as exc:
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "table_id": statbl_id,
            "dtacycle_cd": cycle,
            "dtacycle_cd_requested": requested_cycle,
            "dtacycle_resolution": cycle_resolution,
            "period_requested": period,
            "period_range_requested": period_range,
            "period_request": {
                **period_request,
                "errors": [
                    *(period_request.get("errors") or []),
                    {
                        "code": "PERIOD_REQUEST_APPLICATION_ERROR",
                        "message": str(exc),
                    },
                ],
            },
            "validation_errors": [
                {
                    "code": "PERIOD_REQUEST_APPLICATION_ERROR",
                    "message": str(exc),
                }
            ],
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input", "period_request_normalization_error", "validation_errors"],
                explanation="query_nabo_table could not apply the normalized period request.",
                extra_signals={
                    "source_system": "NABO",
                    "table_id": statbl_id,
                    "period_request": period_request,
                },
            ),
        }
    filtered_rows, filter_errors = _nabo_filter_data_rows(period_rows, filters)
    filter_coverage = _nabo_filter_coverage(period_rows, filters)
    if filter_errors:
        return {
            "status": "unsupported",
            "source_system": "NABO",
            "table_id": statbl_id,
            "dtacycle_cd": cycle,
            "dtacycle_cd_requested": requested_cycle,
            "dtacycle_resolution": cycle_resolution,
            "period_request": period_request,
            "validation_errors": filter_errors,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_extraction",
                final_answer_expected=False,
                markers=["invalid_input", "validation_errors"],
                explanation="query_nabo_table filters failed validation.",
                extra_signals={
                    "source_system": "NABO",
                    "table_id": statbl_id,
                    "dtacycle_resolution": cycle_resolution,
                    "period_request": period_request,
                },
            ),
        }

    item_meta_error = None
    item_meta_map: dict[str, dict[str, Any]] = {}
    try:
        item_meta_map = await _nabo_item_meta_map(statbl_id, key)
        if table_exists is None and item_meta_map:
            table_exists = True
    except Exception as exc:
        item_meta_error = str(exc)
    if table_exists is None and not api_rows:
        try:
            table_check = await _nabo_fetch_rows("Sttsapitbl", key, {"STATBL_ID": statbl_id, "pSize": 1}, max_rows=1)
            table_exists = bool(table_check.get("rows") or [])
        except Exception:
            table_exists = None

    rows = [_nabo_data_record(row, item_meta_map) for row in filtered_rows]
    latest_period_in_returned_rows = max((row.get("period") or "" for row in rows), default=None)
    latest_period = None if payload.get("truncated") else latest_period_in_returned_rows
    markers = []
    if period_request.get("mode") in {"single", "range", "latest"}:
        markers.append("period_input_normalized")
    if cycle_resolution.get("source") != "caller":
        markers.append("dtacycle_auto_resolved")
    if item_meta_map:
        markers.append("item_metadata_joined")
    if item_meta_error:
        markers.append("metadata_partial")
    if filter_coverage.get("missing_filter_values"):
        markers.append("partial_filter_match")
    if not rows:
        markers.append("empty_rows")
        if not api_rows:
            if table_exists is False:
                markers.extend(["not_matched", "not_matched_table"])
            else:
                markers.append("complete_fanout_miss")
        elif not period_rows:
            markers.append("period_filter_empty")
        else:
            markers.append("filter_no_match")
    missing_value_examples = [
        {
            "period": row.get("period"),
            "item": ((row.get("dimensions") or {}).get("ITEM") or {}).get("label"),
            "item_code": ((row.get("dimensions") or {}).get("ITEM") or {}).get("code"),
            "class": ((row.get("dimensions") or {}).get("CLASS") or {}).get("label"),
            "class_code": ((row.get("dimensions") or {}).get("CLASS") or {}).get("code"),
            "unit": row.get("unit"),
            "symbol": row.get("symbol"),
        }
        for row in rows
        if row.get("value") is None
    ][:10]
    missing_value_count = sum(1 for row in rows if row.get("value") is None)
    if missing_value_count:
        markers.append("missing_values")
        if missing_value_count == len(rows):
            markers.append("all_values_missing")
    if payload.get("truncated"):
        markers.append("truncated_by_max_rows")
    status = "executed" if rows else "empty"
    return {
        "status": status,
        "source_system": "NABO",
        "provider": "국회예산정책처 재정경제통계시스템",
        "table_id": statbl_id,
        "dtacycle_cd": cycle,
        "dtacycle_cd_requested": requested_cycle,
        "dtacycle_resolution": cycle_resolution,
        "dtacycle_cd_suggestions": dtacycle_suggestions,
        "dtacycle_supported_values": ["YY", "QY", "MM"],
        "dtacycle_guidance": _nabo_dtacycle_guidance(dtacycle_suggestions),
        "period_requested": period,
        "period_range_requested": period_range,
        "period_request": period_request,
        "latest_period": latest_period,
        "latest_period_in_returned_rows": latest_period_in_returned_rows,
        "latest_period_is_complete": not bool(payload.get("truncated")),
        "row_count": len(rows),
        "source_row_count": len(api_rows),
        "period_filtered_row_count": len(period_rows),
        "item_metadata_joined": bool(item_meta_map),
        "item_metadata_error": item_meta_error,
        "total_count": payload.get("total_count"),
        "max_rows": payload.get("max_rows"),
        "truncated": payload.get("truncated", False),
        "filters_applied": filters or {},
        "filter_coverage": filter_coverage,
        "missing_value_count": missing_value_count,
        "missing_value_examples": missing_value_examples,
        "rows": rows,
        "must_know": {
            "source_system": "NABO",
            "provider": "국회예산정책처 재정경제통계시스템",
            "raw_extraction_only": True,
            "unit_from_ui_nm": True,
            "do_not_label_as_kosis": True,
            "dtacycle_cd_resolved": cycle,
            "period_request_mode": period_request.get("mode"),
            "item_full_name_joined_from_explore_metadata": bool(item_meta_map),
            "period_range_forms_supported": ["period_range", "2010:2024", "2010-2024", {"start": "2010", "end": "2024"}],
        },
        "mcp_output_contract": _mcp_tool_output_contract(
            role="raw_extraction",
            final_answer_expected=False,
            markers=markers,
            explanation="query_nabo_table returns NABO raw rows normalized for downstream LLM analysis.",
            extra_signals={
                "source_system": "NABO",
                "table_id": statbl_id,
                "row_count": len(rows),
                "latest_period": latest_period,
                "latest_period_in_returned_rows": latest_period_in_returned_rows,
                "latest_period_is_complete": not bool(payload.get("truncated")),
                "truncated": payload.get("truncated", False),
                "truncated_by_max_rows": payload.get("truncated", False),
                "missing_value_count": missing_value_count,
                "missing_value_examples": missing_value_examples,
                "dtacycle_resolution": cycle_resolution,
                "period_request": period_request,
                "period_filtered_row_count": len(period_rows),
                "filter_coverage": filter_coverage,
                "item_metadata_joined": bool(item_meta_map),
                "item_metadata_error": item_meta_error,
                "table_exists": table_exists,
            },
        ),
    }


@mcp.tool()
async def search_nabo_terms(
    term: str,
    limit: int = 10,
    api_key: Optional[str] = None,
) -> dict:
    """[📖] NABOSTATS 통계 용어사전을 검색한다."""
    try:
        key = _resolve_nabo_key(api_key)
    except RuntimeError as exc:
        if _is_missing_nabo_key_error(exc):
            return _missing_nabo_key_response("search_nabo_terms", term=term, limit=limit)
        raise
    safe_limit = max(1, min(int(limit or 10), NABO_MAX_PAGE_SIZE))
    try:
        payload = await _nabo_fetch_rows(
            "DicApiList",
            key,
            {"DIC_TITLE": term, "pIndex": 1, "pSize": safe_limit},
            max_rows=safe_limit,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "source_system": "NABO",
            "term": term,
            "error": str(exc),
            "mcp_output_contract": _mcp_tool_output_contract(
                role="dictionary_search",
                final_answer_expected=False,
                markers=["fanout_call_failed"],
                explanation="NABO dictionary search failed during OpenAPI access.",
                extra_signals={"source_system": "NABO"},
            ),
        }
    terms = [
        {
            "source_system": "NABO",
            "term_id": str(row.get("DIC_SEQ") or ""),
            "title": row.get("DIC_TITLE"),
            "content": row.get("DIC_CONTENT"),
            "raw": row,
        }
        for row in payload.get("rows", [])
    ]
    markers = ["search_empty"] if not terms else []
    return {
        "status": "executed" if terms else "empty",
        "source_system": "NABO",
        "term": term,
        "result_count": len(terms),
        "total_count": payload.get("total_count", len(terms)),
        "terms": terms,
        "mcp_output_contract": _mcp_tool_output_contract(
            role="dictionary_search",
            final_answer_expected=False,
            markers=markers,
            explanation="NABO dictionary search returns term definitions only.",
            extra_signals={"source_system": "NABO", "result_count": len(terms)},
        ),
    }


@mcp.tool()
async def search_stats(
    query: str,
    source: str = "all",
    limit: int = 10,
    kosis_api_key: Optional[str] = None,
    nabo_api_key: Optional[str] = None,
) -> dict:
    """[🔎] KOSIS/NABO 통합 통계표 후보 검색 facade."""
    source_norm = str(source or "all").lower()
    if source_norm not in {"all", "kosis", "nabo"}:
        return {
            "status": "unsupported",
            "error": "source must be one of all, kosis, nabo.",
            "source": source,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="table_search",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="search_stats source must be all, kosis, or nabo.",
            ),
        }

    results: list[dict[str, Any]] = []
    source_payloads: dict[str, Any] = {}
    if source_norm in {"all", "kosis"}:
        kosis_payload = await search_kosis(query, limit=limit, api_key=kosis_api_key)
        source_payloads["kosis"] = kosis_payload
        for row in (kosis_payload.get("결과") or [])[:limit]:
            results.append({
                "source_system": "KOSIS",
                "provider": "통계청 KOSIS",
                "table_id": row.get("통계표ID") or row.get("TBL_ID"),
                "org_id": row.get("기관ID") or row.get("ORG_ID"),
                "table_name": row.get("통계표명") or row.get("TBL_NM"),
                "raw": row,
            })
    if source_norm in {"all", "nabo"}:
        nabo_payload = await search_nabo_tables(query, limit=limit, api_key=nabo_api_key)
        source_payloads["nabo"] = nabo_payload
        results.extend((nabo_payload.get("tables") or [])[:limit])

    markers = []
    if not results:
        markers.append("search_empty")
    missing_sources = [
        name for name, payload in source_payloads.items()
        if isinstance(payload, dict) and payload.get("code") == STATUS_MISSING_API_KEY
    ]
    if missing_sources:
        markers.append("missing_api_key")
    source_systems_present = sorted({
        str(row.get("source_system"))
        for row in results
        if row.get("source_system")
    })
    cross_source_definition_check_required = (
        source_norm == "all"
        and "KOSIS" in source_systems_present
        and "NABO" in source_systems_present
    )
    if cross_source_definition_check_required:
        markers.append("cross_source_definition_check_required")
    return {
        "status": "executed" if results else "empty",
        "query": query,
        "source": source_norm,
        "result_count": len(results),
        "results": results,
        "source_payloads": source_payloads,
        "must_know": {
            "mixed_sources": source_norm == "all",
            "caller_must_preserve_source_system": True,
            "kosis_and_nabo_tables_are_not_interchangeable": True,
            "source_systems_present": source_systems_present,
            "definition_comparison_required": cross_source_definition_check_required,
            "do_not_treat_cross_source_results_as_equivalent": cross_source_definition_check_required,
        },
        "mcp_output_contract": _mcp_tool_output_contract(
            role="table_search",
            final_answer_expected=False,
            markers=markers,
            explanation="search_stats merges table candidates but does not claim cross-source equivalence.",
            extra_signals={
                "source": source_norm,
                "result_count": len(results),
                "missing_sources": missing_sources,
                "source_systems_present": source_systems_present,
                "definition_comparison_required": cross_source_definition_check_required,
            },
        ),
    }














@mcp.tool()
async def resolve_concepts(
    org_id: str,
    tbl_id: str,
    concepts: list[str],
    axes_to_search: Optional[list[str]] = None,
    limit: int = 5,
    api_key: Optional[str] = None,
) -> dict:
    """[🧩] 자연어 개념을 특정 KOSIS 표의 메타 코드 후보로 해석한다.

    이 도구는 수도권·산업군·연령대 같은 도메인 정의를 자체 사전으로
    만들지 않는다. LLM이 정한 개념 문자열을 표의 OBJ_ID/ITM_NM/ITM_ID
    메타와 비교해 후보 코드만 반환한다. 같은 표현도 표마다 코드가 다를
    수 있으므로 결과는 valid_only_for(tbl_id)로만 사용해야 한다.
    """
    if not org_id or not tbl_id:
        return {
            "상태": "failed",
            "status": "unsupported",
            "error": "org_id and tbl_id are required.",
            "오류": "org_id와 tbl_id가 필요합니다.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="concept_resolution",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="resolve_concepts requires org_id and tbl_id before metadata lookup.",
            ),
        }
    if not isinstance(concepts, list) or not concepts:
        return {
            "상태": "failed",
            "status": "unsupported",
            "error": "concepts must be a non-empty list.",
            "오류": "concepts는 비어 있지 않은 리스트여야 합니다.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="concept_resolution",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="resolve_concepts cannot resolve an empty concepts list.",
                extra_signals={"concept_count": 0},
            ),
        }

    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response(
                "resolve_concepts",
                org_id=org_id,
                tbl_id=tbl_id,
                concepts=concepts,
            )
        raise
    async with httpx.AsyncClient() as client:
        try:
            name_rows, item_rows = await asyncio.gather(
                _fetch_meta(client, key, org_id, tbl_id, "TBL"),
                _fetch_meta(client, key, org_id, tbl_id, "ITM"),
            )
        except Exception as exc:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_STAT_NOT_FOUND,
                "code": STATUS_STAT_NOT_FOUND,
                "error": f"Metadata lookup failed: {exc}",
                "오류": f"메타 조회 실패: {exc}",
                "org_id": org_id,
                "tbl_id": tbl_id,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="concept_resolution",
                    final_answer_expected=False,
                    markers=["metadata_failed", "not_matched"],
                    explanation="resolve_concepts could not load table metadata; caller should verify org_id/tbl_id or search again.",
                    extra_signals={"org_id": org_id, "tbl_id": tbl_id},
                ),
            }
    if not isinstance(item_rows, list) or not item_rows:
        return {
            "상태": "failed",
            "status": "unsupported",
            "코드": STATUS_STAT_NOT_FOUND,
            "code": STATUS_STAT_NOT_FOUND,
            "error": "No classification metadata is available.",
            "오류": "분류축 메타가 없습니다.",
            "org_id": org_id,
            "tbl_id": tbl_id,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="concept_resolution",
                final_answer_expected=False,
                markers=["metadata_failed", "not_matched"],
                explanation="No classification metadata is available, so concepts cannot be resolved for this table.",
                extra_signals={"org_id": org_id, "tbl_id": tbl_id},
            ),
        }

    axes, axis_order = _build_axis_codebook(item_rows)
    allowed_axes = set(axes_to_search or [])
    if allowed_axes:
        allowed_axes.update(
            obj_id
            for obj_id, axis in axes.items()
            if str(axis.get("OBJ_NM") or "") in allowed_axes
        )
    table_name = None
    if isinstance(name_rows, list) and name_rows:
        table_name = name_rows[0].get("TBL_NM") or name_rows[0].get("tblNm")

    matches_by_concept: list[dict[str, Any]] = []
    filters: dict[str, list[str]] = {}
    unresolved: list[str] = []
    ambiguities: list[dict[str, Any]] = []
    resolved_context: list[dict[str, Any]] = []

    for concept in concepts:
        concept_matches: list[dict[str, Any]] = []
        for obj_id in axis_order:
            if allowed_axes and obj_id not in allowed_axes:
                continue
            axis = axes[obj_id]
            for code, meta in (axis.get("items") or {}).items():
                score = max(
                    _concept_match_score(str(concept), meta.get("label"), code),
                    _concept_match_score(str(concept), meta.get("label_en"), code),
                )
                if score <= 0:
                    continue
                concept_matches.append({
                    "score": score,
                    "OBJ_ID": obj_id,
                    "OBJ_NM": axis.get("OBJ_NM"),
                    "ITM_ID": code,
                    "ITM_NM": meta.get("label"),
                    "UNIT_NM": meta.get("unit"),
                    "UP_ITM_ID": meta.get("parent"),
                })
        concept_matches.sort(key=lambda row: (-int(row["score"]), str(row["OBJ_ID"]), str(row["ITM_ID"])))
        parent_context = {
            str(item.get("ITM_ID"))
            for item in resolved_context
            if item.get("ITM_ID")
        }
        context_best = None
        if parent_context:
            context_best = next(
                (
                    row for row in concept_matches
                    if str(row.get("UP_ITM_ID") or "") in parent_context and int(row.get("score") or 0) >= 60
                ),
                None,
            )
            if context_best is not None:
                concept_matches = [
                    context_best,
                    *[
                        row for row in concept_matches
                        if row.get("OBJ_ID") != context_best.get("OBJ_ID")
                        or row.get("ITM_ID") != context_best.get("ITM_ID")
                    ],
                ]
        top = concept_matches[:limit]
        matches_by_concept.append({"concept": concept, "matches": top})
        if not top:
            unresolved.append(str(concept))
            continue
        best = dict(top[0])
        selection_reason = "context_aware_parent_match" if context_best is not None else "highest_score_then_axis_code"
        context_evidence = None
        if context_best is not None:
            parent_id = str(context_best.get("UP_ITM_ID") or "")
            parent = next((item for item in resolved_context if str(item.get("ITM_ID")) == parent_id), None)
            if parent:
                context_evidence = (
                    f"previous concept {parent.get('concept')!r} matched ITM_ID={parent_id}; "
                    f"current candidate has UP_ITM_ID={parent_id}"
                )
            else:
                context_evidence = f"current candidate has UP_ITM_ID={parent_id}"
        best["best_selection_reason"] = selection_reason
        best["context_evidence"] = context_evidence
        best["disambiguation_strategy"] = {
            "primary": "context_aware_parent_match",
            "fallback": "highest_score_then_axis_code",
            "caller_can_override": "by specifying the parent concept first or constraining axes_to_search",
        }
        close = [
            row for row in top
            if row["score"] >= best["score"] - 5 and (
                row["OBJ_ID"] != best["OBJ_ID"] or row["ITM_ID"] != best["ITM_ID"]
            )
        ]
        if close:
            ambiguities.append({"concept": concept, "best": best, "also_possible": close})
        if best["score"] >= 60:
            filters.setdefault(str(best["OBJ_ID"]), [])
            if best["ITM_ID"] not in filters[str(best["OBJ_ID"])]:
                filters[str(best["OBJ_ID"])].append(str(best["ITM_ID"]))
            resolved_context.append({
                "concept": concept,
                "OBJ_ID": best.get("OBJ_ID"),
                "ITM_ID": best.get("ITM_ID"),
                "ITM_NM": best.get("ITM_NM"),
            })

    status = "resolved" if filters and not unresolved else "partial" if filters else "needs_concept_selection"
    concept_markers: list[str] = []
    if status == "needs_concept_selection":
        concept_markers.append("not_matched")
    elif status == "partial":
        concept_markers.append("concept_partial_resolution")
    if unresolved:
        concept_markers.append("concept_unresolved")
    if ambiguities:
        concept_markers.append("concept_ambiguous")
    concept_explanation = (
        "No concept was resolved to a metadata code; caller must refine the concept strings or pick from matches_by_concept."
        if status == "needs_concept_selection" else
        "Some concepts were resolved while others remain unresolved or ambiguous; inspect unresolved/ambiguities before using filters."
        if status == "partial" or unresolved or ambiguities else
        "All concepts resolved to metadata codes valid for this table."
    )
    return {
        "상태": status,
        "status": status,
        "verification_level": "metadata_match",
        "confidence": "medium" if status == "resolved" else "low",
        "org_id": org_id,
        "tbl_id": tbl_id,
        "table_name": table_name,
        "valid_only_for": tbl_id,
        "concepts": concepts,
        "filters": filters,
        "matches_by_concept": matches_by_concept,
        "unresolved": unresolved,
        "ambiguities": ambiguities,
        "available_axes": [
            {"OBJ_ID": obj_id, "OBJ_NM": axes[obj_id].get("OBJ_NM"), "item_count": len(axes[obj_id].get("items") or {})}
            for obj_id in axis_order
        ],
        "mcp_output_contract": _mcp_tool_output_contract(
            role="concept_resolution",
            final_answer_expected=False,
            markers=concept_markers,
            explanation=concept_explanation,
            extra_signals={
                "resolved_concept_count": sum(1 for entry in matches_by_concept if entry.get("matches")),
                "unresolved_concept_count": len(unresolved),
                "ambiguous_concept_count": len(ambiguities),
                "valid_only_for": tbl_id,
            },
        ),
        "주의": [
            "resolve_concepts는 표 메타 안에서 문자열 후보를 찾을 뿐, 수도권·산업군·가법성 같은 도메인 정의를 만들지 않습니다.",
            "filters는 같은 tbl_id에만 유효합니다. 다른 표에는 재사용하지 마세요.",
        ],
    }


@mcp.tool()
async def select_table_for_query(
    query: str,
    required_dimensions: Optional[list[str]] = None,
    indicator: Optional[str] = None,
    search_terms: Optional[list[str]] = None,
    infer_dimensions: bool = False,
    reject_if_missing_dimensions: bool = True,
    limit: int = 8,
    api_key: Optional[str] = None,
) -> dict:
    """[🧭] 자연어 질문에 맞는 KOSIS 통계표 후보를 메타데이터 기반으로 고른다.

    이 도구는 특정 질문/통계표 ID를 하드코딩하지 않는다. LLM이 넘긴
    required_dimensions를 기준으로 search_kosis 후보 표의 ITM/PRD 메타를
    확인해 필요한 분류축(region, industry, age, sex, time 등)을 만족하는지
    점수화한다. 실제 코드 매핑과 값 조회는 resolve_concepts/query_table
    단계의 책임이다. infer_dimensions는 legacy 보조 옵션이며 기본값은 False다.
    """
    inferred_dimensions = _infer_required_dimensions_from_query(query) if infer_dimensions else []
    required = _normalize_required_dimensions([
        *(required_dimensions or []),
        *inferred_dimensions,
    ])
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response(
                "select_table_for_query",
                query=query,
                required_dimensions=required,
                indicator=indicator,
            )
        raise
    explicit_indicator = indicator.strip() if isinstance(indicator, str) and indicator.strip() else None
    query_text = query.strip() if isinstance(query, str) else ""
    if explicit_indicator:
        effective_indicator, indicator_source = explicit_indicator, "explicit"
    elif query_text:
        effective_indicator, indicator_source = query_text, "query_fallback"
    else:
        effective_indicator, indicator_source = None, "none"
    indicator_raw_input = effective_indicator
    indicator_normalization = None
    if effective_indicator:
        indicator_normalization = await _normalize_indicator_from_kosis_meta(effective_indicator, api_key=key)
        if indicator_normalization.get("source") == "kosis_meta_match":
            effective_indicator = str(indicator_normalization.get("normalized") or effective_indicator)
    search_queries = [
        term.strip()
        for term in [query, *(search_terms or []), explicit_indicator or "", effective_indicator or ""]
        if isinstance(term, str) and term.strip()
    ]
    search_queries = list(dict.fromkeys(search_queries))
    searches = await asyncio.gather(*[
        search_kosis(term, limit=limit, use_routing=True, api_key=key)
        for term in search_queries
    ])
    raw_candidates: list[dict[str, Any]] = []
    for term, search in zip(search_queries, searches):
        tier_a = search.get("Tier_A_직접_매핑") or search.get("Tier_A_吏곸젒_留ㅽ븨")
        tier_tbl_id = _row_first(tier_a, ["통계표ID", "?듦퀎?쏧D", "TBL_ID"]) if isinstance(tier_a, dict) else None
        tier_org_id = _row_first(tier_a, ["기관ID", "湲곌?ID", "ORG_ID"]) if isinstance(tier_a, dict) else None
        if isinstance(tier_a, dict) and tier_tbl_id:
            raw_candidates.append({
                "통계표명": _row_first(tier_a, ["통계표", "?듦퀎??", "통계표명", "TBL_NM"]),
                "통계표ID": tier_tbl_id,
                "기관ID": tier_org_id,
                "source": "tier_a_direct_mapping",
                "search_term": term,
            })
        for row in _search_result_rows(search):
            row_tbl_id = _row_first(row, ["통계표ID", "?듦퀎?쏧D", "TBL_ID"])
            row_org_id = _row_first(row, ["기관ID", "湲곌?ID", "ORG_ID"])
            if row_tbl_id and row_org_id:
                raw_candidates.append({
                    **row,
                    "통계표ID": row_tbl_id,
                    "기관ID": row_org_id,
                    "통계표명": _row_first(row, ["통계표명", "?듦퀎?쒕챸", "TBL_NM"]),
                    "source": row.get("검색어") or "search_kosis",
                    "search_term": term,
                })

    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, Any]] = []
    scorer = MetadataCompatibilityScorer(
        required_dimensions=required,
        indicator=effective_indicator,
        reject_if_missing_dimensions=reject_if_missing_dimensions,
    )
    async with httpx.AsyncClient() as client:
        for row in raw_candidates:
            org_id = str(row.get("기관ID") or "")
            tbl_id = str(row.get("통계표ID") or "")
            if not org_id or not tbl_id or (org_id, tbl_id) in seen:
                continue
            seen.add((org_id, tbl_id))
            try:
                name_rows, item_rows, period_rows = await asyncio.gather(
                    _fetch_meta(client, key, org_id, tbl_id, "TBL"),
                    _fetch_meta(client, key, org_id, tbl_id, "ITM"),
                    _fetch_meta(client, key, org_id, tbl_id, "PRD"),
                )
            except Exception as exc:
                candidates.append({
                    "org_id": org_id,
                    "tbl_id": tbl_id,
                    "table_name": row.get("통계표명"),
                    "status": "metadata_failed",
                    "error": str(exc),
                    "source": row.get("source"),
                    "search_term": row.get("search_term"),
                })
                continue
            profile = TableMetadataProfile.from_rows(
                org_id=org_id,
                tbl_id=tbl_id,
                candidate_row=row,
                name_rows=name_rows,
                item_rows=item_rows,
                period_rows=period_rows,
            )
            candidates.append(scorer.evaluate(profile).to_response())
    _annotate_table_candidate_ranking(candidates)
    candidates.sort(key=_table_candidate_sort_key)
    selected = [c for c in candidates if c.get("status") == "selected"]
    if selected:
        selected[0]["selection_reasons"] = _table_selection_reasons(selected[0], selected)
    warnings = []
    if not required:
        warnings.append(
            "required_dimensions is empty; table selection can only use search/routing evidence and cannot verify axis coverage."
        )
    selection_markers: list[str] = []
    if not selected:
        selection_markers.append("not_matched")
    if any(c.get("status") == "not_matched_indicator" for c in candidates):
        selection_markers.append("indicator_evidence_empty")
    if isinstance(indicator_normalization, dict) and indicator_normalization.get("source") == "kosis_meta_match":
        selection_markers.append("indicator_normalized")
    if indicator_source == "query_fallback":
        selection_markers.append("indicator_inferred_from_query")
        warnings.append(
            "indicator 인자가 명시되지 않아 query 전체를 indicator 증거 매칭에 사용했습니다. "
            "후보가 부정확하거나 selected가 비면 더 구체적인 indicator를 명시해 재호출하세요."
        )
    elif indicator_source == "none":
        warnings.append(
            "indicator와 query가 모두 비어 있어 indicator 증거 검증이 수행되지 않았습니다."
        )
    explanation = (
        "No candidate satisfied both metadata dimensions and indicator evidence."
        if not selected else
        "Some candidates were rejected because indicator evidence was empty."
        if "indicator_evidence_empty" in selection_markers else
        "A metadata-compatible table candidate was selected."
    )
    return {
        "상태": "selected" if selected else "needs_table_selection",
        "status": "selected" if selected else "needs_table_selection",
        "query": query,
        "indicator": indicator,
        "indicator_used": effective_indicator,
        "indicator_raw_input": indicator_raw_input,
        "indicator_normalization": indicator_normalization,
        "indicator_source": indicator_source,
        "search_terms_used": search_queries,
        "required_dimensions": required,
        "infer_dimensions": infer_dimensions,
        "inferred_dimensions": _normalize_required_dimensions(inferred_dimensions),
        "reject_if_missing_dimensions": reject_if_missing_dimensions,
        "selected": selected[0] if selected else None,
        "alternatives": candidates[:limit],
        "rejected": [c for c in candidates if c.get("status") != "selected"][:limit],
        "ranking_criteria": [
            {"order": 1, "field": "indicator_score_band", "applied": True},
            {"order": 2, "field": "latest_period_year", "applied": True},
            {"order": 3, "field": "indicator_score", "applied": True},
            {"order": 4, "field": "cadence_diversity", "applied": True},
            {"order": 5, "field": "item_count", "applied": True},
            {"order": 6, "field": "score", "applied": True},
        ],
        "warnings": warnings,
        "mcp_output_contract": _mcp_tool_output_contract(
            role="table_selection",
            final_answer_expected=False,
            markers=selection_markers,
            explanation=explanation,
            extra_signals={
                "selected_count": len(selected),
                "candidate_count": len(candidates),
                "indicator_required": effective_indicator is not None,
                "indicator_source": indicator_source,
            },
        ),
        "주의": [
            "select_table_for_query는 표 후보와 축 충족 여부만 판단합니다.",
            "required_dimensions가 비어 있으면 축 충족 검증이 약해집니다. 가능하면 LLM이 필요한 차원을 명시하세요.",
            "ITM_ID/OBJ_ID 코드 매핑은 resolve_concepts 또는 explore_table 이후 query_table로 수행하세요.",
        ],
    }


@mcp.tool()
async def curation_status(detail: bool = False) -> dict:
    """[🛠] 큐레이션 데이터 현황 조회.

    Args:
        detail: True면 broken/needs_check 항목 상세 목록까지.
    """
    summary = _curation_stats_summary()
    if not detail:
        return summary

    by_status: dict[str, list] = {"verified": [], "needs_check": [], "broken": [], "unverified": []}
    for key, p in TIER_A_STATS.items():
        by_status.setdefault(p.verification_status, []).append({
            "키": key,
            "통계표": p.tbl_nm,
            "설명": p.description,
            "메모": p.note,
        })
    summary["상세"] = by_status
    summary["주의"] = (
        "broken = KOSIS 호출 실패 확정. quick_stat 시 자동 폴백. "
        "needs_check = 파라미터 보정 필요. discover_metadata.py로 메타조회 후 수정 가능."
    )
    return summary


@mcp.tool()
async def check_stat_availability(
    query: str,
    live_period_check: bool = False,
    api_key: Optional[str] = None,
) -> dict:
    """[🛠] 특정 통계가 즉시 호출 가능한지 미리 확인.

    챗봇이 "X 통계 알려줘" 요청을 받기 전에, X가 Tier A 매핑되고
    검증되어 있는지 확인. broken/needs_check면 대안 안내.

    Args:
        query: 통계 키워드 ("인구", "소상공인", "출산율" 등)
        live_period_check: True면 KOSIS 메타 API(getMeta&type=PRD)를
            호출해 통계표의 실제 최신 수록 시점과 노화도를 함께 반환.
            False면 curation 메모(스냅샷)만 반환. 추가 호출이 발생하므로
            대량 호출 시에는 False 유지.
    """
    p = _curation_lookup(query)
    if not p:
        hints = _routing_hints(query)
        return {
            "쿼리": query,
            "Tier_A_매핑": False,
            "권고": (
                "Tier A 정밀 매핑 없음. search_kosis로 검색 폴백 사용 권장."
                if hints else
                "Tier A·B 매핑 모두 없음. 쿼리를 더 구체적으로."
            ),
            "Tier_B_추천검색어": hints[:5] if hints else [],
        }

    status_messages = {
        "verified": "✅ 검증됨 — quick_stat으로 즉시 호출 가능",
        "needs_check": "⚠️ 파라미터 보정 필요 — 호출 시도 가능하나 결과 신뢰도 낮음",
        "broken": "❌ 호출 실패 확정 — quick_stat은 폴백으로 작동, KOSIS 사이트에서 신 통계표 ID 확인 필요",
        "unverified": "❓ 미검증 — 호출 시도 가능하나 결과 확인 필수",
    }

    result: dict[str, Any] = {
        "쿼리": query,
        "Tier_A_매핑": True,
        "통계표": p.tbl_nm,
        "통계표ID": p.tbl_id,
        "기관ID": p.org_id,
        "설명": p.description,
        "단위": p.unit,
        "검증_상태": p.verification_status,
        "상태_의미": status_messages.get(p.verification_status, "알 수 없음"),
        "메모": p.note,
        "지원_지역": list(p.region_scheme.keys()) if p.region_scheme else "지역 분류 없음 (전국만)",
        "주기": p.supported_periods,
    }

    if live_period_check and p.verification_status != "broken":
        try:
            period_rows = await _fetch_period_range(p.org_id, p.tbl_id, api_key)
        except Exception as exc:
            result["⚠️ 라이브_수록기간_조회_실패"] = repr(exc)
            return result
        if not period_rows:
            result["⚠️ 라이브_수록기간"] = "KOSIS 메타 API가 수록 시점을 반환하지 않음"
            return result
        latest = _pick_finest_period(period_rows)
        live_period = str(
            latest.get("END_PRD_DE") or latest.get("endPrdDe")
            or latest.get("PRD_DE") or latest.get("prdDe") or ""
        )
        live_se = latest.get("PRD_SE") or latest.get("prdSe")
        start_period = (
            latest.get("STRT_PRD_DE") or latest.get("strtPrdDe") or ""
        )
        # _period_age_years only parses YYYY[MM]; strip "2026 1/4" to
        # "2026" so a quarterly-only table still reports an age.
        parseable = re.match(r"(\d{4})(?:\.(\d{2}))?", live_period)
        age = NaturalLanguageAnswerEngine._period_age_years(
            "".join(filter(None, parseable.groups())) if parseable else live_period
        )
        result["라이브_수록기간"] = {
            "주기": live_se,
            "시작_수록시점": start_period,
            "최신_수록시점": live_period,
            "period_age_years": age,
        }
        # Compare with what the curation note claims, if it carries a snapshot
        snapshot_match = re.search(r"\((\d{4}(?:\.\d{2})?)\s", p.note or "")
        if snapshot_match:
            snapshot_period = snapshot_match.group(1).replace(".", "")
            if live_period and not str(live_period).startswith(snapshot_period[:4]):
                result["⚠️ 메모_vs_KOSIS_drift"] = (
                    f"curation 메모 스냅샷={snapshot_period} → KOSIS 실제 최신={live_period}. "
                    "curation 메모 갱신이 필요할 수 있음."
                )
        if age is not None and age >= 1.0:
            result["⚠️ 데이터_신선도"] = (
                f"KOSIS 실제 최신 시점 {live_period} (약 {age:.1f}년 경과). "
                "이 통계표가 갱신을 멈췄거나 갱신 주기가 깁니다."
            )
    return result


















@mcp.tool()
async def query_table(
    org_id: str,
    tbl_id: str,
    filters: dict[str, Any],
    period_range: Optional[list[str]] = None,
    aggregation: str = "none",
    group_by: Optional[list[str]] = None,
    api_key: Optional[str] = None,
) -> dict:
    """[🧪] 검증된 메타 코드로 KOSIS 표를 raw 조회한다.

    filters는 explore_table/resolve_concepts가 반환한 OBJ_ID와 ITM_ID만 받는다.
    여러 코드는 서버 내부 fan-out으로 조회한다. 기본 aggregation="none"은
    개별 rows만 반환한다. aggregation="sum_by_group"은 호출자가 가법성을
    명시적으로 책임지는 경우에만 합산한다.
    """
    if not org_id or not tbl_id:
        return {
            "상태": "failed",
            "status": "unsupported",
            "error": "org_id and tbl_id are required.",
            "오류": "org_id와 tbl_id가 필요합니다.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_data_query",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="query_table requires org_id and tbl_id before metadata lookup.",
            ),
        }
    if aggregation not in {"none", "sum_by_group"}:
        return {
            "상태": "failed",
            "status": "unsupported",
            "코드": "INVALID_AGGREGATION",
            "code": "INVALID_AGGREGATION",
            "error": "aggregation must be one of: none, sum_by_group.",
            "오류": "aggregation은 none 또는 sum_by_group만 지원합니다.",
            "allowed_aggregations": ["none", "sum_by_group"],
            "mcp_output_contract": _mcp_tool_output_contract(
                role="raw_data_query",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="aggregation must be one of the supported query_table modes.",
                extra_signals={"allowed_aggregations": ["none", "sum_by_group"]},
            ),
        }

    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response(
                "query_table",
                org_id=org_id,
                tbl_id=tbl_id,
                filters=filters,
                period_range=period_range,
            )
        raise
    fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    async with httpx.AsyncClient() as client:
        try:
            name_rows, item_rows, period_rows = await asyncio.gather(
                _fetch_meta(client, key, org_id, tbl_id, "TBL"),
                _fetch_meta(client, key, org_id, tbl_id, "ITM"),
                _fetch_meta(client, key, org_id, tbl_id, "PRD"),
            )
        except Exception as exc:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_STAT_NOT_FOUND,
                "code": STATUS_STAT_NOT_FOUND,
                "error": f"Metadata lookup failed: {exc}",
                "오류": f"메타 조회 실패: {exc}",
                "기관ID": org_id,
                "통계표ID": tbl_id,
                "org_id": org_id,
                "tbl_id": tbl_id,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_data_query",
                    final_answer_expected=False,
                    markers=["metadata_failed", "not_matched"],
                    explanation="query_table could not load table metadata; caller should verify org_id/tbl_id or search again.",
                    extra_signals={"org_id": org_id, "tbl_id": tbl_id},
                ),
            }

        if not isinstance(item_rows, list) or not item_rows:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_STAT_NOT_FOUND,
                "code": STATUS_STAT_NOT_FOUND,
                "error": "No classification metadata is available, so raw extraction cannot be verified.",
                "오류": "분류축 메타가 없어 raw 호출을 검증할 수 없습니다.",
                "기관ID": org_id,
                "통계표ID": tbl_id,
                "org_id": org_id,
                "tbl_id": tbl_id,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_data_query",
                    final_answer_expected=False,
                    markers=["metadata_failed", "not_matched"],
                    explanation="No classification metadata is available, so raw extraction cannot be verified.",
                    extra_signals={"org_id": org_id, "tbl_id": tbl_id},
                ),
            }

        axes, axis_order = _build_axis_codebook(item_rows)
        normalized_filters, errors, auto_defaults = _validate_query_table_filters(filters, axes, axis_order)
        metadata_source = {
            "org_id": org_id,
            "tbl_id": tbl_id,
            "source": "statisticsData.do?method=getMeta&type=TBL/ITM/PRD",
            "fetched_at": fetched_at,
            "metadata_tool": "explore_table",
            "url": _kosis_view_url(org_id, tbl_id),
        }
        if errors or normalized_filters is None:
            available_axes = [
                {"OBJ_ID": obj_id, "OBJ_NM": axes[obj_id].get("OBJ_NM")}
                for obj_id in axis_order
            ]
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_INVALID_FILTER_CODE,
                "code": STATUS_INVALID_FILTER_CODE,
                "error": "filters validation failed",
                "오류": "filters 검증 실패",
                "검증_오류": errors,
                "validation_errors": errors,
                "available_axes": available_axes,
                "metadata_source": metadata_source,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_data_query",
                    final_answer_expected=False,
                    markers=["invalid_input", "validation_errors"],
                    explanation="query_table filters failed metadata validation; caller must correct axes or item codes before retrying.",
                    extra_signals={
                        "validation_error_count": len(errors),
                        "available_axes": available_axes,
                    },
                ),
            }

        selected_period = _pick_query_table_period_row(period_rows, period_range) if isinstance(period_rows, list) else None
        period_type_label = _period_type(selected_period)
        period_type = _api_period_type(period_type_label)
        effective_period_range = period_range
        auto_default_period_range: Optional[list[str]] = None
        if not effective_period_range and selected_period and selected_period.get("END_PRD_DE"):
            end_period = str(selected_period.get("END_PRD_DE"))
            effective_period_range = [end_period, end_period]
            auto_default_period_range = effective_period_range
        if effective_period_range and selected_period and selected_period.get("END_PRD_DE"):
            requested_bounds = [str(p).strip() for p in effective_period_range if str(p or "").strip()]
            if requested_bounds and all(_is_latest_period_text(bound) for bound in requested_bounds):
                end_period = str(selected_period.get("END_PRD_DE"))
                effective_period_range = [end_period, end_period]
                auto_default_period_range = effective_period_range
        period_format_error = _period_range_format_error(effective_period_range, selected_period)
        if period_format_error:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": period_format_error["code"],
                "code": period_format_error["code"],
                "error": period_format_error["error"],
                "오류": period_format_error["오류"],
                "filters_used": normalized_filters,
                "period_range": effective_period_range,
                "period_range_received": period_format_error.get("period_range_received"),
                "suggested_period_range": period_format_error.get("suggested_period_range"),
                "period_format_examples": period_format_error.get("period_format_examples"),
                "period_type_check": period_format_error.get("period_type_check"),
                "period_metadata": {
                    "cadence": selected_period.get("PRD_SE") if selected_period else None,
                    "start_period": selected_period.get("STRT_PRD_DE") if selected_period else None,
                    "latest_period": selected_period.get("END_PRD_DE") if selected_period else None,
                } if selected_period else None,
                "metadata_source": metadata_source,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_data_query",
                    final_answer_expected=False,
                    markers=["invalid_input", "invalid_period_format"],
                    explanation="period_range must use KOSIS period codes or be normalized by the caller before query_table.",
                    extra_signals={
                        "period_range_received": period_format_error.get("period_range_received"),
                        "suggested_period_range": period_format_error.get("suggested_period_range"),
                        "period_format_examples": period_format_error.get("period_format_examples"),
                        "period_type_check": period_format_error.get("period_type_check"),
                    },
                ),
            }
        period_error = _validate_query_period_range(effective_period_range, selected_period)
        if period_error:
            period_guidance = _period_error_guidance(period_error, selected_period, effective_period_range)
            period_markers = _period_error_markers(period_error, selected_period)
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": period_error["code"],
                "code": period_error["code"],
                "error": period_error["error"],
                "오류": period_error["오류"],
                "filters_used": normalized_filters,
                "period_range": effective_period_range,
                "period_range_received": period_error.get("period_range_received"),
                "suggested_period_range": period_guidance.get("suggested_period_range"),
                "available_period_range": period_error.get("available_period_range"),
                "period_format_examples": period_guidance.get("period_format_examples"),
                "period_type_check": period_guidance.get("period_type_check"),
                "period_metadata": {
                    "cadence": selected_period.get("PRD_SE") if selected_period else None,
                    "start_period": selected_period.get("STRT_PRD_DE") if selected_period else None,
                    "latest_period": selected_period.get("END_PRD_DE") if selected_period else None,
                } if selected_period else None,
                "metadata_source": metadata_source,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_data_query",
                    final_answer_expected=False,
                    markers=period_markers,
                    explanation="Requested period is not directly compatible with the table period metadata; caller must adjust period_range before retrying.",
                    extra_signals={
                        "period_range_received": period_error.get("period_range_received"),
                        "suggested_period_range": period_guidance.get("suggested_period_range"),
                        "available_period_range": period_error.get("available_period_range"),
                        "period_format_examples": period_guidance.get("period_format_examples"),
                        "period_type_check": period_guidance.get("period_type_check"),
                    },
                ),
            }

        normalized_rows_from_fanout: list[dict[str, Any]] = []
        try:
            fanout_filters = _fanout_filter_sets(normalized_filters)
            if len(fanout_filters) > QUERY_TABLE_MAX_FANOUT:
                return {
                    "상태": "failed",
                    "status": "unsupported",
                    "코드": STATUS_FANOUT_LIMIT_EXCEEDED,
                    "code": STATUS_FANOUT_LIMIT_EXCEEDED,
                    "error": (
                        f"query_table fan-out would create {len(fanout_filters)} KOSIS calls, "
                        f"above the configured limit {QUERY_TABLE_MAX_FANOUT}."
                    ),
                    "오류": (
                        f"query_table fan-out 호출 수가 {len(fanout_filters)}개로 제한 "
                        f"{QUERY_TABLE_MAX_FANOUT}개를 초과합니다."
                    ),
                    "filters_used": normalized_filters,
                    "fanout": {
                        "planned_call_count": len(fanout_filters),
                        "max_allowed": QUERY_TABLE_MAX_FANOUT,
                        "concurrency": QUERY_TABLE_CONCURRENCY,
                        "per_call_timeout_seconds": QUERY_TABLE_CALL_TIMEOUT,
                    },
                    "권고": "필터 코드를 줄여 나눠 호출하거나 KOSIS_MCP_QUERY_TABLE_MAX_FANOUT 값을 조정하세요.",
                    "metadata_source": metadata_source,
                    "mcp_output_contract": _mcp_tool_output_contract(
                        role="raw_extraction",
                        final_answer_expected=False,
                        markers=["max_fanout_exceeded"],
                        explanation="The requested raw extraction was rejected before KOSIS calls to prevent call explosion.",
                        extra_signals={
                            "planned_call_count": len(fanout_filters),
                            "max_allowed": QUERY_TABLE_MAX_FANOUT,
                        },
                    ),
                }

            semaphore = asyncio.Semaphore(QUERY_TABLE_CONCURRENCY)

            async def fetch_filter_set(filter_set: dict[str, list[str]]) -> Any:
                params = {
                    **_query_table_params(
                        org_id,
                        tbl_id,
                        filter_set,
                        axis_order,
                        effective_period_range,
                        period_type,
                    ),
                    "apiKey": key,
                }
                async with semaphore:
                    try:
                        return await asyncio.wait_for(
                            _kosis_call(client, "Param/statisticsParameterData.do", params),
                            timeout=QUERY_TABLE_CALL_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        return {"_error": "timeout", "_timeout_seconds": QUERY_TABLE_CALL_TIMEOUT}
                    except Exception as exc:
                        return {"_error": f"{type(exc).__name__}: {exc}"}

            row_groups = await asyncio.gather(*[
                fetch_filter_set(filter_set)
                for filter_set in fanout_filters
            ])
            for filter_set, group in zip(fanout_filters, row_groups):
                if isinstance(group, list):
                    normalized_rows_from_fanout.extend(
                        _normalize_query_table_rows(group, filter_set, axes, axis_order)
                    )
            fanout_report = _fanout_coverage_report(fanout_filters, row_groups)
        except Exception as exc:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_RUNTIME_ERROR,
                "code": STATUS_RUNTIME_ERROR,
                "error": f"KOSIS raw extraction failed: {exc}",
                "오류": f"KOSIS raw 호출 실패: {exc}",
                "filters_used": normalized_filters,
                "aggregation": aggregation,
                "metadata_source": metadata_source,
                "mcp_output_contract": _mcp_tool_output_contract(
                    role="raw_extraction",
                    final_answer_expected=False,
                    markers=["runtime_error"],
                    explanation="query_table failed during raw KOSIS extraction; caller must not infer missing rows.",
                    extra_signals={"aggregation": aggregation},
                ),
            }

    table_name = None
    table_name_eng = None
    if isinstance(name_rows, list) and name_rows:
        table_name = name_rows[0].get("TBL_NM") or name_rows[0].get("tblNm")
        table_name_eng = name_rows[0].get("TBL_NM_ENG") or name_rows[0].get("tblNmEng")
    latest_period = selected_period
    normalized_rows = normalized_rows_from_fanout
    aggregated_axes: list[str] = []
    aggregation_report: Optional[dict[str, Any]] = None
    if aggregation == "sum_by_group":
        normalized_rows, aggregated_axes, aggregation_report = _aggregate_rows_sum_by_group(
            normalized_rows,
            normalized_filters,
            group_by,
        )
    latest_period_value = str(latest_period.get("END_PRD_DE")) if latest_period and latest_period.get("END_PRD_DE") else None
    nature = _query_table_data_nature(table_name, effective_period_range, latest_period_value)
    aggregation_assumption = "caller_asserted_additive" if aggregation == "sum_by_group" else None
    aggregation_warning = (
        "sum_by_group은 호출자가 선택한 지표와 축이 가법적이라고 판단했다는 전제에서만 수행됩니다. "
        "비율·지수·평균·증감률 지표는 합산하면 통계적으로 의미가 없을 수 있습니다."
        if aggregation == "sum_by_group" else None
    )
    contract_markers: list[str] = []
    if fanout_report.get("partial_coverage"):
        contract_markers.append("partial_fanout_coverage")
    if fanout_report.get("complete_miss"):
        contract_markers.append("complete_fanout_miss")
    if fanout_report.get("failed_calls"):
        contract_markers.append("fanout_call_failed")
    if not normalized_rows:
        contract_markers.append("empty_rows")
    if nature.get("data_nature") == "projection":
        contract_markers.append("projection_data")
    if latest_period is None:
        contract_markers.append("period_metadata_missing")
    if aggregation_report and aggregation_report.get("dropped_row_count"):
        contract_markers.append("aggregation_dropped_rows")
        contract_markers.append("non_numeric_aggregation_input")
    result = {
        "상태": "executed",
        "status": "executed",
        "verification_level": "explored_raw",
        "confidence": "medium",
        "aggregation": aggregation,
        "group_by": group_by or None,
        "aggregated_axes": aggregated_axes,
        "aggregation_report": aggregation_report,
        "aggregation_assumption": aggregation_assumption,
        "aggregation_warning": aggregation_warning,
        "기관ID": org_id,
        "통계표ID": tbl_id,
        "통계표명": table_name,
        "통계표명_영문": table_name_eng,
        "org_id": org_id,
        "tbl_id": tbl_id,
        "table_name": table_name,
        "table_name_en": table_name_eng,
        "filters_used": normalized_filters,
        "auto_default_filters": auto_defaults,
        "period_range": effective_period_range,
        "period_type": period_type,
        "period_type_label": period_type_label,
        "auto_default_period_range": auto_default_period_range,
        "fanout": {
            "enabled": any(len(codes) > 1 for codes in normalized_filters.values()),
            **fanout_report,
            "max_allowed": QUERY_TABLE_MAX_FANOUT,
            "concurrency": QUERY_TABLE_CONCURRENCY,
            "per_call_timeout_seconds": QUERY_TABLE_CALL_TIMEOUT,
            "reason": "KOSIS multi-code parameters can fail with code 21, so the server uses verified single-code calls.",
        },
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
        "metadata_source": metadata_source,
        "수록기간": {
            "주기": latest_period.get("PRD_SE") if latest_period else None,
            "시작_수록시점": latest_period.get("STRT_PRD_DE") if latest_period else None,
            "최신_수록시점": latest_period.get("END_PRD_DE") if latest_period else None,
        } if latest_period else None,
        "period_metadata": {
            "cadence": latest_period.get("PRD_SE") if latest_period else None,
            "start_period": latest_period.get("STRT_PRD_DE") if latest_period else None,
            "latest_period": latest_period.get("END_PRD_DE") if latest_period else None,
        } if latest_period else None,
        "주의": [
            "query_table은 raw extraction 도구입니다. 기본 aggregation=none에서는 합산·평균·비율·해석을 수행하지 않습니다.",
            "aggregation=sum_by_group은 호출자가 해당 값의 가법성을 명시적으로 책임진다는 전제에서만 사용하세요.",
            *( [aggregation_warning] if aggregation_warning else [] ),
            "confidence는 값의 품질이 아니라 코드 매핑과 호출 조건의 검증 수준입니다.",
        ],
        "warnings": [
            "query_table is a raw extraction tool. aggregation=none does not aggregate, average, calculate ratios, or interpret values.",
            "aggregation=sum_by_group assumes the caller has verified additivity for the selected statistic.",
            *( [aggregation_warning] if aggregation_warning else [] ),
            "confidence describes mapping and call-condition verification, not statistical data quality.",
        ],
        "mcp_output_contract": _mcp_tool_output_contract(
            role="raw_extraction",
            final_answer_expected=False,
            markers=contract_markers,
            explanation=(
                "One or more fan-out calls failed; disclose the failed call details before using the data."
                if "fanout_call_failed" in contract_markers else
                "Some rows were dropped during aggregation because their values were non-numeric."
                if "aggregation_dropped_rows" in contract_markers else
                "Some fan-out calls returned no rows; disclose partial coverage before using the data."
                if "partial_fanout_coverage" in contract_markers else
                "No rows were returned for the verified filters and period."
                if "empty_rows" in contract_markers else
                "Raw rows returned for the verified filters and period."
            ),
            extra_signals={
                "coverage_ratio": fanout_report.get("coverage_ratio"),
                "successful_calls": fanout_report.get("successful_calls"),
                "failed_calls": fanout_report.get("failed_calls"),
                "empty_results": fanout_report.get("empty_results"),
                "data_nature": nature.get("data_nature"),
                "period_nature": nature.get("period_nature"),
                "projection_horizon_years": nature.get("projection_horizon_years"),
                "period_metadata_available": latest_period is not None,
                "aggregation_report": aggregation_report,
            },
        ),
    }
    result.update({key: value for key, value in nature.items() if value is not None})
    return result


@mcp.tool()
async def compute_indicator(
    operation: str,
    input_rows: list[dict],
    denominator_rows: Optional[list[dict]] = None,
    group_by: Optional[list[str]] = None,
    match_keys: Optional[list[str]] = None,
    scale_factor: Optional[float] = None,
    decimals: int = 4,
) -> dict:
    """[🧮] 허용된 산식 enum만 사용해 query_table rows에 산술을 수행한다.

    이 도구는 caller(LLM)가 제공한 raw 값에 산술과 구조 검증만 수행한다.
    가법성 판단, 단위 변환, 지표 정의는 caller 책임이다. 입력은 query_table
    응답의 rows를 그대로 넘긴다 (필드: period/value/unit/dimensions/raw).

    operation:
        - growth_rate         : 그룹 내 인접 period 변화율 (%)
        - cagr                : 그룹 내 시작/끝 period 사이 연평균 복합 성장률 (%)
        - yoy_pct             : 같은 intra-year period의 전년 대비 변화율 (%)
        - yoy_diff            : 같은 intra-year period의 전년 대비 변화량 (단위 보존)
        - share               : input_rows 합 또는 denominator_rows 매칭 분모 대비 비율 (%)
        - per_capita          : numerator/denominator * scale_factor (단위는 caller 책임)
        - ratio               : 단순 A/B (단위 없음)
        - sum_additive_rows   : group_by별 행 합산. caller가 가법성 책임 명시.

    Args:
        operation: 산식 enum 이름.
        input_rows: query_table.rows 호환 list.
        denominator_rows: per_capita/ratio/share에 사용되는 분모 list (선택).
        group_by: 그룹화 축 (OBJ_ID 리스트). 미지정 시 dimensions의 non-ITEM 축 자동 사용.
        match_keys: per_capita/ratio/share의 분자/분모 매칭 축. 미지정 시 group_by 규칙 따름.
        scale_factor: per_capita/ratio 결과에 곱할 단위 스케일 (예: 1000). share는 무시.
        decimals: 결과 반올림 자릿수.
    """
    original_operation = operation
    operation_alias_used = None
    if isinstance(operation, str) and operation not in INDICATOR_OPERATIONS:
        mapped = OPERATION_ALIASES_KO.get(operation)
        if mapped:
            operation_alias_used = {
                "input": operation,
                "mapped_to": mapped,
                "source": "operation_alias_ko",
            }
            operation = mapped
    op = INDICATOR_OPERATIONS.get(operation) if isinstance(operation, str) else None
    if op is None:
        did_you_mean = OPERATION_ALIASES_KO.get(original_operation) if isinstance(original_operation, str) else None
        return {
            "status": "invalid_input",
            "상태": "invalid_input",
            "error": f"Unknown operation: {original_operation!r}",
            "오류": f"알 수 없는 operation: {original_operation!r}",
            "did_you_mean": did_you_mean,
            "valid_operations": list(INDICATOR_OPERATIONS.keys()),
            "operation_catalog": _indicator_operation_catalog(),
            "korean_to_enum_map": dict(OPERATION_ALIASES_KO),
            "mcp_output_contract": _mcp_tool_output_contract(
                role="indicator_computation",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="operation must be one of the allowed enum names.",
                extra_signals={
                    "valid_operations": list(INDICATOR_OPERATIONS.keys()),
                },
            ),
        }
    if not isinstance(input_rows, list) or not input_rows:
        return {
            "status": "invalid_input",
            "상태": "invalid_input",
            "error": "input_rows must be a non-empty list.",
            "오류": "input_rows는 비어 있지 않은 리스트여야 합니다.",
            "operation": operation,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="indicator_computation",
                final_answer_expected=False,
                markers=["invalid_input"],
                explanation="input_rows is empty; nothing to compute.",
                extra_signals={"operation": operation, "result_count": 0},
            ),
        }
    if op.requires_denominator and not denominator_rows:
        return {
            "status": "invalid_input",
            "상태": "invalid_input",
            "error": f"{operation} requires denominator_rows.",
            "오류": f"{operation}은(는) denominator_rows가 필요합니다.",
            "operation": operation,
            "mcp_output_contract": _mcp_tool_output_contract(
                role="indicator_computation",
                final_answer_expected=False,
                markers=["invalid_input", "missing_denominator"],
                explanation=f"{operation} cannot run without denominator_rows.",
                extra_signals={"operation": operation, "result_count": 0},
            ),
        }

    outcome = op.compute(
        input_rows,
        denominator_rows=denominator_rows,
        match_keys=match_keys,
        group_by=group_by,
        scale_factor=scale_factor,
        decimals=int(decimals) if decimals is not None else 4,
    )
    markers = list(outcome.markers)
    if outcome.unmatched:
        markers.append("period_or_group_mismatch")
    if any(
        isinstance(result.unit_transformation, dict)
        and result.unit_transformation.get("caller_must_resolve")
        for result in outcome.results
    ):
        markers.append("unit_caller_resolution_required")
    if outcome.status == "invalid_input":
        markers.append("invalid_input")
    elif outcome.status == "partial":
        markers.append("partial_computation")

    explanation = (
        f"compute_indicator({operation}) produced {len(outcome.results)} value(s)"
        + (f"; {len(outcome.unmatched)} unmatched/skipped." if outcome.unmatched else ".")
    )
    additivity_caller_asserted = (
        op.aggregation_caller_asserted
        or "share_total_from_input_rows" in markers
        or any(
            bool(result.inputs.get("additivity_caller_asserted"))
            for result in outcome.results
        )
    )
    return {
        "status": outcome.status,
        "상태": outcome.status,
        "operation": operation,
        "operation_raw_input": original_operation,
        "operation_alias_used": operation_alias_used,
        "verification_level": "caller_inputs_only",
        "confidence": "medium" if outcome.results else "low",
        "results": [r.to_dict() for r in outcome.results],
        "unmatched": outcome.unmatched,
        "validation_errors": outcome.validation_errors,
        "computation_metadata": outcome.extra,
        "mcp_output_contract": _mcp_tool_output_contract(
            role="indicator_computation",
            final_answer_expected=False,
            markers=markers,
            explanation=explanation,
            extra_signals={
                "operation": operation,
                "operation_raw_input": original_operation,
                "operation_alias_used": operation_alias_used,
                "result_count": len(outcome.results),
                "unmatched_count": len(outcome.unmatched),
                "additivity_caller_asserted": additivity_caller_asserted,
                "unit_caller_resolution_required": "unit_caller_resolution_required" in markers,
                "computation_metadata": outcome.extra,
            },
        ),
        "주의": [
            "compute_indicator는 caller가 제공한 값에 산술만 수행합니다. 가법성/단위/지표 정의는 caller(LLM) 책임입니다.",
            "input_rows·denominator_rows의 시점·축 정합성은 caller가 사전에 확인하세요. 일치하지 않는 항목은 unmatched로 반환됩니다.",
        ],
    }


@mcp.tool()
async def explore_table(
    org_id: str,
    tbl_id: str,
    industry_term: Optional[str] = None,
    axes_to_include: Optional[list[str]] = None,
    compact: bool = False,
    include_english_labels: bool = True,
    sample_limit: int = 30,
    api_key: Optional[str] = None,
) -> dict:
    """[🧭] Pull the metadata KOSIS publishes for a single statistical
    table — classification axes, item codes, units, supported periods,
    and the recorded period window — so callers can build a quick_stat
    request without having to hard-code obj_l1/obj_l2/itm_id values.

    The dev-guide endpoints behind this tool are statisticsData.do?
    method=getMeta&type=TBL|ITM|PRD|SOURCE. Each call is cheap; we
    fan them out in parallel and return a single response.

    Args:
        org_id: KOSIS 기관 ID (e.g. "101" for 통계청).
        tbl_id: 통계표 ID (e.g. "DT_1IN1502").
        industry_term: optional industry/category name. If supplied,
            the response includes a resolved_industry block pointing at
            the matching ITM_ID so the caller can pass it as objL2 to
            quick_stat. Use this to bridge "제조업"/"음식점업" to KOSIS
            internal codes dynamically rather than via TIER_A_STATS.
        axes_to_include: optional OBJ_ID or axis-name fragments to return.
        compact: if true, return only a sample of long axes.
        include_english_labels: if false, omit English labels to reduce tokens.
        sample_limit: max items per axis in compact mode.

    Returns: dict with table name, classification rows, period range,
        contact info, and (optionally) the resolved industry row plus
        a suggested quick_stat call template.
    """
    if not org_id or not tbl_id:
        return {
            "오류": "org_id와 tbl_id가 모두 필요합니다.",
            "권고": "search_kosis 응답의 통계표ID·기관ID 필드를 사용",
        }
    try:
        key = _resolve_key(api_key)
    except RuntimeError as exc:
        if _is_missing_key_error(exc):
            return _missing_api_key_response("explore_table", org_id=org_id, tbl_id=tbl_id)
        raise
    async with httpx.AsyncClient() as client:
        async def safe_fetch(meta_type: str, extra: Optional[dict] = None) -> Any:
            try:
                return await _fetch_meta(client, key, org_id, tbl_id, meta_type, extra)
            except Exception as exc:
                return {"오류": f"{meta_type} 조회 실패: {exc}"}

        name_rows, item_rows, period_rows, source_rows = await asyncio.gather(
            safe_fetch("TBL"),
            safe_fetch("ITM"),
            safe_fetch("PRD"),
            safe_fetch("SOURCE"),
        )

    meta_errors = {
        meta_type: rows.get("오류")
        for meta_type, rows in (
            ("TBL", name_rows),
            ("ITM", item_rows),
            ("PRD", period_rows),
            ("SOURCE", source_rows),
        )
        if isinstance(rows, dict) and rows.get("오류")
    }
    meta_counts = {
        "TBL": len(name_rows) if isinstance(name_rows, list) else 0,
        "ITM": len(item_rows) if isinstance(item_rows, list) else 0,
        "PRD": len(period_rows) if isinstance(period_rows, list) else 0,
        "SOURCE": len(source_rows) if isinstance(source_rows, list) else 0,
    }
    if not meta_errors and not any(meta_counts.values()):
        return {
            "상태": "failed",
            "status": "failed",
            "코드": STATUS_STAT_NOT_FOUND,
            "code": STATUS_STAT_NOT_FOUND,
            "오류": "KOSIS 메타 API가 해당 org_id/tbl_id에 대해 어떤 메타도 반환하지 않았습니다.",
            "기관ID": org_id,
            "통계표ID": tbl_id,
            "조회_결과": meta_counts,
            "권고": "기관ID와 통계표ID를 다시 확인하거나 search_kosis로 통계표를 재검색하세요.",
            "mcp_output_contract": _mcp_tool_output_contract(
                role="table_metadata",
                final_answer_expected=False,
                markers=["metadata_failed", "not_matched"],
                explanation="No metadata was returned for the requested table; caller must re-search or verify IDs.",
                extra_signals={
                    "metadata_errors": [],
                    "metadata_counts": meta_counts,
                },
            ),
        }

    classifications: dict[str, dict[str, Any]] = {}
    if isinstance(item_rows, list):
        for row in item_rows:
            obj_id = str(row.get("OBJ_ID") or "")
            if not obj_id:
                continue
            axis = classifications.setdefault(obj_id, {
                "OBJ_NM": row.get("OBJ_NM"),
                "OBJ_NM_ENG": row.get("OBJ_NM_ENG"),
                "items": [],
            })
            axis["items"].append({
                "ITM_ID": row.get("ITM_ID"),
                "ITM_NM": row.get("ITM_NM"),
                "ITM_NM_ENG": row.get("ITM_NM_ENG"),
                "UP_ITM_ID": row.get("UP_ITM_ID"),
                "UNIT_NM": row.get("UNIT_NM"),
            })

    if axes_to_include:
        wanted_raw = [str(axis) for axis in axes_to_include if str(axis or "").strip()]
        wanted = {_compact_text(axis) for axis in wanted_raw}
        wanted_dimensions = set(_normalize_required_dimensions(wanted_raw))
        classifications = {
            obj_id: axis
            for obj_id, axis in classifications.items()
            if _compact_text(obj_id) in wanted
            or any(term in _compact_text(str(axis.get("OBJ_NM") or "")) for term in wanted)
            or any(_axis_matches_dimension(str(axis.get("OBJ_NM") or ""), dim) for dim in wanted_dimensions)
        }
    if compact:
        limit = max(1, int(sample_limit or 30))
        for axis in classifications.values():
            items = axis.get("items") or []
            axis["item_count"] = len(items)
            axis["truncated"] = len(items) > limit
            axis["items"] = items[:limit]
    if not include_english_labels:
        for axis in classifications.values():
            axis.pop("OBJ_NM_ENG", None)
            for item in axis.get("items") or []:
                if isinstance(item, dict):
                    item.pop("ITM_NM_ENG", None)

    table_name = None
    table_name_eng = None
    if isinstance(name_rows, list) and name_rows:
        first = name_rows[0]
        table_name = first.get("TBL_NM") or first.get("tblNm")
        table_name_eng = first.get("TBL_NM_ENG") or first.get("tblNmEng")

    period_summary = None
    used_period = None
    if isinstance(period_rows, list) and period_rows:
        latest = _pick_finest_period(period_rows)
        used_period = str(
            latest.get("END_PRD_DE") or latest.get("endPrdDe")
            or latest.get("PRD_DE") or latest.get("prdDe") or ""
        )
        period_summary = {
            "수록주기": latest.get("PRD_SE") or latest.get("prdSe"),
            "시작_수록시점": (
                latest.get("STRT_PRD_DE") or latest.get("strtPrdDe") or ""
            ),
            "최신_수록시점": used_period,
            "수록주기_개수": len(period_rows),
        }

    period_age = None
    if used_period:
        parseable = re.match(r"(\d{4})(?:\.(\d{2}))?", used_period)
        period_age = NaturalLanguageAnswerEngine._period_age_years(
            "".join(filter(None, parseable.groups())) if parseable else used_period
        )

    contact = None
    if isinstance(source_rows, list) and source_rows:
        first = source_rows[0]
        contact = {
            "조사명": first.get("JOSA_NM") or first.get("josaNm"),
            "담당부서": first.get("DEPT_NM") or first.get("deptNm"),
            "전화": first.get("DEPT_PHONE") or first.get("deptPhone"),
        }

    item_list = item_rows if isinstance(item_rows, list) else []
    items_with_units = sum(1 for row in item_list if row.get("UNIT_NM"))
    items_with_english = sum(1 for row in item_list if row.get("ITM_NM_ENG"))
    metadata_coverage = {
        "통계표명": bool(table_name),
        "통계표명_영문": bool(table_name_eng),
        "수록기간": bool(period_summary),
        "출처": bool(contact),
        "분류축_개수": len(classifications),
        "항목_개수": len(item_list),
        "단위_있는_항목": items_with_units,
        "영문라벨_있는_항목": items_with_english,
        "조회_결과": meta_counts,
    }
    if meta_errors:
        metadata_coverage["조회_실패"] = meta_errors

    result: dict[str, Any] = {
        "통계표ID": tbl_id,
        "기관ID": org_id,
        "통계표명": table_name,
        "통계표명_영문": table_name_eng,
        "수록기간": period_summary,
        "used_period": used_period,
        "period_age_years": period_age,
        "출처": contact,
        "분류축": classifications,
        "메타_완성도": metadata_coverage,
        "출처_KOSIS_API": "statisticsData.do?method=getMeta&type=TBL/ITM/PRD/SOURCE",
    }

    if period_age is not None and period_age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"이 통계표의 최신 시점은 {used_period} (약 {period_age:.1f}년 경과)"
        )

    if not industry_term:
        industry_axes = [
            {"OBJ_ID": obj_id, "OBJ_NM": axis.get("OBJ_NM")}
            for obj_id, axis in classifications.items()
            if any(token in str(axis.get("OBJ_NM") or "") for token in ("산업", "업종", "분류"))
        ]
        if industry_axes:
            result["industry_term_안내"] = {
                "안내": (
                    "industry_term을 함께 주면 해당 산업·업종 표현을 이 통계표의 ITM_ID로 "
                    "매핑해 resolved_industry 블록에 표시합니다."
                ),
                "감지된_분류축": industry_axes,
                "예시_호출": f"explore_table('{org_id}', '{tbl_id}', industry_term='제조업')",
            }

    if industry_term:
        match = _resolve_classification_term(industry_term, item_rows if isinstance(item_rows, list) else [])
        if match is not None:
            result["resolved_industry"] = {
                "입력": industry_term,
                "매칭_ITM_NM": match.get("ITM_NM"),
                "ITM_ID": match.get("ITM_ID"),
                "OBJ_ID": match.get("OBJ_ID"),
                "단위": match.get("UNIT_NM"),
                "권고_호출": (
                    f"quick_stat은 obj_l2/obj_l3 직접 노출이 없으므로, KOSIS 직접 API "
                    f"호출(statisticsData.do)에 objL?={match.get('OBJ_ID')}, "
                    f"itmId={match.get('ITM_ID')}로 사용."
                ),
            }
        else:
            result["resolved_industry"] = {
                "입력": industry_term,
                "매칭_ITM_NM": None,
                "안내": "분류축에서 일치하는 항목을 찾지 못했습니다. 분류축 리스트를 확인해 입력 표현을 조정하세요.",
            }

    explore_markers: list[str] = []
    if meta_errors:
        explore_markers.append("metadata_partial")
    if period_age is not None and period_age >= 1.0:
        explore_markers.append("stale_metadata")
    if industry_term and not result.get("resolved_industry", {}).get("ITM_ID"):
        explore_markers.append("industry_unresolved")
    explore_explanation = (
        f"Latest period is {used_period} (~{period_age:.1f} years old); disclose staleness before quoting figures."
        if "stale_metadata" in explore_markers else
        "Some metadata endpoints failed; treat missing axes/items as unknown rather than absent."
        if meta_errors else
        "Metadata snapshot returned for the requested table."
    )
    result["mcp_output_contract"] = _mcp_tool_output_contract(
        role="table_metadata",
        final_answer_expected=False,
        markers=explore_markers,
        explanation=explore_explanation,
        extra_signals={
            "period_age_years": period_age,
            "axis_count": len(classifications),
            "metadata_errors": list(meta_errors.keys()),
        },
    )
    return result


@mcp.tool()
async def decode_error(error_code: str) -> dict:
    """[🛠] KOSIS 에러 코드 한국어 설명. 공식 코드(10/20/30 등) 외에도
    내부/네트워크 코드(E001, INVALID_PARAM, -1 등)를 인식한다."""
    raw = "" if error_code is None else str(error_code)
    code = raw.strip()
    if not code:
        return {
            "코드": "",
            "의미": "빈 코드 — 응답에 에러 코드 필드가 없거나 비어 있음",
            "권고": "원본 응답의 다른 필드(`오류`, `결과`, HTTP 상태)를 확인",
        }
        # Try exact, then upper, then lowercase. KOSIS official codes
        # are numeric so case-insensitive lookup is safe for non-numeric variants.
    candidates = [code, code.upper(), code.lower()]
    for cand in candidates:
        if cand in ERROR_MAP:
            return {
                "코드": cand,
                "의미": ERROR_MAP[cand],
                "공식코드_여부": cand in {"10", "11", "20", "21", "30", "31", "40", "41", "42", "50"},
            }
    return {
        "코드": code,
        "의미": "알 수 없음 — KOSIS 공식 코드(10/11/20/21/30/31/40/41/50)도, 알려진 내부 코드도 아님",
        "지원_코드": sorted(ERROR_MAP.keys()),
        "권고": "응답 본문 전체를 함께 확인하고, KOSIS 고객센터에 코드와 호출 파라미터를 첨부해 문의",
    }


# ============================================================================

def main() -> None:
    """Console entry point for `kosis-analysis-mcp`."""
    mcp.run()


if __name__ == "__main__":
    main()
