from __future__ import annotations

import asyncio
import io
import sys
from pathlib import Path
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import kosis_mcp_server
from kosis_analysis.metadata import (
    MetadataCompatibilityScorer,
    TableMetadataProfile,
    _fanout_coverage_report,
)


def test_fanout_partial_coverage() -> None:
    fanout_filters = [
        {"A": ["00"], "ITEM": ["T1"]},
        {"A": ["11"], "ITEM": ["T1"]},
        {"A": ["21"], "ITEM": ["T1"]},
    ]
    row_groups = [
        [{"DT": "1"}],
        [],
        [],
    ]
    report = _fanout_coverage_report(fanout_filters, row_groups)
    assert report["call_count"] == 3, report
    assert report["successful_calls"] == 1, report
    assert report["empty_results"] == 2, report
    assert report["coverage_ratio"] == 0.3333, report
    assert report["partial_coverage"] is True, report
    assert report["missing_codes_by_axis"]["A"] == ["11", "21"], report


def test_indicator_evidence_required_when_indicator_supplied() -> None:
    profile = TableMetadataProfile.from_rows(
        "101",
        "TBL_FAKE",
        {"통계표명": "지가변동률"},
        [{"TBL_NM": "지가변동률"}],
        [
            {"OBJ_ID": "A", "OBJ_NM": "지역별", "ITM_ID": "00", "ITM_NM": "전국"},
        ],
        [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}],
    )
    result = MetadataCompatibilityScorer(["region"], indicator="행복지수").evaluate(profile).to_response()
    assert result["status"] == "not_matched_indicator", result
    assert result["compatibility"]["verification_level"] == "not_matched", result
    assert result["compatibility"]["not_matched_reason"] == "indicator_evidence_empty", result


def test_indicator_evidence_not_required_for_axis_only_exploration() -> None:
    profile = TableMetadataProfile.from_rows(
        "101",
        "TBL_FAKE",
        {"통계표명": "지역 통계"},
        [{"TBL_NM": "지역 통계"}],
        [
            {"OBJ_ID": "A", "OBJ_NM": "지역별", "ITM_ID": "00", "ITM_NM": "전국"},
        ],
        [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}],
    )
    result = MetadataCompatibilityScorer(["region"], indicator=None).evaluate(profile).to_response()
    assert result["status"] == "selected", result


async def test_select_table_falls_back_to_query_indicator() -> None:
    """When indicator is omitted, query must drive evidence matching (no silent pass)."""
    original_search = kosis_mcp_server.search_kosis
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_search(term: str, limit: int = 10, use_routing: bool = True, api_key: Any = None) -> dict[str, Any]:
        return {
            "결과": [
                {"통계표명": "읍면동별 지가변동률", "통계표ID": "DT_31501N_010", "기관ID": "101"},
            ],
            "Tier_A_직접_매핑": None,
        }

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "읍면동별 지가변동률"}]
        if kind == "ITM":
            return [{"OBJ_ID": "A", "OBJ_NM": "지역별", "ITM_ID": "00", "ITM_NM": "전국"}]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}]
        return []

    try:
        kosis_mcp_server.search_kosis = fake_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.select_table_for_query(
            "행복지수",
            required_dimensions=["region"],
            api_key="dummy",
        )
    finally:
        kosis_mcp_server.search_kosis = original_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["indicator_source"] == "query_fallback", result
    assert result["indicator_used"] == "행복지수", result
    assert result["selected"] is None, result
    assert result["status"] == "needs_table_selection", result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "indicator_evidence_empty" in markers, markers
    assert "indicator_inferred_from_query" in markers, markers
    rejected = result["rejected"]
    assert rejected and rejected[0]["status"] == "not_matched_indicator", rejected


async def test_resolve_concepts_contract_on_unresolved() -> None:
    """resolve_concepts must surface unresolved/ambiguous concepts via mcp_output_contract."""
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "임의 표"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "A", "OBJ_NM": "지역별", "ITM_ID": "00", "ITM_NM": "전국"},
                {"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": "지표A"},
            ]
        return []

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.resolve_concepts(
            "101",
            "TBL_FAKE",
            ["수도권", "반도체 산업"],
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["status"] == "needs_concept_selection", result
    contract = result["mcp_output_contract"]
    markers = contract["current_signals"]["markers_present"]
    assert "not_matched" in markers, markers
    assert "concept_unresolved" in markers, markers
    assert contract["current_signals"]["has_failures"] is True, contract
    assert contract["current_signals"]["unresolved_concept_count"] == 2, contract


async def test_explore_table_contract_on_stale_period() -> None:
    """explore_table must mark stale_metadata when latest period is over a year old."""
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str, extra: Any = None) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "임의 표"}]
        if kind == "ITM":
            return [{"OBJ_ID": "A", "OBJ_NM": "지역별", "ITM_ID": "00", "ITM_NM": "전국"}]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2010", "END_PRD_DE": "2020"}]
        if kind == "SOURCE":
            return [{"DEPT_NM": "x"}]
        return []

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.explore_table("101", "TBL_FAKE", api_key="dummy")
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    contract = result["mcp_output_contract"]
    markers = contract["current_signals"]["markers_present"]
    assert "stale_metadata" in markers, markers
    assert contract["current_signals"]["period_age_years"] is not None, contract
    assert contract["current_signals"]["period_age_years"] >= 1.0, contract


async def test_search_kosis_contract_on_empty_results() -> None:
    """search_kosis must surface search_empty marker when no candidates are returned."""
    original_hints = kosis_mcp_server._routing_hints
    original_call = kosis_mcp_server._kosis_call

    def fake_hints(query: str) -> list[str]:
        return []

    async def fake_call(client: Any, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    try:
        kosis_mcp_server._routing_hints = fake_hints  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = fake_call  # type: ignore[assignment]
        result = await kosis_mcp_server.search_kosis("zzz_no_match_zzz", api_key="dummy")
    finally:
        kosis_mcp_server._routing_hints = original_hints  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = original_call  # type: ignore[assignment]

    contract = result["mcp_output_contract"]
    markers = contract["current_signals"]["markers_present"]
    assert "search_empty" in markers, markers
    assert contract["current_signals"]["result_count"] == 0, contract


def _make_row(period: str, value: Any, region_code: str, region_label: str, item_code: str = "T1", item_label: str = "지표", unit: str = "명") -> dict[str, Any]:
    return {
        "period": period,
        "value": value,
        "unit": unit,
        "dimensions": {
            "ITEM": {"code": item_code, "label": item_label, "unit": unit},
            "A": {"code": region_code, "label": region_label, "unit": None},
        },
        "raw": {},
    }


async def test_compute_indicator_growth_rate() -> None:
    rows = [
        _make_row("2020", "100", "11", "서울"),
        _make_row("2021", "110", "11", "서울"),
        _make_row("2022", "121", "11", "서울"),
    ]
    result = await kosis_mcp_server.compute_indicator(operation="growth_rate", input_rows=rows)
    assert result["status"] == "ok", result
    assert len(result["results"]) == 2, result
    assert result["results"][0]["value"] == 10.0, result
    assert result["results"][1]["value"] == 10.0, result
    contract = result["mcp_output_contract"]
    assert contract["current_signals"]["result_count"] == 2, contract


async def test_compute_indicator_growth_rate_single_row_has_reason() -> None:
    result = await kosis_mcp_server.compute_indicator(
        operation="growth_rate",
        input_rows=[_make_row("2023", "100", "11", "서울")],
    )
    assert result["status"] == "invalid_input", result
    assert result["validation_errors"], result
    assert "at least 2 rows" in result["validation_errors"][0], result
    assert result["unmatched"][0]["reason"] == "insufficient_periods", result


async def test_compute_indicator_per_capita_with_denominator_zero() -> None:
    numerator = [
        _make_row("2023", "1000", "11", "서울", unit="명"),
        _make_row("2023", "2000", "21", "부산", unit="명"),
    ]
    denominator = [
        _make_row("2023", "10", "11", "서울", item_code="POP", item_label="인구", unit="천명"),
        _make_row("2023", "0", "21", "부산", item_code="POP", item_label="인구", unit="천명"),
    ]
    result = await kosis_mcp_server.compute_indicator(
        operation="per_capita",
        input_rows=numerator,
        denominator_rows=denominator,
        match_keys=["A"],
    )
    assert result["status"] == "partial", result
    assert len(result["results"]) == 1, result
    assert result["results"][0]["value"] == 100.0, result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "denominator_zero" in markers, markers
    assert "period_or_group_mismatch" in markers, markers


async def test_compute_indicator_unit_transformation_requires_caller_label() -> None:
    numerator = [
        _make_row("2023", "4960000", "11", "서울", unit="억원"),
    ]
    denominator = [
        _make_row("2023", "9386000", "11", "서울", item_code="POP", item_label="인구", unit="명"),
    ]
    result = await kosis_mcp_server.compute_indicator(
        operation="per_capita",
        input_rows=numerator,
        denominator_rows=denominator,
        match_keys=["A"],
        scale_factor=100000000,
        decimals=0,
    )
    assert result["status"] == "ok", result
    row = result["results"][0]
    assert row["unit"] is None, row
    assert row["unit_raw"] == "억원", row
    assert row["unit_denominator"] == "명", row
    assert row["unit_resolved"] is None, row
    assert row["unit_caller_should_label"] is None, row
    assert row["unit_transformation"]["caller_must_resolve"] is True, row
    assert "억원" in row["unit_transformation"]["expression"], row
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "unit_caller_resolution_required" in markers, markers


async def test_compute_indicator_share_uses_input_total() -> None:
    rows = [
        _make_row("2023", "25", "11", "서울"),
        _make_row("2023", "75", "21", "부산"),
    ]
    result = await kosis_mcp_server.compute_indicator(operation="share", input_rows=rows)
    assert result["status"] == "ok", result
    assert {r["value"] for r in result["results"]} == {25.0, 75.0}, result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "share_total_from_input_rows" in markers, markers
    assert result["mcp_output_contract"]["current_signals"]["additivity_caller_asserted"] is True, result


async def test_compute_indicator_yoy_pct_matches_intra_year() -> None:
    rows = [
        _make_row("202301", "100", "11", "서울"),
        _make_row("202401", "120", "11", "서울"),
        _make_row("202304", "200", "11", "서울"),
        _make_row("202404", "210", "11", "서울"),
    ]
    result = await kosis_mcp_server.compute_indicator(operation="yoy_pct", input_rows=rows)
    assert result["status"] in ("ok", "partial"), result
    values_by_period = {r["period"]: r["value"] for r in result["results"]}
    assert values_by_period.get("202401") == 20.0, result
    assert values_by_period.get("202404") == 5.0, result


async def test_compute_indicator_unknown_operation_is_rejected() -> None:
    result = await kosis_mcp_server.compute_indicator(
        operation="median",
        input_rows=[_make_row("2023", "1", "11", "서울")],
    )
    assert result["status"] == "invalid_input", result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "invalid_input" in markers, markers
    assert result["mcp_output_contract"]["current_signals"]["has_failures"] is True, result
    assert "growth_rate" in result["valid_operations"], result


async def test_compute_indicator_korean_operation_alias() -> None:
    rows = [
        _make_row("2020", "100", "11", "서울"),
        _make_row("2021", "110", "11", "서울"),
    ]
    result = await kosis_mcp_server.compute_indicator(operation="변화율", input_rows=rows)
    assert result["status"] == "ok", result
    assert result["operation"] == "growth_rate", result
    assert result["operation_alias_used"]["mapped_to"] == "growth_rate", result
    catalog = result["mcp_output_contract"]["current_signals"]
    assert catalog["operation_alias_used"]["input"] == "변화율", catalog


def test_compute_indicator_in_planner_available_tools() -> None:
    """planner must advertise compute_indicator as available_now once the tool exists."""
    from kosis_analysis.planner import QueryWorkflowPlanner
    assert "compute_indicator" in QueryWorkflowPlanner.AVAILABLE_TOOLS


async def test_search_kosis_preserves_original_query() -> None:
    calls: list[str] = []
    original_hints = kosis_mcp_server._routing_hints
    original_call = kosis_mcp_server._kosis_call

    def fake_hints(query: str) -> list[str]:
        assert query == "청년 실업률"
        return ["청년 고용률", "청년 취업자"]

    async def fake_call(client: Any, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append(str(params.get("searchNm")))
        return []

    try:
        kosis_mcp_server._routing_hints = fake_hints  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = fake_call  # type: ignore[assignment]
        result = await kosis_mcp_server.search_kosis("청년 실업률", use_routing=True, api_key="dummy")
    finally:
        kosis_mcp_server._routing_hints = original_hints  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = original_call  # type: ignore[assignment]

    assert calls[:3] == ["청년 실업률", "청년 고용률", "청년 취업자"], calls
    assert result["search_terms_used"][:3] == ["청년 실업률", "청년 고용률", "청년 취업자"], result
    assert result["original_query_preserved"] is True, result


async def test_quick_tool_schemas_do_not_require_unsupported() -> None:
    tools = await kosis_mcp_server.mcp.list_tools()
    by_name = {tool.name: tool.inputSchema for tool in tools}
    for name in ("quick_stat", "quick_trend", "quick_region_compare"):
        schema = by_name[name]
        assert "unsupported" not in (schema.get("required") or []), schema
        assert "unsupported" not in (schema.get("properties") or {}), schema
        assert "extra_params" in (schema.get("properties") or {}), schema


async def test_missing_api_key_returns_structured_payload() -> None:
    import kosis_analysis.client as client_mod

    original_default = client_mod.API_KEY_DEFAULT
    try:
        client_mod.API_KEY_DEFAULT = ""
        result = await kosis_mcp_server.search_kosis("인구")
    finally:
        client_mod.API_KEY_DEFAULT = original_default

    assert result["status"] == "failed", result
    assert result["code"] == "MISSING_KEY", result
    assert result["mcp_output_contract"]["current_signals"]["has_failures"] is True, result


async def test_select_table_prefers_fresh_candidate_on_indicator_near_tie() -> None:
    original_search = kosis_mcp_server.search_kosis
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_search(term: str, limit: int = 10, use_routing: bool = True, api_key: Any = None) -> dict[str, Any]:
        return {
            "결과": [
                {"통계표명": "물가지수 오래된 표", "통계표ID": "OLD", "기관ID": "101"},
                {"통계표명": "물가지수 최신 표", "통계표ID": "NEW", "기관ID": "101"},
            ],
            "Tier_A_직접_매핑": None,
        }

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            name = "소비자물가지수 오래된 표" if tbl_id == "OLD" else "소비자물가지수 최신 표"
            return [{"TBL_NM": name}]
        if kind == "ITM":
            label = "소비자물가지수" if tbl_id == "OLD" else "전체"
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": label}]
        if kind == "PRD":
            latest = "2023" if tbl_id == "NEW" else "2015"
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2010", "END_PRD_DE": latest}]
        return []

    try:
        kosis_mcp_server.search_kosis = fake_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.select_table_for_query(
            "소비자물가지수",
            indicator="소비자물가지수",
            api_key="dummy",
        )
    finally:
        kosis_mcp_server.search_kosis = original_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["selected"]["tbl_id"] == "NEW", result
    assert result["selected"]["ranking_features"]["indicator_score"] == 3, result
    assert result["alternatives"][1]["ranking_features"]["indicator_score"] == 5, result
    assert result["selected"]["ranking_features"]["latest_period_year"] == 2023, result
    assert result["ranking_criteria"][1]["field"] == "latest_period_year", result
    assert any("freshest" in reason for reason in result["selected"]["selection_reasons"]), result


async def test_indicator_normalization_prefers_parenthetical_acronym() -> None:
    original_search = kosis_mcp_server.search_kosis
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_search(term: str, limit: int = 10, use_routing: bool = True, api_key: Any = None) -> dict[str, Any]:
        return {
            "결과": [
                {"통계표명": "GDP 디플레이터", "통계표ID": "DEF", "기관ID": "101"},
                {"통계표명": "국내총생산", "통계표ID": "GDP", "기관ID": "101"},
            ],
            "Tier_A_직접_매핑": None,
        }

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "ITM" and tbl_id == "DEF":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "D1", "ITM_NM": "GDP디플레이터"}]
        if kind == "ITM" and tbl_id == "GDP":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "G1", "ITM_NM": "국내총생산(GDP)"}]
        return []

    try:
        kosis_mcp_server.search_kosis = fake_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server._normalize_indicator_from_kosis_meta("GDP", api_key="dummy")
    finally:
        kosis_mcp_server.search_kosis = original_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["normalized"] == "국내총생산(GDP)", result
    assert result["matched_field"] == "ITM_NM", result
    assert result["alternatives"][0]["name"] == "GDP디플레이터", result


async def test_indicator_normalization_uses_route_search_terms() -> None:
    original_search = kosis_mcp_server.search_kosis
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_search(term: str, limit: int = 10, use_routing: bool = True, api_key: Any = None) -> dict[str, Any]:
        if term == "월평균임금":
            rows = [{"통계표명": "임금 현황", "통계표ID": "AVG", "기관ID": "101"}]
        else:
            rows = [{"통계표명": "임금 형태", "통계표ID": "TYPE", "기관ID": "101"}]
        return {"결과": rows, "Tier_A_직접_매핑": None}

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "ITM" and tbl_id == "TYPE":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": "월급제"}]
        if kind == "ITM" and tbl_id == "AVG":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "A1", "ITM_NM": "월평균임금"}]
        return []

    try:
        kosis_mcp_server.search_kosis = fake_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server._normalize_indicator_from_kosis_meta(
            "월급",
            api_key="dummy",
            search_queries=["월평균임금"],
        )
    finally:
        kosis_mcp_server.search_kosis = original_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["normalized"] == "월평균임금", result
    assert result["normalization_query"] == "월평균임금", result
    assert result["alternatives"][0]["name"] == "월급제", result


async def test_indicator_normalization_prefers_relevant_table_name() -> None:
    original_search = kosis_mcp_server.search_kosis
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_search(term: str, limit: int = 10, use_routing: bool = True, api_key: Any = None) -> dict[str, Any]:
        return {
            "결과": [
                {"통계표명": "연도별 전력수급 실적", "통계표ID": "POWER", "기관ID": "101"},
                {"통계표명": "경제성장률(불변가격)", "통계표ID": "GROWTH", "기관ID": "101"},
            ],
            "Tier_A_직접_매핑": None,
        }

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "ITM" and tbl_id == "POWER":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "P1", "ITM_NM": "경제성장률"}]
        if kind == "ITM" and tbl_id == "GROWTH":
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "G1", "ITM_NM": "경제성장률(기준년가격 GDP)"}]
        return []

    try:
        kosis_mcp_server.search_kosis = fake_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server._normalize_indicator_from_kosis_meta("성장률", api_key="dummy")
    finally:
        kosis_mcp_server.search_kosis = original_search  # type: ignore[assignment]
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["tbl_id"] == "GROWTH", result
    assert result["table_name_relevance_score"] > 0, result
    assert result["alternatives"][0]["tbl_id"] == "POWER", result


async def test_plan_query_indicator_normalization_from_kosis_meta() -> None:
    original_normalize = kosis_mcp_server._normalize_indicator_from_kosis_meta

    async def fake_normalize(user_input: Any, api_key: Any = None, **_: Any) -> dict[str, Any]:
        raw = str(user_input)
        if raw == "GDP":
            return {
                "raw_input": raw,
                "normalized": "국내총생산(GDP)",
                "source": "kosis_meta_match",
                "match_evidence": "ITM_NM contains 'GDP'",
                "alternatives": ["GRDP", "GDP 디플레이터"],
            }
        return {"raw_input": raw, "normalized": raw, "source": "passthrough", "alternatives": []}

    try:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = fake_normalize  # type: ignore[assignment]
        result = await kosis_mcp_server.plan_query("GDP")
    finally:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = original_normalize  # type: ignore[assignment]

    dims = result["intended_dimensions"]
    assert dims["indicator"] == "국내총생산(GDP)", result
    assert dims["indicator_raw_input"] == "GDP", result
    assert dims["indicator_normalization"]["source"] == "kosis_meta_match", result
    assert result["metrics"][0]["name"] == "국내총생산(GDP)", result


async def test_plan_query_normalized_metric_references_are_synced() -> None:
    original_normalize = kosis_mcp_server._normalize_indicator_from_kosis_meta

    async def fake_normalize(user_input: Any, api_key: Any = None, **_: Any) -> dict[str, Any]:
        raw = str(user_input)
        if raw == "GDP":
            return {
                "raw_input": raw,
                "normalized": "국내총생산(GDP)",
                "source": "kosis_meta_match",
                "match_evidence": "ITM_NM contains 'GDP'",
                "alternatives": [],
            }
        return {"raw_input": raw, "normalized": raw, "source": "passthrough", "alternatives": []}

    try:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = fake_normalize  # type: ignore[assignment]
        result = await kosis_mcp_server.plan_query("한국 GDP 증가했어?")
    finally:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = original_normalize  # type: ignore[assignment]

    assert result["intended_dimensions"]["indicator"] == "국내총생산(GDP)", result
    task_metrics = [
        metric
        for task in result.get("analysis_tasks") or []
        for metric in task.get("metrics") or []
    ]
    assert "국내총생산(GDP)" in task_metrics, result
    assert "GDP" not in task_metrics, result


async def test_plan_query_change_implication_inference_log() -> None:
    original_normalize = kosis_mcp_server._normalize_indicator_from_kosis_meta

    async def fake_normalize(user_input: Any, api_key: Any = None, **_: Any) -> dict[str, Any]:
        raw = str(user_input)
        return {"raw_input": raw, "normalized": raw, "source": "passthrough", "alternatives": []}

    try:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = fake_normalize  # type: ignore[assignment]
        result = await kosis_mcp_server.plan_query("물가 많이 올랐어?")
    finally:
        kosis_mcp_server._normalize_indicator_from_kosis_meta = original_normalize  # type: ignore[assignment]

    assert result["intent"] == "growth_rate", result
    assert any(task["type"] == "yoy_pct_or_growth_rate" for task in result["analysis_tasks"]), result
    assert result["analysis_tasks_inference_log"][0]["source_field"] == "query_text", result


async def test_plan_query_empty_and_nonstat_need_clarification() -> None:
    empty = await kosis_mcp_server.plan_query("")
    assert empty["status"] == "needs_clarification", empty
    assert empty["route_intents"] == [], empty

    weather = await kosis_mcp_server.plan_query("날씨 어때?")
    assert weather["status"] == "needs_clarification", weather
    assert weather["route_intents"] == [], weather
    assert weather["metrics"] == [], weather
    assert weather["analysis_tasks"] == [], weather
    assert weather["suggested_workflow"] == [], weather
    assert weather["evidence_workflow"] == [], weather


async def test_resolve_concepts_ambiguity_best_selection_reason() -> None:
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "지역 표"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "11", "ITM_NM": "서울특별시"},
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "11020", "ITM_NM": "중구", "UP_ITM_ID": "11"},
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "26010", "ITM_NM": "중구", "UP_ITM_ID": "26"},
            ]
        return []

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.resolve_concepts(
            "101",
            "TBL_FAKE",
            ["서울", "중구"],
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    ambiguity = result["ambiguities"][0]
    assert ambiguity["best"]["ITM_ID"] == "11020", result
    assert ambiguity["best"]["best_selection_reason"] == "context_aware_parent_match", result
    assert "UP_ITM_ID=11" in ambiguity["best"]["context_evidence"], result
    assert ambiguity["best"]["disambiguation_strategy"]["primary"] == "context_aware_parent_match", result


async def test_resolve_concepts_empty_list_has_contract() -> None:
    result = await kosis_mcp_server.resolve_concepts("101", "TBL_FAKE", [], api_key="dummy")
    assert result["status"] == "unsupported", result
    contract = result["mcp_output_contract"]
    assert contract["role"] == "concept_resolution", contract
    markers = contract["current_signals"]["markers_present"]
    assert "invalid_input" in markers, markers


def test_answer_query_deprecated_contract() -> None:
    result = kosis_mcp_server._attach_gemma_deprecation_warning({
        "status": "partial",
        "dropped_dimensions": ["aggregation"],
        "부분충족_사유": ["합계 의도가 단일값으로 축소됨"],
    })
    contract = result["mcp_output_contract"]
    assert contract["role"] == "deprecated_shortcut", result
    markers = contract["current_signals"]["markers_present"]
    assert "partial_fulfillment" in markers, markers
    assert contract["current_signals"]["dropped_dimensions"] == ["aggregation"], contract


async def test_query_table_invalid_filter_has_contract() -> None:
    original_fetch_meta = kosis_mcp_server._fetch_meta

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "임의 표"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": "지표"},
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "11", "ITM_NM": "서울"},
            ]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}]
        return []

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        result = await kosis_mcp_server.query_table(
            "101",
            "TBL_FAKE",
            filters={"ITEM": ["FAKE_CODE"], "BAD_AXIS": ["X"]},
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]

    assert result["code"] == "INVALID_FILTER_CODE", result
    contract = result["mcp_output_contract"]
    assert contract["current_signals"]["has_failures"] is True, contract
    markers = contract["current_signals"]["markers_present"]
    assert "invalid_input" in markers, markers
    assert "validation_errors" in markers, markers


async def test_query_table_period_error_has_contract_and_format_guidance() -> None:
    original_fetch_meta = kosis_mcp_server._fetch_meta
    original_call = kosis_mcp_server._kosis_call

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "monthly table"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "ITEM", "OBJ_NM": "item", "ITM_ID": "T1", "ITM_NM": "metric"},
                {"OBJ_ID": "A", "OBJ_NM": "region", "ITM_ID": "11", "ITM_NM": "Seoul"},
            ]
        if kind == "PRD":
            return [{"PRD_SE": "M", "STRT_PRD_DE": "202001", "END_PRD_DE": "202412"}]
        return []

    async def fake_call(*_: Any, **__: Any) -> list[dict[str, Any]]:
        raise AssertionError("raw KOSIS call should not run for invalid period")

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = fake_call  # type: ignore[assignment]
        result = await kosis_mcp_server.query_table(
            "101",
            "TBL_FAKE",
            filters={"ITEM": ["T1"], "A": ["11"]},
            period_range=["2024-Q1", "2024-Q1"],
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = original_call  # type: ignore[assignment]

    assert result["code"] == "PERIOD_NOT_FOUND", result
    assert result["suggested_period_range"] == ["202401", "202403"], result
    assert result["period_format_examples"]["api_period_type"] == "M", result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "period_not_found" in markers, markers
    assert "period_type_mismatch" in markers, markers


async def test_query_table_projection_data_marker() -> None:
    original_fetch_meta = kosis_mcp_server._fetch_meta
    original_call = kosis_mcp_server._kosis_call

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "population projection"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "ITEM", "OBJ_NM": "item", "ITM_ID": "T1", "ITM_NM": "population", "UNIT_NM": "people"},
                {"OBJ_ID": "A", "OBJ_NM": "region", "ITM_ID": "11", "ITM_NM": "Seoul"},
            ]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2050"}]
        return []

    async def fake_call(*_: Any, **__: Any) -> list[dict[str, Any]]:
        return [{"PRD_DE": "2050", "DT": "123", "ITM_ID": "T1", "C1": "11", "UNIT_NM": "people"}]

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = fake_call  # type: ignore[assignment]
        result = await kosis_mcp_server.query_table(
            "101",
            "TBL_PROJ",
            filters={"ITEM": ["T1"], "A": ["11"]},
            period_range=["2050", "2050"],
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = original_call  # type: ignore[assignment]

    assert result["data_nature"] == "projection", result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "projection_data" in markers, markers
    assert result["mcp_output_contract"]["current_signals"]["data_nature"] == "projection", result


async def test_query_table_fanout_limit_rejects_before_raw_call() -> None:
    original_fetch_meta = kosis_mcp_server._fetch_meta
    original_call = kosis_mcp_server._kosis_call
    original_limit = kosis_mcp_server.QUERY_TABLE_MAX_FANOUT

    async def fake_fetch_meta(client: Any, key: Any, org_id: str, tbl_id: str, kind: str) -> list[dict[str, Any]]:
        if kind == "TBL":
            return [{"TBL_NM": "임의 표"}]
        if kind == "ITM":
            return [
                {"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": "지표"},
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "11", "ITM_NM": "서울"},
                {"OBJ_ID": "A", "OBJ_NM": "지역", "ITM_ID": "21", "ITM_NM": "부산"},
                {"OBJ_ID": "B", "OBJ_NM": "성별", "ITM_ID": "1", "ITM_NM": "남자"},
                {"OBJ_ID": "B", "OBJ_NM": "성별", "ITM_ID": "2", "ITM_NM": "여자"},
            ]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}]
        return []

    async def fake_call(*_: Any, **__: Any) -> list[dict[str, Any]]:
        raise AssertionError("raw KOSIS call should not run after fanout-limit rejection")

    try:
        kosis_mcp_server._fetch_meta = fake_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = fake_call  # type: ignore[assignment]
        kosis_mcp_server.QUERY_TABLE_MAX_FANOUT = 2
        result = await kosis_mcp_server.query_table(
            "101",
            "TBL_FAKE",
            filters={"ITEM": ["T1"], "A": ["11", "21"], "B": ["1", "2"]},
            api_key="dummy",
        )
    finally:
        kosis_mcp_server._fetch_meta = original_fetch_meta  # type: ignore[assignment]
        kosis_mcp_server._kosis_call = original_call  # type: ignore[assignment]
        kosis_mcp_server.QUERY_TABLE_MAX_FANOUT = original_limit

    assert result["code"] == "FANOUT_LIMIT_EXCEEDED", result
    markers = result["mcp_output_contract"]["current_signals"]["markers_present"]
    assert "max_fanout_exceeded" in markers, markers


async def test_http_auth_middleware_requires_token_when_configured() -> None:
    from kosis_http_server import OptionalBearerAuthMiddleware

    async def dummy_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def run_scope(headers: list[tuple[bytes, bytes]], path: str = "/mcp") -> int:
        statuses: list[int] = []

        async def send(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                statuses.append(int(message["status"]))

        app = OptionalBearerAuthMiddleware(dummy_app, "secret")
        await app({"type": "http", "path": path, "headers": headers}, None, send)
        return statuses[0]

    assert await run_scope([]) == 401
    assert await run_scope([(b"authorization", b"Bearer secret")]) == 204
    assert await run_scope([], "/healthz") == 200


async def main() -> None:
    tests = [
        ("fanout_partial_coverage", lambda: test_fanout_partial_coverage()),
        ("indicator_evidence_required", lambda: test_indicator_evidence_required_when_indicator_supplied()),
        ("axis_only_exploration", lambda: test_indicator_evidence_not_required_for_axis_only_exploration()),
        ("select_table_query_fallback", lambda: test_select_table_falls_back_to_query_indicator()),
        ("resolve_concepts_contract", lambda: test_resolve_concepts_contract_on_unresolved()),
        ("explore_table_stale_contract", lambda: test_explore_table_contract_on_stale_period()),
        ("search_kosis_empty_contract", lambda: test_search_kosis_contract_on_empty_results()),
        ("compute_growth_rate", lambda: test_compute_indicator_growth_rate()),
        ("compute_growth_rate_single_row_reason", lambda: test_compute_indicator_growth_rate_single_row_has_reason()),
        ("compute_per_capita_denominator_zero", lambda: test_compute_indicator_per_capita_with_denominator_zero()),
        ("compute_unit_transformation", lambda: test_compute_indicator_unit_transformation_requires_caller_label()),
        ("compute_share_input_total", lambda: test_compute_indicator_share_uses_input_total()),
        ("compute_yoy_pct_intra_year", lambda: test_compute_indicator_yoy_pct_matches_intra_year()),
        ("compute_unknown_operation", lambda: test_compute_indicator_unknown_operation_is_rejected()),
        ("compute_korean_operation_alias", lambda: test_compute_indicator_korean_operation_alias()),
        ("compute_indicator_in_planner", lambda: test_compute_indicator_in_planner_available_tools()),
        ("search_query_preserved", lambda: test_search_kosis_preserves_original_query()),
        ("quick_schema_no_unsupported", lambda: test_quick_tool_schemas_do_not_require_unsupported()),
        ("missing_key_structured", lambda: test_missing_api_key_returns_structured_payload()),
        ("select_table_freshness_near_tiebreak", lambda: test_select_table_prefers_fresh_candidate_on_indicator_near_tie()),
        ("indicator_normalization_parenthetical_acronym", lambda: test_indicator_normalization_prefers_parenthetical_acronym()),
        ("indicator_normalization_route_terms", lambda: test_indicator_normalization_uses_route_search_terms()),
        ("indicator_normalization_table_context", lambda: test_indicator_normalization_prefers_relevant_table_name()),
        ("plan_indicator_normalization", lambda: test_plan_query_indicator_normalization_from_kosis_meta()),
        ("plan_normalized_metric_refs", lambda: test_plan_query_normalized_metric_references_are_synced()),
        ("plan_change_inference_log", lambda: test_plan_query_change_implication_inference_log()),
        ("plan_empty_nonstat_clarification", lambda: test_plan_query_empty_and_nonstat_need_clarification()),
        ("resolve_ambiguity_reason", lambda: test_resolve_concepts_ambiguity_best_selection_reason()),
        ("resolve_empty_contract", lambda: test_resolve_concepts_empty_list_has_contract()),
        ("answer_query_deprecated_contract", lambda: test_answer_query_deprecated_contract()),
        ("query_table_invalid_filter_contract", lambda: test_query_table_invalid_filter_has_contract()),
        ("query_table_period_error_contract", lambda: test_query_table_period_error_has_contract_and_format_guidance()),
        ("query_table_projection_marker", lambda: test_query_table_projection_data_marker()),
        ("query_table_fanout_limit", lambda: test_query_table_fanout_limit_rejects_before_raw_call()),
        ("http_auth_middleware", lambda: test_http_auth_middleware_requires_token_when_configured()),
    ]
    passed = 0
    for name, fn in tests:
        result = fn()
        if asyncio.iscoroutine(result):
            await result
        print(f"{name}: PASS")
        passed += 1
    print(f"SUMMARY {passed}/{len(tests)} PASS")


if __name__ == "__main__":
    asyncio.run(main())
