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


async def main() -> None:
    tests = [
        ("fanout_partial_coverage", lambda: test_fanout_partial_coverage()),
        ("indicator_evidence_required", lambda: test_indicator_evidence_required_when_indicator_supplied()),
        ("axis_only_exploration", lambda: test_indicator_evidence_not_required_for_axis_only_exploration()),
        ("select_table_query_fallback", lambda: test_select_table_falls_back_to_query_indicator()),
        ("resolve_concepts_contract", lambda: test_resolve_concepts_contract_on_unresolved()),
        ("explore_table_stale_contract", lambda: test_explore_table_contract_on_stale_period()),
        ("search_kosis_empty_contract", lambda: test_search_kosis_contract_on_empty_results()),
        ("search_query_preserved", lambda: test_search_kosis_preserves_original_query()),
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
