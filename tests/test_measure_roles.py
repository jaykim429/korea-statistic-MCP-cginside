import asyncio

import kosis_mcp_server as server
from kosis_analysis.metadata import MetadataCompatibilityScorer, TableMetadataProfile
from kosis_analysis.measure_roles import annotation_measure, apply_annotation_measure
from kosis_analysis.evidence import build_stat_evidence


def test_official_definition_proves_measure_not_population_item_or_currency_alone():
    rows = [{"CMMT_DC": "1. 수출액 기준 : 통관기초자료로 작성되는 수출통계 전체"}]
    definition = annotation_measure(rows, [{"UNIT_NM": "달러"}])
    assert definition["measure"] == "수출액"
    assert definition["source"] == "CMMT"
    assert annotation_measure([], [{"UNIT_NM": "달러"}]) is None
    assert annotation_measure(rows, [{"UNIT_NM": "%"}]) is None
    assert annotation_measure([{"CMMT_DC": "기업규모는 수출액 기준으로 나눕니다"}], [{"UNIT_NM": "달러"}]) is None
    assert annotation_measure(rows + [{"CMMT_DC": "2. 매출액 기준 : 조사기준"}], [{"UNIT_NM": "달러"}]) is None


def test_definition_applies_only_to_matching_executed_units_and_never_overwrites_real_item():
    definition = annotation_measure([{"CMMT_DC": "수출액 기준 : 통관자료"}], [{"UNIT_NM": "달러"}])
    payload = {"rows": [{"unit": "달러", "value": 12, "dimensions": {"ITEM": {"label": "중소기업"}}}]}
    apply_annotation_measure(payload, definition)
    assert payload["actual_measure"] == "수출액"
    for item, unit in [("매출액", "달러"), ("중소기업", "%"), ("중소기업", "원")]:
        p = {"rows": [{"unit": unit, "value": 12, "dimensions": {"ITEM": {"label": item}}}]}
        apply_annotation_measure(p, definition)
        assert "actual_measure" not in p


def test_joint_dimensions_need_independent_axes_even_when_required_item_matches():
    rows = [
        {"OBJ_ID": "ITEM", "ITM_ID": "V", "ITM_NM": "기업수", "OBJ_NM": "항목"},
        {"OBJ_ID": "A", "ITM_ID": "R", "ITM_NM": "지역별", "OBJ_NM": "특성별"},
        {"OBJ_ID": "A", "ITM_ID": "I", "ITM_NM": "업종별", "OBJ_NM": "특성별"},
        {"OBJ_ID": "B", "ITM_ID": "S", "ITM_NM": "중소기업", "OBJ_NM": "기업규모별"},
    ]
    p = TableMetadataProfile.from_rows("1", "T", {}, [{"TBL_NM": "기업수"}], rows, [])
    result = MetadataCompatibilityScorer(["region", "industry"], "기업수", required_items=["중소기업"], required_breakdowns=["region", "industry"]).evaluate(p)
    assert result.status == "rejected_missing_dimensions"
    assert set(result.missing_dimensions) == {"region", "industry"}


def test_same_name_historical_and_different_owner_tables_survive_search(monkeypatch):
    async def call(*args):
        return [{"ORG_ID": org, "TBL_ID": table, "TBL_NM": "벤처기업 경영성과 현황", "STRT_PRD_DE": year, "END_PRD_DE": year}
                for org, table, year in [("142", "NEW", "2024"), ("142", "OLD", "2020"), ("143", "OLD", "2020")]]
    monkeypatch.setattr(server, "_kosis_call", call)
    result = asyncio.run(server._search_kosis_keywords("벤처기업", ["벤처기업"], 10, api_key="dummy"))
    assert {(r["기관ID"], r["통계표ID"]) for r in result["결과"]} == {("142", "NEW"), ("142", "OLD"), ("143", "OLD")}


def test_one_axis_cannot_satisfy_two_breakdown_roles():
    rows = [{"OBJ_ID": "ITEM", "ITM_ID": "V", "ITM_NM": "기업수", "OBJ_NM": "항목"},
            {"OBJ_ID": "A", "ITM_ID": "1", "ITM_NM": "서울", "OBJ_NM": "지역별 산업별"},
            {"OBJ_ID": "A", "ITM_ID": "2", "ITM_NM": "제조업", "OBJ_NM": "지역별 산업별"}]
    p = TableMetadataProfile.from_rows("1", "T", {}, [{"TBL_NM": "기업수"}], rows, [])
    assert MetadataCompatibilityScorer(["region", "industry"], "기업수", required_breakdowns=["region", "industry"]).evaluate(p).status == "rejected_missing_dimensions"


def test_explicit_breakdown_remains_required_when_legacy_dimensions_are_relaxed():
    rows = [{"OBJ_ID": "ITEM", "ITM_ID": "V", "ITM_NM": "기업수", "OBJ_NM": "항목"},
            {"OBJ_ID": "A", "ITM_ID": "1", "ITM_NM": "중소기업", "OBJ_NM": "특성별"}]
    p = TableMetadataProfile.from_rows("1", "T", {}, [{"TBL_NM": "기업수"}], rows, [])
    assert MetadataCompatibilityScorer([], "기업수", required_items=["중소기업"], required_breakdowns=["region"]).evaluate(p).status == "rejected_missing_dimensions"


def test_definition_is_same_measure_proof_in_metadata_and_execution():
    definition = annotation_measure([{"CMMT_DC": "1. 수출액 기준 : 통관자료"}], [{"UNIT_NM": "달러"}])
    rows = [{"OBJ_ID": "ITEM", "ITM_ID": "S", "ITM_NM": "중소기업", "OBJ_NM": "항목"}]
    p = TableMetadataProfile.from_rows("142", "T", {}, [{"TBL_NM": "지역별 중소기업 수출"}], rows, [], measure_definition=definition)
    assert MetadataCompatibilityScorer([], "수출액").evaluate(p).status == "selected"
    assert MetadataCompatibilityScorer([], "기업수").evaluate(p).status == "rejected_measurement"


def test_discovery_alias_is_not_measure_equivalence():
    from kosis_analysis.rules import measure_of
    from kosis_analysis.text_match import _query_match_quality
    assert _query_match_quality("중소기업 수출액", "지역별 중소기업 수출")["coverage_ratio"] == 1
    assert measure_of("지역별 중소기업 수출") is None
    p = TableMetadataProfile.from_rows("142", "T", {}, [{"TBL_NM": "지역별 중소기업 수출"}],
        [{"OBJ_ID": "ITEM", "ITM_ID": "S", "ITM_NM": "중소기업"}], [])
    assert MetadataCompatibilityScorer([], "수출액").evaluate(p).status == "not_matched_indicator"


def test_provider_suppression_is_known_missing_not_a_network_or_zero_value():
    payload = {"status": "executed", "org_id": "142", "tbl_id": "T", "rows": [
        {"value": 79, "unit": "개", "period": "2023"},
        {"value": None, "value_raw": "*", "missing_reason": "suppressed", "unit": "개", "period": "2023"},
    ]}
    result = build_stat_evidence(payload, tool="query_table")
    assert result["fulfillment"]["status"] == "exact"
    assert result["evidence"]["observations"][1]["value"] is None
    assert result["evidence"]["observations"][1]["observation_state"] == "suppressed"
    # A generic null, missing units, all-withheld response, or failed calls remain unusable.
    for symbol in ["", "-", "..."]:
        payload["rows"][1]["value_raw"] = symbol
        assert build_stat_evidence(payload, tool="query_table")["fulfillment"]["completeness"] == "incomplete"
    payload["rows"][1]["value_raw"] = "*"
    payload["rows"] = [payload["rows"][1]]
    assert build_stat_evidence(payload, tool="query_table")["fulfillment"]["completeness"] == "incomplete"


def test_numeric_zero_is_not_a_missing_value():
    from kosis_analysis.metadata import _normalize_stat_value
    for value in [0, "0", "0.0"]:
        row = _normalize_stat_value(value)
        assert row["value"] == 0
        assert row["missing_reason"] is None
