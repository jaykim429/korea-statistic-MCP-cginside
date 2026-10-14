"""Discovery semantics cannot replace measurement or executed scope proof."""
import asyncio

import pytest
import kosis_mcp_server as server
from kosis_analysis.rules import measure_of, survey_population, survey_allows_question
from kosis_analysis.metadata import TableMetadataProfile, MetadataCompatibilityScorer


@pytest.mark.parametrize("query,indicator,expected", [
    ("전통시장 매출 통계 있어?", "매출액", ["전통시장"]),
    ("온라인쇼핑 거래액 알려줘", "거래액", ["온라인쇼핑"]),
    ("에너지 소비량 업종별로 알려줘", "소비량", []),
    ("학생 수 학교급별로 알려줘", "학생수", []),
    ("2024년 중소기업 수출액", "수출액", ["중소기업"]),
    ("청년고용률", "고용률", ["청년"]),
    ("1인가구 가구수", "가구수", ["1인가구"]),
    ("알 수 없는 대상 거래액", "거래액", ["없는", "대상"]),
])
def test_discovery_subjects_keep_unknown_nouns_but_not_measure_or_breakdown(query, indicator, expected):
    assert server._discovery_subject_terms(query, indicator) == expected


def test_subject_relevance_precedes_generic_measure_hit_count_but_is_not_scope_proof():
    related = {"status": "selected", "tbl_id": "RELATED", "table_name": "연평균매출액",
               "survey_name": "「아산시전통시장및상점가조사」",
               "measure_compatibility": {"asked_measure": "매출액", "relation": "exact", "evidence": [{"source": "ITEM", "measure": "매출액"}]},
               "compatibility": {"indicator_score": 3, "verification_level": "metadata_partial"},
               "population_compatibility": {"requested_terms": ["전통시장"], "matched_items": [], "unverified_terms": ["전통시장"]}}
    unrelated = {**related, "tbl_id": "UNRELATED", "table_name": "매출액 변화", "survey_name": "ICT중소기업실태조사",
                 "compatibility": {"indicator_score": 9}}
    server._annotate_table_candidate_ranking([related, unrelated], query="전통시장 매출", indicator="매출액")
    assert server._table_candidate_sort_key(related) < server._table_candidate_sort_key(unrelated)
    assert related["subject_relevance"]["matched_terms"] == ["전통시장"]
    assert related["population_compatibility"]["unverified_terms"] == ["전통시장"]
    assert server._compact_table_candidate(related)["subject_relevance"] == related["subject_relevance"]


def test_named_metric_subject_does_not_enable_population_measure_priority():
    old = {"status": "selected", "tbl_id": "OLD", "table_name": "소비자물가지수",
           "compatibility": {"indicator_score": 5}, "periods": [{"latest_period": "2015"}],
           "measure_compatibility": {"asked_measure": "지수", "relation": "exact", "evidence": [{"source": "ITEM", "measure": "지수"}]},
           "population_compatibility": {"requested_terms": []}}
    new = {**old, "tbl_id": "NEW", "table_name": "소비자물가지수 최신 표", "compatibility": {"indicator_score": 3},
           "periods": [{"latest_period": "2023"}], "measure_compatibility": {"asked_measure": "지수", "relation": "exact", "evidence": [{"source": "table_name", "measure": "지수"}]}}
    server._annotate_table_candidate_ranking([old, new], query="소비자물가지수", indicator="소비자물가지수")
    assert server._table_candidate_sort_key(new) < server._table_candidate_sort_key(old)


def test_explicit_search_keyword_cannot_replace_the_full_measure(monkeypatch):
    async def normalize(raw, **kwargs):
        return {"source": "passthrough", "normalized": raw}
    async def search(*args, **kwargs):
        return {"결과": [{"기관ID": "1", "통계표ID": "ENERGY", "통계표명": "업종별 에너지소비량"}]}
    async def meta(client, key, org, table, kind):
        return {"TBL": [{"TBL_NM": "업종별 에너지소비량"}], "ITM": [
            {"OBJ_ID": "ITEM", "ITM_ID": "V", "ITM_NM": "에너지사용량", "UNIT_NM": "천toe"},
            {"OBJ_ID": "A", "OBJ_NM": "업종별", "ITM_ID": "M", "ITM_NM": "제조업"},
            {"OBJ_ID": "A", "OBJ_NM": "업종별", "ITM_ID": "E", "ITM_NM": "전기업"},
        ], "PRD": [{"PRD_SE": "Y", "END_PRD_DE": "2024"}]}[kind]
    async def survey(*args):
        return []
    monkeypatch.setattr(server, "_normalize_indicator_from_kosis_meta", normalize)
    monkeypatch.setattr(server, "search_kosis", search)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    result = asyncio.run(server.select_table_for_query("에너지 소비량 업종별", indicator="소비량", required_breakdowns=["industry"], api_key="dummy"))
    assert result["selected"]["measure_compatibility"]["asked_measure"] == "에너지소비량"
    assert result["selected"]["measure_compatibility"]["relation"] == "exact"


def test_verified_online_survey_scope_is_not_generic_trade_or_a_missing_extra_condition():
    assert survey_population("「온라인쇼핑동향조사」") == "온라인쇼핑"
    assert survey_allows_question("온라인쇼핑 거래액", "온라인쇼핑동향조사")
    assert not survey_allows_question("거래액", "온라인쇼핑동향조사")
    assert not survey_allows_question("온라인쇼핑 제외 거래액", "온라인쇼핑동향조사")
    assert survey_population("온라인쇼핑 관련 조사") is None
    assert measure_of("전통시장 매출") is None  # lexical alias does not become identity


@pytest.mark.parametrize("survey,matched,missing", [
    ("「온라인쇼핑동향조사」", ["온라인쇼핑"], ["중소기업"]),
    ("온라인쇼핑 관련 조사", [], ["온라인쇼핑", "중소기업"]),
])
def test_metadata_survey_scope_never_proves_an_additional_population(survey, matched, missing):
    profile = TableMetadataProfile("1", "T", "거래액", {"ITEM": {"items": {"V": {"label": "거래액", "unit": "백만원"}}}}, ["ITEM"], [], survey_name=survey)
    result = MetadataCompatibilityScorer([], "거래액", measure_request="중소기업 온라인쇼핑 거래액", population_terms=["온라인쇼핑", "중소기업"]).evaluate(profile).to_response()
    assert result["population_compatibility"]["matched_items"] == matched
    assert result["population_compatibility"]["unverified_terms"] == missing
    assert result["compatibility"]["verification_level"] == "metadata_partial"


def test_required_fixed_population_can_be_proved_by_the_same_exact_survey_but_not_an_extra_population():
    profile = TableMetadataProfile("1", "T", "주요지표", {"ITEM": {"items": {"V": {"label": "종사자수", "unit": "명"}}}},
                                   ["ITEM"], [], survey_name="「소상공인실태조사」")
    result = MetadataCompatibilityScorer([], "종사자수", required_items=["소상공인"], measure_request="소상공인 종사자수").evaluate(profile).to_response()
    assert result["status"] == "selected"
    assert result["item_evidence"][0]["source"] == "registered_survey_scope"
    missing = MetadataCompatibilityScorer([], "종사자수", required_items=["소상공인", "여성"],
                                         measure_request="여성 소상공인 종사자수").evaluate(profile).to_response()
    assert missing["status"] == "rejected_missing_items"


def test_explore_preserves_every_real_cadence_without_additional_provider_calls(monkeypatch):
    calls = []
    async def meta(client, key, org, table, kind, extra=None):
        calls.append(kind)
        return {"TBL": [{"TBL_NM": "성장률"}], "ITM": [{"OBJ_ID": "ITEM", "ITM_ID": "G", "ITM_NM": "성장률", "UNIT_NM": "%"}],
                "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "2000", "END_PRD_DE": "2025"},
                        {"PRD_SE": "Q", "STRT_PRD_DE": "200001", "END_PRD_DE": "202504"}], "SOURCE": []}[kind]
    monkeypatch.setattr(server, "_fetch_meta", meta)
    result = asyncio.run(server.explore_table("1", "T", api_key="dummy"))
    assert len(result["periods"]) == 2
    assert {row["cadence"] for row in result["periods"]} == {"Y", "Q"}
    assert sorted(calls) == ["ITM", "PRD", "SOURCE", "TBL"]
    assert result["actual_query_supported"] is False


@pytest.mark.parametrize("measure,unit,status", [("종사자수", "명", "selected"), ("사업체수", "개", "needs_table_selection")])
def test_neutral_verified_discovery_anchor_survives_the_same_metadata_budget(monkeypatch, measure, unit, status):
    inspected = set()
    async def normalize(raw, **kwargs):
        return {"source": "passthrough", "normalized": raw}
    async def search(*args, **kwargs):
        return {"Tier_A_직접_매핑": {"기관ID": "1", "통계표ID": "ANCHOR", "통계표": "주요지표"},
                "결과": [{"기관ID": "1", "통계표ID": f"OTHER{i}", "통계표명": f"소상공인 의견조사{i}"} for i in range(14)] +
                [{"기관ID": "1", "통계표ID": "ANCHOR", "통계표명": "주요지표", "candidate_basis": "verified_curation_table"}]}
    async def meta(client, key, org, table, kind):
        inspected.add(table)
        return {"TBL": [{"TBL_NM": "주요지표" if table == "ANCHOR" else "소상공인 의견조사"}],
                "ITM": [{"OBJ_ID": "ITEM", "ITM_ID": "V", "ITM_NM": measure, "UNIT_NM": unit},
                        {"OBJ_ID": "B", "OBJ_NM": "산업별", "ITM_ID": "C", "ITM_NM": "제조업"},
                        {"OBJ_ID": "B", "OBJ_NM": "산업별", "ITM_ID": "G", "ITM_NM": "도소매업"}],
                "PRD": [{"PRD_SE": "Y", "END_PRD_DE": "2024"}]}[kind]
    async def survey(*args):
        return [{"JOSA_NM": "「소상공인실태조사」"}]
    monkeypatch.setattr(server, "_normalize_indicator_from_kosis_meta", normalize)
    monkeypatch.setattr(server, "search_kosis", search)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    result = asyncio.run(server.select_table_for_query("소상공인 종사자 수 업종별", indicator="종사자수",
                          required_breakdowns=["industry"], api_key="dummy", limit=8))
    assert "ANCHOR" in inspected
    assert len(inspected) == 12  # Same max(limit + 4, 12); never raise provider budget.
    assert "stat_evidence" not in result  # Discovery is never executed numeric proof.
    assert result["status"] == status
