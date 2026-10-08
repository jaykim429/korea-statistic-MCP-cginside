import asyncio

import pytest

import kosis_mcp_server as server
from kosis_curation import DEFAULT_ROUTER, TIER_A_STATS
from kosis_analysis.rules import measure_relation
from kosis_analysis.metadata import TableMetadataProfile, MetadataCompatibilityScorer
from kosis_analysis.rules import survey_allows_question
from kosis_analysis.rules import measure_of


@pytest.mark.parametrize("unit", ["Wh", "kWh", "MWh", "GWh", "TWh"])
def test_electricity_equivalence_requires_actual_energy_unit(unit):
    assert measure_relation("발전량", "생산량", (unit,)) == "exact"


@pytest.mark.parametrize("unit", ["toe", "톤", "MW", "", "MWh/명"])
def test_no_global_production_generation_alias(unit):
    assert measure_relation("발전량", "생산량", (unit,)) == "incompatible"
    assert measure_relation("발전량", "생산량") == "incompatible"


def test_household_and_broader_daily_waste_are_distinct_verified_queries(monkeypatch):
    calls = []

    async def fetch(*args, **kwargs):
        calls.append((args, kwargs))
        param = args[2]
        return [{"PRD_DE": "2024", "DT": "17045141.1" if param.org_id == "106" else "65098.6",
                 "UNIT_NM": param.unit, "C1": param.obj_l1, "C1_NM": "합계",
                 "C2": param.obj_l2, "C2_NM": "발생량", "ITM_ID": param.item_id, "ITM_NM": param.description}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    for query in ["생활폐기물 발생량", "생활계폐기물 발생량"]:
        key = DEFAULT_ROUTER.match_direct_stat_key(query)
        assert key == query.replace(" ", "")
        result = asyncio.run(server._quick_stat_core(query, api_key="dummy"))
        assert result["단위"] == TIER_A_STATS[key].unit
        assert result["tbl_id"] == TIER_A_STATS[key].tbl_id
        assert result["dimensions"]["region"]["label"] == "전국"
    assert calls[0][0][2].tbl_id != calls[1][0][2].tbl_id


def test_region_comparison_preserves_returned_codes_and_measure(monkeypatch):
    async def fetch(*args, **kwargs):
        return [{"PRD_DE": "2024", "DT": "123", "C1": "11", "C1_NM": "서울",
                 "ITM_ID": "T1", "ITM_NM": "지역내총생산", "UNIT_NM": "백만원"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    result = asyncio.run(server._quick_region_compare_core("지역내총생산", api_key="dummy"))
    assert result["표"][0]["dimensions"]["ITEM"]["code"] == "T1"
    assert result["표"][0]["dimensions"]["region"]["code"] == "11"


def test_compact_representative_value_matches_declared_period_not_first_row():
    payload = {"status": "executed", "답변유형": "tier_a_trend", "used_period": "2025", "단위": "명",
               "표": [{"시점": "2021", "값": "260562"}, {"시점": "2025", "값": "254341"}]}
    compact = server._compact_answer_query_response(payload, query="출생아 수 최근 5년", region="전국")
    assert compact["value"] == "254341"
    assert compact["used_period"] == "2025"


def test_compact_preserves_zero_and_does_not_pick_arbitrary_comparison_row():
    scalar = {"status": "executed", "value": 0, "unit": "%", "used_period": "2025",
              "표": [{"시점": "2025", "값": 99}]}
    assert server._compact_answer_query_response(scalar, query="실업률", region="전국")["value"] == 0
    comparison = {"status": "executed", "used_period": "2025", "단위": "개", "표": [
        {"시점": "2025", "지역": "서울", "값": 1}, {"시점": "2025", "지역": "부산", "값": 2}]}
    assert "value" not in server._compact_answer_query_response(comparison, query="지역 비교", region="전국")


def test_source_survey_population_is_not_widened_by_a_generic_table_title():
    profile = TableMetadataProfile.from_rows("437", "TEST", {}, [{"TBL_NM": "임금근로자 평균 임금"}],
        [{"OBJ_ID": "ITEM", "ITM_ID": "T1", "ITM_NM": "평균임금", "UNIT_NM": "만원"}], [],
        [{"JOSA_NM": "「북한이탈주민실태조사」"}])
    general = MetadataCompatibilityScorer([], indicator="임금근로자 평균임금").evaluate(profile).to_response()
    assert general["status"] == "rejected_survey_population"
    assert general["survey_name"] == "「북한이탈주민실태조사」"
    specific = MetadataCompatibilityScorer([], indicator="북한이탈주민 평균임금").evaluate(profile).to_response()
    assert specific["status"] == "selected"
    assert not survey_allows_question("북한이탈주민 제외 임금", profile.survey_name)
    assert survey_allows_question("임금", "unknown")


def test_survey_population_survives_value_evidence_normalization():
    result = server._finalize_answer_query_response({"status": "executed", "value": "261.4", "unit": "만원",
        "used_period": "2025", "source": "KOSIS", "survey_name": "북한이탈주민실태조사"},
        query="임금", region="전국", verbose=False)
    assert result["stat_evidence"]["evidence"]["survey_name"] == "북한이탈주민실태조사"


def test_compact_selection_preserves_survey_name_for_the_chatbot_judge():
    survey = "「북한이탈주민실태조사」"
    assert server._compact_table_candidate({"survey_name": survey})["survey_name"] == survey


@pytest.mark.parametrize("query", ["여성 기업 수는?", "기업수는！", "기업수는.", "기업수："])
def test_question_punctuation_does_not_erase_count_measure(query):
    assert measure_of(query) == "기업수"


@pytest.mark.parametrize("query", ["기업수출?", "기업 수입?", "기업 수의계약？"])
def test_punctuation_does_not_turn_a_different_word_into_count(query):
    assert measure_of(query) is None
