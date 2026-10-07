import pytest

from kosis_mcp_server import NaturalLanguageAnswerEngine as Engine


def payload(**overrides):
    return {
        "상태": "executed", "답변유형": "tier_a_value",
        "org_id": "101", "tbl_id": "DT_1YL15006",
        "route": {"direct_stat_key": "상용근로자_월평균임금"},
        **overrides,
    }


@pytest.mark.parametrize("query", ["상용근로자 월평균임금 얼마야?", "월평균 임금 알려줘"])
def test_executed_mean_metric_is_not_missing_an_average(query):
    result = payload()
    route = {"intents": ["STAT_AVERAGE"], "slots": {}}
    assert Engine._fulfillment_gap(result, query, route) is None
    assert Engine._intent_execution_warnings(result, query, route) == []


def test_mean_metric_trend_preserves_other_intent_requirements():
    result = payload(답변유형="tier_a_trend")
    route = {"intents": ["STAT_AVERAGE", "STAT_TIME_SERIES"], "slots": {}}
    assert Engine._fulfillment_gap(result, "최근 5년 월평균 임금 추이", route) is None
    route["intents"].append("STAT_COMPARISON")
    assert "comparison" in Engine._fulfillment_gap(result, "최근 5년 월평균 임금 추이", route)["dropped_dimensions"]


@pytest.mark.parametrize("query", [
    "최근 5년 평균 임금", "월평균 임금의 최근 5년 평균", "지역별 월평균 임금의 평균",
])
def test_extra_averaging_is_not_silently_fulfilled(query):
    assert not Engine._intrinsic_average_fulfilled(payload(), query)
    gap = Engine._fulfillment_gap(payload(), query, {"intents": ["STAT_AVERAGE"]})
    assert "average" in gap["dropped_dimensions"]


@pytest.mark.parametrize("overrides", [
    {"tbl_id": "DT_OTHER"}, {"org_id": "999"}, {"상태": "failed"},
    {"route": {"direct_stat_key": "unknown_mean"}},
    {"route": {"direct_stat_key": "임금상승률"}}, {"답변유형": "search_and_plan"},
])
def test_only_verified_executed_metric_identity_can_fulfill_intrinsic_average(overrides):
    assert not Engine._intrinsic_average_fulfilled(payload(**overrides), "월평균 임금")
