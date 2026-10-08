import asyncio
from datetime import datetime

import pytest

import kosis_mcp_server as server
from kosis_analysis.quick import leftover_gate_terms, _quick_stat_unsupported_dimensions


@pytest.mark.parametrize("cadence", ["Y", "M", "Q"])
def test_current_latest_refetches_and_filters_future_periods(monkeypatch, cadence):
    now = datetime.now()
    current = str(now.year) if cadence == "Y" else f"{now.year}{now.month if cadence == 'M' else (now.month - 1) // 3 + 1:02d}"
    future = str(now.year + 30) if cadence == "Y" else f"{now.year + 30}01"
    calls = []

    async def fetch(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("latest_n"):
            return [{"PRD_DE": future, "DT": "999"}]
        # Providers may return an out-of-range row. It cannot escape the boundary.
        return [{"PRD_DE": current, "DT": "123"}, {"PRD_DE": future, "DT": "999"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    rows = asyncio.run(server._fetch_latest_current_series(None, "dummy", server.TIER_A_STATS["노령화지수"], "00", period_type=cadence, latest_n=1))
    assert rows == [{"PRD_DE": current, "DT": "123"}]
    assert calls[-1]["end_year"] == current


def test_explicit_available_preserves_future_and_default_latest_does_not(monkeypatch):
    current = str(datetime.now().year)
    future = str(datetime.now().year + 30)

    async def fetch(*args, **kwargs):
        return [{"PRD_DE": future if kwargs.get("latest_n") else current, "DT": "123", "UNIT_NM": "지수"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    latest = asyncio.run(server._quick_stat_core("노령화지수", period="latest", api_key="dummy"))
    available = asyncio.run(server._quick_stat_core("노령화지수", period="latest_available", api_key="dummy"))
    assert latest["used_period"] == current
    assert latest["data_nature"] == "mixed"
    assert available["used_period"] == future
    assert available["data_nature"] == "projection"


def test_explicit_future_value_remains_available_and_labelled(monkeypatch):
    future = str(datetime.now().year + 30)

    async def fetch(*args, **kwargs):
        assert kwargs["end_year"] == future
        return [{"PRD_DE": future, "DT": "123", "UNIT_NM": "지수"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    value = asyncio.run(server._quick_stat_core("노령화지수", period=future, api_key="dummy"))
    assert value["used_period"] == future
    assert value["data_nature"] == "projection"
    assert "추계" in value["answer"]


@pytest.mark.parametrize("group", ["연도별", "년도별", "연별"])
def test_time_grouping_is_not_population_but_single_value_remains_blocked(group):
    query = f"혼인 건수 최근 {group} 추이"
    param = server.TIER_A_STATS["혼인건수"]
    assert leftover_gate_terms(query, param) == []
    assert "time_series" in _quick_stat_unsupported_dimensions(query, param)


@pytest.mark.parametrize("group,cadence", [("월별", "M"), ("월간", "M"), ("분기별", "Q"), ("분기간", "Q"), ("연도별", "Y"), ("연간", "Y")])
def test_temporal_grouping_queries_the_requested_cadence(monkeypatch, group, cadence):
    calls = []

    async def fetch(*args, **kwargs):
        calls.append(kwargs)
        return [{"PRD_DE": "2025" if cadence == "Y" else "202501", "DT": "123"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    query = f"혼인 건수 최근 {group} 추이"
    assert leftover_gate_terms(query, server.TIER_A_STATS["혼인건수"]) == []
    result = asyncio.run(server._quick_trend_core(query, api_key="dummy", years=5))
    assert calls[0]["period_type"] == cadence
    assert result["수록주기"] == cadence


def test_unsupported_monthly_grouping_is_not_silently_mapped_to_annual():
    result = asyncio.run(server._quick_trend_core("노령화지수 월별 추이", api_key="dummy"))
    assert result["코드"] == "PERIOD_TYPE_UNSUPPORTED"


def test_trend_latest_period_is_not_the_first_observation():
    result = server.NaturalLanguageAnswerEngine._finalize_response({
        "상태": "executed", "답변유형": "tier_a_trend", "표": [
            {"시점": "2021", "값": 1}, {"시점": "2025", "값": 2}, {"시점": "2023", "값": 3},
        ],
    })
    assert result["used_period"] == "2025"


def test_default_trend_excludes_future_but_explicit_range_preserves_it(monkeypatch):
    current = str(datetime.now().year)
    future = str(datetime.now().year + 30)

    async def fetch(*args, **kwargs):
        period = future if kwargs.get("latest_n") or kwargs.get("end_year") == future else current
        return [{"PRD_DE": period, "DT": "123", "UNIT_NM": "지수"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    latest = asyncio.run(server._quick_trend_core("노령화지수", api_key="dummy", years=5))
    explicit = asyncio.run(server._quick_trend_core("노령화지수", api_key="dummy", start_year=current, end_year=future))
    assert latest["used_period"] == current
    assert explicit["used_period"] == future
    assert explicit["data_nature"] == "projection"


def test_compact_projection_retains_nature_in_evidence():
    payload = {"status": "executed", "value": 123, "unit": "지수", "used_period": "2052", "source": "KOSIS",
               "org_id": "101", "tbl_id": "DT_TEST", "data_nature": "projection", "data_quality_note": "전망 자료"}
    result = server._finalize_answer_query_response(payload, query="2052년 노령화지수", region="전국", verbose=False)
    assert result["stat_evidence"]["evidence"]["data_nature"] == "projection"


def test_latest_year_wording_is_not_an_unknown_population():
    assert leftover_gate_terms("한국 기대수명 최신 연도", server.TIER_A_STATS["기대수명"]) == []


def test_trade_balance_annual_cadence_is_not_an_unknown_population():
    assert leftover_gate_terms("무역수지 최근 연간 추이", server.TIER_A_STATS["무역수지"]) == []


def test_recent_value_uses_latest_policy_not_default_annual_trend(monkeypatch):
    calls = []

    async def stat(query, region, period, api_key):
        calls.append((query, period))
        return {"값": 63, "단위": "%", "시점": "202608", "지역": "전국", "answer": "2026년 8월 63%"}

    async def trend(*args, **kwargs):
        raise AssertionError("A recent value is not a recent-years trend")

    monkeypatch.setattr(server, "quick_stat", stat)
    monkeypatch.setattr(server, "_quick_trend_core", trend)
    result = asyncio.run(server.NaturalLanguageAnswerEngine("dummy")._answer_direct("고용률 최근 수치", "전국", "고용률"))
    assert result["답변유형"] == "tier_a_value"
    assert calls == [("고용률", "latest")]


@pytest.mark.parametrize("question", ["고용률 최근 수치", "소비자물가지수 최근 수치", "합계출산율 최근 수치 알려줘"])
def test_latest_value_intent_does_not_create_missing_time_series(question):
    import kosis_curation as curation
    assert "STAT_TIME_SERIES" not in curation.DEFAULT_ROUTER.classify_intents(question)
    assert "STAT_TIME_SERIES" in curation.DEFAULT_ROUTER.classify_intents(question + " 최근 5년 추이")
