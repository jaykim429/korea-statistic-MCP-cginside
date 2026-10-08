import asyncio
import pytest
import kosis_curation as c
import kosis_mcp_server as s
from kosis_analysis.quick import leftover_gate_terms
from kosis_analysis.rules import measure_of, measure_relation
from kosis_analysis.metadata import _measure_compatibility


@pytest.mark.parametrize("question,actual,relation", [
    ("소상공인 매출액", "기업체당 매출액", "incompatible"),
    ("기업체당 매출액", "기업체당 매출액", "exact"),
    ("1000명당 발생건수", "1명당 발생건수", "incompatible"),
])
def test_actual_measurement_denominator_is_not_erased(question, actual, relation):
    axes = {"ITEM": {"items": {"T1": {"label": actual, "unit": "백만원"}}}}
    assert _measure_compatibility("주요지표", axes, question)["relation"] == relation


def test_search_broadens_closed_series_quality_and_measure_alias_without_changing_requested_indicator(monkeypatch):
    calls = []

    async def search(client, path, params):
        calls.append(params["searchNm"])
        return []

    monkeypatch.setattr(s, "_kosis_call", search)
    result = asyncio.run(s._search_kosis_keywords("소매판매액지수 계절조정", ["소매판매액지수 계절조정"], 8, api_key="dummy"))
    assert calls == ["소매판매액지수 계절조정", "소매판매액지수"]
    assert result["입력"] == "소매판매액지수 계절조정"
    calls.clear()
    result = asyncio.run(s._search_kosis_keywords("서울 인구이동", ["서울 인구이동"], 8, api_key="dummy"))
    assert calls == ["서울 인구이동", "서울 이동자수"]
    assert result["입력"] == "서울 인구이동"


def test_curated_seed_is_only_a_candidate_and_counts_surviving_rows(monkeypatch):
    async def search(*args, **kwargs):
        return []

    monkeypatch.setattr(s, "_kosis_call", search)
    result = asyncio.run(s._search_kosis_keywords("소상공인사업체수 업종별", ["소상공인사업체수"], 8, api_key="dummy"))
    assert result["결과"]
    assert result["result_count"] == len(result["결과"])
    assert all(row["candidate_basis"] == "verified_curation_table" for row in result["결과"])
    assert all("filters" not in row and "value" not in row for row in result["결과"])
    unknown = asyncio.run(s._search_kosis_keywords("zzz_unknown_zzz", ["zzz_unknown_zzz"], 8, api_key="dummy"))
    assert unknown["result_count"] == 0
    assert unknown["결과"] == []
    sales = asyncio.run(s._search_kosis_keywords("소상공인 매출액", ["소상공인 매출액"], 8, api_key="dummy"))
    assert any(row["통계표ID"] == "DT_3ME0100" for row in sales["결과"])
    assert all("filters" not in row and "value" not in row for row in sales["결과"])


def test_native_trend_retains_month_count_instead_of_interpreting_it_as_years(monkeypatch):
    calls = []

    async def fetch(*args, **kwargs):
        calls.append(kwargs)
        return [{"PRD_DE": "202608", "DT": "2.1"}]

    monkeypatch.setattr(s, "_fetch_series", fetch)
    result = asyncio.run(s._quick_trend_core("실업률 최근 12개월", api_key="dummy", years=5))
    assert calls[0]["period_type"] == "M"
    assert calls[0]["latest_n"] == 12
    assert result["수록주기"] == "M"


@pytest.mark.parametrize("question,count,cadence", [
    ("실업률 최근 12개월", 12, "M"),
    ("출생아 수 지난 6개월", 6, "M"),
    ("GDP 최근 4분기", 4, "Q"),
    ("출생아 수 최근 2년 월별", 2, "Y"),
])
def test_closed_relative_windows(question, count, cadence):
    assert c.requested_series_window(question) == (count, cadence)
    assert c.requests_time_series(question)
    assert c.requested_series_window("창업기업 5년 생존율") is None


def test_latest_phrase_does_not_erase_an_unknown_population():
    param = c.TIER_A_STATS["초미세먼지"] if "초미세먼지" in c.TIER_A_STATS else c.TIER_A_STATS["실업률"]
    query = "초미세먼지" if "초미세먼지" in c.TIER_A_STATS else "실업률"
    assert leftover_gate_terms(f"{query} 최신값", param) == []
    assert leftover_gate_terms(f"최신기술 {query}", param)


def test_migration_measure_is_not_population_or_a_non_migrant():
    assert measure_relation(measure_of("인구이동"), measure_of("총이동자")) == "exact"
    assert measure_relation(measure_of("인구이동"), measure_of("인구(1세이상)")) == "incompatible"
    assert measure_relation(measure_of("인구이동"), measure_of("비이동자")) == "incompatible"
    assert measure_of("인구밀도") is None


@pytest.mark.parametrize("code,label,table,proved", [
    ("00", "전국", "DT_1B040A3", True),
    ("11", "서울", "DT_1B040A3", True),
    ("11", "전국", "DT_1B040A3", False),
    ("00", "전국", "OTHER", False),
    ("CN", "중국", "DT_1B040A3", False),
])
def test_country_proof_requires_actual_verified_region(code, label, table, proved):
    raw = {"status": "executed", "org_id": "101", "tbl_id": table, "source": "KOSIS",
           "route": {"direct_stat_key": "인구"}, "rows": [{"value": "123", "unit": "명", "period": "2025",
           "dimensions": {"ITEM": {"label": "총인구수"}, "region": {"code": code, "label": label}}}]}
    result = s._finalize_answer_query_response(raw, query="우리나라 인구", region="전국", verbose=True)
    assert bool(result["stat_evidence"]["evidence"].get("geography")) is proved
