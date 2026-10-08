import asyncio

import pytest

import kosis_mcp_server as server
from kosis_curation import DEFAULT_ROUTER, TIER_A_STATS
from kosis_analysis.rules import measure_relation


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
