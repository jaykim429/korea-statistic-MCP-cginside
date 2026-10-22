"""Curation is a table catalog lead, never fixed subgroup/coordinate approval."""
import asyncio
from dataclasses import replace

import kosis_mcp_server as server
from kosis_curation import QuickStatParam


def param(table, description, *, unit="개", **extra):
    return QuickStatParam(org_id="999", tbl_id=table, tbl_nm="중립적인 표 제목", description=description,
                          obj_l1="CURATED_TOTAL", obj_l2="WRONG_POPULATION", item_id="CURATED_ITEM", unit=unit, **extra)


def search(monkeypatch, query, entries, limit=8):
    async def empty(*args, **kwargs):
        return []
    monkeypatch.setattr(server, "TIER_A_STATS", entries)
    monkeypatch.setattr(server, "_kosis_call", empty)
    return asyncio.run(server._search_kosis_keywords(query, [query], limit, api_key="dummy"))


def test_new_measure_in_known_population_reserves_measure_and_population_leads(monkeypatch):
    entries = {"수제조합_사업체수": param("COUNT", "수제조합 사업체수"),
               "다른집단_매출액": param("SALES", "다른집단 매출액", unit="백만원")}
    result = search(monkeypatch, "수제조합 매출액", entries)
    assert [r["통계표ID"] for r in result["결과"]] == ["SALES", "COUNT"]
    assert result["result_count"] == 2
    for row in result["결과"]:
        assert not ({"filters", "obj_l1", "obj_l2", "item_id", "value", "actual_query_supported"} & row.keys())
    assert result["결과"][0]["curation_discovery_roles"]["measure_match"] is True
    assert result["결과"][0]["curation_discovery_roles"]["subject_match"] is False


def test_same_measure_lead_does_not_import_its_curated_population(monkeypatch):
    result = search(monkeypatch, "새로운집단 매출액", {"다른집단_매출액": param("SALES", "다른집단 매출액", unit="원")})
    assert [r["통계표ID"] for r in result["결과"]] == ["SALES"]
    assert "WRONG_POPULATION" not in str(result["결과"])


def test_total_and_per_enterprise_are_not_equal_measure_leads(monkeypatch):
    result = search(monkeypatch, "신규집단 매출액", {
        "가_평균": param("AVERAGE", "기업체당 매출액", unit="백만원"),
        "나_총액": param("TOTAL", "매출액", unit="백만원"),
    })
    assert [r["통계표ID"] for r in result["결과"]] == ["TOTAL"]


def test_unknown_request_is_not_permission_to_seed_every_verified_table(monkeypatch):
    result = search(monkeypatch, "알수없는대상 알수없는측정", {"다른집단_매출액": param("SALES", "다른집단 매출액")})
    assert result["결과"] == []


def test_only_current_verified_deduplicated_identities_enter_the_same_cap(monkeypatch):
    current = param("CURRENT", "매출액", unit="원")
    result = search(monkeypatch, "신규집단 매출액", {
        "가_매출액": current, "나_매출액": current,
        "다_매출액": replace(current, tbl_id="OLD", replacement_status="replaced"),
        "라_매출액": replace(current, tbl_id="UNVERIFIED", verification_status="unverified"),
        "마_매출액": replace(current, tbl_id="SECOND"),
        "바_매출액": replace(current, tbl_id="THIRD"),
    })
    assert len(result["결과"]) == 2
    assert len({r["통계표ID"] for r in result["결과"]}) == 2
    assert not {"OLD", "UNVERIFIED"} & {r["통계표ID"] for r in result["결과"]}


def test_metadata_must_resolve_the_new_population_and_measure_not_curated_coordinates(monkeypatch):
    entries = {"수제조합_사업체수": param("COUNT", "수제조합 사업체수"),
               "다른집단_매출액": param("SALES", "다른집단 매출액", unit="원")}
    monkeypatch.setattr(server, "TIER_A_STATS", entries)
    calls = []
    async def empty(*args, **kwargs):
        return []
    async def normalize(raw, **kwargs):
        return {"source": "passthrough", "normalized": raw}
    async def candidate_search(query, **kwargs):
        return await server._search_kosis_keywords(query, [query], kwargs.get("limit", 8), api_key="dummy")
    async def meta(client, key, org, table, kind):
        calls.append(table)
        return {"TBL": [{"TBL_NM": "표 제목"}],
                "ITM": [{"OBJ_ID": "ITEM", "ITM_ID": "REAL_ITEM", "ITM_NM": "매출액" if table == "SALES" else "사업체수", "UNIT_NM": "원" if table == "SALES" else "개"},
                        {"OBJ_ID": "UNSEEN_SIZE_AXIS", "OBJ_NM": "집단별", "ITM_ID": "REAL_MEMBER", "ITM_NM": "수제조합"}],
                "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}]}[kind]
    monkeypatch.setattr(server, "_kosis_call", empty)
    monkeypatch.setattr(server, "_normalize_indicator_from_kosis_meta", normalize)
    monkeypatch.setattr(server, "search_kosis", candidate_search)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", empty)
    monkeypatch.setattr(server, "_fetch_annotation_measure", empty)
    result = asyncio.run(server.select_table_for_query("수제조합 매출액", indicator="매출액", required_items=["수제조합"], api_key="dummy"))
    assert "SALES" in calls
    assert result["selected"]["tbl_id"] == "SALES"
    assert result["selected"]["population_compatibility"]["matched_items"] == ["수제조합"]
    assert "stat_evidence" not in result
    assert "WRONG_POPULATION" not in str(result)
