import asyncio

import kosis_mcp_server as server


def test_original_population_is_not_squeezed_out_by_normalized_measure_titles(monkeypatch):
    async def normalization(*args, **kwargs):
        return {"source": "kosis_meta_match", "normalized": "수출액"}

    rows = [{"기관ID": "101", "통계표ID": f"GENERIC{i}", "통계표명": f"다른 분야 수출액 {i}"} for i in range(16)]
    rows.append({"기관ID": "142", "통계표ID": "REQUESTED", "통계표명": "지역별 중소기업 수출"})
    inspected = set()

    async def search(*args, **kwargs):
        return {"결과": rows}

    async def meta(client, key, org_id, tbl_id, kind):
        inspected.add(tbl_id)
        return {"TBL": [{"TBL_NM": next(row["통계표명"] for row in rows if row["통계표ID"] == tbl_id)}],
            "ITM": [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "VALUE", "ITM_NM": "수출액", "UNIT_NM": "달러"}],
            "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"}]}[kind]

    async def survey(*args):
        return []

    monkeypatch.setattr(server, "_normalize_indicator_from_kosis_meta", normalization)
    monkeypatch.setattr(server, "search_kosis", search)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    result = asyncio.run(server.select_table_for_query("중소기업 수출액", indicator="중소기업 수출액", api_key="dummy"))
    assert "REQUESTED" in inspected
    assert len(inspected) <= 12  # Do not fix retrieval by increasing provider load.
    assert result["selected"]["tbl_id"] == "REQUESTED"
