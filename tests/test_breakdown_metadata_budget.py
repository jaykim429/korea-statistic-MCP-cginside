"""명시한 분해(업종별 등)를 가진 표가 메타 확인 예산에서 밀려나지 않는다 — 왜 있나.

실측(2026-11-03, 챗봇 S3·X6 「에너지 소비량을 업종별로 알려줘」): 이름이 지표와 겹치는 「1인당 최종에너지 소비량」·도시 통계 표가
메타 확인 상한(12)을 채워, 축 검색으로만 나오는 「제조업부문 업종별 에너지소비량」 이 확인 전에 잘렸다.
축 검색은 「산업별 …」 만 붙였는데 KOSIS 표명은 「업종별」 을 많이 쓴다.
"""
import asyncio

import kosis_mcp_server as server


def _install(monkeypatch, routed_rows, lexical_rows_by_query, inspected):
    async def normalize(raw, **kwargs):
        return {"source": "passthrough", "normalized": raw}

    async def search(query, **kwargs):
        if kwargs.get("use_routing"):
            return {"결과": routed_rows}
        return {"결과": lexical_rows_by_query.get(query, [])}

    def name_of(tbl_id):
        for row in [*routed_rows, *[r for rows in lexical_rows_by_query.values() for r in rows]]:
            if row["통계표ID"] == tbl_id:
                return row["통계표명"]
        return ""

    async def meta(client, key, org, tbl_id, kind):
        inspected.add(tbl_id)
        if kind == "TBL":
            return [{"TBL_NM": name_of(tbl_id)}]
        if kind == "PRD":
            return [{"PRD_SE": "Y", "STRT_PRD_DE": "2019", "END_PRD_DE": "2023"}]
        if kind == "ITM":
            axis = [{"OBJ_ID": "IND", "OBJ_NM": "업종별", "ITM_ID": "a", "ITM_NM": "제조업"},
                    {"OBJ_ID": "IND", "OBJ_NM": "업종별", "ITM_ID": "b", "ITM_NM": "광업"}] if "업종별" in name_of(tbl_id) else \
                   [{"OBJ_ID": "A", "OBJ_NM": "행정구역별", "ITM_ID": "11", "ITM_NM": "서울특별시"}]
            return [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T1", "ITM_NM": "에너지소비량", "UNIT_NM": "천toe"}, *axis]
        return []

    async def survey(*args, **kwargs):
        return []

    async def annotation(*args, **kwargs):
        return None

    monkeypatch.setattr(server, "search_kosis", search)
    monkeypatch.setattr(server, "_normalize_indicator_from_kosis_meta", normalize)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    monkeypatch.setattr(server, "_fetch_annotation_measure", annotation)


def test_explicit_breakdown_title_survives_metadata_budget(monkeypatch):
    routed = [{"기관ID": "110", "통계표ID": f"CITY{i}", "통계표명": f"최종 에너지소비량 {i}"} for i in range(16)]
    lexical = {"업종별 에너지소비량": [{"기관ID": "337", "통계표ID": "BY_INDUSTRY", "통계표명": "제조업부문 업종별 에너지소비량"}]}
    inspected: set[str] = set()
    _install(monkeypatch, routed, lexical, inspected)
    result = asyncio.run(server.select_table_for_query(
        "에너지소비량", indicator="에너지소비량", required_dimensions=["industry"], required_breakdowns=["industry"], api_key="dummy"))
    assert "BY_INDUSTRY" in inspected
    assert len(inspected) <= 12  # 상한을 늘려서 풀지 않는다 — 분당 호출 한도
    assert result["selected"]["tbl_id"] == "BY_INDUSTRY"


def test_without_breakdown_request_title_cue_does_not_reorder(monkeypatch):
    # 분해를 요청하지 않았으면 축 낱말이 든 제목을 앞세우지 않는다(축 낱말 편향 방지).
    routed = [{"기관ID": "110", "통계표ID": f"CITY{i}", "통계표명": f"최종 에너지소비량 {i}"} for i in range(16)]
    routed.append({"기관ID": "337", "통계표ID": "BY_INDUSTRY", "통계표명": "제조업부문 업종별 에너지소비량"})
    inspected: set[str] = set()
    _install(monkeypatch, routed, {}, inspected)
    asyncio.run(server.select_table_for_query("에너지소비량", indicator="에너지소비량", api_key="dummy"))
    assert "BY_INDUSTRY" not in inspected
