"""추계표의 기본 시점은 「가장 먼 미래」가 아니라 올해다.

실측(2026-11-06, 101/DT_1YL12501E 「노령화지수(시도)」, 수록 1970~2052): 기간 없이 조회하면 마지막 시점 2052년의
추계치 522.4 를 돌려줬다. 「노령화지수 알려줘」 의 답은 올해 값이다 — 26년 뒤 추계치를 현재 값처럼 내면 틀린 답이다.
미래 시점은 사용자가 그 해를 물었을 때만 낸다.
"""
import asyncio
from datetime import datetime

import kosis_mcp_server as server

THIS_YEAR = str(datetime.now().year)


def install(monkeypatch, end="2052", name="노령화지수(시도)", rows=None):
    calls = []

    async def meta(client, key, org_id, tbl_id, kind):
        return {
            "TBL": [{"TBL_NM": name}],
            "ITM": [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T10", "ITM_NM": "노령화지수", "UNIT_NM": ""}],
            "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "1970", "END_PRD_DE": end}],
        }[kind]

    async def survey(*args):
        return [{"JOSA_NM": "장래인구추계"}]

    async def fetch(client, path, params):
        calls.append({key: value for key, value in params.items() if key != "apiKey"})
        period = params.get("endPrdDe") or end
        return rows if rows is not None else [{"PRD_DE": period, "DT": "200.1", "ITM_ID": "T10", "ITM_NM": "노령화지수", "UNIT_NM": ""}]

    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    monkeypatch.setattr(server, "_kosis_call", fetch)
    return calls


def run(**kwargs):
    return asyncio.run(server.query_table("101", "DT_1YL12501E", {"ITEM": ["T10"]}, api_key="dummy", **kwargs))


def test_implicit_period_on_projection_table_is_this_year(monkeypatch):
    calls = install(monkeypatch)
    result = run()
    assert (calls[0]["startPrdDe"], calls[0]["endPrdDe"]) == (THIS_YEAR, THIS_YEAR)
    assert result["auto_default_period_range"] == [THIS_YEAR, THIS_YEAR]
    assert [r["period"] for r in result["rows"]] == [THIS_YEAR]


def test_latest_keyword_on_projection_table_is_this_year(monkeypatch):
    calls = install(monkeypatch)
    run(period_range=["latest", "latest"])
    assert (calls[0]["startPrdDe"], calls[0]["endPrdDe"]) == (THIS_YEAR, THIS_YEAR)


def test_explicit_future_year_is_kept(monkeypatch):
    calls = install(monkeypatch)
    result = run(period_range=["2052", "2052"])
    assert (calls[0]["startPrdDe"], calls[0]["endPrdDe"]) == ("2052", "2052")
    assert result["period_nature"] == "future_projection"


def test_observed_table_still_uses_its_last_period(monkeypatch):
    last = str(int(THIS_YEAR) - 1)
    calls = install(monkeypatch, end=last, name="주민등록인구")
    run()
    assert (calls[0]["startPrdDe"], calls[0]["endPrdDe"]) == (last, last)


def test_projection_starting_in_the_future_keeps_its_first_year(monkeypatch):
    # 수록이 올해 뒤에서 시작하는 표는 올해가 범위 밖이다 — 수록 첫해로 둔다.
    first = str(int(THIS_YEAR) + 3)

    async def meta(client, key, org_id, tbl_id, kind):
        return {
            "TBL": [{"TBL_NM": "장래가구추계"}],
            "ITM": [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T10", "ITM_NM": "가구수", "UNIT_NM": "가구"}],
            "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": first, "END_PRD_DE": "2052"}],
        }[kind]

    calls = install(monkeypatch)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    run()
    assert (calls[0]["startPrdDe"], calls[0]["endPrdDe"]) == (first, first)


def test_empty_this_year_recovers_nearest_past_period_not_far_future(monkeypatch):
    # 5년 간격 추계(…2025, 2030…2100): 올해가 비면 「표의 최근 5개」(2080~2100)가 아니라 올해 이전 가장 가까운 시점.
    # 실측 101/DT_2KAA202 「부양비 및 노령화지수」(1950~2100) 가 2026 빈 결과 뒤 항목 선택으로 넘어갔다.
    calls = []
    past = str(int(THIS_YEAR) - 1)

    async def meta(client, key, org_id, tbl_id, kind):
        return {
            "TBL": [{"TBL_NM": "부양비 및 노령화지수"}],
            "ITM": [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T10", "ITM_NM": "노령화지수", "UNIT_NM": ""}],
            "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "1950", "END_PRD_DE": "2100"}],
        }[kind]

    async def survey(*args):
        return [{"JOSA_NM": "세계와 한국의 인구현황 및 전망"}]

    async def fetch(client, path, params):
        calls.append({key: value for key, value in params.items() if key != "apiKey"})
        if len(calls) == 1:
            return []
        if params.get("newEstPrdCnt"):
            return [{"PRD_DE": "2100", "DT": "600.0", "ITM_ID": "T10", "ITM_NM": "노령화지수", "UNIT_NM": ""}]
        return [{"PRD_DE": past, "DT": "200.1", "ITM_ID": "T10", "ITM_NM": "노령화지수", "UNIT_NM": ""}]

    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    monkeypatch.setattr(server, "_kosis_call", fetch)
    result = run()
    assert len(calls) == 2 and "newEstPrdCnt" not in calls[1]
    assert int(calls[1]["startPrdDe"]) < int(THIS_YEAR) and calls[1]["endPrdDe"] == THIS_YEAR
    assert [r["period"] for r in result["rows"]] == [past]


def test_projection_quality_note_is_written_for_the_reader_not_the_model():
    # 앱은 data_quality_note 를 사용자 안내로 그대로 싣는다. 실측 S07 답 끝에
    # 「…실측값처럼 단정하지 말고 추계 기준임을 표시하세요.」 가 나갔다 — 모델에게 하는 지시는 추계_안내 에 둔다.
    from kosis_analysis.periods import _query_table_data_nature
    note = _query_table_data_nature("노령화지수(시도)", [THIS_YEAR, THIS_YEAR], "2052")["data_quality_note"]
    assert "추계" in note
    assert not any(word in note for word in ("표시하세요", "단정하지 말고"))
