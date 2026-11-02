"""단순 질문의 잘못된 거절 두 건 — 실제 KOSIS 원응답으로 고정한다.

SIM18 「소비자물가 상승률 알려줘」: 통계청 101/DT_1J22042 「월별 소비자물가 등락률」 의 ITEM T03
「전년동월비(%)」 는 단위가 항목명 괄호에만 있고(원행 UNIT_NM 없음), 측정어 「전년동월비」 를 몰라
값이 단위 없는 불완전 근거로 떨어졌다. answer_query 는 지수 표(DT_1J22001)로 가서 거절했다.

CMP04 「농가 수 알려줘」: 101/INH_1EA1045 「농가수(시도/시/군/구)」 의 ITEM 은 영농형태 분류(계·논벼…)라
actual_measure 가 「계」 로 나가 값 계약이 거절했다. 측정어 「농가수」 는 표 제목에만 있다.

견본: tests/fixtures/simple_refusals/kosis-raw.json — 라이브 KOSIS 원응답(메타 TBL/ITM/PRD·조사명·자료 행).
"""
import asyncio
import json
from copy import deepcopy
from pathlib import Path

import pytest

import kosis_mcp_server as server
from kosis_analysis.measure_roles import apply_title_total_measure
from kosis_analysis.metadata import _build_axis_codebook, _measure_compatibility
from kosis_analysis.rules import GEOGRAPHY_RULES, is_change_measure, is_country_axis, measure_of, measure_relation, title_total_measure, _MEASURES, UNIT_RULES
from kosis_analysis.units import official_coordinate_unit
from kosis_curation import TIER_A_STATS, DEFAULT_ROUTER

RAW = json.loads((Path(__file__).parent / "fixtures" / "simple_refusals" / "kosis-raw.json").read_text(encoding="utf-8"))


def data_rows(tbl_id, **params):
    for call in RAW["data_calls"]:
        if call["params"]["tblId"] == tbl_id and all(call["params"].get(k) == v for k, v in params.items()):
            return deepcopy(call["result"])
    raise KeyError((tbl_id, params))


def install(monkeypatch, tbl_id, rows, meta=None):
    meta = meta or RAW["meta"][tbl_id]

    async def fetch_meta(client, key, org_id, tbl, kind):
        assert tbl == tbl_id
        return deepcopy(meta[kind]) if kind in meta else []

    async def survey(*args):
        return deepcopy(RAW["survey"][tbl_id])

    async def call(client, path, params):
        return deepcopy(rows)

    monkeypatch.setattr(server, "_fetch_meta", fetch_meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    monkeypatch.setattr(server, "_kosis_call", call)


def query(tbl_id, filters):
    return asyncio.run(server.query_table("101", tbl_id, filters, api_key="dummy"))


# ---------------------------------------------------------------- SIM18 (1) 항목명 괄호의 단위

def test_cpi_yoy_caption_percent_is_the_observation_unit(monkeypatch):
    rows = data_rows("DT_1J22042", itmId="T03", objL1="0")
    assert "UNIT_NM" not in rows[0]  # 원응답에 단위가 없다
    install(monkeypatch, "DT_1J22042", rows)
    result = query("DT_1J22042", {"ITEM": ["T03"], "I": ["0"]})
    evidence = result["stat_evidence"]
    assert [(row["period"], row["value"], row["unit"]) for row in result["rows"]] == [("202609", 2.9, "%")]
    assert evidence["fulfillment"]["completeness"] == "complete"
    assert evidence["fulfillment"]["missing_fields"] == []
    binding = evidence["evidence"]["unit_coordinate_binding"]
    assert binding["contract_version"] == "stat-unit-coordinates/v2" and binding["unit"] == "%"
    assert binding["source_units"] == [""]


@pytest.mark.parametrize("case", UNIT_RULES["labelOnlyUnitCases"])
def test_shared_caption_unit_contract(case):
    assert official_coordinate_unit("", case["dimensions"]) == case["unit"]


def test_caption_never_overrides_a_source_unit():
    # 원행이 단위를 주면 그것이 주인이다. 괄호는 빈 단위만 채운다.
    assert official_coordinate_unit("원", {"ITEM": {"label": "전년동월비(%)"}}) is None


# ---------------------------------------------------------------- SIM18 (2) 측정 어휘

@pytest.mark.parametrize("case", _MEASURES["contextScopedEquivalenceCases"])
def test_shared_price_change_vocabulary_contract(case):
    asked, actual = measure_of(case["askedText"]), measure_of(case["actualText"])
    assert measure_relation(asked, actual, tuple(case["units"]), context=case["askedText"]) == case["relation"]


def test_price_context_is_required_not_assumed():
    # 맥락 없이 부르면(기존 호출자) 일반어가 구체 기준을 받지 않는다.
    assert measure_relation("상승률", "전년동월비", ("%",)) == "incompatible"


def test_discovery_sees_the_yoy_item_only_for_a_price_rate_request():
    axes, _ = _build_axis_codebook(RAW["meta"]["DT_1J22042"]["ITM"])
    name = RAW["meta"]["DT_1J22042"]["TBL"][0]["TBL_NM"]
    assert _measure_compatibility(name, axes, "소비자물가 상승률")["relation"] == "exact"
    assert _measure_compatibility(name, axes, "소비자물가지수")["relation"] == "incompatible"
    assert _measure_compatibility(name, axes, "취업자 증가율")["relation"] == "incompatible"


# ---------------------------------------------------------------- SIM18 (3) answer_query 큐레이션

@pytest.mark.parametrize("question,key", [
    ("소비자물가 상승률 알려줘", "소비자물가상승률"),
    ("물가상승률", "소비자물가상승률"),
    ("소비자물가 등락률", "소비자물가상승률"),
    # 수준 질문은 그대로 지수다.
    ("소비자물가 알려줘", "소비자물가지수"),
    ("소비자물가지수 알려줘", "소비자물가지수"),
    ("물가 알려줘", "소비자물가지수"),
])
def test_rate_and_level_questions_route_to_different_series(question, key):
    assert DEFAULT_ROUTER.match_direct_stat_key(question) == key


def test_curated_cpi_rate_uses_the_published_monthly_yoy_coordinates():
    param = TIER_A_STATS["소비자물가상승률"]
    assert (param.org_id, param.tbl_id, param.item_id, param.obj_l1, param.unit) == ("101", "DT_1J22042", "T03", "0", "%")
    assert param.supported_periods == ("M",)
    assert [(alt.org_id, alt.tbl_id) for alt in param.alternatives] == [("101", "DT_1J22041")]
    assert measure_of(param.measurement_name) == "상승률"


def install_series(monkeypatch, tbl_id, rows):
    install(monkeypatch, tbl_id, rows)

    async def survey(*args):
        return deepcopy(RAW["survey"][tbl_id])

    monkeypatch.setattr(server, "_fetch_survey_rows", survey)


def test_answer_query_returns_the_published_rate_not_a_rate_of_the_index(monkeypatch):
    install_series(monkeypatch, "DT_1J22042", data_rows("DT_1J22042", itmId="T03", objL1="0"))
    result = asyncio.run(server.answer_query("소비자물가 상승률 알려줘", api_key="dummy"))
    evidence = result["stat_evidence"]
    assert result["status"] == "executed" and result["answer_type"] == "tier_a_value"
    assert (str(result["value"]), result["unit"], result["used_period"]) == ("2.9", "%", "202609")
    assert evidence["evidence"]["table"]["table_id"] == "DT_1J22042"
    assert evidence["fulfillment"]["status"] == "exact"
    assert evidence["fulfillment"]["completeness"] == "complete"
    assert [row["unit"] for row in evidence["evidence"]["observations"]] == ["%"]


def test_a_curated_rate_is_not_re_differenced(monkeypatch):
    # 실측 버그: 「임금상승률 알려줘」 가 상승률의 변화율 +55.00% 를 냈다.
    calls = []

    async def fetch(*args, **kwargs):
        calls.append(kwargs)
        return [{"PRD_DE": "2025", "DT": "3.1", "UNIT_NM": "%", "ITM_ID": "T002", "ITM_NM": "임금상승률", "C1": "00", "C1_NM": "전국"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    result = asyncio.run(server.answer_query("임금상승률 알려줘", api_key="dummy"))
    assert result.get("answer_type") == "tier_a_value"
    assert str(result.get("value")) == "3.1"
    assert all(call.get("latest_n") == 1 for call in calls)


@pytest.mark.parametrize("question", ["소비자물가지수 상승률 알려줘", "소비자물가지수 전년대비 얼마나 올랐어"])
def test_index_change_questions_do_not_take_the_published_rate_shortcut(question):
    # 지수 키는 % 가 아니라 계산 경로가 그대로 맡는다.
    assert not server.NaturalLanguageAnswerEngine._asks_curated_rate_itself(question, TIER_A_STATS["소비자물가지수"])


@pytest.mark.parametrize("case", _MEASURES["changeMeasureCases"], ids=lambda case: case["text"])
def test_shared_change_measure_contract(case):
    assert is_change_measure(case["text"]) is case["change"]


@pytest.mark.parametrize("question", [
    # 비교 기준이 다르면 공표 전년동월비 키가 답이 아니다(리뷰 F1).
    "소비자물가 상승률 전월 대비", "전월 대비 소비자물가 상승률", "소비자물가 월간 상승률",
    "연간 소비자물가 상승률", "소비자물가 상승률 전분기 대비",
    # 변화어가 남으면 비율 자체가 아니다.
    "소비자물가 상승률 증감", "소비자물가 상승률 변화", "소비자물가 상승률 작년과 차이",
    "소비자물가 상승률 %p", "소비자물가 상승률 몇 포인트",
])
def test_basis_or_change_words_block_the_published_rate_shortcut(question):
    assert not server.NaturalLanguageAnswerEngine._asks_curated_rate_itself(question, TIER_A_STATS["소비자물가상승률"])


@pytest.mark.parametrize("question", ["소비자물가 상승률 %p", "임금상승률 얼마나 늘었어", "소비자물가 상승률 증가"])
def test_change_of_a_published_rate_is_refused_not_re_differenced(monkeypatch, question):
    # 실측(f79b0d8 이전 빌드): 「소비자물가 상승률 %p」 → tier_a_growth_rate 변화율 +123.08%.
    async def fetch(*args, **kwargs):
        raise AssertionError("a published rate must not be fetched for a relative-change calculation")

    monkeypatch.setattr(server, "_fetch_series", fetch)
    result = asyncio.run(server.answer_query(question, api_key="dummy"))
    assert result.get("answer_type") != "tier_a_growth_rate"
    assert result.get("status") != "executed"


def test_change_of_a_rate_still_goes_to_calculation():
    param = TIER_A_STATS["소비자물가상승률"]
    assert server.NaturalLanguageAnswerEngine._asks_curated_rate_itself("소비자물가 상승률 알려줘", param)
    assert not server.NaturalLanguageAnswerEngine._asks_curated_rate_itself("소비자물가 상승률 전년대비 얼마나 늘었어", param)


# ---------------------------------------------------------------- CMP04 표 제목의 측정어

@pytest.mark.parametrize("case", _MEASURES["titleTotalMeasure"]["cases"], ids=lambda case: case["title"])
def test_shared_title_measure_contract(case):
    assert title_total_measure(case["title"]) == case["measure"]


@pytest.mark.parametrize("region,value", [("000", 973707), ("37", 162801)])
def test_total_row_of_a_classification_item_measures_the_title(monkeypatch, region, value):
    install(monkeypatch, "INH_1EA1045", data_rows("INH_1EA1045", itmId="T00", objL1=region))
    result = query("INH_1EA1045", {"ITEM": ["T00"], "A": [region]})
    evidence = result["stat_evidence"]["evidence"]
    assert [(row["value"], row["unit"]) for row in result["rows"]] == [(value, "가구")]
    assert (evidence["actual_measure"], evidence["actual_measure_evidence"]) == ("농가수", "official_title_total_item")
    role = evidence["measure_role_evidence"]
    assert role["contract_version"] == "kosis-title-measure/v1"
    assert (role["table_name"], role["item_code"], role["item_label"]) == ("농가수(시도/시/군/구)", "T00", "계")
    assert role["item_labels"] == ["계", "논벼", "식량작물", "채소·산나물", "과수", "기타작물", "축산"]
    assert result["stat_evidence"]["fulfillment"]["completeness"] == "complete"


def test_a_member_row_is_never_the_title_measure(monkeypatch):
    install(monkeypatch, "INH_1EA1045", data_rows("INH_1EA1045", itmId="T01", objL1="000"))
    result = query("INH_1EA1045", {"ITEM": ["T01"], "A": ["000"]})
    evidence = result["stat_evidence"]["evidence"]
    assert (evidence["actual_measure"], evidence["actual_measure_evidence"]) == ("논벼", "ITEM_dimension")
    assert evidence["measure_role_evidence"] is None


def title_payload(**overrides):
    rows = data_rows("INH_1EA1045", itmId="T00", objL1="000")
    payload = {"org_id": "101", "tbl_id": "INH_1EA1045", "table_name": "농가수(시도/시/군/구)", "aggregation": "none",
               "rows": [{"period": "2024", "value": 973707, "unit": "가구",
                         "dimensions": {"ITEM": {"code": "T00", "label": "계", "unit": None},
                                        "A": {"code": "000", "label": "전국", "unit": None}}}]}
    assert rows[0]["ITM_NM"] == "계"
    payload.update(overrides)
    return payload


def farm_axes(extra_items=(), item_axis_name="항목", region_axis_name="행정구역별"):
    meta = deepcopy(RAW["meta"]["INH_1EA1045"]["ITM"])
    for row in meta:
        if row["OBJ_ID"] == "ITEM":
            row["OBJ_NM"] = item_axis_name
        else:
            row["OBJ_NM"] = region_axis_name
    meta.extend({"OBJ_ID": "ITEM", "OBJ_NM": item_axis_name, "ITM_ID": code, "ITM_NM": label} for code, label in extra_items)
    return _build_axis_codebook(meta)[0]


def test_title_binding_baseline():
    payload = title_payload()
    apply_title_total_measure(payload, farm_axes())
    assert payload["actual_measure"] == "농가수"


@pytest.mark.parametrize("mutate", [
    # ITEM 축에 측정어가 하나라도 있으면 ITEM 이 측정축이다.
    lambda p: ({}, farm_axes(extra_items=[("T90", "농가인구")])),
    # 총계가 둘이면 어느 것인지 고르지 않는다.
    lambda p: ({}, farm_axes(extra_items=[("T91", "합계")])),
    # 제목에 한정어·다른 측정어.
    lambda p: ({"table_name": "전업농가수(시도/시/군/구)"}, farm_axes()),
    lambda p: ({"table_name": "농가수(전업)"}, farm_axes()),
    lambda p: ({"table_name": "농가수 및 농가인구(시도)"}, farm_axes()),
    # 사람 수 단위는 농가 수가 아니다.
    lambda p: ({"rows": [{**p["rows"][0], "unit": "명"}]}, farm_axes()),
    # 다른 축이 측정축이다.
    lambda p: ({}, farm_axes(region_axis_name="지표")),
    # CMMT 정의나 다른 근거가 이미 있다.
    lambda p: ({"measure_role_evidence": {"contract_version": "kosis-measure-role/v1"}}, farm_axes()),
    lambda p: ({"actual_measure": "계"}, farm_axes()),
    # 합산 결과는 관측 행이 아니다.
    lambda p: ({"aggregation": "sum_by_group"}, farm_axes()),
    # 다른 분류 축의 부분 집단(경영주 30대)은 농가수 전체가 아니다(리뷰 F4).
    lambda p: ({"rows": [{**p["rows"][0], "dimensions": {**p["rows"][0]["dimensions"],
                                                         "C2": {"code": "3", "label": "30대", "unit": None}}}]}, farm_axes()),
])
def test_title_binding_refuses_every_ambiguous_case(mutate):
    payload = title_payload()
    overrides, axes = mutate(payload)
    payload.update(overrides)
    before = payload.get("actual_measure")
    apply_title_total_measure(payload, axes)
    assert payload.get("actual_measure") == before
    assert payload.get("actual_measure_evidence") != "official_title_total_item"


def test_mixed_total_and_member_rows_do_not_bind():
    payload = title_payload()
    payload["rows"].append({"period": "2024", "value": 364578, "unit": "가구",
                            "dimensions": {"ITEM": {"code": "T01", "label": "논벼", "unit": None},
                                           "A": {"code": "000", "label": "전국", "unit": None}}})
    apply_title_total_measure(payload, farm_axes())
    assert "actual_measure" not in payload


# ---------------------------------------------------------------- 검토 5차 G1b 외국 좌표

@pytest.mark.parametrize("case", GEOGRAPHY_RULES["countryAxisCases"], ids=lambda case: case["axis"])
def test_shared_country_axis_contract(case):
    assert is_country_axis(case["axis"]) is case["country"]


def test_query_table_names_its_axes_so_a_foreign_coordinate_is_visible(monkeypatch):
    install(monkeypatch, "INH_1EA1045", data_rows("INH_1EA1045", itmId="T00", objL1="37"))
    result = query("INH_1EA1045", {"ITEM": ["T00"], "A": ["37"]})
    assert result["stat_evidence"]["evidence"]["coordinate_axis_names"] == {"ITEM": "항목", "A": "행정구역별"}
