"""StatEvidenceEnvelope v1 contract tests.

The envelope is additive: existing tool response fields remain available while
clients get one stable place to inspect execution, fulfillment, and evidence.
"""

import asyncio
import pytest
from kosis_curation import TIER_A_STATS

from kosis_analysis.evidence import attach_stat_evidence, build_stat_evidence, kosis_row_dimensions


def test_raw_kosis_scope_is_retained_and_regions_are_derived_from_returned_codes():
    dimensions = kosis_row_dimensions({"C1": "IM_I", "C1_NM": "숙박 및 음식점업", "C2": "SEOUL", "C2_NM": "서울특별시",
                                      "C3": "SMALL", "C3_NM": "소상공인", "ITM_ID": "T1", "ITM_NM": "기업수"},
                                     region_field="C2", region_labels={"SEOUL": "서울"})
    assert dimensions["C1"]["label"] == "숙박 및 음식점업"
    assert dimensions["C3"]["label"] == "소상공인"
    assert dimensions["region"] == {"code": "SEOUL", "label": "서울"}
    assert kosis_row_dimensions({"C2": "OTHER"}, region_field="C2", region_labels={"SEOUL": "서울"}) == {}


def test_direct_value_and_trend_keep_executed_population_labels(monkeypatch):
    import kosis_mcp_server as server

    async def fetch(*_args, **_kwargs):
        return [{"DT": "846531", "PRD_DE": "2023", "UNIT_NM": "개", "ITM_ID": "T001", "ITM_NM": "기업수",
                 "C1": "IM_I", "C1_NM": "숙박 및 음식점업", "C2": "15142C501", "C2_NM": "전국",
                 "C3": "16142T2524", "C3_NM": "소상공인"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    key = "숙박음식점업_소상공인_사업체수"
    value = asyncio.run(server._quick_stat_core(key, "전국", "2023", "dummy"))
    trend = asyncio.run(server._quick_trend_core(key, "전국", 1, "dummy", start_year="2023", end_year="2023"))
    assert value["dimensions"]["C3"]["label"] == "소상공인"
    assert trend["시계열"][0]["dimensions"]["C1"]["label"] == "숙박 및 음식점업"
    for payload in [value, {**trend, "status": "executed"}]:
        contract = build_stat_evidence(payload, tool="answer_query")
        assert contract["evidence"]["observations"][0]["dimensions"]["region"]["label"] == "전국"


@pytest.mark.parametrize("key", [key for key, param in TIER_A_STATS.items()
                                 if param.region_scheme is None and param.verification_status == "verified"])
def test_nonregional_curated_value_and_trend_do_not_require_a_region_scheme(monkeypatch, key):
    import kosis_mcp_server as server
    param = server.TIER_A_STATS[key]

    async def fetch(*_args, **_kwargs):
        return [{"DT": "83.69", "PRD_DE": "2024", "UNIT_NM": param.unit,
                 "ITM_ID": param.item_id, "ITM_NM": param.description,
                 "C1": "050", "C1_NM": "0세"}]

    monkeypatch.setattr(server, "_fetch_series", fetch)
    value = asyncio.run(server._quick_stat_core(key, "전국", "2024", "dummy"))
    trend = asyncio.run(server._quick_trend_core(key, "전국", 1, "dummy", start_year="2024", end_year="2024"))
    assert value["값"] == "83.69"
    assert trend["시계열"][0]["값"] == "83.69"
    assert value["dimensions"]["C1"]["label"] == "0세"
    assert "region" not in value["dimensions"]
    assert "region" not in trend["시계열"][0]["dimensions"]


@pytest.mark.parametrize("unit", ["개%", "명 %", "백만원 %p", "USD/%"])
def test_mixed_units_are_preserved_but_not_complete_exact_evidence(unit):
    result = attach_stat_evidence({"status": "executed", "value": 123, "unit": unit,
                                  "period": "2024", "org_id": "101", "tbl_id": "DT_TEST"}, tool="query_table")
    contract = result["stat_evidence"]
    assert contract["execution"]["status"] == "executed"
    assert contract["fulfillment"]["status"] == "partial"
    assert contract["fulfillment"]["missing_fields"] == ["unit"]
    assert contract["evidence"]["observations"][0]["unit"] == unit


@pytest.mark.parametrize("unit", ["%", "%p", "백만원", "천명", "개", "kWh"])
def test_single_unit_family_is_still_exact(unit):
    contract = build_stat_evidence({"status": "executed", "value": 123, "unit": unit,
                                   "period": "2024", "org_id": "101", "tbl_id": "DT_TEST"}, tool="query_table")
    assert contract["fulfillment"]["status"] == "exact"


def test_single_value_response_has_complete_exact_evidence():
    payload = {
        "status": "executed",
        "값": "123",
        "단위": "개",
        "used_period": "2025",
        "출처": "통계청 KOSIS",
        "org_id": "101",
        "tbl_id": "DT_TEST",
        "통계표": "시도별 기업수",
        "actual_measure": "기업수",
        "measure_basis": "기업체 단위",
    }

    envelope = build_stat_evidence(payload, tool="quick_stat")

    assert envelope["contract_version"] == "stat-evidence/v1"
    assert envelope["execution"] == {
        "status": "executed",
        "code": None,
        "failure_class": None,
        "retryable": False,
    }
    assert envelope["fulfillment"]["status"] == "exact"
    assert envelope["fulfillment"]["completeness"] == "complete"
    assert envelope["fulfillment"]["missing_fields"] == []
    assert envelope["evidence"]["observations"] == [
        {"value": "123", "unit": "개", "period": "2025", "dimensions": {}}
    ]
    assert envelope["evidence"]["actual_measure"] == "기업수"
    assert envelope["evidence"]["measure_basis"] == "기업체 단위"
    assert envelope["evidence"]["period"] == {
        "requested": None,
        "used": "2025",
        "available": None,
        "selection_mode": None,
        "cadence": None,
    }
    assert envelope["evidence"]["table"] == {
        "org_id": "101",
        "table_id": "DT_TEST",
        "name": "시도별 기업수",
    }


def test_explicit_dimensionless_unit_is_complete():
    envelope = build_stat_evidence(
        {
            "status": "executed",
            "value": 98.2,
            "unit": "무단위",
            "used_period": "2025.01",
            "source": "KOSIS",
            "org_id": "101",
            "tbl_id": "DT_INDEX",
        },
        tool="answer_query",
    )

    assert envelope["fulfillment"]["completeness"] == "complete"
    assert "unit" not in envelope["fulfillment"]["missing_fields"]


def test_missing_unit_prevents_exact_fulfillment():
    envelope = build_stat_evidence(
        {
            "status": "executed",
            "value": 10,
            "used_period": "2025",
            "source": "KOSIS",
            "org_id": "101",
            "tbl_id": "DT_TEST",
        },
        tool="answer_query",
    )

    assert envelope["execution"]["status"] == "executed"
    assert envelope["fulfillment"]["status"] == "partial"
    assert envelope["fulfillment"]["completeness"] == "incomplete"
    assert envelope["fulfillment"]["missing_fields"] == ["unit"]


def test_parent_unit_and_period_apply_to_each_answer_row():
    envelope = build_stat_evidence(
        {
            "status": "executed",
            "단위": "개",
            "시점": "2025",
            "출처": "KOSIS",
            "org_id": "101",
            "tbl_id": "DT_COMPARE",
            "표": [{"지역": "서울", "값": 3}, {"지역": "부산", "값": 2}],
        },
        tool="answer_query",
    )

    assert envelope["fulfillment"]["status"] == "exact"
    assert envelope["evidence"]["observations"] == [
        {
            "value": 3,
            "unit": "개",
            "period": "2025",
            "dimensions": {"region": {"code": None, "label": "서울"}},
        },
        {
            "value": 2,
            "unit": "개",
            "period": "2025",
            "dimensions": {"region": {"code": None, "label": "부산"}},
        },
    ]
    assert envelope["fulfillment"]["resolved_concepts"] == [
        {"dimension": "region", "code": None, "label": "부산", "verified_in_rows": True},
        {"dimension": "region", "code": None, "label": "서울", "verified_in_rows": True},
    ]


def test_dropped_dimension_is_a_semantic_partial_not_execution_failure():
    envelope = build_stat_evidence(
        {
            "status": "partial",
            "값": 5.4,
            "단위": "%",
            "used_period": "2025",
            "출처": "통계청 KOSIS",
            "org_id": "101",
            "tbl_id": "DT_RATE",
            "dropped_dimensions": ["age"],
        },
        tool="answer_query",
        requested_concepts=[{"dimension": "age", "label": "청년"}],
    )

    assert envelope["execution"]["status"] == "executed"
    assert envelope["fulfillment"]["status"] == "partial"
    assert envelope["fulfillment"]["missing_concepts"] == ["age"]


def test_timeout_is_retryable_infrastructure_failure():
    envelope = build_stat_evidence(
        {"status": "failed", "code": "RUNTIME_ERROR", "error": "request timeout"},
        tool="answer_query",
    )

    assert envelope["execution"]["status"] == "failed"
    assert envelope["execution"]["failure_class"] == "infrastructure"
    assert envelope["execution"]["retryable"] is True
    assert envelope["fulfillment"]["status"] == "unavailable"


def test_no_rows_is_nonretryable_no_data():
    envelope = build_stat_evidence(
        {
            "status": "no_data",
            "rows": [],
            "org_id": "101",
            "tbl_id": "DT_EMPTY",
            "filters_used": {"ITEM": ["T001"]},
        },
        tool="query_table",
    )

    assert envelope["execution"]["status"] == "no_data"
    assert envelope["execution"]["failure_class"] == "no_rows"
    assert envelope["execution"]["retryable"] is False
    assert envelope["fulfillment"]["missing_concepts"] == ["ITEM:T001"]


def test_empty_fanout_timeout_is_failure_not_nonretryable_absence():
    envelope = build_stat_evidence({"status": "no_data", "rows": [],
        "fanout": {"failed_calls": 1, "call_details": [{"status": "failed", "error": "timeout"}]}}, tool="query_table")
    assert envelope["execution"] == {"status": "failed", "code": None, "failure_class": "infrastructure", "retryable": True}


def test_empty_fanout_unknown_failure_is_not_proved_no_rows():
    envelope = build_stat_evidence({"status": "no_data", "rows": [],
        "fanout": {"failed_calls": 1, "call_details": [{"status": "failed", "error": "provider response unrecognized"}]}}, tool="query_table")
    assert envelope["execution"]["status"] == "failed"
    assert envelope["execution"]["failure_class"] != "no_rows"


def test_failed_other_filter_does_not_erase_observed_zero():
    envelope = build_stat_evidence({"status": "executed", "org_id": "101", "tbl_id": "PARTIAL",
        "rows": [{"value": 0, "unit": "개", "period": "2024"}],
        "fanout": {"failed_calls": 1, "call_details": [{"status": "failed", "error": "timeout"}]}}, tool="query_table")
    assert envelope["execution"]["status"] == "executed"
    assert envelope["evidence"]["observations"][0]["value"] == 0


def test_query_rows_become_observations_and_verified_concept_bindings():
    payload = {
        "status": "executed",
        "org_id": "101",
        "tbl_id": "DT_ROWS",
        "table_name": "기업 활동",
        "filters_used": {"ITEM": ["T001"], "A": ["00"]},
        "rows": [
            {
                "value": 7,
                "unit": "개",
                "period": "2025",
                "dimensions": {
                    "ITEM": {"code": "T001", "label": "기업수"},
                    "A": {"code": "00", "label": "전국"},
                },
            }
        ],
    }

    envelope = build_stat_evidence(payload, tool="query_table")

    assert envelope["fulfillment"]["status"] == "exact"
    assert envelope["fulfillment"]["missing_concepts"] == []
    assert envelope["fulfillment"]["resolved_concepts"] == [
        {"dimension": "A", "code": "00", "label": "전국", "verified_in_rows": True},
        {"dimension": "ITEM", "code": "T001", "label": "기업수", "verified_in_rows": True},
    ]
    assert envelope["evidence"]["actual_measure"] == "기업수"
    assert envelope["evidence"]["actual_measure_evidence"] == "ITEM_dimension"


def test_attach_is_additive_and_does_not_mutate_input():
    payload = {"status": "failed", "error": "bad input", "custom": {"keep": True}}

    result = attach_stat_evidence(payload, tool="quick_stat")

    assert result is not payload
    assert "stat_evidence" not in payload
    assert result["custom"] == {"keep": True}
    assert result["stat_evidence"]["tool"] == "quick_stat"


def test_everyday_term_difference_can_remain_exact_when_evidence_is_complete():
    """A terminology warning alone must not turn a usable result into a refusal."""
    envelope = build_stat_evidence(
        {
            "status": "executed",
            "값": 8298915,
            "단위": "개",
            "used_period": "2023",
            "출처": "통계청 KOSIS",
            "org_id": "142",
            "tbl_id": "DT_COMPANY",
            "actual_measure": "중소기업 기업수",
            "⚠️ 모집단_불일치": "일상어 기업과 통계상 사업체는 작성 기준이 다를 수 있음",
        },
        tool="quick_stat",
    )

    assert envelope["execution"]["status"] == "executed"
    assert envelope["fulfillment"]["status"] == "exact"
    assert envelope["evidence"]["actual_measure"] == "중소기업 기업수"


def test_ignored_shortcut_parameter_is_structured_as_partial_fulfillment():
    envelope = build_stat_evidence(
        {
            "status": "executed",
            "값": 1,
            "단위": "개",
            "used_period": "2025",
            "출처": "KOSIS",
            "org_id": "101",
            "tbl_id": "DT_TEST",
            "mcp_output_contract": {
                "current_signals": {
                    "markers_present": ["shortcut_response", "ignored_params"],
                    "ignored_params": ["industry"],
                }
            },
        },
        tool="quick_stat",
    )

    assert envelope["execution"]["status"] == "executed"
    assert envelope["fulfillment"]["status"] == "partial"
    assert envelope["fulfillment"]["missing_concepts"] == ["parameter:industry"]


def test_quick_stat_public_tool_attaches_envelope(monkeypatch):
    import kosis_mcp_server as server

    async def fake_core(*_args, **_kwargs):
        return {
            "값": 1,
            "단위": "개",
            "used_period": "2025",
            "출처": "통계청 KOSIS",
            "org_id": "101",
            "tbl_id": "DT_TEST",
        }

    monkeypatch.setattr(server, "_quick_stat_core", fake_core)
    result = asyncio.run(server.quick_stat("테스트", api_key="dummy"))

    assert result["값"] == 1
    assert result["stat_evidence"]["tool"] == "quick_stat"
    assert result["stat_evidence"]["fulfillment"]["status"] == "exact"


def test_answer_query_compact_response_keeps_envelope():
    import kosis_mcp_server as server

    result = server._finalize_answer_query_response(
        {
            "status": "executed",
            "value": 1,
            "unit": "개",
            "used_period": "2025",
            "source": "KOSIS",
            "org_id": "101",
            "tbl_id": "DT_TEST",
        },
        query="기업 수",
        region="전국",
        verbose=False,
    )

    assert result["value"] == 1
    assert result["stat_evidence"]["tool"] == "answer_query"


def test_answer_query_exposes_curated_actual_measure():
    import kosis_mcp_server as server

    result = server._finalize_answer_query_response(
        {
            "status": "executed",
            "value": 8298915,
            "unit": "개",
            "used_period": "2023",
            "source": "KOSIS",
            "org_id": "142",
            "tbl_id": server.TIER_A_STATS["중소기업_사업체수"].tbl_id,
            "route": {"direct_stat_key": "중소기업_사업체수"},
        },
        query="중소기업 사업체 수",
        region="전국",
        verbose=False,
    )

    evidence = result["stat_evidence"]["evidence"]
    assert evidence["actual_measure"] == "중소기업 기업수"
    assert evidence["actual_measure_evidence"] == "verified_curation_description"
    assert evidence["measure_basis"] == "기업 단위(업종별 매출액·자산 기준으로 중소기업 규모 분류)"


def test_curation_identity_cannot_be_attached_to_an_unrelated_executed_table():
    import kosis_mcp_server as server
    result = server._finalize_answer_query_response({"status": "executed", "value": 123, "unit": "개",
                                                    "period": "2024", "org_id": "101", "tbl_id": "WRONG",
                                                    "route": {"direct_stat_key": "중소기업_사업체수"}},
                                                   query="중소기업 기업 수", region="전국", verbose=False)
    assert result["stat_evidence"]["evidence"]["actual_measure_evidence"] != "verified_curation_description"


def test_query_table_public_tool_wraps_early_failures(monkeypatch):
    import kosis_mcp_server as server

    async def fake_core(**_kwargs):
        return {"status": "unsupported", "code": "INVALID_FILTER_CODE", "error": "bad filter"}

    monkeypatch.setattr(server, "_query_table_core", fake_core)
    result = asyncio.run(server.query_table("101", "DT_TEST", {"ITEM": ["bad"]}))

    assert result["code"] == "INVALID_FILTER_CODE"
    assert result["stat_evidence"]["execution"]["status"] == "failed"
    assert result["stat_evidence"]["execution"]["failure_class"] == "invalid_request"
