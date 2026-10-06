"""StatEvidenceEnvelope v1 contract tests.

The envelope is additive: existing tool response fields remain available while
clients get one stable place to inspect execution, fulfillment, and evidence.
"""

import asyncio

from kosis_analysis.evidence import attach_stat_evidence, build_stat_evidence


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
            "tbl_id": "DT_COMPANY",
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


def test_query_table_public_tool_wraps_early_failures(monkeypatch):
    import kosis_mcp_server as server

    async def fake_core(**_kwargs):
        return {"status": "unsupported", "code": "INVALID_FILTER_CODE", "error": "bad filter"}

    monkeypatch.setattr(server, "_query_table_core", fake_core)
    result = asyncio.run(server.query_table("101", "DT_TEST", {"ITEM": ["bad"]}))

    assert result["code"] == "INVALID_FILTER_CODE"
    assert result["stat_evidence"]["execution"]["status"] == "failed"
    assert result["stat_evidence"]["execution"]["failure_class"] == "invalid_request"
