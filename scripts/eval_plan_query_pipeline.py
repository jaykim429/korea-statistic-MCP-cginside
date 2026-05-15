from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import _attach_gemma_deprecation_warning, plan_query


CASES: list[dict[str, Any]] = [
    {
        "name": "grdp_single_query_no_rd_pollution",
        "query": "서울 1인당 GRDP",
        "metrics": ["GRDP"],
        "must_not_metrics": ["R&D 투자 규모"],
        "quarantined_metrics": ["R&D 투자 규모"],
    },
    {
        "name": "composite_query_preserves_all_metrics",
        "query": "최근 5년간 소상공인 사업체 수, 종사자 수, 매출액, 폐업률을 한 표로 정리해줘.",
        "metrics": ["사업체 수", "종사자 수", "매출액", "폐업률"],
        "evidence_bundle": True,
    },
    {
        "name": "top_and_bottom_separate_tasks",
        "query": "2024년 GRDP가 가장 높은 시도 3개와 가장 낮은 시도 3개를 알려줘.",
        "rank_orders": ["desc", "asc"],
        "rank_limit": 3,
        "must_not_metrics": ["R&D 투자 규모"],
        "quarantined_metrics": ["R&D 투자 규모"],
    },
    {
        "name": "year_range_parsing",
        "query": "2015년부터 2023년까지 출생아 수 추이",
        "time_type": "year_range",
        "time_start": "2015",
        "time_end": "2023",
    },
    {
        "name": "relative_year_last_year",
        "query": "작년 출생아 수",
        "time_type": "relative_year",
        "time_offset": -1,
    },
    {
        "name": "simple_lookup_not_composite",
        "query": "서울 인구",
        "analysis_mode": "simple_lookup",
        "evidence_bundle": False,
        "analysis_task_count": 0,
        "contract_role": "planning_only",
        "final_answer_expected": False,
        "handoff_final_answer_expected": False,
        "canonical_workflow": "evidence_workflow",
        "compact_omits_control_fields": True,
    },
    {
        "name": "regions_vs_region_separation",
        "query": "서울이랑 부산 인구 비교",
        "semantic_regions": ["서울", "부산"],
        "table_required_dimensions": ["region"],
        "must_not_table_required_dimensions": ["regions"],
    },
    {
        "name": "time_normalization_not_conflict",
        "query": "2024년 GRDP",
        "time_type": "year",
        "no_time_conflict": True,
    },
    {
        "name": "trend_time_group_not_table_axis",
        "query": "최근 5년간 실업률 추이",
        "metrics": ["실업률"],
        "analysis_mode": "analytical_single_metric",
        "time_type": "relative_period",
        "table_required_dimensions": ["time"],
        "must_not_table_required_dimensions": ["year"],
        "task_types": ["trend"],
    },
    {
        "name": "ppi_alias_maps_to_producer_price_index",
        "query": "PPI latest",
        "metrics": ["생산자물가지수"],
        "must_not_status": "needs_clarification",
    },
    {
        "name": "business_count_intent_from_daily_phrase",
        "query": "치킨집 얼마나 있어?",
        "metrics": ["사업체 수"],
        "table_required_dimensions": ["industry"],
        "must_not_status": "needs_clarification",
        "evidence_workflow_nonempty": True,
    },
    {
        "name": "clarification_has_failure_signal",
        "query": "한국 좀 어때",
        "status_expected": "needs_clarification",
        "current_signal_has_failures": True,
        "current_signal_has_caveats": True,
        "current_signal_markers": ["needs_clarification", "missing_metrics"],
    },
    {
        "name": "ambiguous_no_indicator_has_caveat",
        "query": "통계 보여줘",
        "status_expected": "needs_clarification",
        "current_signal_has_failures": True,
        "current_signal_has_caveats": True,
        "current_signal_markers": ["needs_clarification", "missing_metrics"],
    },
    {
        "name": "crypto_missing_metric_has_caveat",
        "query": "대한민국 비트코인 채굴량",
        "status_expected": "needs_clarification",
        "current_signal_has_failures": True,
        "current_signal_has_caveats": True,
        "current_signal_markers": ["needs_clarification", "missing_metrics"],
    },
    {
        "name": "clean_simple_lookup_single_caveat",
        "query": "2023년 합계출산율",
        "metrics_length": 1,
        "quarantined_metrics_length": 0,
        "current_signal_exact_markers": ["metric_availability_unverified"],
        "analysis_mode": "simple_lookup",
        "evidence_bundle": False,
    },
    {
        "name": "monthly_trend_month_not_dimension",
        "query": "월별 소비자물가지수 추이",
        "metrics": ["소비자물가지수"],
        "table_required_dimensions": ["time"],
        "must_not_table_required_dimensions": ["month"],
        "task_types": ["trend"],
    },
    {
        "name": "quarterly_trend_quarter_not_dimension",
        "query": "분기별 GDP 추이",
        "metrics": ["GDP"],
        "table_required_dimensions": ["time"],
        "must_not_table_required_dimensions": ["quarter"],
        "task_types": ["trend"],
    },
    {
        "name": "industry_and_year_split",
        "query": "산업별 연도별 사업체수 추이",
        "metrics": ["사업체 수"],
        "table_required_dimensions": ["industry", "time"],
        "must_not_table_required_dimensions": ["year"],
        "task_types": ["trend"],
    },
    {
        "name": "single_metric_ranking_is_analytical",
        "query": "2024년 시도별 인구 순위",
        "metrics_length": 1,
        "task_types": ["rank"],
        "analysis_mode": "analytical_single_metric",
        "evidence_bundle": False,
    },
    {
        "name": "single_metric_calculation_is_analytical",
        "query": "서울 1인당 GRDP",
        "metrics_length": 1,
        "calculations": ["per_capita"],
        "task_types": ["per_capita"],
        "analysis_mode": "analytical_single_metric",
        "evidence_bundle": False,
    },
    {
        "name": "mode_bundle_consistency_simple",
        "query": "한국 인구",
        "analysis_mode": "simple_lookup",
        "evidence_bundle_matches_mode": True,
    },
    {
        "name": "mode_bundle_consistency_analytical",
        "query": "최근 5년간 실업률 추이",
        "analysis_mode": "analytical_single_metric",
        "evidence_bundle_matches_mode": True,
    },
    {
        "name": "mode_bundle_consistency_composite",
        "query": "소상공인 사업체수와 종사자수 비교",
        "analysis_mode": "composite_analysis",
        "evidence_bundle_matches_mode": True,
        "metrics_length_gte": 2,
    },
    {
        "name": "workflow_sync_planned_nonempty",
        "query": "치킨집 얼마나 있어?",
        "must_not_status": "needs_clarification",
        "evidence_workflow_nonempty": True,
        "suggested_workflow_not_richer": True,
    },
    {
        "name": "canonical_workflow_nonempty_when_planned",
        "query": "서울 1인당 GRDP",
        "canonical_workflow": "evidence_workflow",
        "evidence_workflow_nonempty": True,
    },
    {
        "name": "birth_rate_and_count_visible",
        "query": "출생율이 가장 낮은 시도 Top 5와 출생아 수 Top 5가 같은지 비교해줘.",
        "metrics": ["조출생률", "출생아수"],
        "table_required_dimensions": ["region"],
        "task_types": ["rank", "rank_compare", "rank_overlap"],
    },
    {
        "name": "birth_rate_direct_key_wins_over_generic_birth",
        "query": "출생률이 가장 낮은 시도 Top 5를 알려줘.",
        "metrics": ["조출생률"],
        "must_not_metrics": ["\"출생\""],
        "table_required_dimensions": ["region"],
        "task_types": ["rank"],
    },
    {
        "name": "birth_count_direct_key_wins_over_generic_birth",
        "query": "작년 출생아 수",
        "metrics": ["출생아수"],
        "must_not_metrics": ["\"출생\""],
        "time_type": "relative_year",
        "time_offset": -1,
        "analysis_mode": "simple_lookup",
    },
    {
        "name": "time_expression_tilde_range",
        "query": "2015~2024 인구 변화",
        "time_type": "year_range",
        "time_start": "2015",
        "time_end": "2024",
    },
    {
        "name": "regression_grdp_trend_combo",
        "query": "최근 5년간 서울 GRDP 추이",
        "metrics": ["GRDP"],
        "must_not_metrics": ["R&D 투자 규모"],
        "quarantined_metrics": ["R&D 투자 규모"],
        "time_type": "relative_period",
        "table_required_dimensions": ["region", "time"],
        "must_not_table_required_dimensions": ["year"],
        "task_types": ["trend"],
    },
    {
        "name": "regression_comparison_range_combo",
        "query": "2015년부터 2024년까지 서울과 부산의 인구 변화",
        "metrics_length": 1,
        "semantic_regions": ["서울", "부산"],
        "table_required_dimensions": ["region", "time"],
        "must_not_table_required_dimensions": ["regions", "year"],
        "time_type": "year_range",
        "time_start": "2015",
        "time_end": "2024",
    },
    {
        "name": "regression_top_bottom_calculation",
        "query": "2024년 1인당 GRDP가 가장 높은 시도 3개와 가장 낮은 시도 3개",
        "rank_orders": ["desc", "asc"],
        "rank_limit": 3,
        "calculations": ["per_capita"],
        "must_not_metrics": ["R&D 투자 규모"],
        "quarantined_metrics": ["R&D 투자 규모"],
    },
    {
        "name": "preserve_unmapped_industry_phrase",
        "query": "반도체 매출액",
        "metrics": ["매출액"],
        "semantic_industry": "반도체",
        "concepts": ["반도체"],
        "table_required_dimensions": ["industry"],
    },
    {
        "name": "comparison_intent_creates_task",
        "query": "서울과 부산 인구 비교",
        "metrics": ["인구"],
        "semantic_regions": ["서울", "부산"],
        "table_required_dimensions": ["region"],
        "must_not_table_required_dimensions": ["regions"],
        "task_types": ["compare_dimensions"],
        "analysis_mode": "analytical_single_metric",
        "evidence_bundle": False,
        "evidence_workflow_required_dimensions": ["region"],
    },
    {
        "name": "yoy_growth_uses_previous_year_period",
        "query": "2023년 전년 대비 소상공인 수 증가율",
        "metrics": ["소상공인 수"],
        "time_type": "point_compare",
        "time_periods": ["2022", "2023"],
        "task_types": ["growth_rate"],
        "analysis_mode": "analytical_single_metric",
        "evidence_bundle": False,
        "analysis_task_count": 1,
    },
]


def _blob(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _missing(blob: str, expected: list[str]) -> list[str]:
    return [item for item in expected if item not in blob]


async def main() -> None:
    rows: list[dict[str, Any]] = []
    for case in CASES:
        result = await plan_query(case["query"])
        problems: list[Any] = []
        metrics_blob = _blob(result.get("metrics") or [])
        quarantined_blob = _blob(result.get("quarantined_metrics") or [])

        if "status_expected" in case and result.get("status") != case["status_expected"]:
            problems.append({"status": result.get("status"), "expected": case["status_expected"]})
        if "must_not_status" in case and result.get("status") == case["must_not_status"]:
            problems.append({"forbidden_status": result.get("status")})

        missing_metrics = _missing(metrics_blob, case.get("metrics", []))
        if missing_metrics:
            problems.append({"missing_metrics": missing_metrics, "metrics": result.get("metrics")})
        forbidden_metrics = [item for item in case.get("must_not_metrics", []) if item in metrics_blob]
        if forbidden_metrics:
            problems.append({"forbidden_metrics": forbidden_metrics, "metrics": result.get("metrics")})
        missing_quarantined = _missing(quarantined_blob, case.get("quarantined_metrics", []))
        if missing_quarantined:
            problems.append({
                "missing_quarantined_metrics": missing_quarantined,
                "quarantined_metrics": result.get("quarantined_metrics"),
            })
        if "metrics_length" in case:
            metrics_count = len(result.get("metrics") or [])
            if metrics_count != case["metrics_length"]:
                problems.append({"metrics_length": metrics_count, "expected": case["metrics_length"]})
        if "metrics_length_gte" in case:
            metrics_count = len(result.get("metrics") or [])
            if metrics_count < case["metrics_length_gte"]:
                problems.append({"metrics_length": metrics_count, "expected_gte": case["metrics_length_gte"]})
        if "quarantined_metrics_length" in case:
            quarantined_count = len(result.get("quarantined_metrics") or [])
            if quarantined_count != case["quarantined_metrics_length"]:
                problems.append({
                    "quarantined_metrics_length": quarantined_count,
                    "expected": case["quarantined_metrics_length"],
                })

        if "evidence_bundle" in case and result.get("evidence_bundle") is not case["evidence_bundle"]:
            problems.append({"evidence_bundle": result.get("evidence_bundle"), "expected": case["evidence_bundle"]})
        if "analysis_mode" in case and result.get("analysis_mode") != case["analysis_mode"]:
            problems.append({"analysis_mode": result.get("analysis_mode"), "expected": case["analysis_mode"]})
        if "analysis_task_count" in case:
            count = len(result.get("analysis_tasks") or [])
            if count != case["analysis_task_count"]:
                problems.append({"analysis_task_count": count, "expected": case["analysis_task_count"]})
        if "task_types" in case:
            task_types = [task.get("type") for task in result.get("analysis_tasks") or []]
            missing_tasks = [task for task in case["task_types"] if task not in task_types]
            if missing_tasks:
                problems.append({"missing_task_types": missing_tasks, "actual": task_types})
        if "calculations" in case:
            calculations = result.get("calculations") or []
            missing_calculations = [item for item in case["calculations"] if item not in calculations]
            if missing_calculations:
                problems.append({"missing_calculations": missing_calculations, "actual": calculations})

        contract = result.get("mcp_output_contract") or {}
        if "contract_role" in case and contract.get("role") != case["contract_role"]:
            problems.append({"contract_role": contract.get("role"), "expected": case["contract_role"]})
        if (
            "final_answer_expected" in case
            and contract.get("final_answer_expected") is not case["final_answer_expected"]
        ):
            problems.append({
                "final_answer_expected": contract.get("final_answer_expected"),
                "expected": case["final_answer_expected"],
            })
        if "handoff_final_answer_expected" in case:
            handoff = result.get("handoff_to_llm") or {}
            if handoff.get("final_answer_expected") is not case["handoff_final_answer_expected"]:
                problems.append({
                    "handoff_final_answer_expected": handoff.get("final_answer_expected"),
                    "expected": case["handoff_final_answer_expected"],
                })
        if "canonical_workflow" in case:
            canonical = result.get("canonical_fields") or {}
            if canonical.get("workflow") != case["canonical_workflow"]:
                problems.append({"canonical_workflow": canonical.get("workflow"), "expected": case["canonical_workflow"]})
        if case.get("compact_omits_control_fields"):
            for field in ("deprecated_fields", "recommended_tool_manifest", "route", "llm_guardrails", "llm_rules"):
                if field in result:
                    problems.append({"compact_field_should_be_omitted": field})
            profile = result.get("response_profile") or {}
            if profile.get("compact") is not True:
                problems.append({"response_profile": profile})
        deprecated = result.get("deprecated_fields") or {}
        missing_deprecated = [field for field in case.get("deprecated_fields", []) if field not in deprecated]
        if missing_deprecated:
            problems.append({"missing_deprecated_fields": missing_deprecated, "actual": deprecated})
        if contract:
            current_signals = contract.get("current_signals") or {}
            if "has_caveats" not in current_signals or "markers_present" not in current_signals:
                problems.append({"current_signals": current_signals})
            if (
                "current_signal_has_failures" in case
                and current_signals.get("has_failures") is not case["current_signal_has_failures"]
            ):
                problems.append({
                    "current_signal_has_failures": current_signals.get("has_failures"),
                    "expected": case["current_signal_has_failures"],
                    "current_signals": current_signals,
                })
            if (
                "current_signal_has_caveats" in case
                and current_signals.get("has_caveats") is not case["current_signal_has_caveats"]
            ):
                problems.append({
                    "current_signal_has_caveats": current_signals.get("has_caveats"),
                    "expected": case["current_signal_has_caveats"],
                    "current_signals": current_signals,
                })
            missing_markers = [
                marker for marker in case.get("current_signal_markers", [])
                if marker not in (current_signals.get("markers_present") or [])
            ]
            if missing_markers:
                problems.append({"missing_current_signal_markers": missing_markers, "current_signals": current_signals})
            if "current_signal_exact_markers" in case:
                markers = current_signals.get("markers_present") or []
                if markers != case["current_signal_exact_markers"]:
                    problems.append({
                        "current_signal_exact_markers": markers,
                        "expected": case["current_signal_exact_markers"],
                    })
        manifest = result.get("recommended_tool_manifest") or {}
        expose = manifest.get("expose") or []
        hide = manifest.get("hide_by_default") or []
        if manifest and (manifest.get("version") != "1.0" or manifest.get("compatible_with") != ">=0.6.0"):
            problems.append({"manifest_version": manifest})
        missing_exposed = [tool for tool in case.get("manifest_exposes", []) if tool not in expose]
        if missing_exposed:
            problems.append({"missing_manifest_expose": missing_exposed, "actual": expose})
        missing_hidden = [tool for tool in case.get("manifest_hides", []) if tool not in hide]
        if missing_hidden:
            problems.append({"missing_manifest_hide": missing_hidden, "actual": hide})

        rank_tasks = [task for task in result.get("analysis_tasks") or [] if task.get("type") == "rank"]
        if "rank_orders" in case:
            orders = sorted(task.get("order") for task in rank_tasks)
            expected_orders = sorted(case["rank_orders"])
            if orders != expected_orders:
                problems.append({"rank_orders": orders, "expected": expected_orders})
        if "rank_limit" in case:
            limits = [task.get("limit") for task in rank_tasks]
            if not limits or any(limit != case["rank_limit"] for limit in limits):
                problems.append({"rank_limits": limits, "expected": case["rank_limit"]})

        time_request = result.get("time_request") or {}
        if "time_type" in case and time_request.get("type") != case["time_type"]:
            problems.append({"time_type": time_request.get("type"), "expected": case["time_type"]})
        if "time_start" in case and time_request.get("start") != case["time_start"]:
            problems.append({"time_start": time_request.get("start"), "expected": case["time_start"]})
        if "time_end" in case and time_request.get("end") != case["time_end"]:
            problems.append({"time_end": time_request.get("end"), "expected": case["time_end"]})
        if "time_offset" in case and time_request.get("offset") != case["time_offset"]:
            problems.append({"time_offset": time_request.get("offset"), "expected": case["time_offset"]})
        if "time_periods" in case and time_request.get("periods") != case["time_periods"]:
            problems.append({"time_periods": time_request.get("periods"), "expected": case["time_periods"]})
        if case.get("no_time_conflict"):
            time_conflicts = [
                item for item in result.get("conflict_decisions") or []
                if isinstance(item, dict) and "time" in str(item.get("type"))
            ]
            if time_conflicts:
                problems.append({"time_conflicts": time_conflicts})

        semantic = result.get("semantic_dimensions") or {}
        if "semantic_regions" in case:
            regions = semantic.get("regions")
            if regions != case["semantic_regions"]:
                problems.append({"semantic_regions": regions, "expected": case["semantic_regions"]})
        if "semantic_industry" in case and semantic.get("industry") != case["semantic_industry"]:
            problems.append({"semantic_industry": semantic.get("industry"), "expected": case["semantic_industry"]})
        missing_concepts = [concept for concept in case.get("concepts", []) if concept not in (result.get("concepts") or [])]
        if missing_concepts:
            problems.append({"missing_concepts": missing_concepts, "actual": result.get("concepts")})
        table_required = result.get("table_required_dimensions") or result.get("required_dimensions") or []
        missing_dims = [dim for dim in case.get("table_required_dimensions", []) if dim not in table_required]
        if missing_dims:
            problems.append({"missing_table_required_dimensions": missing_dims, "actual": table_required})
        forbidden_dims = [dim for dim in case.get("must_not_table_required_dimensions", []) if dim in table_required]
        if forbidden_dims:
            problems.append({"forbidden_table_required_dimensions": forbidden_dims, "actual": table_required})
        if case.get("evidence_workflow_nonempty") and not result.get("evidence_workflow"):
            problems.append({"evidence_workflow": result.get("evidence_workflow")})
        if "evidence_workflow_required_dimensions" in case:
            workflow = result.get("evidence_workflow") or []
            actual_dims = []
            if workflow:
                actual_dims = ((workflow[0].get("args_template") or {}).get("required_dimensions") or [])
            if actual_dims != case["evidence_workflow_required_dimensions"]:
                problems.append({
                    "evidence_workflow_required_dimensions": actual_dims,
                    "expected": case["evidence_workflow_required_dimensions"],
                })
        if case.get("suggested_workflow_not_richer") and result.get("status") == "planned":
            suggested_count = len(result.get("suggested_workflow") or [])
            evidence_count = len(result.get("evidence_workflow") or [])
            if suggested_count > evidence_count:
                problems.append({
                    "suggested_workflow_count": suggested_count,
                    "evidence_workflow_count": evidence_count,
                })
        if case.get("evidence_bundle_matches_mode"):
            expected_bundle = result.get("analysis_mode") == "composite_analysis"
            if result.get("evidence_bundle") is not expected_bundle:
                problems.append({
                    "evidence_bundle": result.get("evidence_bundle"),
                    "expected_from_mode": expected_bundle,
                    "analysis_mode": result.get("analysis_mode"),
                })

        rows.append({
            "name": case["name"],
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "analysis_mode": result.get("analysis_mode"),
            "metrics": result.get("metrics"),
            "quarantined_metrics": result.get("quarantined_metrics"),
            "semantic_dimensions": result.get("semantic_dimensions"),
            "table_required_dimensions": table_required,
            "time_request": time_request,
            "analysis_tasks": result.get("analysis_tasks"),
            "conflict_decisions": result.get("conflict_decisions"),
        })

    warned = _attach_gemma_deprecation_warning({"status": "executed"})
    warning_problems: list[Any] = []
    if warned.get("deprecation_warning") or warned.get("llm_guardrails"):
        warning_problems.append({
            "removed_control_fields": {
                "deprecation_warning": warned.get("deprecation_warning"),
                "llm_guardrails": warned.get("llm_guardrails"),
            }
        })
    if warned.get("tool_mode") != "convenience":
        warning_problems.append({"tool_mode": warned.get("tool_mode")})
    contract = warned.get("mcp_output_contract") or {}
    if contract.get("role") != "convenience_tool":
        warning_problems.append({"contract_role": contract.get("role")})
    rows.append({
        "name": "answer_query_is_convenience_tool",
        "status": "PASS" if not warning_problems else "FAIL",
        "problems": warning_problems,
    })

    print(json.dumps(rows, ensure_ascii=False, indent=2))
    failed = [row for row in rows if row["status"] != "PASS"]
    print(f"\nSUMMARY {len(rows) - len(failed)}/{len(rows)} PASS")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
