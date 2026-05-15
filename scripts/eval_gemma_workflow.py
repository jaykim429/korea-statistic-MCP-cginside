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

from kosis_mcp_server import plan_query


CASES: list[dict[str, Any]] = [
    {
        "name": "multidim_population",
        "query": "2020년 서울 30대 여성 인구",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region", "age", "sex", "time"],
        "concepts": ["인구", "서울", "30대", "여성", "2020"],
        "indicator_alternatives_min": 3,
        "future_expected_table": "DT_1BPB001",
        "future_must_not_select_tables": ["DT_1B040A3"],
    },
    {
        "name": "per_capita_grdp",
        "query": "서울 1인당 GRDP 알려줘",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region"],
        "concepts": ["GRDP", "서울", "per_capita"],
        "consistency_warning_types": ["indicator_conflict"],
        "router_slots_overridden": ["indicator"],
    },
    {
        "name": "metro_aging_speed",
        "query": "광역시 중 고령화 비중이 가장 빠른 곳",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region_group", "age", "time"],
        "concepts": ["고령인구비중", "광역시", "65세 이상", "share", "growth_rate"],
        "compute_operations": ["share", "growth_rate"],
        "consistency_warnings_len": 0,
    },
    {
        "name": "metro_aging_rate_rank",
        "query": "광역시 중 고령화율이 가장 높은 곳",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region_group", "age"],
        "concepts": ["고령인구비중", "광역시", "65세 이상", "share"],
        "compute_operations": ["share"],
    },
    {
        "name": "simple_birth",
        "query": "출생아 수 알려줘",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "concepts": ["출생아수"],
        "consistency_warnings_len": 0,
    },
    {
        "name": "region_pair_comparison",
        "query": "서울과 부산 인구 비교",
        "expected_intent": "comparison",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "required_dimensions": ["region"],
        "concepts": ["인구", "서울", "부산"],
        "dimension_checks": {"regions": ["서울", "부산"]},
    },
    {
        "name": "explicit_year_range_trend",
        "query": "2015년부터 2023년까지 출생아 수 추이",
        "expected_intent": "trend",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "required_dimensions": ["time"],
        "concepts": ["출생아수", "2015", "2023"],
        "dimension_checks": {"time.type": "year_range", "time.start": "2015", "time.end": "2023"},
        "period_range": ["2015", "2023"],
    },
    {
        "name": "relative_year_last_year",
        "query": "작년 서울 실업률",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "required_dimensions": ["region", "time"],
        "concepts": ["실업률", "서울", "작년"],
        "dimension_checks": {"time.type": "relative_year", "time.offset": -1},
    },
    {
        "name": "marriage_colloquial",
        "query": "요즘 결혼 안 해?",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "concepts": ["혼인건수"],
    },
    {
        "name": "english_unemployment",
        "query": "unemployment rate in Seoul",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "required_dimensions": ["region"],
        "concepts": ["실업률", "서울"],
    },
    {
        "name": "cpi_abbreviation",
        "query": "CPI 최신값",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "concepts": ["소비자물가지수"],
    },
    {
        "name": "grdp_abbreviation_negative",
        "query": "GRDP 최신값",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table"],
        "concepts": ["GRDP"],
        "consistency_warning_types": ["indicator_conflict"],
        "router_slots_overridden": ["indicator"],
        "future_must_not_match_concepts": ["R&D 투자 규모"],
    },
    {
        "name": "metro_grdp_per_capita_conflict",
        "query": "광역시 1인당 GRDP",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region_group"],
        "concepts": ["GRDP", "광역시", "per_capita"],
        "consistency_warning_types": ["indicator_conflict"],
        "router_slots_overridden": ["indicator"],
    },
    {
        "name": "chicken_business_closure",
        "query": "치킨집 폐업률 알려줘",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "concepts": ["음식점업", "폐업"],
        "compute_operations": ["share"],
    },
    {
        "name": "ambiguous_low_confidence",
        "query": "한국 좀 어때",
        "expected_status": "needs_clarification",
        "expected_workflow": [],
        "concepts": [],
    },
]


def _contains_all(actual: list[Any], expected: list[Any]) -> list[Any]:
    actual_set = {str(item) for item in actual}
    return [item for item in expected if str(item) not in actual_set]


async def main() -> None:
    rows = []
    for case in CASES:
        result = await plan_query(case["query"])
        workflow = [step.get("tool") for step in result.get("suggested_workflow", [])]
        period_range = None
        compute_operations = []
        for step in result.get("suggested_workflow", []):
            if not isinstance(step, dict):
                continue
            if step.get("tool") == "query_table":
                period_range = (step.get("args") or {}).get("period_range")
            if step.get("tool") == "compute_indicator":
                args = step.get("args") or {}
                compute_operations = args.get("operations") or [args.get("operation")]
        problems = []
        if "expected_status" in case and result.get("status") != case["expected_status"]:
            problems.append({"status": result.get("status"), "expected": case["expected_status"]})
        if "expected_intent" in case and result.get("intent") != case["expected_intent"]:
            problems.append({"intent": result.get("intent"), "expected": case["expected_intent"]})
        missing_workflow = _contains_all(workflow, case.get("expected_workflow", []))
        if missing_workflow:
            problems.append({"missing_workflow": missing_workflow, "actual": workflow})
        missing_dimensions = _contains_all(result.get("required_dimensions", []), case.get("required_dimensions", []))
        if missing_dimensions:
            problems.append({"missing_dimensions": missing_dimensions, "actual": result.get("required_dimensions", [])})
        missing_concepts = _contains_all(result.get("concepts", []), case.get("concepts", []))
        if missing_concepts:
            problems.append({"missing_concepts": missing_concepts, "actual": result.get("concepts", [])})
        missing_compute_ops = _contains_all(compute_operations, case.get("compute_operations", []))
        if missing_compute_ops:
            problems.append({"missing_compute_operations": missing_compute_ops, "actual": compute_operations})
        dimensions = result.get("intended_dimensions") or {}
        for path, expected in case.get("dimension_checks", {}).items():
            current: Any = dimensions
            for part in path.split("."):
                current = current.get(part) if isinstance(current, dict) else None
            if isinstance(expected, list):
                missing = _contains_all(current if isinstance(current, list) else [], expected)
                if missing:
                    problems.append({"dimension": path, "missing": missing, "actual": current})
            elif current != expected:
                problems.append({"dimension": path, "actual": current, "expected": expected})
        if "period_range" in case and period_range != case["period_range"]:
            problems.append({"period_range": period_range, "expected": case["period_range"]})
        if "indicator_alternatives_min" in case:
            alternatives = dimensions.get("indicator_alternatives")
            if not isinstance(alternatives, list) or len(alternatives) < case["indicator_alternatives_min"]:
                problems.append({"indicator_alternatives": alternatives})
        warnings = result.get("consistency_warnings", [])
        if "consistency_warnings_len" in case and len(warnings) != case["consistency_warnings_len"]:
            problems.append({"consistency_warnings_len": len(warnings), "warnings": warnings})
        warning_types = [w.get("type") for w in warnings if isinstance(w, dict)]
        missing_warning_types = _contains_all(warning_types, case.get("consistency_warning_types", []))
        if missing_warning_types:
            problems.append({"missing_consistency_warning_types": missing_warning_types, "actual": warning_types})
        overridden = result.get("router_slots_overridden") or {}
        missing_overrides = [slot for slot in case.get("router_slots_overridden", []) if slot not in overridden]
        if missing_overrides:
            problems.append({"missing_router_slot_overrides": missing_overrides, "actual": overridden})
        rows.append({
            "name": case["name"],
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "intent": result.get("intent"),
            "workflow": workflow,
            "period_range": period_range,
            "compute_operations": compute_operations,
            "consistency_warnings": warnings,
            "router_slots_overridden": result.get("router_slots_overridden") or {},
            "required_dimensions": result.get("required_dimensions"),
            "intended_dimensions": result.get("intended_dimensions"),
            "concepts": result.get("concepts"),
            "future_expectations": {
                key: value
                for key, value in case.items()
                if key.startswith("future_")
            },
        })
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
