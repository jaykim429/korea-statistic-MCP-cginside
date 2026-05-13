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
        "future_expected_table": "DT_1BPB001",
        "future_must_not_select_tables": ["DT_1B040A3"],
    },
    {
        "name": "per_capita_grdp",
        "query": "서울 1인당 GRDP 알려줘",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region"],
        "concepts": ["GRDP", "서울", "per_capita"],
    },
    {
        "name": "metro_aging_speed",
        "query": "광역시 중 고령화 비중이 가장 빠른 곳",
        "expected_workflow": ["select_table_for_query", "resolve_concepts", "query_table", "compute_indicator"],
        "required_dimensions": ["region_group", "age", "time"],
        "concepts": ["고령인구비중", "광역시", "65세 이상", "share", "growth_rate"],
        "compute_operations": ["share", "growth_rate"],
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
        "concepts": ["출생"],
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
        "future_must_not_match_concepts": ["R&D 투자 규모"],
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
        compute_operations = []
        for step in result.get("suggested_workflow", []):
            if isinstance(step, dict) and step.get("tool") == "compute_indicator":
                args = step.get("args") or {}
                compute_operations = args.get("operations") or [args.get("operation")]
        problems = []
        if "expected_status" in case and result.get("status") != case["expected_status"]:
            problems.append({"status": result.get("status"), "expected": case["expected_status"]})
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
        rows.append({
            "name": case["name"],
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "intent": result.get("intent"),
            "workflow": workflow,
            "compute_operations": compute_operations,
            "required_dimensions": result.get("required_dimensions"),
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
