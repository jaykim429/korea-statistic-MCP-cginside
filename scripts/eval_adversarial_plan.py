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
        "name": "rate_vs_count_birth",
        "query": "출생율이 가장 낮은 시도 Top 5와 출생아 수 Top 5가 같은지 비교해줘.",
        "metrics": ["출생율", "출생아 수"],
        "dimensions": ["region"],
        "tasks": ["rank", "rank_compare", "rank_overlap"],
    },
    {
        "name": "business_cycle_not_gyeonggi_region",
        "query": "소상공인 경기전망지수와 실제 매출 증가율이 업종별로 같은 방향인지 최근 12개월 기준으로 비교해줘.",
        "metrics": ["경기전망지수", "매출액"],
        "dimensions": ["industry", "time", "scale"],
        "tasks": ["growth_rate", "compare_metrics"],
        "must_not_dimension_values": ["경기"],
    },
    {
        "name": "condition_filter_increase_decrease",
        "query": "중소기업 수는 늘었는데 평균 종사자 수는 줄어든 업종이 있는지 최근 5년 기준으로 찾아줘.",
        "metrics": ["중소기업 수", "종사자 수"],
        "dimensions": ["industry", "time", "scale"],
        "tasks": ["condition_filter", "growth_rate"],
    },
    {
        "name": "multi_group_gap_and_industry",
        "query": "수도권과 비수도권의 소상공인 매출 격차가 커진 업종 Top 10을 최근 5년 기준으로 알려줘.",
        "metrics": ["매출액"],
        "dimensions": ["region_group", "industry", "time", "scale"],
        "comparison_targets": ["수도권", "비수도권"],
        "tasks": ["gap_by_dimension", "growth_rate", "rank"],
    },
    {
        "name": "scale_bucket_share",
        "query": "매출 1억 미만 소상공인 비중이 높은 업종과 낮은 업종을 각각 5개씩 알려줘.",
        "metrics": ["비중"],
        "dimensions": ["industry", "scale", "sales_size"],
        "tasks": ["share_by_group", "top_bottom_rank"],
    },
    {
        "name": "explicit_range_rank_change",
        "query": "2019년부터 2024년까지 중소기업 매출 상위 10개 업종의 순위 변동을 보여줘.",
        "metrics": ["매출액"],
        "dimensions": ["industry", "time", "scale"],
        "tasks": ["rank", "rank_change"],
        "time_type": "year_range",
    },
    {
        "name": "month_over_month",
        "query": "전월 대비 소상공인 경기전망지수가 가장 크게 하락한 업종을 최근 12개월에서 찾아줘.",
        "metrics": ["경기전망지수"],
        "dimensions": ["industry", "time", "scale"],
        "tasks": ["rank", "change_compare"],
        "must_not_dimension_values": ["경기"],
    },
    {
        "name": "five_regions_two_metrics",
        "query": "서울, 부산, 대구, 광주, 대전의 소상공인 수와 평균 매출액을 비교하고 차이가 큰 지역을 찾아줘.",
        "metrics": ["소상공인 수", "평균 매출액"],
        "dimensions": ["regions", "scale"],
        "comparison_targets": ["서울", "부산", "대구", "광주", "대전"],
        "tasks": ["compare_metrics", "gap_by_dimension"],
    },
    {
        "name": "ambiguous_population_basis",
        "query": "2045년 서울 30대 여성 인구와 2020년 값을 비교해서 증가율을 알려줘.",
        "metrics": ["인구"],
        "dimensions": ["region", "age", "sex", "time"],
        "tasks": ["growth_rate", "point_compare"],
        "time_type": "point_compare",
    },
    {
        "name": "policy_funding_vs_closure",
        "query": "정책자금 지원은 증가했는데 폐업률이 감소하지 않은 업종을 찾아줘.",
        "metrics": ["정책자금", "폐업률"],
        "dimensions": ["industry"],
        "tasks": ["condition_filter", "growth_rate"],
    },
    {
        "name": "industry_and_employee_size_cross",
        "query": "업종별·종사자 규모별 중소기업 수를 최근 기준으로 보여주고 가장 큰 조합 Top 10을 알려줘.",
        "metrics": ["중소기업 수"],
        "dimensions": ["industry", "employee_size", "time"],
        "tasks": ["rank"],
    },
    {
        "name": "rank_overlap",
        "query": "창업률 상위 지역과 폐업률 상위 지역이 겹치는지 확인해줘.",
        "metrics": ["창업률", "폐업률"],
        "dimensions": ["region"],
        "tasks": ["rank", "rank_compare", "rank_overlap"],
    },
]


def _blob(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _missing(actual: Any, expected: list[str]) -> list[str]:
    text = _blob(actual)
    return [item for item in expected if item not in text]


async def main() -> None:
    rows = []
    for case in CASES:
        result = await plan_query(case["query"])
        problems: list[Any] = []
        for key, field in [
            ("metrics", result.get("metrics")),
            ("dimensions", result.get("dimensions")),
            ("comparison_targets", result.get("comparison_targets")),
        ]:
            miss = _missing(field, case.get(key, []))
            if miss:
                problems.append({f"missing_{key}": miss, "actual": field})
        task_types = [task.get("type") for task in result.get("analysis_tasks") or [] if isinstance(task, dict)]
        miss_tasks = [task for task in case.get("tasks", []) if task not in task_types]
        if miss_tasks:
            problems.append({"missing_tasks": miss_tasks, "actual": task_types})
        if case.get("time_type"):
            time_request = result.get("time_request") or {}
            if time_request.get("type") != case["time_type"]:
                problems.append({"time_type": time_request.get("type"), "expected": case["time_type"]})
        for bad in case.get("must_not_dimension_values", []):
            if bad in _blob(result.get("dimensions")):
                problems.append({"forbidden_dimension_value": bad, "dimensions": result.get("dimensions")})
        rows.append({
            "name": case["name"],
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "metrics": result.get("metrics"),
            "dimensions": result.get("dimensions"),
            "comparison_targets": result.get("comparison_targets"),
            "time_request": result.get("time_request"),
            "analysis_tasks": result.get("analysis_tasks"),
        })
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    failed = [row for row in rows if row["status"] != "PASS"]
    print(f"\nSUMMARY {len(rows) - len(failed)}/{len(rows)} PASS")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
