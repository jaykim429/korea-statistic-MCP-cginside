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
        "name": "sme_large_growth_gap",
        "query": "최근 10년간 중소기업 수와 대기업 수의 증가율을 비교하고, 어느 시점부터 격차가 확대되었는지 알려줘.",
        "metrics": ["중소기업 수", "대기업 수"],
        "dimensions": ["time", "scale"],
        "tasks": ["growth_rate", "gap_change_point", "compare_metrics"],
        "time_type": "relative_period",
    },
    {
        "name": "smallbiz_industry_top_bottom_since_2020",
        "query": "2020년 이후 소상공인 사업체 수가 증가한 업종과 감소한 업종을 각각 Top 5로 정리해줘.",
        "metrics": ["사업체 수"],
        "dimensions": ["industry", "time", "scale"],
        "tasks": ["top_bottom_change", "rank"],
        "time_type": "since_year",
    },
    {
        "name": "covid_before_after_average",
        "query": "코로나 이전 3년과 이후 3년의 소상공인 사업체 수 평균을 비교해서 구조적 변화가 있었는지 분석해줘.",
        "metrics": ["사업체 수"],
        "dimensions": ["time", "scale"],
        "tasks": ["period_average_compare"],
        "time_type": "named_period_compare",
    },
    {
        "name": "region_industry_share",
        "query": "시도별 중소기업 수를 비교하되, 각 지역에서 가장 비중이 높은 업종도 함께 알려줘.",
        "metrics": ["중소기업 수"],
        "dimensions": ["region", "industry", "scale"],
        "tasks": ["share_by_group", "rank"],
    },
    {
        "name": "metro_nonmetro_industry_gap",
        "query": "수도권과 비수도권의 소상공인 사업체 수 차이를 비교하고, 업종별로 격차가 가장 큰 분야를 찾아줘.",
        "metrics": ["사업체 수"],
        "dimensions": ["region_group", "industry", "scale"],
        "comparison_targets": ["수도권", "비수도권"],
        "tasks": ["gap_by_dimension"],
    },
    {
        "name": "cities_smallbiz_and_closure",
        "query": "서울, 부산, 대구, 광주, 대전의 소상공인 수와 폐업률을 함께 비교해줘.",
        "metrics": ["소상공인 수", "폐업률"],
        "dimensions": ["regions", "scale"],
        "comparison_targets": ["서울", "부산", "대구", "광주", "대전"],
        "tasks": ["compare_metrics"],
    },
    {
        "name": "region_rank_compare_workers",
        "query": "지역별 중소기업 수 순위와 종사자 수 순위를 비교해서, 기업 수는 많지만 고용 규모는 낮은 지역을 찾아줘.",
        "metrics": ["중소기업 수", "종사자 수", "기업 수"],
        "dimensions": ["region", "scale"],
        "tasks": ["rank_compare", "compare_metrics"],
    },
]


def _contains(actual: list[Any], expected: list[str]) -> list[str]:
    joined = "\n".join(json.dumps(item, ensure_ascii=False) for item in actual)
    return [item for item in expected if item not in joined]


async def main() -> None:
    rows: list[dict[str, Any]] = []
    for case in CASES:
        result = await plan_query(case["query"])
        metrics = result.get("metrics") or []
        dimensions = result.get("dimensions") or []
        tasks = result.get("analysis_tasks") or []
        targets = result.get("comparison_targets") or []
        time_request = result.get("time_request") or {}
        problems: list[Any] = []
        if result.get("evidence_bundle") is not True:
            problems.append({"evidence_bundle": result.get("evidence_bundle")})
        for label, actual_key in [
            ("metrics", metrics),
            ("dimensions", dimensions),
            ("tasks", [task.get("type") for task in tasks if isinstance(task, dict)]),
            ("comparison_targets", targets),
        ]:
            missing = _contains(actual_key, case.get(label, []))
            if missing:
                problems.append({f"missing_{label}": missing, "actual": actual_key})
        if case.get("time_type") and time_request.get("type") != case["time_type"]:
            problems.append({"time_type": time_request.get("type"), "expected": case["time_type"]})
        rows.append({
            "name": case["name"],
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "analysis_mode": result.get("analysis_mode"),
            "metrics": metrics,
            "dimensions": dimensions,
            "comparison_targets": targets,
            "analysis_tasks": tasks,
            "time_request": time_request,
        })
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    failed = [row for row in rows if row["status"] != "PASS"]
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
