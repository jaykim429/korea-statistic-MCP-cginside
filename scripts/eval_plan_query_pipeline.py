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

        if "evidence_bundle" in case and result.get("evidence_bundle") is not case["evidence_bundle"]:
            problems.append({"evidence_bundle": result.get("evidence_bundle"), "expected": case["evidence_bundle"]})
        if "analysis_mode" in case and result.get("analysis_mode") != case["analysis_mode"]:
            problems.append({"analysis_mode": result.get("analysis_mode"), "expected": case["analysis_mode"]})
        if "analysis_task_count" in case:
            count = len(result.get("analysis_tasks") or [])
            if count != case["analysis_task_count"]:
                problems.append({"analysis_task_count": count, "expected": case["analysis_task_count"]})

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
        table_required = result.get("table_required_dimensions") or result.get("required_dimensions") or []
        missing_dims = [dim for dim in case.get("table_required_dimensions", []) if dim not in table_required]
        if missing_dims:
            problems.append({"missing_table_required_dimensions": missing_dims, "actual": table_required})
        forbidden_dims = [dim for dim in case.get("must_not_table_required_dimensions", []) if dim in table_required]
        if forbidden_dims:
            problems.append({"forbidden_table_required_dimensions": forbidden_dims, "actual": table_required})

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

    print(json.dumps(rows, ensure_ascii=False, indent=2))
    failed = [row for row in rows if row["status"] != "PASS"]
    print(f"\nSUMMARY {len(rows) - len(failed)}/{len(rows)} PASS")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
