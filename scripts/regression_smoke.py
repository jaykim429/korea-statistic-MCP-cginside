from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import answer_query, quick_region_compare, quick_stat, quick_trend


TestFn = Callable[..., Awaitable[dict[str, Any]]]


TESTS: list[dict[str, Any]] = [
    {
        "name": "sme_sales_direct",
        "tool": quick_stat,
        "args": ("중소기업 매출액", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        "name": "sme_sales_seoul_filter",
        "tool": quick_stat,
        "args": ("중소기업 매출액", "서울", "latest"),
        "expect": {"success": True, "region": "서울", "period": "2023"},
    },
    {
        "name": "large_company_sales_direct",
        "tool": quick_stat,
        "args": ("대기업 매출액", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        "name": "sme_sales_region_compare",
        "tool": quick_region_compare,
        "args": ("중소기업 매출액",),
        "expect": {"success": True, "region_count": 17},
    },
    {
        "name": "sme_business_count_region_compare",
        "tool": quick_region_compare,
        "args": ("중소기업 사업체수",),
        "expect": {"success": True, "region_count": 17},
    },
    {
        "name": "sme_sales_trend",
        "tool": quick_trend,
        "args": ("중소기업 매출액", "전국", 5),
        "expect": {"success": True, "data_count": 5},
    },
    {
        "name": "answer_sme_sales",
        "tool": answer_query,
        "args": ("중소기업 매출액 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "direct_key": "중소기업_매출액"},
    },
    {
        "name": "answer_sme_sales_region_compare",
        "tool": answer_query,
        "args": ("중소기업 매출액을 시도별로 비교해줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_region_comparison"},
    },
    {
        "name": "answer_seoul_sme_sales",
        "tool": answer_query,
        "args": ("서울 중소기업 매출액 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "region": "서울"},
    },
    {
        "name": "answer_gyeonggi_sme_business_count",
        "tool": answer_query,
        "args": ("경기 중소기업 사업체수 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "region": "경기"},
    },
    {
        "name": "gdp_region_misuse_blocked",
        "tool": quick_stat,
        "args": ("GDP", "서울", "latest"),
        "expect": {"error": True},
    },
    {
        "name": "housing_price_monthly_stat",
        "tool": quick_stat,
        "args": ("주택매매가격지수", "전국", "latest"),
        "expect": {"success": True},
    },
    {
        "name": "housing_price_monthly_trend",
        "tool": quick_trend,
        "args": ("주택매매가격지수", "전국", 5),
        "expect": {"success": True},
    },
]


def first_table_region(result: dict[str, Any]) -> str | None:
    table = result.get("표") or []
    if table and isinstance(table[0], dict):
        return table[0].get("지역")
    return None


def summarize(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or []
    return {
        "error": result.get("오류"),
        "empty_result": result.get("결과") == "데이터 없음",
        "status": result.get("상태"),
        "answer_type": result.get("답변유형"),
        "value": result.get("값"),
        "unit": result.get("단위"),
        "period": result.get("시점"),
        "region": result.get("지역") or first_table_region(result),
        "stat_name": result.get("통계명"),
        "region_count": result.get("지역수"),
        "data_count": result.get("데이터수"),
        "direct_key": (result.get("route") or {}).get("direct_stat_key"),
        "first_row": table[0] if table else None,
    }


def check(result: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    summary = summarize(result)
    failed = bool(summary["error"] or summary["empty_result"])

    if expect.get("success") and failed:
        problems.append("expected_success_but_failed")
    if expect.get("error") and not summary["error"]:
        problems.append("expected_error_but_succeeded")
    if "status" in expect and summary["status"] != expect["status"]:
        problems.append(f"status={summary['status']}")
    if "answer_type" in expect and summary["answer_type"] != expect["answer_type"]:
        problems.append(f"answer_type={summary['answer_type']}")
    if "direct_key" in expect and summary["direct_key"] != expect["direct_key"]:
        problems.append(f"direct_key={summary['direct_key']}")
    if "region" in expect and summary["region"] != expect["region"]:
        problems.append(f"region={summary['region']}")
    if "period" in expect and str(summary["period"]) != expect["period"]:
        problems.append(f"period={summary['period']}")
    if "region_count" in expect and summary["region_count"] != expect["region_count"]:
        problems.append(f"region_count={summary['region_count']}")
    if "data_count" in expect and summary["data_count"] != expect["data_count"]:
        problems.append(f"data_count={summary['data_count']}")
    return problems


async def main() -> None:
    rows = []
    for test in TESTS:
        try:
            result = await test["tool"](*test["args"])
            problems = check(result, test["expect"])
            rows.append(
                {
                    "name": test["name"],
                    "status": "PASS" if not problems else "FAIL",
                    "problems": problems,
                    "summary": summarize(result),
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "name": test["name"],
                    "status": "EXCEPTION",
                    "problems": [repr(exc)],
                    "summary": {},
                }
            )
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
