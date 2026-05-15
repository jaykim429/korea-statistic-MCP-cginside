from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import answer_query, quick_region_compare, quick_stat, stat_time_compare


AsyncTool = Callable[..., Awaitable[Any]]


CASES: list[dict[str, Any]] = [
    {
        "name": "quick_sme_sales_explicit_year_region",
        "tool": quick_stat,
        "args": ("중소기업 매출액", "서울", "2020"),
        "expect": {"success": True, "region": "서울", "period": "2020"},
    },
    {
        "name": "answer_sme_sales_explicit_year_region",
        "tool": answer_query,
        "args": ("2020년 서울 중소기업 매출액 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "region": "서울", "period": "2020"},
    },
    {
        "name": "quick_housing_explicit_month",
        "tool": quick_stat,
        "args": ("주택매매가격지수", "서울", "2026.03"),
        "expect": {"success": True, "region": "서울", "period": "202603"},
    },
    {
        "name": "answer_housing_explicit_month",
        "tool": answer_query,
        "args": ("2026년 3월 서울 집값 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "region": "서울", "period": "202603"},
    },
    {
        "name": "quick_housing_region_compare_explicit_month",
        "tool": quick_region_compare,
        "args": ("주택매매가격지수", "2026.03"),
        "expect": {"success": True, "region_count": 17, "period": "202603"},
    },
    {
        "name": "quick_population_this_quarter_parsed",
        "tool": quick_stat,
        "args": ("인구", "전국", "이번 분기"),
        "expect": {"status": "failed", "code": "PERIOD_NOT_FOUND"},
    },
    {
        "name": "quick_housing_previous_quarter_range",
        "tool": quick_stat,
        "args": ("주택매매가격지수", "전국", "지난 분기"),
        "expect": {"success": True, "period": "202603"},
    },
    {
        "name": "answer_sme_sales_yoy",
        "tool": answer_query,
        "args": ("중소기업 매출액 전년 대비 증가율 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_growth_rate", "min_table_rows": 2},
    },
    {
        "name": "answer_seoul_housing_mom",
        "tool": answer_query,
        "args": ("서울 집값 전월 대비 변화율 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_growth_rate", "region": "서울", "min_table_rows": 2},
    },
    {
        "name": "time_compare_explicit_periods",
        "tool": stat_time_compare,
        "args": ("중소기업 매출액", "전국", "2021", "2023", 5),
        "expect": {"status": "executed", "compare_start": "2021", "compare_end": "2023"},
    },
    {
        "name": "quick_invalid_region_blocked",
        "tool": quick_stat,
        "args": ("중소기업 매출액", "화성시", "latest"),
        "expect": {"error": True},
    },
    {
        "name": "answer_old_missing_period_not_latest",
        "tool": answer_query,
        "args": ("1990년 중소기업 매출액 알려줘",),
        "expect": {"status": "failed"},
    },
]


def first_table_row(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or result.get("data") or []
    if table and isinstance(table[0], dict):
        return table[0]
    return {}


def summarize(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"kind": type(result).__name__, "repr": repr(result)[:240]}

    first = first_table_row(result)
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    comparison = result.get("비교") or result.get("comparison") or {}
    return {
        "kind": "dict",
        "error": result.get("오류") or result.get("error"),
        "empty_result": result.get("결과") == "데이터 없음",
        "status": result.get("상태") or result.get("status"),
        "code": result.get("코드") or result.get("code"),
        "answer_type": result.get("답변유형") or result.get("answer_type") or metadata.get("answer_type"),
        "answer": result.get("answer"),
        "value": result.get("값") or result.get("value"),
        "unit": result.get("단위") or result.get("unit") or metadata.get("unit"),
        "period": result.get("시점") or result.get("used_period") or metadata.get("period") or first.get("시점"),
        "region": result.get("지역") or result.get("region") or metadata.get("region") or first.get("지역"),
        "region_count": result.get("지역수"),
        "table_rows": len(result.get("표") or result.get("data") or []),
        "direct_key": (result.get("route") or {}).get("direct_stat_key"),
        "compare_start": (comparison.get("시작") or {}).get("시점"),
        "compare_end": (comparison.get("종료") or {}).get("시점"),
        "first_row": first,
    }


def check(summary: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    has_error = bool(summary.get("error") or summary.get("empty_result"))

    if expect.get("success") and has_error:
        problems.append("expected_success_but_failed")
    if expect.get("error") and not summary.get("error"):
        problems.append("expected_error_but_succeeded")
    for key in ("status", "code", "answer_type", "region", "period", "region_count", "compare_start", "compare_end"):
        if key in expect and str(summary.get(key)) != str(expect[key]):
            problems.append(f"{key}={summary.get(key)}")
    if "min_table_rows" in expect and (summary.get("table_rows") or 0) < expect["min_table_rows"]:
        problems.append(f"table_rows={summary.get('table_rows')}")
    return problems


async def main() -> None:
    rows: list[dict[str, Any]] = []
    for case in CASES:
        try:
            result = await case["tool"](*case["args"])
            summary = summarize(result)
            problems = check(summary, case["expect"])
            rows.append(
                {
                    "name": case["name"],
                    "status": "PASS" if not problems else "FAIL",
                    "problems": problems,
                    "summary": summary,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "name": case["name"],
                    "status": "ERROR",
                    "problems": [type(exc).__name__],
                    "summary": {"exception": str(exc)},
                }
            )

    failed = [row for row in rows if row["status"] != "PASS"]
    print(json.dumps({"total": len(rows), "passed": len(rows) - len(failed), "failed": len(failed), "rows": rows}, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
