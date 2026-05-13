from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import (
    answer_query, check_stat_availability, explore_table,
    quick_region_compare, quick_stat, quick_trend,
)


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
        "name": "quick_stat_sme_total_sales_alias",
        "tool": quick_stat,
        "args": ("중소기업 총매출", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        "name": "answer_busan_sme_sales_trend",
        "tool": answer_query,
        "args": ("부산 중소기업 매출액 최근 5년 추이 보여줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_trend", "direct_key": "중소기업_매출액", "region": "부산", "data_count": 5},
    },
    {
        "name": "answer_sme_vs_large_sales",
        "tool": answer_query,
        "args": ("중소기업과 대기업 매출액을 비교해줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_composite_comparison"},
    },
    {
        "name": "answer_sme_vs_large_sales_share",
        "tool": answer_query,
        "args": ("중소기업 전체 매출액과 대기업 매출액을 비교하고 전체 매출에서 중소기업 비중을 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_composite_comparison"},
    },
    {
        "name": "answer_ai_stats_search",
        "tool": answer_query,
        "args": ("AI 관련 통계 찾아줘",),
        "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan"},
    },
    {
        "name": "answer_wind_stats_search",
        "tool": answer_query,
        "args": ("풍력발전과 해상풍력 통계 찾아줘",),
        "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan"},
    },
    {
        "name": "answer_construction_stats_search",
        "tool": answer_query,
        "args": ("건설이나 건축 관련 통계 찾아줘",),
        "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan"},
    },
    {
        "name": "answer_housing_seoul_latest",
        "tool": answer_query,
        "args": ("서울 집값 최신 지수 알려줘",),
        "expect": {"status": "executed", "answer_type": "tier_a_value", "direct_key": "주택매매가격지수", "region": "서울"},
    },
    {
        "name": "quick_trend_seoul_housing_alias",
        "tool": quick_trend,
        "args": ("집값", "서울", 5),
        "expect": {"success": True, "region": "서울", "data_count": 5},
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
    {
        "name": "answer_explicit_period_growth_2019_2023",
        "tool": answer_query,
        "args": ("2019년 대비 2023년 중소기업 매출액 증가율 알려줘",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_growth_rate",
            "comparison_start_prefix": "2019",
            "comparison_end_prefix": "2023",
        },
    },
    {
        "name": "answer_explicit_period_growth_2020_2023",
        "tool": answer_query,
        "args": ("2020년 대비 2023년 중소기업 사업체수 증가율",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_growth_rate",
            "comparison_start_prefix": "2020",
            "comparison_end_prefix": "2023",
        },
    },
    {
        "name": "answer_explicit_period_growth_2020_2022",
        "tool": answer_query,
        "args": ("2020년 대비 2022년 중소기업 사업체수 증가율",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_growth_rate",
            "comparison_start_prefix": "2020",
            "comparison_end_prefix": "2022",
        },
    },
    {
        "name": "answer_explicit_period_growth_both_out_of_range",
        "tool": answer_query,
        "args": ("1990년 대비 2050년 중소기업 사업체수 증가율",),
        "expect": {
            "status": "failed",
            "answer_type": "tier_a_growth_rate_failed",
            "code": "PERIOD_NOT_FOUND",
        },
    },
    {
        "name": "answer_explicit_period_growth_one_out_of_range",
        "tool": answer_query,
        "args": ("2018년 대비 2023년 중소기업 사업체수 증가율",),
        "expect": {
            "status": "failed",
            "answer_type": "tier_a_growth_rate_failed",
            "code": "PERIOD_NOT_FOUND",
        },
    },
    {
        "name": "explore_table_housing_price_index",
        "tool": explore_table,
        "args": ("408", "DT_30404_B012"),
        "expect": {"explore_table_has_classifications": True},
    },
    {
        "name": "explore_table_invalid_id_fails",
        "tool": explore_table,
        "args": ("101", "NO_SUCH_TABLE"),
        "expect": {
            "status": "failed",
            "code": "STAT_NOT_FOUND",
        },
    },
    {
        "name": "explore_table_industry_manufacturing_parent",
        "tool": explore_table,
        "args": ("142", "DT_BR_C001", "제조업"),
        "expect": {"resolved_industry_itm_id": "IM_C"},
    },
    {
        "name": "explore_table_industry_beverage_child",
        "tool": explore_table,
        "args": ("142", "DT_BR_C001", "음료 제조업"),
        "expect": {"resolved_industry_itm_id": "IM_C_11"},
    },
    {
        "name": "check_stat_availability_live_period",
        "tool": check_stat_availability,
        "args": ("실업률", True),
        "expect": {"check_stat_live_period_present": True},
    },
    {
        # Round 6 Step 2b: KSIC 동적 확장 라이브 검증 (제조업)
        "name": "ksic_manufacturing_sme_sales",
        "tool": quick_stat,
        "args": ("제조업_중소기업_매출액", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        # Round 6 Step 2b: 음식점업도 라이브 동작하는지 (소상공인 우선 도메인)
        "name": "ksic_food_sme_business_count",
        "tool": quick_stat,
        "args": ("숙박음식점업_중소기업_사업체수", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        # Round 6 Step 2c: 소상공인 변형 라이브 검증 (음식점업)
        "name": "ksic_food_sosanggong_business_count",
        "tool": quick_stat,
        "args": ("숙박음식점업_소상공인_사업체수", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        # Round 6 Step 2c: 도소매업 소상공인 매출액
        "name": "ksic_retail_sosanggong_sales",
        "tool": quick_stat,
        "args": ("도소매업_소상공인_매출액", "전국", "latest"),
        "expect": {"success": True, "region": "전국", "period": "2023"},
    },
    {
        "name": "answer_top_5_sme_business_count",
        "tool": answer_query,
        "args": ("중소기업 사업체수가 가장 많은 5곳 알려줘",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_top_n",
            "table_len": 5,
        },
    },
    {
        "name": "answer_seoul_sme_sales_share",
        "tool": answer_query,
        "args": ("서울 중소기업 매출액이 전국에서 차지하는 비중",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_share_ratio",
            "region": "서울",
        },
    },
    {
        "name": "answer_seoul_gyeonggi_sme_sum",
        "tool": answer_query,
        "args": ("서울과 경기 중소기업 사업체수 합계 알려줘",),
        "expect": {
            "status": "executed",
            "answer_type": "tier_a_region_sum",
            "regions_in_sum": ["서울", "경기"],
        },
    },
]


def first_table_region(result: dict[str, Any]) -> str | None:
    table = result.get("표") or []
    if table and isinstance(table[0], dict):
        return table[0].get("지역")
    return None


def summarize(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or []
    comparison = result.get("비교") or {}
    calc = result.get("계산") or {}
    return {
        "error": result.get("오류"),
        "code": result.get("코드"),
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
        "comparison_start": (comparison.get("시작") or {}).get("시점"),
        "comparison_end": (comparison.get("종료") or {}).get("시점"),
        "growth_rate": comparison.get("변화율_퍼센트"),
        "table_len": len(table),
        "share_pct": calc.get("비중_퍼센트"),
        "sum_regions": calc.get("포함_지역"),
        "used_period": result.get("used_period"),
        "period_age_years": result.get("period_age_years"),
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
    if "code" in expect and summary["code"] != expect["code"]:
        problems.append(f"code={summary['code']}")
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
    if "comparison_start_prefix" in expect:
        start = str(summary["comparison_start"] or "")
        if not start.startswith(expect["comparison_start_prefix"]):
            problems.append(f"comparison_start={summary['comparison_start']}")
    if "comparison_end_prefix" in expect:
        end = str(summary["comparison_end"] or "")
        if not end.startswith(expect["comparison_end_prefix"]):
            problems.append(f"comparison_end={summary['comparison_end']}")
    if "table_len" in expect and summary["table_len"] != expect["table_len"]:
        problems.append(f"table_len={summary['table_len']}")
    if "regions_in_sum" in expect:
        regions = list(summary.get("sum_regions") or [])
        missing = [r for r in expect["regions_in_sum"] if r not in regions]
        if missing:
            problems.append(f"missing_sum_regions={missing}")
    if expect.get("explore_table_has_classifications"):
        axes = (result or {}).get("분류축") or {}
        if not isinstance(axes, dict) or not axes:
            problems.append("explore_table_no_classifications")
    if "resolved_industry_itm_id" in expect:
        resolved = (result or {}).get("resolved_industry") or {}
        if resolved.get("ITM_ID") != expect["resolved_industry_itm_id"]:
            problems.append(f"resolved_industry_itm_id={resolved.get('ITM_ID')}")
    if expect.get("check_stat_live_period_present"):
        live = (result or {}).get("라이브_수록기간") or {}
        if not isinstance(live, dict) or not live.get("최신_수록시점"):
            problems.append("live_period_missing")
    if expect.get("status") == "executed" and summary["used_period"] in (None, ""):
        problems.append("missing_used_period")
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
