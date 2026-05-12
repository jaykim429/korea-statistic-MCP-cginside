from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mcp.types import ImageContent, TextContent

from kosis_mcp_server import (
    analyze_trend,
    answer_query,
    browse_topic,
    chain_full_analysis,
    chart_compare_regions,
    chart_correlation,
    chart_dashboard,
    chart_distribution,
    chart_dual_axis,
    chart_heatmap,
    chart_line,
    correlate_stats,
    daily_term_lookup,
    detect_outliers,
    forecast_stat,
    indicator_dependency_map,
    quick_region_compare,
    quick_stat,
    quick_trend,
    search_kosis,
    stat_time_compare,
    verify_stat_claims,
)


AsyncTool = Callable[..., Awaitable[Any]]


CASES: list[dict[str, Any]] = [
    # L1: direct statistics by data family
    {"group": "direct.population", "name": "population_seoul", "tool": quick_stat, "args": ("인구", "서울", "latest"), "expect": {"success": True, "region": "서울"}},
    {"group": "direct.demography", "name": "fertility_busan", "tool": quick_stat, "args": ("합계출산율", "부산", "latest"), "expect": {"success": True, "region": "부산"}},
    {"group": "direct.labor", "name": "unemployment_seoul", "tool": quick_stat, "args": ("실업률", "서울", "latest"), "expect": {"success": True, "region": "서울"}},
    {"group": "direct.labor", "name": "solo_self_employed", "tool": quick_stat, "args": ("고용원이 없는 자영업자", "전국", "latest"), "expect": {"success": True}},
    {"group": "direct.economy", "name": "gdp_national", "tool": quick_stat, "args": ("GDP", "전국", "latest"), "expect": {"success": True}},
    {"group": "direct.price", "name": "cpi_daegu", "tool": quick_stat, "args": ("소비자물가지수", "대구", "latest"), "expect": {"success": True, "region": "대구"}},
    {"group": "direct.trade", "name": "exports_national", "tool": quick_stat, "args": ("수출액", "전국", "latest"), "expect": {"success": True}},
    {"group": "direct.monthly", "name": "housing_seoul", "tool": quick_stat, "args": ("주택매매가격지수", "서울", "latest"), "expect": {"success": True, "region": "서울", "period_prefix": "2026"}},
    {"group": "direct.business", "name": "sme_workers_gyeonggi", "tool": quick_stat, "args": ("중소기업 종사자수", "경기", "latest"), "expect": {"success": True, "region": "경기"}},
    {"group": "direct.smallbiz", "name": "smallbiz_jeju", "tool": quick_stat, "args": ("소상공인 사업체수", "제주", "latest"), "expect": {"success": True, "region": "제주"}},

    # L1: regional comparisons
    {"group": "region.population", "name": "population_region_compare", "tool": quick_region_compare, "args": ("인구",), "expect": {"success": True, "region_count": 17}},
    {"group": "region.labor", "name": "unemployment_region_compare", "tool": quick_region_compare, "args": ("실업률",), "expect": {"success": True, "region_count": 17}},
    {"group": "region.price", "name": "cpi_region_compare", "tool": quick_region_compare, "args": ("소비자물가지수",), "expect": {"success": True, "region_count": 17}},
    {"group": "region.monthly", "name": "housing_region_compare", "tool": quick_region_compare, "args": ("주택매매가격지수",), "expect": {"success": True, "region_count": 17}},
    {"group": "region.business", "name": "sme_sales_region_compare", "tool": quick_region_compare, "args": ("중소기업 매출액",), "expect": {"success": True, "region_count": 17}},
    {"group": "guardrail", "name": "gdp_region_compare_blocked", "tool": quick_region_compare, "args": ("GDP",), "expect": {"error": True}},

    # L2: analysis APIs
    {"group": "analysis.trend", "name": "trend_population", "tool": analyze_trend, "args": ("인구", "전국", 10), "expect": {"success": True, "min_data_count": 3}},
    {"group": "analysis.compare", "name": "time_compare_sme_sales", "tool": stat_time_compare, "args": ("중소기업 매출액", "전국", None, None, 5), "expect": {"status": "executed"}},
    {"group": "analysis.forecast", "name": "forecast_population", "tool": forecast_stat, "args": ("인구", "전국", 10, 3), "expect": {"success": True, "forecast_count": 3}},
    {"group": "analysis.outlier", "name": "outlier_housing_seoul", "tool": detect_outliers, "args": ("주택매매가격지수", "서울", 12), "expect": {"success": True}},
    {"group": "analysis.correlation", "name": "corr_unemployment_employment", "tool": correlate_stats, "args": ("실업률", "고용률", "전국", 10), "expect": {"success": True, "min_common_count": 4}},
    {"group": "metadata.formula", "name": "dependency_closure_rate", "tool": indicator_dependency_map, "args": ("폐업률",), "expect": {"status": "mapped"}},

    # L3: visualization APIs
    {"group": "chart.line", "name": "chart_sme_sales_line", "tool": chart_line, "args": ("중소기업 매출액", "전국", 5), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.bar", "name": "chart_sme_sales_regions", "tool": chart_compare_regions, "args": ("중소기업 매출액", ["서울", "경기", "부산"]), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.distribution", "name": "chart_unemployment_distribution", "tool": chart_distribution, "args": ("실업률", "latest", ["서울", "부산"]), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.heatmap", "name": "chart_unemployment_heatmap", "tool": chart_heatmap, "args": ("실업률", ["서울", "부산", "경기"], 5), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.correlation", "name": "chart_corr_unemployment_employment", "tool": chart_correlation, "args": ("실업률", "고용률", "전국", 10), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.dual", "name": "chart_dual_birth_housing", "tool": chart_dual_axis, "args": ("출생아수", "주택매매가격지수", "전국", 10), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chart.dashboard", "name": "chart_dashboard_unemployment", "tool": chart_dashboard, "args": ("실업률", "전국"), "expect": {"image_count": 1, "text_count": 1}},
    {"group": "chain", "name": "chain_unemployment", "tool": chain_full_analysis, "args": ("실업률", "전국"), "expect": {"min_items": 1}},

    # Natural-language routing and search
    {"group": "nl.direct", "name": "nl_gyeonggido_sme_workers", "tool": answer_query, "args": ("경기도 중소기업 종사자수 알려줘",), "expect": {"status": "executed", "answer_type": "tier_a_value", "region": "경기"}},
    {"group": "nl.composite", "name": "nl_sme_large_sales_share", "tool": answer_query, "args": ("중소기업 전체 매출액과 대기업 매출액을 비교하고 전체 매출에서 중소기업 비중을 알려줘",), "expect": {"status": "executed", "answer_type": "tier_a_composite_comparison"}},
    {"group": "nl.composite", "name": "nl_seoul_sme_smallbiz_count", "tool": answer_query, "args": ("서울의 중소기업 수와 소상공인 사업체 수를 비교해줘",), "expect": {"status": "executed", "answer_type": "tier_a_composite", "region": "서울"}},
    {"group": "nl.search", "name": "nl_ai_stats", "tool": answer_query, "args": ("AI 관련 통계 찾아줘",), "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan", "min_search_results": 1}},
    {"group": "nl.search", "name": "nl_wind_stats", "tool": answer_query, "args": ("풍력발전과 해상풍력 통계 찾아줘",), "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan", "min_search_results": 1}},
    {"group": "nl.search", "name": "nl_construction_stats", "tool": answer_query, "args": ("건설이나 건축 관련 통계 찾아줘",), "expect": {"status": "needs_table_selection", "answer_type": "search_and_plan", "min_search_results": 1}},
    {"group": "nl.guardrail", "name": "nl_house_fertility_relation", "tool": answer_query, "args": ("집값과 출산율 관계를 분석해줘",), "expect": {"status": "needs_table_selection"}},
    {"group": "nl.guardrail", "name": "nl_policy_effect", "tool": answer_query, "args": ("정책자금 지원을 받은 소상공인의 생존율이 높은지 분석해줘",), "expect": {"status": "needs_table_selection"}},

    # Metadata/search helpers
    {"group": "helper.term", "name": "term_chicken", "tool": daily_term_lookup, "args": ("치킨집",), "expect": {"success": True}},
    {"group": "helper.topic", "name": "topic_list", "tool": browse_topic, "args": (None,), "expect": {"success": True}},
    {"group": "helper.search", "name": "search_ai", "tool": search_kosis, "args": ("인공지능산업실태조사", 5, True), "expect": {"min_result_count": 1}},
]


def value_at(result: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        cur: Any = result
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok:
            return cur
    return None


def list_summary(result: list[Any]) -> dict[str, Any]:
    image_count = sum(isinstance(item, ImageContent) for item in result)
    text_items = [item for item in result if isinstance(item, TextContent)]
    return {
        "kind": "list",
        "items": len(result),
        "image_count": image_count,
        "text_count": len(text_items),
        "text": [getattr(item, "text", "")[:240] for item in text_items[:2]],
    }


def dict_summary(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or []
    first_row = table[0] if isinstance(table, list) and table else None
    return {
        "kind": "dict",
        "error": result.get("오류"),
        "empty_result": result.get("결과") == "데이터 없음",
        "status": result.get("상태"),
        "code": result.get("코드"),
        "answer_type": result.get("답변유형"),
        "answer": result.get("answer"),
        "value": result.get("값"),
        "unit": result.get("단위"),
        "period": result.get("시점"),
        "region": result.get("지역") or (first_row or {}).get("지역"),
        "stat_name": result.get("통계명"),
        "region_count": result.get("지역수"),
        "data_count": result.get("데이터수"),
        "common_count": result.get("공통_시점수"),
        "forecast_count": len(result.get("예측") or []),
        "result_count": result.get("결과수"),
        "search_count": len(result.get("검색결과") or result.get("검색_후보") or []),
        "direct_key": (result.get("route") or {}).get("direct_stat_key"),
        "first_row": first_row,
    }


def summarize(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return dict_summary(result)
    if isinstance(result, list):
        return list_summary(result)
    return {"kind": type(result).__name__, "repr": repr(result)[:240]}


def check(summary: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    has_error = bool(summary.get("error") or summary.get("empty_result"))

    if expect.get("success") and has_error:
        problems.append("expected_success_but_failed")
    if expect.get("error") and not summary.get("error"):
        problems.append("expected_error_but_succeeded")
    if "status" in expect and summary.get("status") != expect["status"]:
        problems.append(f"status={summary.get('status')}")
    if "answer_type" in expect and summary.get("answer_type") != expect["answer_type"]:
        problems.append(f"answer_type={summary.get('answer_type')}")
    if "region" in expect and summary.get("region") != expect["region"]:
        problems.append(f"region={summary.get('region')}")
    if "region_count" in expect and summary.get("region_count") != expect["region_count"]:
        problems.append(f"region_count={summary.get('region_count')}")
    if "period_prefix" in expect and not str(summary.get("period") or "").startswith(expect["period_prefix"]):
        problems.append(f"period={summary.get('period')}")
    if "min_data_count" in expect and (summary.get("data_count") or 0) < expect["min_data_count"]:
        problems.append(f"data_count={summary.get('data_count')}")
    if "min_common_count" in expect and (summary.get("common_count") or 0) < expect["min_common_count"]:
        problems.append(f"common_count={summary.get('common_count')}")
    if "forecast_count" in expect and summary.get("forecast_count") != expect["forecast_count"]:
        problems.append(f"forecast_count={summary.get('forecast_count')}")
    if "image_count" in expect and summary.get("image_count") != expect["image_count"]:
        problems.append(f"image_count={summary.get('image_count')}")
    if "text_count" in expect and summary.get("text_count") != expect["text_count"]:
        problems.append(f"text_count={summary.get('text_count')}")
    if "min_items" in expect and (summary.get("items") or 0) < expect["min_items"]:
        problems.append(f"items={summary.get('items')}")
    if "min_search_results" in expect and (summary.get("search_count") or 0) < expect["min_search_results"]:
        problems.append(f"search_count={summary.get('search_count')}")
    if "min_result_count" in expect and (summary.get("result_count") or 0) < expect["min_result_count"]:
        problems.append(f"result_count={summary.get('result_count')}")
    return problems


async def enrich_verify(rows: list[dict[str, Any]]) -> None:
    """Run payload-shape verification on a few representative answer_query outputs."""
    for row in rows:
        if row["name"] not in {"nl_gyeonggido_sme_workers", "nl_sme_large_sales_share", "nl_ai_stats"}:
            continue
        result = row.get("_raw_result")
        if isinstance(result, dict):
            verification = await verify_stat_claims(result)
            row["verification"] = dict_summary(verification)
            row["verification"]["verified"] = verification.get("verified")


async def main() -> None:
    rows: list[dict[str, Any]] = []
    for case in CASES:
        try:
            result = await case["tool"](*case["args"])
            summary = summarize(result)
            problems = check(summary, case["expect"])
            rows.append(
                {
                    "group": case["group"],
                    "name": case["name"],
                    "status": "PASS" if not problems else "FAIL",
                    "problems": problems,
                    "summary": summary,
                    "_raw_result": result,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "group": case["group"],
                    "name": case["name"],
                    "status": "EXCEPTION",
                    "problems": [repr(exc)],
                    "summary": {},
                }
            )

    await enrich_verify(rows)
    for row in rows:
        row.pop("_raw_result", None)

    aggregate: dict[str, dict[str, int]] = {}
    for row in rows:
        group = row["group"].split(".")[0]
        aggregate.setdefault(group, {"PASS": 0, "FAIL": 0, "EXCEPTION": 0})
        aggregate[group][row["status"]] += 1

    output = {
        "total": len(rows),
        "passed": sum(1 for row in rows if row["status"] == "PASS"),
        "failed": sum(1 for row in rows if row["status"] != "PASS"),
        "by_group": aggregate,
        "rows": rows,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
