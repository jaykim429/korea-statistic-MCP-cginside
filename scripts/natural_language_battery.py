"""Comprehensive natural-language Q -> answer battery.

Drives answer_query (the public natural-language entry point) across the
intent dispatch matrix so every handler is exercised in the same shape
the agent uses. Reports per-category pass/fail with the assertions that
matter for that intent, plus Stage-3 used_period / period_age_years
coverage on every executed answer.
"""
from __future__ import annotations

import argparse
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

from kosis_mcp_server import answer_query


# Each case: query, expectations on the structured answer.
# expect supported keys:
#   answer_type: required exact match on 답변유형
#   status: required exact match on 상태 (default "executed")
#   region: 표[0]["지역"] or result["지역"] equals
#   region_in_first_row: list of acceptable values for 표[0]["지역"]
#   table_min_len, table_len
#   used_period_prefix: str | list[str]  (verifies Stage 3 metadata)
#   comparison_start_prefix, comparison_end_prefix
#   share_pct_between: (low, high)
#   sum_includes_regions: list[str]
#   warning_contains: substring expected in 검증_주의

CASES: list[dict[str, Any]] = [
    # ── 1. 단일값 (단일 시점, 단일 지역) ───────────────────────────────
    {"group": "single_value", "name": "population_seoul",
     "query": "서울 인구 알려줘",
     "expect": {"answer_type": "tier_a_value", "region": "서울", "used_period_prefix": "20"}},
    {"group": "single_value", "name": "fertility_busan",
     "query": "부산 합계출산율 최신값",
     "expect": {"answer_type": "tier_a_value", "region": "부산", "used_period_prefix": "20"}},
    {"group": "single_value", "name": "unemployment_seoul",
     "query": "서울 실업률 최신값 알려줘",
     "expect": {"answer_type": "tier_a_value", "region": "서울", "used_period_prefix": "20"}},
    {"group": "single_value", "name": "gdp_national",
     "query": "우리나라 GDP 알려줘",
     "expect": {"answer_type": "tier_a_value", "used_period_prefix": "20"}},
    {"group": "single_value", "name": "cpi_daegu",
     "query": "대구 소비자물가지수",
     "expect": {"answer_type": "tier_a_value", "region": "대구", "used_period_prefix": "20"}},
    {"group": "single_value", "name": "housing_seoul",
     "query": "서울 주택매매가격지수 최신",
     "expect": {"answer_type": "tier_a_value", "region": "서울", "used_period_prefix": "20"}},

    # ── 2. 시계열 추이 ────────────────────────────────────────────────
    {"group": "trend", "name": "busan_sme_sales_5y",
     "query": "부산 중소기업 매출액 최근 5년 추이 보여줘",
     "expect": {"answer_type": "tier_a_trend", "region": "부산", "table_min_len": 4}},
    {"group": "trend", "name": "seoul_housing_5y",
     "query": "서울 집값 최근 5년 추이",
     "expect": {"answer_type": "tier_a_trend", "region": "서울", "table_min_len": 4}},

    # ── 3. 증가율 (명시 기간 vs latest 폴백) ───────────────────────────
    {"group": "growth_explicit", "name": "sme_sales_2019_2023",
     "query": "2019년 대비 2023년 중소기업 매출액 증가율 알려줘",
     "expect": {"answer_type": "tier_a_growth_rate",
                "comparison_start_prefix": "2019", "comparison_end_prefix": "2023",
                "used_period_prefix": "2023"}},
    {"group": "growth_explicit", "name": "sme_count_2020_2023",
     "query": "2020년 대비 2023년 중소기업 사업체수 증가율",
     "expect": {"answer_type": "tier_a_growth_rate",
                "comparison_start_prefix": "2020", "comparison_end_prefix": "2023"}},
    {"group": "growth_default", "name": "sme_sales_recent",
     "query": "중소기업 매출액 최근 5년 증가율",
     "expect": {"answer_type": "tier_a_growth_rate"}},

    # ── 4. 시도별 비교 ────────────────────────────────────────────────
    {"group": "region_compare", "name": "sme_sales_by_region",
     "query": "중소기업 매출액을 시도별로 비교해줘",
     "expect": {"answer_type": "tier_a_region_comparison", "table_min_len": 17}},
    {"group": "region_compare", "name": "unemployment_by_region",
     "query": "실업률 시도별 현황",
     "expect": {"answer_type": "tier_a_region_comparison", "table_min_len": 17}},

    # ── 5. Top N (#11) ────────────────────────────────────────────────
    {"group": "top_n", "name": "sme_count_top_5",
     "query": "중소기업 사업체수가 가장 많은 5곳 알려줘",
     "expect": {"answer_type": "tier_a_top_n", "table_len": 5}},
    {"group": "top_n", "name": "sme_count_top_3",
     "query": "중소기업 사업체수 상위 3개 시도",
     "expect": {"answer_type": "tier_a_top_n", "table_len": 3}},
    {"group": "top_n", "name": "sme_sales_bottom_5",
     "query": "중소기업 매출액이 가장 적은 5곳 알려줘",
     "expect": {"answer_type": "tier_a_top_n", "table_len": 5}},

    # ── 6. 비중 (#5) ──────────────────────────────────────────────────
    {"group": "share_ratio", "name": "seoul_sme_sales_share",
     "query": "서울 중소기업 매출액이 전국에서 차지하는 비중",
     "expect": {"answer_type": "tier_a_share_ratio", "region": "서울",
                "share_pct_between": (10.0, 50.0)}},
    {"group": "share_ratio", "name": "gyeonggi_sme_count_share",
     "query": "경기 중소기업 사업체수가 전국에서 차지하는 비중",
     "expect": {"answer_type": "tier_a_share_ratio", "region": "경기",
                "share_pct_between": (10.0, 50.0)}},

    # ── 7. 합산 (#12) ─────────────────────────────────────────────────
    {"group": "aggregation", "name": "seoul_gyeonggi_sme_count_sum",
     "query": "서울과 경기 중소기업 사업체수 합계 알려줘",
     "expect": {"answer_type": "tier_a_region_sum",
                "sum_includes_regions": ["서울", "경기"]}},
    {"group": "aggregation", "name": "seoul_busan_sme_sales_sum",
     "query": "서울과 부산 중소기업 매출액 합산",
     "expect": {"answer_type": "tier_a_region_sum",
                "sum_includes_regions": ["서울", "부산"]}},

    # ── 8. 복합 핸들러 (기존 SME 특수 분기) ───────────────────────────
    {"group": "composite", "name": "sme_vs_large_sales",
     "query": "중소기업과 대기업 매출액을 비교해줘",
     "expect": {"answer_type": "tier_a_composite_comparison"}},
    {"group": "composite", "name": "sme_smallbiz_counts",
     "query": "서울의 중소기업 수와 소상공인 사업체 수를 비교해줘",
     "expect": {"answer_type": "tier_a_composite"}},
    {"group": "composite", "name": "sme_workers_per_business",
     "query": "중소기업 사업체당 평균 종사자 수",
     "expect": {"answer_type": "tier_a_composite_calculation"}},

    # ── 9. 검색 폴백 ──────────────────────────────────────────────────
    {"group": "search_fallback", "name": "ai_stats_search",
     "query": "AI 관련 통계 찾아줘",
     "expect": {"answer_type": "search_and_plan", "status": "needs_table_selection"}},
    {"group": "search_fallback", "name": "wind_stats_search",
     "query": "풍력발전 통계 찾아줘",
     "expect": {"answer_type": "search_and_plan", "status": "needs_table_selection"}},
    {"group": "search_fallback", "name": "construction_stats_search",
     "query": "건설 관련 통계 찾아줘",
     "expect": {"answer_type": "search_and_plan", "status": "needs_table_selection"}},

    # ── 10. 가드레일 / 엣지케이스 ──────────────────────────────────────
    {"group": "guardrail", "name": "future_period_blocked",
     "query": "2030년 중소기업 매출액",
     "expect": {}},
    {"group": "guardrail", "name": "vague_query",
     "query": "우리나라 잘 살아?",
     "expect": {}},

    # ── 11. Stage 4 의도/실행 불일치 경고 ──────────────────────────────
    # 두 지역을 언급했지만 합계/Top N/비중 키워드가 없어 단일값으로 폴백되는 케이스.
    # 사용자가 "서울과 경기"라고 말한 의도가 응답에 반영되지 않을 때 경고 트레일이
    # 떠야 한다 — 정직한 "조용한 오답" 방지 장치.
    {"group": "mismatch_warning", "name": "comparison_targets_dropped",
     "query": "서울과 경기 인구 알려줘",
     "expect": {"answer_type": "tier_a_value", "warning_contains": "비교 대상"}},

    # ── 12. Stage 5: region aliases (#8) ──────────────────────────────
    # "서울특별시" / "서울시" 같은 행정 정식 명칭과 영문 표기가 17개 시도 enum에
    # 동일하게 매핑돼야 한다.
    {"group": "region_alias", "name": "seoul_full_admin_name",
     "query": "서울특별시 인구 최신값",
     "expect": {"answer_type": "tier_a_value", "region": "서울"}},
    {"group": "region_alias", "name": "gyeonggi_do_form",
     "query": "경기도 중소기업 사업체수",
     "expect": {"answer_type": "tier_a_value", "region": "경기"}},

    # ── 13. Stage 5: population mismatch (#7) ──────────────────────────
    # "기업 수" 어휘로 물었는데 매핑은 "사업체 수"로 되는 silent substitution.
    # 응답 자체는 나오되 모집단 차이 경고가 떠야 한다.
    {"group": "population_mismatch", "name": "enterprise_count_warning",
     "query": "중소기업 기업수 알려줘",
     "expect": {"answer_type": "tier_a_value", "warning_contains": "모집단 다름"}},

    # ── 14. Stage 6: composite regions ─────────────────────────────────
    # "수도권"·"비수도권"처럼 행정 단위가 아닌 합성 지역도 17개 시도로 전개해
    # 합산 또는 비중 계산을 해야 한다.
    {"group": "composite_region", "name": "sudokwon_sum",
     "query": "수도권 중소기업 사업체수 합계",
     "expect": {"answer_type": "tier_a_region_sum"}},
    {"group": "composite_region", "name": "sudokwon_share",
     "query": "수도권 중소기업 사업체수 비중",
     "expect": {"answer_type": "tier_a_composite_share_ratio"}},
    {"group": "composite_region", "name": "yeongnam_sum",
     "query": "영남권 중소기업 매출액",
     "expect": {"answer_type": "tier_a_region_sum"}},

    # ── 15. Stage 6: extended top-N patterns ──────────────────────────
    {"group": "top_n_extended", "name": "rank_5_until",
     "query": "중소기업 사업체수 5위까지 알려줘",
     "expect": {"answer_type": "tier_a_top_n", "table_len": 5}},

    # ── 16. Stage 7: NL response polish ───────────────────────────────
    # "은(는)" 일괄 표기와 "YYYY.MM" raw 시점이 자연스러운 한국어로 다듬어져야 함.
    # 두 케이스 모두 answer 필드에 raw 표기가 남아 있으면 FAIL.
    {"group": "nl_polish", "name": "no_eun_neun_placeholder",
     "query": "서울 인구 최신값",
     "expect": {"answer_type": "tier_a_value", "region": "서울",
                "answer_excludes": "은(는)"}},
    {"group": "nl_polish", "name": "monthly_period_humanized",
     "query": "서울 주택매매가격지수 최신",
     "expect": {"answer_type": "tier_a_value", "region": "서울",
                "answer_excludes": "2026.0", "answer_contains": "년"}},

    # ── 17. Stage 8: false-positive fix on trend with year reference ──
    # "2020년 이후 인구 추이" 같은 쿼리에서 2020은 기준 시작점이지 사용 시점
    # 불일치가 아님 — tier_a_trend 응답에는 year mismatch 경고 발동 안 되어야 함.
    {"group": "fp_safety", "name": "trend_with_year_reference",
     "query": "2020년 이후 인구 추이",
     "expect": {"answer_type": "tier_a_trend",
                "warning_excludes": "명시된 연도"}},

    # ── 18. Stage 8/4: SHARE_RATIO intent + 전국 → mismatch warning ───
    # "전국 중소기업 매출액 비중" — 분자 분모가 동일하므로 share_ratio 디스패치
    # 자체는 차단되지만 의도는 SHARE_RATIO이므로 응답에 경고 트레일이 떠야 함.
    {"group": "fp_safety", "name": "share_ratio_for_전국_warns",
     "query": "전국 중소기업 매출액 비중",
     "expect": {"warning_contains": "STAT_SHARE_RATIO"}},

    # ── 19. Stage 6: top-N 폴백 N=5 ───────────────────────────────────
    # 명시적 숫자 없이 "가장 많은 곳"이라고만 물어도 dispatcher가 N=5 기본값으로
    # 폴백해 5개 row를 반환해야 한다.
    {"group": "top_n_default", "name": "no_explicit_n",
     "query": "중소기업 사업체수가 가장 많은 곳 알려줘",
     "expect": {"answer_type": "tier_a_top_n", "table_len": 5}},

    # ── 20. Stage 13: cross-tool routing for advanced intents (#19) ────
    # STAT_CORRELATION 의도는 search_fallback로 빠지되 응답에 correlate_stats
    # 호출 힌트가 포함되어야 한다.
    {"group": "tool_routing", "name": "correlation_routes_to_correlate_stats",
     "query": "인구와 GDP 상관관계 분석해줘",
     "expect": {"status": "needs_table_selection",
                "answer_type": "search_and_plan",
                "tool_hint_contains": "correlate_stats"}},
]


def _table_first_region(result: dict[str, Any]) -> str | None:
    table = result.get("표") or []
    if table and isinstance(table[0], dict):
        return table[0].get("지역")
    return None


def _summarize(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or []
    comparison = result.get("비교") or {}
    calc = result.get("계산") or {}
    return {
        "status": result.get("상태"),
        "answer_type": result.get("답변유형"),
        "region": result.get("지역") or _table_first_region(result),
        "table_len": len(table),
        "used_period": result.get("used_period"),
        "period_age_years": result.get("period_age_years"),
        "comparison_start": (comparison.get("시작") or {}).get("시점"),
        "comparison_end": (comparison.get("종료") or {}).get("시점"),
        "growth_rate": comparison.get("변화율_퍼센트"),
        "share_pct": calc.get("비중_퍼센트"),
        "sum_regions": calc.get("포함_지역"),
        "first_row_keys": sorted((table[0].keys()) if table and isinstance(table[0], dict) else []),
        "warnings": result.get("검증_주의") or [],
        "answer": result.get("answer"),
    }


def _check(result: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    s = _summarize(result)
    problems: list[str] = []

    expected_status = expect.get("status", "executed" if expect.get("answer_type") else None)
    if expected_status and s["status"] != expected_status:
        problems.append(f"status={s['status']}!={expected_status}")

    if "answer_type" in expect and s["answer_type"] != expect["answer_type"]:
        problems.append(f"answer_type={s['answer_type']}!={expect['answer_type']}")

    if "region" in expect and s["region"] != expect["region"]:
        problems.append(f"region={s['region']}!={expect['region']}")

    if "table_min_len" in expect and s["table_len"] < expect["table_min_len"]:
        problems.append(f"table_len={s['table_len']}<min{expect['table_min_len']}")

    if "table_len" in expect and s["table_len"] != expect["table_len"]:
        problems.append(f"table_len={s['table_len']}!={expect['table_len']}")

    if "used_period_prefix" in expect:
        used = str(s["used_period"] or "")
        prefixes = expect["used_period_prefix"]
        if isinstance(prefixes, str):
            prefixes = [prefixes]
        if not any(used.startswith(p) for p in prefixes):
            problems.append(f"used_period={s['used_period']}")

    if "comparison_start_prefix" in expect:
        start = str(s["comparison_start"] or "")
        if not start.startswith(expect["comparison_start_prefix"]):
            problems.append(f"comparison_start={s['comparison_start']}")

    if "comparison_end_prefix" in expect:
        end = str(s["comparison_end"] or "")
        if not end.startswith(expect["comparison_end_prefix"]):
            problems.append(f"comparison_end={s['comparison_end']}")

    if "share_pct_between" in expect:
        lo, hi = expect["share_pct_between"]
        pct = s["share_pct"]
        if pct is None or not (lo <= float(pct) <= hi):
            problems.append(f"share_pct={pct} not in [{lo},{hi}]")

    if "sum_includes_regions" in expect:
        regions = list(s.get("sum_regions") or [])
        missing = [r for r in expect["sum_includes_regions"] if r not in regions]
        if missing:
            problems.append(f"missing_sum_regions={missing}")

    if "warning_contains" in expect:
        warnings_text = " | ".join(s.get("warnings") or [])
        if expect["warning_contains"] not in warnings_text:
            problems.append(f"warning_missing={expect['warning_contains']}")

    if "answer_excludes" in expect:
        answer_text = str(s.get("answer") or "")
        if expect["answer_excludes"] in answer_text:
            problems.append(f"answer_contains_raw={expect['answer_excludes']!r}")

    if "answer_contains" in expect:
        answer_text = str(s.get("answer") or "")
        if expect["answer_contains"] not in answer_text:
            problems.append(f"answer_missing={expect['answer_contains']!r}")

    if "warning_excludes" in expect:
        warnings_text = " | ".join(s.get("warnings") or [])
        if expect["warning_excludes"] in warnings_text:
            problems.append(f"warning_unexpected={expect['warning_excludes']!r}")

    if expected_status == "executed" and not s.get("used_period"):
        problems.append("stage3_missing_used_period")

    return problems


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", help="filter to a single category", default=None)
    parser.add_argument("--name", help="filter to a single case name", default=None)
    parser.add_argument("--summary-only", action="store_true",
                        help="print pass/fail table without full result dump")
    args = parser.parse_args()

    cases = CASES
    if args.group:
        cases = [c for c in cases if c["group"] == args.group]
    if args.name:
        cases = [c for c in cases if c["name"] == args.name]

    results: list[dict[str, Any]] = []
    for case in cases:
        try:
            result = await answer_query(case["query"])
            problems = _check(result, case["expect"])
            row = {
                "group": case["group"],
                "name": case["name"],
                "query": case["query"],
                "status": "PASS" if not problems else "FAIL",
                "problems": problems,
                "summary": _summarize(result),
            }
        except Exception as exc:
            row = {
                "group": case["group"],
                "name": case["name"],
                "query": case["query"],
                "status": "EXCEPTION",
                "problems": [repr(exc)],
                "summary": {},
            }
        results.append(row)

    if args.summary_only:
        groups: dict[str, dict[str, int]] = {}
        for r in results:
            g = groups.setdefault(r["group"], {"PASS": 0, "FAIL": 0, "EXCEPTION": 0})
            g[r["status"]] = g.get(r["status"], 0) + 1
        print(f"{'group':<22s} {'PASS':>5s} {'FAIL':>5s} {'EXC':>5s}")
        print("-" * 42)
        for group, counts in groups.items():
            print(f"{group:<22s} {counts['PASS']:>5d} {counts['FAIL']:>5d} {counts['EXCEPTION']:>5d}")
        print()
        for r in results:
            if r["status"] != "PASS":
                print(f"  {r['status']:<10s} {r['group']}/{r['name']}: {r['problems']}")
                print(f"             query: {r['query']}")
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))

    fail_count = sum(1 for r in results if r["status"] != "PASS")
    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
