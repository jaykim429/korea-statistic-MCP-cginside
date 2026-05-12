from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import answer_query, quick_region_compare, quick_stat, quick_trend


CASES: list[tuple[str, Any, tuple[Any, ...]]] = [
    ("quick_stat_sme_total_sales", quick_stat, ("중소기업 총매출", "전국", "latest")),
    ("quick_stat_sme_whole_sales", quick_stat, ("중소기업 전체 매출", "전국", "latest")),
    ("answer_seoul_sme_total_sales", answer_query, ("서울의 중소기업 총매출 알려줘",)),
    ("answer_busan_sme_sales_trend", answer_query, ("부산 중소기업 매출액 최근 5년 추이 보여줘",)),
    ("answer_sme_vs_large_sales", answer_query, ("중소기업과 대기업 매출액을 비교해줘",)),
    ("answer_sme_vs_large_sales_share", answer_query, ("중소기업 전체 매출액과 대기업 매출액을 비교하고 전체 매출에서 중소기업 비중을 알려줘",)),
    ("answer_sme_smallbiz_counts_seoul", answer_query, ("서울의 중소기업 수와 소상공인 사업체 수를 비교해줘",)),
    ("answer_ai_stats_search", answer_query, ("AI 관련 통계 찾아줘",)),
    ("answer_wind_stats_search", answer_query, ("풍력발전과 해상풍력 통계 찾아줘",)),
    ("answer_construction_stats_search", answer_query, ("건설이나 건축 관련 통계 찾아줘",)),
    ("answer_housing_seoul_latest", answer_query, ("서울 집값 최신 지수 알려줘",)),
    ("quick_region_sme_workers", quick_region_compare, ("중소기업 종사자수",)),
    ("quick_trend_seoul_housing", quick_trend, ("집값", "서울", 5)),
]


def compact(result: dict[str, Any]) -> dict[str, Any]:
    table = result.get("표") or []
    return {
        "오류": result.get("오류"),
        "결과": result.get("결과"),
        "상태": result.get("상태"),
        "답변유형": result.get("답변유형"),
        "answer": result.get("answer"),
        "값": result.get("값"),
        "단위": result.get("단위"),
        "시점": result.get("시점"),
        "지역": result.get("지역"),
        "통계명": result.get("통계명"),
        "지역수": result.get("지역수"),
        "데이터수": result.get("데이터수"),
        "direct_key": (result.get("route") or {}).get("direct_stat_key"),
        "표_앞2개": table[:2],
        "검색결과_앞3개": (result.get("검색결과") or result.get("검색_후보") or [])[:3],
    }


async def main() -> None:
    rows = []
    for name, fn, args in CASES:
        try:
            result = await fn(*args)
            rows.append({"name": name, "summary": compact(result)})
        except Exception as exc:
            rows.append({"name": name, "exception": repr(exc)})
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
