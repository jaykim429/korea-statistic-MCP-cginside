from __future__ import annotations

import re
from typing import Any, Optional

from kosis_curation import QuickStatParam, REGION_COMPOSITES, canonical_region as _canonical_region
from kosis_analysis.metadata import _compact_text

STATUS_UNVERIFIED_FORMULA = "UNVERIFIED_FORMULA"


_DIRECT_REGION_NAMES = (
    "전국", "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
    "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원도",
    "강원특별자치도", "충청북도", "충청남도", "전라북도", "전북특별자치도",
    "전라남도", "경상북도", "경상남도", "제주특별자치도",
)


def _extract_single_region_from_query(query: str) -> Optional[str]:
    q = str(query or "")
    matches: list[str] = []
    for name in sorted(_DIRECT_REGION_NAMES, key=len, reverse=True):
        if name in q:
            canonical = _canonical_region(name) or name
            if canonical not in matches:
                matches.append(canonical)
    return matches[0] if len(matches) == 1 else None


def _extract_single_year_from_query(query: str) -> Optional[str]:
    years = list(dict.fromkeys(re.findall(r"(19\d{2}|20\d{2})", str(query or ""))))
    return years[0] if len(years) == 1 else None


def _quick_stat_unsupported_dimensions(query: str) -> list[str]:
    q = str(query or "")
    compact = _compact_text(q)
    dimensions: list[str] = []
    if (
        re.search(r"\d+\s*[-~]\s*\d+\s*세", q)
        or re.search(r"\d{2,3}\s*대", q)
        or any(term in compact for term in ("청년", "연령별", "연령", "나이"))
    ):
        dimensions.append("age")
    if any(term in compact for term in ("여성", "여자", "남성", "남자", "성별")):
        dimensions.append("gender")
    if any(term in compact for term in ("추이", "시계열", "최근10년", "최근5년", "팬데믹기간")):
        dimensions.append("time_series")
    if any(term in compact for term in ("vs", "대비", "비교")):
        dimensions.append("comparison")
    if any(region in compact for region in (_compact_text(name) for name in REGION_COMPOSITES)):
        dimensions.append("region_group")
    return list(dict.fromkeys(dimensions))


def _unsupported_quick_stat_response(
    query: str,
    param: QuickStatParam,
    dimensions: list[str],
    region: str,
    period: str,
) -> dict[str, Any]:
    return {
        "상태": "failed",
        "코드": STATUS_UNVERIFIED_FORMULA,
        "status": "unsupported",
        "이행_상태": "unsupported",
        "질문": query,
        "answer": (
            "quick_stat은 단일 통계값 도구라 질문에 포함된 추가 필터를 안전하게 반영하지 못합니다. "
            "기본값으로 대체하지 않고 중단했습니다."
        ),
        "통계표": param.tbl_nm,
        "통계표ID": param.tbl_id,
        "기관ID": param.org_id,
        "요청_지역": region,
        "요청_기간": period,
        "누락_차원": dimensions,
        "dropped_dimensions": dimensions,
        "권고": [
            f"explore_table('{param.org_id}', '{param.tbl_id}')로 분류축을 확인하세요.",
            "연령·성별·권역·시계열 조건은 raw 다축 호출 도구가 필요합니다.",
        ],
    }


def _attach_ignored_params(result: Any, ignored: list[str], context: str) -> Any:
    """Attach unsupported-parameter warning to a quick-stat-like response."""
    if not ignored or not isinstance(result, dict):
        return result
    result["⚠️ 무시된_파라미터"] = ignored
    result["⚠️ 무시된_파라미터_안내"] = (
        f"{context} 함수는 정해진 파라미터(query/region/period 등)만 받습니다. "
        "industry, scale, sector 등 추가 슬라이싱은 자연어 query에 키워드를 "
        "포함하거나 search_kosis로 통계표 ID를 먼저 확인해야 합니다."
    )
    return result
