"""
KOSIS MCP — 조회 + 분석 + 시각화 통합 서버
==========================================

3계층 설계:
  L1 Quick    — 사용자 질의 90% 처리 (큐레이션 + 폴백)
  L2 Analysis — 회귀/상관/분포/예측/이상치 (scipy)
  L3 Viz      — 실제 SVG 차트 (MCP image content 반환)
  Chain       — L1+L2+L3 종합 워크플로우

설치:
    pip install "mcp[cli]" httpx scipy numpy

환경변수:
    KOSIS_API_KEY=발급받은_인증키
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import httpx
import numpy as np
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent
from scipy import stats as scipy_stats

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ============================================================================
# 상수
# ============================================================================

KOSIS_BASE = "https://kosis.kr/openapi"
API_KEY_DEFAULT = os.environ.get("KOSIS_API_KEY", "")
HTTP_TIMEOUT = 30.0

ERROR_MAP = {
    # Official KOSIS API error codes
    "10": "인증키 누락",
    "11": "인증키 만료",
    "20": "필수 변수 누락",
    "21": "잘못된 변수",
    "30": "결과 없음",
    "31": "결과 초과 (4만셀)",
    "40": "호출 제한",
    "41": "ROW 제한",
    "42": "사용자별 이용 제한 — KOSIS 관리자에게 문의 필요",
    "50": "서버 오류",
    # Common non-official codes observed in the wild
    "E001": "내부 오류 (E001) — KOSIS 공식 코드가 아닌 래퍼/네트워크 레이어 실패. 재시도 후에도 반복되면 통계표 변경 또는 차단된 파라미터 의심",
    "E002": "내부 오류 (E002) — 응답 파싱 실패 가능성",
    "INVALID_PARAM": "잘못된 파라미터 — 요청 변수(통계표 ID, 분류값, 기간) 형식 또는 조합이 KOSIS 검증을 통과하지 못함",
    "INVALID_KEY": "잘못된 인증키 — KOSIS_API_KEY 값을 다시 확인",
    "MISSING_KEY": "인증키 미설정 — KOSIS_API_KEY 환경변수가 비어 있음",
    "TIMEOUT": "요청 타임아웃 — 네트워크 또는 KOSIS 서버 응답 지연",
    "NETWORK": "네트워크 오류 — DNS/연결/SSL 실패",
    "-1": "일반 실패 — 구체적 사유가 응답에 포함되지 않음. KOSIS 일시 장애 또는 알 수 없는 클라이언트 오류",
    "0": "성공이지만 데이터 없음 — 정상 호출이나 매칭 행이 0건",
}

STATUS_EXECUTED = "EXECUTED"
STATUS_NEEDS_TABLE_SELECTION = "NEEDS_TABLE_SELECTION"
STATUS_STAT_NOT_FOUND = "STAT_NOT_FOUND"
STATUS_PERIOD_NOT_FOUND = "PERIOD_NOT_FOUND"
STATUS_PERIOD_RANGE_REQUESTED = "PERIOD_RANGE_REQUESTED"
STATUS_UNVERIFIED_FORMULA = "UNVERIFIED_FORMULA"
STATUS_DENOMINATOR_REQUIRED = "DENOMINATOR_REQUIRED"
STATUS_RUNTIME_ERROR = "RUNTIME_ERROR"

FORMULA_DEPENDENCIES: dict[str, dict[str, Any]] = {
    "share_ratio": {
        "canonical": "비중/구성비",
        "aliases": ["비중", "비율", "구성비", "차지하는 비중"],
        "formula": "부분 / 전체 * 100",
        "required_stats": ["분자 지표", "분모 지표"],
        "checks": ["분모 정의", "동일 기준시점", "동일 모집단", "단위 정합성"],
        "caution": "서울 소상공인 비중처럼 분모가 전국인지 서울 전체 사업체인지 반드시 확인해야 합니다.",
    },
    "growth_rate": {
        "canonical": "증가율/변화율",
        "aliases": ["증가율", "감소율", "변화율", "전년 대비", "전월 대비"],
        "formula": "(현재값 - 비교시점값) / 비교시점값 * 100",
        "required_stats": ["현재 시점 값", "비교 시점 값"],
        "checks": ["비교 시점", "0 또는 음수 기준값", "연/월/분기 주기"],
        "caution": "증감은 절대 차이이고 증가율은 기준값 대비 비율입니다.",
    },
    "average_workers_per_business": {
        "canonical": "사업체당 평균 종사자 수",
        "aliases": ["사업체당 평균 종사자", "기업당 평균 고용", "평균 고용 인원"],
        "formula": "종사자 수 / 사업체 수",
        "required_stats": ["종사자 수", "사업체 수"],
        "checks": ["기업체 기준과 사업체 기준 혼합 여부", "동일 기준시점", "동일 대상 범위"],
        "caution": "사업체 수와 기업 수는 집계 단위가 다르므로 평균 산식에 섞으면 안 됩니다.",
    },
    "unemployment_rate": {
        "canonical": "실업률",
        "aliases": ["실업률", "청년 실업률", "고령층 실업률", "여성 실업률", "남성 실업률"],
        "formula": "실업자 수 / 경제활동인구 * 100",
        "required_stats": ["실업자 수", "경제활동인구"],
        "checks": ["대상군(연령·성별) 일치", "경제활동인구 분모", "동일 기준시점"],
        "caution": "청년·여성 등 부분군 실업률은 분자와 분모를 같은 대상군으로 제한해야 합니다.",
    },
    "closure_rate": {
        "canonical": "폐업률",
        "aliases": ["폐업률", "폐업 비율", "망한 가게 비율"],
        "formula": "폐업 수 / 기준 사업체 수 * 100",
        "required_stats": ["폐업 수", "기준 사업체 수"],
        "checks": ["작성기관 산식", "분모 기준", "기간 기준"],
        "caution": "실제 폐업률 산식은 작성기관 기준을 우선해야 합니다.",
    },
    "startup_rate": {
        "canonical": "창업률",
        "aliases": ["창업률", "창업 비율", "개업률"],
        "formula": "창업 수 / 기준 사업체 수 * 100",
        "required_stats": ["창업 수", "기준 사업체 수"],
        "checks": ["분모 기준", "신설/창업 정의", "기간 기준"],
        "caution": "신설 법인, 창업기업, 사업자등록 신규 등 출처별 정의가 다릅니다.",
    },
    "survival_rate": {
        "canonical": "생존율",
        "aliases": ["생존율", "살아남은 비율", "3년 생존율", "5년 생존율"],
        "formula": "생존 기업 수 / 창업 기업 수 * 100",
        "required_stats": ["창업 코호트", "생존 기업 수"],
        "checks": ["코호트 기준", "1년/3년/5년 기간", "폐업 정의"],
        "caution": "생존율은 특정 창업연도 코호트 기준인지 확인해야 합니다.",
    },
    "loan_to_sales": {
        "canonical": "매출 대비 대출 비중",
        "aliases": ["매출 대비 대출", "대출 부담", "금융 부담"],
        "formula": "대출 잔액 / 매출액 * 100",
        "required_stats": ["대출 잔액", "매출액"],
        "checks": ["잔액/신규대출 구분", "명목 금액 단위", "업종·지역 기준 일치"],
        "caution": "대출은 금융권·정책자금·보증 실적 등 출처별 포괄범위가 다릅니다.",
    },
    "net_startup": {
        "canonical": "순창업/순증가",
        "aliases": ["순창업", "순증가", "창업 폐업 차이"],
        "formula": "창업 수 - 폐업 수",
        "required_stats": ["창업 수", "폐업 수"],
        "checks": ["기간 일치", "업종·지역 기준 일치", "창업/폐업 정의"],
        "caution": "순창업은 생존율이나 실제 고용 증가를 직접 의미하지 않습니다.",
    },
}

# ============================================================================
# 큐레이션 데이터 — kosis_curation 모듈에서 로드
# ============================================================================

from kosis_curation import (
    QuickStatParam,
    REGION_COMPOSITES,
    REGION_DEMOGRAPHIC,
    TIER_A_STATS,
    TOPICS,
    canonical_region as _canonical_region,
    lookup as _curation_lookup,
    route_query as _route_query,
    routing_hints as _routing_hints,
    topic_hints as _topic_hints,
    stats_summary as _curation_stats_summary,
)

# Phase 2 추가 차트 4종 (별도 모듈)
from kosis_charts_extra import (
    chart_heatmap_svg,
    chart_distribution_svg,
    chart_dual_axis_svg,
    chart_dashboard_svg,
)




# ============================================================================
# 헬퍼
# ============================================================================

def _resolve_key(provided: Optional[str]) -> str:
    key = provided or API_KEY_DEFAULT
    if not key:
        raise RuntimeError("KOSIS_API_KEY 설정 필요")
    return key


def _lookup_quick(query: str) -> Optional[QuickStatParam]:
    """큐레이션 모듈에 위임 (Tier A 정밀 매핑 + 동의어 + 부분 일치)."""
    return _curation_lookup(query)


async def _kosis_call(client: httpx.AsyncClient, endpoint: str, params: dict) -> list[dict]:
    url = f"{KOSIS_BASE}/{endpoint}"
    clean = {k: v for k, v in params.items() if v not in (None, "")}
    resp = await client.get(url, params=clean, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "err" in data:
        code = str(data["err"])
        if code == "30":
            return []
        raise RuntimeError(f"[KOSIS {code}] {ERROR_MAP.get(code, '미상')}")
    return data if isinstance(data, list) else [data]


async def _fetch_meta(
    client: httpx.AsyncClient,
    api_key: str,
    org_id: str,
    tbl_id: str,
    meta_type: str,
    extra_params: Optional[dict] = None,
) -> list[dict]:
    """Generic KOSIS `getMeta` call. meta_type values from the dev guide:

    TBL   — table name (국문/영문)
    ORG   — owning organization name
    PRD   — recorded period summary (start / end / 주기)
    ITM   — classification (objL*) and item (itmId) catalog with names
            and units — the dynamic alternative to hard-coding industry
            codes into TIER_A_STATS
    CMMT  — annotations attached to the table
    UNIT  — unit registry
    SOURCE — survey contact information
    WGT   — weighting metadata
    NCD   — last-updated timestamp per period
    """
    params: dict[str, Any] = {
        "method": "getMeta",
        "type": meta_type,
        "apiKey": api_key,
        "orgId": org_id,
        "tblId": tbl_id,
        "format": "json",
        "jsonVD": "Y",
    }
    if extra_params:
        params.update(extra_params)
    return await _kosis_call(client, "statisticsData.do", params)


async def _fetch_classifications(
    org_id: str, tbl_id: str, api_key: Optional[str] = None,
) -> list[dict]:
    """Return every (OBJ_ID, OBJ_NM, ITM_ID, ITM_NM, UNIT_NM) tuple for
    a KOSIS table. Lets dispatchers map "제조업" → the right obj_l2
    code without baking industry codes into curation."""
    key = _resolve_key(api_key)
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "ITM")


async def _fetch_period_range(
    org_id: str, tbl_id: str,
    api_key: Optional[str] = None,
    detail: bool = False,
) -> list[dict]:
    """Return PRD_SE / STRT_PRD_DE / END_PRD_DE for a KOSIS table.

    Many tables expose multiple cadences (month/quarter/year) and the
    meta endpoint returns one row per cadence — use `_pick_finest_period`
    on the result to select the freshest cadence row, since `[-1]` is
    not guaranteed to be the most granular one.

    detail=True asks for every recorded timepoint (large response on
    monthly tables); detail=False asks only for the summary row (cheap,
    what staleness checks need)."""
    key = _resolve_key(api_key)
    extra = {"detail": "Y"} if detail else None
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "PRD", extra)


async def _fetch_table_name(
    org_id: str, tbl_id: str, api_key: Optional[str] = None,
) -> list[dict]:
    """Return TBL_NM / TBL_NM_ENG for a KOSIS table."""
    key = _resolve_key(api_key)
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "TBL")


# KOSIS PRD meta returns one row per cadence — pick the finest one so
# staleness checks reflect the most granular data available.
# Korean: 월 > 분기 > 반기 > 년 / English aliases: M Q H Y.
_PRD_FINENESS = {
    "월": 0, "M": 0, "MM": 0,
    "분기": 1, "Q": 1, "QQ": 1,
    "반기": 2, "H": 2, "HF": 2,
    "년": 3, "연": 3, "Y": 3, "A": 3,
}


def _pick_finest_period(period_rows: list[dict]) -> Optional[dict]:
    """Return the PRD row with the finest cadence (월 > 분기 > 반기 >
    년). Returns None for an empty / falsy list."""
    if not period_rows:
        return None
    def rank(row: dict) -> int:
        se = str(row.get("PRD_SE") or row.get("prdSe") or "").strip()
        return _PRD_FINENESS.get(se, 99)
    return sorted(period_rows, key=rank)[0]


def _period_type(row: Optional[dict]) -> Optional[str]:
    if not row:
        return None
    value = str(row.get("PRD_SE") or row.get("prdSe") or "").strip()
    return value or None


def _is_yearly_period_type(period_type: Optional[str]) -> bool:
    return str(period_type or "").strip() in {"Y", "A", "\ub144", "year", "annual"}


def _api_period_type(period_type: Optional[str]) -> Optional[str]:
    value = str(period_type or "").strip()
    aliases = {
        "\uc6d4": "M",
        "M": "M",
        "MM": "M",
        "\ubd84\uae30": "Q",
        "Q": "Q",
        "QQ": "Q",
        "\ubc18\uae30": "H",
        "H": "H",
        "HF": "H",
        "\ub144": "Y",
        "\uc5f0": "Y",
        "Y": "Y",
        "A": "Y",
    }
    return aliases.get(value, value or None)


def _period_range_looks_yearly(period_range: Optional[list[str]]) -> bool:
    if not period_range:
        return False
    bounds = [str(p).strip() for p in period_range if str(p or "").strip()]
    return bool(bounds) and all(re.fullmatch(r"\d{4}", p) for p in bounds)


def _pick_query_table_period_row(
    period_rows: list[dict],
    period_range: Optional[list[str]],
) -> Optional[dict]:
    if not period_rows:
        return None
    if _period_range_looks_yearly(period_range):
        for row in period_rows:
            if _is_yearly_period_type(_period_type(row)):
                return row
    return _pick_finest_period(period_rows)


def _resolve_classification_term(
    term: str,
    classifications: list[dict],
    obj_id: Optional[str] = None,
) -> Optional[dict]:
    """Find the KOSIS classification entry whose ITM_NM matches `term`.

    Returns the full row (including ITM_ID for use as objL*/itmId in
    quick_stat) or None when no match is found. Pass obj_id to narrow
    to a specific classification axis (e.g. an industry axis vs a region
    axis on the same table). Prefer exact normalized labels after removing
    KOSIS code/range adornments (e.g. "C.제조업(10~34)" -> "제조업")
    before falling back to substring matches."""
    if not term:
        return None
    normalized = re.sub(r"\s+", "", term).lower()

    def normalize_label(value: Any) -> str:
        return re.sub(r"\s+", "", str(value or "")).lower()

    def canonical_label(value: Any) -> str:
        label = normalize_label(value)
        label = re.sub(r"^[a-z]\.", "", label)
        label = re.sub(r"^\d+[.)]?", "", label)
        label = re.sub(r"\([^)]*\)", "", label)
        return label.strip()

    def is_parent(row: dict) -> bool:
        return not str(row.get("UP_ITM_ID") or "").strip()

    def score(row: dict) -> Optional[tuple[int, int, int, str]]:
        itm_id = normalize_label(row.get("ITM_ID"))
        label = normalize_label(row.get("ITM_NM"))
        canonical = canonical_label(row.get("ITM_NM"))
        if not label and not itm_id:
            return None

        parent_bonus = 0 if is_parent(row) else 1
        if itm_id == normalized:
            rank = 0
        elif canonical == normalized:
            rank = 1
        elif label == normalized:
            rank = 2
        elif canonical.startswith(normalized):
            rank = 3
        elif label.startswith(normalized):
            rank = 4
        elif normalized in canonical:
            rank = 5
        elif normalized in label:
            rank = 6
        else:
            return None
        return (rank, parent_bonus, len(canonical or label), label)

    candidates = classifications
    if obj_id:
        candidates = [c for c in classifications if str(c.get("OBJ_ID") or "") == str(obj_id)]
    matches: list[tuple[tuple[int, int, int, str], dict]] = []
    for row in candidates:
        row_score = score(row)
        if row_score is not None:
            matches.append((row_score, row))
    if not matches:
        return None
    matches.sort(key=lambda pair: pair[0])
    return matches[0][1]


def _kosis_view_url(org_id: str, tbl_id: str) -> str:
    return f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}"


def _format_number(v: Any) -> str:
    try:
        n = float(v)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.3f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        return str(v)


def _format_display_number(v: Any, decimals: Optional[int] = None) -> str:
    if decimals is None:
        return _format_number(v)
    try:
        return f"{float(v):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(v)


def _compact_text(text: str) -> str:
    return re.sub(r"[\s_\-·/()]+", "", str(text)).lower()


def _parse_year_token(text: str) -> Optional[str]:
    if not text:
        return None
    year_now = datetime.now().year
    m = re.match(r"^(\d{4})", text.strip())
    if m:
        return m.group(1)
    if "재작년" in text:
        return str(year_now - 2)
    if any(t in text for t in ("작년", "지난해", "전년")):
        return str(year_now - 1)
    if any(t in text for t in ("올해", "금년")):
        return str(year_now)
    return None


def _is_latest_period_text(text: Any) -> bool:
    if text is None:
        return True
    compact = re.sub(r"\s+", "", str(text)).lower()
    return compact in {
        "",
        "latest",
        "최근",
        "최신",
        "가장최근",
        "제일최근",
        "최근값",
        "최신값",
        "최신치",
        "최근시점",
        "최신시점",
        "현재",
    }


def _relative_year(compact_text: str) -> Optional[int]:
    """Resolve 재작년/작년/올해/금년 references to an absolute year."""
    if not compact_text:
        return None
    now = datetime.now().year
    if "재작년" in compact_text:
        return now - 2
    if any(t in compact_text for t in ("작년", "지난해", "전년")):
        return now - 1
    if any(t in compact_text for t in ("올해", "금년", "이번해", "당해")):
        return now
    return None


def _current_quarter() -> int:
    return (datetime.now().month - 1) // 3 + 1


def _parse_month_token(text: str) -> Optional[str]:
    if not text:
        return None
    compact = re.sub(r"\s+", "", str(text))
    m = re.search(r"(19\d{2}|20\d{2})(?:[.\-/]|년)?(0?[1-9]|1[0-2])월?", compact)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}"
    # Relative-year + month: "올해 4월", "작년 12월"
    rel_year = _relative_year(compact)
    if rel_year is not None:
        month_m = re.search(r"(0?[1-9]|1[0-2])월", compact)
        if month_m:
            return f"{rel_year}{int(month_m.group(1)):02d}"
        if "이번달" in compact or "금월" in compact:
            return f"{rel_year}{datetime.now().month:02d}"
        if "지난달" in compact or "전월" in compact:
            now = datetime.now()
            month = now.month - 1
            year = now.year
            if month == 0:
                month = 12
                year -= 1
            return f"{year}{month:02d}"
    if "이번달" in compact or "금월" in compact:
        return f"{datetime.now().year}{datetime.now().month:02d}"
    if "지난달" in compact or "전월" in compact:
        now = datetime.now()
        month = now.month - 1
        year = now.year
        if month == 0:
            month = 12
            year -= 1
        return f"{year}{month:02d}"
    return None


def _parse_quarter_token(text: str) -> Optional[str]:
    if not text:
        return None
    compact = re.sub(r"\s+", "", str(text)).upper()
    m = re.search(r"(19\d{2}|20\d{2})(?:년)?([1-4])분기", compact)
    if not m:
        m = re.search(r"(19\d{2}|20\d{2})Q([1-4])", compact)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Relative-year + quarter: "올해 1분기", "작년 4분기"
    rel_year = _relative_year(compact)
    if rel_year is not None:
        q_m = re.search(r"([1-4])분기|Q([1-4])", compact)
        if q_m:
            quarter = q_m.group(1) or q_m.group(2)
            return f"{rel_year}{quarter}"
        if "이번분기" in compact or "당분기" in compact:
            return f"{rel_year}{_current_quarter()}"
    # Bare 이번 분기 / 지난 분기 without explicit year defaults to now
    if "이번분기" in compact or "당분기" in compact:
        return f"{datetime.now().year}{_current_quarter()}"
    if "지난분기" in compact or "전분기" in compact:
        q = _current_quarter() - 1
        year = datetime.now().year
        if q == 0:
            q = 4
            year -= 1
        return f"{year}{q}"
    return None


def _detect_half_year_request(text: str) -> Optional[str]:
    """Return an advisory string when 상반기/하반기 keywords appear.

    KOSIS does not publish 상반기/하반기 aggregates as first-class
    periods, so we cannot honor them as a period code. Surface an
    explicit message instead of silently dropping to year."""
    if not text:
        return None
    compact = re.sub(r"\s+", "", str(text))
    if "상반기" in compact:
        return "상반기는 KOSIS 표준 주기에 없음 — 1분기·2분기를 따로 조회하거나 월별 누계 사용"
    if "하반기" in compact:
        return "하반기는 KOSIS 표준 주기에 없음 — 3분기·4분기를 따로 조회하거나 월별 누계 사용"
    return None


def _period_bounds(period: str, period_type: str) -> tuple[Optional[str], Optional[str]]:
    """Convert natural period text into KOSIS start/end period codes."""
    if _is_latest_period_text(period):
        return None, None

    # Parse finer-grained tokens first even when the target table is annual.
    # That lets callers surface a precision-downgrade trail for natural
    # periods such as "이번 분기" instead of treating them as unparseable.
    quarter = _parse_quarter_token(period)
    month = None if quarter else _parse_month_token(period)

    if period_type == "M":
        if month:
            return month, month
        if quarter:
            year, q = int(quarter[:4]), int(quarter[4])
            start_month = (q - 1) * 3 + 1
            return f"{year}{start_month:02d}", f"{year}{start_month + 2:02d}"

    if period_type == "Q":
        if quarter:
            return quarter, quarter
        if month:
            year, mon = int(month[:4]), int(month[4:])
            return f"{year}{((mon - 1) // 3) + 1}", f"{year}{((mon - 1) // 3) + 1}"

    year = _parse_year_token(period)
    if not year and quarter:
        year = quarter[:4]
    if not year and month:
        year = month[:4]
    if not year:
        return None, None
    if period_type == "M":
        return f"{year}01", f"{year}12"
    if period_type == "Q":
        return f"{year}1", f"{year}4"
    return year, year


def _detect_precision_downgrade(period: str, period_type: str) -> Optional[str]:
    """Return a warning string when the requested period asks for finer
    granularity (month/quarter) than the table supports.

    Without this signal, _period_bounds silently falls back to the year
    component of a quarter/month token, so the caller never learns that
    a "2025Q1" request was answered with 2025 annual data."""
    if not period or period == "latest":
        return None
    text = str(period)
    # Quarter takes precedence — "2025년 1분기" must not be misread as
    # January via the month regex.
    has_explicit_quarter = bool(re.search(r"(분기|Q[1-4])", text, re.IGNORECASE))
    has_quarter = has_explicit_quarter or _parse_quarter_token(period) is not None
    has_month = (not has_explicit_quarter) and _parse_month_token(period) is not None
    if has_quarter and period_type not in {"Q", "M"}:
        return (
            f"분기 정밀도 요청({period}) → 이 통계표는 {period_type} 주기만 지원하므로 "
            "연 단위로 응답 — 분기 차이는 반영되지 않음"
        )
    if has_month and period_type != "M":
        return (
            f"월 정밀도 요청({period}) → 이 통계표는 {period_type} 주기만 지원하므로 "
            "연 단위로 응답 — 월 차이는 반영되지 않음"
        )
    return None


def _extract_year_range(text: str) -> tuple[Optional[str], Optional[str]]:
    """Extract (start_year, end_year) from explicit comparison phrasing.

    Recognizes patterns like "2019년 대비 2023년", "2019~2023", "2019년부터 2023년".
    Returns (None, None) when fewer than two plausible 4-digit years are present.
    Order follows text order so "2019 대비 2023" → start=2019, end=2023.
    """
    if not text:
        return None, None
    years = re.findall(r"(19\d{2}|20\d{2})", text)
    if len(years) < 2:
        return None, None
    start, end = years[0], years[1]
    if start == end:
        return None, None
    return start, end


def _extract_open_start_year(text: str) -> Optional[str]:
    """Extract a single start year from open-ended range phrases.

    "2020년부터", "2020년 이후", "since 2020" ask for a series, not a
    single period. Two-year ranges are handled by _extract_year_range.
    """
    if not text:
        return None
    years = re.findall(r"(19\d{2}|20\d{2})", str(text))
    if len(years) != 1:
        return None
    compact = re.sub(r"\s+", "", str(text))
    year = years[0]
    if re.search(rf"{year}년?(?:부터|이후|이래|이뒤)", compact):
        return year
    if re.search(rf"(?:since|from){year}", compact, re.IGNORECASE):
        return year
    return None


def _periods_per_year(period_type: str) -> int:
    if period_type == "M":
        return 12
    if period_type == "Q":
        return 4
    return 1


def _latest_count_for_years(years: int, period_type: str) -> int:
    return max(1, int(years or 1)) * _periods_per_year(period_type)


def _default_period_type(param: QuickStatParam) -> str:
    periods = tuple(getattr(param, "supported_periods", ()) or ("Y",))
    if "Y" in periods:
        return "Y"
    return periods[0]


def _format_period_label(period: Any, period_type: str) -> str:
    text = str(period or "")
    if period_type == "M" and len(text) >= 6:
        return f"{text[:4]}.{text[4:6]}"
    if period_type == "Q" and len(text) >= 5:
        return f"{text[:4]}년 {text[-1]}분기"
    if len(text) >= 4:
        return f"{text[:4]}년"
    return text


def _format_aggregated_dt(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:.10f}".rstrip("0").rstrip(".")


async def _fetch_series(
    client: httpx.AsyncClient, key: str, param: QuickStatParam,
    region_code: Optional[str], period_type: str = "Y",
    start_year: Optional[str] = None, end_year: Optional[str] = None,
    latest_n: Optional[int] = None,
) -> list[dict]:
    p = {
        "method": "getList", "apiKey": key,
        "orgId": param.org_id, "tblId": param.tbl_id,
        "objL1": param.obj_l1, "itmId": param.item_id,
        "prdSe": period_type, "format": "json", "jsonVD": "Y",
    }
    if param.obj_l2:
        p["objL2"] = param.obj_l2
    if getattr(param, "obj_l3", None):
        p["objL3"] = param.obj_l3
    if region_code:
        region_obj = getattr(param, "region_obj", "obj_l1")
        if region_obj == "obj_l1":
            p["objL1"] = region_code
        elif region_obj == "obj_l2":
            p["objL2"] = region_code
        elif region_obj == "obj_l3":
            p["objL3"] = region_code
    if start_year and end_year:
        p["startPrdDe"] = start_year
        p["endPrdDe"] = end_year
    elif latest_n:
        p["newEstPrdCnt"] = str(latest_n)

    obj_l2_list = getattr(param, "obj_l2_list", ()) or ()
    if obj_l2_list and getattr(param, "aggregation", None) == "sum":
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        base_rows: dict[str, dict] = {}

        for obj_l2 in obj_l2_list:
            component_params = dict(p)
            component_params["objL2"] = obj_l2
            rows = await _kosis_call(client, "Param/statisticsParameterData.do", component_params)
            for row in rows:
                period = row.get("PRD_DE") or row.get("시점")
                raw_value = row.get("DT") or row.get("값")
                if not period or raw_value in (None, ""):
                    continue
                try:
                    value = float(str(raw_value).replace(",", ""))
                except ValueError:
                    continue
                sums[period] = sums.get(period, 0.0) + value
                counts[period] = counts.get(period, 0) + 1
                base_rows.setdefault(period, dict(row))

        aggregated: list[dict] = []
        for period in sorted(sums):
            if counts.get(period) != len(obj_l2_list):
                continue
            row = base_rows[period]
            row["DT"] = _format_aggregated_dt(sums[period])
            row["UNIT_NM"] = param.unit
            row["ITM_NM"] = param.description
            row["C2"] = ",".join(obj_l2_list)
            row["C2_NM"] = "합산"
            row["_AGGREGATION"] = "sum"
            row["_COMPONENT_OBJ_L2"] = list(obj_l2_list)
            aggregated.append(row)
        return aggregated

    return await _kosis_call(client, "Param/statisticsParameterData.do", p)


def _values_from_series(series: list[dict]) -> tuple[list[str], list[float]]:
    pairs = []
    for r in series:
        try:
            t = r.get("시점") or r.get("PRD_DE")
            v = float(r.get("값") or r.get("DT"))
            if t:
                pairs.append((t, v))
        except (ValueError, TypeError):
            continue
    pairs.sort(key=lambda p: p[0])
    return [p[0] for p in pairs], [p[1] for p in pairs]


def _region_field_names(param: QuickStatParam) -> tuple[str, str]:
    region_obj = getattr(param, "region_obj", "obj_l1")
    if region_obj == "obj_l2":
        return "C2", "C2_NM"
    if region_obj == "obj_l3":
        return "C3", "C3_NM"
    return "C1", "C1_NM"


@dataclass
class AnswerStat:
    key: str
    label: str
    value: float
    formatted: str
    unit: str
    period: str
    table: str
    region: str
    source: str = "통계청 KOSIS"

    def to_row(self) -> dict[str, Any]:
        return {
            "지표": self.label,
            "값": self.formatted,
            "원값": self.value,
            "단위": self.unit,
            "시점": self.period,
            "지역": self.region,
            "통계표": self.table,
            "출처": self.source,
        }


@dataclass(frozen=True)
class AnswerPlan:
    """Execution decision for answer_query.

    The planner decides *what* should run. The engine still owns the actual
    KOSIS calls and rendering so this first refactor stays behavior-preserving.
    """
    action: str
    region: str
    direct_key: Optional[str] = None
    params: dict[str, Any] = field(default_factory=dict)


class AnswerPlanner:
    """Turn a routed natural-language query into one executable action."""

    def __init__(self, engine: "NaturalLanguageAnswerEngine") -> None:
        self.engine = engine

    def build(self, query: str, region: str, route_payload: dict[str, Any]) -> AnswerPlan:
        effective_region = self.engine._effective_region(route_payload, region)
        direct_key = self.engine._infer_direct_stat_key(query, route_payload)

        if self.engine._is_self_employed_sme_population_question(query):
            return AnswerPlan("mixed_population", effective_region)
        if self.engine._is_sme_large_sales_question(query):
            return AnswerPlan("sme_large_sales", effective_region)
        if self.engine._is_sme_smallbiz_count_question(query):
            return AnswerPlan("sme_smallbiz_counts", effective_region)
        if self.engine._is_sme_employee_average_question(query):
            return AnswerPlan("sme_employee_average", effective_region)
        if self.engine._is_dynamic_ratio_question(query):
            return AnswerPlan("dynamic_ratio", effective_region)

        if self.engine._needs_advanced_analysis_plan(route_payload):
            intents = route_payload.get("intents") or []
            if "STAT_CORRELATION" in intents:
                stat_x, stat_y = self.engine._extract_correlation_pair(query)
                if stat_x and stat_y:
                    return AnswerPlan(
                        "auto_correlate",
                        effective_region,
                        params={"stat_x": stat_x, "stat_y": stat_y},
                    )
            return AnswerPlan("search_fallback", effective_region)

        if direct_key:
            composites = self.engine._extract_composite_regions(query)
            if composites:
                operation = (
                    "share"
                    if self.engine._is_share_ratio_question(query, route_payload)
                    else "sum"
                )
                return AnswerPlan(
                    "composite_aggregate",
                    effective_region,
                    direct_key,
                    {"composite": composites[0], "operation": operation},
                )

            if self.engine._is_aggregation_question(query):
                extras = self.engine._extract_extra_regions(query, effective_region, route_payload)
                regions = [effective_region] + [r for r in extras if r != effective_region]
                if len(regions) >= 2:
                    return AnswerPlan("region_sum", effective_region, direct_key, {"regions": regions})

            if self.engine._is_top_n_question(query, route_payload):
                return AnswerPlan(
                    "top_n",
                    effective_region,
                    direct_key,
                    {
                        "top_n": self.engine._extract_top_n(query) or 5,
                        "include_share_ratio": self.engine._is_share_ratio_question(query, route_payload),
                    },
                )

            if self.engine._is_region_compare_question(query):
                return AnswerPlan("region_compare", effective_region, direct_key)

            if (
                self.engine._is_share_ratio_question(query, route_payload)
                and effective_region != "전국"
                and not self.engine._is_growth_question(query, route_payload)
            ):
                return AnswerPlan("share_ratio", effective_region, direct_key)

            return AnswerPlan("direct", effective_region, direct_key)

        return AnswerPlan("search_fallback", effective_region)


@dataclass(frozen=True)
class WorkflowStep:
    step: int
    tool: str
    purpose: str
    args: dict[str, Any]
    available_now: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "tool": self.tool,
            "purpose": self.purpose,
            "args": self.args,
            "available_now": self.available_now,
        }


class QueryWorkflowPlanner:
    """Plan a safe procedural workflow for a statistical question.

    MUST NOT:
    - choose a final KOSIS table ID; that belongs to select_table_for_query
    - map natural concepts to ITM_ID/OBJ_ID codes; that belongs to resolve_concepts
    - fetch actual statistical values; that belongs to query_table
    - calculate ratios, sums, growth rates, or per-capita values; that belongs to compute_indicator

    This class extracts intent, dimensions, and the next tool sequence only.
    """

    AVAILABLE_TOOLS = {"plan_query", "search_kosis", "explore_table", "query_table"}
    EN_REGION_ALIASES = {
        "seoul": "서울", "busan": "부산", "daegu": "대구", "incheon": "인천",
        "gwangju": "광주", "daejeon": "대전", "ulsan": "울산", "sejong": "세종",
        "gyeonggi": "경기", "gangwon": "강원", "chungbuk": "충북", "chungnam": "충남",
        "jeonbuk": "전북", "jeonnam": "전남", "gyeongbuk": "경북", "gyeongnam": "경남",
        "jeju": "제주", "korea": "전국", "national": "전국",
    }
    EN_INDICATOR_ALIASES = {
        "unemployment rate": "실업률",
        "employment rate": "고용률",
        "population": "인구",
        "cpi": "소비자물가지수",
        "consumer price index": "소비자물가지수",
        "grdp": "GRDP",
        "gdp": "GDP",
    }

    def build(self, query: str) -> dict[str, Any]:
        route_payload = NaturalLanguageAnswerEngine._route_payload(query)
        dimensions = self._dimensions(query, route_payload)
        calculations = self._calculations(query, route_payload)
        intent = self._intent(route_payload, calculations)
        concepts = self._concepts(dimensions, calculations)
        required = self._required_dimensions(dimensions, calculations)

        confidence = "medium"
        if dimensions.get("indicator") and required:
            confidence = "high"
        elif route_payload.get("route", {}).get("type") == "miss":
            confidence = "low"
        if self._needs_clarification(dimensions, concepts, calculations, route_payload):
            return self._clarification_response(query, dimensions, concepts, calculations, route_payload)

        workflow = self._workflow(query, intent, required, concepts, dimensions, calculations)

        return {
            "상태": "planned",
            "status": "planned",
            "answer": None,
            "intent": intent,
            "query": query,
            "verification_level": "planning_only",
            "confidence": confidence,
            "intended_dimensions": dimensions,
            "required_dimensions": required,
            "concepts": concepts,
            "calculations": calculations,
            "suggested_workflow": [step.to_dict() for step in workflow],
            "next_call": workflow[0].to_dict() if workflow else None,
            "route": route_payload.get("route", {}),
            "router_slots": route_payload.get("slots", {}),
            "validation": route_payload.get("validation", {}),
            "must_not": [
                "통계표 ID 확정 금지",
                "ITM_ID/OBJ_ID 코드 매핑 금지",
                "실제 값 반환 금지",
                "산술·산식 계산 금지",
            ],
            "notes": [
                "plan_query는 절차형 레일만 생성합니다. 값 조회와 계산은 후속 도구가 담당합니다.",
                "available_now=false인 도구는 후속 PR에서 추가될 예정인 파이프라인 단계입니다.",
            ],
        }

    @staticmethod
    def _needs_clarification(
        dimensions: dict[str, Any],
        concepts: list[str],
        calculations: list[str],
        route_payload: dict[str, Any],
    ) -> bool:
        route = route_payload.get("route") or {}
        if route.get("type") != "miss":
            return False
        has_statistical_anchor = bool(
            dimensions.get("indicator")
            or dimensions.get("event")
            or dimensions.get("industry")
            or calculations
        )
        return not has_statistical_anchor and not concepts

    @staticmethod
    def _clarification_response(
        query: str,
        dimensions: dict[str, Any],
        concepts: list[str],
        calculations: list[str],
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "상태": "needs_clarification",
            "status": "needs_clarification",
            "answer": None,
            "intent": "unknown",
            "query": query,
            "verification_level": "planning_only",
            "confidence": "low",
            "intended_dimensions": dimensions,
            "required_dimensions": [],
            "concepts": concepts,
            "calculations": calculations,
            "suggested_workflow": [],
            "next_call": None,
            "suggested_clarification_questions": [
                "어떤 통계 지표를 보고 싶으신가요? 예: 인구, 실업률, 소비자물가지수, GRDP",
                "지역이나 기간 기준이 있나요? 예: 서울 2020년, 최근 5년, 전국 최신",
                "단일값, 추이, 지역 비교, 비중 계산 중 어떤 형태가 필요하신가요?",
            ],
            "route": route_payload.get("route", {}),
            "router_slots": route_payload.get("slots", {}),
            "validation": route_payload.get("validation", {}),
            "must_not": [
                "통계표 ID 확정 금지",
                "ITM_ID/OBJ_ID 코드 매핑 금지",
                "실제 값 반환 금지",
                "산술·산식 계산 금지",
            ],
            "notes": [
                "통계 지표나 분석 대상이 충분히 드러나지 않아 실행 레일을 만들지 않았습니다.",
                "질문을 구체화한 뒤 plan_query를 다시 호출하세요.",
            ],
        }

    def _dimensions(self, query: str, route_payload: dict[str, Any]) -> dict[str, Any]:
        q_norm = _compact_text(query)
        q_lower = query.lower()
        slots = route_payload.get("slots") or {}
        dimensions: dict[str, Any] = {}

        indicator = self._indicator(query, q_norm, q_lower, route_payload)
        if indicator:
            dimensions["indicator"] = indicator
        if "폐업" in q_norm:
            dimensions["event"] = "폐업"
        if "창업" in q_norm:
            dimensions["event"] = "창업"
        if "생존" in q_norm:
            dimensions["event"] = "생존"

        region = slots.get("region") if isinstance(slots, dict) else None
        if not region:
            region = self._region(q_norm, q_lower)
        if region:
            dimensions["region"] = region

        composite = self._composite_region(q_norm)
        if composite:
            dimensions["region_group"] = composite

        age = self._age(query, q_norm)
        if age:
            dimensions["age"] = age

        sex = self._sex(q_norm, q_lower)
        if sex:
            dimensions["sex"] = sex

        time_value = slots.get("time") if isinstance(slots, dict) else None
        time_value = time_value or self._time(query, q_norm)
        if time_value:
            dimensions["time"] = time_value

        industry = slots.get("industry") if isinstance(slots, dict) else None
        if industry:
            dimensions["industry"] = industry
        return dimensions

    def _indicator(self, query: str, q_norm: str, q_lower: str, route_payload: dict[str, Any]) -> Optional[str]:
        manual = [
            ("고령화", "고령인구비중"),
            ("고령인구", "65세이상인구"),
            ("인구", "인구"),
            ("출생", "출생"),
            ("혼인", "혼인"),
            ("치킨", "음식점업"),
            ("폐업", "폐업"),
        ]
        for token, label in manual:
            if _compact_text(token) in q_norm:
                return label
        direct_key = (route_payload.get("route") or {}).get("direct_stat_key")
        if direct_key:
            return str(direct_key)
        slots = route_payload.get("slots") or {}
        if isinstance(slots, dict) and slots.get("indicator"):
            return str(slots["indicator"])
        for alias, label in self.EN_INDICATOR_ALIASES.items():
            if alias in q_lower:
                return label
        return None

    def _region(self, q_norm: str, q_lower: str) -> Optional[str]:
        for region in sorted(REGION_DEMOGRAPHIC.keys(), key=len, reverse=True):
            if region != "전국" and _compact_text(region) in q_norm:
                return region
        if "전국" in q_norm:
            return "전국"
        for alias, region in self.EN_REGION_ALIASES.items():
            if alias in q_lower:
                return region
        return None

    @staticmethod
    def _composite_region(q_norm: str) -> Optional[str]:
        for name in sorted(REGION_COMPOSITES, key=len, reverse=True):
            if _compact_text(name) in q_norm:
                return name
        if "광역시" in q_norm:
            return "광역시"
        return None

    @staticmethod
    def _age(query: str, q_norm: str) -> Optional[dict[str, Any]]:
        decade = re.search(r"(\d{2})\s*대", query)
        if decade:
            n = int(decade.group(1))
            return {"label": f"{n}대", "type": "decade", "range": [n, n + 9]}
        explicit = re.search(r"(\d{1,3})\s*[-~]\s*(\d{1,3})\s*세", query)
        if explicit:
            return {"label": f"{explicit.group(1)}-{explicit.group(2)}세", "type": "range", "range": [int(explicit.group(1)), int(explicit.group(2))]}
        if "청년" in q_norm:
            return {"label": "청년", "type": "named_group", "range": [20, 34]}
        if "고령" in q_norm or "65세이상" in q_norm:
            return {"label": "65세 이상", "type": "lower_bound", "range": [65, None]}
        return None

    @staticmethod
    def _sex(q_norm: str, q_lower: str) -> Optional[str]:
        if any(term in q_norm for term in ("여성", "여자", "여자인구")) or re.search(r"\b(female|women|woman)\b", q_lower):
            return "여성"
        if any(term in q_norm for term in ("남성", "남자", "남자인구")) or re.search(r"\b(male|men|man)\b", q_lower):
            return "남성"
        return None

    @staticmethod
    def _time(query: str, q_norm: str) -> Optional[dict[str, Any]]:
        years = re.findall(r"(19\d{2}|20\d{2})", query)
        if len(years) >= 2 and any(term in q_norm for term in ("부터", "까지", "대비", "~")):
            return {"type": "range", "start": years[0], "end": years[1]}
        if years:
            return {"type": "year", "value": years[0]}
        if any(term in q_norm for term in ("최근", "최신", "현재")):
            return {"type": "latest", "value": "latest"}
        return None

    @staticmethod
    def _calculations(query: str, route_payload: dict[str, Any]) -> list[str]:
        q_norm = _compact_text(query)
        slots = route_payload.get("slots") or {}
        calculations = list(slots.get("calculation") or []) if isinstance(slots, dict) else []
        if any(term in q_norm for term in ("1인당", "인당")) or "percapita" in query.lower():
            calculations.append("per_capita")
        if any(term in q_norm for term in ("비중", "비율", "구성비", "차지", "고령화율", "고령화")):
            calculations.append("share")
        if any(term in q_norm for term in ("폐업률", "창업률", "생존율")):
            calculations.append("share")
        if any(term in q_norm for term in ("증가율", "변화율", "감소율")):
            calculations.append("growth_rate")
        if any(term in q_norm for term in ("가장빠른", "빠른곳", "속도", "빨라")):
            calculations.append("growth_rate")
        if any(term in q_norm for term in ("추이", "시계열", "최근5년", "최근10년")):
            calculations.append("time_series")
        return list(dict.fromkeys(calculations))

    @staticmethod
    def _intent(route_payload: dict[str, Any], calculations: list[str]) -> str:
        if "per_capita" in calculations or "share" in calculations:
            return "computed_indicator"
        if "time_series" in calculations:
            return "trend"
        if "growth_rate" in calculations:
            return "growth_rate"
        intents = route_payload.get("intents") or []
        if "STAT_RANKING" in intents:
            return "ranking"
        if "STAT_COMPARISON" in intents:
            return "comparison"
        return "single_value"

    @staticmethod
    def _required_dimensions(dimensions: dict[str, Any], calculations: list[str]) -> list[str]:
        required: list[str] = []
        for key in ("region", "region_group", "age", "sex", "industry", "time"):
            if dimensions.get(key):
                required.append(key)
        if any(op in calculations for op in ("time_series", "growth_rate")) and "time" not in required:
            required.append("time")
        return required

    @staticmethod
    def _concepts(dimensions: dict[str, Any], calculations: list[str]) -> list[str]:
        concepts: list[str] = []
        for key in ("indicator", "region", "region_group", "industry", "sex", "event"):
            value = dimensions.get(key)
            if isinstance(value, str):
                concepts.append(value)
        age = dimensions.get("age")
        if isinstance(age, dict) and age.get("label"):
            concepts.append(str(age["label"]))
        time_value = dimensions.get("time")
        if isinstance(time_value, dict):
            concepts.extend(str(v) for k, v in time_value.items() if k in {"value", "start", "end"} and v)
        concepts.extend(calculations)
        return list(dict.fromkeys(concepts))

    def _workflow(
        self,
        query: str,
        intent: str,
        required: list[str],
        concepts: list[str],
        dimensions: dict[str, Any],
        calculations: list[str],
    ) -> list[WorkflowStep]:
        period_range = self._period_range(dimensions.get("time"))
        steps = [
            WorkflowStep(
                1,
                "select_table_for_query",
                "질문 의도와 필요한 분류축을 만족하는 KOSIS 통계표를 고릅니다.",
                {
                    "query": query,
                    "required_dimensions": required,
                    "indicator": dimensions.get("indicator"),
                    "reject_if_missing_dimensions": True,
                },
                "select_table_for_query" in self.AVAILABLE_TOOLS,
            ),
            WorkflowStep(
                2,
                "resolve_concepts",
                "선택된 표의 메타데이터 안에서 자연어 개념을 OBJ_ID/ITM_ID 코드로 바꿉니다.",
                {
                    "org_id": "<selected.org_id>",
                    "tbl_id": "<selected.tbl_id>",
                    "concepts": concepts,
                    "valid_only_for": "<selected.tbl_id>",
                },
                "resolve_concepts" in self.AVAILABLE_TOOLS,
            ),
            WorkflowStep(
                3,
                "query_table",
                "검증된 코드로 KOSIS raw rows를 조회합니다. 합산과 계산은 하지 않습니다.",
                {
                    "org_id": "<selected.org_id>",
                    "tbl_id": "<selected.tbl_id>",
                    "filters": "<resolve_concepts.filters>",
                    "period_range": period_range or "<resolve_concepts.period_range>",
                    "aggregation": "none",
                },
                True,
            ),
        ]
        compute_ops = [op for op in calculations if op in {"per_capita", "share", "growth_rate", "cagr", "yoy_diff", "yoy_pct"}]
        age = dimensions.get("age")
        if isinstance(age, dict) and age.get("type") in {"decade", "named_group", "lower_bound"}:
            compute_ops.append("sum_additive_rows")
        if compute_ops:
            steps.append(WorkflowStep(
                4,
                "compute_indicator",
                "허용된 산식 enum만 사용해 raw rows를 계산합니다.",
                {
                    "operation": compute_ops[0],
                    "operations": list(dict.fromkeys(compute_ops)),
                    "allowed_operations": ["per_capita", "share", "ratio", "growth_rate", "cagr", "yoy_diff", "yoy_pct", "sum_additive_rows"],
                    "input_rows": "<query_table.rows>",
                    "intent": intent,
                },
                "compute_indicator" in self.AVAILABLE_TOOLS,
            ))
        return steps

    @staticmethod
    def _period_range(time_value: Any) -> Optional[list[str]]:
        if isinstance(time_value, dict):
            if time_value.get("type") == "year" and time_value.get("value"):
                return [str(time_value["value"]), str(time_value["value"])]
            if time_value.get("type") == "range" and time_value.get("start") and time_value.get("end"):
                return [str(time_value["start"]), str(time_value["end"])]
        return None


class NaturalLanguageAnswerEngine:
    """자연어 질문을 실제 실행 가능한 답변 또는 안전한 후보 답변으로 변환."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @staticmethod
    def _norm(text: str) -> str:
        return _compact_text(text)

    @staticmethod
    def _to_float(value: Any) -> float:
        return float(str(value).replace(",", ""))

    async def _latest_stat(self, key: str, label: Optional[str] = None, region: str = "전국") -> AnswerStat:
        result = await quick_stat(key, region, "latest", self.api_key)
        if "오류" in result or "값" not in result:
            raise RuntimeError(str(result.get("오류") or result))
        param = TIER_A_STATS.get(key)
        return AnswerStat(
            key=key,
            label=label or (param.description if param else key),
            value=self._to_float(result["값"]),
            formatted=_format_number(result["값"]),
            unit=result.get("단위", param.unit if param else ""),
            period=str(result.get("시점", "")),
            table=result.get("통계표", param.tbl_nm if param else ""),
            region=region,
        )

    @staticmethod
    def _route_payload(query: str) -> dict[str, Any]:
        return _route_query(query).to_agent_payload()

    @staticmethod
    def _effective_region(route_payload: dict[str, Any], region: str) -> str:
        """Use an explicit tool argument first, then a region parsed from the query."""
        if region and region != "전국":
            return region
        slot_region = route_payload.get("slots", {}).get("region")
        if isinstance(slot_region, str) and slot_region:
            return slot_region
        return region or "전국"

    @staticmethod
    def _same_period(stats: list[AnswerStat]) -> bool:
        return len({s.period for s in stats}) == 1

    @staticmethod
    def _validation_notes(route_payload: dict[str, Any]) -> list[str]:
        warnings = route_payload.get("validation", {}).get("warnings", [])
        notes = list(warnings)
        if "기업 수·사업체 수·자영업자 수는 모집단 기준이 다르므로 혼동 금지." not in notes:
            notes.append("기업 수·사업체 수·자영업자 수는 모집단 기준이 다르므로 혼동 금지.")
        return notes

    @staticmethod
    def _period_argument(query: str, route_payload: dict[str, Any]) -> str:
        slots = route_payload.get("slots", {})
        time_slot = slots.get("time") if isinstance(slots, dict) else None
        if _parse_month_token(query) or _parse_quarter_token(query):
            return query
        if isinstance(time_slot, dict):
            if time_slot.get("type") in {"year", "month", "quarter"}:
                return str(time_slot.get("value") or query)
            if time_slot.get("type") == "latest":
                return "latest"
        if any(term in query for term in ("재작년", "작년", "지난해", "전년", "올해", "금년")):
            return query
        return "latest"

    def _is_growth_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        if "STAT_GROWTH_RATE" in route_payload.get("intents", []):
            return True
        q = self._norm(query)
        return any(term in q for term in ("전년대비", "전월대비", "증가율", "감소율", "변화율", "얼마나늘", "얼마나줄"))

    @staticmethod
    def _growth_period_count(query: str) -> int:
        m = re.search(r"최근\s*(\d+)\s*(?:년|개월)", query)
        if m:
            return max(2, int(m.group(1)))
        return 2

    def _is_sme_smallbiz_count_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and "소상공인" in q
            and any(term in q for term in ("수", "사업체", "기업"))
            and not any(term in q for term in ("업종별", "지역별", "폐업률", "창업률", "매출"))
        )

    def _is_sme_employee_average_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and any(term in q for term in ("종사자", "고용"))
            and any(term in q for term in ("사업체당", "기업당", "평균", "함께", "비교"))
        )

    def _is_region_compare_question(self, query: str) -> bool:
        q = self._norm(query)
        return any(term in q for term in ("시도별", "지역별", "광역시별", "17개시도", "지역순위"))

    @staticmethod
    def _extract_top_n(query: str) -> Optional[int]:
        compact = re.sub(r"\s+", "", str(query))
        patterns = [
            r"top(\d+)",
            r"상위(\d+)",
            r"하위(\d+)",
            r"가장많은(\d+)[곳개]",
            r"가장적은(\d+)[곳개]",
            r"가장많은(?:시도|지역)(\d+)[곳개]",
            r"가장적은(?:시도|지역)(\d+)[곳개]",
            r"많은(\d+)[곳개]",
            r"적은(\d+)[곳개]",
            r"많은(?:시도|지역)(\d+)[곳개]",
            r"적은(?:시도|지역)(\d+)[곳개]",
            r"(?:시도|지역)(\d+)[곳개]",
            r"(\d+)개시도",
            r"(\d+)개지역",
            r"(\d+)[곳개]",
            r"(\d+)위까지",
            r"1위부터(\d+)위",
            r"순위(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, compact, re.IGNORECASE)
            if m:
                try:
                    n = int(m.group(1))
                    if 1 <= n <= 17:
                        return n
                except ValueError:
                    continue
        return None

    def _is_top_n_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        if "STAT_RANKING" in route_payload.get("intents", []):
            return True
        if self._extract_top_n(query) is not None:
            return True
        q = self._norm(query)
        return any(term in q for term in ("가장많", "가장적", "상위", "하위", "topn"))

    def _is_share_ratio_question(self, query: str, route_payload: dict[str, Any]) -> bool:
        # Survival/closure rate is a time-based dynamic ratio, not a
        # part/whole 구성비 — refuse to dispatch share_ratio for those
        # verbal patterns (#27).
        if self._is_dynamic_ratio_question(query):
            return False
        slots = route_payload.get("slots") or {}
        calc = slots.get("calculation") if isinstance(slots, dict) else None
        if isinstance(calc, list) and "share_ratio" in calc:
            return True
        if "STAT_SHARE_RATIO" in route_payload.get("intents", []):
            return True
        q = self._norm(query)
        return any(term in q for term in ("비중", "비율", "차지", "구성비"))

    @classmethod
    def _is_dynamic_ratio_question(cls, query: str) -> bool:
        """Detect 'time-cohort ratio' verbal cues that the share_ratio
        keyword scanner mistakes for 부분/전체 비중."""
        q = re.sub(r"\s+", "", str(query))
        return any(term in q for term in (
            "살아남", "생존율", "잔존율", "폐업률", "창업률", "유지율",
        ))

    @staticmethod
    def _is_aggregation_question(query: str) -> bool:
        q = re.sub(r"\s+", "", str(query))
        return any(term in q for term in ("합계", "합산", "총합", "더하"))

    @staticmethod
    def _extract_extra_regions(query: str, primary_region: str, route_payload: dict[str, Any]) -> list[str]:
        slots = route_payload.get("slots") or {}
        candidates: list[str] = []
        comp = slots.get("comparison_target") if isinstance(slots, dict) else None
        if isinstance(comp, list):
            candidates.extend(comp)
        q_compact = re.sub(r"\s+", "", str(query))
        known_regions = [
            "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
            "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
        ]
        for region in known_regions:
            if region in q_compact and region != primary_region and region not in candidates:
                candidates.append(region)
        return [r for r in candidates if r in known_regions]

    @staticmethod
    def _extract_composite_regions(query: str) -> list[str]:
        """Return the list of composite-region labels (수도권, 비수도권, …)
        that appear in the raw query. Longer labels win when one is a
        substring of another (비수도권 contains 수도권) so the more
        specific composite is selected."""
        q_compact = re.sub(r"\s+", "", str(query))
        candidates = sorted(
            (name for name in REGION_COMPOSITES if name in q_compact),
            key=len,
            reverse=True,
        )
        deduped: list[str] = []
        for name in candidates:
            if not any(name != existing and name in existing for existing in deduped):
                deduped.append(name)
        return deduped

    @classmethod
    def _expand_composite_to_components(cls, composite: str) -> list[str]:
        return list(REGION_COMPOSITES.get(composite, []))

    _COMPOSITE_PER_CALL_TIMEOUT = 12.0
    _COMPOSITE_TOTAL_BUDGET = 60.0

    async def _answer_composite_aggregate(
        self,
        query: str,
        direct_key: str,
        composite: str,
        operation: str = "sum",
    ) -> dict[str, Any]:
        """Handle composite-region queries (수도권 사업체수, 영남권 비중 등).

        operation="sum" returns tier_a_region_sum for the components.
        operation="share" computes (sum of components) / 전국 * 100.

        All component calls run in parallel with a per-call timeout
        and an overall budget — sequential iteration of 14 regions
        (비수도권) used to chain 14 × 30 s timeouts on a slow KOSIS
        response, which presented to callers as a multi-minute hang
        and starved every other in-flight request (#30)."""
        components = self._expand_composite_to_components(composite)
        if not components:
            return await self._answer_search_fallback(query)
        components = components[:17]
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        async def fetch_one(region: str) -> tuple[str, dict[str, Any]]:
            try:
                return region, await asyncio.wait_for(
                    _quick_stat_core(direct_key, region, period, self.api_key),
                    timeout=self._COMPOSITE_PER_CALL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return region, {"오류": "호출 타임아웃"}
            except Exception as exc:  # network/protocol failures shouldn't poison the whole batch
                return region, {"오류": f"{type(exc).__name__}: {exc}"}

        try:
            gathered = await asyncio.wait_for(
                asyncio.gather(*(fetch_one(r) for r in components)),
                timeout=self._COMPOSITE_TOTAL_BUDGET,
            )
        except asyncio.TimeoutError:
            return {
                "상태": "failed",
                "코드": STATUS_RUNTIME_ERROR,
                "답변유형": "tier_a_composite_timeout",
                "질문": query,
                "answer": (
                    f"{composite}({'+'.join(components)}) 합산이 전체 예산({self._COMPOSITE_TOTAL_BUDGET:.0f}초)을 "
                    "초과해 중단했습니다. KOSIS 응답 지연 가능성 — 잠시 후 재시도하거나 시도별 단일 호출로 분리하세요."
                ),
                "구성_지역": components,
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }

        rows: list[dict[str, Any]] = []
        subtotal = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region, stat in gathered:
            if not isinstance(stat, dict) or "오류" in stat or "값" not in stat:
                missing.append(region)
                continue
            try:
                value = self._to_float(stat["값"])
            except (KeyError, ValueError):
                missing.append(region)
                continue
            subtotal += value
            unit = unit or stat.get("단위", "")
            used_period = used_period or str(stat.get("시점", ""))
            table = table or stat.get("통계표", "")
            rows.append({
                "지역": region,
                "값": _format_number(value),
                "원본값": value,
                "단위": stat.get("단위", unit),
                "시점": stat.get("시점", ""),
                "통계표": stat.get("통계표"),
            })

        if not rows:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        if missing:
            notes.append(f"{composite} 합산에서 제외된 지역: {', '.join(missing)} (데이터 없음)")
        distinct_periods = {row["시점"] for row in rows}
        if len(distinct_periods) > 1:
            notes.append(f"{composite} 구성 지역 간 시점 불일치: {sorted(distinct_periods)}")

        base_payload: dict[str, Any] = {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "질문": query,
            "표": rows,
            "구성_지역": components,
            "추천_시각화": ["bar_chart"],
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        if operation == "share":
            whole = await quick_stat(direct_key, "전국", period, self.api_key)
            if "오류" in whole or "값" not in whole:
                return await self._answer_search_fallback(query, route_payload)
            try:
                whole_value = self._to_float(whole["값"])
            except (KeyError, ValueError):
                return await self._answer_search_fallback(query, route_payload)
            if not whole_value:
                return await self._answer_search_fallback(query, route_payload)
            share = subtotal / whole_value * 100
            whole_period = str(whole.get("시점", ""))
            if used_period and whole_period and used_period != whole_period:
                notes.append(f"분자 시점 {used_period} ↔ 분모 시점 {whole_period} 불일치")
            return {
                **base_payload,
                "답변유형": "tier_a_composite_share_ratio",
                "answer": (
                    f"{used_period} 기준 {composite}({'+'.join(components)})의 "
                    f"{stat_label}은(는) {_format_number(subtotal)}{unit}로, "
                    f"전국({_format_number(whole_value)}{unit}) 대비 약 {share:.2f}%입니다."
                ),
                "계산": {
                    "분자": _format_number(subtotal),
                    "분모": _format_number(whole_value),
                    "비중_퍼센트": round(share, 2),
                    "산식": f"({' + '.join(components)}) / 전국 * 100",
                    "동일시점_여부": used_period == whole_period,
                },
                "검증_주의": notes,
            }

        return {
            **base_payload,
            "답변유형": "tier_a_region_sum",
            "answer": (
                f"{used_period} 기준 {composite}({'+'.join(components)})의 "
                f"{stat_label} 합계는 {_format_number(subtotal)}{unit}입니다."
            ),
            "계산": {
                "합계": _format_number(subtotal),
                "포함_지역": [r["지역"] for r in rows],
                "제외_지역": missing,
                "산식": " + ".join(r["지역"] for r in rows),
                "합성지역": composite,
            },
            "검증_주의": notes,
        }

    def _is_sme_large_sales_question(self, query: str) -> bool:
        q = self._norm(query)
        return (
            "중소기업" in q
            and any(term in q for term in ("대기업", "중소기업외"))
            and any(term in q for term in ("매출", "매출액"))
            and any(term in q for term in ("비교", "비중", "차이", "전체매출"))
        )

    def _mixed_self_employed_business_key(self, query: str) -> Optional[tuple[str, str]]:
        q = self._norm(query)
        has_self_employed = any(term in q for term in ("자영업자", "자영업", "개인사업자"))
        has_business_count = any(term in q for term in ("사업체", "사업체수", "기업수", "업체수", "업체"))
        asks_jointly = any(term in q for term in ("비교", "차이", "비중", "대비", "같이", "함께", "와", "과"))
        if not (has_self_employed and has_business_count and asks_jointly):
            return None
        if "중소기업" in q:
            return "중소기업_사업체수", "중소기업 사업체 수"
        if "소상공인" in q:
            return "소상공인_사업체수", "소상공인 사업체 수"
        return "전체사업체수", "총 사업체 수"

    def _is_self_employed_sme_population_question(self, query: str) -> bool:
        return self._mixed_self_employed_business_key(query) is not None

    @staticmethod
    def _needs_advanced_analysis_plan(route_payload: dict[str, Any]) -> bool:
        advanced = {"STAT_CORRELATION", "STAT_REGRESSION", "POLICY_EFFECT_ANALYSIS"}
        return any(intent in advanced for intent in route_payload.get("intents", []))

    def _infer_direct_stat_key(self, query: str, route_payload: dict[str, Any]) -> Optional[str]:
        direct_key = route_payload["route"].get("direct_stat_key")
        if direct_key:
            return direct_key
        q = self._norm(query)
        if "중소기업" in q and any(term in q for term in ("매출", "매출액")):
            return "중소기업_매출액"
        if "대기업" in q and any(term in q for term in ("매출", "매출액")):
            return "대기업_매출액"
        if "중소기업" in q and any(term in q for term in ("종사자", "고용")):
            return "중소기업_종사자수"
        if "중소기업" in q and any(term in q for term in ("사업체", "기업수", "업체수", "중소기업수")):
            return "중소기업_사업체수"
        if "소상공인" in q and any(term in q for term in ("사업체", "업체수", "소상공인수")):
            return "소상공인_사업체수"
        return None

    async def _answer_sme_smallbiz_counts(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        sme = await self._latest_stat("중소기업_사업체수", "중소기업 사업체 수", region)
        smallbiz = await self._latest_stat("소상공인_사업체수", "소상공인 사업체 수", region)
        stats = [sme, smallbiz]

        diff = sme.value - smallbiz.value
        smallbiz_share = (smallbiz.value / sme.value * 100) if sme.value else None
        comparison = {
            "차이_중소기업-소상공인": _format_number(diff),
            "소상공인_대비_중소기업_비중": round(smallbiz_share, 2) if smallbiz_share is not None else None,
            "동일시점_여부": self._same_period(stats),
        }
        answer = (
            f"{sme.period}년 기준 {region} 중소기업 사업체 수는 {sme.formatted}{sme.unit}, "
            f"소상공인 사업체 수는 {smallbiz.formatted}{smallbiz.unit}입니다. "
            f"소상공인은 중소기업 중 더 작은 규모 요건을 만족하는 하위 집단에 가까우므로, "
            f"두 지표는 포함 범위와 작성 기준 차이 때문에 값이 다릅니다."
        )
        if smallbiz_share is not None:
            answer += f" 이 기준에서는 소상공인 사업체 수가 중소기업 사업체 수의 약 {smallbiz_share:.2f}%입니다."

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite",
            "질문": query,
            "answer": answer,
            "표": [s.to_row() for s in stats],
            "계산": {
                **comparison,
                "산식": "차이 = 중소기업 사업체 수 - 소상공인 사업체 수; 비중 = 소상공인 사업체 수 / 중소기업 사업체 수 * 100",
            },
            "해석": [
                "중소기업은 더 넓은 기업 규모 범주이고, 소상공인은 상시근로자 수 등 요건이 더 좁은 집단입니다.",
                "사업체 기준 통계이므로 법인·경영 단위의 기업체 수와 직접 동일시하면 안 됩니다.",
            ],
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_self_employed_sme_population_warning(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        business_key, business_label = self._mixed_self_employed_business_key(query) or (
            "중소기업_사업체수",
            "중소기업 사업체 수",
        )
        try:
            business = await self._latest_stat(business_key, business_label, region)
            self_employed = await self._latest_stat("자영업자수", "자영업자 수", region)
        except RuntimeError:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        notes.append(
            f"이 질의는 자영업자(종사상지위별 취업자)와 {business_label}(사업체 단위)를 함께 언급합니다. "
            "두 모집단은 단위와 작성 기준이 달라 비율·차이를 자동 계산하지 않았습니다."
        )
        if not self._same_period([business, self_employed]):
            notes.append(f"시점 불일치: {business_label} {business.period}, 자영업자 수 {self_employed.period}")

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_population_mixed_comparison",
            "질문": query,
            "answer": (
                f"{region} 기준 {business_label}와 자영업자 수를 각각 조회했습니다. "
                "두 지표는 모집단이 달라 직접 비율이나 차이로 계산하지 않았습니다."
            ),
            "표": [business.to_row(), self_employed.to_row()],
            "모집단_주의": {
                business_key: "사업체 단위 사업체 수",
                "자영업자수": "종사상지위별 취업자 중 자영업자",
                "자동계산": "차이·비율 계산 안 함",
            },
            "추천_후속": [
                "자영업자 수만 조회하려면 quick_stat('자영업자수', region='전국', period='latest')",
                "사업체 단위 비교가 필요하면 같은 사업체조사 계열 통계표를 선택",
            ],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_sme_employee_average(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        businesses = await self._latest_stat("중소기업_사업체수", "중소기업 사업체 수", region)
        workers = await self._latest_stat("중소기업_종사자수", "중소기업 종사자 수", region)
        avg = workers.value / businesses.value if businesses.value else 0.0
        answer = (
            f"{businesses.period}년 기준 {region} 중소기업 사업체 수는 "
            f"{businesses.formatted}{businesses.unit}, 종사자 수는 {workers.formatted}{workers.unit}입니다. "
            f"단순 계산한 사업체당 평균 종사자 수는 약 {avg:.2f}명입니다."
        )

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite_calculation",
            "질문": query,
            "answer": answer,
            "표": [businesses.to_row(), workers.to_row()],
            "계산": {
                "사업체당_평균_종사자수": round(avg, 2),
                "산식": "중소기업 종사자 수 / 중소기업 사업체 수",
                "동일시점_여부": self._same_period([businesses, workers]),
            },
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_sme_large_sales(self, query: str, region: str) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        sme = await self._latest_stat("중소기업_매출액", "중소기업 매출액", region)
        large = await self._latest_stat("대기업_매출액", "대기업 매출액", region)
        total = sme.value + large.value
        diff = large.value - sme.value
        sme_share = (sme.value / total * 100) if total else None
        large_share = (large.value / total * 100) if total else None

        answer = (
            f"{sme.period}년 기준 {region} 중소기업 매출액은 {sme.formatted}{sme.unit}, "
            f"대기업 매출액은 {large.formatted}{large.unit}입니다. "
            f"두 값을 합산한 전체 기업 매출 대비 중소기업 비중은 약 {sme_share:.2f}%입니다."
        )

        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_composite_comparison",
            "질문": query,
            "answer": answer,
            "표": [sme.to_row(), large.to_row()],
            "계산": {
                "전체_매출액_합산": _format_number(total),
                "대기업-중소기업_차이": _format_number(diff),
                "중소기업_비중": round(sme_share, 2) if sme_share is not None else None,
                "대기업_비중": round(large_share, 2) if large_share is not None else None,
                "동일시점_여부": self._same_period([sme, large]),
                "산식": "중소기업 비중 = 중소기업 매출액 / (중소기업 매출액 + 대기업 매출액) * 100",
            },
            "해석": [
                "KOSIS 원표의 대기업 축은 '중소기업 외' 코드입니다. 법령상 대기업만의 범위와 다를 수 있어 출처 기준을 함께 표시해야 합니다.",
                "금액 단위는 억원이며, 지역·산업·기업규모 기준이 동일한 값끼리 비교했습니다.",
            ],
            "추천_시각화": ["bar_chart"],
            "검증_주의": self._validation_notes(route_payload),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_direct(
        self,
        query: str,
        region: str,
        direct_key: Optional[str] = None,
        route_payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        route_payload = route_payload or self._route_payload(query)
        direct_key = direct_key or self._infer_direct_stat_key(query, route_payload) or query
        route_payload["route"]["direct_stat_key"] = direct_key
        q = self._norm(query)

        if self._is_growth_question(query, route_payload):
            period_count = self._growth_period_count(query)
            start_p, end_p = _extract_year_range(query)
            if start_p and end_p:
                span = max(int(end_p) - int(start_p) + 1, period_count)
            else:
                span = period_count
            comparison = await stat_time_compare(direct_key, region, start_p, end_p, span, self.api_key)
            if comparison.get("상태") != "executed":
                comparison["답변유형"] = "tier_a_growth_rate_failed"
                comparison["route"] = route_payload["route"]
                comparison["검증_주의"] = self._validation_notes(route_payload)
                return comparison
            notes = self._validation_notes(route_payload)
            used = comparison.get("비교") or {}
            used_start = (used.get("시작") or {}).get("시점")
            used_end = (used.get("종료") or {}).get("시점")
            if start_p and used_start and not str(used_start).startswith(start_p):
                notes.append(f"요청 시작 시점 {start_p} → 사용 시점 {used_start}로 변경됨")
            if end_p and used_end and not str(used_end).startswith(end_p):
                notes.append(f"요청 종료 시점 {end_p} → 사용 시점 {used_end}로 변경됨")
            return {
                "상태": "executed",
                "코드": STATUS_EXECUTED,
                "답변유형": "tier_a_growth_rate",
                "질문": query,
                "answer": comparison.get("answer"),
                "표": comparison.get("표", []),
                "비교": comparison.get("비교"),
                "요청_시작": start_p,
                "요청_종료": end_p,
                "지역": region,
                "통계표": (comparison.get("표") or [{}])[0].get("통계표"),
                "추천_시각화": ["line_chart"],
                "검증_주의": notes,
                "route": route_payload["route"],
                "출처": comparison.get("출처", "통계청 KOSIS"),
            }

        if any(term in q for term in ("추이", "최근", "시계열", "그래프", "선그래프", "분석")):
            years_match = re.search(r"최근\s*(\d+)\s*년", query)
            years = int(years_match.group(1)) if years_match else 5
            open_start_year = _extract_open_start_year(query)
            trend = await _quick_trend_core(
                direct_key,
                region,
                years,
                self.api_key,
                start_year=open_start_year,
            )
            if "오류" in trend:
                return await self._answer_search_fallback(query, route_payload)
            if open_start_year:
                answer = (
                    f"{region}의 {trend.get('통계명', direct_key)} {open_start_year}년부터 최신까지 "
                    f"{len(trend.get('시계열', []))}개 시점 자료를 조회했습니다."
                )
            else:
                answer = (
                    f"{region}의 {trend.get('통계명', direct_key)} 최근 {len(trend.get('시계열', []))}개 시점 "
                    f"자료를 조회했습니다."
                )
            notes = list(route_payload["validation"].get("warnings", []))
            period_interpretation = trend.get("⚠️ 기간_해석")
            if period_interpretation:
                notes.append(period_interpretation)
            result: dict[str, Any] = {
                "상태": "executed",
                "코드": STATUS_EXECUTED,
                "답변유형": "tier_a_trend",
                "질문": query,
                "answer": answer,
                "표": trend.get("시계열", []),
                "단위": trend.get("단위"),
                "지역": region,
                "데이터수": len(trend.get("시계열", [])),
                "통계표": trend.get("통계표"),
                "추천_시각화": ["line_chart"],
                "검증_주의": notes,
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }
            if "분석" in q:
                result["분석"] = await analyze_trend(direct_key, region, max(years, 5), self.api_key)
            return result

        open_start_year = _extract_open_start_year(query)
        if open_start_year:
            trend = await _quick_trend_core(
                direct_key,
                region,
                5,
                self.api_key,
                start_year=open_start_year,
            )
            if "오류" not in trend:
                return {
                    "상태": "executed",
                    "코드": STATUS_EXECUTED,
                    "답변유형": "tier_a_trend",
                    "질문": query,
                    "answer": (
                        f"{region}의 {trend.get('통계명', direct_key)} {open_start_year}년부터 최신까지 "
                        f"{len(trend.get('시계열', []))}개 시점 자료를 조회했습니다."
                    ),
                    "표": trend.get("시계열", []),
                    "단위": trend.get("단위"),
                    "지역": region,
                    "데이터수": len(trend.get("시계열", [])),
                    "통계표": trend.get("통계표"),
                    "추천_시각화": ["line_chart"],
                    "검증_주의": route_payload["validation"].get("warnings", []),
                    "route": route_payload["route"],
                    "출처": "통계청 KOSIS",
                }

        period = self._period_argument(query, route_payload)
        stat = await quick_stat(direct_key, region, period, self.api_key)
        if "오류" in stat:
            return await self._answer_search_fallback(query, route_payload)
        if "값" not in stat:
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "답변유형": "tier_a_value_failed",
                "질문": query,
                "answer": "요청한 조건의 KOSIS 데이터가 없습니다. 최신값으로 대체하지 않고 중단했습니다.",
                "상세": stat,
                "지역": region,
                "요청_기간": period,
                "검증_주의": self._validation_notes(route_payload),
                "route": route_payload["route"],
                "출처": stat.get("출처", "통계청 KOSIS"),
            }
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_value",
            "질문": query,
            "answer": stat.get("answer"),
            "표": [{
                "지표": direct_key,
                "값": _format_number(stat.get("값")),
                "단위": stat.get("단위"),
                "시점": stat.get("시점"),
                "지역": stat.get("지역"),
                "통계표": stat.get("통계표"),
            }],
            "검증_주의": route_payload["validation"].get("warnings", []),
            "route": route_payload["route"],
            "출처": stat.get("출처", "통계청 KOSIS"),
        }

    async def _answer_region_compare(self, query: str, direct_key: Optional[str] = None) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        direct_key = direct_key or self._infer_direct_stat_key(query, route_payload) or query
        comparison = await quick_region_compare(direct_key, api_key=self.api_key)
        if "오류" in comparison:
            return await self._answer_search_fallback(query, route_payload)
        route_payload["route"]["direct_stat_key"] = direct_key

        rows = comparison.get("표", [])
        top = rows[:3]
        bottom = rows[-3:] if len(rows) >= 3 else rows
        unit = comparison.get("단위", "")
        answer = (
            f"{comparison.get('시점')}년 기준 {comparison.get('통계명', direct_key)} 시도별 비교 결과, "
            f"가장 많은 지역은 {top[0]['지역']}({top[0]['값']}{unit})입니다."
            if top else
            "시도별 비교 데이터를 조회했습니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_region_comparison",
            "질문": query,
            "answer": answer,
            "표": rows,
            "상위": top,
            "하위": bottom,
            "추천_시각화": ["bar_chart"],
            "검증_주의": route_payload["validation"].get("warnings", []),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_top_n(
        self,
        query: str,
        direct_key: str,
        top_n: int,
        include_share_ratio: bool = False,
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        comparison = await quick_region_compare(direct_key, api_key=self.api_key)
        if "오류" in comparison:
            return await self._answer_search_fallback(query, route_payload)
        route_payload["route"]["direct_stat_key"] = direct_key

        rows = comparison.get("표", [])
        q = self._norm(query)
        descending = not any(term in q for term in ("가장적", "적은", "하위", "낮은"))
        ordered = rows if descending else list(reversed(rows))
        selected = ordered[:top_n]
        unit = comparison.get("단위", "")
        period = comparison.get("시점")
        direction = "많은" if descending else "적은"
        rank_label = "상위" if descending else "하위"
        if selected:
            leader = selected[0]
            answer = (
                f"{period} 기준 {comparison.get('통계명', direct_key)} {rank_label} {len(selected)}개 지역 중 "
                f"가장 {direction} 지역은 {leader['지역']}({leader['값']}{unit})입니다."
            )
        else:
            answer = "상위/하위 비교 데이터를 조회하지 못했습니다."

        notes = self._validation_notes(route_payload)
        payload: dict[str, Any] = {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_top_n",
            "질문": query,
            "answer": answer,
            "표": selected,
            "전체_지역수": len(rows),
            "요청_top_n": top_n,
            "정렬": "내림차순" if descending else "오름차순",
            "추천_시각화": ["bar_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }
        if not include_share_ratio or not selected:
            return payload

        try:
            subtotal = sum(
                self._to_float(row.get("원값", row.get("값")))
                for row in selected
            )
        except (TypeError, ValueError):
            return payload

        whole = await quick_stat(direct_key, "전국", str(period or "latest"), self.api_key)
        if "오류" in whole or "값" not in whole:
            return payload
        try:
            whole_value = self._to_float(whole["값"])
        except (KeyError, ValueError):
            return payload
        if not whole_value:
            return payload

        whole_period = str(whole.get("시점", ""))
        if period and whole_period and str(period) != whole_period:
            notes.append(f"분자 시점 {period} ↔ 분모 시점 {whole_period} 불일치")
        share = subtotal / whole_value * 100
        stat_label = comparison.get("통계명", direct_key)
        payload.update({
            "답변유형": "tier_a_top_n_share_ratio",
            "answer": (
                f"{period} 기준 {stat_label} {rank_label} {len(selected)}개 지역"
                f"({'+'.join(row['지역'] for row in selected)}) 합계는 "
                f"{_format_number(subtotal)}{unit}로, 전국({_format_number(whole_value)}{unit}) 대비 "
                f"약 {share:.2f}%입니다."
            ),
            "계산": {
                "분자": _format_number(subtotal),
                "분모": _format_number(whole_value),
                "비중_퍼센트": round(share, 2),
                "포함_지역": [row["지역"] for row in selected],
                "산식": f"({' + '.join(row['지역'] for row in selected)}) / 전국 * 100",
                "동일시점_여부": str(period or "") == whole_period,
            },
            "추천_시각화": ["bar_chart", "pie_chart"],
            "검증_주의": notes,
        })
        return payload

    async def _answer_share_ratio(
        self,
        query: str,
        region: str,
        direct_key: str,
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        part = await quick_stat(direct_key, region, period, self.api_key)
        whole = await quick_stat(direct_key, "전국", period, self.api_key)
        if "오류" in part or "오류" in whole or "값" not in part or "값" not in whole:
            return await self._answer_search_fallback(query, route_payload)
        try:
            part_value = self._to_float(part["값"])
            whole_value = self._to_float(whole["값"])
        except (KeyError, ValueError):
            return await self._answer_search_fallback(query, route_payload)
        if not whole_value:
            return await self._answer_search_fallback(query, route_payload)

        part_period = str(part.get("시점", ""))
        whole_period = str(whole.get("시점", ""))
        unit = part.get("단위", "")
        table = part.get("통계표")
        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        share = part_value / whole_value * 100
        notes = self._validation_notes(route_payload)
        if part_period != whole_period:
            notes.append(f"분자 시점 {part_period} ↔ 분모 시점 {whole_period} 불일치 — 동일 시점 확인 필요")

        answer = (
            f"{part_period} 기준 {region} {stat_label}은(는) "
            f"{_format_number(part_value)}{unit}로, "
            f"전국({_format_number(whole_value)}{unit}) 대비 약 {share:.2f}% 입니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_share_ratio",
            "질문": query,
            "answer": answer,
            "표": [
                {"지표": stat_label, "분자": _format_number(part_value), "단위": unit, "시점": part_period, "지역": region, "통계표": table},
                {"지표": stat_label, "분모": _format_number(whole_value), "단위": unit, "시점": whole_period, "지역": "전국", "통계표": table},
            ],
            "계산": {
                "분자": _format_number(part_value),
                "분모": _format_number(whole_value),
                "비중_퍼센트": round(share, 2),
                "산식": "지역값 / 전국값 * 100",
                "동일시점_여부": part_period == whole_period,
            },
            "추천_시각화": ["bar_chart", "pie_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_region_sum(
        self,
        query: str,
        direct_key: str,
        regions: list[str],
    ) -> dict[str, Any]:
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        regions = regions[:17]

        async def fetch_one(region: str) -> tuple[str, dict[str, Any]]:
            try:
                return region, await asyncio.wait_for(
                    _quick_stat_core(direct_key, region, period, self.api_key),
                    timeout=self._COMPOSITE_PER_CALL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return region, {"오류": "호출 타임아웃"}
            except Exception as exc:
                return region, {"오류": f"{type(exc).__name__}: {exc}"}

        try:
            gathered = await asyncio.wait_for(
                asyncio.gather(*(fetch_one(r) for r in regions)),
                timeout=self._COMPOSITE_TOTAL_BUDGET,
            )
        except asyncio.TimeoutError:
            return {
                "상태": "failed",
                "코드": STATUS_RUNTIME_ERROR,
                "답변유형": "tier_a_region_sum_timeout",
                "질문": query,
                "answer": (
                    f"{', '.join(regions)} 합산이 전체 예산({self._COMPOSITE_TOTAL_BUDGET:.0f}초)을 초과해 "
                    "중단했습니다. KOSIS 응답 지연 가능성 — 잠시 후 재시도하거나 단일 호출로 분리하세요."
                ),
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }

        rows: list[dict[str, Any]] = []
        total = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region, stat in gathered:
            if not isinstance(stat, dict) or "오류" in stat or "값" not in stat:
                missing.append(region)
                continue
            try:
                value = self._to_float(stat["값"])
            except (KeyError, ValueError):
                missing.append(region)
                continue
            total += value
            unit = unit or stat.get("단위", "")
            used_period = used_period or str(stat.get("시점", ""))
            table = table or stat.get("통계표", "")
            rows.append({
                "지역": region,
                "값": _format_number(value),
                "원본값": value,
                "단위": stat.get("단위", unit),
                "시점": stat.get("시점", ""),
                "통계표": stat.get("통계표"),
            })

        if not rows:
            return await self._answer_search_fallback(query, route_payload)

        notes = self._validation_notes(route_payload)
        if missing:
            notes.append(f"합계에서 제외된 지역: {', '.join(missing)} (데이터 없음)")
        distinct_periods = {row["시점"] for row in rows}
        if len(distinct_periods) > 1:
            notes.append(f"지역별 시점 불일치: {sorted(distinct_periods)}")

        param = TIER_A_STATS.get(direct_key)
        stat_label = param.description if param else direct_key.replace("_", " ")
        answer = (
            f"{used_period} 기준 {', '.join(r['지역'] for r in rows)} {stat_label} 합계는 "
            f"{_format_number(total)}{unit} 입니다."
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_region_sum",
            "질문": query,
            "answer": answer,
            "표": rows,
            "계산": {
                "합계": _format_number(total),
                "포함_지역": [r["지역"] for r in rows],
                "제외_지역": missing,
                "산식": " + ".join(r["지역"] for r in rows),
            },
            "추천_시각화": ["bar_chart"],
            "검증_주의": notes,
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

    async def _answer_dynamic_ratio_advisory(
        self,
        query: str,
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Route survival/closure/cohort-ratio queries to
        indicator_dependency_map instead of misclassifying them as
        share_ratio (#27).

        'X 살아남는 비율' is a time-cohort survival rate. The keyword
        scanner used to pick "비율" alone and dispatch share_ratio,
        which produced single-point part/whole answers that did not
        address the user's question. We surface the right formula
        spec and search candidates so the caller can step into the
        cohort-aware analysis."""
        q = re.sub(r"\s+", "", str(query))
        if any(t in q for t in ("폐업", "폐업률")):
            indicator = "폐업률"
        elif any(t in q for t in ("창업", "창업률")):
            indicator = "창업률"
        else:
            indicator = "생존율"
        formula_spec = await indicator_dependency_map(indicator)
        dynamic_terms = [
            f"{indicator} {query}",
            f"{indicator} 업종별",
            f"{indicator} 산업별",
            f"기업생멸행정통계 {indicator}",
            f"신생기업 {indicator}",
        ]
        search = await _search_kosis_keywords(
            f"{indicator} {query}",
            dynamic_terms,
            8,
            self.api_key,
            used_routing=True,
        )
        return {
            "상태": "needs_table_selection",
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "답변유형": "dynamic_ratio_advisory",
            "질문": query,
            "answer": (
                f"이 질문은 '{indicator}'에 해당하는 동태 지표입니다. "
                "정태 비중(share_ratio)이 아니라 시간-코호트 기반 산식이 필요합니다. "
                "indicator_dependency_map의 산식과 search_kosis 후보 표를 함께 검토하세요."
            ),
            "지표": indicator,
            "산식_사양": formula_spec,
            "검색결과": search.get("결과", []),
            "사용된_검색어": search.get("사용된_검색어", []),
            "추천_도구_호출": [
                f"indicator_dependency_map('{indicator}') — 산식·필요 통계·검증 포인트 확인",
                f"search_kosis('{indicator} {query[:30]}') — KOSIS 통계표 후보 조회",
            ],
            "route": route_payload.get("route", {}),
        }

    async def _answer_search_fallback(self, query: str, route_payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        route_payload = route_payload or self._route_payload(query)
        # Fold route terms and slot-parsed industry/scale into the
        # actual KOSIS search keywords so route.search_terms and
        # 사용된_검색어 stay consistent (#28).
        slots = route_payload.get("slots") or {}
        slot_terms: list[str] = []
        query_enrichment: list[str] = []
        if isinstance(slots, dict):
            for slot_key in ("industry", "scale", "target"):
                value = slots.get(slot_key)
                if isinstance(value, str) and value:
                    slot_terms.append(value)
                    if value not in query:
                        query_enrichment.append(value)
        enriched_query = (query + " " + " ".join(query_enrichment)).strip() if query_enrichment else query

        route = route_payload.get("route") or {}
        route_terms = [
            str(term).strip()
            for term in (route.get("search_terms") or [])
            if str(term).strip()
        ]
        search_keywords: list[str] = []
        for term in route_terms:
            missing_slot_terms = [slot_term for slot_term in slot_terms if slot_term not in term]
            if missing_slot_terms:
                search_keywords.append((" ".join([*missing_slot_terms, term])).strip())
            search_keywords.append(term)
        if not search_keywords:
            search_keywords.append(enriched_query)

        deduped_keywords = list(dict.fromkeys(search_keywords))[:6]
        search = await _search_kosis_keywords(
            enriched_query,
            deduped_keywords,
            8,
            self.api_key,
            used_routing=bool(route_terms),
        )
        slot_enrichment = None
        if slot_terms or route_terms:
            slot_enrichment = {
                "slot_terms": slot_terms,
                "route_search_terms": route_terms,
                "최종검색어": search.get("사용된_검색어", []),
            }
        intents = route_payload.get("intents") or []
        tool_hints = self._tool_routing_hints(query, intents)
        next_steps = [
            "후보 통계표의 기준시점, 단위, 분류코드, 분모를 확인합니다.",
            "동일 기준으로 시계열·비교·비중·증가율 계산을 수행합니다.",
            "표/그래프/해석/유의사항을 함께 응답합니다.",
        ]
        if tool_hints:
            next_steps = [
                "이 의도는 전용 도구로 바로 처리 가능 — 아래 추천_도구_호출 참고",
                *next_steps,
            ]
        return {
            "상태": "needs_table_selection",
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "답변유형": "search_and_plan",
            "질문": query,
            "answer": (
                "이 질문은 여러 통계표·분류코드·산식이 필요한 복합 질의입니다. "
                "아래 후보 통계표 중 적합한 표를 선택한 뒤 실제 수치 계산을 진행해야 합니다."
            ),
            "의도": intents,
            "슬롯": route_payload["slots"],
            "실행계획": route_payload["analysis_plan"],
            "검증": route_payload["validation"],
            "검색결과": search.get("결과", []),
            "사용된_검색어": search.get("사용된_검색어", []),
            "검색어_슬롯보강": slot_enrichment,
            "추천_도구_호출": tool_hints,
            "다음단계": next_steps,
            "route": route_payload["route"],
        }

    def _extract_correlation_pair(self, query: str) -> tuple[Optional[str], Optional[str]]:
        """Identify exactly two Tier-A stat keys mentioned in the query.

        Returns (None, None) when fewer than two distinct, verified
        keys are found — that ambiguous case stays on the safe
        hint-only path instead of guessing which two stats to feed to
        correlate_stats.

        Longer descriptions are matched first so '고령인구' wins over
        '인구', then the matched substring is removed so the second
        scan does not re-count overlapping tokens."""
        if not query:
            return (None, None)
        q_norm = self._norm(query)
        remaining = q_norm
        candidates: list[tuple[str, str]] = []
        for key, param in TIER_A_STATS.items():
            if param.verification_status not in {"verified", "needs_check"}:
                continue
            description = self._norm(param.description)
            display_key = self._norm(key.replace("_", " "))
            candidates.append((key, max(description, display_key, key=len)))
        candidates.sort(key=lambda kv: -len(kv[1]))

        found: list[str] = []
        for key, marker in candidates:
            if not marker:
                continue
            if marker in remaining and key not in found:
                found.append(key)
                remaining = remaining.replace(marker, " ", 1)
                if len(found) >= 2:
                    break
        if len(found) < 2:
            return (None, None)
        return (found[0], found[1])

    async def _answer_auto_correlate(
        self,
        query: str,
        stat_x: str,
        stat_y: str,
        region: str,
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        years_match = re.search(r"최근\s*(\d+)\s*년", query)
        years = int(years_match.group(1)) if years_match else 10
        result = await correlate_stats(stat_x, stat_y, region, years, self.api_key)
        if not isinstance(result, dict) or "오류" in result:
            return await self._answer_search_fallback(query, route_payload)
        notes = list(result.get("주의") or [])
        notes.append("상관계수는 인과관계를 의미하지 않습니다.")
        notes.append(f"자동 위임: answer_query → correlate_stats('{stat_x}', '{stat_y}', region='{region}', years={years})")
        used_period = (
            result.get("종료시점")
            or result.get("end_period")
            or result.get("기간_종료")
            or str(datetime.now().year)
        )
        return {
            "상태": "executed",
            "코드": STATUS_EXECUTED,
            "답변유형": "tier_a_auto_correlation",
            "질문": query,
            "answer": result.get("answer") or result.get("해석") or (
                f"{stat_x}과 {stat_y}의 최근 {years}년 상관관계 분석을 수행했습니다."
            ),
            "변수": [stat_x, stat_y],
            "지역": region,
            "기간": years,
            "used_period": str(used_period),
            "결과": result,
            "추천_시각화": ["chart_correlation"],
            "검증_주의": notes,
            "route": {
                **route_payload.get("route", {}),
                "delegated_to": "correlate_stats",
            },
            "출처": "통계청 KOSIS",
        }

    def _tool_routing_hints(self, query: str, intents: list[str]) -> list[str]:
        """Cross-tool routing — when the search fallback fires for an
        intent that has a dedicated MCP tool, return concrete call
        suggestions instead of just listing candidate tables."""
        hints: list[str] = []
        if "STAT_CORRELATION" in intents:
            hints.append(
                "correlate_stats(stat_x='<지표A>', stat_y='<지표B>', region='전국', years=10) — "
                "Pearson/Spearman 자동 계산. 상관관계 의도는 이 도구가 정답이며, "
                "answer_query는 후보 표 선택 단계에서 멈추는 안전 폴백입니다."
            )
        if "STAT_REGRESSION" in intents:
            hints.append(
                "correlate_stats 결과의 상관계수와 함께, stat_time_compare로 시점 변화를 보조 분석. "
                "회귀계수 자체는 별도 분석(Python statsmodels 등) 필요."
            )
        if "STAT_OUTLIER_DETECTION" in intents:
            hints.append(
                "detect_outliers(query='<지표>', region='<지역>', years=12) — Z-score 기반 이상치 탐지."
            )
        if "STAT_FORECAST" in intents:
            hints.append(
                "forecast_stat(query='<지표>', region='<지역>', years=10, horizon=3) — 단순 외삽 예측."
            )
        if "POLICY_EFFECT_ANALYSIS" in intents:
            hints.append(
                "정책효과는 단일 호출로 답할 수 없음. correlate_stats + stat_time_compare로 "
                "before/after 구간을 따로 비교하되 인과관계 단정은 금물."
            )
        return hints

    async def answer(self, query: str, region: str = "전국") -> dict[str, Any]:
        route_payload = self._route_payload(query)
        result = await self._dispatch(query, region, precomputed_route=route_payload)
        return self._finalize_response(result, query=query, route_payload=route_payload)

    async def _dispatch(
        self,
        query: str,
        region: str,
        precomputed_route: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        route_payload = precomputed_route or self._route_payload(query)
        plan = AnswerPlanner(self).build(query, region, route_payload)
        return await self._execute_plan(query, route_payload, plan)

    async def _execute_plan(
        self,
        query: str,
        route_payload: dict[str, Any],
        plan: AnswerPlan,
    ) -> dict[str, Any]:
        params = plan.params
        direct_key = plan.direct_key

        if plan.action == "mixed_population":
            return await self._answer_self_employed_sme_population_warning(query, plan.region)
        if plan.action == "sme_large_sales":
            return await self._answer_sme_large_sales(query, plan.region)
        if plan.action == "sme_smallbiz_counts":
            return await self._answer_sme_smallbiz_counts(query, plan.region)
        if plan.action == "sme_employee_average":
            return await self._answer_sme_employee_average(query, plan.region)
        if plan.action == "dynamic_ratio":
            return await self._answer_dynamic_ratio_advisory(query, route_payload)
        if plan.action == "auto_correlate":
            return await self._answer_auto_correlate(
                query,
                params["stat_x"],
                params["stat_y"],
                plan.region,
                route_payload,
            )
        if plan.action == "composite_aggregate" and direct_key:
            return await self._answer_composite_aggregate(
                query,
                direct_key,
                params["composite"],
                operation=params["operation"],
            )
        if plan.action == "region_sum" and direct_key:
            return await self._answer_region_sum(query, direct_key, params["regions"])
        if plan.action == "top_n" and direct_key:
            return await self._answer_top_n(
                query,
                direct_key,
                params["top_n"],
                include_share_ratio=params["include_share_ratio"],
            )
        if plan.action == "region_compare" and direct_key:
            return await self._answer_region_compare(query, direct_key)
        if plan.action == "share_ratio" and direct_key:
            return await self._answer_share_ratio(query, plan.region, direct_key)
        if plan.action == "direct" and direct_key:
            if not route_payload["route"].get("direct_stat_key"):
                route_payload["route"]["direct_stat_key"] = direct_key
            return await self._answer_direct(query, plan.region, direct_key, route_payload)
        return await self._answer_search_fallback(query, route_payload)

    @staticmethod
    def _period_age_years(period: str) -> Optional[float]:
        if not period:
            return None
        text = str(period)
        m = re.match(r"^(\d{4})(\d{2})?", text)
        if not m:
            return None
        now = datetime.now()
        year = int(m.group(1))
        if m.group(2):
            month = int(m.group(2))
            age = (now.year - year) + (now.month - month) / 12.0
        else:
            age = float(now.year - year)
        return round(age, 2)

    @classmethod
    def _extract_used_period(cls, result: dict[str, Any]) -> Optional[str]:
        comparison = result.get("비교") or {}
        end = (comparison.get("종료") or {}).get("시점")
        if end:
            return str(end)
        rows = result.get("표") or []
        for row in rows:
            if isinstance(row, dict):
                period = row.get("시점")
                if period:
                    return str(period)
        period = result.get("시점")
        if period:
            return str(period)
        return None

    _INTENT_FULFILLMENT: dict[str, frozenset[str]] = {
        "STAT_RANKING": frozenset({
            "tier_a_top_n", "tier_a_top_n_share_ratio",
            "tier_a_region_comparison", "tier_a_region_sum",
        }),
        "STAT_SHARE_RATIO": frozenset({
            "tier_a_share_ratio", "tier_a_composite_comparison",
            "tier_a_composite", "tier_a_composite_share_ratio",
            "tier_a_top_n_share_ratio",
        }),
        "STAT_GROWTH_RATE": frozenset({
            "tier_a_growth_rate", "tier_a_trend",
        }),
        "STAT_TIME_SERIES": frozenset({
            "tier_a_trend", "tier_a_growth_rate",
        }),
        "STAT_AVERAGE": frozenset({
            "tier_a_composite_calculation", "tier_a_composite",
        }),
        "STAT_COMPARISON": frozenset({
            "tier_a_region_comparison", "tier_a_composite_comparison",
            "tier_a_composite_share_ratio", "tier_a_top_n",
            "tier_a_top_n_share_ratio", "tier_a_region_sum",
        }),
    }

    _YEAR_MISMATCH_ANSWER_TYPES: frozenset[str] = frozenset({
        "tier_a_value", "tier_a_growth_rate", "tier_a_share_ratio",
    })

    _INTENT_DROPPED_DIMENSIONS: dict[str, tuple[str, ...]] = {
        "STAT_RANKING": ("ranking",),
        "STAT_SHARE_RATIO": ("share_ratio",),
        "STAT_GROWTH_RATE": ("growth_rate",),
        "STAT_TIME_SERIES": ("time_series",),
        "STAT_AVERAGE": ("average",),
        "STAT_COMPARISON": ("comparison",),
    }

    @classmethod
    def _intent_execution_warnings(
        cls,
        result: dict[str, Any],
        query: str,
        route_payload: Optional[dict[str, Any]],
    ) -> list[str]:
        if not route_payload:
            return []
        warnings: list[str] = []
        answer_type = str(result.get("답변유형") or "")
        intents = route_payload.get("intents") or []
        slots = route_payload.get("slots") or {}

        for intent_label, fulfilling_types in cls._INTENT_FULFILLMENT.items():
            if intent_label in intents and answer_type not in fulfilling_types:
                warnings.append(
                    f"의도 {intent_label} 감지됐으나 응답 유형은 {answer_type or '미지정'} — "
                    "단일값/요약에 그쳤을 수 있으니 시도별 비교나 비중 계산을 별도 요청 필요"
                )

        if cls._is_aggregation_question(query) and answer_type not in {
            "tier_a_region_sum", "tier_a_composite_share_ratio", "tier_a_top_n_share_ratio",
        }:
            warnings.append(
                f"합계/합산 키워드 감지됐으나 응답 유형은 {answer_type or '미지정'} — "
                "두 번째 지역을 인식하지 못해 단일 지역 값으로 폴백했을 수 있음"
            )

        comparison_targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if isinstance(comparison_targets, list) and comparison_targets and answer_type in {
            "tier_a_value", "tier_a_trend", "tier_a_growth_rate"
        }:
            warnings.append(
                f"비교 대상 {comparison_targets} 이(가) 감지됐으나 응답은 단일 대상 — "
                "비교/합산이 누락됐을 수 있음"
            )

        years_in_query = [y for y in re.findall(r"(19\d{2}|20\d{2})", query)]
        used_period = result.get("used_period")
        if (
            years_in_query
            and used_period
            and answer_type in cls._YEAR_MISMATCH_ANSWER_TYPES
        ):
            used_str = str(used_period)
            if not any(used_str.startswith(y) for y in years_in_query):
                warnings.append(
                    f"질문에 명시된 연도 {years_in_query} 와 사용 시점 {used_str} 불일치 — "
                    "요청 기간을 만족하지 못했을 수 있음"
                )

        direct_key = str((result.get("route") or {}).get("direct_stat_key") or "")
        q_compact = re.sub(r"\s+", "", query)
        asks_enterprises = bool(re.search(r"기업\s*수", query)) and "사업체" not in q_compact
        if asks_enterprises and "사업체수" in direct_key:
            warnings.append(
                "쿼리 어휘 '기업 수' → 매핑 통계 '사업체 수' (모집단 다름) — "
                "법인 단위 기업체 수와 사업체 수는 통계 작성 기준이 다릅니다"
            )
        asks_establishments = "사업체수" in q_compact or "사업체 수" in query
        if asks_establishments and "_기업수" in direct_key:
            warnings.append(
                "쿼리 어휘 '사업체 수' → 매핑 통계 '기업 수' (모집단 다름)"
            )
        return warnings

    @classmethod
    def _fulfillment_gap(
        cls,
        result: dict[str, Any],
        query: str,
        route_payload: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not route_payload:
            return None
        answer_type = str(result.get("답변유형") or "")
        intents = route_payload.get("intents") or []
        slots = route_payload.get("slots") or {}
        q_compact = re.sub(r"\s+", "", query)

        dropped: list[str] = []
        reasons: list[str] = []
        for intent_label, fulfilling_types in cls._INTENT_FULFILLMENT.items():
            if intent_label not in intents or answer_type in fulfilling_types:
                continue
            for dim in cls._INTENT_DROPPED_DIMENSIONS.get(intent_label, ()):
                dropped.append(dim)
            reasons.append(f"{intent_label} 의도가 {answer_type or '미지정'} 응답으로 완전히 충족되지 않음")

        comparison_targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if comparison_targets and answer_type in {"tier_a_value", "tier_a_trend", "tier_a_growth_rate", "tier_a_composite"}:
            dropped.append("comparison")
            reasons.append(f"비교 대상 {comparison_targets} 이(가) 단일 응답으로 축소됨")

        age_requested = (
            bool(re.search(r"\d+\s*[-~]\s*\d+\s*세", query))
            or any(term in q_compact for term in ("청년", "연령별", "연령", "나이"))
        )
        if age_requested and answer_type not in {"search_and_plan"}:
            dropped.append("age")
            reasons.append("연령/청년 조건을 만족하는 분류축 호출이 실행되지 않음")

        if cls._is_aggregation_question(query) and answer_type not in {
            "tier_a_region_sum", "tier_a_composite_share_ratio", "tier_a_top_n_share_ratio",
        }:
            dropped.append("aggregation")
            reasons.append("합계/합산 의도가 단일값 또는 부분 집계로 축소됨")

        dropped = list(dict.fromkeys(dropped))
        if not dropped:
            return None
        return {
            "이행_상태": "partial",
            "status": "partial",
            "누락_차원": dropped,
            "dropped_dimensions": dropped,
            "부분충족_사유": list(dict.fromkeys(reasons)),
        }

    @staticmethod
    def _pick_josa_eun_neun(word: str) -> str:
        """Return 은 or 는 based on final-syllable batchim. Falls back to
        는 for non-Hangul tails (English, digits, parentheses, …)."""
        if not word:
            return "는"
        last = word[-1]
        codepoint = ord(last)
        if 0xAC00 <= codepoint <= 0xD7A3:
            return "은" if (codepoint - 0xAC00) % 28 != 0 else "는"
        return "는"

    @classmethod
    def _polish_answer_text(cls, text: Optional[str]) -> Optional[str]:
        """Smooth common surface-level fragments in answer strings:
        - X은(는) → X은 or X는 by Korean batchim
        - YYYY.MM (raw monthly period) → YYYY년 M월
        - YYYY.QQ patterns left alone (quarter labels already humane)
        - Collapses 년년 / 월월 artifacts the substitutions can leave.
        - Appends human-readable unit conversion in parentheses after
          KOSIS canonical units (천명, 명, 개, 대, 건, 억원, 십억원, 천달러)."""
        if not text:
            return text
        def fix_josa(m: re.Match) -> str:
            word = m.group(1)
            return f"{word}{cls._pick_josa_eun_neun(word)}"
        text = re.sub(r"(\S+?)은\(는\)", fix_josa, text)
        text = re.sub(
            r"(?<!\d)(19\d{2}|20\d{2})\.(0[1-9]|1[0-2]|[1-9])(?!\d)",
            lambda m: f"{m.group(1)}년 {int(m.group(2))}월",
            text,
        )
        text = re.sub(r"년년", "년", text)
        text = re.sub(r"월월", "월", text)
        text = re.sub(
            r"(\d[\d,]*(?:\.\d+)?)\s*(천명|십억원|천달러|억원|명|개|대|건)(?!\s*\()",
            cls._append_humanized_unit,
            text,
        )
        return text

    @staticmethod
    def _humanize_value(value: float, unit: str) -> Optional[str]:
        """KOSIS canonical units → reader-friendly Korean expression.

        Returns None when no humanization is warranted (e.g. a 천명
        value below 10 thousand people just becomes more confusing in
        a different form)."""
        if unit == "천명":
            people = value * 1000
            if people >= 1e8:
                return f"약 {people / 1e8:.2f}억 명"
            if people >= 1e4:
                return f"약 {round(people / 1e4):,}만 명"
            return None
        if unit in {"명", "개", "대", "건"}:
            if value >= 1e8:
                return f"약 {value / 1e8:.2f}억 {unit}"
            if value >= 1e5:
                return f"약 {round(value / 1e4):,}만 {unit}"
            return None
        if unit == "억원":
            if value >= 1e4:
                jo = value / 1e4
                return f"약 {jo:,.2f}조원"
            return None
        if unit == "십억원":
            eok = value * 10
            if eok >= 1e4:
                return f"약 {eok / 1e4:,.2f}조원"
            if eok >= 1:
                return f"약 {eok:,.0f}억원"
            return None
        if unit == "천달러":
            usd = value * 1000
            if usd >= 1e12:
                return f"약 {usd / 1e12:.2f}조 달러"
            if usd >= 1e8:
                return f"약 {usd / 1e8:,.0f}억 달러"
            return None
        return None

    @classmethod
    def _append_humanized_unit(cls, m: re.Match) -> str:
        raw_value = m.group(1)
        unit = m.group(2)
        original = m.group(0)
        try:
            value = float(raw_value.replace(",", ""))
        except ValueError:
            return original
        humanized = cls._humanize_value(value, unit)
        if not humanized:
            return original
        return f"{original} ({humanized})"

    @classmethod
    def _finalize_response(
        cls,
        result: dict[str, Any],
        query: Optional[str] = None,
        route_payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(result, dict):
            return result

        if isinstance(result.get("answer"), str):
            result["answer"] = cls._polish_answer_text(result["answer"])

        if result.get("상태") != "executed":
            return result
        used = result.get("used_period") or cls._extract_used_period(result)
        notes = list(result.get("검증_주의") or [])
        if used:
            result["used_period"] = str(used)
            age = cls._period_age_years(used)
            if age is not None:
                result["period_age_years"] = age
                if age >= 1.0:
                    warn = f"사용 시점 {used} (약 {age:.1f}년 경과) — 최신 데이터가 아닐 수 있음"
                    if warn not in notes:
                        notes.append(warn)

        if query is not None:
            mismatch_warnings = cls._intent_execution_warnings(result, query, route_payload)
            for w in mismatch_warnings:
                if w not in notes:
                    notes.append(w)
            gap = cls._fulfillment_gap(result, query, route_payload)
            if gap:
                result.update(gap)

        if notes:
            result["검증_주의"] = notes
        return result


# ============================================================================
# SVG 차트 생성 (외부 라이브러리 없이)
# ============================================================================

def _svg_header(w: int = 640, h: int = 380) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="sans-serif">'
    )


def _svg_to_image(svg: str) -> TextContent:
    """Wrap an SVG payload in a MCP-compatible content block.

    The MCP spec accepts ImageContent only for raster mime types
    (image/png, image/jpeg, image/gif, image/webp). Claude Desktop and
    Claude Code clients reject image/svg+xml outright, which used to
    break every chart tool with a content-format error.

    We now emit the SVG inside a fenced ```svg block as TextContent.
    Web embeds that render markdown+SVG (claude.ai, browser MCP
    clients) still show the chart; CLI clients see the markup as code,
    which is far better than a hard failure."""
    return TextContent(type="text", text=f"```svg\n{svg}\n```")


def _chart_line_svg(
    series: list[tuple[str, float]],
    title: str, ylabel: str = "",
    source: str = "", note: str = "",
) -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 60

    if not series:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    labels = [s[0] for s in series]
    values = [s[1] for s in series]
    vmin, vmax = min(values), max(values)
    if vmin == vmax:
        vmin, vmax = vmin - 1, vmax + 1
    span = vmax - vmin
    plot_w = W - PL - PR
    plot_h = H - PT - PB

    def x(i):
        if len(values) == 1:
            return PL + plot_w / 2
        return PL + i * plot_w / (len(values) - 1)

    def y(v):
        return PT + plot_h - (v - vmin) / span * plot_h

    points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))

    y_ticks = []
    for i in range(5):
        v = vmin + span * i / 4
        py = y(v)
        y_ticks.append(
            f'<line x1="{PL}" y1="{py:.1f}" x2="{W-PR}" y2="{py:.1f}" stroke="#eee" stroke-width="0.5"/>'
            f'<text x="{PL-8}" y="{py+4:.1f}" text-anchor="end" font-size="10" fill="#666">{_format_number(v)}</text>'
        )

    step = max(1, len(labels) // 8)
    x_labels = []
    for i, lab in enumerate(labels):
        if i % step == 0 or i == len(labels) - 1:
            x_labels.append(
                f'<text x="{x(i):.1f}" y="{H-PB+18}" text-anchor="middle" font-size="10" fill="#666">{lab}</text>'
            )

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        *y_ticks,
        f'<polyline fill="none" stroke="#2563eb" stroke-width="2" points="{points}"/>',
    ]
    for i, v in enumerate(values):
        parts.append(f'<circle cx="{x(i):.1f}" cy="{y(v):.1f}" r="3" fill="#2563eb"/>')
    parts.extend(x_labels)
    if ylabel:
        parts.append(
            f'<text x="14" y="{H//2}" text-anchor="middle" font-size="10" fill="#666" '
            f'transform="rotate(-90,14,{H//2})">{ylabel}</text>'
        )
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    if note:
        parts.append(f'<text x="{W-PR}" y="{H-8}" text-anchor="end" font-size="9" fill="#888">{note}</text>')
    parts.append('</svg>')
    return "".join(parts)


def _chart_bar_svg(items: list[tuple[str, float]], title: str, source: str = "") -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 80

    if not items:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    values = [s[1] for s in items]
    vmin = min(0, min(values))
    vmax = max(values)
    if vmin == vmax:
        vmax = vmin + 1
    span = vmax - vmin

    plot_w = W - PL - PR
    plot_h = H - PT - PB
    n = len(items)
    bar_w = plot_w / n * 0.7
    gap = plot_w / n * 0.3

    def y(v):
        return PT + plot_h - (v - vmin) / span * plot_h

    def x_pos(i):
        return PL + i * plot_w / n + gap / 2

    y_ticks = []
    for i in range(5):
        v = vmin + span * i / 4
        py = y(v)
        y_ticks.append(
            f'<line x1="{PL}" y1="{py:.1f}" x2="{W-PR}" y2="{py:.1f}" stroke="#eee" stroke-width="0.5"/>'
            f'<text x="{PL-8}" y="{py+4:.1f}" text-anchor="end" font-size="10" fill="#666">{_format_number(v)}</text>'
        )

    bars = []
    for i, (lab, v) in enumerate(items):
        bx = x_pos(i)
        by = y(max(v, 0))
        bh = abs(y(v) - y(0))
        bars.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="#2563eb"/>'
            f'<text x="{bx+bar_w/2:.1f}" y="{H-PB+18}" text-anchor="middle" font-size="10" fill="#666" '
            f'transform="rotate(-30,{bx+bar_w/2:.1f},{H-PB+18})">{lab}</text>'
        )

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        *y_ticks, *bars,
    ]
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _chart_scatter_svg(
    points: list[tuple[float, float]],
    title: str, xlabel: str = "", ylabel: str = "",
    source: str = "", r_value: Optional[float] = None,
) -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 60

    if not points:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    if xmin == xmax:
        xmin, xmax = xmin - 1, xmax + 1
    if ymin == ymax:
        ymin, ymax = ymin - 1, ymax + 1
    xspan, yspan = xmax - xmin, ymax - ymin
    plot_w = W - PL - PR
    plot_h = H - PT - PB

    def xp(v):
        return PL + (v - xmin) / xspan * plot_w

    def yp(v):
        return PT + plot_h - (v - ymin) / yspan * plot_h

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        f'<line x1="{PL}" y1="{H-PB}" x2="{W-PR}" y2="{H-PB}" stroke="#888" stroke-width="0.5"/>',
        f'<line x1="{PL}" y1="{PT}" x2="{PL}" y2="{H-PB}" stroke="#888" stroke-width="0.5"/>',
    ]
    for x, y in points:
        parts.append(f'<circle cx="{xp(x):.1f}" cy="{yp(y):.1f}" r="4" fill="#2563eb" opacity="0.65"/>')

    if len(points) >= 2:
        slope, intercept, *_ = scipy_stats.linregress(xs, ys)
        x1, x2 = xmin, xmax
        y1, y2 = slope * x1 + intercept, slope * x2 + intercept
        parts.append(
            f'<line x1="{xp(x1):.1f}" y1="{yp(y1):.1f}" x2="{xp(x2):.1f}" y2="{yp(y2):.1f}" '
            f'stroke="#dc2626" stroke-width="1.5" stroke-dasharray="5,3"/>'
        )

    if xlabel:
        parts.append(f'<text x="{W//2}" y="{H-30}" text-anchor="middle" font-size="11" fill="#444">{xlabel}</text>')
    if ylabel:
        parts.append(
            f'<text x="18" y="{H//2}" text-anchor="middle" font-size="11" fill="#444" '
            f'transform="rotate(-90,18,{H//2})">{ylabel}</text>'
        )
    if r_value is not None:
        parts.append(
            f'<text x="{W-PR-10}" y="{PT+18}" text-anchor="end" font-size="11" fill="#dc2626" font-weight="600">'
            f'r = {r_value:.3f}</text>'
        )
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    parts.append("</svg>")
    return "".join(parts)


# ============================================================================
# MCP 서버 — 도구 정의
# ============================================================================

mcp = FastMCP("kosis-analysis")


# ---- L1: Quick Layer ----

_QUICK_STAT_SUPPORTED_PARAMS = frozenset({"query", "region", "period", "api_key"})


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


@mcp.tool()
async def quick_stat(
    query: str, region: str = "전국", period: str = "latest",
    api_key: Optional[str] = None,
    **unsupported: Any,
) -> dict:
    """[⚡] 자연어로 통계 단일값 즉시 조회.

    동작 순서:
      1. Tier A 정밀 매핑 발견 시 → 즉시 호출
      2. Tier B 라우팅 힌트 발견 시 → 추천 검색어로 KOSIS 검색
      3. 폴백: 원본 query로 KOSIS 검색

    Args:
        query: 통계 키워드 ("인구", "실업률", "중소기업 사업체수")
        region: 17개 시도명 (기본 "전국"). 영문·풀네임도 자동 정규화
                (Seoul, 서울특별시, 서울시 → 서울).
        period: "latest" 또는 "2023", "2023.03", "작년", "올해"

    지원하지 않는 파라미터(예: industry, scale, sector)는 응답의
    `⚠️ 무시된_파라미터` 필드에 노출됩니다. 해당 슬라이싱은 자연어
    `query`에 키워드를 포함하거나 search_kosis로 통계표를 직접
    선택해야 합니다.
    """
    ignored_params = sorted(k for k in unsupported if k not in _QUICK_STAT_SUPPORTED_PARAMS)
    result = await _quick_stat_core(query, region, period, api_key)
    return _attach_ignored_params(result, ignored_params, "quick_stat")


async def _quick_stat_core(
    query: str, region: str = "전국", period: str = "latest",
    api_key: Optional[str] = None,
) -> dict:
    key = _resolve_key(api_key)
    param = _lookup_quick(query)
    query_region = _extract_single_region_from_query(query)
    if region == "전국" and query_region:
        region = query_region
    query_year = _extract_single_year_from_query(query)
    if _is_latest_period_text(period) and query_year:
        period = query_year
    canonical = _canonical_region(region) or region

    # === Tier A 히트: 즉시 호출 ===
    if param:
        unsupported_dimensions = _quick_stat_unsupported_dimensions(query)
        if unsupported_dimensions:
            return _unsupported_quick_stat_response(
                query,
                param,
                unsupported_dimensions,
                canonical,
                period,
            )

        # broken 상태는 호출 시도조차 안 함 — 사용자에게 명확히 알리고 폴백
        if param.verification_status == "broken":
            hints = _routing_hints(query)
            return {
                "결과": f'⚠️ Tier A 매핑 "{query}"는 KOSIS에서 호출 실패 상태로 표시됨',
                "사유": param.note or "검증 실패",
                "권고": "Tier B 라우팅 결과로 폴백합니다.",
                "추천_검색어": hints[:5] if hints else [],
                "사용자_조치": (
                    f'KOSIS 사이트(kosis.kr)에서 "{param.tbl_nm}"을 직접 검색해 '
                    f'올바른 통계표 ID·항목 ID를 찾은 후 kosis_curation.py 수정 필요'
                ),
            }

        # 미검증 통계표면 경고 메시지 첨부 (broken 제외, needs_check만)
        verification_warning = None
        if param.verification_status != "verified":
            verification_warning = (
                f"⚠️ 이 통계표는 검증되지 않았거나 파라미터 보정이 필요합니다 "
                f"(status: {param.verification_status}). 사유: {param.note or '미상'}"
            )

        region_code = None
        if param.region_scheme:
            region_code = param.region_scheme.get(canonical)
            if not region_code:
                return {
                    "오류": f'지역 "{region}" 이 통계에서 미지원',
                    "정규화_지역": canonical if canonical != region else None,
                    "지원_지역": list(param.region_scheme.keys()),
                    "통계표": param.tbl_nm,
                }
        elif canonical != "전국":
            return {
                "오류": f'이 통계는 지역별 조회가 검증되지 않았습니다: "{region}"',
                "지원_지역": ["전국"],
                "통계표": param.tbl_nm,
                "권고": "지역별 값으로 포장하지 않도록 차단했습니다. search_kosis로 지역 분류가 있는 통계표를 먼저 확인하세요.",
            }
        region = canonical

        period_type = _default_period_type(param)
        effective_period = "latest" if _is_latest_period_text(period) else period
        latest_alias = str(period) if str(period) != "latest" and effective_period == "latest" else None
        range_start = _extract_open_start_year(effective_period)
        if effective_period != "latest" and range_start:
            years_hint = max(1, datetime.now().year - int(range_start) + 1)
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_RANGE_REQUESTED,
                "오류": f'기간 "{period}"은 단일 시점이 아니라 열린 범위 요청입니다.',
                "answer": (
                    f"'{period}'은 {range_start}년부터 최신까지의 시계열 요청으로 해석됩니다. "
                    "quick_stat은 단일값 도구이므로 최신값으로 대체하지 않고 중단했습니다."
                ),
                "통계표": param.tbl_nm,
                "요청_기간": period,
                "해석된_시작시점": range_start,
                "추천_도구_호출": [
                    f"quick_trend('{query}', region='{region}', years={years_hint})",
                    f"answer_query('{range_start}년부터 {query} 추이')",
                ],
            }
        start_period, end_period = _period_bounds(effective_period, period_type)
        precision_downgrade = _detect_precision_downgrade(effective_period, period_type)
        half_year_advisory = _detect_half_year_request(effective_period)
        if effective_period != "latest" and not start_period:
            return {
                "오류": f'기간 "{period}"을(를) 해석할 수 없습니다.',
                "통계표": param.tbl_nm,
                "지원_기간유형": period_type,
            }
        async with httpx.AsyncClient() as client:
            try:
                data = await _fetch_series(
                    client, key, param, region_code,
                    period_type=period_type,
                    start_year=start_period, end_year=end_period,
                    latest_n=1 if not start_period else None,
                )
            except RuntimeError as e:
                return {"오류": str(e), "통계표": param.tbl_nm, "권고": verification_warning}

        if not data:
            relative_hint = None
            normalized = re.sub(r"\s+", "", str(period or ""))
            if any(term in normalized for term in ("작년", "지난해", "전년", "재작년", "올해", "금년")):
                resolved_year = _parse_year_token(period)
                if resolved_year:
                    relative_hint = (
                        f"'{period}' → {resolved_year}년으로 해석했지만 이 통계표의 수록 시점에 "
                        f"포함되지 않음. 최신값을 원하면 period='latest'를 사용하세요."
                    )
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "결과": "데이터 없음",
                "answer": (
                    f"요청한 기간 '{period}'의 {param.description} 데이터가 KOSIS 통계표에 없습니다. "
                    "최신값으로 대체하지 않고 중단했습니다."
                ),
                "통계표": param.tbl_nm,
                "요청_기간": period,
                "해석된_기간": [start_period, end_period],
                "권고": verification_warning,
                "⚠️ 정밀도_다운그레이드": precision_downgrade,
                "⚠️ 기간_해석": relative_hint,
            }

        data.sort(key=lambda r: str(r.get("PRD_DE") or ""))
        row = data[-1]
        period_label = _format_period_label(row.get("PRD_DE"), period_type)
        used_period = str(row.get("PRD_DE") or "")
        age = NaturalLanguageAnswerEngine._period_age_years(used_period)
        answer_text = NaturalLanguageAnswerEngine._polish_answer_text(
            f"{period_label} {region}의 {param.description}은(는) "
            f"{_format_display_number(row.get('DT'), param.display_decimals)} {param.unit}입니다."
        )
        result = {
            "answer": answer_text,
            "값": row.get("DT"), "단위": param.unit,
            "시점": row.get("PRD_DE"),
            "used_period": used_period,
            "period_age_years": age,
            "지역": region, "통계표": param.tbl_nm,
            "출처": "통계청 KOSIS",
        }
        if age is not None and age >= 1.0:
            result["⚠️ 데이터_신선도"] = (
                f"사용 시점 {used_period} (약 {age:.1f}년 경과) — 최신 데이터가 아닐 수 있음. "
                f"수록기간 메타는 explore_table('{param.org_id}', '{param.tbl_id}')로 확인하세요."
            )
        if verification_warning:
            result["⚠️ 검증_상태"] = verification_warning
        if precision_downgrade:
            result["⚠️ 정밀도_다운그레이드"] = precision_downgrade
        if half_year_advisory:
            result["⚠️ 상하반기"] = half_year_advisory
        if latest_alias:
            result["⚠️ 기간_해석"] = f"'{latest_alias}' → latest로 해석"
        # Population-mismatch warning previously fired only through
        # answer_query's _finalize_response. Direct quick_stat callers
        # were getting silent label substitution — close that gap so
        # the trail is consistent across both entry points.
        q_compact = re.sub(r"\s+", "", str(query))
        asks_enterprises = bool(re.search(r"기업\s*수", query)) and "사업체" not in q_compact
        param_describes_sites = "사업체" in (param.description or "")
        if asks_enterprises and param_describes_sites:
            result["⚠️ 모집단_불일치"] = (
                "쿼리 어휘 '기업 수' → 매핑 통계 '사업체 수' (모집단 다름) — "
                "법인 단위 기업체 수와 사업체 수는 통계 작성 기준이 다릅니다"
            )
        return result

    # === Tier B 라우팅: 추천 검색어로 폴백 ===
    hints = _routing_hints(query)
    search_keywords = hints[:3] if hints else [query]

    async with httpx.AsyncClient() as client:
        all_results = []
        for keyword in search_keywords:
            try:
                r = await _kosis_call(client, "statisticsSearch.do", {
                    "method": "getList", "apiKey": key,
                    "searchNm": keyword, "format": "json", "jsonVD": "Y", "resultCount": 3,
                })
                for item in r:
                    item["_검색어"] = keyword
                all_results.extend(r)
            except RuntimeError:
                continue

    # 중복 제거 (통계표 ID 기준)
    seen = set()
    unique = []
    for item in all_results:
        tid = item.get("TBL_ID")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(item)

    return {
        "결과": "Tier A 정밀 매핑 없음. 검색 결과 반환.",
        "사용된_검색어": search_keywords,
        "검색_후보": [
            {
                "통계표": r.get("TBL_NM"),
                "통계표ID": r.get("TBL_ID"),
                "기관ID": r.get("ORG_ID"),
                "검색어": r.get("_검색어"),
                "URL": r.get("LINK_URL") or r.get("TBL_VIEW_URL"),
            }
            for r in unique[:8]
        ],
        "안내": (
            "후보 중 적합한 통계표를 골라서 KOSIS 사이트에서 확인하거나, "
            "더 구체적인 키워드로 다시 시도하세요."
        ),
    }


@mcp.tool()
async def quick_trend(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
    **unsupported: Any,
) -> dict:
    """[⚡] 시계열 데이터 조회 (분석/시각화 입력으로 사용).

    Args:
        query: 통계 키워드
        region: 지역 (영문·풀네임 자동 정규화)
        years: 최근 N년 (기본 10)

    지원하지 않는 파라미터는 quick_stat과 동일하게 응답의
    `⚠️ 무시된_파라미터` 필드에 노출됩니다.
    """
    ignored_params = sorted(k for k in unsupported if k not in {"query", "region", "years", "api_key"})
    result = await _quick_trend_core(query, region, years, api_key)
    return _attach_ignored_params(result, ignored_params, "quick_trend")


async def _quick_trend_core(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
    start_year: Optional[str] = None,
    end_year: Optional[str] = None,
) -> dict:
    key = _resolve_key(api_key)
    param = _lookup_quick(query)
    if not param:
        return {"오류": f'"{query}" 사전 매핑 없음'}
    canonical = _canonical_region(region) or region

    region_code = None
    if param.region_scheme:
        region_code = param.region_scheme.get(canonical)
        if not region_code:
            return {"오류": f'지역 "{region}" 미지원', "지원_지역": list(param.region_scheme.keys())}
    elif canonical != "전국":
        return {"오류": f'"{query}"는 지역별 시계열 조회가 검증되지 않았습니다.', "지원_지역": ["전국"]}
    region = canonical

    period_type = _default_period_type(param)
    latest_count = _latest_count_for_years(years, period_type)
    start_period = end_period = None
    if start_year:
        start_period, _ = _period_bounds(start_year, period_type)
        _, end_period = _period_bounds(end_year or str(datetime.now().year), period_type)

    async with httpx.AsyncClient() as client:
        data = await _fetch_series(
            client,
            key,
            param,
            region_code,
            period_type=period_type,
            start_year=start_period,
            end_year=end_period,
            latest_n=None if start_period else latest_count,
        )

    data.sort(key=lambda r: str(r.get("PRD_DE") or ""))
    series = [{"시점": r.get("PRD_DE"), "값": r.get("DT")} for r in data]
    used_period = str(series[-1]["시점"]) if series else ""
    age = NaturalLanguageAnswerEngine._period_age_years(used_period)
    result = {
        "통계명": param.description, "지역": region, "단위": param.unit,
        "시계열": series,
        "데이터수": len(data), "통계표": param.tbl_nm,
        "used_period": used_period,
        "period_age_years": age,
        "수록주기": period_type,
        "요청_기간_년": years,
        "요청_시점수": latest_count if not start_period else None,
    }
    if start_period:
        result["요청_시작시점"] = start_period
        result["요청_종료시점"] = end_period
    elif period_type in {"M", "Q"}:
        unit_label = "개월" if period_type == "M" else "분기"
        result["⚠️ 기간_해석"] = (
            f"years={years} → {period_type} 주기 통계이므로 최근 {latest_count}개 {unit_label} 시점 조회"
        )
    if age is not None and age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"시계열 최신 시점 {used_period} (약 {age:.1f}년 경과) — "
            "최신 데이터가 아닐 수 있음"
        )
    return result


@mcp.tool()
async def quick_region_compare(
    query: str,
    period: str = "latest",
    sort: str = "desc",
    api_key: Optional[str] = None,
    **unsupported: Any,
) -> dict:
    """[⚡] 지역/시도별 값을 한 번에 비교.

    지역 분류가 검증된 Tier A 통계만 지원합니다. 예:
    "중소기업 사업체수", "소상공인 사업체수", "실업률".
    """
    ignored_params = sorted(k for k in unsupported if k not in {"query", "period", "sort", "api_key"})
    result = await _quick_region_compare_core(query, period, sort, api_key)
    return _attach_ignored_params(result, ignored_params, "quick_region_compare")


async def _quick_region_compare_core(
    query: str, period: str = "latest", sort: str = "desc",
    api_key: Optional[str] = None,
) -> dict:
    key = _resolve_key(api_key)
    param = _lookup_quick(query)
    if not param:
        return {"오류": f'"{query}" 사전 매핑 없음'}
    if not param.region_scheme:
        return {
            "오류": f'"{query}"는 지역별 분류가 검증되지 않았습니다.',
            "통계표": param.tbl_nm,
            "권고": "search_kosis로 지역 분류가 있는 통계표를 먼저 확인하세요.",
        }

    period_type = _default_period_type(param)
    effective_period = "latest" if _is_latest_period_text(period) else period
    start_period, end_period = _period_bounds(effective_period, period_type)
    if effective_period != "latest" and not start_period:
        return {
            "오류": f'기간 "{period}"을(를) 해석할 수 없습니다.',
            "통계표": param.tbl_nm,
            "지원_기간유형": period_type,
        }
    async with httpx.AsyncClient() as client:
        try:
            data = await _fetch_series(
                client,
                key,
                param,
                "ALL",
                period_type=period_type,
                start_year=start_period,
                end_year=end_period,
                latest_n=1 if not start_period else None,
            )
        except RuntimeError as e:
            return {"오류": str(e), "통계표": param.tbl_nm}

    if data:
        latest_period = max(str(row.get("PRD_DE") or "") for row in data)
        data = [row for row in data if str(row.get("PRD_DE") or "") == latest_period]

    code_field, name_field = _region_field_names(param)
    regions_by_code = {code: name for name, code in param.region_scheme.items()}
    allowed_codes = set(regions_by_code)
    rows = []
    for row in data:
        code = row.get(code_field)
        if code not in allowed_codes:
            continue
        region_name = regions_by_code.get(code) or row.get(name_field)
        if not region_name or region_name == "전국":
            continue
        try:
            value = float(str(row.get("DT")).replace(",", ""))
        except (TypeError, ValueError):
            continue
        rows.append({
            "지역": region_name,
            "값": _format_number(value),
            "원값": value,
            "단위": param.unit,
            "시점": row.get("PRD_DE"),
            "통계표": param.tbl_nm,
        })

    reverse = sort != "asc"
    rows.sort(key=lambda r: r["원값"], reverse=reverse)
    latest_period = rows[0]["시점"] if rows else None
    used_period = str(latest_period or "")
    age = NaturalLanguageAnswerEngine._period_age_years(used_period)
    result = {
        "통계명": param.description,
        "시점": latest_period,
        "used_period": used_period,
        "period_age_years": age,
        "단위": param.unit,
        "정렬": "내림차순" if reverse else "오름차순",
        "지역수": len(rows),
        "표": rows,
        "출처": "통계청 KOSIS",
    }
    if age is not None and age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"비교 기준 시점 {used_period} (약 {age:.1f}년 경과) — "
            "최신 데이터가 아닐 수 있음"
        )
    return result


@mcp.tool()
async def daily_term_lookup(daily_term: str) -> dict:
    """[📖] 일상용어/도메인 키워드를 통계 검색어로 변환.

    Tier B 라우팅 사전 (100+ 항목): 중소기업, 소상공인, 업종, 일상어 등.

    예:
      "월세" → ["주택 임대료", "전월세전환율", "주거비"]
      "치킨집" → ["음식점업", "분식 및 김밥 전문점"]
      "BSI" → ["중소기업 경기실사지수", "기업경기실사지수"]
    """
    daily_term = daily_term.strip()

    # 먼저 Tier A 직접 매핑 있는지 확인
    tier_a = _curation_lookup(daily_term)
    if tier_a:
        return {
            "입력": daily_term,
            "정밀_매핑": {
                "통계표": tier_a.tbl_nm,
                "설명": tier_a.description,
                "검증상태": tier_a.verification_status,
            },
            "안내": "Tier A 직접 매핑 발견. quick_stat 또는 quick_trend로 바로 호출 가능.",
        }

    # Tier B 라우팅
    hints = _routing_hints(daily_term)
    return {
        "입력": daily_term,
        "추천_검색어": hints or [daily_term],
        "매핑여부": bool(hints),
        "안내": (
            f'"{daily_term}"에 대한 {len(hints)}개 검색어 제안. '
            f'각 검색어를 search_kosis 또는 quick_stat에 넘겨 시도하세요.'
        ) if hints else f'"{daily_term}" 매핑 없음. KOSIS 통합검색으로 폴백 권장.',
    }


@mcp.tool()
async def browse_topic(topic: Optional[str] = None) -> dict:
    """[📖] 주제별 대표 통계 둘러보기.

    13개 주제: 인구·가구, 고용·노동, 물가·소비, 주거·부동산, 경제·성장,
    중소기업·소상공인, 업종·산업, 금융·재정, 복지·소득, 교육, 보건·의료,
    환경·기후, 지역.
    """
    if not topic:
        return {
            "전체_주제": list(TOPICS.keys()),
            "안내": "각 주제명을 다시 넘겨서 대표 통계 목록을 받으세요.",
        }
    hints = _topic_hints(topic)
    if not hints:
        return {"오류": f'주제 "{topic}" 없음', "가능_주제": list(TOPICS.keys())}
    return {
        "주제": topic,
        "대표_통계": hints,
        "안내": "각 통계명을 search_kosis 또는 quick_stat에 넘겨 호출하세요.",
    }


# ---- L2: Analysis Layer ----

@mcp.tool()
async def analyze_trend(
    query: str, region: str = "전국", years: int = 20,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] 통계적 추세 분석.

    제공:
      - 선형회귀 (기울기, R², p-value)
      - 평균 변화율, 변동성
      - 극값, 최근 변화
    """
    series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result.get("시계열", []))
    if len(values) < 3:
        return {"오류": "분석에 충분한 데이터 없음 (3개 미만)"}

    x = np.arange(len(values))
    y = np.array(values)
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, y)
    r2 = r_value ** 2

    changes = np.diff(y) / np.abs(y[:-1]) * 100
    avg_growth = float(np.mean(changes))
    volatility = float(np.std(changes))

    if p_value < 0.05:
        trend_label = "유의한 증가 추세" if slope > 0 else "유의한 감소 추세"
    elif volatility > 20:
        trend_label = "변동성 큼 (뚜렷한 추세 없음)"
    else:
        trend_label = "안정 (유의한 추세 없음)"

    max_idx = int(np.argmax(y))
    min_idx = int(np.argmin(y))
    recent_change = (
        float((y[-1] - y[-2]) / abs(y[-2]) * 100) if len(y) >= 2 and y[-2] != 0 else 0.0
    )

    return {
        "통계명": series_result.get("통계명"), "지역": region,
        "기간": f"{times[0]} ~ {times[-1]}",
        "데이터수": len(values), "추세_라벨": trend_label,
        "선형회귀": {
            "기울기_연간": round(slope, 4), "R제곱": round(r2, 4),
            "p_value": round(p_value, 4), "유의": p_value < 0.05,
        },
        "변화율": {
            "평균_퍼센트": round(avg_growth, 2),
            "변동성_퍼센트": round(volatility, 2),
            "최근_퍼센트": round(recent_change, 2),
            "최근_구간": {
                "시작": times[-2],
                "끝": times[-1],
            },
        },
        "극값": {
            "최댓값": {"시점": times[max_idx], "값": values[max_idx]},
            "최솟값": {"시점": times[min_idx], "값": values[min_idx]},
        },
        "해석": (
            f"{years}년간 {trend_label}. "
            f"평균 연 {avg_growth:+.2f}% 변화, "
            f"회귀 R²={r2:.2f} (p={p_value:.3f})."
        ),
        "단위": series_result.get("단위"),
    }


@mcp.tool()
async def correlate_stats(
    query_a: str, query_b: str,
    region: str = "전국", years: int = 15,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] 두 통계의 상관관계 분석 (Pearson + Spearman).

    상관 ≠ 인과 면책 자동 첨부.
    """
    a = await quick_trend(query_a, region, years, api_key)
    b = await quick_trend(query_b, region, years, api_key)
    if "오류" in a or "오류" in b:
        return {"오류": "데이터 수집 실패"}

    ta, va = _values_from_series(a["시계열"])
    tb, vb = _values_from_series(b["시계열"])
    common = sorted(set(ta) & set(tb))
    if len(common) < 4:
        return {"오류": f"공통 시점 부족 ({len(common)}개)"}

    da = {t: v for t, v in zip(ta, va)}
    db = {t: v for t, v in zip(tb, vb)}
    aa = [da[t] for t in common]
    bb = [db[t] for t in common]

    pr, pp = scipy_stats.pearsonr(aa, bb)
    sr, sp = scipy_stats.spearmanr(aa, bb)

    def interpret(r: float) -> str:
        absr = abs(r)
        if absr < 0.2: s = "거의 무관"
        elif absr < 0.4: s = "약한 상관"
        elif absr < 0.7: s = "중간 상관"
        else: s = "강한 상관"
        return f"{s} ({'양의' if r > 0 else '음의'})"

    return {
        "통계_A": a.get("통계명"), "통계_B": b.get("통계명"),
        "지역": region, "공통_시점수": len(common),
        "기간": f"{common[0]} ~ {common[-1]}",
        "Pearson": {
            "상관계수": round(pr, 4), "p_value": round(pp, 4),
            "해석": interpret(pr),
        },
        "Spearman": {
            "상관계수": round(sr, 4), "p_value": round(sp, 4),
            "해석": interpret(sr),
        },
        "면책": "상관관계는 인과관계를 의미하지 않습니다.",
        "정합데이터": list(zip(common, aa, bb)),
    }


@mcp.tool()
async def forecast_stat(
    query: str, region: str = "전국",
    history_years: int = 15, horizon: int = 5,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] 선형 외삽 + 95% 신뢰구간 예측.

    Args:
        history_years: 과거 데이터 기간
        horizon: 미래 예측 기간 (년)
    """
    series_result = await quick_trend(query, region, history_years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result["시계열"])
    if len(values) < 4:
        return {"오류": "예측에 데이터 부족"}

    x = np.arange(len(values))
    y = np.array(values)
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, y)
    residuals = y - (slope * x + intercept)
    rmse = float(np.sqrt(np.mean(residuals ** 2)))

    last_year = int(times[-1][:4])
    future_x = np.arange(len(values), len(values) + horizon)
    future_y = slope * future_x + intercept
    ci = 1.96 * rmse

    forecasts = []
    for i, yr in enumerate(range(last_year + 1, last_year + 1 + horizon)):
        forecasts.append({
            "시점": str(yr),
            "예측값": round(float(future_y[i]), 2),
            "하한": round(float(future_y[i] - ci), 2),
            "상한": round(float(future_y[i] + ci), 2),
        })

    return {
        "통계명": series_result.get("통계명"), "지역": region,
        "과거_기간": f"{times[0]} ~ {times[-1]}",
        "예측": forecasts, "모델": "선형회귀 외삽",
        "RMSE": round(rmse, 4), "R제곱": round(r_value ** 2, 4),
        "면책": "단순 추세 외삽. 정책·외부 충격 미반영. 보조 참고용.",
        "단위": series_result.get("단위"),
    }


@mcp.tool()
async def detect_outliers(
    query: str, region: str = "전국", years: int = 20,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] Z-score 기반 이상치 탐지 (|z| > 2.5)."""
    series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return series_result
    times, values = _values_from_series(series_result["시계열"])
    if len(values) < 5:
        return {"오류": "탐지에 데이터 부족"}

    y = np.array(values)
    mean, std = float(np.mean(y)), float(np.std(y))
    if std == 0:
        return {"이상치": [], "안내": "변동성 없음"}

    z_scores = (y - mean) / std
    outliers = [
        {
            "시점": times[i], "값": values[i],
            "z_score": round(float(z), 2),
            "평균_대비_편차": round((values[i] - mean) / mean * 100, 1),
        }
        for i, z in enumerate(z_scores) if abs(z) > 2.5
    ]
    return {
        "통계명": series_result.get("통계명"), "이상치": outliers,
        "평균": round(mean, 2), "표준편차": round(std, 2),
        "방법": "Z-score (|z| > 2.5)",
    }


# ---- L3: Viz Layer ----

@mcp.tool()
async def chart_line(
    query: str, region: str = "전국", years: int = 10,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 시계열 라인 차트 SVG (챗봇에 인라인 렌더링)."""
    s = await quick_trend(query, region, years, api_key)
    if "오류" in s:
        return [TextContent(type="text", text=str(s))]
    times, values = _values_from_series(s["시계열"])
    if not times:
        return [TextContent(type="text", text="데이터 없음")]
    svg = _chart_line_svg(
        list(zip(times, values)),
        title=f"{s.get('통계명')} ({region})",
        ylabel=s.get("단위", ""),
        source=f"KOSIS · {s.get('통계표')}",
        note=f"최근: {times[-1]}",
    )
    return [
        _svg_to_image(svg),
        TextContent(type="text", text=f"{s.get('통계명')} 시계열 — {region}, {len(times)}개 시점"),
    ]


@mcp.tool()
async def chart_compare_regions(
    query: str, regions: list[str], period: str = "latest",
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 지역별 막대 비교 차트.

    Args:
        regions: ["서울", "부산", "대구"] 같은 지역 리스트
    """
    items = []
    for r in regions:
        stat = await quick_stat(query, r, period, api_key)
        if "값" in stat:
            try:
                items.append((r, float(stat["값"])))
            except (ValueError, TypeError):
                continue

    if not items:
        return [TextContent(type="text", text="비교 가능한 데이터 없음")]

    svg = _chart_bar_svg(items, title=f"{query} — 지역 비교", source="KOSIS")
    return [
        _svg_to_image(svg),
        TextContent(type="text", text=f"{query} 지역 비교 ({len(items)}개 지역)"),
    ]


@mcp.tool()
async def chart_correlation(
    query_a: str, query_b: str,
    region: str = "전국", years: int = 15,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 두 통계 산점도 + 회귀선."""
    corr = await correlate_stats(query_a, query_b, region, years, api_key)
    if "오류" in corr:
        return [TextContent(type="text", text=str(corr))]
    aligned = corr.get("정합데이터", [])
    if len(aligned) < 3:
        return [TextContent(type="text", text="데이터 부족")]

    points = [(p[1], p[2]) for p in aligned]
    svg = _chart_scatter_svg(
        points,
        title=f"{corr['통계_A']} vs {corr['통계_B']}",
        xlabel=corr["통계_A"], ylabel=corr["통계_B"],
        source="KOSIS", r_value=corr["Pearson"]["상관계수"],
    )
    summary = (
        f"Pearson r={corr['Pearson']['상관계수']}, "
        f"p={corr['Pearson']['p_value']} ({corr['Pearson']['해석']})"
    )
    return [_svg_to_image(svg), TextContent(type="text", text=summary)]


# ---- Phase 2: 추가 차트 4종 ----

@mcp.tool()
async def chart_heatmap(
    query: str,
    regions: Optional[list[str]] = None,
    years: int = 10,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 지역 × 시점 매트릭스 히트맵.

    각 지역의 시계열을 한 차트에 색상으로 표시. 17개 시도 변화 비교에 유용.

    Args:
        query: 통계 키워드 (Tier A 매핑 필요. 예: "출산율", "실업률")
        regions: 비교할 지역. None이면 17개 시도 전체.
        years: 최근 N년 (기본 10)
    """
    param = _curation_lookup(query)
    if not param:
        return [TextContent(type="text", text=f'"{query}" Tier A 매핑 없음. chart_heatmap은 정밀 매핑된 통계만 지원.')]
    if not param.region_scheme:
        return [TextContent(type="text", text=f'"{query}"는 지역 분류가 없어 히트맵 불가.')]

    # 기본 지역 = 전체 시도 (전국 제외)
    if regions is None:
        regions = [r for r in param.region_scheme.keys() if r != "전국"]

    # 각 지역의 시계열 수집
    all_years: set[str] = set()
    region_data: dict[str, dict[str, float]] = {}
    for r in regions:
        result = await quick_trend(query, r, years, api_key)
        if "오류" in result:
            continue
        times, values = _values_from_series(result.get("시계열", []))
        region_data[r] = dict(zip(times, values))
        all_years.update(times)

    if not region_data or not all_years:
        return [TextContent(type="text", text="히트맵 생성에 충분한 데이터 없음")]

    sorted_years = sorted(all_years)
    # 매트릭스: rows=regions, cols=years
    matrix: list[list[Optional[float]]] = []
    valid_rows: list[str] = []
    for r in regions:
        if r in region_data:
            row = [region_data[r].get(y) for y in sorted_years]
            matrix.append(row)
            valid_rows.append(r)

    svg = chart_heatmap_svg(
        matrix, valid_rows, sorted_years,
        title=f"{param.description} — 지역 × 시점",
        source=f"KOSIS · {param.tbl_nm}",
        unit=param.unit,
    )
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=f"{param.description} 히트맵 — {len(valid_rows)}개 지역 × {len(sorted_years)}년",
        ),
    ]


@mcp.tool()
async def chart_distribution(
    query: str,
    period: str = "latest",
    highlight_regions: Optional[list[str]] = None,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 시도별 값 분포 (히스토그램 + 박스플롯).

    한 시점에서 17개 시도 값이 어떻게 퍼져 있는지. 중앙값/사분위/평균 표시.

    Args:
        query: 통계 키워드
        period: 비교 시점 (기본 "latest")
        highlight_regions: 분포선상에 강조 표시할 지역 (예: ["서울", "부산"])
    """
    param = _curation_lookup(query)
    if not param or not param.region_scheme:
        return [TextContent(type="text", text=f'"{query}" 지역별 분류 통계가 아님')]

    regions = [r for r in param.region_scheme.keys() if r != "전국"]
    values: list[float] = []
    annotations: list[tuple[str, float]] = []
    for r in regions:
        stat = await quick_stat(query, r, period, api_key)
        if "값" in stat:
            try:
                v = float(stat["값"])
                values.append(v)
                if highlight_regions and r in highlight_regions:
                    annotations.append((r, v))
            except (ValueError, TypeError):
                continue

    if len(values) < 5:
        return [TextContent(type="text", text=f"분포 그리기에 데이터 부족 ({len(values)}개)")]

    svg = chart_distribution_svg(
        values,
        title=f"{param.description} — 시도별 분포",
        bins=min(12, len(values)),
        unit=param.unit,
        source=f"KOSIS · {param.tbl_nm}",
        annotation_labels=annotations if annotations else None,
    )

    # 간단한 통계 요약
    mean = sum(values) / len(values)
    sorted_v = sorted(values)
    median = sorted_v[len(sorted_v) // 2]
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"{param.description} 분포 — 시도 {len(values)}개, "
                f"평균 {mean:.2f}, 중앙값 {median:.2f}, "
                f"범위 [{min(values):.2f}, {max(values):.2f}]"
            ),
        ),
    ]


@mcp.tool()
async def chart_dual_axis(
    query_a: str, query_b: str,
    region: str = "전국",
    years: int = 10,
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 두 통계를 단위 다른 축으로 한 차트에 (이중 Y축).

    단위가 다른 두 통계의 시간적 관계를 한눈에. 예: 출산율(명) vs 집값(지수).

    Args:
        query_a: 왼쪽 축 통계 (파란 실선)
        query_b: 오른쪽 축 통계 (빨간 점선)
        region: 같은 지역으로 정합
    """
    a = await quick_trend(query_a, region, years, api_key)
    b = await quick_trend(query_b, region, years, api_key)
    if "오류" in a or "오류" in b:
        return [TextContent(type="text", text=f"데이터 수집 실패: A={a.get('오류','OK')}, B={b.get('오류','OK')}")]

    ta, va = _values_from_series(a["시계열"])
    tb, vb = _values_from_series(b["시계열"])
    series_a = list(zip(ta, va))
    series_b = list(zip(tb, vb))

    if not series_a or not series_b:
        return [TextContent(type="text", text="시계열 데이터 부족")]

    svg = chart_dual_axis_svg(
        series_a, series_b,
        label_a=a.get("통계명", query_a),
        label_b=b.get("통계명", query_b),
        title=f"{a.get('통계명')} vs {b.get('통계명')} ({region})",
        unit_a=a.get("단위", ""),
        unit_b=b.get("단위", ""),
        source="KOSIS",
    )

    common = set(ta) & set(tb)
    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"이중축 비교: {a.get('통계명')} (왼쪽) vs {b.get('통계명')} (오른쪽). "
                f"공통 시점 {len(common)}개. 상관관계는 correlate_stats로 확인."
            ),
        ),
    ]


@mcp.tool()
async def chart_dashboard(
    query: str, region: str = "전국",
    api_key: Optional[str] = None,
) -> list:
    """[🎨] 4분할 종합 대시보드 한 장.

    한 통계를 다각도로: 시계열+예측 / 지역비교 / 핵심지표 / 인사이트.
    `chain_full_analysis`의 데이터를 그래픽으로 정리.

    Args:
        query: 통계 키워드
        region: 시계열의 기준 지역
    """
    param = _curation_lookup(query)
    if not param:
        return [TextContent(type="text", text=f'"{query}" Tier A 매핑 없음')]

    # 시계열
    series_result = await quick_trend(query, region, 15, api_key)
    times, values = _values_from_series(series_result.get("시계열", []))
    timeseries = list(zip(times, values))

    # 추세 분석
    trend = await analyze_trend(query, region, 15, api_key)

    # 예측 (시계열이 충분하면)
    forecast_pts: list[tuple[str, float, float, float]] = []
    if len(values) >= 4:
        forecast = await forecast_stat(query, region, 15, 5, api_key)
        if "예측" in forecast:
            for f in forecast["예측"]:
                forecast_pts.append(
                    (f["시점"], f["예측값"], f["하한"], f["상한"])
                )

    # 지역 비교 (top N)
    items: list[tuple[str, float]] = []
    if param.region_scheme:
        for r in list(param.region_scheme.keys())[:8]:
            if r == "전국":
                continue
            stat = await quick_stat(query, r, "latest", api_key)
            if "값" in stat:
                try:
                    items.append((r, float(stat["값"])))
                except (ValueError, TypeError):
                    continue

    # 핵심 지표 요약
    summary: dict = {}
    if "선형회귀" in trend:
        lr = trend["선형회귀"]
        summary["기울기/년"] = lr.get("기울기_연간")
        summary["R²"] = lr.get("R제곱")
    if "변화율" in trend:
        analysis_period = str(trend.get("기간") or "")
        summary["분석기간"] = f"{analysis_period} · {trend.get('데이터수')}개 시점"
        summary["평균 변화율"] = f"{trend['변화율']['평균_퍼센트']:+.2f}%"
        recent_window = trend["변화율"].get("최근_구간") or {}
        recent_start = str(recent_window.get("시작") or "")
        recent_end = str(recent_window.get("끝") or "")
        recent_label = (
            "최근 변화"
            if recent_start and recent_end else "최근 변화"
        )
        recent_value = f"{trend['변화율']['최근_퍼센트']:+.2f}%"
        if recent_start and recent_end:
            recent_value = f"{recent_start}→{recent_end}: {recent_value}"
        summary[recent_label] = recent_value
    if "극값" in trend:
        max_point = trend["극값"]["최댓값"]
        summary["분석기간 내 최댓값"] = f"{max_point['시점']}: {max_point['값']}"
    summary["해석"] = trend.get("해석", "")

    svg = chart_dashboard_svg(
        title=f"{param.description} ({region}) — 종합",
        timeseries=timeseries,
        items=items,
        summary=summary,
        forecast=forecast_pts if forecast_pts else None,
        unit=param.unit,
        source=f"KOSIS · {param.tbl_nm}",
    )

    return [
        _svg_to_image(svg),
        TextContent(
            type="text",
            text=(
                f"{param.description} 대시보드 — 시계열 {len(timeseries)}개 + "
                f"예측 {len(forecast_pts)}년 + 지역 비교 {len(items)}개. "
                f"추세·요약 분석기간: {trend.get('기간', '확인 불가')}"
            ),
        ),
    ]


# ---- Chain Layer ----

@mcp.tool()
async def chain_full_analysis(
    query: str, region: str = "전국",
    api_key: Optional[str] = None,
) -> list:
    """[⛓] 종합 분석: 통계+추세+예측+이상치+차트 한 번에.

    "출산율 분석해줘", "청년 실업률 봐줘" 같은 요청에 사용.
    """
    latest = await quick_stat(query, region, "latest", api_key)
    if "오류" in latest:
        return [TextContent(type="text", text=str(latest))]

    trend = await analyze_trend(query, region, 20, api_key)
    forecast = await forecast_stat(query, region, 15, 5, api_key)
    outliers = await detect_outliers(query, region, 20, api_key)

    series_result = await quick_trend(query, region, 20, api_key)
    times, values = _values_from_series(series_result.get("시계열", []))
    chart_svg = ""
    if times:
        chart_svg = _chart_line_svg(
            list(zip(times, values)),
            title=f"{trend.get('통계명')} ({region})",
            ylabel=trend.get("단위", ""), source="KOSIS",
        )

    summary = {
        "주제": query, "지역": region,
        "최신값": latest.get("answer"),
        "추세_분석": {
            "라벨": trend.get("추세_라벨"),
            "해석": trend.get("해석"),
        },
        "5년_예측": forecast.get("예측", [])[:5] if "예측" in forecast else None,
        "이상치": outliers.get("이상치", [])[:3],
        "출처": "통계청 KOSIS",
    }

    result = [TextContent(type="text", text=str(summary))]
    if chart_svg:
        result.insert(0, _svg_to_image(chart_svg))
    return result


@mcp.tool()
async def plan_query(query: str) -> dict:
    """[🧭] Gemma용 절차형 KOSIS 분석 계획을 만든다.

    질문을 차원과 작업 단계로 분해합니다.

    MUST NOT:
    - 통계표 ID 확정 (select_table_for_query의 책임)
    - 코드 매핑 (resolve_concepts의 책임)
    - 실제 값 반환 (query_table의 책임)
    - 산술/산식 (compute_indicator의 책임)

    역할은 의도 추출과 워크플로우 제안까지만입니다.
    """
    planner = QueryWorkflowPlanner()
    return planner.build(query)


@mcp.tool()
async def answer_query(
    query: str,
    region: str = "전국",
    api_key: Optional[str] = None,
) -> dict:
    """[🤖][Deprecated for Gemma chatbot manifests] 자연어 질문을 실제 답변 또는 안전한 분석계획으로 생성.

    검증된 Tier A 질문은 KOSIS API를 호출해 수치·표·계산·해석을 반환하고,
    복합/상위어 질문은 실제 KOSIS 검색 후보와 분석계획을 반환한다.
    Gemma 기반 챗봇에서는 silent failure를 줄이기 위해 plan_query →
    select_table_for_query → resolve_concepts → query_table → compute_indicator
    절차형 파이프라인을 우선 사용한다.
    """
    key = _resolve_key(api_key)
    engine = NaturalLanguageAnswerEngine(key)
    try:
        return await engine.answer(query, region)
    except RuntimeError as e:
        return {"상태": "failed", "코드": STATUS_RUNTIME_ERROR, "오류": str(e), "질문": query}


@mcp.tool()
async def verify_stat_claims(answer_payload: dict[str, Any]) -> dict:
    """[✅] answer_query 결과의 수치·출처·산식 검증 상태를 점검.

    실제 원자료를 재호출하지 않고, 챗봇 응답 payload가 최소한의 통계 응답
    요건(값, 단위, 기준시점, 통계표, 출처, 산식)을 갖췄는지 확인한다.
    """
    if not isinstance(answer_payload, dict):
        return {
            "verified": False,
            "코드": STATUS_RUNTIME_ERROR,
            "issues": ["입력은 answer_query가 반환한 dict payload여야 합니다."],
        }

    status_code = answer_payload.get("코드")
    status = answer_payload.get("상태")
    if status_code == STATUS_NEEDS_TABLE_SELECTION or status == "needs_table_selection":
        return {
            "verified": False,
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "검증_결과": "통계표 선택 필요",
            "issues": ["아직 수치 claim이 아니라 후보 통계표와 분석계획 단계입니다."],
            "next_steps": answer_payload.get("다음단계", []),
        }

    rows = answer_payload.get("표") or []
    issues: list[str] = []
    warnings: list[str] = []
    claims: list[dict[str, Any]] = []

    if not rows:
        issues.append("표 또는 근거 행이 없어 수치 claim을 검증할 수 없습니다.")
    if isinstance(rows, list):
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                issues.append(f"{idx}번째 표 행이 dict 형식이 아닙니다.")
                continue
            missing = [
                field for field in ("값", "단위", "시점", "통계표")
                if row.get(field) in (None, "")
            ]
            if missing:
                issues.append(f"{idx}번째 표 행 누락 필드: {', '.join(missing)}")
            else:
                claims.append({
                    "지표": row.get("지표"),
                    "값": row.get("값"),
                    "단위": row.get("단위"),
                    "시점": row.get("시점"),
                    "통계표": row.get("통계표"),
                })
    else:
        issues.append("표 필드는 list 형식이어야 합니다.")

    calculation = answer_payload.get("계산")
    if calculation:
        if not isinstance(calculation, dict):
            issues.append("계산 필드는 dict 형식이어야 합니다.")
        elif "산식" not in calculation:
            issues.append("계산 결과에 산식이 없습니다.")
        elif calculation.get("동일시점_여부") is False:
            warnings.append("계산에 사용한 지표들의 기준시점이 서로 다릅니다.")

    if not answer_payload.get("출처") and not any(isinstance(row, dict) and row.get("출처") for row in rows):
        issues.append("출처 정보가 없습니다.")
    if "검증_주의" not in answer_payload and "검증" not in answer_payload:
        warnings.append("검증 주의사항 또는 validation profile이 없습니다.")

    verified = not issues and bool(claims)
    return {
        "verified": verified,
        "코드": STATUS_EXECUTED if verified else STATUS_UNVERIFIED_FORMULA,
        "검증_결과": "통과" if verified else "보완 필요",
        "claims": claims,
        "issues": issues,
        "warnings": warnings,
    }


@mcp.tool()
async def stat_time_compare(
    query: str,
    region: str = "전국",
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    years: int = 5,
    api_key: Optional[str] = None,
) -> dict:
    """[📊] 한 통계의 시작·종료 시점 차이와 변화율 계산.

    start_period/end_period를 생략하면 최근 N개 시계열의 첫 시점과 마지막
    시점을 비교한다.
    """
    explicit_periods = bool(start_period and end_period)
    if explicit_periods:
        key = _resolve_key(api_key)
        param = _lookup_quick(query)
        if not param:
            return {
                "상태": "failed",
                "코드": STATUS_STAT_NOT_FOUND,
                "오류": f'"{query}" 사전 매핑 없음',
                "질문": query,
            }

        canonical = _canonical_region(region) or region
        region_code = None
        if param.region_scheme:
            region_code = param.region_scheme.get(canonical)
            if not region_code:
                return {
                    "상태": "failed",
                    "코드": STATUS_PERIOD_NOT_FOUND,
                    "오류": f'지역 "{region}" 미지원',
                    "지원_지역": list(param.region_scheme.keys()),
                    "질문": query,
                }
        elif canonical != "전국":
            return {
                "상태": "failed",
                "코드": STATUS_PERIOD_NOT_FOUND,
                "오류": f'"{query}"는 지역별 시계열 조회가 검증되지 않았습니다.',
                "지원_지역": ["전국"],
                "질문": query,
            }

        period_type = _default_period_type(param)
        start_bounds = _period_bounds(str(start_period or ""), period_type)
        end_bounds = _period_bounds(str(end_period or ""), period_type)
        fetch_start = start_bounds[0] or str(start_period or "")
        fetch_end = end_bounds[1] or str(end_period or "")
        fetch_low, fetch_high = sorted([fetch_start, fetch_end])

        async with httpx.AsyncClient() as client:
            data = await _fetch_series(
                client,
                key,
                param,
                region_code,
                period_type=period_type,
                start_year=fetch_low,
                end_year=fetch_high,
            )
        if not data:
            fallback = await _quick_trend_core(query, canonical, max(years, 5), api_key)
            data = [
                {"PRD_DE": row.get("시점"), "DT": row.get("값")}
                for row in (fallback.get("시계열") or [])
            ]
        series_result = {
            "통계명": param.description,
            "지역": canonical,
            "단위": param.unit,
            "시계열": [{"시점": r.get("PRD_DE"), "값": r.get("DT")} for r in data],
            "통계표": param.tbl_nm,
        }
        region = canonical
    else:
        series_result = await quick_trend(query, region, years, api_key)
    if "오류" in series_result:
        return {
            "상태": "failed",
            "코드": STATUS_STAT_NOT_FOUND,
            "오류": series_result.get("오류"),
            "질문": query,
        }

    times, values = _values_from_series(series_result.get("시계열", []))
    if len(values) < 2:
        return {
            "상태": "failed",
            "코드": STATUS_PERIOD_NOT_FOUND,
            "오류": "비교 가능한 시점이 2개 미만입니다.",
            "질문": query,
        }

    points = dict(zip(times, values))

    def pick(period: Optional[str], default_period: str) -> tuple[str, float] | None:
        if not period:
            return default_period, points[default_period]
        period = str(period)
        for candidate in times:
            if candidate == period or candidate.startswith(period):
                return candidate, points[candidate]
        return None

    start = pick(start_period, times[0])
    end = pick(end_period, times[-1])
    if start is None or end is None:
        return {
            "상태": "failed",
            "코드": STATUS_PERIOD_NOT_FOUND,
            "오류": "요청한 비교 시점을 시계열에서 찾지 못했습니다.",
            "요청_시작": start_period,
            "요청_종료": end_period,
            "가능_시점": times,
            "질문": query,
        }

    start_t, start_v = start
    end_t, end_v = end
    diff = end_v - start_v
    rate = (diff / abs(start_v) * 100) if start_v else None
    direction = "증가" if diff > 0 else "감소" if diff < 0 else "변화 없음"
    unit = series_result.get("단위", "")
    answer = (
        f"{region} {series_result.get('통계명', query)}은 {start_t} {start_v:,.3f}{unit}에서 "
        f"{end_t} {end_v:,.3f}{unit}로 {direction}했습니다."
    )
    if rate is not None:
        answer += f" 변화율은 {rate:+.2f}%입니다."

    return {
        "상태": "executed",
        "코드": STATUS_EXECUTED,
        "질문": query,
        "answer": answer,
        "비교": {
            "시작": {"시점": start_t, "값": start_v},
            "종료": {"시점": end_t, "값": end_v},
            "증감": round(diff, 4),
            "변화율_퍼센트": round(rate, 2) if rate is not None else None,
            "방향": direction,
            "산식": "(종료값 - 시작값) / 시작값 * 100",
        },
        "표": [
            {"시점": t, "값": v, "단위": unit, "통계표": series_result.get("통계표")}
            for t, v in zip(times, values)
        ],
        "출처": "통계청 KOSIS",
        "검증_주의": ["변화율과 증감은 구분해서 해석해야 합니다."],
    }


@mcp.tool()
async def indicator_dependency_map(indicator: str) -> dict:
    """[🧭] 비중·증가율·폐업률 등 산식형 지표의 필요 통계와 검증 포인트 안내."""
    q = _compact_text(indicator)
    subgroup_terms = [
        ("청년", "청년"),
        ("청소년", "청소년"),
        ("고령", "고령층"),
        ("노인", "고령층"),
        ("여성", "여성"),
        ("남성", "남성"),
    ]
    for key, spec in FORMULA_DEPENDENCIES.items():
        aliases = [spec["canonical"], *spec.get("aliases", [])]
        if any(_compact_text(alias) in q or q in _compact_text(alias) for alias in aliases):
            result = {
                "상태": "mapped",
                "코드": STATUS_EXECUTED,
                "입력": indicator,
                "dependency_key": key,
                "지표": spec["canonical"],
                "산식": spec["formula"],
                "필요_통계": spec["required_stats"],
                "검증_포인트": spec["checks"],
                "주의": spec["caution"],
            }
            subgroup = next((label for term, label in subgroup_terms if term in indicator), None)
            if subgroup and key == "unemployment_rate":
                result["대상군"] = subgroup
                result["부분군_적용"] = (
                    f"{subgroup} 실업률은 같은 산식을 쓰되, 분자=실업자 수와 "
                    f"분모=경제활동인구를 모두 {subgroup} 대상군으로 제한해야 합니다."
                )
                result["추천_검색어"] = [
                    f"{subgroup} 실업률",
                    f"{subgroup} 실업자",
                    f"{subgroup} 경제활동인구",
                ]
            return result

    route_payload = _route_query(indicator).to_agent_payload()
    return {
        "상태": "needs_definition",
        "코드": STATUS_DENOMINATOR_REQUIRED,
        "입력": indicator,
        "answer": "해당 표현은 고정 산식형 지표로 확정하지 못했습니다. 지표·분모·비교시점을 먼저 정해야 합니다.",
        "추천_검색어": route_payload["route"].get("search_terms", []),
        "의도": route_payload.get("intents", []),
        "검증": route_payload.get("validation", {}),
    }


# ---- Utility ----

async def _search_kosis_keywords(
    query: str,
    keywords: list[str],
    limit: int,
    api_key: Optional[str] = None,
    used_routing: bool = False,
) -> dict:
    key = _resolve_key(api_key)
    keywords = [
        keyword.strip()
        for keyword in keywords
        if isinstance(keyword, str) and keyword.strip()
    ]
    if not keywords:
        keywords = [query]
    keywords = list(dict.fromkeys(keywords))

    async with httpx.AsyncClient() as client:
        all_results = []
        for kw in keywords:
            try:
                r = await _kosis_call(client, "statisticsSearch.do", {
                    "method": "getList", "apiKey": key,
                    "searchNm": kw, "format": "json", "jsonVD": "Y", "resultCount": limit,
                })
                for item in r:
                    item["_검색어"] = kw
                all_results.extend(r)
            except RuntimeError:
                continue

    # 중복 제거
    seen = set()
    unique = []
    for item in all_results:
        tid = item.get("TBL_ID")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(item)

    # Surface any Tier A direct mapping that covers this query — search
    # alone could miss the canonical table because KOSIS' search index
    # ranks weakly-related tables higher than the verified one (#23).
    tier_a_match = _lookup_quick(query)
    tier_a_hint: Optional[dict[str, Any]] = None
    if tier_a_match is not None:
        tier_a_hint = {
            "지표": tier_a_match.description,
            "통계표": tier_a_match.tbl_nm,
            "통계표ID": tier_a_match.tbl_id,
            "기관ID": tier_a_match.org_id,
            "단위": tier_a_match.unit,
            "주기": list(tier_a_match.supported_periods),
            "검증상태": tier_a_match.verification_status,
            "권고_호출": (
                f"quick_stat('{query}', region='전국', period='latest') 으로 바로 호출 가능. "
                "search 결과를 다시 매핑할 필요 없음."
            ),
        }

    return {
        "입력": query,
        "라우팅_사용": used_routing,
        "사용된_검색어": keywords,
        "결과수": len(unique),
        "Tier_A_직접_매핑": tier_a_hint,
        "결과": [
            {
                "통계표명": r.get("TBL_NM"),
                "통계표ID": r.get("TBL_ID"),
                "기관ID": r.get("ORG_ID"),
                "수록기간": f"{r.get('STRT_PRD_DE')} ~ {r.get('END_PRD_DE')}",
                "검색어": r.get("_검색어"),
                "URL": r.get("LINK_URL") or r.get("TBL_VIEW_URL"),
            }
            for r in unique[:limit]
        ],
    }


@mcp.tool()
async def search_kosis(
    query: str, limit: int = 10,
    use_routing: bool = True,
    api_key: Optional[str] = None,
) -> dict:
    """[🔍] KOSIS 통합검색. Tier B 라우팅 사전으로 검색어를 자동 보강.

    동작:
      1. use_routing=True (기본)일 때 Tier B 사전에서 추천 검색어 추출
      2. 추천어가 있으면 각각으로 검색해서 결과 통합
      3. 추천어 없으면 원본 query로 검색

    예: "치킨집" → Tier B가 "음식점업, 분식 및 김밥 전문점" 추천 → 두 키워드로 검색

    Args:
        query: 검색어 (자연어 가능)
        limit: 최대 반환 결과 수
        use_routing: Tier B 라우팅 사용 여부 (기본 True)
    """
    keywords = [query]
    used_routing = False
    if use_routing:
        hints = _routing_hints(query)
        if hints:
            keywords = hints[:3]
            used_routing = True

    return await _search_kosis_keywords(
        query,
        keywords,
        limit,
        api_key,
        used_routing=used_routing,
    )


@mcp.tool()
async def curation_status(detail: bool = False) -> dict:
    """[🛠] 큐레이션 데이터 현황 조회.

    Args:
        detail: True면 broken/needs_check 항목 상세 목록까지.
    """
    summary = _curation_stats_summary()
    if not detail:
        return summary

    by_status: dict[str, list] = {"verified": [], "needs_check": [], "broken": [], "unverified": []}
    for key, p in TIER_A_STATS.items():
        by_status.setdefault(p.verification_status, []).append({
            "키": key,
            "통계표": p.tbl_nm,
            "설명": p.description,
            "메모": p.note,
        })
    summary["상세"] = by_status
    summary["주의"] = (
        "broken = KOSIS 호출 실패 확정. quick_stat 시 자동 폴백. "
        "needs_check = 파라미터 보정 필요. discover_metadata.py로 메타조회 후 수정 가능."
    )
    return summary


@mcp.tool()
async def check_stat_availability(
    query: str,
    live_period_check: bool = False,
    api_key: Optional[str] = None,
) -> dict:
    """[🛠] 특정 통계가 즉시 호출 가능한지 미리 확인.

    챗봇이 "X 통계 알려줘" 요청을 받기 전에, X가 Tier A 매핑되고
    검증되어 있는지 확인. broken/needs_check면 대안 안내.

    Args:
        query: 통계 키워드 ("인구", "소상공인", "출산율" 등)
        live_period_check: True면 KOSIS 메타 API(getMeta&type=PRD)를
            호출해 통계표의 실제 최신 수록 시점과 노화도를 함께 반환.
            False면 curation 메모(스냅샷)만 반환. 추가 호출이 발생하므로
            대량 호출 시에는 False 유지.
    """
    p = _curation_lookup(query)
    if not p:
        hints = _routing_hints(query)
        return {
            "쿼리": query,
            "Tier_A_매핑": False,
            "권고": (
                "Tier A 정밀 매핑 없음. search_kosis로 검색 폴백 사용 권장."
                if hints else
                "Tier A·B 매핑 모두 없음. 쿼리를 더 구체적으로."
            ),
            "Tier_B_추천검색어": hints[:5] if hints else [],
        }

    status_messages = {
        "verified": "✅ 검증됨 — quick_stat으로 즉시 호출 가능",
        "needs_check": "⚠️ 파라미터 보정 필요 — 호출 시도 가능하나 결과 신뢰도 낮음",
        "broken": "❌ 호출 실패 확정 — quick_stat은 폴백으로 작동, KOSIS 사이트에서 신 통계표 ID 확인 필요",
        "unverified": "❓ 미검증 — 호출 시도 가능하나 결과 확인 필수",
    }

    result: dict[str, Any] = {
        "쿼리": query,
        "Tier_A_매핑": True,
        "통계표": p.tbl_nm,
        "통계표ID": p.tbl_id,
        "기관ID": p.org_id,
        "설명": p.description,
        "단위": p.unit,
        "검증_상태": p.verification_status,
        "상태_의미": status_messages.get(p.verification_status, "알 수 없음"),
        "메모": p.note,
        "지원_지역": list(p.region_scheme.keys()) if p.region_scheme else "지역 분류 없음 (전국만)",
        "주기": p.supported_periods,
    }

    if live_period_check and p.verification_status != "broken":
        try:
            period_rows = await _fetch_period_range(p.org_id, p.tbl_id, api_key)
        except Exception as exc:
            result["⚠️ 라이브_수록기간_조회_실패"] = repr(exc)
            return result
        if not period_rows:
            result["⚠️ 라이브_수록기간"] = "KOSIS 메타 API가 수록 시점을 반환하지 않음"
            return result
        latest = _pick_finest_period(period_rows)
        live_period = str(
            latest.get("END_PRD_DE") or latest.get("endPrdDe")
            or latest.get("PRD_DE") or latest.get("prdDe") or ""
        )
        live_se = latest.get("PRD_SE") or latest.get("prdSe")
        start_period = (
            latest.get("STRT_PRD_DE") or latest.get("strtPrdDe") or ""
        )
        # _period_age_years only parses YYYY[MM]; strip "2026 1/4" to
        # "2026" so a quarterly-only table still reports an age.
        parseable = re.match(r"(\d{4})(?:\.(\d{2}))?", live_period)
        age = NaturalLanguageAnswerEngine._period_age_years(
            "".join(filter(None, parseable.groups())) if parseable else live_period
        )
        result["라이브_수록기간"] = {
            "주기": live_se,
            "시작_수록시점": start_period,
            "최신_수록시점": live_period,
            "period_age_years": age,
        }
        # Compare with what the curation note claims, if it carries a snapshot
        snapshot_match = re.search(r"\((\d{4}(?:\.\d{2})?)\s", p.note or "")
        if snapshot_match:
            snapshot_period = snapshot_match.group(1).replace(".", "")
            if live_period and not str(live_period).startswith(snapshot_period[:4]):
                result["⚠️ 메모_vs_KOSIS_drift"] = (
                    f"curation 메모 스냅샷={snapshot_period} → KOSIS 실제 최신={live_period}. "
                    "curation 메모 갱신이 필요할 수 있음."
                )
        if age is not None and age >= 1.0:
            result["⚠️ 데이터_신선도"] = (
                f"KOSIS 실제 최신 시점 {live_period} (약 {age:.1f}년 경과). "
                "이 통계표가 갱신을 멈췄거나 갱신 주기가 깁니다."
            )
    return result


def _build_axis_codebook(item_rows: list[dict]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    axes: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in item_rows:
        obj_id = str(row.get("OBJ_ID") or "")
        itm_id = str(row.get("ITM_ID") or "")
        if not obj_id or not itm_id:
            continue
        if obj_id not in axes:
            axes[obj_id] = {
                "OBJ_NM": row.get("OBJ_NM"),
                "OBJ_NM_ENG": row.get("OBJ_NM_ENG"),
                "items": {},
            }
            order.append(obj_id)
        axes[obj_id]["items"][itm_id] = {
            "code": itm_id,
            "label": row.get("ITM_NM"),
            "label_en": row.get("ITM_NM_ENG"),
            "unit": row.get("UNIT_NM"),
            "parent": row.get("UP_ITM_ID"),
        }
    return axes, order


def _suggest_axis_codes(axis: dict[str, Any], bad_code: str, limit: int = 8) -> list[dict[str, Any]]:
    items = axis.get("items") or {}
    bad_norm = _compact_text(bad_code)
    scored: list[tuple[int, str, dict[str, Any]]] = []
    for code, meta in items.items():
        label = str(meta.get("label") or "")
        label_norm = _compact_text(label)
        score = 3
        if bad_code == code:
            score = 0
        elif bad_norm and (bad_norm in _compact_text(code) or bad_norm in label_norm):
            score = 1
        elif bad_norm and any(part and part in label_norm for part in re.split(r"[^0-9A-Za-z가-힣]+", bad_norm)):
            score = 2
        scored.append((score, code, meta))
    scored.sort(key=lambda row: (row[0], len(str(row[2].get("label") or "")), row[1]))
    return [
        {"code": code, "label": meta.get("label"), "unit": meta.get("unit")}
        for _, code, meta in scored[:limit]
    ]


def _validate_query_table_filters(
    filters: dict[str, Any],
    axes: dict[str, dict[str, Any]],
    axis_order: list[str],
) -> tuple[Optional[dict[str, list[str]]], list[dict[str, Any]], dict[str, list[str]]]:
    if not isinstance(filters, dict):
        return None, [{"오류": "filters는 {OBJ_ID: [ITM_ID, ...]} 형식의 객체여야 합니다."}], {}

    normalized: dict[str, list[str]] = {}
    errors: list[dict[str, Any]] = []
    auto_defaults: dict[str, list[str]] = {}

    for axis_id, raw_codes in filters.items():
        axis = str(axis_id)
        if axis not in axes:
            errors.append({
                "axis": axis,
                "오류": "존재하지 않는 분류축",
                "available_axes": [
                    {"OBJ_ID": obj_id, "OBJ_NM": axes[obj_id].get("OBJ_NM")}
                    for obj_id in axis_order
                ],
            })
            continue
        codes = raw_codes if isinstance(raw_codes, list) else [raw_codes]
        clean_codes = [str(code) for code in codes if str(code or "").strip()]
        if not clean_codes:
            errors.append({"axis": axis, "오류": "비어 있는 필터 코드"})
            continue
        items = axes[axis]["items"]
        for code in clean_codes:
            if code not in items:
                errors.append({
                    "axis": axis,
                    "code": code,
                    "오류": "분류축에 없는 ITM_ID",
                    "suggested_codes": _suggest_axis_codes(axes[axis], code),
                })
        normalized[axis] = clean_codes

    item_axis = "ITEM" if "ITEM" in axes else None
    if item_axis and item_axis not in normalized:
        item_codes = list((axes[item_axis].get("items") or {}).keys())
        if len(item_codes) == 1:
            normalized[item_axis] = [item_codes[0]]
            auto_defaults[item_axis] = [item_codes[0]]
        else:
            errors.append({
                "axis": item_axis,
                "오류": "ITEM 축은 명시해야 합니다.",
                "suggested_codes": _suggest_axis_codes(axes[item_axis], ""),
            })

    for axis in axis_order:
        if axis == "ITEM" or axis in normalized:
            continue
        axis_codes = list((axes[axis].get("items") or {}).keys())
        if len(axis_codes) == 1:
            normalized[axis] = [axis_codes[0]]
            auto_defaults[axis] = [axis_codes[0]]
        else:
            errors.append({
                "axis": axis,
                "오류": "다중 값을 가진 분류축은 명시해야 합니다. 전체 조회가 필요하면 원하는 ITM_ID들을 모두 전달하세요.",
                "suggested_codes": _suggest_axis_codes(axes[axis], ""),
            })

    if errors:
        return None, errors, auto_defaults
    return normalized, [], auto_defaults


def _query_table_params(
    org_id: str,
    tbl_id: str,
    filters: dict[str, list[str]],
    axis_order: list[str],
    period_range: Optional[list[str]],
    period_type: Optional[str],
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "method": "getList",
        "apiKey": None,
        "orgId": org_id,
        "tblId": tbl_id,
        "format": "json",
        "jsonVD": "Y",
    }
    if period_type:
        params["prdSe"] = period_type
    data_axis_index = 0
    for axis in axis_order:
        if axis != "ITEM":
            data_axis_index += 1
        codes = filters.get(axis)
        if not codes:
            continue
        value = ",".join(codes)
        if axis == "ITEM":
            params["itmId"] = value
        else:
            params[f"objL{data_axis_index}"] = value
    if period_range:
        bounds = [str(p) for p in period_range if str(p or "").strip()]
        if len(bounds) == 1:
            params["startPrdDe"] = bounds[0]
            params["endPrdDe"] = bounds[0]
        elif len(bounds) >= 2:
            params["startPrdDe"] = bounds[0]
            params["endPrdDe"] = bounds[1]
    return params


def _normalize_query_table_rows(
    rows: list[dict],
    filters: dict[str, list[str]],
    axes: dict[str, dict[str, Any]],
    axis_order: list[str],
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    data_axis_index = 0
    data_axis_map: dict[int, str] = {}
    for axis in axis_order:
        if axis == "ITEM":
            continue
        data_axis_index += 1
        if filters.get(axis):
            data_axis_map[data_axis_index] = axis

    for row in rows:
        dimensions: dict[str, Any] = {}
        item_code = str(row.get("ITM_ID") or row.get("ITM_ID1") or "")
        item_label = row.get("ITM_NM")
        if "ITEM" in axes:
            if item_code in axes["ITEM"]["items"]:
                meta = axes["ITEM"]["items"][item_code]
                item_label = item_label or meta.get("label")
                dimensions["ITEM"] = {"code": item_code, "label": item_label, "unit": meta.get("unit")}
            elif len(filters.get("ITEM", [])) == 1:
                code = filters["ITEM"][0]
                meta = axes["ITEM"]["items"].get(code, {})
                dimensions["ITEM"] = {"code": code, "label": meta.get("label"), "unit": meta.get("unit")}

        for idx, axis in data_axis_map.items():
            code = str(row.get(f"C{idx}") or "")
            label = row.get(f"C{idx}_NM")
            if not code and len(filters.get(axis, [])) == 1:
                code = filters[axis][0]
            meta = (axes.get(axis, {}).get("items") or {}).get(code, {})
            dimensions[axis] = {
                "code": code,
                "label": label or meta.get("label"),
                "unit": meta.get("unit"),
            }

        normalized_rows.append({
            "period": row.get("PRD_DE"),
            "value": row.get("DT"),
            "unit": row.get("UNIT_NM") or (dimensions.get("ITEM") or {}).get("unit"),
            "dimensions": dimensions,
            "raw": row,
        })
    return normalized_rows


@mcp.tool()
async def query_table(
    org_id: str,
    tbl_id: str,
    filters: dict[str, Any],
    period_range: Optional[list[str]] = None,
    api_key: Optional[str] = None,
) -> dict:
    """[🧪] 검증된 메타 코드로 KOSIS 표를 raw 조회한다.

    filters는 explore_table이 반환한 OBJ_ID와 ITM_ID만 받는다. 여러 코드를
    넘겨도 합산/평균을 하지 않고 KOSIS 원행을 개별 rows로 반환한다.
    """
    if not org_id or not tbl_id:
        return {
            "상태": "failed",
            "status": "unsupported",
            "error": "org_id and tbl_id are required.",
            "오류": "org_id와 tbl_id가 필요합니다.",
        }

    key = _resolve_key(api_key)
    fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    async with httpx.AsyncClient() as client:
        try:
            name_rows, item_rows, period_rows = await asyncio.gather(
                _fetch_meta(client, key, org_id, tbl_id, "TBL"),
                _fetch_meta(client, key, org_id, tbl_id, "ITM"),
                _fetch_meta(client, key, org_id, tbl_id, "PRD"),
            )
        except Exception as exc:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_STAT_NOT_FOUND,
                "code": STATUS_STAT_NOT_FOUND,
                "error": f"Metadata lookup failed: {exc}",
                "오류": f"메타 조회 실패: {exc}",
                "기관ID": org_id,
                "통계표ID": tbl_id,
                "org_id": org_id,
                "tbl_id": tbl_id,
            }

        if not isinstance(item_rows, list) or not item_rows:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_STAT_NOT_FOUND,
                "code": STATUS_STAT_NOT_FOUND,
                "error": "No classification metadata is available, so raw extraction cannot be verified.",
                "오류": "분류축 메타가 없어 raw 호출을 검증할 수 없습니다.",
                "기관ID": org_id,
                "통계표ID": tbl_id,
                "org_id": org_id,
                "tbl_id": tbl_id,
            }

        axes, axis_order = _build_axis_codebook(item_rows)
        normalized_filters, errors, auto_defaults = _validate_query_table_filters(filters, axes, axis_order)
        metadata_source = {
            "org_id": org_id,
            "tbl_id": tbl_id,
            "source": "statisticsData.do?method=getMeta&type=TBL/ITM/PRD",
            "fetched_at": fetched_at,
            "metadata_tool": "explore_table",
            "url": _kosis_view_url(org_id, tbl_id),
        }
        if errors or normalized_filters is None:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_UNVERIFIED_FORMULA,
                "code": STATUS_UNVERIFIED_FORMULA,
                "error": "filters validation failed",
                "오류": "filters 검증 실패",
                "검증_오류": errors,
                "validation_errors": errors,
                "available_axes": [
                    {"OBJ_ID": obj_id, "OBJ_NM": axes[obj_id].get("OBJ_NM")}
                    for obj_id in axis_order
                ],
                "metadata_source": metadata_source,
            }

        selected_period = _pick_query_table_period_row(period_rows, period_range) if isinstance(period_rows, list) else None
        period_type_label = _period_type(selected_period)
        period_type = _api_period_type(period_type_label)
        effective_period_range = period_range
        auto_default_period_range: Optional[list[str]] = None
        if not effective_period_range and selected_period and selected_period.get("END_PRD_DE"):
            end_period = str(selected_period.get("END_PRD_DE"))
            effective_period_range = [end_period, end_period]
            auto_default_period_range = effective_period_range

        params = _query_table_params(
            org_id,
            tbl_id,
            normalized_filters,
            axis_order,
            effective_period_range,
            period_type,
        )
        params["apiKey"] = key
        try:
            rows = await _kosis_call(client, "Param/statisticsParameterData.do", params)
        except Exception as exc:
            return {
                "상태": "failed",
                "status": "unsupported",
                "코드": STATUS_RUNTIME_ERROR,
                "code": STATUS_RUNTIME_ERROR,
                "error": f"KOSIS raw extraction failed: {exc}",
                "오류": f"KOSIS raw 호출 실패: {exc}",
                "filters_used": normalized_filters,
                "aggregation": "none",
                "metadata_source": metadata_source,
            }

    table_name = None
    table_name_eng = None
    if isinstance(name_rows, list) and name_rows:
        table_name = name_rows[0].get("TBL_NM") or name_rows[0].get("tblNm")
        table_name_eng = name_rows[0].get("TBL_NM_ENG") or name_rows[0].get("tblNmEng")
    latest_period = selected_period
    normalized_rows = _normalize_query_table_rows(rows, normalized_filters, axes, axis_order)
    return {
        "상태": "executed",
        "status": "executed",
        "verification_level": "explored_raw",
        "confidence": "medium",
        "aggregation": "none",
        "기관ID": org_id,
        "통계표ID": tbl_id,
        "통계표명": table_name,
        "통계표명_영문": table_name_eng,
        "org_id": org_id,
        "tbl_id": tbl_id,
        "table_name": table_name,
        "table_name_en": table_name_eng,
        "filters_used": normalized_filters,
        "auto_default_filters": auto_defaults,
        "period_range": effective_period_range,
        "period_type": period_type,
        "period_type_label": period_type_label,
        "auto_default_period_range": auto_default_period_range,
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
        "metadata_source": metadata_source,
        "수록기간": {
            "주기": latest_period.get("PRD_SE") if latest_period else None,
            "시작_수록시점": latest_period.get("STRT_PRD_DE") if latest_period else None,
            "최신_수록시점": latest_period.get("END_PRD_DE") if latest_period else None,
        } if latest_period else None,
        "period_metadata": {
            "cadence": latest_period.get("PRD_SE") if latest_period else None,
            "start_period": latest_period.get("STRT_PRD_DE") if latest_period else None,
            "latest_period": latest_period.get("END_PRD_DE") if latest_period else None,
        } if latest_period else None,
        "주의": [
            "query_table은 raw extraction 도구입니다. 합산·평균·비율·해석을 수행하지 않습니다.",
            "confidence는 값의 품질이 아니라 코드 매핑과 호출 조건의 검증 수준입니다.",
        ],
        "warnings": [
            "query_table is a raw extraction tool. It does not aggregate, average, calculate ratios, or interpret values.",
            "confidence describes mapping and call-condition verification, not statistical data quality.",
        ],
    }


@mcp.tool()
async def explore_table(
    org_id: str,
    tbl_id: str,
    industry_term: Optional[str] = None,
    api_key: Optional[str] = None,
) -> dict:
    """[🧭] Pull the metadata KOSIS publishes for a single statistical
    table — classification axes, item codes, units, supported periods,
    and the recorded period window — so callers can build a quick_stat
    request without having to hard-code obj_l1/obj_l2/itm_id values.

    The dev-guide endpoints behind this tool are statisticsData.do?
    method=getMeta&type=TBL|ITM|PRD|SOURCE. Each call is cheap; we
    fan them out in parallel and return a single response.

    Args:
        org_id: KOSIS 기관 ID (e.g. "101" for 통계청).
        tbl_id: 통계표 ID (e.g. "DT_1IN1502").
        industry_term: optional industry/category name. If supplied,
            the response includes a resolved_industry block pointing at
            the matching ITM_ID so the caller can pass it as objL2 to
            quick_stat. Use this to bridge "제조업"/"음식점업" to KOSIS
            internal codes dynamically rather than via TIER_A_STATS.

    Returns: dict with table name, classification rows, period range,
        contact info, and (optionally) the resolved industry row plus
        a suggested quick_stat call template.
    """
    if not org_id or not tbl_id:
        return {
            "오류": "org_id와 tbl_id가 모두 필요합니다.",
            "권고": "search_kosis 응답의 통계표ID·기관ID 필드를 사용",
        }
    key = _resolve_key(api_key)
    async with httpx.AsyncClient() as client:
        async def safe_fetch(meta_type: str, extra: Optional[dict] = None) -> Any:
            try:
                return await _fetch_meta(client, key, org_id, tbl_id, meta_type, extra)
            except Exception as exc:
                return {"오류": f"{meta_type} 조회 실패: {exc}"}

        name_rows, item_rows, period_rows, source_rows = await asyncio.gather(
            safe_fetch("TBL"),
            safe_fetch("ITM"),
            safe_fetch("PRD"),
            safe_fetch("SOURCE"),
        )

    meta_errors = {
        meta_type: rows.get("오류")
        for meta_type, rows in (
            ("TBL", name_rows),
            ("ITM", item_rows),
            ("PRD", period_rows),
            ("SOURCE", source_rows),
        )
        if isinstance(rows, dict) and rows.get("오류")
    }
    meta_counts = {
        "TBL": len(name_rows) if isinstance(name_rows, list) else 0,
        "ITM": len(item_rows) if isinstance(item_rows, list) else 0,
        "PRD": len(period_rows) if isinstance(period_rows, list) else 0,
        "SOURCE": len(source_rows) if isinstance(source_rows, list) else 0,
    }
    if not meta_errors and not any(meta_counts.values()):
        return {
            "상태": "failed",
            "코드": STATUS_STAT_NOT_FOUND,
            "오류": "KOSIS 메타 API가 해당 org_id/tbl_id에 대해 어떤 메타도 반환하지 않았습니다.",
            "기관ID": org_id,
            "통계표ID": tbl_id,
            "조회_결과": meta_counts,
            "권고": "기관ID와 통계표ID를 다시 확인하거나 search_kosis로 통계표를 재검색하세요.",
        }

    classifications: dict[str, dict[str, Any]] = {}
    if isinstance(item_rows, list):
        for row in item_rows:
            obj_id = str(row.get("OBJ_ID") or "")
            if not obj_id:
                continue
            axis = classifications.setdefault(obj_id, {
                "OBJ_NM": row.get("OBJ_NM"),
                "OBJ_NM_ENG": row.get("OBJ_NM_ENG"),
                "items": [],
            })
            axis["items"].append({
                "ITM_ID": row.get("ITM_ID"),
                "ITM_NM": row.get("ITM_NM"),
                "ITM_NM_ENG": row.get("ITM_NM_ENG"),
                "UP_ITM_ID": row.get("UP_ITM_ID"),
                "UNIT_NM": row.get("UNIT_NM"),
            })

    table_name = None
    table_name_eng = None
    if isinstance(name_rows, list) and name_rows:
        first = name_rows[0]
        table_name = first.get("TBL_NM") or first.get("tblNm")
        table_name_eng = first.get("TBL_NM_ENG") or first.get("tblNmEng")

    period_summary = None
    used_period = None
    if isinstance(period_rows, list) and period_rows:
        latest = _pick_finest_period(period_rows)
        used_period = str(
            latest.get("END_PRD_DE") or latest.get("endPrdDe")
            or latest.get("PRD_DE") or latest.get("prdDe") or ""
        )
        period_summary = {
            "수록주기": latest.get("PRD_SE") or latest.get("prdSe"),
            "시작_수록시점": (
                latest.get("STRT_PRD_DE") or latest.get("strtPrdDe") or ""
            ),
            "최신_수록시점": used_period,
            "수록주기_개수": len(period_rows),
        }

    period_age = None
    if used_period:
        parseable = re.match(r"(\d{4})(?:\.(\d{2}))?", used_period)
        period_age = NaturalLanguageAnswerEngine._period_age_years(
            "".join(filter(None, parseable.groups())) if parseable else used_period
        )

    contact = None
    if isinstance(source_rows, list) and source_rows:
        first = source_rows[0]
        contact = {
            "조사명": first.get("JOSA_NM") or first.get("josaNm"),
            "담당부서": first.get("DEPT_NM") or first.get("deptNm"),
            "전화": first.get("DEPT_PHONE") or first.get("deptPhone"),
        }

    item_list = item_rows if isinstance(item_rows, list) else []
    items_with_units = sum(1 for row in item_list if row.get("UNIT_NM"))
    items_with_english = sum(1 for row in item_list if row.get("ITM_NM_ENG"))
    metadata_coverage = {
        "통계표명": bool(table_name),
        "통계표명_영문": bool(table_name_eng),
        "수록기간": bool(period_summary),
        "출처": bool(contact),
        "분류축_개수": len(classifications),
        "항목_개수": len(item_list),
        "단위_있는_항목": items_with_units,
        "영문라벨_있는_항목": items_with_english,
        "조회_결과": meta_counts,
    }
    if meta_errors:
        metadata_coverage["조회_실패"] = meta_errors

    result: dict[str, Any] = {
        "통계표ID": tbl_id,
        "기관ID": org_id,
        "통계표명": table_name,
        "통계표명_영문": table_name_eng,
        "수록기간": period_summary,
        "used_period": used_period,
        "period_age_years": period_age,
        "출처": contact,
        "분류축": classifications,
        "메타_완성도": metadata_coverage,
        "출처_KOSIS_API": "statisticsData.do?method=getMeta&type=TBL/ITM/PRD/SOURCE",
    }

    if period_age is not None and period_age >= 1.0:
        result["⚠️ 데이터_신선도"] = (
            f"이 통계표의 최신 시점은 {used_period} (약 {period_age:.1f}년 경과)"
        )

    if not industry_term:
        industry_axes = [
            {"OBJ_ID": obj_id, "OBJ_NM": axis.get("OBJ_NM")}
            for obj_id, axis in classifications.items()
            if any(token in str(axis.get("OBJ_NM") or "") for token in ("산업", "업종", "분류"))
        ]
        if industry_axes:
            result["industry_term_안내"] = {
                "안내": (
                    "industry_term을 함께 주면 해당 산업·업종 표현을 이 통계표의 ITM_ID로 "
                    "매핑해 resolved_industry 블록에 표시합니다."
                ),
                "감지된_분류축": industry_axes,
                "예시_호출": f"explore_table('{org_id}', '{tbl_id}', industry_term='제조업')",
            }

    if industry_term:
        match = _resolve_classification_term(industry_term, item_rows if isinstance(item_rows, list) else [])
        if match is not None:
            result["resolved_industry"] = {
                "입력": industry_term,
                "매칭_ITM_NM": match.get("ITM_NM"),
                "ITM_ID": match.get("ITM_ID"),
                "OBJ_ID": match.get("OBJ_ID"),
                "단위": match.get("UNIT_NM"),
                "권고_호출": (
                    f"quick_stat은 obj_l2/obj_l3 직접 노출이 없으므로, KOSIS 직접 API "
                    f"호출(statisticsData.do)에 objL?={match.get('OBJ_ID')}, "
                    f"itmId={match.get('ITM_ID')}로 사용."
                ),
            }
        else:
            result["resolved_industry"] = {
                "입력": industry_term,
                "매칭_ITM_NM": None,
                "안내": "분류축에서 일치하는 항목을 찾지 못했습니다. 분류축 리스트를 확인해 입력 표현을 조정하세요.",
            }

    return result


@mcp.tool()
async def decode_error(error_code: str) -> dict:
    """[🛠] KOSIS 에러 코드 한국어 설명. 공식 코드(10/20/30 등) 외에도
    내부/네트워크 코드(E001, INVALID_PARAM, -1 등)를 인식한다."""
    raw = "" if error_code is None else str(error_code)
    code = raw.strip()
    if not code:
        return {
            "코드": "",
            "의미": "빈 코드 — 응답에 에러 코드 필드가 없거나 비어 있음",
            "권고": "원본 응답의 다른 필드(`오류`, `결과`, HTTP 상태)를 확인",
        }
        # Try exact, then upper, then lowercase. KOSIS official codes
        # are numeric so case-insensitive lookup is safe for non-numeric variants.
    candidates = [code, code.upper(), code.lower()]
    for cand in candidates:
        if cand in ERROR_MAP:
            return {
                "코드": cand,
                "의미": ERROR_MAP[cand],
                "공식코드_여부": cand in {"10", "11", "20", "21", "30", "31", "40", "41", "42", "50"},
            }
    return {
        "코드": code,
        "의미": "알 수 없음 — KOSIS 공식 코드(10/11/20/21/30/31/40/41/50)도, 알려진 내부 코드도 아님",
        "지원_코드": sorted(ERROR_MAP.keys()),
        "권고": "응답 본문 전체를 함께 확인하고, KOSIS 고객센터에 코드와 호출 파라미터를 첨부해 문의",
    }


# ============================================================================

def main() -> None:
    """Console entry point for `kosis-analysis-mcp`."""
    mcp.run()


if __name__ == "__main__":
    main()
