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

import base64
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import httpx
import numpy as np
from mcp.server.fastmcp import FastMCP
from mcp.types import ImageContent, TextContent
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


def _format_number(v: Any) -> str:
    try:
        n = float(v)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.3f}".rstrip("0").rstrip(".")
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


def _parse_month_token(text: str) -> Optional[str]:
    if not text:
        return None
    compact = re.sub(r"\s+", "", str(text))
    m = re.search(r"(19\d{2}|20\d{2})(?:[.\-/]|년)?(0?[1-9]|1[0-2])월?", compact)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}"
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
    return None


def _period_bounds(period: str, period_type: str) -> tuple[Optional[str], Optional[str]]:
    """Convert natural period text into KOSIS start/end period codes."""
    if not period or period == "latest":
        return None, None

    if period_type == "M":
        month = _parse_month_token(period)
        if month:
            return month, month

    if period_type == "Q":
        quarter = _parse_quarter_token(period)
        if quarter:
            return quarter, quarter

    year = _parse_year_token(period)
    if not year:
        return None, None
    if period_type == "M":
        return f"{year}01", f"{year}12"
    if period_type == "Q":
        return f"{year}1", f"{year}4"
    return year, year


def _extract_year_range(text: str) -> tuple[Optional[str], Optional[str]]:
    """Extract (start_year, end_year) from explicit comparison phrasing.

    Recognizes patterns like "2019년 대비 2023년", "2019~2023", "2019년부터 2023년".
    Returns (None, None) when fewer than two plausible 4-digit years are present.
    Order follows text order so "2019 대비 2023" → start=2019, end=2023.
    """
    if not text:
        return None, None
    cur = datetime.now().year
    years = [y for y in re.findall(r"(19\d{2}|20\d{2})", text) if int(y) <= cur + 1]
    if len(years) < 2:
        return None, None
    start, end = years[0], years[1]
    if start == end:
        return None, None
    return start, end


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
            r"많은(\d+)[곳개]",
            r"적은(\d+)[곳개]",
            r"(\d+)개시도",
            r"(\d+)개지역",
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
        slots = route_payload.get("slots") or {}
        calc = slots.get("calculation") if isinstance(slots, dict) else None
        if isinstance(calc, list) and "share_ratio" in calc:
            return True
        if "STAT_SHARE_RATIO" in route_payload.get("intents", []):
            return True
        q = self._norm(query)
        return any(term in q for term in ("비중", "비율", "차지", "구성비"))

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
        Other ops fall back through the dispatcher."""
        components = self._expand_composite_to_components(composite)
        if not components:
            return await self._answer_search_fallback(query)
        route_payload = self._route_payload(query)
        route_payload["route"]["direct_stat_key"] = direct_key
        period = self._period_argument(query, route_payload)

        rows: list[dict[str, Any]] = []
        subtotal = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region in components:
            stat = await quick_stat(direct_key, region, period, self.api_key)
            if "오류" in stat or "값" not in stat:
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
            trend = await quick_trend(direct_key, region, years, self.api_key)
            if "오류" in trend:
                return await self._answer_search_fallback(query, route_payload)
            answer = (
                f"{region}의 {trend.get('통계명', direct_key)} 최근 {len(trend.get('시계열', []))}개 시점 "
                f"자료를 조회했습니다."
            )
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
                "검증_주의": route_payload["validation"].get("warnings", []),
                "route": route_payload["route"],
                "출처": "통계청 KOSIS",
            }
            if "분석" in q:
                result["분석"] = await analyze_trend(direct_key, region, max(years, 5), self.api_key)
            return result

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
        if selected:
            leader = selected[0]
            answer = (
                f"{period} 기준 {comparison.get('통계명', direct_key)} 상위 {len(selected)}개 지역 중 "
                f"가장 {direction} 지역은 {leader['지역']}({leader['값']}{unit})입니다."
            )
        else:
            answer = "상위/하위 비교 데이터를 조회하지 못했습니다."
        return {
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
            "검증_주의": route_payload["validation"].get("warnings", []),
            "route": route_payload["route"],
            "출처": "통계청 KOSIS",
        }

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

        rows: list[dict[str, Any]] = []
        total = 0.0
        unit = ""
        used_period = ""
        table = ""
        missing: list[str] = []
        for region in regions:
            stat = await quick_stat(direct_key, region, period, self.api_key)
            if "오류" in stat or "값" not in stat:
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

    async def _answer_search_fallback(self, query: str, route_payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        route_payload = route_payload or self._route_payload(query)
        search = await search_kosis(query, 8, True, self.api_key)
        return {
            "상태": "needs_table_selection",
            "코드": STATUS_NEEDS_TABLE_SELECTION,
            "답변유형": "search_and_plan",
            "질문": query,
            "answer": (
                "이 질문은 여러 통계표·분류코드·산식이 필요한 복합 질의입니다. "
                "아래 후보 통계표 중 적합한 표를 선택한 뒤 실제 수치 계산을 진행해야 합니다."
            ),
            "의도": route_payload["intents"],
            "슬롯": route_payload["slots"],
            "실행계획": route_payload["analysis_plan"],
            "검증": route_payload["validation"],
            "검색결과": search.get("결과", []),
            "사용된_검색어": search.get("사용된_검색어", []),
            "다음단계": [
                "후보 통계표의 기준시점, 단위, 분류코드, 분모를 확인합니다.",
                "동일 기준으로 시계열·비교·비중·증가율 계산을 수행합니다.",
                "표/그래프/해석/유의사항을 함께 응답합니다.",
            ],
            "route": route_payload["route"],
        }

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
        effective_region = self._effective_region(route_payload, region)
        inferred_direct_key = self._infer_direct_stat_key(query, route_payload)
        if self._is_sme_large_sales_question(query):
            return await self._answer_sme_large_sales(query, effective_region)
        if self._is_sme_smallbiz_count_question(query):
            return await self._answer_sme_smallbiz_counts(query, effective_region)
        if self._is_sme_employee_average_question(query):
            return await self._answer_sme_employee_average(query, effective_region)
        if self._needs_advanced_analysis_plan(route_payload):
            return await self._answer_search_fallback(query, route_payload)

        composites = self._extract_composite_regions(query)
        if inferred_direct_key and composites:
            composite = composites[0]
            operation = "share" if self._is_share_ratio_question(query, route_payload) else "sum"
            return await self._answer_composite_aggregate(
                query, inferred_direct_key, composite, operation=operation,
            )

        if inferred_direct_key and self._is_aggregation_question(query):
            extras = self._extract_extra_regions(query, effective_region, route_payload)
            regions = [effective_region] + [r for r in extras if r != effective_region]
            if len(regions) >= 2:
                return await self._answer_region_sum(query, inferred_direct_key, regions)

        if inferred_direct_key and self._is_top_n_question(query, route_payload):
            top_n = self._extract_top_n(query) or 5
            return await self._answer_top_n(query, inferred_direct_key, top_n)

        if inferred_direct_key and self._is_region_compare_question(query):
            return await self._answer_region_compare(query, inferred_direct_key)

        if (
            inferred_direct_key
            and self._is_share_ratio_question(query, route_payload)
            and effective_region != "전국"
            and not self._is_growth_question(query, route_payload)
        ):
            return await self._answer_share_ratio(query, effective_region, inferred_direct_key)

        if inferred_direct_key and not route_payload["route"].get("direct_stat_key"):
            route_payload["route"]["direct_stat_key"] = inferred_direct_key
        if route_payload["route"].get("direct_stat_key"):
            return await self._answer_direct(query, effective_region, inferred_direct_key, route_payload)
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
            "tier_a_top_n", "tier_a_region_comparison", "tier_a_region_sum",
        }),
        "STAT_SHARE_RATIO": frozenset({
            "tier_a_share_ratio", "tier_a_composite_comparison",
            "tier_a_composite", "tier_a_composite_share_ratio",
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
    }

    _YEAR_MISMATCH_ANSWER_TYPES: frozenset[str] = frozenset({
        "tier_a_value", "tier_a_growth_rate", "tier_a_share_ratio",
    })

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

        if cls._is_aggregation_question(query) and answer_type != "tier_a_region_sum":
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
        - Collapses 년년 / 월월 artifacts the substitutions can leave."""
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
        return text

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


def _svg_to_image(svg: str) -> ImageContent:
    data = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return ImageContent(type="image", mimeType="image/svg+xml", data=data)


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

@mcp.tool()
async def quick_stat(
    query: str, region: str = "전국", period: str = "latest",
    api_key: Optional[str] = None,
) -> dict:
    """[⚡] 자연어로 통계 단일값 즉시 조회.

    동작 순서:
      1. Tier A 정밀 매핑 발견 시 → 즉시 호출
      2. Tier B 라우팅 힌트 발견 시 → 추천 검색어로 KOSIS 검색
      3. 폴백: 원본 query로 KOSIS 검색

    Args:
        query: 통계 키워드 ("인구", "실업률", "중소기업 사업체수")
        region: 17개 시도명 (기본 "전국")
        period: "latest" 또는 "2023", "작년"
    """
    key = _resolve_key(api_key)
    param = _lookup_quick(query)
    canonical = _canonical_region(region) or region

    # === Tier A 히트: 즉시 호출 ===
    if param:
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
        start_period, end_period = _period_bounds(period, period_type)
        if period != "latest" and not start_period:
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
            return {"결과": "데이터 없음", "통계표": param.tbl_nm, "권고": verification_warning}

        data.sort(key=lambda r: str(r.get("PRD_DE") or ""))
        row = data[-1]
        period_label = _format_period_label(row.get("PRD_DE"), period_type)
        result = {
            "answer": f"{period_label} {region}의 {param.description}은(는) "
                      f"{_format_number(row.get('DT'))} {param.unit}입니다.",
            "값": row.get("DT"), "단위": param.unit,
            "시점": row.get("PRD_DE"),
            "지역": region, "통계표": param.tbl_nm,
            "출처": "통계청 KOSIS",
        }
        if verification_warning:
            result["⚠️ 검증_상태"] = verification_warning
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
) -> dict:
    """[⚡] 시계열 데이터 조회 (분석/시각화 입력으로 사용).

    Args:
        query: 통계 키워드
        region: 지역
        years: 최근 N년 (기본 10)
    """
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

    async with httpx.AsyncClient() as client:
        data = await _fetch_series(
            client,
            key,
            param,
            region_code,
            period_type=_default_period_type(param),
            latest_n=years,
        )

    return {
        "통계명": param.description, "지역": region, "단위": param.unit,
        "시계열": [{"시점": r.get("PRD_DE"), "값": r.get("DT")} for r in data],
        "데이터수": len(data), "통계표": param.tbl_nm,
    }


@mcp.tool()
async def quick_region_compare(
    query: str,
    period: str = "latest",
    sort: str = "desc",
    api_key: Optional[str] = None,
) -> dict:
    """[⚡] 지역/시도별 값을 한 번에 비교.

    지역 분류가 검증된 Tier A 통계만 지원합니다. 예:
    "중소기업 사업체수", "소상공인 사업체수", "실업률".
    """
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
    start_period, end_period = _period_bounds(period, period_type)
    if period != "latest" and not start_period:
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
    return {
        "통계명": param.description,
        "시점": latest_period,
        "단위": param.unit,
        "정렬": "내림차순" if reverse else "오름차순",
        "지역수": len(rows),
        "표": rows,
        "출처": "통계청 KOSIS",
    }


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
        summary["평균 변화율"] = f"{trend['변화율']['평균_퍼센트']:+.2f}%"
        summary["최근 변화"] = f"{trend['변화율']['최근_퍼센트']:+.2f}%"
    if "극값" in trend:
        summary["최댓값"] = f"{trend['극값']['최댓값']['시점'][:4]}: {trend['극값']['최댓값']['값']}"
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
                f"예측 {len(forecast_pts)}년 + 지역 비교 {len(items)}개"
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
async def answer_query(
    query: str,
    region: str = "전국",
    api_key: Optional[str] = None,
) -> dict:
    """[🤖] 자연어 질문을 실제 답변 또는 안전한 분석계획으로 생성.

    검증된 Tier A 질문은 KOSIS API를 호출해 수치·표·계산·해석을 반환하고,
    복합/상위어 질문은 실제 KOSIS 검색 후보와 분석계획을 반환한다.
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
    for key, spec in FORMULA_DEPENDENCIES.items():
        aliases = [spec["canonical"], *spec.get("aliases", [])]
        if any(_compact_text(alias) in q or q in _compact_text(alias) for alias in aliases):
            return {
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
    key = _resolve_key(api_key)
    keywords = [query]
    used_routing = False
    if use_routing:
        hints = _routing_hints(query)
        if hints:
            keywords = hints[:3]
            used_routing = True

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

    return {
        "입력": query,
        "라우팅_사용": used_routing,
        "사용된_검색어": keywords,
        "결과수": len(unique),
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
async def check_stat_availability(query: str) -> dict:
    """[🛠] 특정 통계가 즉시 호출 가능한지 미리 확인.

    챗봇이 "X 통계 알려줘" 요청을 받기 전에, X가 Tier A 매핑되고
    검증되어 있는지 확인. broken/needs_check면 대안 안내.

    Args:
        query: 통계 키워드 ("인구", "소상공인", "출산율" 등)
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

    return {
        "쿼리": query,
        "Tier_A_매핑": True,
        "통계표": p.tbl_nm,
        "설명": p.description,
        "단위": p.unit,
        "검증_상태": p.verification_status,
        "상태_의미": status_messages.get(p.verification_status, "알 수 없음"),
        "메모": p.note,
        "지원_지역": list(p.region_scheme.keys()) if p.region_scheme else "지역 분류 없음 (전국만)",
        "주기": p.supported_periods,
    }


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
                "공식코드_여부": cand in {"10", "11", "20", "21", "30", "31", "40", "41", "50"},
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
