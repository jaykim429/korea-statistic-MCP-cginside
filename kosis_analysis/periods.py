from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

STATUS_INVALID_PERIOD_RANGE = "INVALID_PERIOD_RANGE"
STATUS_PERIOD_NOT_FOUND = "PERIOD_NOT_FOUND"

# KOSIS PRD meta returns one row per cadence; pick the finest one so
# staleness checks reflect the most granular data available.
# Korean: 월 > 분기 > 반기 > 년 / English aliases: M Q H Y.
_PRD_FINENESS = {
    "월": 0, "M": 0, "MM": 0,
    "분기": 1, "Q": 1, "QQ": 1,
    "반기": 2, "H": 2, "HF": 2,
    "년": 3, "연": 3, "Y": 3, "A": 3,
}


def _pick_finest_period(period_rows: list[dict]) -> Optional[dict]:
    """Return the PRD row with the finest cadence (월 > 분기 > 반기 > 년)."""
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
    rel_year = _relative_year(compact)
    if rel_year is not None:
        q_m = re.search(r"([1-4])분기|Q([1-4])", compact)
        if q_m:
            quarter = q_m.group(1) or q_m.group(2)
            return f"{rel_year}{quarter}"
        if "이번분기" in compact or "당분기" in compact:
            return f"{rel_year}{_current_quarter()}"
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
    """Return an advisory string when 상반기/하반기 keywords appear."""
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
    """Warn when requested period precision is finer than the table supports."""
    if not period or period == "latest":
        return None
    text = str(period)
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
    """Extract (start_year, end_year) from explicit comparison phrasing."""
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
    """Extract a single start year from open-ended range phrases."""
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


def _format_period_label(period: Any, period_type: str) -> str:
    text = str(period or "")
    if period_type == "M" and len(text) >= 6:
        return f"{text[:4]}.{text[4:6]}"
    if period_type == "Q" and len(text) >= 5:
        return f"{text[:4]}년 {text[-1]}분기"
    if len(text) >= 4:
        return f"{text[:4]}년"
    return text


def _api_period_de(value: Any) -> str:
    text = str(value or "").strip()
    monthly = re.fullmatch(r"(\d{4})\.(\d{2})", text)
    if monthly:
        return f"{monthly.group(1)}{monthly.group(2)}"
    return text


def _normalize_period_bound(value: Any, *, upper: bool = False) -> Optional[int]:
    text = re.sub(r"\D", "", str(value or ""))
    if not text:
        return None
    if len(text) == 4:
        return int(text + ("99" if upper else "00"))
    return int(text)


def _validate_query_period_range(
    period_range: Optional[list[str]],
    selected_period: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not period_range:
        return None
    bounds = [str(p).strip() for p in period_range if str(p or "").strip()]
    if not bounds:
        return None
    if len(bounds) == 1:
        bounds = [bounds[0], bounds[0]]
    start, end = bounds[0], bounds[1]
    start_num = _normalize_period_bound(start, upper=False)
    end_num = _normalize_period_bound(end, upper=True)
    if start_num is not None and end_num is not None and start_num > end_num:
        return {
            "code": STATUS_INVALID_PERIOD_RANGE,
            "error": "period_range start is later than end.",
            "오류": "period_range 시작 시점이 종료 시점보다 큽니다.",
            "period_range_received": [start, end],
            "suggested_period_range": [end, start],
        }
    if not selected_period:
        return None
    available_start = selected_period.get("STRT_PRD_DE")
    available_end = selected_period.get("END_PRD_DE")
    available_start_num = _normalize_period_bound(available_start, upper=False)
    available_end_num = _normalize_period_bound(available_end, upper=True)
    if (
        start_num is not None
        and end_num is not None
        and available_start_num is not None
        and available_end_num is not None
        and (end_num < available_start_num or start_num > available_end_num)
    ):
        return {
            "code": STATUS_PERIOD_NOT_FOUND,
            "error": "Requested period is outside the table's recorded period range.",
            "오류": "요청 시점이 통계표 수록 범위 밖입니다.",
            "period_range_received": [start, end],
            "available_period_range": [str(available_start), str(available_end)],
        }
    return None


def _query_table_data_nature(
    table_name: Optional[str],
    period_range: Optional[list[str]],
    latest_period: Optional[str],
) -> dict[str, Any]:
    name = str(table_name or "")
    requested_end = None
    if period_range:
        bounds = [str(p).strip() for p in period_range if str(p or "").strip()]
        if bounds:
            requested_end = bounds[-1]
    requested_num = _normalize_period_bound(requested_end, upper=True)
    latest_num = _normalize_period_bound(latest_period, upper=True)
    projection_by_name = any(term in name for term in ("추계", "장래", "전망", "예측"))
    current_year = datetime.now().year
    requested_year = int(str(requested_num)[:4]) if requested_num is not None else None
    latest_year = int(str(latest_num)[:4]) if latest_num is not None else None
    projection_by_requested_future = requested_year is not None and requested_year > current_year
    projection_by_future_series = latest_year is not None and latest_year > current_year + 1
    if projection_by_name or projection_by_requested_future or projection_by_future_series:
        horizon = None
        if requested_year is not None:
            horizon = requested_year - current_year
        return {
            "data_nature": "projection",
            "period_nature": "future_projection" if horizon is not None and horizon > 0 else "projection_series",
            "projection_horizon_years": horizon if horizon is not None and horizon > 0 else None,
            "data_quality_note": "통계표명 또는 요청 시점 기준으로 추계/전망 성격의 데이터입니다. 실측값처럼 단정하지 말고 추계 기준임을 표시하세요.",
            "추계_안내": "이 데이터는 추계/전망 성격일 수 있습니다. 사용자 응답에 기준을 함께 표시하세요.",
        }
    return {
        "data_nature": "observed",
        "period_nature": "observed_or_reported",
        "projection_horizon_years": None,
        "data_quality_note": None,
    }
