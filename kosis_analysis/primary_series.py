"""Registered primary-source series with native identity, never fabricated KOSIS IDs.

Registry entries contain schema/scope, not observations. Every answer fetches the
official table and verifies row, unit, coordinates and requested period coverage.
Only supported request roles match; extra population/measure constraints fail closed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Optional
from urllib.parse import urlencode

import httpx


@dataclass(frozen=True)
class PrimarySeries:
    provider: str
    indicator_id: str
    dataset_id: str
    series_id: str
    measure: str
    unit: str
    population: str
    quality_note: str

    @property
    def identity(self) -> dict:
        return {"contract_version": "stat-source/v1", "provider": self.provider,
                "indicator_id": self.indicator_id, "dataset_id": self.dataset_id, "series_id": self.series_id}


SERIES = (PrimarySeries("enara", "5007", "500701", "T01", "벤처기업수", "천 개", "벤처기업",
    "e-나라지표의 벤처기업수(천 개) 공표값입니다. 소수 첫째 자리로 반올림되어 정확한 개별 기업 수로 환산할 수 없습니다. "
    "벤처기업통계의 확인유형에는 예비벤처가 포함되므로 별도의 법인 수·유효 벤처확인기업 수와 동일시하지 않습니다."),)


def valid_native_identity(identity: object) -> bool:
    return isinstance(identity, dict) and any(identity == series.identity for series in SERIES)


class AnnualTableParser(HTMLParser):
    def __init__(self, series: PrimarySeries):
        super().__init__(convert_charrefs=True)
        self.series = series
        self.in_table = False
        self.in_row = False
        self.cell: Optional[tuple[str, str, list[str]]] = None
        self.cells: list[tuple[str, str, str]] = []
        self.matches: list[list[tuple[str, str, str]]] = []
        self.unit_parts: list[str] = []
        self.in_unit = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "table":
            self.in_table = attrs.get("id") == "t_Table_" + self.series.dataset_id
        if tag == "p" and attrs.get("id") == "sttsCdUnit":
            self.in_unit = True
        if self.in_table and tag == "tr":
            self.in_row, self.cells = True, []
        if self.in_row and tag in {"td", "th"}:
            self.cell = (tag, attrs.get("item-id", ""), [])

    def handle_data(self, data):
        if self.cell:
            self.cell[2].append(data)
        if self.in_unit:
            self.unit_parts.append(data)

    def handle_endtag(self, tag):
        if tag == "p":
            self.in_unit = False
        if tag in {"td", "th"} and self.cell:
            self.cells.append((self.cell[0], self.cell[1], "".join(self.cell[2]).strip()))
            self.cell = None
        if tag == "tr" and self.in_row:
            if any(kind == "th" and code == self.series.series_id for kind, code, _ in self.cells):
                self.matches.append(self.cells)
            self.in_row = False
        if tag == "table":
            self.in_table = False

    def observations(self) -> list[dict]:
        compact = lambda value: re.sub(r"\s+", "", value)
        if len(self.matches) != 1 or compact("".join(self.unit_parts)) != compact(f"[단위 : {self.series.unit}]"):
            raise ValueError("Official source table identity/unit does not match the registered schema")
        cells = self.matches[0]
        labels = [value for kind, code, value in cells if kind == "th" and code == self.series.series_id]
        if labels != [self.series.measure + "(" + self.series.unit.replace(" ", "\xa0") + ")"] and (
            len(labels) != 1 or compact(labels[0]) != compact(self.series.measure + "(" + self.series.unit + ")")):
            raise ValueError("Official source series label does not match")
        rows = []
        for kind, code, raw in cells:
            if kind != "td":
                continue
            if not re.fullmatch(r"(?:19|20)\d{2}Y", code) or not re.fullmatch(r"\d+(?:,\d{3})*(?:\.\d+)?", raw):
                raise ValueError("Unverified period or missing/non-numeric official observation")
            rows.append({"value": raw.replace(",", ""), "period": code[:-1], "unit": self.series.unit,
                         "dimensions": {"ITEM": {"code": self.series.series_id, "label": self.series.measure},
                                        "population": {"label": self.series.population}, "region": {"label": "전국"}}})
        if not rows or len({row["period"] for row in rows}) != len(rows):
            raise ValueError("Missing or duplicate official period coordinates")
        return sorted(rows, key=lambda row: row["period"])


def match_primary_series(query: str, region: str = "전국") -> Optional[PrimarySeries]:
    if region != "전국":
        return None
    text = re.sub(r"(?:19|20)\d{2}\s*년?", " ", query)
    text = re.sub(r"(?:최근|지난)\s*\d{1,2}\s*년(?:간|동안)?", " ", text)
    text = re.sub(r"(?:부터|까지|연도별|년도별|연별|추이|시계열|최근|최신|전국|e-나라지표|이나라지표|엑셀|표로|파일|저장|알려줘|알려주세요|보여줘|보여주세요|해줘|주세요|어떻게돼|얼마야|몇개야)", " ", text)
    text = re.sub(r"[\s~～–—:?!.,\-]+", "", text)
    return next((series for series in SERIES if text == series.measure), None)


async def query_primary_series(query: str, region: str = "전국", start_year: Optional[str] = None,
                               end_year: Optional[str] = None, *, client: Optional[httpx.AsyncClient] = None) -> Optional[dict]:
    series = match_primary_series(query, region)
    if not series:
        return None
    years = re.findall(r"(?:19|20)\d{2}", query)
    recent = re.search(r"(?:최근|지난)\s*(\d{1,2})\s*년", query)
    current = datetime.now(timezone.utc).year
    start = start_year or (min(years) if years else str(current - min(int(recent[1]), 30)) if recent else str(current - 5))
    end = end_year or (max(years) if years else str(current))
    if not re.fullmatch(r"(?:19|20)\d{2}", str(start)) or not re.fullmatch(r"(?:19|20)\d{2}", str(end)) or not 0 <= int(end) - int(start) < 30:
        return {"status": "failed", "code": "INVALID_PERIOD", "error": "Explicit annual period range must be ordered and at most 30 years"}
    explicit = bool(years or start_year or end_year)
    params = {"stts_cd": series.dataset_id, "idx_cd": series.indicator_id, "freq": "Y", "period": f"{start}:{end}"}
    url = "https://www.index.go.kr/unity/potal/eNara/sub/showStblGams3.do?" + urlencode(params)
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=False) as owned_client:
                response = await owned_client.get(url)
        else:
            response = await client.get(url)
        response.raise_for_status()
        parser = AnnualTableParser(series)
        parser.feed(response.content.decode("utf-8"))
        rows = [row for row in parser.observations() if start <= row["period"] <= end]
        if not rows or explicit and {row["period"] for row in rows} != {str(y) for y in range(int(start), int(end) + 1)}:
            return {"status": "PERIOD_NOT_FOUND", "code": "PERIOD_NOT_FOUND", "source": "e-나라지표", "error": "Requested annual coverage is not present; no latest-year substitution"}
        if recent and not explicit:
            rows = rows[-min(int(recent[1]), 30):]
        elif not explicit:
            rows = rows[-1:]
        return {"status": "executed", "code": "EXECUTED", "answer_type": "primary_source_series",
                "actual_query_supported": True, "verification_level": "actual_data", "query": query, "region": region,
                "source": "e-나라지표 · 벤처기업협회 벤처기업통계", "source_url": url, "source_identity": series.identity,
                "table_name": "벤처기업수", "actual_measure": series.measure, "actual_measure_evidence": "queried_item_label",
                "period_type": "Y", "unit": series.unit, "data_quality_note": series.quality_note,
                "requested_period": [start, end] if explicit else None,
                "used_period": rows[-1]["period"], "rows": rows,
                "answer": "\n".join(f'{row["period"]}년: {row["value"]} {row["unit"]}' for row in rows) + "\n" + series.quality_note}
    except (httpx.HTTPError, UnicodeError, ValueError) as exc:
        return {"status": "failed", "code": "RUNTIME_ERROR", "source": "e-나라지표", "error": f"Primary source retrieval/validation failed: {type(exc).__name__}"}
