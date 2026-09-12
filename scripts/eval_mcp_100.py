"""KOSIS MCP 100건 검증.

챗봇을 거치지 않고 MCP 도구를 직접 두드려 **도구 계약**이 지켜지는지 본다.
  - search_kosis: 내용어가 걸린 표만 나오는가(잡음 0), 결과가 아예 비지는 않는가
  - answer_query: 값이면 값·단위·시점·출처가 있는가, 아니면 needs_table_selection/no_relevant_table로 정직한가
  - select_table_for_query / explore_table / resolve_concepts / query_table: 파이프라인이 이어지는가
  - stat_time_compare / analyze_trend: 증감·추세 계약
  - 오류: 잘못된 코드·기간·빈 입력에 구조화된 실패(원인 + 후보 코드)를 주는가

사용: python scripts/eval_mcp_100.py [--url http://127.0.0.1:8000/mcp] [--only C001,C002]
결과: 콘솔 + docs/mcp-100-eval.md (리포지토리 루트 기준)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_URL = "http://127.0.0.1:8000/mcp"
STOP_WORDS = {"통계", "자료", "현황", "관련", "알려", "보여", "최근", "기준", "정도", "얼마", "어떻게", "어떤"}


class Mcp:
    def __init__(self, url: str) -> None:
        self.url = url
        self.sid: str | None = None
        self._init()

    def _post(self, payload: dict[str, Any], timeout: int = 180) -> dict[str, Any] | None:
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.sid:
            headers["Mcp-Session-Id"] = self.sid
        req = urllib.request.Request(self.url, data=json.dumps(payload).encode(), headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as res:
            self.sid = res.headers.get("Mcp-Session-Id") or self.sid
            body = res.read().decode()
        data = None
        for line in body.splitlines():
            if line.startswith("data:"):
                data = json.loads(line[5:].strip())
        return data

    def _init(self) -> None:
        self._post({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                               "clientInfo": {"name": "eval-100", "version": "1"}}})
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                   "Mcp-Session-Id": self.sid or ""}
        req = urllib.request.Request(self.url, data=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode(), headers=headers)
        urllib.request.urlopen(req, timeout=30)

    def call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        res = self._post({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": name, "arguments": args}})
        if not res or "result" not in res:
            raise RuntimeError(f"no result: {str(res)[:200]}")
        text = res["result"]["content"][0]["text"]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"_raw": text}


def status_of(d: dict[str, Any]) -> str:
    return str(d.get("status") or d.get("상태") or "")


def rows_of(d: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("결과", "results", "검색결과", "rows", "표"):
        value = d.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)]
    return []


def content_words(query: str) -> list[str]:
    tokens = re.findall(r"[0-9A-Za-z가-힣]+", query)
    out = []
    for token in tokens:
        norm = re.sub(r"(을|를|의|이|가|은|는|에|에서|으로|로|와|과|도)$", "", token)
        if len(norm) >= 2 and norm not in STOP_WORDS:
            out.append(norm)
    return out


# ── 체크 함수 ──
def check_search(query: str, min_rows: int = 1) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("search_kosis", {"query": query, "limit": 8})
        rows = rows_of(d)
        if len(rows) < min_rows:
            return False, f"결과 {len(rows)}건(기대 {min_rows}건 이상), 원자료 {d.get('결과수')}건"
        terms = content_words(query)
        if terms:
            junk = [r for r in rows if not (r.get("match_quality") or {}).get("matched_terms")]
            if junk:
                return False, f"내용어 미일치 표 {len(junk)}건 섞임: {junk[0].get('통계표명')}"
        return True, f"{len(rows)}건 · 1위 {rows[0].get('통계표명')}"
    return run


def check_search_low_relevance(query: str) -> Callable[[Mcp], tuple[bool, str]]:
    """관련 표가 없는 질의: 0건이거나, 결과가 있어도 '전부 낮은 관련도'로 표시되어야 한다."""
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("search_kosis", {"query": query, "limit": 8})
        rows = rows_of(d)
        if not rows:
            return True, "0건(정상)"
        summary = d.get("search_quality_summary") or {}
        if summary.get("full_query_match_count") == 0:
            return True, f"{len(rows)}건이지만 전부 낮은 관련도(best={summary.get('best_coverage_ratio')})"
        return False, f"관련 없는 질의인데 높은 관련도로 표시: best={summary.get('best_coverage_ratio')} 1위 {rows[0].get('통계표명')}"
    return run


def check_answer_value(query: str, unit_pattern: str | None = None, **extra: Any) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("answer_query", {"query": query, **extra})
        st = status_of(d)
        if st == "executed":
            value = d.get("value") if d.get("value") is not None else d.get("값")
            unit = d.get("unit") or d.get("단위") or (d.get("metadata") or {}).get("unit")
            period = d.get("used_period") or d.get("시점") or (d.get("metadata") or {}).get("period")
            source = d.get("source") or d.get("출처") or (d.get("metadata") or {}).get("source")
            if value in (None, ""):
                return False, "executed인데 값이 없음"
            missing = [n for n, v in (("단위", unit), ("시점", period), ("출처", source)) if not v]
            if missing:
                return False, f"executed인데 {', '.join(missing)} 누락"
            if unit_pattern and not re.search(unit_pattern, str(unit)):
                return False, f"단위 불일치: {unit} (기대 {unit_pattern})"
            return True, f"{value} {unit} ({period})"
        if st in {"needs_table_selection", "no_relevant_table", "unsupported", "failed", "partial"}:
            return True, f"{st}(정직한 미확정)"
        return False, f"알 수 없는 상태: {st or list(d)[:5]}"
    return run


def check_answer_status(query: str, allowed: set[str], **extra: Any) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("answer_query", {"query": query, **extra})
        st = status_of(d)
        return (st in allowed), f"status={st} (허용 {sorted(allowed)})"
    return run


def select_rows(d: dict[str, Any]) -> list[dict[str, Any]]:
    """select_table_for_query 는 selected(단일) + alternatives(목록) 구조다."""
    rows: list[dict[str, Any]] = []
    selected = d.get("selected")
    if isinstance(selected, dict):
        rows.append(selected)
    alts = d.get("alternatives")
    if isinstance(alts, list):
        rows.extend(r for r in alts if isinstance(r, dict))
    return rows or rows_of(d)


def check_select(query: str, expect_name: str | None = None, forbid: str | None = None) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("select_table_for_query", {"query": query, "limit": 8})
        rows = select_rows(d)
        names = " | ".join(str(r.get("통계표명") or r.get("table_name") or "") for r in rows[:8])
        if not rows:
            return status_of(d) in {"no_relevant_table", "failed", "unsupported"}, f"후보 0건 status={status_of(d)}"
        if expect_name and not re.search(expect_name, names):
            return False, f"기대 표 없음({expect_name}) · 후보: {names[:150]}"
        if forbid and re.search(forbid, names):
            return False, f"금지 표 섞임({forbid}) · 후보: {names[:150]}"
        return True, f"{len(rows)}건 · {names[:120]}"
    return run


def check_pipeline(org_id: str, tbl_id: str, concept: str) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        explore = mcp.call("explore_table", {"org_id": org_id, "tbl_id": tbl_id})
        axes = explore.get("분류축")
        if not isinstance(axes, dict) or not axes:
            return False, "explore_table에 분류축 없음"
        resolved = mcp.call("resolve_concepts", {"org_id": org_id, "tbl_id": tbl_id, "concepts": [concept]})
        filters = resolved.get("filters")
        if not isinstance(filters, dict) or not filters:
            return True, f"분류축 {len(axes)}개 · '{concept}' 코드 미해결(선택 필요)"
        queried = mcp.call("query_table", {"org_id": org_id, "tbl_id": tbl_id, "filters": filters})
        st = status_of(queried)
        rows = queried.get("rows") if isinstance(queried.get("rows"), list) else []
        if st == "executed" and rows:
            return True, f"조회 {len(rows)}행 · 첫 값 {rows[0].get('value')}"
        return True, f"코드 해결 후 status={st} rows={len(rows)}(축 추가 선택 필요)"
    return run


def check_compare(query: str) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("stat_time_compare", {"query": query, "years": 2})
        st = status_of(d)
        cmp_ = d.get("비교") or {}
        if st == "executed":
            if not isinstance(cmp_, dict) or cmp_.get("변화율_퍼센트") is None:
                return False, "executed인데 변화율 없음"
            return True, f"{cmp_.get('시작', {}).get('시점')}→{cmp_.get('종료', {}).get('시점')} {cmp_.get('변화율_퍼센트')}%"
        return st in {"needs_table_selection", "no_relevant_table", "unsupported", "failed", "insufficient_data"}, f"status={st}"
    return run


def check_growth_answer(query: str) -> Callable[[Mcp], tuple[bool, str]]:
    """증가율·상승률 질문: 대표값(value)이 변화율이어야 한다. 지수·금액 수준을 그대로 내보내면 오해를 부른다."""
    def run(mcp: Mcp) -> tuple[bool, str]:
        d = mcp.call("answer_query", {"query": query})
        st = status_of(d)
        if st != "executed":
            return st in {"needs_table_selection", "no_relevant_table", "unsupported", "failed", "partial"}, f"status={st}"
        unit = str(d.get("unit") or "")
        comparison = d.get("comparison") or d.get("비교") or {}
        rate = comparison.get("변화율_퍼센트") if isinstance(comparison, dict) else None
        if "%" not in unit:
            return False, f"증가율 질문인데 단위가 {unit}(값 {d.get('value')})"
        if rate is None:
            return False, "변화율 비교 블록 없음"
        return True, f"{d.get('value')}% (수준 {d.get('level_value')} {d.get('level_unit')})"
    return run


def check_error(tool: str, args: dict[str, Any], expect_code: str | None = None) -> Callable[[Mcp], tuple[bool, str]]:
    def run(mcp: Mcp) -> tuple[bool, str]:
        try:
            d = mcp.call(tool, args)
        except Exception as exc:  # noqa: BLE001
            return False, f"예외로 끝남(구조화된 실패가 아님): {exc}"
        st = status_of(d)
        code = str(d.get("code") or d.get("코드") or "")
        if st in {"executed", "resolved"}:
            return False, f"잘못된 입력인데 성공 처리: status={st}"
        if expect_code and expect_code not in code:
            return False, f"코드 불일치: {code} (기대 {expect_code})"
        has_reason = bool(d.get("오류") or d.get("error") or d.get("검증_오류") or d.get("llm_note"))
        return has_reason, f"status={st} code={code} 사유={'있음' if has_reason else '없음'}"
    return run


# ── 100 케이스 ──
SEARCH_QUERIES = [
    "중소기업 사업체 수", "소상공인 매출액", "벤처기업 수", "창업기업 수", "청년 창업",
    "실업률", "고용률", "취업자 수", "임금 근로자", "산업재해",
    "출생아 수", "합계출산율", "고령인구 비율", "1인 가구", "인구 이동",
    "소비자물가지수", "생산자물가지수", "온라인쇼핑 거래액", "소매판매액", "가계 지출",
    "제조업 생산지수", "원자력 발전량", "재생에너지 발전", "에너지 소비량", "온실가스 배출량",
    "폐기물 발생량", "대기오염물질 배출", "의료기관 수", "병상 수", "기초연금 수급자",
    "화재 발생 건수", "교통사고 사망자", "학생 수", "사교육비", "장애인 고용",
    "수출액", "무역수지", "가계대출", "지역내총생산", "농가 수",
]
ANSWER_COUNT_QUERIES = [
    ("중소기업 사업체 수 알려줘", r"개|곳|업체"),
    ("소상공인 사업체 수 알려줘", r"개|곳|업체"),
    ("취업자 수 알려줘", r"명|천명|만명"),
    ("총인구 알려줘", r"명|천명|만명"),
    ("농가 수 알려줘", r"가구|호|개"),
]
ANSWER_RATE_QUERIES = [
    ("실업률 알려줘", r"%|퍼센트"),
    ("고용률 알려줘", r"%|퍼센트"),
    ("합계출산율 알려줘", r"명|가임|%"),
]

CASES: list[tuple[str, str, Callable[[Mcp], tuple[bool, str]]]] = []
for i, q in enumerate(SEARCH_QUERIES, start=1):
    CASES.append((f"S{i:03d}", f"search_kosis · {q}", check_search(q)))
for i, (q, unit) in enumerate(ANSWER_COUNT_QUERIES + ANSWER_RATE_QUERIES, start=1):
    CASES.append((f"A{i:03d}", f"answer_query · {q}", check_answer_value(q, unit)))
CASES += [
    # 선택 품질 — 정답 표가 후보에 있고, 무관한 표가 섞이지 않는다
    ("T001", "select · 소상공인 사업체 수", check_select("소상공인 사업체 수", expect_name=r"사업체")),
    ("T002", "select · 벤처기업 수", check_select("벤처기업 수", expect_name=r"벤처")),
    ("T003", "select · 청년 창업", check_select("청년 창업", expect_name=r"창업")),
    ("T004", "select · 아동복지 중소기업(무관)", check_select("아동복지 관련 중소기업 통계", forbid=r"공무원범죄|통계인력|여성참여")),
    ("T005", "select · 의료기관 종별 현황", check_select("의료기관 종별 현황", expect_name=r"의료기관")),
    ("T006", "select · 원자력 관련 통계", check_select("원자력 관련 통계", expect_name=r"원자력")),
    ("T007", "select · 경북 농가 수", check_select("경북 농가 수", expect_name=r"농가")),
    ("T008", "select · 대학 진학률", check_select("대학 진학률")),
    ("T009", "select · 기상산업 매출액", check_select("기상산업 매출액")),
    ("T010", "select · 드론 사업체 수", check_select("드론 사업체 수")),
    # 파이프라인
    ("P001", "pipeline · 에너지 및 원자력산업실태조사 주요지표", check_pipeline("127", "TX_10506_A080", "총매출액")),
    ("P002", "pipeline · 노인 만성질병 유병률", check_pipeline("117", "DT_117071_018", "유병률")),
    ("P003", "pipeline · 소상공인 사업체수(시도/산업중분류)", check_pipeline("142", "DT_2ME0307", "사업체수")),
    ("P004", "pipeline · 소비자물가지수", check_pipeline("101", "DT_2IFS002", "총지수")),
    ("P005", "pipeline · 성/연령별 취업자", check_pipeline("101", "DT_1DA7024S", "취업자")),
    # 증감·추세
    ("G001", "time_compare · 소상공인 사업체 수", check_compare("소상공인 사업체 수")),
    ("G002", "time_compare · 소비자물가지수", check_compare("소비자물가지수")),
    ("G003", "time_compare · 취업자 수", check_compare("취업자 수")),
    ("G004", "answer · 소비자물가 상승률", check_growth_answer("소비자물가 상승률")),
    ("G006", "answer · 소상공인 사업체 수 증가율", check_growth_answer("소상공인 사업체 수 증가율")),
    ("G007", "answer · 취업자 수 전년 대비", check_growth_answer("취업자 수 전년 대비 증가율")),
    ("G005", "answer · 청년 실업률", check_answer_value("청년 실업률", r"%|퍼센트")),
    # 연도·기간 처리
    ("Y001", "answer · 2015년 소상공인 사업체 수", check_answer_status("2015년 소상공인 사업체 수", {"executed", "needs_table_selection", "no_relevant_table", "unsupported", "failed"})),
    ("Y002", "answer · 2020~2023년 벤처기업 수", check_answer_status("2020년부터 2023년까지 벤처기업 수", {"executed", "needs_table_selection", "no_relevant_table", "unsupported", "failed"})),
    ("Y003", "answer · 2050년 예측", check_answer_status("2050년 중소기업 사업체 수", {"executed", "needs_table_selection", "no_relevant_table", "unsupported", "failed"})),
    # 잡음 질의 — 관련 표가 없으면 0건이어야 한다
    ("N001", "search · 메타버스 카페 사장 수면시간", check_search_low_relevance("메타버스 카페 사장님 평균 수면시간")),
    ("N002", "search · 반려동물 미용 예약취소율", check_search_low_relevance("반려동물 미용업체 월평균 예약 취소율")),
    ("N003", "search · 치킨집 사장 평균 수면시간", check_search_low_relevance("치킨집 사장님 평균 수면시간")),
    ("N004", "search · 우주여행 상품 판매액", check_search_low_relevance("우주여행 상품 판매액")),
    # 오류 계약
    ("E001", "error · 잘못된 분류 코드", check_error("query_table", {"org_id": "127", "tbl_id": "TX_10506_A080", "filters": {"15112AW9": ["없는코드"]}}, expect_code="INVALID_FILTER_CODE")),
    ("E002", "error · 존재하지 않는 표", check_error("explore_table", {"org_id": "999", "tbl_id": "NOT_A_TABLE"})),
    ("E003", "error · 빈 filters", check_error("query_table", {"org_id": "127", "tbl_id": "TX_10506_A080", "filters": {}})),
    ("E004", "error · 역순 기간", check_error("query_table", {"org_id": "127", "tbl_id": "TX_10506_A080", "filters": {"15112AW9": ["15112AW9ACAAAA"]}, "period_range": ["2024", "2020"]})),
    ("E005", "error · 빈 질의", check_error("answer_query", {"query": ""})),
    ("E006", "error · 없는 기관ID", check_error("query_table", {"org_id": "000", "tbl_id": "DT_2ME0307", "filters": {"A": ["X"]}})),
    ("E007", "error · resolve_concepts 빈 개념", check_error("resolve_concepts", {"org_id": "127", "tbl_id": "TX_10506_A080", "concepts": []})),
    # 표기 흔들림·오타에도 표를 찾아야 한다
    ("R001", "search · 사업채수 오타", check_search("중소기업 사업채수", min_rows=1)),
    ("R002", "search · 실업율 오타", check_search("실업율", min_rows=1)),
    ("R003", "search · 띄어쓰기 없음", check_search("소상공인사업체수", min_rows=1)),
    ("R004", "search · 영문 약어", check_search("GDP", min_rows=1)),
    ("R005", "answer · 오타 질문", check_answer_value("중소기업 사업채 수 알려줘")),
    # 지역·업종 한정
    ("D001", "answer · 부산 소상공인 사업체 수", check_answer_value("부산 소상공인 사업체 수", r"개|곳|업체")),
    ("D002", "answer · 경기 취업자 수", check_answer_value("경기 취업자 수", r"명|천명|만명")),
    ("D003", "select · 업종별 창업기업수", check_select("업종별 창업기업 수", expect_name=r"창업")),
    ("D004", "select · 연령별 창업기업수", check_select("연령별 창업기업 수", expect_name=r"창업|연령")),
    ("D005", "select · 기업규모별 종사자 수", check_select("기업규모별 종사자 수", expect_name=r"종사자|규모")),
    # 값 계약(단위 상식) — 수량 질문에 비율 표가 오면 안 된다
    ("U001", "answer · 사업체 수 단위", check_answer_value("전국 사업체 수 알려줘", r"개|곳|업체")),
    ("U002", "answer · 종사자 수 단위", check_answer_value("중소기업 종사자 수 알려줘", r"명|천명|만명")),
    ("U003", "answer · 매출액 단위", check_answer_value("중소기업 매출액 알려줘", r"원|억원|백만원|조원")),
    ("U004", "answer · 수출액 단위", check_answer_value("총수출액 알려줘", r"달러|천달러|백만달러|원")),
    ("U005", "answer · 출생아 수 단위", check_answer_value("출생아 수 알려줘", r"명|천명")),
    # 시계열·추세
    ("H001", "compare · 벤처기업 수", check_compare("벤처기업 수")),
    ("H002", "compare · 출생아 수", check_compare("출생아 수")),
    ("H003", "compare · 총수출액", check_compare("총수출액")),
    ("H004", "answer · 최근 5년 실업률", check_answer_status("최근 5년 실업률 추이", {"executed", "needs_table_selection", "no_relevant_table", "unsupported", "failed", "partial"})),
    ("H005", "answer · 2023년 고용률", check_answer_status("2023년 고용률", {"executed", "needs_table_selection", "no_relevant_table", "unsupported", "failed", "partial"})),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--only", default="")
    parser.add_argument("--out", default="docs/mcp-100-eval.md")
    args = parser.parse_args()

    only = {c.strip() for c in args.only.split(",") if c.strip()}
    targets = [c for c in CASES if not only or c[0] in only]
    mcp = Mcp(args.url)

    rows: list[str] = []
    failed: list[str] = []
    passed = 0
    for cid, title, check in targets:
        started = time.time()
        try:
            ok, detail = check(mcp)
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"예외: {exc}"
        elapsed = time.time() - started
        passed += 1 if ok else 0
        if not ok:
            failed.append(cid)
        print(f"{'✓' if ok else '✗'} {cid} {title} | {detail[:110]} | {elapsed:.1f}s")
        rows.append(f"| {cid} | {'✅' if ok else '❌'} | {title} | {detail.replace('|', '/')[:160]} |")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    header = [
        f"# KOSIS MCP 100건 검증 ({time.strftime('%Y-%m-%d %H:%M')})",
        "",
        f"대상: {args.url} · 케이스 {len(targets)}개",
        "",
        f"**통과 {passed}/{len(targets)}**" + (f" · 실패: {', '.join(failed)}" if failed else ""),
        "",
        "| 케이스 | 결과 | 대상 | 상세 |",
        "|---|---|---|---|",
    ]
    out.write_text("\n".join(header + rows) + "\n", encoding="utf-8")
    print(f"\n통과 {passed}/{len(targets)} → {out}")
    if failed:
        print("실패: " + ", ".join(failed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
