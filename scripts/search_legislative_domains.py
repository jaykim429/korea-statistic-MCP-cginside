"""Round-6 Step 3: batch-search KOSIS for statistical tables across the
domains that drive National Assembly committee bill review and ministry
policy analysis.

각 상임위·분야마다 핵심 키워드 1~3개로 search_kosis를 호출해 상위 5개
통계표 후보를 식별. 사용자가 한 번 실행하면 50~80개의 통계표 ID 풀이
모이고, 그 풀에서 우선순위가 높은 것들을 validate_expansion_candidates
패턴으로 메타 검증한 후 Tier A에 추가한다.

분야 분류는 국회 17개 상임위 중심:
- 기재위 / 정무위 / 행안위 / 교육위 / 과방위
- 외통위 / 국방위 / 문체위 / 농수산위
- 산자중기위 / 복지위 / 환노위 / 국토위 / 여가위

이미 우리 큐레이션에 잘 잡힌 분야(중소기업·소상공인 = 산자중기위 일부)는
의도적으로 키워드를 줄여 중복 탐색을 피한다.

Run:
    .\.venv-kosis\Scripts\python.exe scripts\search_legislative_domains.py
"""
from __future__ import annotations
import asyncio
import io
import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import search_kosis


# 상임위 → [핵심 검색 키워드 리스트]. KOSIS 검색 결과 상위 3~5개
# 통계표를 후보로 식별. 분야별로 4~6개 쿼리를 잡아 14개 분야 × 평균 5
# 쿼리 = 약 70 검색 호출. KOSIS 검색은 가벼워 90s 안에 끝나야 함.
DOMAINS: dict[str, list[str]] = {
    "기재": [
        "국세수입", "국가채무", "조세부담률", "통합재정수지", "외환보유액",
    ],
    "정무": [
        "가계대출", "공정거래 신고", "가맹점", "보훈대상자", "신용카드 사용",
    ],
    "행안": [
        "지방재정자립도", "공무원 수", "화재 발생건수", "자연재해 피해",
    ],
    "교육": [
        "초중고 학생수", "교원 수", "사교육비", "대학 진학률", "학교 수",
    ],
    "과방": [
        "연구개발비", "이동전화 가입자", "초고속인터넷", "방송사업 매출",
    ],
    "외통": [
        "재외동포", "외국인 등록자", "다문화가구",
    ],
    "국방": [
        "병력수", "국방비",
    ],
    "문체": [
        "문화시설 수", "체육시설", "출판물", "공연시설", "박물관",
    ],
    "농수산": [
        "농가수", "농가소득", "식량자급률", "수산업 생산량", "어가소득",
    ],
    "산자중기": [
        "산업생산지수", "벤처기업 수", "창업기업", "수출 중소기업", "프랜차이즈",
    ],
    "복지": [
        "기초생활보장수급자", "국민연금 가입자", "건강보험 가입자",
        "노인장기요양", "기초연금 수급자", "병상수",
    ],
    "환노": [
        "산업재해 발생", "최저임금", "근로시간", "재활용률",
        "온실가스 배출량", "신재생에너지",
    ],
    "국토": [
        "주택보급률", "미분양주택", "공동주택 공시가격",
        "대중교통 수송", "항공여객", "도시계획",
    ],
    "여가": [
        "여성경제활동참가율", "한부모가구", "보육시설", "청소년",
    ],
}


PER_QUERY_TIMEOUT = 15.0


async def _safe_search(query: str) -> dict:
    try:
        return await asyncio.wait_for(
            search_kosis(query, limit=5),
            timeout=PER_QUERY_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return {"오류": f"검색 timeout {PER_QUERY_TIMEOUT}s"}
    except Exception as exc:
        return {"오류": repr(exc)}


def _extract_candidates(result: dict) -> list[dict]:
    """search_kosis 응답: {"결과": [...]} — 통계표명/통계표ID/기관ID 포함."""
    if not isinstance(result, dict):
        return []
    if result.get("오류"):
        return [{"_error": result["오류"]}]
    rows = result.get("결과") or []
    tier_a = result.get("Tier_A_직접_매핑")
    out: list[dict] = []
    if tier_a:
        # Tier A 직접 매핑이 있으면 최상단에 표시 — 이미 큐레이션 보유
        out.append({
            "통계표ID":  tier_a.get("통계표ID"),
            "기관ID":   tier_a.get("기관ID"),
            "통계표명":  f"[Tier A 보유] {tier_a.get('통계표')}",
            "수록기간":  None,
        })
    for r in rows[:5]:
        if not isinstance(r, dict):
            continue
        out.append({
            "통계표ID":  r.get("통계표ID"),
            "기관ID":   r.get("기관ID"),
            "통계표명":  r.get("통계표명"),
            "수록기간":  r.get("수록기간"),
        })
    return out


async def main() -> None:
    all_results: dict[str, dict[str, list[dict]]] = {}
    for domain, queries in DOMAINS.items():
        domain_block: dict[str, list[dict]] = {}
        coros = [_safe_search(q) for q in queries]
        results = await asyncio.gather(*coros)
        for q, r in zip(queries, results):
            domain_block[q] = _extract_candidates(r)
        all_results[domain] = domain_block

    out_dir = ROOT / "artifacts" / "legislative_search"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "domain_candidates.json"
    out_path.write_text(json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8")

    # 콘솔에 분야별 상위 후보를 요약 출력. 큐레이션 추가 우선순위를
    # 한눈에 볼 수 있도록 분야 안에서도 첫 통계표만 노출.
    print(f"분야별 통계표 후보 검색 완료 — 상세 리포트: {out_path}")
    print()
    print(f"{'분야':<10s} {'질의':<22s} 후보 통계표 (상위 1개)")
    print("-" * 96)
    for domain, queries in all_results.items():
        for query, candidates in queries.items():
            if not candidates:
                print(f"{domain:<10s} {query:<22s} (결과 없음)")
                continue
            if candidates[0].get("_error"):
                print(f"{domain:<10s} {query:<22s} ❌ {candidates[0]['_error']}")
                continue
            top = candidates[0]
            tbl_id = top.get("통계표ID") or "?"
            tbl_nm = top.get("통계표명") or "?"
            # 통계표명은 길어서 60자에서 자름
            short_nm = (tbl_nm[:55] + "..") if len(str(tbl_nm)) > 57 else tbl_nm
            print(f"{domain:<10s} {query:<22s} {tbl_id:<22s} {short_nm}")


if __name__ == "__main__":
    asyncio.run(main())
