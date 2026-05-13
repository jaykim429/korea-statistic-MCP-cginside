"""Round-6 Step 3: dump ITM/PRD metadata for the 19 highest-confidence
statistical tables identified by search_legislative_domains.py across
14 National Assembly committee domains. Output drives a single curation
PR that should land 30~50 new Tier A keywords.

각 통계표마다 분류축(ITEM/지역/기타)의 항목 코드와 단위를 풀로 노출.
시군구 axis는 시도 18개만 추출하면 충분하므로 25개 sample.
"""
from __future__ import annotations
import asyncio
import io
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import explore_table


TARGETS = [
    # (org_id, tbl_id, 분야, 라벨)
    ("301", "DT_102N_A001",      "기재",   "국가채무현황"),
    ("101", "TX_11007_A783",     "기재",   "조세부담률"),
    ("301", "DT_102N_AD01",      "기재",   "통합재정수지"),
    ("301", "DT_151Y005",        "정무",   "예금취급기관 가계대출"),
    ("101", "DT_1YL20921",       "행안",   "재정자립도(시도)"),
    ("101", "DT_1YL8601",        "행안",   "화재발생건수(시도)"),
    ("101", "DT_1YL21171",       "교육",   "교원1인당 학생수(시도)"),
    ("301", "DT_KBA0001",        "과방",   "연구개발비 및 GDP대비"),
    ("101", "DT_1B040A6",        "외통",   "체류외국인"),
    ("101", "DT_122009_001",     "국방",   "국방예산추이"),
    ("114", "DT_1EA1201",        "농수산", "농가경제 주요지표"),
    ("114", "DT_1E4B131",        "농수산", "어가소득"),
    ("301", "DT_1JH20202",       "산자중기", "전산업생산지수"),
    ("101", "DT_1YL20551E_1",    "산자중기", "창업기업수(시도)"),
    ("101", "DT_1YL13801E",      "복지",   "국민기초생활수급자(시도)"),
    ("101", "DT_1YL21311",       "환노",   "생활폐기물 재활용률(시도)"),
    ("106", "DT_106N_99_2800019", "환노",  "국가 온실가스 배출량"),
    ("101", "DT_1YL202112E",     "환노",   "신재생에너지 생산량(시도)"),
    ("101", "DT_1YL13401E",      "국토",   "주택보급률(시도)"),
]

LARGE_AXIS_SAMPLE = 22
SMALL_AXIS_LIMIT = 60


async def main() -> None:
    for org_id, tbl_id, domain, label in TARGETS:
        print("=" * 84)
        print(f"[{domain}] {label} — orgId={org_id} tblId={tbl_id}")
        print("=" * 84)
        try:
            result = await explore_table(org_id, tbl_id)
        except Exception as exc:
            print(f"  ❌ explore_table 실패: {exc!r}\n")
            continue
        if not isinstance(result, dict) or result.get("오류"):
            print(f"  ❌ {result}\n")
            continue
        print(f"통계표명: {result.get('통계표명')}")
        print(f"수록기간: {result.get('수록기간')}")
        axes = result.get("분류축") or {}
        for obj_id, axis in axes.items():
            items = axis.get("items") or []
            limit = SMALL_AXIS_LIMIT if len(items) <= 30 else LARGE_AXIS_SAMPLE
            print(f"\n  OBJ_ID={obj_id} ({axis.get('OBJ_NM')}) — {len(items)}개")
            for it in items[:limit]:
                print(f"    ITM_ID={it.get('ITM_ID')!s:<22s} "
                      f"ITM_NM={it.get('ITM_NM')!s:<40s} "
                      f"UP={it.get('UP_ITM_ID')!s:<14s} "
                      f"UNIT={it.get('UNIT_NM')!s}")
            if len(items) > limit:
                print(f"    ... +{len(items) - limit}개 더")
        print()


if __name__ == "__main__":
    asyncio.run(main())
