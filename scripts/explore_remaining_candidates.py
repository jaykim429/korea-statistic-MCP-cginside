"""Round-6 Step 2d: dump full ITM/PRD metadata for the 8 validate_expansion
candidates we haven't yet added to Tier A. The curator uses this output
to pick exact ITM_IDs and decide region_scheme mappings.

8개 통계표:
- DT_1B8000G   인구동태 (출생/사망/혼인/이혼) — 종류별 axis 활용
- INH_1B83A09  평균 초혼연령 (남편/아내)
- DT_1YL20171E 아파트전세가격지수
- DT_1YL20731  1인당 자동차 등록대수
- DT_1YL20981  인구 천명당 의료기관 종사 의사수
- DT_1YL21051  자동차 천대당 교통사고 발생건수
- DT_106N_03_0200145  초미세먼지 PM2.5
- DT_106N_03_0200045  미세먼지 PM10
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
    ("101", "DT_1B8000G",     "인구동태(출생사망혼인이혼)"),
    ("101", "INH_1B83A09",    "평균 초혼연령"),
    ("101", "DT_1YL20171E",   "아파트전세가격지수"),
    ("101", "DT_1YL20731",    "자동차 등록대수"),
    ("101", "DT_1YL20981",    "의사수"),
    ("101", "DT_1YL21051",    "교통사고 발생건수"),
    ("106", "DT_106N_03_0200145", "초미세먼지 PM2.5"),
    ("106", "DT_106N_03_0200045", "미세먼지 PM10"),
]

# 각 axis마다 최대 표시할 아이템 수. 시군구 axis는 256개라
# 핵심(시도) 18개만 확인하면 충분 — 전체 표시는 ITEM/규모 axis에만 적용
ITEM_AXIS_LIMIT = 50
LARGE_AXIS_SAMPLE = 25


async def main() -> None:
    for org_id, tbl_id, label in TARGETS:
        print("=" * 80)
        print(f"{label} — orgId={org_id} tblId={tbl_id}")
        print("=" * 80)
        result = await explore_table(org_id, tbl_id)
        print(f"통계표명: {result.get('통계표명')}")
        print(f"수록기간: {result.get('수록기간')}")
        axes = result.get("분류축") or {}
        for obj_id, axis in axes.items():
            items = axis.get("items") or []
            limit = ITEM_AXIS_LIMIT if len(items) <= 25 else LARGE_AXIS_SAMPLE
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
