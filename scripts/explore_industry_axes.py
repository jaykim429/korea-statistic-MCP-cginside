"""Round-6 Step 2b: dump the industry classification axis of the
KSIC-broken-out KOSIS tables we already touch, so we can mass-add
keyword entries for each KSIC mid-section (도소매, 음식점, 제조업,
건설, 운수, 정보통신, 금융, 부동산, 전문과학, 교육, 보건복지,
예술스포츠).

Targets (all org_id=142 except where noted):
- DT_BR_A001  중소기업 매출액
- DT_BR_B001  중소기업 사업체수 (추정 위치)
- DT_BR_C001  중소기업 종사자수
- DT_3ME0100  시도/산업중분류별 주요지표

We print the full ITM axis (not just OBJ_ID=ITEM) so the curator can
see the industry codes.
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

from kosis_mcp_server import explore_table

TARGETS = [
    ("142", "DT_BR_A001",  "중소기업 매출액"),
    ("142", "DT_BR_B001",  "중소기업 사업체수 (추정)"),
    ("142", "DT_BR_C001",  "중소기업 종사자수"),
    ("142", "DT_3ME0100",  "시도/산업중분류별 주요지표 (소상공인 등)"),
]


async def main() -> None:
    for org_id, tbl_id, label in TARGETS:
        print("=" * 76)
        print(f"{label} — orgId={org_id} tblId={tbl_id}")
        print("=" * 76)
        result = await explore_table(org_id, tbl_id)
        print(f"통계표명: {result.get('통계표명')}")
        print(f"수록기간: {result.get('수록기간')}")
        axes = result.get("분류축") or {}
        for obj_id, axis in axes.items():
            items = axis.get("items") or []
            print(f"\n  OBJ_ID={obj_id} ({axis.get('OBJ_NM')}) — {len(items)}개")
            for it in items:  # full list, no trim
                print(f"    ITM_ID={it.get('ITM_ID')!s:<24s} "
                      f"ITM_NM={it.get('ITM_NM')!s:<32s} "
                      f"UP={it.get('UP_ITM_ID')!s:<12s} "
                      f"UNIT={it.get('UNIT_NM')!s}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
