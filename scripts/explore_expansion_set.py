"""Print full explore_table output for the 4 easiest Tier-A expansion
candidates (Group A + E) so the curator can pick exact ITM_IDs.

Run:
    .\.venv-kosis\Scripts\python.exe scripts\explore_expansion_set.py
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
    ("101", "DT_1YL12501E", "노령화지수"),
    ("101", "DT_1YL15006", "월평균임금"),
    ("101", "DT_1YL3001", "범죄"),
    ("314", "DT_TRD_TGT_ENT_AGG_MONTH", "외래관광객"),
]


async def main() -> None:
    for org_id, tbl_id, label in TARGETS:
        print("=" * 72)
        print(f"{label} — orgId={org_id} tblId={tbl_id}")
        print("=" * 72)
        result = await explore_table(org_id, tbl_id)
        # Print only the parts a curator needs to pick ITM_IDs: table
        # name, period, classification axes with their full item lists
        print(f"통계표명: {result.get('통계표명')}")
        print(f"수록기간: {result.get('수록기간')}")
        axes = result.get("분류축") or {}
        for obj_id, axis in axes.items():
            items = axis.get("items") or []
            print(f"\n  OBJ_ID={obj_id} ({axis.get('OBJ_NM')}) — {len(items)}개 아이템")
            for it in items[:30]:
                print(f"    ITM_ID={it.get('ITM_ID')!s:<12s} ITM_NM={it.get('ITM_NM')!s:<28s} "
                      f"UP_ITM_ID={it.get('UP_ITM_ID')!s:<10s} UNIT={it.get('UNIT_NM')!s}")
            if len(items) > 30:
                print(f"    ... +{len(items) - 30}개 더 (전체는 artifacts에 저장)")
        print()


if __name__ == "__main__":
    asyncio.run(main())
