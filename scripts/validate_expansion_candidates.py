"""Round-6 Stage 31: validate Tier A expansion candidates against the
live KOSIS metadata API before we commit them to `kosis_curation.py`.

Source: comparison with Dayoooun/korea-stats-mcp's `quickStatsParams.ts`
surfaced 17 keyword groups across 14 distinct tables that we do not yet
cover (인구동태 비율, 결혼 연령, 노령화지수, 임금, 아파트전세, 자동차,
범죄, 관광, 교통사고, 의사수, 미세먼지). The other project hard-codes
these tblIds; before trusting them as our own Tier A entries we want
to confirm:

  1. the table still exists (TBL meta returns a name)
  2. the recorded period range is current (END_PRD_DE recent)
  3. the classification axes (objL1..L3) line up with how we expect to
     pass region / item parameters into quick_stat
  4. for tables that should be region-broken-out (시도), a region axis
     actually exists in ITM meta

This script does NOT mutate curation. It produces a JSON report under
`artifacts/expansion_audit/expansion_meta.json` plus a console summary
so a human can review and approve before edits land in
`kosis_curation.py`.

Run:
    $env:KOSIS_API_KEY = "..."
    .\.venv-kosis\Scripts\python.exe scripts\validate_expansion_candidates.py
"""
from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from typing import Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import (
    _fetch_table_name,
    _fetch_classifications,
    _fetch_period_range,
    _pick_finest_period,
)


# Candidates: each row groups one KOSIS table with the keyword(s) the
# other project mapped to it. `expected_axis_hint` is a free-text note
# of what we'd expect the ITM/OBJ axes to look like so the auditor can
# spot when the live schema disagrees with the assumed shape.
CANDIDATES: list[dict[str, Any]] = [
    # 인구동태 — same table feeds 5 different rate keywords
    {"category": "인구동태",
     "keywords": ["조출생률", "조사망률", "자연증가율", "조혼인율", "조이혼율",
                  "출생아수_월별", "사망자수_월별", "혼인건수_월별", "이혼건수_월별"],
     "org_id": "101", "tbl_id": "DT_1B8000G",
     "expected_axis_hint": "지표 분류축(출생/사망/혼인/이혼 × 건수/율), 시점 월별"},
    # 결혼 연령
    {"category": "결혼연령",
     "keywords": ["평균초혼연령_남", "평균초혼연령_여"],
     "org_id": "101", "tbl_id": "INH_1B83A09",
     "expected_axis_hint": "남편/아내 분류축, 단위=세"},
    # 노령화지수
    {"category": "고령화",
     "keywords": ["노령화지수"],
     "org_id": "101", "tbl_id": "DT_1YL12501E",
     "expected_axis_hint": "시도별 노령화지수 (65세이상/15세미만×100)"},
    # 임금
    {"category": "임금",
     "keywords": ["상용근로자_월평균임금"],
     "org_id": "101", "tbl_id": "DT_1YL15006",
     "expected_axis_hint": "상용근로자 월평균 임금, 단위=원"},
    # 아파트 전세
    {"category": "주거",
     "keywords": ["아파트전세가격지수"],
     "org_id": "101", "tbl_id": "DT_1YL20171E",
     "expected_axis_hint": "주택유형(아파트) × 시도 × 시점"},
    # 자동차
    {"category": "자동차",
     "keywords": ["자동차등록대수"],
     "org_id": "101", "tbl_id": "DT_1YL20731",
     "expected_axis_hint": "시도별 자동차 등록대수, 단위=대"},
    # 범죄
    {"category": "범죄",
     "keywords": ["범죄발생건수", "범죄율"],
     "org_id": "101", "tbl_id": "DT_1YL3001",
     "expected_axis_hint": "시도 × 죄종, 단위=건/천명당"},
    # 관광
    {"category": "관광",
     "keywords": ["외래관광객수"],
     "org_id": "314", "tbl_id": "DT_TRD_TGT_ENT_AGG_MONTH",
     "expected_axis_hint": "월별 외래관광객 (국적별), 단위=명"},
    # 교통사고
    {"category": "교통사고",
     "keywords": ["교통사고발생건수"],
     "org_id": "101", "tbl_id": "DT_1YL21051",
     "expected_axis_hint": "시도 × 사고유형, 단위=건"},
    # 의사
    {"category": "의료",
     "keywords": ["의사수"],
     "org_id": "101", "tbl_id": "DT_1YL20981",
     "expected_axis_hint": "시도별 의료기관 종사 의사수, 단위=명"},
    # 미세먼지
    {"category": "환경",
     "keywords": ["초미세먼지_PM25"],
     "org_id": "106", "tbl_id": "DT_106N_03_0200145",
     "expected_axis_hint": "측정소/시도 × 시점, 단위=μg/m³"},
    {"category": "환경",
     "keywords": ["미세먼지_PM10"],
     "org_id": "106", "tbl_id": "DT_106N_03_0200045",
     "expected_axis_hint": "측정소/시도 × 시점, 단위=μg/m³"},
]


PER_CALL_TIMEOUT = 12.0
TOTAL_BUDGET = 120.0


def _summarize_axes(itm_rows: list[dict]) -> dict[str, Any]:
    """Group ITM rows by OBJ_ID so the auditor can see how many
    classification axes the table exposes and what they hold. Trims to
    a sample of items per axis to keep the report readable."""
    axes: dict[str, dict[str, Any]] = {}
    for row in itm_rows or []:
        obj_id = str(row.get("OBJ_ID") or "")
        if not obj_id:
            continue
        axis = axes.setdefault(obj_id, {
            "OBJ_NM": row.get("OBJ_NM"),
            "OBJ_NM_ENG": row.get("OBJ_NM_ENG"),
            "item_count": 0,
            "sample_items": [],
            "all_units": set(),
        })
        axis["item_count"] += 1
        if len(axis["sample_items"]) < 8:
            axis["sample_items"].append({
                "ITM_ID": row.get("ITM_ID"),
                "ITM_NM": row.get("ITM_NM"),
                "UP_ITM_ID": row.get("UP_ITM_ID"),
                "UNIT_NM": row.get("UNIT_NM"),
            })
        unit = row.get("UNIT_NM")
        if unit:
            axis["all_units"].add(unit)
    # JSON-serializable cleanup
    for a in axes.values():
        a["all_units"] = sorted(a["all_units"])
    return axes


def _looks_like_region_axis(axis: dict[str, Any]) -> bool:
    """A region axis usually has 17–18 items (전국 + 17 시도) and item
    names containing 시도 keywords. Heuristic — auditor still reviews."""
    items = axis.get("sample_items") or []
    region_hits = sum(
        1 for it in items
        if any(token in str(it.get("ITM_NM") or "")
               for token in ("서울", "부산", "대구", "인천", "광주", "대전",
                             "울산", "세종", "경기", "강원", "충북", "충남",
                             "전북", "전남", "경북", "경남", "제주", "전국"))
    )
    return region_hits >= 2 and axis.get("item_count", 0) >= 6


async def _validate_one(candidate: dict[str, Any]) -> dict[str, Any]:
    """Fan out TBL+ITM+PRD meta in parallel for one candidate, then
    fold the results into a single per-candidate report row."""
    org_id = candidate["org_id"]
    tbl_id = candidate["tbl_id"]
    base = {
        "category": candidate["category"],
        "keywords": candidate["keywords"],
        "org_id": org_id,
        "tbl_id": tbl_id,
        "expected_axis_hint": candidate["expected_axis_hint"],
    }
    async def safe(coro_factory, label):
        try:
            return await asyncio.wait_for(coro_factory(), timeout=PER_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            return {"_error": f"{label} meta timeout {PER_CALL_TIMEOUT}s"}
        except Exception as exc:
            return {"_error": f"{label} fetch failed: {exc!r}"}

    name_rows, item_rows, period_rows = await asyncio.gather(
        safe(lambda: _fetch_table_name(org_id, tbl_id), "TBL"),
        safe(lambda: _fetch_classifications(org_id, tbl_id), "ITM"),
        safe(lambda: _fetch_period_range(org_id, tbl_id), "PRD"),
    )

    table_name = None
    table_error = None
    if isinstance(name_rows, dict) and name_rows.get("_error"):
        table_error = name_rows["_error"]
    elif isinstance(name_rows, list) and name_rows:
        table_name = name_rows[0].get("TBL_NM") or name_rows[0].get("tblNm")

    axes = None
    item_error = None
    has_region_axis = False
    if isinstance(item_rows, dict) and item_rows.get("_error"):
        item_error = item_rows["_error"]
    elif isinstance(item_rows, list):
        axes = _summarize_axes(item_rows)
        has_region_axis = any(_looks_like_region_axis(a) for a in axes.values())

    period_summary = None
    period_error = None
    if isinstance(period_rows, dict) and period_rows.get("_error"):
        period_error = period_rows["_error"]
    elif isinstance(period_rows, list) and period_rows:
        latest = _pick_finest_period(period_rows)
        period_summary = {
            "cadences": sorted({
                str(r.get("PRD_SE") or r.get("prdSe") or "").strip()
                for r in period_rows if r.get("PRD_SE") or r.get("prdSe")
            }),
            "finest_cadence": latest.get("PRD_SE") or latest.get("prdSe"),
            "start": latest.get("STRT_PRD_DE") or latest.get("strtPrdDe"),
            "end": latest.get("END_PRD_DE") or latest.get("endPrdDe"),
        }

    # Classification: alive + axes present + period recent → ✅, anything
    # missing → ⚠️ needs_attention, hard errors → ❌
    errors = [e for e in (table_error, item_error, period_error) if e]
    if errors:
        status = "❌ dead_or_blocked"
    elif not table_name or not axes or not period_summary:
        status = "⚠️ partial_metadata"
    else:
        status = "✅ alive"

    base.update({
        "status": status,
        "table_name": table_name,
        "errors": errors or None,
        "axes": axes,
        "has_region_axis_heuristic": has_region_axis,
        "period": period_summary,
    })
    return base


async def main() -> None:
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[_validate_one(c) for c in CANDIDATES]),
            timeout=TOTAL_BUDGET,
        )
    except asyncio.TimeoutError:
        print(f"❌ 전체 검증 예산 {TOTAL_BUDGET}s 초과")
        return

    summary: dict[str, int] = {}
    for row in results:
        summary[row["status"]] = summary.get(row["status"], 0) + 1

    out_dir = ROOT / "artifacts" / "expansion_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "expansion_meta.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"확장 후보: {len(results)}개 통계표")
    print()
    print(f"{'상태':<22s} 카운트")
    print("-" * 32)
    for status in sorted(summary.keys()):
        print(f"{status:<22s} {summary[status]}")
    print()
    print(f"상세 리포트: {out_path}")
    print()
    print("=" * 70)
    for row in results:
        print(f"{row['status']:<22s} {row['category']:<8s} {row['tbl_id']:<28s} "
              f"orgId={row['org_id']}")
        if row.get("table_name"):
            print(f"    통계표명     : {row['table_name']}")
        if row.get("period"):
            p = row["period"]
            print(f"    수록기간     : {p['start']} ~ {p['end']} (주기={p['cadences']})")
        if row.get("axes"):
            print(f"    분류축 {len(row['axes'])}개:")
            for obj_id, axis in row["axes"].items():
                region_flag = " [시도 추정]" if _looks_like_region_axis(axis) else ""
                units = "/".join(axis["all_units"][:3]) if axis["all_units"] else "-"
                print(f"      {obj_id} {axis['OBJ_NM']!s:<24s} "
                      f"items={axis['item_count']:<3d} units={units}{region_flag}")
        if row.get("errors"):
            for e in row["errors"]:
                print(f"    ❌ {e}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
