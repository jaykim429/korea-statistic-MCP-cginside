"""Round-6 Stage 30: compare every TIER_A_STATS `supported_periods`
tuple against what KOSIS actually publishes via the meta API.

`audit_note_vs_call.py` (Stage 17) catches drift in the *value* a
curation note was written with — same period, different number, or
period regressed. This script catches the orthogonal drift in *which
cadences* the table exposes: a curation entry can declare ("Y",) while
KOSIS quietly added monthly rows, and the natural-language router will
keep rejecting "올해 3월 X" because the slot validator believes the
table is annual-only.

Walks every entry (skipping broken mappings), calls
`_fetch_period_range`, normalizes the PRD_SE values KOSIS returns
(월/분기/반기/년) to the curation alphabet (M/Q/H/Y), and classifies:

  ✅ match              declared cadences match the live PRD set
  ⚠️ under_declared    KOSIS exposes a finer cadence than curation
                        declares (e.g. live M+Q+Y, curation only Y)
  ⚠️ over_declared     curation declares a cadence KOSIS does not
                        publish (probably stale or table renamed)
  ⚠️ both_drift        both directions of difference present
  ❌ fetch_failed      meta call errored or returned empty

Output: console summary + artifacts/period_audit/period_meta.json
with the per-entry diff. Run:

    $env:KOSIS_API_KEY = "..."
    .\.venv-kosis\Scripts\python.exe scripts\audit_period_meta.py

Parallelization mirrors the composite_aggregate fix from Stage 21:
all per-entry meta calls run via asyncio.gather with a per-call
timeout of 12s and an overall budget of 90s so one slow table cannot
stall the audit.
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

from kosis_curation import TIER_A_STATS
from kosis_mcp_server import _fetch_period_range


# KOSIS PRD_SE → curation alphabet. Keep liberal: KOSIS has used both
# Korean and Latin abbreviations across endpoints. Anything we cannot
# map gets surfaced as an "unknown" cadence so the audit does not
# silently drop unexpected values.
_PRD_TO_ALPHA = {
    "월": "M", "M": "M", "MM": "M",
    "분기": "Q", "Q": "Q", "QQ": "Q",
    "반기": "H", "H": "H", "HF": "H",
    "년": "Y", "연": "Y", "Y": "Y", "A": "Y",
}

PER_CALL_TIMEOUT = 12.0
TOTAL_BUDGET = 90.0


def _normalize_live_periods(period_rows: list[dict]) -> tuple[set[str], list[str]]:
    """Return (mapped cadence set, list of raw PRD_SE values we could
    not map). The second list lets the auditor flag truly novel KOSIS
    cadences instead of silently mis-classifying them as match."""
    mapped: set[str] = set()
    unknown: list[str] = []
    for row in period_rows or []:
        se = str(row.get("PRD_SE") or row.get("prdSe") or "").strip()
        if not se:
            continue
        alpha = _PRD_TO_ALPHA.get(se)
        if alpha:
            mapped.add(alpha)
        else:
            unknown.append(se)
    return mapped, unknown


async def _audit_one(key: str, param) -> dict[str, Any]:
    """Per-entry: fetch live PRD, diff against curation. Wraps the
    meta call in asyncio.wait_for so a single hang cannot stall the
    whole audit."""
    declared = set(param.supported_periods or ())
    try:
        period_rows = await asyncio.wait_for(
            _fetch_period_range(param.org_id, param.tbl_id),
            timeout=PER_CALL_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return {
            "key": key, "tbl_id": param.tbl_id,
            "declared": sorted(declared),
            "status": "❌ fetch_failed",
            "error": f"meta timeout {PER_CALL_TIMEOUT}s",
        }
    except Exception as exc:
        return {
            "key": key, "tbl_id": param.tbl_id,
            "declared": sorted(declared),
            "status": "❌ fetch_failed",
            "error": repr(exc),
        }
    if not period_rows:
        return {
            "key": key, "tbl_id": param.tbl_id,
            "declared": sorted(declared),
            "status": "❌ fetch_failed",
            "error": "PRD 메타가 빈 응답",
        }
    live, unknown = _normalize_live_periods(period_rows)
    missing_in_curation = sorted(live - declared)
    extra_in_curation = sorted(declared - live)
    if not missing_in_curation and not extra_in_curation and not unknown:
        status = "✅ match"
    elif missing_in_curation and extra_in_curation:
        status = "⚠️ both_drift"
    elif missing_in_curation:
        status = "⚠️ under_declared"
    elif extra_in_curation:
        status = "⚠️ over_declared"
    else:
        # only unknown KOSIS cadences — treat as needs-attention
        status = "⚠️ unknown_cadence"
    out = {
        "key": key,
        "tbl_id": param.tbl_id,
        "declared": sorted(declared),
        "live": sorted(live),
        "missing_in_curation": missing_in_curation,
        "extra_in_curation": extra_in_curation,
        "status": status,
    }
    if unknown:
        out["unmapped_kosis_PRD_SE"] = sorted(set(unknown))
    return out


async def main() -> None:
    targets = [
        (key, param) for key, param in TIER_A_STATS.items()
        if param.verification_status != "broken"
    ]
    broken = [
        {"key": key, "tbl_id": param.tbl_id, "status": "🚧 broken_mapping"}
        for key, param in TIER_A_STATS.items()
        if param.verification_status == "broken"
    ]

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[_audit_one(k, p) for k, p in targets]),
            timeout=TOTAL_BUDGET,
        )
    except asyncio.TimeoutError:
        print(f"❌ 전체 감사 예산 {TOTAL_BUDGET}s 초과")
        return

    rows = list(results) + broken

    summary: dict[str, int] = {}
    for row in rows:
        summary[row["status"]] = summary.get(row["status"], 0) + 1

    out_dir = ROOT / "artifacts" / "period_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "period_meta.json"
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"감사 대상: {len(rows)}개 Tier-A 키 ({len(targets)}개 호출, {len(broken)}개 broken 스킵)")
    print()
    print(f"{'상태':<22s} 카운트")
    print("-" * 32)
    for status in sorted(summary.keys()):
        print(f"{status:<22s} {summary[status]}")
    print()
    print(f"상세 리포트: {out_path}")
    print()

    drifting = [
        r for r in rows
        if str(r.get("status", "")).startswith("⚠️")
    ]
    if drifting:
        print("드리프트 항목:")
        for r in drifting:
            extras = []
            if r.get("missing_in_curation"):
                extras.append(f"+추가필요={r['missing_in_curation']}")
            if r.get("extra_in_curation"):
                extras.append(f"-제거후보={r['extra_in_curation']}")
            if r.get("unmapped_kosis_PRD_SE"):
                extras.append(f"unmapped={r['unmapped_kosis_PRD_SE']}")
            extra_str = " ".join(extras) if extras else ""
            print(f"  {r['status']:<22s} {r['key']:<28s} "
                  f"declared={r.get('declared')} live={r.get('live')} {extra_str}")

    failed = [r for r in rows if str(r.get("status", "")).startswith("❌")]
    if failed:
        print()
        print("조회 실패:")
        for r in failed:
            print(f"  {r['status']:<22s} {r['key']:<28s} {r.get('error')}")


if __name__ == "__main__":
    asyncio.run(main())
