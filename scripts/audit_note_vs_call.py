"""Round-4 Stage 17: detect drift between TIER_A_STATS note text and
the value quick_stat currently returns.

Most TIER_A_STATS entries carry a `note` field that captures the
value/period observed when the mapping was verified. Over time these
get stale — KOSIS publishes a new period, the curation note is not
updated, and the tool keeps surfacing the older timepoint without any
signal that the canonical snapshot has moved.

This script walks every entry, parses the most recent (period, value)
pair out of the note, calls _quick_stat_core for the same key with
region="전국", and classifies the comparison:

  ✅ match              note period and value both match within 0.1 %
  ⚠️ period_advanced    quick_stat returns a newer period — note stale
  ⚠️ period_regressed   quick_stat returns an older period — mapping
                         dropped behind what was verified
  🔴 value_drift        same period, value disagrees beyond 0.1 %
  ❓ note_unparsable    note had no recognizable (period value) pair
  ❌ call_failed        quick_stat returned an error or no value

Run:
    $env:KOSIS_API_KEY = "..."
    $env:PYTHONIOENCODING = "utf-8"
    .\.venv-kosis\Scripts\python.exe scripts\audit_note_vs_call.py
"""
from __future__ import annotations

import asyncio
import io
import json
import re
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
from kosis_mcp_server import _quick_stat_core


# Two recognizable shapes inside a verification note:
#   "(2026.03 100.3799306)"
#   "(2025년 총인구 51,117,378명)"
# We take the last parenthesized chunk so multi-period notes such as
# "(2025 5,619.6 / 2026.03 5,672.6)" surface the newer pair.
_PAREN_RE = re.compile(r"\(([^()]+)\)")
_PERIOD_RE = re.compile(r"\b(19\d{2}|20\d{2})(?:[.\-/]\s*(\d{1,2})|년)?")
_VALUE_RE = re.compile(r"(?<![\d.])(-?\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")


def _parse_note(note: str) -> Optional[dict[str, Any]]:
    """Pick the latest (period, value) snapshot from a note string.

    A single paren can hold several snapshots like
    "(2025 5,619.6 / 2026.03 5,672.6)" so we scan each chunk for
    every (period, value) pair and keep the one with the latest
    period across the entire note."""
    if not note:
        return None
    chunks = _PAREN_RE.findall(note)
    snapshots: list[dict[str, Any]] = []
    for chunk in chunks:
        cursor = 0
        while cursor < len(chunk):
            period = _PERIOD_RE.search(chunk, cursor)
            if not period:
                break
            period_str = period.group(1)
            if period.group(2):
                period_str = f"{period.group(1)}{int(period.group(2)):02d}"
            tail = chunk[period.end():]
            value_match = _VALUE_RE.search(tail)
            if not value_match:
                cursor = period.end()
                continue
            try:
                value = float(value_match.group(0).replace(",", ""))
            except ValueError:
                cursor = period.end()
                continue
            snapshots.append({"period": period_str, "value": value, "chunk": chunk.strip()})
            # advance past this value so the next iteration finds the
            # following period in the same chunk
            cursor = period.end() + value_match.end()
    if not snapshots:
        return None
    snapshots.sort(key=lambda s: (len(s["period"]), s["period"]))
    return snapshots[-1]


def _classify(note_snapshot: Optional[dict[str, Any]], tool_result: dict[str, Any]) -> dict[str, Any]:
    if note_snapshot is None:
        return {"status": "❓ note_unparsable"}
    if tool_result.get("오류") or tool_result.get("결과") == "데이터 없음":
        return {"status": "❌ call_failed", "tool_error": tool_result.get("오류") or tool_result.get("결과")}
    raw_value = tool_result.get("값")
    if raw_value is None:
        return {"status": "❌ call_failed", "tool_error": "값 필드 없음"}
    try:
        tool_value = float(str(raw_value).replace(",", ""))
    except (TypeError, ValueError):
        return {"status": "❌ call_failed", "tool_error": f"값 파싱 실패: {raw_value!r}"}
    tool_period = str(tool_result.get("시점") or "")
    note_period = note_snapshot["period"]
    note_value = note_snapshot["value"]
    same_period = tool_period == note_period or tool_period.startswith(note_period)
    rel_diff = abs(tool_value - note_value) / max(abs(note_value), 1e-9)
    out = {
        "note_period": note_period,
        "note_value": note_value,
        "tool_period": tool_period,
        "tool_value": tool_value,
        "rel_diff": round(rel_diff, 6),
    }
    if same_period and rel_diff < 0.001:
        out["status"] = "✅ match"
    elif same_period:
        out["status"] = "🔴 value_drift"
    elif tool_period > note_period:
        out["status"] = "⚠️ period_advanced"
    else:
        out["status"] = "⚠️ period_regressed"
    return out


async def main() -> None:
    rows: list[dict[str, Any]] = []
    for key, param in TIER_A_STATS.items():
        if param.verification_status == "broken":
            rows.append({"key": key, "status": "🚧 broken_mapping", "note": param.note})
            continue
        snapshot = _parse_note(param.note)
        try:
            tool_result = await _quick_stat_core(key, "전국", "latest")
        except Exception as exc:
            rows.append({
                "key": key, "status": "❌ call_failed",
                "tool_error": repr(exc),
                "note": param.note,
            })
            continue
        verdict = _classify(snapshot, tool_result)
        rows.append({
            "key": key,
            "tbl_id": param.tbl_id,
            "note": param.note,
            **verdict,
        })

    summary: dict[str, int] = {}
    for row in rows:
        summary[row["status"]] = summary.get(row["status"], 0) + 1

    out_dir = ROOT / "artifacts" / "note_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "note_vs_call.json"
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"감사 대상: {len(rows)}개 Tier-A 키")
    print()
    print(f"{'상태':<22s} 카운트")
    print("-" * 32)
    for status in sorted(summary.keys()):
        print(f"{status:<22s} {summary[status]}")
    print()
    print(f"상세 리포트: {out_path}")
    print()
    drifting = [r for r in rows if str(r.get("status", "")).startswith(("🔴", "⚠️"))]
    if drifting:
        print("드리프트 감지 키:")
        for r in drifting:
            print(f"  {r['status']:<22s} {r['key']:<20s} "
                  f"note={r.get('note_period')} {r.get('note_value')} | "
                  f"tool={r.get('tool_period')} {r.get('tool_value')}")


if __name__ == "__main__":
    asyncio.run(main())
