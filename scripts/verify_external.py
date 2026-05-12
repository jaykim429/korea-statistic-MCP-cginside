"""External verification: KOSIS web display vs quick_stat result.

Picks representative Tier-A keys, opens each KOSIS statHtml table page
in Playwright, attempts to extract the latest displayed value, then
calls _quick_stat_core for the same key and compares.

Output structure:
    {key}.png + {key}.html screenshot/source so the user can verify
    visually when the scraping heuristic mis-locates the cell.
    A single drift_report.json with classifications:
      ✅ match              — same period, same value
      ⚠️ period_drift       — value differs but periods also differ
      🔴 value_drift        — periods match but values differ
      ℹ️ method_diff        — table IDs differ (cross-source comparison)
      ❓ scrape_failed      — could not locate value cell; check screenshot

Run:
    pip install playwright
    playwright install chromium
    $env:KOSIS_API_KEY = "..."
    .\.venv-kosis\Scripts\python.exe scripts\verify_external.py
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

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("ERROR: Playwright not installed. Run:")
    print("  pip install playwright")
    print("  playwright install chromium")
    sys.exit(1)

from kosis_curation import TIER_A_STATS
from kosis_mcp_server import _quick_stat_core


# Keys representing the audit's headline drift cases. Edit this list to
# expand verification coverage; each entry must exist in TIER_A_STATS.
VERIFY_KEYS: list[str] = [
    "GDP",
    "실업률",
    "전체사업체수",
    "주민등록인구",
    "주택매매가격지수",
]

ARTIFACT_DIR = ROOT / "artifacts" / "external_verify"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


def table_url(param: Any) -> str:
    return f"https://kosis.kr/statHtml/statHtml.do?orgId={param.org_id}&tblId={param.tbl_id}"


_NUMERIC_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")


def _parse_number(text: str) -> Optional[float]:
    if not text:
        return None
    m = _NUMERIC_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


async def _scrape_latest(page) -> dict[str, Any]:
    """Best-effort: locate the rightmost numeric cell of the data grid.

    KOSIS renders pivot tables with various DOM layouts; we probe a few
    common selectors and fall back to scanning every visible <td>."""
    await page.wait_for_load_state("networkidle", timeout=20000)
    candidates = [
        "#mainTable tbody tr",
        ".table-area table tbody tr",
        "#twogrid table tbody tr",
        "table.table-data tbody tr",
        "table tbody tr",
    ]
    last_value: Optional[float] = None
    last_text: Optional[str] = None
    last_header: Optional[str] = None
    for selector in candidates:
        rows = await page.query_selector_all(selector)
        if not rows:
            continue
        for row in rows[-3:]:  # bottom rows often hold the latest period
            cells = await row.query_selector_all("td")
            for cell in reversed(cells):
                text = (await cell.text_content() or "").strip()
                value = _parse_number(text)
                if value is not None:
                    last_value = value
                    last_text = text
                    break
            if last_value is not None:
                break
        if last_value is not None:
            break

    headers = await page.query_selector_all("thead th, table.table-data thead th")
    if headers:
        last = await headers[-1].text_content()
        if last:
            last_header = last.strip()
    return {
        "scraped_value": last_value,
        "scraped_text": last_text,
        "last_header_text": last_header,
    }


def _classify(web: dict[str, Any], tool: dict[str, Any], param) -> str:
    web_val = web.get("scraped_value")
    tool_val_raw = tool.get("값")
    if web_val is None:
        return "❓ scrape_failed"
    tool_val = _parse_number(str(tool_val_raw)) if tool_val_raw is not None else None
    if tool_val is None:
        return "❓ tool_no_value"
    if abs(web_val - tool_val) / max(abs(web_val), 1) < 0.001:
        return "✅ match"
    web_header = (web.get("last_header_text") or "").strip()
    tool_period = str(tool.get("시점") or "")
    if web_header and tool_period and web_header[:4] != tool_period[:4]:
        return "⚠️ period_drift"
    return "🔴 value_drift"


async def main() -> None:
    report: list[dict[str, Any]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ko-KR",
        )
        page = await context.new_page()
        for key in VERIFY_KEYS:
            param = TIER_A_STATS.get(key)
            if param is None:
                report.append({"key": key, "status": "❓ not_in_tier_a"})
                continue
            url = table_url(param)
            entry: dict[str, Any] = {
                "key": key,
                "tbl_id": param.tbl_id,
                "tbl_nm": param.tbl_nm,
                "url": url,
                "note_in_curation": param.note,
            }
            print(f"\n=== {key} ({param.tbl_id}) ===")
            print(f"  url: {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                final_url = page.url
                entry["final_url"] = final_url
                if "sso.kosis.kr" in final_url:
                    entry["status"] = "🚧 sso_required"
                    print(f"  ⚠️ KOSIS SSO 게이트 — 이 표는 로그인 필요")
                    report.append(entry)
                    continue
                web = await _scrape_latest(page)
                entry.update(web)
                screenshot = ARTIFACT_DIR / f"{key}.png"
                html_path = ARTIFACT_DIR / f"{key}.html"
                await page.screenshot(path=str(screenshot), full_page=True)
                html_path.write_text(await page.content(), encoding="utf-8")
                entry["screenshot"] = str(screenshot)
                entry["html_dump"] = str(html_path)
            except Exception as exc:
                entry["status"] = "❓ navigation_error"
                entry["error"] = repr(exc)
                report.append(entry)
                print(f"  ❌ {exc!r}")
                continue

            try:
                tool_result = await _quick_stat_core(key, "전국", "latest")
            except Exception as exc:
                entry["tool_error"] = repr(exc)
                report.append(entry)
                continue
            entry["tool_value"] = tool_result.get("값")
            entry["tool_period"] = tool_result.get("시점")
            entry["tool_unit"] = tool_result.get("단위")
            entry["status"] = _classify(web, tool_result, param)
            print(f"  KOSIS web : {web.get('scraped_text')} (header: {web.get('last_header_text')})")
            print(f"  quick_stat: {entry['tool_period']} {entry['tool_value']} {entry['tool_unit']}")
            print(f"  → {entry['status']}")
            report.append(entry)
        await browser.close()

    report_path = ARTIFACT_DIR / "drift_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n전체 리포트: {report_path}")
    print("스크린샷·HTML 덤프: artifacts/external_verify/")
    print("\n상태 요약:")
    for entry in report:
        print(f"  {entry.get('status', '?'):20s} {entry['key']}")


if __name__ == "__main__":
    asyncio.run(main())
