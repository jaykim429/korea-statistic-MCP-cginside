"""단위가 비어 있는 항목이 상위에서 단위를 물려받을 수 있는 표가 얼마나 되나.

「에너지 및 원자력산업실태조사 주요지표」 하나에서 나온 결함이 부류인지 세어 본다.
계층 축의 묶음 단계에만 단위를 적는 표가 흔하다면, 단위 없는 숫자가 그만큼 나가고
읽는 쪽이 단위를 지어낸다(실측: 실제 억원인데 "백만원"이라 적어 1만 배 오차).

각 표의 분류축 항목을 훑어, **부모에는 단위가 있는데 자기는 비어 있는** 항목을 센다.
그런 항목이 하나라도 있으면 그 표는 이 수정의 수혜 대상이다.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter

MCP = "http://127.0.0.1:8000/mcp"


def call(tool: str, args: dict) -> str:
    def post(payload: dict, sid: str | None = None):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if sid:
            headers["mcp-session-id"] = sid
        req = urllib.request.Request(MCP, data=json.dumps(payload).encode(), headers=headers)
        res = urllib.request.urlopen(req, timeout=300)
        return res, res.read().decode("utf-8", "replace")

    res, _ = post({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                   "clientInfo": {"name": "audit", "version": "1"}},
    })
    sid = res.headers.get("mcp-session-id")
    post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, sid)
    _, body = post({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": tool, "arguments": args}}, sid)
    for line in body.splitlines():
        if line.startswith("data: "):
            body = line[6:]
            break
    parsed = json.loads(body)
    content = parsed.get("result", {}).get("content", [])
    return content[0]["text"] if content else ""


def inheritable(raw_items) -> list[tuple[str, str, str]]:
    """(항목명, 물려받을 단위, 준 조상) 목록. 자기 단위가 있으면 대상이 아니다.

    explore_table 의 items 는 리스트이고 키는 ITM_ID/UP_ITM_ID/UNIT_NM/ITM_NM 이다.
    """
    rows = raw_items if isinstance(raw_items, list) else list((raw_items or {}).values())
    by_id = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("ITM_ID") or row.get("code") or "")
        if code:
            by_id[code] = row

    def unit_of(row) -> str:
        return str(row.get("UNIT_NM") or row.get("unit") or "")

    def parent_of(row) -> str:
        return str(row.get("UP_ITM_ID") or row.get("parent") or "")

    def label_of(row, fallback: str) -> str:
        return str(row.get("ITM_NM") or row.get("label") or fallback)

    found = []
    for code, row in by_id.items():
        if unit_of(row):
            continue
        seen = set()
        cur = parent_of(row)
        while cur and cur not in seen and len(seen) < 10:
            seen.add(cur)
            parent = by_id.get(cur) or {}
            unit = unit_of(parent)
            if unit:
                found.append((label_of(row, code), unit, label_of(parent, cur)))
                break
            cur = parent_of(parent)
    return found


def main() -> int:
    targets = json.load(open(sys.argv[1], encoding="utf-8"))
    tables_with = 0
    total_items = Counter()
    lines: list[str] = []
    for org_id, tbl_id, name in targets:
        try:
            explored = json.loads(call("explore_table", {"org_id": org_id, "tbl_id": tbl_id}))
        except Exception as err:  # 표가 사라졌거나 메타가 없을 수 있다
            lines.append(f"  (건너뜀) {tbl_id} {name}: {err}")
            continue
        axes = explored.get("분류축") or {}
        hits = []
        for axis_id, axis in axes.items():
            for label, unit, ancestor in inheritable(axis.get("items") or {}):
                hits.append((axis.get("OBJ_NM") or axis_id, label, unit, ancestor))
        if hits:
            tables_with += 1
            total_items[tbl_id] = len(hits)
            lines.append(f"\n{name} ({org_id}/{tbl_id}) — {len(hits)}개")
            for axis_name, label, unit, ancestor in hits[:5]:
                lines.append(f"    [{axis_name}] {label} ← {unit} (from {ancestor})")
    head = [
        f"표 {len(targets)}개 중 단위를 물려받을 항목이 있는 표: {tables_with}개",
        f"물려받을 항목 총합: {sum(total_items.values())}개",
    ]
    report = "\n".join(head + lines)
    # 콘솔이 cp949 라 한글에서 터진다 — 리포트는 UTF-8 파일로 쓴다.
    out_path = sys.argv[2] if len(sys.argv) > 2 else "unit_inheritance_report.txt"
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write(report)
    print(f"tables={len(targets)} affected={tables_with} items={sum(total_items.values())} -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
