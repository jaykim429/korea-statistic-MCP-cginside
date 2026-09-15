"""모집단 한정어가 조용히 버려지는지 센다.

"고령자 고용률" 이 그냥 "고용률" 과 **같은 값**(63.3%)으로 답해졌다. 한정어가 버려지고
전체 값이 고령자 값인 것처럼 나간 것이다. 65세 이상 고용률은 37~38% 수준이니 값이 틀렸다.

quick_stat 에는 이미 가드가 있다 — "질문에 포함된 추가 필터를 안전하게 반영하지 못합니다"
로 거부한다("여성 고용률" 실측). 다만 그 가드가 아는 어휘가 좁아 '고령자' 같은 말이 샌다.

각 한정어에 대해 (한정어 + 측정) 과 (측정) 을 나란히 물어 **값이 같으면 버려진 것**으로 센다.
어휘를 손으로 늘리기 전에 어느 말이 실제로 새는지부터 재려는 것이다.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request

MCP = "http://127.0.0.1:8000/mcp"

MEASURES = ["고용률", "실업률"]
QUALIFIERS = [
    "고령자", "노인", "고령", "청년", "청소년", "여성", "남성",
    "장애인", "외국인", "중장년", "65세 이상", "20대", "1인가구",
]


def call(tool: str, args: dict) -> dict:
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
    return json.loads(content[0]["text"]) if content else {}


def answer(query: str) -> tuple[str, str, str]:
    """(상태, 값, 답변머리)"""
    try:
        out = call("answer_query", {"query": query})
    except Exception as err:
        return ("error", "", str(err)[:80])
    return (
        str(out.get("status") or ""),
        str(out.get("value") or ""),
        str(out.get("answer") or "")[:90],
    )


def main() -> int:
    lines: list[str] = []
    dropped: list[str] = []
    guarded: list[str] = []
    resolved: list[str] = []
    for measure in MEASURES:
        base_status, base_value, base_answer = answer(measure)
        lines.append(f"\n=== 기준: {measure} → {base_status} {base_value}  {base_answer}")
        time.sleep(0.4)
        for qualifier in QUALIFIERS:
            query = f"{qualifier} {measure}"
            status, value, ans = answer(query)
            if status != "executed":
                verdict = "거부(가드 동작)"
                guarded.append(query)
            elif value and value == base_value:
                verdict = "** 한정어 버려짐 **"
                dropped.append(query)
            else:
                verdict = "다른 값(해석됨)"
                resolved.append(query)
            lines.append(f"  {query:<16} {status:<12} {value:<8} {verdict}")
            lines.append(f"      {ans}")
            time.sleep(0.4)

    head = [
        f"한정어 {len(QUALIFIERS)}개 x 측정 {len(MEASURES)}개 = {len(QUALIFIERS) * len(MEASURES)}건",
        f"  한정어 버려짐(오답): {len(dropped)}건 -> {', '.join(dropped) or '없음'}",
        f"  거부(가드 동작):     {len(guarded)}건",
        f"  해석됨:              {len(resolved)}건",
    ]
    out_path = sys.argv[1] if len(sys.argv) > 1 else "dropped_qualifiers.txt"
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(head + lines))
    print(f"dropped={len(dropped)} guarded={len(guarded)} resolved={len(resolved)} -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
