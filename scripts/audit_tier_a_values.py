"""Tier A 지표가 지금도 실제로 값을 내는지 전수 조사.

TIER_A_STATS 는 표·항목·축 코드를 박아 둔 목록이라, KOSIS 쪽에서 표가 개편되면 조용히
값을 못 가져온다. note 에는 "검증 OK (2025년 683,577,063천달러)" 라고 적혀 있는데
지금은 값이 없는 경우가 실제로 있었다(수출액). 어느 지표가 그런지 한 번에 센다.

레이트 리미터(180콜/60초)를 감안해 간격을 둔다. 챗봇 검증과 동시에 돌리지 말 것.
"""
import json
import os
import sys
import time
import urllib.request

OUT_PATH = os.getenv('OUT', 'artifacts/tier_a_values.txt')

sys.path.insert(0, '.')
import kosis_curation as c  # noqa: E402

MCP = os.getenv('STAT_MCP_URL', 'http://127.0.0.1:8000/mcp')


def call(tool: str, args: dict) -> dict:
    def post(payload, sid=None):
        req = urllib.request.Request(
            MCP, data=json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json',
                     'Accept': 'application/json, text/event-stream',
                     **({'mcp-session-id': sid} if sid else {})})
        r = urllib.request.urlopen(req, timeout=180)
        return r, r.read().decode('utf-8', 'replace')

    r, _ = post({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                 'params': {'protocolVersion': '2024-11-05', 'capabilities': {},
                            'clientInfo': {'name': 'audit', 'version': '1'}}})
    sid = r.headers.get('mcp-session-id')
    post({'jsonrpc': '2.0', 'method': 'notifications/initialized', 'params': {}}, sid)
    _, body = post({'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
                    'params': {'name': tool, 'arguments': args}}, sid)
    for line in body.splitlines():
        if line.startswith('data: '):
            body = line[6:]
            break
    out = json.loads(body)
    content = out.get('result', {}).get('content', [])
    text = content[0].get('text', '{}') if content else '{}'
    try:
        return json.loads(text)
    except Exception:
        return {'_raw': text[:300]}


def main():
    keys = list(c.TIER_A_STATS)
    dead, alive = [], []
    for i, key in enumerate(keys, 1):
        param = c.TIER_A_STATS[key]
        try:
            res = call('quick_stat', {'query': key})
        except Exception as e:
            res = {'status': 'error', 'error': str(e)[:120]}
        # quick_stat 은 한글 키를 쓴다(값·단위·시점). 영문 value 로 읽으면 전부 '값 없음'이 된다.
        value = res.get('값') if res.get('값') not in (None, '') else res.get('value')
        ok = value is not None
        (alive if ok else dead).append((key, res.get('status'), param.tbl_id, param.replacement_status, (param.note or '')[:60]))
        if i % 20 == 0:
            print(f'  ... {i}/{len(keys)}', flush=True)
        time.sleep(0.35)

    lines = [f'Tier A {len(keys)}개 · 값 나옴 {len(alive)} · 값 없음 {len(dead)}', '',
             '## 값을 내지 못하는 지표 (표·항목 코드가 현행과 어긋났을 가능성)', '']
    for key, status, tbl, repl, note in dead:
        lines.append(f'  - {key}  status={status} tbl={tbl} repl={repl}  note={note}')
    os.makedirs(os.path.dirname(OUT_PATH) or '.', exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'alive={len(alive)} dead={len(dead)}')


if __name__ == '__main__':
    main()
