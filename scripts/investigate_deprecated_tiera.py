"""표가 끝난 Tier A 지표의 교체 후보를 조사한다.

GRDP·전체사업체수·아파트전세가격지수는 검증은 돼 있으나 원 표가 종료됐다(replacement_status=deprecated).
지금은 값 대신 후보를 내므로 옛 수치를 현재값처럼 주지는 않지만, 셋 다 실무 질문 빈도가 높다.

교체는 코드 수정이 아니라 **통계 확정**이다. 표를 잘못 고르면 그럴듯한 틀린 값이 나가므로
(이번 라운드에 실제로 겪었다), 이 스크립트는 **후보와 그 값을 보여 주기만** 하고 고르지 않는다.
사람이 공개 수치와 대조해 판단한다.

사용:
    python scripts/investigate_deprecated_tiera.py          # 기본 3개 지표
    TARGET=GRDP python scripts/investigate_deprecated_tiera.py

챗봇 라이브 검증과 동시에 돌리지 말 것 — KOSIS 레이트 리미터(180콜/60초)를 공유한다.
"""
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, '.')
import kosis_curation as c  # noqa: E402

MCP = os.getenv('STAT_MCP_URL', 'http://127.0.0.1:8000/mcp')
OUT_PATH = os.getenv('OUT', 'artifacts/deprecated_tiera_candidates.md')

# 지표별 검색어와, 값이 맞는지 사람이 대조할 때 쓸 기준
TARGETS = {
    'GRDP': {
        'search': ['지역내총생산', '지역내총생산 시도', 'GRDP 시도별'],
        'expect': '전국 합계는 명목 GDP 규모(수천조 원대)와 비슷해야 한다. 시도축이 18개 안팎이어야 한다.',
    },
    '전체사업체수': {
        'search': ['전국사업체조사 사업체수', '기업통계 사업체수', '경제총조사 사업체수'],
        'expect': '전국 총 사업체 수는 600만~800만 개 규모여야 한다(소상공인 약 610만, 중소기업 약 830만과 정합).',
    },
    '아파트전세가격지수': {
        'search': ['아파트 전세가격지수', '주택가격동향 전세', '전세가격지수 아파트'],
        'expect': '기준시점 100 근처의 지수여야 하고, 월별(M) 주기여야 한다. 기준년이 언제인지 확인할 것.',
    },
}


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
                            'clientInfo': {'name': 'investigate', 'version': '1'}}})
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
        return {'_raw': text[:400]}


def rows_of(payload: dict) -> list:
    for key in ('results', 'tables', 'data', '검색결과', 'candidates'):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def main():
    only = os.getenv('TARGET')
    targets = {k: v for k, v in TARGETS.items() if not only or k == only}
    lines = ['# 표가 끝난 Tier A 지표 — 교체 후보 조사', '',
             '이 문서는 **후보를 보여 주기만** 한다. 어느 표를 쓸지는 값을 공개 수치와 대조해 사람이 정한다.', '']

    for key, spec in targets.items():
        param = c.TIER_A_STATS[key]
        lines += [f'## {key}', '',
                  f'- 현재 표: `{param.org_id}/{param.tbl_id}` — {param.tbl_nm} (종료)',
                  f'- 현재 설정: item=`{param.item_id}` obj_l1=`{param.obj_l1}` 단위=`{param.unit}` 주기=`{param.supported_periods}`',
                  f'- 확인 기준: {spec["expect"]}', '', '### 검색 후보', '']
        seen = set()
        for term in spec['search']:
            try:
                res = call('search_kosis', {'query': term, 'limit': 8})
            except Exception as e:
                lines.append(f'- (검색 실패: {term} — {str(e)[:80]})')
                continue
            for row in rows_of(res):
                if not isinstance(row, dict):
                    continue
                org = str(row.get('org_id') or row.get('기관ID') or '')
                tbl = str(row.get('tbl_id') or row.get('통계표ID') or '')
                name = str(row.get('tbl_nm') or row.get('통계표명') or row.get('name') or '')
                if not tbl or (org, tbl) in seen:
                    continue
                seen.add((org, tbl))
                lines.append(f'- `{org}/{tbl}` — {name}')
            time.sleep(0.4)
        lines.append('')

        lines += ['### 후보 상세 (축·항목·최신시점)', '']
        for org, tbl in list(seen)[:6]:
            try:
                meta = call('explore_table', {'org_id': org, 'tbl_id': tbl})
            except Exception as e:
                lines.append(f'- `{org}/{tbl}` 조회 실패: {str(e)[:80]}')
                continue
            axes = meta.get('분류축') or meta.get('axes') or meta.get('classifications')
            items = meta.get('항목') or meta.get('items')
            period = meta.get('수록기간') or meta.get('period_range') or meta.get('periods')
            lines.append(f'- `{org}/{tbl}`')
            lines.append(f'  - 수록기간: {json.dumps(period, ensure_ascii=False)[:160]}')
            lines.append(f'  - 항목: {json.dumps(items, ensure_ascii=False)[:240]}')
            lines.append(f'  - 분류축: {json.dumps(axes, ensure_ascii=False)[:320]}')
            time.sleep(0.4)
        lines.append('')

    os.makedirs(os.path.dirname(OUT_PATH) or '.', exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'wrote {OUT_PATH} ({len(lines)} lines)')


if __name__ == '__main__':
    main()
