"""단위·자릿수가 맞는지 데이터 스스로 말하게 하는 점검.

## 왜 필요한가

Tier A 의 `unit` 은 사람이 손으로 적은 상수다. 표의 실제 단위와 대조하는 장치가 없어서,
틀려도 아무도 모른다. 실측: 수출액이 `683,609 천달러`(약 6.8억 달러)로 나갔다 — 한국의
2024년 수출은 6,836억 달러다. 자릿수는 맞고 **단위 라벨만 1000배 틀렸다**.

값과 단위가 모두 붙어 있으므로 기존 검증(모양 검사)은 전부 통과한다. 라이브 375케이스도
못 잡았다. 그래서 "크기가 맞나"를 사람의 기대치가 아니라 **데이터에서** 끌어내야 한다.
사람이 191개 지표의 정상 범위를 손으로 적으면, 그 목록이 또 검증되지 않은 상수가 된다.

## 세 층

1. **표 메타 대조** — KOSIS 표가 항목 단위(UNIT_NM)를 제공하면 그것이 정답이다.
   커레이션 unit 과 다르면 표를 믿는다.
2. **검증 기록 드리프트** — note 에 남은 검증 당시 값("검증 OK (2025년 683,577,063천달러)")과
   지금 값의 자릿수를 견준다. 표가 개편되거나 항목이 바뀌면 여기서 드러난다.
   이 층이 수출액을 잡는다(683,577,063 -> 683,609, 1000배).
3. **형제 지표 정합** — 같은 표를 쓰는 지표끼리는 크기가 비슷해야 한다. 단위가 같은 지표들의
   자릿수 분포도 함께 보여 사람이 훑을 수 있게 한다.

각 층은 **외부 지식 없이** 저장소와 표가 이미 가진 것만 쓴다.

사용:
    python scripts/audit_unit_consistency.py
    TARGET=수출액 python scripts/audit_unit_consistency.py

챗봇 라이브 검증과 동시에 돌리지 말 것 — KOSIS 레이트 리미터(180콜/60초)를 공유한다.
"""
import json
import math
import os
import re
import sys
import time
import urllib.request
from collections import defaultdict

sys.path.insert(0, '.')
import kosis_curation as c  # noqa: E402

MCP = os.getenv('STAT_MCP_URL', 'http://127.0.0.1:8000/mcp')
OUT_MD = os.getenv('OUT', 'artifacts/unit_consistency.md')
OUT_JSON = os.getenv('OUT_JSON', 'artifacts/unit_consistency.json')

# 단위 접두어가 품은 배수. 라벨이 한 칸 어긋나면 값이 1000배 틀어진다.
SCALE_WORDS = {'천': 1e3, '만': 1e4, '백만': 1e6, '십만': 1e5, '억': 1e8, '조': 1e12}


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
                            'clientInfo': {'name': 'unit-audit', 'version': '1'}}})
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


def to_number(text) -> float | None:
    if text is None:
        return None
    s = str(text).replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return None


# 값 뒤에 붙는 단위 표기. 이게 붙어야 '기록된 값'으로 인정한다.
# '년'은 넣지 않는다 — '2025년'의 2025 를 값으로 읽는다.
UNIT_SUFFIX = r'(?:천달러|천불|백만달러|억달러|달러|백만원|천원|만원|억원|조원|원|천명|만명|명|개소|개|가구|호|건|대|톤|%|퍼센트|퍼밀|‰|지수|명/)'


def recorded_value(note: str) -> float | None:
    """note 에 적힌 검증 당시 값.

    '검증 OK (2025년 683,577,063천달러)' → 683577063

    단위가 붙은 숫자만 값으로 친다. 예전에는 아무 긴 숫자나 집어서 소수점 꼬리와 코드 조각을
    값으로 읽었다 — '0.800'에서 800, '100.3799306'에서 3799306, '16142T2524'에서 16142.
    13건 중 11건이 그렇게 나온 오탐이었다. 노이즈를 내는 점검은 무시당하므로 좁게 잡는다.
    값을 못 읽으면 이 층은 그 지표를 건너뛴다(틀린 경보보다 낫다).
    """
    if not note:
        return None
    # '검증 당시 값'이므로 검증 문맥이 있어야 한다. 없으면 축 개수 같은 숫자를 값으로 읽는다
    # (실측: 자동차등록대수 note 의 "시군구 axis 256개" 를 값 256 으로 읽어 5자릿수 경보를 냈다).
    if not re.search(r'검증|확인|Verified|verified', note):
        return None
    candidates = []
    for m in re.finditer(r'(?<![\d.])(\d[\d,]*(?:\.\d+)?)\s*' + UNIT_SUFFIX, note):
        # 코드 한가운데가 아니어야 한다 (16142T2524 같은 것)
        tail = note[m.end():m.end() + 1]
        if tail.isalnum():
            continue
        value = to_number(m.group(1))
        if value and value > 0:
            candidates.append(value)
    return max(candidates, default=None)


# 같은 뜻의 다른 표기. 이걸 구분하면 경보만 늘고 고칠 것은 없다.
UNIT_SYNONYMS = {'천불': '천달러', '백만달러': '100만달러', '백만불': '100만달러', '지수': ''}


def normalize_unit(unit: str) -> str:
    u = str(unit or '').replace(' ', '').replace('＝', '=')
    return UNIT_SYNONYMS.get(u, u)


def source_unit(meta: dict, item_id: str) -> str | None:
    """표가 스스로 말하는 항목 단위. 없으면 None."""
    axes = meta.get('분류축') or {}
    item_axis = axes.get('ITEM') or {}
    for item in item_axis.get('items') or []:
        if str(item.get('ITM_ID')) == str(item_id):
            unit = item.get('UNIT_NM')
            return str(unit).strip() if unit else None
    return None


def magnitude_gap(a: float, b: float) -> float:
    """자릿수 차이(log10). 1000배면 3."""
    if not a or not b or a <= 0 or b <= 0:
        return 0.0
    return abs(math.log10(a) - math.log10(b))


def main():
    only = os.getenv('TARGET')
    keys = [k for k in c.TIER_A_STATS if not only or k == only]
    findings = []
    rows = []

    for i, key in enumerate(keys, 1):
        param = c.TIER_A_STATS[key]
        row = {'key': key, 'unit': param.unit, 'tbl': f'{param.org_id}/{param.tbl_id}',
               'replacement': param.replacement_status, 'flags': []}

        # 지금 값
        try:
            res = call('quick_stat', {'query': key})
        except Exception as e:
            res = {'error': str(e)[:100]}
        now = to_number(res.get('값') if res.get('값') not in (None, '') else res.get('value'))
        row['now'] = now
        row['now_unit'] = res.get('단위') or res.get('unit')
        row['period'] = res.get('시점') or res.get('period')

        # ── 2층: 검증 기록 드리프트 ──
        rec = recorded_value(param.note or '')
        row['recorded'] = rec
        if rec and now:
            gap = magnitude_gap(rec, now)
            row['drift_orders'] = round(gap, 2)
            if gap >= 2:
                row['flags'].append(f'검증기록과 자릿수 {gap:.1f} 차이 (기록 {rec:,.0f} vs 현재 {now:,.0f})')

        # ── 1층: 데이터가 싣고 온 단위와 대조 ──
        # 예전에는 explore_table 의 항목 메타(UNIT_NM)를 봤는데 대개 null 이라 거의 못 잡았다.
        # 정답은 **조회 결과 행**에 실려 있다 — quick_stat 이 이제 그것을 그대로 돌려준다.
        src = str(row.get('now_unit') or '').strip()
        row['source_unit'] = src or None
        # 지수 기준(2020＝100)과 복합 표기("명 건")는 항목 단위가 아니다 — 서버도 쓰지 않는다
        ambiguous_src = ('=' in src.replace('＝', '=')) or (' ' in src)
        if src and not ambiguous_src and param.unit and normalize_unit(src) != normalize_unit(str(param.unit)):
            row['flags'].append(f'데이터가 싣고 온 단위는 "{src}" 인데 커레이션은 "{param.unit}"')

        rows.append(row)
        if row['flags']:
            findings.append(row)
        if i % 20 == 0:
            print(f'  ... {i}/{len(keys)}', flush=True)
        time.sleep(0.35)

    # ── 3층: 형제 지표 정합 ──
    by_table = defaultdict(list)
    by_unit = defaultdict(list)
    for r in rows:
        if not r.get('now'):
            continue
        by_unit[str(r['unit'])].append(r)
        # 업종별 행렬(제조업_중소기업_매출액 …)은 업종 규모 차이가 원래 크다 — 광업은 제조업의
        # 1/200 이어도 정상이다. 같은 측정의 업종 슬라이스끼리 비교하면 전부 경보가 된다.
        if r['key'].count('_') >= 2:
            continue
        # 단위가 다르면 크기를 견줄 수 없다(같은 표에 건수와 ‰ 가 함께 있다)
        by_table[(r['tbl'], str(r['unit']))].append(r)

    sibling_flags = []
    for (tbl, _unit), group in by_table.items():
        if len(group) < 2:
            continue
        mags = [math.log10(r['now']) for r in group if r['now'] > 0]
        if not mags:
            continue
        mid = sorted(mags)[len(mags) // 2]
        for r in group:
            if r['now'] > 0 and abs(math.log10(r['now']) - mid) >= 2:
                sibling_flags.append(f"같은 표 `{tbl}` 안에서 {r['key']} 만 자릿수가 동떨어짐 ({r['now']:,.0f} {r['unit']})")

    # ── 보고 ──
    lines = ['# 단위·자릿수 점검', '',
             f'Tier A {len(rows)}개 · 의심 {len(findings)}개 · 형제 불일치 {len(sibling_flags)}개', '',
             '값과 단위가 모두 붙어 있으면 기존 검증은 통과한다. 이 점검은 **크기가 맞는지**를 본다.', '',
             '## 의심 항목', '']
    if findings:
        for r in sorted(findings, key=lambda x: -len(x['flags'])):
            lines.append(f"### {r['key']}  (`{r['tbl']}`, {r['replacement']})")
            lines.append(f"- 현재: {r.get('now')} {r.get('now_unit')} ({r.get('period')})")
            if r.get('source_unit') is not None:
                lines.append(f"- 표가 말하는 단위: {r['source_unit']}")
            for f in r['flags']:
                lines.append(f"- ⚠️ {f}")
            lines.append('')
    else:
        lines += ['(없음)', '']

    lines += ['## 형제 지표 불일치', '']
    lines += [f'- ⚠️ {f}' for f in sibling_flags] or ['(없음)']

    lines += ['', '## 단위별 자릿수 분포 (사람이 훑어보는 용도)', '',
              '| 단위 | 지표 수 | 최소 | 중앙 | 최대 |', '|---|---|---|---|---|']
    for unit, group in sorted(by_unit.items(), key=lambda kv: -len(kv[1])):
        vals = sorted(r['now'] for r in group if r['now'] > 0)
        if not vals:
            continue
        lines.append(f'| {unit} | {len(vals)} | {vals[0]:,.0f} | {vals[len(vals) // 2]:,.0f} | {vals[-1]:,.0f} |')

    os.makedirs(os.path.dirname(OUT_MD) or '.', exist_ok=True)
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump({'rows': rows, 'sibling_flags': sibling_flags}, f, ensure_ascii=False, indent=2)
    print(f'의심 {len(findings)} · 형제 불일치 {len(sibling_flags)} → {OUT_MD}')


if __name__ == '__main__':
    main()
