"""MCP 가 실제로 답할 수 있는 질문만 남기기 위한 사전 조사.

챗봇을 거치지 않고 MCP answer_query 에 직접 물어, 값이 나오는 질문만 검증 세트로 채택한다.
"MCP 먼저 보면서 가능한 질문 뽑고" 를 추측이 아니라 실측으로 한다.

후보는 kosis_curation.TIER_A_STATS(191개 중 서로 다른 지표 83개)와 TOPICS(16개 분야)에서 뽑았고,
표현은 커레이션 키가 아니라 실무자가 실제로 쓰는 말로 적었다 — 키를 그대로 쓰면
"질문을 알아듣는가"가 아니라 "키가 맞는가"만 보게 된다.

챗봇 라이브 검증과 **동시에 돌리지 말 것** — MCP 는 챗봇과 같은 KOSIS 레이트 리미터(180콜/60초)를 쓴다.
"""
import json
import os
import time
import urllib.request

OUT_PATH = os.getenv('OUT', 'artifacts/probe_answerable_questions.json')

MCP = os.getenv('STAT_MCP_URL', 'http://127.0.0.1:8000/mcp')

# 분야별로 고르게 폈다. 괄호 안은 이 질문이 기대는 커레이션 지표.
CANDIDATES = [
    # ── 인구·가구 (7)
    ('S01', '우리나라 전체 인구가 몇 명이야?'),
    ('S02', '합계출산율 최근 수치 알려줘'),
    ('S03', '작년 출생아 수는?'),
    ('S04', '기대수명 얼마나 돼?'),
    ('S05', '고령인구 비율 알려줘'),
    ('S06', '혼인 건수 최근 추이 보여줘'),
    ('S07', '노령화지수 알려줘'),
    # ── 고용·노동 (7)
    ('S08', '실업률 알려줘'),
    ('S09', '청년 실업률은 얼마야?'),
    ('S10', '고용률 최근 수치'),
    ('S11', '취업자 수 알려줘'),
    ('S12', '상용근로자 월평균임금 얼마야?'),
    ('S13', '자영업자 수 알려줘'),
    ('S14', '고용원 없는 자영업자 수는?'),
    # ── 경제·물가·무역 (7)
    ('S15', 'GDP 얼마야?'),
    ('S16', '경제성장률 알려줘'),
    ('S17', '소비자물가지수 최근 수치'),
    ('S18', '수출액 알려줘'),
    ('S19', '수입액은?'),
    ('S20', '무역수지 알려줘'),
    ('S21', '지역내총생산 시도별로 보여줘'),
    # ── 중소기업·소상공인 (8)
    ('S22', '중소기업 사업체 수 알려줘'),
    ('S23', '중소기업 종사자 수는?'),
    ('S24', '중소기업 매출액 알려줘'),
    ('S25', '소상공인 사업체 수 알려줘'),
    ('S26', '전체 사업체 수 얼마야?'),
    ('S27', '창업기업 수 알려줘'),
    ('S28', '법인 창업기업 수는?'),
    ('S29', '중소기업 경기동행종합지수 알려줘'),
    # ── 업종별 (6)
    ('S30', '제조업 중소기업 사업체 수 알려줘'),
    ('S31', '도소매업 소상공인 매출액은?'),
    ('S32', '숙박음식점업 소상공인 사업체 수'),
    ('S33', '건설업 중소기업 종사자 수 알려줘'),
    ('S34', '정보통신업 중소기업 매출액'),
    ('S35', '보건복지업 소상공인 종사자 수'),
    # ── 주거·부동산 (4)
    ('S36', '주택매매가격지수 알려줘'),
    ('S37', '전세가격지수 최근 추이'),
    ('S38', '주택보급률 얼마야?'),
    ('S39', '가계대출 총잔액 알려줘'),
    # ── 환경·에너지 (5)
    ('S40', '초미세먼지 농도 알려줘'),
    ('S41', '태양광 발전량 얼마야?'),
    ('S42', '풍력 발전량 알려줘'),
    ('S43', '생활폐기물 발생량은?'),
    ('S44', '1인당 온실가스 배출량 알려줘'),
    # ── 복지·교육·보건·안전·교통 (6)
    ('S45', '기초생활보장 수급자 수 알려줘'),
    ('S46', '교원 1인당 학생 수는?'),
    ('S47', '의사 수 알려줘'),
    ('S48', '교통사고 발생 건수 알려줘'),
    ('S49', '화재 발생 건수는?'),
    ('S50', '자동차 등록 대수 알려줘'),
]


def call(tool: str, args: dict) -> dict:
    """MCP Streamable HTTP 한 번 왕복. 세션 협상 포함."""
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
                            'clientInfo': {'name': 'probe', 'version': '1'}}})
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


def main():
    rows = []
    for cid, q in CANDIDATES:
        started = time.time()
        try:
            res = call('answer_query', {'query': q})
        except Exception as e:
            res = {'status': 'error', 'error': str(e)[:160]}
        status = res.get('status', '?')
        value = res.get('value')
        unit = res.get('unit')
        period = res.get('period')
        answerable = status in ('executed', 'partial') and value is not None
        rows.append({'id': cid, 'q': q, 'status': status, 'value': value,
                     'unit': unit, 'period': period, 'answerable': answerable,
                     'sec': round(time.time() - started, 1)})
        mark = 'O' if answerable else 'X'
        print(f'{mark} {cid} status={status} value={value} unit={unit} period={period} ({rows[-1]["sec"]}s)')
        time.sleep(0.4)  # 레이트 리미터 여유

    ok = [r for r in rows if r['answerable']]
    print(f'\n답 가능 {len(ok)}/{len(rows)}')
    print('불가:', ', '.join(r['id'] for r in rows if not r['answerable']) or '없음')
    os.makedirs(os.path.dirname(OUT_PATH) or '.', exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    main()
