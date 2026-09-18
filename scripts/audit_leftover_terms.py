# -*- coding: utf-8 -*-
"""잔여 한정어 관문을 챗봇 회귀 질문 전체에 오프라인으로 돌린다.

왜 있나.
관문을 `warn` 으로 넣고 라이브 회귀(90분)를 한 번 더 도는 대신, **질문을 러너에서 그대로 꺼내**
순수 함수만 돌려 조율한다. 라이브 회귀 한 번을 아끼는 근거가 이 스크립트이므로, 입력이 무엇이었는지
재현 가능해야 한다 — 질문 JSON 을 손으로 만들어 두지 않고 **여기서 직접 추출한다**.

추출 총량이 예상과 다르면 **그 자리에서 멈춘다.** 러너가 바뀌거나 정규식이 빗나가 질문 일부만 긁힌 채
"조율 끝" 이라고 판단하면 라이브 회귀를 생략한 근거가 무너진다. 세었다고 믿지 말고 못 박는다.

잔여는 네 종류로 갈라 고친다. **종류마다 고치는 자리가 다르다.**
  1 순수 대화 표현      → `_CONVERSATION_FILLERS`
  2 지표 의미인데 안 덮임 → `SYNONYMS`·`description`·`encoded_terms`. **잡말에 넣지 않는다**
  3 실제 한정어         → **그대로 둔다.** 이것이 관문의 존재 이유다
  4 조사·토크나이저 오류 → 어간 규칙 수정. **잡말로 숨기지 않는다**

쓰는 법 (네트워크 없음, 저장소 루트에서):
    python scripts/audit_leftover_terms.py
    python scripts/audit_leftover_terms.py --runners ../scripts --expect 353
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import kosis_curation as kc  # noqa: E402
from kosis_analysis.quick import (  # noqa: E402
    _quick_stat_unsupported_dimensions,
    leftover_qualifier_terms,
)

#: 회귀 러너. 챗봇 저장소(상위 폴더)의 scripts/ 에 있다.
RUNNERS = (
    "eval-stat-50.mjs",
    "eval-chatbot-100.mjs",
    "eval-expert-stats.mjs",
    "eval-conversation-chat.mjs",
    "eval-policy-questions.mjs",
    "eval-assistant-quality.mjs",
    "eval-stat-chat.mjs",
    "eval-general-chat.mjs",
    "eval-documents.mjs",
)

#: 러너의 질문은 두 꼴로 적힌다 — `Q('…')` 헬퍼와 `q: '…'` 필드.
_QUESTION_PATTERNS = (
    re.compile(r"\bQ\(\s*(['\"])(.+?)\1", re.S),
    re.compile(r"\bq:\s*(['\"])(.+?)\1", re.S),
)


def extract_questions(runner_dir: str) -> list[str]:
    """러너 파일에서 질문 문자열을 순서대로 꺼낸다. 중복은 접는다."""
    seen: set[str] = set()
    out: list[str] = []
    for name in RUNNERS:
        path = os.path.join(runner_dir, name)
        if not os.path.exists(path):
            print(f"러너 없음: {path}")
            continue
        source = io.open(path, encoding="utf-8").read()
        for pattern in _QUESTION_PATTERNS:
            for match in pattern.finditer(source):
                question = match.group(2).strip()
                if len(question) < 2 or question in seen:
                    continue
                seen.add(question)
                out.append(question)
    return out


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    default_runners = os.path.normpath(os.path.join(here, "..", "..", "scripts"))

    parser = argparse.ArgumentParser()
    parser.add_argument("--runners", default=default_runners, help="회귀 러너가 있는 폴더")
    # 2026-09-18 실측 353개(중복 접은 뒤). 러너가 바뀌면 여기서 멈춘다.
    parser.add_argument("--expect", type=int, default=353, help="예상 질문 수. 다르면 중단한다")
    parser.add_argument("--show", type=int, default=0, help="잔여 목록을 몇 건까지 보일지(0=전부)")
    args = parser.parse_args()

    questions = extract_questions(args.runners)
    if not questions:
        print(f"질문을 하나도 못 찾았다: {args.runners}")
        return 1
    if args.expect and len(questions) != args.expect:
        print(f"추출 {len(questions)}개 ≠ 예상 {args.expect}개 — 중단한다.")
        print("러너가 바뀌었거나 추출 정규식이 빗나갔다. 이 상태의 조율 결과는 믿을 수 없다.")
        return 1

    router = kc.DEFAULT_ROUTER
    tier_a = 0
    pre_refused = 0
    with_leftover: list[tuple[str, str, list[str]]] = []
    without_leftover = 0

    for question in questions:
        key = router.match_direct_stat_key(question) or ""
        param = kc.TIER_A_STATS.get(key) if key else None
        if param is None:
            continue  # 검색 경로. 관문 A 와 무관하다.
        tier_a += 1
        # 운영과 같은 순서로 본다 — 다섯 감지기가 먼저 거부하면 잔여 관문은 돌지 않는다.
        # 이 줄이 없으면 이미 막히는 질문(연령별·여성·추이·수도권…)까지 세어 조율을 헷갈리게 한다.
        if _quick_stat_unsupported_dimensions(question, param):
            pre_refused += 1
            continue
        terms = leftover_qualifier_terms(question, param, key)
        if terms:
            with_leftover.append((question, key, terms))
        else:
            without_leftover += 1

    print(f"전체 질문        {len(questions)}")
    print(f"Tier A 매칭      {tier_a}")
    print(f"감지기가 먼저 거부 {pre_refused}     <- 잔여 관문까지 오지 않는다")
    print(f"잔여 관문 대상    {tier_a - pre_refused}     <- 관문 A 가 실제로 도는 대상")
    print(f"잔여 있음        {len(with_leftover)}")
    print(f"잔여 없음        {without_leftover}")
    print(f"비-Tier A 건너뜀 {len(questions) - tier_a}     <- 검색 경로. 관문 A 와 무관")
    print()

    if not with_leftover:
        print("잔여 없음 — 조율할 것이 없다.")
        return 0

    tally: dict[str, int] = {}
    for _question, _key, terms in with_leftover:
        for term in terms:
            tally[term] = tally.get(term, 0) + 1

    print("잔여 낱말 빈도 (많은 순) — 네 종류로 갈라 고친다")
    for term, count in sorted(tally.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {count:3}  {term}")
    print()

    rows = with_leftover if not args.show else with_leftover[: args.show]
    print(f"질문별 ({len(rows)}/{len(with_leftover)})")
    for question, key, terms in rows:
        print(f"  {'·'.join(terms):20} | key={key:24} | {question}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
