# -*- coding: utf-8 -*-
"""항목 라벨 규칙의 Python adapter — 정본 JSON 을 읽어 쓴다.

정본은 Nuxt 쪽 ``server/utils/stat/rules/`` 다. 여기 ``rules/`` 는 빌드 전에 복사된 **사본**이고,
``scripts/sync_rules.py --check`` 가 어긋남을 잡는다.

이 모듈 밖에서 총계 라벨이나 정규화를 **다시 정의하지 않는다** —
그렇게 해서 세 곳에 내용이 다른 목록이 생겼다(실측 2026-09-18).

**비문자열 falsy 처리는 Nuxt 와 다르다.** 여기는 ``str(label or "")`` 이라 ``0`` 과 ``False``
가 빈 문자열이 되는데, Nuxt 는 ``String(name ?? '')`` 이라 ``"0"`` · ``"false"`` 가 된다.
어느 쪽에 맞춰도 한쪽 동작이 바뀌므로 **이 단계에서는 각자의 지금 동작을 그대로 둔다**
(설계 8.1-3). 계약 fixture 는 문자열 입력만 담는다.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_RULES_DIR = Path(__file__).resolve().parent.parent / "rules"

with (_RULES_DIR / "total-label.json").open(encoding="utf-8") as _f:
    _TOTAL = json.load(_f)

_BROAD_WORDS = tuple(_TOTAL["shared"]) + tuple(_TOTAL["broadOnly"])
_STRICT_WORDS = tuple(_TOTAL["shared"])


def _build(words: tuple[str, ...]) -> "re.Pattern[str]":
    return re.compile(r"^(?:%s)$" % "|".join(re.escape(w) for w in words))


_BROAD = _build(_BROAD_WORDS)
_STRICT = _build(_STRICT_WORDS)


def normalize_item_label(label: Any) -> str:
    """괄호 꼬리를 떼고 공백을 지운다.

    ``중소기업(300인 미만)`` 과 ``중소기업`` 은 같은 집단이다. KOSIS 는 같은 집단을
    표마다 다르게 적는다. Nuxt ``normalizeItemLabel`` 과 같은 규칙이다.
    """
    text = str(label or "").strip()
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
    return re.sub(r"\s+", "", text)


def is_total_label(label: Any) -> bool:
    """축 항목이 총계인가 — 넓은 목록으로 본다.

    총계 항목은 좁히는 것이 아니므로 한정어 증거로 세지 않는다.
    """
    return _BROAD.match(normalize_item_label(label)) is not None


def is_total_label_strict(label: Any) -> bool:
    """좁은 목록. 지금 MCP 에는 쓰는 곳이 없고, 정본과 짝을 맞추려 함께 둔다."""
    return _STRICT.match(normalize_item_label(label)) is not None
