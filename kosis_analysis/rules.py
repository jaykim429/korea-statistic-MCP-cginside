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

with (_RULES_DIR / "industry-labels.json").open(encoding="utf-8") as _f:
    INDUSTRY_LABELS = json.load(_f)["categories"]

with (_RULES_DIR / "measure-labels.json").open(encoding="utf-8") as _f:
    _MEASURES = json.load(_f)

with (_RULES_DIR / "population-labels.json").open(encoding="utf-8") as _f:
    _POPULATIONS = json.load(_f)


def survey_population(survey: str | None) -> str | None:
    normalized = re.sub(r'''[\s「」『』"']''', "", str(survey or ""))
    return next((entry["population"] for entry in _POPULATIONS["scopedSurveys"] if entry["name"] == normalized), None)


def survey_allows_question(question: str | None, survey: str | None) -> bool:
    population = survey_population(survey)
    if not population:
        return True
    compact = re.sub(r"\s+", "", str(question or ""))
    return population in compact and f"비{population}" not in compact and not re.search(
        re.escape(population) + r"(?:을|를|이|가)?(?:제외|이외|아닌|말고)", compact)


def measure_of(text: Any) -> str | None:
    """Same noun and word-end contract as the TS adapter; classification is not a measure."""
    tokens = [t for t in re.split(_MEASURES["tokenSeparators"], str(text or "")) if t]
    body, ends, token_ids, rests = "", [], [], []
    for token_id, token in enumerate(tokens):
        for i, char in enumerate(token):
            rest = token[i + 1:]
            body += char
            ends.append(not rest or rest in _MEASURES["particleTail"])
            token_ids.append(token_id)
            rests.append(rest)
    best = None
    for noun in _MEASURES["nouns"]:
        at = body.rfind(noun)
        while at >= 0:
            last = at + len(noun) - 1
            count_end = (noun not in _MEASURES["countNouns"] and noun != "인구") or ends[last] or any(
                rests[last].startswith(suffix) for suffix in _MEASURES["countSuffixes"])
            if count_end and (token_ids[at] == token_ids[last] or ends[last]):
                rank = (last, len(noun))
                if best is None or rank > best[0]:
                    best = (rank, noun)
                break
            at = body.rfind(noun, 0, at + len(noun) - 1)
    return best[1] if best else None


def canonical_measure(measure: str | None) -> str | None:
    return _MEASURES["equivalentMeasures"].get(measure, measure)


def measurement_normalizer(text: str) -> str | None:
    match = re.search(_MEASURES["perUnitNormalizerPattern"], re.sub(r"\s+", "", text))
    return match.group(0) if match else None


def measure_basis_compatible(question: str, actual: str) -> bool:
    # Both directions matter: neither a total nor a per-unit observation can
    # silently substitute for the other, even when their currency is identical.
    return measurement_normalizer(actual) == measurement_normalizer(question)


def measure_relation(asked: str | None, actual: str | None, units: tuple[str, ...] = ()) -> str:
    asked = _MEASURES["equivalentMeasures"].get(asked, asked)
    actual = _MEASURES["equivalentMeasures"].get(actual, actual)
    if not asked or not actual:
        return "unknown"
    if asked == actual:
        return "exact"
    if units and any(asked in rule["measures"] and actual in rule["measures"]
                     and all(unit.strip() in rule["units"] for unit in units)
                     for rule in _MEASURES["unitScopedEquivalences"]):
        return "exact"
    if any(asked in group and actual in group for group in _MEASURES["proxyGroups"]):
        return "proxy"
    return "incompatible"


def canonical_industry_label(label: str) -> str | None:
    """Complete registered category only, including its correct KSIC section prefix."""
    compact = re.sub(r"\s+", "", label)
    compact = re.sub(r"\(\d{2}(?:[~–-]\d{2})?\)$", "", compact)
    for category in INDUSTRY_LABELS:
        for alias in category["aliases"]:
            name = re.sub(r"\s+", "", alias)
            if compact in (name, f'{category["section"]}.{name}'):
                return category["canonical"]
    return None

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


def canonical_population_label(label: Any) -> str:
    """Whole-label aliases shared with TS; no substring or unknown synonym approval."""
    normalized = normalize_item_label(label)
    return next((group[0] for group in _POPULATIONS["equivalentGroups"] if normalized in group), normalized)


def is_total_label(label: Any) -> bool:
    """축 항목이 총계인가 — 넓은 목록으로 본다.

    총계 항목은 좁히는 것이 아니므로 한정어 증거로 세지 않는다.
    """
    return _BROAD.match(normalize_item_label(label)) is not None


def is_total_label_strict(label: Any) -> bool:
    """좁은 목록. 지금 MCP 에는 쓰는 곳이 없고, 정본과 짝을 맞추려 함께 둔다."""
    return _STRICT.match(normalize_item_label(label)) is not None
