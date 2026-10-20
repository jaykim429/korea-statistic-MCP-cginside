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
from functools import lru_cache
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

_SCOPE_REQUEST = _POPULATIONS['requestScope']
_NON_POSITIVE_SCOPE_TAIL = re.compile(
    r'^[\s」』),]*(?:(?:' + '|'.join(re.escape(particle) for particle in _SCOPE_REQUEST['particles'])
    + r')\s*)?(?:' + '|'.join('(?:' + pattern + ')' for pattern in _SCOPE_REQUEST['nonPositiveSuffixPatterns']) + ')')

with (_RULES_DIR / "geography-labels.json").open(encoding="utf-8") as _f:
    GEOGRAPHY_RULES = json.load(_f)
with (_RULES_DIR / "unit-labels.json").open(encoding="utf-8") as _f:
    UNIT_RULES = json.load(_f)

HEADING_RULES = _MEASURES['sourceCountHeading']


def source_count_heading(title: str, item_label: str) -> dict | None:
    match = re.search(HEADING_RULES['headingPattern'], title)
    if not match or measurement_normalizer(match[1]):
        return None
    family = measure_of(match[1])
    pattern = '|'.join(r'\s*'.join(re.escape(char) for char in noun) for noun in sorted(_MEASURES['nouns'], key=len, reverse=True))
    nouns = {measure_of(value) for value in re.findall(pattern, match[1])}
    if not family or family not in _MEASURES['countNouns'] or len(nouns) != 1 or not re.sub(r'\s+', '', match[1]).endswith(family):
        return None
    members = [re.sub(r'\s+', '', value) for value in re.split(HEADING_RULES['memberSeparatorPattern'], match[2]) if value.strip()]
    member = re.sub(r'\s+', '', item_label)
    if len(members) < 2 or len(set(members)) != len(members) or measure_of(item_label):
        return None
    rate = re.fullmatch(HEADING_RULES['rateSuffixPattern'], member)
    kind = 'rate' if rate and (rate[1] in members or member in members) else 'count' if not rate and member in members else None
    return {'family': family, 'member': item_label, 'kind': kind,
            'measure': f'{family} · {item_label}' if kind == 'count' else item_label} if kind else None


def count_measure_unit_matches(measure: str, unit: str) -> bool:
    unit = re.sub(r'\s+', '', unit)
    if measure in _MEASURES['personCounts']:
        return bool(re.fullmatch(r'(?:천|만|백만|억)?명', unit))
    rule = next((rule for rule in HEADING_RULES['countUnitPatterns'] if re.search(rule['measurePattern'], measure)), None)
    return bool(re.fullmatch(rule['unitPattern'] if rule else UNIT_RULES['countUnitPattern'], unit))


def source_heading_subjects(heading: dict) -> list[str]:
    compose = heading['kind'] == 'count' and any(re.fullmatch(rule['measurePattern'],heading['family']) and heading['member'] in rule['members'] for rule in HEADING_RULES['memberSubjectRules'])
    return [heading['member'], heading['member'] + heading['family'][:-1], heading['member'] + heading['family']] if compose else [heading['member']]


def regional_kind(label: Any) -> str:
    name = re.sub(r'\s+', '', str(label or ''))
    if name == '전국':
        return 'national'
    variants = next((group for group in _POPULATIONS['equivalentGroups'] if name in group), [name])
    if any(re.search(GEOGRAPHY_RULES['provincePattern'], alias) for alias in variants):
        return 'province'
    if re.search(GEOGRAPHY_RULES['localityPattern'], name):
        return 'locality'
    if re.search(GEOGRAPHY_RULES['municipalityPattern'], name):
        return 'city' if name.endswith('시') else 'county' if name.endswith('군') else 'district'
    return 'unknown'


def metadata_regional_scope(name: Any) -> str | None:
    text = re.sub(r'''[\s「」『』"']''', '', str(name or ''))
    groups = [g for g in _POPULATIONS['equivalentGroups'] if any(re.search(GEOGRAPHY_RULES['provincePattern'], a) for a in g)]
    aliases = sorted([a for g in groups for a in g[1:]], key=len, reverse=True)
    prefix = next((a for a in aliases if text.startswith(a)), None)
    if not prefix:
        match = re.match(r'^[가-힣]+?(?:특별자치도|특별자치시|광역시|특별시)', text)
        prefix = match.group(0) if match else None
    return canonical_population_label(prefix) if prefix else None


def survey_population(survey: str | None) -> str | None:
    normalized = re.sub(r'''[\s「」『』"']''', "", str(survey or ""))
    return next((entry["population"] for entry in _POPULATIONS["scopedSurveys"] if entry["name"] == normalized), None)


def survey_subject(survey: str | None) -> str | None:
    """Named official subject only; not reviewed exclusions or population completeness."""
    normalized = re.sub(r'''[\s「」『』"']''', '', str(survey or ''))
    grammar = _POPULATIONS['surveySubjectGrammar']
    match = re.fullmatch(grammar['pattern'], normalized)
    subject = match[1] if match else None
    return subject if subject and not re.search(r'별|및', subject) and subject not in grammar['neutralSubjects'] else None


def population_scope_requested(question: str | None, phrase: str | None) -> bool:
    """Same scope-operator contract as TS; never deletes a requested condition."""
    subject = re.sub(r'\s+', '', str(phrase or ''))
    if not subject:
        return False
    text = str(question or '')
    compact = re.sub(r'\s+', '', text)
    if subject not in compact or any(prefix + subject in compact for prefix in _SCOPE_REQUEST['negativePrefixes']):
        return False
    matches = list(re.finditer(r'\s*'.join(re.escape(char) for char in subject), text))
    return bool(matches) and all(not _NON_POSITIVE_SCOPE_TAIL.match(text[match.end():]) for match in matches)


def survey_allows_question(question: str | None, survey: str | None) -> bool:
    population = survey_population(survey)
    if not population:
        return True
    return population_scope_requested(question, population)


def source_population_frame(org_id: str | None, survey_name: str | None, periods: list[str]) -> dict | None:
    """Reviewed survey universes bind identity and every observed year, not requested filters."""
    if not org_id or not survey_name or not periods or any(not re.fullmatch(r'(?:19|20)\d{2}(?:\d{2})?', str(p)) for p in periods):
        return None
    key = re.sub(r'[\s「」『』"\']', '', survey_name)
    matches = [frame for frame in _POPULATIONS.get('sourcePopulationFrames', [])
               if org_id in frame['orgIds'] and any(re.sub(r'[\s「」『』"\']', '', n) == key for n in frame['surveyNames'])
               and all(frame['periodStart'] <= p[:4] <= frame['periodEnd'] for p in periods)]
    if len(matches) != 1:
        return None
    frame = matches[0]
    return {'id': frame['id'], 'coverage': frame['coverage'], 'survey_name': survey_name,
            'definition': frame['definition'], 'exclusions': frame['exclusions'],
            'references': frame['references'], 'reviewed_at': frame['reviewedAt']}


@lru_cache(maxsize=1)
def _metric_scope_patterns():
    """Same closed source grammar as TS; not measure identity or a product list."""
    grammar = _POPULATIONS['tableMetricHeading']
    heads = [re.escape(head) for head in grammar['additionalHeads']]
    for suffix in grammar['measureSuffixes']:
        stems = '|'.join(re.escape(stem) for stem in sorted(
            [noun[:-len(suffix)] for noun in _MEASURES['nouns'] if noun.endswith(suffix)], key=len, reverse=True))
        heads.append(rf'(?:(?:{stems})[·,])*(?:{stems}){re.escape(suffix)}')
    head = '(?:' + '|'.join(heads) + ')'
    listing = rf'{head}(?:(?:[·,]|및)*{head})*'
    return re.compile(rf'^{listing}$'), re.compile(rf'^([0-9A-Za-z가-힣]+?){listing}$')


def _metric_heading_scope(table_name: str, survey_name: str | None) -> list[dict[str, str]]:
    compact = re.sub(r'\s+', '', re.sub(r'^(?:[0-9A-Za-z가-힣]+별[\s/]+)+', '', table_name.strip()))
    if len(compact) > 512:
        return []
    whole, scoped = _metric_scope_patterns()
    if whole.fullmatch(compact):
        return []
    match = scoped.search(compact)
    prefix = match.group(1) if match else ''
    if (not prefix or prefix in _POPULATIONS['tableMetricHeading']['neutralPrefixes'] or re.search(r'별|및', prefix)
            or re.search(r'(?:여부|유무)$', prefix) or re.search(_MEASURES['perUnitNormalizerPattern'], prefix)
            or any(noun.lower() in prefix.lower() for noun in _MEASURES['nouns'])):
        return []
    survey = re.sub(r'''[\s「」『』"']''', '', str(survey_name or ''))
    if not any(survey == prefix + suffix for suffix in _POPULATIONS['tableMetricHeading']['surveySuffixes']):
        return []
    return [{'id': f'table-metric:{prefix}', 'label': f'{prefix} 범위', 'prefix': prefix}]


def intrinsic_table_scopes(table_name: str | None, survey_name: str | None = None) -> list[dict[str, str]]:
    """Official fixed table universes, not query echoes or selected row filters."""
    compact = re.sub(r"\s+", "", str(table_name or ""))
    registered = [{"id": rule["id"], "label": rule["label"], **({"dimension": rule["dimension"]} if rule.get("dimension") else {})}
            for rule in _POPULATIONS.get("intrinsicTableScopes", [])
            if any(phrase in compact for phrase in rule["tablePhrases"])
            and not any(phrase in compact for phrase in rule["excludes"])]
    inferred = []
    for rule in _POPULATIONS.get("tablePopulationHeads", []):
        match = re.search(r"(?:^|별)([^/·,:()별]*?)" + re.escape(rule["head"]) + r"수(?:$|[(/·,:])", compact)
        prefix = match.group(1) if match else ""
        if prefix and prefix not in rule["neutralPrefixes"]:
            inferred.append({"id": f"table-population:{rule['head']}:{prefix}", "label": f"{prefix} {rule['head']}",
                             "prefix": prefix + rule['head'] if rule.get('requireHeadForSinglePrefix') and len(prefix) == 1 else prefix})
    metric_scopes = _metric_heading_scope(str(table_name or ''), survey_name)
    subject = survey_subject(survey_name)
    subjects = [{'id': f'survey-subject:{subject}', 'label': f'{subject} 조사 범위',
                 'prefix': subject, 'dimension': 'survey-subject'}] if subject and not metric_scopes else []
    return registered + inferred + metric_scopes + subjects


def measure_of(text: Any) -> str | None:
    """Same noun and word-end contract as the TS adapter; classification is not a measure."""
    formatted = re.sub(_MEASURES["sourceLineBreakPattern"], " ", str(text or ""), flags=re.I)
    tokens = [t for t in re.split(_MEASURES["tokenSeparators"], formatted) if t]
    body, ends, token_ids, rests = "", [], [], []
    for token_id, token in enumerate(tokens):
        for i, char in enumerate(token):
            rest = token[i + 1:]
            body += char
            ends.append(not rest or rest in _MEASURES["particleTail"])
            token_ids.append(token_id)
            rests.append(rest)
    matching_body = re.sub(r"[A-Z]", lambda match: match.group().lower(), body)
    best = None
    for noun in _MEASURES["nouns"]:
        matching_noun = re.sub(r"[A-Z]", lambda match: match.group().lower(), noun)
        at = matching_body.rfind(matching_noun)
        while at >= 0:
            last = at + len(noun) - 1
            count_end = (noun not in _MEASURES["countNouns"] and noun != "인구") or ends[last] or any(
                rests[last].startswith(suffix) for suffix in _MEASURES["countSuffixes"])
            if count_end and (token_ids[at] == token_ids[last] or ends[last]):
                rank = (last, len(noun))
                if best is None or rank > best[0]:
                    best = (rank, noun)
                break
            at = matching_body.rfind(matching_noun, 0, at + len(noun) - 1)
    return best[1] if best else None


def canonical_measure(measure: str | None) -> str | None:
    return _MEASURES["equivalentMeasures"].get(measure, measure)


def measure_discovery_aliases(measure: str | None) -> tuple[str, ...]:
    """Lexical discovery only. These are NOT measurement equivalences."""
    return tuple(_MEASURES.get("discoveryAliases", {}).get(canonical_measure(measure), []))


def discovery_measure_of(text: Any) -> str | None:
    """Search vocabulary only: a bare registered alias is not value identity."""
    known = measure_of(text)
    if known:
        return known
    tokens = [token for token in re.split(_MEASURES["tokenSeparators"], str(text or "")) if token]
    return next((measure for token in reversed(tokens)
                 for measure, aliases in _MEASURES.get("discoveryAliases", {}).items()
                 if token in aliases), None)


def source_measure_of(text: Any) -> str | None:
    formatted = re.sub(_MEASURES['sourceLineBreakPattern'], ' ', str(text or ''), flags=re.I)
    opened = re.search(r'[（(]', formatted)
    head = formatted[:opened.start()] if opened else formatted
    primary = measure_of(head)
    explanation = formatted[opened.start():] if opened else ''
    if primary and re.search(r'[）)]\s*$', formatted) and (any(definition['name'] == primary for definition in _MEASURES['namedRateDefinitions'])
            or re.search(_MEASURES['perUnitNormalizerPattern'], re.sub(r'\s+', '', explanation)) or re.search(r'÷|×|분자|분모', explanation)):
        return primary
    return measure_of(text)


def measurement_normalizer(text: str, source_definition=False) -> str | None:
    match = re.search(_MEASURES["perUnitNormalizerPattern"], re.sub(r"\s+", "", text))
    if match:
        return _MEASURES['normalizerAliases'].get(match.group(0), match.group(0))
    measure = source_measure_of(text) if source_definition else measure_of(text)
    return next((definition['normalizer'] for definition in _MEASURES['namedRateDefinitions'] if definition['name'] == measure), None)


def normalize_measurement_basis_phrases(text: str) -> str:
    """Equivalent denominator spellings, without changing any other request noun."""
    return re.sub(_MEASURES['perUnitNormalizerPattern'],
                  lambda match: _MEASURES['normalizerAliases'].get(match.group(0), match.group(0)),
                  re.sub(r'\s+', '', text))


def is_measurement_axis(name: Any) -> bool:
    """An official axis role, never inferred from a user's chosen label/unit."""
    return bool(re.fullmatch(_MEASURES['measurementAxisNamePattern'], re.sub(r'\s+', '', str(name or ''))))


def measure_basis_compatible(question: str, actual: str) -> bool:
    # Both directions matter: neither a total nor a per-unit observation can
    # silently substitute for the other, even when their currency is identical.
    return measurement_normalizer(actual, source_definition=True) == measurement_normalizer(question)


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
