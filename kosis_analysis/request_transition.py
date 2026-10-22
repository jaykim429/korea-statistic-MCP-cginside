"""Bounded correction roles, shared with the chatbot's JSON grammar.

A replacement starts a fresh lookup; it is not a population equivalence,
coordinate proof or permission to bypass residual qualifier validation.
"""
from __future__ import annotations
import json
import re
from pathlib import Path
from .rules import measure_of, regional_kind

_DIR = Path(__file__).resolve().parents[1] / 'rules'
_RULES = json.loads((_DIR / 'request-transition.json').read_text(encoding='utf-8'))
_MEASURES = json.loads((_DIR / 'measure-labels.json').read_text(encoding='utf-8'))
_POPULATIONS = json.loads((_DIR / 'population-labels.json').read_text(encoding='utf-8'))
_METRICS = '|'.join(re.escape(head) for head in _POPULATIONS['tableMetricHeading']['additionalHeads']) + '|' + '|'.join(
    r'\s*'.join(re.escape(char) for char in noun) for noun in sorted(_MEASURES['nouns'], key=len, reverse=True))

def _has_metric(text: str) -> bool:
    return bool(measure_of(text) or re.search(_METRICS, text))

def request_transition(question: str) -> dict | None:
    text = question.strip()
    if not text or len(text) > 240 or re.search(_RULES['protectedPattern'], text):
        return None
    if re.search(_RULES['complementPattern'], text):
        return {'kind': 'complement'} if _has_metric(text) else None
    connectors = list(re.finditer(_RULES['connectorPattern'], text))
    if len(connectors) != 1:
        return None
    connector = connectors[0]
    displaced, target = text[:connector.start()].strip(), text[connector.end():].strip()
    period = re.search(_RULES['leadingPeriodPattern'], displaced)
    prefix = period[1] if period else None
    if period:
        displaced = period[2]
    source_concept = re.sub(r'\s+', '', re.sub(r'(?:은|는|이|가|을|를)$', '', displaced))
    if (not re.fullmatch(_RULES['nominalPattern'], displaced) or _has_metric(displaced)
            or any(noun.endswith('수') and noun[:-1] == source_concept for noun in _MEASURES['countNouns'])
            or re.search(_RULES['sourceCoordinationPattern'], displaced)
            or re.search(_RULES['sourceReferencePattern'], displaced)):
        return None
    subject = re.sub(r'[.!?？]+$', '', re.sub(_RULES['tailPattern'], '', target)).strip()
    if (not re.fullmatch(_RULES['nominalPattern'], subject) or not _has_metric(subject)
            or re.search(_RULES['sourceCoordinationPattern'], subject)
            or (prefix and re.search(_RULES['periodPattern'], subject))
            or len(list(re.finditer(_METRICS, subject))) != 1):
        return None
    regional_swap = regional_kind(displaced) == 'province' and regional_kind(subject.split()[0]) == 'province'
    if connector[0] != '대신' and not re.search(_RULES['targetResetPattern'], subject) and not regional_swap:
        return None
    return {'kind': 'replacement', 'query': f'{prefix + " " if prefix else ""}{target}', 'displaced': displaced}

def execution_question(question: str) -> str:
    transition = request_transition(question)
    return transition['query'] if transition and transition['kind'] == 'replacement' else question
