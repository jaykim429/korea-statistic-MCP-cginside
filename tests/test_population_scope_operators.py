"""The same canonical scope cases as the TS adapter; no query-word deletion."""
import json
from pathlib import Path

import pytest

from kosis_analysis.rules import population_scope_requested, survey_allows_question

RULES = json.loads((Path(__file__).resolve().parents[1] / 'rules' / 'population-labels.json').read_text(encoding='utf-8'))
CASES = RULES['requestScope']['cases']


@pytest.mark.parametrize('case', CASES, ids=[case['question'] for case in CASES])
def test_shared_positive_scope_mentions(case):
    assert population_scope_requested(case['question'], case['phrase']) is case['requested']


@pytest.mark.parametrize('case', [case for case in CASES if case['phrase'] == '소상공인'], ids=lambda case: case['question'])
def test_registered_survey_uses_same_scope_cases(case):
    assert survey_allows_question(case['question'], '소상공인실태조사') is case['requested']


def test_unknown_survey_policy_is_not_changed():
    assert survey_allows_question('사업체 수', '미등록 조사')
