"""Shared source-heading grammar: no table IDs or product dictionary."""
import json
from pathlib import Path
import pytest
from kosis_analysis.rules import survey_subject

RULES = json.loads((Path(__file__).resolve().parents[1] / 'rules' / 'population-labels.json').read_text(encoding='utf-8'))

@pytest.mark.parametrize('case', RULES['surveySubjectGrammar']['cases'])
def test_shared_official_survey_subject_grammar(case):
    assert survey_subject(case['survey']) == case['subject']
