"""Producer/consumer agree on correction roles, not on numeric approval."""
import json
from pathlib import Path
import pytest
from kosis_analysis.request_transition import request_transition, execution_question

ROOT = Path(__file__).resolve().parents[1]
CASES = json.loads((ROOT / 'rules' / 'request-transition.json').read_text(encoding='utf-8'))['cases']

@pytest.mark.parametrize('case', CASES)
def test_shared_role_contract(case):
    transition = request_transition(case['question'])
    assert (transition or {}).get('kind', 'unchanged') == case['kind']
    assert execution_question(case['question']) == case.get('query', case['question'])
