"""Producer/consumer agree on correction roles, not on numeric approval."""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock
import pytest
from kosis_analysis.request_transition import request_transition, execution_question

ROOT = Path(__file__).resolve().parents[1]
CASES = json.loads((ROOT / 'rules' / 'request-transition.json').read_text(encoding='utf-8'))['cases']

@pytest.mark.parametrize('case', CASES)
def test_shared_role_contract(case):
    transition = request_transition(case['question'])
    assert (transition or {}).get('kind', 'unchanged') == case['kind']
    assert execution_question(case['question']) == case.get('query', case['question'])

def test_answer_query_resolves_before_primary_or_curated_execution(monkeypatch):
    import kosis_mcp_server as server
    primary = AsyncMock(return_value={'status': 'unsupported'})
    monkeypatch.setattr(server, 'query_primary_series', primary)
    monkeypatch.setattr(server, '_finalize_answer_query_response', lambda result, **kwargs: result)
    asyncio.run(server.answer_query('위생용품 말고 전체 무역수지 알려줘'))
    assert primary.await_args.args[0] == '전체 무역수지 알려줘'

def test_quick_stat_keeps_residual_gate_and_passes_only_explicit_new_scope(monkeypatch):
    import kosis_mcp_server as server
    core = AsyncMock(return_value={'status': 'unsupported'})
    monkeypatch.setattr(server, '_quick_stat_core', core)
    monkeypatch.setattr(server, 'attach_stat_evidence', lambda result, **kwargs: result)
    asyncio.run(server.quick_stat('위생용품 말고 전체 무역수지 알려줘', api_key='dummy'))
    assert core.await_args.args[0] == '전체 무역수지 알려줘'
    exclusion = '전체에서 위생용품을 제외한 무역수지 알려줘'
    asyncio.run(server.quick_stat(exclusion, api_key='dummy'))
    assert core.await_args.args[0] == exclusion

def test_quick_trend_resolves_before_period_and_unsupported_dimension_checks():
    import inspect
    import kosis_mcp_server as server
    source = inspect.getsource(server.quick_trend)
    assert source.index('query = execution_question(query)') < source.index('_resolve_tool_source_system(query, source_system)')
