import asyncio
from copy import deepcopy
import pytest
import kosis_mcp_server as server
from kosis_analysis.ranking import ranked_comparison
from kosis_analysis.native_regions import select_native_region_rows
from kosis_analysis.evidence import build_stat_evidence
from kosis_analysis.answering import AnswerPlan
from test_native_region_scope import sample


def comparison_fixture():
    param, _, raw, _ = sample()
    selection = select_native_region_rows(raw, param)
    basis = server._curated_series_basis(param, selection['rows'])
    return param, {'통계명': '범죄율', '단위': '천명당 건', '시점': '2024',
        **basis, 'regional_selection': selection['regional_selection'],
        '표': [{'지역': row['dimensions']['C1']['label'], '원값': float(row['value']),
                '값': str(row['value']), '단위': row['unit'], '시점': row['period'], 'dimensions': row['dimensions']}
               for row in basis['rows']]}


@pytest.mark.parametrize('descending,expected', [(True, ['CHILD_B', 'CHILD_A']), (False, ['11', 'CHILD_A'])])
def test_rank_sorts_actual_values_not_transport_order_and_retains_candidate_owner(descending, expected):
    param, comparison = comparison_fixture()
    before = deepcopy(comparison)
    result = ranked_comparison(comparison, param.org_id, param.tbl_id, 2, descending)
    assert result['ranking']['selected_codes'] == expected
    assert len(result['rows']) == 2 and len(result['ranking']['candidate_rows']) == 3
    assert {node['code'] for node in result['regional_coordinates']['axes'][0]['coordinates']} == set(expected)
    assert comparison == before


@pytest.mark.parametrize('mutation', ['missing', 'unresolved', 'coverage_count', 'table', 'unit', 'period',
    'value', 'suppressed', 'duplicate', 'label', 'strata', 'no_metadata', 'wrong_unit_binding'])
def test_rank_never_approves_incomplete_or_mixed_candidates(mutation):
    param, comparison = comparison_fixture()
    coverage = comparison['regional_selection']
    if mutation == 'missing': coverage['complete'] = False
    if mutation == 'unresolved': coverage['unresolved_coordinates'] = [{'code': 'NEW'}]
    if mutation == 'coverage_count': comparison['rows'].pop()
    if mutation == 'table': coverage['table_id'] = 'OTHER'
    if mutation == 'unit': comparison['rows'][0]['unit'] = '%'
    if mutation == 'period': comparison['rows'][0]['period'] = '2023'
    if mutation == 'value': comparison['rows'][0]['value'] = 'NaN'
    if mutation == 'suppressed': comparison['rows'][0]['value'] = '*'
    if mutation == 'duplicate': comparison['rows'][0]['dimensions'] = deepcopy(comparison['rows'][1]['dimensions'])
    if mutation == 'label': comparison['rows'][0]['dimensions']['C1']['label'] = '서울은행'
    if mutation == 'strata': comparison['rows'][0]['dimensions']['ITEM']['code'] = 'OTHER'
    if mutation == 'no_metadata': coverage['basis'] = 'verified_curation_region_scheme'
    if mutation == 'wrong_unit_binding': comparison['unit_coordinate_binding'] = {'contract_version': 'INVALID'}
    assert ranked_comparison(comparison, param.org_id, param.tbl_id, 2) is None


def test_zero_and_cutoff_ties_have_deterministic_explicit_policy():
    param, comparison = comparison_fixture()
    for row in comparison['rows']: row['value'] = 0
    result = ranked_comparison(comparison, param.org_id, param.tbl_id, 2)
    assert result['ranking']['selected_codes'] == ['11', 'CHILD_A']
    assert result['ranking']['cutoff_tied'] is True
    assert ranked_comparison(comparison, param.org_id, param.tbl_id, 10)['ranking']['selected_codes'] == ['11', 'CHILD_A', 'CHILD_B']


@pytest.mark.parametrize('count', [0, -1, True, 1.5, 1001])
def test_invalid_counts_are_not_sliced(count):
    param, comparison = comparison_fixture()
    assert ranked_comparison(comparison, param.org_id, param.tbl_id, count) is None


def test_answer_consumer_and_envelope_preserve_subset_and_complete_rank_proof(monkeypatch):
    param, comparison = comparison_fixture()
    calls = []
    async def compare(*args, **kwargs):
        calls.append(kwargs.get('period'))
        return comparison
    monkeypatch.setattr(server, 'quick_region_compare', compare)
    result = asyncio.run(server.NaturalLanguageAnswerEngine('dummy')._answer_top_n('2024년 범죄율 상위 2개 지역', '범죄율', 2))
    assert calls == ['2024']
    assert len(result['표']) == len(result['rows']) == 2
    evidence = build_stat_evidence(result, tool='answer_query')
    assert evidence['evidence']['ranking']['selected_codes'] == ['CHILD_B', 'CHILD_A']
    assert evidence['fulfillment']['completeness'] == 'complete'
    compact = server._compact_answer_query_response(server.attach_stat_evidence(result, tool='answer_query'), query='', region='전국')
    assert compact['stat_evidence']['evidence']['ranking'] == evidence['evidence']['ranking']


def test_rank_of_a_rate_is_not_authority_to_add_rates_for_a_share(monkeypatch):
    _, comparison = comparison_fixture()
    async def compare(*args, **kwargs): return comparison
    async def forbidden(*args, **kwargs): pytest.fail('rate cannot request an additive denominator')
    monkeypatch.setattr(server, 'quick_region_compare', compare)
    monkeypatch.setattr(server, 'quick_stat', forbidden)
    result = asyncio.run(server.NaturalLanguageAnswerEngine('dummy')._answer_top_n('2024년 범죄율 상위 2개 지역 비중', '범죄율', 2, True))
    assert '계산' not in result and result['warnings'] == ['partial_fulfillment']


@pytest.mark.parametrize('action', ['top_n', 'region_compare'])
def test_structured_single_period_reaches_regional_execution_before_sorting(monkeypatch, action):
    calls = []
    async def ranked(self, query, direct_key, top_n, include_share_ratio=False, period=None):
        calls.append(period)
        return {'상태': 'executed'}
    async def compared(self, query, direct_key, period=None):
        calls.append(period)
        return {'상태': 'executed'}
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_top_n', ranked)
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_region_compare', compared)
    query = '범죄율 상위 3개 지역' if action == 'top_n' else '범죄율 시도별 비교'
    plan = AnswerPlan(action, '전국', '범죄율', {'top_n': 3, 'include_share_ratio': False})
    result = asyncio.run(server.NaturalLanguageAnswerEngine('dummy')._execute_plan(query, {}, plan, start_year='2023', end_year='2023'))
    assert result['상태'] == 'executed' and calls == ['2023']


@pytest.mark.parametrize('query,start,end', [('범죄율 상위 3개 지역', '2023', '2024'),
    ('2024년 범죄율 상위 3개 지역', '2023', '2023'), ('범죄율 상위 3개 지역', '2023', None)])
def test_rank_does_not_silently_substitute_latest_for_conflicting_or_multi_period_requests(monkeypatch, query, start, end):
    async def forbidden(*args, **kwargs): pytest.fail('incompatible period executed')
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_top_n', forbidden)
    result = asyncio.run(server.NaturalLanguageAnswerEngine('dummy')._execute_plan(query, {},
        AnswerPlan('top_n', '전국', '범죄율', {'top_n': 3, 'include_share_ratio': False}), start_year=start, end_year=end))
    assert result['코드'] == 'REGIONAL_PERIOD_SCOPE_UNSUPPORTED'
