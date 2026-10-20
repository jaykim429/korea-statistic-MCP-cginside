import asyncio
from copy import deepcopy
from dataclasses import replace
import pytest
import kosis_mcp_server as server
from kosis_curation import TIER_A_STATS
from kosis_analysis.native_regions import native_region_scope, resolve_native_region_code, bind_native_regions, native_region_payload, select_native_region_rows
from kosis_analysis.geography import regional_frontier
from kosis_analysis.evidence import build_stat_evidence
from kosis_analysis.answering import AnswerPlan


def sample():
    param = replace(TIER_A_STATS['범죄율'], region_scheme={'전국': '00', '서울': '11', '광주': '24', '전남': '36'})
    nodes = [('00', '전국', None), ('11', '서울특별시', None), ('GROUP', '새 통합특별시', None),
             ('CHILD_A', '광주광역시', 'GROUP'), ('CHILD_B', '전라남도', 'GROUP'), ('LOCAL', '광주시', 'CHILD_B')]
    metadata = [{'OBJ_ID': 'ITEM', 'OBJ_NM': '항목', 'ITM_ID': 'T10', 'ITM_NM': '인구 천명당 범죄발생건수', 'UNIT_NM': '건'}]
    metadata += [{'ORG_ID': param.org_id, 'TBL_ID': param.tbl_id, 'OBJ_ID': 'REGION', 'OBJ_NM': '행정구역별', 'ITM_ID': code,
                  'ITM_NM': label, 'UP_ITM_ID': parent} for code, label, parent in nodes]
    rows = [{'ITM_ID': 'T10', 'ITM_NM': '인구 천명당 범죄발생건수', 'C1': code, 'C1_NM': label, 'DT': str(index),
             'UNIT_NM': '천명당 건', 'PRD_DE': '2024'} for index, (code, label, _) in enumerate(nodes)]
    scope = native_region_scope(param, metadata)
    return param, metadata, bind_native_regions(rows, param, scope), scope


def test_region_identity_resolves_current_metadata_not_old_codes_or_name_substrings():
    param, _, _, scope = sample()
    assert resolve_native_region_code(param, '24', scope) == 'CHILD_A'
    assert resolve_native_region_code(param, '36', scope) == 'CHILD_B'
    assert resolve_native_region_code(param, '11', scope) == '11'
    assert resolve_native_region_code(param, 'ALL', scope) == 'ALL'
    assert resolve_native_region_code(param, 'LOCAL', scope) == 'LOCAL'
    for code in ['UNKNOWN']:
        with pytest.raises(RuntimeError): resolve_native_region_code(param, code, scope)


@pytest.mark.parametrize('code,label', [('OPAQUE_TOTAL','계'), ('NEW_NATION','총계'), ('NO_NUMERIC_PREFIX','합계')])
def test_official_geographic_total_is_axis_bounded_not_a_code_or_population_alias(code,label):
    param=replace(TIER_A_STATS['실업률'],region_scheme={'전국':'OLD','서울':'S','부산':'B'})
    metadata=[{'OBJ_ID':'R','OBJ_NM':'시도별','ITM_ID':c,'ITM_NM':n} for c,n in [(code,label),('S_NEW','서울특별시'),('B_NEW','부산광역시')]]
    scope=native_region_scope(param,metadata)
    assert resolve_native_region_code(param,'OLD',scope)==code


@pytest.mark.parametrize('mutation',['missing_member','duplicate_total','unknown_branch','municipality','nested_total','unverified','old','other_source','another_population'])
def test_generic_total_never_excuses_missing_or_ambiguous_source_scope(mutation):
    param=replace(TIER_A_STATS['실업률'],region_scheme={'전국':'OLD','서울':'S','부산':'B'})
    metadata=[{'OBJ_ID':'R','OBJ_NM':'시도별','ITM_ID':c,'ITM_NM':n} for c,n in [('TOTAL','계'),('S_NEW','서울특별시'),('B_NEW','부산광역시')]]
    if mutation=='missing_member': metadata.pop()
    if mutation=='duplicate_total': metadata.append({**metadata[0],'ITM_ID':'TOTAL2'})
    if mutation=='unknown_branch': metadata.append({**metadata[0],'ITM_ID':'CHINA','ITM_NM':'중국'})
    if mutation=='municipality': metadata[1]['ITM_NM']='서울시 중구'
    if mutation=='nested_total': metadata[0]['UP_ITM_ID']='S_NEW'
    if mutation=='unverified': param=replace(param,verification_status='unverified')
    if mutation=='old': param=replace(param,replacement_status='deprecated')
    if mutation=='another_population': metadata.append({**metadata[0],'ITM_ID':'WOMEN','ITM_NM':'여성'})
    scope=native_region_scope(param,metadata)
    if mutation=='other_source': scope['table_id']='OTHER'
    with pytest.raises(RuntimeError): resolve_native_region_code(param,'OLD',scope)


@pytest.mark.parametrize('mutation', ['axis_role', 'axis_order', 'table', 'org', 'duplicate', 'dangling', 'cycle'])
def test_invalid_metadata_cannot_authorize_a_region_code_or_hierarchy(mutation):
    param, metadata, _, _ = sample()
    if mutation == 'axis_role':
        for row in metadata[1:]: row['OBJ_NM'] = '기업규모별'
    if mutation == 'axis_order':
        metadata.insert(1, {'OBJ_ID': 'OTHER', 'OBJ_NM': '규모별', 'ITM_ID': 'ALL', 'ITM_NM': '전체'})
    if mutation in ['table', 'org']: metadata[1]['TBL_ID' if mutation == 'table' else 'ORG_ID'] = 'OTHER'
    if mutation == 'duplicate': metadata.append({**metadata[2], 'ITM_NM': '부산광역시'})
    if mutation == 'dangling': metadata[-1]['UP_ITM_ID'] = 'MISSING'
    if mutation == 'cycle': metadata[3]['UP_ITM_ID'] = 'CHILD_A'
    assert native_region_scope(param, metadata) is None


def test_two_official_codes_for_the_same_identity_are_not_resolved_arbitrarily():
    param, metadata, _, _ = sample()
    metadata.append({**metadata[4], 'ITM_ID': 'ANOTHER_GWANGJU'})
    scope = native_region_scope(param, metadata)
    with pytest.raises(RuntimeError): resolve_native_region_code(param, '24', scope)


def test_disjoint_frontier_includes_reparented_members_and_excludes_rollup_and_municipality():
    param, _, rows, scope = sample()
    selected = select_native_region_rows(rows, param)
    assert selected['regional_selection']['complete']
    assert selected['regional_selection']['selected_codes'] == ['11', 'CHILD_A', 'CHILD_B']
    assert [node['code'] for node in selected['regional_selection']['excluded_same_level_rollups']] == ['GROUP']
    assert regional_frontier(scope['nodes'], 'province') == selected['regional_selection']['expected_coordinates']
    assert selected['regional_selection']['observation_periods'] == ['2024']
    assert 'classification_effective_date' not in selected['regional_selection']
    context = native_region_payload(selected['rows'], param)
    coordinates = context['regional_coordinates']['axes'][0]['coordinates']
    assert {node['code'] for node in coordinates} == {'11', 'CHILD_A', 'CHILD_B'}
    assert next(node for node in coordinates if node['code'] == 'CHILD_A')['ancestors'][0]['code'] == 'GROUP'


@pytest.mark.parametrize('value', ['0', '-', '*', 'NaN', 'Infinity', ''])
def test_real_zero_is_retained_but_missing_and_nonfinite_members_make_coverage_partial(value):
    param, _, rows, _ = sample()
    rows[3]['DT'] = value
    result = select_native_region_rows(rows, param)
    assert result['regional_selection']['complete'] is (value == '0')
    assert ('CHILD_A' in result['regional_selection']['selected_codes']) is (value == '0')
    if value != '0': assert result['regional_selection']['missing_coordinates'][0]['code'] == 'CHILD_A'


def test_absent_member_does_not_promote_its_parent_to_a_complete_comparison():
    param, _, rows, _ = sample()
    result = select_native_region_rows([row for row in rows if row['C1'] != 'CHILD_A'], param)
    assert not result['regional_selection']['complete']
    assert result['regional_selection']['missing_coordinates'][0]['reason'] == 'no_observation'
    assert 'GROUP' not in result['regional_selection']['selected_codes']


@pytest.mark.parametrize('mutation', ['table', 'code', 'label', 'mixed_context', 'unbound_row'])
def test_current_subset_revalidates_every_row_and_table(mutation):
    param, _, rows, _ = sample()
    rows = deepcopy(rows[2:5])
    if mutation == 'table': rows[0]['_REGION_COORDINATES']['table_id'] = 'OTHER'
    if mutation == 'code': rows[-1]['C1'] = 'UNKNOWN'
    if mutation == 'label': rows[-1]['C1_NM'] = '광주시'
    if mutation == 'mixed_context': rows[1]['_REGION_COORDINATES'] = {**rows[1]['_REGION_COORDINATES'], 'metadata_fetched_at': 'OTHER'}
    if mutation == 'unbound_row': rows[-1].pop('_REGION_COORDINATES')
    assert native_region_payload(rows, param)['코드'] == 'REGION_BASIS_MISMATCH'


def test_duplicate_member_is_not_deduplicated_or_summed():
    param, _, rows, _ = sample()
    assert select_native_region_rows(rows + [deepcopy(rows[3])], param)['코드'] == 'REGION_BASIS_MISMATCH'


def test_failed_metadata_keeps_legacy_codes_but_exposes_missing_and_unresolved_regions():
    param, _, rows, _ = sample()
    for row in rows:
        row.pop('_REGION_SCOPE')
        row.pop('_REGION_COORDINATES')
    result = select_native_region_rows(rows, param)
    proof = result['regional_selection']
    assert proof['basis'] == 'verified_curation_region_scheme' and not proof['complete']
    assert {node['code'] for node in proof['missing_coordinates']} == {'24', '36'}
    assert {node['code'] for node in proof['unresolved_coordinates']} == {'GROUP', 'CHILD_A', 'CHILD_B'}
    assert 'metadata_fetched_at' not in proof


def test_mixed_metadata_scopes_never_fall_back_to_an_older_book():
    param, _, rows, _ = sample()
    rows[-1].pop('_REGION_SCOPE')
    assert select_native_region_rows(rows, param)['코드'] == 'REGION_BASIS_MISMATCH'


def test_fetch_owner_translates_codes_and_reuses_the_same_metadata_for_units(monkeypatch):
    param, metadata, rows, _ = sample()
    calls = []
    async def meta(*args): return metadata
    async def values(client, endpoint, args):
        calls.append(args)
        return [deepcopy(next(row for row in rows if row['C1'] == args['objL1']))]
    monkeypatch.setattr(server, '_fetch_meta', meta)
    monkeypatch.setattr(server, '_kosis_call', values)
    result = asyncio.run(server._fetch_series(None, 'dummy', param, '24', start_year='2024', end_year='2024'))
    assert calls[0]['objL1'] == 'CHILD_A'
    basis = server._curated_series_basis(param, result)
    assert basis['regional_coordinates']['axes'][0]['coordinates'][0]['code'] == 'CHILD_A'


@pytest.mark.parametrize('consumer', ['comparison', 'top_n'])
def test_missing_regions_never_become_global_rank_claims_and_requested_year_is_preserved(monkeypatch, consumer):
    calls = []
    async def compare(query, api_key=None, period='latest'):
        calls.append(period)
        return {'통계명': '범죄율', '단위': '천명당 건', '시점': period, '표': [{'지역': '서울', '값': '1'}],
                'regional_selection': {'complete': False}, 'warnings': ['partial_fulfillment'], '범위_안내': '일부 지역 미확인'}
    monkeypatch.setattr(server, 'quick_region_compare', compare)
    engine = server.NaturalLanguageAnswerEngine('dummy')
    result = asyncio.run(engine._answer_region_compare('2023년 범죄율 시도별', '범죄율') if consumer == 'comparison'
                         else engine._answer_top_n('2023년 범죄율 상위 3개', '범죄율', 3))
    assert calls == ['2023']
    assert result['answer'] == '일부 지역 미확인'
    assert not result.get('상위') and not result.get('하위')
    assert build_stat_evidence(result, tool='answer_query')['fulfillment']['status'] != 'exact'


def test_action_gate_accepts_supported_comparison_not_the_scalar_tools_capabilities(monkeypatch):
    called = []
    async def compare(self, query, direct_key):
        called.append(query)
        return {'상태': 'executed', 'answer': 'actual comparison'}
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_region_compare', compare)
    engine = server.NaturalLanguageAnswerEngine('dummy')
    plan = AnswerPlan('region_compare', '전국', '범죄율')
    result = asyncio.run(engine._execute_plan('2024년 범죄율 시도별로 비교해줘', {}, plan))
    assert result['상태'] == 'executed' and len(called) == 1


@pytest.mark.parametrize('question', ['2024년 여성 범죄율 시도별로 비교해줘', '2024년 청년 범죄율 시도별로 비교해줘',
                                   '2024년 외국인 범죄율 시도별로 비교해줘', '2024년 범죄율 시군구별로 비교해줘'])
def test_action_capabilities_do_not_erase_extra_population_or_another_grain(monkeypatch, question):
    async def forbidden(*args, **kwargs): pytest.fail('unsupported population/grain executed')
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_region_compare', forbidden)
    result = asyncio.run(server.NaturalLanguageAnswerEngine('dummy')._execute_plan(question, {}, AnswerPlan('region_compare', '전국', '범죄율')))
    assert result['코드'] == 'UNVERIFIED_FORMULA'
