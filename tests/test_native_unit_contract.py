"""Actual KOSIS source shapes, positive/negative coordinates and every native consumer."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import pytest

import kosis_mcp_server as server
from kosis_curation import TIER_A_STATS
from kosis_analysis.native_units import normalize_native_units, native_unit_payload, source_unit
from kosis_analysis.units import apply_official_coordinate_unit, bound_unit_coordinates, official_coordinate_unit
from kosis_analysis.evidence import build_stat_evidence


def raw_row(unit='명 건', item='T1', label='출생사망혼인이혼', code='10', kind='출생아수(명)', value='254341', region='00', region_label='전국'):
    return {'ITM_ID': item, 'ITM_NM': label, 'C1': region, 'C1_NM': region_label, 'C2': code, 'C2_NM': kind,
            'UNIT_NM': unit, 'DT': value, 'PRD_DE': '2025'}


def metadata(row):
    # Official order ITEM/B/A must not be sorted alphabetically into ITEM/A/B.
    return [{'OBJ_ID': axis, 'ITM_ID': row[field], 'ITM_NM': row[label]} for axis, field, label in
            [('ITEM', 'ITM_ID', 'ITM_NM'), ('B', 'C1', 'C1_NM'), ('A', 'C2', 'C2_NM')]]


def normalized(row=None, param=None):
    row = row or raw_row()
    return normalize_native_units([row], param or TIER_A_STATS['출생아수'], 'Y', metadata(row))


@pytest.mark.parametrize('source,curated,expected', [
    ('100만달러', '천달러', '100만달러'), ('명', '천명', '명'), ('건', '천명당 건', '건'),
    ('명 가임여자 1명당 명', '명', '명 가임여자 1명당 명'), ('원/명', '원', '원/명'),
    ('2020＝100', '지수', '지수'), ('금액=100', '지수', '금액=100'),
])
def test_no_whitespace_substring_or_scale_guess(source, curated, expected):
    assert server._effective_kosis_unit(source, curated)[0] == expected


def test_official_category_unit_matches_actual_axis_order_and_keeps_raw_values():
    rows = normalized()
    assert rows[0]['DT'] == '254341'
    assert rows[0]['UNIT_NM'] == '명'
    context = native_unit_payload(rows, '101', TIER_A_STATS['출생아수'].tbl_id)
    assert context['rows'][0]['source_unit'] == '명 건'
    assert context['rows'][0]['dimensions']['C2']['label'] == '출생아수(명)'
    proof = context['unit_coordinate_binding']
    assert proof['basis'] == 'official_coordinate_metadata'
    assert 'measure_name' not in proof and 'reviewed_at' not in proof
    assert bound_unit_coordinates(proof, context['rows'], '101', TIER_A_STATS['출생아수'].tbl_id)


@pytest.mark.parametrize('mutation', ['code', 'label', 'source', 'unit', 'extra', 'binding_table', 'metadata_unit', 'missing_code'])
def test_changed_or_unbound_coordinates_are_not_reapproved(mutation):
    rows = normalized()
    row = rows[0]
    if mutation == 'code': row['C2'] = 'OTHER'
    if mutation == 'label': row['C2_NM'] = '혼인건수(건)'
    if mutation == 'source': row['_SOURCE_UNIT_NM'] = '명 가구'
    if mutation == 'unit': row['UNIT_NM'] = '천명'
    if mutation == 'extra': row['_UNIT_OBSERVATION']['dimensions']['C3'] = {'code': 'F', 'label': '여성'}
    if mutation == 'binding_table': row['_UNIT_BINDING']['table_id'] = 'OTHER'
    if mutation == 'metadata_unit': row['_UNIT_OBSERVATION']['dimensions']['C2']['unit'] = '천명'
    if mutation == 'missing_code': row.pop('C1')
    assert native_unit_payload(rows, '101', TIER_A_STATS['출생아수'].tbl_id)['코드'] == 'UNIT_BASIS_MISMATCH'


def test_unknown_metadata_or_conflicting_unit_does_not_guess_from_curation():
    raw = raw_row()
    meta = metadata(raw)
    meta[-1]['ITM_NM'] = '다른 분류'
    rows = normalize_native_units([raw], TIER_A_STATS['출생아수'], 'Y', meta)
    assert rows[0]['UNIT_NM'] == '명 건'
    assert server._curated_series_basis(TIER_A_STATS['출생아수'], rows)['코드'] == 'UNIT_UNRESOLVED'
    meta = metadata(raw)
    meta[-1]['UNIT_NM'] = '백만원'
    assert normalize_native_units([raw], TIER_A_STATS['출생아수'], 'Y', meta)[0]['UNIT_NM'] == '명 건'


def test_selected_rate_item_owns_denominator_not_sibling_or_table_title():
    rate = {'ITM_ID': 'T10', 'ITM_NM': '인구 천명당 범죄발생건수＜br＞(A÷B×1000)', 'C1': '00', 'C1_NM': '전국', 'UNIT_NM': '건', 'DT': '30.9', 'PRD_DE': '2024'}
    meta = [{'OBJ_ID': 'ITEM', 'ITM_ID': 'T10', 'ITM_NM': rate['ITM_NM'], 'UNIT_NM': '건'},
            {'OBJ_ID': 'ITEM', 'ITM_ID': 'T001', 'ITM_NM': '범죄발생건수＜br＞(A)', 'UNIT_NM': '건'},
            {'OBJ_ID': 'A', 'ITM_ID': '00', 'ITM_NM': '전국'}]
    rows = normalize_native_units([rate], TIER_A_STATS['범죄율'], 'Y', meta)
    assert rows[0]['UNIT_NM'] == '천명당 건'
    assert native_unit_payload(rows, '101', TIER_A_STATS['범죄율'].tbl_id)['rows'][0]['source_unit'] == '건'
    absolute = {**rate, 'ITM_ID': 'T001', 'ITM_NM': '범죄발생건수＜br＞(A)', 'DT': '1583108'}
    assert normalize_native_units([absolute], TIER_A_STATS['범죄율'], 'Y', meta)[0]['UNIT_NM'] == '건'


def test_selected_per_thousand_category_is_not_birth_count():
    row = raw_row(code='11', kind='조출생률(천명당)', value='5')
    rows = normalized(row, TIER_A_STATS['조출생률'])
    assert rows[0]['UNIT_NM'] == '‰'
    assert rows[0]['DT'] == '5'
    assert native_unit_payload(rows, '101', TIER_A_STATS['조출생률'].tbl_id)['unit_coordinate_binding']['unit'] == '‰'


def test_subset_binding_contains_only_current_regions_after_filtering():
    nationwide, seoul = raw_row(), raw_row(region='11', region_label='서울특별시')
    rows = normalize_native_units([nationwide, seoul], TIER_A_STATS['출생아수'], 'Y', metadata(nationwide)+metadata(seoul))
    context = native_unit_payload(rows[1:], '101', TIER_A_STATS['출생아수'].tbl_id)
    assert context['unit_coordinate_binding']['required_coordinates']['C1'] == ['11']
    assert bound_unit_coordinates(context['unit_coordinate_binding'], context['rows'], '101', TIER_A_STATS['출생아수'].tbl_id)


@pytest.mark.parametrize('consumer', ['scalar', 'trend', 'region'])
def test_each_native_consumer_uses_source_scale_and_canonical_evidence(monkeypatch, consumer):
    param = replace(TIER_A_STATS['출생아수'], unit='천명')
    data = [raw_row(unit='명', region='11', region_label='서울특별시'), raw_row(unit='명', region='21', region_label='부산광역시', value='100')]
    async def fetched(*args, **kwargs): return deepcopy(data)
    monkeypatch.setattr(server, '_fetch_series', fetched)
    monkeypatch.setattr(server, '_lookup_quick', lambda query: param)
    monkeypatch.setattr(server, '_resolve_key', lambda key: 'dummy')
    if consumer == 'scalar': result = asyncio.run(server._quick_stat_core('출생아수', '서울', '2025', 'dummy'))
    elif consumer == 'trend': result = asyncio.run(server._quick_trend_core('출생아수', '서울', 1, 'dummy'))
    else: result = asyncio.run(server._quick_region_compare_core('출생아수', '2025', 'desc', 'dummy'))
    assert result['단위'] == '명'
    assert all(row['unit'] == '명' for row in result['rows'])
    envelope = build_stat_evidence(result, tool='quick_stat' if consumer == 'scalar' else 'quick_trend')
    assert envelope['fulfillment']['completeness'] == 'complete'
    assert all(row['unit'] == '명' for row in envelope['evidence']['observations'])
    if consumer == 'region': assert all(row['단위'] == '명' for row in result['표'])


@pytest.mark.parametrize('units,duplicate,should_fail', [(['명', '명'], False, False), (['명', '천명'], False, True), (['명', '명'], True, True), (['%', '%'], False, True)])
def test_aggregation_never_sums_mixed_units_or_duplicate_components(monkeypatch, units, duplicate, should_fail):
    param = replace(TIER_A_STATS['출생아수'], aggregation='sum', obj_l2_list=('10','12'), unit='천명')
    async def call(client, endpoint, params):
        row = raw_row(unit=units[0 if params.get('objL2') == '10' else 1], value='0' if params.get('objL2') == '10' else '3')
        return [row, dict(row)] if duplicate else [row]
    monkeypatch.setattr(server, '_kosis_call', call)
    async def no_region_metadata(*args, **kwargs): return []
    monkeypatch.setattr(server, '_fetch_meta', no_region_metadata)
    if should_fail:
        with pytest.raises(RuntimeError): asyncio.run(server._fetch_series(None, 'dummy', param, '00', latest_n=1))
    else:
        rows = asyncio.run(server._fetch_series(None, 'dummy', param, '00', latest_n=1))
        assert rows[0]['DT'] == '3' and rows[0]['UNIT_NM'] == '명'


@pytest.mark.parametrize('chart,mixed', [('heatmap',False), ('heatmap',True), ('distribution',False), ('distribution',True), ('dashboard',False), ('dashboard',True)])
def test_native_chart_labels_follow_source_units_and_reject_mixed_scales(monkeypatch, chart, mixed):
    import json
    param = replace(TIER_A_STATS['출생아수'], unit='천명',
                    region_scheme={'전국':'00','서울':'11','부산':'26','대구':'27','인천':'28','광주':'29'})
    monkeypatch.setattr(server, '_curation_lookup', lambda query: param)
    async def trend(*args, **kwargs):
        region = kwargs.get('region') or '전국'
        return {'단위': '천명' if mixed and region == '부산' else '명', '지역': region,
                '시계열':[{'시점':'2024','값':'1'},{'시점':'2025','값':'2'}]}
    async def stat(query, region, *args, **kwargs):
        return {'값':'2','단위':'천명' if mixed and region == '부산' else '명','지역':region,'시점':'2025'}
    async def analysis(*args, **kwargs): return {}
    monkeypatch.setattr(server, 'quick_trend', trend)
    monkeypatch.setattr(server, 'quick_stat', stat)
    monkeypatch.setattr(server, 'analyze_trend', analysis)
    if chart == 'heatmap': result = asyncio.run(server.chart_heatmap('출생아수', years=2, api_key='dummy'))
    elif chart == 'distribution': result = asyncio.run(server.chart_distribution('출생아수', api_key='dummy'))
    else: result = asyncio.run(server.chart_dashboard('출생아수', years=2, api_key='dummy'))
    payload = json.loads(result[0].text) if isinstance(result, list) else result
    if mixed:
        assert payload['code'] == 'UNIT_MISMATCH' and 'svg' not in payload
    else:
        assert payload['unit'] == '명'
        assert all(row['unit'] == '명' for row in payload['rows'])
        assert '천명' not in payload['svg']
        if chart == 'dashboard': assert all(row['period'] != 'latest' for row in payload['rows'])


@pytest.mark.parametrize('question,start,end', [('2025년 출생아수 알려줘',None,None), ('출생아수 최근 2년 추이',None,None), ('출생아수 2024년부터 2025년까지', '2024','2025')])
def test_answer_query_adapter_preserves_canonical_rows_and_unit_proof(monkeypatch, question, start, end):
    param = TIER_A_STATS['출생아수']
    context = server._curated_series_basis(param, normalized())
    scalar = {**context, '값':'254341','시점':'2025','지역':'전국','answer':'실제 값',
              'dimensions':{'ITEM':{'code':'T1','label':'출생사망혼인이혼'}}}
    trend = {**context,'지역':'전국','수록주기':'Y','used_period':'2025','시계열':[{'시점':'2025','값':'254341'}]}
    async def stat(*args,**kwargs): return scalar
    async def series(*args,**kwargs): return trend
    monkeypatch.setattr(server,'quick_stat',stat)
    monkeypatch.setattr(server,'_quick_trend_core',series)
    engine=server.NaturalLanguageAnswerEngine('dummy')
    result=asyncio.run(engine._answer_direct(question,'전국',direct_key='출생아수',start_year=start,end_year=end))
    assert result['rows'] == context['rows']
    assert result['unit_coordinate_binding'] == context['unit_coordinate_binding']
    final=server._finalize_answer_query_response(result,query=question,region='전국',verbose=True)
    assert final['stat_evidence']['evidence']['unit_coordinate_binding'] == context['unit_coordinate_binding']
    assert final['stat_evidence']['evidence']['observations'][0]['source_unit'] == '명 건'


@pytest.mark.parametrize('value,expected', [('0','0'), ('254341','254341'), ('-',None), ('*',None), ('NaN',None), ('',None)])
def test_native_unit_projection_preserves_numeric_and_missing_value_states(value,expected):
    param=TIER_A_STATS['출생아수']
    context=native_unit_payload(normalized(raw_row(value=value)),param.org_id,param.tbl_id)
    assert context['rows'][0]['value'] == expected
    context.update(org_id=param.org_id,tbl_id=param.tbl_id,source='KOSIS')
    contract=build_stat_evidence(context,tool='quick_stat')
    assert contract['fulfillment']['completeness'] == ('complete' if expected is not None else 'incomplete')
    if value == '*': assert contract['evidence']['observations'][0]['observation_state'] == 'suppressed'
