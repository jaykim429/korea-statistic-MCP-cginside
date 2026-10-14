from copy import deepcopy
from dataclasses import replace

import pytest
from kosis_curation import SeriesContext, TIER_A_STATS
from kosis_analysis.evidence import build_stat_evidence


def payload():
    param = TIER_A_STATS['경제성장률']
    return {'status': 'executed', 'org_id': param.org_id, 'tbl_id': param.tbl_id, 'source': 'KOSIS',
            'rows': [{'value': '2.2', 'period': '2024', 'unit': '%', 'dimensions': {
                'ITEM': {'code': param.item_id, 'label': '주요지표(연간지표)'},
                'C1': {'code': param.obj_l1, 'label': '국내총생산(실질성장률)'}}}]}


@pytest.mark.parametrize('tool', ['answer_query', 'quick_stat', 'quick_trend', 'query_table'])
def test_current_series_context_survives_every_raw_tool(tool):
    p = payload()
    if tool == 'query_table': p['coordinate_axes'] = ['ITEM', 'C1']
    e = build_stat_evidence(p, tool=tool)['evidence']
    assert e['geography']['country'] == 'KR'
    assert e['series_context']['compiler'] == '한국은행'
    assert e['series_context']['binding']['all_observations_matched'] is True


@pytest.mark.parametrize('change', ['table', 'item', 'unit', 'coordinate', 'extra_axis', 'aggregation', 'second_row'])
def test_no_context_is_inferred_from_a_table_or_partial_match(change):
    p = payload()
    if change == 'table': p['tbl_id'] = 'WRONG'
    if change == 'item': p['rows'][0]['dimensions']['ITEM']['code'] = 'WRONG'
    if change == 'unit': p['rows'][0]['unit'] = '십억원'
    if change == 'coordinate': p['rows'][0]['dimensions']['C1']['code'] = 'WRONG'
    if change == 'extra_axis': p['rows'][0]['dimensions']['C2'] = {'code': 'JP', 'label': '일본'}
    if change == 'aggregation': p['aggregation'] = 'sum_by_group'
    if change == 'second_row':
        p['rows'].append(deepcopy(p['rows'][0])); p['rows'][1]['dimensions']['C1']['code'] = 'WRONG'
    e = build_stat_evidence(p, tool='answer_query')['evidence']
    assert not e.get('series_context')


def test_input_rows_analysis_is_not_an_executed_kosis_scope():
    p = payload(); p['source'] = 'input_rows'
    assert not build_stat_evidence(p, tool='analyze_trend')['evidence'].get('series_context')


@pytest.mark.parametrize('change', ['unverified', 'deprecated', 'unknown_context', 'ambiguous_context'])
def test_registry_declarations_need_review_and_unique_bound_meaning(monkeypatch, change):
    import kosis_curation
    param = TIER_A_STATS['경제성장률']
    altered = replace(param, verification_status='unverified') if change == 'unverified' else (
        replace(param, replacement_status='deprecated') if change == 'deprecated' else (
            replace(param, series_context=None) if change == 'unknown_context' else param))
    registry = {'first': altered}
    if change == 'ambiguous_context': registry['second'] = replace(param, series_context=replace(param.series_context, country='JP'))
    monkeypatch.setattr(kosis_curation, 'TIER_A_STATS', registry)
    assert not build_stat_evidence(payload(), tool='answer_query')['evidence'].get('series_context')


@pytest.mark.parametrize('value', [None, 'wrong', 1, []])
def test_malformed_coordinates_do_not_crash_or_prove_context(value):
    p = payload(); p['rows'][0]['dimensions']['C1'] = value
    assert not build_stat_evidence(p, tool='answer_query')['evidence'].get('series_context')


def test_failed_execution_never_acquires_a_current_series_declaration():
    p = payload(); p['status'] = 'failed'
    assert not build_stat_evidence(p, tool='answer_query')['evidence'].get('series_context')


@pytest.mark.parametrize('tool', ['answer_query', 'quick_stat', 'quick_trend', 'query_table'])
def test_domestic_region_proof_is_common_to_all_raw_paths(tool):
    param = TIER_A_STATS['인구']
    p = {'status': 'executed', 'source': 'KOSIS', 'org_id': param.org_id, 'tbl_id': param.tbl_id,
         'coordinate_axes': ['ITEM', 'REGION'], 'rows': [{'value': '123', 'period': '2024', 'unit': '명',
          'dimensions': {'ITEM': {'code': param.item_id, 'label': '총인구수'},
                         ('REGION' if tool == 'query_table' else 'C1'): {'code': '11', 'label': '서울특별시'}}}]}
    assert build_stat_evidence(p, tool=tool)['evidence']['geography']['country'] == 'KR'
    p['rows'][0]['dimensions']['region'] = {'code': 'JP', 'label': '일본'}
    assert not build_stat_evidence(p, tool=tool)['evidence'].get('geography')
