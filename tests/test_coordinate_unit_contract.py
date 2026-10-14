from copy import deepcopy
from dataclasses import replace
from kosis_curation import TIER_A_STATS
from kosis_analysis.units import apply_curated_coordinate_unit
from kosis_analysis.units import is_ambiguous_unit, reviewed_unit_alternative, bound_unit_coordinates
from kosis_analysis.rules import UNIT_RULES
from kosis_analysis.curated_identity import apply_curated_coordinate_measure
from kosis_analysis.evidence import attach_stat_evidence
import pytest

PARAM = TIER_A_STATS['GRDP']
AXES = {
    'ITEM': {'OBJ_NM': '항목', 'items': {'T1': {'label': '명목'}, 'T2': {'label': '실질'}}},
    'AREA': {'OBJ_NM': '행정구역별', 'items': {'11': {'label': '서울특별시'}, '31': {'label': '경기도'}}},
    'ECON': {'OBJ_NM': '경제활동별', 'items': {'Z10': {'label': '지역내총생산(시장가격)'}}},
}

def payload():
    return {'org_id': PARAM.org_id, 'tbl_id': PARAM.tbl_id, 'aggregation': 'none', 'period_type': 'Y',
        'metadata_source': {'org_id': PARAM.org_id, 'tbl_id': PARAM.tbl_id, 'metadata_tool': 'explore_table'},
        'rows': [{'value': value, 'unit': '백만원 %p', 'period': '2024', 'dimensions': {
            'ITEM': {'code': 'T1', 'label': '명목'}, 'AREA': {'code': code, 'label': label},
            'ECON': {'code': 'Z10', 'label': '지역내총생산(시장가격)'}}}
            for code, label, value in [('11', '서울특별시', 575035349), ('31', '경기도', 651417234)]]}

def apply(result, registry=None, metadata=None):
    apply_curated_coordinate_unit(result, ['ITEM', 'AREA', 'ECON'], registry or {'grdp': PARAM}, metadata or AXES)

def test_actual_nominal_coordinates_resolve_the_unit_without_changing_values():
    result = payload()
    apply(result)
    assert [row['unit'] for row in result['rows']] == ['백만원', '백만원']
    assert [row['source_unit'] for row in result['rows']] == ['백만원 %p', '백만원 %p']
    assert [row['value'] for row in result['rows']] == [575035349, 651417234]
    assert result['unit_coordinate_binding']['contract_version'] == 'stat-unit-coordinates/v1'
    assert result['unit_coordinate_binding']['required_coordinates'] == {'ITEM': ['T1'], 'AREA': ['11', '31'], 'ECON': ['Z10']}

def test_table_identity_and_a_unit_token_are_not_enough_without_reviewed_labels():
    result = payload()
    apply(result, {'unchecked': replace(PARAM, unit_coordinate_labels=None)})
    assert 'unit_coordinate_binding' not in result

def test_wrong_one_row_coordinate_does_not_partially_mutate_the_result():
    result = payload(); result['rows'][1]['dimensions']['ECON']['code'] = 'WRONG'
    before = deepcopy(result)
    apply(result)
    assert result == before

@pytest.mark.parametrize('case', UNIT_RULES['cases'])
def test_shared_unit_alternative_grammar(case):
    assert is_ambiguous_unit(case['source']) == case['ambiguous']
    assert reviewed_unit_alternative(case['source'], case['unit']) == case['alternative']

@pytest.mark.parametrize('mutation', ['item', 'label', 'code', 'region_label', 'extra', 'unit', 'metadata_unit', 'metadata_label', 'metadata_missing', 'review', 'deprecated', 'candidate', 'unverified', 'aggregation', 'cadence', 'ratio', 'scale', 'ambiguous_registry'])
def test_unit_resolution_rejects_unverified_or_conflicting_current_coordinates(mutation):
    result = payload(); metadata = deepcopy(AXES); registry = {'grdp': PARAM}
    if mutation == 'item': result['rows'][1]['dimensions']['ITEM'] = {'code': 'T2', 'label': '실질'}
    if mutation == 'label': result['rows'][1]['dimensions']['ECON']['label'] = '다른 지표'
    if mutation == 'code': result['rows'][1]['dimensions']['ECON']['code'] = 'OTHER'
    if mutation == 'region_label': result['rows'][1]['dimensions']['AREA']['label'] = '일본'
    if mutation == 'extra': result['rows'][1]['dimensions']['GENDER'] = {'code': 'F', 'label': '여성'}
    if mutation == 'unit': result['rows'][1]['unit'] = '억원'
    if mutation == 'metadata_unit': metadata['ITEM']['items']['T1']['unit'] = '%p'
    if mutation == 'metadata_label': metadata['ECON']['items']['Z10']['label'] = '다른 측정'
    if mutation == 'metadata_missing': del metadata['ECON']['items']['Z10']
    if mutation == 'review': registry = {'grdp': replace(PARAM, verified_at=None)}
    if mutation in ('deprecated', 'candidate'): registry = {'grdp': replace(PARAM, replacement_status=mutation)}
    if mutation == 'unverified': registry = {'grdp': replace(PARAM, verification_status='unverified')}
    if mutation == 'aggregation': result['aggregation'] = 'sum_by_group'
    if mutation == 'cadence': result['period_type'] = 'M'
    if mutation == 'ratio': result['rows'][1]['unit'] = '백만원/%p'
    if mutation == 'scale': registry = {'grdp': replace(PARAM, unit='원')}
    if mutation == 'ambiguous_registry': registry['other'] = replace(PARAM, description='다른 측정')
    before = deepcopy(result)
    apply(result, registry, metadata)
    assert result == before

def test_the_same_reviewed_contract_works_under_different_table_ids_not_a_whitelist():
    other = replace(PARAM, org_id='999', tbl_id='OTHER')
    result = payload(); result.update(org_id='999', tbl_id='OTHER')
    result['metadata_source'].update(org_id='999', tbl_id='OTHER')
    apply(result, {'other': other})
    assert all(row['unit'] == '백만원' for row in result['rows'])

def test_envelope_retains_original_unit_and_coordinate_bound_measure_context():
    result = payload(); result.update(status='executed', source='KOSIS')
    apply(result)
    apply_curated_coordinate_measure(result, ['ITEM', 'AREA', 'ECON'], {'grdp': PARAM}, AXES)
    envelope = attach_stat_evidence(result, tool='query_table')['stat_evidence']
    assert envelope['fulfillment']['completeness'] == 'complete'
    assert envelope['evidence']['actual_measure'] == PARAM.measurement_name
    assert envelope['evidence']['observations'][0]['source_unit'] == '백만원 %p'
    assert bound_unit_coordinates(envelope['evidence']['unit_coordinate_binding'], envelope['evidence']['observations'], PARAM.org_id, PARAM.tbl_id)
