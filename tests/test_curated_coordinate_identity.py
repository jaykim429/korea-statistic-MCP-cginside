from copy import deepcopy
from dataclasses import replace
from kosis_curation import TIER_A_STATS
from kosis_analysis.curated_identity import apply_curated_coordinate_measure

PARAM = TIER_A_STATS['혼인건수']

def payload():
    return {'org_id': PARAM.org_id, 'tbl_id': PARAM.tbl_id, 'aggregation': 'none', 'rows': [
        {'value': 54150, 'period': '202401', 'unit': PARAM.unit, 'dimensions': {
            'ITEM': {'code': PARAM.item_id, 'label': '출생사망혼인이혼'},
            'REGION': {'code': '00', 'label': '전국'}, 'DYNAMIC': {'code': '20', 'label': '혼인건수(건)'}}}]}

def apply(result, registry=None):
    apply_curated_coordinate_measure(result, ['ITEM', 'REGION', 'DYNAMIC'], registry or TIER_A_STATS)

def test_fresh_independent_axis_coordinates_prove_broad_item_metric():
    result = payload()
    apply(result)
    assert result['measure_definition'] == PARAM.description
    assert result['actual_measure_evidence'] == 'verified_curation_description'
    assert result['curation_measure_binding']['required_coordinates']['DYNAMIC'] == ['20']
    assert result['curation_measure_binding']['all_observations_matched'] is True

def test_table_or_request_label_alone_never_proves_metric():
    for field, value in [('org_id', 'WRONG'), ('tbl_id', 'WRONG'), ('aggregation', 'sum_by_group')]:
        result = payload(); result[field] = value; apply(result)
        assert 'measure_definition' not in result
    result = payload(); del result['rows'][0]['dimensions']['DYNAMIC']; apply(result)
    assert 'measure_definition' not in result
    result = payload(); result['rows'][0]['dimensions']['DYNAMIC']['code'] = 'WRONG'; apply(result)
    assert 'measure_definition' not in result

def test_all_rows_item_codes_and_units_must_match():
    for change in ('item', 'unit', 'scope'):
        result = payload(); second = deepcopy(result['rows'][0]); result['rows'].append(second)
        if change == 'item': second['dimensions']['ITEM']['code'] = 'WRONG'
        elif change == 'unit': second['unit'] = '%'
        else: second['dimensions']['DYNAMIC']['code'] = 'WRONG'
        apply(result)
        assert 'measure_definition' not in result

def test_unverified_or_ambiguous_curation_is_not_identity():
    result = payload(); apply(result, {'candidate': replace(PARAM, verification_status='unverified')})
    assert 'measure_definition' not in result
    result = payload(); apply(result, {'first': PARAM, 'other': replace(PARAM, description='출생아 수')})
    assert 'measure_definition' not in result

def test_actual_item_and_official_definition_keep_precedence():
    result = payload(); result['rows'][0]['dimensions']['ITEM']['label'] = '출생아수'; apply(result)
    assert 'measure_definition' not in result
    result = payload(); result['actual_measure'] = '공식 주석 정의'; apply(result)
    assert result['actual_measure'] == '공식 주석 정의'

def test_metric_name_is_separate_from_formula_definition_on_generic_items():
    teacher = replace(TIER_A_STATS['교원1인당학생수'], measure_name='교원 1인당 학생수')
    result = {'org_id': teacher.org_id, 'tbl_id': teacher.tbl_id, 'rows': [
        {'value': 13.57, 'unit': '명', 'dimensions': {'ITEM': {'code': 'T10', 'label': '교육지표'},
            'SGG': {'code': '00', 'label': '전국'}}}]}
    apply_curated_coordinate_measure(result, ['ITEM', 'SGG'], {'teacher': teacher})
    assert result['actual_measure'] == teacher.measure_name
    assert result['measure_definition'] == teacher.description
    assert result['curation_measure_binding']['measure_name'] == teacher.measure_name

def test_formatted_actual_item_still_takes_precedence_over_curation():
    result = payload()
    result['rows'][0]['dimensions']['ITEM']['label'] = '출생아수＜br＞(A)'
    apply(result)
    assert 'actual_measure' not in result

def test_broad_count_unit_requires_selected_official_category_and_verified_codes():
    metadata = {'DYNAMIC': {'items': {'20': {'label': '혼인건수(건)'}}}}
    result = payload(); result['rows'][0]['unit'] = '명 건'
    apply_curated_coordinate_measure(result, ['ITEM', 'REGION', 'DYNAMIC'], TIER_A_STATS, metadata)
    assert result['measure_definition'] == PARAM.description
    assert result['rows'][0]['unit'] == '건'
    assert result['rows'][0]['source_unit'] == '명 건'
    assert result['curation_measure_binding']['unit_resolutions'][0]['basis'] == 'verified_coordinates_and_official_category_unit'
    for unit, meta in [('명 건', None), ('명/건', metadata), ('% 명 건', metadata), ('명 건', {'DYNAMIC': {'items': {'20': {'label': '혼인건수(명)'}}}})]:
        result = payload(); result['rows'][0]['unit'] = unit
        apply_curated_coordinate_measure(result, ['ITEM', 'REGION', 'DYNAMIC'], TIER_A_STATS, meta)
        assert 'measure_definition' not in result
        assert result['rows'][0]['unit'] == unit
