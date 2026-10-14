from copy import deepcopy
import pytest
from kosis_analysis.measure_roles import apply_classification_measure


def sample():
    # Reduced official metadata and current execution for a broad ITEM family.
    axes = {'ITEM': {'OBJ_NM': '항목', 'items': {'V': {'label': '주요지표'}}},
        'METRIC': {'OBJ_NM': '지표현황별', 'items': {
            'SALES': {'label': '총매출액', 'unit': '억원', 'parent': 'ROOT'},
            'WORKERS': {'label': '종사자수', 'unit': '명', 'parent': 'ROOT'}}}}
    p = {'org_id': '127', 'tbl_id': 'T', 'rows': [{'value': 359158, 'unit': '억원', 'period': '2024',
        'dimensions': {'ITEM': {'code': 'V', 'label': '주요지표'}, 'METRIC': {'code': 'SALES', 'label': '총매출액'}}}]}
    return p, axes


def test_official_metric_axis_proves_only_the_current_named_coordinate():
    p, axes = sample()
    p['rows'].append(deepcopy(p['rows'][0]))
    apply_classification_measure(p, axes)
    assert p['actual_measure'] == '총매출액'
    assert p['actual_measure_evidence'] == 'official_measurement_coordinate'
    assert p['measure_role_evidence']['items'] == [{'code': 'SALES', 'label': '총매출액', 'unit': '억원'}]
    assert p['measure_role_evidence']['item_codes'] == ['V']


@pytest.mark.parametrize('change', ['axis_role', 'code', 'label', 'unit', 'missing_unit', 'item', 'aggregation', 'known_item', 'mixed_metrics', 'multiple_metric_axes'])
def test_currency_names_or_request_hints_never_replace_coordinate_proof(change):
    p, axes = sample()
    p['query'] = '총매출액'
    row = p['rows'][0]
    if change == 'axis_role': axes['METRIC']['OBJ_NM'] = '기업규모별'
    elif change == 'code': row['dimensions']['METRIC']['code'] = 'UNVERIFIED'
    elif change == 'label': row['dimensions']['METRIC']['label'] = '매출액'
    elif change == 'unit': row['unit'] = '원'
    elif change == 'missing_unit': axes['METRIC']['items']['SALES']['unit'] = None
    elif change == 'item': row['dimensions']['ITEM']['code'] = 'UNVERIFIED'
    elif change == 'aggregation': p['aggregation'] = 'sum_by_group'
    elif change == 'known_item':
        row['dimensions']['ITEM']['label'] = axes['ITEM']['items']['V']['label'] = '영업이익'
    elif change == 'mixed_metrics':
        other = deepcopy(row); other['unit'] = '명'
        other['dimensions']['METRIC'] = {'code': 'WORKERS', 'label': '종사자수'}
        p['rows'].append(other)
    elif change == 'multiple_metric_axes':
        axes['OTHER'] = deepcopy(axes['METRIC']); row['dimensions']['OTHER'] = deepcopy(row['dimensions']['METRIC'])
    apply_classification_measure(p, axes)
    assert 'actual_measure' not in p


def test_existing_real_item_or_annotation_keeps_precedence():
    p, axes = sample(); p['actual_measure'] = '공식 정의'
    apply_classification_measure(p, axes)
    assert p['actual_measure'] == '공식 정의'

def test_discovery_knows_metric_axis_but_does_not_infer_measure_from_population_axis():
    from kosis_analysis.metadata import TableMetadataProfile, MetadataCompatibilityScorer
    _, axes = sample()
    rows = [{'OBJ_ID': axis, 'OBJ_NM': data['OBJ_NM'], 'ITM_ID': code,
        'ITM_NM': meta['label'], 'UNIT_NM': meta.get('unit')}
        for axis, data in axes.items() for code, meta in data['items'].items()]
    profile = TableMetadataProfile.from_rows('127', 'T', {}, [{'TBL_NM': '주요지표'}], rows, [])
    result = MetadataCompatibilityScorer([], '총매출액').evaluate(profile)
    assert result.status == 'selected'
    assert result.measure_compatibility['evidence'][0]['source'] == 'ITM_measurement_axis'
    for row in rows:
        if row['OBJ_ID'] == 'METRIC': row['OBJ_NM'] = '기업규모별'
    profile = TableMetadataProfile.from_rows('127', 'T', {}, [{'TBL_NM': '주요지표'}], rows, [])
    assert MetadataCompatibilityScorer([], '총매출액').evaluate(profile).status == 'not_matched_indicator'
