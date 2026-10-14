from copy import deepcopy
from dataclasses import replace
import pytest
from kosis_analysis.rules import GEOGRAPHY_RULES, regional_kind
from kosis_analysis.geography import project_metadata_coordinates, classify_nodes, native_regional_coordinates
from kosis_curation import TIER_A_STATS


@pytest.mark.parametrize('case', GEOGRAPHY_RULES['cases'], ids=[case['label'] for case in GEOGRAPHY_RULES['cases']])
def test_shared_official_geographic_kind(case):
    assert regional_kind(case['label']) == case['kind']


def sample():
    axes = {'R': {'OBJ_NM': '행정구역별', 'items': {
        'PROVINCE': {'label': '경기도', 'parent': None},
        'CITY': {'label': '수원시', 'parent': 'PROVINCE'},
        'WARD': {'label': '수원시 장안구', 'parent': 'CITY'}}}}
    rows = [{'unit': '%', 'value': 20, 'dimensions': {'R': {'code': 'WARD', 'label': '수원시 장안구'}}}]
    return axes, rows


def test_actual_metric_rows_bind_full_parent_path_and_submunicipal_kind():
    axes, rows = sample()
    result = project_metadata_coordinates('101', 'T', axes, rows)
    selected = result['axes'][0]['coordinates'][0]
    assert selected['kind'] == 'district' and selected['level'] == 'submunicipality'
    assert [node['code'] for node in selected['ancestors']] == ['CITY', 'PROVINCE']
    assert result['all_observations_matched'] is True


@pytest.mark.parametrize('change', ['code', 'label', 'missing_axis', 'wrong_axis_role', 'another_row', 'cycle'])
def test_request_or_dimension_name_never_replaces_actual_row_metadata(change):
    axes, rows = sample()
    if change == 'code': rows[0]['dimensions']['R']['code'] = 'UNKNOWN'
    if change == 'label': rows[0]['dimensions']['R']['label'] = '경기도'
    if change == 'missing_axis': rows[0]['dimensions'] = {}
    if change == 'wrong_axis_role': axes['R']['OBJ_NM'] = '기업규모별'
    if change == 'another_row': rows.append({'dimensions': {'R': {'code': 'UNKNOWN', 'label': '경기도'}}})
    if change == 'cycle': axes['R']['items']['PROVINCE']['parent'] = 'WARD'
    assert project_metadata_coordinates('101', 'T', axes, rows) is None


def test_native_reviewed_book_uses_current_returned_code_and_label_not_caller_region():
    param = TIER_A_STATS['인구']
    table = {'org_id': param.org_id, 'table_id': param.tbl_id}
    rows = [{'dimensions': {'C1': {'code': '11', 'label': '서울특별시'}}}]
    result = native_regional_coordinates({'region': '부산'}, rows, table, 'quick_stat', {'population': param})
    assert result['basis'] == 'verified_curation_region_scheme'
    assert result['axes'][0]['coordinates'][0]['label'] == '서울특별시'
    for changed in [replace(param, verification_status='unverified'), replace(param, replacement_status='deprecated')]:
        assert native_regional_coordinates({}, rows, table, 'quick_stat', {'population': changed}) is None
    for code, label in [('UNKNOWN', '서울특별시'), ('11', '부산광역시'), ('11', '종로구')]:
        wrong = deepcopy(rows); wrong[0]['dimensions']['C1'] = {'code': code, 'label': label}
        assert native_regional_coordinates({}, wrong, table, 'quick_stat', {'population': param}) is None
    assert native_regional_coordinates({'aggregation': 'sum_by_group'}, rows, table, 'quick_stat', {'population': param}) is None
    assert native_regional_coordinates({}, rows, table, 'analyze_trend', {'population': param}) is None
