"""Official native region coordinates, distinct from units and legacy search aliases.

Current metadata locates the real region axis; observed periods do not become
legal classification effective dates. Every public subset is rebound to its rows.
"""
from copy import deepcopy
from datetime import datetime, timezone
import math
import re

from .evidence import kosis_row_dimensions
from .geography import classify_nodes, project_metadata_coordinates, regional_frontier
from .metadata import _build_axis_codebook, _normalize_stat_value
from .rules import GEOGRAPHY_RULES, canonical_population_label, regional_kind


def native_region_scope(param, metadata):
    if not metadata or not param.region_scheme:
        return None
    axes, order = _build_axis_codebook(metadata)
    classifications = [axis for axis in order if axis != 'ITEM']
    index = ('obj_l1', 'obj_l2', 'obj_l3').index(param.region_obj)
    if index >= len(classifications):
        return None
    axis_id = classifications[index]
    axis = axes[axis_id]
    if not re.search(GEOGRAPHY_RULES['axisPattern'], re.sub(r'\s+', '', str(axis.get('OBJ_NM') or ''))):
        return None
    relevant = [row for row in metadata if str(row.get('OBJ_ID') or '') == axis_id]
    if any(row.get('ORG_ID') and str(row['ORG_ID']) != param.org_id
           or row.get('TBL_ID') and str(row['TBL_ID']) != param.tbl_id for row in relevant):
        return None
    seen = {}
    for row in relevant:
        code, identity = str(row.get('ITM_ID') or ''), (row.get('ITM_NM'), row.get('UP_ITM_ID'))
        if code in seen and seen[code] != identity:
            return None
        seen[code] = identity
    nodes = [{'code': code, 'label': str(item.get('label') or ''), 'parent': item.get('parent')}
             for code, item in axis['items'].items()]
    classified = classify_nodes(nodes)
    if not classified or any(node.get('parent') and node['parent'] not in classified for node in nodes):
        return None
    return {'org_id': param.org_id, 'table_id': param.tbl_id, 'axis_id': 'C'+str(index+1),
            'official_axis_id': axis_id, 'axis_name': axis['OBJ_NM'], 'nodes': nodes,
            'metadata_fetched_at': datetime.now(timezone.utc).isoformat()}


def resolve_native_region_code(param, code, scope):
    if not scope or not code or code == 'ALL':
        return code
    book = {str(value): name for name, value in param.region_scheme.items()}
    expected = book.get(str(code))
    # Dynamically resolved codes are already official; do not reinterpret them
    # through a different table's legacy codebook.
    if expected is None:
        if any(node['code'] == str(code) for node in scope['nodes']):
            return code
        raise RuntimeError('[REGION_COORDINATE_UNRESOLVED] 요청 지역 코드가 현재 공식 분류에 없습니다.')
    matches = [node['code'] for node in scope['nodes']
               if canonical_population_label(node['label']) == canonical_population_label(expected)]
    if len(matches) != 1:
        raise RuntimeError('[REGION_COORDINATE_UNRESOLVED] 요청 지역을 현재 공식 코드·라벨에 유일하게 연결할 수 없습니다.')
    return matches[0]


def bind_native_regions(rows, param, scope):
    if not scope or not rows:
        return rows
    axis, by_code = scope['axis_id'], {node['code']: node for node in scope['nodes']}
    observations = []
    for row in rows:
        actual = kosis_row_dimensions(row).get(axis) or {}
        node = by_code.get(actual.get('code'))
        if not node or actual.get('label') != node['label']:
            for current in rows:
                current['_REGION_ERROR'] = '현재 원행의 지역 코드·라벨이 공식 메타와 다릅니다.'
            return rows
        observations.append({'dimensions': {axis: actual}})
    declaration = project_metadata_coordinates(param.org_id, param.tbl_id,
        {axis: {'OBJ_NM': scope['axis_name'], 'items': {node['code']: {'label': node['label'], 'parent': node.get('parent')} for node in scope['nodes']}}}, observations)
    if not declaration:
        for row in rows:
            row['_REGION_ERROR'] = '현재 원행의 지역 계층 근거를 연결할 수 없습니다.'
        return rows
    declaration['metadata_fetched_at'] = scope['metadata_fetched_at']
    for row in rows:
        row['_REGION_SCOPE'] = scope
        row['_REGION_COORDINATES'] = declaration
    return rows


def native_region_payload(rows, param):
    if any(row.get('_REGION_ERROR') for row in rows):
        return {'오류': next(row['_REGION_ERROR'] for row in rows if row.get('_REGION_ERROR')), '코드': 'REGION_BASIS_MISMATCH'}
    contexts = [row.get('_REGION_COORDINATES') for row in rows]
    if not any(contexts) or any(row.get('_AGGREGATION') for row in rows):
        return {}
    if not all(context == contexts[0] for context in contexts):
        return {'오류': '현재 원행의 지역 근거가 서로 다릅니다.', '코드': 'REGION_BASIS_MISMATCH'}
    declaration = deepcopy(contexts[0])
    if (declaration.get('org_id'), declaration.get('table_id')) != (param.org_id, param.tbl_id):
        return {'오류': '현재 표와 지역 근거가 다릅니다.', '코드': 'REGION_BASIS_MISMATCH'}
    for axis in declaration['axes']:
        actuals = [kosis_row_dimensions(row).get(axis['axis_id']) or {} for row in rows]
        if any(not any(node['code'] == actual.get('code') and node['label'] == actual.get('label')
                       for node in axis['coordinates']) for actual in actuals):
            return {'오류': '현재 행이 지역 근거의 코드·라벨을 벗어났습니다.', '코드': 'REGION_BASIS_MISMATCH'}
        selected = {actual['code'] for actual in actuals}
        axis['coordinates'] = [node for node in axis['coordinates'] if node['code'] in selected]
    declaration['observation_periods'] = sorted({str(row.get('PRD_DE') or '') for row in rows})
    return {'regional_coordinates': declaration}


def select_native_region_rows(rows, param):
    """Full province frontier and missing observations, never a fixed code/count filter."""
    if any(row.get('_REGION_ERROR') for row in rows):
        return native_region_payload(rows, param)
    scopes = [row.get('_REGION_SCOPE') for row in rows]
    official = bool(scopes and scopes[0] and all(scope == scopes[0] for scope in scopes))
    if any(scopes) and not official:
        return {'오류': '현재 행의 지역 메타 근거가 서로 다릅니다.', '코드': 'REGION_BASIS_MISMATCH'}
    if official:
        scope = scopes[0]
    elif param.region_scheme and param.verification_status == 'verified' and param.replacement_status == 'current':
        # Without fresh metadata, retain only the reviewed identities and expose
        # omissions/unknown regional rows. Never synthesize a new official tree.
        scope = {'axis_id': 'C'+str(('obj_l1', 'obj_l2', 'obj_l3').index(param.region_obj)+1),
                 'nodes': [{'code': str(code), 'label': label, 'parent': None} for label, code in param.region_scheme.items()]}
    else:
        return None
    members = regional_frontier(scope['nodes'], 'province')
    if not members:
        return None
    wanted = {node['code']: node for node in members}
    axis = scope['axis_id']
    selected, found, missing, unresolved = [], {}, [], []
    for row in rows:
        code = str(row.get(axis) or '')
        if code not in wanted:
            if not official and regional_kind(row.get(axis+'_NM')) == 'province':
                unresolved.append({'code': code, 'label': row.get(axis+'_NM')})
            continue
        if canonical_population_label(row.get(axis+'_NM')) != canonical_population_label(wanted[code]['label']):
            return {'오류': '원행의 지역 라벨이 선택한 지역 근거와 다릅니다.', '코드': 'REGION_BASIS_MISMATCH'}
        if code in found:
            return {'오류': '같은 시점의 지역 값이 중복되어 비교하지 않았습니다.', '코드': 'REGION_BASIS_MISMATCH'}
        found[code] = row
        value = _normalize_stat_value(row.get('DT'))['value']
        if value is not None and math.isfinite(float(value)):
            selected.append(row)
    available = {str(row[axis]) for row in selected}
    for code, node in wanted.items():
        if code not in available:
            missing.append({**node, 'reason': 'missing_value' if code in found else 'no_observation'})
    selected_codes = {str(row[axis]) for row in selected}
    ancestors = {ancestor['code'] for node in (rows[0].get('_REGION_COORDINATES') or {'axes': [{'coordinates': []}]})['axes'][0]['coordinates']
                 if node['code'] in selected_codes for ancestor in node['ancestors']}
    classified = classify_nodes(scope['nodes']) or {}
    return {'rows': selected, 'labels': {code: node['label'] for code, node in wanted.items()},
        'regional_selection': {'contract_version': 'stat-regional-selection/v1', 'basis': 'official_hierarchy' if official else 'verified_curation_region_scheme',
            'org_id': param.org_id, 'table_id': param.tbl_id, 'axis_id': axis, 'grain': 'province',
            'observation_periods': sorted({str(row.get('PRD_DE') or '') for row in rows}),
            **({'metadata_fetched_at': scope['metadata_fetched_at']} if official else {}), 'expected_coordinates': members,
            'selected_codes': sorted(selected_codes), 'missing_coordinates': missing,
            'excluded_same_level_rollups': [node for node in scope['nodes'] if node['code'] in ancestors and node['code'] not in wanted
                                           and classified.get(node['code'], {}).get('level') == 'province'],
            'unresolved_coordinates': unresolved, 'complete': not missing and not unresolved}}
