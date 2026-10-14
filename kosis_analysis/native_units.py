"""One source-unit owner for native scalar, trend, comparison and calculation paths.

Internal row context survives sorting/period slicing. Public canonical rows and
bindings are projected only for the actual returned subset, never inferred from
the request or a more-specific-looking curated unit string.
"""
from copy import deepcopy
from datetime import datetime, timezone
import re

from .curated_identity import apply_curated_coordinate_measure
from .evidence import kosis_row_dimensions
from .metadata import _build_axis_codebook, _normalize_query_table_rows, _normalize_stat_value
from .measure_roles import apply_classification_measure
from .rules import UNIT_RULES
from .units import apply_curated_coordinate_unit, apply_official_coordinate_unit, bound_unit_coordinates, flat_unit_alternatives


def source_unit(unit):
    raw = str(unit or '').strip()
    return ('지수', raw) if re.fullmatch(UNIT_RULES['indexBasePattern'], raw) else (raw, '')


def needs_unit_metadata(rows, param):
    return any(not str(row.get('UNIT_NM') or '').strip() or flat_unit_alternatives(row.get('UNIT_NM'))
               or source_unit(row.get('UNIT_NM'))[1]
               or '당' in param.unit and str(row.get('UNIT_NM') or '').strip() != param.unit for row in rows)


def normalize_native_units(rows, param, period_type, item_meta=None):
    result = []
    for row in rows:
        copied = dict(row)
        unit, base = source_unit(row.get('UNIT_NM'))
        copied['UNIT_NM'] = unit
        copied['_SOURCE_UNIT_NM'] = str(row.get('UNIT_NM') or '').strip()
        if base:
            copied['_INDEX_BASE'] = base
        result.append(copied)
    if not result or not item_meta:
        return result
    axes, order = _build_axis_codebook(item_meta)
    classifications = [axis for axis in order if axis != 'ITEM']
    if 'ITEM' not in order or len(classifications) > 10:
        return result
    fields = {'ITEM': 'ITM_ID', **{axis: 'C'+str(index+1) for index, axis in enumerate(classifications)}}
    # Do not synthesize missing actual codes from a default filter or ignore an extra axis.
    if any(any(row.get(field) in (None, '') for field in fields.values())
           or any(row.get('C'+str(index)) not in (None, '') for index in range(len(classifications)+1, 11)) for row in rows):
        return result
    filters = {axis: sorted({str(row[field]) for row in rows}) for axis, field in fields.items()}
    normalized = _normalize_query_table_rows(rows, filters, axes, order)
    if any(any(not isinstance(coordinate, dict) or not coordinate.get('label')
               or ((axes.get(axis) or {}).get('items') or {}).get(coordinate.get('code'), {}).get('label') != coordinate['label']
               for axis, coordinate in observed['dimensions'].items()) for observed in normalized):
        return result
    for raw, observed in zip(rows, normalized):
        observed['source_unit'] = str(raw.get('UNIT_NM') or '').strip()
    payload = {'org_id': param.org_id, 'tbl_id': param.tbl_id, 'period_type': period_type, 'rows': normalized,
        'metadata_source': {'org_id': param.org_id, 'tbl_id': param.tbl_id, 'metadata_tool': 'explore_table',
                            'fetched_at': datetime.now(timezone.utc).isoformat()}}
    # Reviewed definitions and official unit-only metadata are distinct authorities.
    apply_curated_coordinate_unit(payload, order, {'selected': param}, axes)
    apply_official_coordinate_unit(payload, order, axes)
    apply_classification_measure(payload, axes)
    apply_curated_coordinate_measure(payload, order, {'selected': param}, axes)
    remap = {'ITEM': 'ITEM', **{axis: 'C'+str(index+1) for index, axis in enumerate(classifications)}}
    binding = deepcopy(payload.get('unit_coordinate_binding'))
    if binding:
        for key in ('required_coordinates', 'coordinate_labels', 'coordinate_units'):
            if key in binding:
                binding[key] = {remap[axis]: value for axis, value in binding[key].items()}
    identity = {key: deepcopy(payload[key]) for key in ('actual_measure', 'actual_measure_evidence', 'measure_definition', 'measure_role_evidence', 'curation_measure_binding') if payload.get(key)}
    if identity.get('measure_role_evidence', {}).get('axis_id'):
        identity['measure_role_evidence']['axis_id'] = remap[identity['measure_role_evidence']['axis_id']]
    if identity.get('curation_measure_binding'):
        identity['curation_measure_binding']['required_coordinates'] = {remap[axis]: codes for axis, codes in identity['curation_measure_binding']['required_coordinates'].items()}
    for copied, observed in zip(result, normalized):
        if not binding and observed.get('unit') != copied['UNIT_NM']:
            continue  # An uncorroborated metadata/default cannot replace the actual source unit.
        copied['UNIT_NM'] = observed.get('unit') or ''
        copied['_UNIT_OBSERVATION'] = {key: deepcopy(value) for key, value in observed.items() if key not in ('raw', 'dimensions')}
        copied['_UNIT_OBSERVATION']['dimensions'] = {remap[axis]: dim for axis, dim in observed['dimensions'].items()}
        if observed.get('index_base'):
            copied['_INDEX_BASE'] = observed['index_base']
        if binding:
            copied['_UNIT_BINDING'] = binding
        if identity:
            copied['_MEASURE_IDENTITY'] = identity
    return result


def native_unit_payload(rows, org_id, table_id, param=None):
    observations = []
    for row in rows:
        unit, base = source_unit(row.get('UNIT_NM'))
        observed = deepcopy(row.get('_UNIT_OBSERVATION')) or {
            'period': row.get('PRD_DE'), 'value': row.get('DT'), 'unit': unit,
            'dimensions': kosis_row_dimensions(row, region_field='C'+str(('obj_l1','obj_l2','obj_l3').index(param.region_obj)+1) if param else None,
                region_labels={str(code): name for name, code in param.region_scheme.items()} if param and param.region_scheme else None),
        }
        # Actual row data always wins over retained internal context.
        # Same value-state owner as query_table: '-' / suppressed / non-finite
        # values must not become an executed number merely by retaining raw text.
        value_info = _normalize_stat_value(row.get('DT'))
        if value_info['value'] is not None:
            value_info['value'] = row.get('DT')  # Preserve the native public value type for valid numbers.
        observed.pop('symbol', None)
        observed.update(value_info, period=row.get('PRD_DE'), unit=unit)
        if row.get('_UNIT_BINDING'):
            for axis, coordinate in observed['dimensions'].items():
                field, label_field = ('ITM_ID', 'ITM_NM') if axis == 'ITEM' else (axis, axis+'_NM')
                if str(row.get(field) or '') != coordinate.get('code') or row.get(label_field) and row[label_field] != coordinate.get('label'):
                    return {'오류': '현재 원행의 분류 좌표가 단위 근거와 다릅니다.', '코드': 'UNIT_BASIS_MISMATCH'}
        original = row.get('_SOURCE_UNIT_NM', row.get('UNIT_NM'))
        observed['source_unit'] = str(original or '').strip()
        observations.append(observed)
    payload = {'rows': observations}
    contexts = [row.get('_UNIT_BINDING') for row in rows]
    if any(contexts):
        if not all(context == contexts[0] for context in contexts):
            return {'오류': '현재 행의 단위 근거가 서로 다릅니다.', '코드': 'UNIT_BASIS_MISMATCH'}
        binding = deepcopy(contexts[0])
        for axis in binding['required_coordinates']:
            selected = {str((row.get('dimensions', {}).get(axis) or {}).get('code') or '') for row in observations}
            if not selected <= set(binding['required_coordinates'][axis]):
                return {'오류': '현재 행이 검증된 단위 좌표를 벗어났습니다.', '코드': 'UNIT_BASIS_MISMATCH'}
            binding['required_coordinates'][axis] = sorted(selected)
            for key in ('coordinate_labels', 'coordinate_units'):
                if key in binding:
                    binding[key][axis] = {code: binding[key][axis][code] for code in selected}
        selected_sources = {row['source_unit'] for row in observations}
        if not selected_sources <= set(binding['source_units']):
            return {'오류': '현재 원 단위가 검증된 단위 근거와 다릅니다.', '코드': 'UNIT_BASIS_MISMATCH'}
        binding['source_units'] = sorted(selected_sources)
        if not bound_unit_coordinates(binding, observations, org_id, table_id):
            return {'오류': '현재 행의 단위·좌표·원 단위 근거가 일치하지 않습니다.', '코드': 'UNIT_BASIS_MISMATCH'}
        payload['unit_coordinate_binding'] = binding
    identities = [row.get('_MEASURE_IDENTITY') for row in rows]
    if identities and identities[0] and all(identity == identities[0] for identity in identities):
        payload.update(identities[0])
    return payload
