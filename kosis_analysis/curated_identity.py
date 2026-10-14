"""Measurement definitions bound to freshly executed table coordinates.

Some KOSIS tables put the metric on a classification axis and use a broad
ITEM family. A verified mapping proves meaning only when every current row
matches its ITEM, fixed classifications and unit. Table identity alone is not
a metric proof. No request words or caller-supplied definition enter this path.
"""
from kosis_analysis.rules import measure_of
import re


def curated_required_coordinates(param, axis_order):
    """The same fixed coordinates own measure identity and reviewed series context."""
    axes = [axis for axis in axis_order if axis != 'ITEM']
    constraints = {'ITEM': {param.item_id}}
    for index, attribute in enumerate(('obj_l1', 'obj_l2', 'obj_l3')):
        fixed = getattr(param, attribute, None)
        if not fixed:
            continue
        if index >= len(axes):
            return None
        scheme = param.region_scheme if getattr(param, 'region_obj', 'obj_l1') == attribute else None
        constraints[axes[index]] = set(scheme.values()) if scheme else {fixed}
    return constraints


def apply_curated_coordinate_measure(payload, axis_order, registry, axis_codebook=None):
    rows = payload.get('rows') or []
    if not rows or payload.get('aggregation', 'none') != 'none' or payload.get('actual_measure'):
        return
    axes = [axis for axis in axis_order if axis != 'ITEM']
    item_labels = {(row.get('dimensions', {}).get('ITEM') or {}).get('label') for row in rows}
    if any(measure_of(str(label or '')) for label in item_labels):
        return  # Actual ITEM meaning takes precedence over a curation name.
    from .units import bound_unit_coordinates
    unit_context = bound_unit_coordinates(payload.get('unit_coordinate_binding'), rows, payload.get('org_id'), payload.get('tbl_id'))
    if unit_context and unit_context.get('basis') == 'verified_curation_coordinates':
        payload.update(actual_measure=unit_context['measure_name'], measure_definition=unit_context['definition'],
                       actual_measure_evidence='verified_curation_unit_coordinates')
        return
    matches = {}
    for param in registry.values():
        if param.verification_status != 'verified' or param.replacement_status == 'deprecated' or param.aggregation:
            continue
        if (payload.get('org_id'), payload.get('tbl_id')) != (param.org_id, param.tbl_id):
            continue
        if not measure_of(param.measurement_name) or not axes:
            continue
        constraints = curated_required_coordinates(param, axis_order)
        if constraints is None:
            continue
        def resolved_unit(row):
            if row.get('unit') == param.unit:
                return param.unit
            # A broad ITEM can declare a list of count units, not a composite
            # rate. Resolve it only with a selected official category's unit.
            tokens = str(row.get('unit') or '').split()
            if len(tokens) < 2 or param.unit not in tokens or not set(tokens) <= {'명', '건', '개', '호', '가구', '개소', '대'}:
                return None
            for axis in constraints:
                code = str((row.get('dimensions', {}).get(axis) or {}).get('code') or '')
                meta = ((axis_codebook or {}).get(axis, {}).get('items') or {}).get(code, {})
                label = str(meta.get('label') or '')
                match = re.search(r'\(([^()]+)\)\s*$', label)
                if match and match.group(1).strip() == param.unit:
                    return param.unit
            return None
        if any(resolved_unit(row) is None or any(
            str((row.get('dimensions', {}).get(axis) or {}).get('code') or '') not in codes
            for axis, codes in constraints.items()) for row in rows):
            continue
        matches[(param.measurement_name, param.description)] = (constraints, param.unit)
    if len(matches) != 1:
        return  # Ambiguous definitions never get resolved by the user's wording.
    (measure_name, definition), (constraints, unit) = next(iter(matches.items()))
    unit_resolutions = []
    for row in rows:
        if row['unit'] != unit:
            unit_resolutions.append({'source_unit': row['unit'], 'selected_unit': unit,
                'basis': 'verified_coordinates_and_official_category_unit'})
            row['source_unit'] = row['unit']
            row['unit'] = unit
    payload.update(actual_measure=measure_name, measure_definition=definition,
                   actual_measure_evidence='verified_curation_description',
                   curation_measure_binding={'contract_version': 'curation-coordinates/v1',
                       'org_id': payload['org_id'], 'tbl_id': payload['tbl_id'], 'definition': definition,
                       'measure_name': measure_name,
                       'required_coordinates': {axis: sorted(codes) for axis, codes in constraints.items()},
                       'all_observations_matched': True, 'unit_resolutions': unit_resolutions})
