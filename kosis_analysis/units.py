"""Units belong to the selected source coordinates, never the user's wording."""
import re
from urllib.parse import urlencode, urlparse, parse_qs

from .curated_identity import curated_required_coordinates
from .rules import canonical_population_label, UNIT_RULES


def is_ambiguous_unit(unit):
    text = str(unit or '')
    return bool(re.search(UNIT_RULES['percentPattern'], text, re.I) and re.search(UNIT_RULES['numeratorPattern'], text, re.I) or flat_unit_alternatives(text))


def flat_unit_alternatives(source):
    text = str(source or '').strip()
    if re.search(UNIT_RULES['alternativeForbiddenPattern'], text):
        return []
    tokens = re.split(UNIT_RULES['alternativeSeparatorPattern'], text)
    return tokens if len(tokens) >= 2 and all(re.fullmatch(UNIT_RULES['alternativeUnitPattern'], token, re.I) for token in tokens) else []


def official_coordinate_unit(source, dimensions):
    """Unit-only source interpretation, not proof of metric or population."""
    source = str(source or '').strip()
    coordinates = [dim for dim in dimensions.values() if isinstance(dim, dict)]
    if re.fullmatch(UNIT_RULES['indexBasePattern'], source):
        return '지수'
    units = {dim['unit'] for dim in coordinates if dim.get('unit')}
    if not source:
        return next(iter(units)) if len(units) == 1 and not is_ambiguous_unit(next(iter(units))) else None
    alternatives = flat_unit_alternatives(source)
    if alternatives:
        if any(unit not in alternatives and set(flat_unit_alternatives(unit)) != set(alternatives) for unit in units):
            return None  # A precise conflicting axis unit is not an alternative-list hint.
        candidates = units.intersection(alternatives)
        for dim in coordinates:
            match = re.search(UNIT_RULES['unitSuffixPattern'], str(dim.get('label') or ''))
            suffix = match.group(1).strip() if match else None
            if suffix in alternatives:
                candidates.add(suffix)
            if suffix == '천명당' and all(re.fullmatch(UNIT_RULES['countUnitPattern'], unit) for unit in alternatives):
                candidates.add('‰')
        return next(iter(candidates)) if len(candidates) == 1 else None
    item = dimensions.get('ITEM') or {}
    denominator = re.search(UNIT_RULES['perPopulationPattern'], str(item.get('label') or ''))
    if denominator and re.fullmatch(UNIT_RULES['countUnitPattern'], source) and all(unit == source for unit in units):
        return denominator.group(1) + '명당 ' + source
    return None


def apply_official_coordinate_unit(payload, axis_order, axis_codebook):
    """Resolve only when every current coordinate is corroborated by official metadata."""
    rows, metadata = payload.get('rows') or [], payload.get('metadata_source') or {}
    if not rows or payload.get('unit_coordinate_binding') or payload.get('aggregation', 'none') != 'none':
        return
    if not axis_order or 'ITEM' not in axis_order or len(set(axis_order)) != len(axis_order) or len(axis_order) > 11:
        return
    if (metadata.get('org_id'), metadata.get('tbl_id'), metadata.get('metadata_tool')) != (payload.get('org_id'), payload.get('tbl_id'), 'explore_table') or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)', metadata.get('fetched_at') or ''):
        return
    selected = {axis: {} for axis in axis_order}
    declared_units = {axis: {} for axis in axis_order}
    resolutions = []
    for row in rows:
        dimensions = row.get('dimensions')
        if not isinstance(dimensions, dict) or set(dimensions) != set(axis_order):
            return
        for axis in axis_order:
            coordinate = dimensions[axis]
            if not isinstance(coordinate, dict):
                return
            code, label = coordinate.get('code'), coordinate.get('label')
            meta = ((axis_codebook.get(axis) or {}).get('items') or {}).get(code)
            if not code or not label or not meta or meta.get('label') != label:
                return
            from .metadata import _resolved_item_unit
            unit = _resolved_item_unit(axis_codebook[axis]['items'], code) or ''
            if (coordinate.get('unit') or '') != unit:
                return
            selected[axis][code], declared_units[axis][code] = label, unit
        source = str(row.get('source_unit', row.get('unit')) or '')
        unit = official_coordinate_unit(source, dimensions)
        if not unit:
            return
        resolutions.append((source, unit))
    if len({unit for _, unit in resolutions}) != 1:
        return
    unit = resolutions[0][1]
    for row, (source, _) in zip(rows, resolutions):
        row.update(unit=unit, source_unit=source)
        if re.fullmatch(UNIT_RULES['indexBasePattern'], source):
            row['index_base'] = source
    payload['unit_coordinate_binding'] = {
        'contract_version': 'stat-unit-coordinates/v2', 'basis': 'official_coordinate_metadata',
        'org_id': payload['org_id'], 'table_id': payload['tbl_id'], 'unit': unit,
        'metadata_fetched_at': metadata['fetched_at'], 'metadata_tool': 'explore_table',
        'references': ['https://kosis.kr/statHtml/statHtml.do?' + urlencode({'orgId': payload['org_id'], 'tblId': payload['tbl_id']})],
        'required_coordinates': {axis: sorted(labels) for axis, labels in selected.items()},
        'coordinate_labels': selected, 'coordinate_units': declared_units,
        'source_units': sorted({source for source, _ in resolutions}), 'all_observations_matched': True,
    }


def reviewed_unit_alternative(source_unit, selected_unit):
    """A flat alternative list is not a ratio, index base or scale conversion."""
    source = str(source_unit or '').strip()
    if not is_ambiguous_unit(source) or re.search(UNIT_RULES['alternativeForbiddenPattern'], source):
        return False
    tokens = re.split(UNIT_RULES['alternativeSeparatorPattern'], source)
    return len(tokens) >= 2 and selected_unit in tokens and all(re.fullmatch(UNIT_RULES['alternativeUnitPattern'], unit, re.I) for unit in tokens)

def apply_curated_coordinate_unit(payload, axis_order, registry, axis_codebook):
    """Resolve a reviewed unit only after current source coordinates corroborate it."""
    rows = payload.get('rows') or []
    metadata = payload.get('metadata_source') or {}
    if not rows or payload.get('aggregation', 'none') != 'none' or not isinstance(axis_order, list) or len(axis_order) > 4 or any(not isinstance(axis, str) or not axis for axis in axis_order) or len(set(axis_order)) != len(axis_order) or not isinstance(axis_codebook, dict):
        return
    if (metadata.get('org_id'), metadata.get('tbl_id'), metadata.get('metadata_tool')) != (payload.get('org_id'), payload.get('tbl_id'), 'explore_table'):
        return
    axes = [axis for axis in axis_order if axis != 'ITEM']
    matches = {}
    for param in registry.values():
        reviewed = getattr(param, 'unit_coordinate_labels', None)
        if not isinstance(reviewed, dict) or not reviewed or param.region_obj not in ('obj_l1', 'obj_l2', 'obj_l3') or param.verification_status != 'verified' or param.replacement_status != 'current' or param.aggregation:
            continue
        if (payload.get('org_id'), payload.get('tbl_id')) != param.table_identity or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', param.verified_at or ''):
            continue
        if payload.get('period_type') and payload['period_type'] not in param.supported_periods:
            continue
        constraints = curated_required_coordinates(param, axis_order)
        if not constraints or set(constraints) != set(axis_order):
            continue  # Unbound classifications may change the metric or unit.
        expected = {'ITEM': reviewed.get('ITEM', {})}
        for index, attribute in enumerate(('obj_l1', 'obj_l2', 'obj_l3')):
            if not getattr(param, attribute, None):
                continue
            if index >= len(axes):
                break
            if attribute == param.region_obj and param.region_scheme:
                expected[axes[index]] = {str(code): name for name, code in param.region_scheme.items()}
            else:
                expected[axes[index]] = reviewed.get(attribute, {})
        if set(expected) != set(constraints) or any(not labels for labels in expected.values()):
            continue
        selected = {axis: {} for axis in constraints}
        source_units = set()
        valid = True
        for row in rows:
            if not isinstance(row, dict):
                valid = False; break
            dimensions = row.get('dimensions') or {}
            if not isinstance(dimensions, dict) or set(dimensions) != set(constraints) or any(not isinstance(dimension, dict) for dimension in dimensions.values()) or not reviewed_unit_alternative(row.get('unit'), param.unit):
                valid = False; break
            for axis, allowed in constraints.items():
                coordinate = dimensions.get(axis) or {}
                code, label = str(coordinate.get('code') or ''), coordinate.get('label')
                actual = ((axis_codebook.get(axis) or {}).get('items') or {}).get(code) or {}
                declared = expected[axis].get(code)
                if code not in allowed or not label or actual.get('label') != label or not declared:
                    valid = False; break
                is_region = axis != 'ITEM' and axes.index(axis) == ('obj_l1', 'obj_l2', 'obj_l3').index(param.region_obj) and param.region_scheme
                if (canonical_population_label(label) != canonical_population_label(declared) if is_region else label != declared):
                    valid = False; break
                if any(value and value != param.unit for value in (coordinate.get('unit'), actual.get('unit'))):
                    valid = False; break  # Never overrule an explicit coordinate-specific unit.
                selected[axis][code] = label
            if not valid:
                break
            source_units.add(row['unit'])
        if valid:
            key = (param.unit, param.measurement_name, param.description, param.verified_at)
            matches[key] = (selected, source_units)
    if len(matches) != 1:
        return
    (unit, measure, definition, reviewed_at), (selected, source_units) = next(iter(matches.items()))
    for row in rows:
        row['source_unit'] = row['unit']
        row['unit'] = unit
    payload['unit_coordinate_binding'] = {
        'contract_version': 'stat-unit-coordinates/v1', 'basis': 'verified_curation_coordinates',
        'org_id': payload['org_id'], 'table_id': payload['tbl_id'], 'unit': unit,
        'measure_name': measure, 'definition': definition, 'reviewed_at': reviewed_at,
        'references': ['https://kosis.kr/statHtml/statHtml.do?' + urlencode({'orgId': payload['org_id'], 'tblId': payload['tbl_id']})],
        'required_coordinates': {axis: sorted(labels) for axis, labels in selected.items()},
        'coordinate_labels': selected, 'source_units': sorted(source_units), 'all_observations_matched': True,
    }


def bound_unit_coordinates(binding, rows, org_id, table_id):
    """A declaration is meaningful only for all current labels/codes/units."""
    if not isinstance(binding, dict) or not rows:
        return None
    official = binding.get('contract_version') == 'stat-unit-coordinates/v2' and binding.get('basis') == 'official_coordinate_metadata'
    reviewed = binding.get('contract_version') == 'stat-unit-coordinates/v1' and binding.get('basis') == 'verified_curation_coordinates'
    if not official and not reviewed:
        return None
    if not org_id or not table_id or (binding.get('org_id'), binding.get('table_id')) != (org_id, table_id) or binding.get('all_observations_matched') is not True:
        return None
    unit, constraints, labels = binding.get('unit'), binding.get('required_coordinates'), binding.get('coordinate_labels')
    if not isinstance(unit, str) or not unit or is_ambiguous_unit(unit) or not isinstance(constraints, dict) or not constraints or not isinstance(labels, dict) or set(labels) != set(constraints) or 'ITEM' not in constraints or len(constraints) > (11 if official else 4):
        return None
    if reviewed and (not binding.get('measure_name') or not binding.get('definition') or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', binding.get('reviewed_at') or '')):
        return None
    declarations = binding.get('coordinate_units')
    if official and (binding.get('metadata_tool') != 'explore_table' or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)', binding.get('metadata_fetched_at') or '') or not isinstance(declarations, dict) or set(declarations) != set(constraints)):
        return None
    sources = binding.get('source_units')
    if not isinstance(sources, list) or not sources or any(not isinstance(source, str) or reviewed and not reviewed_unit_alternative(source, unit) for source in sources):
        return None
    references = binding.get('references')
    if not isinstance(references, list) or not references or len(references) > 10:
        return None
    for reference in references:
        if not isinstance(reference, str):
            return None
        url = urlparse(reference)
        query = parse_qs(url.query)
        if url.scheme != 'https' or url.hostname != 'kosis.kr' or url.username or url.password or url.path != '/statHtml/statHtml.do' or query.get('orgId') != [org_id] or query.get('tblId') != [table_id]:
            return None
    for axis, codes in constraints.items():
        if not isinstance(codes, list) or not codes or any(not isinstance(code, str) or not code for code in codes) or len(set(codes)) != len(codes) or not isinstance(labels[axis], dict) or set(labels[axis]) != set(codes):
            return None
        if official and (not isinstance(declarations[axis], dict) or set(declarations[axis]) != set(codes) or any(not isinstance(value, str) for value in declarations[axis].values())):
            return None
    for row in rows:
        if not isinstance(row, dict) or row.get('unit') != unit or row.get('source_unit', '') not in sources:
            return None
        dimensions = row.get('dimensions')
        if not isinstance(dimensions, dict) or set(dimensions) != set(constraints):
            return None
        for axis, codes in constraints.items():
            coordinate = dimensions.get(axis)
            if not isinstance(coordinate, dict) or coordinate.get('code') not in codes or not coordinate.get('label') or coordinate['label'] != labels[axis][coordinate['code']]:
                return None
            if official and (coordinate.get('unit') or '') != declarations[axis][coordinate['code']]:
                return None
            if reviewed and coordinate.get('unit') and coordinate['unit'] != unit:
                return None
        if official and official_coordinate_unit(row.get('source_unit', ''), dimensions) != unit:
            return None
    if any({row['dimensions'][axis]['code'] for row in rows} != set(codes) for axis, codes in constraints.items()):
        return None
    return binding
