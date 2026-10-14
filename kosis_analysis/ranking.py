"""Ranking owns the complete candidate scope and the selected observation subset.

It is not an aggregation or a population exemption. No rank is produced from a
partial frontier, mixed coordinates, missing values, units or periods.
"""
from copy import deepcopy
import math

from .units import bound_unit_coordinates, is_ambiguous_unit
from .rules import canonical_population_label


def _number(value):
    if value is None or isinstance(value, bool) or str(value).strip() == '':
        raise ValueError('missing rank value')
    result = float(str(value).replace(',', ''))
    if not math.isfinite(result):
        raise ValueError('non-finite rank value')
    return result


def ranked_comparison(comparison, org_id, table_id, count, descending=True):
    """Return independently sortable candidates plus a rebound selected context."""
    if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= 1000:
        return None
    candidates = comparison.get('rows') or []
    coverage = comparison.get('regional_selection') or {}
    geography = comparison.get('regional_coordinates') or {}
    if (not candidates or len(candidates) > 1000
            or coverage.get('contract_version') != 'stat-regional-selection/v1'
            or coverage.get('basis') != 'official_hierarchy' or coverage.get('complete') is not True
            or coverage.get('grain') != 'province'
            or (coverage.get('org_id'), coverage.get('table_id')) != (org_id, table_id)
            or coverage.get('missing_coordinates') or coverage.get('unresolved_coordinates')
            or geography.get('contract_version') != 'stat-regional-coordinates/v1'
            or geography.get('basis') != 'official_metadata' or geography.get('all_observations_matched') is not True
            or (geography.get('org_id'), geography.get('table_id')) != (org_id, table_id)):
        return None
    axis_id = coverage.get('axis_id')
    axes = [axis for axis in geography.get('axes', []) if axis.get('axis_id') == axis_id]
    if len(axes) != 1:
        return None
    expected = coverage.get('expected_coordinates') or []
    book = {node.get('code'): node.get('label') for node in expected}
    coordinates = axes[0].get('coordinates') or []
    if (len(book) != len(expected) or len(book) != len(candidates)
            or len(coordinates) != len(book)
            or {node.get('code'): node.get('label') for node in coordinates} != book
            or any(node.get('level') != 'province' for node in coordinates)):
        return None
    try:
        codes = [(row.get('dimensions', {}).get(axis_id) or {}).get('code') for row in candidates]
        if (len(set(codes)) != len(codes) or set(codes) != set(book)
                or set(coverage.get('selected_codes') or []) != set(book)
                or any((row.get('dimensions', {}).get(axis_id) or {}).get('label') != book[code]
                       for row, code in zip(candidates, codes))):
            return None
        if any(row.get('dimensions', {}).get('region') and (
                row['dimensions']['region'].get('code') != code
                or canonical_population_label(row['dimensions']['region'].get('label', '')) != canonical_population_label(book[code]))
               for row, code in zip(candidates, codes)):
            return None
        units, periods = {row.get('unit') for row in candidates}, {row.get('period') for row in candidates}
        if (len(units) != 1 or not next(iter(units)) or is_ambiguous_unit(next(iter(units)))
                or len(periods) != 1 or not next(iter(periods))
                or set(coverage.get('observation_periods') or []) != periods
                or set(geography.get('observation_periods') or []) != periods):
            return None
        # A region ranking cannot mix ITEMs or other population strata.
        def strata(row):
            return {axis: coordinate for axis, coordinate in row.get('dimensions', {}).items()
                    if axis not in (axis_id, 'region')}
        if not strata(candidates[0]).get('ITEM') or any(strata(row) != strata(candidates[0]) for row in candidates):
            return None
        binding = comparison.get('unit_coordinate_binding')
        if binding and not bound_unit_coordinates(binding, candidates, org_id, table_id):
            return None
        ordered = sorted(candidates, key=lambda row: (
            -_number(row.get('value')) if descending else _number(row.get('value')),
            (row['dimensions'][axis_id]['code']),
        ))
    except (TypeError, ValueError, KeyError):
        return None
    selected = ordered[:count]
    selected_codes = [row['dimensions'][axis_id]['code'] for row in selected]
    context = {key: deepcopy(comparison[key]) for key in (
        'actual_measure', 'actual_measure_evidence', 'measure_definition', 'measure_role_evidence',
        'curation_measure_binding', 'measure_basis', 'aggregation', 'source_url',
        'unit_coordinate_binding', 'regional_coordinates',
    ) if key in comparison}
    context['rows'] = deepcopy(selected)
    if binding:
        subset = context['unit_coordinate_binding']
        for axis in subset['required_coordinates']:
            codes = sorted({row['dimensions'][axis]['code'] for row in selected})
            subset['required_coordinates'][axis] = codes
            for key in ('coordinate_labels', 'coordinate_units'):
                if key in subset:
                    subset[key][axis] = {code: subset[key][axis][code] for code in codes}
        subset['source_units'] = sorted({row.get('source_unit', '') for row in selected})
        if not bound_unit_coordinates(subset, selected, org_id, table_id):
            return None
    for axis in context['regional_coordinates']['axes']:
        codes = {row['dimensions'][axis['axis_id']]['code'] for row in selected}
        axis['coordinates'] = [node for node in axis['coordinates'] if node['code'] in codes]
    cutoff_tied = len(ordered) > len(selected) and _number(ordered[len(selected)-1]['value']) == _number(ordered[len(selected)]['value'])
    context['ranking'] = {
        'contract_version': 'stat-ranking/v1', 'operation': 'top_n',
        'org_id': org_id, 'table_id': table_id, 'axis_id': axis_id, 'grain': 'province',
        'requested_count': count, 'direction': 'desc' if descending else 'asc',
        'tie_policy': 'value_then_code', 'cutoff_tied': cutoff_tied, 'selected_codes': selected_codes,
        'candidate_rows': deepcopy(candidates), 'candidate_coverage': deepcopy(coverage),
        'candidate_regional_coordinates': deepcopy(geography),
        **({'candidate_unit_coordinate_binding': deepcopy(binding)} if binding else {}),
    }
    return context
