"""Request-independent country/compiler/basis bound to current KOSIS coordinates.

Registry declarations are not table-wide exemptions. No caller flags, request
words, data values or organization IDs alone supply a reviewed series context.
"""
import re
from dataclasses import asdict
from urllib.parse import urlparse

from .curated_identity import curated_required_coordinates
from .rules import is_total_label

RAW_KOSIS_TOOLS = {'answer_query', 'quick_stat', 'quick_trend', 'query_table'}
DOMESTIC_REGIONS = {'전국', '서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종',
                    '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주'}


def coordinate(row, axis):
    dimensions = row.get('dimensions')
    value = dimensions.get(axis) if isinstance(dimensions, dict) else None
    return value if isinstance(value, dict) else {}


def bound_series_context(payload, observations, table, source, tool, registry):
    if tool not in RAW_KOSIS_TOOLS or not observations or 'KOSIS' not in str(source.get('label') or '').upper():
        return None, None
    if payload.get('aggregation', 'none') != 'none' or source.get('native_identity'):
        return None, None
    # query_table uses fresh official OBJ_IDs; native provider rows use C1..C8.
    axis_order = payload.get('coordinate_axes') if tool == 'query_table' else [f'C{i}' for i in range(1, 9)]
    if not isinstance(axis_order, list) or not axis_order or len(set(axis_order)) != len(axis_order):
        return None, None
    axes = [axis for axis in axis_order if axis != 'ITEM']
    contexts = {}
    domestic_matches = []
    from kosis_curation import canonical_region
    for param in registry.values():
        if param.verification_status != 'verified' or param.replacement_status != 'current' or param.aggregation:
            continue
        if (table.get('org_id'), table.get('table_id')) != (param.org_id, param.tbl_id):
            continue
        # Region geography is independent of measurement: every current region
        # must agree with the registered domestic codebook and any explicit label.
        if param.region_scheme and '전국' in param.region_scheme:
            index = ('obj_l1', 'obj_l2', 'obj_l3').index(param.region_obj)
            if index < len(axes):
                axis = axes[index]
                domestic_codes = {code: name for name, code in param.region_scheme.items() if name in DOMESTIC_REGIONS}
                def domestic(row):
                    selected = coordinate(row, axis)
                    alias = coordinate(row, 'region')
                    if not selected.get('code'):
                        selected = alias  # Existing native region-only evidence.
                    code, label = str(selected.get('code') or ''), str(selected.get('label') or '')
                    named = canonical_region(label) or label
                    valid = code in domestic_codes and (not label or (named == domestic_codes[code] if named in DOMESTIC_REGIONS else is_total_label(label)))
                    if alias.get('code'):
                        alias_name = canonical_region(str(alias.get('label') or '')) or str(alias.get('label') or '')
                        valid = valid and str(alias['code']) == code and (not alias_name or (alias_name == domestic_codes.get(code) if alias_name in DOMESTIC_REGIONS else is_total_label(alias_name)))
                    return valid
                if all(domestic(row) for row in observations):
                    domestic_matches.append({'country': 'KR', 'basis': 'verified_curation_region_scheme'})
        scope = getattr(param, 'series_context', None)
        if scope is None or not isinstance(scope.country, str) or not re.fullmatch(r'[A-Z]{2}', scope.country) or not isinstance(scope.compiler, str) or not scope.compiler.strip() or not scope.references:
            continue
        if scope.price_basis not in {'real', 'nominal', 'not_applicable'} or not isinstance(scope.reviewed_at, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', scope.reviewed_at):
            continue
        if any(not isinstance(url, str) or urlparse(url).scheme != 'https' or not urlparse(url).netloc for url in scope.references):
            continue
        constraints = curated_required_coordinates(param, axis_order)
        if constraints is None:
            continue
        def matches(row):
            dimensions = row.get('dimensions')
            if not isinstance(dimensions, dict):
                return False
            if row.get('unit') != param.unit or any(str(coordinate(row, axis).get('code') or '') not in codes
                                                  for axis, codes in constraints.items()):
                return False
            # An unbound extra classification could change the declared series.
            for axis, dimension in dimensions.items():
                if axis in constraints:
                    continue
                if not isinstance(dimension, dict):
                    return False
                code = str(dimension.get('code') or '')
                if axis == 'region' and code in {value for key in constraints for value in constraints[key]}:
                    continue  # A provider region alias of an already-bound axis.
                if code:
                    return False
            return True
        if not all(matches(row) for row in observations):
            continue
        context = {'contract_version': 'stat-series-context/v1', 'basis': 'verified_curation_coordinates',
                   **asdict(scope), 'binding': {'org_id': param.org_id, 'table_id': param.tbl_id, 'unit': param.unit,
                        'required_coordinates': {axis: sorted(codes) for axis, codes in constraints.items()},
                        'all_observations_matched': True}}
        contexts[repr(context)] = context
    if len(contexts) == 1:
        context = next(iter(contexts.values()))
        return context, {'country': context['country'], 'basis': 'verified_curation_coordinates'}
    return None, domestic_matches[0] if domestic_matches else None
