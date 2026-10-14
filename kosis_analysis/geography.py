"""Official administrative kinds/hierarchy bound to current observations.

No request text, table-ID exceptions, fixed province count or code-length rule
can replace actual metadata or a reviewed native region codebook.
"""
import re
from .rules import GEOGRAPHY_RULES, regional_kind, canonical_population_label
from .branches import metadata_branches


def classify_nodes(nodes, branch_root=None):
    by_code = {node['code']: node for node in nodes}
    if len(by_code) != len(nodes) or any(not node.get('code') or not node.get('label') for node in nodes):
        return None
    classified = {}
    for node in nodes:
        kind = 'unknown' if node['code'] == branch_root else regional_kind(node['label'])
        ancestors, visited, parent = [], {node['code']}, node.get('parent')
        while parent and parent in by_code:
            if parent in visited:
                return None
            visited.add(parent); ancestor = by_code[parent]
            ancestors.append(ancestor); parent = ancestor.get('parent')
        municipal = kind in {'city', 'county', 'district'}
        scoped_ancestors = ancestors[:next((i+1 for i,n in enumerate(ancestors) if n['code'] == branch_root), 0)] if branch_root else ancestors
        level = ('submunicipality' if any(item['code'] != branch_root and regional_kind(item['label']) in {'city', 'county', 'district'} for item in scoped_ancestors)
                 else 'municipality') if municipal else kind
        classified[node['code']] = {**node, 'kind': kind, 'level': level}
    return classified


def regional_frontier(nodes, grain):
    """Disjoint requested level: same-level rollups do not duplicate their members."""
    classified = classify_nodes(nodes)
    if not classified:
        return None
    matches = [node for node in classified.values() if (node['kind'] == grain
               if grain in {'city', 'county', 'district'} else node['level'] == grain)]
    wanted = {node['code']: node for node in matches}

    def ancestors(node):
        parent = node.get('parent')
        while parent and parent in classified:
            yield classified[parent]
            parent = classified[parent].get('parent')

    return [node for node in matches
            if not any(other['code'] != node['code'] and other['level'] == node['level']
                       and any(parent['code'] == node['code'] for parent in ancestors(other)) for other in matches)
            and not any(parent['code'] in wanted and parent['level'] != node['level'] for parent in ancestors(node))]


def project_metadata_coordinates(org_id, table_id, axes, rows):
    projected = []
    for axis_id, axis in axes.items():
        name = str(axis.get('OBJ_NM') or '')
        branches = [b for b in metadata_branches(axes) if b['axis_id'] == axis_id and b['dimension'] == 'region'
                    and all(str((row.get('dimensions') or {}).get(axis_id, {}).get('code') or '') in b['member_codes'] for row in rows)]
        branch = branches[0] if len(branches) == 1 else None
        if not re.search(GEOGRAPHY_RULES['axisPattern'], re.sub(r'\s+', '', name)) and not branch:
            continue
        nodes = [{'code': code, 'label': str(meta.get('label') or ''), 'parent': meta.get('parent')}
                 for code, meta in (axis.get('items') or {}).items()]
        classified = classify_nodes(nodes, branch['root_code'] if branch else None)
        if not classified:
            continue
        coordinates = {}
        for row in rows:
            actual = (row.get('dimensions') or {}).get(axis_id) or {}
            node = classified.get(str(actual.get('code') or ''))
            if not node or node['label'] != actual.get('label'):
                coordinates = {}; break
            ancestors, parent = [], node.get('parent')
            while parent and parent in classified:
                ancestor = classified[parent]; ancestors.append(ancestor); parent = ancestor.get('parent')
            coordinates[node['code']] = {**node, 'ancestors': ancestors}
        if coordinates:
            projected.append({'axis_id': axis_id, 'axis_name': name, 'coordinates': list(coordinates.values()),
                              **({'branch_root': {'code': branch['root_code'], 'label': branch['root_label']}} if branch else {})})
    return {'contract_version': 'stat-regional-coordinates/v1', 'basis': 'official_metadata',
        'org_id': org_id, 'table_id': table_id, 'axes': projected, 'all_observations_matched': True} if projected else None


def native_regional_coordinates(payload, rows, table, tool, registry):
    if tool not in {'answer_query', 'quick_stat', 'quick_trend'} or payload.get('aggregation', 'none') != 'none':
        return None
    matches = {}
    for param in registry.values():
        if param.verification_status != 'verified' or param.replacement_status != 'current' or param.aggregation or not param.region_scheme:
            continue
        if (table.get('org_id'), table.get('table_id')) != (param.org_id, param.tbl_id):
            continue
        index = ('obj_l1', 'obj_l2', 'obj_l3').index(param.region_obj) + 1
        axis_id = 'C'+str(index)
        codebook = {str(code): name for name, code in param.region_scheme.items()}
        coordinates = {}
        for row in rows:
            actual = (row.get('dimensions') or {}).get(axis_id) or {}
            code = str(actual.get('code') or '')
            name = codebook.get(code)
            if not name or not actual.get('label') or canonical_population_label(actual['label']) != canonical_population_label(name):
                coordinates = {}; break
            kind = regional_kind(actual['label'])
            if kind not in {'national', 'province'}:
                coordinates = {}; break
            coordinates[code] = {'code': code, 'label': actual['label'], 'parent': None,
                'kind': kind, 'level': kind, 'ancestors': []}
        if coordinates:
            axes = [{'axis_id': axis_id, 'axis_name': '행정구역', 'coordinates': list(coordinates.values())}]
            matches[repr(axes)] = axes
    return {'contract_version': 'stat-regional-coordinates/v1', 'basis': 'verified_curation_region_scheme',
        'org_id': table['org_id'], 'table_id': table['table_id'], 'axes': next(iter(matches.values())),
        'all_observations_matched': True} if len(matches) == 1 else None
