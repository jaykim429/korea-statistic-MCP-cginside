"""Official flat branch groups on mixed classification axes. No ID conventions."""
import json
import re
from pathlib import Path
from .rules import regional_kind

RULES = json.loads((Path(__file__).resolve().parent.parent / 'rules' / 'branch-labels.json').read_text(encoding='utf-8'))

def derive_axis_branches(axis_id, nodes):
    by_code = {node['code']: node for node in nodes}
    if len(by_code) != len(nodes) or any(not n.get('code') or not n.get('label') or n.get('parent') and n['parent'] not in by_code for n in nodes):
        return []
    for node in nodes:
        seen, parent = {node['code']}, node.get('parent')
        while parent:
            if parent in seen:
                return []
            seen.add(parent); parent = by_code[parent].get('parent')
    result = []
    for root in nodes:
        rule = next((r for r in RULES['dimensions'] if re.search(r['pattern'], re.sub(r'\s+', '', root['label']))), None)
        children = [n for n in nodes if n.get('parent') == root['code']]
        if not rule or len(children) < 2 or any(any(n.get('parent') == c['code'] for n in nodes) for c in children):
            continue
        if rule.get('memberKinds') and any(regional_kind(c['label']) not in rule['memberKinds'] for c in children):
            continue
        result.append({'axis_id': axis_id, 'root_code': root['code'], 'root_label': root['label'], 'parent_code': root.get('parent'),
                       'dimension': rule['dimension'], 'member_codes': [n['code'] for n in children],
                       **({'grain': rule['grain']} if rule.get('grain') else {})})
    return result

def metadata_branches(axes):
    return [branch for axis_id, axis in axes.items() if axis_id != 'ITEM'
            for branch in derive_axis_branches(axis_id, [{'code': code, 'label': str(meta.get('label') or ''), 'parent': meta.get('parent')}
                                                        for code, meta in (axis.get('items') or {}).items()])]
