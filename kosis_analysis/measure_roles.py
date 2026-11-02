"""Official measure provenance when ITEM is a population, not a measurement.

Never infer a measure from the request or currency alone. Only an unambiguous
definition heading in CMMT, corroborated by UNIT, can fill an unknown ITEM role.
"""
from __future__ import annotations

import re
from typing import Any

from kosis_analysis.rules import measure_of, is_measurement_axis


def _unit_kind(unit: str) -> str | None:
    if re.search(r"%|퍼센트|백분율", unit):
        return "percent"
    if re.search(r"원|달러|USD|KRW|엔|유로", unit, re.I):
        return "money"
    if re.fullmatch(r"(?:천|만|백만)?\s*(?:개|명|건|가구|호|개소|대)", unit):
        return "count"
    return None


def annotation_measure(comments: list[dict], units: list[dict]) -> dict[str, Any] | None:
    definitions = []
    for row in comments:
        # A definition heading is distinct from a mention inside a classification
        # criterion (e.g. enterprise size classified using export/sales amounts).
        text = str(row.get("CMMT_DC") or "").strip()
        heading = re.match(r"^(?:\d+[.)]\s*)?([가-힣\s]+?)\s*(?:기준|정의)\s*[:：]", text)
        if not heading:
            continue
        label = heading.group(1).strip()
        measure = measure_of(label)
        if measure and re.sub(r"\s+", "", label) == measure:
            definitions.append((measure, text))
    measures = {entry[0] for entry in definitions}
    if len(measures) != 1:
        return None
    measure = next(iter(measures))
    names = list(dict.fromkeys(str(row.get("UNIT_NM") or "").strip() for row in units if row.get("UNIT_NM")))
    expected = "percent" if re.search(r"비중|비율|률$", measure) else "money" if re.search(r"액$|금액|이익|비용|소득|임금", measure) else "count" if measure.endswith("수") else None
    if not expected or not names or any(_unit_kind(unit) != expected for unit in names):
        return None
    return {"measure": measure, "name": measure, "source": "CMMT", "definition": definitions[0][1], "units": names}


def apply_annotation_measure(payload: dict, definition: dict | None) -> None:
    rows = payload.get("rows") or []
    if not definition or not rows or payload.get("actual_measure"):
        return
    for row in rows:
        label = ((row.get("dimensions") or {}).get("ITEM") or {}).get("label")
        if measure_of(label) or row.get("unit") not in definition["units"]:
            return
    payload.update(actual_measure=definition["measure"], actual_measure_evidence="official_annotation_definition",
                   measure_definition=definition["definition"], measure_role_evidence=definition)


def apply_classification_measure(payload: dict, axes: dict) -> None:
    """Bind a unique official metric-axis leaf to every freshly executed row.

    Broad ITEM families do not hide a measured classification. A population or
    mixed tree is not a measurement axis merely because a child has currency.
    No table IDs, request terms or bare unit guesses participate in this proof.
    """
    rows = payload.get('rows') or []
    if not rows or payload.get('aggregation', 'none') != 'none' or payload.get('actual_measure'):
        return
    item_codes = set()
    for row in rows:
        item = (row.get('dimensions') or {}).get('ITEM') or {}
        meta = ((axes.get('ITEM') or {}).get('items') or {}).get(str(item.get('code') or ''))
        if not meta or item.get('label') != meta.get('label') or measure_of(meta.get('label')):
            return  # Real ITEM meaning always wins.
        item_codes.add(str(item['code']))
    matches = []
    for axis_id, axis in axes.items():
        if axis_id == 'ITEM' or not is_measurement_axis(axis.get('OBJ_NM')):
            continue
        selected = []
        for row in rows:
            coordinate = (row.get('dimensions') or {}).get(axis_id) or {}
            code = str(coordinate.get('code') or '')
            meta = (axis.get('items') or {}).get(code)
            if not meta or not meta.get('unit') or row.get('unit') != meta['unit'] or coordinate.get('label') != meta.get('label') or not measure_of(meta.get('label')):
                break
            selected.append({'code': code, 'label': meta['label'], 'unit': meta['unit']})
        else:
            # Multiple named metrics remain separate observations, not one identity.
            distinct = {tuple(item.items()) for item in selected}
            if len(distinct) == 1:
                matches.append((axis_id, axis['OBJ_NM'], selected[0]))
    if len(matches) != 1 or not payload.get('org_id') or not payload.get('tbl_id'):
        return
    axis_id, axis_name, selected = matches[0]
    payload.update(actual_measure=selected['label'], actual_measure_evidence='official_measurement_coordinate',
        measure_role_evidence={'contract_version': 'kosis-measure-coordinate/v1', 'source': 'ITM',
            'org_id': payload['org_id'], 'tbl_id': payload['tbl_id'], 'axis_id': axis_id, 'axis_name': axis_name,
            'measure': selected['label'], 'items': [selected], 'item_codes': sorted(item_codes),
            'all_observations_matched': True})


def apply_title_total_measure(payload: dict, axes: dict) -> None:
    """Bind the table title's single count measure to the total row of a classification ITEM.

    「농가수(시도/시/군/구)」 puts the measure only in the title; ITEM is 영농형태별
    (계·논벼·과수…). The total row of that classification measures the title's
    농가수. Never when any ITEM is itself a measure, the selected ITEM is not the
    sole root total, a non-ITEM axis is a measurement axis, a CMMT definition exists,
    or the observed unit is not a count unit for that measure.
    """
    from kosis_analysis.rules import count_measure_unit_matches, is_total_label, title_total_measure
    from kosis_analysis.units import is_ambiguous_unit
    rows = payload.get('rows') or []
    if (not rows or payload.get('aggregation', 'none') != 'none' or payload.get('actual_measure')
            or payload.get('measure_role_evidence')):
        return
    org_id, tbl_id, title = payload.get('org_id'), payload.get('tbl_id'), payload.get('table_name')
    measure = title_total_measure(title)
    items = ((axes or {}).get('ITEM') or {}).get('items') or {}
    if not org_id or not tbl_id or not measure or not items or len(items) > 1000:
        return
    if any(measure_of(meta.get('label')) for meta in items.values()):
        return  # A measure-like ITEM makes ITEM the measurement axis.
    if any(is_measurement_axis(axis.get('OBJ_NM')) for axis_id, axis in axes.items() if axis_id != 'ITEM'):
        return
    totals = [code for code, meta in items.items() if is_total_label(meta.get('label')) and not meta.get('parent')]
    if len(totals) != 1:
        return
    total = totals[0]
    total_label = items[total].get('label')
    units = set()
    for row in rows:
        dimensions = row.get('dimensions') if isinstance(row.get('dimensions'), dict) else {}
        item = dimensions.get('ITEM') if isinstance(dimensions.get('ITEM'), dict) else {}
        if str(item.get('code') or '') != total or item.get('label') != total_label:
            return  # A non-total row is a member of the classification, not the title measure.
        unit = str(row.get('unit') or '').strip()
        if not unit or is_ambiguous_unit(unit) or not count_measure_unit_matches(measure, unit):
            return
        if any(isinstance(dim, dict) and measure_of(dim.get('label')) for axis, dim in dimensions.items() if axis != 'ITEM'):
            return
        units.add(unit)
    payload.update(actual_measure=measure, actual_measure_evidence='official_title_total_item',
        measure_role_evidence={'contract_version': 'kosis-title-measure/v1', 'source': 'TBL',
            'org_id': org_id, 'tbl_id': tbl_id, 'table_name': title, 'measure': measure,
            'item_axis': 'ITEM', 'item_code': total, 'item_label': total_label,
            'item_labels': [meta.get('label') for meta in items.values()],
            'units': sorted(units), 'all_observations_matched': True})
