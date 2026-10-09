"""Official measure provenance when ITEM is a population, not a measurement.

Never infer a measure from the request or currency alone. Only an unambiguous
definition heading in CMMT, corroborated by UNIT, can fill an unknown ITEM role.
"""
from __future__ import annotations

import re
from typing import Any

from kosis_analysis.rules import measure_of


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
