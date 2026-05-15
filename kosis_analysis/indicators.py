"""Indicator computation operations for the KOSIS MCP server.

The compute_indicator MCP tool dispatches to operations defined here. Each
operation receives normalized query_table rows (shape: {period, value, unit,
dimensions, raw}) and returns a ComputationOutcome describing the computed
values plus any unmatched / invalid inputs.

Design choices (kept intentionally narrow):
  - The module performs arithmetic and structural validation only. Unit
    conversion, group additivity, and indicator semantics are caller (LLM)
    responsibility; the response surfaces caller_asserted markers when the
    operation depends on those assumptions.
  - No domain-specific lookup tables (no region groups, no industry maps,
    no unit dictionaries). Periods are sorted lexicographically because the
    KOSIS PRD_DE format is already sort-stable across Y/H/Q/M cadences.
  - Operations are classes registered in OPERATIONS; adding a new enum
    member only requires a new subclass.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence


def _to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text in {"-", ".", "..", "...", "NA", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


_YEAR_PREFIX_RE = re.compile(r"^(\d{4})")


def _year_of_period(period: Any) -> Optional[int]:
    match = _YEAR_PREFIX_RE.match(str(period or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _yoy_lag_key(period: Any) -> Optional[str]:
    """Return the period one year earlier with the same intra-year suffix."""
    text = str(period or "")
    if len(text) < 4:
        return None
    try:
        prev_year = int(text[:4]) - 1
    except ValueError:
        return None
    return f"{prev_year:04d}{text[4:]}"


def _row_dimensions(row: dict[str, Any]) -> dict[str, Any]:
    dims = row.get("dimensions")
    return dims if isinstance(dims, dict) else {}


def _dimension_codes(row: dict[str, Any], axes: Sequence[str]) -> tuple[str, ...]:
    dims = _row_dimensions(row)
    return tuple(str((dims.get(axis) or {}).get("code") or "") for axis in axes)


def _non_item_axes(row: dict[str, Any]) -> list[str]:
    return [axis for axis in _row_dimensions(row).keys() if axis != "ITEM"]


def _row_label(row: dict[str, Any]) -> Optional[str]:
    for axis, meta in _row_dimensions(row).items():
        if axis == "ITEM":
            continue
        label = (meta or {}).get("label")
        if label:
            return str(label)
    return None


def _row_unit(row: dict[str, Any]) -> Optional[str]:
    unit = row.get("unit")
    if unit:
        return str(unit)
    item = _row_dimensions(row).get("ITEM") or {}
    return str(item.get("unit")) if item.get("unit") else None


def _row_source_system(row: dict[str, Any]) -> Optional[str]:
    value = row.get("source_system") or row.get("provider")
    if value:
        return str(value)
    raw = row.get("raw")
    if isinstance(raw, dict):
        value = raw.get("source_system") or raw.get("provider")
        if value:
            return str(value)
    return None


def _profile_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    units = sorted({unit for row in rows if (unit := _row_unit(row))})
    sources = sorted({source for row in rows if (source := _row_source_system(row))})
    table_ids = sorted({
        str(value)
        for row in rows
        for value in (
            row.get("tbl_id"),
            row.get("table_id"),
            row.get("statbl_id"),
            (row.get("raw") or {}).get("TBL_ID") if isinstance(row.get("raw"), dict) else None,
            (row.get("raw") or {}).get("STATBL_ID") if isinstance(row.get("raw"), dict) else None,
        )
        if value
    })
    return {
        "row_count": len(rows),
        "units": units,
        "distinct_unit_count": len(units),
        "source_systems": sources,
        "mixed_source_systems": len(sources) > 1,
        "table_ids": table_ids,
    }


def _unit_mismatch_profile(rows: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
    profile = _profile_rows(rows)
    if profile["distinct_unit_count"] <= 1:
        return None
    profile.update({
        "requires_unit_conversion": True,
        "reason": "distinct_units_in_additive_group",
    })
    return profile


def _normalize_match_keys(
    rows: Sequence[dict[str, Any]],
    explicit: Optional[Sequence[str]],
) -> list[str]:
    if explicit:
        return [str(key) for key in explicit if str(key or "").strip()]
    for row in rows:
        axes = _non_item_axes(row)
        if axes:
            return axes
    return []


@dataclass
class IndicatorResult:
    value: Optional[float]
    period: Optional[str] = None
    label: Optional[str] = None
    unit: Optional[str] = None
    unit_raw: Optional[str] = None
    unit_denominator: Optional[str] = None
    unit_resolved: Optional[str] = None
    unit_caller_should_label: Optional[str] = None
    unit_transformation: Optional[dict[str, Any]] = None
    dimensions: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    note: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "value": self.value,
            "period": self.period,
            "label": self.label,
            "unit": self.unit,
            "dimensions": self.dimensions,
            "inputs": self.inputs,
        }
        if self.unit_raw is not None:
            payload["unit_raw"] = self.unit_raw
        if self.unit_denominator is not None:
            payload["unit_denominator"] = self.unit_denominator
        if self.unit_resolved is not None or self.unit_transformation is not None:
            payload["unit_resolved"] = self.unit_resolved
        if self.unit_caller_should_label is not None or self.unit_transformation is not None:
            payload["unit_caller_should_label"] = self.unit_caller_should_label
        if self.unit_transformation is not None:
            payload["unit_transformation"] = self.unit_transformation
        if self.note:
            payload["note"] = self.note
        return payload


@dataclass
class ComputationOutcome:
    status: str  # "ok" | "partial" | "invalid_input"
    results: list[IndicatorResult] = field(default_factory=list)
    unmatched: list[dict[str, Any]] = field(default_factory=list)
    validation_errors: list[str] = field(default_factory=list)
    markers: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


def _round(value: Optional[float], decimals: int) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), int(decimals))
    except (TypeError, ValueError):
        return None


class IndicatorOperation:
    """Base class for compute_indicator operations.

    Subclasses override compute(). The dispatcher passes the same keyword
    surface to every operation; each one is responsible for ignoring
    arguments it does not use.
    """

    name: str = ""
    requires_denominator: bool = False
    aggregation_caller_asserted: bool = False
    description: str = ""
    aliases_ko: tuple[str, ...] = ()

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        denominator_rows: Optional[list[dict[str, Any]]] = None,
        match_keys: Optional[Sequence[str]] = None,
        group_by: Optional[Sequence[str]] = None,
        scale_factor: Optional[float] = None,
        decimals: int = 4,
    ) -> ComputationOutcome:
        raise NotImplementedError


class _PairOverPeriodOperation(IndicatorOperation):
    """Shared helper for growth_rate / cagr / yoy_*: scan groups across time."""

    def _groups(
        self,
        rows: list[dict[str, Any]],
        group_by: Optional[Sequence[str]],
    ) -> dict[tuple[str, ...], list[dict[str, Any]]]:
        axes = list(group_by) if group_by else _normalize_match_keys(rows, None)
        groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = _dimension_codes(row, axes) if axes else ()
            groups[key].append(row)
        return groups


class GrowthRate(_PairOverPeriodOperation):
    name = "growth_rate"
    description = "Period-over-period percent change within each group (sorted by period)."
    aliases_ko = ("변화율", "증감률", "성장률", "증가율", "감소율")

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        group_by: Optional[Sequence[str]] = None,
        decimals: int = 4,
        **_: Any,
    ) -> ComputationOutcome:
        outcome = ComputationOutcome(status="ok")
        for key, group_rows in self._groups(rows, group_by).items():
            ordered = sorted(group_rows, key=lambda r: str(r.get("period") or ""))
            if len(ordered) < 2:
                outcome.unmatched.append({
                    "reason": "insufficient_periods",
                    "group_codes": list(key),
                    "row_count": len(ordered),
                })
                outcome.validation_errors.append(
                    "growth_rate requires at least 2 rows per group."
                )
                if "insufficient_periods" not in outcome.markers:
                    outcome.markers.append("insufficient_periods")
                continue
            for i in range(1, len(ordered)):
                prev_row = ordered[i - 1]
                cur_row = ordered[i]
                prev_v = _to_number(prev_row.get("value"))
                cur_v = _to_number(cur_row.get("value"))
                prev_p = str(prev_row.get("period") or "")
                cur_p = str(cur_row.get("period") or "")
                if prev_v is None or cur_v is None:
                    outcome.unmatched.append({
                        "period": cur_p,
                        "previous_period": prev_p,
                        "reason": "non_numeric_value",
                    })
                    continue
                if prev_v == 0:
                    outcome.unmatched.append({
                        "period": cur_p,
                        "previous_period": prev_p,
                        "reason": "previous_zero",
                    })
                    if "denominator_zero" not in outcome.markers:
                        outcome.markers.append("denominator_zero")
                    continue
                value = (cur_v - prev_v) / prev_v * 100
                inputs = {"current": cur_v, "previous": prev_v, "previous_period": prev_p}
                note = None
                if prev_v < 0:
                    inputs["negative_base"] = True
                    note = (
                        "Percent change uses a negative base. For deficits, losses, or balances, "
                        "the sign can be counterintuitive; compare yoy_diff/absolute change."
                    )
                    if "negative_base_growth_rate" not in outcome.markers:
                        outcome.markers.append("negative_base_growth_rate")
                    outcome.extra.setdefault("calculation_alternatives", [
                        {
                            "operation": "yoy_diff",
                            "reason": "Absolute change is often easier to interpret when the base value is negative.",
                        }
                    ])
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=cur_p,
                    label=_row_label(cur_row),
                    unit="%",
                    dimensions=_row_dimensions(cur_row),
                    inputs=inputs,
                    note=note,
                ))
        outcome.status = _final_status(outcome)
        return outcome


class Cagr(_PairOverPeriodOperation):
    name = "cagr"
    description = "Compound annual growth rate between the earliest and latest period per group."
    aliases_ko = ("연평균성장률", "연평균증가율")

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        group_by: Optional[Sequence[str]] = None,
        decimals: int = 4,
        **_: Any,
    ) -> ComputationOutcome:
        outcome = ComputationOutcome(status="ok")
        for key, group_rows in self._groups(rows, group_by).items():
            ordered = sorted(group_rows, key=lambda r: str(r.get("period") or ""))
            if len(ordered) < 2:
                outcome.unmatched.append({"reason": "insufficient_periods", "group_codes": list(key)})
                continue
            start_row, end_row = ordered[0], ordered[-1]
            start_v = _to_number(start_row.get("value"))
            end_v = _to_number(end_row.get("value"))
            start_year = _year_of_period(start_row.get("period"))
            end_year = _year_of_period(end_row.get("period"))
            if start_v is None or end_v is None:
                outcome.unmatched.append({"reason": "non_numeric_value", "group_codes": list(key)})
                continue
            if start_year is None or end_year is None or end_year == start_year:
                outcome.unmatched.append({"reason": "indeterminate_year_span", "group_codes": list(key)})
                continue
            if start_v <= 0 or end_v <= 0:
                outcome.unmatched.append({"reason": "non_positive_value", "group_codes": list(key)})
                continue
            years = end_year - start_year
            value = ((end_v / start_v) ** (1.0 / years) - 1.0) * 100
            outcome.results.append(IndicatorResult(
                value=_round(value, decimals),
                period=str(end_row.get("period") or ""),
                label=_row_label(end_row),
                unit="%",
                dimensions=_row_dimensions(end_row),
                inputs={
                    "start_value": start_v,
                    "end_value": end_v,
                    "start_period": str(start_row.get("period") or ""),
                    "end_period": str(end_row.get("period") or ""),
                    "years": years,
                },
            ))
        outcome.status = _final_status(outcome)
        return outcome


class _YearOverYear(_PairOverPeriodOperation):
    mode: str = "pct"

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        group_by: Optional[Sequence[str]] = None,
        decimals: int = 4,
        **_: Any,
    ) -> ComputationOutcome:
        outcome = ComputationOutcome(status="ok")
        for key, group_rows in self._groups(rows, group_by).items():
            by_period: dict[str, dict[str, Any]] = {}
            for row in group_rows:
                period = str(row.get("period") or "")
                if period:
                    by_period[period] = row
            for period, row in by_period.items():
                lag_key = _yoy_lag_key(period)
                lag_row = by_period.get(lag_key) if lag_key else None
                if lag_row is None:
                    outcome.unmatched.append({"period": period, "reason": "no_yoy_match"})
                    continue
                cur_v = _to_number(row.get("value"))
                prev_v = _to_number(lag_row.get("value"))
                if cur_v is None or prev_v is None:
                    outcome.unmatched.append({"period": period, "reason": "non_numeric_value"})
                    continue
                if self.mode == "pct":
                    if prev_v == 0:
                        outcome.unmatched.append({"period": period, "reason": "previous_zero"})
                        if "denominator_zero" not in outcome.markers:
                            outcome.markers.append("denominator_zero")
                        continue
                    value = (cur_v - prev_v) / prev_v * 100
                    unit = "%"
                    inputs = {"current": cur_v, "previous": prev_v, "previous_period": lag_key}
                    note = None
                    if prev_v < 0:
                        inputs["negative_base"] = True
                        note = (
                            "Year-over-year percent change uses a negative base. For deficits, losses, "
                            "or balances, yoy_diff/absolute change may be less misleading."
                        )
                        if "negative_base_growth_rate" not in outcome.markers:
                            outcome.markers.append("negative_base_growth_rate")
                        outcome.extra.setdefault("calculation_alternatives", [
                            {
                                "operation": "yoy_diff",
                                "reason": "Absolute change is often easier to interpret when the previous value is negative.",
                            }
                        ])
                else:
                    value = cur_v - prev_v
                    unit = _row_unit(row)
                    inputs = {"current": cur_v, "previous": prev_v, "previous_period": lag_key}
                    note = None
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=period,
                    label=_row_label(row),
                    unit=unit,
                    dimensions=_row_dimensions(row),
                    inputs=inputs,
                    note=note,
                ))
        outcome.status = _final_status(outcome)
        return outcome


class YoyPct(_YearOverYear):
    name = "yoy_pct"
    mode = "pct"
    description = "Year-over-year percent change at matching intra-year periods."
    aliases_ko = ("전년대비", "전년대비변화율", "전년동기대비", "전년동기대비변화율")


class YoyDiff(_YearOverYear):
    name = "yoy_diff"
    mode = "diff"
    description = "Year-over-year absolute change at matching intra-year periods."
    aliases_ko = ("전년대비차이", "전년대비증감", "전년동기대비증감", "증감")


class _RatioLike(IndicatorOperation):
    """Numerator / denominator matched by key tuples. Scale and unit are caller-driven."""

    requires_denominator = False  # Share can fall back to numerator total
    output_unit: Optional[str] = None
    multiply_by: float = 1.0

    def _unit_payload(
        self,
        numerator_row: dict[str, Any],
        denominator_row: Optional[dict[str, Any]],
        *,
        effective_scale: float,
        denominator_source: str,
    ) -> dict[str, Any]:
        numerator_unit = _row_unit(numerator_row)
        denominator_unit = _row_unit(denominator_row or {}) if denominator_row is not None else numerator_unit
        if self.name == "share":
            formula = "(numerator / denominator) * 100"
            expression = f"({numerator_unit or 'unknown'} / {denominator_unit or 'unknown'}) * 100"
            unit_resolved = "%"
            caller_must_resolve = False
        else:
            formula = "(numerator * scale_factor) / denominator"
            expression = (
                f"({numerator_unit or 'unknown'} * {effective_scale:g})"
                f" / {denominator_unit or 'unknown'}"
            )
            unit_resolved = None
            caller_must_resolve = True
        return {
            "unit": unit_resolved,
            "unit_raw": numerator_unit,
            "unit_denominator": denominator_unit,
            "unit_resolved": unit_resolved,
            "unit_caller_should_label": None if caller_must_resolve else unit_resolved,
            "unit_transformation": {
                "numerator_unit": numerator_unit,
                "denominator_unit": denominator_unit,
                "scale_factor": effective_scale,
                "formula": formula,
                "expression": expression,
                "denominator_source": denominator_source,
                "caller_must_resolve": caller_must_resolve,
            },
        }

    def _denominator_index(
        self,
        rows: list[dict[str, Any]],
        match_keys: list[str],
    ) -> tuple[dict[tuple, dict[str, Any]], list[dict[str, Any]], set[tuple]]:
        index: dict[tuple, dict[str, Any]] = {}
        duplicates: list[dict[str, Any]] = []
        duplicate_keys: set[tuple] = set()
        for row in rows:
            key = (str(row.get("period") or ""), _dimension_codes(row, match_keys))
            if key in index:
                duplicate_keys.add(key)
                duplicates.append({
                    "period": key[0],
                    "group_codes": list(key[1]),
                    "reason": "duplicate_denominator_key",
                })
                continue
            index[key] = row
        return index, duplicates, duplicate_keys

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        denominator_rows: Optional[list[dict[str, Any]]] = None,
        match_keys: Optional[Sequence[str]] = None,
        scale_factor: Optional[float] = None,
        decimals: int = 4,
        **_: Any,
    ) -> ComputationOutcome:
        outcome = ComputationOutcome(status="ok")
        effective_scale = float(scale_factor) if scale_factor is not None else self.multiply_by
        axes = _normalize_match_keys(rows, match_keys)

        if denominator_rows:
            index, duplicate_denominators, duplicate_keys = self._denominator_index(denominator_rows, axes)
            if duplicate_denominators:
                outcome.unmatched.extend(duplicate_denominators)
                outcome.validation_errors.append(
                    "denominator_rows contains duplicate rows for the same period and match_keys."
                )
                if "duplicate_denominator_key" not in outcome.markers:
                    outcome.markers.append("duplicate_denominator_key")
            for row in rows:
                period = str(row.get("period") or "")
                key = (period, _dimension_codes(row, axes))
                if key in duplicate_keys:
                    outcome.unmatched.append({
                        "period": period,
                        "group_codes": list(key[1]),
                        "reason": "duplicate_denominator_key",
                    })
                    continue
                denom_row = index.get(key)
                if denom_row is None:
                    outcome.unmatched.append({
                        "period": period,
                        "group_codes": list(key[1]),
                        "reason": "no_denominator_match",
                    })
                    continue
                num = _to_number(row.get("value"))
                den = _to_number(denom_row.get("value"))
                if num is None or den is None:
                    outcome.unmatched.append({"period": period, "reason": "non_numeric_value"})
                    continue
                if den == 0:
                    outcome.unmatched.append({"period": period, "reason": "denominator_zero"})
                    if "denominator_zero" not in outcome.markers:
                        outcome.markers.append("denominator_zero")
                    continue
                if self.name == "share":
                    numerator_unit = _row_unit(row)
                    denominator_unit = _row_unit(denom_row)
                    if numerator_unit and denominator_unit and numerator_unit != denominator_unit:
                        outcome.unmatched.append({
                            "period": period,
                            "group_codes": list(key[1]),
                            "reason": "unit_mismatch",
                            "numerator_unit": numerator_unit,
                            "denominator_unit": denominator_unit,
                        })
                        if "unit_mismatch" not in outcome.markers:
                            outcome.markers.append("unit_mismatch")
                        if "unit_conversion_required" not in outcome.markers:
                            outcome.markers.append("unit_conversion_required")
                        outcome.extra["unit_profile"] = {
                            "numerator": _profile_rows([row]),
                            "denominator": _profile_rows([denom_row]),
                        }
                        continue
                value = num / den * effective_scale
                unit_payload = self._unit_payload(
                    row,
                    denom_row,
                    effective_scale=effective_scale,
                    denominator_source="external_denominator_rows",
                )
                if unit_payload["unit_transformation"]["caller_must_resolve"]:
                    if "unit_caller_resolution_required" not in outcome.markers:
                        outcome.markers.append("unit_caller_resolution_required")
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=period,
                    label=_row_label(row),
                    unit=unit_payload["unit"],
                    unit_raw=unit_payload["unit_raw"],
                    unit_denominator=unit_payload["unit_denominator"],
                    unit_resolved=unit_payload["unit_resolved"],
                    unit_caller_should_label=unit_payload["unit_caller_should_label"],
                    unit_transformation=unit_payload["unit_transformation"],
                    dimensions=_row_dimensions(row),
                    inputs={
                        "numerator": num,
                        "denominator": den,
                        "denominator_source": "external_denominator_rows",
                        "denominator_label": _row_label(denom_row),
                        "scale_factor": effective_scale,
                        "additivity_caller_asserted": False,
                    },
                ))
        elif self.name == "share":
            # Share without an explicit denominator divides by structurally comparable input totals.
            usable: list[tuple[float, dict[str, Any]]] = []
            totals_by_period: dict[str, float] = defaultdict(float)
            rows_by_period: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                v = _to_number(row.get("value"))
                if v is None:
                    outcome.unmatched.append({
                        "period": str(row.get("period") or ""),
                        "reason": "non_numeric_value",
                    })
                    continue
                period = str(row.get("period") or "")
                totals_by_period[period] += v
                rows_by_period[period].append(row)
                usable.append((v, row))
            if not usable:
                outcome.status = "invalid_input"
                outcome.validation_errors.append("share requires at least one numeric input row.")
                outcome.markers.append("invalid_input")
                return outcome
            unit_mismatches = {
                period: profile
                for period, period_rows in rows_by_period.items()
                if (profile := _unit_mismatch_profile(period_rows))
            }
            if unit_mismatches:
                outcome.status = "invalid_input"
                outcome.validation_errors.append(
                    "share with input_rows total requires comparable units within each denominator group."
                )
                outcome.markers.extend(["unit_mismatch", "unit_conversion_required", "invalid_input"])
                outcome.extra["unit_profile"] = _profile_rows(rows)
                outcome.extra["unit_mismatches_by_period"] = unit_mismatches
                return outcome
            zero_periods = [period for period, total in totals_by_period.items() if total == 0]
            if len(zero_periods) == len(totals_by_period):
                outcome.markers.append("denominator_zero")
                outcome.status = "invalid_input"
                outcome.validation_errors.append("share total is zero; cannot divide.")
                return outcome
            periods = sorted(totals_by_period.keys())
            denominator_grouping = "period" if len(periods) > 1 else "all_input_rows"
            if denominator_grouping == "period":
                outcome.markers.append("share_total_grouped_by_period")
            for v, row in usable:
                period = str(row.get("period") or "")
                total = totals_by_period.get(period, 0.0)
                if total == 0:
                    outcome.unmatched.append({
                        "period": period,
                        "reason": "denominator_zero",
                    })
                    if "denominator_zero" not in outcome.markers:
                        outcome.markers.append("denominator_zero")
                    continue
                value = v / total * 100
                unit_payload = self._unit_payload(
                    row,
                    row,
                    effective_scale=100.0,
                    denominator_source="input_rows_total_sum",
                )
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=str(row.get("period") or ""),
                    label=_row_label(row),
                    unit=unit_payload["unit"],
                    unit_raw=unit_payload["unit_raw"],
                    unit_denominator=unit_payload["unit_denominator"],
                    unit_resolved=unit_payload["unit_resolved"],
                    unit_caller_should_label=unit_payload["unit_caller_should_label"],
                    unit_transformation=unit_payload["unit_transformation"],
                    dimensions=_row_dimensions(row),
                    inputs={
                        "numerator": v,
                        "denominator": total,
                        "denominator_total": total,
                        "denominator_source": "input_rows_total_sum",
                        "denominator_label": "input_rows_total_sum",
                        "denominator_grouping": denominator_grouping,
                        "denominator_group_key": period if denominator_grouping == "period" else None,
                        "additivity_caller_asserted": True,
                    },
                ))
            outcome.markers.append("share_total_from_input_rows")
            outcome.extra["share_denominator_grouping"] = {
                "grouping": denominator_grouping,
                "periods": periods,
                "totals_by_period": dict(totals_by_period),
            }
        else:
            outcome.status = "invalid_input"
            outcome.validation_errors.append(
                f"{self.name} requires denominator_rows."
            )
            outcome.markers.append("invalid_input")
            return outcome

        outcome.status = _final_status(outcome)
        return outcome


class Share(_RatioLike):
    name = "share"
    output_unit = "%"
    multiply_by = 100.0
    description = "Percent share against denominator_rows (or the input_rows total if omitted)."
    aliases_ko = ("비중", "구성비", "점유율")


class PerCapita(_RatioLike):
    name = "per_capita"
    requires_denominator = True
    description = "Numerator divided by a per-capita denominator (e.g., population). scale_factor controls units."
    aliases_ko = ("1인당", "인당", "인구당")


class Ratio(_RatioLike):
    name = "ratio"
    requires_denominator = True
    description = "Plain A/B ratio matched by period and dimension codes."
    aliases_ko = ("비율", "배율")


class SumAdditiveRows(IndicatorOperation):
    name = "sum_additive_rows"
    aggregation_caller_asserted = True
    description = (
        "Sum row values within caller-defined groups. Caller asserts the rows are additive "
        "(do not use for rates, ratios, indices, or averages)."
    )
    aliases_ko = ("합계", "합산", "총합")

    def compute(
        self,
        rows: list[dict[str, Any]],
        *,
        group_by: Optional[Sequence[str]] = None,
        decimals: int = 4,
        **_: Any,
    ) -> ComputationOutcome:
        outcome = ComputationOutcome(status="ok")
        axes = list(group_by) if group_by else []
        groups: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            period = str(row.get("period") or "")
            key = (period, _dimension_codes(row, axes))
            bucket = groups.setdefault(key, {
                "period": period,
                "total": 0.0,
                "count": 0,
                "sample": row,
                "unit": _row_unit(row),
                "rows": [],
            })
            bucket["rows"].append(row)
            v = _to_number(row.get("value"))
            if v is None:
                outcome.unmatched.append({"period": period, "reason": "non_numeric_value"})
                continue
            bucket["total"] += v
            bucket["count"] += 1
        unit_mismatches: list[dict[str, Any]] = []
        for (period, codes), bucket in groups.items():
            mismatch = _unit_mismatch_profile(bucket["rows"])
            if not mismatch:
                continue
            mismatch.update({
                "period": period,
                "group_codes": list(codes),
            })
            unit_mismatches.append(mismatch)
        if unit_mismatches:
            outcome.status = "invalid_input"
            outcome.validation_errors.append(
                "sum_additive_rows requires all rows in each additive group to use the same unit."
            )
            outcome.markers.extend(["unit_mismatch", "unit_conversion_required", "invalid_input"])
            outcome.extra["unit_profile"] = _profile_rows(rows)
            outcome.extra["unit_mismatches"] = unit_mismatches
            return outcome
        for (period, codes), bucket in groups.items():
            if bucket["count"] == 0:
                continue
            sample = bucket["sample"]
            sample_dims = _row_dimensions(sample)
            projected_dims = {axis: sample_dims.get(axis) for axis in axes if axis in sample_dims}
            outcome.results.append(IndicatorResult(
                value=_round(bucket["total"], decimals),
                period=period,
                label=_row_label(sample),
                unit=bucket["unit"],
                dimensions=projected_dims or sample_dims,
                inputs={"row_count": bucket["count"], "group_codes": list(codes)},
                note="Caller asserted additivity; do not use this for rates/indices/averages.",
            ))
        outcome.markers.append("additivity_caller_asserted")
        outcome.status = _final_status(outcome)
        return outcome


def _final_status(outcome: ComputationOutcome) -> str:
    if outcome.results and outcome.unmatched:
        return "partial"
    if outcome.results:
        return "ok"
    return "invalid_input"


OPERATIONS: dict[str, IndicatorOperation] = {
    op.name: op for op in [
        GrowthRate(),
        Cagr(),
        YoyPct(),
        YoyDiff(),
        Share(),
        PerCapita(),
        Ratio(),
        SumAdditiveRows(),
    ]
}

OPERATION_ALIASES_KO: dict[str, str] = {
    alias: op.name
    for op in OPERATIONS.values()
    for alias in op.aliases_ko
}


def operation_catalog() -> list[dict[str, Any]]:
    """Public catalog of operations for tool discovery (LLM / docs)."""
    return [
        {
            "name": op.name,
            "description": op.description,
            "aliases_ko": list(op.aliases_ko),
            "requires_denominator_rows": op.requires_denominator,
            "aggregation_caller_asserted": op.aggregation_caller_asserted,
        }
        for op in OPERATIONS.values()
    ]
