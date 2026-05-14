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
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=cur_p,
                    label=_row_label(cur_row),
                    unit="%",
                    dimensions=_row_dimensions(cur_row),
                    inputs={"current": cur_v, "previous": prev_v, "previous_period": prev_p},
                ))
        outcome.status = _final_status(outcome)
        return outcome


class Cagr(_PairOverPeriodOperation):
    name = "cagr"
    description = "Compound annual growth rate between the earliest and latest period per group."

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
                else:
                    value = cur_v - prev_v
                    unit = _row_unit(row)
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=period,
                    label=_row_label(row),
                    unit=unit,
                    dimensions=_row_dimensions(row),
                    inputs={"current": cur_v, "previous": prev_v, "previous_period": lag_key},
                ))
        outcome.status = _final_status(outcome)
        return outcome


class YoyPct(_YearOverYear):
    name = "yoy_pct"
    mode = "pct"
    description = "Year-over-year percent change at matching intra-year periods."


class YoyDiff(_YearOverYear):
    name = "yoy_diff"
    mode = "diff"
    description = "Year-over-year absolute change at matching intra-year periods."


class _RatioLike(IndicatorOperation):
    """Numerator / denominator matched by key tuples. Scale and unit are caller-driven."""

    requires_denominator = False  # Share can fall back to numerator total
    output_unit: Optional[str] = None
    multiply_by: float = 1.0

    def _denominator_index(
        self,
        rows: list[dict[str, Any]],
        match_keys: list[str],
    ) -> dict[tuple, dict[str, Any]]:
        index: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            key = (str(row.get("period") or ""), _dimension_codes(row, match_keys))
            index[key] = row
        return index

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
            index = self._denominator_index(denominator_rows, axes)
            for row in rows:
                period = str(row.get("period") or "")
                key = (period, _dimension_codes(row, axes))
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
                value = num / den * effective_scale
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=period,
                    label=_row_label(row),
                    unit=self.output_unit or _row_unit(row),
                    dimensions=_row_dimensions(row),
                    inputs={
                        "numerator": num,
                        "denominator": den,
                        "denominator_label": _row_label(denom_row),
                        "scale_factor": effective_scale,
                    },
                ))
        elif self.name == "share":
            # Share without an explicit denominator divides by the sum of inputs.
            total = 0.0
            usable: list[tuple[float, dict[str, Any]]] = []
            for row in rows:
                v = _to_number(row.get("value"))
                if v is None:
                    outcome.unmatched.append({
                        "period": str(row.get("period") or ""),
                        "reason": "non_numeric_value",
                    })
                    continue
                total += v
                usable.append((v, row))
            if total == 0:
                outcome.markers.append("denominator_zero")
                outcome.status = "invalid_input"
                outcome.validation_errors.append("share total is zero; cannot divide.")
                return outcome
            for v, row in usable:
                value = v / total * 100
                outcome.results.append(IndicatorResult(
                    value=_round(value, decimals),
                    period=str(row.get("period") or ""),
                    label=_row_label(row),
                    unit="%",
                    dimensions=_row_dimensions(row),
                    inputs={"numerator": v, "denominator_total": total},
                ))
            outcome.markers.append("share_total_from_input_rows")
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


class PerCapita(_RatioLike):
    name = "per_capita"
    requires_denominator = True
    description = "Numerator divided by a per-capita denominator (e.g., population). scale_factor controls units."


class Ratio(_RatioLike):
    name = "ratio"
    requires_denominator = True
    description = "Plain A/B ratio matched by period and dimension codes."


class SumAdditiveRows(IndicatorOperation):
    name = "sum_additive_rows"
    aggregation_caller_asserted = True
    description = (
        "Sum row values within caller-defined groups. Caller asserts the rows are additive "
        "(do not use for rates, ratios, indices, or averages)."
    )

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
            })
            v = _to_number(row.get("value"))
            if v is None:
                outcome.unmatched.append({"period": period, "reason": "non_numeric_value"})
                continue
            bucket["total"] += v
            bucket["count"] += 1
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


def operation_catalog() -> list[dict[str, Any]]:
    """Public catalog of operations for tool discovery (LLM / docs)."""
    return [
        {
            "name": op.name,
            "description": op.description,
            "requires_denominator_rows": op.requires_denominator,
            "aggregation_caller_asserted": op.aggregation_caller_asserted,
        }
        for op in OPERATIONS.values()
    ]
