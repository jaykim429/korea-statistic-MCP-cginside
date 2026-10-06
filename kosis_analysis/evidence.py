"""Stable, additive evidence contract for KOSIS MCP tool responses.

Tool payloads grew organically and intentionally keep their legacy Korean and
English fields.  This module does not replace those payloads.  It projects them
into a small common envelope so clients do not have to infer execution success,
semantic fulfillment, or citation completeness from answer prose.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional


CONTRACT_VERSION = "stat-evidence/v1"


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if _present(value):
            return value
    return None


@lru_cache(maxsize=1)
def current_build_commit() -> str:
    """Return the image build commit without making it a runtime dependency."""
    configured = str(os.getenv("MCP_BUILD_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if configured:
        return configured
    candidates = (
        Path("/app/.build-commit"),
        Path(__file__).resolve().parents[1] / ".build-commit",
    )
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            continue
        if value:
            return value
    return "unknown"


def _payload_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("rows", "data", "results", "표", "시계열", "데이터"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _normalize_observation(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    value = _first_present(row, "value", "값", "DT")
    unit = _first_present(row, "unit", "단위", "UNIT_NM")
    period = _first_present(row, "period", "시점", "used_period", "PRD_DE")
    dimensions = dict(row.get("dimensions")) if isinstance(row.get("dimensions"), dict) else {}
    semantic_dimensions = {
        "region": _first_present(row, "region", "지역"),
        "sex": _first_present(row, "gender", "sex", "성별"),
        "age": _first_present(row, "age", "연령"),
        "industry": _first_present(row, "industry", "산업"),
        "enterprise_scale": _first_present(row, "enterprise_scale", "기업규모", "규모"),
    }
    for dimension, label in semantic_dimensions.items():
        if _present(label) and dimension not in dimensions:
            dimensions[dimension] = {"code": None, "label": label}
    if not any((_present(value), _present(unit), _present(period), bool(dimensions))):
        return None
    return {"value": value, "unit": unit, "period": period, "dimensions": dimensions}


def _observations(payload: dict[str, Any]) -> list[dict[str, Any]]:
    default_unit = _first_present(payload, "unit", "단위")
    default_period = _first_present(payload, "period", "시점", "used_period")
    normalized: list[dict[str, Any]] = []
    for row in _payload_rows(payload):
        item = _normalize_observation(row)
        if not item:
            continue
        if not _present(item.get("unit")) and _present(default_unit):
            item["unit"] = default_unit
        if not _present(item.get("period")) and _present(default_period):
            item["period"] = default_period
        normalized.append(item)
    if normalized:
        return normalized
    scalar = _normalize_observation(payload)
    return [scalar] if scalar else []


def _table(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "org_id": _first_present(payload, "org_id", "기관ID"),
        "table_id": _first_present(payload, "table_id", "tbl_id", "통계표ID"),
        "name": _first_present(payload, "table_name", "통계표", "통계표명"),
    }


def _source(payload: dict[str, Any], tool: str) -> dict[str, Any]:
    label = _first_present(payload, "source", "출처")
    if not label and tool in {"quick_stat", "answer_query", "query_table"}:
        label = "통계청 KOSIS"
    url = _first_present(payload, "source_url", "출처_URL")
    if not url and label and "KOSIS" in str(label).upper():
        url = "https://kosis.kr"
    return {"label": label, "url": url}


def _concept_key(dimension: Any, code: Any) -> str:
    return f"{dimension}:{code}"


def _concepts_from_filters(payload: dict[str, Any]) -> list[dict[str, Any]]:
    filters = payload.get("filters_used")
    if not isinstance(filters, dict):
        return []
    concepts: list[dict[str, Any]] = []
    for dimension, raw_codes in filters.items():
        codes = raw_codes if isinstance(raw_codes, list) else [raw_codes]
        for code in codes:
            if _present(code):
                concepts.append({"dimension": str(dimension), "code": str(code)})
    return sorted(concepts, key=lambda item: (item["dimension"], item["code"]))


def _concepts_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        dimensions = row.get("dimensions")
        if not isinstance(dimensions, dict):
            continue
        for dimension, raw_meta in dimensions.items():
            meta = raw_meta if isinstance(raw_meta, dict) else {"label": raw_meta}
            code = meta.get("code")
            label = meta.get("label")
            if not _present(code) and not _present(label):
                continue
            identity = str(code) if _present(code) else f"label:{label}"
            key = (str(dimension), identity)
            unique[key] = {
                "dimension": str(dimension),
                "code": str(code) if _present(code) else None,
                "label": label,
                "verified_in_rows": True,
            }
    return [unique[key] for key in sorted(unique)]


def _normalize_requested_concepts(
    payload: dict[str, Any],
    requested_concepts: Optional[Iterable[Any]],
) -> list[Any]:
    if requested_concepts is not None:
        return list(requested_concepts)
    return _concepts_from_filters(payload)


def _execution_status(payload: dict[str, Any], observations: list[dict[str, Any]]) -> str:
    status = str(_first_present(payload, "execution_status", "status", "상태") or "").lower()
    capability = str(payload.get("capability_state") or "").lower()
    if status in {"failed", "unsupported", "invalid_input", "error"}:
        return "failed"
    if status in {"no_data", "empty"} or capability == "no_data":
        return "no_data"
    if status in {"executed", "partial", "success", "ok"} or capability == "query_executed":
        return "executed"
    if payload.get("actual_query_supported") is True or observations:
        return "executed"
    return "unknown"


def _contract_signals(payload: dict[str, Any]) -> dict[str, Any]:
    contract = payload.get("mcp_output_contract")
    if not isinstance(contract, dict):
        return {}
    signals = contract.get("current_signals")
    return signals if isinstance(signals, dict) else {}


def _markers(payload: dict[str, Any]) -> set[str]:
    signals = _contract_signals(payload)
    return {str(marker).lower() for marker in signals.get("markers_present") or []}


def _failure(payload: dict[str, Any], execution_status: str) -> tuple[Optional[str], bool]:
    if execution_status == "no_data":
        return "no_rows", False
    if execution_status != "failed":
        return None, False

    code = str(_first_present(payload, "code", "코드") or "").upper()
    message = str(_first_present(payload, "error", "오류") or "").lower()
    markers = _markers(payload)
    combined = " ".join((code.lower(), message, " ".join(markers)))
    if any(token in combined for token in ("timeout", "runtime_error", "network", "rate_limit", "connection")):
        return "infrastructure", True
    if "period" in combined and any(token in combined for token in ("not_found", "unavailable", "invalid")):
        return "period_unavailable", False
    if any(token in combined for token in ("invalid_input", "invalid_", "validation", "required", "unsupported")):
        return "invalid_request", False
    if any(token in combined for token in ("partial_coverage", "fanout_call_failed")):
        return "partial_coverage", True
    return "unknown", False


def _missing_evidence_fields(
    execution_status: str,
    observations: list[dict[str, Any]],
    source: dict[str, Any],
    table: dict[str, Any],
) -> list[str]:
    if execution_status != "executed":
        return []
    missing: list[str] = []
    if not observations:
        missing.append("observations")
    else:
        for field in ("value", "unit", "period"):
            if any(not _present(row.get(field)) for row in observations):
                missing.append(field)
    if not _present(source.get("label")):
        missing.append("source")
    if not _present(table.get("org_id")):
        missing.append("table.org_id")
    if not _present(table.get("table_id")):
        missing.append("table.table_id")
    return missing


def _actual_measure(payload: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[Any, Optional[str]]:
    explicit = _first_present(payload, "actual_measure", "실제_측정항목")
    if _present(explicit):
        return explicit, str(payload.get("actual_measure_evidence") or "response_field")
    labels = {
        str((row.get("dimensions") or {}).get("ITEM", {}).get("label"))
        for row in rows
        if isinstance(row.get("dimensions"), dict)
        and isinstance(row["dimensions"].get("ITEM"), dict)
        and _present(row["dimensions"]["ITEM"].get("label"))
    }
    if len(labels) == 1:
        return next(iter(labels)), "ITEM_dimension"
    return None, None


def _period_contract(payload: dict[str, Any], observations: list[dict[str, Any]]) -> dict[str, Any]:
    used_values = list(dict.fromkeys(
        str(row["period"])
        for row in observations
        if _present(row.get("period"))
    ))
    used: Any = used_values[0] if len(used_values) == 1 else used_values
    metadata = payload.get("period_metadata") if isinstance(payload.get("period_metadata"), dict) else {}
    requested = _first_present(payload, "requested_period", "요청_기간", "period_range")
    available = _first_present(payload, "available_period", "가용_기간")
    if not available and metadata:
        start = metadata.get("start_period")
        end = metadata.get("latest_period")
        available = [start, end] if _present(start) or _present(end) else None
    return {
        "requested": requested,
        "used": used or None,
        "available": available,
        "selection_mode": _first_present(payload, "period_selection_mode", "최신값_선택정책"),
        "cadence": _first_present(payload, "period_type", "선택_수록주기") or metadata.get("cadence"),
    }


def build_stat_evidence(
    payload: dict[str, Any],
    *,
    tool: str,
    requested_concepts: Optional[Iterable[Any]] = None,
) -> dict[str, Any]:
    """Project one legacy tool payload into ``StatEvidenceEnvelope v1``."""
    observations = _observations(payload)
    table = _table(payload)
    source = _source(payload, tool)
    execution_status = _execution_status(payload, observations)
    failure_class, retryable = _failure(payload, execution_status)
    missing_fields = _missing_evidence_fields(execution_status, observations, source, table)

    requested = _normalize_requested_concepts(payload, requested_concepts)
    resolved = _concepts_from_rows(observations)
    resolved_keys = {_concept_key(item["dimension"], item["code"]) for item in resolved}
    filter_keys = {
        _concept_key(item.get("dimension"), item.get("code"))
        for item in requested
        if isinstance(item, dict) and _present(item.get("dimension")) and _present(item.get("code"))
    }
    dropped = list(_first_present(payload, "dropped_dimensions", "누락_차원") or [])
    signals = _contract_signals(payload)
    ignored_params = [str(item) for item in signals.get("ignored_params") or []]
    missing_concepts = sorted(
        (filter_keys - resolved_keys)
        | {str(item) for item in dropped}
        | {f"parameter:{item}" for item in ignored_params}
    )

    fanout = payload.get("fanout") if isinstance(payload.get("fanout"), dict) else {}
    markers = _markers(payload)
    semantic_partial = bool(
        dropped
        or ignored_params
        or fanout.get("partial_coverage")
        or markers.intersection({"partial_fanout_coverage", "partial_fulfillment", "dropped_dimensions"})
    )
    if execution_status in {"failed", "no_data"}:
        fulfillment_status = "unavailable"
    elif execution_status == "executed" and not missing_fields and not missing_concepts and not semantic_partial:
        fulfillment_status = "exact"
    elif execution_status == "executed":
        fulfillment_status = "partial"
    else:
        fulfillment_status = "unknown"

    actual_measure, actual_measure_evidence = _actual_measure(payload, observations)
    evidence = {
        "observations": observations,
        "source": source,
        "table": table,
        "actual_measure": actual_measure,
        "actual_measure_evidence": actual_measure_evidence,
        "measure_basis": _first_present(payload, "measure_basis", "측정_기준"),
        "period": _period_contract(payload, observations),
    }
    return {
        "contract_version": CONTRACT_VERSION,
        "tool": tool,
        "build_commit": current_build_commit(),
        "execution": {
            "status": execution_status,
            "code": _first_present(payload, "code", "코드"),
            "failure_class": failure_class,
            "retryable": retryable,
        },
        "fulfillment": {
            "status": fulfillment_status,
            "completeness": "complete" if not missing_fields and execution_status == "executed" else "incomplete",
            "missing_fields": missing_fields,
            "requested_concepts": requested,
            "resolved_concepts": resolved,
            "missing_concepts": missing_concepts,
        },
        "evidence": evidence,
    }


def attach_stat_evidence(
    payload: dict[str, Any],
    *,
    tool: str,
    requested_concepts: Optional[Iterable[Any]] = None,
) -> dict[str, Any]:
    """Return a copy with an additive ``stat_evidence`` member."""
    result = dict(payload)
    result["stat_evidence"] = build_stat_evidence(
        result,
        tool=tool,
        requested_concepts=requested_concepts,
    )
    return result
