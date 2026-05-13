from __future__ import annotations

import re
from itertools import product
from typing import Any, Optional

from kosis_analysis.periods import _api_period_de

STATUS_INVALID_FILTER_CODE = "INVALID_FILTER_CODE"
STATUS_DENOMINATOR_REQUIRED = "DENOMINATOR_REQUIRED"

DIMENSION_AXIS_KEYWORDS = {
    "region": ("시도", "지역", "행정구역", "권역", "광역", "도별", "시군구", "province", "region"),
    "regions": ("시도", "지역", "행정구역", "권역", "광역", "도별", "시군구", "province", "region"),
    "region_group": ("시도", "지역", "행정구역", "권역", "광역", "도별", "시군구", "province", "region"),
    "industry": ("산업", "업종", "산업분류", "industry", "ksic"),
    "age": ("연령", "나이", "연령계층", "age"),
    "sex": ("성별", "성", "남녀", "sex", "gender"),
    "time": ("시점", "기간", "연도", "월", "분기", "period", "time"),
    "scale": ("규모", "기업규모", "종사자규모", "매출액규모", "scale", "size"),
    "size": ("규모", "기업규모", "종사자규모", "매출액규모", "scale", "size"),
    "employee_size": ("종사자규모", "종사자 규모", "인원규모", "employee size"),
    "sales_size": ("매출액규모", "매출 규모", "매출액 규모", "매출구간", "sales size"),
}


def _compact_text(text: str) -> str:
    return re.sub(r"[\s_\-·/()]+", "", str(text)).lower()


def _axis_matches_dimension(axis_name: str, dimension: str) -> bool:
    if dimension == "time":
        return True
    text = _compact_text(axis_name).lower()
    return any(_compact_text(token).lower() in text for token in DIMENSION_AXIS_KEYWORDS.get(dimension, (dimension,)))


def _infer_required_dimensions_from_query(query: str) -> list[str]:
    q = _compact_text(query)
    inferred: list[str] = []
    if any(term in q for term in ("업종", "업종별", "산업별", "산업대분류", "산업중분류", "제조업", "도소매업", "음식숙박업")):
        inferred.append("industry")
    if any(term in q for term in ("지역별", "시도별", "광역시", "수도권", "비수도권", "서울", "부산", "대구", "광주", "대전", "경기도", "경기지역")):
        inferred.append("region")
    if any(term in q for term in ("연령", "연령별", "청년", "고령", "60대", "30대")):
        inferred.append("age")
    if any(term in q for term in ("여성", "남성", "성별")):
        inferred.append("sex")
    if any(term in q for term in ("최근", "추이", "증가율", "변화", "코로나", "이전", "이후", "년간", "전년")):
        inferred.append("time")
    if any(term in q for term in ("규모별", "기업규모", "종사자규모", "매출액규모", "소상공인", "소기업", "중기업", "대기업")):
        inferred.append("scale")
    return list(dict.fromkeys(inferred))


def _normalize_required_dimensions(dimensions: list[str]) -> list[str]:
    aliases = {
        "지역": "region",
        "지역별": "region",
        "시도": "region",
        "시도별": "region",
        "권역": "region_group",
        "광역시": "region_group",
        "업종": "industry",
        "업종별": "industry",
        "산업": "industry",
        "산업별": "industry",
        "연령": "age",
        "연령별": "age",
        "나이": "age",
        "성별": "sex",
        "성": "sex",
        "기간": "time",
        "시점": "time",
        "연도": "time",
        "규모": "scale",
        "규모별": "scale",
        "기업규모": "scale",
        "종사자규모": "employee_size",
        "종사자규모별": "employee_size",
        "매출액규모": "sales_size",
        "매출규모": "sales_size",
    }
    normalized: list[str] = []
    for dim in dimensions:
        key = str(dim or "").strip()
        if not key:
            continue
        normalized.append(aliases.get(key, key))
    return list(dict.fromkeys(normalized))


def _dimension_coverage(
    axes: dict[str, dict[str, Any]],
    period_rows: list[dict],
    required_dimensions: list[str],
) -> tuple[list[str], list[str], dict[str, list[dict[str, Any]]]]:
    matched: list[str] = []
    missing: list[str] = []
    evidence: dict[str, list[dict[str, Any]]] = {}
    for dim in required_dimensions:
        if dim == "time":
            if period_rows:
                matched.append(dim)
                evidence[dim] = [
                    {
                        "cadence": row.get("PRD_SE"),
                        "start_period": row.get("STRT_PRD_DE"),
                        "latest_period": row.get("END_PRD_DE"),
                    }
                    for row in period_rows[:3]
                ]
            else:
                missing.append(dim)
            continue
        hits = [
            {"OBJ_ID": obj_id, "OBJ_NM": axis.get("OBJ_NM")}
            for obj_id, axis in axes.items()
            if _axis_matches_dimension(str(axis.get("OBJ_NM") or ""), dim)
        ]
        if hits:
            matched.append(dim)
            evidence[dim] = hits
        else:
            missing.append(dim)
    return matched, missing, evidence


def _indicator_evidence(
    table_name: Optional[str],
    axes: dict[str, dict[str, Any]],
    indicator: Optional[str],
) -> tuple[int, list[str]]:
    if not indicator:
        return 0, []
    target = _compact_text(indicator)
    evidence: list[str] = []
    score = 0
    if target and target in _compact_text(table_name or ""):
        score += 3
        evidence.append(f"table_name:{table_name}")
    for axis in axes.values():
        for meta in (axis.get("items") or {}).values():
            label = str(meta.get("label") or "")
            if target and target in _compact_text(label):
                score += 2
                evidence.append(f"item:{label}")
                if len(evidence) >= 5:
                    return score, evidence
    return score, evidence


def _concept_match_score(concept: str, label: Any, code: str) -> int:
    target = _compact_text(str(concept or "")).lower()
    text = _compact_text(str(label or "")).lower()
    code_text = _compact_text(str(code or "")).lower()
    if not target or not text:
        return 0
    if target == text or target == code_text:
        return 100
    if target in text:
        return 80
    if text in target and len(text) >= 2:
        return 60
    tokens = [tok for tok in re.split(r"[^0-9a-zA-Z가-힣]+", target) if tok]
    if tokens:
        overlap = sum(1 for tok in tokens if tok in text)
        if overlap:
            return 40 + min(20, overlap * 5)
    digits = re.findall(r"\d+", target)
    if digits and all(digit in text for digit in digits):
        return 45
    return 0


def _build_axis_codebook(item_rows: list[dict]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    axes: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in item_rows:
        obj_id = str(row.get("OBJ_ID") or "")
        itm_id = str(row.get("ITM_ID") or "")
        if not obj_id or not itm_id:
            continue
        if obj_id not in axes:
            axes[obj_id] = {
                "OBJ_NM": row.get("OBJ_NM"),
                "OBJ_NM_ENG": row.get("OBJ_NM_ENG"),
                "items": {},
            }
            order.append(obj_id)
        axes[obj_id]["items"][itm_id] = {
            "code": itm_id,
            "label": row.get("ITM_NM"),
            "label_en": row.get("ITM_NM_ENG"),
            "unit": row.get("UNIT_NM"),
            "parent": row.get("UP_ITM_ID"),
        }
    return axes, order


def _suggest_axis_codes(axis: dict[str, Any], bad_code: str, limit: int = 8) -> list[dict[str, Any]]:
    items = axis.get("items") or {}
    bad_norm = _compact_text(bad_code)
    preferred_order = {
        "00": 0, "0": 0,
        "11": 1, "21": 2, "22": 3, "23": 4, "24": 5, "25": 6, "26": 7, "27": 8,
        "31": 9, "32": 10, "33": 11, "34": 12, "35": 13, "36": 14, "37": 15, "38": 16, "39": 17,
    }
    scored: list[tuple[int, int, int, int, str, dict[str, Any]]] = []
    for code, meta in items.items():
        label = str(meta.get("label") or "")
        label_norm = _compact_text(label)
        score = 3
        if bad_code == code:
            score = 0
        elif bad_norm and (bad_norm in _compact_text(code) or bad_norm in label_norm):
            score = 1
        elif bad_norm and any(part and part in label_norm for part in re.split(r"[^0-9A-Za-z가-힣]+", bad_norm)):
            score = 2
        parent_rank = 0 if not meta.get("parent") else 1
        order_rank = preferred_order.get(str(code), 100)
        scored.append((score, parent_rank, order_rank, len(str(code)), str(code), meta))
    scored.sort(key=lambda row: (row[0], row[1], row[2], row[3], row[4]))
    return [
        {"code": code, "label": meta.get("label"), "unit": meta.get("unit")}
        for _, _, _, _, code, meta in scored[:limit]
    ]


def _validate_query_table_filters(
    filters: dict[str, Any],
    axes: dict[str, dict[str, Any]],
    axis_order: list[str],
) -> tuple[Optional[dict[str, list[str]]], list[dict[str, Any]], dict[str, list[str]]]:
    if not isinstance(filters, dict):
        return None, [{"오류": "filters는 {OBJ_ID: [ITM_ID, ...]} 형식의 객체여야 합니다."}], {}

    normalized: dict[str, list[str]] = {}
    errors: list[dict[str, Any]] = []
    auto_defaults: dict[str, list[str]] = {}

    for axis_id, raw_codes in filters.items():
        axis = str(axis_id)
        if axis not in axes:
            errors.append({
                "axis": axis,
                "오류": "존재하지 않는 분류축",
                "available_axes": [
                    {"OBJ_ID": obj_id, "OBJ_NM": axes[obj_id].get("OBJ_NM")}
                    for obj_id in axis_order
                ],
            })
            continue
        codes = raw_codes if isinstance(raw_codes, list) else [raw_codes]
        clean_codes = [str(code) for code in codes if str(code or "").strip()]
        if not clean_codes:
            errors.append({"axis": axis, "오류": "비어 있는 필터 코드"})
            continue
        items = axes[axis]["items"]
        for code in clean_codes:
            if code not in items:
                errors.append({
                    "axis": axis,
                    "code": code,
                    "오류": "분류축에 없는 ITM_ID",
                    "suggested_codes": _suggest_axis_codes(axes[axis], code),
                })
        normalized[axis] = clean_codes

    item_axis = "ITEM" if "ITEM" in axes else None
    if item_axis and item_axis not in normalized:
        item_codes = list((axes[item_axis].get("items") or {}).keys())
        if len(item_codes) == 1:
            normalized[item_axis] = [item_codes[0]]
            auto_defaults[item_axis] = [item_codes[0]]
        else:
            errors.append({
                "axis": item_axis,
                "오류": "ITEM 축은 명시해야 합니다.",
                "suggested_codes": _suggest_axis_codes(axes[item_axis], ""),
            })

    for axis in axis_order:
        if axis == "ITEM" or axis in normalized:
            continue
        axis_codes = list((axes[axis].get("items") or {}).keys())
        if len(axis_codes) == 1:
            normalized[axis] = [axis_codes[0]]
            auto_defaults[axis] = [axis_codes[0]]
        else:
            errors.append({
                "axis": axis,
                "오류": "다중 값을 가진 분류축은 명시해야 합니다. 전체 조회가 필요하면 원하는 ITM_ID들을 모두 전달하세요.",
                "suggested_codes": _suggest_axis_codes(axes[axis], ""),
            })

    if errors:
        return None, errors, auto_defaults
    return normalized, [], auto_defaults


def _query_table_params(
    org_id: str,
    tbl_id: str,
    filters: dict[str, list[str]],
    axis_order: list[str],
    period_range: Optional[list[str]],
    period_type: Optional[str],
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "method": "getList",
        "apiKey": None,
        "orgId": org_id,
        "tblId": tbl_id,
        "format": "json",
        "jsonVD": "Y",
    }
    if period_type:
        params["prdSe"] = period_type
    data_axis_index = 0
    for axis in axis_order:
        if axis != "ITEM":
            data_axis_index += 1
        codes = filters.get(axis)
        if not codes:
            continue
        value = ",".join(codes)
        if axis == "ITEM":
            params["itmId"] = value
        else:
            params[f"objL{data_axis_index}"] = value
    if period_range:
        bounds = [str(p) for p in period_range if str(p or "").strip()]
        if len(bounds) == 1:
            params["startPrdDe"] = _api_period_de(bounds[0])
            params["endPrdDe"] = _api_period_de(bounds[0])
        elif len(bounds) >= 2:
            params["startPrdDe"] = _api_period_de(bounds[0])
            params["endPrdDe"] = _api_period_de(bounds[1])
    return params


def _normalize_query_table_rows(
    rows: list[dict],
    filters: dict[str, list[str]],
    axes: dict[str, dict[str, Any]],
    axis_order: list[str],
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    data_axis_index = 0
    data_axis_map: dict[int, str] = {}
    for axis in axis_order:
        if axis == "ITEM":
            continue
        data_axis_index += 1
        if filters.get(axis):
            data_axis_map[data_axis_index] = axis

    for row in rows:
        dimensions: dict[str, Any] = {}
        item_code = str(row.get("ITM_ID") or row.get("ITM_ID1") or "")
        item_label = row.get("ITM_NM")
        if "ITEM" in axes:
            if item_code in axes["ITEM"]["items"]:
                meta = axes["ITEM"]["items"][item_code]
                item_label = item_label or meta.get("label")
                dimensions["ITEM"] = {"code": item_code, "label": item_label, "unit": meta.get("unit")}
            elif len(filters.get("ITEM", [])) == 1:
                code = filters["ITEM"][0]
                meta = axes["ITEM"]["items"].get(code, {})
                dimensions["ITEM"] = {"code": code, "label": meta.get("label"), "unit": meta.get("unit")}

        for idx, axis in data_axis_map.items():
            code = str(row.get(f"C{idx}") or "")
            label = row.get(f"C{idx}_NM")
            if not code and len(filters.get(axis, [])) == 1:
                code = filters[axis][0]
            meta = (axes.get(axis, {}).get("items") or {}).get(code, {})
            dimensions[axis] = {
                "code": code,
                "label": label or meta.get("label"),
                "unit": meta.get("unit"),
            }

        normalized_rows.append({
            "period": row.get("PRD_DE"),
            "value": row.get("DT"),
            "unit": row.get("UNIT_NM") or (dimensions.get("ITEM") or {}).get("unit"),
            "dimensions": dimensions,
            "raw": row,
        })
    return normalized_rows


def _fanout_filter_sets(filters: dict[str, list[str]]) -> list[dict[str, list[str]]]:
    """Return one single-code filter set per Cartesian product.

    KOSIS often rejects comma-joined multi-code parameters with code 21. The
    public contract can still accept multi-code filters by faning out safely
    inside the server and merging raw rows afterward.
    """
    axes = list(filters.keys())
    code_groups = [filters[axis] or [] for axis in axes]
    if not axes or any(not codes for codes in code_groups):
        return [filters]
    return [
        {axis: [code] for axis, code in zip(axes, combo)}
        for combo in product(*code_groups)
    ]


def _to_number(value: Any) -> Optional[float]:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _aggregate_rows_sum_by_group(
    rows: list[dict[str, Any]],
    filters: dict[str, list[str]],
    group_by: Optional[list[str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    preserve = set(group_by or [])
    preserve.add("ITEM")
    aggregated_axes = [
        axis
        for axis, codes in filters.items()
        if axis != "ITEM" and len(codes) > 1 and axis not in preserve
    ]
    buckets: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        dimensions = row.get("dimensions") or {}
        preserved_dimensions = {
            axis: meta
            for axis, meta in dimensions.items()
            if axis in preserve or axis not in aggregated_axes
        }
        key = (
            row.get("period"),
            row.get("unit"),
            tuple(
                (axis, (meta or {}).get("code"))
                for axis, meta in sorted(preserved_dimensions.items())
            ),
        )
        number = _to_number(row.get("value"))
        if number is None:
            continue
        bucket = buckets.setdefault(key, {
            "period": row.get("period"),
            "value": 0.0,
            "unit": row.get("unit"),
            "dimensions": preserved_dimensions,
            "aggregation": {
                "operation": "sum_by_group",
                "aggregated_axes": aggregated_axes,
                "source_row_count": 0,
                "source_codes": {axis: [] for axis in aggregated_axes},
            },
        })
        bucket["value"] += number
        bucket["aggregation"]["source_row_count"] += 1
        for axis in aggregated_axes:
            meta = dimensions.get(axis) or {}
            code = meta.get("code")
            if code and code not in bucket["aggregation"]["source_codes"][axis]:
                bucket["aggregation"]["source_codes"][axis].append(code)
    result = list(buckets.values())
    for row in result:
        value = row["value"]
        row["value"] = int(value) if float(value).is_integer() else value
    return result, aggregated_axes
