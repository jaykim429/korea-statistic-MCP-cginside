"""Official survey explanations have a separate evidence role from observations.

API schema: https://sso.kosis.kr/openapi/devGuide/devGuide_0401List.do
Survey metadata may describe design, not realized error, vintage or causal identification.
"""
from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RULES = json.loads((Path(__file__).resolve().parent.parent / "rules" / "methodology-labels.json").read_text(encoding="utf-8"))
FACETS = {facet["id"]: facet for facet in RULES["facets"]}
FIELD_LABELS = RULES["fieldLabels"]


def request_for(question: str) -> dict[str, Any] | None:
    bounded = question[:500]
    facets = [name for name, facet in FACETS.items() if re.search(facet["pattern"], bounded)]
    if not facets:
        return None
    if all(name == "base_year" for name in facets) and re.search(r"^(?:이|그|앞선|방금|이전)\s*(?:값|수치|답변)", bounded.strip()):
        return None
    metadata_ask = re.search(r"표본|표집|오차|신뢰구간|조사\s*(?:방법|방식|설계|체계)|자료\s*수집|인과", bounded)
    metadata_ask = metadata_ask or re.search(r"기준\s*(?:년(?:도)?|연도|시점).*(?:언제|무엇|뭐|알려|설명|어떻게|왜)|(?:언제|무엇|뭐).*기준\s*(?:년(?:도)?|연도|시점)", bounded)
    if not metadata_ask:
        return None
    topic = re.sub("|".join("(?:" + facet["pattern"] + ")" for facet in FACETS.values()), " ", bounded)
    for pattern in RULES["topicStripPatterns"]:
        topic = re.sub(pattern, " ", topic)
    topic = re.sub(r"[?!.，,]", " ", topic)
    topic = re.sub(r"\s+", " ", topic).strip()
    value_action = any(re.search(RULES["valueActionPattern"], clause) and not any(re.search(f["pattern"], clause) for f in FACETS.values())
                       for clause in re.split(r"그리고|(?:와|과|및)\s*", bounded))
    return {"facets": facets, "topic": topic, "requestedYears": list(dict.fromkeys(re.findall(r"((?:19|20)\d{2})\s*년", bounded))),
            "role": "mixed" if value_action and "causal" not in facets else "methodology"}


def plain_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    # Decode escaped markup twice, then preserve paragraph boundaries before stripping tags.
    value = html.unescape(html.unescape(value))
    value = re.sub(r"<\s*(?:br\s*/?|/p|/div|/li)\s*>", "\n", value, flags=re.I)
    value = re.sub(r"<[^>]*>", "", value)
    return re.sub(r"[ \t]+", " ", value).strip()


def explanation_fields(rows: list[dict], units: list[dict] | None = None) -> dict[str, list[str]]:
    fields: dict[str, list[str]] = {}
    for row in rows:
        for key in [*FIELD_LABELS, "confmNo"]:
            text = plain_text(row.get(key))
            if text and text not in fields.setdefault(key, []):
                fields[key].append(text)
    # A date or an old description string is not a current index base.
    bases = [plain_text(row.get("UNIT_NM")) for row in units or [] if re.search(FACETS["base_year"]["coveragePattern"], plain_text(row.get("UNIT_NM")))]
    if bases:
        fields["indexBase"] = list(dict.fromkeys(bases))
    return fields


def methodology_payload(question: str, request: dict, org_id: str, tbl_id: str, rows: list[dict], units: list[dict] | None = None) -> dict:
    fields = explanation_fields(rows, units)
    names = fields.get("statsNm", [])
    base = {"contract_version": RULES["contractVersion"], "role": "methodology", "question": question,
            "requested_facets": request["facets"], "requested_years": request["requestedYears"],
            "actual_query_supported": False, "retrieved_at": datetime.now(timezone.utc).isoformat()}
    if len(names) != 1:
        return {**base, "status": "ambiguous_source", "fields": [], "missing_facets": request["facets"],
                "explanation": "공식 조사명이 없거나 여러 조사명이 있어 요청한 조사 설명으로 확정하지 않았습니다."}
    selected: dict[str, list[str]] = {}
    covered, partial = [], []
    for name in request["facets"]:
        facet = FACETS[name]
        relevant = {key: fields[key] for key in facet["fields"] if fields.get(key)}
        # An attachment marker contains no explanatory evidence.
        relevant = {key: [text for text in texts if not re.fullmatch(r"(?:첨부파일 있음|파일 첨부|해당없음|지수작성 안함)[ .]*", text)]
                    for key, texts in relevant.items()}
        relevant = {key: texts for key, texts in relevant.items() if texts}
        if name == "base_year":
            # Prefer a fresh table unit; otherwise show an explicit official base statement.
            relevant = {key: texts for key, texts in relevant.items() if any(re.search(facet["coveragePattern"], text) for text in texts)}
            if "indexBase" in relevant:
                relevant = {"indexBase": relevant["indexBase"]}
        selected.update(relevant)
        content = "\n".join(text for texts in relevant.values() for text in texts)
        reason_not_proved = name == "base_year" and re.search(r"왜|이유", question)
        if relevant and not reason_not_proved and not facet.get("partialOnly") and (not facet.get("coveragePattern") or re.search(facet["coveragePattern"], content)):
            covered.append(name)
        elif relevant:
            partial.append(name)
    if fields.get("examinTrgetPd"):
        selected["examinTrgetPd"] = fields["examinTrgetPd"]
    missing = [name for name in request["facets"] if name not in covered]
    warnings = ["조회한 통계설명자료의 내용입니다. 특정 연도·지역·지표별로 적용되는 규모나 실현 오차까지 검증한 결과는 아닙니다."]
    if "sample_error" in missing:
        warnings.append("표본 설계·추정 방법과 실현된 표본오차는 다릅니다. 이 설명자료만으로 모든 지표에 공통인 오차율을 만들지 않았습니다. 원하는 지표·연도·집계 범위를 정한 뒤 해당 결과의 오차자료를 확인해야 합니다.")
    if request["requestedYears"]:
        warnings.append("요청한 " + ", ".join(request["requestedYears"]) + "년의 설계·규모 적용 여부는 현재 설명자료에서 별도로 확인하지 못했습니다.")
    if "base_year" in missing and re.search(r"왜|이유", question):
        warnings.append("지수 기준 표시만으로 기준년 선택·변경의 이유까지 증명하지 않았습니다.")
    return {**base, "status": "explained" if not missing and not request["requestedYears"] else "partial",
            "survey_name": names[0], "source": {"label": "KOSIS 통계설명자료 · " + names[0],
                "url": f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}", "org_id": org_id, "table_id": tbl_id,
                "approval_number": fields.get("confmNo", [None])[0]},
            "fields": [{"key": key, "label": FIELD_LABELS[key], "text": "\n\n".join(texts)} for key, texts in selected.items()],
            "covered_facets": covered, "partial_facets": partial, "missing_facets": missing, "warnings": warnings}
