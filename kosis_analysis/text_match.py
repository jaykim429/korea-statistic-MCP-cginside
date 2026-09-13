"""질의 어휘 매칭 규칙 — 사용자가 쓴 말과 통계표 이름을 맞춰 보는 순수 함수들.

KOSIS API 도 MCP 프로토콜도 타지 않는다. 표 후보의 순위와 "이 표가 질문에 맞는가"를 이 규칙이
결정하므로, 여기가 틀리면 엉뚱한 표가 답으로 나간다(실측: '실업율'이 0건, '아동복지 관련'의
'관련'이 공무원범죄자 표를 불렀다). 서버를 띄우지 않고 pytest 로 바로 확인할 수 있게 분리했다.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from kosis_analysis.metadata import _compact_text


_QUERY_STOP_TERMS = {
    "알려줘", "알려", "보여줘", "찾아줘", "최근", "추이", "기준", "통계", "자료",
    # 일반어 — 실측: "아동복지 관련 중소기업 통계"에서 '관련'만 걸린 공무원범죄자(직무관련)·통계 관련인력 표가 후보로 나갔다
    "관련", "관해", "관하여", "연관", "현황", "정보", "대한", "대해", "대해서", "조회", "확인", "전체",
    "알려주세요", "보여주세요", "찾아", "얼마", "얼마나", "얼마야", "얼마나요", "있어", "있나요", "있는지",
    "the", "and", "for", "with", "show", "find", "data", "stat", "stats",
}


def _content_search_query(query: Any) -> str:
    """검색어용 질의: 명령형·일반어를 뺀 내용어만 남긴다("아동복지 관련 중소기업 통계" → "아동복지 중소기업")."""
    tokens = _query_tokens_for_matching(query)
    # 연도·기간 표현("2020년부터", "2023년까지", "최근5년")은 검색어가 아니다 — 넣으면 KOSIS 검색이 엉뚱한 표를 낸다(실측 V5)
    tokens = [t for t in tokens if not re.search(r"\d", t) and t not in ("부터", "까지", "사이", "동안", "이후", "이전", "기간")]
    return " ".join(tokens)


# 사용자가 붙여 쓰는 합성어를 끊어 내기 위한 측정 명사. 긴 토큰이 표명에 그대로 없을 때만 쓴다
# (실측: "소상공인사업체수" 는 어떤 표명에도 없어 결과 0건이 됐다).
_MEASURE_NOUNS: tuple[str, ...] = (
    "사업체수", "사업체", "종사자수", "종사자", "근로자수", "근로자", "매출액", "수출액", "수입액",
    "생산액", "부가가치", "영업이익", "취업자수", "취업자", "실업자수", "실업자", "기업수", "가구수",
    "인구수", "학생수", "농가수", "어가수", "발전량", "소비량", "배출량", "처리량", "투자액", "이용률",
    "증가율", "감소율", "상승률", "하락률", "실업률", "고용률", "출산율", "비중", "비율",
)
# 흔한 표기 흔들림 — 표명 어휘와 맞춰야 내용어 필터가 정상 동작한다(실측: "실업율" 결과 0건)
_TOKEN_TYPO_FIXES: tuple[tuple[str, str], ...] = (
    ("실업율", "실업률"), ("고용율", "고용률"), ("출산율", "출산률"), ("증가율", "증가률"),
    ("사업채", "사업체"), ("종사자수", "종사자수"), ("물가상승율", "물가상승률"),
)


def _normalize_typo_token(token: str) -> str:
    """'실업율' → '실업률' 처럼 표명 표기에 맞춘다. 바꿀 게 없으면 그대로."""
    for wrong, right in _TOKEN_TYPO_FIXES:
        if token == wrong:
            return right
    # 일반 규칙: 받침 있는 한자어 뒤의 '율'은 표명에서 '률'로 쓰인다(실업율→실업률, 고용율→고용률)
    if len(token) >= 3 and token.endswith("율"):
        return token[:-1] + "률"
    return token


def _normalize_typo_query(query: Any) -> str:
    """질의 전체의 표기 흔들림을 표명 표기에 맞춘다 — KOSIS 검색 자체가 '실업율'로는 엉뚱한 표를 준다(실측)."""
    text = str(query or "")
    out = []
    for token in re.findall(r"[0-9A-Za-z가-힣]+|[^0-9A-Za-z가-힣]+", text):
        out.append(_normalize_typo_token(token) if re.fullmatch(r"[0-9A-Za-z가-힣]+", token) else token)
    return "".join(out)


def _split_compound_token(token: str) -> list[str]:
    """'소상공인사업체수' → ['소상공인', '사업체수'] 처럼 측정 명사 앞에서 한 번 끊는다."""
    for noun in _MEASURE_NOUNS:
        if len(token) > len(noun) + 1 and token.endswith(noun):
            head = token[: -len(noun)]
            if len(head) >= 2:
                return [head, noun]
    return []


def _query_tokens_for_matching(query: Any) -> list[str]:
    text = str(query or "").replace("R&D", "RD").replace("r&d", "rd")
    tokens = re.findall(r"[0-9A-Za-z가-힣]+", text)
    cleaned: list[str] = []
    for token in tokens:
        norm = _compact_text(token)
        if len(norm) < 2 or norm in _QUERY_STOP_TERMS:
            continue
        cleaned.append(norm)
    return list(dict.fromkeys(cleaned))


def _query_token_matches_text(token: str, text: Any) -> bool:
    norm = _compact_text(token)
    if norm:
        body = _compact_text(str(text or "").replace("R&D", "RD").replace("r&d", "rd"))
        if norm not in body:
            # 표기 흔들림(실업율/실업률)과 붙여 쓴 합성어(소상공인사업체수)를 한 번 더 본다
            fixed = _normalize_typo_token(norm)
            if fixed != norm and fixed in body:
                return True
            parts = _split_compound_token(norm)
            if parts and all(_compact_text(p) in body for p in parts):
                return True
    if not norm:
        return False
    if re.fullmatch(r"[0-9a-z]+", norm):
        return norm in _query_tokens_for_matching(text)
    return norm in _compact_text(str(text or "").replace("R&D", "RD").replace("r&d", "rd"))


def _query_token_weight(token: str) -> float:
    # Korean query tokens tend to carry domain semantics, while short ASCII
    # tokens are often acronyms. Weighting affects ranking only; evidence is
    # still exposed verbatim for the caller to judge.
    return 2.0 if re.search(r"[가-힣]", str(token or "")) else 1.0


def _query_match_quality(query: Any, candidate_text: Any) -> dict[str, Any]:
    tokens = _query_tokens_for_matching(query)
    matched = [token for token in tokens if _query_token_matches_text(token, candidate_text)]
    missing = [token for token in tokens if token not in matched]
    ratio = round(len(matched) / len(tokens), 4) if tokens else 1.0
    # 한국어 질의는 마지막 내용어가 머리 명사다("청년 창업" → 창업 통계, "부산 소상공인 사업체" → 사업체). 머리 명사만 걸린 표가
    # 수식어만 걸린 표보다 앞에 오도록 가중한다. 순위에만 쓰이고 근거(matched_terms)는 그대로 노출된다.
    head = tokens[-1] if len(tokens) >= 2 else None
    def _w(token: str) -> float:
        return _query_token_weight(token) * (1.5 if token == head else 1.0)
    total_weight = sum(_w(token) for token in tokens)
    matched_weight = sum(_w(token) for token in matched)
    weighted_ratio = round(matched_weight / total_weight, 4) if total_weight else 1.0
    return {
        "query_terms": tokens,
        "matched_terms": matched,
        "missing_query_terms": missing,
        "coverage_ratio": ratio,
        "weighted_coverage_ratio": weighted_ratio,
        "match_quality": "high" if ratio >= 0.8 else "medium" if ratio >= 0.5 else "low",
    }


def _match_quality_rank(quality: Optional[dict[str, Any]]) -> int:
    label = (quality or {}).get("match_quality")
    return {"high": 3, "medium": 2, "low": 1}.get(str(label), 0)
