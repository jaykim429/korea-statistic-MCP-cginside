from __future__ import annotations

import logging
import os
import re
from typing import Any, Optional

from kosis_curation import (
    QuickStatParam,
    REGION_COMPOSITES,
    SYNONYMS,
    canonical_region as _canonical_region,
    extract_region_candidate,
    requests_time_series,
    PERIOD_CADENCE_TERMS,
)
from kosis_analysis.metadata import _compact_text
from kosis_analysis.text_match import _QUERY_STOP_TERMS, _query_tokens_for_matching
from kosis_analysis.rules import INDUSTRY_LABELS

STATUS_UNVERIFIED_FORMULA = "UNVERIFIED_FORMULA"


_DIRECT_REGION_NAMES = (
    "전국", "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
    "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원도",
    "강원특별자치도", "충청북도", "충청남도", "전라북도", "전북특별자치도",
    "전라남도", "경상북도", "경상남도", "제주특별자치도",
)


def _extract_single_region_from_query(query: str) -> Optional[str]:
    q = str(query or "")
    candidate = extract_region_candidate(q)
    if candidate and (candidate not in _DIRECT_REGION_NAMES or " " in candidate):
        return candidate
    matches: list[str] = []
    non_region_gyeonggi = re.search(
        r"경기(?:동행|선행|전망|동향|실사|체감|회복|변동|순환|지수)",
        re.sub(r"\s+", "", q),
    )
    for name in sorted(_DIRECT_REGION_NAMES, key=len, reverse=True):
        if name in {"경기", "경기도"} and non_region_gyeonggi:
            continue
        if name in q:
            canonical = _canonical_region(name) or name
            if canonical not in matches:
                matches.append(canonical)
    return matches[0] if len(matches) == 1 else None


def _extract_single_year_from_query(query: str) -> Optional[str]:
    text = str(query or "")
    years = list(dict.fromkeys(
        match.group(0)
        for match in re.finditer(r"(?:19|20)\d{2}", text)
        if not re.match(r"\s*=\s*100\b", text[match.end():])
    ))
    return years[0] if len(years) == 1 else None


def _quick_stat_unsupported_dimensions(
    query: str,
    param: Optional[QuickStatParam] = None,
) -> list[str]:
    q = str(query or "")
    compact = _compact_text(q)
    dimensions: list[str] = []
    if (
        re.search(r"\d+\s*[-~]\s*\d+\s*세", q)
        or re.search(r"\d+\s*세", q)
        or re.search(r"\d{2,3}\s*대", q)
        or any(term in compact for term in ("청년", "연령별", "나이별", "연령대", "연령구간"))
        or ("연령" in compact and "평균초혼연령" not in compact)
        or "나이" in compact
    ):
        dimensions.append("age")
    if any(term in compact for term in ("여성", "여자", "남성", "남자", "성별")):
        dimensions.append("gender")
    if requests_time_series(query) or "팬데믹기간" in compact:
        dimensions.append("time_series")
    if any(term in compact for term in ("vs", "대비", "비교")):
        dimensions.append("comparison")
    if any(region in compact for region in (_compact_text(name) for name in REGION_COMPOSITES)):
        dimensions.append("region_group")
    encoded_dimensions = set(getattr(param, "encoded_dimensions", ()) or ())
    return [
        dimension
        for dimension in dict.fromkeys(dimensions)
        if dimension not in encoded_dimensions
    ]


def _quick_trend_unsupported_dimensions(
    query: str,
    param: Optional[QuickStatParam] = None,
) -> list[str]:
    """Trend lookup supports a time-series request but not extra slicing dimensions."""
    return [
        dimension
        for dimension in _quick_stat_unsupported_dimensions(query, param)
        if dimension != "time_series"
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 잔여 한정어 관문
#
# 왜 있나 (실측, 회귀19).
#   "중국으로 수출액 얼마야?"  → "2026년 7월 **전국의** 수출액은 98,959 100만달러입니다"
#   "고령자 고용률"            → 그냥 "고용률" 과 같은 값
# 큐레이션 조회가 한정어를 무시하고 무한정 지표에 접는다. `match_direct_stat_key("중국으로 수출액 얼마야?")`
# 는 실제로 `수출액` 을 돌려준다 — 값 자체는 정확한 전국 수출액이고, 틀린 것은 거기에 사용자의
# 한정어를 붙여 내보내는 부분이다. 같은 98,959 가 네 질문의 답으로 쓰였다.
#
# 왜 어휘 목록이 아닌가.
# 위 다섯 감지기는 어휘 목록이라 '고령자'·'중소기업'·'중국' 이 샜다. 우리말 모집단 한정어는 열린
# 집합이라 목록은 반드시 샌다. 그래서 **반대로** 센다 — 질문에서 지표·지역·시점·잡말을 덜어 내고
# **남는 내용어**를 한정어로 본다. 지표 이름에 든 말은 남지 않으므로("제조업 중소기업 사업체 수" 는
# 키 `제조업_중소기업_사업체수` 가 통째로 덮는다) 정당한 지표는 거절되지 않는다.
# ─────────────────────────────────────────────────────────────────────────────

_LOG = logging.getLogger(__name__)

GATE_OFF = "off"
GATE_WARN = "warn"
GATE_ENFORCE = "enforce"

#: 관문 동작. 운영 중 긴급 회피가 필요하면 `off` 로 내린다.
LEFTOVER_GATE_MODE = os.environ.get("KOSIS_MCP_LEFTOVER_GATE", GATE_ENFORCE).strip().lower()

#: 전국을 가리키는 말. `_DIRECT_REGION_NAMES` 에 없어서 "우리나라 수출액" 이 오탐 거절됐다(실측).
_NATIONWIDE_TERMS = ("우리나라", "한국", "대한민국", "국내", "전국")

#: 최신 시점을 뜻하는 말. 잡말이 아니라 시간 의미다 — 따로 두어야 회귀 분석 때 근거가 남는다.
#: quick_stat 은 최신 시점을 돌려주므로 결과적으로 덮인다.
_LATEST_PERIOD_TERMS = ("최근", "현재", "지금", "요즘", "현재기준", "최신", "연도", "년도", "시점")

#: 대화 표현. `_QUERY_STOP_TERMS` 가 토큰 **등가**로만 걸려 활용형이 샌다 — 그 위에 더한다.
_CONVERSATION_FILLERS = (
    "말해줘", "말해주세요", "조회해줘", "조회해주세요", "확인해줘", "확인해주세요",
    "보고싶어", "궁금해", "알고싶어", "알수있어", "나와", "주세요", "부탁해",
    "수치", "데이터", "값", "있나", "있는가", "어때", "어떻게", "어떤", "정도", "좀", "몇",
    # 아래는 오프라인 감사(scripts/audit_leftover_terms.py)에서 1번 종류로 분류된 것들이다.
    "뭐야", "뭐가", "어디", "어디야", "언제야", "나눠줘", "알아봐", "알려주고", "전부",
    "그거", "그건", "아까", "아니", "말고", "취소하고", "빨리", "달라",
    "계산하나요", "작성하나요", "작성돼", "포함되나요",
    # 형식·단위 요청. 값 자체는 같고 보여 주는 모양만 다르다 — 한정어가 아니다.
    "차트", "그래프", "시각화", "도표", "엑셀", "파일", "단위", "백만", "천", "억",
)

#: 시점 표현. 토큰 하나가 통째로 이 꼴이면 시점으로 보고 덮는다.
_PERIOD_TOKEN = re.compile(r"^(?:\d{4}년?|\d{1,2}월|\d분기|상반기|하반기|작년|올해|지난해|전년|금년|작년도|금년도)$")

#: 어간을 뗄 조사. **어간이 2자 이상 남을 때만** 뗀다 — "물가"→"물" 사고를 막는다(Nuxt 와 같은 규칙).
_PARTICLE = re.compile(r"(?:이야|이랑|랑|의|으로|로|에서|에|은|는|이|가|을|를|도|과|와|만|까지|부터)$")

#: 조사를 떼면 이것만 남는 말은 내용어가 아니다 — "수는"→"수". 어간 2자 규칙의 예외 처리.
_SHORT_MEASURE_WORD = re.compile(r"^[수액율률량값개명건톤%]$")


def _strip_particle(token: str) -> str:
    """조사를 뗀 어간. 2자 미만이 되면 떼지 않는다."""
    stripped = _PARTICLE.sub("", token)
    return stripped if len(stripped) >= 2 else token


def _strip_byeol(token: str) -> str:
    """`…별` 의 어간. 2자 미만이 되면 떼지 않는다."""
    if token.endswith("별") and len(token) >= 3:
        return token[:-1]
    return token


def _synonyms_of(key: str) -> tuple[str, ...]:
    """그 Tier A 키를 가리키는 별칭들. `SYNONYMS` 는 별칭→키 단방향이라 역으로 훑는다."""
    if not key:
        return ()
    return tuple(alias for alias, canonical in SYNONYMS.items() if canonical == key)


def _covered(query: str, param: Optional[QuickStatParam], matched_key: str) -> tuple[list[str], set[str]]:
    """덮인 말을 두 갈래로 돌려준다 — (부분 문자열로 볼 것, 등가로 볼 것).

    지표 이름은 **부분 문자열**로 본다. KOSIS 는 측정어를 붙여 쓰고(`사업체수`) 사용자는 띄어 쓰므로
    (`사업체 수`) 등가로만 보면 `사업체` 가 한정어로 잘못 남는다(실측).
    잡말·불용어는 **등가**로 본다 — 부분 문자열로 보면 내용어를 삼킨다.
    """
    texts: list[str] = []
    tokens: set[str] = set()

    def add_text(value: Any) -> None:
        compacted = _compact_text(str(value or ""))
        if compacted:
            texts.append(compacted)

    def add_tokens(*values: str) -> None:
        for value in values:
            compacted = _compact_text(value)
            if compacted:
                tokens.add(compacted)

    # 1) 지표 — 키·설명·표명·동의어. 지표 이름에 든 말은 한정어가 아니다.
    add_text(matched_key)
    if param is not None:
        add_text(param.description)
        add_text(param.tbl_nm)
    for alias in _synonyms_of(matched_key):
        add_text(alias)

    # The router composes registered slots independently of word order. The residual
    # gate must recognize the same *complete* official category, not its child industries.
    # Bind the alias to the verified industry code actually encoded by this parameter.
    if param and param.verification_status == "verified":
        for category in INDUSTRY_LABELS:
            if category["canonical"] in matched_key.split("_") and param.obj_l1 == f'IM_{category["section"]}':
                for alias in category["aliases"]:
                    if _compact_text(alias) in _compact_text(query):
                        add_text(alias)

    # 2) 지역 — 질문이 지목한 단일 지역, 시도 이름, 전국 동의어
    region = _extract_single_region_from_query(query)
    if region:
        add_text(region)
    for name in _DIRECT_REGION_NAMES:
        add_text(name)
    for name in _NATIONWIDE_TERMS:
        add_text(name)

    # 3) 최신 시점 표현·잡말·기존 불용어 — 등가로만
    add_tokens(*_LATEST_PERIOD_TERMS)
    add_tokens(*_CONVERSATION_FILLERS)
    add_tokens(*_QUERY_STOP_TERMS)
    # Period grouping is a temporal constraint, not an unknown population.
    # The time-series gate still prevents quick_stat from silently returning one value.
    if param:
        for cadence in param.supported_periods:
            add_tokens(*PERIOD_CADENCE_TERMS.get(cadence, ()))

    # 4) 지표가 스스로 선언한 말
    for term in (getattr(param, "encoded_terms", ()) or ()):
        add_text(term)

    return texts, tokens


def leftover_gate_terms(
    query: str,
    param: Optional[QuickStatParam] = None,
) -> list[str]:
    """관문 판정 — 호출자는 이것만 부르면 된다.

    `enforce` 면 잔여를 그대로 돌려주고(호출자가 거부), `warn` 이면 로그만 남기고 빈 목록을,
    `off` 면 계산조차 하지 않는다. 매칭 키는 여기서 직접 얻는다 — 호출자가 라우터를 알 필요가 없다.
    """
    if LEFTOVER_GATE_MODE == GATE_OFF:
        return []
    from kosis_curation import DEFAULT_ROUTER

    matched_key = DEFAULT_ROUTER.match_direct_stat_key(query) or ""
    terms = leftover_qualifier_terms(query, param, matched_key)
    if terms and LEFTOVER_GATE_MODE != GATE_ENFORCE:
        _LOG.warning("[leftover] query=%r key=%r terms=%r", query, matched_key, terms)
        return []
    return terms


def leftover_qualifier_terms(
    query: str,
    param: Optional[QuickStatParam] = None,
    matched_key: str = "",
) -> list[str]:
    """질문에서 지표·지역·시점·잡말을 덜어 내고 **남는 내용어**. 남으면 그 값으로 답하면 안 된다.

    `…별` 은 일괄 제외하지 않는다 — `업종별·학력별·직업별` 은 축 이름이기 이전에 **사용자가 요청한
    차원**이고, 큐레이션 지표가 그 축으로 나눌 수 없으면 전국 단일값이 나간다. 다만
      · 기존 다섯 감지기가 이미 그 차원을 잡았거나
      · `별` 을 뗀 어간이 덮인 말에 있으면(`제조업별` → `제조업`)
    잔여에서 뺀다. 남길 때는 **`별` 을 뗀 어간**으로 싣는다 — 다음 단계의 축 매핑이 그 형태를 쓴다.
    """
    if not query:
        return []
    texts, tokens = _covered(query, param, matched_key)
    # 이미 다른 감지기가 잡은 차원은 두 번 세지 않는다.
    detected = set(_quick_stat_unsupported_dimensions(query, param))

    def is_covered(term: str) -> bool:
        if not term:
            return True
        if term in tokens:
            return True
        return any(term in text for text in texts)

    leftover: list[str] = []
    for token in _query_tokens_for_matching(query):
        if _PERIOD_TOKEN.match(token):
            continue
        stem = _strip_particle(token)
        # 조사를 떼면 한 글자 계량어만 남는 말("수는" → "수")은 내용어가 아니다.
        # "물가"("물") 같은 지표어를 지키려고 어간 2자 규칙을 쓰므로, 계량어는 따로 걸러 낸다.
        if _SHORT_MEASURE_WORD.match(_PARTICLE.sub("", token) or ""):
            continue
        if _PERIOD_TOKEN.match(stem) or is_covered(token) or is_covered(stem):
            continue
        # 조사를 뗀 **어간**으로 본다 — "분야별로" 는 token 이 '별' 로 끝나지 않는다(실측).
        if stem.endswith("별"):
            base = _strip_byeol(stem)
            if is_covered(base):
                continue
            # 감지기가 이미 잡은 축이면 그쪽 경로에서 거부된다.
            if detected and base in ("연령", "나이", "성", "지역"):
                continue
            stem = base
        if len(stem) < 2:
            continue
        if stem not in leftover:
            leftover.append(stem)
    return leftover


def _unsupported_quick_stat_response(
    query: str,
    param: QuickStatParam,
    dimensions: list[str],
    region: str,
    period: str,
    terms: Optional[list[str]] = None,
) -> dict[str, Any]:
    """빠른 경로가 질문의 조건을 표현하지 못할 때의 거부 응답.

    `dropped_dimensions` 는 감지기가 잡은 **차원 이름**, `dropped_terms` 는 잔여 관문이 남긴
    **낱말**이다. 둘을 섞지 않는다 — 기존 소비자는 차원 이름을 기대한다.
    """
    terms = list(terms or [])
    answer = (
        "quick_stat은 단일 통계값 도구라 질문에 포함된 추가 필터를 안전하게 반영하지 못합니다. "
        "기본값으로 대체하지 않고 중단했습니다."
    )
    if terms:
        quoted = "·".join(f"'{term}'" for term in terms)
        answer += f" 질문의 {quoted} 을(를) 이 지표로는 나눠 볼 수 없습니다."
    return {
        "상태": "failed",
        "코드": STATUS_UNVERIFIED_FORMULA,
        "status": "unsupported",
        "이행_상태": "unsupported",
        "actual_query_supported": False,
        "actual_value_retrieved": False,
        "질문": query,
        "answer": answer,
        "통계표": param.tbl_nm,
        "통계표ID": param.tbl_id,
        "기관ID": param.org_id,
        "요청_지역": region,
        "요청_기간": period,
        "누락_차원": dimensions,
        "dropped_dimensions": dimensions,
        "누락_한정어": terms,
        "dropped_terms": terms,
        "권고": [
            f"explore_table('{param.org_id}', '{param.tbl_id}')로 분류축을 확인하세요.",
            "연령·성별·권역·시계열 조건은 raw 다축 호출 도구가 필요합니다.",
        ],
    }


def _attach_ignored_params(result: Any, ignored: list[str], context: str) -> Any:
    """Attach unsupported-parameter warning to a quick-stat-like response."""
    if not ignored or not isinstance(result, dict):
        return result
    result["⚠️ 무시된_파라미터"] = ignored
    result["⚠️ 무시된_파라미터_안내"] = (
        f"{context} 함수는 정해진 파라미터(query/region/period 등)만 받습니다. "
        "industry, scale, sector 등 추가 슬라이싱은 자연어 query에 키워드를 "
        "포함하거나 search_kosis로 통계표 ID를 먼저 확인해야 합니다."
    )
    return result
