from __future__ import annotations

import os
import time
from typing import Any, Optional

import httpx

KOSIS_BASE = "https://kosis.kr/openapi"
API_KEY_DEFAULT = os.environ.get("KOSIS_API_KEY", "")
HTTP_TIMEOUT = 30.0


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default) or 0)
    except (TypeError, ValueError):
        return default


META_CACHE_TTL = _env_float("KOSIS_MCP_META_CACHE_TTL", 3600.0)
_META_CACHE: dict[tuple[Any, ...], tuple[float, list[dict]]] = {}

ERROR_MAP = {
    # Official KOSIS API error codes
    "10": "인증키 누락",
    "11": "인증키 만료",
    "20": "필수 변수 누락",
    "21": "잘못된 변수",
    "30": "결과 없음",
    "31": "결과 초과 (4만셀)",
    "40": "호출 제한",
    "41": "ROW 제한",
    "42": "사용자별 이용 제한 — KOSIS 관리자에게 문의 필요",
    "50": "서버 오류",
    # Common non-official codes observed in the wild
    "E001": "내부 오류 (E001) — KOSIS 공식 코드가 아닌 래퍼/네트워크 레이어 실패. 재시도 후에도 반복되면 통계표 변경 또는 차단된 파라미터 의심",
    "E002": "내부 오류 (E002) — 응답 파싱 실패 가능성",
    "INVALID_PARAM": "잘못된 파라미터 — 요청 변수(통계표 ID, 분류값, 기간) 형식 또는 조합이 KOSIS 검증을 통과하지 못함",
    "INVALID_KEY": "잘못된 인증키 — KOSIS_API_KEY 값을 다시 확인",
    "MISSING_KEY": "인증키 미설정 — KOSIS_API_KEY 환경변수가 비어 있음",
    "TIMEOUT": "요청 타임아웃 — 네트워크 또는 KOSIS 서버 응답 지연",
    "NETWORK": "네트워크 오류 — DNS/연결/SSL 실패",
    "-1": "일반 실패 — 구체적 사유가 응답에 포함되지 않음. KOSIS 일시 장애 또는 알 수 없는 클라이언트 오류",
    "0": "성공이지만 데이터 없음 — 정상 호출이나 매칭 행이 0건",
}


def _resolve_key(provided: Optional[str]) -> str:
    key = provided or API_KEY_DEFAULT
    if not key:
        raise RuntimeError("KOSIS_API_KEY 설정 필요")
    return key


async def _kosis_call(client: httpx.AsyncClient, endpoint: str, params: dict) -> list[dict]:
    url = f"{KOSIS_BASE}/{endpoint}"
    clean = {k: v for k, v in params.items() if v not in (None, "")}
    resp = await client.get(url, params=clean, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "err" in data:
        code = str(data["err"])
        if code == "30":
            return []
        raise RuntimeError(f"[KOSIS {code}] {ERROR_MAP.get(code, '미상')}")
    return data if isinstance(data, list) else [data]


async def _fetch_meta(
    client: httpx.AsyncClient,
    api_key: str,
    org_id: str,
    tbl_id: str,
    meta_type: str,
    extra_params: Optional[dict] = None,
) -> list[dict]:
    """Generic KOSIS `getMeta` call. meta_type values from the dev guide:

    TBL   — table name (국문/영문)
    ORG   — owning organization name
    PRD   — recorded period summary (start / end / 주기)
    ITM   — classification (objL*) and item (itmId) catalog with names
            and units — the dynamic alternative to hard-coding industry
            codes into TIER_A_STATS
    CMMT  — annotations attached to the table
    UNIT  — unit registry
    SOURCE — survey contact information
    WGT   — weighting metadata
    NCD   — last-updated timestamp per period
    """
    params: dict[str, Any] = {
        "method": "getMeta",
        "type": meta_type,
        "apiKey": api_key,
        "orgId": org_id,
        "tblId": tbl_id,
        "format": "json",
        "jsonVD": "Y",
    }
    if extra_params:
        params.update(extra_params)
    cache_key = (
        org_id,
        tbl_id,
        meta_type,
        tuple((key, str(value)) for key, value in sorted((extra_params or {}).items())),
    )
    now = time.time()
    if META_CACHE_TTL > 0:
        cached = _META_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return [dict(row) for row in cached[1]]
    rows = await _kosis_call(client, "statisticsData.do", params)
    if META_CACHE_TTL > 0:
        _META_CACHE[cache_key] = (now + META_CACHE_TTL, [dict(row) for row in rows])
    return rows


async def _fetch_classifications(
    org_id: str, tbl_id: str, api_key: Optional[str] = None,
) -> list[dict]:
    """Return every (OBJ_ID, OBJ_NM, ITM_ID, ITM_NM, UNIT_NM) tuple for
    a KOSIS table. Lets dispatchers map "제조업" → the right obj_l2
    code without baking industry codes into curation."""
    key = _resolve_key(api_key)
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "ITM")


async def _fetch_period_range(
    org_id: str, tbl_id: str,
    api_key: Optional[str] = None,
    detail: bool = False,
) -> list[dict]:
    """Return PRD_SE / STRT_PRD_DE / END_PRD_DE for a KOSIS table.

    Many tables expose multiple cadences (month/quarter/year) and the
    meta endpoint returns one row per cadence — use `_pick_finest_period`
    on the result to select the freshest cadence row, since `[-1]` is
    not guaranteed to be the most granular one.

    detail=True asks for every recorded timepoint (large response on
    monthly tables); detail=False asks only for the summary row (cheap,
    what staleness checks need)."""
    key = _resolve_key(api_key)
    extra = {"detail": "Y"} if detail else None
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "PRD", extra)


async def _fetch_table_name(
    org_id: str, tbl_id: str, api_key: Optional[str] = None,
) -> list[dict]:
    """Return TBL_NM / TBL_NM_ENG for a KOSIS table."""
    key = _resolve_key(api_key)
    async with httpx.AsyncClient() as client:
        return await _fetch_meta(client, key, org_id, tbl_id, "TBL")

