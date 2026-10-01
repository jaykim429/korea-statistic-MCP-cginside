# -*- coding: utf-8 -*-
"""explore_table 이 메타 조회를 모두 실패했을 때 그 오류를 남기는가.

왜 있나 (2026-10-01 CEO 리뷰 C3).
KOSIS 가 죽어 메타 넷(TBL·ITM·PRD·SOURCE)이 모두 타임아웃이면 explore_table 은
"표를 찾지 못했다"(STAT_NOT_FOUND)를 내고 **metadata_errors 를 빈 목록으로** 보냈다.
챗봇은 장애를 표 탓으로 읽어 "이 표로는 값을 낼 수 없어요" 라고 할 수밖에 없었다.
오류 문구는 meta_errors 에 이미 있었다 — 버리지 않게 한다.
"""
from __future__ import annotations

import asyncio

import kosis_mcp_server as server


def test_전부_실패하면_메타_오류를_남긴다(monkeypatch):
    async def boom(*_args, **_kwargs):
        raise RuntimeError("[KOSIS TIMEOUT] 요청 타임아웃 — 네트워크 또는 KOSIS 서버 응답 지연")

    monkeypatch.setattr(server, "_fetch_meta", boom)
    monkeypatch.setattr(server, "_resolve_key", lambda _key: "TEST_KEY")
    out = asyncio.run(server.explore_table(org_id="101", tbl_id="DT_X"))
    signals = out["mcp_output_contract"]["current_signals"]
    assert "metadata_failed" in signals["markers_present"]
    assert signals["metadata_errors"], "메타 오류가 비어 있으면 챗봇이 장애를 표 탓으로 읽는다"
    assert all("[KOSIS TIMEOUT]" in e for e in signals["metadata_errors"])
