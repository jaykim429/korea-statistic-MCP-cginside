"""질문 낱말 ↔ KOSIS 표 제목 낱말 다리(앱 사이클 7).

정답 표는 제목으로 검색하면 0~4위로 나오는데, 질문 낱말(사육두수·약국 수·신규 창업·화재 사망)로는 상위 40 안에도
없었다. 다리는 그 갈래의 표 제목 어휘를 내고, search_kosis 는 Tier B 추천어와 따로 그 앞 2 개를 더 검색한다
(Tier B 앞 3 개는 의도어 「지역별」 이 먼저 채운다). 넓은 낱말에는 내지 않는다 — 「농가 소득」 에 마릿수 표,
「국회의원 수당」 에 요양기관 표가 끼면 다른 질문의 잡음이 된다.
"""
import asyncio

import pytest

import kosis_curation as c
import kosis_mcp_server as server


@pytest.mark.parametrize("query,term", [
    ("한우 사육두수 시도별", "농장수 및 마릿수"),
    ("오리 사육 마릿수 지역별", "농장수 및 마릿수"),
    ("돼지 사육 농가 수 시도별", "농장수 및 마릿수"),
    ("한우 농장 수 지역별", "농장수 및 마릿수"),
    ("약국 수 시도별", "종별 요양기관 현황"),
    ("치과의원 수 지역별", "종별 요양기관 현황"),
    ("의료기관 수 지역별", "종별 요양기관 현황"),
    ("커피 전문점 신규 창업 수", "생활밀접업종 신규 사업자 현황"),
    ("편의점 개업 수 지역별", "생활밀접업종 신규 사업자 현황"),
    ("화재 사망자 수 시설 용도별", "장소별 인명피해현황"),
    ("화재 재산피해액 시설 용도별", "장소별 재산피해현황"),
    ("화재 발생 건수 시설 용도별", "장소별 화재발생현황"),
])
def test_bridge_gives_table_title_vocabulary_first(query, term):
    assert term in c.title_vocabulary_hints(query)[:2]


@pytest.mark.parametrize("query", [
    "농가 소득 추이", "스마트팜 도입 현황", "국회의원 수당", "동물병원 수", "의료기관 매출액",
    "창업기업 수 업종별", "화재보험 가입률", "고령인구비율 시도별",
])
def test_bridge_does_not_fire_on_broad_words(query):
    assert c.title_vocabulary_hints(query) == []


def test_rewrites_swap_the_bridge_key_for_table_title_vocabulary():
    assert c.title_vocabulary_rewrites("한우 사육두수 시도별")[0] == "한우 농장수 및 마릿수 시도별"
    assert c.title_vocabulary_rewrites("치과의원 수 지역별")[0] == "종별 요양기관 현황 지역별"
    assert c.title_vocabulary_rewrites("고령인구비율 시도별") == []


def test_bridge_found_table_is_scored_against_the_rewritten_query():
    # 다리로 찾은 표는 원문 낱말(사육두수)과 맞지 않아 낮게 매겨지거나 「내용어 없음」 으로 빠졌다 — 바꿔 쓴 질문으로도 매긴다.
    title = "축종별 시도별 농장수 및 마릿수"
    plain = server._best_query_match_quality("한우 사육두수 시도별", [], title)
    bridged = server._best_query_match_quality("한우 사육두수 시도별", c.title_vocabulary_rewrites("한우 사육두수 시도별"), title)
    assert bridged["weighted_coverage_ratio"] > plain["weighted_coverage_ratio"]
    assert bridged["matched_terms"]
    # 다리가 없는 질문은 지금 그대로.
    assert server._best_query_match_quality("고령인구비율 시도별", [], "시도별 고령인구비율") == server._query_match_quality("고령인구비율 시도별", "시도별 고령인구비율")


def test_search_kosis_searches_the_bridge_terms_beside_tier_b(monkeypatch):
    seen = {}

    async def fake(query, keywords, limit, api_key, **kwargs):
        seen["keywords"] = keywords
        return {"결과": []}

    monkeypatch.setattr(server, "_search_kosis_keywords", fake)
    tool = getattr(server.search_kosis, "fn", server.search_kosis)
    asyncio.run(tool("한우 사육두수 시도별", limit=5))
    assert "농장수 및 마릿수" in seen["keywords"]
    assert "가구수 및 마릿수" in seen["keywords"]
    asyncio.run(tool("고령인구비율 시도별", limit=5))
    assert "농장수 및 마릿수" not in seen["keywords"]
