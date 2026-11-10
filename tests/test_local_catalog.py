"""로컬 KOSIS 표 목록 검색(앱 사이클 8) — 글자 2-gram BM25.

KOSIS 통합검색은 질문 낱말과 제목 낱말이 다르면 정답 표를 상위 40 밖으로 냈다. 전체 표 목록(제목 + 분류 경로)을 로컬에
두고 같은 질문을 다시 찾는다. 사전 없이 띄어쓰기·복합어·어순 차이를 흡수하는 것이 이 단계의 몫이다(동의어는 다음 단계의 벡터).
"""
from kosis_analysis.local_catalog import LocalCatalog, char_bigrams, word_bigrams

ROWS = [
    {"orgId": "101", "tblId": "DT_1EO200", "title": "축종별 시도별 농장수 및 마릿수", "path": ["농림", "가축동향조사"]},
    {"orgId": "354", "tblId": "DT_MIRE01", "title": "시도별 종별 요양기관 현황", "path": ["보건", "의료자원"]},
    {"orgId": "101", "tblId": "DT_1B040A3", "title": "행정구역(시군구)별, 성별 인구수", "path": ["인구", "주민등록인구현황"]},
    {"orgId": "118", "tblId": "DT_11806_N001", "title": "전체 산업재해 현황 및 분석-규모별", "path": ["노동", "산업재해현황"]},
]


def test_char_bigrams_ignore_spacing_and_punctuation():
    assert char_bigrams("요양기관 현황") == char_bigrams("요양기관현황")
    assert char_bigrams("시군구(시도)") == char_bigrams("시군구 시도")


def test_spacing_variant_finds_the_same_table():
    catalog = LocalCatalog(ROWS)
    assert catalog.search("요양기관현황 시도", 2)[0][0]["tblId"] == "DT_MIRE01"
    assert catalog.search("산업 재해 규모", 2)[0][0]["tblId"] == "DT_11806_N001"


def test_word_order_does_not_matter():
    catalog = LocalCatalog(ROWS)
    assert catalog.search("마릿수 농장수 축종별", 1)[0][0]["tblId"] == "DT_1EO200"


def test_path_context_adds_survey_words():
    # 「가축」 은 제목에 없고 분류 경로(가축동향조사)에만 있다 — 경로를 문맥으로 붙이면 찾고, 제목만이면 못 찾는다.
    def score_of(catalog: LocalCatalog, query: str) -> float:
        return next((score for row, score in catalog.search(query, 10) if row["tblId"] == "DT_1EO200"), 0.0)

    assert LocalCatalog(ROWS, with_path=True).search("가축 시도별", 1)[0][0]["tblId"] == "DT_1EO200"
    assert score_of(LocalCatalog(ROWS, with_path=True), "가축") > 0
    assert score_of(LocalCatalog(ROWS, with_path=False), "가축") == 0


def test_word_bounded_grams_do_not_join_neighbouring_words():
    # 공백을 지우고 2-gram 을 만들면 낱말 경계를 넘는 글자 쌍(「개업 수」 의 「업수」)이 「중소기업 수출」 에 맞는다.
    # 낱말 안 2-gram 은 그런 쌍을 만들지 않는다 — 띄어쓰기 차이는 경계 한 쌍만 잃는다.
    rows = [{"orgId": "142", "tblId": "DT_B10062", "title": "지역별 중소기업 수출", "path": ["기업"]}]

    def score(grams: str, query: str) -> float:
        hits = LocalCatalog(rows, with_path=False, grams=grams).search(query, 1)
        return hits[0][1] if hits else 0.0

    assert score("cross", "개업 수") > 0
    assert score("word", "개업 수") == 0
    assert word_bigrams("요양기관 현황") == ["요양", "양기", "기관", "현황"]
    assert LocalCatalog(ROWS, grams="word").search("요양기관현황 시도", 1)[0][0]["tblId"] == "DT_MIRE01"


def test_no_overlap_scores_nothing():
    assert LocalCatalog(ROWS).search("항공 여객 수송", 3) == []


def test_empty_catalog_is_safe():
    assert LocalCatalog([]).search("인구", 5) == []


# --- search_kosis 에 붙인 모양 ---------------------------------------------------------------------------------
import asyncio  # noqa: E402

import kosis_mcp_server as server  # noqa: E402

API_ROWS = [
    {"ORG_ID": "999", "TBL_ID": "NOISE1", "TBL_NM": "시도별 사업체 현황", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"},
    {"ORG_ID": "999", "TBL_ID": "NOISE2", "TBL_NM": "요양병원 시도별 병상", "STRT_PRD_DE": "2020", "END_PRD_DE": "2024"},
]


def run(monkeypatch, query, catalog):
    async def fetch(_client, endpoint, params):
        return [dict(row) for row in API_ROWS] if endpoint == "statisticsSearch.do" else []
    monkeypatch.setattr(server, "TIER_A_STATS", {})
    monkeypatch.setattr(server, "_kosis_call", fetch)
    monkeypatch.setattr(server, "_load_local_catalog", lambda: catalog)
    return asyncio.run(server._search_kosis_keywords(query, [query], 10, api_key="dummy"))


def test_local_hit_joins_the_candidates(monkeypatch):
    result = run(monkeypatch, "요양기관 현황 시도별", LocalCatalog(ROWS))
    ids = [row["통계표ID"] for row in result["결과"]]
    assert "DT_MIRE01" in ids
    assert ids.index("DT_MIRE01") < ids.index("NOISE2")
    assert all("_rrf" not in row for row in result["결과"])


def test_catalog_off_is_the_old_result(monkeypatch):
    before = run(monkeypatch, "요양기관 현황 시도별", None)
    assert [row["통계표ID"] for row in before["결과"]] == [row["통계표ID"] for row in run(monkeypatch, "요양기관 현황 시도별", None)["결과"]]
    assert "DT_MIRE01" not in [row["통계표ID"] for row in before["결과"]]


# --- 목록 어휘로 붙여 쓴 질문 낱말 나누기(사이클 8 H6) ---------------------------------------------------------
# 순위·잡음 관문은 질문 낱말이 표 제목 안에 「통째로」 있어야 일치로 본다. 붙여 쓴 낱말(「요양기관현황시도별」)과
# 조사가 붙은 낱말(「수당은」)은 어느 제목에도 통째로 없어 로컬 목록이 찾은 정답 표가 관문에서 지워졌다(실측: 띄어쓰기
# 형태 실패 121 중 115 가 로컬 상위 8 안). 손 사전 대신 수집한 표 제목의 낱말로 나눈다 — 목록에 있는 낱말은 그대로 둔다.


def test_segment_query_splits_only_words_the_catalog_does_not_know():
    rows = ROWS + [{"orgId": "142", "tblId": "DT_X", "title": "중소기업 수출액", "path": ["기업"]}]
    catalog = LocalCatalog(rows)
    assert catalog.segment_query("요양기관현황시도별") == "요양기관 현황 시도별"
    assert catalog.segment_query("중소기업 수출액은") == "중소기업 수출액"  # 아는 낱말은 쪼개지 않고, 조사만 떨어진다
    assert catalog.segment_query("중소기업수출액") == "중소기업 수출액"
    assert catalog.segment_query("항공 여객") == "항공 여객"  # 모르는 낱말을 덮을 조각이 없으면 그대로


def test_glued_query_keeps_the_local_hit_past_the_noise_gate(monkeypatch):
    result = run(monkeypatch, "요양기관현황시도별", LocalCatalog(ROWS))
    assert "DT_MIRE01" in [row["통계표ID"] for row in result["결과"]]


# --- 로컬 단독 후보의 근거 기준(사이클 8 G) ----------------------------------------------------------------------
# 관문은 내용어가 하나만 맞아도 통과시킨다. KOSIS 에 없는 주제(「비트코인 거래량」)에서 로컬 목록이 측정어만 맞는 표
# (「지역별 전력거래량」)를 잔뜩 더했다(실측: 1 건 → 10 건). 통합검색이 내지 않은 로컬 단독 후보는 질문 내용어의 절반 넘게를 덮어야 들인다.


def test_local_only_candidate_needs_more_than_half_the_content_words(monkeypatch):
    rows = ROWS + [{"orgId": "388", "tblId": "DT_POWER", "title": "지역별 전력거래량", "path": ["에너지"]}]
    monkeypatch.setattr(server, "_LOCAL_MIN_COVERAGE", 0.5)
    ids = [row["통계표ID"] for row in run(monkeypatch, "비트코인 거래량", LocalCatalog(rows))["결과"]]
    assert "DT_POWER" not in ids
    ids = [row["통계표ID"] for row in run(monkeypatch, "요양기관 현황 시도별", LocalCatalog(rows))["결과"]]
    assert "DT_MIRE01" in ids
