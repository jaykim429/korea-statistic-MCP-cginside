"""질의 어휘 매칭 규칙 단위 검증.

이 규칙이 표 후보의 순위와 "이 표가 질문에 맞는가"를 정한다. 여기가 틀리면 엉뚱한 표가
답으로 나가는데, 지금까지는 MCP 104건 라이브 검증으로만 잡혔다(한 번에 몇 분). 순수 함수라
여기서 즉시 확인한다. 케이스는 모두 실제로 틀렸던 질의에서 가져왔다.
"""
import pytest

from kosis_analysis.text_match import (
    _content_search_query,
    _match_quality_rank,
    _normalize_typo_query,
    _normalize_typo_token,
    _query_match_quality,
    _query_token_matches_text,
    _query_tokens_for_matching,
    _split_compound_token,
)


class TestTypoNormalization:
    """'실업율'로 검색하면 KOSIS 자체가 무관한 표를 준다 — 표명 표기에 맞춰 보낸다."""

    @pytest.mark.parametrize(("written", "expected"), [
        ("실업율", "실업률"),
        ("고용율", "고용률"),
        ("물가상승율", "물가상승률"),
        ("사업채", "사업체"),
    ])
    def test_흔한_표기_흔들림을_표명_표기로_바꾼다(self, written, expected):
        assert _normalize_typo_token(written) == expected

    def test_바꿀_것이_없으면_그대로_둔다(self):
        assert _normalize_typo_token("사업체") == "사업체"
        assert _normalize_typo_token("중소기업") == "중소기업"

    def test_두_글자_율은_건드리지_않는다(self):
        # '비율' 같은 짧은 낱말까지 '률'로 바꾸면 멀쩡한 표명이 어긋난다
        assert _normalize_typo_token("비율") == "비율"

    def test_문장_안에서도_바꾸고_사이_공백을_지킨다(self):
        assert _normalize_typo_query("실업율 알려줘") == "실업률 알려줘"
        assert _normalize_typo_query("2024년 고용율 추이") == "2024년 고용률 추이"

    def test_빈_질의도_견딘다(self):
        assert _normalize_typo_query(None) == ""
        assert _normalize_typo_query("") == ""


class TestContentWords:
    """명령형·일반어가 검색어에 섞이면 엉뚱한 표가 후보로 나간다."""

    def test_일반어_관련_을_검색어에서_뺀다(self):
        # 실측: '관련'만 걸려 공무원범죄자(직무관련) 표가 후보에 들어왔다
        assert _content_search_query("아동복지 관련 중소기업 통계") == "아동복지 중소기업"

    def test_명령형과_통계_자료_같은_말은_남기지_않는다(self):
        assert _content_search_query("중소기업 사업체 수 알려줘") == "중소기업 사업체"

    def test_연도_기간_표현은_검색어가_아니다(self):
        # 넣으면 KOSIS 검색이 연도가 들어간 엉뚱한 표를 낸다
        assert "2020" not in _content_search_query("2020년부터 2023년까지 실업률")
        assert "부터" not in _content_search_query("2020년부터 2023년까지 실업률")

    def test_중복_토큰은_한_번만_남는다(self):
        # 한 글자 '수'는 내용어로 치지 않아 떨어지고, 같은 말은 한 번만 남는다
        assert _query_tokens_for_matching("사업체 사업체 수") == ["사업체"]


class TestCompoundSplit:
    """사용자는 '소상공인사업체수'처럼 붙여 쓴다 — 어떤 표명에도 그대로는 없다."""

    def test_측정_명사_앞에서_한_번_끊는다(self):
        assert _split_compound_token("소상공인사업체수") == ["소상공인", "사업체수"]
        assert _split_compound_token("중소기업종사자수") == ["중소기업", "종사자수"]

    def test_끊을_수_없으면_빈_목록을_준다(self):
        assert _split_compound_token("중소기업") == []
        assert _split_compound_token("사업체수") == []


class TestTokenMatching:
    def test_표명에_그대로_있으면_맞다고_본다(self):
        assert _query_token_matches_text("중소기업", "중소기업 사업체 수 현황")

    def test_표기가_흔들려도_맞춘다(self):
        assert _query_token_matches_text("실업율", "실업률 및 고용률")

    def test_붙여_쓴_합성어도_조각으로_맞춘다(self):
        assert _query_token_matches_text("소상공인사업체수", "소상공인 사업체 수 현황")

    def test_없는_말은_맞다고_하지_않는다(self):
        assert not _query_token_matches_text("원자력", "중소기업 사업체 수 현황")


class TestMatchQuality:
    def test_모두_걸리면_high(self):
        q = _query_match_quality("중소기업 사업체", "중소기업 사업체 수 현황")
        assert q["match_quality"] == "high"
        assert q["missing_query_terms"] == []

    def test_하나도_안_걸리면_low_이고_못_걸린_말을_알려준다(self):
        q = _query_match_quality("원자력 발전량", "중소기업 사업체 수")
        assert q["match_quality"] == "low"
        assert set(q["missing_query_terms"]) == {"원자력", "발전량"}

    def test_머리_명사가_걸린_표를_수식어만_걸린_표보다_앞에_둔다(self):
        # "부산 소상공인 사업체" 의 머리 명사는 '사업체' — 그것이 걸린 표가 앞이어야 한다
        head_hit = _query_match_quality("부산 소상공인 사업체", "전국 사업체 조사")
        modifier_hit = _query_match_quality("부산 소상공인 사업체", "부산 인구 통계")
        assert head_hit["weighted_coverage_ratio"] > modifier_hit["weighted_coverage_ratio"]

    def test_등급을_순위로_바꾼다(self):
        assert _match_quality_rank({"match_quality": "high"}) > _match_quality_rank({"match_quality": "medium"})
        assert _match_quality_rank({"match_quality": "low"}) > _match_quality_rank(None)
