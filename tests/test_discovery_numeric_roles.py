"""Discovery words keep identity; temporal cleanup never grants execution scope."""
import pytest

from kosis_analysis.text_match import _content_search_query, _normalize_typo_query, _normalize_typo_token


@pytest.mark.parametrize('query, retained', [
    ('1인가구 수 알려줘', '1인가구'),
    ('20대 고용률 알려줘', '20대'),
    ('65세이상 인구', '65세이상'),
    ('30인미만 사업체 수', '30인미만'),
    ('1인당 온실가스 배출량', '1인당'),
    ('5년생존율 알려줘', '5년생존율'),
    ('5년 생존율 알려줘', '5년'),
    ('업력 5년 미만 기업 수', '5년'),
    ('2020년생 인구 수', '2020년생'),
    ('2020년대 인구', '2020년대'),
    ('제10차 산업분류 기업 수', '제10차'),
    ('10인미만 20대 1인가구', '10인미만'),
    ('3개월 이동평균', '3개월'),
    ('5G 통신 서비스업', '5g'),
    ('업종 코드 2020', '2020'),
    ('지역 코드 1100', '1100'),
    ('산업 코드 1000', '1000'),
    ('2020년부터2023년까지 1인가구', '1인가구'),
    ('13월생 가구원 수', '13월생'),
])
def test_numeric_subjects_are_not_deleted(query, retained):
    assert retained in _content_search_query(query).split()


@pytest.mark.parametrize('query, excluded', [
    ('2020년부터 2023년까지 실업률', ('2020년부터', '2023년까지')),
    ('2020년 2023년 실업률', ('2020년', '2023년')),
    ('2020년부터2023년까지 실업률', ('2020년부터2023년까지',)),
    ('2025년1분기 실업률', ('2025년1분기',)),
    ('2025년12월 실업률', ('2025년12월',)),
    ('최근5년 매출액', ('최근5년',)),
    ('지난12개월 실업률', ('지난12개월',)),
    ('최근 5년 매출액', ('5년',)),
    ('지난 12개월 실업률', ('12개월',)),
    ('직전 4분기 GDP', ('4분기',)),
    ('2025년01월 실업률', ('2025년01월',)),
    ('2025년제1분기 실업률', ('2025년제1분기',)),
])
def test_closed_calendar_or_fused_window_tokens_are_not_search_subjects(query, excluded):
    result = _content_search_query(query).split()
    assert not any(term in result for term in excluded)


@pytest.mark.parametrize('query', ['최근 5년 생존율', '업력 3년 이상', '1인당 매출액', '20대 실업률'])
def test_search_does_not_mutate_the_original_request(query):
    original = query
    _content_search_query(query)
    assert query == original


@pytest.mark.parametrize('written, expected', [
    ('실업율', '실업률'), ('고용율', '고용률'), ('물가상승율', '물가상승률'),
    ('출산율', '출산율'), ('합계출산율', '합계출산율'), ('출산률', '출산율'),
    ('증가율', '증가율'), ('매출액증가율', '매출액증가율'), ('증가률', '증가율'),
    ('감소율', '감소율'), ('이자율', '이자율'), ('상승률', '상승률'),
    ('백분율', '백분율'), ('백분률', '백분율'), ('참가율', '참가율'),
    ('할인율', '할인율'), ('할인률', '할인율'), ('취업률', '취업률'),
    ('생존율', '생존율'), ('생존률', '생존율'), ('비율', '비율'),
    ('환율', '환율'), ('효율', '효율'), ('법률', '법률'),
    ('사업채', '사업체'), ('GDP', 'GDP'), ('rate율', 'rate율'),
])
def test_rate_spelling_preserves_correct_terms_and_repairs_both_directions(written, expected):
    assert _normalize_typo_token(written) == expected
    assert _normalize_typo_token(expected) == expected


def test_normalized_query_preserves_whitespace_and_calendar():
    assert _normalize_typo_query('2024년  출산율\n고용율') == '2024년  출산율\n고용률'


@pytest.mark.parametrize('query, retained', [
    ('최근 5년 1인가구 수', '1인가구'),
    ('지난 12개월 20대 실업률', '20대'),
    ('직전 4분기 1인당 매출액', '1인당'),
    ('5년 생존율', '5년'),
    ('최근5년생존율', '최근5년생존율'),
    ('2020년생 1인가구', '2020년생'),
    ('최근 5년 업종 코드 2020', '2020'),
    ('2025년13월 인구', '2025년13월'),
])
def test_explicit_window_cleanup_keeps_other_numeric_identity(query, retained):
    assert retained in _content_search_query(query).split()
