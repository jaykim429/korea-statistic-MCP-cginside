# -*- coding: utf-8 -*-
"""잔여 한정어 관문 — 질문의 한정어를 표현하지 못하면 값을 내지 않는다.

왜 있나 (실측, 회귀19 2026-09-17).
  "중국으로 수출액 얼마야?"  → "2026년 7월 **전국의** 수출액은 98,959 100만달러입니다"
  "고령자 고용률"            → 그냥 "고용률" 과 같은 값
같은 98,959 가 수출액·총수출액·중소기업 수출액·중국 수출액 **네 질문의 답**으로 쓰였고,
판정기는 값의 모양만 보므로 넷 다 통과로 셌다. 값 자체는 정확한 전국 수출액이다.
틀린 것은 거기에 사용자의 한정어를 붙여 내보내는 부분이다.

왜 어휘 목록이 아닌가.
기존 다섯 감지기(age·gender·time_series·comparison·region_group)는 어휘 목록이라
'고령자'·'중소기업'·'중국' 이 샜다. 우리말 모집단 한정어는 열린 집합이라 목록은 반드시 샌다.
그래서 반대로 센다 — 지표·지역·시점·잡말을 덜어 내고 **남는 내용어**를 한정어로 본다.

매칭 키는 추측이 아니라 `match_direct_stat_key` 를 실제로 불러 확인한 값이다.
"""
from __future__ import annotations

import pytest

import kosis_curation as c
from kosis_analysis.quick import (
    _quick_stat_unsupported_dimensions,
    _strip_particle,
    leftover_qualifier_terms,
)

router = c.DEFAULT_ROUTER


def leftover(query: str) -> list[str]:
    """운영과 같은 방법으로 잔여를 구한다 — 키를 라우터에서 얻어 넘긴다."""
    key = router.match_direct_stat_key(query) or ""
    param = c.TIER_A_STATS.get(key)
    return leftover_qualifier_terms(query, param, key)


def matched_key(query: str) -> str:
    return router.match_direct_stat_key(query) or ""


class TestQualifierSurvives:
    """한정어는 남아야 한다 — 남기지 않으면 전국 값이 그 한정어의 값처럼 나간다."""

    @pytest.mark.parametrize(("query", "expected"), [
        ("중소기업 수출액 알려줘", ["중소기업"]),
        ("중소기업의 수출액", ["중소기업"]),
        ("중국으로 수출액 얼마야?", ["중국"]),
        ("중국에서 수입액", ["중국"]),
        ("대중국 수출액 알려줘", ["대중국"]),
        ("고령자 고용률", ["고령자"]),
        ("고령자 취업자 수 알려줘", ["고령자"]),
        ("장애인 고용률 알려줘", ["장애인"]),
        ("반도체 수출액 알려줘", ["반도체"]),
    ])
    def test_qualifier_is_leftover(self, query: str, expected: list[str]) -> None:
        assert leftover(query) == expected

    def test_matched_key_collapses_to_unqualified_indicator(self) -> None:
        """관문이 필요한 이유 — 한정어가 붙어도 큐레이션은 무한정 지표로 접는다."""
        assert matched_key("중소기업 수출액 알려줘") == "수출액"
        assert matched_key("중국으로 수출액 얼마야?") == "수출액"
        assert matched_key("고령자 고용률") == "고용률"


class TestLegitimateIndicatorPasses:
    """정당한 지표는 거절되지 않는다 — 지표 이름에 든 말은 한정어가 아니다."""

    @pytest.mark.parametrize("query", [
        "수출액 알려줘",
        "전국 수출액",
        "우리나라 수출액",        # 지역 목록에 '우리나라' 가 없어 오탐 거절됐다(실측)
        "총수출액 알려줘",        # 동의어로 덮는다
        "현재 수출액",
        "요즘 합계출산율",
        "작년 출생아 수는?",      # '수는' → '수' 는 계량어지 내용어가 아니다
        "부산 사업체 수",
        "취업자 수 알려줘",
        "실업률 알려줘",
        "중소기업 사업체 수",
        "소상공인 사업체 수 알려줘",
        "전체 사업체 수",
        "제조업 중소기업 사업체 수 알려줘",   # 동적 키가 업종·규모를 통째로 덮는다
        "청년 실업률",                      # 지표 이름 자체에 '청년' 이 들어 있다
    ])
    def test_no_leftover(self, query: str) -> None:
        assert leftover(query) == []

    def test_real_gdp_growth_basis_is_a_verified_metric_definition(self):
        assert leftover('한국 경제성장률 최신 연간 실질 GDP 성장률') == []
        assert '명목' in leftover('명목 GDP 경제성장률')

    def test_dynamic_key_covers_industry_and_scale(self) -> None:
        """동적 확장 108개(업종×기업규모×지표)가 오탐 거절되지 않는다는 근거."""
        assert matched_key("제조업 중소기업 사업체 수 알려줘") == "제조업_중소기업_사업체수"

    @pytest.mark.parametrize("query", [
        "2023년 도매 및 소매업 중소기업 매출액 알려줘",
        "중소기업 도매 및 소매업 기업 수 알려줘",
        "소상공인 숙박 및 음식점업 기업 수 알려줘",
    ])
    def test_composed_official_industry_is_not_an_additional_filter(self, query):
        assert leftover(query) == []

    @pytest.mark.parametrize("query", [
        "도매업 중소기업 매출액", "소매업 중소기업 매출액", "도매 및 소매업 제외 중소기업 매출액",
    ])
    def test_alias_does_not_erase_component_or_exclusion(self, query):
        assert leftover(query)


class TestByeolIsNotDiscarded:
    """`…별` 을 일괄 제외하지 않는다.

    `업종별·학력별·직업별` 은 축 이름이기 이전에 **사용자가 요청한 차원**이다.
    큐레이션 지표가 그 축으로 나눌 수 없으면 전국 단일값이 나가고, 그것이 이 관문이 막으려는 부류다.
    """

    @pytest.mark.parametrize(("query", "expected"), [
        ("학력별 취업자 수", ["학력"]),
        ("업종별 취업자 수", ["업종"]),
        ("취업자 수 산업별로 보여줘", ["산업"]),
        ("사업체 수를 분야별로 나눠줘", ["분야"]),
    ])
    def test_byeol_survives_as_stem(self, query: str, expected: list[str]) -> None:
        """표준형: `별` 을 뗀 **어간**으로 싣는다 — 다음 단계의 축 매핑이 그 형태를 쓴다."""
        assert leftover(query) == expected

    def test_byeol_removed_when_stem_is_covered(self) -> None:
        """`별` 을 뗀 어간이 지표 이름에 있으면 잔여가 아니다.

        동적 키(`제조업_중소기업_사업체수`)를 직접 넘겨 메커니즘만 본다 —
        "제조업별 사업체 수" 라는 문장 자체는 라우터가 Tier A 로 매칭하지 않는다(실측).
        """
        param = c.TIER_A_STATS["제조업_중소기업_사업체수"]
        terms = leftover_qualifier_terms(
            "제조업별 중소기업 사업체 수", param, "제조업_중소기업_사업체수"
        )
        assert terms == []


class TestParticleStemRule:
    """조사를 떼되 낱말을 먹지 않는다 — Nuxt `stripParticleIfStem` 과 같은 규칙."""

    def test_two_char_stem_is_required(self) -> None:
        assert _strip_particle("물가") == "물가"      # '물' 로 만들면 어떤 표명에나 걸린다
        assert _strip_particle("농가") == "농가"
        assert _strip_particle("중소기업의") == "중소기업"
        assert _strip_particle("중국으로") == "중국"

    def test_short_measure_word_is_not_content(self) -> None:
        """'수는' 은 계량어 '수' + 조사다. 어간 2자 규칙만으로는 걸러지지 않아 따로 본다."""
        assert leftover("작년 출생아 수는?") == []


class TestDetectorsStillWork:
    """기존 다섯 감지기의 동작을 함께 고정한다 — 지금까지 시험이 없었다."""

    @pytest.mark.parametrize(("query", "dimension"), [
        ("30대 취업자 수", "age"),
        ("청년 실업률 알려줘", "age"),
        ("여성 취업자 수", "gender"),
        ("취업자 수 추이 보여줘", "time_series"),
        ("작년 대비 취업자 수", "comparison"),
    ])
    def test_detector_fires(self, query: str, dimension: str) -> None:
        assert dimension in _quick_stat_unsupported_dimensions(query, None)

    def test_encoded_dimensions_suppress_detector(self) -> None:
        param = c.QuickStatParam(
            org_id="101", tbl_id="X", tbl_nm="t", description="d",
            obj_l1="00", item_id="T1", unit="명",
            encoded_dimensions=("age",),
        )
        assert "age" not in _quick_stat_unsupported_dimensions("30대 취업자 수", param)


class TestEncodedTerms:
    """지표가 스스로 선언한 말은 잔여가 아니다."""

    def test_encoded_term_covers_leftover(self) -> None:
        param = c.QuickStatParam(
            org_id="101", tbl_id="X", tbl_nm="수출입동향", description="수출액",
            obj_l1="00", item_id="T10", unit="100만달러",
        )
        assert leftover_qualifier_terms("중소기업 수출액", param, "수출액") == ["중소기업"]

        declared = c.QuickStatParam(
            org_id="101", tbl_id="X", tbl_nm="수출입동향", description="수출액",
            obj_l1="00", item_id="T10", unit="100만달러",
            encoded_terms=("중소기업",),
        )
        assert leftover_qualifier_terms("중소기업 수출액", declared, "수출액") == []


def test_empty_query_is_safe() -> None:
    assert leftover_qualifier_terms("", None, "") == []
