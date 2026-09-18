# -*- coding: utf-8 -*-
"""분류축 차원 이름 — 호출자가 보내는 이름이 실제 축에 닿는지 고정한다.

왜 있나 (실측, 2026-09-18).
챗봇(Nuxt)은 성별 축을 `gender` 로 보내는데 이 모듈의 정규 이름은 `sex` 이고 별칭이 없었다.
그래서 `_axis_matches_dimension(축명, "gender")` 가 `DIMENSION_AXIS_KEYWORDS.get("gender", ("gender",))`
로 떨어져 **한글 축 이름에 절대 맞지 않았다.** 성별로 나눠 달라는 요청이 조용히 무시되던 자리다.

그리고 교역 상대 축이 아예 없었다. "중국으로 수출액 얼마야?" 에 전국 수출액 98,959 가 나간 뒤
표를 다시 찾으려 해도 "국가별 축을 가진 표" 를 요구할 방법이 없었다.

축 이름은 통계조사마다 다르고 표준화되어 있지 않다. 그래서 이 키워드들은 **후보를 찾는 힌트**이지
정확성의 근거가 아니다 — 값의 정확성은 항목 라벨로 판정한다. 힌트가 새면 순위만 흔들린다.
"""
from __future__ import annotations

import pytest

from kosis_analysis.metadata import (
    _axis_matches_dimension,
    _infer_required_dimensions_from_query,
    _normalize_required_dimensions,
)


class TestNormalizeAliases:
    @pytest.mark.parametrize(("given", "expected"), [
        (["국가별"], ["country"]),
        (["국가"], ["country"]),
        (["상대국"], ["country"]),
        (["gender"], ["sex"]),          # 없으면 성별 요청이 조용히 사라진다
        (["성별"], ["sex"]),
        (["기업규모"], ["scale"]),
        (["업종별"], ["industry"]),
    ])
    def test_alias_maps_to_canonical(self, given: list[str], expected: list[str]) -> None:
        assert _normalize_required_dimensions(given) == expected

    def test_unknown_passes_through(self) -> None:
        """모르는 이름은 그대로 둔다 — 임의로 바꾸면 호출자가 뜻하지 않은 축을 얻는다."""
        assert _normalize_required_dimensions(["무엇인가"]) == ["무엇인가"]

    def test_duplicates_collapse(self) -> None:
        assert _normalize_required_dimensions(["gender", "성별", "sex"]) == ["sex"]


class TestAxisMatching:
    @pytest.mark.parametrize(("axis_name", "dimension"), [
        ("국가별", "country"),
        ("교역상대국", "country"),
        ("기업규모별", "size"),
        ("기업규모별", "scale"),
        ("산업별", "industry"),
        ("업종", "industry"),
        ("성별", "sex"),
        ("시도별", "region"),
    ])
    def test_axis_matches(self, axis_name: str, dimension: str) -> None:
        assert _axis_matches_dimension(axis_name, dimension) is True

    @pytest.mark.parametrize(("axis_name", "dimension"), [
        ("기업규모별", "country"),
        ("국가별", "industry"),
        ("산업별", "sex"),
    ])
    def test_axis_does_not_match(self, axis_name: str, dimension: str) -> None:
        assert _axis_matches_dimension(axis_name, dimension) is False


class TestInferFromQuery:
    @pytest.mark.parametrize(("query", "dimension"), [
        ("국가별 수출액", "country"),
        ("대중국 수출액", "country"),
        ("수출국별 통계", "country"),
        ("업종별 사업체 수", "industry"),
        ("기업규모별 매출액", "scale"),
    ])
    def test_infers(self, query: str, dimension: str) -> None:
        assert dimension in _infer_required_dimensions_from_query(query)

    def test_plain_question_infers_nothing(self) -> None:
        assert _infer_required_dimensions_from_query("수출액 알려줘") == []
