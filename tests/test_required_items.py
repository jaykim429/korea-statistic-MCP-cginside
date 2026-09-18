# -*- coding: utf-8 -*-
"""항목 라벨로 표를 거르는 조건 — 축 이름에 기대지 않는다.

왜 있나 (실측, 회귀19~21).
"중소기업 수출액" 에 시도별 총수출액 98,959 가 나갔다. 표 선택이 축 **이름**만 보기 때문이다.
`_dimension_coverage` 는 `OBJ_NM` 에 키워드가 들어 있는지만 보고 항목은 보지 않는다.
그런데 KOSIS 축 이름은 통계조사마다 다르고 표준화돼 있지 않다 — 같은 `size` 차원에
규모·기업규모·종사자규모·매출액규모가 모두 쓰인다. 축 이름으로 거르면 정상적으로 샌다.

정확성은 **항목 라벨**이 맡는다. "중소기업" 을 나눠 볼 수 있는 표란 어느 축이든 항목에
'중소기업' 이 있는 표다. 축이 뭐라 불리든 상관없다.

탈락은 점수가 아니라 `status` 가 정한다 — 점수식만 고치면 `rejected_missing_dimensions`
한 줄이 그대로 떨어뜨린다. 그래서 status 규칙을 함께 고정한다.
"""
from __future__ import annotations

import pytest

from kosis_analysis.metadata import (
    MetadataCompatibilityScorer,
    TableMetadataProfile,
    _item_coverage,
    _normalize_item_label,
)


def _profile(axes: dict, table_name: str = "표") -> TableMetadataProfile:
    return TableMetadataProfile(
        org_id="101", tbl_id="T1", table_name=table_name,
        axes=axes, axis_order=list(axes), period_rows=[{"PRD_SE": "Y", "END_PRD_DE": "2024"}],
    )


def _axis(obj_nm: str, labels: list[str]) -> dict:
    return {"OBJ_NM": obj_nm, "items": {f"i{n}": {"label": label} for n, label in enumerate(labels)}}


class TestNormalizeItemLabel:
    @pytest.mark.parametrize("raw,expect", [
        ("중소기업(300인 미만)", "중소기업"),   # 괄호 꼬리는 같은 집단을 달리 적은 것이다
        ("중소 기업", "중소기업"),              # 공백도 표마다 다르다
        ("  대기업  ", "대기업"),
        (None, ""),
    ])
    def test_정규화(self, raw, expect):
        assert _normalize_item_label(raw) == expect


class TestItemCoverage:
    def test_축_이름을_보지_않는다(self):
        # 어느 차원 키워드에도 없는 축 이름이어도 항목이 맞으면 통과한다
        axes = {"A": _axis("규모구분", ["전체", "중소기업", "대기업"])}
        matched, missing, evidence = _item_coverage(axes, ["중소기업"])
        assert matched == ["중소기업"]
        assert missing == []
        assert evidence[0]["OBJ_ID"] == "A"
        assert evidence[0]["ITM_NM"] == "중소기업"

    def test_축_이름이_맞아도_항목이_없으면_떨어진다(self):
        axes = {"A": _axis("기업규모별", ["전체", "300인미만", "300인이상"])}
        matched, missing, _ = _item_coverage(axes, ["중소기업"])
        assert matched == []
        assert missing == ["중소기업"]

    def test_괄호_꼬리를_떼고_본다(self):
        axes = {"A": _axis("x", ["중소기업(300인 미만)", "대기업"])}
        assert _item_coverage(axes, ["중소기업"])[0] == ["중소기업"]

    def test_부분_문자열은_쓰지_않는다(self):
        # '여성' 이 '여성기업' 에 걸리면 다른 집단의 값이 나간다
        axes = {"A": _axis("x", ["여성기업", "일반기업"])}
        matched, missing, _ = _item_coverage(axes, ["여성"])
        assert matched == []
        assert missing == ["여성"]

    def test_총계_항목은_증거가_아니다(self):
        axes = {"A": _axis("x", ["전체", "합계", "중소기업"])}
        assert _item_coverage(axes, ["전체"])[1] == ["전체"]
        assert _item_coverage(axes, ["합계"])[1] == ["합계"]

    def test_여러_축에_걸쳐_찾는다(self):
        axes = {"A": _axis("업종별", ["제조업"]), "B": _axis("규모별", ["중소기업"])}
        matched, missing, _ = _item_coverage(axes, ["제조업", "중소기업"])
        assert sorted(matched) == ["제조업", "중소기업"]
        assert missing == []

    def test_하나라도_없으면_그것만_남는다(self):
        axes = {"A": _axis("국가별", ["중국", "미국"])}
        matched, missing, _ = _item_coverage(axes, ["중국", "중소기업"])
        assert matched == ["중국"]
        assert missing == ["중소기업"]

    def test_조건이_없으면_아무것도_하지_않는다(self):
        assert _item_coverage({"A": _axis("x", ["중소기업"])}, []) == ([], [], [])


class TestScorerStatus:
    """탈락은 status 가 정한다 — 점수만 봐서는 안 된다."""

    def test_항목이_없으면_버린다(self):
        scorer = MetadataCompatibilityScorer([], indicator=None, required_items=["중소기업"])
        result = scorer.evaluate(_profile({"A": _axis("기업규모별", ["전체", "300인미만"])}))
        assert result.status == "rejected_missing_items"
        assert result.missing_items == ["중소기업"]

    def test_항목_증거가_있으면_축_이름_미매칭으로_버리지_않는다(self):
        # 이 한 줄이 스펙 12.5 가 경고한 자리다 — reject_if_missing_dimensions 를 우회해야 한다.
        # 축 이름 '기업형태' 는 size 키워드(규모·기업규모·…)에 걸리지 않는다. 그래도 항목에
        # '중소기업' 이 있으므로 이 표는 물은 것을 나눠 볼 수 있다.
        scorer = MetadataCompatibilityScorer(
            ["size"], indicator=None, reject_if_missing_dimensions=True, required_items=["중소기업"],
        )
        result = scorer.evaluate(_profile({"A": _axis("기업형태", ["전체", "중소기업", "대기업"])}))
        assert result.status == "selected"
        assert result.matched_items == ["중소기업"]
        # 축 이름은 여전히 놓친 것으로 기록된다 — 힌트일 뿐이라 숨기지 않는다
        assert "size" in result.missing_dimensions

    def test_항목_조건이_없으면_기존_동작_그대로다(self):
        scorer = MetadataCompatibilityScorer(["size"], indicator=None, reject_if_missing_dimensions=True)
        result = scorer.evaluate(_profile({"A": _axis("업종별", ["제조업"])}))
        assert result.status == "rejected_missing_dimensions"
        assert result.matched_items == []

    def test_항목_증거가_점수를_축_이름보다_높인다(self):
        by_item = MetadataCompatibilityScorer([], indicator=None, required_items=["중소기업"]).evaluate(
            _profile({"A": _axis("규모구분", ["중소기업", "대기업"])}))
        by_axis_name = MetadataCompatibilityScorer(["size"], indicator=None).evaluate(
            _profile({"A": _axis("기업규모별", ["300인미만", "300인이상"])}))
        assert by_item.score > by_axis_name.score

    def test_증거가_응답에_실린다(self):
        result = MetadataCompatibilityScorer([], indicator=None, required_items=["중소기업"]).evaluate(
            _profile({"A": _axis("규모구분", ["중소기업"])}))
        body = result.to_response()
        assert body["matched_items"] == ["중소기업"]
        assert body["item_evidence"][0]["OBJ_ID"] == "A"
        assert body["compatibility"]["required_items"] == ["중소기업"]
