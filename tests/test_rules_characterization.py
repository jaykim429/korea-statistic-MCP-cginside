# -*- coding: utf-8 -*-
"""규칙의 **지금 동작**을 옮기기 전에 고정한다 (특성화 시험).

왜 있나 (실측 2026-09-18).
총계 라벨과 항목 라벨 정규화가 Nuxt 와 여기 두 벌로 구현돼 있다. 정본 하나로 모으는 것이
목표인데, 옮기면서 동작이 바뀌면 그것을 알아챌 방법이 지금 없다.

Phase 1 동안 기대값이 바뀌면 리팩터링이 동작을 건드린 것이다.
"""
from __future__ import annotations

import pytest

from kosis_analysis.rules import is_total_label as _is_total
from kosis_analysis.rules import normalize_item_label as _normalize_item_label


class _TOTAL_ITEM_LABEL:  # noqa: N801 - 옛 정규식 자리. 호출 모양만 유지하고 기대값은 그대로 둔다.
    @staticmethod
    def match(label):
        return True if _is_total(label) else None


class TestNormalizeNow:
    @pytest.mark.parametrize("raw,expect", [
        ("중소기업(300인 미만)", "중소기업"),
        ("중소 기업", "중소기업"),
        ("  대기업  ", "대기업"),
        ("전체", "전체"),
        (None, ""),
        ("", ""),
    ])
    def test_지금_정규화하는_모양(self, raw, expect):
        assert _normalize_item_label(raw) == expect

    @pytest.mark.parametrize("raw,expect", [(0, ""), (False, ""), ("0", "0")])
    def test_비문자열_falsy_는_빈_문자열이_된다(self, raw, expect):
        """`str(label or "")` 이라 0 과 False 가 빈 문자열이 된다.

        **Nuxt 는 다르다** — `String(name ?? '')` 이라 0 은 "0", false 는 "false" 가 된다
        (실측 2026-09-21). 규칙이 두 곳에 있으면 어긋난다는 것의 실증이다.

        정본으로 모을 때 **어느 쪽에 맞춰도 한쪽 동작이 바뀌므로** 이 차이는 건드리지 않는다.
        계약 fixture 는 문자열 입력만 담고, 비문자열 처리 통일은 별도 결정으로 미룬다
        (설계 8.1-3: 옮기면서 발견한 결함은 적어 두고 따로 고친다).
        """
        assert _normalize_item_label(raw) == expect


class TestTotalNow:
    @pytest.mark.parametrize("label", [
        "계", "소계", "합계", "총계", "전체", "전국",
        "전산업", "전규모", "전업종", "전연령", "전체산업", "전체기업",
    ])
    def test_총계로_보는_낱말(self, label):
        assert _TOTAL_ITEM_LABEL.match(label) is not None

    @pytest.mark.parametrize("label", ["중소기업", "제조업", "서울", "여성"])
    def test_총계가_아닌_낱말(self, label):
        assert _TOTAL_ITEM_LABEL.match(label) is None

    def test_정본은_정규화를_품는다(self):
        """Phase 1c 에서 바뀐 **유일한** 동작이다.

        옛 ``_TOTAL_ITEM_LABEL`` 은 정규식이라 '전체 산업'(공백)이 그대로는 안 걸렸고,
        호출자가 먼저 ``_normalize_item_label`` 을 통과시켜야 했다. 정본
        ``is_total_label`` 은 정규화를 품으므로 바로 걸린다.

        Nuxt 도 같은 자리 한 곳만 바뀌었다(' 전체 ' → true). 그 손질이 호출부마다
        조금씩 달랐던 것이 세 목록이 갈린 원인 중 하나다.

        이 파일에서 Phase 1 동안 기대값이 바뀐 것은 여기뿐이다 — 옮긴 직후 26건 중
        이 1건만 실패했다(실측 2026-09-21). 다른 줄이 바뀌면 리팩터링이 동작을 건드린 것이다.
        """
        assert _TOTAL_ITEM_LABEL.match("전체 산업") is not None
        assert _TOTAL_ITEM_LABEL.match(_normalize_item_label("전체 산업")) is not None
