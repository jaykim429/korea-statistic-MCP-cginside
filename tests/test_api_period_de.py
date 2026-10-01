# -*- coding: utf-8 -*-
"""query_table 이 KOSIS 에 보내는 기간 값(startPrdDe/endPrdDe) 형식.

왜 있나 (2026-10-01, 챗봇 Phase 5a T0 · followups 20).
수록주기가 여럿인 표에서 기간을 주지 않으면 query_table 은 분기 행의 END_PRD_DE 를 시작·끝으로 보낸다.
그 값이 표시 문자열 "2026 2/4" 였고, _api_period_de 는 월("2026.02")만 바꿔 분기를 **그대로** 넘겼다.
KOSIS 는 그것을 "[KOSIS 21] 잘못된 변수" 로 거절했고, 챗봇은 멀쩡한 표에서 막다른 길을 냈다
(관측 실패 넷 중 셋 — 「대륙별·국가별 중소기업 수출」 의 중소기업 수출액이 그 표에 있었다).

KOSIS 의 분기 기간 형식은 YYYY0Q 다 — 같은 표를 newEstPrdCnt 로 조회해 받은 행의 PRD_DE 가 "202602" 였다.
반기("YYYY 1/2")는 실물 형식을 재지 않았으므로 여기서 바꾸지 않는다(재지 않고 추정해 넣지 않는다).
"""
from __future__ import annotations

import pytest

from kosis_analysis.periods import _api_period_de


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("2026 2/4", "202602"),
        ("2022 4/4", "202204"),
        ("2009 1/4", "200901"),
        ("2026  3/4", "202603"),
        ("20262/4", "202602"),
    ],
)
def test_분기_표시를_KOSIS_형식으로_바꾼다(label, expected):
    assert _api_period_de(label) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026.02", "202602"),
        ("2026", "2026"),
        ("202602", "202602"),
        ("20262", "20262"),
        ("", ""),
        (None, ""),
    ],
)
def test_다른_형식은_지금처럼(value, expected):
    assert _api_period_de(value) == expected


def test_분기가_아닌_분수는_건드리지_않는다():
    # 반기 표시는 실물 형식을 재지 않았다 — 그대로 둔다
    assert _api_period_de("2026 1/2") == "2026 1/2"
    assert _api_period_de("2026 5/4") == "2026 5/4"
