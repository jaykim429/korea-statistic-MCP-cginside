# -*- coding: utf-8 -*-
"""규칙 adapter 가 Nuxt 와 **같은 답**을 내는가.

왜 있나 (실측 2026-09-18).
같은 규칙이 두 런타임에 필요한데 Dockerfile 이 ``COPY . .`` 라 코드를 직접 공유할 수 없다.
낱말 목록만 맞춰도 "괄호 꼬리 제거·공백 정규화" 알고리즘은 여전히 두 구현이라 갈릴 수 있다.
그래서 **입력 → 기대 출력**을 fixture 로 두고 양쪽이 같은 파일을 읽는다.

사본이 정본과 어긋나는 것도 여기서 잡는다 — 손으로 베끼면 반드시 어긋난다.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from kosis_analysis.rules import is_total_label, is_total_label_strict, normalize_item_label

ROOT = Path(__file__).resolve().parent.parent
CASES = json.loads((ROOT / "rules" / "item-label-cases.json").read_text(encoding="utf-8"))["cases"]


@pytest.mark.parametrize("case", CASES, ids=[c["input"] or "(빈 문자열)" for c in CASES])
def test_계약_fixture(case):
    """Nuxt tests/stat-rules.spec.ts 가 같은 파일로 같은 검사를 한다."""
    assert normalize_item_label(case["input"]) == case["normalized"]
    assert is_total_label(case["input"]) is case["isTotalBroad"]


def test_fixture_가_비어_있지_않다():
    """fixture 를 못 읽고도 통과하면 이 시험은 아무것도 지키지 않는다."""
    assert len(CASES) >= 5


@pytest.mark.parametrize("word", ["전산업", "전규모", "전업종", "전연령"])
def test_좁은_목록은_넓은_낱말을_총계로_보지_않는다(word):
    assert is_total_label(word) is True
    assert is_total_label_strict(word) is False


def test_사본이_정본과_같다():
    """어긋나 있으면 이 시험이 잡는다. 고치려면 python scripts/sync_rules.py"""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "sync_rules.py"), "--check"],
        capture_output=True, text=True, cwd=str(ROOT),
    )
    assert result.returncode == 0, result.stdout + result.stderr
