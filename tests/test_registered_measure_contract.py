"""Registered metric identity and word-end cases are shared with the app."""
import json
from pathlib import Path

import pytest

from kosis_analysis.rules import canonical_measure, measure_of, measure_relation

CASES = json.loads((Path(__file__).resolve().parent.parent / "rules" / "measure-labels.json").read_text(encoding="utf-8"))["cases"]


@pytest.mark.parametrize("case", CASES, ids=[case["input"] for case in CASES])
def test_registered_measure_contract(case):
    measured = measure_of(case["input"])
    assert measured == case["measure"]
    assert canonical_measure(measured) == case["canonical"]


def test_household_and_corporate_balances_are_not_interchangeable():
    assert measure_relation(measure_of("가계대출 총잔액"), measure_of("가계대출 잔액")) == "exact"
    assert measure_relation(measure_of("가계대출 총잔액"), measure_of("기업 대출잔액")) == "incompatible"


@pytest.mark.parametrize("text", ["매출 성장률", "수출액 증가율", "인구 증가율"])
def test_published_gdp_metric_is_not_any_growth_rate(text):
    assert measure_relation(measure_of(text), measure_of("경제성장률 (실질 GDP 성장률)"), ("%",)) == "incompatible"
