import pytest
from kosis_analysis.evidence import build_stat_evidence


def evidence(rows, **extra):
    return build_stat_evidence({"status": "no_data", "org_id": "142", "tbl_id": "DT_AVAILABILITY",
                                "source": "KOSIS", "rows": rows, **extra}, tool="query_table")


@pytest.mark.parametrize("symbol", ["*", "＊"])
def test_withheld_rows_are_not_absent_rows_or_numeric_success(symbol):
    result = evidence([{"value": None, "value_raw": symbol, "missing_reason": "suppressed", "unit": "억원", "period": "2022"}])
    assert result["execution"]["status"] == "no_data"
    assert result["execution"]["failure_class"] == "observations_withheld"
    assert result["fulfillment"]["status"] == "unavailable"
    assert result["fulfillment"]["completeness"] == "incomplete"
    assert result["evidence"]["observations"][0]["value"] is None


@pytest.mark.parametrize("rows", [[], [{"value": None, "unit": "억원", "period": "2022"}],
                                  [{"value": 0, "unit": "억원", "period": "2022"}]])
def test_absence_blank_and_zero_do_not_manufacture_withholding(rows):
    assert evidence(rows)["execution"]["failure_class"] == "no_rows"


def test_failed_provider_cannot_prove_withholding():
    result = evidence([{"value": None, "value_raw": "*", "missing_reason": "suppressed", "unit": "억원", "period": "2022"}],
                      fanout={"failed_calls": 1, "call_details": [{"status": "failed", "error": "timeout"}]})
    assert result["execution"]["status"] == "failed"
    assert result["execution"]["failure_class"] == "infrastructure"
