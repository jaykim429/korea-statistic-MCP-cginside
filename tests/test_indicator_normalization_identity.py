import asyncio

import pytest
import kosis_mcp_server as server


def normalize(monkeypatch, question, items):
    async def search(*args, **kwargs):
        return {"결과": [{"기관ID": "101", "통계표ID": "META", "통계표명": question}]}

    async def meta(*args):
        return items

    monkeypatch.setattr(server, "search_kosis", search)
    monkeypatch.setattr(server, "_fetch_meta", meta)
    return asyncio.run(server._normalize_indicator_from_kosis_meta(question, api_key="dummy"))


def item(label, axis="ITEM", unit=None):
    return {"OBJ_ID": axis, "ITM_ID": label, "ITM_NM": label, "UNIT_NM": unit}


def test_export_substring_does_not_normalize_to_enterprise_count(monkeypatch):
    result = normalize(monkeypatch, "중소기업 수출액", [item("기업수"), item("수출액", unit="달러")])
    assert result["normalized"] == "수출액"
    assert all(row["name"] != "기업수" for row in result["alternatives"])


@pytest.mark.parametrize("question, labels", [
    ("중소기업 수출액", ["기업수"]),
    ("소상공인 사업체수", ["기업수"]),  # A disclosed proxy is not a lexical synonym.
    ("기업체당 매출액", ["매출액"]),
    ("매출액", ["기업체당 매출액"]),
    ("기업체당 매출액", ["기업체당 영업비용"]),
])
def test_incompatible_or_proxy_measurement_stays_original(monkeypatch, question, labels):
    result = normalize(monkeypatch, question, [item(label) for label in labels])
    assert result["normalized"] == question
    assert result["source"] == "passthrough"


def test_a_classification_label_is_not_a_measurement_normalization(monkeypatch):
    result = normalize(monkeypatch, "중소기업 수출액", [item("수출액", "CLASS")])
    assert result["source"] == "passthrough"
    assert result["normalized"] == "중소기업 수출액"


def test_same_registered_measure_and_per_unit_basis_remain_eligible(monkeypatch):
    result = normalize(monkeypatch, "기업체당매출액", [item("기업체당 매출액", unit="백만원")])
    assert result["source"] == "kosis_meta_match"
    assert result["normalized"] == "기업체당 매출액"


def test_unknown_acronym_still_uses_actual_metadata_not_a_manual_alias(monkeypatch):
    result = normalize(monkeypatch, "GDP", [item("국내총생산(GDP)", unit="억원")])
    assert result["normalized"] == "국내총생산(GDP)"
