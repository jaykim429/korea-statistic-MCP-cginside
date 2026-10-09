import asyncio

import httpx
import pytest

from kosis_analysis.evidence import attach_stat_evidence
from kosis_analysis.primary_series import AnnualTableParser, SERIES, match_primary_series, query_primary_series, valid_native_identity


def html(values=None, unit="천 개", label="벤처기업수(천&nbsp;개)"):
    values = values if values is not None else [("2020Y", "39.5"), ("2021Y", "38.3"), ("2022Y", "35.1"), ("2023Y", "40.1")]
    return '<p id="sttsCdUnit">[단위 : ' + unit + ']</p><table id="t_Table_500701"><tbody><tr>' + (
        '<th item-id="T01">' + label + '</th>' + ''.join(f'<td item-id="{year}">{value}</td>' for year, value in values)) + '</tr></tbody></table>'


def fetch(query, content=None, **kwargs):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=content or html()))) as client:
            return await query_primary_series(query, client=client, **kwargs)
    return asyncio.run(run())


def test_primary_values_retain_native_identity_and_rounded_unit_not_fake_kosis_codes():
    result = fetch("2020~2023년 벤처기업 수 추이 알려줘")
    assert [row["value"] for row in result["rows"]] == ["39.5", "38.3", "35.1", "40.1"]
    assert all(row["unit"] == "천 개" for row in result["rows"])
    assert "반올림" in result["data_quality_note"]
    assert "org_id" not in result and "tbl_id" not in result
    evidence = attach_stat_evidence(result, tool="answer_query")["stat_evidence"]
    assert evidence["fulfillment"]["completeness"] == "complete"
    assert evidence["evidence"]["source"]["native_identity"] == SERIES[0].identity
    assert evidence["evidence"]["table"]["org_id"] is None


@pytest.mark.parametrize("query", ["여성 벤처기업 수", "서울 벤처기업 수", "벤처기업 수 업종별", "벤처확인기업 수", "ICT 벤처기업 수", "벤처기업 매출액", "벤처기업 수 비율", "KOSIS 벤처기업 수"])
def test_unresolved_roles_never_use_a_registered_national_scalar(query):
    assert match_primary_series(query) is None
    assert fetch(query) is None


def test_year_gap_never_substitutes_latest_or_claims_full_coverage():
    result = fetch("2020~2023년 벤처기업 수", html([("2020Y", "39.5"), ("2023Y", "40.1")]))
    assert result["status"] == "PERIOD_NOT_FOUND"
    assert "rows" not in result


@pytest.mark.parametrize("content", [html(unit="%"), html(label="벤처투자액(천 개)"), html([("2020Y", "39.5"), ("2020Y", "39.6")]), html([("2020Y", "-")]), html().replace("t_Table_500701", "t_Table_999999")])
def test_schema_drift_and_missing_values_fail_closed(content):
    assert fetch("2020~2023년 벤처기업 수", content)["status"] == "failed"


def test_unknown_provider_identity_does_not_satisfy_the_source_contract():
    identity = {**SERIES[0].identity, "dataset_id": "999999"}
    assert not valid_native_identity(identity)
    result = attach_stat_evidence({"status": "executed", "source": "unknown", "source_identity": identity,
                                  "rows": [{"value": "1", "unit": "개", "period": "2023"}]}, tool="answer_query")
    assert result["stat_evidence"]["fulfillment"]["completeness"] == "incomplete"


def test_requested_endpoints_and_latest_are_distinct():
    assert len(fetch("벤처기업 수")['rows']) == 1
    assert [row["period"] for row in fetch("벤처기업 수", start_year="2020", end_year="2023")["rows"]] == ["2020", "2021", "2022", "2023"]
