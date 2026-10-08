import asyncio

import pytest
import kosis_mcp_server as server


def install_metadata(monkeypatch, first, recovered):
    calls = []

    async def meta(client, key, org_id, tbl_id, kind):
        return {
            "TBL": [{"TBL_NM": "주요지표"}],
            "ITM": [{"OBJ_ID": "ITEM", "OBJ_NM": "항목", "ITM_ID": "T03", "ITM_NM": "기업체당 매출액", "UNIT_NM": "백만원"}],
            "PRD": [{"PRD_SE": "Y", "STRT_PRD_DE": "2022", "END_PRD_DE": "2024"}],
        }[kind]

    async def survey(*args):
        return [{"JOSA_NM": "소상공인실태조사"}]

    async def fetch(client, path, params):
        calls.append({key: value for key, value in params.items() if key != "apiKey"})
        value = first if len(calls) == 1 else recovered
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(server, "_fetch_meta", meta)
    monkeypatch.setattr(server, "_fetch_survey_rows", survey)
    monkeypatch.setattr(server, "_kosis_call", fetch)
    return calls


def run(filters=None, **kwargs):
    return asyncio.run(server.query_table("142", "REAL", filters or {"ITEM": ["T03"]}, api_key="dummy", **kwargs))


def row(period, value):
    return {"PRD_DE": period, "DT": value, "ITM_ID": "T03", "ITM_NM": "기업체당 매출액", "UNIT_NM": "백만원"}


def test_implicit_latest_recovers_latest_actual_measurement_not_table_wide_year(monkeypatch):
    calls = install_metadata(monkeypatch, [], [row("2022", "198"), row("2023", "199"), row("2024", "-")])
    result = run()
    assert result["status"] == "executed"
    assert [(r["period"], r["value"]) for r in result["rows"]] == [("2023", 199)]
    assert result["period_range"] is None
    assert result["auto_default_period_range"] is None
    assert result["stat_evidence"]["evidence"]["period"]["used"] == "2023"
    assert len(calls) == 2 and calls[1]["newEstPrdCnt"] == 5
    assert calls[0]["itmId"] == calls[1]["itmId"]
    assert result["fanout"]["latest_observation_recovery"][0]["table_latest_period"] == "2024"


def test_explicit_period_is_not_silently_replaced(monkeypatch):
    calls = install_metadata(monkeypatch, [], [row("2023", "199")])
    result = run(period_range=["2024", "2024"])
    assert result["status"] == "no_data" and len(calls) == 1
    assert result["period_range"] == ["2024", "2024"]


def test_latest_zero_is_an_actual_observation_not_a_retry_trigger(monkeypatch):
    calls = install_metadata(monkeypatch, [row("2024", "0")], [row("2023", "199")])
    result = run()
    assert result["rows"][0]["value"] == 0 and len(calls) == 1


@pytest.mark.parametrize("failure", [{"_error": "timeout"}, asyncio.TimeoutError()])
def test_initial_infrastructure_error_does_not_start_data_recovery(monkeypatch, failure):
    calls = install_metadata(monkeypatch, failure, [row("2023", "199")])
    result = run()
    assert result["status"] == "no_data" and len(calls) == 1
    assert result["fanout"]["failed_calls"] == 1


def test_empty_recovery_is_bounded_and_stays_no_data(monkeypatch):
    calls = install_metadata(monkeypatch, [], [])
    result = run()
    assert result["status"] == "no_data" and len(calls) == 2
    assert result["fanout"]["latest_observation_recovery"][0]["status"] == "no_observation"


def test_recovery_timeout_is_retained_as_infrastructure_not_absence(monkeypatch):
    calls = install_metadata(monkeypatch, [], asyncio.TimeoutError())
    result = run()
    assert len(calls) == 2 and result["fanout"]["failed_calls"] == 1
    assert result["fanout"]["latest_observation_recovery"][0]["status"] == "timeout"


def test_multi_group_comparison_does_not_recover_to_different_periods(monkeypatch):
    calls = install_metadata(monkeypatch, [], [])
    # Exercise the actual multi-group boundary without changing request metadata.
    monkeypatch.setattr(server, "_fanout_filter_sets", lambda filters: [dict(filters), dict(filters)])
    result = run()
    assert result["status"] == "no_data" and len(calls) == 2
    assert "latest_observation_recovery" not in result["fanout"]
    assert result["period_range"] == ["2024", "2024"]
