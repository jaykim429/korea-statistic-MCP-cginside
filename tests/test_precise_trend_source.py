"""Fresh arithmetic operands are raw observations, not a fitted regression."""
import asyncio
import pytest
import kosis_mcp_server as server


@pytest.mark.parametrize('cadence,start,end', [('M', '202401', '202402'), ('Q', '2023Q4', '20241'), ('Y', '2023', '2024')])
def test_precise_trend_retains_two_observations_and_request_metadata(monkeypatch, cadence, start, end):
    calls = []
    expected = ['202304', '202401'] if cadence == 'Q' else [start, end]
    async def fetch(*args, **kwargs):
        calls.append(kwargs)
        return [{'PRD_DE': p, 'DT': str(v), 'ITM_ID': 'T1', 'ITM_NM': '혼인건수', 'UNIT_NM': '건'} for p, v in zip(expected, [10, 11])]
    monkeypatch.setattr(server, '_fetch_series', fetch)
    result = asyncio.run(server.quick_trend('혼인건수', api_key='dummy', start_period=start, end_period=end, period_type=cadence))
    assert calls[0]['start_year'] == expected[0]
    assert calls[0]['end_year'] == expected[1]
    assert calls[0]['latest_n'] is None
    assert result['period_selection_mode'] == 'explicit_range'
    assert result['requested_period']['start_period'] == expected[0]
    assert result['데이터수'] == 2
    envelope = result['stat_evidence']
    assert envelope['tool'] == 'quick_trend'
    assert envelope['execution']['status'] == 'executed'
    assert envelope['fulfillment']['status'] == 'exact'
    assert envelope['fulfillment']['completeness'] == 'complete'
    assert envelope['evidence']['source']['label'] == '통계청 KOSIS'
    assert envelope['evidence']['source']['url'].startswith('https://kosis.kr/statHtml/statHtml.do?')
    assert envelope['evidence']['period']['cadence'] == cadence
    assert len(envelope['evidence']['observations']) == 2


@pytest.mark.parametrize('kwargs', [dict(period_type='M', start_period='202413'), dict(period_type='M', start_period='2024Q1'),
    dict(period_type='Q', start_period='2024001'), dict(period_type='Q', start_period='202404', end_period='202401'),
    dict(period_type='M', start_year='2024', start_period='202401'), dict(latest_count=0), dict(latest_count=101)])
def test_invalid_precision_never_fetches_or_silently_shrinks(monkeypatch, kwargs):
    async def forbidden(*args, **kw):
        pytest.fail('invalid request fetched source')
    monkeypatch.setattr(server, '_quick_trend_core', forbidden)
    result = asyncio.run(server.quick_trend('혼인건수', api_key='dummy', **kwargs))
    assert result['status'] == 'invalid_input'


def test_quarter_alias_is_normalized_at_actual_api_boundary(monkeypatch):
    calls = []
    async def call(client, endpoint, params):
        calls.append(params)
        return []
    monkeypatch.setattr(server, '_kosis_call', call)
    async def no_region_metadata(*args, **kwargs): return []
    monkeypatch.setattr(server, '_fetch_meta', no_region_metadata)
    asyncio.run(server._fetch_series(None, 'dummy', server.TIER_A_STATS['혼인건수'], None,
        period_type='Q', start_year='2023Q4', end_year='20241'))
    assert (calls[0]['startPrdDe'], calls[0]['endPrdDe']) == ('202304', '202401')


def test_unmapped_source_has_structured_failure_for_bounded_discovery():
    result = asyncio.run(server.quick_trend('존재하지않는xyz987통계', api_key='dummy'))
    assert result['코드'] == server.STATUS_STAT_NOT_FOUND
    assert result['stat_evidence']['execution']['status'] == 'failed'


def test_suppressed_source_value_is_never_zero(monkeypatch):
    async def fetch(*args, **kwargs):
        return [{'PRD_DE': '202401', 'DT': '*', 'ITM_ID': 'T1', 'ITM_NM': '혼인건수', 'UNIT_NM': '건'},
                {'PRD_DE': '202402', 'DT': '0', 'ITM_ID': 'T1', 'ITM_NM': '혼인건수', 'UNIT_NM': '건'}]
    monkeypatch.setattr(server, '_fetch_series', fetch)
    result = asyncio.run(server.quick_trend('혼인건수', api_key='dummy', start_period='202401', end_period='202402', period_type='M'))
    rows = result['stat_evidence']['evidence']['observations']
    assert rows[0]['value'] is None
    assert rows[0]['observation_state'] == 'suppressed'
    assert rows[1]['value'] == '0'
