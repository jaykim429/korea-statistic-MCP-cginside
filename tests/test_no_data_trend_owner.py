import asyncio
import pytest
import kosis_mcp_server as server


@pytest.mark.parametrize('query,start', [('혼인건수', '2022'), ('혼인건수 최근 5년 추이', None), ('2022년부터 혼인건수 추이', None)])
def test_empty_trend_stays_no_data_through_native_and_answer_projection(monkeypatch, query, start):
    async def empty(*args, **kwargs): return []
    async def forbidden(*args, **kwargs): pytest.fail('empty source reopened discovery')
    monkeypatch.setattr(server, '_fetch_series', empty)
    monkeypatch.setattr(server, '_fetch_latest_current_series', empty)
    monkeypatch.setattr(server.NaturalLanguageAnswerEngine, '_answer_search_fallback', forbidden)
    engine = server.NaturalLanguageAnswerEngine('dummy')
    payload = asyncio.run(engine._answer_direct(query, '전국', '혼인건수', start_year=start, end_year=start))
    assert payload['status'] == payload['상태'] == 'no_data'
    assert payload['코드'] == 'NO_DATA'
    assert not payload['actual_query_supported']
    assert '0개 시점' not in payload['answer']
    for verbose in (False, True):
        result = server._finalize_answer_query_response(payload, query=query, region='전국', verbose=verbose)
        assert result['stat_evidence']['execution']['status'] == 'no_data'
        assert result['stat_evidence']['fulfillment']['status'] == 'unavailable'
        assert result['stat_evidence']['evidence']['observations'] == []
        assert result['stat_evidence']['fulfillment']['resolved_concepts'] == []
        assert result['stat_evidence']['evidence']['table']['table_id'] == server.TIER_A_STATS['혼인건수'].tbl_id


@pytest.mark.parametrize('value', ['0', '*'])
def test_nonempty_observation_is_not_confused_with_absent_rows(monkeypatch,value):
    async def rows(*args, **kwargs):
        return [{'PRD_DE':'2022','DT':value,'ITM_ID':'T1','ITM_NM':'혼인건수','UNIT_NM':'건'}]
    monkeypatch.setattr(server,'_fetch_series',rows)
    result=asyncio.run(server._quick_trend_core('혼인건수','전국',1,'dummy',start_year='2022',end_year='2022'))
    assert result.get('status') != 'no_data'
    assert len(result['시계열']) == 1
    assert result['시계열'][0]['값'] == value


def test_upstream_failure_is_not_relabelled_data_absence(monkeypatch):
    async def failed(*args,**kwargs): raise TimeoutError('upstream timeout')
    monkeypatch.setattr(server,'_fetch_series',failed)
    with pytest.raises(TimeoutError):
        asyncio.run(server._quick_trend_core('혼인건수','전국',1,'dummy',start_year='2022',end_year='2022'))
