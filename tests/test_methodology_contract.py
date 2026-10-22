import asyncio
import pytest
import kosis_mcp_server as server
from kosis_analysis.methodology import RULES, request_for, explanation_fields, methodology_payload


@pytest.mark.parametrize('fixture', RULES['requestCases'], ids=lambda fixture: fixture['question'])
def test_official_reference_requests_share_roles_with_the_application(fixture):
    result = request_for(fixture['question'])
    if not fixture['facets']:
        assert result is None
    else:
        assert result['facets'] == fixture['facets']
        assert result['role'] == fixture['role']
        assert result['requestedYears'] == fixture.get('requestedYears', [])
        if 'topic' in fixture:
            assert result['topic'] == fixture['topic']


@pytest.mark.parametrize('facet,field,query', [
    ('term_definitions', 'mainTermExpl', '임의조사 주요 용어해설 알려줘'),
    ('survey_history', 'examinHistory', '임의조사 조사연혁 알려줘'),
    ('usage_notes', 'dataUserNote', '임의조사 자료 이용시 유의사항 알려줘'),
])
def test_new_official_fields_are_not_numeric_or_continuity_approval(facet, field, query):
    rows = [{'statsNm': '임의조사'}, {field: '<p>2023년: 정의 변경</p>'}]
    result = methodology_payload(query, request_for(query), 'ORG_RANDOM', 'TABLE_RANDOM', rows)
    assert result['status'] == 'explained'
    assert result['actual_query_supported'] is False and result['role'] == 'methodology'
    assert result['covered_facets'] == [facet]
    assert result['fields'] == [{'key': field, 'label': RULES['fieldLabels'][field], 'text': '2023년: 정의 변경'}]
    assert not any(key in result for key in ['data', 'continuity_safe', 'comparable', 'actual_value'])
    assert any('다른 조사' in warning for warning in result['warnings']) if facet == 'term_definitions' else any('보증' in warning for warning in result['warnings'])


@pytest.mark.parametrize('value', ['', '첨부파일 있음', '해당없음'])
def test_absent_history_is_not_a_safety_guarantee(value):
    query = '임의조사 조사연혁 알려줘'
    result = methodology_payload(query, request_for(query), 'X', 'Y', [{'statsNm': '임의조사'}, {'examinHistory': value}, {'dataUserNote': '2023년 기준 변경'}])
    assert result['status'] == 'partial'
    assert result['covered_facets'] == [] and result['missing_facets'] == ['survey_history']
    assert not any(field['key'] == 'examinHistory' for field in result['fields'])
    assert any('보증' in warning for warning in result['warnings'])


def test_explanation_fields_never_merge_history_definitions_or_units():
    query = '2023년 임의조사 주요 용어해설과 조사연혁 알려줘'
    rows = [{'statsNm': '임의조사'}, {'mainTermExpl': '기업체: 법적 단위'},
            {'examinHistory': '2023년: 모집단 변경'}, {'dataUserNote': '사업체와 기업체는 다름'}]
    result = methodology_payload(query, request_for(query), 'X', 'Y', rows)
    assert result['status'] == 'partial' and result['requested_years'] == ['2023']
    assert {field['key']: field['text'] for field in result['fields']} == {
        'mainTermExpl': '기업체: 법적 단위', 'examinHistory': '2023년: 모집단 변경'}
    assert any('2023' in warning for warning in result['warnings'])


@pytest.mark.parametrize('query,topic,facets', [
    ('경제활동인구조사 표본 규모가 얼마야?', '경제활동인구조사', ['sample_size']),
    ('소상공인 실태조사 표본오차 알려줘', '소상공인 실태조사', ['sample_error']),
    ('소비자물가지수 기준년이 언제야?', '소비자물가지수', ['base_year']),
    ('2024년 경제활동인구조사 표본 추출방법 알려줘', '경제활동인구조사', ['sample_design']),
])
def test_request_roles_match_the_application(query, topic, facets):
    assert request_for(query) == {'topic': topic, 'facets': facets, 'requestedYears': ['2024'] if query.startswith('2024') else [], 'role': 'methodology'}


def test_metadata_numbers_and_files_are_not_indicator_observations():
    assert request_for('경제활동인구조사 표본규모와 실업률 값 알려줘')['role'] == 'mixed'
    assert request_for('경제활동인구조사 표본규모 값을 알려줘')['role'] == 'methodology'
    assert request_for('경제활동인구조사 표본규모 엑셀로 만들어줘')['role'] == 'methodology'
    assert request_for('경제활동인구조사 표본규모 PDF로 만들어줘')['topic'] == '경제활동인구조사'
    assert request_for('지역내총생산 실질 기준으로 알려줘') is None
    assert request_for('2020=100 기준 소비자물가지수 값 알려줘') is None
    assert request_for('이 수치의 기준시점이 언제야?') is None
    assert request_for('실업률 기준 연도별 추이 알려줘') is None
    assert request_for('실업률 기준 년도별 추이 알려줘') is None


def test_realized_error_and_specific_vintage_cannot_be_proved_by_design_formulas():
    query = '2024년 소상공인실태조사 표본규모와 표본오차 알려줘'
    rows = [{'statsNm': '소상공인실태조사'}, {'josaUnit': '40,000개 기업체'}, {'sampleSclCfml': '부차모집단별 목표상대표준오차와 네이만 배분'}]
    result = methodology_payload(query, request_for(query), '142', 'TABLE', rows)
    assert result['status'] == 'partial' and result['actual_query_supported'] is False
    assert result['covered_facets'] == ['sample_size'] and 'sample_error' in result['missing_facets']
    assert any('2024' in warning for warning in result['warnings'])
    assert any('실현' in warning for warning in result['warnings'])


def test_current_index_unit_is_not_a_date_or_a_cached_description():
    query = '소비자물가지수 기준년이 언제야?'
    result = methodology_payload(query, request_for(query), '101', 'TABLE', [{'statsNm': '소비자물가조사'}, {'dataUserNote': '2015년부터 작성'}], [{'UNIT_NM': '2020=100'}])
    assert result['status'] == 'explained'
    assert result['fields'] == [{'key': 'indexBase', 'label': '지수 기준 표시', 'text': '2020=100'}]
    incomplete = methodology_payload(query, request_for(query), '101', 'TABLE', [{'statsNm': '소비자물가조사'}, {'dataUserNote': '2020년 이후 소수 2자리'}])
    assert incomplete['status'] == 'partial' and 'base_year' in incomplete['missing_facets']
    why = '소비자물가지수 기준년은 왜 바꾸나요?'
    assert methodology_payload(why, request_for(why), '101', 'TABLE', [{'statsNm': '소비자물가조사'}], [{'UNIT_NM': '2020=100'}])['status'] == 'partial'


def test_duplicate_survey_names_are_deduplicated_but_conflicting_sources_and_attachment_markers_are_not_proof():
    fields = explanation_fields([{'statsNm': 'A'}, {'statsNm': 'A'}, {'dataUserNote': '&lt;p&gt;hello&lt;/p&gt;'}])
    assert fields['statsNm'] == ['A'] and fields['dataUserNote'] == ['hello']
    q = 'A 표본규모 알려줘'
    result = methodology_payload(q, request_for(q), '101', 'TABLE', [{'statsNm': 'A'}, {'statsNm': 'B'}])
    assert result['status'] == 'ambiguous_source' and not result['fields']
    result = methodology_payload('A 표본오차 알려줘', request_for('A 표본오차 알려줘'), '101', 'TABLE', [{'statsNm': 'A'}, {'estEqua': '파일 첨부'}])
    assert not result['fields'] and result['missing_facets'] == ['sample_error']


@pytest.mark.parametrize('query', ['경제활동인구조사 표본 규모가 얼마야?', '임의조사 주요 용어해설 알려줘',
                                  '임의조사 조사연혁 알려줘', '임의조사 자료 이용시 유의사항 알려줘'])
def test_direct_answer_query_routes_methods_without_any_value_query(monkeypatch, query):
    async def explain(query, **kwargs):
        return {'role': 'methodology', 'actual_query_supported': False}
    async def no_value(*args, **kwargs):
        raise AssertionError('metadata was routed into a numerical series query')
    monkeypatch.setattr(server, 'explain_statistic', explain)
    monkeypatch.setattr(server, 'query_primary_series', no_value)
    assert asyncio.run(server.answer_query(query))['role'] == 'methodology'


def test_survey_discovery_is_exact_not_a_numeric_or_title_rank_guess(monkeypatch):
    calls = []
    async def api(client, endpoint, params):
        calls.append((endpoint, params))
        if endpoint == 'statisticsSearch.do':
            return [{'STAT_NM': '다른 조사', 'TBL_NM': '매출액', 'ORG_ID': '999', 'TBL_ID': 'BAD'},
                    {'STAT_NM': '임의조사', 'STAT_ID': 'S', 'ORG_ID': '101', 'TBL_ID': 'TABLE'}]
        return [{'statsNm': '임의조사'}, {'josaUnit': '100가구'}]
    monkeypatch.setattr(server, '_kosis_call', api)
    result = asyncio.run(server.explain_statistic('임의조사 표본규모 알려줘', api_key='dummy'))
    assert result['survey_name'] == '임의조사' and result['source']['table_id'] == 'TABLE'
    assert all('statisticsParameterData' not in call[0] for call in calls)


def test_unknown_surveys_or_transport_failure_do_not_become_zero_or_cached_proof(monkeypatch):
    async def empty(*args):
        return []
    monkeypatch.setattr(server, '_kosis_call', empty)
    result = asyncio.run(server.explain_statistic('알수없는조사 표본규모 알려줘', api_key='dummy'))
    assert result['status'] == 'not_found' and not result['fields']
    async def outage(*args):
        raise RuntimeError('test outage')
    monkeypatch.setattr(server, '_kosis_call', outage)
    result = asyncio.run(server.explain_statistic('알수없는조사 표본규모 알려줘', api_key='dummy'))
    assert result['status'] == 'service_unavailable' and not result['fields']
    assert asyncio.run(server.explain_statistic('임의조사 표본규모 알려줘', org_id='101', api_key='dummy'))['status'] == 'invalid_request'
