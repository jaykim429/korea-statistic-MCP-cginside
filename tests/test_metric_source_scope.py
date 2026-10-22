"""Shared source-heading grammar: no table IDs or product dictionary."""
import json
from pathlib import Path
import pytest
from kosis_analysis.rules import intrinsic_table_scopes, survey_subject
from kosis_analysis.evidence import build_stat_evidence

RULES = json.loads((Path(__file__).resolve().parents[1] / 'rules' / 'population-labels.json').read_text(encoding='utf-8'))

@pytest.mark.parametrize('case', RULES['tableMetricHeading']['cases'])
def test_shared_closed_heading_grammar(case):
    survey = case['prefix'] + '산업현황' if case['prefix'] else None
    scopes = [scope for scope in intrinsic_table_scopes(case['title'], survey) if scope['id'].startswith('table-metric:')]
    assert [scope['prefix'] for scope in scopes] == ([case['prefix']] if case['prefix'] else [])

@pytest.mark.parametrize('subject', ['위생용품', '임의품목', '2차전지', '당근'])
def test_source_envelope_declares_open_set_universe_but_preserves_real_observation(subject):
    payload = {'status': 'executed', 'org_id': 'opaque-org', 'tbl_id': 'opaque-table', 'source': 'KOSIS',
               'table_name': f'{subject} 생산·수출·수입액 무역수지, 시장규모', 'survey_name': f'{subject}산업현황',
               'rows': [{'value': -474304, 'unit': '백만원', 'period': '2025',
                         'dimensions': {'ITEM': {'code': 'opaque-item', 'label': '무역수지'}, 'X': {'label': '전체'}}}]}
    evidence = build_stat_evidence(payload, tool='query_table')
    scope = evidence['evidence']['source_scope']
    assert scope['org_id'] == 'opaque-org' and scope['table_id'] == 'opaque-table'
    assert scope['basis'] == 'official_title_and_survey'
    assert [item['prefix'] for item in scope['constraints']] == [subject]
    assert evidence['evidence']['observations'][0]['value'] == -474304

@pytest.mark.parametrize('survey', [None, '관세청 무역통계', '다른품목산업현황', '기타위생용품산업현황', '주석 버튼을 클릭하세요'])
def test_title_alone_or_conflicting_source_is_unknown(survey):
    scopes = intrinsic_table_scopes('위생용품 생산·수출·수입액 무역수지, 시장규모', survey)
    assert not any(scope['id'].startswith('table-metric:') for scope in scopes)
    # A different official named industry can still have its own independent scope.
    assert all(scope['prefix'] != '위생용품' for scope in scopes)

def test_measurement_basis_is_not_a_source_population():
    assert intrinsic_table_scopes('최종 에너지 소비량', '「한국도시통계」') == []


@pytest.mark.parametrize('case', RULES['surveySubjectGrammar']['cases'])
def test_shared_official_survey_subject_grammar(case):
    assert survey_subject(case['survey']) == case['subject']


@pytest.mark.parametrize('subject', ['나노융합산업', '임의신산업', '2차전지산업'])
def test_generic_table_keeps_its_named_industry_scope_without_inventing_population_review(subject):
    payload = {'status': 'executed', 'org_id': 'opaque-org', 'tbl_id': 'opaque-table', 'source': 'KOSIS',
               'table_name': '기업 규모별 사업체수, 매출액', 'survey_name': f'「{subject}조사」',
               'rows': [{'value': 811, 'unit': '개', 'period': '2023', 'dimensions': {
                   'ITEM': {'code': 'opaque-item', 'label': '사업체수'}, 'SIZE': {'code': 'opaque-member', 'label': '중소기업'}}}]}
    evidence = build_stat_evidence(payload, tool='query_table')
    scope = evidence['evidence']['source_scope']
    assert scope['basis'] == 'official_survey_subject'
    assert scope['constraints'] == [{'id': f'survey-subject:{subject}', 'prefix': subject,
                                    'dimension': 'survey-subject', 'label': f'{subject} 조사 범위'}]
    assert scope['population_frame'] is None
    assert evidence['evidence']['observations'][0]['value'] == 811
