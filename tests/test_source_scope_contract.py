from kosis_analysis.evidence import build_stat_evidence
from kosis_analysis.rules import intrinsic_table_scopes
from kosis_analysis.rules import source_population_frame


def test_fixed_universes_are_independent_of_matching_one_person_rows():
    for title, label in [("거주지역/가구원수별 무주택 가구수", "무주택 가구"),
                         ("거주지역/가구원수별 주택소유 가구수", "주택소유 가구"),
                         ("거주지역/가구원수별 아파트소유 가구수", "아파트소유 가구"),
                         ("지역별 임의조건 가구수", "임의조건 가구")]:
        payload = {"status": "executed", "org_id": "101", "tbl_id": "not-a-special-case",
                   "table_name": title, "source": "KOSIS", "rows": [{"value": 100, "unit": "가구", "period": "2024",
                   "dimensions": {"ITEM": {"code": "T1", "label": "일반가구수"}, "A": {"code": "1", "label": "1인"}}}]}
        envelope = build_stat_evidence(payload, tool="query_table")
        assert envelope["fulfillment"]["completeness"] == "complete"
        scope = envelope["evidence"]["source_scope"]
        assert scope["org_id"] == "101" and scope["table_id"] == "not-a-special-case"
        assert [item["label"] for item in scope["constraints"]] == [label]


def test_comparison_categories_and_unknown_metadata_are_not_fixed_universes():
    assert intrinsic_table_scopes("주택소유 여부별 가구수") == []
    assert intrinsic_table_scopes("성 및 거주지역/가구원수별 가구") == []
    assert intrinsic_table_scopes(None) == []
    assert intrinsic_table_scopes("1인 취업가구 현황") == []
    assert intrinsic_table_scopes("종사상지위별 등록취업자수")[0]["label"] == "등록취업자"


def test_reviewed_population_frame_is_bound_to_survey_identity_and_all_periods():
    payload = {"status": "executed", "org_id": "118", "tbl_id": "any-table-code",
               "survey_name": "「사업체노동실태현황」", "table_name": "임의 축별 사업체수", "source": "KOSIS",
               "rows": [{"value": 299548, "unit": "개", "period": "2024",
                         "dimensions": {"ITEM": {"code": "opaque", "label": "사업체수"}}}]}
    envelope = build_stat_evidence(payload, tool="query_table")
    frame = envelope['evidence']['source_scope']['population_frame']
    assert frame['coverage'] == 'restricted'
    assert frame['exclusions'] and frame['references']
    assert envelope['evidence']['observations'][0]['value'] == 299548
    assert build_stat_evidence({**payload, 'source': '다른 제공기관'}, tool='query_table')['evidence']['source_scope']['population_frame'] is None
    for org, name, periods in [('101', '사업체노동실태현황', ['2024']),
                               ('118', '다른사업체노동실태현황', ['2024']),
                               ('118', '사업체노동실태현황', ['2023']),
                               ('118', '사업체노동실태현황', ['2024', '2025']),
                               ('118', '사업체노동실태현황', [])]:
        assert source_population_frame(org, name, periods) is None


def test_compact_and_verbose_keep_the_same_population_definition():
    import kosis_mcp_server as server
    payload = {'status': 'executed', 'org_id': '118', 'tbl_id': 'opaque-table', 'source': 'KOSIS',
               'table_name': '산업별 사업체수', 'survey_name': '사업체노동실태현황',
               'rows': [{'value': 299548, 'unit': '개', 'period': '2024',
                         'dimensions': {'ITEM': {'code': 'opaque', 'label': '사업체수'}}}]}
    frames = []
    for verbose in (False, True):
        result = server._finalize_answer_query_response(payload.copy(), query='사업체 수', region='전국', verbose=verbose)
        frames.append(result['stat_evidence']['evidence']['source_scope']['population_frame'])
        assert result['stat_evidence']['evidence']['survey_name'] == '사업체노동실태현황'
        assert frames[-1]['coverage'] == 'restricted'
    assert frames[0] == frames[1]
