from kosis_analysis.evidence import build_stat_evidence
from kosis_analysis.rules import intrinsic_table_scopes


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
