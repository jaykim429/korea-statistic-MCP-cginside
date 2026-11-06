"""이름이 「…지수」 인 측정 항목은 단위가 비어도 단위 누락이 아니다.

실측(2026-11-06, 101/DT_1YL12501E 「노령화지수(시도)」): KOSIS 가 노령화지수의 단위를 비워 둬서 값이 「불완전」 으로
버려졌다. 지수는 비율에 100 을 곱한 무차원 값이라 천명·명 같은 단위 모호성이 없다. 다른 항목은 여전히 단위가 있어야 한다.
"""
from kosis_analysis.evidence import build_stat_evidence


def payload(item_label, unit):
    return {"status": "executed", "source": "KOSIS", "org_id": "101", "tbl_id": "DT_1YL12501E", "table_name": "노령화지수(시도)",
            "rows": [{"period": "2026", "unit": unit, "value": 222.7,
                      "dimensions": {"ITEM": {"code": "T10", "label": item_label, "unit": unit},
                                     "SGG": {"code": "00", "label": "전국", "unit": None}}}]}


def missing(item_label, unit):
    return build_stat_evidence(payload(item_label, unit), tool="query_table")["fulfillment"]["missing_fields"]


def test_named_index_without_unit_is_complete():
    assert "unit" not in missing("노령화지수", None)
    assert "unit" not in missing("노령화지수", "")


def test_other_measures_still_need_a_unit():
    assert "unit" in missing("총부양비", None)
    assert "unit" in missing("인구수", None)
    assert "unit" in missing("지수 산출 대상 인구", None)
