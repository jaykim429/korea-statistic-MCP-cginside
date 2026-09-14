"""단위가 비어 있는 항목은 상위 항목에서 물려받는다.

실측 127/TX_10506_A080: '총매출액' 항목에 단위 '백만원'이 있고 그 하위
'원자력발전사업자'는 빈칸이다. 빈칸을 그대로 내보냈더니 챗봇이 단위를 지어냈다.

있는 단위를 덮어쓰지 않는 것이 이 규칙의 핵심이다 — 예전에 "행 단위가 항상 이긴다"로
고쳤다가 복합 단위 표('명 건')를 망친 적이 있다.
"""

from kosis_analysis.metadata import _normalize_query_table_rows, _resolved_item_unit


AXIS_ITEMS = {
    "TOP": {"code": "TOP", "label": "원자력산업", "unit": None, "parent": None},
    "SALES": {"code": "SALES", "label": "총매출액", "unit": "백만원", "parent": "TOP"},
    "OPERATOR": {"code": "OPERATOR", "label": "원자력발전사업자", "unit": None, "parent": "SALES"},
    "PEOPLE": {"code": "PEOPLE", "label": "종사인력", "unit": "명", "parent": "TOP"},
}


def test_비어_있으면_상위에서_물려받는다():
    assert _resolved_item_unit(AXIS_ITEMS, "OPERATOR") == "백만원"


def test_자기_단위가_있으면_그대로_쓴다():
    assert _resolved_item_unit(AXIS_ITEMS, "PEOPLE") == "명"


def test_조상에도_없으면_없는_것이다():
    assert _resolved_item_unit(AXIS_ITEMS, "TOP") is None


def test_모르는_코드는_없는_것이다():
    assert _resolved_item_unit(AXIS_ITEMS, "NOPE") is None
    assert _resolved_item_unit({}, "OPERATOR") is None


def test_부모_고리가_돌아도_멈춘다():
    looping = {
        "A": {"code": "A", "label": "가", "unit": None, "parent": "B"},
        "B": {"code": "B", "label": "나", "unit": None, "parent": "A"},
    }
    assert _resolved_item_unit(looping, "A") is None


def test_조회_행의_단위가_상위에서_채워진다():
    axes = {
        "ITEM": {"OBJ_NM": "항목", "items": {"I1": {"code": "I1", "label": "주요지표", "unit": None, "parent": None}}},
        "AX": {"OBJ_NM": "지표현황별", "items": AXIS_ITEMS},
    }
    rows = _normalize_query_table_rows(
        [{"ITM_ID": "I1", "C1": "OPERATOR", "C1_NM": "원자력발전사업자", "DT": "289159", "PRD_DE": "2024", "UNIT_NM": None}],
        {"ITEM": ["I1"], "AX": ["OPERATOR"]},
        axes,
        ["ITEM", "AX"],
    )
    assert len(rows) == 1
    assert rows[0]["value"] == 289159
    assert rows[0]["dimensions"]["AX"]["unit"] == "백만원"
    assert rows[0]["unit"] == "백만원"


def test_축이_서로_다른_단위를_가지면_행_단위를_고르지_않는다():
    # 틀린 단위보다 없는 편이 낫다.
    axes = {
        "ITEM": {"OBJ_NM": "항목", "items": {"I1": {"code": "I1", "label": "주요지표", "unit": None, "parent": None}}},
        "AX": {"OBJ_NM": "지표현황별", "items": AXIS_ITEMS},
        "BX": {"OBJ_NM": "기타", "items": {"Z": {"code": "Z", "label": "기타", "unit": "건", "parent": None}}},
    }
    rows = _normalize_query_table_rows(
        [{"ITM_ID": "I1", "C1": "OPERATOR", "C1_NM": "원자력발전사업자", "C2": "Z", "C2_NM": "기타", "DT": "1", "PRD_DE": "2024", "UNIT_NM": None}],
        {"ITEM": ["I1"], "AX": ["OPERATOR"], "BX": ["Z"]},
        axes,
        ["ITEM", "AX", "BX"],
    )
    assert rows[0]["unit"] is None


def test_KOSIS_가_준_단위를_덮어쓰지_않는다():
    axes = {
        "ITEM": {"OBJ_NM": "항목", "items": {"I1": {"code": "I1", "label": "주요지표", "unit": None, "parent": None}}},
        "AX": {"OBJ_NM": "지표현황별", "items": AXIS_ITEMS},
    }
    rows = _normalize_query_table_rows(
        [{"ITM_ID": "I1", "C1": "OPERATOR", "C1_NM": "원자력발전사업자", "DT": "5", "PRD_DE": "2024", "UNIT_NM": "명 건"}],
        {"ITEM": ["I1"], "AX": ["OPERATOR"]},
        axes,
        ["ITEM", "AX"],
    )
    assert rows[0]["unit"] == "명 건"
