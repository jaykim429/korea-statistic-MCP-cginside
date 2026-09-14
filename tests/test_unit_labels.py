"""단위 라벨이 스스로 모순되지 않는지 — 네트워크 없이 확인.

단위는 사람이 손으로 적은 상수다. 틀려도 값·단위·출처가 모두 붙으므로 라이브 검증
375케이스가 전부 통과한다. 실측: 수출액이 '천달러'로 적혀 있어 2024년 6,836억 달러가
6.8억 달러로 나갔다 — 1000배 틀린 값을 사용자가 인용할 뻔했다.

서버는 이제 KOSIS 원행이 싣고 온 단위를 우선한다(kosis_mcp_server.quick_stat). 그래도
커레이션 상수가 거짓말이면 다른 경로와 문서가 어긋나므로, 여기서 **note 와 unit 이 서로
모순되지 않는지**를 고정한다. 이 검사는 표를 부르지 않는다.
"""
import re

import pytest

import kosis_curation as c

# note 안에 단위를 적어 둔 항목은 그 단위가 unit 과 같아야 한다.
UNIT_IN_NOTE = re.compile(r'(?:단위[는은:]?\s*|=\s*)([가-힣0-9]+(?:당\s*)?[가-힣]+)')


def test_수출액은_100만달러다():
    """1000배 오류의 회귀 방지. KOSIS 원행이 100만달러라고 말한다."""
    assert c.TIER_A_STATS["수출액"].unit == "100만달러"


def test_범죄율은_비율_단위를_쓴다():
    """'범죄율 31건'은 오해를 부른다 — 인구 천명당 건수다."""
    param = c.TIER_A_STATS["범죄율"]
    assert "천명당" in param.unit
    # 같은 표의 총계 지표는 그대로 '건'
    assert c.TIER_A_STATS["범죄발생건수"].unit == "건"


@pytest.mark.parametrize("key", sorted(c.TIER_A_STATS))
def test_단위가_비어_있지_않다(key):
    param = c.TIER_A_STATS[key]
    assert str(param.unit or "").strip(), f"{key} 에 단위가 없다"


def test_note_에_적힌_단위가_unit_과_어긋나지_않는다():
    """note 에 '단위: X' 나 '= X' 로 적어 뒀으면 unit 과 같아야 한다.

    수출액은 note 가 '천달러'라고 말하는데 unit 은 다른 값이던 적이 있다. 기록과 상수가
    어긋나면 어느 쪽이 맞는지 아무도 모른다.
    """
    mismatches = []
    for key, param in c.TIER_A_STATS.items():
        note = param.note or ""
        # 콜론으로 명시한 것만 '선언된 단위'로 본다. 조사만 보고 자르면 "단위까지 재검증"의
        # '까지'를 단위로 읽고, 한 단어만 자르면 "인구 천명당 건수"에서 '인구'만 읽는다.
        m = re.search(r'단위\s*[:：]\s*([^.;()]+)', note)
        if not m:
            continue
        declared = m.group(1).strip()
        unit = str(param.unit or "").strip()
        # 단위의 낱말이 선언 문구 안에 모두 있으면 같은 뜻으로 본다
        if all(token in declared for token in unit.split()):
            continue
        if declared in unit or unit in declared:
            continue
        mismatches.append(f"{key}: note='{declared}' vs unit='{unit}'")
    assert not mismatches, "note 와 unit 이 어긋난 지표: " + "; ".join(mismatches)
