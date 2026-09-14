"""지표 한 단어 질문을 되물을지 값을 줄지 가르는 규칙.

'매출액 알려줘'를 걸리는 대로 답하다 '블록체인 부문 예상 매출액'을 낸 적이 있어 되묻게 막아 뒀다.
그런데 수출액·수입액·창업기업수는 검증된 현행 Tier A 전국 표가 실재해서, 되물으면 사용자를
한 턴 더 돌게 만든다(실측 S18·S19·S27). 무엇을 되묻고 무엇을 답할지 여기서 고정한다.
"""
import pytest

import kosis_curation as c
from kosis_mcp_server import NaturalLanguageAnswerEngine as Engine


class TestAsksBack:
    """대상이 정해지지 않는 지표는 되물어야 한다."""

    @pytest.mark.parametrize("q", ["매출액 알려줘", "매출액", "종사자수 알려줘", "기업수 알려줘", "영업이익", "생산액", "부가가치"])
    def test_모호한_지표는_되묻는다(self, q):
        assert Engine._bare_indicator(q) is not None

    def test_표가_끝난_지표도_되묻는다(self):
        # 전체사업체수는 verified 이지만 표가 2016 에 끝났다(deprecated).
        # 값을 주면 옛 수치를 현재값처럼 내보내게 된다.
        assert Engine._bare_indicator("사업체수") == "사업체수"
        assert c.TIER_A_STATS["전체사업체수"].replacement_status == "deprecated"


class TestAnswersDirectly:
    """전국 기준이 확정된 지표는 그 값이 곧 답이다."""

    @pytest.mark.parametrize("q", ["수출액", "수출액 알려줘", "수입액", "수입액은", "창업기업수", "창업기업수 알려줘"])
    def test_확정된_지표는_되묻지_않는다(self, q):
        assert Engine._bare_indicator(q) is None

    @pytest.mark.parametrize("key", ["수출액", "수입액", "창업기업수"])
    def test_근거가_실제로_현행_검증_표다(self, key):
        param = c.TIER_A_STATS[key]
        assert param.verification_status == "verified"
        assert param.replacement_status != "deprecated"
        assert param.org_id and param.tbl_id


class TestSettledRule:
    """판별 규칙 자체 — verified 만으로는 부족하다."""

    def test_현행_검증_표가_있으면_참(self):
        assert Engine._has_settled_national_stat("수출액") is True

    def test_표가_없으면_거짓(self):
        assert Engine._has_settled_national_stat("매출액") is False
        assert Engine._has_settled_national_stat("없는지표이름") is False

    def test_deprecated_는_거짓(self):
        assert Engine._has_settled_national_stat("사업체수") is False
