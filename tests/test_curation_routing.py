"""자연어 → Tier A 지표 매핑 검증.

Tier A 는 표·항목·단위까지 확정해 둔 '바로 답할 수 있는' 목록이다. 여기서 엉뚱한 키가 나오면
값이 안 나오는 게 아니라 **그럴듯한 틀린 값**이 사용자에게 간다 — 그래서 라이브 검증보다
여기서 먼저 잡아야 한다. 케이스는 Tier A 191개 일괄 점검에서 실제로 깨졌던 것들이다.
"""
import pytest

import kosis_curation as c

router = c.DEFAULT_ROUTER


class TestSpecificityWins:
    """동의어가 더 구체적인 지표를 가리면 안 된다."""

    @pytest.mark.parametrize("industry", ["제조업", "건설업", "도소매업", "정보통신업", "숙박음식점업"])
    def test_업종이_붙으면_업종_지표로_간다(self, industry):
        # 예전에는 "알려줘" 한 마디에 업종이 사라지고 전국 수치로 답했다(실측 41개 지표)
        assert router.match_direct_stat_key(f"{industry} 중소기업 매출액 알려줘") == f"{industry}_중소기업_매출액"

    def test_업종이_없으면_전국_지표_그대로(self):
        assert router.match_direct_stat_key("중소기업 매출액 알려줘") == "중소기업_매출액"

    def test_사업체수도_같은_규칙(self):
        assert router.match_direct_stat_key("건설업 중소기업 사업체수 알려줘") == "건설업_중소기업_사업체수"
        assert router.match_direct_stat_key("도소매업 소상공인 사업체수 알려줘") == "도소매업_소상공인_사업체수"

    def test_아파트_전세가격지수가_전세가격지수에_먹히지_않는다(self):
        assert router.match_direct_stat_key("아파트전세가격지수 알려줘") == "아파트전세가격지수"


class TestReachable:
    """검증된 표가 있는데 부를 말이 없어 못 닿던 지표들."""

    @pytest.mark.parametrize("q", ["초미세먼지", "초미세먼지 농도", "초미세먼지 농도 알려줘", "PM2.5 알려줘"])
    def test_초미세먼지(self, q):
        # 실측 S40: '농도' 한 단어만 걸려 '요중 납 농도'가 후보로 나갔다
        assert router.match_direct_stat_key(q) == "초미세먼지_PM25"

    @pytest.mark.parametrize("q", ["미세먼지", "미세먼지 농도 알려줘", "PM10 알려줘"])
    def test_미세먼지(self, q):
        assert router.match_direct_stat_key(q) == "미세먼지_PM10"

    @pytest.mark.parametrize("q", ["가계대출", "가계대출 알려줘", "가계부채 얼마야"])
    def test_가계대출(self, q):
        assert router.match_direct_stat_key(q) == "가계대출_총잔액"

    def test_표_정보가_실제로_붙어_있다(self):
        # 매핑만 되고 표가 없으면 소용없다
        for key in ("초미세먼지_PM25", "미세먼지_PM10", "가계대출_총잔액"):
            param = c.TIER_A_STATS[key]
            assert param.org_id and param.tbl_id
            assert param.verification_status == "verified"


class TestStillRouted:
    """기존 매핑이 흔들리지 않았는지 — 자주 쓰는 지표 표본."""

    @pytest.mark.parametrize(("q", "expected"), [
        ("실업률 알려줘", "실업률"),
        ("청년 실업률은 얼마야?", "청년 실업률"),
        ("고용률 최근 수치", "고용률"),
        ("인구 알려줘", "인구"),
        ("총인구 알려줘", "인구"),
        ("합계출산율 최근 수치 알려줘", "합계출산율"),
        ("GDP 얼마야?", "GDP"),
        ("소비자물가지수 최근 수치", "소비자물가지수"),
        ("의사 수 알려줘", "의사수"),
        ("자동차 등록 대수 알려줘", "자동차등록대수"),
    ])
    def test_대표_지표(self, q, expected):
        assert router.match_direct_stat_key(q) == expected


def test_tier_a_전체가_자연어로_닿는다():
    """191개를 한 번에 훑어 회귀를 막는다. 의도적으로 막아 둔 것만 예외로 둔다."""
    import re

    tech_suffix = re.compile(r"[_\s]*(PM\d+|PM\d+\.\d+|계|총잔액)$")
    # 모집단 한정어가 없으면 직접조회를 막는 지표(_BUSINESS_BASE_STATS) — 검색 경로가 받는다
    allowed_misses = {"중소기업_종사자수"}

    missed = []
    for key in c.TIER_A_STATS:
        phrase = tech_suffix.sub("", key.replace("_", " ").strip()).strip() or key
        if router.match_direct_stat_key(f"{phrase} 알려줘") != key:
            missed.append(key)
    assert set(missed) <= allowed_misses, f"자연어로 닿지 않는 지표: {sorted(set(missed) - allowed_misses)}"
