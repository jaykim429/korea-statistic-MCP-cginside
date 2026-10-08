"""자연어 → Tier A 지표 매핑 검증.

Tier A 는 표·항목·단위까지 확정해 둔 '바로 답할 수 있는' 목록이다. 여기서 엉뚱한 키가 나오면
값이 안 나오는 게 아니라 **그럴듯한 틀린 값**이 사용자에게 간다 — 그래서 라이브 검증보다
여기서 먼저 잡아야 한다. 케이스는 Tier A 191개 일괄 점검에서 실제로 깨졌던 것들이다.
"""
import pytest

import kosis_curation as c

router = c.DEFAULT_ROUTER


def test_microbusiness_workers_use_the_actual_survey_item_not_disabled_businesses():
    key = router.match_direct_stat_key("소상공인 종사자 수 알려줘")
    assert key == "소상공인_종사자수"
    param = c.TIER_A_STATS[key]
    assert (param.org_id, param.tbl_id, param.item_id, param.unit) == ("142", "DT_3ME0100", "T02", "명")
    assert router.match_direct_stat_key("소상공인 종사자 수 업종별") != key
    assert router.match_direct_stat_key("소상공인 종사자 수 여성만") != key


class TestBusinessComposition:
    """등록된 업종·규모·지표를 조합한다. 문자열 순서가 조건을 버려서는 안 된다."""

    @pytest.mark.parametrize("industry", [name for name, _ in c._KSIC_SECTIONS])
    @pytest.mark.parametrize("scale", [name for name, _ in c._BR_SCALES])
    @pytest.mark.parametrize("metric", list(c._BR_METRICS))
    @pytest.mark.parametrize("order", [0, 1, 2])
    def test_registered_combinations(self, industry, scale, metric, order):
        slots = [(industry, scale, metric), (scale, industry, metric), (metric, industry, scale)][order]
        assert router.match_direct_stat_key("2023년 " + " ".join(slots) + " 알려줘") == f"{industry}_{scale}_{metric}"

    @pytest.mark.parametrize("question", [
        "2023년 제조업 중소기업 기업 수 알려줘",
        "중소기업 기업수 제조업 알려줘",
    ])
    def test_official_measure_reaches_existing_verified_table(self, question):
        assert router.match_direct_stat_key(question) == "제조업_중소기업_사업체수"
        assert "기업수" in router.lookup(question).description

    @pytest.mark.parametrize("question", [
        "중소기업의 제조업 매출액은?", "2020년부터 2023년까지 제조업 중소기업 매출액 추이",
        "최근 5년 제조업 중소기업 매출액 추이",
    ])
    def test_existing_period_and_particle_forms(self, question):
        assert router.match_direct_stat_key(question) == "제조업_중소기업_매출액"

    @pytest.mark.parametrize(("question", "key"), [
        ("소상공인 숙박 및 음식점업 사업체 수", "숙박음식점업_소상공인_사업체수"),
        ("숙박 및 음식점업 소상공인 기업 수", "숙박음식점업_소상공인_사업체수"),
        ("중소기업 도매 및 소매업 매출액", "도소매업_중소기업_매출액"),
    ])
    def test_complete_official_category_names(self, question, key):
        assert router.match_direct_stat_key(question) == key

    @pytest.mark.parametrize("question", [
        "여성 제조업 중소기업 기업 수", "제조업 청년 중소기업 매출액",
        "제조업 중소기업 소상공인 기업 수", "제조업 건설업 중소기업 기업 수",
        "제조업 중소기업 기업 수와 매출액", "비제조업 중소기업 기업 수",
        "숙박업 소상공인 기업 수", "음식점업 소상공인 기업 수",
        "숙박 및 음식점업 제외 소상공인 기업 수",
        "숙박 및 음식점업 중소기업 이외 기업 수",
    ])
    def test_unresolved_or_conflicting_conditions_do_not_become_direct_total(self, question):
        assert router.match_direct_stat_key(question) is None

    def test_unverified_registered_key_is_not_used(self):
        from dataclasses import replace
        key = "제조업_중소기업_사업체수"
        params = {**c.TIER_A_STATS, key: replace(c.TIER_A_STATS[key], verification_status="unverified")}
        local = c.NaturalLanguageRouter(tier_a_stats=params, synonyms=c.SYNONYMS, tier_b_routing=c.TIER_B_ROUTING, topics=c.TOPICS)
        assert local.match_direct_stat_key("중소기업 제조업 기업 수") is None


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

    @pytest.mark.parametrize(("q", "expected"), [
        ("태양광 발전량 얼마야?", "태양광생산량"),
        ("풍력 발전량 알려줘", "풍력생산량"),
        ("수력 발전량", "수력생산량"),
    ])
    def test_발전량으로도_닿는다(self, q, expected):
        # 전력은 실무에서 "발전량"이라 부르는데 키는 "생산량"이라 검색으로 떨어졌다(실측 S41·S42)
        assert router.match_direct_stat_key(q) == expected

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
