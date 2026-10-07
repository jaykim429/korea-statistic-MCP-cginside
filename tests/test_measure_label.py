"""큐레이션 지표의 이름과 그 표가 세는 것이 어긋나면 안 된다.

실측: "중소기업_사업체수" 가 「시도별·산업중분류별·기업규모별 **기업수**」 를 가리켰다.
그 표의 항목은 T001 '기업수' 하나뿐이라 사업체를 세지 않는다. 그래서 답이
"중소기업 사업체수는 8,298,915개" 로 나갔는데 같은 해 전국 **사업체** 수는 636만이다 —
부분이 전체보다 큰 답이 나간 셈이다.

중소기업 기준은 업종별 매출액·자산이라 종사자규모로 환산할 수 없어 '중소기업 사업체수'
공식 통계는 없다. 그래서 세는 대로 '기업수'라 적는다.

표명과 설명을 같은 잘못된 별칭으로 생성하면 둘의 대조만으로는 잡히지 않는다.
동적 업종 확장은 실제 표 식별자에 따른 공식 측정어도 대조한다.
"""

import re

from kosis_curation import TIER_A_STATS

MEASURE_NOUNS = [
    "사업체수", "종사자수", "근로자수", "취업자수", "실업자수", "기업체수", "기업수", "업체수",
    "자영업자수", "가구수", "인구수", "학생수", "농가수", "어가수",
    "매출액", "수출액", "수입액", "생산액", "부가가치", "영업이익", "투자액", "거래액",
    "발전량", "생산량", "소비량", "배출량", "처리량", "발생량", "등록대수",
    "증가율", "감소율", "상승률", "실업률", "고용률", "출산율", "비중", "비율", "지수",
]


def measure_of(text):
    body = re.sub(r"[\s·,()\[\]{}_/]", "", str(text or ""))
    best = None
    for noun in MEASURE_NOUNS:
        at = body.rfind(noun)
        if at < 0:
            continue
        end = at + len(noun)
        if best is None or end > best[1] or (end == best[1] and len(noun) > len(best[0])):
            best = (noun, end)
    return best[0] if best else None


def test_지표_이름과_표가_세는_것이_같다():
    mismatched = []
    for key, param in TIER_A_STATS.items():
        tbl_nm = getattr(param, "tbl_nm", "") or ""
        desc = getattr(param, "description", "") or ""
        if not tbl_nm:
            continue
        asked = measure_of(desc or key)
        table = measure_of(tbl_nm)
        if asked and table and asked != table:
            mismatched.append(f"{key}: 이름={asked} 표={table} ({tbl_nm})")
    assert not mismatched, "이름과 표의 측정이 어긋난다:\n  " + "\n  ".join(mismatched)


def test_중소기업_지표는_기업을_센다고_밝힌다():
    param = TIER_A_STATS["중소기업_사업체수"]
    # 표가 기업을 세므로 답도 기업이라 말해야 한다. 사업체라 적으면 전체보다 큰 값이 나간다.
    assert "기업수" in param.description
    assert "사업체" not in param.description
    assert "기업수" in param.tbl_nm
    assert "기업 단위" in param.measure_basis
    assert "매출액·자산 기준" in param.measure_basis


def test_all_generated_business_count_aliases_keep_the_official_enterprise_measure():
    generated = {key: param for key, param in TIER_A_STATS.items()
                 if param.tbl_id == "DT_BR_A001" and param.obj_l1.startswith("IM_")}
    assert len(generated) == 36  # 18 industries × SME/small-business scales
    for key, param in generated.items():
        assert key.endswith("_사업체수")  # Existing natural-language routes remain compatible.
        assert measure_of(param.description) == "기업수", key
        assert measure_of(param.tbl_nm) == "기업수", key
        assert "기업 단위" in param.measure_basis, key
        assert param.item_id == "T001" and param.unit == "개", key


def test_other_generated_measures_are_not_renamed_to_enterprise_counts():
    expected = {"DT_BR_B001": ("종사자수", "명"), "DT_BR_C001": ("매출액", "억원")}
    generated = {key: param for key, param in TIER_A_STATS.items()
                 if param.tbl_id in expected and param.obj_l1.startswith("IM_")}
    assert len(generated) == 72
    for key, param in generated.items():
        measure, unit = expected[param.tbl_id]
        assert measure_of(param.description) == measure, key
        assert measure_of(param.tbl_nm) == measure, key
        assert param.unit == unit and not param.measure_basis, key
