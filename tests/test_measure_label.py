"""큐레이션 지표의 이름과 그 표가 세는 것이 어긋나면 안 된다.

실측: "중소기업_사업체수" 가 「시도별·산업중분류별·기업규모별 **기업수**」 를 가리켰다.
그 표의 항목은 T001 '기업수' 하나뿐이라 사업체를 세지 않는다. 그래서 답이
"중소기업 사업체수는 8,298,915개" 로 나갔는데 같은 해 전국 **사업체** 수는 636만이다 —
부분이 전체보다 큰 답이 나간 셈이다.

중소기업 기준은 업종별 매출액·자산이라 종사자규모로 환산할 수 없어 '중소기업 사업체수'
공식 통계는 없다. 그래서 세는 대로 '기업수'라 적는다.

83개 전수 점검으로 이 하나뿐임을 확인했다. 다시 어긋나면 이 검사가 잡는다.
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
