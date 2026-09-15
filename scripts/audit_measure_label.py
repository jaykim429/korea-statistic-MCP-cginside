"""큐레이션 지표의 **이름**과 그 표가 **세는 것**이 어긋나는지 센다.

실측: "중소기업_사업체수" 가 「시도별·산업중분류별·기업규모별 **기업수**」 를 가리킨다.
그 표의 항목은 T001 '기업수' 하나뿐이다 — 사업체를 세지 않는다. 그래서 답이
"중소기업 사업체수는 8,298,915개" 로 나가는데, 같은 해 전국 **사업체** 수는 636만이다.
부분이 전체보다 큰 답이 나간 셈이다.

이름에 든 측정 명사와 통계표명에 든 측정 명사를 견준다. 어긋나면 사람이 확인할 목록에 올린다.
자동으로 고치지 않는다 — 표를 바꿀지 이름을 바꿀지는 자료를 보고 정해야 한다.
"""

from __future__ import annotations

import re
import sys

MEASURE_NOUNS = [
    "사업체수", "종사자수", "근로자수", "취업자수", "실업자수", "기업체수", "기업수", "업체수",
    "자영업자수", "가구수", "인구수", "학생수", "농가수", "어가수",
    "매출액", "수출액", "수입액", "생산액", "부가가치", "영업이익", "투자액", "거래액",
    "발전량", "생산량", "소비량", "배출량", "처리량", "발생량", "등록대수",
    "증가율", "감소율", "상승률", "실업률", "고용률", "출산율", "비중", "비율", "지수",
]


def measure_of(text: str) -> str | None:
    """마지막에 나오는 측정 명사. 공백은 지우되 낱말 경계를 본다."""
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


def main() -> int:
    src = open("kosis_curation.py", encoding="utf-8").read()
    # QuickStatParam 블록마다 key / tbl_nm / description 을 뽑는다
    pattern = re.compile(
        r'"(?P<key>[^"]+)":\s*QuickStatParam\((?P<body>.*?)\n    \),',
        re.S,
    )
    mismatched: list[tuple[str, str, str, str, str]] = []
    total = 0
    for m in pattern.finditer(src):
        key = m.group("key")
        body = m.group("body")
        tbl_nm = (re.search(r'tbl_nm\s*=\s*"([^"]*)"', body) or [None, ""])[1]
        desc = (re.search(r'description\s*=\s*"([^"]*)"', body) or [None, ""])[1]
        if not tbl_nm:
            continue
        total += 1
        asked = measure_of(desc or key)
        table = measure_of(tbl_nm)
        if asked and table and asked != table:
            mismatched.append((key, desc, asked, table, tbl_nm))

    lines = [
        f"큐레이션 지표 {total}개 중 이름과 표의 측정이 어긋난 것: {len(mismatched)}개",
        "",
    ]
    for key, desc, asked, table, tbl_nm in sorted(mismatched):
        lines.append(f"  {key}")
        lines.append(f"     이름이 세는 것: {asked}   (description={desc!r})")
        lines.append(f"     표가 세는 것:   {table}   ({tbl_nm})")
    out_path = sys.argv[1] if len(sys.argv) > 1 else "measure_label_report.txt"
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
    print(f"total={total} mismatched={len(mismatched)} -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
