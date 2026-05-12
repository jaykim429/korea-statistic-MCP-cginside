"""Quick live-test the 14 keywords added in commit 9ba1a9a so the
curator can confirm KOSIS returns sensible values for every new
Tier A entry before more keywords pile on top.
"""
from __future__ import annotations
import asyncio
import io
import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import quick_stat


CASES = [
    ("조출생률", "전국"),
    ("조사망률", "전국"),
    ("자연증가율", "전국"),
    ("조혼인율", "전국"),
    ("조이혼율", "전국"),
    ("자연증가건수", "전국"),
    ("평균초혼연령_남", "전국"),
    ("평균초혼연령_여", "전국"),
    ("아파트전세가격지수", "서울"),
    ("자동차등록대수", "전국"),
    ("의사수", "서울"),
    ("교통사고발생건수", "경기"),
    ("초미세먼지_PM25", "전국"),
    ("미세먼지_PM10", "전국"),
]


async def main() -> None:
    results: list[tuple[str, str, dict]] = []
    for key, region in CASES:
        try:
            r = await quick_stat(key, region, "latest")
        except Exception as exc:
            r = {"오류": repr(exc)}
        results.append((key, region, r))

    print(f"{'키워드':<22s} {'지역':<6s} {'값':<22s} {'단위':<10s} {'시점':<10s} 상태")
    print("-" * 90)
    pass_count = 0
    for key, region, r in results:
        if r.get("오류"):
            status = "❌ " + str(r["오류"])[:40]
            print(f"{key:<22s} {region:<6s} {'':<22s} {'':<10s} {'':<10s} {status}")
            continue
        if r.get("결과") == "데이터 없음":
            status = "⚠️ 데이터 없음"
            print(f"{key:<22s} {region:<6s} {'':<22s} {'':<10s} {'':<10s} {status}")
            continue
        val = str(r.get("값") or "")
        unit = str(r.get("단위") or "")
        period = str(r.get("시점") or "")
        print(f"{key:<22s} {region:<6s} {val:<22s} {unit:<10s} {period:<10s} ✅ PASS")
        pass_count += 1
    print()
    print(f"결과: {pass_count}/{len(CASES)} PASS")


if __name__ == "__main__":
    asyncio.run(main())
