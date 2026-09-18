# -*- coding: utf-8 -*-
"""Tier A 답변이 어느 표에서 나온 값인지 밝히는가.

왜 있나 (실측 C075, 회귀22).
"중소기업 사업체 수 알려줘" → "업종별로 나눠서 보여줘" 가 실패했다. 1턴은 Tier A 직접값으로
753ms 만에 「시도별·산업중분류별·기업규모별 기업수」에서 값을 냈다. **그 표에는 산업중분류별
축이 이미 있다.** 2턴은 그 표에서 축만 바꾸면 되는데, 챗봇이 표를 이어받지 못해 처음부터 다시
찾았다 — 표 선택 판단이 세 번 흔들리고 37초를 쓰고 결국 실패했다.

원인은 두 겹이었다.
  1. `_tier_a_table_identity()` 가 20여 개 답변유형 중 `tier_a_region_comparison` **한 곳에만**
     쓰였다. 가장 흔한 `tier_a_value` 에는 없었다
  2. 그 한 곳조차 챗봇에 닿지 않았다 — MCP 는 `org_id`·`tbl_id`(영문)를 내보내는데 챗봇은
     `기관ID`·`통계표ID`·`ORG_ID`·`TBL_ID` 만 찾는다. **키가 하나도 겹치지 않았다.**

즉 이 기능은 만들어진 뒤 한 번도 동작한 적이 없다. 이 시험이 둘 다 고정한다.
"""
from __future__ import annotations

import io
import re
from pathlib import Path

import pytest

import kosis_curation as curation
from kosis_mcp_server import _tier_a_table_identity

SERVER_SOURCE = Path(__file__).resolve().parent.parent / "kosis_mcp_server.py"

#: 값이 실제로 나온 답변유형 — 후속 턴이 같은 표를 다른 축으로 볼 수 있어야 한다.
#: composite 계열은 표가 여럿일 수 있어 단일 식별자를 싣지 않는다(의도).
VALUE_ANSWER_TYPES = {
    "tier_a_value",
    "tier_a_trend",
    "tier_a_growth_rate",
    "tier_a_top_n",
    "tier_a_share_ratio",
    "tier_a_region_comparison",
}


class TestIdentityPayload:
    def test_챗봇이_찾는_한글_키를_낸다(self):
        key = next(iter(curation.TIER_A_STATS))
        got = _tier_a_table_identity(key)
        # 챗봇(utils/stat-browse-context.ts)은 이 두 키만 본다.
        assert got["기관ID"] == curation.TIER_A_STATS[key].org_id
        assert got["통계표ID"] == curation.TIER_A_STATS[key].tbl_id
        assert got["통계표명"] == curation.TIER_A_STATS[key].tbl_nm

    def test_영문_키도_남긴다(self):
        key = next(iter(curation.TIER_A_STATS))
        got = _tier_a_table_identity(key)
        assert got["org_id"] == got["기관ID"]
        assert got["tbl_id"] == got["통계표ID"]

    @pytest.mark.parametrize("key", [None, "", "없는지표"])
    def test_모르는_지표면_아무것도_싣지_않는다(self, key):
        # 빈 dict 는 ** 전개에서 아무 영향이 없다 — 호출자가 분기할 필요가 없다.
        assert _tier_a_table_identity(key) == {}

    def test_식별자가_비어_있지_않다(self):
        # 큐레이션에 org_id·tbl_id 가 비어 있으면 챗봇이 그 표로 이어갈 수 없다.
        for key, param in curation.TIER_A_STATS.items():
            assert param.org_id.strip(), key
            assert param.tbl_id.strip(), key


class TestEveryValueAnswerCarriesIt:
    """새 답변유형을 더하면서 식별자를 빠뜨리는 것을 잡는다 — 이 결함이 그렇게 생겼다."""

    @staticmethod
    def _sites() -> list[tuple[int, str, bool]]:
        lines = io.open(SERVER_SOURCE, encoding="utf-8").read().split("\n")
        out = []
        for i, row in enumerate(lines, 1):
            m = re.match(r'^\s*"답변유형": "(tier_a_[a-z_0-9]+)",\s*$', row)
            if not m:
                continue
            kind = m.group(1)
            # 그 dict 리터럴이 닫힐 때까지만 본다 — 옆 dict 를 보고 있다고 착각하면 안 된다.
            indent = len(row) - len(row.lstrip())
            body = []
            for nxt in lines[i:]:
                if nxt.strip() and (len(nxt) - len(nxt.lstrip())) < indent:
                    break
                body.append(nxt)
            out.append((i, kind, any("_tier_a_table_identity" in b for b in body)))
        return out

    def test_값을_낸_답변유형은_모두_표를_밝힌다(self):
        missing = [
            f"{kind}({line}행)"
            for line, kind, has in self._sites()
            if kind in VALUE_ANSWER_TYPES and not has
        ]
        assert not missing, "표 식별자가 빠진 답변유형: " + ", ".join(missing)

    def test_검사가_실제로_무언가를_보고_있다(self):
        # 정규식이 빗나가 0건을 훑고도 통과하면 이 시험은 아무것도 지키지 않는다.
        found = {kind for _l, kind, _h in self._sites()}
        assert VALUE_ANSWER_TYPES <= found, f"못 찾은 답변유형: {VALUE_ANSWER_TYPES - found}"
