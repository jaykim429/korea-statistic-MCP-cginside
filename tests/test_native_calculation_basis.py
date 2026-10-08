import asyncio

import kosis_mcp_server as server
from kosis_curation import TIER_A_STATS


def test_proxy_identity_uses_actual_item_and_keeps_curated_scope():
    param = TIER_A_STATS["소상공인_사업체수"]
    basis = server._curated_series_basis(param, [{"ITM_NM": "기업체수", "UNIT_NM": "개"}])
    assert basis["actual_measure"] == "기업체수"
    assert "소상공인" in basis["measure_definition"]
    assert "사업체" not in basis["measure_definition"]
    assert basis["tbl_id"] == param.tbl_id


def test_scalar_and_series_share_unit_precedence_and_do_not_mix_units():
    assert server._effective_kosis_unit("100만달러", "천달러") == ("100만달러", "")
    assert server._effective_kosis_unit("명 건", "명") == ("명", "")
    assert server._effective_kosis_unit("2020＝100", "지수") == ("지수", "2020＝100")
    assert server._effective_kosis_unit("건", "천명당 건") == ("천명당 건", "")
    param = TIER_A_STATS["수출액"]
    assert server._curated_series_basis(param, [{"UNIT_NM": "100만달러"}, {"UNIT_NM": "천달러"}])["코드"] == "UNIT_MISMATCH"


def test_native_comparison_preserves_basis_and_zero(monkeypatch):
    basis = {"actual_measure": "기업체수", "measure_definition": "소상공인 기업체수",
        "org_id": "142", "tbl_id": "TEST", "통계표": "실제 표", "통계명": "소상공인 기업체수",
        "단위": "개", "시계열": [{"시점": "2023", "값": "0"}, {"시점": "2024", "값": "3"}]}

    async def trend(*args, **kwargs):
        return basis

    monkeypatch.setattr(server, "quick_trend", trend)
    result = asyncio.run(server.stat_time_compare("소상공인 사업체 수", api_key="dummy"))
    assert result["actual_measure"] == "기업체수"
    assert result["measure_definition"] == "소상공인 기업체수"
    assert result["tbl_id"] == "TEST"
    assert result["비교"]["시작"]["값"] == 0
    assert result["비교"]["변화율_퍼센트"] is None


def test_native_comparison_keeps_incompatible_unit_failure(monkeypatch):
    async def trend(*args, **kwargs):
        return {"오류": "단위 충돌", "코드": "UNIT_MISMATCH"}

    monkeypatch.setattr(server, "quick_trend", trend)
    result = asyncio.run(server.stat_time_compare("소상공인 사업체 수", api_key="dummy"))
    assert result["상태"] == "failed"
    assert result["코드"] == "UNIT_MISMATCH"
    assert "비교" not in result
