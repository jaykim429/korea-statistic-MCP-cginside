# KOSIS 원응답 견본 — 단순 질문의 잘못된 거절 (SIM18 · CMP04)

`kosis-raw.json` 은 라이브 KOSIS OpenAPI 의 **원응답 그대로**다(2026-11-03 수집, API 키 제거).
손으로 만든 모양이 아니다. KOSIS 가 표를 고치면 다시 받는다.

| 키 | 내용 |
|---|---|
| `meta.<표>.TBL/ITM/PRD` | `statisticsData.do?method=getMeta` 원행 — `101/DT_1J22042`(월별 소비자물가 등락률) · `101/INH_1EA1045`(농가수(시도/시/군/구)) |
| `survey.<표>` | 조사명 원행 |
| `data_calls[]` | `Param/statisticsParameterData.do` 원행 — DT_1J22042 ITEM T03·I 0 (2026.09 2.9, **UNIT_NM 없음**) · INH_1EA1045 ITEM T00·A 000 (2024 973,707 가구) · T00·A 37 경북 (162,801 가구) · T01 논벼·A 000 (364,578 가구) |

쓰는 시험: `tests/test_simple_refusal_contracts.py`.
