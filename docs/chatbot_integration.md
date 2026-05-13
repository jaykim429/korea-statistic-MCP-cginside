# Gemma 챗봇 통합 가이드

이 문서는 Gemma 4 26B 챗봇에서 KOSIS MCP 서버를 노출할 때의 권장
도구 manifest와 운영 규칙을 정리합니다. 목표는 Gemma가 모든 내부 도구
중에서 즉흥적으로 고르는 구조가 아니라, 실패하기 어려운 절차형 레일을
따르도록 만드는 것입니다.

## 권장 도구 Manifest

Gemma 기본 manifest에는 아래 절차형 도구만 노출하는 것을 권장합니다.

- `plan_query`: 모든 통계 질문의 첫 호출입니다. 의도와 다음 단계만 만들고
  실제 값을 조회하지 않습니다.
- `select_table_for_query`: 지역, 연령, 성별, 시점, 산업 등 필수 축을 만족하는
  통계표 후보를 메타데이터 기반으로 고릅니다.
- `resolve_concepts`: `서울`, `30대`, `여성`, `광역시` 같은 자연어 개념을
  선택된 표의 코드 후보로 변환합니다.
- `explore_table`: 표의 축과 코드 메타데이터가 필요할 때 사용합니다.
- `query_table`: 검증된 `OBJ_ID`/`ITM_ID` 필터로 KOSIS 원자료를 추출합니다.
- `search_kosis`: `plan_query` 또는 후속 선택기가 표 후보를 필요로 할 때만
  사용합니다.

같은 manifest에 추가될 예정인 도구는 아래와 같습니다.

- `compute_indicator`: `per_capita`, `share`, `ratio`, `growth_rate`처럼
  허용된 enum 산식만 계산합니다.

## 기본적으로 숨길 도구

아래 도구는 내부 운영, 회귀 테스트, 전문가용 manifest에는 남길 수 있지만
Gemma 기본 manifest에는 넣지 않는 것을 권장합니다.

- `answer_query`: 직접 답변을 생성할 수 있어 partial fulfillment를 숨길 수
  있으므로 Gemma manifest에서는 deprecated입니다.
- `quick_stat`, `quick_trend`, `quick_region_compare`: 빠른 Tier A shortcut이지만
  절차형 계획 레일을 우회합니다.
- `chain_full_analysis`, `chart_*`: 고수준 분석/시각화 도구입니다. Gemma는 먼저
  검증된 raw 데이터를 모은 뒤 필요할 때 표현 방식을 결정하는 편이 안전합니다.
- `verify_stat_claims`, `decode_error`, 기타 진단용 helper: admin/debug
  manifest에서만 노출하는 편이 좋습니다.

## 운영 흐름

1. 챗봇 라우터가 사용자 질문이 통계 질문인지 먼저 판단합니다.
2. 통계 질문이면 항상 `plan_query(query)`를 먼저 호출합니다.
3. `plan_query.next_call` 또는 `suggested_workflow`를 따라갑니다.
4. `consistency_warnings`가 비어 있지 않으면 `router_slots`보다
   `intended_dimensions`를 우선합니다.
5. `query_table`에는 메타데이터 기반 도구가 반환한 코드만 넣습니다.
6. 산식은 `compute_indicator`의 허용 enum으로만 계산합니다.

## 일관성 규칙

`plan_query`는 `intended_dimensions`와 `router_slots`를 함께 반환합니다.
두 필드의 권위는 다릅니다.

- `intended_dimensions`: 사용자 의도를 기준으로 만든 primary 계획입니다.
- `router_slots`: legacy router가 남긴 secondary 디버깅 컨텍스트입니다.

둘이 충돌하면 `plan_query`는 아래처럼 정책과 경고를 함께 반환합니다.

```json
{
  "consistency_policy": {
    "rule": "primary_wins",
    "primary_source": "intended_dimensions",
    "secondary_source": "router_slots"
  },
  "consistency_warnings": [
    {
      "type": "indicator_conflict",
      "primary": "GRDP",
      "router_slot": "R&D 투자 규모",
      "resolution": "primary_indicator_wins"
    }
  ],
  "router_slots_overridden": {
    "indicator": {
      "original": "R&D 투자 규모",
      "used": "GRDP",
      "resolution": "primary_indicator_wins"
    }
  }
}
```

Gemma는 `consistency_warnings`를 차단 조건이 아니라 가드레일로 취급해야 합니다.
응답 생성에는 `primary` 값을 사용하고, 경고는 로그나 사용자-visible caveat에
보존하는 것을 권장합니다.
