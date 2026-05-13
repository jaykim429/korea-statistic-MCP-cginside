# Gemma 챗봇 통합 가이드

이 문서는 Gemma 4 26B 챗봇에서 KOSIS MCP 서버를 붙일 때의 권장 도구
manifest와 운영 규칙을 정리합니다. 목표는 Gemma가 많은 도구 중 즉흥적으로
고르는 구조가 아니라, 실패하기 어려운 절차형 레일을 따르도록 만드는 것입니다.

## 기본 Manifest

Gemma 기본 manifest에는 아래 도구만 노출하는 것을 권장합니다.

- `plan_query`: 모든 통계 질문의 첫 호출입니다. 의도, 지표, 차원, 시간,
  다음 단계만 만들고 실제 값을 조회하지 않습니다.
- `select_table_for_query`: 필요한 분류축을 만족하는 통계표 후보를 KOSIS
  메타데이터 기반으로 고릅니다.
- `resolve_concepts`: `서울`, `30대`, `여성`, `광역시` 같은 자연어 개념을
  선택된 표의 코드 후보로 변환합니다.
- `explore_table`: 표의 축과 코드 메타데이터가 더 필요할 때 사용합니다.
- `query_table`: 검증된 `OBJ_ID`/`ITM_ID` 필터로 KOSIS 원자료를 추출합니다.
- `search_kosis`: 표 후보가 필요하지만 `select_table_for_query`가 충분하지
  않을 때만 사용합니다.

전문가 또는 분석용 manifest에만 선택적으로 노출할 도구:

- `analyze_trend`, `chain_full_analysis`
- `chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_dashboard`

기본 manifest에서 숨길 도구:

- `answer_query`: deprecated shortcut입니다. 직접 답변을 생성할 수 있어
  partial fulfillment를 숨길 수 있습니다.
- `quick_stat`, `quick_trend`, `quick_region_compare`: 빠르지만 절차형 검증
  레일을 우회합니다.
- `verify_stat_claims`, `decode_error`, 일회성 진단 helper

기본 manifest 예시는 [gemma_manifest.default.json](gemma_manifest.default.json)을
참고하세요.

## 운영 흐름

1. 챗봇 라우터가 사용자 질문이 통계 질문인지 먼저 판단합니다.
2. 통계 질문이면 항상 `plan_query(query)`를 먼저 호출합니다.
3. `plan_query.next_call`, `suggested_workflow`, `evidence_workflow` 중 가장
   구체적인 레일을 따릅니다.
4. `metrics`는 요청 지표, `quarantined_metrics`는 충돌 또는 오염 의심 지표입니다.
   quarantined 지표를 답변에 포함하지 않습니다.
5. `semantic_dimensions`는 LLM 친화 의미 표현이고, `table_required_dimensions`는
   KOSIS 표 메타 축 매칭용 표현입니다. 후속 표 선택에는 `table_required_dimensions`
   를 사용합니다.
6. `query_table`에는 메타데이터 기반 도구가 반환한 코드만 넣습니다.
7. 산식은 `compute_indicator`가 생기기 전까지 챗봇이 수행하되, `analysis_tasks`
   와 `mcp_output_contract.llm_rules`를 따라 근거와 한계를 함께 표시합니다.

## 응답 계약

`plan_query`는 답변이 아니라 증거 수집 계획입니다. 응답에는 아래 계약이 들어갑니다.

- `mcp_output_contract.role`: `planning_only` 또는 `clarification_required`
- `mcp_output_contract.final_answer_expected`: 항상 `false`
- `mcp_output_contract.follow_up_required`: 후속 도구 호출 필요 여부
- `mcp_output_contract.failure_markers`: LLM이 실패·부분충족을 감지해야 하는 필드
- `mcp_output_contract.current_signals`: 현재 응답에서 실제로 켜진 caveat 요약
- `recommended_tool_manifest`: 기본/전문가/숨김 도구 목록
- `canonical_fields`: Gemma가 우선 읽어야 하는 정규 필드
- `deprecated_fields`: 호환을 위해 남긴 구 필드와 대체 필드 안내

Gemma는 `answer`가 `null`인 계획 응답을 사용자에게 최종 답처럼 말하면 안 됩니다.
최종 답변은 `query_table` 등 후속 도구가 만든 rows와 검증 메타를 모은 뒤 생성합니다.

## 실패 처리 규칙

아래 필드가 있으면 “자료 없음 또는 한계 있음”을 사용자에게 명시합니다.

- `status: "needs_clarification"`
- `status: "unsupported"`
- `validation_errors`
- `missing_metrics`
- `quarantined_metrics`
- `coverage_ratio`가 낮은 결과
- `row_count: 0`
- `availability: "missing"` 또는 `availability: "not_matched"`

이 경우 Gemma는 사전 지식이나 추정으로 빈 값을 채우면 안 됩니다. 가능한 후속
조치는 질문 재정의, 표 후보 재검색, 다른 지표 대체 제안입니다.

## 일관성 규칙

`router_slots`는 증거 후보일 뿐 명령이 아닙니다. `plan_query`는 후보를
정규화·검증·격리한 뒤 `metrics`, `analysis_tasks`, `conflict_decisions`를
생성합니다.

```json
{
  "metrics": [{"name": "GRDP", "role": "primary"}],
  "quarantined_metrics": [
    {
      "name": "R&D 투자 규모",
      "reason": "router_indicator_conflicts_with_intended"
    }
  ],
  "conflict_decisions": [
    {
      "type": "indicator_quarantine",
      "kept_metric": "GRDP",
      "candidate": "R&D 투자 규모"
    }
  ]
}
```

Gemma는 `metrics`만 후속 조회 대상으로 사용하고, `quarantined_metrics`는 로그나
경고로만 보존합니다.

호환을 위해 아래 legacy 필드는 당분간 유지됩니다. Gemma 기본 manifest에서는
정규 필드를 우선합니다.

- `suggested_workflow` → `evidence_workflow`
- `consistency_warnings` → `conflict_decisions`
- `router_slots_overridden` → `conflict_decisions`, `quarantined_metrics`
- `required_dimensions` → `table_required_dimensions`

## 복합 질문 처리

보고서형·복합 질문은 MCP가 문장을 완성하는 것이 아니라 evidence bundle을 만듭니다.

예:

```text
최근 5년간 소상공인 사업체 수, 종사자 수, 매출액, 폐업률을 한 표로 정리해줘.
```

권장 처리:

1. `plan_query`가 `metrics[]`, `time_request`, `analysis_tasks[]`를 추출합니다.
2. 각 metric별로 `select_table_for_query`와 `resolve_concepts`를 수행합니다.
3. `query_table`로 raw rows를 모읍니다.
4. 매칭 실패 metric은 `missing_metrics` 또는 `availability: "not_matched"`로 둡니다.
5. 챗봇 LLM이 HTML, 표, 보고서 문장을 생성합니다.

중요한 원칙: MCP는 KOSIS 메타에서 동적으로 알 수 있는 것만 자동화합니다. 수도권
정의, 산업 그룹 정의, 정책적 해석 같은 도메인 지식은 챗봇 LLM이 담당하고,
MCP는 코드 검증, 원자료 조회, 메타 보존을 담당합니다.
