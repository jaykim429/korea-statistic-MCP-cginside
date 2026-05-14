# KOSIS Statistics

Use this skill when the user asks for Korean official statistics, KOSIS data,
regional comparisons, statistical trends, chart generation, or statistics-based
policy/business interpretation.

## Tool Routing

- Start with `plan_query` for chatbot-style natural-language statistics questions,
  then follow `select_table_for_query`, `resolve_concepts`, `query_table`, and
  `compute_indicator` when the plan asks for evidence collection.
- Use `answer_query` only as a quick Tier A compatibility shortcut. Treat its
  `deprecation_warning` and `검증_주의` fields as load-bearing.
- Use `verify_stat_claims` after `answer_query` when a numeric answer is used in
  a report, policy memo, or user-facing conclusion.
- Use `indicator_dependency_map` before answering ratio/rate questions such as
  share, closure rate, startup rate, survival rate, or loan-to-sales burden.
- Use `stat_time_compare`, `analyze_trend`, `correlate_stats`,
  `forecast_stat`, and `detect_outliers` for explicit analysis requests.
- Use chart tools only when the user asks for a graph, chart, dashboard, map-like
  comparison, or visual summary.

## Safety Rules

- Do not invent a single value when `answer_query` returns
  `NEEDS_TABLE_SELECTION`.
- Preserve the result's 기준시점, 단위, 통계표, 출처, and 검증_주의 in the final answer.
  Also surface `used_period` and `period_age_years` when present — they tell the
  user which timepoint was actually consumed and how stale it is.
- Treat `검증_주의` entries as load-bearing, not decoration. Entries that begin
  with "요청 시작 시점 …", "사용 시점 … 경과", "의도 … 감지됐으나", or
  "비교 대상 …" mean the answer did not fully satisfy the question; reflect
  that uncertainty in the response instead of citing the number bare.
- For correlation, regression, and policy-effect requests, state that
  correlation or before/after comparison does not prove causality.
- For business counts, distinguish 기업 수, 사업체 수, and 자영업자 수.
