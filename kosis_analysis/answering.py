from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class AnswerStat:
    key: str
    label: str
    value: float
    formatted: str
    unit: str
    period: str
    table: str
    region: str
    source: str = "통계청 KOSIS"

    def to_row(self) -> dict[str, Any]:
        return {
            "지표": self.label,
            "값": self.formatted,
            "원값": self.value,
            "단위": self.unit,
            "시점": self.period,
            "지역": self.region,
            "통계표": self.table,
            "출처": self.source,
        }


@dataclass(frozen=True)
class AnswerPlan:
    """Execution decision for answer_query.

    The planner decides *what* should run. The engine still owns the actual
    KOSIS calls and rendering so this first refactor stays behavior-preserving.
    """
    action: str
    region: str
    direct_key: Optional[str] = None
    params: dict[str, Any] = field(default_factory=dict)


class AnswerPlanner:
    """Turn a routed natural-language query into one executable action."""

    def __init__(self, engine: "NaturalLanguageAnswerEngine") -> None:
        self.engine = engine

    def build(self, query: str, region: str, route_payload: dict[str, Any]) -> AnswerPlan:
        effective_region = self.engine._effective_region(route_payload, region)
        direct_key = self.engine._infer_direct_stat_key(query, route_payload)

        if self.engine._is_self_employed_sme_population_question(query):
            return AnswerPlan("mixed_population", effective_region)
        if self.engine._is_sme_large_sales_question(query):
            return AnswerPlan("sme_large_sales", effective_region)
        if self.engine._is_sme_smallbiz_count_question(query):
            return AnswerPlan("sme_smallbiz_counts", effective_region)
        if self.engine._is_sme_employee_average_question(query):
            return AnswerPlan("sme_employee_average", effective_region)
        if self.engine._is_dynamic_ratio_question(query):
            return AnswerPlan("dynamic_ratio", effective_region)

        if self.engine._needs_advanced_analysis_plan(route_payload):
            intents = route_payload.get("intents") or []
            if "STAT_CORRELATION" in intents:
                stat_x, stat_y = self.engine._extract_correlation_pair(query)
                if stat_x and stat_y:
                    return AnswerPlan(
                        "auto_correlate",
                        effective_region,
                        params={"stat_x": stat_x, "stat_y": stat_y},
                    )
            return AnswerPlan("search_fallback", effective_region)

        if direct_key:
            composites = self.engine._extract_composite_regions(query)
            if composites:
                operation = (
                    "share"
                    if self.engine._is_share_ratio_question(query, route_payload)
                    else "sum"
                )
                return AnswerPlan(
                    "composite_aggregate",
                    effective_region,
                    direct_key,
                    {"composite": composites[0], "operation": operation},
                )

            if self.engine._is_aggregation_question(query):
                extras = self.engine._extract_extra_regions(query, effective_region, route_payload)
                regions = [effective_region] + [r for r in extras if r != effective_region]
                if len(regions) >= 2:
                    return AnswerPlan("region_sum", effective_region, direct_key, {"regions": regions})

            if self.engine._is_top_n_question(query, route_payload):
                return AnswerPlan(
                    "top_n",
                    effective_region,
                    direct_key,
                    {
                        "top_n": self.engine._extract_top_n(query) or 5,
                        "include_share_ratio": self.engine._is_share_ratio_question(query, route_payload),
                    },
                )

            if self.engine._is_region_compare_question(query):
                return AnswerPlan("region_compare", effective_region, direct_key)

            if (
                self.engine._is_share_ratio_question(query, route_payload)
                and effective_region != "전국"
                and not self.engine._is_growth_question(query, route_payload)
            ):
                return AnswerPlan("share_ratio", effective_region, direct_key)

            return AnswerPlan("direct", effective_region, direct_key)

        return AnswerPlan("search_fallback", effective_region)
