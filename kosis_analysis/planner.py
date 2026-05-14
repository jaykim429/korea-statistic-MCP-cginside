from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional

from kosis_curation import (
    REGION_COMPOSITES,
    REGION_DEMOGRAPHIC,
    route_query as _route_query,
)
from kosis_analysis.metadata import _compact_text, _normalize_required_dimensions


@dataclass(frozen=True)
class WorkflowStep:
    step: int
    tool: str
    purpose: str
    args: dict[str, Any]
    available_now: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "tool": self.tool,
            "purpose": self.purpose,
            "args": self.args,
            "available_now": self.available_now,
        }


class QueryWorkflowPlanner:
    """Plan a safe procedural workflow for a statistical question.

    MUST NOT:
    - choose a final KOSIS table ID; that belongs to select_table_for_query
    - map natural concepts to ITM_ID/OBJ_ID codes; that belongs to resolve_concepts
    - fetch actual statistical values; that belongs to query_table
    - calculate ratios, sums, growth rates, or per-capita values; that belongs to compute_indicator

    This class extracts intent, dimensions, and the next tool sequence only.
    """

    GEMMA_DEFAULT_TOOLS = [
        "plan_query",
        "select_table_for_query",
        "resolve_concepts",
        "explore_table",
        "query_table",
        "compute_indicator",
        "search_kosis",
    ]
    GEMMA_EXPERT_TOOLS = [
        "analyze_trend",
        "chain_full_analysis",
        "chart_line",
        "chart_compare_regions",
        "chart_correlation",
        "chart_dashboard",
    ]
    GEMMA_HIDDEN_TOOLS = [
        "answer_query",
        "quick_stat",
        "quick_trend",
        "quick_region_compare",
        "verify_stat_claims",
        "decode_error",
    ]
    AVAILABLE_TOOLS = set(GEMMA_DEFAULT_TOOLS)
    EN_REGION_ALIASES = {
        "seoul": "서울", "busan": "부산", "daegu": "대구", "incheon": "인천",
        "gwangju": "광주", "daejeon": "대전", "ulsan": "울산", "sejong": "세종",
        "gyeonggi": "경기", "gangwon": "강원", "chungbuk": "충북", "chungnam": "충남",
        "jeonbuk": "전북", "jeonnam": "전남", "gyeongbuk": "경북", "gyeongnam": "경남",
        "jeju": "제주", "korea": "전국", "national": "전국",
    }
    EN_INDICATOR_ALIASES = {
        "gdp deflator": "GDP 디플레이터",
        "gross domestic product": "GDP",
        "gross national product": "국민총생산",
        "gross national income": "국민총소득",
        "gross domestic income": "국내총소득",
        "gross regional domestic product": "GRDP",
        "purchasing power parity": "구매력평가",
        "economically active population": "경제활동인구",
        "labor force participation rate": "경제활동참가율",
        "foreign direct investment": "외국인직접투자",
        "mergers and acquisitions": "인수합병",
        "unemployment rate": "실업률",
        "employment rate": "고용률",
        "population": "인구",
        "cpi": "소비자물가지수",
        "consumer price index": "소비자물가지수",
        "ppi": "생산자물가지수",
        "producer price index": "생산자물가지수",
        "grdp": "GRDP",
        "gdp": "GDP",
        "gnp": "국민총생산",
        "gni": "국민총소득",
        "gdi": "국내총소득",
        "ppp": "구매력평가",
        "eap": "경제활동인구",
        "ur": "실업률",
        "lfpr": "경제활동참가율",
        "fdi": "외국인직접투자",
        "m&a": "인수합병",
    }
    CONSISTENCY_POLICY = {
        "rule": "primary_wins",
        "primary_source": "intended_dimensions",
        "secondary_source": "router_slots",
        "note": "충돌 시 intended_dimensions가 권위입니다. router_slots는 디버깅용 보조 정보입니다.",
    }
    GENERIC_ROUTER_INDICATORS = {"비중", "비율", "구성비", "증가율", "감소율", "변화율"}
    TIME_GROUP_BY_TERMS = {
        "year", "years", "annual", "month", "months", "monthly",
        "quarter", "quarters", "period", "prd", "time",
        "연도", "연", "년", "월", "분기", "시점", "기간",
    }
    INDICATOR_ALTERNATIVES = {
        "인구": [
            {"label": "주민등록인구", "default": True, "scope": "주민등록 기준, 외국인 제외"},
            {"label": "추계인구", "scope": "장래인구추계 기준, 내·외국인 포함 가능, 미래 시점은 추계"},
            {"label": "인구주택총조사 인구", "scope": "센서스 기준, 조사 주기와 기준시점 확인 필요"},
        ],
    }

    def build(self, query: str) -> dict[str, Any]:
        route_payload = _route_query(query).to_agent_payload()
        dimensions = self._dimensions(query, route_payload)
        calculations = self._calculations(query, route_payload)
        intent = self._intent(route_payload, calculations, dimensions)
        concepts = self._concepts(dimensions, calculations)
        semantic_required = self._required_dimensions(query, dimensions, calculations, route_payload)
        table_required = self._table_required_dimensions(semantic_required)
        raw_metrics = self._metrics(query, dimensions, route_payload)
        metrics, quarantined_metrics, metric_decisions = self._validate_metrics(raw_metrics, dimensions, route_payload, query)
        bundle_dimensions = self._bundle_dimensions(query, dimensions, semantic_required, route_payload)
        semantic_dimensions = self._semantic_dimensions(dimensions, route_payload)
        comparison_targets = self._comparison_targets(query, dimensions, route_payload)
        time_request = self._time_request(dimensions, route_payload)
        rank_candidates = self._rank_candidates(query, route_payload)
        analysis_tasks = self._analysis_tasks(
            query,
            intent,
            dimensions,
            calculations,
            route_payload,
            metrics,
            bundle_dimensions,
            rank_candidates,
        )
        presentation = self._presentation_spec(query, route_payload)
        handoff_to_llm = self._handoff_to_llm(query, analysis_tasks, presentation)
        analysis_mode = self._analysis_mode(metrics, bundle_dimensions, comparison_targets, analysis_tasks, handoff_to_llm)
        evidence_bundle = analysis_mode == "composite_analysis"
        conflict_decisions = metric_decisions + self._time_conflict_decisions(dimensions, route_payload)
        consistency_warnings = self._legacy_consistency_warnings(conflict_decisions)

        confidence = "medium"
        if dimensions.get("indicator") and table_required:
            confidence = "high"
        elif route_payload.get("route", {}).get("type") == "miss":
            confidence = "low"
        if self._needs_clarification(dimensions, concepts, calculations, route_payload, metrics, analysis_tasks):
            return self._clarification_response(query, dimensions, concepts, calculations, route_payload)

        workflow = self._workflow(query, intent, table_required, concepts, dimensions, calculations)

        return {
            "상태": "planned",
            "status": "planned",
            "answer": None,
            "intent": intent,
            "query": query,
            "verification_level": "planning_only",
            "confidence": confidence,
            "intended_dimensions": dimensions,
            "required_dimensions": table_required,
            "semantic_required_dimensions": semantic_required,
            "table_required_dimensions": table_required,
            "semantic_dimensions": semantic_dimensions,
            "concepts": concepts,
            "calculations": calculations,
            "analysis_mode": analysis_mode,
            "evidence_bundle": evidence_bundle,
            "metrics": metrics,
            "raw_metric_candidates": raw_metrics,
            "quarantined_metrics": quarantined_metrics,
            "dimensions": bundle_dimensions,
            "comparison_targets": comparison_targets,
            "time_request": time_request,
            "analysis_tasks": analysis_tasks,
            "presentation": presentation,
            "handoff_to_llm": handoff_to_llm,
            "metric_availability_policy": {
                "initial_availability": "unknown",
                "resolved_by": "select_table_for_query",
                "allowed_statuses": ["matched", "weak_match", "not_matched"],
                "note": "plan_query extracts requested metrics only; KOSIS availability is verified later from metadata.",
            },
            "mcp_output_contract": self._output_contract(
                role="planning_only",
                final_answer_expected=False,
                follow_up_required=True,
                metrics=metrics,
                quarantined_metrics=quarantined_metrics,
                conflict_decisions=conflict_decisions,
                analysis_tasks=analysis_tasks,
            ),
            "recommended_tool_manifest": self._tool_manifest_profile(),
            "canonical_fields": {
                "workflow": "evidence_workflow",
                "conflicts": "conflict_decisions",
                "table_dimensions": "table_required_dimensions",
            },
            "deprecated_fields": {
                "suggested_workflow": "Legacy compatibility field. Prefer evidence_workflow.",
                "consistency_warnings": "Legacy compatibility field. Prefer conflict_decisions.",
                "router_slots_overridden": "Legacy compatibility field. Prefer conflict_decisions and quarantined_metrics.",
                "required_dimensions": "Legacy compatibility field. Prefer table_required_dimensions.",
            },
            "consistency_policy": {
                "rule": "candidate_normalize_validate_emit",
                "note": "router_slots는 증거 후보입니다. metrics와 analysis_tasks는 정제·격리된 후보에서만 생성됩니다.",
            },
            "conflict_decisions": conflict_decisions,
            "consistency_warnings": consistency_warnings,
            "router_slots_overridden": self._router_slot_overrides(consistency_warnings),
            "suggested_workflow": [step.to_dict() for step in workflow],
            "evidence_workflow": self._evidence_workflow(metrics, bundle_dimensions, analysis_tasks),
            "next_call": workflow[0].to_dict() if workflow else None,
            "route": route_payload.get("route", {}),
            "router_slots": route_payload.get("slots", {}),
            "validation": route_payload.get("validation", {}),
            "must_not": [
                "통계표 ID 확정 금지",
                "ITM_ID/OBJ_ID 코드 매핑 금지",
                "실제 값 반환 금지",
                "산술·산식 계산 금지",
            ],
            "notes": [
                "plan_query는 절차형 레일만 생성합니다. 값 조회와 계산은 후속 도구가 담당합니다.",
                "available_now=false인 도구는 후속 PR에서 추가될 예정인 파이프라인 단계입니다.",
            ],
        }

    @staticmethod
    def _needs_clarification(
        dimensions: dict[str, Any],
        concepts: list[str],
        calculations: list[str],
        route_payload: dict[str, Any],
        metrics: Optional[list[dict[str, Any]]] = None,
        analysis_tasks: Optional[list[dict[str, Any]]] = None,
    ) -> bool:
        route = route_payload.get("route") or {}
        if not metrics and not analysis_tasks:
            return True
        if route.get("type") != "miss":
            return False
        if metrics or analysis_tasks:
            return False
        has_statistical_anchor = bool(
            dimensions.get("indicator")
            or dimensions.get("event")
            or dimensions.get("industry")
            or calculations
            or route.get("type") != "miss"
        )
        return not has_statistical_anchor and not concepts

    @staticmethod
    def _clarification_response(
        query: str,
        dimensions: dict[str, Any],
        concepts: list[str],
        calculations: list[str],
        route_payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "상태": "needs_clarification",
            "status": "needs_clarification",
            "answer": None,
            "intent": "unknown",
            "query": query,
            "verification_level": "planning_only",
            "confidence": "low",
            "intended_dimensions": dimensions,
            "required_dimensions": [],
            "concepts": concepts,
            "calculations": calculations,
            "analysis_mode": "needs_clarification",
            "evidence_bundle": False,
            "metrics": [],
            "dimensions": [],
            "comparison_targets": [],
            "time_request": None,
            "analysis_tasks": [],
            "presentation": {"formats": []},
            "handoff_to_llm": {
                "final_answer_expected": False,
                "reason": "insufficient_statistical_anchor",
            },
            "metric_availability_policy": {
                "initial_availability": "unknown",
                "resolved_by": "select_table_for_query",
                "allowed_statuses": ["matched", "weak_match", "not_matched"],
            },
            "mcp_output_contract": QueryWorkflowPlanner._output_contract(
                role="clarification_required",
                final_answer_expected=False,
                follow_up_required=False,
                metrics=[],
                quarantined_metrics=[],
                conflict_decisions=[],
                analysis_tasks=[],
            ),
            "recommended_tool_manifest": QueryWorkflowPlanner._tool_manifest_profile(),
            "canonical_fields": {
                "workflow": "evidence_workflow",
                "conflicts": "conflict_decisions",
                "table_dimensions": "table_required_dimensions",
            },
            "deprecated_fields": {
                "suggested_workflow": "Legacy compatibility field. Prefer evidence_workflow.",
                "consistency_warnings": "Legacy compatibility field. Prefer conflict_decisions.",
                "router_slots_overridden": "Legacy compatibility field. Prefer conflict_decisions and quarantined_metrics.",
                "required_dimensions": "Legacy compatibility field. Prefer table_required_dimensions.",
            },
            "consistency_policy": QueryWorkflowPlanner.CONSISTENCY_POLICY,
            "consistency_warnings": [],
            "router_slots_overridden": {},
            "suggested_workflow": [],
            "evidence_workflow": [],
            "next_call": None,
            "suggested_clarification_questions": [
                "어떤 통계 지표를 보고 싶으신가요? 예: 인구, 실업률, 소비자물가지수, GRDP",
                "지역이나 기간 기준이 있나요? 예: 서울 2020년, 최근 5년, 전국 최신",
                "단일값, 추이, 지역 비교, 비중 계산 중 어떤 형태가 필요하신가요?",
            ],
            "route": route_payload.get("route", {}),
            "router_slots": route_payload.get("slots", {}),
            "validation": route_payload.get("validation", {}),
            "must_not": [
                "통계표 ID 확정 금지",
                "ITM_ID/OBJ_ID 코드 매핑 금지",
                "실제 값 반환 금지",
                "산술·산식 계산 금지",
            ],
            "notes": [
                "통계 지표나 분석 대상이 충분히 드러나지 않아 실행 레일을 만들지 않았습니다.",
                "질문을 구체화한 뒤 plan_query를 다시 호출하세요.",
            ],
        }

    @classmethod
    def _tool_manifest_profile(cls) -> dict[str, Any]:
        return {
            "profile": "gemma_default",
            "version": "1.0",
            "compatible_with": ">=0.6.0",
            "expose": cls.GEMMA_DEFAULT_TOOLS,
            "expert_optional": cls.GEMMA_EXPERT_TOOLS,
            "hide_by_default": cls.GEMMA_HIDDEN_TOOLS,
            "rule": "Gemma should enter statistics workflows through plan_query, then follow the procedural evidence workflow.",
        }

    @staticmethod
    def _output_contract(
        *,
        role: str,
        final_answer_expected: bool,
        follow_up_required: bool,
        metrics: list[dict[str, Any]],
        quarantined_metrics: list[dict[str, Any]],
        conflict_decisions: list[dict[str, Any]],
        analysis_tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_signals = QueryWorkflowPlanner._current_failure_signals(
            metrics=metrics,
            quarantined_metrics=quarantined_metrics,
            conflict_decisions=conflict_decisions,
            analysis_tasks=analysis_tasks,
            role=role,
        )
        return {
            "role": role,
            "final_answer_expected": final_answer_expected,
            "follow_up_required": follow_up_required,
            "machine_readable_status": True,
            "current_signals": current_signals,
            "llm_rules": [
                "Do not present plan_query output as a statistical answer.",
                "Do not invent values for metrics whose availability is unknown, weak, missing, or unsupported.",
                "Use metrics, quarantined_metrics, conflict_decisions, and time_request when deciding the next tool call.",
                "If a follow-up tool returns unsupported or empty rows, report that limitation instead of filling the gap from prior knowledge.",
            ],
            "failure_markers": [
                "needs_clarification",
                "unsupported",
                "not_matched",
                "missing_metrics",
                "quarantined_metrics",
                "validation_errors",
                "coverage_ratio",
            ],
        }

    @staticmethod
    def _current_failure_signals(
        *,
        metrics: list[dict[str, Any]],
        quarantined_metrics: list[dict[str, Any]],
        conflict_decisions: list[dict[str, Any]],
        analysis_tasks: list[dict[str, Any]],
        role: str,
    ) -> dict[str, Any]:
        unknown_metrics = [
            metric.get("name")
            for metric in metrics
            if metric.get("availability") in {None, "unknown", "weak_match", "missing", "not_matched"}
        ]
        markers: list[str] = []
        if role == "clarification_required":
            markers.append("needs_clarification")
        if not metrics:
            markers.append("missing_metrics")
        if quarantined_metrics:
            markers.append("quarantined_metrics")
        if conflict_decisions:
            markers.append("conflict_decisions")
        if unknown_metrics:
            markers.append("metric_availability_unverified")
        return {
            "has_failures": bool(quarantined_metrics or not metrics or role == "clarification_required"),
            "has_caveats": bool(markers),
            "markers_present": markers,
            "quarantined_metrics_count": len(quarantined_metrics),
            "conflict_decisions_count": len(conflict_decisions),
            "unknown_metric_count": len(unknown_metrics),
            "analysis_task_count": len(analysis_tasks),
            "unknown_metrics": unknown_metrics,
            "explanation": (
                "The planner could not identify an executable metric. Ask a more specific statistical question."
                if not metrics else
                "plan_query has not verified metric availability yet; follow-up metadata tools must confirm tables and codes."
                if unknown_metrics else "No immediate planning caveats detected."
            ),
        }

    @staticmethod
    def _table_required_dimensions(required: list[str]) -> list[str]:
        """Convert semantic dimensions to KOSIS metadata axis requirements."""
        mapping = {
            "regions": "region",
            "industries": "industry",
            "ages": "age",
        }
        output: list[str] = []
        for item in required:
            normalized = mapping.get(item, item)
            if normalized not in output:
                output.append(normalized)
        return output

    @staticmethod
    def _semantic_dimensions(dimensions: dict[str, Any], route_payload: dict[str, Any]) -> dict[str, Any]:
        semantic: dict[str, Any] = {}
        for key in ("region", "regions", "region_group", "age", "sex", "industry", "time", "event"):
            if dimensions.get(key) is not None:
                semantic[key] = dimensions[key]
        slots = route_payload.get("slots") or {}
        if isinstance(slots, dict):
            if slots.get("target"):
                semantic["scale"] = slots["target"]
            elif slots.get("scale"):
                semantic["scale"] = slots["scale"]
        return semantic

    def _validate_metrics(
        self,
        raw_metrics: list[dict[str, Any]],
        dimensions: dict[str, Any],
        route_payload: dict[str, Any],
        query: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        slots = route_payload.get("slots") or {}
        router_indicator = slots.get("indicator") if isinstance(slots, dict) else None
        intended_indicator = dimensions.get("indicator")
        conflict = self._indicator_conflicts(intended_indicator, router_indicator)
        explicit_multi = self._explicit_multi_metric_query(query, slots)

        metrics: list[dict[str, Any]] = []
        quarantined: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add_metric(metric: dict[str, Any]) -> None:
            name = metric.get("name")
            if not isinstance(name, str) or not name.strip():
                return
            key = self._metric_key(name)
            if key in seen:
                return
            seen.add(key)
            item = dict(metric)
            item.setdefault("availability", "unknown")
            metrics.append(item)

        for metric in raw_metrics:
            source = metric.get("source")
            name = metric.get("name")
            if conflict and not explicit_multi and source == "router_slots.indicator" and name == router_indicator:
                quarantined.append({
                    "name": name,
                    "reason": "router_indicator_conflicts_with_intended",
                    "source": source,
                    "availability": "not_used_in_plan",
                })
                decisions.append({
                    "type": "indicator_quarantine",
                    "candidate": name,
                    "source": source,
                    "reason": "conflicts_with_intended_indicator",
                    "kept_metric": intended_indicator,
                    "policy": "conflicting_router_quarantined",
                })
                continue
            add_metric(metric)

        if not metrics and raw_metrics:
            add_metric(raw_metrics[0])
        return metrics, quarantined, decisions

    @staticmethod
    def _explicit_multi_metric_query(query: str, slots: Any) -> bool:
        secondary = slots.get("secondary_indicators") if isinstance(slots, dict) else None
        if isinstance(secondary, list) and secondary:
            return True
        return any(token in query for token in (",", "·", "/"))

    def _time_conflict_decisions(
        self,
        dimensions: dict[str, Any],
        route_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        slots = route_payload.get("slots") or {}
        if not isinstance(slots, dict):
            return []
        primary_time = dimensions.get("time")
        slot_time = slots.get("time")
        if not self._value_conflicts(primary_time, slot_time):
            return []
        return [{
            "type": "time_conflict",
            "primary": primary_time,
            "router_slot": slot_time,
            "resolution": "intended_time_used",
            "policy": "query_or_intended_time_preferred",
        }]

    @staticmethod
    def _legacy_consistency_warnings(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        for decision in decisions:
            if decision.get("type") == "indicator_quarantine":
                warnings.append({
                    "type": "indicator_conflict",
                    "slot": "indicator",
                    "primary": decision.get("kept_metric"),
                    "router_slot": decision.get("candidate"),
                    "resolution": "conflicting_router_quarantined",
                    "message": (
                        f"router_slots.indicator '{decision.get('candidate')}' was quarantined "
                        f"because it conflicts with intended indicator '{decision.get('kept_metric')}'."
                    ),
                })
            elif decision.get("type") == "time_conflict":
                warnings.append({
                    "type": "time_conflict",
                    "slot": "time",
                    "primary": decision.get("primary"),
                    "router_slot": decision.get("router_slot"),
                    "resolution": "intended_time_used",
                    "message": "router_slots.time differs from normalized intended time.",
                })
        return warnings

    @staticmethod
    def _consistency_warnings(
        dimensions: dict[str, Any],
        route_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        slots = route_payload.get("slots") or {}
        if not isinstance(slots, dict):
            return warnings

        primary_indicator = dimensions.get("indicator")
        slot_indicator = slots.get("indicator")
        if QueryWorkflowPlanner._indicator_conflicts(primary_indicator, slot_indicator):
            warnings.append({
                "type": "indicator_conflict",
                "slot": "indicator",
                "primary": primary_indicator,
                "router_slot": slot_indicator,
                "resolution": "primary_indicator_wins",
                "message": (
                    f"plan_query primary indicator '{primary_indicator}' differs from "
                    f"router_slots.indicator '{slot_indicator}'. Use primary indicator."
                ),
            })

        primary_region = dimensions.get("regions") or dimensions.get("region")
        slot_region = slots.get("region")
        if QueryWorkflowPlanner._scalar_conflicts(primary_region, slot_region):
            warnings.append({
                "type": "region_conflict",
                "slot": "region",
                "primary": primary_region,
                "router_slot": slot_region,
                "resolution": "primary_region_wins",
                "message": (
                    f"plan_query primary region '{primary_region}' differs from "
                    f"router_slots.region '{slot_region}'. Use primary region."
                ),
            })

        primary_time = dimensions.get("time")
        slot_time = slots.get("time")
        if QueryWorkflowPlanner._value_conflicts(primary_time, slot_time):
            warnings.append({
                "type": "time_conflict",
                "slot": "time",
                "primary": primary_time,
                "router_slot": slot_time,
                "resolution": "primary_time_wins",
                "message": "plan_query intended time differs from router_slots.time. Use intended_dimensions.time.",
            })
        return warnings

    @staticmethod
    def _router_slot_overrides(warnings: list[dict[str, Any]]) -> dict[str, Any]:
        overrides: dict[str, Any] = {}
        for warning in warnings:
            slot = warning.get("slot")
            if not slot:
                continue
            overrides[str(slot)] = {
                "original": warning.get("router_slot"),
                "used": warning.get("primary"),
                "resolution": warning.get("resolution"),
            }
        return overrides

    @staticmethod
    def _scalar_conflicts(primary: Any, secondary: Any) -> bool:
        return (
            isinstance(primary, str)
            and isinstance(secondary, str)
            and bool(primary)
            and bool(secondary)
            and primary != secondary
        )

    @staticmethod
    def _indicator_conflicts(primary: Any, secondary: Any) -> bool:
        if not QueryWorkflowPlanner._scalar_conflicts(primary, secondary):
            return False
        if str(secondary) in QueryWorkflowPlanner.GENERIC_ROUTER_INDICATORS:
            return False
        p_norm = _compact_text(str(primary))
        s_norm = _compact_text(str(secondary))
        if p_norm and s_norm and (p_norm in s_norm or s_norm in p_norm):
            return False
        return True

    @staticmethod
    def _value_conflicts(primary: Any, secondary: Any) -> bool:
        if not primary or not secondary:
            return False
        if isinstance(primary, dict) and isinstance(secondary, dict):
            if QueryWorkflowPlanner._time_values_equivalent(primary, secondary):
                return False
        return primary != secondary

    @staticmethod
    def _time_values_equivalent(primary: dict[str, Any], secondary: dict[str, Any]) -> bool:
        if primary == secondary:
            return True
        primary_type = primary.get("type")
        secondary_type = secondary.get("type")
        if primary_type == secondary_type == "relative_period":
            if primary.get("value") and primary.get("value") == secondary.get("value"):
                return True
            if primary.get("years") and secondary.get("years") and int(primary["years"]) == int(secondary["years"]):
                return True
            if primary.get("months") and secondary.get("months") and int(primary["months"]) == int(secondary["months"]):
                return True
        if {primary_type, secondary_type} == {"year", "month"}:
            year_value = str(primary.get("value") if primary_type == "year" else secondary.get("value") or "")
            month_value = str(primary.get("value") if primary_type == "month" else secondary.get("value") or "")
            return bool(year_value and month_value.startswith(year_value) and month_value.endswith("01"))
        return False

    def _dimensions(self, query: str, route_payload: dict[str, Any]) -> dict[str, Any]:
        q_norm = _compact_text(query)
        q_lower = query.lower()
        slots = route_payload.get("slots") or {}
        dimensions: dict[str, Any] = {}

        indicator = self._indicator(query, q_norm, q_lower, route_payload)
        if indicator:
            dimensions["indicator"] = indicator
            alternatives = self.INDICATOR_ALTERNATIVES.get(indicator)
            if alternatives:
                dimensions["indicator_alternatives"] = alternatives
                dimensions["disambiguation_suggested"] = (
                    f"{indicator}는 여러 공식 통계 기준이 있습니다. 기준을 명시하면 더 정확합니다."
                )
        if "폐업" in q_norm:
            dimensions["event"] = "폐업"
        if "창업" in q_norm:
            dimensions["event"] = "창업"
        if "생존" in q_norm:
            dimensions["event"] = "생존"

        region = slots.get("region") if isinstance(slots, dict) else None
        if self._is_false_gyeonggi_region(q_norm, region):
            region = None
        if not region:
            region = self._region(q_norm, q_lower)
        if region:
            dimensions["region"] = region

        regions = self._comparison_regions(slots, q_norm, q_lower)
        if regions:
            dimensions["regions"] = regions

        composite = self._composite_region(q_norm)
        if composite:
            dimensions["region_group"] = composite

        age = self._age(query, q_norm)
        if age:
            dimensions["age"] = age

        sex = self._sex(q_norm, q_lower)
        if sex:
            dimensions["sex"] = sex

        parsed_time = self._time(query, q_norm)
        slot_time = slots.get("time") if isinstance(slots, dict) else None
        time_value = parsed_time or slot_time
        if time_value:
            dimensions["time"] = time_value

        industry = slots.get("industry") if isinstance(slots, dict) else None
        if not industry and any(term in q_norm for term in ("업종", "업종별", "산업별", "산업대분류", "산업중분류")):
            industry = "업종"
        if not industry and any(term in q_norm for term in ("치킨", "음식점")):
            industry = "음식점업"
        if not industry:
            industry = self._industry_phrase(query, q_norm)
        if industry:
            dimensions["industry"] = industry
        return dimensions

    def _indicator(self, query: str, q_norm: str, q_lower: str, route_payload: dict[str, Any]) -> Optional[str]:
        direct_key = (route_payload.get("route") or {}).get("direct_stat_key")
        if direct_key:
            return str(direct_key)
        manual = [
            ("고령화", "고령인구비중"),
            ("고령인구", "65세이상인구"),
            ("인구", "인구"),
            ("출생", "출생"),
            ("혼인", "혼인"),
            ("폐업률", "폐업률"),
            ("창업률", "창업률"),
            ("생존율", "생존율"),
            ("폐업", "폐업"),
        ]
        for token, label in manual:
            if _compact_text(token) in q_norm:
                return label
        slots = route_payload.get("slots") or {}
        if isinstance(slots, dict) and slots.get("indicator"):
            return str(slots["indicator"])
        for alias, label in self.EN_INDICATOR_ALIASES.items():
            if self._alias_in_query(alias, q_lower):
                return label
        implicit_count = self._implicit_count_indicator(query, q_norm, route_payload)
        if implicit_count:
            return implicit_count
        return None

    @staticmethod
    def _alias_in_query(alias: str, q_lower: str) -> bool:
        """Match English abbreviations without leaking short tokens into normal words."""
        if not alias:
            return False
        if re.fullmatch(r"[a-z]{2,5}", alias):
            return re.search(rf"(?<![a-z]){re.escape(alias)}(?![a-z])", q_lower) is not None
        if alias == "m&a":
            return re.search(r"(?<![a-z])m\s*&\s*a(?![a-z])", q_lower) is not None
        return alias in q_lower

    @staticmethod
    def _implicit_count_indicator(query: str, q_norm: str, route_payload: dict[str, Any]) -> Optional[str]:
        """Infer a generic count metric for business/place existence questions."""
        count_terms = (
            "얼마나있", "얼마나있어", "몇개", "몇곳", "몇군데",
            "개수", "몇개나", "몇곳이나", "현황", "수는", "수가",
        )
        if not any(term in q_norm for term in count_terms):
            return None
        slots = route_payload.get("slots") or {}
        target = ""
        if isinstance(slots, dict):
            target = " ".join(str(slots.get(key) or "") for key in ("target", "scale", "industry"))
        business_context = any(
            term in q_norm or term in _compact_text(target)
            for term in (
                "사업체", "기업", "업체", "가게", "점포", "치킨집",
                "음식점", "소상공인", "중소기업", "대기업", "회사", "업종",
            )
        )
        if not business_context:
            return None
        if "기업" in q_norm or "중소기업" in q_norm or "대기업" in q_norm:
            return "기업 수"
        return "사업체 수"

    def _region(self, q_norm: str, q_lower: str) -> Optional[str]:
        for region in sorted(REGION_DEMOGRAPHIC.keys(), key=len, reverse=True):
            if self._is_false_gyeonggi_region(q_norm, region):
                continue
            if region != "전국" and _compact_text(region) in q_norm:
                return region
        if "전국" in q_norm:
            return "전국"
        for alias, region in self.EN_REGION_ALIASES.items():
            if alias in q_lower:
                return region
        return None

    def _comparison_regions(self, slots: Any, q_norm: str, q_lower: str) -> list[str]:
        regions: list[str] = []
        targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if isinstance(targets, list):
            for target in targets:
                if isinstance(target, str):
                    if self._is_false_gyeonggi_region(q_norm, target):
                        continue
                    region = self._normalize_region(target, target.lower())
                    if region:
                        regions.append(region)
        for region in sorted(REGION_DEMOGRAPHIC.keys(), key=len, reverse=True):
            if self._is_false_gyeonggi_region(q_norm, region):
                continue
            if region != "전국" and _compact_text(region) in q_norm:
                regions.append(region)
        for alias, region in self.EN_REGION_ALIASES.items():
            if alias in q_lower:
                regions.append(region)
        unique = list(dict.fromkeys(regions))
        return unique if len(unique) >= 2 else []

    def _normalize_region(self, text: str, text_lower: str) -> Optional[str]:
        norm = _compact_text(text)
        for region in sorted(REGION_DEMOGRAPHIC.keys(), key=len, reverse=True):
            if region != "전국" and _compact_text(region) in norm:
                return region
        if "전국" in norm:
            return "전국"
        for alias, region in self.EN_REGION_ALIASES.items():
            if alias in text_lower:
                return region
        return None

    @staticmethod
    def _industry_phrase(query: str, q_norm: str) -> Optional[str]:
        """Preserve user-supplied industry terms without assigning a domain code."""
        metric_markers = (
            "평균 매출액", "매출액", "사업체 수", "사업체수", "기업 수", "기업수",
            "종사자 수", "종사자수", "폐업률", "창업률", "생존율", "수출액",
        )
        marker_pattern = "|".join(re.escape(marker).replace(r"\ ", r"\s*") for marker in metric_markers)
        match = re.search(rf"^\s*(.+?)\s*(?:{marker_pattern})", query)
        if not match:
            return None
        candidate = match.group(1)
        candidate = re.sub(r"(?:최근\s*\d+\s*(?:년|개월)|\d{4}\s*년|작년|올해|전년|지난해|재작년)", " ", candidate)
        candidate = re.sub(r"(?:시도별|지역별|업종별|산업별|산업대분류|산업중분류)", " ", candidate)
        candidate = candidate.strip(" -·,/")
        candidate = re.sub(r"^(?:간|동안)\s*", "", candidate).strip(" -·,/")
        compact = _compact_text(candidate)
        if not compact or len(compact) < 2:
            return None
        stop_terms = {
            "소상공인", "중소기업", "대기업", "기업", "사업체", "전체", "전국",
            "한국", "대한민국", "국내", "평균",
        }
        stop_terms.update(_compact_text(region) for region in REGION_DEMOGRAPHIC)
        stop_terms.update(_compact_text(region) for region in REGION_COMPOSITES)
        if compact in stop_terms:
            return None
        if any(token in compact for token in ("와", "과", "랑", "및")):
            return None
        return candidate

    @staticmethod
    def _composite_region(q_norm: str) -> Optional[str]:
        for name in sorted(REGION_COMPOSITES, key=len, reverse=True):
            if _compact_text(name) in q_norm:
                return name
        if "광역시" in q_norm:
            return "광역시"
        return None

    @staticmethod
    def _age(query: str, q_norm: str) -> Optional[dict[str, Any]]:
        decade = re.search(r"(\d{2})\s*대", query)
        if decade:
            n = int(decade.group(1))
            return {"label": f"{n}대", "type": "decade", "range": [n, n + 9]}
        explicit = re.search(r"(\d{1,3})\s*[-~]\s*(\d{1,3})\s*세", query)
        if explicit:
            return {"label": f"{explicit.group(1)}-{explicit.group(2)}세", "type": "range", "range": [int(explicit.group(1)), int(explicit.group(2))]}
        if "청년" in q_norm:
            return {"label": "청년", "type": "named_group", "range": [20, 34]}
        if "고령" in q_norm or "65세이상" in q_norm:
            return {"label": "65세 이상", "type": "lower_bound", "range": [65, None]}
        return None

    @staticmethod
    def _sex(q_norm: str, q_lower: str) -> Optional[str]:
        if any(term in q_norm for term in ("여성", "여자", "여자인구")) or re.search(r"\b(female|women|woman)\b", q_lower):
            return "여성"
        if any(term in q_norm for term in ("남성", "남자", "남자인구")) or re.search(r"\b(male|men|man)\b", q_lower):
            return "남성"
        return None

    @staticmethod
    def _time(query: str, q_norm: str) -> Optional[dict[str, Any]]:
        years = re.findall(r"(19\d{2}|20\d{2})", query)
        if years and any(term in q_norm for term in ("전년대비", "전년동기대비")):
            year = int(years[0])
            return {
                "type": "point_compare",
                "periods": [str(year - 1), str(years[0])],
                "basis": "previous_year",
            }
        if "코로나" in q_norm and "이전" in q_norm and "이후" in q_norm:
            return {
                "type": "named_period_compare",
                "label": "코로나 이전/이후",
                "periods": [
                    {"label": "코로나 이전 3년", "default_start": "2017", "default_end": "2019"},
                    {"label": "코로나 이후 3년", "default_start": "2020", "default_end": "2022"},
                ],
                "requires_llm_confirmation": True,
            }
        recent_months = re.search(r"최근\s*(\d+)\s*개월", query)
        if recent_months:
            months_count = int(recent_months.group(1))
            return {
                "type": "relative_period",
                "value": f"최근 {months_count}개월",
                "months": months_count,
                "default_rule": f"latest_month_included_{months_count}_months",
            }
        recent = re.search(r"최근\s*(\d+)\s*년", query)
        if recent:
            years_count = int(recent.group(1))
            return {
                "type": "relative_period",
                "value": f"최근 {years_count}년",
                "years": years_count,
                "default_rule": f"latest_year_included_{years_count}_years",
            }
        if (
            len(years) >= 2
            and any(term in q_norm for term in ("값", "비교", "대비", "증가율", "변화율"))
            and not any(term in q_norm for term in ("부터", "까지", "에서"))
        ):
            return {"type": "point_compare", "periods": list(dict.fromkeys([years[0], years[-1]]))}
        if len(years) >= 2 and (
            any(term in q_norm for term in ("부터", "까지", "에서", "~"))
            or re.search(r"(?:19\d{2}|20\d{2})\s*[-~]\s*(?:19\d{2}|20\d{2})", query)
        ):
            return {"type": "year_range", "start": years[0], "end": years[1]}
        if years and any(term in q_norm for term in ("이후", "부터")):
            return {"type": "since_year", "start": years[0], "end": "latest_available_period"}
        if years:
            return {"type": "year", "value": years[0]}
        relative_years = {
            "올해": 0,
            "금년": 0,
            "작년": -1,
            "전년": -1,
            "지난해": -1,
            "재작년": -2,
        }
        for token, offset in relative_years.items():
            if token in q_norm:
                return {"type": "relative_year", "label": token, "offset": offset}
        relative_months = {
            "이번달": 0,
            "이번월": 0,
            "지난달": -1,
            "전월": -1,
        }
        for token, offset in relative_months.items():
            if token in q_norm:
                return {"type": "relative_month", "label": token, "offset": offset}
        if any(term in q_norm for term in ("최근", "최신", "현재")):
            return {"type": "latest", "value": "latest"}
        return None

    @staticmethod
    def _is_false_gyeonggi_region(q_norm: str, region: Any) -> bool:
        if str(region or "") != "경기":
            return False
        return any(
            term in q_norm
            for term in (
                "경기전망", "경기지수", "체감경기", "경기동향", "경기현황",
                "경기전망지수", "기업경기", "기업경기실사", "경기실사지수",
            )
        )

    @staticmethod
    def _calculations(query: str, route_payload: dict[str, Any]) -> list[str]:
        q_norm = _compact_text(query)
        slots = route_payload.get("slots") or {}
        calculations = list(slots.get("calculation") or []) if isinstance(slots, dict) else []
        if any(term in q_norm for term in ("1인당", "인당")) or "percapita" in query.lower():
            calculations.append("per_capita")
        if any(term in q_norm for term in ("비중", "비율", "구성비", "차지", "고령화율", "고령화")):
            calculations.append("share")
        if any(term in q_norm for term in ("폐업률", "창업률", "생존율")):
            calculations.append("share")
        if any(term in q_norm for term in ("증가율", "변화율", "감소율")):
            calculations.append("growth_rate")
        if any(term in q_norm for term in ("전년대비", "전년동기대비")):
            calculations.append("growth_rate")
        if any(term in q_norm for term in ("가장빠른", "빠른곳", "속도", "빨라")):
            calculations.append("growth_rate")
        if any(term in q_norm for term in ("추이", "시계열", "최근5년", "최근10년")):
            calculations.append("time_series")
        return list(dict.fromkeys(calculations))

    @staticmethod
    def _intent(route_payload: dict[str, Any], calculations: list[str], dimensions: dict[str, Any]) -> str:
        if "per_capita" in calculations or "share" in calculations:
            return "computed_indicator"
        time_value = dimensions.get("time")
        if isinstance(time_value, dict) and time_value.get("type") == "year_range":
            return "trend"
        if "time_series" in calculations:
            return "trend"
        if "growth_rate" in calculations:
            return "growth_rate"
        intents = route_payload.get("intents") or []
        if "STAT_RANKING" in intents:
            return "ranking"
        if "STAT_COMPARISON" in intents:
            return "comparison"
        return "single_value"

    @staticmethod
    def _required_dimensions(
        query: str,
        dimensions: dict[str, Any],
        calculations: list[str],
        route_payload: Optional[dict[str, Any]] = None,
    ) -> list[str]:
        q_norm = _compact_text(query)
        required: list[str] = []
        if dimensions.get("regions"):
            required.append("regions")
        elif dimensions.get("region"):
            required.append("region")
        for key in ("region_group", "age", "sex", "industry", "time"):
            if dimensions.get(key):
                required.append(key)
        if any(term in q_norm for term in ("시도별", "지역별", "지역", "시도")) and not any(
            dim in required for dim in ("region", "regions", "region_group")
        ):
            required.append("region")
        if any(term in q_norm for term in ("업종별", "산업별", "업종", "산업")) and "industry" not in required:
            required.append("industry")
        if any(term in q_norm for term in ("성별", "남녀")) and "sex" not in required:
            required.append("sex")
        if "종사자규모" in q_norm or "종사자규모별" in q_norm or "종사자 규모별" in query:
            if "employee_size" not in required:
                required.append("employee_size")
        if "매출" in q_norm and any(term in q_norm for term in ("미만", "이상", "규모", "구간")):
            if "sales_size" not in required:
                required.append("sales_size")
        slots = (route_payload or {}).get("slots") or {}
        group_by = slots.get("group_by") if isinstance(slots, dict) else None
        if isinstance(group_by, list):
            for dim in group_by:
                if not isinstance(dim, str):
                    continue
                if QueryWorkflowPlanner._is_time_group_by(dim):
                    if "time" not in required:
                        required.append("time")
                    continue
                for normalized in _normalize_required_dimensions([dim]):
                    if normalized not in required:
                        required.append(normalized)
        comparison_targets = slots.get("comparison_target") if isinstance(slots, dict) else None
        if (
            isinstance(comparison_targets, list)
            and any(QueryWorkflowPlanner._is_region_like_target(target, q_norm) for target in comparison_targets)
            and "region" not in required
            and "regions" not in required
            and "region_group" not in required
        ):
            required.append("region")
        if any(op in calculations for op in ("time_series", "growth_rate")) and "time" not in required:
            required.append("time")
        return required

    @classmethod
    def _is_time_group_by(cls, value: str) -> bool:
        norm = _compact_text(value).lower()
        return norm in cls.TIME_GROUP_BY_TERMS

    @staticmethod
    def _is_region_like_target(target: Any, q_norm: str = "") -> bool:
        if not isinstance(target, str) or not target:
            return False
        if QueryWorkflowPlanner._is_false_gyeonggi_region(q_norm, target):
            return False
        norm = _compact_text(target)
        if norm in {_compact_text(region) for region in REGION_DEMOGRAPHIC}:
            return True
        if norm in {_compact_text(region) for region in REGION_COMPOSITES}:
            return True
        return any(token in norm for token in ("수도권", "비수도권", "광역시", "시도", "지역"))

    @staticmethod
    def _concepts(dimensions: dict[str, Any], calculations: list[str]) -> list[str]:
        concepts: list[str] = []
        for key in ("indicator", "region", "region_group", "industry", "sex", "event"):
            value = dimensions.get(key)
            if isinstance(value, str):
                concepts.append(value)
        regions = dimensions.get("regions")
        if isinstance(regions, list):
            concepts.extend(str(region) for region in regions if region)
        age = dimensions.get("age")
        if isinstance(age, dict) and age.get("label"):
            concepts.append(str(age["label"]))
        time_value = dimensions.get("time")
        if isinstance(time_value, dict):
            concepts.extend(str(v) for k, v in time_value.items() if k in {"value", "start", "end", "label"} and v)
            periods = time_value.get("periods")
            if isinstance(periods, list):
                concepts.extend(str(v) for v in periods if v)
        concepts.extend(calculations)
        return list(dict.fromkeys(concepts))

    @staticmethod
    def _metric_key(name: Any) -> str:
        compact = _compact_text(str(name or "")).lower()
        aliases = {
            "출생률": "조출생률",
            "출생율": "조출생률",
            "조출생률": "조출생률",
            "출생아수": "출생아수",
            "출생아수(명)": "출생아수",
            "소상공인수": "소상공인사업체수",
            "소상공인사업체수": "소상공인사업체수",
            "소상공인_사업체수": "소상공인사업체수",
            "중소기업수": "중소기업사업체수",
            "중소기업사업체수": "중소기업사업체수",
            "중소기업_사업체수": "중소기업사업체수",
        }
        return aliases.get(compact, re.sub(r"[\s_]+", "", compact))

    @staticmethod
    def _metric_display_name(name: str) -> str:
        compact = _compact_text(name).lower()
        aliases = {
            "출생률": "조출생률",
            "출생율": "조출생률",
            "조출생률": "조출생률",
            "출생아수": "출생아수",
            "출생아수(명)": "출생아수",
            "소상공인수": "소상공인 수",
            "소상공인사업체수": "소상공인 수",
            "소상공인_사업체수": "소상공인 수",
            "중소기업수": "중소기업 수",
            "중소기업사업체수": "중소기업 수",
            "중소기업_사업체수": "중소기업 수",
        }
        return aliases.get(compact, name.strip())

    def _metrics(self, query: str, dimensions: dict[str, Any], route_payload: dict[str, Any]) -> list[dict[str, Any]]:
        slots = route_payload.get("slots") or {}
        metrics: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(name: Any, role: str, source: str) -> None:
            if not isinstance(name, str) or not name.strip():
                return
            key = self._metric_key(name)
            if not key or key in seen:
                return
            seen.add(key)
            metrics.append({
                "name": self._metric_display_name(name),
                "role": role,
                "source": source,
                "availability": "unknown",
            })

        add(dimensions.get("indicator"), "primary", "intended_dimensions")
        if isinstance(slots, dict):
            add(slots.get("indicator"), "primary" if not metrics else "comparison", "router_slots.indicator")
            for item in slots.get("secondary_indicators") or []:
                add(item, "comparison", "router_slots.secondary_indicators")

        metric_phrases = [
            "중소기업 수", "대기업 수", "소상공인 수", "사업체 수", "기업 수",
            "종사자 수", "고용 규모", "매출액", "평균 매출액", "폐업률", "폐업 수",
            "창업률", "창업 수", "생존율", "경기전망지수", "임대료", "부채비율",
            "출생률", "출생율", "출생아 수", "수출액", "R&D 투자", "정책자금", "지원 규모",
        ]
        compact_query = _compact_text(query)
        for phrase in metric_phrases:
            if _compact_text(phrase) in compact_query:
                add(phrase, "mentioned", "query_phrase")

        if not metrics and dimensions.get("event"):
            add(dimensions["event"], "primary", "intended_dimensions.event")
        if not metrics and "통계" in compact_query:
            match = re.search(r"([가-힣A-Za-z0-9·/()]+통계)", str(query))
            if match:
                add(match.group(1), "mentioned", "query_phrase")
        if not metrics and any(term in compact_query for term in ("상위", "하위", "순위", "top")):
            match = re.search(
                r"^\s*(.+?)\s*(?:상위|하위|순위|Top|TOP|top)",
                str(query),
            )
            if match:
                candidate = re.sub(r"^(시도별|지역별|업종별|산업별|성별|연령별)\s*", "", match.group(1).strip())
                candidate = re.sub(r"\s+(기준|관련)$", "", candidate).strip(" -·/")
                add(candidate, "mentioned", "query_rank_phrase")
        return metrics

    @staticmethod
    def _bundle_dimensions(
        query: str,
        dimensions: dict[str, Any],
        required: list[str],
        route_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        slots = route_payload.get("slots") or {}
        output: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(name: str, source: str, value: Any = None) -> None:
            if not name or name in seen:
                return
            seen.add(name)
            item = {"name": name, "source": source}
            if value is not None:
                item["value"] = value
            output.append(item)

        for name in required:
            add(name, "required_dimensions", dimensions.get(name))
        if isinstance(slots, dict):
            for name in slots.get("group_by") or []:
                if isinstance(name, str):
                    if QueryWorkflowPlanner._is_time_group_by(name):
                        add("time", "router_slots.group_by", dimensions.get("time"))
                        continue
                    add(name, "router_slots.group_by")
            target = slots.get("target")
            if target:
                add("scale", "router_slots.target", target)
            scale = slots.get("scale")
            if scale:
                add("scale", "router_slots.scale", scale)
        q_norm = _compact_text(query)
        if "시도별" in q_norm or "지역별" in q_norm:
            add("region", "query_group_by")
        if "업종별" in q_norm or "산업별" in q_norm:
            add("industry", "query_group_by")
        return output

    @staticmethod
    def _comparison_targets(query: str, dimensions: dict[str, Any], route_payload: dict[str, Any]) -> list[str]:
        q_norm = _compact_text(query)
        targets: list[str] = []
        slots = route_payload.get("slots") or {}
        if isinstance(slots, dict):
            for item in slots.get("comparison_target") or []:
                if isinstance(item, str):
                    if QueryWorkflowPlanner._is_false_gyeonggi_region(q_norm, item):
                        continue
                    targets.append(item)
        regions = dimensions.get("regions")
        if isinstance(regions, list):
            targets.extend(str(item) for item in regions if item)
        group = dimensions.get("region_group")
        if isinstance(group, str) and group:
            targets.append(group)
        return list(dict.fromkeys(targets))

    @staticmethod
    def _time_request(dimensions: dict[str, Any], route_payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        value = dimensions.get("time")
        slots = route_payload.get("slots") or {}
        granularity = None
        group_by = slots.get("group_by") if isinstance(slots, dict) else None
        if isinstance(group_by, list):
            for item in group_by:
                if isinstance(item, str) and QueryWorkflowPlanner._is_time_group_by(item):
                    granularity = _compact_text(item).lower()
                    break
        if isinstance(value, dict):
            result = dict(value)
            if granularity and "granularity" not in result:
                result["granularity"] = "year" if granularity in {"year", "years", "annual", "연도", "연", "년"} else granularity
            return result
        slot_time = slots.get("time") if isinstance(slots, dict) else None
        if isinstance(slot_time, dict):
            result = dict(slot_time)
            if granularity and "granularity" not in result:
                result["granularity"] = "year" if granularity in {"year", "years", "annual", "연도", "연", "년"} else granularity
            return result
        return None

    @staticmethod
    def _rank_candidates(query: str, route_payload: dict[str, Any]) -> list[dict[str, Any]]:
        q_lower = query.lower()
        q_norm = _compact_text(query)
        slots = route_payload.get("slots") or {}
        limit = None
        limit_match = re.search(r"(\d+)\s*(?:개|곳|위|개의|곳씩|개씩)", query)
        if limit_match:
            limit = int(limit_match.group(1))
        elif top_match := re.search(r"\btop\s*(\d+)\b", q_lower):
            limit = int(top_match.group(1))

        top_terms = ("가장높", "높은", "상위", "최고", "최대", "top")
        bottom_terms = ("가장낮", "낮은", "하위", "최저", "최소", "bottom")
        has_top = any(term in q_norm or term in q_lower for term in top_terms)
        has_bottom = any(term in q_norm or term in q_lower for term in bottom_terms)

        ranks: list[dict[str, Any]] = []
        if has_top:
            ranks.append({"order": "desc", "limit": limit, "label": "top", "raw_phrase": "top"})
        if has_bottom:
            ranks.append({"order": "asc", "limit": limit, "label": "bottom", "raw_phrase": "bottom"})
        if not ranks and isinstance(slots, dict) and slots.get("sort"):
            ranks.append({
                "order": slots.get("sort"),
                "limit": slots.get("limit") or limit,
                "label": None,
                "raw_phrase": "router_slots.sort",
            })
        return ranks

    @staticmethod
    def _analysis_tasks(
        query: str,
        intent: str,
        dimensions: dict[str, Any],
        calculations: list[str],
        route_payload: dict[str, Any],
        metrics: list[dict[str, Any]],
        bundle_dimensions: list[dict[str, Any]],
        rank_candidates: Optional[list[dict[str, Any]]] = None,
    ) -> list[dict[str, Any]]:
        q_norm = _compact_text(query)
        slots = route_payload.get("slots") or {}
        metric_names = [m["name"] for m in metrics if m.get("name")]
        dimension_names = [d["name"] for d in bundle_dimensions if d.get("name")]
        tasks: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add(task_type: str, **kwargs: Any) -> None:
            key = (task_type, json.dumps(kwargs, ensure_ascii=False, sort_keys=True, default=str))
            if key in seen:
                return
            seen.add(key)
            item = {"type": task_type}
            item.update(kwargs)
            tasks.append(item)

        if "per_capita" in calculations:
            add("per_capita", metrics=metric_names, dimensions=dimension_names)
        if intent == "trend" or "time_series" in calculations:
            add("trend", metrics=metric_names, dimensions=["time"] if "time" in dimension_names else dimension_names)
        if "share" in calculations or any(term in q_norm for term in ("비중", "비율", "구성비")):
            add("share_by_group", metrics=metric_names, dimensions=dimension_names)
        has_yoy_growth = "growth_rate" in calculations and any(term in q_norm for term in ("전년대비", "전년동기대비"))
        if "growth_rate" in calculations or any(term in q_norm for term in ("증가율", "감소율", "변화율", "변화", "증가", "감소", "늘었", "줄어", "커진", "하락")):
            add("growth_rate", metrics=metric_names, dimensions=dimension_names)
        if not has_yoy_growth and ("change" in calculations or any(term in q_norm for term in ("변화", "증가", "감소", "늘었", "줄었", "줄어", "차이", "격차", "대비", "하락"))):
            add("change_compare", metrics=metric_names, dimensions=dimension_names)
        if intent == "comparison" or (len(metric_names) == 1 and not has_yoy_growth and any(term in q_norm for term in ("비교", "대비", "차이"))):
            add("compare_dimensions", metrics=metric_names, dimensions=dimension_names)
        sort = slots.get("sort") if isinstance(slots, dict) else None
        limit = slots.get("limit") if isinstance(slots, dict) else None
        ranks = rank_candidates or []
        if ranks:
            for rank in ranks:
                add(
                    "rank",
                    metrics=metric_names,
                    dimensions=dimension_names,
                    order=rank.get("order"),
                    limit=rank.get("limit"),
                    label=rank.get("label"),
                )
        elif intent == "ranking" or sort or limit or any(term in q_norm for term in ("top", "상위", "하위", "순위", "가장")):
            add("rank", metrics=metric_names, dimensions=dimension_names, order=sort, limit=limit)
        if "상위" in q_norm and "하위" in q_norm:
            add("top_bottom_rank", metrics=metric_names, dimensions=dimension_names, limit=limit)
        if any(term in q_norm for term in ("증가한", "감소한")) and any(term in q_norm for term in ("각각", "각")):
            add("top_bottom_change", metrics=metric_names, dimensions=dimension_names, limit=limit)
        if any(term in q_norm for term in ("높은", "낮은")) and any(term in q_norm for term in ("각각", "각")):
            add("top_bottom_rank", metrics=metric_names, dimensions=dimension_names, limit=limit)
        if "격차" in q_norm or "차이가큰" in q_norm or "차이가가장큰" in q_norm:
            add("gap_by_dimension", metrics=metric_names, dimensions=dimension_names)
        if any(term in q_norm for term in ("격차가큰", "차이가큰", "가장큰차이", "차이가가장큰")):
            add("gap_rank", metrics=metric_names, dimensions=dimension_names, limit=limit)
        if any(term in q_norm for term in ("어느시점부터", "언제부터", "확대되었")):
            add("gap_change_point", metrics=metric_names, dimensions=dimension_names)
        if "순위" in q_norm and "비교" in q_norm:
            add("rank_compare", metrics=metric_names, dimensions=dimension_names)
        if any(term in q_norm for term in ("같은지", "겹치는지", "일치하는지", "겹치")):
            add("rank_compare", metrics=metric_names, dimensions=dimension_names)
            add("rank_overlap", metrics=metric_names, dimensions=dimension_names)
        if any(term in q_norm for term in ("순위변동", "순위가바뀌", "순위가어떻게변", "순위변화")):
            add("rank_change", metrics=metric_names, dimensions=dimension_names)
        if (
            isinstance(dimensions.get("time"), dict)
            and dimensions["time"].get("type") == "point_compare"
            and dimensions["time"].get("basis") != "previous_year"
        ):
            add("point_compare", metrics=metric_names, dimensions=dimension_names, periods=dimensions["time"].get("periods"))
        if "이전" in q_norm and "이후" in q_norm and "평균" in q_norm:
            add("period_average_compare", metrics=metric_names, time=dimensions.get("time"))
        if any(term in q_norm for term in ("증가했는데", "늘었는데")) and any(term in q_norm for term in ("감소", "줄")):
            add("condition_filter", condition="metric_a_increase_and_metric_b_decrease", metrics=metric_names, dimensions=dimension_names)
        if any(term in q_norm for term in ("원인", "이유", "배경", "설명")):
            add("relationship_check", metrics=metric_names, dimensions=dimension_names)
        if len(metric_names) > 1 and any(term in q_norm for term in ("비교", "함께", "동시에")):
            add("compare_metrics", metrics=metric_names, dimensions=dimension_names)
        return tasks

    @staticmethod
    def _presentation_spec(query: str, route_payload: dict[str, Any]) -> dict[str, Any]:
        slots = route_payload.get("slots") or {}
        formats = list(slots.get("output_format") or []) if isinstance(slots, dict) else []
        q_norm = _compact_text(query)
        if any(term in q_norm for term in ("표", "테이블")):
            formats.append("table")
        if any(term in q_norm for term in ("그래프", "차트", "선그래프", "막대그래프")):
            formats.append("chart")
        if any(term in q_norm for term in ("지도", "맵")):
            formats.append("map")
        if any(term in q_norm for term in ("보고서", "브리프", "발표자료")):
            formats.append("report")
        return {"formats": list(dict.fromkeys(formats))}

    @staticmethod
    def _handoff_to_llm(query: str, analysis_tasks: list[dict[str, Any]], presentation: dict[str, Any]) -> dict[str, Any]:
        q_norm = _compact_text(query)
        report_generation = any(fmt in presentation.get("formats", []) for fmt in ("report", "summary"))
        causal = any(term in q_norm for term in ("원인", "이유", "배경", "설명", "해석"))
        return {
            "final_answer_expected": False,
            "final_answer_condition": "after_evidence_collection_only",
            "deprecated_fields": {
                "final_answer_expected": "Use mcp_output_contract.final_answer_expected for plan output. Synthesis is allowed only after evidence collection."
            },
            "report_generation": report_generation,
            "causal_language": "cautious" if causal else "not_requested",
            "must_disclose": [
                "not_matched metrics from select_table_for_query",
                "coverage_ratio below 1.0 from query_table",
                "caller_asserted aggregations",
            ],
            "llm_responsibility": [
                "define domain groups supplied by the user or chatbot",
                "choose narrative framing",
                "avoid inventing missing data",
            ],
            "uses_analysis_tasks": [task.get("type") for task in analysis_tasks],
        }

    @staticmethod
    def _analysis_mode(
        metrics: list[dict[str, Any]],
        dimensions: list[dict[str, Any]],
        comparison_targets: list[str],
        analysis_tasks: list[dict[str, Any]],
        handoff_to_llm: dict[str, Any],
    ) -> str:
        task_count = len(analysis_tasks)
        if len(metrics) >= 2:
            return "composite_analysis"
        if len(metrics) == 1 and task_count == 0:
            return "simple_lookup"
        if len(metrics) == 1 and task_count >= 1:
            return "analytical_single_metric"
        if task_count >= 2 or len(dimensions) >= 2 or len(comparison_targets) >= 2:
            return "composite_analysis"
        if handoff_to_llm.get("report_generation") or handoff_to_llm.get("causal_language") == "cautious":
            return "composite_analysis"
        return "simple_lookup"

    @staticmethod
    def _is_evidence_bundle(
        metrics: list[dict[str, Any]],
        dimensions: list[dict[str, Any]],
        comparison_targets: list[str],
        analysis_tasks: list[dict[str, Any]],
        handoff_to_llm: dict[str, Any],
    ) -> bool:
        if len(metrics) > 1 or len(dimensions) > 1 or len(comparison_targets) > 1:
            return True
        if len(analysis_tasks) > 1:
            return True
        if handoff_to_llm.get("report_generation") or handoff_to_llm.get("causal_language") == "cautious":
            return True
        return False

    @staticmethod
    def _evidence_workflow(
        metrics: list[dict[str, Any]],
        dimensions: list[dict[str, Any]],
        analysis_tasks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not metrics:
            return []
        required_dimensions = QueryWorkflowPlanner._table_required_dimensions([
            str(d.get("name")) for d in dimensions if d.get("name")
        ])
        return [
            {
                "step": 1,
                "operation": "for_each_metric",
                "tool": "select_table_for_query",
                "args_template": {
                    "indicator": "<metric.name>",
                    "required_dimensions": required_dimensions,
                    "reject_if_missing_dimensions": True,
                },
                "fills": "metrics[].availability and metric_table_mapping",
            },
            {
                "step": 2,
                "operation": "for_each_matched_table",
                "tool": "resolve_concepts",
                "args_template": {
                    "org_id": "<metric_table_mapping[].org_id>",
                    "tbl_id": "<metric_table_mapping[].tbl_id>",
                    "concepts": "<query concepts plus LLM-supplied group definitions>",
                },
            },
            {
                "step": 3,
                "operation": "for_each_resolved_table",
                "tool": "query_table",
                "args_template": {
                    "filters": "<resolve_concepts.filters>",
                    "period_range": "<time_request or table latest range>",
                    "aggregation": "none",
                },
            },
            {
                "step": 4,
                "operation": "apply_analysis_tasks",
                "tasks": analysis_tasks,
                "performed_by": "compute_indicator for enum calculations; chatbot LLM for synthesis",
            },
        ]

    def _workflow(
        self,
        query: str,
        intent: str,
        required: list[str],
        concepts: list[str],
        dimensions: dict[str, Any],
        calculations: list[str],
    ) -> list[WorkflowStep]:
        period_range = self._period_range(dimensions.get("time"))
        steps = [
            WorkflowStep(
                1,
                "select_table_for_query",
                "질문 의도와 필요한 분류축을 만족하는 KOSIS 통계표를 고릅니다.",
                {
                    "query": query,
                    "required_dimensions": required,
                    "indicator": dimensions.get("indicator"),
                    "indicator_alternatives": dimensions.get("indicator_alternatives"),
                    "comparison_targets": dimensions.get("regions"),
                    "reject_if_missing_dimensions": True,
                },
                "select_table_for_query" in self.AVAILABLE_TOOLS,
            ),
            WorkflowStep(
                2,
                "resolve_concepts",
                "선택된 표의 메타데이터 안에서 자연어 개념을 OBJ_ID/ITM_ID 코드로 바꿉니다.",
                {
                    "org_id": "<selected.org_id>",
                    "tbl_id": "<selected.tbl_id>",
                    "concepts": concepts,
                    "valid_only_for": "<selected.tbl_id>",
                },
                "resolve_concepts" in self.AVAILABLE_TOOLS,
            ),
            WorkflowStep(
                3,
                "query_table",
                "검증된 코드로 KOSIS raw rows를 조회합니다. 합산과 계산은 하지 않습니다.",
                {
                    "org_id": "<selected.org_id>",
                    "tbl_id": "<selected.tbl_id>",
                    "filters": "<resolve_concepts.filters>",
                    "period_range": period_range or "<resolve_concepts.period_range>",
                    "aggregation": "none",
                },
                True,
            ),
        ]
        compute_ops = [op for op in calculations if op in {"per_capita", "share", "growth_rate", "cagr", "yoy_diff", "yoy_pct"}]
        age = dimensions.get("age")
        if isinstance(age, dict) and age.get("type") in {"decade", "named_group", "lower_bound"}:
            compute_ops.append("sum_additive_rows")
        if compute_ops:
            steps.append(WorkflowStep(
                4,
                "compute_indicator",
                "허용된 산식 enum만 사용해 raw rows를 계산합니다.",
                {
                    "operation": compute_ops[0],
                    "operations": list(dict.fromkeys(compute_ops)),
                    "allowed_operations": ["per_capita", "share", "ratio", "growth_rate", "cagr", "yoy_diff", "yoy_pct", "sum_additive_rows"],
                    "input_rows": "<query_table.rows>",
                    "intent": intent,
                },
                "compute_indicator" in self.AVAILABLE_TOOLS,
            ))
        return steps

    @staticmethod
    def _period_range(time_value: Any) -> Optional[list[str]]:
        if isinstance(time_value, dict):
            if time_value.get("type") == "year" and time_value.get("value"):
                return [str(time_value["value"]), str(time_value["value"])]
            if time_value.get("type") in {"range", "year_range"} and time_value.get("start") and time_value.get("end"):
                return [str(time_value["start"]), str(time_value["end"])]
            if time_value.get("type") == "point_compare" and time_value.get("periods"):
                periods = [str(p) for p in time_value["periods"] if p]
                return [min(periods), max(periods)] if periods else None
            if time_value.get("type") == "since_year" and time_value.get("start"):
                return [str(time_value["start"]), "<latest_available_period>"]
            if time_value.get("type") == "relative_period" and time_value.get("years"):
                years = int(time_value["years"])
                return [f"<latest_available_period_minus_{max(0, years - 1)}>", "<latest_available_period>"]
            if time_value.get("type") == "relative_period" and time_value.get("months"):
                months = int(time_value["months"])
                return [f"<latest_available_period_minus_{max(0, months - 1)}_months>", "<latest_available_period>"]
        return None
