from __future__ import annotations

import asyncio
import io
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kosis_mcp_server import explore_table, plan_query, search_kosis


SEED_QUERIES = [
    "중소기업 사업체수 업종별 지역별 기업규모별",
    "중소기업 종사자수 업종별 지역별",
    "중소기업 매출액 업종별 지역별 기업규모별",
    "소상공인 사업체수 업종별 지역별",
    "소상공인 종사자수 업종별 지역별",
    "소상공인 매출액 업종별 지역별",
    "소상공인 폐업률 창업률 생존율 업종별 지역별",
    "창업기업 생존율 업종별 지역별",
    "창업기업수 업종별 시도별",
    "기업생멸 소멸률 창업률 업종별",
    "정책자금 지원 업종별 지역별",
    "중소기업 대출 부채비율 업종별",
    "소상공인 경기전망지수 업종별 월별",
    "중소기업 경기전망지수 업종별 월별",
    "사업체당 평균 종사자수 업종별 지역별",
    "매출액 규모별 중소기업 수",
    "종사자 규모별 사업체 수",
    "여성 대표 소상공인 업종별 지역별",
    "청년 창업 업종별 지역별 생존율",
    "인구 성별 연령별 시도별",
    "장래인구추계 성 연령 시도",
    "주민등록인구 성별 연령별 시도",
    "실업률 고용률 시도별 성별 연령별",
    "소비자물가지수 월별 지수",
    "아파트 매매가격지수 전세가격지수 지역별 월별",
    "GRDP 지역내총생산 시도별",
    "수출액 중소기업 업종별 지역별",
    "R&D 투자 중소기업 업종별",
    "정책지원 수혜기업 수 업종별 지역별",
    "폐업률 매출액 임대료 경기전망지수 소상공인",
]

MAX_TABLES = 70
SEARCH_LIMIT = 6


DIMENSION_HINTS = {
    "region": ("시도", "지역", "행정구역", "시군구", "권역", "province", "region"),
    "industry": ("산업", "업종", "산업분류", "KSIC", "중분류", "대분류", "industry"),
    "age": ("연령", "나이", "연령계층", "age"),
    "sex": ("성별", "남녀", "성", "sex", "gender"),
    "scale": ("기업규모", "규모", "종사자규모", "매출액규모", "size", "scale"),
    "employee_size": ("종사자규모", "종사자 규모"),
    "sales_size": ("매출액규모", "매출 규모", "매출액 규모"),
    "time": ("시점", "기간", "연도", "월", "분기", "period"),
}

TASK_BY_FLAG = {
    "rate_or_ratio": ["rank", "rank_compare", "share_by_group"],
    "index_table": ["change_compare", "base_period_warning"],
    "future_projection": ["point_compare", "projection_warning"],
    "long_region_axis": ["rank", "top_bottom_rank"],
    "hierarchical_axis": ["rollup_warning"],
    "mixed_units": ["unit_disambiguation"],
    "missing_units": ["unit_warning"],
    "region_industry_scale": ["rank", "share_by_group", "gap_by_dimension"],
    "age_sex_region": ["point_compare", "sum_additive_rows"],
    "multi_axis_complex": ["evidence_bundle"],
}


def compact(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).lower()


def extract_candidates(search: dict[str, Any], seed: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    direct = search.get("Tier_A_직접_매핑")
    if isinstance(direct, dict) and direct.get("통계표ID") and direct.get("기관ID"):
        rows.append({
            "org_id": str(direct["기관ID"]),
            "tbl_id": str(direct["통계표ID"]),
            "table_name": str(direct.get("통계표") or ""),
            "source": "tier_a_direct",
            "seed": seed,
        })
    for row in search.get("결과") or []:
        if row.get("기관ID") and row.get("통계표ID"):
            rows.append({
                "org_id": str(row["기관ID"]),
                "tbl_id": str(row["통계표ID"]),
                "table_name": str(row.get("통계표명") or row.get("통계표") or ""),
                "source": str(row.get("검색어") or "search"),
                "seed": seed,
            })
    return rows


def axis_dimension(axis_name: str, obj_id: str) -> str:
    text = compact(f"{axis_name} {obj_id}")
    for dim, hints in DIMENSION_HINTS.items():
        if any(compact(hint) in text for hint in hints):
            return dim
    if obj_id == "ITEM":
        return "metric_item"
    return "other"


def period_nature(period: Any, table_name: str) -> dict[str, Any]:
    if not isinstance(period, dict):
        return {}
    latest = str(period.get("최신_수록시점") or "")
    start = str(period.get("시작_수록시점") or "")
    cadence = str(period.get("수록주기") or "")
    current_year = datetime.now().year
    latest_year = None
    match = re.match(r"(\d{4})", latest)
    if match:
        latest_year = int(match.group(1))
    future = bool(latest_year and latest_year > current_year)
    projection = future or any(word in table_name for word in ("추계", "전망", "예측", "장래"))
    return {
        "start": start,
        "latest": latest,
        "cadence": cadence,
        "latest_year": latest_year,
        "future_projection": projection,
    }


def analyze_table(candidate: dict[str, str], meta: dict[str, Any]) -> dict[str, Any]:
    table_name = str(meta.get("통계표명") or candidate.get("table_name") or "")
    axes = meta.get("분류축") or {}
    period = period_nature(meta.get("수록기간"), table_name)
    axis_summaries: list[dict[str, Any]] = []
    dims: list[str] = []
    flags: set[str] = set()
    units: set[str] = set()
    total_items = 0
    for obj_id, axis in axes.items():
        if not isinstance(axis, dict):
            continue
        axis_name = str(axis.get("OBJ_NM") or obj_id)
        items = axis.get("items") or []
        item_count = int(axis.get("item_count") or len(items))
        total_items += item_count
        dim = axis_dimension(axis_name, str(obj_id))
        dims.append(dim)
        axis_units = {str(item.get("UNIT_NM")) for item in items if isinstance(item, dict) and item.get("UNIT_NM")}
        units |= axis_units
        hierarchical = any(isinstance(item, dict) and item.get("UP_ITM_ID") for item in items)
        if hierarchical:
            flags.add("hierarchical_axis")
        if dim == "region" and item_count >= 50:
            flags.add("long_region_axis")
        if dim == "industry" and item_count >= 30:
            flags.add("long_industry_axis")
        axis_summaries.append({
            "obj_id": obj_id,
            "name": axis_name,
            "dimension": dim,
            "item_count": item_count,
            "sample_items": [
                item.get("ITM_NM")
                for item in items[:8]
                if isinstance(item, dict)
            ],
            "has_hierarchy": hierarchical,
            "units": sorted(axis_units),
        })
    dims = list(dict.fromkeys(dim for dim in dims if dim != "other"))
    if len(axes) >= 3:
        flags.add("multi_axis_complex")
    if {"region", "industry", "scale"}.issubset(set(dims)):
        flags.add("region_industry_scale")
    if {"region", "age", "sex"}.issubset(set(dims)):
        flags.add("age_sex_region")
    if len(units) > 1:
        flags.add("mixed_units")
    if not units:
        flags.add("missing_units")
    if "%" in "".join(units) or any(word in table_name for word in ("률", "율", "비율", "비중", "구성비", "평균")):
        flags.add("rate_or_ratio")
    if "지수" in table_name or "=100" in table_name or "100)" in table_name:
        flags.add("index_table")
    if period.get("future_projection"):
        flags.add("future_projection")
    tasks = sorted({task for flag in flags for task in TASK_BY_FLAG.get(flag, [])})
    return {
        **candidate,
        "table_name": table_name,
        "period": period,
        "dimensions": dims,
        "axis_count": len(axes),
        "total_item_count": total_items,
        "axis_summaries": axis_summaries,
        "units": sorted(units),
        "flags": sorted(flags),
        "suggested_tasks": tasks,
    }


def question_from_table(table: dict[str, Any]) -> list[dict[str, Any]]:
    name = str(table.get("table_name") or "통계")
    metric = re.sub(r"\(.*?\)", "", name).strip() or name
    dims = set(table.get("dimensions") or [])
    flags = set(table.get("flags") or [])
    questions: list[dict[str, Any]] = []
    if {"region", "industry"}.issubset(dims):
        questions.append({
            "question": f"지역별·업종별 {metric}을 비교하고 격차가 가장 큰 업종을 찾아줘.",
            "expected_dimensions": ["region", "industry"],
            "expected_tasks": ["gap_by_dimension", "rank"],
        })
    if {"region", "industry", "scale"}.issubset(dims):
        questions.append({
            "question": f"기업규모별로 {metric}이 높은 지역과 업종 Top 10을 보여줘.",
            "expected_dimensions": ["region", "industry", "scale"],
            "expected_tasks": ["rank"],
        })
    if "long_region_axis" in flags:
        questions.append({
            "question": f"시도별 {metric} 상위 5개와 하위 5개를 알려줘.",
            "expected_dimensions": ["region"],
            "expected_tasks": ["rank", "top_bottom_rank"],
        })
    if "rate_or_ratio" in flags and "region" in dims:
        questions.append({
            "question": f"{metric} 상위 지역과 관련 절대값 상위 지역이 같은지 비교해줘.",
            "expected_dimensions": ["region"],
            "expected_tasks": ["rank_compare", "rank_overlap"],
        })
    elif "rate_or_ratio" in flags:
        questions.append({
            "question": f"{metric} 상위 항목과 관련 절대값 상위 항목이 같은지 비교해줘.",
            "expected_dimensions": [],
            "expected_tasks": ["rank_compare", "rank_overlap"],
        })
    if "index_table" in flags:
        questions.append({
            "question": f"최근 12개월 {metric}의 전월 대비 변동이 가장 큰 시점을 찾아줘.",
            "expected_dimensions": ["time"],
            "expected_tasks": ["change_compare", "rank"],
        })
    if "future_projection" in flags:
        questions.append({
            "question": f"2045년 {metric}와 2020년 값을 비교해서 증가율을 알려줘.",
            "expected_dimensions": ["time"],
            "expected_tasks": ["point_compare", "growth_rate"],
        })
    return questions[:3]


def summarize(analyses: list[dict[str, Any]], plan_checks: list[dict[str, Any]]) -> dict[str, Any]:
    flag_counts = Counter(flag for row in analyses for flag in row.get("flags", []))
    dim_counts = Counter(dim for row in analyses for dim in row.get("dimensions", []))
    task_counts = Counter(task for row in analyses for task in row.get("suggested_tasks", []))
    failed = [row for row in plan_checks if row.get("status") != "PASS"]
    return {
        "table_count": len(analyses),
        "flag_counts": dict(flag_counts.most_common()),
        "dimension_counts": dict(dim_counts.most_common()),
        "task_counts": dict(task_counts.most_common()),
        "generated_question_count": len(plan_checks),
        "generated_question_pass": len(plan_checks) - len(failed),
        "generated_question_fail": len(failed),
        "top_failure_examples": failed[:10],
    }


def contains_all(actual: Any, expected: list[str]) -> list[str]:
    text = json.dumps(actual, ensure_ascii=False)
    return [item for item in expected if item not in text]


async def check_generated_questions(questions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results = []
    for item in questions:
        result = await plan_query(item["question"])
        task_types = [task.get("type") for task in result.get("analysis_tasks") or [] if isinstance(task, dict)]
        problems: list[dict[str, Any]] = []
        missing_dims = contains_all(result.get("dimensions"), item.get("expected_dimensions", []))
        missing_tasks = [task for task in item.get("expected_tasks", []) if task not in task_types]
        if missing_dims:
            problems.append({"missing_dimensions": missing_dims, "actual": result.get("dimensions")})
        if missing_tasks:
            problems.append({"missing_tasks": missing_tasks, "actual": task_types})
        results.append({
            "question": item["question"],
            "source_table": item.get("source_table"),
            "status": "PASS" if not problems else "FAIL",
            "problems": problems,
            "metrics": result.get("metrics"),
            "dimensions": result.get("dimensions"),
            "analysis_tasks": result.get("analysis_tasks"),
        })
    return results


async def main() -> None:
    artifacts = ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    searches = await asyncio.gather(*[
        search_kosis(seed, limit=SEARCH_LIMIT, use_routing=True)
        for seed in SEED_QUERIES
    ])
    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for seed, search in zip(SEED_QUERIES, searches):
        for candidate in extract_candidates(search, seed):
            key = (candidate["org_id"], candidate["tbl_id"])
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    candidates = candidates[:MAX_TABLES]
    metas = await asyncio.gather(*[
        explore_table(
            row["org_id"],
            row["tbl_id"],
            axes_to_include=["region", "industry", "age", "sex", "scale", "ITEM"],
            compact=True,
            include_english_labels=False,
            sample_limit=25,
        )
        for row in candidates
    ])
    analyses = [
        analyze_table(candidate, meta)
        for candidate, meta in zip(candidates, metas)
        if isinstance(meta, dict) and not meta.get("오류")
    ]
    generated_questions: list[dict[str, Any]] = []
    used_questions: set[str] = set()
    for row in analyses:
        for question in question_from_table(row):
            q = question["question"]
            if q in used_questions:
                continue
            used_questions.add(q)
            generated_questions.append({
                **question,
                "source_table": {
                    "org_id": row["org_id"],
                    "tbl_id": row["tbl_id"],
                    "table_name": row["table_name"],
                    "flags": row["flags"],
                    "dimensions": row["dimensions"],
                },
            })
    plan_checks = await check_generated_questions(generated_questions[:80])
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "seed_queries": SEED_QUERIES,
        "summary": summarize(analyses, plan_checks),
        "tables": analyses,
        "generated_questions": generated_questions,
        "plan_checks": plan_checks,
    }
    out_path = artifacts / "kosis_metadata_pattern_report.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"\nWROTE {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
