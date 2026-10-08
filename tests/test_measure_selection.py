from kosis_analysis.metadata import MetadataCompatibilityScorer, TableMetadataProfile


def profile(title, items, extra=None):
    axes = {"ITEM": {"OBJ_NM": "항목", "items": {
        str(i): {"label": label, "unit": unit} for i, (label, unit) in enumerate(items)
    }}, **(extra or {})}
    return TableMetadataProfile("101", "TEST", title, axes, list(axes), [])


def test_export_population_overlap_is_not_count_evidence():
    result = MetadataCompatibilityScorer([], "기업수").evaluate(profile(
        "대륙별·국가별 중소기업 수출", [("총수출", None), ("중소기업", None)]))
    assert result.status != "selected"
    assert result.indicator_evidence == []


def test_measure_cannot_be_proved_by_classification_label():
    result = MetadataCompatibilityScorer([], "기업수").evaluate(profile(
        "매출액", [("매출액", "억원")], {"C1": {"items": {"N": {"label": "기업수"}}}}))
    assert result.status != "selected"


def test_full_population_request_can_match_real_measure_metadata():
    result = MetadataCompatibilityScorer([], "제조업 중소기업 기업수").evaluate(profile(
        "산업별 기업규모별 현황", [("기업수", "개"), ("매출액", "억원")]))
    assert result.status == "selected"
    assert result.to_response()["measure_compatibility"]["relation"] == "exact"


def test_unknown_items_are_not_false_measurement_conflicts():
    result = MetadataCompatibilityScorer([], "기업수").evaluate(profile("기업수 현황", [("활동", None)]))
    assert result.status == "selected"
    assert result.to_response()["measure_compatibility"]["unknown_items"] == ["활동"]


def test_casual_organization_count_proxy_remains_candidate():
    result = MetadataCompatibilityScorer([], "사업체수").evaluate(profile("기업 현황", [("기업수", "개")]))
    assert result.status == "selected"
    assert result.to_response()["measure_compatibility"]["relation"] == "proxy"


def test_incompatible_measurement_is_structured():
    result = MetadataCompatibilityScorer([], "기업수").evaluate(profile("기업 매출액", [("매출액", "억원")]))
    assert result.status == "rejected_measurement"
    assert result.to_response()["measure_compatibility"]["relation"] == "incompatible"


def test_equivalent_wage_measure_and_count_suffixes_survive():
    result = MetadataCompatibilityScorer([], "월평균임금").evaluate(profile("근로자 현황", [("임금", "원")]))
    assert result.status == "selected"
    from kosis_analysis.rules import measure_of
    assert measure_of("제조업 종사자수등록기반시도") == "종사자수"
    assert measure_of("중소기업수출") is None
    assert measure_of("소비자물가지수출처") == "지수"


def test_real_measure_precedes_newer_population_overlap():
    from kosis_mcp_server import _table_candidate_sort_key
    exact = {"status": "selected", "measure_compatibility": {"relation": "exact"},
             "ranking_features": {"ranking_penalty": 1, "latest_period_year": 2023}}
    unknown = {"status": "selected", "measure_compatibility": {"relation": "unknown"},
               "ranking_features": {"ranking_penalty": 0, "latest_period_year": 2026}}
    assert _table_candidate_sort_key(exact) < _table_candidate_sort_key(unknown)


def test_actual_measure_item_precedes_title_with_ambiguous_activity_items():
    from kosis_mcp_server import _table_candidate_sort_key
    scorer = MetadataCompatibilityScorer([], "기업수", population_terms=["제조업", "중소기업"])
    real = scorer.evaluate(profile("기업규모별 기업수", [("기업수", "개")],
        {"C1": {"items": {"S": {"label": "중소기업"}}}})).to_response()
    ambiguous = scorer.evaluate(profile("기업수(활동/신생/소멸)", [("활동", None), ("신생", None)],
        {"C1": {"items": {"S": {"label": "중소기업"}, "C": {"label": "제조업"}}}})).to_response()
    assert ambiguous["status"] == "selected"  # Still a candidate, not falsely unavailable.
    assert _table_candidate_sort_key(real) < _table_candidate_sort_key(ambiguous)


def test_plain_metric_keeps_freshness_when_item_is_generic_total():
    from kosis_mcp_server import _table_candidate_sort_key
    scorer = MetadataCompatibilityScorer([], "소비자물가지수")
    old = scorer.evaluate(profile("소비자물가지수", [("소비자물가지수", None)])).to_response()
    fresh = scorer.evaluate(profile("소비자물가지수", [("전체", None)])).to_response()
    old["ranking_features"] = {"indicator_score_band": 1, "latest_period_year": 2015}
    fresh["ranking_features"] = {"indicator_score_band": 1, "latest_period_year": 2023}
    assert _table_candidate_sort_key(fresh) < _table_candidate_sort_key(old)


def test_population_coverage_accepts_closed_gender_alias_but_not_gender_enterprises():
    from kosis_mcp_server import _annotate_table_candidate_ranking
    scorer = MetadataCompatibilityScorer([], "여성 실업률", population_terms=["여성"])
    real = scorer.evaluate(profile("성별 실업률", [("실업률", "%")],
        {"C1": {"items": {"F": {"label": "여자"}}}})).to_response()
    wrong = scorer.evaluate(profile("기업 실업률", [("실업률", "%")],
        {"C1": {"items": {"F": {"label": "여성기업"}}}})).to_response()
    assert real["population_compatibility"]["matched_items"] == ["여성"]
    assert wrong["population_compatibility"]["matched_items"] == []
    _annotate_table_candidate_ranking([real], query="여성 실업률", indicator="여성 실업률")
    assert real["query_match_quality"]["coverage_ratio"] == 1


def test_population_label_evidence_precedes_title_overlap_but_keeps_unknowns():
    from kosis_mcp_server import _annotate_table_candidate_ranking, _table_candidate_sort_key
    scorer = MetadataCompatibilityScorer([], "기업수", population_terms=["중소기업", "여성"])
    real = scorer.evaluate(profile("기업규모별 기업수", [("기업수", "개")],
        {"C1": {"items": {"S": {"label": "중소기업"}}}})).to_response()
    other = scorer.evaluate(profile("제조업 100대 기업수", [("기업수", "개")])).to_response()
    assert real["population_compatibility"]["matched_items"] == ["중소기업"]
    assert real["population_compatibility"]["unverified_terms"] == ["여성"]
    assert real["compatibility"]["verification_level"] == "metadata_partial"
    _annotate_table_candidate_ranking([real, other], query="중소기업 기업수", indicator="기업수")
    assert _table_candidate_sort_key(real) < _table_candidate_sort_key(other)
    assert real["query_match_quality"]["coverage_ratio"] == 1


def test_implicit_home_scope_prefers_national_table_without_claiming_unknown_scope_verified():
    from kosis_mcp_server import _annotate_table_candidate_ranking, _table_candidate_sort_key, _compact_table_candidate
    scorer = MetadataCompatibilityScorer([], "실업률", population_terms=["여성"])
    national = scorer.evaluate(profile("성별 실업률", [("실업률", "%")],
        {"S": {"OBJ_NM": "성별", "items": {"F": {"label": "여자"}}}})).to_response()
    international = scorer.evaluate(profile("경제활동 상태별 실업률", [("실업률", "%")], {
        "S": {"OBJ_NM": "성별", "items": {"F": {"label": "여자"}}},
        "N": {"OBJ_NM": "국가별", "items": {"KR": {"label": "대한민국"}, "FR": {"label": "프랑스"}}},
    })).to_response()
    international["periods"] = [{"latest_period": "2026"}]
    _annotate_table_candidate_ranking([international, national], query="여성 실업률", indicator="실업률")
    assert _table_candidate_sort_key(national) < _table_candidate_sort_key(international)
    assert national["geographic_scope"]["kind"] == "unknown"
    assert _compact_table_candidate(international)["geographic_scope"]["kind"] == "country_comparison"


def test_explicit_country_and_comparison_do_not_receive_implicit_home_penalty():
    from kosis_mcp_server import _annotate_table_candidate_ranking
    candidate = MetadataCompatibilityScorer([], "실업률").evaluate(profile("경제활동 상태별 실업률", [("실업률", "%")], {
        "N": {"OBJ_NM": "국가별", "items": {"KR": {"label": "대한민국"}, "FR": {"label": "프랑스"}}},
    })).to_response()
    for question in ("프랑스 여성 실업률", "OECD 여성 실업률", "국가별 실업률 비교"):
        _annotate_table_candidate_ranking([candidate], query=question, indicator="실업률")
        assert not any(p["type"] == "international_table_for_domestic_query" for p in candidate.get("ranking_penalties", []))


def test_trade_partner_axis_is_not_mistaken_for_foreign_population_scope():
    from kosis_mcp_server import _annotate_table_candidate_ranking
    candidate = MetadataCompatibilityScorer([], "수출액").evaluate(profile("국가별 중소기업 수출액", [("수출액", "달러")], {
        "N": {"OBJ_NM": "교역상대국", "items": {"KR": {"label": "대한민국"}, "FR": {"label": "프랑스"}}},
    })).to_response()
    _annotate_table_candidate_ranking([candidate], query="중소기업 수출액", indicator="수출액")
    assert candidate["geographic_scope"]["kind"] == "trade_partner"
    assert not any(p["type"] == "international_table_for_domestic_query" for p in candidate.get("ranking_penalties", []))


def test_country_axes_with_survey_annotations_preserve_explicit_country_intent():
    from kosis_mcp_server import _annotate_table_candidate_ranking
    candidate = MetadataCompatibilityScorer([], "실업률").evaluate(profile("실업률", [("실업률", "%")], {
        "N": {"OBJ_NM": "국가별", "items": {"KR": {"label": "대한민국(LFS 15+)"}, "FR": {"label": "프랑스(LFS 15+)"}}},
    })).to_response()
    assert candidate["geographic_scope"]["kind"] == "country_comparison"
    _annotate_table_candidate_ranking([candidate], query="프랑스 여성 실업률", indicator="실업률")
    assert not any(p["type"] == "international_table_for_domestic_query" for p in candidate.get("ranking_penalties", []))
