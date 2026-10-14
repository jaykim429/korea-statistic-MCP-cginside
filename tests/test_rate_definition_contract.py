import pytest
from kosis_analysis.rules import measure_of, source_measure_of, measurement_normalizer, measure_basis_compatible
from kosis_curation import lookup
from kosis_analysis.quick import leftover_qualifier_terms

@pytest.mark.parametrize('text,expected', [('조출생률 (인구 천명당 출생아수)','조출생률'),('조사망률 (인구 천명당 사망자수)','조사망률'),('조혼인율 (인구 천명당 혼인건수)','조혼인율'),('매출액(매출액÷사업체수)','매출액')])
def test_primary_definition_name(text,expected):
    assert source_measure_of(text)==expected

@pytest.mark.parametrize('text,expected', [('천명당','1000명당'),('1000명당','1000명당'),('만명당','10000명당'),('10만명당','100000명당'),('십만명당','100000명당'),('1명당','1인당')])
def test_normalizer_aliases(text,expected):
    assert measurement_normalizer('인구 '+text+' 건수')==expected

@pytest.mark.parametrize('question,item', [('2025년 조출생률','T1'),('2024년 인구 천명당 범죄발생건수 알려줘','T10'),('2024년 인구 1000명당 범죄발생건수 알려줘','T10'),('2024년 범죄발생건수 알려줘','T001'),('범죄율','T10')])
def test_registered_definition_selection_does_not_pick_the_same_nouns_total(question,item):
    assert lookup(question).item_id==item

@pytest.mark.parametrize('question', ['인구 만명당 범죄발생건수','인구 천명당 의사수','기업당 매출액'])
def test_unregistered_basis_is_discovery_not_total(question):
    assert lookup(question) is None

def test_named_rate_basis_not_confused_with_total_or_other_scale():
    assert measure_basis_compatible('조출생률','조출생률 (인구 천명당 출생아수)')
    assert not measure_basis_compatible('출생아수','조출생률 (인구 천명당 출생아수)')
    assert not measure_basis_compatible('인구 만명당 범죄발생건수','인구 천명당 범죄발생건수')


@pytest.mark.parametrize('basis', ['천명당', '1000명당', '1000인당'])
def test_residual_gate_recognizes_the_same_denominator_as_lookup(basis):
    question = '2024년 인구 ' + basis + ' 범죄발생건수 알려줘'
    param = lookup(question)
    assert param and param.item_id == 'T10'
    assert leftover_qualifier_terms(question, param) == []


@pytest.mark.parametrize('question', ['2024년 여성 인구 1000명당 범죄발생건수', '2024년 인구 만명당 범죄발생건수', '2024년 인구 1000명당 사고발생건수', '기업당원 범죄발생건수'])
def test_equivalent_basis_never_erases_another_unknown_qualifier(question):
    assert leftover_qualifier_terms(question, lookup('인구 천명당 범죄발생건수'))
