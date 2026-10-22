"""A comparison cue is not an instruction to divide employees by enterprises."""
import asyncio

import pytest

from kosis_analysis.answering import AnswerPlanner
from kosis_mcp_server import NaturalLanguageAnswerEngine as Engine


COMPARISONS = [
    '중소기업이랑 대기업 종사자 수 비교해줘',
    '중소기업과 대기업 고용을 비교해줘',
    '중소기업 종사자 수와 사업체 수 함께 알려줘',
    '중소기업 종사자 수를 지역별로 비교해줘',
    '중소기업 고용과 대기업 고용을 함께 보여줘',
    '중소기업 종사자 수 알려줘',
]
AVERAGES = [
    '중소기업 사업체당 종사자 수 알려줘',
    '중소기업 기업당 고용 인원 알려줘',
    '중소기업 평균 종사자 수 알려줘',
    '중소기업 사업체당 평균 종사자 수와 기업 수 함께 알려줘',
    '중소기업 사업체당 종사자 수를 비교해줘',
]


@pytest.mark.parametrize('question', COMPARISONS)
def test_comparison_or_joint_lookup_does_not_imply_employee_average(question):
    assert not Engine('dummy')._is_sme_employee_average_question(question)


@pytest.mark.parametrize('question', AVERAGES)
def test_explicit_average_and_per_enterprise_requests_still_select_average(question):
    assert Engine('dummy')._is_sme_employee_average_question(question)


@pytest.mark.parametrize('question', COMPARISONS)
def test_real_planner_does_not_turn_comparison_into_an_average(question):
    engine = Engine('dummy')
    plan = AnswerPlanner(engine).build(question, '전국', engine._route_payload(question))
    assert plan.action != 'sme_employee_average'


@pytest.mark.parametrize('question', AVERAGES)
def test_real_planner_retains_explicit_average_route(question):
    engine = Engine('dummy')
    plan = AnswerPlanner(engine).build(question, '전국', engine._route_payload(question))
    assert plan.action == 'sme_employee_average'


@pytest.mark.parametrize('question', COMPARISONS[:2])
def test_dispatch_never_executes_unrequested_employee_average(monkeypatch, question):
    engine = Engine('dummy')

    async def forbidden(*args, **kwargs):
        pytest.fail('unrequested employee average executed')

    async def selected_lookup(*args, **kwargs):
        return {'상태': 'executed', 'answer': 'lookup fixture, not a completed comparison'}

    monkeypatch.setattr(engine, '_answer_sme_employee_average', forbidden)
    monkeypatch.setattr(engine, '_answer_direct', selected_lookup)
    monkeypatch.setattr(engine, '_answer_search_fallback', selected_lookup)
    asyncio.run(engine._dispatch(question, '전국'))


def test_dispatch_still_executes_an_explicit_average(monkeypatch):
    engine = Engine('dummy')
    called = []

    async def average(question, region):
        called.append((question, region))
        return {'상태': 'executed', 'answer': 'explicit average fixture'}

    monkeypatch.setattr(engine, '_answer_sme_employee_average', average)
    question = '중소기업 사업체당 평균 종사자 수 알려줘'
    result = asyncio.run(engine._dispatch(question, '부산'))
    assert called == [(question, '부산')]
    assert result['answer'] == 'explicit average fixture'
