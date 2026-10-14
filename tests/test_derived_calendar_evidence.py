import asyncio
import math

import pytest
import kosis_mcp_server as server
from kosis_analysis.indicators import OPERATIONS, elapsed_years


@pytest.mark.parametrize('start,end,cadence,years', [('2020','2022','Y',2),('202401','202407','M',0.5),('2023Q4','2024Q2','Q',0.5)])
def test_elapsed_calendar_time_not_row_count(start,end,cadence,years):
    assert elapsed_years(start,end,cadence)==years
    result=server._growth_materials([start,end],[100,121],unit='개',cadence=cadence)
    assert result['경과_연수']==years
    assert result['연평균증가율_퍼센트']==pytest.approx(round(((121/100)**(1/years)-1)*100,4))


@pytest.mark.parametrize('periods', [['2024','2024'],['202401','202407'],['bad','2024']])
def test_unknown_or_duplicate_calendar_is_not_annual_growth(periods):
    assert server._growth_materials(periods,[100,121])['연평균증가율_퍼센트'] is None


def test_nonfinite_is_not_a_computed_value():
    assert server._growth_materials(['2023','2024'],[100,math.inf])=={}
    result=OPERATIONS['cagr'].compute([{'period':'2023','value':100},{'period':'2024','value':'NaN'}])
    assert not result.results


def test_native_numeric_zero_survives_and_nonfinite_does_not():
    times,values=server._values_from_series([{'시점':'2020','값':0},{'시점':'2021','값':'1,000'},{'시점':'2022','값':'NaN'}])
    assert times==['2020','2021']
    assert values==[0,1000]


def test_native_calculation_carries_quality_and_freshness_notes():
    basis=server._native_calculation_basis({'data_quality_note':'설계 변경','⚠️ 데이터_신선도':'최신 수록 2023','⚠️ 잠정치':'잠정치'})
    assert all(note in basis['data_quality_note'] for note in ['설계 변경','최신 수록 2023','잠정치'])


def test_explicit_monthly_comparison_fetches_months_and_preserves_cadence(monkeypatch):
    seen=[]
    async def fetch(*args,**kwargs):
        seen.append(kwargs['period_type'])
        return [{'PRD_DE':period,'DT':str(value),'ITM_ID':'T10','ITM_NM':'수출액','C1':'00','C1_NM':'전국','UNIT_NM':'100만달러'} for period,value in [('202301',100),('202401',110)]]
    monkeypatch.setattr(server,'_fetch_series',fetch)
    result=asyncio.run(server.stat_time_compare('수출액 월별',start_period='202301',end_period='202401',api_key='dummy'))
    assert seen==['M']
    assert result['stat_evidence']['evidence']['period']['cadence']=='M'


@pytest.mark.parametrize('base', [0, -100])
def test_native_comparison_does_not_assert_an_ordinary_rate_from_nonpositive_base(monkeypatch, base):
    async def trend(*args, **kwargs):
        return {'통계명':'수출액','actual_measure':'수출액','actual_measure_evidence':'queried_item_label',
                'org_id':'101','tbl_id':'TEST','통계표':'수출액','단위':'100만달러','수록주기':'Y',
                '시계열':[{'시점':'2023','값':base},{'시점':'2024','값':10}]}
    monkeypatch.setattr(server, 'quick_trend', trend)
    result = asyncio.run(server.stat_time_compare('수출액', api_key='dummy'))
    assert result['비교']['증감'] == 10 - base
    assert result['비교']['변화율_퍼센트'] is None
    assert '0 또는 음수' in ' '.join(result['검증_주의'])


def test_cagr_monthly_requires_cadence_and_uses_fractional_years():
    rows=[{'period':'202401','value':100,'unit':'개'},{'period':'202407','value':110,'unit':'개'}]
    assert not OPERATIONS['cagr'].compute(rows).results
    assert OPERATIONS['cagr'].compute(rows,cadence='M').results[0].value==pytest.approx(21)


def test_time_operations_do_not_merge_item_identity():
    rows=[{'period':'2023','value':100,'unit':'개','dimensions':{'ITEM':{'code':'A'}}},
          {'period':'2024','value':200,'unit':'개','dimensions':{'ITEM':{'code':'B'}}}]
    assert not OPERATIONS['cagr'].compute(rows).results
    assert not OPERATIONS['growth_rate'].compute(rows).results


@pytest.mark.parametrize('operation',['growth_rate','cagr','yoy_pct','yoy_diff'])
def test_duplicate_period_is_not_resolved_by_last_row_wins(operation):
    rows=[{'period':'2023','value':100},{'period':'2023','value':200},{'period':'2024','value':300}]
    assert not OPERATIONS[operation].compute(rows).results


def test_native_analysis_preserves_observations_and_provided_arrays_do_not_gain_evidence(monkeypatch):
    async def trend(*args,**kwargs):
        return {'통계명':'사업체수','actual_measure':'사업체수','actual_measure_evidence':'queried_item_label',
                'org_id':'101','tbl_id':'TEST','통계표':'사업체수','단위':'개','수록주기':'Y',
                '시계열':[{'시점':str(2020+i),'값':100+i*10,'dimensions':{'ITEM':{'code':'A','label':'사업체수'}}} for i in range(3)]}
    monkeypatch.setattr(server,'quick_trend',trend)
    native=asyncio.run(server.analyze_trend('사업체수',api_key='dummy'))
    assert native['stat_evidence']['tool']=='analyze_trend'
    assert len(native['stat_evidence']['evidence']['observations'])==3
    assert native['stat_evidence']['evidence']['observations'][0]['dimensions']['ITEM']['code']=='A'
    provided=asyncio.run(server.analyze_trend('사업체수',input_rows=[{'period':str(2020+i),'value':100+i*10,'unit':'개'} for i in range(3)]))
    assert provided['verification_level']=='provided_rows'
    assert 'stat_evidence' not in provided


def test_native_series_withheld_value_keeps_a_missing_state_not_a_literal_number(monkeypatch):
    async def trend(*args, **kwargs):
        return {'통계명':'사업체수','actual_measure':'사업체수','actual_measure_evidence':'queried_item_label',
                'org_id':'101','tbl_id':'TEST','통계표':'사업체수','단위':'개','수록주기':'Y',
                '시계열':[{'시점':'2020','값':100},{'시점':'2021','값':'*'},{'시점':'2022','값':121},{'시점':'2023','값':130}]}
    monkeypatch.setattr(server, 'quick_trend', trend)
    native = asyncio.run(server.analyze_trend('사업체수', api_key='dummy'))
    withheld = native['stat_evidence']['evidence']['observations'][1]
    assert withheld['value'] is None
    assert withheld['observation_state'] == 'suppressed'
