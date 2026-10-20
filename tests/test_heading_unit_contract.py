from copy import deepcopy
import pytest
from kosis_analysis.rules import HEADING_RULES, source_count_heading
from kosis_analysis.units import apply_official_coordinate_unit, bound_unit_coordinates, official_heading_item_unit
from kosis_analysis.evidence import attach_stat_evidence


def sample(member='활동', source='개%'):
    labels = {'OPAQUE': member, 'BIRTH': '신생', 'DEATH': '소멸'}
    axes = {'ITEM': {'OBJ_NM': '항목', 'items': {code:{'label':label,'unit':None} for code,label in labels.items()}},
            'REGION': {'OBJ_NM': '지역별', 'items': {'ALL': {'label':'전국','unit':None}}}}
    payload = {'org_id':'X','tbl_id':'UNREGISTERED','table_name':'기업수(활동/신생/소멸)', 'status':'executed','source':'KOSIS',
        'metadata_source': {'org_id':'X','tbl_id':'UNREGISTERED','metadata_tool':'explore_table','fetched_at':'2026-10-14T01:00:00+00:00'},
        'rows':[{'value':7641751,'period':'2024','unit':source,'dimensions':{'ITEM':{'code':'OPAQUE','label':member,'unit':None},
            'REGION':{'code':'ALL','label':'전국','unit':None}}}]}
    return payload, axes


@pytest.mark.parametrize('case', HEADING_RULES['cases'])
def test_shared_heading_grammar(case):
    assert source_count_heading(case['title'],case['item']) == case['result']


@pytest.mark.parametrize('member,source,unit', [('활동','개%','개'),('신생','개 %','개'),('소멸','개,%','개'),('신생률','개%','%'),('소멸률','개 %','%'),('활동','개','개')])
def test_joint_source_role_resolves_without_changing_values(member, source, unit):
    payload, axes = sample(member, source)
    apply_official_coordinate_unit(payload, ['ITEM','REGION'], axes)
    assert payload['rows'][0]['value'] == 7641751
    assert payload['rows'][0]['unit'] == unit
    assert payload['rows'][0]['source_unit'] == source
    assert payload['actual_measure_evidence'] == 'official_heading_item_coordinate'
    proof = payload['unit_coordinate_binding']
    assert proof['contract_version'] == 'stat-unit-coordinates/v3'
    assert 'reviewed_at' not in proof and 'definition' not in proof
    assert bound_unit_coordinates(proof,payload['rows'],'X','UNREGISTERED')
    assert attach_stat_evidence(payload,tool='query_table')['stat_evidence']['fulfillment']['completeness'] == 'complete'


@pytest.mark.parametrize('mutation', ['code','label','extra','metadata_label','metadata_unit','unit','title','date','owner','aggregation','mixed_items','request_only','multiple','denominator'])
def test_uncorroborated_source_never_partially_mutates_payload(mutation):
    payload, axes = sample(); row = payload['rows'][0]
    if mutation == 'code': row['dimensions']['ITEM']['code']='UNKNOWN'
    if mutation == 'label': row['dimensions']['ITEM']['label']='수탁거래 현황'
    if mutation == 'extra': row['dimensions']['OTHER']={'code':'F','label':'여성'}
    if mutation == 'metadata_label': axes['ITEM']['items']['OPAQUE']['label']='다른 항목'
    if mutation == 'metadata_unit': axes['ITEM']['items']['OPAQUE']['unit']=row['dimensions']['ITEM']['unit']='명'
    if mutation == 'unit': row['unit']='명%'
    if mutation == 'title': payload['table_name']='기업수 관련 현황'
    if mutation == 'date': payload['metadata_source']['fetched_at']='unknown'
    if mutation == 'owner': payload['metadata_source']['org_id']='OTHER'
    if mutation == 'aggregation': payload['aggregation']='sum_by_group'
    if mutation == 'mixed_items': payload['rows'].append({**deepcopy(row),'dimensions':{**deepcopy(row['dimensions']),'ITEM':{'code':'BIRTH','label':'신생','unit':None}}})
    if mutation == 'request_only': payload['table_name']=''; payload['query']='기업수(활동/신생/소멸)'
    if mutation == 'multiple': payload['table_name']='기업수 및 종사자수(활동/신생/소멸)'
    if mutation == 'denominator': payload['table_name']='기업당 기업수(활동/신생/소멸)'
    before=deepcopy(payload)
    apply_official_coordinate_unit(payload,['ITEM','REGION'],axes)
    assert payload == before


def test_proof_is_tied_to_original_source_title_and_all_current_coordinates():
    payload, axes = sample()
    apply_official_coordinate_unit(payload,['ITEM','REGION'],axes)
    for mutation in ('title','unit','coordinate','marker','measure','missing_proof'):
        bad=deepcopy(payload)
        if mutation == 'title': bad['table_name']='다른 표'
        if mutation == 'unit': bad['rows'][0]['source_unit']='명%'
        if mutation == 'coordinate': bad['rows'][0]['dimensions']['ITEM']['label']='신생'
        if mutation == 'marker': bad['unit_coordinate_binding']['all_observations_matched']=False
        if mutation == 'measure': bad['actual_measure']='수출액'
        if mutation == 'missing_proof': del bad['unit_coordinate_binding']
        assert attach_stat_evidence(bad,tool='query_table')['stat_evidence']['fulfillment']['completeness'] == 'incomplete'


def test_discovery_uses_the_same_declared_member_relation_without_becoming_value_proof():
    from kosis_analysis.metadata import TableMetadataProfile, MetadataCompatibilityScorer
    payload, axes = sample('신생')
    items = [{'OBJ_ID':axis, 'OBJ_NM':meta['OBJ_NM'], 'ITM_ID':code, 'ITM_NM':item['label']}
             for axis,meta in axes.items() for code,item in meta['items'].items()]
    profile = TableMetadataProfile.from_rows('X','UNREGISTERED',{},[{'TBL_NM':payload['table_name']}],items,[])
    result = MetadataCompatibilityScorer([], '신생기업수', required_items=['신생기업']).evaluate(profile)
    assert result.status == 'selected'
    assert result.item_evidence[0]['ITM_ID'] in ['OPAQUE','BIRTH']
    assert result.item_evidence[0]['ITM_NM'] == '신생'
    assert profile.item_coverage(['여성','신생기업'])[1] == ['여성']
    for title in ['기업수 관련 현황','기업수 및 종사자수(활동/신생)','종사자수(활동/신생)']:
        other = TableMetadataProfile.from_rows('X','UNREGISTERED',{},[{'TBL_NM':title}],items,[])
        assert other.item_coverage(['신생기업'])[1] == ['신생기업']
    other = TableMetadataProfile.from_rows('X','UNREGISTERED',{},[{'TBL_NM':'기업수(전체/여성)'}],
        [{'OBJ_ID':'ITEM','OBJ_NM':'항목','ITM_ID':'F','ITM_NM':'여성'}],[])
    assert other.item_coverage(['여성기업'])[1] == ['여성기업']


def test_precise_coordinate_units_still_bind_the_measure_and_mixed_heading_rates_stay_percent():
    payload,axes=sample()
    payload['rows'][0]['dimensions']['ITEM']['unit']=axes['ITEM']['items']['OPAQUE']['unit']='개'
    apply_official_coordinate_unit(payload,['ITEM','REGION'],axes)
    assert payload['unit_coordinate_binding']['contract_version']=='stat-unit-coordinates/v3'
    assert payload['actual_measure']=='기업수 · 활동'
    payload,axes=sample('신생률')
    payload['table_name']='기업수(활동/신생률/소멸률)'
    apply_official_coordinate_unit(payload,['ITEM','REGION'],axes)
    assert payload['rows'][0]['unit']=='%'
