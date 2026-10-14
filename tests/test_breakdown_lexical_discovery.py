import asyncio
import kosis_mcp_server as server

def test_explicit_breakdown_searches_outside_shortcut_pool_and_keeps_metadata_gate(monkeypatch):
    calls=[]
    async def normalize(raw, **kwargs):
        return {'source':'passthrough','normalized':raw}
    async def search(query, **kwargs):
        calls.append((query,kwargs['use_routing']))
        rows=[{'기관ID':'101','통계표ID':f'LOCAL{i}','통계표명':'경기도 산업별 사업체수'} for i in range(16)] if kwargs['use_routing'] else [
            {'기관ID':'101','통계표ID':'GLOBAL','통계표명':'시도 산업별 사업체수'},
            {'기관ID':'101','통계표ID':'WRONG','통계표명':'산업별 매출액'}]
        return {'결과':rows}
    async def meta(client,key,org,table,kind):
        if kind=='TBL':return [{'TBL_NM':'산업별 매출액' if table=='WRONG' else '시도 산업별 사업체수' if table=='GLOBAL' else '경기도 산업별 사업체수'}]
        if kind=='PRD':return [{'PRD_SE':'Y','STRT_PRD_DE':'2020','END_PRD_DE':'2024'}]
        if kind=='ITM':return [
            {'OBJ_ID':'ITEM','OBJ_NM':'항목','ITM_ID':'M','ITM_NM':'매출액' if table=='WRONG' else '사업체수','UNIT_NM':'원' if table=='WRONG' else '개'},
            {'OBJ_ID':'IND','OBJ_NM':'산업별','ITM_ID':'a','ITM_NM':'제조업'},
            {'OBJ_ID':'IND','OBJ_NM':'산업별','ITM_ID':'b','ITM_NM':'건설업'}]
        return []
    async def source(*args,**kwargs):return [{'JOSA_NM':'전국사업체조사'}]
    async def annotation(*args,**kwargs):return None
    monkeypatch.setattr(server,'search_kosis',search)
    monkeypatch.setattr(server,'_normalize_indicator_from_kosis_meta',normalize)
    monkeypatch.setattr(server,'_fetch_meta',meta)
    monkeypatch.setattr(server,'_fetch_survey_rows',source)
    monkeypatch.setattr(server,'_fetch_annotation_measure',annotation)
    result=asyncio.run(server.select_table_for_query('사업체수',indicator='사업체수',required_dimensions=['industry'],required_breakdowns=['industry'],api_key='dummy'))
    assert ('산업별 사업체수',False) in calls
    all_candidates=[result['selected'],*result['alternatives']]
    assert any(c['tbl_id']=='GLOBAL' and c['status']=='selected' for c in all_candidates if c)
    assert not any(c['tbl_id']=='WRONG' and c['status']=='selected' for c in all_candidates if c)
