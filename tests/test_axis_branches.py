from kosis_analysis.branches import derive_axis_branches
from kosis_analysis.metadata import MetadataCompatibilityScorer, TableMetadataProfile
from kosis_analysis.geography import project_metadata_coordinates

NODES = [{'code':'owner','label':'인천시','parent':None}, {'code':'g','label':'군구','parent':'owner'},
         {'code':'r1','label':'강화군','parent':'g'}, {'code':'r2','label':'중구','parent':'g'},
         {'code':'sex','label':'대표자 성별','parent':'owner'}, {'code':'f','label':'여성','parent':'sex'}, {'code':'m','label':'남성','parent':'sex'}]

def profile():
    axes={'ITEM':{'OBJ_NM':'항목','items':{'RATE':{'label':'폐업률','unit':'%'}}},
          'arbitrary':{'OBJ_NM':'특성별','items':{n['code']:n for n in NODES}}}
    return TableMetadataProfile('204','T','소상공인 폐업률',axes,list(axes),[],survey_name='인천광역시소상공인통계')

def test_real_branch_dimensions_are_discoverable_but_not_a_joint_matrix():
    assert [b['dimension'] for b in derive_axis_branches('arbitrary',NODES)] == ['region','gender']
    assert MetadataCompatibilityScorer(['region'],'폐업률',required_breakdowns=['region']).evaluate(profile()).status == 'selected'
    joint=MetadataCompatibilityScorer(['region','sex'],'폐업률',required_breakdowns=['region','sex']).evaluate(profile())
    assert joint.status != 'selected'

def test_invalid_tree_or_nonregional_members_are_not_promoted():
    for nodes in [NODES+[NODES[0]], [{**NODES[0],'parent':'r1'},*NODES[1:]], [{**n,'label':'여성'} if n['code']=='r1' else n for n in NODES]]:
        assert not any(b['dimension']=='region' for b in derive_axis_branches('x',nodes))

def test_current_branch_rows_get_bound_region_proof_not_other_marginals():
    rows=[{'dimensions':{'arbitrary':{'code':code,'label':next(n['label'] for n in NODES if n['code']==code)}}} for code in ['r1','r2']]
    proof=project_metadata_coordinates('204','T',profile().axes,rows)
    assert proof['axes'][0]['branch_root']=={'code':'g','label':'군구'}
    assert all(n['level']=='municipality' for n in proof['axes'][0]['coordinates'])
    assert project_metadata_coordinates('204','T',profile().axes,[{'dimensions':{'arbitrary':{'code':'f','label':'여성'}}}]) is None
