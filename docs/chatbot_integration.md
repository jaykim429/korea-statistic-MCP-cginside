# Gemma 챗봇 통합 가이드

이 문서는 Gemma 4 26B 챗봇에서 KOSIS/NABO MCP 서버를 붙일 때의 권장 도구
manifest와 운영 규칙을 정리합니다. 목표는 LLM의 선택 자유도를 줄이는 것이
아니라, 선택에 필요한 원자료·메타데이터·한계 정보를 충분히 제공해 실수를
줄이는 것입니다.

## 기본 Manifest

### 요청 역할과 순위 연산의 근거

2026-10-13 검수 빌드 `104cfd6-v350-dirty`는 `stat-ranking/v1`에 전체 후보 관측/공식 비교 범위/단위·지역 근거와 방향/개수/동률 정책/선택 코드를 전달합니다. canonical `rows`는 실제 선택 행이며 단위 바인딩/`regional_coordinates`도 다시 좁힙니다. 전체 17시도의 단위 코드 목록을 선택한 3행에 그대로 붙이지 않습니다. 결측·비공개·중복·혼합 모집단/ITEM·시점·단위 또는 공식 전체 후보 범위를 확인하지 못하면 전체 순위를 제공하지 않습니다.

소비자는 전체 후보 범위/모든 좌표·단위/정렬/선택을 독립적으로 재검산해야 합니다. 모집단·지역 조건은 선두만이 아니라 정렬 전 모든 후보에 적용하세요. 닫힌 순위 연산 표현을 구별하되 ‘상위’를 불용어로 지우거나 순위 근거로 여성·청년 등 다른 조건을 면제하지 마세요. 경계 동률은 지역 코드 순으로 요청 개수만 표시했다고 알립니다. 후보가 요청 개수보다 적으면 실제 전체 지역 수를 밝힙니다.

`answer_query`의 `start_year/end_year` 단일 시점은 실제 비교/순위 후보 조회에도 적용합니다. 여러 시점이나 문장과 인자의 충돌을 최신값으로 대체하지 않습니다. 연도가 바뀌면 이전 상위 지역만 재조회해 같은 순위로 승인하면 안 됩니다. 순위는 합산 권한도 아니므로 비율/지수/분모가 있는 지표를 더해 전국 대비 비중을 만들지 않습니다. 전국 분모의 시점·단위가 다르면 비중을 계산하지 않습니다.

최신 직접 MCP 5문항은 정상 순위 4문항/여성 조건 미제공 1문항이고 실제 앱 UI 10턴은 값 제공 9턴/미제공 1턴입니다. 전체 순위 후보 68행과 화면 33행은 별도 원자료와 일치했고 로컬/배포 24소스가 같습니다. 고정 지역 후속과 광주시/광주광역시 전환도 재검수했습니다. 최초 연도 후속 실패는 수정 이력으로 보존합니다.

이 최신 실행에는 파일/차트 요청이 없었습니다. 순위 후속 산출물은 fresh 전체 후보 조회와 같은 연산을 이행하는지 따로 검수해야 합니다. 복합 측정 단위 범례의 해석/표시, 법적 분류·자료 발표 판, 기존 발견/기간/복합·운영 준비와 최신 전체 395문항/cold-start는 남습니다. 아래 이전 단계의 결과를 최신 전체 성공률로 합산하지 마세요.

### 이전 v314 기준 공식 지역 좌표와 비교 범위

2026-10-13 검수 빌드 `104cfd6-v314-dirty`는 native 지역 슬롯을 현재 공식 축 순서/전체 라벨로 연결합니다. 원행의 코드·라벨·표를 대조하고 같은 수준의 부모 합계/자식을 중복 비교하지 않습니다. 누락·비공개·비유한 값·중복·혼합 근거는 전체 비교/순위로 승인하지 않습니다. `regional_coordinates`는 현재 반환 부분집합의 좌표이며 `regional_selection`은 비교 대상과 누락 범위를 설명합니다. 메타 조회 시각/관측 기간은 법적 지역 분류 시행일이나 자료 발표일이 아닙니다.

직접 MCP 5지표/85관측은 별도 KOSIS 원행과 일치했고 배포/로컬 23소스가 같습니다. 일반 자연어 비교는 실제 실행 계획의 역할을 사용하며 여성·청년·외국인·다른 지역 수준은 임의로 생략하지 않습니다. 비교 응답의 지표 이름에 포함된 ‘합계’도 추가 합산 의도와 구별하고 분모 앞 숫자를 절대 인구수로 확대하지 않습니다.

소비자는 기본 메타 단위 ‘건’과 검증된 관측 단위 ‘천명당 건’을 구별해야 합니다. 자기 분자·분모·주제가 알려졌어도 관측 단위가 아직 확인되지 않은 후보를 exact 값으로 승인하지 마세요. 단위 확인이 필요한 후보의 fresh 조회와 지표 자체가 미확정인 후보의 항목 선택은 다른 작업입니다.

현재 앱의 단일 지역 후속은 고정 지역과 이전 시도별 비교 역할을 함께 남겨 ‘광주만/전남만’에도 전체 17시도를 반복합니다. 최신 실제 UI 6턴은 정상 비교 2턴/이 범위 실패 2턴/미제공 2턴입니다. 상위 N개도 직접 행 3개 반환과 앱의 요청 이행/전체 범위·단위 근거 전달이 별개로 남습니다. 이들을 자동 값 검증 통과나 정직한 미제공으로 복구 성공에 넣지 마세요. v320의 XLSX 2개/170셀·차트 51점은 원자료와 일치했지만 최신 전체 회귀/법적 분류 판·기존 과제/운영 검수는 미완료입니다. 미리보기 프로세스는 검사/PNG 저장 뒤 비정상 종료했고 native Excel 재열기는 하지 않았습니다.

### 지표 정의와 선택 표 후속 조회

2026-10-13 검수 빌드 `104cfd6-v290-dirty`는 공식 정의의 주 지표와 설명 속 분자·분모를 구별합니다. 등록된 율 이름은 자기 분자/분모/단위와만 동등하며 총수나 다른 분모를 동의어로 만들지 않습니다. `천명당`/`1000명당` 등의 같은 분모 표기는 계열 선택과 잔여 조건 검사에서도 공유합니다. 요청한 다른 조건을 일괄 삭제하지 않습니다.

소비자는 후보의 묶음 ITEM을 다른 지표의 관측으로 판정하지 말고 불명확한 메타로 유지해야 합니다. 실제 조회 뒤 측정 정의/공식 좌표/단위/모든 행의 모집단·지역·시점을 검증합니다. 선택한 ITEM의 주제 비교에 줄바꿈과 닫힌 산식/선행 분모 표기를 투영할 수 있지만 원 라벨과 단위·수치·좌표는 보존합니다. 제목/미선택 항목/임의 괄호·제외어로 조건 충족을 승인하지 마세요.

최종 앱 API 8문항은 값을 제공했고 실제 UI 13턴은 정상 값·출력 11턴과 미제공 2턴입니다. 조출생률/천명당 범죄의 차트와 같은 분모 표현이 복구됐으며 XLSX 2개/차트 2점을 대조했습니다. 이 검수에서 22소스의 로컬/배포 해시는 같습니다. 기존 범죄 지역 누락·공식 지역 계층/분류 판, 묶음 ITEM/수식/복합 단위 표시, 발견/기간/복합·운영·최신 전체 회귀는 남습니다. 미리보기 native 프로세스는 저장 후 비정상 종료했으며 native Excel 재열기는 하지 않았습니다. 다음 단락의 v269 실패는 수정 전 이력입니다.

### 원 단위와 공식 좌표의 단위 근거

2026-10-13 검수 빌드 `104cfd6-v269-dirty`는 native 원행 단위를 공백/등록 문자열 길이로 교체하지 않습니다. 단일값·시계열·지역 비교와 관련 차트는 실제 단위를 쓰고 원 단위·분모·지수 기준을 보존합니다. 합산은 서로 다른 단위/중복 구성요소/비율·지수의 단순 합을 거부합니다. 실제 메타 순서로 native C 슬롯을 연결하며 답변 어댑터도 canonical 행/근거를 보존합니다. 0·결측·비공개·비유한 수를 기존 공통 값 정규화 규칙으로 구별하고 유효한 native 값의 공개 타입은 유지합니다.

기존 v1 검토 정의와 별도로 `stat-unit-coordinates/v2`, `basis: "official_coordinate_metadata"`는 단위 전용 근거입니다. org_id/table_id·unit·metadata_tool/metadata_fetched_at·공개 references·required_coordinates/coordinate_labels/coordinate_units·source_units·all_observations_matched를 결합합니다. 현재 모든 행의 표·코드·라벨·분류 단위·원 단위와 해석이 일치해야 유효합니다. 측정 이름/정의/검토일을 만들어 넣거나 메타 조회 시각을 자료 발표일로 해석하지 않습니다.

단위 목록 `명 건`과 분모 표기 `명 가임여자 1명당 명`, 지수 기준 `2020＝100`은 다릅니다. 단위 확정으로 수치를 배율 변환하거나 요청한 지표/모집단/지역/시점의 조건을 면제하지 않습니다. 소비자는 같은 공유 단위 문법과 모든 현재 행을 대조해야 합니다.

직접 MCP의 값/단위 일치가 챗봇 전체 성공은 아닙니다. 최신 API는 8턴 중 값 제공 6턴이며 조출생률/천명당 범죄의 지표·계열 선택 문제가 남습니다. 범죄 지역 비교는 새 공식 코드/부모 관계 때문에 광주·전남을 누락합니다. 다른 표에도 추가 합계 분류가 있으므로 고정 코드 두 개를 추가하거나 모든 `ALL` 행을 같은 지역 수준으로 승인해서 해결하지 마세요. 공식 계층·요청 수준·자료 시점의 분류 판과 전체 UI/파일·회귀 검수가 필요합니다. 아래 v216/native 미완료 설명은 수정 전 이력입니다.

### 챗봇의 현재 출력 요청과 설정 변경

후속 54의 소비자는 현재 파일 요청/대화 내 지속 선호/설정 변경을 구별합니다. 과거 일회성 파일 작업을 새 주제에 반복하지 않고 형식 취소 뒤 차트에서도 현재 표·기간·지역·좌표를 유지합니다. 설정 변경은 조회/산출물 실행과 다르며 기존 표/조건을 유지한 확인 답변만 제공합니다. 이후 실제 파일/차트에는 fresh 근거 검증이 필요합니다.

명시한 선호/취소는 모델용 이력을 줄이기 전에 사용자 메시지에서 추출합니다. AI 발언/과거 일회성 작업/인용/새 대상은 선호가 아닙니다. 설정/선호·클라이언트 verified 표시·이전 수치는 MCP 실행/조건 충족 근거가 아닙니다. 큰 교차표의 서버 출력은 기존대로이며 현재 여러 형식 산출물/복합 요청 전체 이행을 완료한 것은 아닙니다.

현재 MCP 소스/빌드 `104cfd6-v216-dirty`는 무변경입니다. 아래 v216 단락의 새 주제 파일 문제는 챗봇에서 교정했지만 native 단위 소유권은 남습니다. scalar/시계열은 공백 단위를 등록 단위로 바꾸고 지역 비교는 등록 단위를 사용합니다. 원자료의 `명 건` 같은 단위 목록, `명 가임여자 1명당 명` 같은 분모, `백만원 %p` 같은 복수 단위, 지수 기준을 구별해야 합니다. 실제 코드/라벨/단위·공식 메타 없이 보완하지 말고 원 단위를 보존하세요. native 슬롯은 공식 메타 순서에 연결하며 알파벳 순으로 추측하지 않습니다.

### 선택한 좌표의 단위 근거

2026-10-13 검수 빌드 `104cfd6-v216-dirty`는 비집계 `query_table`에서 `evidence.unit_coordinate_binding`을 제공합니다. `contract_version: "stat-unit-coordinates/v1"`, `basis: "verified_curation_coordinates"`, 실제 org/table·unit·measure_name/definition·reviewed_at·공개 표 references·required_coordinates/coordinate_labels·source_units·all_observations_matched를 결합합니다. 행의 `source_unit`은 선택 전 공식 원 단위입니다.

current/verified 검토 정의의 모든 축을 현재 공식 메타/실제 행의 코드·라벨과 대조한 경우에만 안전한 원 단위 목록 중 선택합니다. 명목 GRDP의 `백만원 %p`는 검토한 ITEM 명목/시장가격/지역 좌표에서 백만원이지만 실질·기여도까지 확대하지 않습니다. 추가 축·집계·다른 코드/라벨·분모/지수 기준/배율·미검증 또는 중복 정의는 승인하지 않습니다. 원 수치는 바꾸지 않고 알려진 공식 ITEM/측정축이 우선합니다.

소비자는 이번 조회의 모든 행/원 단위/표/코드·라벨/공개 참조를 독립 검증해야 합니다. 단위 목록 문법은 TS/Python 공유 정본이고 정의 검토일은 자료 발표일·갱신일이 아닙니다. 표 ID·화폐·요청 문장·이전 답변의 단위는 근거가 아닙니다.

명목 fresh 재조회/차트/파일은 실제 원자료와 일치했지만 native 초기 경로의 원 단위/동일 좌표 근거는 별도 개선 대상입니다. 화면의 새 주제에 이전 파일 요청이 따라오는 문제, 실질·기여도·분기 발견/조회 안내와 이전 운영 과제도 남습니다. 자동 PASS나 정직한 미제공을 상용 성공으로 세지 않습니다. 아래 지역 좌표 단락의 v185 단위 실패는 수정 전 이력이며 native와 fresh 전체 경로의 단위 소유권이 완료됐다는 뜻은 아닙니다.

### 공식 지역 좌표와 요청한 지역 수준

2026-10-13 검수 빌드 `104cfd6-v185-dirty`는 비집계 `query_table`의 fresh 메타와 실제 반환 코드·라벨에서 `evidence.regional_coordinates`를 제공합니다. `contract_version: "stat-regional-coordinates/v1"`, org/table, `basis`, `all_observations_matched`, 각 축의 `axis_id`/`axis_name`과 선택 좌표의 code/label/parent/kind/level/ancestors를 결합합니다.

공식 부모 관계로 시도, 기초 시군구, 그 아래 일반구/읍면동을 구별합니다. 코드 길이·고정 지역 개수·표 제목·호출자가 적은 지역명만으로 지역 수준을 결정하지 않습니다. native 근거는 current/verified·비집계 선언과 검토된 지역 코드북의 실제 코드/라벨이 모두 일치할 때만 제공합니다. 수치의 단위나 국가 근거를 대신하지 않습니다.

소비자는 현재 표와 모든 관측의 좌표 및 완전한 부모 관계를 독립적으로 대조하고 원하는 지역 수준을 확인해야 합니다. 새 지역 단위 요청은 이전 단위를 교체하며, 지원하지 않는 단위 선택으로 표 전체를 제거하지 않습니다. 일부 조회는 전체 비교나 전 지역 수치 순위로 승인하지 않습니다. 앱의 `display_slices`는 공식 메타의 선택 계획을 현재 행과 결합한 표시 범위이고 MCP가 사용자 요청 전체를 이행했다는 선언이 아닙니다.

이번 실제 검수에서 지역내총생산 native 첫 답변은 값/단위를 제공하지만 fresh 표 재조회에는 표 전체 단위 `백만원 %p`가 함께 내려와 명목 항목의 단위를 확정하지 못해 후속 차트가 거절됩니다. 항목 메타에는 단위가 없습니다. GRDP 약어의 후보 단계, 특정 지역 내부 분해, MCP 내부 20초 제한 초과와 표시 품질도 열린 과제입니다. 안전한 거절이나 자동 PASS를 상용 성공으로 세지 않습니다.

### 공식 측정 이름과 분류축의 측정 근거

2026-10-13 검수 빌드 `104cfd6-v163-dirty`는 공식 줄바꿈 표기를 측정 낱말 경계로 읽습니다. 원 라벨은 보존하며 `QuickStatParam.measure_name`은 측정 이름, `description`은 설명식/정의 역할입니다. 설명식의 마지막 피연산자를 지표로 선택하거나 임의 괄호 제거로 의미를 바꾸지 않습니다.

측정 이름이 ITEM이 아닌 공식 측정축에 있는 경우 `actual_measure_evidence: "official_measurement_coordinate"`와 `measure_role_evidence`를 제공합니다.
`contract_version: "kosis-measure-coordinate/v1"`, `source: "ITM"`, org/table, `axis_id`/`axis_name`, `measure`, 선택 `items`의 code/label/unit, `item_codes`, `all_observations_matched`가 결합됩니다.
공식 축 역할과 모든 fresh 행의 실제 코드·라벨·단위가 일치하고 지표가 하나일 때만 보완합니다. 알려진 실제 ITEM이 우선하며 집계·모호한 축·혼합 지표·단위 불일치·요청 문장/표명/화폐만으로 측정을 추론하지 않습니다.
소비자는 선언만 믿지 않고 현재 표·관측 코드/단위를 다시 확인합니다. 메타의 측정 후보는 실행/모집단/주기 충족 근거가 아닙니다. 기존 all-of/verified-only/Case O/표 선택 후 fresh 재조회 정책을 유지합니다.

일반 통계 엑셀 소비자는 원 시점 코드를 유지하고 검증된 연/월/분기 표기를 함께 표시합니다. 같은 `202602`를 월/분기로 추측하지 않습니다. 지역 계층·최신 주기 표현·후속 구체화/현재 출력 의도와 운영 준비는 별도의 열린 과제입니다.

### 국가 근거와 선택 표 재조회

2026-10-13 검수 빌드 `104cfd6-v134-dirty`는 검토된 계열의 국가·작성기관·가격 기준을 `evidence.series_context`로 전달합니다.
`contract_version: "stat-series-context/v1"`, `basis: "verified_curation_coordinates"`와 실제 org/table·unit·`required_coordinates`·`all_observations_matched`를 함께 제공합니다.
current/verified·비집계 선언의 ITEM와 필수 고정 분류 코드·단위가 fresh 관측 전체와 일치해야 합니다. 표/기관/요청 문장이나 추가 미결합 축만으로 승인하지 않습니다.
`answer_query`, `quick_stat`, `quick_trend`, `query_table`이 같은 evidence projection을 사용하며 `query_table.coordinate_axes`는 fresh OBJ_ID 순서입니다.
국내 지역 코드북의 국가 근거도 공통 경계에서 지역 코드/라벨을 대조합니다. 다른 출처·잘못된 단위/좌표·집계·미검증/모호한 선언은 계열 근거로 사용할 수 없습니다.

소비자는 분류축에 국가가 없다는 이유만으로 표를 조회 불가로 판정하지 않습니다. 한국/대한민국/우리나라 조건은 원 질문에 남겨 새 조회의 국가 근거로 최종 검증합니다.
이는 기존 승인이나 클라이언트 맥락을 믿으라는 뜻이 아닙니다. 국가 근거가 없거나 다른 경우 값/파일을 승인하지 않고, 추가 모집단·명목/실질·지역/업종/규모 조건은 모두 유지합니다.
`compiler`는 검토된 계열 작성기관이며 배포처 KOSIS와 구분합니다. `reviewed_at`은 정의 검토일이고 자료 발표일/갱신일이 아닙니다.
실제 SOURCE/ORG/NCD의 메타 역할·발표/갱신 판과 등록 밖 계열 범위는 추가 검수 대상입니다. 자동 통과나 정직한 미제공을 상용 성공으로 세지 않습니다.

### 지표 이름과 공개 출처 링크

2026-10-13 검수 빌드 `104cfd6-v115-dirty`의 TS/Python 공유 규칙은 GDP 성장률·국내총생산 성장률·경제성장률을 같은 등록 지표로 다룬다.
일반 성장률이나 다른 대상의 증가율을 GDP로 승인하지 않는다. 국가·명목/실질·추가 모집단 조건은 여전히 별도 근거가 필요하다.
클라이언트는 지표 구성 요소를 가짜 모집단으로 읽지 않아야 하지만, 검증되지 않은 실제 조건을 버려서도 안 된다.

`evidence.source.url`은 명시적인 출처 URL이 우선이다. KOSIS 출처에 URL이 없고 실제 org/table 식별자가 있으면 해당 공개 통계표 링크를 제공한다.
식별자는 인코딩된 URL 파라미터이며 일반 문장의 내부 코드와 다르다. 답변/후속 문장 정리에서 이 URL을 홈페이지나 잘못된 문자열로 바꾸지 않는다.
표 링크는 국가·작성기관·발표/갱신 판을 증명하지 않는다. 위의 검토된 계열 근거와도 자료 발표/갱신 판을 혼동하지 않는다.
이전 빌드의 대한민국 GDP 다른 계열 선택 실패는 국가 근거/재조회 검증으로 교정됐지만 최신 전체 회귀와 등록 밖 계열까지 완료한 것은 아니다.

### 정확한 원 시계열과 측정 좌표

이전 검수 빌드 `104cfd6-v102-dirty`부터 `quick_trend`에 `period_type`, `start_period/end_period`, `latest_count`를 제공한다.
정밀 범위는 연도 인자와 함께 쓰지 않으며, 최신 시점 수는 1~100이다. 단순 산술은 회귀 도구의 최소 표본 수를 가용성 조건으로 쓰지 않는다.
도구 응답에는 원 관측의 출처·수록 주기·요청 시점과 StatEvidenceEnvelope가 포함된다. 소비자는 대상 시점과 전월/전년 등의 비교 입력 시점을 분리해야 한다.
분기 조회 입력은 [KOSIS 공식 형식 YYYYQQ(QQ 01~04)](https://kosis.kr/openapi/devGuide/devGuide_0701List.do)으로 정규화한다. 내부 표시 별칭을 달력 좌표로 비교하고 연간 값으로 정밀도를 낮추지 않는다.

`query_table`의 넓은 ITEM 가족에서 세부 지표는 검증된 매핑의 ITEM·고정 분류 코드·단위가 fresh 행 전체와 일치할 때만 보완한다.
`curation_measure_binding`은 실제 표와 필수 좌표를 함께 남긴다. `명 건`처럼 복수 개수 단위가 있는 경우에는 공식 선택 분류의 단위까지 일치해야 해석하며 `source_unit`과 `unit_resolutions`를 보존한다.
합산·혼합/다른 단위·미검증/중복 정의·코드 부족은 이 근거로 승인하지 않는다. 공식 ITEM/주석의 측정 의미가 우선하며, 표 ID나 요청 문장만으로 의미를 부여하지 않는다.

native 응답과 재조회 모두 같은 측정·모집단/기간 검증을 거쳐야 한다. 이전 턴의 수치나 client verified 표시는 새 파일/계산 근거가 아니다.
이전 v115 검수의 자동 통과에는 GDP 국가 근거/계열 선택 실패가 포함됐으며 위의 국가 근거와 재조회 경계에서 교정했다. 국제 비교 계열과 국내 확정 계열을 같은 값으로 취급하지 말고 작성기관·계열·발표/갱신 기준을 확인해야 한다.

### 원자료와 파생 계산의 역할 및 달력 기간

2026-10-13 후속 검수에서는 원자료 조회와 계산 실행의 실제 소유권을 대조했습니다.
native 시계열의 `*`는 null/suppressed로 정규화하며 0으로 바꾸지 않습니다. 알려진 끝점의 CAGR은 중간 비공개를 공개하고 계산할 수 있지만
비공개 끝점을 다른 시점으로 밀거나 원 값을 추정해서는 안 됩니다.
직접 `stat_time_compare`의 일반 상대율은 양수 시작값에만 반환합니다. 0/음수이면 원값·절대 증감·미계산 사유를 사용하세요.
산식과 다른 절댓값 분모의 상대율을 같은 산식이라고 표시하지 않습니다.

소비자의 계산 후속도 선택 표/모든 실제 분류 좌표·지역/요청 기간을 이어받아 fresh 메타/행 검증을 수행해야 합니다.
이번 실행의 같은 축/항목에서 검증된 바인딩만 원자료 검증과 계산에 공유하세요. 클라이언트의 verified 문자열이나 이전 성공 상태는 증명이 아닙니다.
분류 코드 장식의 좁은 매칭은 항목 이름 전체 일치 뒤의 navigation이며 실제 행의 조건 충족을 대신하지 않습니다.
계산 불가를 표 자체의 조회 불가로 승격하지 않고, 완료 문장은 실제 파일/차트 생성 결과만 말합니다.

최종 표적 검수는 앱 2,229시험/MCP 1,113시험, API 76실행/162턴과 실제 UI 35턴, 파일 41개입니다.
MCP `104cfd6-v90-dirty`/`review-v90-20261012`, 배포/로컬 9파일 해시가 같습니다. 아래 10월 12일 수치는 이전 검수 이력입니다.
상세 월/분기 끝점·다중 시점/제공 숫자, native 초기 조회의 표/필터 capture·누락 시점/사유 안내와 복합/전체 회귀·운영 준비는 남습니다.
전체 1인 가구와 2019년 소상공인 비교의 미제공을 복구 완료로 세지 않습니다.

native `analyze_trend`는 현재 조회한 원 시계열의 실제 ITEM/분류·단위·수록 기간·품질/추계/수정 경고를 전달합니다.
회귀 계수·증감·CAGR을 원 관측값으로 해석하지 마세요. 호출자가 제공한 `input_rows`는 provided_rows이며 KOSIS 실행 증명이 아닙니다.
소비자는 원 질문의 측정·범위·단위·기간을 별도로 확인한 뒤 같은 원자료 그룹에서 계산해야 합니다.

`compute_indicator`의 CAGR에는 필요한 경우 `cadence`(Y/M/Q)를 전달하세요. 연/월/분기 좌표의 실제 경과 연수를 사용하며,
6자리 시점만으로 월별 주기를 임의 추정하지 않습니다. 누락 시점의 개수를 연수로 대체하지 않으며 중복/비유한 수치는 승인하지 않습니다.
증감액과 상대율, 원 단위가 `%`인 값의 `%p` 변화를 구분하세요. 비양수 시작값의 일반 상대 증가율이나 비양수 끝점의 CAGR은 별도 한계가 필요합니다.
숫자 0은 누락값이 아닙니다. 원자료의 품질/적용 범위 경고를 계산 결과/파일에서 제거하지 마세요.

앱의 `stat-derived/v1`은 role=derived와 시작/종료 원 수치·시점·단위·산식·경고를 보관하고 답변/엑셀/단위별 차트/보고서가 이를 공유합니다.
파일 후속은 원자료를 다시 조회하며 클라이언트 숫자나 이전 성공 플래그를 새 증명으로 사용하지 않습니다.
명시 월별 비교 조회를 보강했지만 상세 월/분기 끝점, 선택 표/필터 상속과 직접 MCP 음수 기준율 산식은 추가 검수 과제입니다.
계산 실패를 표 자체의 조회 불가로 승격하지 마세요.

2026-10-12 최종 표적 검수는 앱 2,221시험/MCP 1,110시험, API 67실행/129턴 및 실제 UI 24턴/첨부 33개입니다.
MCP `104cfd6-v85-dirty` 배포/로컬 9파일 해시는 같습니다. E32의 2019년 비교는 미복구이며 안전한 미제공을 값 제공 성공으로 세지 않습니다.
최신 395문항·cold-start·운영 준비와 복합/비중/출처 비교는 남습니다. 상용 전체 완료가 아닙니다.

### 공식 조사 설명은 관측값과 다른 역할입니다

`tools/list`에서 `explain_statistic` 제공 여부를 확인하고, 표본 규모/오차/설계·조사 방법·지수 기준년 질문에는
`explain_statistic(query, org_id?, tbl_id?)`를 사용합니다. `answer_query`도 순수 방법론 질문을 이 경로로 분리합니다.
명시적인 표 좌표가 있으면 공식 조사와 원 질문의 연결을 다시 확인하며, 좌표 한쪽만 넘기거나 무관한 표를 선택했다고 승인하지 않습니다.
확인된 큐레이션 연결 또는 정확한 조사명의 유일한 공식 검색 결과로
[KOSIS 통계설명자료 API](https://sso.kosis.kr/openapi/devGuide/devGuide_0401List.do)를 조회합니다.

응답은 `stat-methodology/v1`, `role=methodology`, `actual_query_supported=false`이며 `stat_evidence`의 관측 실행을 대신하지 않습니다.
`fields`의 공식 필드/라벨/실제 텍스트, `covered_facets`/`partial_facets`/`missing_facets`, `source`, `warnings`를 함께 표시하세요.
`explained`/`partial`의 설명자료와 `not_found`/`needs_topic`/`ambiguous_source`/`service_unavailable`을 구분합니다.
표본 설계·산식·목표 RSE는 실현 표본오차가 아닙니다. 특정 연도·지역의 적용이 미확인인 경고를 제거하지 않습니다.
명시적인 지수 기준 표시도 그 기준년을 선택·변경한 이유의 증명은 아닙니다. 인과 해석의 일반 설명은 효과 추정값이 아닙니다.

설명 엑셀/문서는 관측 행이 아닌 공식 텍스트 필드와 적용 범위/출처를 담고, 설명 숫자로 가짜 시계열 차트를 만들지 않습니다.
자료를 다시 확인하는 후속 상태에는 질문과 유효한 표 좌표만 남기세요. 새 설명이 실패한 뒤 파일 요청이 더 오래된 관측 파일을 재사용해서는 안 됩니다.
`이 값은 몇 년 기준이야?` 같은 표시 내용 설명과 지수 기준년/연도 변경/재조회는 다른 역할입니다.
차트를 모델 입력에서 제외하더라도 최신 사용자 표시 이력은 설명 경로에 보존해야 합니다. 이 이력은 새 실행 증명으로 인정하지 않습니다.
혼합 방법론+값 질문을 분류하는 것만으로 두 작업을 모두 수행한 것은 아니므로 각 작업의 실제 충족을 확인하세요.

2026-10-12 표적 검수: 앱 2,195시험·MCP 1,093시험, API 41실행/70턴과 실제 UI 13턴/파일 11개.
MCP 빌드 `104cfd6-v81-dirty`, 앱 최종 검수 v82이며 배포/로컬 8파일 해시를 대조했습니다.
실현 표본오차·연도별 적용 자료/모호한 조사 선택·원 설명 필드의 표시 품질, 계산/복합·발견 안정성과 전체 회귀/운영 준비는 남습니다.
표적 자동 통과나 정직한 미제공을 상용 전체 완료로 해석하지 않습니다.

### 2026년 10월 집계 범위와 발견 근거의 보강

실행 근거의 `evidence.source_scope`는 `stat-source-scope/v1` 계약으로 표 ID와 고정 조건을 함께 반환한다.
`basis=official_table_title`은 공식 표 제목의 조건이라는 뜻이지 사용자 질문의 전체 모집단을 충족했다는 뜻은 아니다.
예를 들어 가구원수 축의 '1인'과 일반가구수 ITEM이 맞아도 무주택/주택소유/아파트소유 가구수는 전체 1인 가구 값이 아니다.
소유 상태나 표 ID를 열거한 차단 목록 대신 공유 규칙의 고정 가구수 제목 수식 조건으로 판정한다.
비교 분류표나 주제형 현황 제목은 고정된 제한 집계 제목과 구분한다. 실제 분류에 전체/제한 집단이 함께 있을 수 있다.
미등록 표의 전체 집계 범위까지 증명된 계약은 아니므로 소비자는 원 질문과 실제 조건을 별도로 검증해야 한다.

`select_table_for_query`는 같은 표의 직접 매핑/검증된 발견 후보를 합칠 때 검증된 발견 역할을 보존한다.
중립적 표명의 검증된 후보가 메타 조회 예산 밖으로 밀리지 않도록 하되 기존 `max(limit + 4, 12)` 예산을 유지한다.
큐레이션은 발견 근거이며 실제 값의 승인이 아니다. 공식 조사명으로 고정 모집단을 보완할 때도 동일한 등록 집단만 충족시키며
추가 집단과 명시적 분해 축은 별도로 확인한다. 현재 값·단위·기간·실제 필터의 검증을 생략하지 않는다.

KOSIS 원자료의 C1..C10 열 이름은 ITM 메타의 OBJ_ID와 같다고 가정하지 않는다.
소비자가 원 관측의 좌표를 이어받는 경우 새 메타의 코드·라벨과 유일하게 대응시키고, 캐시/클라이언트 수치는 새 조회 증명으로 사용하지 않는다.

정밀 검증을 중시하는 manifest에는 아래 도구를 중심으로 노출하는 것을 권장합니다.

- `list_supported_statistics`: 챗봇이 1차로 조회할 수 있는 분야·통계·상세를 탐색합니다.
  상세 응답에는 최근 검증일을 노출하지 않으며, 실제 조회 성공은 별도 도구 결과로 판단합니다.
- `browse_kosis_catalog`, `browse_topic`: KOSIS 전체 후보나 큐레이션 주제를 탐색합니다. 이 단계의
  표와 지표는 후보이며 실제값 조회 성공을 뜻하지 않습니다.
- `stat_detail`: 매핑 상세, 지원 지역·기간, 다음 호출 예시를 확인합니다.
  `매핑확정(실조회미확인)`은 실제값 조회 전 상태입니다.
- `quick_stat`: `stat_detail`의 검증된 매핑을 실제 호출합니다. 값, 단위, 시점,
  출처가 모두 반환된 경우에만 조회 성공으로 취급합니다.
- `plan_query`: 복합/정밀 통계 질문의 계획 입구입니다. 의도, 지표, 차원, 시간,
  다음 단계만 만들고 실제 값을 조회하지 않습니다. 단순 수치 조회는 `answer_query`, 순수 방법론은 `explain_statistic`을 우선할 수 있습니다.
- `explain_statistic`: 공식 조사 설명의 실제 필드와 적용 범위를 확인합니다. 숫자가 들어 있어도 관측 수치 조회 성공이 아닙니다.
- `select_table_for_query`: 필요한 분류축을 만족하는 통계표 후보를 KOSIS
  메타데이터 기반으로 고릅니다.
- `resolve_concepts`: `서울`, `30대`, `여성`, `광역시` 같은 자연어 개념을
  선택된 표의 코드 후보로 변환합니다.
- `explore_table`: 표의 축과 코드 메타데이터가 더 필요할 때 사용합니다.
- `query_table`: 검증된 `OBJ_ID`/`ITM_ID` 필터로 KOSIS 원자료를 추출합니다.
- `compute_indicator`: `query_table.rows`에 검산 가능한 산식과 단위 추적 정보를 붙입니다.
- `search_kosis`: 표 후보가 필요하지만 `select_table_for_query`가 충분하지
  않을 때만 사용합니다.
- `search_stats`: KOSIS와 NABOSTATS 표 후보를 함께 찾는 통합 검색 입구입니다.
- `search_nabo_tables`, `explore_nabo_table`, `query_nabo_table`: 국회예산정책처
  재정경제통계시스템(NABOSTATS) 표 검색, 코드 확인, 원자료 조회에 사용합니다.
- `answer_query`: 빠른 자연어 편의 도구입니다. 기본값은 `verbose=false`라서
  얇은 `data`/`metadata`/`notes` 응답을 반환합니다.

전문가 또는 분석용 manifest에 선택적으로 노출할 도구:

- `analyze_trend`, `forecast_stat`, `correlate_stats`, `detect_outliers`, `chain_full_analysis`
- `chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_heatmap`,
  `chart_distribution`, `chart_dual_axis`, `chart_dashboard`
- `search_nabo_terms`: NABO 통계 용어사전이 필요할 때만 사용합니다.

기본 manifest에서 숨길 도구:

- `quick_trend`, `quick_region_compare`: 빠른 편의 도구입니다. 정밀한 표/코드
  검증이 필요한 챗봇에서는 절차형 도구를 우선합니다.
- `verify_stat_claims`, `decode_error`, 일회성 진단 helper

기본 manifest 예시는 [gemma_manifest.default.json](gemma_manifest.default.json)을
참고하세요.

## 운영 흐름

1. 챗봇 라우터가 사용자 질문이 통계 질문인지 먼저 판단합니다.
   순수 방법론 질문이면 `explain_statistic`으로 공식 설명을 확인하며 아래 수치 실행 절차의 성공으로 승격하지 않습니다.
2. 단순 질문이면 `answer_query(query)`로 빠르게 처리할 수 있습니다. 탐색에서 시작한
   경우 `browse_kosis_catalog`/`browse_topic` → `stat_detail` → `quick_stat` 순서로
   진행하며, `quick_stat`에 실제 값이 없으면 즉시 실패 또는 KOSIS 직접 확인을 안내합니다.
   복합 질문, 다중 지표, 산식, 표 선택 근거가 중요한 질문이면 `plan_query(query)`를 먼저 호출합니다.
3. `plan_query.next_call`, `suggested_workflow`, `evidence_workflow` 중 가장
   구체적인 레일을 따릅니다.
4. `metrics`는 요청 지표, `quarantined_metrics`는 충돌 또는 오염 의심 지표입니다.
   quarantined 지표를 답변에 포함하지 않습니다.
5. `multi_metric_request` marker가 있으면 `metrics[]`의 각 지표를 별도
   `select_table_for_query` 경로로 처리합니다. KOSIS 메타가 같은 표를 증명할
   때만 하나의 표를 공유합니다.
6. `semantic_dimensions`는 LLM 친화 의미 표현이고, `table_required_dimensions`는
   KOSIS 표 메타 축 매칭용 표현입니다. 후속 표 선택에는 `table_required_dimensions`
   를 사용합니다.
7. `query_table`에는 메타데이터 기반 도구가 반환한 코드만 넣습니다.
8. NABO 자료가 더 적합하면 `search_stats` 또는 `search_nabo_tables`로 표를 찾고,
   `explore_nabo_table`에서 항목 코드를 확인한 뒤 `query_nabo_table`을 호출합니다.
   NABO 응답은 항상 `source_system: "NABO"`와 `provider`를 보존합니다.
9. 산식은 `compute_indicator`를 쓰거나 LLM이 직접 계산할 수 있습니다.
   `compute_indicator`를 쓰면 `query_table.rows`를 그대로 `input_rows`로 넘기고,
   허용된 enum(`per_capita`, `share`, `ratio`, `growth_rate`, `cagr`,
   `yoy_pct`, `yoy_diff`, `sum_additive_rows`)을 사용합니다. 단위 변환·가법성 판단은 caller가 책임지며 응답 markers
   (`denominator_zero`, `period_or_group_mismatch`, `additivity_caller_asserted`,
   `partial_computation`)로 한계를 표시합니다.

## 분석 도구 원칙

분석 도구는 최종 결론을 대신 쓰지 않습니다. 기본 응답은 아래 재료를 제공합니다.

- `input.data`, `input.x`, `input.y`: LLM이 직접 재계산할 수 있는 원자료 배열
- `must_know`: 단위, 최신 시점, 데이터 나이, 비교/해석 한계
- `data_characteristics`: 단조성, 변동성, 음수/0 포함 여부, index 여부
- `common_pitfalls`: 지수/절대값 혼동, 작은 표본, 상관과 인과 혼동 같은 구체적 함정
- `model_parameters`, `formula`, `residuals`, `fitted_values`: MCP가 계산한 경우 재현 가능한 계산 흔적
- `model_options`: forecast 등에서 LLM이 선택할 수 있는 모델 후보와 caveat

자연어 `해석`은 기본적으로 포함하지 않습니다. 필요할 때만 `include_interpretation=true`를 사용합니다.

## 응답 계약

### 검색 역할과 후보 메타 (2026-10-12)

`select_table_for_query`는 원 질문의 알려진 측정 의미를 짧은 검색어로 덮지 않습니다.
등록된 검색 별칭(`매출` 등)은 검색에만 사용하며 실제 ITEM/단위/분모의 등가 증명으로 사용하지 않습니다.
`subject_relevance`는 실제 표명·조사 출처·항목에 대한 검색 대상 관련성입니다.
이 힌트와 `population_compatibility`는 다른 역할이며, 미확인 모집단을 삭제하거나 값을 승인하는 근거가 아닙니다.
정확히 등록된 공식 조사 범위는 메타의 모집단 조건을 보충할 수 있지만 추가 조건은 그대로 검증해야 합니다.

표 후보의 `survey_name`과 `periods[]`를 사용자 선택까지 보존하세요. `explore_table.periods[]`도 기존 PRD 메타 조회의
모든 지원 주기를 반환합니다. `수록기간`의 최신 한 행을 전체 주기 목록으로 해석하지 않습니다.
2년 주기를 매년 자료로 바꾸지 않으며 분기 요청에 연간 후보를 제시하면 그 차이를 명시합니다.
표 후보뿐 아니라 항목/분류 선택 단계에도 같은 메타를 전달해야 합니다.
이 단계는 `actual_query_supported: false`인 탐색/메타 상태이며, 실행된 관측값이나 요청 완전 충족의 증명이 아닙니다.

### StatEvidenceEnvelope v1

`quick_stat`, `answer_query`, `query_table`은 기존 응답 필드를 유지하면서
`stat_evidence`를 추가로 반환합니다. 챗봇은 도구별 필드명이나 답변 문구를 다시
추론하지 말고 이 공통 계약을 우선 읽습니다.

- `contract_version`: 현재 `stat-evidence/v1`
- `build_commit`: 실행 중인 MCP 이미지의 빌드 커밋. 로컬 소스 실행처럼 알 수 없는
  경우 `unknown`
- `execution.status`: `executed`, `no_data`, `failed`, `unknown`. KOSIS 호출이
  실행됐는지에 관한 상태이며 질문 충족 여부와 다릅니다.
- `execution.failure_class`: `infrastructure`, `invalid_request`,
  `period_unavailable`, `no_rows`, `partial_coverage`, `unknown`
- `execution.retryable`: 같은 요청을 재시도할 가치가 있는 장애인지 여부
- `fulfillment.status`: `exact`, `partial`, `unavailable`, `unknown`. 질문의 조건을
  실제 결과가 충족했는지에 관한 상태
- `fulfillment.missing_fields`: 실행은 됐지만 값·단위·기간·출처·표 식별자 중 빠진 필드
- `fulfillment.requested_concepts`, `resolved_concepts`, `missing_concepts`: 요청한
  분류와 실제 행에서 확인된 분류의 대응. 무시된 shortcut 파라미터도
  `parameter:<name>` 형태로 `missing_concepts`에 들어갑니다.
- `evidence.observations`: 행마다 `value`, `unit`, `period`, `dimensions`를 같은
  형태로 반환합니다. 여러 행을 임의의 대표값 하나로 축약하지 않습니다.
- `evidence.actual_measure`, `actual_measure_evidence`, `measure_basis`: 표가 실제로
  센 항목과 그 근거. `measure_basis`는 근거가 있는 응답에서만 채우며 모르면 비워 둡니다.
- `evidence.period`: `requested`, `used`, `available`, `selection_mode`, `cadence`를
  분리합니다.
- `evidence.source`, `evidence.table`: 출처와 기관/통계표 식별자

`execution.status="executed"`만 보고 답하지 않습니다. 일반 단일값 답변은
`fulfillment.status="exact"`이고 `completeness="complete"`일 때 바로 사용할 수
있습니다. `partial`이면 반환된 값 자체가 유효할 수 있으므로 `missing_concepts`를
확인해 질문의 핵심 조건인지 판단합니다. `failed`이면서
`failure_class="infrastructure"`이면 통계 부재로 말하지 않고 재시도 가능한 서비스
장애로 처리합니다.

#### 일상어와 통계 용어가 다를 때

`기업 수`와 `사업체 수`처럼 일상 대화에서는 비슷하게 쓰이지만 통계 작성 단위가
다를 수 있는 표현은 **경고만으로 답변을 차단하지 않습니다**. 값·기간·단위·표가
완전하면 `evidence.actual_measure`의 정확한 명칭으로 답하고, “통계상 기업체/사업체
기준이라 일상적 기업 수와 다를 수 있다”는 짧은 설명을 붙입니다.

다만 다음 경우에는 정의 차이가 결론을 바꾸므로 확인 질문 또는 대안 표 선택이
필요합니다.

- 기업 수와 사업체 수를 서로 비교하거나 비율·증감률의 분자/분모로 쓰는 질문
- 지원 자격, 법적 정의, 정책 대상 수처럼 행정상 단위가 중요한 질문
- 같은 표현으로 둘 이상의 현행 통계표가 가능하고 값 차이가 큰 질문
- 사용자가 법인 단위, 사업장 단위 등 모집단을 명시한 질문

즉, MCP는 실제 측정항목과 근거를 제공하고, 챗봇은 질문의 위험도와 목적에 따라
“설명 후 답변”과 “먼저 확인”을 구분합니다.

차트 도구는 SVG만 반환하지 않습니다. 성공 응답에는
`capability_state: "query_executed"`, `actual_query_supported: true`, 실제
`rows`, `row_count`, 요청·사용 지역, `svg`가 함께 들어갑니다. 일부 지역이나
시점이 누락된 비교·히트맵·분포는 `actual_query_supported: false`이며 SVG를
최종 차트로 표시하면 안 됩니다.

`plan_query`는 답변이 아니라 증거 수집 계획입니다. 응답에는 아래 계약이 들어갑니다.

- `mcp_output_contract.role`: `planning_only` 또는 `clarification_required`
- `mcp_output_contract.final_answer_expected`: 항상 `false`
- `mcp_output_contract.follow_up_required`: 후속 도구 호출 필요 여부
- `mcp_output_contract.failure_markers`: LLM이 실패·부분충족을 감지해야 하는 필드
- `mcp_output_contract.current_signals`: 현재 응답에서 실제로 켜진 caveat 요약
- `mcp_output_contract.current_signals.marker_guidance`: 켜진 marker별 후속 행동 안내
- `recommended_tool_manifest`: 기본/전문가/숨김 도구 목록
- `canonical_fields`: Gemma가 우선 읽어야 하는 정규 필드
- `deprecated_fields`: 호환을 위해 남긴 구 필드와 대체 필드 안내

Gemma는 `answer`가 `null`인 계획 응답을 사용자에게 최종 답처럼 말하면 안 됩니다.
최종 답변은 `query_table` 등 후속 도구가 만든 rows와 검증 메타를 모은 뒤 생성합니다.

## 실패 처리 규칙

아래 필드가 있으면 “자료 없음 또는 한계 있음”을 사용자에게 명시합니다.

- `status: "needs_clarification"`
- `status: "unsupported"`
- `status: "failed"`와 `capability_state: "service_unavailable"`: 통계 부재가 아니라
  KOSIS 연결·호출 장애입니다. 잠시 후 재시도하거나 KOSIS 통합검색을 안내합니다.
- `actual_query_supported: false`: 탐색·매핑·메타 확인 단계이거나 실제값 조회 실패입니다.
- `validation_errors`
- `missing_metrics`
- `quarantined_metrics`
- `multi_metric_request`: 지표별 표 선택/코드 해소를 분리해야 합니다.
- `heuristic_extraction`: KOSIS 메타로 검증되기 전까지 후보로만 취급합니다.
- `unit_caller_resolution_required`: 계산 결과 단위를 caller가 해석해야 합니다.
- `projection_data`: 미래 추계값입니다. 실측값처럼 단정하지 않습니다.
- `coverage_ratio`가 낮은 결과
- `row_count: 0`
- `availability: "missing"` 또는 `availability: "not_matched"`
- `source_system`이 `KOSIS`인지 `NABO`인지 불명확한 결과
- `dropped_dimensions` / `누락_차원`: 질문에 있는 **차원**(연령·성별·시계열·비교·지역묶음)을
  이 지표로는 나눠 볼 수 없습니다. 기존 다섯 감지기가 잡은 것입니다.
- `dropped_terms` / `누락_한정어`: 지표·지역·시점·대화 표현을 모두 뺐는데도 **남은 말**입니다.
  예: "중소기업 수출액" → `["중소기업"]`, "중국으로 수출액" → `["중국"]`.
  그 값은 **한정어가 적용되지 않은 전체 값**이므로 사용자의 한정어를 붙여 내보내면 안 됩니다.
  `…별` 요청은 `별` 을 뗀 어간으로 옵니다(`업종별` → `업종`).

이 경우 Gemma는 사전 지식이나 추정으로 빈 값을 채우면 안 됩니다. 가능한 후속
조치는 질문 재정의, 표 후보 재검색, 다른 지표 대체 제안입니다.

### 한정어를 버린 값을 내보내지 않습니다 (2026-09-18)

`quick_stat`·`answer_query` 의 빠른 경로는 질문에서 지표·지역·시점·대화 표현을 뺀 **잔여 토큰**이
남으면 값을 내지 않고 `status: "failed"` 로 돌려줍니다. 값 자체는 정확하지만 **사용자가 물은
집단의 값이 아니기** 때문입니다 — "중소기업 수출액" 에 시도별 총수출액을 주면 값은 맞고 답은
틀립니다.

스위치: `KOSIS_MCP_LEFTOVER_GATE` = `off` | `warn` | `enforce` (기본 `enforce`).
`warn` 은 거부하지 않고 로그만 남깁니다. 오탐이 의심되면 `off` 로 즉시 되돌릴 수 있습니다.

덮이는 말은 매칭 키·지표 설명·표 이름·동의어·`encoded_terms`, 지역(전국 동의어
`우리나라·한국·대한민국·국내` 포함), 시점, 대화 잡말입니다. 오프라인으로 재현하려면
`python scripts/audit_leftover_terms.py` (네트워크 없음).

## 일관성 규칙

`router_slots`는 증거 후보일 뿐 명령이 아닙니다. `plan_query`는 후보를
정규화·검증·격리한 뒤 `metrics`, `analysis_tasks`, `conflict_decisions`를
생성합니다.

```json
{
  "metrics": [{"name": "GRDP", "role": "primary"}],
  "quarantined_metrics": [
    {
      "name": "R&D 투자 규모",
      "reason": "router_indicator_conflicts_with_intended"
    }
  ],
  "conflict_decisions": [
    {
      "type": "indicator_quarantine",
      "kept_metric": "GRDP",
      "candidate": "R&D 투자 규모"
    }
  ]
}
```

Gemma는 `metrics`만 후속 조회 대상으로 사용하고, `quarantined_metrics`는 로그나
경고로만 보존합니다.

호환을 위해 아래 legacy 필드는 당분간 유지됩니다. Gemma 기본 manifest에서는
정규 필드를 우선합니다.

- `suggested_workflow` → `evidence_workflow`
- `consistency_warnings` → `conflict_decisions`
- `router_slots_overridden` → `conflict_decisions`, `quarantined_metrics`
- `required_dimensions` → `table_required_dimensions`

## 복합 질문 처리

보고서형·복합 질문은 MCP가 문장을 완성하는 것이 아니라 evidence bundle을 만듭니다.

예:

```text
최근 5년간 소상공인 사업체 수, 종사자 수, 매출액, 폐업률을 한 표로 정리해줘.
경제성장률, 인구 변화율, 합계출산율 추이를 비교해줘.
```

권장 처리:

1. `plan_query`가 `metrics[]`, `time_request`, `analysis_tasks[]`를 추출합니다.
2. 각 metric별로 `select_table_for_query`와 `resolve_concepts`를 수행합니다.
3. `query_table`로 raw rows를 모읍니다.
4. 매칭 실패 metric은 `missing_metrics` 또는 `availability: "not_matched"`로 둡니다.
5. 챗봇 LLM이 HTML, 표, 보고서 문장을 생성합니다.

중요한 원칙: MCP는 KOSIS 메타에서 동적으로 알 수 있는 것만 자동화합니다. 수도권
정의, 산업 그룹 정의, 정책적 해석 같은 도메인 지식은 챗봇 LLM이 담당하고,
MCP는 코드 검증, 원자료 조회, 메타 보존을 담당합니다.

## NABO 통합 규칙

NABO는 KOSIS의 하위 표가 아니라 별도 제공기관입니다. Gemma는 통합 검색 결과를
사용하더라도 출처를 섞어 말하면 안 됩니다.

- `KOSIS_API_KEY`는 KOSIS 도구에, `NABO_API_KEY`는 NABO 도구에 필요합니다.
- `search_stats(source="all")`는 후보 발견용입니다. KOSIS와 NABO 후보가 함께 나오면
  `definition_comparison_required`와 `cross_source_definition_check_required`를 확인하고,
  각 표의 메타데이터와 단위를 따로 확인합니다.
- NABO 조회 흐름은 `search_nabo_tables` → `explore_nabo_table` →
  `query_nabo_table`입니다.
- NABO 주기는 `dtacycle_cd`로 지정합니다. 기본값 `auto`는 표 메타데이터에서 연간
  `YY`, 분기 `QY`, 월간 `MM`을 추론하고 `dtacycle_resolution`에 근거를 남깁니다.
  `dtacycle_cd_suggestions`는 실제 표 주기만 뜻하고, 전체 입력 enum은
  `dtacycle_supported_values`에서 확인합니다.
- NABO 기간은 `period="latest"`, `period="2024"`, `period_range=["2010", "2024"]`,
  `period="2010:2024"`, `period="2010-2024"` 형식을 사용할 수 있습니다. 범위가
  비어 있으면 `period_filter_empty`와 `period_request`를 확인합니다.
- NABO 조회 결과에서 `ITEM.label`이 반복될 수 있으므로 `dimensions.ITEM.full_label`
  또는 `item_full_name`을 우선 사용합니다.
- `truncated_by_max_rows`가 있으면 `latest_period`는 완전한 최신 시점을 뜻하지 않습니다.
  이때는 `latest_period_in_returned_rows`를 “반환된 행 중 최신”으로만 다룹니다.
- 최종 답변에는 “KOSIS 기준”과 “NABO 기준”을 구분해서 씁니다.
