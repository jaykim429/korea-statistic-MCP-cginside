# KOSIS Natural Language MCP Server

KOSIS OpenAPI를 자연어 질의, 통계 조회, 분석, SVG 시각화로 연결하는 MCP 서버입니다.  
챗봇에서는 우선 `answer_query`를 호출하면 됩니다. 정밀 매핑된 Tier A 통계는 바로 API 조회하고, AI/풍력/건설/소상공인 폐업률처럼 후보 선택이 필요한 복합 질의는 KOSIS 검색 후보와 분석 계획을 반환합니다.

## 주요 기능

- 자연어 라우팅: 동의어, 하위어, 상위어, 관점어 기반 검색어 생성
- 직접 조회: 인구, 출산율, 실업률, 자영업자, 중소기업·소상공인 핵심 지표 등 Tier A
- 분석: 추세, 변화율, 상관, 예측, 이상치, 기간 비교
- 시각화: 라인, 막대, 산점도, 히트맵, 분포, 이중축, 대시보드 SVG
- 검증 보조: 답변 payload 검증, 산식형 지표의 분모·필요 통계 안내

## 지원 현황

- Tier A 직접 조회 통계: 34개, 전부 검증됨
- Tier B 자연어 검색 라우팅: 219개
- 동의어/일상어 매핑: 83개
- 주제 브라우징: 16개 주제
- 의도 라벨: 23개
- 기본 라우터: `NaturalLanguageRouter`

대표 질문:

```text
한국 인구 알려줘
최근 5년간 실업률 추이 분석해줘
중소기업 수와 소상공인 사업체 수 비교해줘
AI 관련 통계 찾아줘
풍력발전 설비용량 통계 찾아줘
폐업률 산식과 필요한 통계 알려줘
```

## 0. KOSIS API 키 준비

KOSIS OpenAPI 인증키가 필요합니다. 이 서버는 인증키를 `KOSIS_API_KEY` 환경변수로 받습니다.

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
```

macOS/Linux:

```bash
export KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
```

## 방법 1. 간단 설치 후 짧은 config 사용

korean-law MCP처럼 `command`만 짧게 쓰려면 먼저 실행 명령을 설치합니다.

```powershell
pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"
```

설치 확인:

```powershell
kosis-analysis-mcp
```

위 명령은 MCP stdio 서버를 실행하므로 일반 CLI처럼 결과가 출력되지 않는 것이 정상입니다. 확인 후 `Ctrl+C`로 종료합니다.

Claude Desktop, Cursor, Windsurf 등의 MCP 설정에는 아래처럼 추가합니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "kosis-analysis-mcp",
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

이미 다른 MCP 서버가 있으면 `"mcpServers"` 안에 아래 블록만 추가합니다.

```json
"kosis-analysis": {
  "command": "kosis-analysis-mcp",
  "env": {
    "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
  }
}
```

`kosis-analysis-mcp` 명령을 찾지 못하면 아래 명령으로 설치 위치를 확인한 뒤, 출력된 전체 경로를 `command`에 넣습니다.

```powershell
where kosis-analysis-mcp
```

## 방법 2. Git Clone 후 로컬 경로로 연결

```powershell
git clone https://github.com/jaykim429/korea-statistic-MCP-cginside.git
cd korea-statistic-MCP-cginside
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

서버 파일 경로 확인:

```powershell
Resolve-Path .\kosis_mcp_server.py
```

Claude Desktop 설정 파일 위치:

| OS | 설정 파일 |
| --- | --- |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |

Windows에서 바로 열기:

```powershell
notepad "$env:APPDATA\Claude\claude_desktop_config.json"
```

설정 파일에 추가:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "C:\\path\\to\\korea-statistic-MCP-cginside\\.venv\\Scripts\\python.exe",
      "args": [
        "C:\\path\\to\\korea-statistic-MCP-cginside\\kosis_mcp_server.py"
      ],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

저장 후 Claude Desktop을 완전히 종료했다가 다시 실행합니다.

## 방법 3. 이미 MCP 설정이 있는 경우

기존 설정에 다른 MCP 서버가 있다면 `"mcpServers"` 안에 아래 블록만 추가합니다. 앞 항목 뒤에 쉼표가 필요합니다.

```json
"kosis-analysis": {
  "command": "C:\\path\\to\\korea-statistic-MCP-cginside\\.venv\\Scripts\\python.exe",
  "args": [
    "C:\\path\\to\\korea-statistic-MCP-cginside\\kosis_mcp_server.py"
  ],
  "env": {
    "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
  }
}
```

가상환경을 쓰지 않을 경우 `command`를 `"python"`으로 바꿔도 됩니다. 다만 의존성이 설치된 Python이어야 합니다.

## 방법 4. Cursor / Windsurf에서 사용

Cursor와 Windsurf도 같은 MCP JSON 구조를 사용합니다.

| 앱 | 설정 파일 |
| --- | --- |
| Cursor | 프로젝트 폴더의 `.cursor/mcp.json` |
| Windsurf | 프로젝트 폴더의 `.windsurf/mcp.json` |

예시:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "python",
      "args": [
        "/path/to/korea-statistic-MCP-cginside/kosis_mcp_server.py"
      ],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

Windows라면 경로를 `C:\\path\\to\\...\\kosis_mcp_server.py` 형태로 씁니다.

## 방법 5. Claude Code에서 로컬 MCP로 사용

현재 repo는 Claude Code 플러그인 marketplace 패키지가 아니라 **로컬 stdio MCP 서버**입니다. `pip install` 후에는 아래처럼 짧게 등록할 수 있습니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "kosis-analysis-mcp",
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

`/plugin marketplace add ...` 형태의 한 줄 설치를 지원하려면 별도의 Claude Code 플러그인 패키징이 추가로 필요합니다.

## 방법 6. 터미널에서 서버 직접 실행

MCP 클라이언트 연결 전 서버가 import 가능한지 확인할 때 사용합니다.

```powershell
kosis-analysis-mcp
```

이 명령은 MCP stdio 서버를 실행하므로 터미널에 일반 CLI처럼 결과가 출력되지 않는 것이 정상입니다. 종료는 `Ctrl+C`입니다.

문법/import 확인:

```powershell
python -m py_compile kosis_mcp_server.py kosis_curation.py kosis_charts_extra.py
```

## 방법 7. 원격 URL 커넥터 방식

Claude.ai 웹의 커스텀 커넥터나 `mcp-remote`로 쓰려면 원격 HTTP MCP 서버를 배포합니다. 이 repo는 `/mcp` Streamable HTTP 엔드포인트를 제공하는 `kosis_http_server.py`를 포함합니다.

Render 배포:

1. Render에서 이 GitHub repo를 Web Service로 연결합니다.
2. `render.yaml` Blueprint를 사용합니다.
3. 환경변수 `KOSIS_API_KEY`를 Render 대시보드에 Secret으로 추가합니다.
4. 배포 후 URL은 `https://<your-service>.onrender.com/mcp` 형태입니다.

Docker/Fly.io 배포:

```bash
docker build -t kosis-analysis-mcp .
docker run -p 8000:8000 -e KOSIS_API_KEY=YOUR_KOSIS_API_KEY kosis-analysis-mcp
```

로컬 HTTP 실행:

```powershell
kosis-analysis-mcp-http
```

Claude Desktop에서 `mcp-remote`로 연결:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://your-kosis-mcp.example.com/mcp"
      ]
    }
  }
}
```

HTTP URL을 직접 받는 클라이언트라면:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "url": "https://your-kosis-mcp.example.com/mcp?apiKey=YOUR_KOSIS_API_KEY"
    }
  }
}
```

원격 서버 방식에서는 서버 환경변수에 `KOSIS_API_KEY`를 설정하는 것이 가장 단순합니다.

## 방법 8. npx 한 줄 실행 방식

GitHub repo를 npm 패키지처럼 직접 실행할 수 있습니다. 이 방식은 설치 중 Python 의존성 설치를 시도합니다.

```powershell
npx -y github:jaykim429/korea-statistic-MCP-cginside
```

MCP 설정:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "npx",
      "args": [
        "-y",
        "github:jaykim429/korea-statistic-MCP-cginside"
      ],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

npm에 정식 배포하면 아래처럼 더 짧게 바꿀 수 있습니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "npx",
      "args": [
        "-y",
        "kosis-analysis-mcp"
      ],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY"
      }
    }
  }
}
```

Python 실행 파일을 직접 지정해야 하면 `KOSIS_PYTHON` 환경변수를 추가합니다.

## 방법 9. Claude Code 플러그인

이 repo는 Claude Code plugin 구조도 포함합니다.

```text
/plugin marketplace add jaykim429/korea-statistic-MCP-cginside
/plugin install kosis-analysis@kosis-analysis-marketplace
```

플러그인 MCP 서버는 repo의 `.mcp.json`을 통해 시작됩니다. 사용 전 터미널 환경 또는 Claude Code 실행 환경에 `KOSIS_API_KEY`가 설정되어 있어야 합니다.

```bash
export KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
```

Windows PowerShell:

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
```

## 지원 방식 요약

| 방식 | 현재 상태 | 비고 |
| --- | --- | --- |
| `kosis-analysis-mcp` command | 지원 | `pip install git+...` 후 사용 |
| Git clone + Python stdio | 지원 | 현재 기본 방식 |
| Claude Desktop config | 지원 | `command` + `args` + `env` |
| Cursor / Windsurf config | 지원 | 같은 MCP JSON 구조 |
| Claude Code 로컬 MCP | 지원 가능 | 로컬 MCP 설정 방식 사용 |
| Claude Code plugin marketplace | 지원 | `.claude-plugin`, `.mcp.json` 포함 |
| Claude.ai 웹 URL 커넥터 | 배포 후 지원 | `kosis_http_server.py`, Render/Docker 파일 포함 |
| npx 실행 | 지원 | GitHub package spec 사용, npm 정식 배포 전 |

## 챗봇에서 권장 호출 순서

1. 일반 자연어 질문은 `answer_query`
2. 답변 payload 점검은 `verify_stat_claims`
3. 산식·분모가 필요한 질문은 `indicator_dependency_map`
4. 직접 통계 조회는 `quick_stat`, 시계열은 `quick_trend`
5. 시도별·지역별 비교는 `quick_region_compare`
6. 복합 분석은 `analyze_trend`, `stat_time_compare`, `correlate_stats`, `forecast_stat`, `detect_outliers`
7. 차트는 `chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_heatmap`, `chart_distribution`, `chart_dual_axis`, `chart_dashboard`

## 예시

```text
answer_query("최근 기준 중소기업 수와 소상공인 사업체 수를 함께 보여줘")
answer_query("중소기업 사업체수를 시도별로 비교해줘")
answer_query("2020년 서울 중소기업 매출액 알려줘")
answer_query("서울 집값 전월 대비 변화율 알려줘")
answer_query("2019년 대비 2023년 중소기업 매출액 증가율")
answer_query("중소기업 사업체수가 가장 많은 5곳 알려줘")
answer_query("서울 중소기업 매출액이 전국에서 차지하는 비중")
answer_query("서울과 경기 중소기업 사업체수 합계")
answer_query("AI 관련 통계 찾아줘")
answer_query("최근 5년간 실업률 추이 분석해줘")
quick_region_compare("중소기업 사업체수")
quick_stat("주택매매가격지수", region="서울", period="2026.03")
stat_time_compare("실업률", years=5)
indicator_dependency_map("폐업률")
chart_line("고령인구", region="전국", years=5)
```

`answer_query` 응답 상태:

- `EXECUTED`: 실제 KOSIS API 조회 또는 계산 완료
- `NEEDS_TABLE_SELECTION`: 상위어·복합 질문이라 후보 통계표 선택 필요
- `STAT_NOT_FOUND`: 정밀 매핑 또는 API 조회 실패
- `PERIOD_NOT_FOUND`: 요청한 비교 시점을 찾지 못함
- `DENOMINATOR_REQUIRED`: 비중·비율 등에서 분모 확정 필요

`answer_query` 응답 유형 (`답변유형`):

- `tier_a_value`, `tier_a_trend`, `tier_a_growth_rate`: 단일값·시계열·증가율
- `tier_a_region_comparison`: 17개 시도 비교
- `tier_a_top_n`: "가장 많은 N곳", "상위 N개", "5위까지" 등 순위형 응답
- `tier_a_share_ratio`: 지역값 / 전국값 × 100 비중 계산
- `tier_a_region_sum`: "X와 Y 합계" 다지역 합산
- `tier_a_composite_share_ratio`: 수도권·영남권 등 합성 지역의 합산값 대비 전국 비중
- `tier_a_composite`, `tier_a_composite_calculation`, `tier_a_composite_comparison`: 정밀 매핑된 복합 산식
- `search_and_plan`: 후보 통계표 선택 필요 (Tier B 폴백)

`EXECUTED` 응답은 모두 다음 필드를 노출합니다:

- `used_period`: 실제 사용된 KOSIS 시점 (예: `"2023"`, `"202603"`)
- `period_age_years`: 현재 시점 대비 경과 연수 (실수)
- `검증_주의`: 1년 이상 경과한 데이터, 의도와 응답 유형 불일치 (RANKING/SHARE_RATIO/GROWTH_RATE/TIME_SERIES/AVERAGE), 명시 연도 미준수, 다지역 의도 누락, "기업 수 ↔ 사업체 수" 모집단 silent 매핑 등에 자동 경고 추가

`quick_stat`·`quick_trend`·`quick_region_compare` 직접 호출도 0.4.0부터 동일하게 `used_period`/`period_age_years`/`⚠️ 데이터_신선도` 필드를 노출합니다. answer_query와 직접 호출의 메타 풍부도 비대칭을 해소했습니다.

지원하지 않는 파라미터(`industry`, `scale`, `aggregation`, `group_by` 등 임의의 키)는 `⚠️ 무시된_파라미터` 필드에 노출되어 silent drop을 차단합니다.

`period` 파라미터는 다음을 인식합니다:

- 절대: `"2023"`, `"2023.04"`, `"2023년 4월"`, `"2025Q1"`, `"2025년 1분기"`
- 상대: `"작년"`/`"지난해"`/`"전년"` → 현재년-1, `"올해"` → 현재년, `"재작년"` → 현재년-2

요청한 분기·월 정밀도가 통계표의 작성 주기보다 세분화돼 있으면 응답에 `⚠️ 정밀도_다운그레이드`가 자동 첨부됩니다.

`search_kosis` 응답은 `Tier_A_직접_매핑` 필드를 통해 같은 키워드에 검증된 Tier A 통계표가 있는지 표면화합니다 — KOSIS 검색 인덱스가 약하게 매칭된 통계표를 상위에 올리는 경우에도 정확한 매핑을 놓치지 않습니다.

`STAT_CORRELATION`·`STAT_OUTLIER_DETECTION`·`STAT_FORECAST` 의도가 감지되면 `answer_query`는 `search_and_plan` 응답의 `추천_도구_호출` 필드에 `correlate_stats`/`detect_outliers`/`forecast_stat`의 호출 syntax를 명시합니다. 두 Tier-A 지표가 명확히 추출되는 high-confidence 케이스(예: "실업률과 고용률 상관관계")는 0.5.0부터 `correlate_stats`로 자동 위임돼 `tier_a_auto_correlation`을 반환합니다.

`생존율`·`폐업률`·`창업률` 같은 시간-코호트 기반 동태 지표 질의("음식점업 5년 살아남는 비율" 등)는 정태 비중(`tier_a_share_ratio`)으로 잘못 매핑되지 않고 `dynamic_ratio_advisory`로 분기되어 `indicator_dependency_map`의 산식 사양과 KOSIS 통계표 후보를 같이 반환합니다.

응답 텍스트의 KOSIS 표준 단위(`천명`, `억원`, `십억원`, `천달러`)는 자동으로 사람이 읽기 좋은 형식이 병기됩니다 — 예: `5,688.7 천명 (약 569만 명)`, `33,012,545 억원 (약 3,301.25조원)`.

`period` 표현은 0.5.0부터 다음을 추가로 인식합니다:

- `올해 1분기` / `작년 4분기` / `이번 분기` / `지난 분기`
- `올해 4월` / `지난달` / `이번달`
- `상반기` / `하반기` — KOSIS 표준 주기에 없으므로 `⚠️ 상하반기` 안내 노출

`search_and_plan` 응답은 슬롯에서 추출된 `industry`·`scale`·`target`을 검색어에 자동 보강합니다(`검색어_슬롯보강` 필드). 사용자가 "제조업 중소기업 비중"이라 물으면 검색 키워드에 "제조업"이 자동 포함돼 산업 특화 통계표가 상위에 오릅니다.

다지역 합산·합성지역 핸들러(`_answer_composite_aggregate`, `_answer_region_sum`)는 0.5.0부터 모든 component를 **병렬 호출**하고 per-call 12초·전체 60초 예산을 적용합니다 — 단일 호출이 지연돼도 다른 in-flight 요청이 막히지 않습니다.

차트 도구(`chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_heatmap`, `chart_distribution`, `chart_dual_axis`, `chart_dashboard`, `chain_full_analysis`)는 SVG를 fenced ``` ```svg ``` ``` 블록에 담은 `TextContent`로 반환합니다 — MCP 표준이 `image/svg+xml` ImageContent를 받지 않아 발생하던 콘텐츠 포맷 오류를 회피.

`answer` 자연어 텍스트는 다음 후처리를 거칩니다:

- `X은(는)` 플레이스홀더 → 한글 받침에 따라 `은` 또는 `는` 선택
- `YYYY.MM` 월별 시점 raw 표기 → `YYYY년 M월` (1900~2099 연도 범위만 변환)

지역명은 영문·행정 정식 명칭·단축형 모두 17개 시도 중 하나로 정규화됩니다:

- `Seoul`, `서울특별시`, `서울시`, `seoul` → `서울`
- `경기도`, `Gyeonggi`, `gyeonggi-do` → `경기`
- `대한민국`, `한국`, `korea` → `전국`

합성 지역(`수도권`, `비수도권`, `영남권`, `호남권`, `충청권`)은 구성 17개 시도로 자동 전개됩니다:

- `수도권 사업체수` → 서울 + 경기 + 인천 합산 (`tier_a_region_sum`)
- `수도권 사업체수 비중` → (서울+경기+인천) / 전국 × 100 (`tier_a_composite_share_ratio`)

## 통계 해석 주의

- 기업 수, 사업체 수, 자영업자 수는 서로 다른 모집단입니다.
- 비중, 폐업률, 창업률, 생존율은 분모와 작성기관 산식을 먼저 확인해야 합니다.
- 상관·회귀·정책효과 분석은 인과관계를 자동으로 의미하지 않습니다.
- “최신” 질문은 KOSIS 통계표의 최신 수록 시점을 기준으로 답합니다.
- “2020년”, “2026년 3월”, “전년 대비”, “전월 대비”처럼 기간이 명시된 질문은 해당 기간 또는 기간 비교로 처리하며, 데이터가 없으면 최신값으로 대체하지 않습니다.
- Tier A에 없는 질문은 단일값을 임의로 답하지 않고 검색 후보와 분석 계획을 반환합니다.

## 검증 스크립트

라이브 KOSIS API 회귀 검증:

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
python scripts\regression_smoke.py
python scripts\comprehensive_api_matrix.py
python scripts\temporal_edge_cases.py
python scripts\natural_language_battery.py --summary-only
```

`natural_language_battery.py`는 `answer_query`만 호출하는 자연어 배터리로, 10개 이상의 의도 카테고리(단일값·시계열·증가율·시도별·Top N·비중·합산·복합·검색폴백·가드레일·의도불일치)를 한 번에 검증합니다. `--group <name>` 또는 `--name <case>`로 필터링 가능합니다.

## 파일 구성

- `kosis_mcp_server.py`: MCP 서버와 도구 정의
- `kosis_http_server.py`: Streamable HTTP MCP 서버 엔트리포인트
- `kosis_curation.py`: 자연어 라우터, Tier A/B 큐레이션, 개념 그래프
- `kosis_charts_extra.py`: 추가 SVG 차트 헬퍼
- `scripts/regression_smoke.py`, `scripts/comprehensive_api_matrix.py`, `scripts/temporal_edge_cases.py`, `scripts/natural_language_battery.py`: 라이브 API 회귀 검증 스크립트
- `pyproject.toml`: `kosis-analysis-mcp` 실행 명령과 패키지 메타데이터
- `package.json`, `bin/`, `scripts/`: npx/npm wrapper
- `.claude-plugin/`, `.mcp.json`, `skills/`: Claude Code plugin 구성
- `render.yaml`, `Dockerfile`, `fly.toml.example`: 원격 배포 예시
- `requirements.txt`: 실행 의존성
- `mcp_config.example.json`: MCP 클라이언트 설정 예시
- `.env.example`: 환경변수 예시
