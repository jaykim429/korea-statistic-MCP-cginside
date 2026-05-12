# KOSIS Natural Language MCP Server

KOSIS OpenAPI를 자연어 질의, 통계 조회, 분석, SVG 시각화로 연결하는 MCP 서버입니다.  
챗봇에서는 우선 `answer_query`를 호출하면 됩니다. 정밀 매핑된 Tier A 통계는 바로 API 조회하고, AI/풍력/건설/소상공인 폐업률처럼 후보 선택이 필요한 복합 질의는 KOSIS 검색 후보와 분석 계획을 반환합니다.

## 주요 기능

- 자연어 라우팅: 동의어, 하위어, 상위어, 관점어 기반 검색어 생성
- 직접 조회: 인구, 출산율, 실업률, 자영업자, 중소기업·소상공인 핵심 지표 등 Tier A
- 분석: 추세, 변화율, 상관, 예측, 이상치, 기간 비교
- 시각화: 라인, 막대, 산점도, 히트맵, 분포, 이중축, 대시보드 SVG
- 검증 보조: 답변 payload 검증, 산식형 지표의 분모·필요 통계 안내

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

Claude.ai 웹의 커스텀 커넥터처럼 아래 형태로 쓰려면 원격 HTTP MCP 서버가 별도로 배포되어 있어야 합니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "url": "https://your-kosis-mcp.example.com/mcp?apiKey=YOUR_KOSIS_API_KEY"
    }
  }
}
```

현재 repo는 로컬 stdio 서버라서 위 URL 방식은 아직 지원하지 않습니다. Render, Fly.io, Railway 등에 HTTP MCP 래퍼를 배포하면 지원할 수 있습니다.

## 방법 8. npx 한 줄 실행 방식

아래 형태도 가능하게 만들 수 있지만, 현재는 npm 패키지가 아직 배포되어 있지 않습니다.

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

이 방식을 지원하려면 npm wrapper 패키지를 추가로 만들어 배포해야 합니다.

## 지원 방식 요약

| 방식 | 현재 상태 | 비고 |
| --- | --- | --- |
| `kosis-analysis-mcp` command | 지원 | `pip install git+...` 후 사용 |
| Git clone + Python stdio | 지원 | 현재 기본 방식 |
| Claude Desktop config | 지원 | `command` + `args` + `env` |
| Cursor / Windsurf config | 지원 | 같은 MCP JSON 구조 |
| Claude Code 로컬 MCP | 지원 가능 | 로컬 MCP 설정 방식 사용 |
| Claude Code plugin marketplace | 추가 작업 필요 | 플러그인 패키징 필요 |
| Claude.ai 웹 URL 커넥터 | 추가 작업 필요 | 원격 HTTP MCP 서버 필요 |
| npx 실행 | 추가 작업 필요 | npm wrapper 배포 필요 |

## 챗봇에서 권장 호출 순서

1. 일반 자연어 질문은 `answer_query`
2. 답변 payload 점검은 `verify_stat_claims`
3. 산식·분모가 필요한 질문은 `indicator_dependency_map`
4. 직접 통계 조회는 `quick_stat`, 시계열은 `quick_trend`
5. 복합 분석은 `analyze_trend`, `stat_time_compare`, `correlate_stats`, `forecast_stat`, `detect_outliers`
6. 차트는 `chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_heatmap`, `chart_distribution`, `chart_dual_axis`, `chart_dashboard`

## 예시

```text
answer_query("최근 기준 중소기업 수와 소상공인 사업체 수를 함께 보여줘")
answer_query("AI 관련 통계 찾아줘")
answer_query("최근 5년간 실업률 추이 분석해줘")
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

## 통계 해석 주의

- 기업 수, 사업체 수, 자영업자 수는 서로 다른 모집단입니다.
- 비중, 폐업률, 창업률, 생존율은 분모와 작성기관 산식을 먼저 확인해야 합니다.
- 상관·회귀·정책효과 분석은 인과관계를 자동으로 의미하지 않습니다.
- “최신” 질문은 KOSIS 통계표의 최신 수록 시점을 기준으로 답합니다.
- Tier A에 없는 질문은 단일값을 임의로 답하지 않고 검색 후보와 분석 계획을 반환합니다.

## 파일 구성

- `kosis_mcp_server.py`: MCP 서버와 도구 정의
- `kosis_curation.py`: 자연어 라우터, Tier A/B 큐레이션, 개념 그래프
- `kosis_charts_extra.py`: 추가 SVG 차트 헬퍼
- `pyproject.toml`: `kosis-analysis-mcp` 실행 명령과 패키지 메타데이터
- `requirements.txt`: 실행 의존성
- `mcp_config.example.json`: MCP 클라이언트 설정 예시
- `.env.example`: 환경변수 예시
