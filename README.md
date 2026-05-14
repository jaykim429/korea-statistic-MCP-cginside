# KOSIS/NABO Natural Language MCP Server

KOSIS 국가통계와 NABOSTATS(국회예산정책처 재정경제통계시스템) 데이터를 챗봇이 안전하게 찾아보고 사용할 수 있게 해주는 MCP 서버입니다.

사용자는 “서울 인구 알려줘”, “경제성장률이랑 출산율 추이 비교해줘”, “NABO 재정수지 통계 찾아줘”처럼 자연어로 묻고, 챗봇은 이 서버를 통해 통계표를 찾고, 필요한 코드와 기간을 확인하고, 실제 원자료를 조회합니다.

이 프로젝트의 핵심 목표는 **그럴듯하지만 틀린 통계 답변을 줄이는 것**입니다. MCP 서버가 최종 답을 지어내지 않고, KOSIS/NABO에서 확인 가능한 원자료와 경고 신호를 챗봇에게 넘깁니다. 최종 문장 작성은 Gemma 같은 LLM이 맡되, 값·단위·기간·분모·추계 여부·제공기관 같은 근거를 보고 답하도록 설계했습니다.

## 처음 보는 분을 위한 요약

- **무엇을 하나요?**
  KOSIS와 NABO 통계표를 검색하고, 통계표 안의 지역·성별·연령·항목 코드를 찾아, 실제 통계 원자료를 가져옵니다.

- **왜 필요한가요?**
  통계 질문은 “어떤 표를 골랐는지”, “기간이 맞는지”, “단위가 무엇인지”, “분모가 무엇인지”가 중요합니다. 이 서버는 그런 확인 과정을 응답에 드러내서 챗봇이 조용히 틀린 답을 하지 않게 돕습니다.

- **누가 쓰면 좋나요?**
  Gemma 같은 로컬 LLM 챗봇, Claude Desktop/Cursor/Windsurf MCP 클라이언트, 공공 통계를 자동 조회하는 분석 도구를 만드는 사람에게 적합합니다.

- **가장 권장하는 사용 방식은요?**
  `plan_query`로 먼저 질문을 분석한 뒤, `select_table_for_query` → `resolve_concepts` → `query_table` → 필요 시 `compute_indicator` 순서로 진행합니다.

## 주요 기능

- 자연어 질문을 통계 조회 절차로 나눕니다.
  예: “서울 1인당 GRDP 알려줘” → 지표, 지역, 필요한 분모, 후속 도구 호출 순서로 분해합니다.

- 여러 지표가 섞인 질문도 보존합니다.
  예: “경제성장률, 인구 변화율, 합계출산율 추이” → 세 지표를 각각 `metrics[]`에 남겨 따로 표를 찾게 합니다.

- KOSIS 메타데이터로 먼저 검증합니다.
  통계표에 실제로 지역축, 연령축, 항목 코드가 있는지 확인한 뒤 값을 조회합니다.

- 원자료를 그대로 반환합니다.
  `query_table`은 합산·평균·해석을 몰래 하지 않고 KOSIS 원행을 기간, 값, 단위, 분류 정보와 함께 반환합니다.

- 계산이 필요한 경우에도 한계를 표시합니다.
  1인당 값, 비중, 변화율 같은 계산은 `compute_indicator`가 수행하되, 단위 변환이나 가법성 판단이 필요한 경우 `mcp_output_contract` marker로 caller 책임을 명시합니다.

- 실패와 모호성을 숨기지 않습니다.
  데이터 없음, 기간 불일치, 잘못된 코드, 추계 데이터, 단위 해석 필요 같은 상태를 응답 표면에 드러냅니다.

## 지원 현황

- Tier A 직접 조회 통계: 189개, 전부 검증됨
- Tier B 자연어 검색 라우팅: 219개
- 동의어/일상어 매핑: 90개
- 주제 브라우징: 16개 주제
- 의도 라벨: 23개
- 기본 라우터: `NaturalLanguageRouter`

대표 질문:

```text
한국 인구 알려줘
최근 5년간 실업률 추이 분석해줘
중소기업 수와 소상공인 사업체 수 비교해줘
경제성장률, 인구 변화율, 합계출산율 추이 비교해줘
AI 관련 통계 찾아줘
풍력발전 설비용량 통계 찾아줘
폐업률 산식과 필요한 통계 알려줘
```

## 쉬운 용어 풀이

- **KOSIS**: 통계청 국가통계포털입니다. 여러 기관의 공식 통계표가 모여 있습니다.
- **NABO / NABOSTATS**: 국회예산정책처 재정경제통계시스템입니다. 재정·경제 관련 통계표와 용어사전을 제공합니다.
- **MCP**: 챗봇이 외부 도구를 호출할 수 있게 해주는 연결 방식입니다.
- **통계표**: KOSIS 안의 데이터 표입니다. 같은 “인구”라도 주민등록인구, 추계인구, 총조사 인구처럼 여러 표가 있을 수 있습니다.
- **코드 매핑**: “서울”, “여성”, “30대” 같은 말을 KOSIS 표 안의 실제 코드로 바꾸는 과정입니다.
- **raw rows / 원자료**: MCP가 해석하기 전의 KOSIS 조회 결과입니다.
- **marker**: “단위 해석 필요”, “추계 데이터”, “데이터 없음” 같은 주의 신호입니다.

## 0. API 키 준비

KOSIS 조회에는 KOSIS OpenAPI 인증키가 필요합니다. 이 서버는 인증키를 `KOSIS_API_KEY` 환경변수로 받습니다.

NABOSTATS(국회예산정책처 재정경제통계시스템)까지 함께 쓰려면 `NABO_API_KEY`도 설정합니다. NABO 도구를 쓰지 않을 때는 생략해도 됩니다.

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
$env:NABO_API_KEY="YOUR_NABO_API_KEY"
```

macOS/Linux:

```bash
export KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
export NABO_API_KEY="YOUR_NABO_API_KEY"
```

## MCP 연동 전에 결정할 것

MCP 연결 방식은 크게 두 가지입니다.

| 방식 | 추천 대상 | 장점 | 필요한 것 |
| --- | --- | --- | --- |
| 로컬 stdio MCP | Claude Desktop, Cursor, Windsurf, Claude Code를 내 PC에서 쓰는 경우 | 설정이 단순하고 API 키가 내 PC 밖으로 나가지 않음 | Python, `KOSIS_API_KEY`, 선택적 `NABO_API_KEY` |
| 원격 HTTP MCP | Claude.ai 웹 커넥터나 여러 사람이 같은 서버를 쓰는 경우 | URL 하나로 연결 가능 | Render/Docker 같은 배포 환경, 서버용 `KOSIS_API_KEY`, 선택적 `NABO_API_KEY`, 선택적 접속 토큰 |

처음 써본다면 **방법 1. 간단 설치 후 짧은 config 사용**을 권장합니다. 이미 Git clone으로 개발 중이라면 **방법 2. Git Clone 후 로컬 경로로 연결**을 쓰면 됩니다.

MCP 설정은 대부분 아래 구조를 가집니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "서버를 실행할 명령",
      "args": ["필요하면", "인자"],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
      }
    }
  }
}
```

중요한 점:

- `command`는 MCP 서버를 실행하는 명령입니다.
- `args`는 실행 파일 뒤에 붙는 인자입니다. `kosis-analysis-mcp`처럼 설치된 명령을 쓰면 보통 필요 없습니다.
- `env`에는 KOSIS API 키를 넣습니다. NABO 통계도 쓸 계획이면 `NABO_API_KEY`도 함께 넣습니다. API 키를 질문창에 직접 쓰지 마세요.
- JSON 파일에 기존 MCP 서버가 있다면 `"mcpServers"` 안에 `kosis-analysis` 블록만 추가합니다.

## 방법 1. 간단 설치 후 짧은 config 사용

korean-law MCP처럼 `command`만 짧게 쓰려면 먼저 실행 명령을 설치합니다.

이 명령은 **Claude나 Cursor의 채팅창에 입력하는 것이 아니라, 내 컴퓨터의 터미널에서 실행**합니다.

터미널 여는 방법:

| OS | 어디서 실행하나요? |
| --- | --- |
| Windows | 시작 메뉴에서 `PowerShell` 또는 `Windows Terminal`을 열고 실행 |
| macOS | `응용 프로그램` → `유틸리티` → `터미널`을 열고 실행 |
| Linux | 사용하는 배포판의 Terminal 앱을 열고 실행 |

실행 위치는 아무 폴더여도 괜찮습니다. 이 방식은 GitHub에서 패키지를 받아 Python 환경에 설치하는 명령이라, 특정 프로젝트 폴더 안으로 들어갈 필요가 없습니다.

Windows PowerShell 예시:

```powershell
pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"
```

macOS/Linux 터미널 예시:

```bash
pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"
```

`pip` 명령을 찾지 못하면 아래처럼 Python을 통해 실행합니다.

Windows:

```powershell
python -m pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"
```

macOS/Linux:

```bash
python3 -m pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"
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
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
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
    "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
    "NABO_API_KEY": "YOUR_NABO_API_KEY"
  }
}
```

`kosis-analysis-mcp` 명령을 찾지 못하면 아래 명령으로 설치 위치를 확인한 뒤, 출력된 전체 경로를 `command`에 넣습니다.

```powershell
where kosis-analysis-mcp
```

macOS/Linux에서는:

```bash
which kosis-analysis-mcp
```

## 방법 2. Git Clone 후 로컬 경로로 연결

이 방식은 코드를 내 컴퓨터에 내려받아 직접 연결하는 방법입니다. 먼저 터미널에서 작업할 폴더로 이동합니다.

예를 들어 Windows에서 문서 폴더 아래에 설치하려면:

```powershell
cd "$HOME\Documents"
```

macOS/Linux에서 홈 폴더 아래에 설치하려면:

```bash
cd ~
```

그 다음 아래 명령을 실행합니다.

Windows PowerShell:

```powershell
git clone https://github.com/jaykim429/korea-statistic-MCP-cginside.git
cd korea-statistic-MCP-cginside
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

macOS/Linux:

```bash
git clone https://github.com/jaykim429/korea-statistic-MCP-cginside.git
cd korea-statistic-MCP-cginside
python3 -m venv .venv
source .venv/bin/activate
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

macOS에서 바로 열기:

```bash
open -a TextEdit "$HOME/Library/Application Support/Claude/claude_desktop_config.json"
```

파일이 없으면 새로 만들어도 됩니다. 설정 파일에 아래처럼 추가합니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "C:\\path\\to\\korea-statistic-MCP-cginside\\.venv\\Scripts\\python.exe",
      "args": [
        "C:\\path\\to\\korea-statistic-MCP-cginside\\kosis_mcp_server.py"
      ],
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
      }
    }
  }
}
```

저장 후 Claude Desktop을 완전히 종료했다가 다시 실행합니다.

Claude Desktop에서 확인하는 방법:

1. Claude Desktop을 재시작합니다.
2. 새 대화를 엽니다.
3. 도구/MCP 목록에 `kosis-analysis`가 보이는지 확인합니다.
4. “한국 인구 알려줘. KOSIS MCP로 확인해줘.”처럼 물어봅니다.
5. 연결이 정상이라면 Claude가 `plan_query` 같은 도구를 호출할 수 있습니다.

## 방법 3. 이미 MCP 설정이 있는 경우

기존 설정에 다른 MCP 서버가 있다면 `"mcpServers"` 안에 아래 블록만 추가합니다. 앞 항목 뒤에 쉼표가 필요합니다.

```json
"kosis-analysis": {
  "command": "C:\\path\\to\\korea-statistic-MCP-cginside\\.venv\\Scripts\\python.exe",
  "args": [
    "C:\\path\\to\\korea-statistic-MCP-cginside\\kosis_mcp_server.py"
  ],
  "env": {
    "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
    "NABO_API_KEY": "YOUR_NABO_API_KEY"
  }
}
```

가상환경을 쓰지 않을 경우 `command`를 `"python"`으로 바꿔도 됩니다. 다만 의존성이 설치된 Python이어야 합니다.

쉼표 위치가 가장 흔한 실수입니다. 예를 들어 기존 서버가 하나 있다면 이런 형태가 되어야 합니다.

```json
{
  "mcpServers": {
    "other-server": {
      "command": "other-command"
    },
    "kosis-analysis": {
      "command": "kosis-analysis-mcp",
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
      }
    }
  }
}
```

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
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
      }
    }
  }
}
```

Windows라면 경로를 `C:\\path\\to\\...\\kosis_mcp_server.py` 형태로 씁니다.

프로젝트마다 MCP 설정을 따로 두면, 해당 프로젝트를 열었을 때만 KOSIS 도구가 노출됩니다. 모든 프로젝트에서 쓰고 싶다면 각 앱이 지원하는 전역 MCP 설정 위치를 사용하세요.

## 방법 5. Claude Code에서 로컬 MCP로 사용

현재 repo는 Claude Code 플러그인 marketplace 패키지가 아니라 **로컬 stdio MCP 서버**입니다. `pip install` 후에는 아래처럼 짧게 등록할 수 있습니다.

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "kosis-analysis-mcp",
      "env": {
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
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

직접 실행했을 때 아무 출력 없이 기다리는 것은 정상입니다. MCP stdio 서버는 사용자가 터미널에서 명령을 입력하는 CLI가 아니라, Claude/Cursor 같은 MCP 클라이언트가 표준입출력으로 말을 거는 서버입니다.

## 방법 7. 원격 URL 커넥터 방식

Claude.ai 웹의 커스텀 커넥터나 `mcp-remote`로 쓰려면 원격 HTTP MCP 서버를 배포합니다. 이 repo는 `/mcp` Streamable HTTP 엔드포인트를 제공하는 `kosis_http_server.py`를 포함합니다.

Render 배포:

1. Render에서 이 GitHub repo를 Web Service로 연결합니다.
2. `render.yaml` Blueprint를 사용합니다.
3. 환경변수 `KOSIS_API_KEY`를 Render 대시보드에 Secret으로 추가합니다. NABO 도구도 쓸 경우 `NABO_API_KEY`도 Secret으로 추가합니다.
4. 공개 URL로 노출할 경우 `KOSIS_MCP_AUTH_TOKEN`도 Secret으로 추가해 `/mcp` 호출을 보호합니다.
5. `render.yaml`은 메타 캐시와 fan-out 안전장치(`KOSIS_MCP_META_CACHE_TTL`, `KOSIS_MCP_QUERY_TABLE_MAX_FANOUT`, `KOSIS_MCP_QUERY_TABLE_CONCURRENCY`, `KOSIS_MCP_QUERY_TABLE_CALL_TIMEOUT`)를 함께 설정합니다.
6. 배포 후 URL은 `https://<your-service>.onrender.com/mcp` 형태입니다.

Docker/Fly.io 배포:

```bash
docker build -t kosis-analysis-mcp .
docker run -p 8000:8000 \
  -e KOSIS_API_KEY=YOUR_KOSIS_API_KEY \
  -e NABO_API_KEY=YOUR_NABO_API_KEY \
  -e KOSIS_MCP_AUTH_TOKEN=YOUR_CONNECTOR_TOKEN \
  kosis-analysis-mcp
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
      "url": "https://your-kosis-mcp.example.com/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_CONNECTOR_TOKEN"
      }
    }
  }
}
```

원격 서버 방식에서는 KOSIS/NABO 인증키를 URL에 넣지 말고 서버 환경변수 `KOSIS_API_KEY`, `NABO_API_KEY`로만 설정하세요. `KOSIS_MCP_AUTH_TOKEN`이 설정된 서버는 `Authorization: Bearer ...` 또는 `x-kosis-mcp-token` 헤더가 있어야 `/mcp` 요청을 받습니다.

원격 연결 확인:

- 브라우저에서 `/mcp`를 직접 열었을 때 사람이 읽는 페이지가 나오지 않아도 이상하지 않습니다. MCP 엔드포인트는 일반 웹페이지가 아닙니다.
- Render 무료 플랜은 잠들어 있을 수 있어 첫 요청이 느릴 수 있습니다.
- 서버 로그에서 `uvicorn`이 정상 시작됐는지, 환경변수 `KOSIS_API_KEY`와 필요한 경우 `NABO_API_KEY`가 들어갔는지 확인하세요.

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
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
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
        "KOSIS_API_KEY": "YOUR_KOSIS_API_KEY",
        "NABO_API_KEY": "YOUR_NABO_API_KEY"
      }
    }
  }
}
```

## 연결이 안 될 때 확인할 것

1. `KOSIS_API_KEY`가 들어갔나요?
   - MCP 설정의 `env` 안에 넣었는지 확인합니다.
   - 터미널에서 직접 실행할 때는 현재 셸에 환경변수가 있어야 합니다.
   - NABO 도구에서 `missing_api_key`가 나오면 같은 방식으로 `NABO_API_KEY`를 추가합니다.

2. `command` 경로가 맞나요?
   - Windows: `where kosis-analysis-mcp`
   - macOS/Linux: `which kosis-analysis-mcp`
   - 찾지 못하면 `pip install "git+https://github.com/jaykim429/korea-statistic-MCP-cginside.git"`를 다시 실행합니다.

3. JSON 문법이 맞나요?
   - 중괄호 `{}`와 대괄호 `[]`가 닫혔는지 확인합니다.
   - 기존 MCP 서버 뒤에 새 서버를 추가할 때 쉼표가 필요합니다.

4. Claude Desktop을 완전히 재시작했나요?
   - 설정 파일을 바꾼 뒤에는 앱을 종료했다가 다시 켜야 합니다.

5. 서버를 직접 실행하면 아무 출력이 없나요?
   - 정상일 수 있습니다. `kosis-analysis-mcp`는 MCP stdio 서버라서 일반 CLI처럼 결과를 출력하지 않습니다.

6. `ModuleNotFoundError`가 나오나요?
   - Git clone 방식이면 가상환경을 켜고 `pip install -e .`를 실행했는지 확인합니다.
   - 설정의 `command`가 그 가상환경의 Python을 가리키는지 확인합니다.

7. 원격 URL 방식에서 401이 나오나요?
   - 서버에 `KOSIS_MCP_AUTH_TOKEN`이 설정된 경우, 클라이언트가 `Authorization: Bearer YOUR_CONNECTOR_TOKEN` 또는 `x-kosis-mcp-token` 헤더를 보내야 합니다.

8. KOSIS 조회가 실패하나요?
   - API 키가 올바른지, KOSIS OpenAPI 사용 권한이 있는지 확인합니다.
   - 응답에 `missing_api_key`, `unsupported`, `validation_errors`, `search_empty` 같은 marker가 있으면 그 marker 설명을 먼저 따릅니다.

9. NABO 조회가 실패하나요?
   - `NABO_API_KEY`가 설정되어 있는지 확인합니다.
   - NABO 통계표는 `STATBL_ID`와 `DTACYCLE_CD`가 필요합니다. 먼저 `search_nabo_tables`나 `search_stats`로 표를 찾고, `explore_nabo_table`로 항목 코드를 확인한 뒤 `query_nabo_table`을 호출합니다.

Python 실행 파일을 직접 지정해야 하면 `KOSIS_PYTHON` 환경변수를 추가합니다.

## 방법 9. Claude Code 플러그인

이 repo는 Claude Code plugin 구조도 포함합니다.

```text
/plugin marketplace add jaykim429/korea-statistic-MCP-cginside
/plugin install kosis-analysis@kosis-analysis-marketplace
```

플러그인 MCP 서버는 repo의 `.mcp.json`을 통해 시작됩니다. 사용 전 터미널 환경 또는 Claude Code 실행 환경에 `KOSIS_API_KEY`가 설정되어 있어야 합니다. NABO 도구까지 쓰려면 `NABO_API_KEY`도 설정합니다.

```bash
export KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
export NABO_API_KEY="YOUR_NABO_API_KEY"
```

Windows PowerShell:

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
$env:NABO_API_KEY="YOUR_NABO_API_KEY"
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

Gemma 4 26B처럼 로컬 LLM을 MCP 클라이언트로 쓸 때는 답을 한 번에 만들려는 흐름보다, 아래처럼 단계별로 확인하는 방식을 권장합니다. 이유는 단순합니다. 통계 질문은 “값”만 맞으면 되는 게 아니라, **표·기간·단위·분모·지역 코드**가 같이 맞아야 하기 때문입니다.

1. `plan_query`
   사용자의 질문을 먼저 읽고 “무슨 지표를 원하는지”, “어떤 지역·기간·분류가 필요한지”, “다음에 어떤 도구를 불러야 하는지”를 계획합니다. 이 단계에서는 실제 값을 가져오지 않습니다.

2. `select_table_for_query`
   질문에 맞는 KOSIS 통계표 후보를 고릅니다. 예를 들어 사용자가 “반도체 수출”을 물었다면, 산업별 축이 없는 표는 거절할 수 있습니다.

3. `resolve_concepts`
   “서울”, “30대”, “여성”, “광역시” 같은 말을 선택된 통계표 안의 실제 코드 후보로 바꿉니다.

4. `query_table`
   검증된 코드로 KOSIS 원자료를 조회합니다. 이 도구는 합산, 평균, 비율 계산을 몰래 하지 않고 원행을 반환합니다.

5. `compute_indicator`
   필요한 경우 1인당 값, 비중, 변화율, CAGR 같은 계산을 수행합니다. 단위 변환이나 “합산해도 되는 값인지” 같은 해석은 응답 marker로 caller에게 책임을 남깁니다.

고속 경로도 유지됩니다. 차이는 “어느 정도까지 MCP가 대신 처리하느냐”입니다.

- 단순 Tier A 조회는 `quick_stat`, 시계열은 `quick_trend`
- 시도별·지역별 비교는 `quick_region_compare`
- 자연어 편의 응답은 `answer_query`가 담당합니다. 기본값은 `verbose=false`라서 `data`, `metadata`, `notes`, `diagnostics` 중심의 얇은 응답을 반환합니다.
- 복합 분석 재료는 `analyze_trend`, `stat_time_compare`, `correlate_stats`, `forecast_stat`, `detect_outliers`
- 차트는 `chart_line`, `chart_compare_regions`, `chart_correlation`, `chart_heatmap`, `chart_distribution`, `chart_dual_axis`, `chart_dashboard`

## 예시

챗봇에게는 아래처럼 자연어로 물을 수 있습니다.

```text
plan_query("2020년 서울 30대 여성 인구")
plan_query("서울 1인당 GRDP 알려줘")
plan_query("광역시 중 고령화 비중이 가장 빠른 곳")
plan_query("경제성장률, 인구 변화율, 합계출산율 추이")
query_table("101", "DT_1DA7004S", filters={"ITEM": ["T80"], "A": ["00"]}, period_range=["2025", "2025"])
```

아래처럼 바로 물어볼 수도 있습니다. 자세한 진단 필드까지 보고 싶으면 `verbose=true`를 넘기면 됩니다.

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
answer_query("서울 인구 알려줘", verbose=true)
quick_region_compare("중소기업 사업체수")
quick_stat("주택매매가격지수", region="서울", period="2026.03")
detect_outliers("합계출산율", method="detrended_zscore")
indicator_dependency_map("폐업률")
chart_line("고령인구", region="전국", years=5)
```

`plan_query` 응답은 실제 값을 반환하지 않고 다음처럼 절차형 레일만 반환합니다:

- `status: "planned"`
- `intent`: `single_value`, `trend`, `comparison`, `computed_indicator` 등
- `analysis_mode`: `simple_lookup`, `analytical_single_metric`, `composite_analysis`, `needs_clarification`
- `intended_dimensions`: 질문에서 감지한 `region`, `age`, `sex`, `time`, `industry` 등
- `table_required_dimensions`: 후보 통계표가 반드시 가져야 할 KOSIS 메타 축 의미 (`year`, `month`, `quarter` 같은 시간 단위는 축으로 넘기지 않고 `time_request.granularity`로 보존)
- `semantic_dimensions`: LLM이 이해해야 할 의미 차원 (`regions`, `region_group`, `industry` 등)
- `concepts`: 후속 `resolve_concepts`가 코드로 바꿀 자연어 개념
- `metrics`: 질문에서 요청된 지표 후보. availability는 `select_table_for_query`가 KOSIS 메타로 검증하기 전까지 `unknown`
- `indicator_candidates`: 명시적 다중 지표 질문에서 segment별 지표 후보를 보존합니다. 단일 지표 질문에는 불필요하게 붙이지 않습니다.
- `quarantined_metrics`: 라우터 오염 가능성이 있어 실행 계획에서 제외한 후보 (예: `GRDP` 질문에 섞인 `R&D 투자 규모`)
- `analysis_tasks`: `trend`, `rank`, `per_capita`, `share_by_group`, `growth_rate`, `compare_metrics` 같은 후속 분석 의도
- `evidence_workflow`: `select_table_for_query` → `resolve_concepts` → `query_table` → 필요 시 `compute_indicator`
- `next_call`: Gemma가 그대로 따라갈 수 있는 다음 도구 호출 템플릿
- `mcp_output_contract`: 실패/주의 마커와 기계 판독용 신호를 노출합니다. 이 필드는 내부 진단용이며 최종 답변 문구를 강제하지 않습니다.

쉽게 말해 `plan_query`는 “답”이 아니라 “답을 찾는 계획서”입니다. `answer`가 `null`이어도 정상입니다.

다중 지표 질문에서 `mcp_output_contract.current_signals.markers_present`에 `multi_metric_request`가 있으면, Gemma는 각 metric을 별도 `select_table_for_query` 경로로 처리합니다. KOSIS 메타가 같은 표를 증명할 때만 하나의 표를 공유합니다.

`query_table` 응답은 raw extraction 전용입니다:

- `verification_level: "explored_raw"`
- `confidence`: 값의 품질이 아니라 코드 매핑과 호출 조건의 검증 수준
- `aggregation: "none"`
- `metadata_source`: 검증에 사용한 KOSIS 메타 엔드포인트, 조회 시각, 원본 URL
- `rows[]`: KOSIS 원행을 기간·값·단위·분류 차원과 함께 반환
- `data_nature`, `period_nature`: 장래인구추계 같은 미래 추계값은 `projection_data` marker와 함께 실측값처럼 쓰지 않도록 표시

잘못된 `OBJ_ID`나 `ITM_ID`가 들어오면 KOSIS 값을 호출하지 않고 `status: "unsupported"`, `validation_errors`, `suggested_codes`를 반환합니다.

즉, 이 서버는 “잘 모르겠는데 값처럼 보이는 것”을 만들어내기보다, “이 코드는 이 표에 없다”, “이 기간은 지원하지 않는다”, “이 데이터는 추계다” 같은 사실을 먼저 알려주는 쪽을 선택합니다.

운영 안정성용 환경변수:

- `KOSIS_MCP_META_CACHE_TTL`: `getMeta` 응답 캐시 TTL 초. 기본 `3600`.
- `KOSIS_MCP_QUERY_TABLE_MAX_FANOUT`: `query_table` 다중 코드 fan-out 최대 호출 수. 기본 `80`.
- `KOSIS_MCP_QUERY_TABLE_CONCURRENCY`: fan-out 동시 호출 수. 기본 `8`.
- `KOSIS_MCP_QUERY_TABLE_CALL_TIMEOUT`: fan-out 개별 호출 timeout 초. 기본 `15`.

`plan_query`는 실패도 표면화합니다:

- metric을 뽑지 못하면 `status: "needs_clarification"`과 `markers_present: ["needs_clarification", "missing_metrics"]`
- 복수 지표를 감지하면 `markers_present`에 `multi_metric_request`를 넣고, `metrics[]`, `concepts`, `analysis_tasks[].metrics`를 동기화합니다.
- 휴리스틱으로 뽑힌 지표·산업·지역그룹은 `heuristic_extraction` marker와 `caller_must_verify_with_kosis_meta`로 KOSIS 메타 검증 필요성을 드러냅니다.
- 단순 lookup은 `analysis_mode: "simple_lookup"`과 빈 `analysis_tasks`
- 단일 지표 추세/순위/산식은 `analysis_mode: "analytical_single_metric"`
- 복수 지표·복수 task·다지역 비교는 `analysis_mode: "composite_analysis"`와 `evidence_bundle: true`
- `월별`, `분기별`, `연도별` 같은 표현은 KOSIS 축이 아니라 시간 granularity로 보존됩니다.

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

`STAT_CORRELATION`·`STAT_OUTLIER_DETECTION`·`STAT_FORECAST` 의도가 감지되면 `answer_query`는 필요한 분석 재료를 찾을 수 있도록 관련 도구 이름과 후보 통계표를 함께 노출합니다. 두 Tier-A 지표가 명확히 추출되는 high-confidence 케이스(예: "실업률과 고용률 상관관계")는 `correlate_stats` 재료로 연결될 수 있습니다.

`생존율`·`폐업률`·`창업률` 같은 시간-코호트 기반 동태 지표 질의("음식점업 5년 살아남는 비율" 등)는 정태 비중(`tier_a_share_ratio`)으로 잘못 매핑되지 않고 `dynamic_ratio_advisory`로 분기되어 `indicator_dependency_map`의 산식 사양과 KOSIS 통계표 후보를 같이 반환합니다.

응답 텍스트의 KOSIS 표준 단위(`천명`, `억원`, `십억원`, `천달러`)는 자동으로 사람이 읽기 좋은 형식이 병기됩니다 — 예: `5,688.7 천명 (약 569만 명)`, `33,012,545 억원 (약 3,301.25조원)`.

`period` 표현은 0.5.0부터 다음을 추가로 인식합니다:

- `올해 1분기` / `작년 4분기` / `이번 분기` / `지난 분기`
- `올해 4월` / `지난달` / `이번달`
- `상반기` / `하반기` — KOSIS 표준 주기에 없으므로 `⚠️ 상하반기` 안내 노출

`search_and_plan` 응답은 슬롯에서 추출된 `industry`·`scale`·`target`을 검색어에 자동 보강합니다(`검색어_슬롯보강` 필드). 사용자가 "제조업 중소기업 비중"이라 물으면 검색 키워드에 "제조업"이 자동 포함돼 산업 특화 통계표가 상위에 오릅니다.

다지역 합산·합성지역 핸들러(`_answer_composite_aggregate`, `_answer_region_sum`)는 0.5.0부터 모든 component를 **병렬 호출**하고 per-call 12초·전체 60초 예산을 적용합니다 — 단일 호출이 지연돼도 다른 in-flight 요청이 막히지 않습니다.

0.6.0부터 KOSIS `getMeta` 엔드포인트를 직접 활용하는 메타/원자료 도구가 추가되었습니다:

- `explore_table(org_id, tbl_id, industry_term?)` — 통계표 한 개의 `TBL`/`ITM`/`PRD`/`SOURCE` 메타를 **병렬**로 가져와 분류축(objL1~3) 아이템 카탈로그, 수록기간, 작성기관 연락처를 단일 응답으로 반환합니다. `industry_term`을 넘기면 `ITM_NM` 매칭으로 `ITM_ID`를 동적으로 해결해 quick_stat·직접 KOSIS 호출에 산업 코드를 하드코딩하지 않아도 됩니다.
- `check_stat_availability(query, live_period_check=True)` — Tier A curation 메모뿐 아니라 KOSIS 메타 API의 실제 최신 수록 시점을 같이 조회합니다. 메모 스냅샷과 라이브 수록 시점이 어긋나면 `⚠️ 메모_vs_KOSIS_drift`, 데이터가 1년 이상 정체돼 있으면 `⚠️ 데이터_신선도`를 자동 첨부합니다.
- `query_table(org_id, tbl_id, filters, period_range?)` — `explore_table`로 검증 가능한 분류축 코드만 받아 KOSIS raw rows를 조회합니다. 여러 코드가 들어와도 서버는 합산하지 않고 개별 행을 반환하며, 잘못된 코드는 `suggested_codes`와 함께 거절합니다.

NABOSTATS(국회예산정책처 재정경제통계시스템) OpenAPI도 같은 MCP에서 조회할 수 있습니다. NABO 도구는 KOSIS 도구와 별개 제공기관이므로, 응답의 `source_system: "NABO"`와 `provider`를 답변에 보존해야 합니다.

- `search_nabo_tables(query, limit?)` — NABO 통계표 후보를 검색합니다. 결과에는 `STATBL_ID`, 표명, 주기 정보, 조회 가능한 `dtacycle_cd_suggestion`이 들어갑니다.
- `explore_nabo_table(statbl_id)` — 선택한 NABO 표의 항목·분류 코드와 주기 후보를 확인합니다. 값을 조회하기 전 코드 매핑용으로 사용합니다. 응답의 `dtacycle_cd_suggestions`와 `dtacycle_guidance`를 보면 연간(`YY`), 분기(`QY`), 월간(`MM`) 중 어떤 주기로 조회할지 판단할 수 있습니다.
- `query_nabo_table(statbl_id, dtacycle_cd="auto", period?, period_range?, filters?)` — NABO 원자료를 `period`, `value`, `unit`, `dimensions`, `raw` 형태로 정규화해 반환합니다. `dtacycle_cd="auto"`이면 표 메타데이터의 주기명을 보고 `YY`/`QY`/`MM`을 선택하고, 그 근거를 `dtacycle_resolution`에 남깁니다. `filters`에는 `ITEM`, `CLASS`, `GROUP`, `period` 계열 키를 사용할 수 있습니다.
- `search_nabo_terms(term, limit?)` — NABO 통계 용어사전을 검색합니다.
- `search_stats(query, source="all")` — KOSIS와 NABO 표 후보를 함께 찾는 통합 검색 입구입니다. 통합 검색은 후보를 합쳐 보여줄 뿐, 두 제공기관의 통계가 같은 정의라고 단정하지 않습니다.

예시:

```text
search_stats("GDP", source="all")
search_nabo_tables("재정수지")
explore_nabo_table("T192213006109866")
query_nabo_table("T192213006109866", period="latest", filters={"ITEM": ["10001"]})
query_nabo_table("T192213006109866", period_range=["2010", "2024"], filters={"ITEM": ["10001"]})
query_nabo_table("T192213006109866", period="2010:2024", filters={"ITEM": ["10001"]})
```

NABO 기간 입력은 단일 시점(`"2024"`), 최신(`"latest"`), 배열 범위(`period_range=["2010", "2024"]`), 문자열 범위(`"2010:2024"`, `"2010-2024"`), 객체 범위(`{"start": "2010", "end": "2024"}`)를 지원합니다. 범위 조회는 NABO API가 직접 지원하지 않는 경우 MCP가 원자료를 받은 뒤 기간 필터를 적용하고, `period_request`와 `period_filtered_row_count`에 처리 과정을 남깁니다.

`dtacycle_cd_suggestions`는 해당 NABO 표 메타데이터에서 확인된 실제 주기만 담습니다. 입력 가능한 전체 enum은 `dtacycle_supported_values`를 보세요. 예를 들어 연간 표에서 `dtacycle_cd="QY"`를 요청하면 빈 결과가 아니라 `period_type_incompatible`와 `dtacycle_mismatch`로 거절합니다.

NABO 원자료는 같은 `ITEM.label`이 여러 코드에서 반복될 수 있습니다. `query_nabo_table`은 항목 메타데이터를 조인해 각 행의 `item_full_name`과 `dimensions.ITEM.full_label`에 `임금근로자>실업급여계정>수입` 같은 전체 경로를 함께 넣습니다. 답변에서는 짧은 `label`만 보지 말고 `full_label`을 우선 확인하세요.

질문에 “NABO 기준”, “국회예산정책처”, “재정경제통계시스템”이 명시되면 `plan_query`는 KOSIS 표 선택 대신 `search_nabo_tables` → `explore_nabo_table` → `query_nabo_table` 흐름을 제안합니다. 이때 `source_preference: "NABO"`와 `nabo_indicator_normalization`을 확인하면 어떤 후보 문구와 NABO 메타데이터가 metric 추출에 쓰였는지 볼 수 있습니다.

챗봇용 절차형 입구로 `plan_query(query)`가 제공됩니다. `plan_query`는 의도·차원·개념·다음 도구 호출 템플릿만 반환하며, 통계표 ID 확정·코드 매핑·값 조회·산술을 하지 않습니다. 복잡한 질문에서 LLM이 직접 경로를 고르고 검증할 수 있도록 돕는 계획 전용 도구입니다.

챗봇에 노출할 도구 manifest는 [docs/chatbot_integration.md](docs/chatbot_integration.md)와 [docs/gemma_manifest.default.json](docs/gemma_manifest.default.json)를 참고하세요. `answer_query`는 더 이상 deprecated가 아니며, 빠른 자연어 편의 도구입니다. 정밀 검증이 필요하면 `plan_query` → `select_table_for_query` → `resolve_concepts` → `query_table` → 필요 시 `compute_indicator` 또는 분석 재료 도구를 조합하세요.

분석 계층은 0.9.x부터 “결론 생성”보다 “재현 가능한 재료 제공”을 우선합니다.

- `analyze_trend`: `input.x`, `input.y`, `model_parameters`, `formula`, `fitted_values`, `residuals`를 반환합니다. 자연어 `해석`은 기본적으로 포함하지 않습니다.
- `forecast_stat`: 예측 결론 대신 `data_characteristics`, `model_options`, `computed_examples.linear.forecast_path`를 반환합니다. 기존 `예측` 필드는 `include_legacy_forecast=true`일 때만 포함됩니다.
- `correlate_stats`: Pearson/Spearman/Kendall 계수와 정합 데이터 배열을 반환합니다. “상관은 인과가 아님”은 `must_know`와 `common_pitfalls`에 구조화됩니다.
- `detect_outliers`: 기본값은 `detrended_zscore`이며, `zscore`, `iqr`, `stl`, `all`을 선택할 수 있습니다. 결과와 함께 원자료 배열과 데이터 특성을 반환합니다.

`decode_error`는 비공식 코드뿐 아니라 KOSIS 공식 코드 `42` ("사용자별 이용 제한")을 인식하도록 확장되었습니다.

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

공식 통계도 표의 정의와 작성 기준이 다르면 서로 다른 숫자가 나올 수 있습니다. 챗봇이 답을 만들 때는 아래 사항을 사용자에게 숨기지 않는 것이 좋습니다.

- 기업 수, 사업체 수, 자영업자 수는 서로 다른 모집단입니다.
- 비중, 폐업률, 창업률, 생존율은 분모와 작성기관 산식을 먼저 확인해야 합니다.
- 상관·회귀·정책효과 분석은 인과관계를 자동으로 의미하지 않습니다.
- “최신” 질문은 KOSIS 통계표의 최신 수록 시점을 기준으로 답합니다.
- “2020년”, “2026년 3월”, “전년 대비”, “전월 대비”처럼 기간이 명시된 질문은 해당 기간 또는 기간 비교로 처리하며, 데이터가 없으면 최신값으로 대체하지 않습니다.
- Tier A에 없는 질문은 단일값을 임의로 답하지 않고 검색 후보와 분석 계획을 반환합니다.

예를 들어 “한국 GDP 증가했어?”라는 질문은 단순히 숫자 하나를 찾는 문제가 아닙니다. 어떤 GDP 표를 쓸지, 비교 기간이 전년 대비인지 전기 대비인지, 단위가 원인지 지수인지 확인해야 합니다. 이 서버는 그런 확인 지점을 marker와 contract로 드러냅니다.

## 검증 스크립트

라이브 KOSIS API 회귀 검증:

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
python scripts\regression_smoke.py
python scripts\eval_plan_query_pipeline.py
python scripts\eval_gemma_workflow.py
python scripts\comprehensive_api_matrix.py
python scripts\temporal_edge_cases.py
python scripts\natural_language_battery.py --summary-only
```

NABO 도구까지 확인할 때는 같은 터미널에서 `NABO_API_KEY`도 설정한 뒤 `scripts/eval_tool_contracts.py`를 실행합니다. 이 스크립트는 외부 호출을 mock으로 검증하므로 키가 없어도 기본 계약 테스트는 통과할 수 있습니다.

```powershell
$env:NABO_API_KEY="YOUR_NABO_API_KEY"
python scripts\eval_tool_contracts.py
```

`eval_plan_query_pipeline.py`는 Gemma용 `plan_query` 로컬 회귀 테스트입니다. 현재 36개 케이스가 포함되어 있으며 다음 패턴을 고정합니다:

- metric 없음 / clarification 상태의 `mcp_output_contract.current_signals` false negative 차단
- `year`, `month`, `quarter`가 KOSIS table axis로 흘러가지 않는지 확인
- `simple_lookup` / `analytical_single_metric` / `composite_analysis` 모드와 `evidence_bundle` 일관성 확인
- `PPI`, `GRDP`, `CPI` 같은 약어와 `치킨집 얼마나 있어?` 같은 일상어 개수 질의
- `GRDP` ↔ `R&D 투자 규모` 라우터 오염 격리
- top/bottom 순위, 기간 범위, 다지역 비교, 산식+순위 결합 회귀
- `경제성장률 및 인구 변화율`, `경제성장률, 인구 변화율, 합계출산율 추이` 같은 다중 지표 질의에서 `metrics[]`, `concepts`, `analysis_tasks[].metrics` 동기화

`eval_gemma_workflow.py`는 `plan_query` 중심의 Gemma 절차형 워크플로우 평가셋입니다. 다축 슬라이싱(2020년 서울 30대 여성), 1인당 GRDP, 광역시 고령화 비중, 영문 질의, CPI/GRDP 약어, 치킨집 폐업률 같은 seed case를 통해 필요한 차원·개념·후속 도구 순서가 유지되는지 확인합니다. `future_must_not_select_tables` 같은 필드는 다음 PR에서 `select_table_for_query`가 추가되면 거짓 양성 negative test로 승격할 수 있도록 남겨둔 기대값입니다.

`natural_language_battery.py`는 `answer_query`만 호출하는 자연어 배터리로, 10개 이상의 의도 카테고리(단일값·시계열·증가율·시도별·Top N·비중·합산·복합·검색폴백·가드레일·의도불일치)를 한 번에 검증합니다. `--group <name>` 또는 `--name <case>`로 필터링 가능합니다.

## 파일 구성

- `kosis_mcp_server.py`: MCP 서버와 도구 정의
- `kosis_http_server.py`: Streamable HTTP MCP 서버 엔트리포인트
- `kosis_curation.py`: 자연어 라우터, Tier A/B 큐레이션, 개념 그래프
- `kosis_charts_extra.py`: 추가 SVG 차트 헬퍼
- `docs/chatbot_integration.md`: Gemma 챗봇용 통계 MCP 도구 manifest 및 일관성 규칙
- `scripts/regression_smoke.py`, `scripts/eval_gemma_workflow.py`, `scripts/comprehensive_api_matrix.py`, `scripts/temporal_edge_cases.py`, `scripts/natural_language_battery.py`: 라이브 API/워크플로우 회귀 검증 스크립트
- `pyproject.toml`: `kosis-analysis-mcp` 실행 명령과 패키지 메타데이터
- `package.json`, `bin/`, `scripts/`: npx/npm wrapper
- `.claude-plugin/`, `.mcp.json`, `skills/`: Claude Code plugin 구성
- `render.yaml`, `Dockerfile`, `fly.toml.example`: 원격 배포 예시
- `requirements.txt`: 실행 의존성
- `mcp_config.example.json`: MCP 클라이언트 설정 예시
- `.env.example`: 환경변수 예시
