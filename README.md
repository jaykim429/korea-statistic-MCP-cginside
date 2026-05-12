# KOSIS Natural Language MCP Server

KOSIS OpenAPI를 자연어 질의, 통계 조회, 분석, SVG 시각화로 연결하는 MCP 서버입니다.  
챗봇에서는 우선 `answer_query`를 호출하면 됩니다. 정밀 매핑된 Tier A 통계는 바로 API 조회하고, AI/풍력/건설/소상공인 폐업률처럼 후보 선택이 필요한 복합 질의는 KOSIS 검색 후보와 분석 계획을 반환합니다.

## 주요 기능

- 자연어 라우팅: 동의어, 하위어, 상위어, 관점어 기반 검색어 생성
- 직접 조회: 인구, 출산율, 실업률, 자영업자, 중소기업·소상공인 핵심 지표 등 Tier A
- 분석: 추세, 변화율, 상관, 예측, 이상치, 기간 비교
- 시각화: 라인, 막대, 산점도, 히트맵, 분포, 이중축, 대시보드 SVG
- 검증 보조: 답변 payload 검증, 산식형 지표의 분모·필요 통계 안내

## 설치

```powershell
git clone https://github.com/jaykim429/korea-statistic-MCP-cginside.git
cd korea-statistic-MCP-cginside
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

KOSIS 인증키를 환경변수로 설정합니다.

```powershell
$env:KOSIS_API_KEY="YOUR_KOSIS_API_KEY"
```

## MCP 연결

위에서 `git clone`한 폴더의 `kosis_mcp_server.py` 경로를 Claude Desktop 또는 MCP 클라이언트 설정에 등록합니다.

Windows 예시:

```json
{
  "mcpServers": {
    "kosis-analysis": {
      "command": "python",
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

macOS/Linux 예시:

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

서버 단독 실행:

```powershell
python kosis_mcp_server.py
```

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
- `requirements.txt`: 실행 의존성
- `mcp_config.example.json`: MCP 클라이언트 설정 예시
- `.env.example`: 환경변수 예시
