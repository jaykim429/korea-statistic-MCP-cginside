FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 빌드 전에 `python scripts/sync_rules.py` 를 돌려 정본 규칙을 내려받아야 한다.
# rules/ 는 Nuxt server/utils/stat/rules 의 사본이고, tests/test_rules_contract.py 가 어긋남을 잡는다.
COPY . .

# 이 이미지가 어느 커밋에서 만들어졌나. 챗봇 저장소의 Gate 0 이 돌고 있는 컨테이너의 이 파일을 MCP HEAD 와
# 대조한다(챗봇 followups 18 — 로컬 컨테이너가 열흘 동안 저장소보다 늦어 있었고 아무도 몰랐다).
#   docker build --build-arg GIT_COMMIT="$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)" -t korea-statistic-mcp:local .
# 각인 없이 만들면 'unknown' 이 들어가고 Gate 0 이 어긋남으로 잡는다.
ARG GIT_COMMIT=unknown
RUN echo "$GIT_COMMIT" > /app/.build-commit

EXPOSE 8000

# Readiness gate for the container.
#
# Without this, `docker compose ps` reports no health state at all, so a deploy that
# rebuilt and recreated the container looks identical whether the service came up
# healthy or is refusing every query. Callers then see MCP connection errors and cannot
# tell a broken deploy from a genuine "no such statistic" answer.
#
# Notes on the implementation:
#  - python, not curl/wget: the python:3.11-slim base image ships neither.
#  - 127.0.0.1, not localhost: inside a container localhost can resolve to IPv6 ::1
#    while uvicorn is bound to IPv4 0.0.0.0, which makes the check fail even though the
#    service is fine. docs/LOCAL-SETUP.md in the consuming repo records exactly this
#    false-unhealthy for the sibling korean-law-mcp container.
#  - /readyz, not /healthz: /healthz is liveness and always 200, so it cannot catch a
#    container that started without KOSIS_API_KEY.
#  - shell form so ${PORT} expands; defaults to the port CMD binds below.
#  - prints a one-line reason instead of a Python traceback: this output is what shows up
#    in `docker inspect --format '{{.State.Health.Log}}'`, i.e. what an operator reads
#    while the service is down.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import os,sys,json,urllib.request,urllib.error\nurl='http://127.0.0.1:'+os.environ.get('PORT','8000')+'/readyz'\ntry:\n    body=urllib.request.urlopen(url,timeout=4).read().decode()\n    print('ready',body)\nexcept urllib.error.HTTPError as e:\n    print('not ready: HTTP',e.code,e.read().decode()[:200]); sys.exit(1)\nexcept Exception as e:\n    print('unreachable:',type(e).__name__,e); sys.exit(1)"]

CMD ["uvicorn", "kosis_http_server:app", "--host", "0.0.0.0", "--port", "8000"]
