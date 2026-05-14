"""HTTP entry point for hosted KOSIS MCP deployments.

The main server in kosis_mcp_server.py is a stdio MCP server. This module
reuses the same FastMCP instance and exposes the Streamable HTTP transport at
`/mcp`, which can be connected through mcp-remote or hosted behind Render/Fly.
"""

from __future__ import annotations

import json
import os
from hmac import compare_digest
from typing import Any

from kosis_mcp_server import mcp


class OptionalBearerAuthMiddleware:
    """Protect hosted MCP traffic when KOSIS_MCP_AUTH_TOKEN is configured."""

    def __init__(self, app: Any, token: str | None = None) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        if path in {"/health", "/healthz"}:
            await self._json(send, 200, {"status": "ok", "service": "kosis-analysis-mcp"})
            return

        if self.token and not self._authorized(scope):
            await self._json(
                send,
                401,
                {
                    "error": "unauthorized",
                    "message": "Missing or invalid MCP bearer token.",
                },
                headers=[(b"www-authenticate", b"Bearer")],
            )
            return

        await self.app(scope, receive, send)

    def _authorized(self, scope: dict[str, Any]) -> bool:
        headers = {
            key.lower(): value
            for key, value in scope.get("headers", [])
        }
        auth = headers.get(b"authorization", b"").decode("utf-8", errors="ignore").strip()
        token_header = headers.get(b"x-kosis-mcp-token", b"").decode("utf-8", errors="ignore").strip()
        candidates = []
        if auth.lower().startswith("bearer "):
            candidates.append(auth[7:].strip())
        if token_header:
            candidates.append(token_header)
        return any(compare_digest(candidate, self.token or "") for candidate in candidates)

    @staticmethod
    async def _json(
        send: Any,
        status: int,
        payload: dict[str, Any],
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        response_headers = [
            (b"content-type", b"application/json; charset=utf-8"),
            (b"content-length", str(len(body)).encode("ascii")),
            *(headers or []),
        ]
        await send({"type": "http.response.start", "status": status, "headers": response_headers})
        await send({"type": "http.response.body", "body": body})


def _build_app() -> Any:
    raw_app = mcp.streamable_http_app()
    return OptionalBearerAuthMiddleware(raw_app, os.environ.get("KOSIS_MCP_AUTH_TOKEN"))


app = _build_app()


def main() -> None:
    """Run the Streamable HTTP MCP server with uvicorn."""
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
