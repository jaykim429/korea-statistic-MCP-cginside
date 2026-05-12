"""HTTP entry point for hosted KOSIS MCP deployments.

The main server in kosis_mcp_server.py is a stdio MCP server. This module
reuses the same FastMCP instance and exposes the Streamable HTTP transport at
`/mcp`, which can be connected through mcp-remote or hosted behind Render/Fly.
"""

from __future__ import annotations

import os

from kosis_mcp_server import mcp


app = mcp.streamable_http_app()


def main() -> None:
    """Run the Streamable HTTP MCP server with uvicorn."""
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
