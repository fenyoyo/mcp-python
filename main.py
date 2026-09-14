from pathlib import Path
from fastmcp import FastMCP
from fastmcp.server import create_proxy

mcp = FastMCP("MCP Server")

# Mount a remote HTTP server (URLs work directly)
mcp.mount(create_proxy("http://127.0.0.1:8000/mcp/mihome"), namespace="mihome")
mcp.mount(create_proxy("http://127.0.0.1:8000/mcp/weather"), namespace="weather")

if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8001)
