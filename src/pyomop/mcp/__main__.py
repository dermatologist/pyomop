#!/usr/bin/env python3
"""
Standalone entry point for pyomop MCP server.

This script can be used to start the MCP server directly without going through
the main CLI. Useful for MCP client configurations.

Usage:
    python -m pyomop.mcp
    or
    pyomop-mcp-server (if installed as a script)
"""

import asyncio
import sys

if __name__ == "__main__":
    try:
        from .server import main
        asyncio.run(main())
    except ImportError as e:
        print(f"Error: {e}", file=sys.stderr)
        print("Make sure 'mcp' package is installed: pip install mcp", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("MCP server stopped.", file=sys.stderr)
    except Exception as e:
        print(f"Error starting MCP server: {e}", file=sys.stderr)
        sys.exit(1)