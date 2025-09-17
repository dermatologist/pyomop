"""MCP server integration for pyomop.

This module provides MCP (Model Context Protocol) server functionality
to interact with OMOP CDM databases through pyomop tools.
"""

from .server import main as mcp_server_main

__all__ = ["mcp_server_main"]