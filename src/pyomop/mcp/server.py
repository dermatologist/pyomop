"""MCP server for pyomop CDM database interaction.

This module implements an MCP server that exposes tools for interacting
with OMOP CDM databases using pyomop functionality.

Example usage:
    # Start the server
    python -m pyomop.mcp

    # Or via CLI
    pyomop --mcp-server

    # Or via dedicated script
    pyomop-mcp-server

The server provides tools for:
- Creating CDM databases (empty or with sample data)
- Querying table structures and metadata
- Executing SQL statements with error handling
- Following guided query execution workflows
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import mcp.server.stdio
    import mcp.types as types
    from mcp.server import NotificationOptions, Server
    from mcp.server.models import InitializationOptions
except ImportError:
    raise ImportError("Install 'mcp' to use MCP server features: pip install mcp")

from ..engine_factory import CdmEngineFactory

# Set up logging
logger = logging.getLogger(__name__)

# Initialize the MCP server
server = Server("pyomop-mcp-server")

# Static prompt for database query execution
QUERY_EXECUTION_PROMPT = """
To execute a database query based on free text instruction, follow these steps:

1. **Get usable table names**: Use get_usable_table_names to see all available tables in the CDM
2. **Get relevant table info and columns**: For tables that seem relevant to your query, use:
   - get_table_columns to see column names for specific tables
   - get_single_table_info to get detailed table structure including foreign keys
3. **Construct SQL query**: Based on the table structure, create one or more SQL queries
4. **Run queries**: Use run_sql to execute queries one at a time, keeping track of results
5. **Error handling**: If there's an error in the query when run, correct the error and try again

Always start with understanding the available tables and their structure before writing SQL queries.
"""


@server.list_tools()
async def handle_list_tools() -> List[types.Tool]:
    """List available MCP tools for pyomop CDM interaction."""
    return [
        types.Tool(
            name="create_cdm",
            description="Create an empty CDM database",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "version": {
                        "type": "string",
                        "enum": ["cdm54", "cdm6"],
                        "default": "cdm54",
                        "description": "CDM version to create",
                    },
                },
                "required": ["db_path"],
            },
        ),
        types.Tool(
            name="create_eunomia",
            description="Create a CDM database with eunomia data pre-installed",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "version": {
                        "type": "string",
                        "enum": ["5.3", "5.4"],
                        "default": "5.3",
                        "description": "CDM version to create",
                    },
                    "dataset": {
                        "type": "string",
                        "default": "GiBleed",
                        "description": "Eunomia dataset to load",
                    },
                },
                "required": ["db_path"],
            },
        ),
        types.Tool(
            name="get_engine",
            description="Get database engine for interaction (returns engine URL/type summary)",
            inputSchema={
                "type": "object",
                "properties": {
                    "db": {
                        "type": "string",
                        "default": "sqlite",
                        "description": "Database type (sqlite, mysql, pgsql)",
                    },
                    "host": {
                        "type": "string",
                        "default": "localhost",
                        "description": "Database host (ignored for sqlite)",
                    },
                    "port": {
                        "type": "integer",
                        "default": 5432,
                        "description": "Database port (ignored for sqlite)",
                    },
                    "user": {
                        "type": "string",
                        "default": "root",
                        "description": "Database user (ignored for sqlite)",
                    },
                    "pw": {
                        "type": "string",
                        "default": "pass",
                        "description": "Database password (ignored for sqlite)",
                    },
                    "name": {
                        "type": "string",
                        "default": "cdm.sqlite",
                        "description": "Database name or SQLite filename",
                    },
                    "schema": {
                        "type": "string",
                        "default": "",
                        "description": "PostgreSQL schema to use for CDM",
                    },
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_table_columns",
            description="Get column names for a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to get columns for",
                    },
                    "version": {
                        "type": "string",
                        "enum": ["cdm54", "cdm6"],
                        "default": "cdm54",
                        "description": "CDM version",
                    },
                },
                "required": ["db_path", "table_name"],
            },
        ),
        types.Tool(
            name="get_single_table_info",
            description="Get detailed information about a single table including columns and foreign keys",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to get info for",
                    },
                    "version": {
                        "type": "string",
                        "enum": ["cdm54", "cdm6"],
                        "default": "cdm54",
                        "description": "CDM version",
                    },
                },
                "required": ["db_path", "table_name"],
            },
        ),
        types.Tool(
            name="get_usable_table_names",
            description="Get list of all usable table names in the CDM",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "version": {
                        "type": "string",
                        "enum": ["cdm54", "cdm6"],
                        "default": "cdm54",
                        "description": "CDM version",
                    },
                },
                "required": ["db_path"],
            },
        ),
        types.Tool(
            name="run_sql",
            description="Execute a SQL statement on the CDM database",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Full path to the SQLite database file",
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL statement to execute",
                    },
                    "fetch_results": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether to fetch and return results for SELECT queries",
                    },
                },
                "required": ["db_path", "sql"],
            },
        ),
    ]


@server.list_prompts()
async def handle_list_prompts() -> List[types.Prompt]:
    """List available prompts."""
    return [
        types.Prompt(
            name="query_execution_steps",
            description="Steps to execute a database query based on free text instruction",
        )
    ]


@server.get_prompt()
async def handle_get_prompt(
    name: str, arguments: Optional[Dict[str, str]] = None
) -> types.GetPromptResult:
    """Get a specific prompt."""
    if name == "query_execution_steps":
        return types.GetPromptResult(
            description="Database query execution steps",
            messages=[
                types.PromptMessage(
                    role="assistant",
                    content=types.TextContent(type="text", text=QUERY_EXECUTION_PROMPT),
                )
            ],
        )
    else:
        raise ValueError(f"Unknown prompt: {name}")


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: Optional[Dict[str, Any]] = None
) -> List[types.TextContent]:
    """Handle tool calls."""
    if arguments is None:
        arguments = {}

    try:
        if name == "create_cdm":
            return await _create_cdm(**arguments)
        elif name == "create_eunomia":
            return await _create_eunomia(**arguments)
        elif name == "get_engine":
            engine = await _get_engine(**arguments)
            # Return engine URL/type summary as TextContent
            url = getattr(engine, "url", None)
            url_str = str(url) if url else str(engine)
            return [types.TextContent(type="text", text=f"Engine: {url_str}")]
        elif name == "get_table_columns":
            return await _get_table_columns(**arguments)
        elif name == "get_single_table_info":
            return await _get_single_table_info(**arguments)
        elif name == "get_usable_table_names":
            return await _get_usable_table_names(**arguments)
        elif name == "run_sql":
            return await _run_sql(**arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        logger.error(f"Error in tool {name}: {e}")
        return [types.TextContent(type="text", text=f"Error: {str(e)}")]


async def _create_cdm(db_path: str, version: str = "cdm54") -> List[types.TextContent]:
    """Create an empty CDM database."""
    try:
        # Ensure the directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create engine using _get_engine
        engine = await _get_engine(db="sqlite", name=db_path)

        # Initialize the models
        if version == "cdm6":
            from ..cdm6 import Base
        else:
            from ..cdm54 import Base

        # Use CdmEngineFactory to call init_models, but pass engine
        cdm = CdmEngineFactory(db="sqlite", name=db_path)
        cdm._engine = engine
        await cdm.init_models(Base.metadata)

        return [
            types.TextContent(
                type="text",
                text=f"Successfully created CDM {version} database at: {db_path}",
            )
        ]
    except Exception as e:
        return [
            types.TextContent(
                type="text", text=f"Error creating CDM database: {str(e)}"
            )
        ]


async def _create_eunomia(
    db_path: str, version: str = "5.3", dataset: str = "GiBleed"
) -> List[types.TextContent]:
    """Create a CDM database with eunomia data."""
    try:
        # First create the CDM structure
        await _create_cdm(db_path, version)

        # Load eunomia data
        from ..eunomia import EunomiaData

        engine = await _get_engine(db="sqlite", name=db_path)
        cdm = CdmEngineFactory(db="sqlite", name=db_path)
        cdm._engine = engine
        eunomia = EunomiaData(cdm)

        # Download and load dataset
        zip_path = eunomia.download_eunomia_data(
            dataset_name=dataset, cdm_version=version, verbose=True
        )

        await eunomia.extract_load_data(
            from_path=zip_path,
            dataset_name=dataset,
            cdm_version=version,
            input_format="csv",
            verbose=True,
        )

        return [
            types.TextContent(
                type="text",
                text=f"Successfully created CDM {version} database with {dataset} data at: {db_path}",
            )
        ]
    except Exception as e:
        return [
            types.TextContent(
                type="text",
                text=f"Error creating CDM database with eunomia data: {str(e)}",
            )
        ]


async def _get_engine(
    db="sqlite",
    host="localhost",
    port=5432,
    user="root",
    pw="pass",
    name="cdm.sqlite",
    schema="",
):
    """Get a database engine based on provided parameters."""
    return CdmEngineFactory(
        db=db,
        host=host,
        port=port,
        user=user,
        pw=pw,
        name=name,
        schema=schema,
    ).engine


async def _get_table_columns(
    db_path: str, table_name: str, version: str = "cdm54"
) -> List[types.TextContent]:
    """Get column names for a specific table."""
    try:
        if not Path(db_path).exists():
            return [
                types.TextContent(
                    type="text", text=f"Database file does not exist: {db_path}"
                )
            ]

        # Check if LLM features are available
        try:
            from ..llm_engine import CDMDatabase

            engine = await _get_engine(db="sqlite", name=db_path)
            cdm_db = CDMDatabase(engine, version=version)  # type: ignore

            columns = cdm_db.get_table_columns(table_name)

            return [
                types.TextContent(
                    type="text",
                    text=f"Columns for table '{table_name}': {', '.join(columns)}",
                )
            ]
        except ImportError:
            return [
                types.TextContent(
                    type="text",
                    text="LLM features not available. Install pyomop[llm] to use this tool.",
                )
            ]
    except Exception as e:
        return [
            types.TextContent(
                type="text", text=f"Error getting table columns: {str(e)}"
            )
        ]


async def _get_single_table_info(
    db_path: str, table_name: str, version: str = "cdm54"
) -> List[types.TextContent]:
    """Get detailed information about a single table."""
    try:
        if not Path(db_path).exists():
            return [
                types.TextContent(
                    type="text", text=f"Database file does not exist: {db_path}"
                )
            ]

        # Check if LLM features are available
        try:
            from ..llm_engine import CDMDatabase

            engine = await _get_engine(db="sqlite", name=db_path)
            cdm_db = CDMDatabase(engine, version=version)  # type: ignore

            table_info = cdm_db.get_single_table_info(table_name)

            return [types.TextContent(type="text", text=table_info)]
        except ImportError:
            return [
                types.TextContent(
                    type="text",
                    text="LLM features not available. Install pyomop[llm] to use this tool.",
                )
            ]
    except Exception as e:
        return [
            types.TextContent(type="text", text=f"Error getting table info: {str(e)}")
        ]


async def _get_usable_table_names(
    db_path: str, version: str = "cdm54"
) -> List[types.TextContent]:
    """Get list of all usable table names."""
    try:
        if not Path(db_path).exists():
            return [
                types.TextContent(
                    type="text", text=f"Database file does not exist: {db_path}"
                )
            ]

        # Check if LLM features are available
        try:
            from ..llm_engine import CDMDatabase

            engine = await _get_engine(db="sqlite", name=db_path)
            cdm_db = CDMDatabase(engine, version=version)  # type: ignore

            table_names = cdm_db.get_usable_table_names()

            return [
                types.TextContent(
                    type="text", text=f"Available tables: {', '.join(table_names)}"
                )
            ]
        except ImportError:
            return [
                types.TextContent(
                    type="text",
                    text="LLM features not available. Install pyomop[llm] to use this tool.",
                )
            ]
    except Exception as e:
        return [
            types.TextContent(type="text", text=f"Error getting table names: {str(e)}")
        ]


async def _run_sql(
    db_path: str, sql: str, fetch_results: bool = True
) -> List[types.TextContent]:
    """Execute a SQL statement."""
    try:
        if not Path(db_path).exists():
            return [
                types.TextContent(
                    type="text", text=f"Database file does not exist: {db_path}"
                )
            ]

        engine = await _get_engine(db="sqlite", name=db_path)
        cdm = CdmEngineFactory(db="sqlite", name=db_path)
        cdm._engine = engine

        # Sanitize SQL (basic validation)
        sql = sql.strip()
        if not sql:
            return [types.TextContent(type="text", text="Empty SQL statement provided")]

        # Execute the SQL
        async with cdm.async_session() as session:
            from sqlalchemy import text

            result = await session.execute(text(sql))

            if fetch_results and sql.lower().strip().startswith("select"):
                # Fetch results for SELECT queries
                rows = result.fetchall()
                if rows:
                    # Get column names
                    columns = list(result.keys()) if hasattr(result, "keys") else []

                    # Format results
                    result_text = (
                        f"Query executed successfully. Found {len(rows)} rows.\n"
                    )
                    if columns:
                        result_text += f"Columns: {', '.join(columns)}\n"

                    # Show first few rows
                    for i, row in enumerate(rows[:10]):  # Limit to first 10 rows
                        row_dict = (
                            dict(row._mapping)
                            if hasattr(row, "_mapping")
                            else dict(row)
                        )
                        result_text += f"Row {i+1}: {row_dict}\n"

                    if len(rows) > 10:
                        result_text += f"... and {len(rows) - 10} more rows"

                    return [types.TextContent(type="text", text=result_text)]
                else:
                    return [
                        types.TextContent(
                            type="text",
                            text="Query executed successfully. No rows returned.",
                        )
                    ]
            else:
                # For non-SELECT queries or when not fetching results
                await session.commit()
                return [
                    types.TextContent(
                        type="text",
                        text=f"SQL statement executed successfully: {sql[:100]}...",
                    )
                ]

    except Exception as e:
        # Return error as string without throwing exception
        return [types.TextContent(type="text", text=f"SQL execution error: {str(e)}")]


async def main():
    """Main entry point for the MCP server."""
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting pyomop MCP server")

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="pyomop-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def main_cli():
    """CLI entry point for the MCP server."""
    asyncio.run(main())


if __name__ == "__main__":
    main_cli()
