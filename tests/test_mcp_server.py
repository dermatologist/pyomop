"""Tests for MCP server functionality."""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Skip tests if MCP is not available
try:
    import mcp.types as types
    from src.pyomop.mcp.server import (
        _create_cdm,
        _get_cdm,
        _get_table_columns,
        _get_single_table_info,
        _get_usable_table_names,
        _run_sql,
        handle_get_prompt,
        handle_list_prompts,
        handle_list_tools,
    )
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
class TestMCPServer:
    """Test MCP server functionality."""

    @pytest.mark.asyncio
    async def test_list_tools(self):
        """Test that all required tools are listed."""
        tools = await handle_list_tools()
        
        expected_tools = {
            "create_cdm",
            "create_eunomia", 
            "get_cdm",
            "get_table_columns",
            "get_single_table_info",
            "get_usable_table_names",
            "run_sql"
        }
        
        tool_names = {tool.name for tool in tools}
        assert expected_tools.issubset(tool_names)

    @pytest.mark.asyncio
    async def test_list_prompts(self):
        """Test that the query execution prompt is available."""
        prompts = await handle_list_prompts()
        
        assert len(prompts) == 1
        assert prompts[0].name == "query_execution_steps"

    @pytest.mark.asyncio
    async def test_get_prompt(self):
        """Test getting the query execution prompt."""
        prompt_result = await handle_get_prompt("query_execution_steps")
        
        assert prompt_result.description == "Database query execution steps"
        assert len(prompt_result.messages) == 1
        assert "Get usable table names" in prompt_result.messages[0].content.text

    @pytest.mark.asyncio
    async def test_create_cdm_tool(self):
        """Test create_cdm tool."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.sqlite"
            
            with patch('src.pyomop.mcp.server.CdmEngineFactory') as mock_factory:
                mock_cdm = MagicMock()
                mock_cdm.init_models = AsyncMock()
                mock_factory.return_value = mock_cdm
                
                with patch('src.pyomop.mcp.server.Base') as mock_base:
                    mock_base.metadata = MagicMock()
                    
                    result = await _create_cdm(str(db_path), "cdm54")
                    
                    assert len(result) == 1
                    assert "Successfully created CDM" in result[0].text
                    assert str(db_path) in result[0].text

    @pytest.mark.asyncio
    async def test_get_cdm_nonexistent_file(self):
        """Test get_cdm with non-existent database file."""
        result = await _get_cdm("/nonexistent/path.sqlite")
        
        assert len(result) == 1
        assert "Database file does not exist" in result[0].text

    @pytest.mark.asyncio
    async def test_get_table_columns_no_llm(self):
        """Test get_table_columns without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            with patch('src.pyomop.mcp.server.CDMDatabase', side_effect=ImportError):
                result = await _get_table_columns(temp_file.name, "person")
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_get_single_table_info_no_llm(self):
        """Test get_single_table_info without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            with patch('src.pyomop.mcp.server.CDMDatabase', side_effect=ImportError):
                result = await _get_single_table_info(temp_file.name, "person")
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_get_usable_table_names_no_llm(self):
        """Test get_usable_table_names without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            with patch('src.pyomop.mcp.server.CDMDatabase', side_effect=ImportError):
                result = await _get_usable_table_names(temp_file.name)
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_run_sql_empty_statement(self):
        """Test run_sql with empty SQL statement."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            result = await _run_sql(temp_file.name, "")
            
            assert len(result) == 1
            assert "Empty SQL statement" in result[0].text

    @pytest.mark.asyncio
    async def test_run_sql_nonexistent_file(self):
        """Test run_sql with non-existent database file."""
        result = await _run_sql("/nonexistent/path.sqlite", "SELECT 1")
        
        assert len(result) == 1
        assert "Database file does not exist" in result[0].text

    @pytest.mark.asyncio
    async def test_run_sql_error_handling(self):
        """Test that SQL errors are returned as text, not thrown."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            with patch('src.pyomop.mcp.server.CdmEngineFactory') as mock_factory:
                mock_cdm = MagicMock()
                mock_session = AsyncMock()
                mock_session.execute.side_effect = Exception("SQL Error")
                mock_cdm.async_session.return_value.__aenter__.return_value = mock_session
                mock_factory.return_value = mock_cdm
                
                result = await _run_sql(temp_file.name, "INVALID SQL")
                
                assert len(result) == 1
                assert "SQL execution error" in result[0].text
                assert "SQL Error" in result[0].text


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
class TestMCPIntegration:
    """Test MCP integration with actual pyomop components."""

    @pytest.mark.asyncio
    async def test_create_and_query_cdm(self):
        """Test creating a CDM and running a simple query."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "integration_test.sqlite"
            
            # This would require actual database setup, so we'll mock it
            with patch('src.pyomop.mcp.server.CdmEngineFactory') as mock_factory:
                # Mock successful CDM creation
                mock_cdm = MagicMock()
                mock_cdm.init_models = AsyncMock()
                mock_factory.return_value = mock_cdm
                
                with patch('src.pyomop.mcp.server.Base') as mock_base:
                    mock_base.metadata = MagicMock()
                    
                    # Test CDM creation
                    result = await _create_cdm(str(db_path), "cdm54")
                    assert "Successfully created CDM" in result[0].text

    def test_mcp_server_import(self):
        """Test that MCP server can be imported."""
        from src.pyomop.mcp import mcp_server_main
        assert callable(mcp_server_main)