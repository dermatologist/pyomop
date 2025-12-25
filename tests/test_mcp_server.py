"""Tests for MCP server functionality."""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, Mock

import pytest

# Skip tests if MCP is not available
try:
    import mcp.types as types
    from src.pyomop.mcp.server import (
        _create_cdm,
        _get_table_columns,
        _get_single_table_info,
        _get_usable_table_names,
        _run_sql,
        _example_query,
        _check_sql,
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
            "get_engine",
            "get_table_columns",
            "get_single_table_info",
            "get_usable_table_names",
            "run_sql",
            "example_query",
            "check_sql",
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
            
            with patch('src.pyomop.mcp.server._get_engine') as mock_get_engine:
                with patch('src.pyomop.mcp.server.CdmEngineFactory') as mock_factory:
                    mock_engine = MagicMock()
                    mock_get_engine.return_value = mock_engine
                    
                    mock_cdm = MagicMock()
                    mock_cdm._engine = mock_engine
                    mock_cdm.init_models = AsyncMock()
                    mock_factory.return_value = mock_cdm
                    
                    with patch('src.pyomop.cdm54.Base') as mock_base:
                        mock_base.metadata = MagicMock()
                        
                        result = await _create_cdm(str(db_path), "cdm54")
                        
                        assert len(result) == 1
                        assert "Successfully created CDM" in result[0].text
                        assert str(db_path) in result[0].text

    @pytest.mark.asyncio
    async def test_get_table_columns_no_llm(self):
        """Test get_table_columns without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            # Patch the import to make CDMDatabase unavailable
            with patch.dict('sys.modules', {'src.pyomop.llm_engine': None}):
                result = await _get_table_columns("person", db_path=temp_file.name)
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_get_single_table_info_no_llm(self):
        """Test get_single_table_info without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            # Patch the import to make CDMDatabase unavailable
            with patch.dict('sys.modules', {'src.pyomop.llm_engine': None}):
                result = await _get_single_table_info("person", db_path=temp_file.name)
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_get_usable_table_names_no_llm(self):
        """Test get_usable_table_names without LLM features."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            # Patch the import to make CDMDatabase unavailable
            with patch.dict('sys.modules', {'src.pyomop.llm_engine': None}):
                result = await _get_usable_table_names(db_path=temp_file.name)
                
                assert len(result) == 1
                assert "LLM features not available" in result[0].text

    @pytest.mark.asyncio
    async def test_run_sql_empty_statement(self):
        """Test run_sql with empty SQL statement."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            result = await _run_sql("", db_path=temp_file.name)
            
            assert len(result) == 1
            assert "Empty SQL statement" in result[0].text

    @pytest.mark.asyncio
    async def test_run_sql_error_handling(self):
        """Test that SQL errors are returned as text, not thrown."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            with patch('src.pyomop.mcp.server._get_engine') as mock_factory:
                mock_engine = MagicMock()
                mock_conn = AsyncMock()
                mock_conn.execute.side_effect = Exception("SQL Error")
                mock_engine.begin.return_value.__aenter__.return_value = mock_conn
                mock_factory.return_value = mock_engine
                
                result = await _run_sql("INVALID SQL", db_path=temp_file.name)
                
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
            with patch('src.pyomop.mcp.server._get_engine') as mock_get_engine:
                with patch('src.pyomop.mcp.server.CdmEngineFactory') as mock_factory:
                    mock_engine = MagicMock()
                    mock_get_engine.return_value = mock_engine
                    
                    # Mock successful CDM creation
                    mock_cdm = MagicMock()
                    mock_cdm._engine = mock_engine
                    mock_cdm.init_models = AsyncMock()
                    mock_factory.return_value = mock_cdm
                    
                    with patch('src.pyomop.cdm54.Base') as mock_base:
                        mock_base.metadata = MagicMock()
                        
                        # Test CDM creation
                        result = await _create_cdm(str(db_path), "cdm54")
                        assert "Successfully created CDM" in result[0].text

    def test_mcp_server_import(self):
        """Test that MCP server can be imported."""
        from src.pyomop.mcp import mcp_server_main
        assert callable(mcp_server_main)


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
class TestNewMCPTools:
    """Test new MCP tools: example_query and check_sql."""

    @pytest.mark.asyncio
    async def test_example_query_person(self):
        """Test example_query tool for person table."""
        with patch('requests.get') as mock_get:
            # Mock the requests to return sample content
            mock_response = Mock()
            mock_response.text = "# Example Query\nSELECT * FROM person;"
            mock_get.return_value = mock_response
            
            result = await _example_query("person")
            
            assert len(result) == 1
            assert "Example Query" in result[0].text
            assert mock_get.call_count == 2  # PE02 and PE03

    @pytest.mark.asyncio
    async def test_example_query_condition_occurrence(self):
        """Test example_query tool for condition_occurrence table."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.text = "# Condition Query\nSELECT * FROM condition_occurrence;"
            mock_get.return_value = mock_response
            
            result = await _example_query("condition_occurrence")
            
            assert len(result) == 1
            assert "Condition Query" in result[0].text
            assert mock_get.call_count == 2  # CO01 and CO05

    @pytest.mark.asyncio
    async def test_example_query_error_handling(self):
        """Test example_query tool error handling."""
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("Network error")
            
            result = await _example_query("person")
            
            assert len(result) == 1
            assert "Error fetching example queries" in result[0].text
            assert "Network error" in result[0].text

    @pytest.mark.asyncio
    async def test_example_query_no_examples(self):
        """Test example_query tool when no examples are returned."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.text = ""
            mock_get.return_value = mock_response
            
            result = await _example_query("person")
            
            assert len(result) == 1
            assert "No example queries found" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_basic_validation(self):
        """Test check_sql tool with basic validation (no LLM)."""
        # Patch the import to make it fail
        with patch.dict('sys.modules', {'langchain_community.tools.sql_database.tool': None}):
            # Test valid SQL
            result = await _check_sql("SELECT * FROM person")
            assert len(result) == 1
            assert "Basic SQL validation passed" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_invalid_parentheses(self):
        """Test check_sql with unbalanced parentheses."""
        with patch.dict('sys.modules', {'langchain_community.tools.sql_database.tool': None}):
            result = await _check_sql("SELECT * FROM person WHERE (id = 1")
            
            assert len(result) == 1
            assert "Unbalanced parentheses" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_invalid_quotes(self):
        """Test check_sql with unbalanced quotes."""
        with patch.dict('sys.modules', {'langchain_community.tools.sql_database.tool': None}):
            result = await _check_sql("SELECT * FROM person WHERE name = 'John")
            
            assert len(result) == 1
            assert "Unbalanced single quotes" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_no_keywords(self):
        """Test check_sql with no SQL keywords."""
        with patch.dict('sys.modules', {'langchain_community.tools.sql_database.tool': None}):
            result = await _check_sql("INVALID STATEMENT")
            
            assert len(result) == 1
            assert "doesn't contain a valid SQL statement keyword" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_with_llm(self):
        """Test check_sql with LLM features available."""
        # Skip if LLM dependencies are not available
        pytest.importorskip("langchain_community.tools.sql_database.tool")
        
        with patch('src.pyomop.mcp.server._get_engine') as mock_engine:
            with patch('src.pyomop.llm_engine.CDMDatabase') as mock_cdm_db:
                with patch('langchain_community.tools.sql_database.tool.QuerySQLCheckerTool') as mock_checker:
                    # Mock the checker tool
                    mock_checker_instance = Mock()
                    mock_checker_instance.run.return_value = "Query is valid"
                    mock_checker.return_value = mock_checker_instance
                    
                    # Mock CDMDatabase
                    mock_cdm_db.return_value = Mock()
                    
                    # Mock engine
                    mock_engine.return_value = Mock()
                    
                    result = await _check_sql("SELECT * FROM person")
                    
                    assert len(result) == 1
                    assert "Query is valid" in result[0].text

    @pytest.mark.asyncio
    async def test_check_sql_error_handling(self):
        """Test check_sql error handling."""
        with patch('src.pyomop.mcp.server._get_engine') as mock_engine:
            mock_engine.side_effect = Exception("Database error")
            
            result = await _check_sql("SELECT * FROM person")
            
            assert len(result) == 1
            assert "Error checking SQL" in result[0].text
            assert "Database error" in result[0].text


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
class TestHTTPTransport:
    """Test HTTP transport functionality."""

    def test_http_transport_imports(self):
        """Test that HTTP transport modules can be imported."""
        try:
            from src.pyomop.mcp.server import main_http, main_http_cli
            assert callable(main_http)
            assert callable(main_http_cli)
        except ImportError:
            pytest.skip("HTTP transport dependencies not available")

    def test_http_cli_entry_point(self):
        """Test that HTTP CLI entry point exists."""
        from src.pyomop.mcp.server import main_http_cli
        assert callable(main_http_cli)