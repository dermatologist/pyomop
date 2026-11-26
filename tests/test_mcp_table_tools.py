"""The MCP table tools accept the ``version`` argument from their input schema."""

import pytest
from mcp.types import CallToolRequestParams

from src.pyomop.mcp.server import _create_cdm, handle_call_tool


async def _call(name, arguments):
    result = await handle_call_tool(None, CallToolRequestParams(name=name, arguments=arguments))
    return result.is_error, result.content[0].text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name, arguments",
    [
        ("get_table_columns", {"table_name": "person"}),
        ("get_table_columns", {"table_name": "person", "version": "cdm54"}),
        ("get_single_table_info", {"table_name": "person", "version": "cdm54"}),
        ("get_usable_table_names", {}),
    ],
)
async def test_table_tools_run(tmp_path, name, arguments):
    db_path = str(tmp_path / "cdm.sqlite")
    await _create_cdm(db_path)

    is_error, text = await _call(name, {**arguments, "db_path": db_path})

    assert not is_error, text
    assert "Error" not in text, text


@pytest.mark.asyncio
async def test_table_tools_return_omop_schema(tmp_path):
    pytest.importorskip("langchain_community")
    db_path = str(tmp_path / "cdm.sqlite")
    await _create_cdm(db_path)

    _, columns = await _call("get_table_columns", {"table_name": "person", "version": "cdm54", "db_path": db_path})
    _, info = await _call("get_single_table_info", {"table_name": "person", "db_path": db_path})
    _, tables = await _call("get_usable_table_names", {"db_path": db_path})

    assert columns.startswith("Columns for table 'person': person_id, gender_concept_id")
    assert info.startswith("Table 'person' has columns: person_id (INTEGER)")
    assert "condition_occurrence" in tables.split(": ", 1)[1].split(", ")
