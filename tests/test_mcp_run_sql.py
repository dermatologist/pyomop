"""run_sql returns rows for any statement that produces them, not only SELECT."""

import pytest

# Skip tests if MCP is not available
try:
    from src.pyomop.mcp.server import _create_cdm, _run_sql

    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT person_id, year_of_birth FROM person",
        "WITH p AS (SELECT person_id, year_of_birth FROM person) SELECT * FROM p",
        "-- persons born in 1970\nSELECT person_id, year_of_birth FROM person",
    ],
)
async def test_run_sql_returns_rows(tmp_path, sql):
    db_path = str(tmp_path / "cdm.sqlite")
    await _create_cdm(db_path)
    await _run_sql(
        "INSERT INTO person (person_id, gender_concept_id, year_of_birth,"
        " race_concept_id, ethnicity_concept_id) VALUES (1, 8507, 1970, 0, 0)",
        db_path=db_path,
    )

    text = (await _run_sql(sql, db_path=db_path))[0].text

    assert text.startswith("Query executed successfully. Found 1 rows."), text
    assert "{'person_id': 1, 'year_of_birth': 1970}" in text


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP not available")
@pytest.mark.asyncio
async def test_run_sql_insert_reports_statement(tmp_path):
    db_path = str(tmp_path / "cdm.sqlite")
    await _create_cdm(db_path)

    text = (
        await _run_sql(
            "INSERT INTO person (person_id, gender_concept_id, year_of_birth,"
            " race_concept_id, ethnicity_concept_id) VALUES (1, 8507, 1970, 0, 0)",
            db_path=db_path,
        )
    )[0].text

    assert text.startswith("SQL statement executed successfully"), text
