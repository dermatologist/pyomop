"""Tests for PyHealth export functionality."""

import os
import tempfile
from unittest import mock
from unittest.mock import Mock, AsyncMock

import pytest

# Constants for testing
TEST_TABLES = {
    "person",
    "visit_occurrence",
    "death",
    "condition_occurrence",
    "procedure_occurrence",
    "drug_exposure",
    "measurement",
}


@pytest.fixture
def mock_cdm():
    """Fixture for a mocked CDM engine factory."""
    cdm = Mock()
    engine = Mock()

    # Setup async connection mock
    connection = AsyncMock()
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock(return_value=None)

    # Setup run_sync to handle different functions
    async def side_effect_run_sync(fn):
        if fn.__name__ == "get_available_tables":
            return TEST_TABLES
        if fn.__name__ == "fetch_df":
            # Return a mock dataframe
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=2)
            mock_df.to_csv = Mock()
            return mock_df
        return None

    connection.run_sync = AsyncMock(side_effect=side_effect_run_sync)
    engine.begin.return_value = connection
    cdm.engine = engine
    return cdm


def test_init_defaults(mock_cdm):
    """Test PyHealthExport initialization with defaults."""
    from src.pyomop.pyhealth import PyHealthExport

    # Test default export path (current working directory)
    with mock.patch.dict(os.environ, {}, clear=True):
        exporter = PyHealthExport(mock_cdm)
        assert exporter.cdm == mock_cdm
        assert exporter.export_path == os.getcwd()


def test_init_with_env_var(mock_cdm):
    """Test PyHealthExport initialization with environment variable."""
    from src.pyomop.pyhealth import PyHealthExport

    test_path = "/tmp/pyhealth_data"

    with mock.patch.dict(os.environ, {"PYHEALTH_DATA_FOLDER": test_path}):
        exporter = PyHealthExport(mock_cdm)
        assert exporter.export_path == test_path


def test_init_with_explicit_path(mock_cdm):
    """Test PyHealthExport initialization with explicit path."""
    from src.pyomop.pyhealth import PyHealthExport

    test_path = "/tmp/explicit_path"

    exporter = PyHealthExport(mock_cdm, export_path=test_path)
    assert exporter.export_path == test_path


def test_getters_and_setters(mock_cdm):
    """Test property getters and setters."""
    from src.pyomop.pyhealth import PyHealthExport

    cdm2 = Mock()
    exporter = PyHealthExport(mock_cdm, "/tmp/test")

    # Test initial values
    assert exporter.cdm == mock_cdm
    assert exporter.export_path == "/tmp/test"

    # Test setters
    exporter.cdm = cdm2
    exporter.export_path = "/tmp/new_path"

    assert exporter.cdm == cdm2
    assert exporter.export_path == "/tmp/new_path"


def test_pyhealth_tables_constant():
    """Test that PYHEALTH_TABLES constant contains expected tables."""
    from src.pyomop.pyhealth import PyHealthExport

    expected_tables = [
        "person",
        "visit_occurrence",
        "death",
        "condition_occurrence",
        "procedure_occurrence",
        "drug_exposure",
        "measurement",
    ]

    assert PyHealthExport.PYHEALTH_TABLES == expected_tables


@pytest.mark.asyncio
async def test_export_success(mock_cdm, tmp_path):
    """Test successful export of PyHealth tables."""
    from src.pyomop.pyhealth import PyHealthExport

    # Mock pandas read_sql_query to avoid pandas overhead and dependency
    with mock.patch("pandas.read_sql_query") as mock_read_sql:
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=2)
        mock_df.to_csv = Mock()
        mock_read_sql.return_value = mock_df

        # Override run_sync side effect for this specific test to return our mock_df
        async def fetch_df_side_effect(fn):
            if fn.__name__ == "get_available_tables":
                return TEST_TABLES
            if fn.__name__ == "fetch_df":
                return mock_df
            return None

        mock_cdm.engine.begin.return_value.__aenter__.return_value.run_sync.side_effect = fetch_df_side_effect

        exporter = PyHealthExport(mock_cdm, str(tmp_path))
        exported_files = await exporter.export(verbose=True)

        # Should export all 7 tables
        assert len(exported_files) == 7

        # Check that all expected files are in the list
        expected_files = [f"{table}.csv" for table in PyHealthExport.PYHEALTH_TABLES]
        for expected_file in expected_files:
            expected_path = tmp_path / expected_file
            assert str(expected_path) in exported_files

        # Verify to_csv was called for each table
        assert mock_df.to_csv.call_count == 7


@pytest.mark.asyncio
async def test_export_no_engine():
    """Test export fails when engine is not initialized."""
    from src.pyomop.pyhealth import PyHealthExport

    mock_cdm_none = Mock()
    mock_cdm_none.engine = None
    exporter = PyHealthExport(mock_cdm_none, "/tmp/test")

    with pytest.raises(RuntimeError, match="CDM engine is not initialized"):
        await exporter.export()


@pytest.mark.asyncio
async def test_export_missing_tables(mock_cdm, tmp_path):
    """Test export handles missing tables gracefully."""
    from src.pyomop.pyhealth import PyHealthExport

    # Setup mock to return only a subset of tables
    async def missing_tables_side_effect(fn):
        if fn.__name__ == "get_available_tables":
            return {"person", "visit_occurrence"}
        if fn.__name__ == "fetch_df":
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=2)
            mock_df.to_csv = Mock()
            return mock_df
        return None

    mock_cdm.engine.begin.return_value.__aenter__.return_value.run_sync.side_effect = missing_tables_side_effect

    exporter = PyHealthExport(mock_cdm, str(tmp_path))

    with mock.patch("pandas.read_sql_query") as mock_read_sql:
        mock_df = Mock()
        mock_read_sql.return_value = mock_df

        exported_files = await exporter.export(verbose=True)

        # Should only export the 2 available tables
        assert len(exported_files) == 2
        assert any("person.csv" in f for f in exported_files)
        assert any("visit_occurrence.csv" in f for f in exported_files)


@pytest.mark.asyncio
async def test_export_creates_directory(mock_cdm):
    """Test that export creates the target directory if it doesn't exist."""
    from src.pyomop.pyhealth import PyHealthExport

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a path that doesn't exist yet
        export_path = os.path.join(tmp_dir, "new_directory")
        assert not os.path.exists(export_path)

        exporter = PyHealthExport(mock_cdm, export_path)

        with mock.patch("pandas.read_sql_query"):
            await exporter.export()

            # Directory should now exist
            assert os.path.exists(export_path)
            assert os.path.isdir(export_path)


@pytest.mark.asyncio
async def test_export_verbose_logging(mock_cdm):
    """Test verbose logging output during export."""
    from src.pyomop.pyhealth import PyHealthExport

    exporter = PyHealthExport(mock_cdm, "/tmp/test")

    with mock.patch("pandas.read_sql_query"):
        with mock.patch("src.pyomop.pyhealth._logger") as mock_logger:
            await exporter.export(verbose=True)

            # Should have logged available tables and export progress
            assert mock_logger.info.called
            # Check some expected log messages
            log_calls = [call.args[0] for call in mock_logger.info.call_args_list]
            assert any("Available tables:" in msg for msg in log_calls)
            assert any("Exporting 7 PyHealth tables" in msg for msg in log_calls)
            assert any("Export complete" in msg for msg in log_calls)


def test_import_from_module(mock_cdm):
    """Test that PyHealthExport can be imported from the module."""
    from src.pyomop.pyhealth import PyHealthExport

    # Should be able to create an instance
    exporter = PyHealthExport(mock_cdm)
    assert exporter is not None
    assert hasattr(exporter, "export")
    assert hasattr(exporter, "cdm")
    assert hasattr(exporter, "export_path")
