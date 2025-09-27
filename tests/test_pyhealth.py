"""Tests for PyHealth export functionality."""

import asyncio
import os
import tempfile
from unittest import mock
from unittest.mock import Mock, PropertyMock

import pytest


class DummyEngine:
    """Mock engine for testing purposes."""
    
    def __init__(self):
        self.begin_called = False

        class DummyURL:
            def get_backend_name(self):
                return "sqlite"

        self.url = DummyURL()

    def begin(self):
        self.begin_called = True

        class DummyConn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

            async def run_sync(self, fn):
                # Simulate table list 
                if hasattr(fn, "__name__") and fn.__name__ == "get_available_tables":
                    return {
                        "person", "visit_occurrence", "death", "condition_occurrence",
                        "procedure_occurrence", "drug_exposure", "measurement", "other_table"
                    }
                # Simulate DataFrame fetch
                if hasattr(fn, "__name__") and fn.__name__ == "fetch_df":
                    import pandas as pd
                    return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
                return None

        return DummyConn()


class DummyCdmEngineFactory:
    """Mock CDM engine factory for testing."""
    
    def __init__(self, engine=None):
        self._engine = engine or DummyEngine()
    
    @property
    def engine(self):
        return self._engine


def test_init_defaults():
    """Test PyHealthExport initialization with defaults."""
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine = DummyCdmEngineFactory()
    
    # Test default export path (current working directory)
    with mock.patch.dict(os.environ, {}, clear=True):
        exporter = PyHealthExport(cdm_engine)
        assert exporter.cdm_engine == cdm_engine
        assert exporter.export_path == os.getcwd()


def test_init_with_env_var():
    """Test PyHealthExport initialization with environment variable."""
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine = DummyCdmEngineFactory()
    test_path = "/tmp/pyhealth_data"
    
    with mock.patch.dict(os.environ, {"PYHEALTH_DATA_FOLDER": test_path}):
        exporter = PyHealthExport(cdm_engine)
        assert exporter.export_path == test_path


def test_init_with_explicit_path():
    """Test PyHealthExport initialization with explicit path."""
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine = DummyCdmEngineFactory()
    test_path = "/tmp/explicit_path"
    
    exporter = PyHealthExport(cdm_engine, export_path=test_path)
    assert exporter.export_path == test_path


def test_getters_and_setters():
    """Test property getters and setters."""
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine1 = DummyCdmEngineFactory()
    cdm_engine2 = DummyCdmEngineFactory()
    exporter = PyHealthExport(cdm_engine1, "/tmp/test")
    
    # Test initial values
    assert exporter.cdm_engine == cdm_engine1
    assert exporter.export_path == "/tmp/test"
    
    # Test setters
    exporter.cdm_engine = cdm_engine2
    exporter.export_path = "/tmp/new_path"
    
    assert exporter.cdm_engine == cdm_engine2
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
        "measurement"
    ]
    
    assert PyHealthExport.PYHEALTH_TABLES == expected_tables


@pytest.mark.asyncio
async def test_export_success(tmp_path):
    """Test successful export of PyHealth tables."""
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine = DummyCdmEngineFactory()
    exporter = PyHealthExport(cdm_engine, str(tmp_path))
    
    # Mock pandas DataFrame to_csv method
    with mock.patch("pandas.read_sql_query") as mock_read_sql:
        import pandas as pd
        
        # Create a mock DataFrame
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=2)  # For verbose logging
        mock_read_sql.return_value = mock_df
        
        exported_files = await exporter.export(verbose=True)
        
        # Should export all 7 tables
        assert len(exported_files) == 7
        
        # Check that all expected files are created
        expected_files = [
            "person.csv", "visit_occurrence.csv", "death.csv",
            "condition_occurrence.csv", "procedure_occurrence.csv",
            "drug_exposure.csv", "measurement.csv"
        ]
        
        for expected_file in expected_files:
            expected_path = tmp_path / expected_file
            assert str(expected_path) in exported_files
        
        # Verify to_csv was called for each table
        assert mock_df.to_csv.call_count == 7


@pytest.mark.asyncio
async def test_export_no_engine():
    """Test export fails when engine is not initialized."""
    from src.pyomop.pyhealth import PyHealthExport
    
    # Create CDM engine factory with None engine
    cdm_engine = DummyCdmEngineFactory(engine=None)
    exporter = PyHealthExport(cdm_engine, "/tmp/test")
    
    with pytest.raises(RuntimeError, match="CDM engine is not initialized"):
        await exporter.export()


@pytest.mark.asyncio
async def test_export_missing_tables(tmp_path):
    """Test export handles missing tables gracefully."""
    from src.pyomop.pyhealth import PyHealthExport
    
    # Create engine that only has some tables
    class PartialEngine(DummyEngine):
        def begin(self):
            self.begin_called = True

            class DummyConn:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    pass

                async def run_sync(self, fn):
                    # Only return a subset of tables
                    if hasattr(fn, "__name__") and fn.__name__ == "get_available_tables":
                        return {"person", "visit_occurrence"}  # Missing most tables
                    if hasattr(fn, "__name__") and fn.__name__ == "fetch_df":
                        import pandas as pd
                        return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
                    return None

            return DummyConn()
    
    cdm_engine = DummyCdmEngineFactory(engine=PartialEngine())
    exporter = PyHealthExport(cdm_engine, str(tmp_path))
    
    with mock.patch("pandas.read_sql_query") as mock_read_sql:
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=2)
        mock_read_sql.return_value = mock_df
        
        exported_files = await exporter.export(verbose=True)
        
        # Should only export the 2 available tables
        assert len(exported_files) == 2
        assert any("person.csv" in f for f in exported_files)
        assert any("visit_occurrence.csv" in f for f in exported_files)


@pytest.mark.asyncio
async def test_export_creates_directory():
    """Test that export creates the target directory if it doesn't exist."""
    from src.pyomop.pyhealth import PyHealthExport
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a path that doesn't exist yet
        export_path = os.path.join(tmp_dir, "new_directory")
        assert not os.path.exists(export_path)
        
        cdm_engine = DummyCdmEngineFactory()
        exporter = PyHealthExport(cdm_engine, export_path)
        
        with mock.patch("pandas.read_sql_query") as mock_read_sql:
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=0)
            mock_read_sql.return_value = mock_df
            
            await exporter.export()
            
            # Directory should now exist
            assert os.path.exists(export_path)
            assert os.path.isdir(export_path)


@pytest.mark.asyncio
async def test_export_verbose_logging():
    """Test verbose logging output during export.""" 
    from src.pyomop.pyhealth import PyHealthExport
    
    cdm_engine = DummyCdmEngineFactory()
    exporter = PyHealthExport(cdm_engine, "/tmp/test")
    
    with mock.patch("pandas.read_sql_query") as mock_read_sql:
        with mock.patch("src.pyomop.pyhealth._logger") as mock_logger:
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=5)
            mock_read_sql.return_value = mock_df
            
            await exporter.export(verbose=True)
            
            # Should have logged available tables and export progress
            assert mock_logger.info.called
            # Check some expected log messages
            log_calls = [call.args[0] for call in mock_logger.info.call_args_list]
            assert any("Available tables:" in msg for msg in log_calls)
            assert any("Exporting 7 PyHealth tables" in msg for msg in log_calls)
            assert any("Export complete" in msg for msg in log_calls)


def test_import_from_module():
    """Test that PyHealthExport can be imported from the module."""
    from src.pyomop.pyhealth import PyHealthExport
    
    # Should be able to create an instance
    cdm_engine = DummyCdmEngineFactory()
    exporter = PyHealthExport(cdm_engine)
    assert exporter is not None
    assert hasattr(exporter, 'export')
    assert hasattr(exporter, 'cdm_engine')
    assert hasattr(exporter, 'export_path')