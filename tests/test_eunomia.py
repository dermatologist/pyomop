"""Tests for Eunomia data functionality."""

import asyncio
import os
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from pyomop.eunomia import EunomiaData, download_eunomia_data, extract_load_data, export_data_files, load_data_files


class TestEunomiaData:
    """Test cases for EunomiaData class."""

    def test_init_default(self):
        """Test EunomiaData initialization with defaults."""
        eunomia = EunomiaData()
        assert eunomia.cdm is not None
        assert "EunomiaDatasets" in eunomia.base_url

    def test_init_with_custom_url(self):
        """Test EunomiaData initialization with custom URL."""
        custom_url = "https://example.com/datasets"
        eunomia = EunomiaData(base_url=custom_url)
        assert eunomia.base_url == custom_url

    def test_download_eunomia_data_empty_dataset_name(self):
        """Test download with empty dataset name raises ValueError."""
        eunomia = EunomiaData()
        with pytest.raises(ValueError, match="dataset_name argument must be specified"):
            eunomia.download_eunomia_data("")

    def test_download_eunomia_data_none_dataset_name(self):
        """Test download with None dataset name raises ValueError."""
        eunomia = EunomiaData()
        with pytest.raises(ValueError, match="dataset_name argument must be specified"):
            eunomia.download_eunomia_data(None)

    @patch('requests.get')
    def test_download_eunomia_data_success(self, mock_get):
        """Test successful download of dataset."""
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"test data chunk"]
        mock_get.return_value = mock_response

        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = eunomia.download_eunomia_data(
                dataset_name="GiBleed",
                path_to_data=temp_dir,
                verbose=True
            )
            
            expected_path = os.path.join(temp_dir, "GiBleed_5.3.zip")
            assert result == expected_path
            assert os.path.exists(expected_path)
            
            # Check that the correct URL was requested
            expected_url = f"{eunomia.base_url}/GiBleed/GiBleed_5.3.zip"
            mock_get.assert_called_once_with(expected_url, stream=True, timeout=30)

    @patch('requests.get')
    def test_download_eunomia_data_file_exists_no_overwrite(self, mock_get):
        """Test download when file exists and overwrite=False."""
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create existing file
            existing_file = os.path.join(temp_dir, "GiBleed_5.3.zip")
            with open(existing_file, 'w') as f:
                f.write("existing content")
            
            result = eunomia.download_eunomia_data(
                dataset_name="GiBleed",
                path_to_data=temp_dir,
                overwrite=False,
                verbose=True
            )
            
            assert result == existing_file
            # Should not have made any HTTP request
            mock_get.assert_not_called()

    @patch('requests.get')
    def test_download_eunomia_data_network_error(self, mock_get):
        """Test download with network error."""
        mock_get.side_effect = requests.RequestException("Network error")
        
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(requests.RequestException):
                eunomia.download_eunomia_data(
                    dataset_name="GiBleed",
                    path_to_data=temp_dir
                )

    def test_extract_load_data_invalid_file_extension(self):
        """Test extract_load_data with non-ZIP file raises ValueError."""
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_file = os.path.join(temp_dir, "test.txt")
            with open(invalid_file, 'w') as f:
                f.write("not a zip")
            
            with pytest.raises(ValueError, match="Source must be a .zip file"):
                asyncio.run(eunomia.extract_load_data(
                    from_path=invalid_file,
                    to_path=os.path.join(temp_dir, "test.db")
                ))

    def test_extract_load_data_file_not_found(self):
        """Test extract_load_data with non-existent file raises FileNotFoundError."""
        eunomia = EunomiaData()
        
        with pytest.raises(FileNotFoundError, match="not found"):
            asyncio.run(eunomia.extract_load_data(
                from_path="/nonexistent/file.zip",
                to_path="/tmp/test.db"
            ))

    async def test_load_data_files_invalid_format(self):
        """Test load_data_files with invalid input format raises ValueError."""
        eunomia = EunomiaData()
        
        with pytest.raises(ValueError, match="Unsupported input format"):
            await eunomia.load_data_files(
                data_path="/tmp",
                db_path="/tmp/test.db",
                input_format="invalid"
            )

    async def test_load_data_files_invalid_dbms(self):
        """Test load_data_files with invalid dbms raises ValueError."""
        eunomia = EunomiaData()
        
        with pytest.raises(ValueError, match="Unsupported dbms"):
            await eunomia.load_data_files(
                data_path="/tmp",
                db_path="/tmp/test.db",
                dbms="invalid"
            )

    async def test_load_data_files_nonexistent_path(self):
        """Test load_data_files with non-existent path raises FileNotFoundError."""
        eunomia = EunomiaData()
        
        with pytest.raises(FileNotFoundError, match="Data path does not exist"):
            await eunomia.load_data_files(
                data_path="/nonexistent/path",
                db_path="/tmp/test.db"
            )

    async def test_load_data_files_no_data_files(self):
        """Test load_data_files with directory containing no data files."""
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some non-data files
            with open(os.path.join(temp_dir, "readme.txt"), 'w') as f:
                f.write("not a data file")
            
            with pytest.raises(FileNotFoundError, match="does not contain csv files"):
                await eunomia.load_data_files(
                    data_path=temp_dir,
                    db_path=os.path.join(temp_dir, "test.db")
                )

    async def test_load_data_files_csv_success(self):
        """Test successful loading of CSV files."""
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test CSV files
            person_data = {
                'person_id': [1, 2],
                'gender_concept_id': [8532, 8507],
                'year_of_birth': [1980, 1990]
            }
            person_df = pd.DataFrame(person_data)
            person_df.to_csv(os.path.join(temp_dir, "person.csv"), index=False)
            
            observation_data = {
                'observation_id': [1, 2],
                'person_id': [1, 2],
                'observation_concept_id': [4013886, 4013887]
            }
            observation_df = pd.DataFrame(observation_data)
            observation_df.to_csv(os.path.join(temp_dir, "observation.csv"), index=False)
            
            db_path = os.path.join(temp_dir, "test.db")
            
            await eunomia.load_data_files(
                data_path=temp_dir,
                db_path=db_path,
                verbose=True
            )
            
            # Verify database was created and contains data
            assert os.path.exists(db_path)
            
            # Check data was loaded correctly
            import sqlite3
            conn = sqlite3.connect(db_path)
            try:
                cursor = conn.cursor()
                
                # Check person table
                cursor.execute("SELECT COUNT(*) FROM person")
                assert cursor.fetchone()[0] == 2
                
                # Check observation table
                cursor.execute("SELECT COUNT(*) FROM observation")
                assert cursor.fetchone()[0] == 2
                
            finally:
                conn.close()

    async def test_export_data_files_invalid_format(self):
        """Test export_data_files with invalid output format raises ValueError."""
        eunomia = EunomiaData()
        
        with pytest.raises(ValueError, match="Unsupported output format"):
            await eunomia.export_data_files(
                db_path="/tmp/test.db",
                output_folder="/tmp",
                output_format="invalid"
            )

    async def test_export_data_files_invalid_dbms(self):
        """Test export_data_files with invalid dbms raises ValueError."""
        eunomia = EunomiaData()
        
        with pytest.raises(ValueError, match="Unsupported dbms"):
            await eunomia.export_data_files(
                db_path="/tmp/test.db",
                output_folder="/tmp",
                dbms="invalid"
            )

    async def test_export_data_files_nonexistent_db(self):
        """Test export_data_files with non-existent database raises FileNotFoundError."""
        eunomia = EunomiaData()
        
        with pytest.raises(FileNotFoundError, match="Database file does not exist"):
            await eunomia.export_data_files(
                db_path="/nonexistent/test.db",
                output_folder="/tmp"
            )

    async def test_export_data_files_csv_success(self):
        """Test successful export of data files to CSV."""
        eunomia = EunomiaData()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create and populate a test database
            db_path = os.path.join(temp_dir, "test.db")
            
            import sqlite3
            conn = sqlite3.connect(db_path)
            try:
                cursor = conn.cursor()
                
                # Create and populate test table
                cursor.execute("""
                    CREATE TABLE person (
                        person_id INTEGER,
                        gender_concept_id INTEGER,
                        year_of_birth INTEGER
                    )
                """)
                
                cursor.execute("""
                    INSERT INTO person (person_id, gender_concept_id, year_of_birth)
                    VALUES (1, 8532, 1980), (2, 8507, 1990)
                """)
                
                conn.commit()
                
            finally:
                conn.close()
            
            output_folder = os.path.join(temp_dir, "output")
            
            await eunomia.export_data_files(
                db_path=db_path,
                output_folder=output_folder,
                output_format="csv",
                verbose=True
            )
            
            # Verify CSV file was created
            csv_file = os.path.join(output_folder, "person.csv")
            assert os.path.exists(csv_file)
            
            # Verify content
            exported_df = pd.read_csv(csv_file)
            assert len(exported_df) == 2
            assert list(exported_df.columns) == ['person_id', 'gender_concept_id', 'year_of_birth']


class TestConvenienceFunctions:
    """Test cases for convenience functions."""

    @patch('pyomop.eunomia.EunomiaData.download_eunomia_data')
    def test_download_eunomia_data_convenience(self, mock_download):
        """Test download_eunomia_data convenience function."""
        mock_download.return_value = "/path/to/file.zip"
        
        result = download_eunomia_data("GiBleed", verbose=True)
        
        assert result == "/path/to/file.zip"
        mock_download.assert_called_once_with(
            dataset_name="GiBleed",
            cdm_version="5.3",
            path_to_data=None,
            overwrite=False,
            verbose=True
        )

    @patch('pyomop.eunomia.EunomiaData.extract_load_data')
    async def test_extract_load_data_convenience(self, mock_extract):
        """Test extract_load_data convenience function."""
        await extract_load_data(
            from_path="/path/to/file.zip",
            to_path="/path/to/db.sqlite",
            verbose=True
        )
        
        mock_extract.assert_called_once_with(
            from_path="/path/to/file.zip",
            to_path="/path/to/db.sqlite",
            dbms="sqlite",
            cdm_version="5.3",
            input_format="csv",
            verbose=True
        )

    @patch('pyomop.eunomia.EunomiaData.load_data_files')
    async def test_load_data_files_convenience(self, mock_load):
        """Test load_data_files convenience function."""
        await load_data_files(
            data_path="/path/to/data",
            db_path="/path/to/db.sqlite",
            verbose=True
        )
        
        mock_load.assert_called_once_with(
            data_path="/path/to/data",
            db_path="/path/to/db.sqlite",
            input_format="csv",
            cdm_version="5.3",
            cdm_database_schema="main",
            dbms="sqlite",
            verbose=True,
            overwrite=False
        )

    @patch('pyomop.eunomia.EunomiaData.export_data_files')
    async def test_export_data_files_convenience(self, mock_export):
        """Test export_data_files convenience function."""
        await export_data_files(
            db_path="/path/to/db.sqlite",
            output_folder="/path/to/output",
            verbose=True
        )
        
        mock_export.assert_called_once_with(
            db_path="/path/to/db.sqlite",
            output_folder="/path/to/output",
            output_format="csv",
            dbms="sqlite",
            verbose=True
        )


class TestIntegration:
    """Integration tests for the full workflow."""

    async def test_full_workflow_with_mock_data(self):
        """Test complete workflow from download to export with mock data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock ZIP file with CSV data
            zip_path = os.path.join(temp_dir, "GiBleed_5.3.zip")
            csv_dir = os.path.join(temp_dir, "csv_data")
            os.makedirs(csv_dir)
            
            # Create sample CSV files
            person_data = {
                'person_id': [1, 2, 3],
                'gender_concept_id': [8532, 8507, 8532],
                'year_of_birth': [1980, 1990, 1985]
            }
            person_df = pd.DataFrame(person_data)
            person_df.to_csv(os.path.join(csv_dir, "person.csv"), index=False)
            
            concept_data = {
                'concept_id': [8532, 8507],
                'concept_name': ['FEMALE', 'MALE'],
                'domain_id': ['Gender', 'Gender']
            }
            concept_df = pd.DataFrame(concept_data)
            concept_df.to_csv(os.path.join(csv_dir, "concept.csv"), index=False)
            
            # Create ZIP file
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file_path in Path(csv_dir).glob("*.csv"):
                    zipf.write(file_path, file_path.name)
            
            # Test extract and load
            db_path = os.path.join(temp_dir, "test.db")
            await extract_load_data(
                from_path=zip_path,
                to_path=db_path,
                verbose=True
            )
            
            # Verify database was created
            assert os.path.exists(db_path)
            
            # Test export
            output_dir = os.path.join(temp_dir, "exported")
            await export_data_files(
                db_path=db_path,
                output_folder=output_dir,
                verbose=True
            )
            
            # Verify exported files
            assert os.path.exists(os.path.join(output_dir, "person.csv"))
            assert os.path.exists(os.path.join(output_dir, "concept.csv"))
            
            # Verify data integrity
            exported_person = pd.read_csv(os.path.join(output_dir, "person.csv"))
            assert len(exported_person) == 3
            assert list(exported_person.columns) == ['person_id', 'gender_concept_id', 'year_of_birth']