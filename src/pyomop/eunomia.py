"""OMOP CDM sample data sets functionality.

This module provides functionality to download, extract, and load OMOP CDM
sample datasets from the OHDSI EunomiaDatasets repository. This is a Python
port of the Eunomia R package functionality.
"""

import asyncio
import logging
import os
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from sqlalchemy import text

from .engine_factory import CdmEngineFactory

_logger = logging.getLogger(__name__)


class EunomiaData:
    """Provides access to OMOP CDM sample datasets.
    
    This class offers functionality to download, extract, and load sample
    OMOP CDM datasets from the OHDSI EunomiaDatasets repository.
    """

    def __init__(
        self,
        cdm_engine_factory: Optional[CdmEngineFactory] = None,
        base_url: Optional[str] = None,
    ):
        """Initialize EunomiaData.
        
        Args:
            cdm_engine_factory: CDM engine factory instance. If None, creates SQLite engine.
            base_url: Base URL for dataset downloads. Defaults to EunomiaDatasets repository.
        """
        self.cdm = cdm_engine_factory or CdmEngineFactory()
        self.base_url = base_url or os.getenv(
            "EUNOMIA_DATASETS_URL",
            "https://raw.githubusercontent.com/OHDSI/EunomiaDatasets/main/datasets"
        )

    def download_eunomia_data(
        self,
        dataset_name: str,
        cdm_version: str = "5.3",
        path_to_data: Optional[str] = None,
        overwrite: bool = False,
        verbose: bool = False,
    ) -> str:
        """Download Eunomia data files.
        
        Download the Eunomia data files from https://github.com/OHDSI/EunomiaDatasets
        
        Args:
            dataset_name: The data set name as found on EunomiaDatasets repository.
                         The data set name corresponds to the folder with the data set ZIP files.
            cdm_version: The OMOP CDM version. This version will appear in the suffix of
                        the data file, for example: <datasetName>_<cdmVersion>.zip. Default: '5.3'
            path_to_data: The path where the Eunomia data is stored on the file system.
                         By default the value of the environment variable "EUNOMIA_DATA_FOLDER" is used.
            overwrite: Control whether the existing archive file will be overwritten should it
                      already exist.
            verbose: Provide additional logging details during execution.
            
        Returns:
            The path to the downloaded file.
            
        Raises:
            ValueError: If dataset_name is empty or None.
            requests.RequestException: If download fails.
        """
        if not dataset_name or dataset_name.strip() == "":
            raise ValueError("The dataset_name argument must be specified.")

        if path_to_data is None or path_to_data.strip() == "":
            path_to_data = os.getenv("EUNOMIA_DATA_FOLDER")
            if not path_to_data:
                path_to_data = tempfile.gettempdir()
                if verbose:
                    _logger.warning(
                        f"The path_to_data argument was not specified and the EUNOMIA_DATA_FOLDER "
                        f"environment variable was not set. Using {path_to_data}"
                    )

        # Create directory if it doesn't exist
        os.makedirs(path_to_data, exist_ok=True)

        dataset_name_version = f"{dataset_name}_{cdm_version}"
        zip_name = f"{dataset_name_version}.zip"
        file_path = os.path.join(path_to_data, zip_name)

        if os.path.exists(file_path) and not overwrite:
            if verbose:
                _logger.info(f"Dataset already exists ({file_path}). Specify overwrite=True to overwrite existing zip archive.")
            return file_path

        # Download the file
        url = f"{self.base_url}/{dataset_name}/{zip_name}"
        if verbose:
            _logger.info(f"Downloading from {url}")

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            if verbose:
                _logger.info(f"Downloaded {zip_name} to {file_path}")
                
        except requests.RequestException as e:
            _logger.error(f"Failed to download {url}: {e}")
            raise
            
        return file_path

    async def extract_load_data(
        self,
        from_path: str,
        to_path: str,
        dbms: str = "sqlite",
        cdm_version: str = "5.3",
        input_format: str = "csv",
        verbose: bool = False,
    ) -> None:
        """Extract files from a ZIP file and load into a database.
        
        Extract files from a .ZIP file and creates a OMOP CDM database that is then
        stored in the same directory as the .ZIP file.
        
        Args:
            from_path: The path to the .ZIP file that contains the csv CDM source files.
            to_path: The path to the .sqlite or .duckdb file that will be created.
            dbms: The file based database system to use: 'sqlite' (default) or 'duckdb'.
            cdm_version: The version of the OMOP CDM that are represented in the archive files.
            input_format: The format of the files expected in the archive (csv or parquet).
            verbose: Provide additional logging details during execution.
            
        Raises:
            ValueError: If from_path is not a ZIP file or doesn't exist.
            FileNotFoundError: If the ZIP file doesn't exist.
        """
        if not from_path.lower().endswith('.zip'):
            raise ValueError("Source must be a .zip file")
            
        if not os.path.exists(from_path):
            raise FileNotFoundError(f"Zipped archive '{from_path}' not found!")

        with tempfile.TemporaryDirectory() as unzip_location:
            if verbose:
                _logger.info(f"Unzipping to: {unzip_location}")
                
            with zipfile.ZipFile(from_path, 'r') as zip_ref:
                zip_ref.extractall(unzip_location)
                
            await self.load_data_files(
                data_path=unzip_location,
                db_path=to_path,
                dbms=dbms,
                cdm_version=cdm_version,
                input_format=input_format,
                verbose=verbose,
            )

    async def load_data_files(
        self,
        data_path: str,
        db_path: str,
        input_format: str = "csv",
        cdm_version: str = "5.3",
        cdm_database_schema: str = "main",
        dbms: str = "sqlite",
        verbose: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Load data files into a database (sqlite or duckdb).
        
        Load data from csv or parquet files into a database file (sqlite or duckdb).
        
        Args:
            data_path: The path to the directory containing CDM source files (csv or parquet).
            db_path: The path to the .sqlite or .duckdb file that will be created.
            input_format: The input format of the files to load. Supported formats include csv, parquet.
            cdm_version: The CDM version to create in the resulting database. Supported versions are 5.3 and 5.4.
            cdm_database_schema: The schema in which to create the CDM tables. Default is main.
            dbms: The file-based database system to use: 'sqlite' (default) or 'duckdb'.
            verbose: Provide additional logging details during execution.
            overwrite: Remove and replace an existing data set.
            
        Raises:
            ValueError: If input_format or dbms is not supported.
            FileNotFoundError: If data_path doesn't exist or contains no data files.
        """
        if input_format not in ["csv", "parquet"]:
            raise ValueError(f"Unsupported input format: {input_format}")
            
        if dbms not in ["sqlite", "duckdb"]:
            raise ValueError(f"Unsupported dbms: {dbms}")

        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data path does not exist: {data_path}")

        # Find data files
        data_files = sorted([
            f for f in os.listdir(data_path) 
            if f.lower().endswith(f'.{input_format}')
        ])
        
        if not data_files:
            raise FileNotFoundError(f"Data directory does not contain {input_format} files to load into the database.")

        if verbose:
            _logger.info(f"Found {len(data_files)} {input_format} files to load")

        if overwrite and os.path.exists(db_path):
            if verbose:
                _logger.info(f"Deleting existing file: {db_path}")
            os.unlink(db_path)

        # For now, implement basic SQLite loading
        # Note: For full compatibility, we'd need DDL generation from CommonDataModel
        if dbms == "sqlite":
            await self._load_data_files_sqlite(
                data_path, db_path, data_files, input_format, verbose
            )
        else:
            raise NotImplementedError("DuckDB support not yet implemented")

    async def _load_data_files_sqlite(
        self,
        data_path: str,
        db_path: str,
        data_files: list[str],
        input_format: str,
        verbose: bool,
    ) -> None:
        """Load data files into SQLite database."""
        # Create SQLite connection
        conn = sqlite3.connect(db_path)
        
        try:
            for data_file in data_files:
                if verbose:
                    _logger.info(f"Loading file: {data_file}")
                    
                file_path = os.path.join(data_path, data_file)
                table_name = Path(data_file).stem.lower()
                
                if input_format == "csv":
                    df = pd.read_csv(file_path)
                elif input_format == "parquet":
                    df = pd.read_parquet(file_path)
                else:
                    continue
                    
                # Convert column names to lowercase
                df.columns = df.columns.str.lower()
                
                if verbose:
                    _logger.info(f"Saving table: {table_name} (rows: {len(df)})")
                    
                # Write to SQLite
                df.to_sql(table_name, conn, if_exists='append', index=False)
                
        finally:
            conn.close()

    async def export_data_files(
        self,
        db_path: str,
        output_folder: str,
        output_format: str = "csv",
        dbms: str = "sqlite",
        verbose: bool = False,
    ) -> None:
        """Export data files from a database (sqlite or duckdb).
        
        Helper function to export data to csv or parquet files from a database file.
        
        Args:
            db_path: The path to the source .sqlite or .duckdb file.
            output_folder: The path to the export destination directory.
            output_format: The output format for the files. Supported formats include csv, parquet.
            dbms: The file-based database system to use: 'sqlite' (default) or 'duckdb'.
            verbose: Boolean argument controlling verbose debugging output.
            
        Raises:
            ValueError: If output_format or dbms is not supported.
            FileNotFoundError: If db_path doesn't exist.
        """
        if output_format not in ["csv", "parquet"]:
            raise ValueError(f"Unsupported output format: {output_format}")
            
        if dbms not in ["sqlite", "duckdb"]:
            raise ValueError(f"Unsupported dbms: {dbms}")
            
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file does not exist: {db_path}")

        # Create output directory if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        if dbms == "sqlite":
            await self._export_data_files_sqlite(
                db_path, output_folder, output_format, verbose
            )
        else:
            raise NotImplementedError("DuckDB support not yet implemented")

    async def _export_data_files_sqlite(
        self,
        db_path: str,
        output_folder: str,
        output_format: str,
        verbose: bool,
    ) -> None:
        """Export data files from SQLite database."""
        conn = sqlite3.connect(db_path)
        
        try:
            # Get list of tables
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [row[0] for row in cursor.fetchall()]
            
            if verbose:
                _logger.info(f"Processing {len(table_names)} tables")
            
            for table_name in table_names:
                if verbose:
                    _logger.info(f"Processing {table_name}")
                    
                # Query data
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                # Export to file
                output_file = os.path.join(output_folder, f"{table_name}.{output_format}")
                
                if output_format == "csv":
                    df.to_csv(output_file, index=False)
                elif output_format == "parquet":
                    df.to_parquet(output_file, index=False)
                    
                if verbose:
                    _logger.info(f"Exported {table_name} to {output_file}")
                    
        finally:
            conn.close()


# Convenience functions that match the R API
def download_eunomia_data(
    dataset_name: str,
    cdm_version: str = "5.3",
    path_to_data: Optional[str] = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> str:
    """Download Eunomia data files.
    
    Convenience function that creates an EunomiaData instance and downloads data.
    
    Args:
        dataset_name: The data set name as found on EunomiaDatasets repository.
        cdm_version: The OMOP CDM version. Default: '5.3'
        path_to_data: The path where the Eunomia data is stored on the file system.
        overwrite: Control whether the existing archive file will be overwritten.
        verbose: Provide additional logging details during execution.
        
    Returns:
        The path to the downloaded file.
    """
    eunomia = EunomiaData()
    return eunomia.download_eunomia_data(
        dataset_name=dataset_name,
        cdm_version=cdm_version,
        path_to_data=path_to_data,
        overwrite=overwrite,
        verbose=verbose,
    )


async def extract_load_data(
    from_path: str,
    to_path: str,
    dbms: str = "sqlite",
    cdm_version: str = "5.3",
    input_format: str = "csv",
    verbose: bool = False,
) -> None:
    """Extract files from a ZIP file and load into a database.
    
    Convenience function that creates an EunomiaData instance and extracts/loads data.
    
    Args:
        from_path: The path to the .ZIP file that contains the csv CDM source files.
        to_path: The path to the .sqlite or .duckdb file that will be created.
        dbms: The file based database system to use: 'sqlite' (default) or 'duckdb'.
        cdm_version: The version of the OMOP CDM that are represented in the archive files.
        input_format: The format of the files expected in the archive (csv or parquet).
        verbose: Provide additional logging details during execution.
    """
    eunomia = EunomiaData()
    await eunomia.extract_load_data(
        from_path=from_path,
        to_path=to_path,
        dbms=dbms,
        cdm_version=cdm_version,
        input_format=input_format,
        verbose=verbose,
    )


async def load_data_files(
    data_path: str,
    db_path: str,
    input_format: str = "csv",
    cdm_version: str = "5.3",
    cdm_database_schema: str = "main",
    dbms: str = "sqlite",
    verbose: bool = False,
    overwrite: bool = False,
) -> None:
    """Load data files into a database (sqlite or duckdb).
    
    Convenience function that creates an EunomiaData instance and loads data files.
    
    Args:
        data_path: The path to the directory containing CDM source files.
        db_path: The path to the .sqlite or .duckdb file that will be created.
        input_format: The input format of the files to load.
        cdm_version: The CDM version to create in the resulting database.
        cdm_database_schema: The schema in which to create the CDM tables.
        dbms: The file-based database system to use: 'sqlite' (default) or 'duckdb'.
        verbose: Provide additional logging details during execution.
        overwrite: Remove and replace an existing data set.
    """
    eunomia = EunomiaData()
    await eunomia.load_data_files(
        data_path=data_path,
        db_path=db_path,
        input_format=input_format,
        cdm_version=cdm_version,
        cdm_database_schema=cdm_database_schema,
        dbms=dbms,
        verbose=verbose,
        overwrite=overwrite,
    )


async def export_data_files(
    db_path: str,
    output_folder: str,
    output_format: str = "csv",
    dbms: str = "sqlite",
    verbose: bool = False,
) -> None:
    """Export data files from a database (sqlite or duckdb).
    
    Convenience function that creates an EunomiaData instance and exports data files.
    
    Args:
        db_path: The path to the source .sqlite or .duckdb file.
        output_folder: The path to the export destination directory.
        output_format: The output format for the files.
        dbms: The file-based database system to use: 'sqlite' (default) or 'duckdb'.
        verbose: Boolean argument controlling verbose debugging output.
    """
    eunomia = EunomiaData()
    await eunomia.export_data_files(
        db_path=db_path,
        output_folder=output_folder,
        output_format=output_format,
        dbms=dbms,
        verbose=verbose,
    )