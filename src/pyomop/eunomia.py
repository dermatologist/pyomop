"""OMOP CDM sample data sets functionality.

This module provides functionality to download, extract, and load OMOP CDM
sample datasets from the OHDSI EunomiaDatasets repository. This is a Python
port of the Eunomia R package functionality.
"""

import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import sqlalchemy

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
            "https://raw.githubusercontent.com/OHDSI/EunomiaDatasets/main/datasets",
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
                _logger.info(
                    f"Dataset already exists ({file_path}). Specify overwrite=True to overwrite existing zip archive."
                )
            return file_path

        # Download the file
        url = f"{self.base_url}/{dataset_name}/{zip_name}"
        if verbose:
            _logger.info(f"Downloading from {url}")

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(file_path, "wb") as f:
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
        dataset_name: str,
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
        if not from_path.lower().endswith(".zip"):
            raise ValueError("Source must be a .zip file")

        if not os.path.exists(from_path):
            raise FileNotFoundError(f"Zipped archive '{from_path}' not found!")

        with tempfile.TemporaryDirectory() as unzip_location:
            if verbose:
                _logger.info(f"Unzipping to: {unzip_location}")

            with zipfile.ZipFile(from_path, "r") as zip_ref:
                zip_ref.extractall(unzip_location)

                # log extracted files for debugging
                extracted_files = os.listdir(unzip_location)
                _logger.info(f"Extracted files: {extracted_files}")
                # Add dataset_name and version subfolder to unzip_location
                if dataset_name and dataset_name.strip() != "Synthea27Nj":
                    unzip_location = os.path.join(unzip_location, dataset_name + "_" + cdm_version)
                # wait till extraction is done
                # then load the data files
                if verbose:
                    _logger.info(f"Loading data files from: {unzip_location}")

            await self.load_data_files(
                data_path=unzip_location,
                input_format=input_format,
                verbose=verbose,
            )

    async def load_data_files(
        self,
        data_path: str,
        input_format: str = "csv",
        verbose: bool = False,
    ) -> None:
        """
        Load data files into a database using the SQLAlchemy async engine from self.cdm.

        Args:
            data_path (str): Path to the directory containing CDM source files (csv or parquet).
            input_format (str, optional): Input file format ('csv' or 'parquet'). Defaults to 'csv'.
            verbose (bool, optional): If True, provides additional logging. Defaults to False.

        Raises:
            ValueError: If input_format is not supported.
            FileNotFoundError: If data_path doesn't exist or contains no data files.
            RuntimeError: If the CDM engine is not initialized.
        """
        # Validate input format
        if input_format not in ["csv", "parquet"]:
            raise ValueError(f"Unsupported input format: {input_format}")

        # Check that the data path exists
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data path does not exist: {data_path}")

        # Find all files matching the input format
        data_files = sorted(
            [f for f in os.listdir(data_path) if f.lower().endswith(f".{input_format}")]
        )

        if not data_files:
            raise FileNotFoundError(
                f"Data directory does not contain {input_format} files to load into the database."
            )

        if verbose:
            _logger.info(f"Found {len(data_files)} {input_format} files to load")

        # Get the async engine from the CDM engine factory
        engine = self.cdm.engine
        if engine is None:
            raise RuntimeError("CDM engine is not initialized.")

        # Open a transaction and load each file as a table
        async with engine.begin() as conn:
            # Best-effort: relax FK/constraint enforcement during bulk load per backend
            backend = engine.url.get_backend_name().split("+")[0]
            is_sqlite = backend == "sqlite"
            is_mysql = backend in ("mysql", "mariadb")
            is_pg = backend in ("postgresql", "postgres", "pgsql")
            if is_sqlite:
                await conn.exec_driver_sql("PRAGMA defer_foreign_keys = ON;")
                await conn.exec_driver_sql("PRAGMA foreign_keys = OFF;")
            elif is_mysql:
                await conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=0;")
            elif is_pg:
                # Only affects deferrable constraints; NOT NULL cannot be deferred.
                await conn.exec_driver_sql("SET CONSTRAINTS ALL DEFERRED;")

            try:
                for data_file in data_files:
                    if verbose:
                        _logger.info(f"Loading file: {data_file}")

                    file_path = os.path.join(data_path, data_file)
                    table_name = Path(data_file).stem.lower()

                    # Read the file into a DataFrame
                    if input_format == "csv":
                        df = pd.read_csv(file_path)
                    elif input_format == "parquet":
                        df = pd.read_parquet(file_path)
                    else:
                        continue  # Should not occur due to earlier check

                    # Convert all column names to lowercase for consistency
                    df.columns = df.columns.str.lower()

                    # Introspect existing table schema to find NOT NULL columns
                    def _get_non_nullable(sync_conn, tbl):
                        insp = sqlalchemy.inspect(sync_conn)
                        try:
                            cols = insp.get_columns(tbl)
                        except Exception:
                            return []
                        return [
                            c.get("name") for c in cols if not c.get("nullable", True)
                        ]

                    non_nullable_cols = await conn.run_sync(
                        lambda sc: _get_non_nullable(sc, table_name)
                    )
                    nn_set = {c.lower() for c in (non_nullable_cols or []) if c}

                    # Fill NaNs for non-nullable columns to avoid NOT NULL failures
                    if nn_set:
                        df = df.copy()
                        for col in df.columns:
                            if col not in nn_set:
                                continue
                            dtype = df[col].dtype
                            if pd.api.types.is_string_dtype(dtype) or dtype == object:
                                df[col] = df[col].fillna("")
                            elif pd.api.types.is_integer_dtype(dtype):
                                df[col] = df[col].fillna(0)
                            elif pd.api.types.is_float_dtype(dtype):
                                df[col] = df[col].fillna(0.0)
                            elif pd.api.types.is_bool_dtype(dtype):
                                df[col] = df[col].fillna(False)
                            elif (
                                pd.api.types.is_datetime64_any_dtype(dtype)
                                or col.endswith("_date")
                                or col.endswith("date")
                            ):
                                df[col] = df[col].fillna("1970-01-01")

                    if verbose:
                        _logger.info(f"Saving table: {table_name} (rows: {len(df)})")

                    # Write the DataFrame to the database using the sync connection
                    await conn.run_sync(
                        lambda sync_conn: df.to_sql(
                            table_name,
                            sync_conn,
                            if_exists="append",
                            index=False,
                        )
                    )
            finally:
                # Restore normal constraint behavior
                if is_sqlite:
                    await conn.exec_driver_sql("PRAGMA foreign_keys = ON;")
                elif is_mysql:
                    await conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=1;")
                elif is_pg:
                    await conn.exec_driver_sql("SET CONSTRAINTS ALL IMMEDIATE;")

    async def export_data_files(
        self,
        output_folder: str,
        output_format: str = "csv",
        verbose: bool = False,
    ) -> None:
        """
        Export data files from a database using the SQLAlchemy async engine from self.cdm.

        Args:
            output_folder (str): Path to the export destination directory.
            output_format (str, optional): Output file format ('csv' or 'parquet'). Defaults to 'csv'.
            verbose (bool, optional): If True, provides additional logging. Defaults to False.

        Raises:
            ValueError: If output_format is not supported.
            RuntimeError: If the CDM engine is not initialized.
        """
        # Validate output format
        if output_format not in ["csv", "parquet"]:
            raise ValueError(f"Unsupported output format: {output_format}")

        # Ensure the output directory exists
        os.makedirs(output_folder, exist_ok=True)

        # Get the async engine from the CDM engine factory
        engine = self.cdm.engine
        if engine is None:
            raise RuntimeError("CDM engine is not initialized.")

        # Open a transaction and export each table to a file
        async with engine.begin() as conn:
            # Helper to get all table names in the database
            def get_tables(sync_conn):
                insp = sqlalchemy.inspect(sync_conn)
                return insp.get_table_names()

            table_names = await conn.run_sync(get_tables)

            if verbose:
                _logger.info(f"Processing {len(table_names)} tables")

            for table_name in table_names:
                if verbose:
                    _logger.info(f"Processing {table_name}")

                # Helper to fetch the table as a DataFrame
                def fetch_df(sync_conn):
                    return pd.read_sql_query(f"SELECT * FROM {table_name}", sync_conn)

                df = await conn.run_sync(fetch_df)

                # Export the DataFrame to the specified format
                output_file = os.path.join(
                    output_folder, f"{table_name}.{output_format}"
                )

                if output_format == "csv":
                    df.to_csv(output_file, index=False)
                elif output_format == "parquet":
                    df.to_parquet(output_file, index=False)

                if verbose:
                    _logger.info(f"Exported {table_name} to {output_file}")

    async def run_cohort_sql(self, verbose=True) -> None:
        """
        Create Cohort table and populate it with cohort data from a SQL file.
        The sql file is in the same folder with the name CreateCohortTable.sql
        """

        sql_file = Path(__file__).parent / "CreateCohortTable.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        with open(sql_file, "r") as f:
            sql_script = f.read()

        # Sanitize the SQL script if needed (e.g., remove comments)
        sql_script = "\n".join(
            line for line in sql_script.splitlines() if not line.strip().startswith("--")
        )

        engine = self.cdm.engine
        if engine is None:
            raise RuntimeError("CDM engine is not initialized.")

        async with engine.begin() as conn:
            # For each statement in the script, execute it
            statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
            for statement in statements:
                if statement:
                    if statement.lower().startswith("create") or statement.lower().startswith("insert"):
                        if verbose:
                            _logger.info(f"Executing SQL statement: {statement[:30]}...")
                        await conn.execute(sqlalchemy.text(statement))

