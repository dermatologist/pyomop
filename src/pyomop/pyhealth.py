"""PyHealth compatible export functionality.

This module provides functionality to export OMOP CDM data in a format compatible
with PyHealth machine learning library (https://github.com/sunlabuiuc/PyHealth).
"""

import logging
import os
from typing import List, Optional, TYPE_CHECKING

import pandas as pd
import sqlalchemy

if TYPE_CHECKING:
    from .engine_factory import CdmEngineFactory

_logger = logging.getLogger(__name__)


class PyHealthExport:
    """Export OMOP CDM data in PyHealth compatible format.

    PyHealth is a machine learning library for healthcare data.
    This class exports specific OMOP CDM tables as CSV files with lowercase names
    as expected by PyHealth.

    Args:
        cdm_engine: CdmEngineFactory instance with initialized engine.
        export_path: Path to export directory. If None, uses PYHEALTH_DATA_FOLDER
                    environment variable or current working directory.
    """

    # Tables to export for PyHealth compatibility
    PYHEALTH_TABLES = [
        "person",
        "visit_occurrence",
        "death",
        "condition_occurrence",
        "procedure_occurrence",
        "drug_exposure",
        "measurement"
    ]

    def __init__(self, cdm_engine: "CdmEngineFactory", export_path: Optional[str] = None):
        """Initialize PyHealthExport.

        Args:
            cdm_engine: CdmEngineFactory instance with initialized engine.
            export_path: Path to export directory. If None, uses PYHEALTH_DATA_FOLDER
                        environment variable or current working directory.
        """
        self._cdm_engine = cdm_engine
        if export_path is None:
            export_path = os.environ.get("PYHEALTH_DATA_FOLDER", os.getcwd())
        self._export_path = export_path

    @property
    def cdm_engine(self) -> "CdmEngineFactory":
        """Get the CDM engine factory."""
        return self._cdm_engine

    @cdm_engine.setter
    def cdm_engine(self, value: "CdmEngineFactory") -> None:
        """Set the CDM engine factory."""
        self._cdm_engine = value

    @property
    def export_path(self) -> str:
        """Get the export path."""
        return self._export_path

    @export_path.setter
    def export_path(self, value: str) -> None:
        """Set the export path."""
        self._export_path = value

    async def export(self, verbose: bool = False) -> List[str]:
        """Export PyHealth compatible tables as CSV files.

        Exports the following tables to CSV files with lowercase names:
        - person.csv
        - visit_occurrence.csv
        - death.csv
        - condition_occurrence.csv
        - procedure_occurrence.csv
        - drug_exposure.csv
        - measurement.csv

        Args:
            verbose: If True, provides additional logging.

        Returns:
            List of exported file paths.

        Raises:
            RuntimeError: If the CDM engine is not initialized.
            FileNotFoundError: If a required table doesn't exist in the database.
        """
        # Ensure the export directory exists
        os.makedirs(self._export_path, exist_ok=True)

        # Get the async engine from the CDM engine factory
        engine = self._cdm_engine.engine
        if engine is None:
            raise RuntimeError("CDM engine is not initialized.")

        exported_files = []

        # Open a transaction and export each required table
        async with engine.begin() as conn:
            # Helper to get all available table names
            def get_available_tables(sync_conn):
                insp = sqlalchemy.inspect(sync_conn)
                return set(insp.get_table_names())

            available_tables = await conn.run_sync(get_available_tables)

            if verbose:
                _logger.info(f"Available tables: {sorted(available_tables)}")
                _logger.info(f"Exporting {len(self.PYHEALTH_TABLES)} PyHealth tables")

            for table_name in self.PYHEALTH_TABLES:
                if table_name not in available_tables:
                    if verbose:
                        _logger.warning(f"Table {table_name} not found in database, skipping")
                    continue

                if verbose:
                    _logger.info(f"Exporting table: {table_name}")

                # Helper to fetch the table as a DataFrame
                def fetch_df(sync_conn):
                    return pd.read_sql_query(f"SELECT * FROM {table_name}", sync_conn)

                df = await conn.run_sync(fetch_df)

                # Export the DataFrame to CSV with lowercase filename
                output_file = os.path.join(self._export_path, f"{table_name.lower()}.csv")
                df.to_csv(output_file, index=False)
                exported_files.append(output_file)

                if verbose:
                    _logger.info(f"Exported {table_name} ({len(df)} rows) to {output_file}")

        if verbose:
            _logger.info(f"Export complete. {len(exported_files)} files exported to {self._export_path}")

        return exported_files