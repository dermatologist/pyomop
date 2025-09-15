# Eunomia Data Functionality

This document describes the OMOP CDM sample data functionality ported from the R Eunomia package.

## Overview

The `pyomop.eunomia` module provides functionality to download, extract, and load OMOP CDM sample datasets from the OHDSI EunomiaDatasets repository. This is a Python port of the [Eunomia R package](https://github.com/OHDSI/Eunomia) functionality.

## Features

- **Download sample datasets**: Download OMOP CDM sample datasets from the EunomiaDatasets repository
- **Extract and load data**: Extract ZIP archives and load data into SQLite databases  
- **Data import/export**: Load CSV/Parquet files into databases and export back to files
- **CLI integration**: Command-line interface for easy dataset management
- **Async support**: Asynchronous operations for better performance

## Quick Start

### Using the Class Interface

```python
import asyncio
from pyomop.eunomia import EunomiaData

async def main():
    # Initialize
    eunomia = EunomiaData()
    
    # Download a dataset
    zip_path = eunomia.download_eunomia_data(
        dataset_name="GiBleed",
        verbose=True
    )
    
    # Extract and load into database
    await eunomia.extract_load_data(
        from_path=zip_path,
        to_path="my_dataset.db",
        verbose=True
    )
    
    # Export data back to CSV
    await eunomia.export_data_files(
        db_path="my_dataset.db",
        output_folder="exported_data",
        verbose=True
    )

asyncio.run(main())
```

### Using Convenience Functions

```python
import asyncio
from pyomop.eunomia import download_eunomia_data, extract_load_data

async def main():
    # Download dataset
    zip_path = download_eunomia_data("GiBleed", verbose=True)
    
    # Load into database
    await extract_load_data(zip_path, "dataset.db", verbose=True)

asyncio.run(main())
```

### Command Line Interface

```bash
# Download and load a dataset
pyomop --eunomia-dataset GiBleed --create --version cdm54

# Specify custom path for datasets
pyomop --eunomia-dataset Synthea --eunomia-path ./datasets --create
```

## Available Datasets

The following datasets are available from the EunomiaDatasets repository:

- **GiBleed**: Gastrointestinal bleeding cohort
- **Synthea**: Synthetic patient data
- **ClinicalTrials**: Clinical trials data
- **Covid19**: COVID-19 related data

Refer to the [EunomiaDatasets repository](https://github.com/OHDSI/EunomiaDatasets) for the complete list.

## API Reference

### EunomiaData Class

#### `__init__(cdm_engine_factory=None, base_url=None)`

Initialize EunomiaData instance.

**Parameters:**
- `cdm_engine_factory`: CDM engine factory instance (optional)
- `base_url`: Base URL for dataset downloads (optional)

#### `download_eunomia_data(dataset_name, cdm_version="5.3", path_to_data=None, overwrite=False, verbose=False)`

Download Eunomia dataset.

**Parameters:**
- `dataset_name` (str): Dataset name from EunomiaDatasets repository
- `cdm_version` (str): OMOP CDM version (default: "5.3")
- `path_to_data` (str): Download directory (optional)
- `overwrite` (bool): Overwrite existing files (default: False)
- `verbose` (bool): Enable verbose logging (default: False)

**Returns:** Path to downloaded ZIP file

#### `extract_load_data(from_path, to_path, dbms="sqlite", cdm_version="5.3", input_format="csv", verbose=False)`

Extract ZIP file and load into database.

**Parameters:**
- `from_path` (str): Path to ZIP file
- `to_path` (str): Database file path
- `dbms` (str): Database system ("sqlite" or "duckdb")
- `cdm_version` (str): CDM version
- `input_format` (str): File format ("csv" or "parquet")
- `verbose` (bool): Enable verbose logging

#### `load_data_files(data_path, db_path, input_format="csv", cdm_version="5.3", cdm_database_schema="main", dbms="sqlite", verbose=False, overwrite=False)`

Load data files into database.

**Parameters:**
- `data_path` (str): Directory containing data files
- `db_path` (str): Database file path
- `input_format` (str): File format ("csv" or "parquet")
- `cdm_version` (str): CDM version
- `cdm_database_schema` (str): Database schema
- `dbms` (str): Database system
- `verbose` (bool): Enable verbose logging
- `overwrite` (bool): Replace existing database

#### `export_data_files(db_path, output_folder, output_format="csv", dbms="sqlite", verbose=False)`

Export database to files.

**Parameters:**
- `db_path` (str): Database file path
- `output_folder` (str): Output directory
- `output_format` (str): Output format ("csv" or "parquet")
- `dbms` (str): Database system
- `verbose` (bool): Enable verbose logging

### Convenience Functions

The module also provides convenience functions that match the R API:

- `download_eunomia_data(...)`: Download dataset
- `extract_load_data(...)`: Extract and load data (async)
- `load_data_files(...)`: Load data files (async)
- `export_data_files(...)`: Export data files (async)

## Environment Variables

- `EUNOMIA_DATA_FOLDER`: Default directory for dataset storage
- `EUNOMIA_DATASETS_URL`: Custom base URL for dataset downloads

## Error Handling

The module provides comprehensive error handling:

- `ValueError`: Invalid parameters or formats
- `FileNotFoundError`: Missing files or directories
- `requests.RequestException`: Network/download errors

## Examples

See `examples/eunomia_example.py` for a complete working example.

## Limitations

- Currently supports SQLite databases (DuckDB support planned)
- Requires pandas and requests dependencies
- Network access required for dataset downloads

## Contributing

Contributions are welcome! Please see the main project CONTRIBUTING.md for guidelines.