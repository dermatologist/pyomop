"""Command-line interface for pyomop.

Provides commands to create CDM tables, load vocabulary CSVs, and import FHIR
Bulk Export data into an OMOP database.
"""

import asyncio
import sys

import click

from . import __version__
from .engine_factory import CdmEngineFactory
from .vocabulary import CdmVocabulary


@click.command()
@click.option("--create", "-c", is_flag=True, help="Create CDM tables (see --version).")
@click.option(
    "--dbtype",
    "-t",
    multiple=False,
    default="sqlite",
    help="Database Type for creating CDM (sqlite, mysql or pgsql)",
)
@click.option("--host", "-h", multiple=False, default="localhost", help="Database host")
@click.option("--port", "-p", multiple=False, default="5432", help="Database port")
@click.option("--user", "-u", multiple=False, default="root", help="Database user")
@click.option("--pw", "-w", multiple=False, default="pass", help="Database password")
@click.option(
    "--version",
    "-v",
    multiple=False,
    default="cdm54",
    help="CDM version (cdm54 (default) or cdm6)",
)
@click.option(
    "--name", "-n", multiple=False, default="cdm.sqlite", help="Database name"
)
@click.option(
    "--schema",
    "-s",
    multiple=False,
    default="",
    help="Database schema (for pgsql)",
)
@click.option(
    "--vocab",
    "-i",
    multiple=False,
    default="",
    help="Folder with vocabulary files (csv) to import",
)
@click.option(
    "--input",
    "-f",
    "input_path",
    type=click.Path(exists=True, file_okay=False),
    help="Input folder with FHIR bundles or ndjson files.",
)
@click.option(
    "--eunomia-dataset",
    "-e",
    "eunomia_dataset",
    help="Download and load Eunomia dataset (e.g., 'GiBleed', 'Synthea')",
)
@click.option(
    "--eunomia-path",
    default="",
    help="Path to store/find Eunomia datasets (uses EUNOMIA_DATA_FOLDER env var if not specified)",
)
@click.option(
    "--connection-info",
    is_flag=True,
    help="Display connection information for the database",
)
@click.option(
    "--mcp-server",
    is_flag=True,
    help="Start MCP server for stdio interaction",
)
def cli(
    version,
    create,
    dbtype,
    host,
    port,
    user,
    pw,
    name,
    schema,
    vocab,
    input_path,
    eunomia_dataset,
    eunomia_path,
    connection_info,
    mcp_server,
):
    cdm = None  # ensure cdm is always defined
    # clear database name if not sqlite
    if dbtype != "sqlite" and name == "cdm.sqlite":
        name = ""
    if create:
        click.echo(f"Creating CDM {version} tables in {dbtype} database {name}")
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        # initialize default engine
        engine = cdm.engine
        if version == "cdm54":
            from .cdm54 import Base

            asyncio.run(cdm.init_models(Base.metadata))
        else:  # default cdm6
            from .cdm6 import Base

            asyncio.run(cdm.init_models(Base.metadata))
        click.echo("Done")
    if vocab != "":
        click.echo(f"Creating CDM {version} vocabulary in {dbtype} database {name}")
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        _vocab = CdmVocabulary(cdm)
        asyncio.run(_vocab.create_vocab(vocab))
        click.echo("Done")

    if input_path:
        click.echo(f"Loading FHIR data from {input_path} into {dbtype} database {name}")
        import json
        import sys
        import tempfile
        from pathlib import Path

        import fhiry.parallel as fp

        from pyomop.loader import CdmCsvLoader

        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        # initialize default engine
        engine = cdm.engine

        # read config file to config_json
        config = {"REMOVE": ["text.div", "meta"], "RENAME": {}}
        config_json = json.dumps(config)
        config_file = None  # TODO: Fix
        if config_file:
            try:
                with open(config_file, "r") as f:
                    config_json = json.load(f)
            except Exception as e:
                click.echo(f"Error reading config file: {e}", err=True)
                sys.exit(1)
        # Try ndjson first, fallback to process
        ndjson_files = list(Path(input_path).glob("*.ndjson"))
        if ndjson_files:
            df = fp.ndjson(input_path, config_json=config_json)
        else:
            df = fp.process(input_path, config_json=config_json)
        if df.empty:
            click.echo("No data found.", err=True)
            sys.exit(1)
        else:
            # writing df to temp_file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                df.to_csv(temp_file.name, index=False)
                click.echo(f"Data written to temporary file: {temp_file.name}")
        # Load CSV into OMOP tables using mapping
        loader = CdmCsvLoader(cdm)
        asyncio.run(
            loader.load(
                csv_path=temp_file.name,
                chunk_size=500,
            )
        )
        click.echo("Done")

    if eunomia_dataset:
        click.echo(
            f"Downloading and loading Eunomia dataset '{eunomia_dataset}' into {dbtype} database {name}"
        )
        from .eunomia import EunomiaData

        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        eunomia = EunomiaData(cdm)

        # Download dataset
        zip_path = eunomia.download_eunomia_data(
            dataset_name=eunomia_dataset,
            cdm_version=version,
            path_to_data=eunomia_path if eunomia_path else None,
            verbose=True,
        )
        click.echo(f"Downloaded dataset to: {zip_path}")

        # Extract and load into the configured database
        try:
            asyncio.run(
                eunomia.extract_load_data(
                    from_path=zip_path,
                    dataset_name=eunomia_dataset,
                    cdm_version=version,
                    input_format="csv",
                    verbose=True
                )
            )
            click.echo(f"Loaded dataset into configured database: {name}")
        except Exception as e:
            click.echo(f"Error loading dataset: {e}", err=True)
            raise

        # Run CreateCohortTable.sql to create cohort table and populate it
        try:
            asyncio.run(eunomia.run_cohort_sql())
            click.echo("Cohort table created and populated.")
        except Exception as e:
            click.echo(f"Error creating/populating cohort table: {e}", err=True)
            raise
        click.echo("Done")
    if cdm and connection_info:
        click.echo(click.style("Database connection information:", fg="green"))
        click.echo(cdm.print_connection_info())
    
    if mcp_server:
        click.echo("Starting pyomop MCP server...")
        try:
            from .mcp import mcp_server_main
            asyncio.run(mcp_server_main())
        except ImportError:
            click.echo("MCP server requires 'mcp' package. Install with: pip install mcp", err=True)
            sys.exit(1)
        except KeyboardInterrupt:
            click.echo("MCP server stopped.")
        except Exception as e:
            click.echo(f"Error starting MCP server: {e}", err=True)
            sys.exit(1)

def main_routine():
    """Top-level runner used by ``python -m pyomop``."""
    click.echo("_________________________________________")
    click.echo("Pyomop v" + __version__ + " by Bell Eapen ( https://nuchange.ca ) ")
    cli()  # run the main function
    click.echo("Pyomop done.")


if __name__ == "__main__":
    main_routine()
