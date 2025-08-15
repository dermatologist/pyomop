"""Command-line interface for pyomop.

Provides commands to create CDM tables, load vocabulary CSVs, and import FHIR
Bulk Export data into an OMOP database.
"""

import asyncio

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
    "--name", "-n", multiple=False, default="cdm6.sqlite", help="Database name"
)
@click.option(
    "--schema",
    "-s",
    multiple=False,
    default="public",
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
def cli(version, create, dbtype, host, port, user, pw, name, schema, vocab, input_path):
    """pyomop CLI entrypoint.

    Args:
        version: CDM version ("cdm54" or "cdm6").
        create: If True, (re)create CDM tables.
        dbtype: Database type ("sqlite", "mysql", "pgsql").
        host: Database host.
        port: Database port.
        user: Database user.
        pw: Database password.
        name: Database name or SQLite filename.
        schema: Database schema (PostgreSQL).
        vocab: Folder with vocabulary CSV files to import.
        input_path: Folder with FHIR bundles or ndjson files to import.
    """
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


def main_routine():
    """Top-level runner used by ``python -m pyomop``."""
    click.echo("_________________________________________")
    click.echo("Pyomop v" + __version__ + " working:.....")
    cli()  # run the main function
    click.echo("Pyomop done.")


if __name__ == "__main__":
    main_routine()
