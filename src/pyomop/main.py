import click
import asyncio
from . import CdmEngineFactory
from . import CdmVocabulary
from . import metadata
from . import __version__


@click.command()
@click.option("--verbose", "-v", is_flag=True, help="Will print verbose messages.")
@click.option(
    "--create", "-c", is_flag=True, help="Create CDM6 schema on the database."
)
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
def cli(verbose, create, dbtype, host, port, user, pw, name, schema, vocab):
    if verbose:
        print("verbose")
    if create:
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        # initialize default engine
        engine = cdm.engine
        asyncio.run(cdm.init_models(metadata))
    if vocab != "":
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        _vocab = CdmVocabulary(cdm)
        _vocab.create_vocab(vocab)
        print("Done")


def main_routine():
    click.echo("_________________________________________")
    click.echo("Pyomop v" + __version__ + " working:.....")
    cli()  # run the main function
    click.echo("Pyomop done.")


if __name__ == "__main__":
    main_routine()
