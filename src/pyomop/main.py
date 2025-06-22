import click
import asyncio
from . import CdmEngineFactory
from . import CdmVocabulary
from . import __version__


@click.command()
@click.option(
    "--create", "-c", is_flag=True, help="Create CDM tables (see --version)."
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
@click.option("--version", "-v", multiple=False, default="cdm54", help="CDM version (cdm54 (default) or cdm6)")
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
def cli(version, create, dbtype, host, port, user, pw, name, schema, vocab):
    if create:
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        # initialize default engine
        engine = cdm.engine
        if version == "cdm54":
            from .cdm54 import Base
            asyncio.run(cdm.init_models(Base.metadata))
        else:  # default cdm6
            from .cdm6 import Base
            asyncio.run(cdm.init_models(Base.metadata))
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
