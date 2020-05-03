import click
from engine_factory import CdmEngineFactory
from cdm6_tables import metadata


@click.command()
@click.option('--verbose', '-v', is_flag=True, help="Will print verbose messages.")
@click.option('--create', '-c', is_flag=True, help="Create CDM6 schema on the database.")
@click.option('--dbtype', '-t', multiple=False, default='sqlite',
              help='Database Type for creating CDM (sqlite, mysql or pgsql')
@click.option('--host', '-h', multiple=False, default='localhost',
              help='Database host')
@click.option('--port', '-p', multiple=False, default='5432',
              help='Database port')
@click.option('--user', '-u', multiple=False, default='root',
              help='Database user')
@click.option('--pw', '-w', multiple=False, default='pass',
              help='Database password')
@click.option('--name', '-n', multiple=False, default='cdm6.sqlite',
              help='Database name')
@click.option('--schema', '-s', multiple=False, default='public',
              help='Database schema (for pgsql)')
def cli(verbose, create, dbtype, host, port, user, pw, name, schema):
    if verbose:
        print("verbose")
    if create:
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        engine = cdm.engine
        metadata.create_all(engine)

def main_routine():
    click.echo("_________________________________________")
    click.echo("Pyomop working:.....")
    cli()  # run the main function
    click.echo("Pyomop done.")


if __name__ == '__main__':
    main_routine()