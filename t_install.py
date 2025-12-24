import asyncio
import datetime

from sqlalchemy import select

from pyomop import CdmEngineFactory, CdmVector, CdmVocabulary
from pyomop.cdm54 import Base, Person


async def main():
    cdm = CdmEngineFactory()  # Creates SQLite database by default
    # Postgres example (db='mysql' also supported)
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='cdm6')

    engine = cdm.engine
    # Create Tables if required
    await cdm.init_models(Base.metadata)
    # Create vocabulary if required
    vocab = CdmVocabulary(cdm, version="cdm54")
    # vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

    async with cdm.session() as session:  # type: ignore
        async with session.begin():
            session.add(
                Person(
                    person_id=100,
                    gender_concept_id=8532,
                    gender_source_concept_id=8512,
                    year_of_birth=1980,
                    month_of_birth=1,
                    day_of_birth=1,
                    birth_datetime=datetime.datetime(1980, 1, 1),
                    race_concept_id=8552,
                    race_source_concept_id=8552,
                    ethnicity_concept_id=38003564,
                    ethnicity_source_concept_id=38003564,
                )
            )
            session.add(
                Person(
                    person_id=101,
                    gender_concept_id=8532,
                    gender_source_concept_id=8512,
                    year_of_birth=1980,
                    month_of_birth=1,
                    day_of_birth=1,
                    birth_datetime=datetime.datetime(1980, 1, 1),
                    race_concept_id=8552,
                    race_source_concept_id=8552,
                    ethnicity_concept_id=38003564,
                    ethnicity_source_concept_id=38003564,
                )
            )

        # Query the Person
        stmt = select(Person).where(Person.person_id == 100)
        result = await session.execute(stmt)
        for row in result.scalars():
            print(row)
            assert row.person_id == 100

        # Query the person pattern 2
        person = await session.get(Person, 100)
        print(person)
        assert person is not None
        assert person.person_id == 100

    # Convert result to a pandas dataframe
    vec = CdmVector()

    # https://github.com/OHDSI/QueryLibrary/blob/master/inst/shinyApps/QueryLibrary/queries/person/PE02.md
    result = await vec.query_library(cdm, resource="person", query_name="PE02")
    df = vec.result_to_df(result)
    print("DataFrame from result:")
    print(df.head())

    result = await vec.execute(cdm, query="SELECT * from person;")
    print("Executing custom query:")
    df = vec.result_to_df(result)
    print("DataFrame from result:")
    print(df.head())

    # Close engine
    await engine.dispose()  # type: ignore


# Run the main function
asyncio.run(main())
