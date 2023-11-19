from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata
from sqlalchemy.future import select
import datetime
import asyncio

async def main():
    cdm = CdmEngineFactory()  # Creates SQLite database by default
    # Postgres example (db='mysql' also supported)
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='cdm6')

    engine = cdm.engine
    # Create Tables if required
    await cdm.init_models(metadata)
    # Create vocabulary if required
    vocab = CdmVocabulary(cdm)
    # vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

    # Add a cohort
    async with cdm.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
        await session.commit()

    # Query the cohort
    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.subject_id == 100

    # Query the cohort pattern 2
    cohort = await session.get(Cohort, 1)
    print(cohort)
    assert cohort.subject_id == 100

    # Close session
    await session.close()
    await engine.dispose()

    # Convert result to a pandas dataframe
    vec = CdmVector()
    vec.result = result
    print(vec.df.dtypes)

    result = await vec.sql_df(cdm, 'TEST') # TEST is defined in sqldict.py
    for row in result:
        print(row)

    result = await vec.sql_df(cdm, query='SELECT * from cohort')
    for row in result:
        print(row)


# Run the main function
asyncio.run(main())