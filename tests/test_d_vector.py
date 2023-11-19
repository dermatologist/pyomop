import asyncio
import pytest

@staticmethod
def test_create_vector(pyomop_fixture, metadata_fixture, vector_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(metadata_fixture))
    asyncio.run(create_vector(pyomop_fixture, vector_fixture, engine))


async def create_vector(pyomop_fixture, vector_fixture, engine):
    from src.pyomop import Cohort
    import datetime
    from sqlalchemy.future import select

    # Add a cohort
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
        await session.commit()

    # Query the cohort
    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)

    vector_fixture.result = result.scalars()
    print(vector_fixture.df.dtypes)
    assert vector_fixture.df.empty is False

    result2 = await vector_fixture.sql_df(pyomop_fixture, 'TEST')
    for row in result2:
        print(row)
    assert row.subject_id == 100

    await session.close()
    await engine.dispose()



