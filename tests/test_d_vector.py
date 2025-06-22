import asyncio
import pytest
import os


@staticmethod
def test_create_vector_cdm6(
    pyomop_fixture, cdm6_metadata_fixture, vector_fixture, capsys
):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm6_metadata_fixture))
    asyncio.run(cdm6_create_vector(pyomop_fixture, vector_fixture, engine))
    asyncio.run(query_library_(pyomop_fixture, vector_fixture))


@staticmethod
def test_create_vector_cdm54(
    pyomop_fixture, cdm54_metadata_fixture, vector_fixture, capsys
):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm54_metadata_fixture))
    asyncio.run(cdm54_create_vector(pyomop_fixture, vector_fixture, engine))


async def cdm6_create_vector(pyomop_fixture, vector_fixture, engine):
    from src.pyomop.cdm6 import Cohort, Person
    import datetime
    from sqlalchemy.future import select

    # Add a cohort
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Cohort(
                    cohort_definition_id=2,
                    subject_id=100,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
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
        await session.commit()

    # Query the cohort
    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)


    result2 = await vector_fixture.execute(pyomop_fixture, "TEST")
    found = False
    for row in result2:
        print(row)
        assert row.subject_id == 100
        found = True
    assert found, "No rows returned from result2"

    await session.close()
    await engine.dispose()


async def cdm54_create_vector(pyomop_fixture, vector_fixture, engine):
    from src.pyomop.cdm54 import Cohort
    import datetime
    from sqlalchemy.future import select

    # Add a cohort
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Cohort(
                    cohort_definition_id=2,
                    subject_id=100,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
        await session.commit()

    # Query the cohort
    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)


    result2 = await vector_fixture.execute(pyomop_fixture, "TEST")
    found = False
    for row in result2:
        print(row)
        assert row.subject_id == 100
        found = True
    assert found, "No rows returned from result2"

    await session.close()
    await engine.dispose()

async def query_library_(pyomop_fixture, vector_fixture):
    result = await vector_fixture.execute(pyomop_fixture, "TEST")
    print("Executing TEST query:")
    assert result is not None
    df = vector_fixture.result_to_df(result)
    print(df.head())
    result = await vector_fixture.query_library(
        pyomop_fixture)
    assert result is not None
    print("Executing query library:")
    df = vector_fixture.result_to_df(result)
    print(df.head())


