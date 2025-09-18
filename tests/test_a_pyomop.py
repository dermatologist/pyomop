import asyncio
import os

import pytest


@staticmethod
def test_create_person_cdm6(pyomop_fixture, cdm6_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm6_metadata_fixture))
    asyncio.run(cdm6_create_person(pyomop_fixture, engine))


@staticmethod
def test_create_person_cdm54(pyomop_fixture, cdm54_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm54_metadata_fixture))
    asyncio.run(cdm54_create_person(pyomop_fixture, engine))


def cdm6_test_create_person(pyomop_fixture, cdm6_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm6_metadata_fixture))
    asyncio.run(cdm6_create_person(pyomop_fixture, engine))


async def cdm6_create_person(pyomop_fixture, engine):
    import datetime

    from sqlalchemy.future import select

    from src.pyomop.cdm6 import Person

    # Add a person
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Person(
                    person_id=100,
                    gender_concept_id=8532,
                    gender_source_concept_id=2,
                    year_of_birth=1980,
                    month_of_birth=1,
                    day_of_birth=1,
                    birth_datetime=datetime.datetime(1980, 1, 1),
                    race_concept_id=2,
                    race_source_concept_id=2,
                    ethnicity_concept_id=3,
                    ethnicity_source_concept_id=2,
                )
            )
        await session.commit()

    # Query the person
    stmt = select(Person).where(Person.person_id == 100)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.person_id == 100

    # Query the person by PK
    person = await session.get(Person, 100)
    print(person)
    assert person.person_id == 100

    await session.close()
    await engine.dispose()


async def cdm54_create_person(pyomop_fixture, engine):
    import datetime

    from sqlalchemy.future import select

    from src.pyomop.cdm54 import Person

    # Add a person
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Person(
                    person_id=100,
                    gender_concept_id=8532,
                    gender_source_concept_id=2,
                    year_of_birth=1980,
                    month_of_birth=1,
                    day_of_birth=1,
                    birth_datetime=datetime.datetime(1980, 1, 1),
                    race_concept_id=2,
                    race_source_concept_id=2,
                    ethnicity_concept_id=3,
                    ethnicity_source_concept_id=2,
                )
            )
        await session.commit()

    # Query the person
    stmt = select(Person).where(Person.person_id == 100)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.person_id == 100

    # Query the person by PK
    person = await session.get(Person, 100)
    print(person)
    assert person.person_id == 100

    await session.close()
    await engine.dispose()
