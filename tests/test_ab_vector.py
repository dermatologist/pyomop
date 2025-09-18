import asyncio
import os

import pytest


@staticmethod
def test_create_patient_cdm6(pyomop_fixture, cdm6_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm6_metadata_fixture))
    asyncio.run(create_patient_cdm6(pyomop_fixture, engine))


@staticmethod
def test_create_patient_cdm54(pyomop_fixture, cdm54_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm54_metadata_fixture))
    asyncio.run(create_patient_cdm54(pyomop_fixture, engine))


async def create_patient_cdm6(pyomop_fixture, engine):
    import datetime

    from sqlalchemy.future import select

    from src.pyomop.cdm6 import ConditionOccurrence, Person

    # Add a patient
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Person(
                    person_id=1,
                    gender_concept_id=100,
                    gender_source_concept_id=10,
                    year_of_birth=2000,
                    race_concept_id=200,
                    race_source_concept_id=20,
                    ethnicity_concept_id=300,
                    ethnicity_source_concept_id=30,
                )
            )
        await session.commit()

    # Add couple of conditions
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                ConditionOccurrence(
                    condition_occurrence_id=2,
                    person_id=1,
                    condition_concept_id=123,
                    condition_start_date=datetime.datetime.now(),
                    condition_end_date=datetime.datetime.now(),
                    condition_start_datetime=datetime.datetime.now(),
                    condition_end_datetime=datetime.datetime.now(),
                    condition_type_concept_id=32020,
                    condition_source_concept_id=12345,
                    condition_status_concept_id=329

                )
            )
            session.add(
                ConditionOccurrence(
                    condition_occurrence_id=3,
                    person_id=1,
                    condition_concept_id=456,
                    condition_start_date=datetime.datetime.now(),
                    condition_end_date=datetime.datetime.now(),
                    condition_start_datetime=datetime.datetime.now(),
                    condition_end_datetime=datetime.datetime.now(),
                    condition_type_concept_id=32020,
                    condition_source_concept_id=12345,
                    condition_status_concept_id=329
                )
            )
        await session.commit()

    # Query the condition for patient 1
    stmt = (
        select(ConditionOccurrence)
        .where(ConditionOccurrence.person_id == 1)
        .order_by(ConditionOccurrence.condition_occurrence_id)
    )
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.person_id == 1


async def create_patient_cdm54(pyomop_fixture, engine):
    import datetime

    from sqlalchemy.future import select

    from src.pyomop.cdm54 import ConditionOccurrence, Person

    # Add a patient
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Person(
                    person_id=1,
                    gender_concept_id=100,
                    gender_source_concept_id=10,
                    year_of_birth=2000,
                    race_concept_id=200,
                    race_source_concept_id=20,
                    ethnicity_concept_id=300,
                    ethnicity_source_concept_id=30,
                )
            )
        await session.commit()

    # Add couple of conditions
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                ConditionOccurrence(
                    condition_occurrence_id=2,
                    person_id=1,
                    condition_concept_id=123,
                    condition_start_date=datetime.datetime.now(),
                    condition_end_date=datetime.datetime.now(),
                    condition_start_datetime=datetime.datetime.now(),
                    condition_end_datetime=datetime.datetime.now(),
                    condition_type_concept_id=32020,
                    condition_source_concept_id=12345,
                    condition_status_concept_id=329
                )
            )
            session.add(
                ConditionOccurrence(
                    condition_occurrence_id=3,
                    person_id=1,
                    condition_concept_id=456,
                    condition_start_date=datetime.datetime.now(),
                    condition_end_date=datetime.datetime.now(),
                    condition_start_datetime=datetime.datetime.now(),
                    condition_end_datetime=datetime.datetime.now(),
                    condition_type_concept_id=32020,
                    condition_source_concept_id=12345,
                    condition_status_concept_id=329
                )
            )
        await session.commit()

    # Query the condition for patient 1
    stmt = (
        select(ConditionOccurrence)
        .where(ConditionOccurrence.person_id == 1)
        .order_by(ConditionOccurrence.condition_occurrence_id)
    )
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.person_id == 1
