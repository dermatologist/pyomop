import asyncio
import pytest
import os


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
    from src.pyomop.cdm6 import Person
    from src.pyomop.cdm6 import Cohort
    import datetime
    from sqlalchemy.future import select

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

    # Add couple of cohorts
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Cohort(
                    cohort_definition_id=2,
                    subject_id=1,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
            session.add(
                Cohort(
                    cohort_definition_id=3,
                    subject_id=1,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
        await session.commit()

    # Query the cohort for patient 1
    stmt = select(Cohort).where(Cohort.subject_id == 1).order_by(Cohort._id)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.subject_id == 1


async def create_patient_cdm54(pyomop_fixture, engine):
    from src.pyomop.cdm54 import Person
    from src.pyomop.cdm54 import Cohort
    import datetime
    from sqlalchemy.future import select

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

    # Add couple of cohorts
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(
                Cohort(
                    cohort_definition_id=2,
                    subject_id=1,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
            session.add(
                Cohort(
                    cohort_definition_id=3,
                    subject_id=1,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now(),
                )
            )
        await session.commit()

    # Query the cohort for patient 1
    stmt = select(Cohort).where(Cohort.subject_id == 1).order_by(Cohort._id)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.subject_id == 1
