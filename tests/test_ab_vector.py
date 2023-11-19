import asyncio
import pytest

@staticmethod
def test_create_patient(pyomop_fixture, metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(metadata_fixture))
    asyncio.run(create_patient(pyomop_fixture, engine))


async def create_patient(pyomop_fixture,engine):
    from src.pyomop import Person
    from src.pyomop import Cohort
    import datetime
    from sqlalchemy.future import select

    # Add a patient
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(Person(
                gender_concept_id=100,
                year_of_birth=2000,
                race_concept_id=200,
                ethnicity_concept_id=300
            ))
        await session.commit()

    # Add couple of cohorts
    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=1,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
            session.add(Cohort(cohort_definition_id=3, subject_id=1,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
        await session.commit()

    # Query the cohort for patient 1
    stmt = select(Cohort).where(Cohort.subject_id == 1).order_by(Cohort._id)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.subject_id == 1
