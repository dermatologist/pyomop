import asyncio
import pytest

@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory
    cdm = CdmEngineFactory()
    return cdm

@pytest.fixture
def metadata_fixture():
    from src.pyomop import metadata
    return metadata

@staticmethod
@pytest.mark.asyncio
def test_create_db(pyomop_fixture, metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    asyncio.run(pyomop_fixture.init_models(metadata_fixture))
    asyncio.run(myasync(pyomop_fixture, engine))


async def myasync(pyomop_fixture,engine):
    from src.pyomop import Cohort
    import datetime
    from sqlalchemy.future import select

    async with pyomop_fixture.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
        await session.commit()

    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)

    for row in result.scalars():
        print(row)
        assert row.subject_id == 100

    await session.commit()
    await engine.dispose()

