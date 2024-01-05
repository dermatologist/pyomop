import asyncio
import pytest
from llama_index.llms import Vertex
from llama_index import SQLDatabase

@staticmethod
def test_create_cohort(pyomop_fixture, metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(metadata_fixture))
    asyncio.run(create_llm_query(pyomop_fixture, engine))


async def create_llm_query(pyomop_fixture,engine):
    from src.pyomop import Cohort, CdmLLMQuery
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
    for row in result.scalars():
        print(row)
        assert row.subject_id == 100

    # Query the cohort pattern 2
    cohort = await session.get(Cohort, 1)
    print(cohort)
    assert cohort.subject_id == 100

    llm = Vertex()
    sql_database = await SQLDatabase(engine, include_tables=[
        "cohort",
    ])
    query_engine = CdmLLMQuery(sql_database, llm=llm)
    response  = await query_engine.query("What is the cohort definition id for subject 100?")
    print(response)

    await session.close()
    await engine.dispose()

