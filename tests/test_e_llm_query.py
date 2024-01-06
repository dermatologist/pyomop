import asyncio
import pytest
from llama_index.llms import Vertex
from src.pyomop.llm_engine import CDMDatabase
import os

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

        response = "I'm running in CI with no LLM"
        if "CI" not in os.environ or not os.environ["CI"] or "GITHUB_RUN_ID" not in os.environ or "DOCSDIR" not in os.environ:

            llm = Vertex(
                model="chat-bison",
            )
            sql_database = CDMDatabase(engine, include_tables=[
                "cohort",
            ])
            query_engine = CdmLLMQuery(sql_database, llm=llm)

            response  = query_engine.query("Show each in table cohort with a subject id of 100?")
    print(response)
    await session.close()
    await engine.dispose()

