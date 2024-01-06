import asyncio
import pytest
import os
import datetime

@staticmethod
def test_create_cohort(pyomop_fixture, metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(metadata_fixture))
    asyncio.run(create_llm_query(pyomop_fixture, engine))


async def create_llm_query(pyomop_fixture,engine):
    response = "I'm running in CI with no LLM"
    try:
        from src.pyomop import Cohort, CdmLLMQuery
        from llama_index.llms import Vertex
        from src.pyomop.llm_engine import CDMDatabase
        # Add a cohort
        async with pyomop_fixture.session() as session:
            async with session.begin():
                session.add(Cohort(cohort_definition_id=2, subject_id=100,
                    cohort_end_date=datetime.datetime.now(),
                    cohort_start_date=datetime.datetime.now()))
            await session.commit()

            # Use any LLM that llama_index supports
            llm = Vertex(
                model="chat-bison",
            )
            sql_database = CDMDatabase(engine, include_tables=[
                "cohort",
            ])
            query_engine = CdmLLMQuery(sql_database, llm=llm)

            response  = query_engine.query("Show each in table cohort with a subject id of 100?")
    except ImportError:
        pass
    ## If we are running in CI, we don't have access to the LLM
    print(response)
    await session.close()
    await engine.dispose()

