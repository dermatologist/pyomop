import asyncio
import pytest
import os
import datetime


def test_create_cohort_cdm54(pyomop_fixture, cdm54_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm54_metadata_fixture))
    asyncio.run(create_llm_query(pyomop_fixture, engine))


async def create_llm_query(pyomop_fixture, engine):
    from src.pyomop.cdm54 import Cohort
    from src.pyomop import CdmLLMQuery
    from langchain_core.language_models.fake import FakeListLLM
    from src.pyomop.llm_engine import CDMDatabase

    fake_llm = FakeListLLM(
        responses=[
            "select * from cohort where subject_id = 1",
            "select * from cohort where subject_id = 1",
        ]
    )
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

            # Use any LLM that llama_index supports
            llm = fake_llm
            sql_database = CDMDatabase(
                engine,
                include_tables=[
                    "cohort",
                ],
            )
            query_engine = CdmLLMQuery(sql_database, llm=llm).query_engine

            response = query_engine.query(
                "Show each in table cohort with a subject id of 100?"
            )
    await session.close()
    await engine.dispose()

    ## If we are running in CI, we don't have access to the LLM
    print(response)
