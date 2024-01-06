from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata, CdmLLMQuery, CDMDatabase
from sqlalchemy.future import select
import datetime
import asyncio
from llama_index.llms import Vertex, OpenAI, AzureOpenAI

async def main():
    cdm = CdmEngineFactory()  # Creates SQLite database by default
    # Postgres example (db='mysql' also supported)
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='cdm6')

    engine = cdm.engine
    # Create Tables if required
    await cdm.init_models(metadata)


    # Add a cohort
    async with cdm.session() as session:
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
    print(response)

    # Close session
    await session.close()
    await engine.dispose()

# Run the main function
asyncio.run(main())