"""Example of using LLMs with PyOMOP

pip install pyomop[llm]

"""

from pyomop import CdmEngineFactory, Cohort, metadata, CdmLLMQuery, CDMDatabase
import datetime
import asyncio
# Import any LLMs that llama_index supports and you have access to
# Require OpenAI API key to use OpenAI LLMs
from llama_index.llms.google_genai import GoogleGenAI

async def main():
    # Create a sqllite database by default
    # You can also connect to an existing CDM database using the CdmEngineFactory
    cdm = CdmEngineFactory()
    # Postgres example below (db='mysql' also supported)
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='cdm6')

    engine = cdm.engine
    # Create Tables if required
    await cdm.init_models(metadata)

    async with cdm.session() as session:
        async with session.begin():

            # Adding  a cohort just for the example (not required if you have a OMOP CDM database)
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
            await session.commit()

            # Use any LLM that llama_index supports
            llm = GoogleGenAI(
                model="gemini-2.0-flash",
                api_key="some-key",  # Replace this with your key
            )
            # Include tables that you want to query
            sql_database = CDMDatabase(engine, include_tables=[
                "cohort",
            ])
            query_engine = CdmLLMQuery(sql_database, llm=llm).query_engine
            # Try any complex query.
            response  = query_engine.query("Show each in table cohort with a subject id of 100?")
    print(response)
    """

| cohort_id | subject_id | cohort_name |
|---|---|---|
| 1 | 100 | Math |

    """
    # Close session
    await session.close()
    await engine.dispose()

# Run the main function
asyncio.run(main())
