"""Example of using LLMs with PyOMOP to query OMOP CDM databases.

This example demonstrates how to use Large Language Models with PyOMOP to query
an OMOP Common Data Model (CDM) database using natural language.

Prerequisites:
    pip install pyomop[llm]

Environment Variables:
    - GOOGLE_GENAI_API_KEY: Your Google Gemini API key (or configure another LLM)

Database Setup:
    Before running this example, create and populate a database with sample data:
    ```bash
    python -m pyomop -e Synthea27Nj -v 5.4 -n cdm_synthea.sqlite
    ```
    
    Alternatively, you can use an empty database (uncomment the table creation below).

Key OMOP CDM Tables:
    - person: Patient demographics
    - observation_period: Patient observation periods
    - visit_occurrence: Healthcare visits
    - condition_occurrence: Diagnoses
    - drug_exposure: Medications
    - procedure_occurrence: Procedures
    - measurement: Lab results
    - observation: Clinical observations
    - death: Mortality data
    - concept: Standardized vocabularies

For more information: https://ohdsi.github.io/CommonDataModel/
"""

import asyncio
import os
import re

from sqlalchemy import text

from pyomop import CDMDatabase, CdmEngineFactory, CdmLLMQuery, CdmVector
from pyomop.cdm54 import Base

# Import any LLMs that llama_index supports
# Example: Google Gemini (requires GOOGLE_GENAI_API_KEY)
from llama_index.llms.google_genai import GoogleGenAI


async def main() -> None:
    """Main function demonstrating LLM-powered OMOP CDM queries."""
    
    # ========================================================================
    # Step 1: Connect to OMOP CDM Database
    # ========================================================================
    # Connect to a pre-populated database (created using the command above)
    cdm = CdmEngineFactory(
        db="sqlite",
        name="cdm_synthea.sqlite",
    )
    
    # For PostgreSQL or MySQL:
    # cdm = CdmEngineFactory(
    #     db='pgsql',  # or 'mysql'
    #     host='localhost',
    #     port=5432,
    #     user='username',
    #     pw='password',
    #     name='omop_cdm',
    #     schema='cdm54'
    # )

    engine = cdm.engine
    
    # Uncomment to create tables if using an empty database:
    # await cdm.init_models(Base.metadata)
    
    # ========================================================================
    # Step 2: Configure LLM and Query Engine
    # ========================================================================
    # Initialize LLM (using Google Gemini as example)
    llm = GoogleGenAI(
        model="gemini-2.0-flash",
        api_key=os.getenv("GOOGLE_GENAI_API_KEY"),
    )
    
    # Define important OMOP CDM tables to include in query context
    # These are the most commonly used tables for clinical research
    important_tables = [
        "person",              # Patient demographics
        "observation_period",  # Patient observation periods
        "visit_occurrence",    # Healthcare visits
        "condition_occurrence",# Diagnoses
        "drug_exposure",       # Medications
        "procedure_occurrence",# Procedures
        "measurement",         # Lab results and vitals
        "observation",         # General clinical observations
        "death",              # Mortality data
        "concept",            # Standardized vocabularies
        "provider",           # Healthcare providers
    ]
    
    # Create SQL database wrapper with OMOP CDM metadata
    sql_database = CDMDatabase(
        engine,  # type: ignore
        include_tables=important_tables,
        version="cdm54",  # Use 'cdm6' for CDM version 6.0
    )
    
    # Create LLM-powered query engine
    # similarity_top_k controls how many tables are retrieved for each query
    query_engine = CdmLLMQuery(
        sql_database,
        llm=llm,
        similarity_top_k=3,  # Retrieve top 3 most relevant tables
    ).query_engine
    
    print("✓ LLM Query Engine configured")
    print(f"  Available tables: {', '.join(important_tables)}")
    print()
    
    # ========================================================================
    # Step 3: Execute Example Queries
    # ========================================================================
    # Example queries demonstrating different complexity levels
    queries = [
        "How many patients are in the database?",
        "What are the top 5 most common conditions?",
        "Show me patients who have been prescribed drugs for more than 30 days.",
        "What are the most frequently prescribed drugs? Include drug name and count.",
    ]
    
    for i, query_text in enumerate(queries, 1):
        print(f"{'=' * 80}")
        print(f"Query {i}: {query_text}")
        print(f"{'=' * 80}")
        
        try:
            # Execute query using LLM (async version)
            response = await query_engine.aquery(query_text)
            
            print(f"Response: {response}\n")
            
            # Display metadata if available
            if hasattr(response, "metadata") and response.metadata:
                print(f"Metadata: {response.metadata}\n")
                
                # Extract SQL query from metadata
                sql_query = response.metadata.get("sql_query", "")
                if sql_query:
                    print(f"Generated SQL:\n{sql_query}\n")
                    
                    # ============================================================
                    # Manual SQL Execution (for verification and testing)
                    # ============================================================
                    # The LLM should execute queries automatically, but this
                    # provides a backup for verification and testing
                    
                    print("Executing SQL manually for verification...")
                    
                    # Split by newlines and filter empty statements
                    sqls = [s.strip() for s in sql_query.split("\n") if s.strip()]
                    
                    vec = CdmVector()
                    async with cdm.session() as session:  # type: ignore
                        async with session.begin():
                            for sql in sqls:
                                # Only execute SELECT statements
                                if re.search(r"\bSELECT\b", sql, re.IGNORECASE):
                                    try:
                                        # Method 1: Using CdmVector for DataFrame conversion
                                        result = await vec.execute(cdm, query=sql)
                                        df = vec.result_to_df(result)
                                        print("Results as DataFrame:")
                                        print(df.head(10))
                                        print()
                                        
                                    except Exception as e:
                                        print(f"Error executing SQL: {e}")
                                        
                                        # Method 2: Direct execution using session
                                        try:
                                            result = await session.execute(text(sql))
                                            rows = result.all()
                                            print(f"Results ({len(rows)} rows):")
                                            for row in rows[:10]:
                                                print(row)
                                            if len(rows) > 10:
                                                print(f"... and {len(rows) - 10} more rows")
                                            print()
                                        except Exception as e2:
                                            print(f"Error with alternative method: {e2}")
                                            print()
        
        except Exception as e:
            print(f"Error: {e}")
            print()
    
    # ========================================================================
    # Step 4: Cleanup
    # ========================================================================
    await engine.dispose()  # type: ignore
    print("✓ Database connections closed")


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
