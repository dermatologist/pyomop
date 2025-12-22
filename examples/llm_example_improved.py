"""Comprehensive Example of Using LLMs with PyOMOP for OMOP CDM Queries.

This example demonstrates how to use Large Language Models (LLMs) to query
an OMOP Common Data Model (CDM) database using natural language. It showcases:

1. Setting up a connection to a populated OMOP CDM database
2. Configuring an LLM-powered query engine with multiple important CDM tables
3. Executing complex queries including drug-related analyses
4. Both LLM-generated and manually executed SQL queries

Prerequisites:
    pip install pyomop[llm]

Environment Variables:
    - GOOGLE_GENAI_API_KEY: Your Google Gemini API key (or use another LLM provider)

Database Setup:
    Before running this example, create and populate a database:
    ```bash
    python -m pyomop -e Synthea27Nj -v 5.4 -n cdm_synthea.sqlite
    ```

Important OMOP CDM Tables:
    - person: Demographics and patient information
    - observation_period: Time periods when patients are observed
    - visit_occurrence: Healthcare visits and encounters
    - condition_occurrence: Diagnoses and medical conditions
    - drug_exposure: Medication prescriptions and administrations
    - procedure_occurrence: Medical procedures performed
    - measurement: Laboratory test results and vital signs
    - observation: General clinical observations
    - death: Mortality information
    - concept: Standardized vocabularies and terminologies
    - provider: Healthcare provider information

For more information on OMOP CDM tables:
    https://ohdsi.github.io/CommonDataModel/
"""

import asyncio
import datetime
import os
import re
from typing import Any

from sqlalchemy import text

from pyomop import CDMDatabase, CdmEngineFactory, CdmLLMQuery, CdmVector
from pyomop.cdm54 import Base

# Import any LLMs that llama_index supports
# Example: Using Google Gemini (requires GOOGLE_GENAI_API_KEY environment variable)
from llama_index.llms.google_genai import GoogleGenAI


# You can also use OpenAI, Anthropic, or other LLM providers supported by llama-index
# from llama_index.llms.openai import OpenAI
# from llama_index.llms.anthropic import Anthropic


async def main() -> None:
    """Main function demonstrating LLM-powered OMOP CDM queries."""
    
    # ============================================================================
    # Step 1: Connect to the OMOP CDM Database
    # ============================================================================
    print("=" * 80)
    print("Step 1: Connecting to OMOP CDM Database")
    print("=" * 80)
    
    # Connect to the pre-populated SQLite database created by:
    # python -m pyomop -e Synthea27Nj -v 5.4 -n cdm_synthea.sqlite
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
    
    # Initialize tables if needed (not necessary for pre-populated database)
    # await cdm.init_models(Base.metadata)
    
    print(f"✓ Connected to database: cdm_synthea.sqlite\n")
    
    # ============================================================================
    # Step 2: Display Database Statistics
    # ============================================================================
    print("=" * 80)
    print("Step 2: Database Statistics")
    print("=" * 80)
    
    async with cdm.session() as session:  # type: ignore
        async with session.begin():
            # Query record counts for key tables
            stats_queries = {
                "Patients (person)": "SELECT COUNT(*) FROM person",
                "Observation Periods": "SELECT COUNT(*) FROM observation_period",
                "Visit Occurrences": "SELECT COUNT(*) FROM visit_occurrence",
                "Conditions": "SELECT COUNT(*) FROM condition_occurrence",
                "Drug Exposures": "SELECT COUNT(*) FROM drug_exposure",
                "Procedures": "SELECT COUNT(*) FROM procedure_occurrence",
                "Measurements": "SELECT COUNT(*) FROM measurement",
                "Observations": "SELECT COUNT(*) FROM observation",
                "Deaths": "SELECT COUNT(*) FROM death",
                "Concepts": "SELECT COUNT(*) FROM concept",
            }
            
            for stat_name, query in stats_queries.items():
                result = await session.execute(text(query))
                count = result.scalar()
                print(f"  {stat_name:<30}: {count:>10,}")
            
            print()
    
    # ============================================================================
    # Step 3: Configure LLM and Query Engine
    # ============================================================================
    print("=" * 80)
    print("Step 3: Configuring LLM Query Engine")
    print("=" * 80)
    
    # Initialize LLM (using Google Gemini as example)
    # Requires GOOGLE_GENAI_API_KEY environment variable
    llm = GoogleGenAI(
        model="gemini-2.0-flash",
        api_key=os.getenv("GOOGLE_GENAI_API_KEY"),
    )
    
    # Alternative LLM examples:
    # llm = OpenAI(model="gpt-4", api_key=os.getenv("OPENAI_API_KEY"))
    # llm = Anthropic(model="claude-3-opus-20240229", api_key=os.getenv("ANTHROPIC_API_KEY"))
    
    # Define the important OMOP CDM tables to include in the query context
    # These are the most commonly used tables for clinical research queries
    important_tables = [
        "person",              # Patient demographics
        "observation_period",  # Patient observation periods
        "visit_occurrence",    # Healthcare visits
        "condition_occurrence",# Diagnoses
        "drug_exposure",       # Medications
        "procedure_occurrence",# Procedures
        "measurement",         # Lab results and vitals
        "observation",         # General clinical observations
        "death",              # Mortality
        "concept",            # Vocabularies (for lookups)
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
    
    print(f"✓ LLM configured: {llm.model}")
    print(f"✓ Tables available for querying: {len(important_tables)}")
    print(f"  Tables: {', '.join(important_tables)}")
    print()
    
    # ============================================================================
    # Step 4: Example Queries
    # ============================================================================
    print("=" * 80)
    print("Step 4: Executing LLM-Powered Queries")
    print("=" * 80)
    print()
    
    # Define example queries ranging from simple to complex
    example_queries = [
        {
            "name": "Simple Patient Query",
            "query": "How many patients are in the database?",
            "description": "Basic count query on person table",
        },
        {
            "name": "Age Distribution",
            "query": "Show the distribution of patient ages by gender",
            "description": "Demographic analysis with grouping",
        },
        {
            "name": "Drug Exposure Analysis",
            "query": "What are the top 5 most commonly prescribed drugs? Include the drug concept name and count.",
            "description": "Complex query joining drug_exposure and concept tables",
        },
        {
            "name": "Drug Duration Analysis",
            "query": "Find patients who were on drug exposures for more than 30 days. Show person_id, drug_concept_id, start date, end date, and duration in days.",
            "description": "Drug exposure duration analysis with date calculations",
        },
        {
            "name": "Condition-Drug Association",
            "query": "Find the most common drugs prescribed for patients with diabetes (condition_concept_id in the 201820 range). Show drug concept ID and count.",
            "description": "Complex multi-table join analyzing condition-drug relationships",
        },
        {
            "name": "Measurement Summary",
            "query": "What are the most frequently recorded measurement types? Show concept_id and count.",
            "description": "Analysis of laboratory tests and vital signs",
        },
        {
            "name": "Visit Analysis",
            "query": "Show the distribution of visit types with their counts",
            "description": "Healthcare utilization analysis",
        },
    ]
    
    # Execute each example query
    for i, example in enumerate(example_queries, 1):
        print(f"{'─' * 80}")
        print(f"Query {i}: {example['name']}")
        print(f"{'─' * 80}")
        print(f"Description: {example['description']}")
        print(f"Natural Language Query: \"{example['query']}\"")
        print()
        
        try:
            # Execute query using LLM
            print("🤖 Querying with LLM...")
            response = await query_engine.aquery(example["query"])
            
            print(f"LLM Response: {response}")
            print()
            
            # Extract and display SQL query if available
            if hasattr(response, "metadata") and response.metadata:
                sql_query = response.metadata.get("sql_query", "")
                if sql_query:
                    print(f"Generated SQL:\n{sql_query}")
                    print()
                    
                    # ================================================================
                    # Step 5: Manual SQL Execution (for verification and testing)
                    # ================================================================
                    # This section manually executes the SQL for testing purposes
                    # The LLM should execute queries automatically, but this provides
                    # a backup mechanism and allows result verification
                    
                    print("📊 Executing SQL manually for verification...")
                    
                    # Split SQL into individual statements (handle multi-statement responses)
                    sqls = [s.strip() for s in sql_query.split(";") if s.strip()]
                    
                    vec = CdmVector()
                    async with cdm.session() as session:  # type: ignore
                        async with session.begin():
                            for sql in sqls:
                                # Only execute SELECT statements
                                if re.search(r"\bSELECT\b", sql, re.IGNORECASE):
                                    try:
                                        # Execute using CdmVector for DataFrame conversion
                                        result = await vec.execute(cdm, query=sql)
                                        df = vec.result_to_df(result)
                                        
                                        print(f"Results ({len(df)} rows):")
                                        if not df.empty:
                                            print(df.to_string(index=False, max_rows=10))
                                            if len(df) > 10:
                                                print(f"... ({len(df) - 10} more rows)")
                                        else:
                                            print("  (No results)")
                                        print()
                                        
                                    except Exception as e:
                                        print(f"⚠ Error executing SQL: {e}")
                                        print()
            else:
                print("ℹ No SQL query metadata available in response")
                print()
        
        except Exception as e:
            print(f"❌ Error: {e}")
            print()
    
    # ============================================================================
    # Step 6: Cleanup
    # ============================================================================
    print("=" * 80)
    print("Step 6: Cleanup")
    print("=" * 80)
    
    # Close database connections
    await engine.dispose()  # type: ignore
    
    print("✓ Database connections closed")
    print()
    print("=" * 80)
    print("Example completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())
