"""LLM query utilities over the OMOP CDM schema.

This module wires langchain components to an OMOP-aware ``CDMDatabase`` so
you can build SQL query engines that know about your CDM tables. All LLM-related
imports are optional and performed lazily at runtime.
"""

import logging
import time
from typing import Any

import requests
from langchain.tools import tool
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_core.language_models import BaseLanguageModel

from .llm_engine import CDMDatabase

logger = logging.getLogger(__name__)


class CdmLLMQuery:
    """Helper that prepares an LLM-backed SQL query engine for OMOP.

    It constructs an SQL agent that can generate and execute SQL queries
    against OMOP CDM tables using an LLM.

    Args:
        sql_database: A ``CDMDatabase`` instance connected to the OMOP DB.
        llm: A langchain LLM instance (BaseLanguageModel).
        **kwargs: Reserved for future expansion.
    """

    def __init__(
        self,
        sql_database: CDMDatabase,
        llm: BaseLanguageModel,
        **kwargs: Any,
    ) -> None:
        self._sql_database = sql_database
        self._llm = llm

        # Create SQL toolkit and agent
        toolkit = MyToolKit(db=sql_database, llm=llm)  # type: ignore
        self._tools = toolkit.get_tools()

        # Create SQL agent using the default agent type
        # This is more flexible and works with various LLM types
        # Use agent_executor_kwargs to enable error handling
        # Disable streaming for tool-calling agents to avoid "Tools not supported in streaming mode" error
        try:
            llm.streaming = False # type: ignore
        except:
            # Some LLMs don't support setting streaming directly
            pass
        self._agent = create_sql_agent(
            llm=llm,
            toolkit=toolkit,
            verbose=True,
            agent_type="tool-calling",
            agent_executor_kwargs={"handle_parsing_errors": True},
        )

        # The agent itself is the query engine for langchain >1.0
        self._query_engine = self._agent

    @property
    def tools(self) -> list[Any]:
        """List of SQL tools available in the query engine."""
        return self._tools

    @property
    def query_engine(self) -> Any:
        """An SQL agent executor over the CDM tables."""
        return self._query_engine


# create a langchain tool
@tool
def example_query_tool(table_name: str) -> str:
    """
    Generate a couple of example queries for the given table name.
    This will help you understand the context of the table.
    Args:
        table_name: The name of the OMOP CDM table.
        One of "person", "condition_occurrence", "condition_era", "drug_exposure", "drug_era", "observation".
    Returns:
        A string with example queries for the table.
    """
    example = ""
    try:
        if table_name.lower() == "person":
            # read the following markdown file from the website and return its content
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/person/PE02.md"
            ).text
            time.sleep(0.5)  # Rate limit delay
            example += "\n"
            example += requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/person/PE03.md"
            ).text
        elif table_name.lower() == "condition_occurrence":
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/condition_occurrence/CO01.md"
            ).text
            time.sleep(0.5)  # Rate limit delay
            example += "\n"
            example += requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/condition_occurrence/CO05.md"
            ).text
        elif table_name.lower() == "condition_era":
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/condition_era/CE01.md"
            ).text
            time.sleep(0.5)  # Rate limit delay
            example += "\n"
            example += requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/condition_era/CE02.md"
            ).text
        elif table_name.lower() == "drug_exposure":
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/drug_exposure/DEX01.md"
            ).text
            time.sleep(0.5)  # Rate limit delay
            example += "\n"
            example += requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/drug_exposure/DEX02.md"
            ).text
        elif table_name.lower() == "drug_era":
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/drug_era/DER01.md"
            ).text
            time.sleep(0.5)  # Rate limit delay
            example += "\n"
            example += requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/drug_era/DER04.md"
            ).text
        elif table_name.lower() == "observation":
            example = requests.get(
                "https://raw.githubusercontent.com/OHDSI/QueryLibrary/refs/heads/master/inst/shinyApps/QueryLibrary/queries/observation/O01.md"
            ).text
    except Exception as e:
        logger.warning(f"Error fetching example queries for {table_name}: {e}")
    logger.info(f"Dynamic prompt tool called for table: {table_name}")
    logger.info(
        f"Example returned: {example[:200] if example else '(empty)'}..."
    )  # Log first 200 characters
    return example


class MyToolKit(SQLDatabaseToolkit):
    """Custom toolkit that includes the example query tool."""

    def __init__(self, db: CDMDatabase, llm: BaseLanguageModel) -> None:
        super().__init__(db=db, llm=llm)  # type: ignore

    def get_tools(self) -> list[Any]:
        """Get the list of tools including the example query tool."""
        tools = super().get_tools()
        tools.append(example_query_tool)
        return tools
