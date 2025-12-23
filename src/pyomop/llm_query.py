"""LLM query utilities over the OMOP CDM schema.

This module wires langchain components to an OMOP-aware ``CDMDatabase`` so
you can build SQL query engines that know about your CDM tables. All LLM-related
imports are optional and performed lazily at runtime.
"""

from typing import Any

from langchain_core.language_models import BaseLanguageModel

from .llm_engine import CDMDatabase


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
        # Lazy import optional dependencies so the package imports without them
        try:
            from langchain_community.agent_toolkits.sql.base import create_sql_agent
            from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
        except ImportError as e:  # pragma: no cover
            raise ImportError("Install 'pyomop[llm]' to use LLM query features.") from e

        self._sql_database = sql_database
        self._llm = llm

        # Create SQL toolkit and agent
        toolkit = SQLDatabaseToolkit(db=sql_database, llm=llm)
        self._tools = toolkit.get_tools()

        # Create SQL agent using the default agent type
        # This is more flexible and works with various LLM types
        self._agent = create_sql_agent(
            llm=llm,
            toolkit=toolkit,
            verbose=False,
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


