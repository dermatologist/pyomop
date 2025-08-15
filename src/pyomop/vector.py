"""Utilities to execute queries and convert results to DataFrames.

Exposes a small helper class around async SQLAlchemy execution and integration
with OHDSI QueryLibrary.
"""

import asyncio
from logging import getLogger

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.inspection import inspect

from .sqldict import CDMSQL

logger = getLogger(__name__)


# https://gist.github.com/dermatologist/f436cb461a3290732a27c4dc040229f9
# Thank you! https://gist.github.com/garaud
class CdmVector(object):
    """Query execution utility for OMOP CDM.

    Methods let you run raw SQL or QueryLibrary snippets and turn results into
    pandas DataFrames.
    """

    async def execute(self, cdm, sqldict=None, query=None, chunksize=1000):
        """Execute a SQL query asynchronously.

        Args:
            cdm: CdmEngineFactory instance.
            sqldict: Optional key from ``CDMSQL`` to pick a canned query.
            query: Raw SQL string (used if provided).
            chunksize: Unused; kept for future streaming support.

        Returns:
            SQLAlchemy AsyncResult.
        """
        if sqldict:
            query = CDMSQL[sqldict]
        if not isinstance(query, str) or not query:
            raise ValueError("Query must be a non-empty string.")
        logger.info(f"Executing query: {query}")
        async with cdm.session() as session:
            result = await session.execute(text(query))
        await session.close()
        return result

    def result_to_df(self, result):
        """Convert a Result to a DataFrame.

        Args:
            result: SQLAlchemy Result or AsyncResult.

        Returns:
            pandas.DataFrame of result mappings.
        """
        list_of_dicts = result.mappings().all()
        """Convert a list of dictionaries to a DataFrame."""
        if not list_of_dicts:
            return pd.DataFrame()
        return pd.DataFrame(list_of_dicts)

    async def query_library(self, cdm, resource="person", query_name="PE02"):
        """Fetch a query from OHDSI QueryLibrary and execute it.

        Args:
            cdm: CdmEngineFactory instance.
            resource: Query resource subfolder (e.g., "person").
            query_name: Query markdown file name (e.g., "PE02").

        Returns:
            SQLAlchemy AsyncResult.
        """
        # Get the markdown from the query library repository: https://github.com/OHDSI/QueryLibrary/blob/master/inst/shinyApps/QueryLibrary/queries/person/PE02.md
        url = f"https://raw.githubusercontent.com/OHDSI/QueryLibrary/master/inst/shinyApps/QueryLibrary/queries/{resource}/{query_name}.md"
        markdown = requests.get(url)
        if markdown.status_code != 200:
            raise ValueError(f"Query {query_name} not found in the Query Library.")
        query = markdown.text.split("```sql")[1].split("```")[0].strip()
        # remove @cdm. and @vocab. references
        query = query.replace("@cdm.", "").replace("@vocab.", "")
        if not query:
            raise ValueError(f"Query {query_name} is empty.")
        return await self.execute(cdm, query=query)
