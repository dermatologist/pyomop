import asyncio
import pandas as pd
from sqlalchemy.inspection import inspect
from sqlalchemy import text
import requests
from .sqldict import CDMSQL
from logging import getLogger

logger = getLogger(__name__)

# https://gist.github.com/dermatologist/f436cb461a3290732a27c4dc040229f9
# Thank you! https://gist.github.com/garaud
class CdmVector(object):

    async def execute(self, cdm, sqldict=None, query=None, chunksize=1000):
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
        list_of_dicts = result.mappings().all()
        """Convert a list of dictionaries to a DataFrame."""
        if not list_of_dicts:
            return pd.DataFrame()
        return pd.DataFrame(list_of_dicts)

    async def query_library(self, cdm, resource="person", query_name= "PE02"):
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
