import asyncio
import pandas as pd
from sqlalchemy.inspection import inspect
from sqlalchemy import text
from .sqldict import CDMSQL
# https://gist.github.com/dermatologist/f436cb461a3290732a27c4dc040229f9
# Thank you! https://gist.github.com/garaud
class CdmVector(object):

    def __init__(self, result=None):
        self._result = result
        self._df = None

    @property
    def df(self):
        if self._df is None:
            self.create_df()
        return self._df

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, value):
        self._result = value

    def query_to_list(self):
        """List of result
        Return: columns name, list of result
        """
        result_list = []
        instance = None
        if self._result is None:
            return None, []
        for obj in self._result:
            instance = inspect(obj)
            items = instance.attrs.items()
            result_list.append([x.value for _,x in items])
        if instance is None:
            return None, []
        return instance.attrs.keys(), result_list

    def create_df(self, _names=None):
        names, data = self.query_to_list()
        if(_names):
            names = _names
        self._df = pd.DataFrame.from_records(data, columns=names)

    async def sql_df(self, cdm, sqldict=None, query=None, chunksize=1000):
        if sqldict:
            query=CDMSQL[sqldict]
        async with cdm.session() as session:
            result = await session.execute(text(query))
        self._result = result
        await session.close()
        return result
