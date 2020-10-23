import pandas as pd
from sqlalchemy.inspection import inspect
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
        for obj in self._result:
            instance = inspect(obj)
            items = instance.attrs.items()
            result_list.append([x.value for _,x in items])
        return instance.attrs.keys(), result_list

    def create_df(self, _names=None):
        names, data = self.query_to_list()
        if(_names):
            names = _names
        self._df = pd.DataFrame.from_records(data, columns=names)
    
    def sql_df(self, cdm, sqldict=None, query=None, chunksize=None):
        if sqldict:
            query=CDMSQL[sqldict]
        if chunksize:
            self._df = pd.read_sql_query(query, cdm.engine)
        else:
            self._df = pd.read_sql_query(query, cdm.engine, chunksize)  
  
