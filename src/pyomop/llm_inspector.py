"""SQL wrapper around SQLDatabase in langchain."""
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import MetaData, create_engine, insert, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

class LLMInspetor:

    def __init__(
            self,
            engine: Engine,
    ):
        self._engine = engine


    def get_table_names(self):
        table_names = inspect(self._engine).get_table_names()
        return table_names