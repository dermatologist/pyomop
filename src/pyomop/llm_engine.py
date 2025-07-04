"""SQL wrapper around SQLDatabase in langchain."""

from typing import Any, List, Optional
from overrides import override

from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from llama_index.core import SQLDatabase


class CDMDatabase(SQLDatabase):

    def __init__(
        self,
        engine: Engine,
        schema: Optional[str] = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: bool = False,
        max_string_length: int = 300,
        version: str = "cdm54",
    ):
        """Create engine from database URI."""
        self._engine = engine
        self._schema = schema
        if include_tables and ignore_tables:
            raise ValueError("Cannot specify both include_tables and ignore_tables")

        if version == "cdm6":
            from .cdm6 import Base
        else:
            from .cdm54 import Base
        metadata = Base.metadata

        #! Inspect is not supported in SQL Alchemy 1.4. So getting info from metadata
        # self._inspector = inspect(self._engine)

        # including view support by adding the views as well as tables to the all
        # tables list if view_support is True
        self._all_tables = set(metadata.tables.keys())

        self._include_tables = set(include_tables) if include_tables else set()
        if self._include_tables:
            missing_tables = self._include_tables - self._all_tables
            if missing_tables:
                raise ValueError(
                    f"include_tables {missing_tables} not found in database"
                )
        self._ignore_tables = set(ignore_tables) if ignore_tables else set()
        if self._ignore_tables:
            missing_tables = self._ignore_tables - self._all_tables
            if missing_tables:
                raise ValueError(
                    f"ignore_tables {missing_tables} not found in database"
                )
        usable_tables = self.get_usable_table_names()
        self._usable_tables = set(usable_tables) if usable_tables else self._all_tables

        if not isinstance(sample_rows_in_table_info, int):
            raise TypeError("sample_rows_in_table_info must be an integer")

        self._sample_rows_in_table_info = sample_rows_in_table_info
        self._indexes_in_table_info = indexes_in_table_info

        self._custom_table_info = custom_table_info
        if self._custom_table_info:
            if not isinstance(self._custom_table_info, dict):
                raise TypeError(
                    "table_info must be a dictionary with table names as keys and the "
                    "desired table info as values"
                )
            # only keep the tables that are also present in the database
            intersection = set(self._custom_table_info).intersection(self._all_tables)
            self._custom_table_info = {
                table: info
                for table, info in self._custom_table_info.items()
                if table in intersection
            }

        self._max_string_length = max_string_length

        self._metadata = metadata or MetaData()

        # including view support if view_support = true
        # self._metadata.reflect(
        #     views=view_support,
        #     bind=self._engine,
        #     only=list(self._usable_tables),
        #     schema=self._schema,
        # )

    @override
    def get_table_columns(self, table_name: str) -> List[Any]:
        """Get table columns."""
        return self._metadata.tables[table_name].columns.keys()

    @override
    def get_single_table_info(self, table_name: str) -> str:
        """Get table info for a single table."""
        # same logic as table_info, but with specific table names
        template = (
            "Table '{table_name}' has columns: {columns}, "
            "and foreign keys: {foreign_keys}."
        )
        columns = []
        # print(self._metadata.tables[table_name].foreign_keys)
        foreign_keys = []
        for column in self._metadata.tables[table_name].columns:
            columns.append(f"{column.name} ({column.type!s})")
            for foreign_key in column.foreign_keys:
                foreign_keys.append(
                    f"{column.name} -> "
                    f"{foreign_key.column.table.name}.{foreign_key.column.name}"
                )
        column_str = ", ".join(columns)
        foreign_key_str = ", ".join(foreign_keys)

        #     if column.get("comment"):
        #         columns.append(
        #             f"{column['name']} ({column['type']!s}): "
        #             f"'{column.get('comment')}'"
        #         )
        #     else:
        #         columns.append(f"{column['name']} ({column['type']!s})")

        # column_str = ", ".join(columns)
        # foreign_keys = []
        # for foreign_key in self._inspector.get_foreign_keys(table_name):
        #     foreign_keys.append(
        #         f"{foreign_key['constrained_columns']} -> "
        #         f"{foreign_key['referred_table']}.{foreign_key['referred_columns']}"
        #     )
        # foreign_key_str = ", ".join(foreign_keys)
        return template.format(
            table_name=table_name, columns=column_str, foreign_keys=foreign_key_str
        )
