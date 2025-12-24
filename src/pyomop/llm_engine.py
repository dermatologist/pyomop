"""LLM-oriented SQLDatabase wrapper for OMOP CDM.

This module provides utilities for connecting LLMs to OMOP CDM databases
using langchain's SQL toolkit and agents. It uses the OMOP CDM metadata
from this package's SQLAlchemy models to enable LLM-powered query components
to reason about available tables, columns, and foreign keys.

This file is import-safe even when the optional LLM extras are not installed;
in that case, attempting to instantiate ``CDMDatabase`` will raise a clear
ImportError directing you to install ``pyomop[llm]``.
"""

from typing import Any, cast

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine

try:
    from sqlalchemy.ext.asyncio import AsyncEngine
except Exception:  # pragma: no cover
    AsyncEngine = None  # type: ignore[assignment,misc]

try:  # Optional dependency
    from langchain_community.utilities import SQLDatabase as _SQLDatabase
    _LLM_AVAILABLE = True
except Exception:  # pragma: no cover
    _LLM_AVAILABLE = False


if _LLM_AVAILABLE:
    SQLDatabase = _SQLDatabase
else:
    class SQLDatabase:  # type: ignore[no-redef]
        """Minimal stub to allow import without LLM extras."""

        pass


class CDMDatabase(SQLDatabase):
    """OMOP-aware SQLDatabase for LLM query engines.

    This class wraps langchain's ``SQLDatabase`` to use the OMOP CDM
    SQLAlchemy metadata bundled with this package, making it easy to expose
    concise schema information to LLM components.

    Args:
        engine: SQLAlchemy ``Engine`` connected to the OMOP database.
        schema: Optional database schema name.
        ignore_tables: Tables to hide from the LLM context.
        include_tables: Explicit subset of tables to expose.
        sample_rows_in_table_info: Number of sample rows to include in table info.
        max_string_length: Max length of generated descriptions.
        version: OMOP CDM version label ("cdm54" or "cdm6").
    """

    def __init__(
        self,
        engine: Engine,
        schema: str | None = None,
        ignore_tables: list[str] | None = None,
        include_tables: list[str] | None = None,
        sample_rows_in_table_info: int = 3,
        max_string_length: int = 300,
        version: str = "cdm54",
    ) -> None:
        if not _LLM_AVAILABLE:  # pragma: no cover - import-safe guard
            raise ImportError("Install 'pyomop[llm]' to use LLM features.")

        # Basic configuration
        self._engine = engine
        self._schema = schema
        self._max_string_length = max_string_length

        if include_tables and ignore_tables:
            raise ValueError("Cannot specify both include_tables and ignore_tables")

        # Load OMOP metadata for the chosen version
        Base: Any
        if version == "cdm6":
            from .cdm6 import Base
        else:
            from .cdm54 import Base

        metadata = cast(MetaData, Base.metadata)

        # All known tables
        self._all_tables = set(metadata.tables.keys())

        # Validate include/ignore lists
        self._include_tables = set(include_tables) if include_tables else set()
        if self._include_tables:
            missing = self._include_tables - self._all_tables
            if missing:
                raise ValueError(f"include_tables {missing} not found in OMOP metadata")

        self._ignore_tables = set(ignore_tables) if ignore_tables else set()
        if self._ignore_tables:
            missing = self._ignore_tables - self._all_tables
            if missing:
                raise ValueError(f"ignore_tables {missing} not found in OMOP metadata")

        if self._include_tables:
            usable = set(self._include_tables)
        elif self._ignore_tables:
            usable = self._all_tables - self._ignore_tables
        else:
            usable = set(self._all_tables)
        self._usable_tables = usable

        if not isinstance(sample_rows_in_table_info, int):
            raise TypeError("sample_rows_in_table_info must be an integer")
        self._sample_rows_in_table_info = sample_rows_in_table_info

        self._metadata = metadata

        # Initialize parent SQLDatabase
        # Convert AsyncEngine to sync if needed (langchain requires sync engine)
        parent_engine: Engine
        if AsyncEngine is not None and isinstance(self._engine, AsyncEngine):
            url_str = str(self._engine.url)
            # Convert common async driver URLs to sync variants
            url_str = (
                url_str.replace("+aiosqlite", "")
                .replace("+asyncpg", "")
                .replace("+psycopg_async", "+psycopg2")
            )
            parent_engine = create_engine(url_str)
        else:
            parent_engine = self._engine

        super().__init__(
            engine=parent_engine,
            schema=schema,
            include_tables=sorted(self._usable_tables) if self._usable_tables else None,
            sample_rows_in_table_info=sample_rows_in_table_info,
        )

    # --- Helper methods for LLM context ---
    def get_table_columns(self, table_name: str) -> list[str]:
        """Return list of column names for a table.

        This uses the OMOP SQLAlchemy ``MetaData`` instead of DB inspector.
        """
        return [col.name for col in self._metadata.tables[table_name].columns]

    def get_single_table_info(self, table_name: str) -> str:
        """Return a concise description of columns and foreign keys for a table.

        The format is compatible with langchain's SQL components.
        """
        template = "Table '{table_name}' has columns: {columns}, and foreign keys: {foreign_keys}."
        columns: list[str] = []
        foreign_keys: list[str] = []
        for column in self._metadata.tables[table_name].columns:
            columns.append(f"{column.name} ({column.type!s})")
            for fk in column.foreign_keys:
                foreign_keys.append(
                    f"{column.name} -> {fk.column.table.name}.{fk.column.name}"
                )
        column_str = ", ".join(columns)
        fk_str = ", ".join(foreign_keys)
        return template.format(
            table_name=table_name, columns=column_str, foreign_keys=fk_str
        )

    def usable_tables(self) -> list[str]:
        """Return the sorted list of tables exposed to the LLM.

        This respects include/ignore settings passed at initialization.
        """
        return sorted(self._usable_tables)

    # Backwards compat helper name used in some code paths
    def get_usable_table_names(self) -> list[str]:  # pragma: no cover - thin wrapper
        return self.usable_tables()

