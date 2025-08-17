"""Engine and session factory for OMOP CDM databases.

This module provides an asynchronous SQLAlchemy engine factory with helpers to
create/init CDM schemas and obtain async sessions across supported backends
(SQLite, MySQL, PostgreSQL).
"""

# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session
# from sqlalchemy.ext.automap import automap_base

import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.ext.automap import automap_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CdmEngineFactory(object):
    """Factory to create async SQLAlchemy engines and sessions for OMOP CDM.

    Supports SQLite (default), MySQL, and PostgreSQL. Exposes convenience
    properties for the configured engine and async session maker.

    Args:
        db: Database type: "sqlite", "mysql", or "pgsql".
        host: Database host (ignored for SQLite).
        port: Database port (ignored for SQLite).
        user: Database user (ignored for SQLite).
        pw: Database password (ignored for SQLite).
        name: Database name or SQLite filename.
        schema: PostgreSQL schema to use for CDM.
    """

    def __init__(
        self,
        db="sqlite",
        host="localhost",
        port=5432,
        user="root",
        pw="pass",
        name="cdm.sqlite",
        schema="public",
    ):
        self._db = db
        self._name = name
        self._host = host
        self._port = port
        self._user = user
        self._pw = pw
        self._schema = schema
        self._engine = None
        self._base = None

    async def init_models(self, metadata):
        """Drop and re-create all tables from provided metadata.

        This is mainly used for tests and quick local setups.

        Args:
            metadata: SQLAlchemy ``MetaData`` containing table definitions.

        Raises:
            ValueError: If the engine has not been initialized.
        """
        if self._engine is None:
            raise ValueError("Database engine is not initialized.")
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)

    @property
    def db(self):
        """Return the configured database type (sqlite/mysql/pgsql)."""
        return self._db

    @property
    def host(self):
        """Return the configured database host (if applicable)."""
        return self._host

    @property
    def port(self):
        """Return the configured database port (if applicable)."""
        return self._port

    @property
    def name(self):
        """Return the configured database name or SQLite filename."""
        return self._name

    @property
    def user(self):
        """Return the configured database user (if applicable)."""
        return self._user

    @property
    def pw(self):
        """Return the configured database password (if applicable)."""
        return self._pw

    @property
    def schema(self):
        """Return the configured schema (PostgreSQL)."""
        return self._schema

    @property
    def base(self):
        """Return automapped classes when an engine exists, otherwise None."""
        if self.engine is not None:  # Not self_engine
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            return Base.classes
        return None

    @property
    def engine(self):
        """Create or return the async engine for the configured backend.

        Returns:
            Async engine instance bound to the configured database.
        """
        if self._db == "sqlite":
            # Schemas are not supported for SQLite; warn if a non-default schema was provided
            if self._schema and self._schema not in ("", "public"):
                logger.warning(
                    "Schema is not supported for SQLite; ignoring schema='%s'",
                    self._schema,
                )
            self._engine = create_async_engine("sqlite+aiosqlite:///" + self._name)
        elif self._db == "mysql":
            # Schemas are not supported for MySQL in the same way as PostgreSQL; warn and ignore
            if self._schema and self._schema not in ("", "public"):
                logger.warning(
                    "Schema is not supported for MySQL; ignoring schema='%s'",
                    self._schema,
                )
            mysql_url = "mysql://{}:{}@{}:{}/{}"
            mysql_url = mysql_url.format(
                self._user, self._pw, self._host, self._port, self._name
            )
            self._engine = create_async_engine(
                mysql_url, isolation_level="READ UNCOMMITTED"
            )
        elif self._db == "pgsql":
            pgsql_url = "postgresql+asyncpg://{}:{}@{}:{}/{}"
            pgsql_url = pgsql_url.format(
                self._user, self._pw, self._host, self._port, self._name
            )
            connect_args = {}
            # If a schema is provided, set the PostgreSQL search_path so that all
            # operations (reflection, DDL/DML) use this schema by default.
            if self._schema and self._schema != "":
                connect_args = {"server_settings": {"search_path": self._schema}}
            self._engine = create_async_engine(pgsql_url, connect_args=connect_args)
        else:
            # Unknown DB type; create no engine and warn
            logger.warning("Unknown database type '%s'—no engine created.", self._db)
            return None
        return self._engine

    @property
    def session(self):
        """Return an async_sessionmaker for creating AsyncSession objects."""
        if self._engine is not None:
            async_session = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
            return async_session
        return None

    @property
    def async_session(self):
        """Alias for session to maintain backward compatibility."""
        if self._engine is not None:
            async_session = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
            return async_session
        return None

    @db.setter
    def db(self, value):
        self._db = value

    @name.setter
    def name(self, value):
        self._name = value

    @port.setter
    def port(self, value):
        self._port = value

    @host.setter
    def host(self, value):
        self._host = value

    @user.setter
    def user(self, value):
        self._user = value

    @pw.setter
    def pw(self, value):
        self._pw = value

    @schema.setter
    def schema(self, value):
        self._schema = value
