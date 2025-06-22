# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session
# from sqlalchemy.ext.automap import automap_base

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.ext.automap import automap_base


class CdmEngineFactory(object):

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
        if self._engine is None:
            raise ValueError("Database engine is not initialized.")
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)

    @property
    def db(self):
        return self._db

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def name(self):
        return self._name

    @property
    def user(self):
        return self._user

    @property
    def pw(self):
        return self._pw

    @property
    def schema(self):
        return self._schema

    @property
    def base(self):
        if self.engine is not None:  # Not self_engine
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            return Base.classes
        return None

    @property
    def engine(self):
        if self._db == "sqlite":
            self._engine = create_async_engine("sqlite+aiosqlite:///" + self._name)
        if self._db == "mysql":
            mysql_url = "mysql://{}:{}@{}:{}/{}"
            mysql_url = mysql_url.format(
                self._user, self._pw, self._host, self._port, self._name
            )
            self._engine = create_async_engine(
                mysql_url, isolation_level="READ UNCOMMITTED"
            )
        if self._db == "pgsql":
            # https://stackoverflow.com/questions/9298296/sqlalchemy-support-of-postgres-schemas
            dbschema = "{},public"  # Searches left-to-right
            dbschema = dbschema.format(self._schema)
            pgsql_url = "postgresql+psycopg2://{}:{}@{}:{}/{}"
            pgsql_url = pgsql_url.format(
                self._user, self._pw, self._host, self._port, self._name
            )
            self._engine = create_async_engine(
                pgsql_url, connect_args={"options": "-csearch_path={}".format(dbschema)}
            )
        return self._engine

    @property
    def session(self):
        if self._engine is not None:
            async_session = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
            return async_session
        return None

    @property
    def async_session(self):
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
