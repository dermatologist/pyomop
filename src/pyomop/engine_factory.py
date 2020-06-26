from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

class CdmEngineFactory(object):

    def __init__(self, db = 'sqlite', 
                host = 'localhost', port = 5432, 
                user = 'root', pw='pass',
                name = 'cdm6.sqlite', schema = 'public'):
        self._db = db
        self._name = name
        self._host = host
        self._port = port
        self._user = user
        self._pw = pw
        self._schema = schema
        self._engine = None
        self._base = None
    
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
    def engine(self):
        return self.engine

    @property
    def base(self):
        if self.engine is not None:  # Not self_engine
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            return Base.classes
        return None
    
    @property
    def engine(self):
        if self._db is 'sqlite':
            self._engine = create_engine("sqlite:///"+self._name)
        if self._db is 'mysql':
            mysql_url = 'mysql://{}:{}@{}:{}/{}'
            mysql_url = mysql_url.format(self._user, self._pw, self._host, self._port, self._name)
            self._engine = create_engine(mysql_url, isolation_level="READ UNCOMMITTED")
        if self._db is 'pgsql':
            # https://stackoverflow.com/questions/9298296/sqlalchemy-support-of-postgres-schemas
            dbschema = '{},public'  # Searches left-to-right
            dbschema = dbschema.format(self._schema)
            pgsql_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'
            pgsql_url = pgsql_url.format(self._user, self._pw,
                                        self._host, self._port, self._name)
            self._engine = create_engine(
                pgsql_url,
                connect_args={'options': '-csearch_path={}'.format(dbschema)})
        return self._engine

    @property
    def session(self):
        if self._engine is not None:
            return Session(self._engine)
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



    
    