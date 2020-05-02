from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

class CdmEngineFactory(object):

    def __init__(self, db = 'sqlite', host = 'localhost', port = 3306, name = 'cdm6.sqlite', schema = ''):
        self._db = db
        self._name = name
        self._host = host
        self._port = port
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
    def schema(self):
        return self._schema

    @property
    def engine(self):
        return self._engine

    @property
    def base(self):
        if self._engine is not None:
            Base = automap_base()
            Base.prepare(self._engine, reflect=True)
            return Base.classes
        return None
    
    @property
    def engine(self):
        if self._db is 'sqlite':
            self._engine = create_engine("sqlite:///"+self._name)
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

    @schema.setter
    def schema(self, value):
        self._schema = value



    
    