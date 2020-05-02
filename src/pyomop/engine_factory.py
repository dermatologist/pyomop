from sqlalchemy import create_engine

class CdmEngineFactory(object):

    def __init__(self, db = 'sqlite', host = 'localhost', port = 3306, name = 'cdm6.sqlite', schema = ''):
        self._db = db
        self._name = name
        self._host = host
        self._port = port
        self._schema = schema
    
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

    def get_engine(self):
        if self._db is 'sqlite':
            engine = create_engine("sqlite:///"+self._name)
        return engine