from engine_factory import CdmEngineFactory
from cdm6_tables import metadata

engine = CdmEngineFactory()

db = engine.get_engine()
metadata.create_all(db)