from pyomop import CdmEngineFactory, CdmVector
import asyncio

from tests import test_d_vector

cdm = CdmEngineFactory()  # Creates SQLite database by default

engine = cdm.engine
vec = CdmVector()

asyncio.run(test_d_vector.create_vector(cdm, vec, engine))
