import asyncio

from pyomop import CdmEngineFactory
from pyomop.cdm54 import Base
from pyomop.loader import CdmCsvLoader
from pyomop.vocabulary import CdmVocabulary


RESET_DB = True
async def main():
    # Create a local SQLite CDM and initialize tables
    cdm = CdmEngineFactory(
        # db="pgsql",
        # host="10.0.0.211",
        # port=5432,
        # user="postgres",
        # pw="pword",
        # name="postgres",
        # schema="public",
    )
    engine = cdm.engine
    if RESET_DB:
        await cdm.init_models(Base.metadata)

    vocab = CdmVocabulary(cdm, version="cdm54")
    await vocab.create_vocab("~/Downloads/omop-vocab")


if __name__ == "__main__":
    asyncio.run(main())
