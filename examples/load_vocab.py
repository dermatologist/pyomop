import asyncio

from pyomop import CdmEngineFactory
from pyomop.cdm54 import Base
from pyomop.loader import CdmCsvLoader
from pyomop.vocabulary import CdmVocabulary


async def main():
    # Create a local SQLite CDM and initialize tables
    cdm = CdmEngineFactory()
    engine = cdm.engine
    await cdm.init_models(Base.metadata)

    vocab = CdmVocabulary(cdm, version="cdm54")
    await vocab.create_vocab("~/Downloads/omop-vocab")


if __name__ == "__main__":
    asyncio.run(main())
