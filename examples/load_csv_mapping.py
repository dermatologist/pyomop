import asyncio

from src.pyomop import CdmEngineFactory
from src.pyomop.cdm54 import Base
from src.pyomop.loader import CdmCsvLoader


async def main():
    # Create a local SQLite CDM and initialize tables
    cdm = CdmEngineFactory(db="sqlite", name="cdm_example.sqlite")
    await cdm.init_models(Base.metadata)

    # Load CSV into OMOP tables using mapping
    loader = CdmCsvLoader(cdm)
    await loader.load(
        csv_path="tests/patient3.csv",
        mapping_path="examples/mapping.example.json",
        chunk_size=500,
    )


if __name__ == "__main__":
    asyncio.run(main())
