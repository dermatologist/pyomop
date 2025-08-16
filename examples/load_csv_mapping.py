import asyncio

from pyomop import CdmEngineFactory
from pyomop.cdm54 import Base
from pyomop.loader import CdmCsvLoader
from pyomop.vocabulary import CdmVocabulary

RESET_DB = False  # Set True to drop and recreate all tables (will delete existing data)


async def main():
    # Create a local SQLite CDM and initialize tables
    cdm = CdmEngineFactory(
        # db="pgsql",
        # host="10.0.0.211",
        # port=5432,
        # user="postgres",
        # pw="pword",
        # name="postgres",
        # schema="public"
    )
    engine = cdm.engine
    if RESET_DB:
        await cdm.init_models(Base.metadata)

    loader = CdmCsvLoader(cdm)
    # Load CSV into OMOP tables using mapping
    await loader.load(
        csv_path="tests/fhir_export.csv",
        # mapping_path="tests/mapping.example.json",
        chunk_size=500,
    )


if __name__ == "__main__":
    asyncio.run(main())
