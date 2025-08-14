import asyncio

from pyomop import CdmEngineFactory
from pyomop.cdm54 import Base
from pyomop.loader import CdmCsvLoader


async def main():
    # Create a local SQLite CDM and initialize tables
    cdm = CdmEngineFactory(
        db="pgsql",
        host="10.0.0.211",
        port=5432,
        user="postgres",
        pw="pword",
        name="postgres",
        schema="public"
    )
    engine = cdm.engine
    await cdm.init_models(Base.metadata)

    # Load CSV into OMOP tables using mapping
    loader = CdmCsvLoader(cdm)
    await loader.load(
        csv_path="tests/fhir_export.csv",
        mapping_path="examples/mapping.example.json",
        chunk_size=500,
    )


if __name__ == "__main__":
    asyncio.run(main())
