import asyncio
import datetime
from pyomop import CdmEngineFactory
from pyomop.cdm54 import Person, Base # Import CDM v5.4 models

async def main():
    # 1. Initialize Engine (Default: SQLite)
    cdm = CdmEngineFactory()

    # 2. Initialize Tables (Create Schema)
    await cdm.init_models(Base.metadata)

    # 3. Create a Session and Add Data
    async with cdm.session() as session:
        async with session.begin():
            new_person = Person(
                person_id=100,
                year_of_birth=1990,
                month_of_birth=1,
                day_of_birth=1,
                birth_datetime=datetime.datetime(1990, 1, 1),
                gender_concept_id=8532, # Female
                race_concept_id=8527,   # White
                ethnicity_concept_id=38003564 # Not Hispanic
            )
            session.add(new_person)
        await session.commit()

        # Query Data
        person = await session.get(Person, 100)
        print(f"Found Person: ID={person.person_id}, Year={person.year_of_birth}")

        # Close session
        await session.close()

    # Dispose engine
    await cdm.engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
