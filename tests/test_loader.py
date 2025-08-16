import asyncio
from pathlib import Path

import pandas as pd
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.automap import automap_base


def test_loader_person_backfill_and_fk_fix(tmp_path):
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.loader import CdmCsvLoader
    from src.pyomop.vocabulary import CdmVocabulary

    async def run():
        db_path = tmp_path / "test_loader.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(db_path))
        _ = cdm.engine  # initialize engine
        await cdm.init_models(Base.metadata)

        vocab = CdmVocabulary(cdm)
        await vocab.create_vocab("tests")

        loader = CdmCsvLoader(cdm)
        await loader.load(
            csv_path="tests/fhir_export.csv",
            mapping_path=str(Path("src/pyomop/mapping.default.json")),
            chunk_size=200,
        )

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prepare(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prepare)
            person = getattr(BaseMap.classes, "person").__table__

            total_persons = (
                await session.execute(select(func.count()).select_from(person))
            ).scalar_one()
            assert total_persons > 0

            with_bd = (
                await session.execute(
                    select(func.count())
                    .select_from(person)
                    .where(person.c.birth_datetime.isnot(None))
                )
            ).scalar_one()
            if with_bd > 0:
                zeros_y = (
                    await session.execute(
                        select(func.count())
                        .select_from(person)
                        .where(
                            person.c.birth_datetime.isnot(None),
                            (person.c.year_of_birth.is_(None))
                            | (person.c.year_of_birth == 0),
                        )
                    )
                ).scalar_one()
                zeros_m = (
                    await session.execute(
                        select(func.count())
                        .select_from(person)
                        .where(
                            person.c.birth_datetime.isnot(None),
                            (person.c.month_of_birth.is_(None))
                            | (person.c.month_of_birth == 0),
                        )
                    )
                ).scalar_one()
                zeros_d = (
                    await session.execute(
                        select(func.count())
                        .select_from(person)
                        .where(
                            person.c.birth_datetime.isnot(None),
                            (person.c.day_of_birth.is_(None))
                            | (person.c.day_of_birth == 0),
                        )
                    )
                ).scalar_one()
                assert zeros_y == 0
                assert zeros_m == 0
                assert zeros_d == 0

            try:
                observation = getattr(BaseMap.classes, "observation").__table__
                persons_alias = person.alias()
                orphans = (
                    await session.execute(
                        select(func.count())
                        .select_from(
                            observation.outerjoin(
                                persons_alias,
                                observation.c.person_id == persons_alias.c.person_id,
                            )
                        )
                        .where(persons_alias.c.person_id.is_(None))
                    )
                ).scalar_one()
                obs_total = (
                    await session.execute(select(func.count()).select_from(observation))
                ).scalar_one()
                if obs_total > 0:
                    assert orphans == 0
            except AttributeError:
                pass

    asyncio.run(run())


def test_concept_mapping_uses_first_of_comma_separated(tmp_path):
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.loader import CdmCsvLoader
    from src.pyomop.vocabulary import CdmVocabulary

    async def run():
        db_path = tmp_path / "test_loader_concept.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(db_path))
        _ = cdm.engine  # initialize engine
        await cdm.init_models(Base.metadata)

        vocab = CdmVocabulary(cdm)
        await vocab.create_vocab("tests")

        loader = CdmCsvLoader(cdm)
        await loader.load(
            csv_path="tests/fhir_export.csv",
            mapping_path=str(Path("src/pyomop/mapping.default.json")),
            chunk_size=200,
        )

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prepare(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prepare)

            concept = getattr(BaseMap.classes, "concept").__table__
            person = getattr(BaseMap.classes, "person").__table__
            observation = getattr(BaseMap.classes, "observation").__table__

            code_row = (
                await session.execute(
                    select(concept.c.concept_code, concept.c.concept_id).limit(1)
                )
            ).first()
            assert code_row is not None
            test_code, expected_cid = code_row

            obs_pk_col = list(observation.primary_key.columns)[0]
            obs_row = (
                await session.execute(
                    select(obs_pk_col, observation.c.person_id).limit(1)
                )
            ).first()
            if obs_row is None:
                pid_row = (
                    await session.execute(select(person.c.person_id).limit(1))
                ).first()
                assert pid_row is not None
                pid = pid_row[0]
                ins_stmt = insert(observation).values(
                    person_id=pid,
                    observation_concept_id=0,
                    observation_type_concept_id=0,
                    observation_date=pd.Timestamp("2020-01-01").date(),
                    observation_source_value="placeholder",
                )
                await session.execute(ins_stmt)
                await session.commit()
                obs_row = (
                    await session.execute(
                        select(obs_pk_col, observation.c.person_id).limit(1)
                    )
                ).first()

            assert obs_row is not None
            obs_id = obs_row[0]

            upd = (
                update(observation)
                .where(obs_pk_col == obs_id)
                .values(
                    observation_source_value=f"{test_code},OTHER",
                    observation_concept_id=0,
                )
            )
            await session.execute(upd)

            import json

            mapping = json.loads(Path("src/pyomop/mapping.default.json").read_text())
            await loader.apply_concept_mappings(session, BaseMap, mapping)
            await session.commit()

            check = (
                await session.execute(
                    select(observation.c.observation_concept_id).where(
                        obs_pk_col == obs_id
                    )
                )
            ).scalar_one()
            assert check == expected_cid

    asyncio.run(run())
