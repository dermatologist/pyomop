import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import numpy as np
import pandas as pd
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
)
from sqlalchemy.ext.automap import AutomapBase, automap_base


class CdmVocabulary(object):
    def __init__(self, cdm, version="cdm54"):
        self._concept_id = 0
        self._concept_name = ""
        self._domain_id = ""
        self._vocabulary_id = ""
        self._concept_class_id = ""
        self._concept_code = ""
        self._cdm = cdm
        self._engine = cdm.engine
        self._maker = async_sessionmaker(self._engine, class_=AsyncSession)
        self._scope = async_scoped_session(self._maker, scopefunc=asyncio.current_task)
        self._version = version

    @property
    def concept_id(self):
        return self._concept_id

    @property
    def concept_code(self):
        return self._concept_code

    @property
    def concept_name(self):
        return self._concept_name

    @property
    def vocabulary_id(self):
        return self._vocabulary_id

    @property
    def domain_id(self):
        return self._domain_id

    @concept_id.setter
    def concept_id(self, concept_id):
        self._concept_id = concept_id
        _concept = asyncio.run(self.get_concept(concept_id))
        self._concept_name = _concept.concept_name
        self._domain_id = _concept.domain_id
        self._vocabulary_id = _concept.vocabulary_id
        self._concept_class_id = _concept.concept_class_id
        self._concept_code = _concept.concept_code

    async def get_concept(self, concept_id):
        if self._version == "cdm6":
            from .cdm6 import Concept
        else:
            from .cdm54 import Concept
        stmt = select(Concept).where(Concept.concept_id == concept_id)
        async with self._cdm.session() as session:
            _concept = await session.execute(stmt)
        return _concept.scalar_one()

    async def get_concept_by_code(self, concept_code, vocabulary_id):
        if self._version == "cdm6":
            from .cdm6 import Concept
        else:
            from .cdm54 import Concept
        stmt = (
            select(Concept)
            .where(Concept.concept_code == concept_code)
            .where(Concept.vocabulary_id == vocabulary_id)
        )
        async with self._cdm.session() as session:
            _concept = await session.execute(stmt)
        return _concept.scalar_one()

    def set_concept(self, concept_code, vocabulary_id=None):
        self._concept_code = concept_code
        try:
            if vocabulary_id is not None:
                self._vocabulary_id = vocabulary_id
                _concept = asyncio.run(
                    self.get_concept_by_code(concept_code, vocabulary_id)
                )
            else:
                raise ValueError(
                    "vocabulary_id must be provided when setting concept by code."
                )

            self._concept_name = _concept.concept_name
            self._domain_id = _concept.domain_id
            self._concept_id = _concept.concept_id
            self._concept_class_id = _concept.concept_class_id
            self._concept_code = _concept.concept_code

        except:
            self._vocabulary_id = 0
            self._concept_id = 0

    def create_vocab(self, folder, sample=None):
        try:
            df = pd.read_csv(
                folder + "/DRUG_STRENGTH.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            # convert valid_start_date and valid_end_date to datetime
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            asyncio.run(self.write_vocab(df, "drug_strength", "replace"))
            # df.to_sql('drug_strength', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/CONCEPT.csv", sep="\t", nrows=sample, on_bad_lines="skip"
            )
            # convert valid_start_date and valid_end_date to datetime
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            asyncio.run(self.write_vocab(df, "concept", "replace"))
            # df.to_sql('concept', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/CONCEPT_RELATIONSHIP.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            # convert valid_start_date and valid_end_date to datetime
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            asyncio.run(self.write_vocab(df, "concept_relationship", "replace"))
            # df.to_sql('concept_relationship', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/CONCEPT_ANCESTOR.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            asyncio.run(self.write_vocab(df, "concept_ancestor", "replace"))
            # df.to_sql('concept_ancester', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/CONCEPT_SYNONYM.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            asyncio.run(self.write_vocab(df, "concept_synonym", "replace"))
            # df.to_sql('concept_synonym', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/VOCABULARY.csv", sep="\t", nrows=sample, on_bad_lines="skip"
            )
            asyncio.run(self.write_vocab(df, "vocabulary", "replace"))
            # df.to_sql('vocabulary', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/RELATIONSHIP.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            asyncio.run(self.write_vocab(df, "relationship", "replace"))
            # df.to_sql('relationship', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/CONCEPT_CLASS.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
            )
            asyncio.run(self.write_vocab(df, "concept_class", "replace"))
            # df.to_sql('concept_class', con=self._engine, if_exists = 'replace')
            df = pd.read_csv(
                folder + "/DOMAIN.csv", sep="\t", nrows=sample, on_bad_lines="skip"
            )
            asyncio.run(self.write_vocab(df, "domain", "replace"))
            # df.to_sql('domain', con=self._engine, if_exists = 'replace')
        except Exception as e:
            print(f"An error occurred while creating the vocabulary: {e}")

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._scope() as session:
            yield session

    async def write_vocab(self, df, table, if_exists="replace", chunk_size=1000):
        async with self.get_session() as session:
            conn = await session.connection()
            automap: AutomapBase = automap_base()

            def prepare_automap(sync_conn):
                automap.prepare(autoload_with=sync_conn)

            await conn.run_sync(prepare_automap)
            mapper = getattr(automap.classes, table)
            stmt = insert(mapper)

            for _, group in df.groupby(np.arange(df.shape[0], dtype=int) // chunk_size):
                await session.execute(stmt, group.to_dict("records"))
            await session.commit()
            await session.close()
