from . import CdmEngineFactory
import pandas as pd
from .cdm6_tables import Concept
import asyncio
class CdmVocabulary(object):
    def __init__(self, cdm):
        self._concept_id = 0
        self._concept_name = ''
        self._domain_id = ''
        self._vocabulary_id = ''
        self._concept_class_id = ''
        self._concept_code = ''
        self._cdm = cdm
        self._engine = cdm.engine

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
        # _concept = self._cdm.session.query(Concept).filter_by(concept_id=concept_id).one()
        _concept = asyncio.run(self.get_concept(concept_id))
        self._concept_name = _concept.concept_name
        self._domain_id = _concept.domain_id
        self._vocabulary_id = _concept.vocabulary_id
        self._concept_class_id = _concept.concept_class_id
        self._concept_code = _concept.concept_code

    async def get_concept(self, concept_id):
        stmt = self._cdm.session.query(Concept).filter_by(concept_id=concept_id).one()
        _concept = await self._cdm.session.execute(stmt)
        return _concept

    async def get_concept_by_code(self, concept_code, vocabulary_id):
        stmt = self._cdm.session.query(Concept).filter_by(concept_code=concept_code) \
            .filter_by(vocabulary_id=vocabulary_id).one()
        _concept = await self._cdm.session.execute(stmt)
        return _concept

    def set_concept(self, concept_code, vocabulary_id=None):
        self._concept_code = concept_code
        try:
            if vocabulary_id is not None:
                self._vocabulary_id = vocabulary_id
                _concept = asyncio.run(self.get_concept_by_code(concept_code, vocabulary_id))
            else:
                _concept = asyncio.run(self.get_concept_by_code(concept_code))
                self._vocabulary_id = _concept.vocabulary_id

            self._concept_name = _concept.concept_name
            self._domain_id = _concept.domain_id
            self._concept_id = _concept.concept_id
            self._concept_class_id = _concept.concept_class_id
            self._concept_code = _concept.concept_code

        except:
            self._vocabulary_id = 0
            self._concept_id = 0

    def create_vocab(self, folder, sample=10):
        if sample < 1000: # nrows=sample
            try:
                df = pd.read_csv(folder + '/DRUG_STRENGTH.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'drug_strength', 'replace'))
                # df.to_sql('drug_strength', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'concept', 'replace'))
                # df.to_sql('concept', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_RELATIONSHIP.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'concept_relationship', 'replace'))
                # df.to_sql('concept_relationship', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_ANCESTOR.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'concept_ancester', 'replace'))
                # df.to_sql('concept_ancester', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_SYNONYM.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'concept_synonym', 'replace'))
                # df.to_sql('concept_synonym', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/VOCABULARY.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'vocabulary', 'replace'))
                # df.to_sql('vocabulary', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/RELATIONSHIP.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'relationship', 'replace'))
                # df.to_sql('relationship', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_CLASS.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'concept_class', 'replace'))
                # df.to_sql('concept_class', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/DOMAIN.csv', sep='\t', nrows=sample, on_bad_lines='skip')
                asyncio.run(self.write_vocab(df, 'domain', 'replace'))
                # df.to_sql('domain', con=self._engine, if_exists = 'replace')
            except ValueError:
                print("Oops!  Could not write vocabulary")
        else:
            try:
                df = pd.read_csv(folder + '/DRUG_STRENGTH.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('drug_strength', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('concept', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_RELATIONSHIP.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('concept_relationship', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_ANCESTOR.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('concept_ancester', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_SYNONYM.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('concept_synonym', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/VOCABULARY.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('vocabulary', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/RELATIONSHIP.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('relationship', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/CONCEPT_CLASS.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('concept_class', con=self._engine, if_exists = 'replace')
                df = pd.read_csv(folder + '/DOMAIN.csv', sep='\t', on_bad_lines='skip')
                df.to_sql('domain', con=self._engine, if_exists = 'replace')
            except ValueError:
                print("Oops!  Could not write vocabulary")

    async def write_vocab(self, df, table, if_exists='replace'):
        # async with engine.begin() as conn:
        #     await conn.run_sync(df.to_sql, table, con=conn, if_exists=if_exists)
        async with self._cdm.session() as session:
            conn = await session.connection()
            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    table,
                    if_exists=if_exists,
                ),
            )