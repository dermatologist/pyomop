from pyomop import CdmEngineFactory, CdmVocabulary, Cohort, Vocabulary, metadata
from sqlalchemy.sql import select
import datetime

cdm = CdmEngineFactory()  # Creates SQLite database by default

engine = cdm.engine
# Create Tables 
metadata.create_all(engine)
# Create vocabulary
vocab = CdmVocabulary(cdm)
# vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

# SQLAlchemy as ORM
session =  cdm.session
session.add(Cohort(cohort_definition_id=2, subject_id=100, 
            cohort_end_date=datetime.datetime.now(), 
            cohort_start_date=datetime.datetime.now()))
session.commit()

s = select([Cohort])
result = session.execute(s)
for row in result:
    print(row)
result.close()
for v in session.query(Vocabulary).order_by(Vocabulary.vocabulary_name):
    print(v.vocabulary_name)