from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata
from sqlalchemy.sql import select
import datetime

cdm = CdmEngineFactory()  # Creates SQLite database by default

engine = cdm.engine
## Create Tables if required
metadata.create_all(engine)
## Create vocabulary if required
vocab = CdmVocabulary(cdm)
# vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

# SQLAlchemy as ORM
session =  cdm.session
session.add(Cohort(cohort_definition_id=2, subject_id=100, 
            cohort_end_date=datetime.datetime.now(), 
            cohort_start_date=datetime.datetime.now()))
session.commit()

result = session.query(Cohort).all()
for row in result:
    print(row)

# Convert result to a pandas dataframe
vec = CdmVector()
vec.result = result
print(vec.df.dtypes)

# Execute a query and convert it to dataframe
vec.sql_df(cdm, 'TEST') # TEST is defined in sqldict.py
print(vec.df.dtypes)
# OR
vec.sql_df(cdm, query='SELECT * from cohort')
print(vec.df.dtypes)
