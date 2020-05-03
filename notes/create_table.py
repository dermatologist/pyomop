from engine_factory import CdmEngineFactory
from cdm6_tables import metadata
from sqlalchemy.sql import select
import datetime

cdm = CdmEngineFactory()

engine = cdm.engine
metadata.create_all(engine)
metadata.reflect(bind=engine)
for table in reversed(metadata.sorted_tables):
    print(table)



Cohort = cdm.base.cohort
session =  cdm.session
session.add(Cohort(cohort_definition_id=2, subject_id=100, 
            cohort_end_date=datetime.datetime.now(), 
            cohort_start_date=datetime.datetime.now()))
session.commit()

s = select([Cohort])
result = session.execute(s)
for row in result:
    print(row)