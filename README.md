# pyomop

OMOP CDM utils


## Description

A longer description of your project goes here...

## Installation

```
pip install pyomop
```

## Usage

```
from pyomop import CdmEngineFactory
from pyomop import metadata
from sqlalchemy.sql import select
import datetime

cdm = CdmEngineFactory()

engine = cdm.engine
# Create Tables 
metadata.create_all(engine)

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

```

## Note

This project has been set up using PyScaffold 3.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.
