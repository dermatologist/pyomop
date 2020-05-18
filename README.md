# pyomop

OMOP CDM utils

## Description

The OMOP Common Data Model allows for the systematic analysis of healthcare observational databases. This is a python library to do various operations on the CDM v6 compliant databases. This is WIP and may change considerably. Please don't use it in production yet.

### Support
* Postgres
* MySQL
* SqLite

## Installation

```
pip install https://github.com/dermatologist/pyomop/releases/download/1.1.1/pyomop-1.1.1-py2.py3-none-any.whl

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

