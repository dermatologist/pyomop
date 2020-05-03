# pyomop

OMOP CDM utils

## Description

A python library for using the OMOP Common Data Model.

## Installation

```
pip install https://github.com/dermatologist/pyomop/releases/download/0.1.0/pyomop-0.1.0-py2.py3-none-any.whl

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

