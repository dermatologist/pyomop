# pyomop

OMOP CDM utils

## Description

The [OHSDI](https://www.ohdsi.org/) OMOP Common Data Model allows for the systematic analysis of healthcare observational databases. This is a python library to use the CDM v6 compliant databases.

### Support
* Postgres
* MySQL
* SqLite
* More to follow..

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

cdm = CdmEngineFactory()  # Creates SQLite database by default

engine = cdm.engine
# Create Tables 
metadata.create_all(engine)
# Create vocabulary
vocab = CdmVocabulary(cdm)
vocab.create_vocab('/path/to/csv/files')

# SQLAlchemy as ORM
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

## command-line usage

```
pyomop -help
```

## What to expect

* Integration with machine learning libraries

## Contributors

* [Bell Eapen](https://nuchange.ca)