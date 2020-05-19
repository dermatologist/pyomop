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
pip install pyomop
```

### latest
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

## Other useful libraries

* [cui-cdm](https://github.com/E-Health/cui-cdm) - CUI to Concept mapping
* [cui-embed](https://github.com/dermatologist/cui-embed) - CUI embeddings (Word2Vec) for similarity search.
* [Hephaestus](https://github.com/dermatologist/hephaestus) - For ETL
