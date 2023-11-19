# pyomop

![Libraries.io SourceRank](https://img.shields.io/librariesio/sourcerank/pypi/pyomop)
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)
[![PyPI download total](https://img.shields.io/pypi/dm/pyomop.svg)](https://pypi.python.org/pypi/pyomop/)
[![Build](https://github.com/dermatologist/pyomop/workflows/Python%20Test/badge.svg)](https://nuchange.ca)

* *Inspired by [@jbadger3's](https://github.com/jbadger3) [inspectomop](https://github.com/jbadger3/inspectomop)*

### [Documentation](https://dermatologist.github.io/pyomop/)

## Description

The [OHSDI](https://www.ohdsi.org/) OMOP Common Data Model allows for the systematic analysis of healthcare observational databases. This is a python library to use the CDM v6 compliant databases using SQLAlchemy as the ORM. **pyomop** also supports converting query results to a pandas dataframe (see below) for use in machine learning pipelines. See some useful [SQL Queries here.](https://github.com/OHDSI/QueryLibrary)

## Installation (stable)

```
pip install pyomop

```

## Installation (current)

* git clone this repository and:
```
pip install -e .
```

## Usage >= 4.0.0 (Async) Example
```
from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata
from sqlalchemy.future import select
import datetime
import asyncio

async def main():
    cdm = CdmEngineFactory()  # Creates SQLite database by default
    # Postgres example (db='mysql' also supported)
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='cdm6')

    engine = cdm.engine
    # Create Tables if required
    await cdm.init_models(metadata)
    # Create vocabulary if required
    vocab = CdmVocabulary(cdm)
    # vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

    # Add a cohort
    async with cdm.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
        await session.commit()

    # Query the cohort
    stmt = select(Cohort).where(Cohort.subject_id == 100)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.subject_id == 100

    # Query the cohort pattern 2
    cohort = await session.get(Cohort, 1)
    print(cohort)
    assert cohort.subject_id == 100

    # Convert result to a pandas dataframe
    vec = CdmVector()
    vec.result = result
    print(vec.df.dtypes)

    result = await vec.sql_df(cdm, 'TEST') # TEST is defined in sqldict.py
    for row in result:
        print(row)

    result = await vec.sql_df(cdm, query='SELECT * from cohort')
    for row in result:
        print(row)


    # Close session
    await session.close()
    await engine.dispose()

# Run the main function
asyncio.run(main())
```

## Usage <=3.2.0

```

from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata
from sqlalchemy.sql import select
import datetime

cdm = CdmEngineFactory()  # Creates SQLite database by default

# Postgres example (db='mysql' also supported)
# cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
#                       user='', pw='',
#                       name='', schema='cdm6')


engine = cdm.engine
# Create Tables if required
metadata.create_all(engine)
# Create vocabulary if required
vocab = CdmVocabulary(cdm)
# vocab.create_vocab('/path/to/csv/files')  # Uncomment to load vocabulary csv files

# Create a Cohort (SQLAlchemy as ORM)
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
print(vec.df.dtypes) # vec.df is a pandas dataframe
# OR
vec.sql_df(cdm, query='SELECT * from cohort')
print(vec.df.dtypes) # vec.df is a pandas dataframe


```

## command-line usage

```
pyomop -help
```

## Other utils

**Want to convert FHIR to pandas data frame? Try [fhiry](https://github.com/dermatologist/fhiry)**

**Use the same functions in [.NET](https://github.com/dermatologist/omopcdm-dot-net) and [Golang](https://github.com/E-Health/gocdm)!**

### Support
* Postgres
* MySQL
* SqLite
* More to follow..

## Give us a star ⭐️
If you find this project useful, give us a star. It helps others discover the project.

## Contributors

* [Bell Eapen](https://nuchange.ca) | [![Twitter Follow](https://img.shields.io/twitter/follow/beapen?style=social)](https://twitter.com/beapen)
* PRs welcome. See CONTRIBUTING.md
