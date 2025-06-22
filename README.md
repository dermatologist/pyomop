# pyomop

![Libraries.io SourceRank](https://img.shields.io/librariesio/sourcerank/pypi/pyomop)
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)
[![PyPI download total](https://img.shields.io/pypi/dm/pyomop.svg)](https://pypi.python.org/pypi/pyomop/)
[![Build](https://github.com/dermatologist/pyomop/workflows/Python%20Test/badge.svg)](https://nuchange.ca)
[![Documentation](https://badgen.net/badge/icon/documentation?icon=libraries&label)](https://dermatologist.github.io/pyomop/)

## ✨ Overview

**pyomop** is a Python library for working with [OHDSI](https://www.ohdsi.org/) OMOP Common Data Model (CDM) v5.4 or v6 compliant databases using SQLAlchemy as the ORM. It supports converting query results to pandas DataFrames for machine learning pipelines and provides utilities for working with OMOP vocabularies. Table definitions are based on the [omop-cdm](https://github.com/thehyve/omop-cdm) library. Pyomop is designed to be a lightweight, easy-to-use library for researchers and developers experimenting and testing with OMOP CDM databases.

- Supports SQLite, PostgreSQL, and MySQL. (All tables are in the default schema) (See usage below for more details)
- LLM-based natural language queries via llama-index. [Usage](examples/llm_example.py).
- Execute [QueryLibrary](https://github.com/OHDSI/QueryLibrary). (See usage below for more details)

## Installation

**Stable release:**
```
pip install pyomop
```

**Development version:**
```
git clone https://github.com/dermatologist/pyomop.git
cd pyomop
pip install -e .
```

**LLM support:**
```
pip install pyomop[llm]
```
See [llm_example.py](examples/llm_example.py) for usage.

## 🔧 Usage


```python
from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector
# cdm6 and cdm54 are supported
from pyomop.cdm54 import Person, Cohort, Vocabulary, Base
from sqlalchemy.future import select
import datetime
import asyncio

async def main():
    cdm = CdmEngineFactory()
    engine = cdm.engine
    await cdm.init_models(Base.metadata)
    vocab = CdmVocabulary(cdm, version='cdm54') # or 'cdm6' for v6
    # vocab.create_vocab('/path/to/csv/files')

    async with cdm.session() as session:
        async with session.begin():
            session.add(Cohort(cohort_definition_id=2, subject_id=100,
                cohort_end_date=datetime.datetime.now(),
                cohort_start_date=datetime.datetime.now()))
            session.add(
                Person(
                    person_id=100,
                    gender_concept_id=8532,
                    gender_source_concept_id=8512,
                    year_of_birth=1980,
                    month_of_birth=1,
                    day_of_birth=1,
                    birth_datetime=datetime.datetime(1980, 1, 1),
                    race_concept_id=8552,
                    race_source_concept_id=8552,
                    ethnicity_concept_id=38003564,
                    ethnicity_source_concept_id=38003564,
                )
            )
        await session.commit()

        stmt = select(Cohort).where(Cohort.subject_id == 100)
        result = await session.execute(stmt)
        for row in result.scalars():
            print(row)

        cohort = await session.get(Cohort, 1)
        print(cohort)

        vec = CdmVector()

        # supports QueryLibrary queries
        # https://github.com/OHDSI/QueryLibrary/blob/master/inst/shinyApps/QueryLibrary/queries/person/PE02.md
        result = await vec.query_library(cdm, resource='person', query_name='PE02')
        df = vec.result_to_df(result)
        print("DataFrame from result:")
        print(df.head())

        result = await vec.execute(cdm, query='SELECT * from cohort;')
        print("Executing custom query:")
        df = vec.result_to_df(result)
        print("DataFrame from result:")
        print(df.head())

        # access sqlalchemy result directly
        for row in result:
            print(row)


    await session.close()
    await engine.dispose()

asyncio.run(main())
```


### Command-line

```
pyomop -help
```

## Additional Tools

- **Convert FHIR to pandas DataFrame:** [fhiry](https://github.com/dermatologist/fhiry)
- **.NET and Golang OMOP CDM:** [.NET](https://github.com/dermatologist/omopcdm-dot-net), [Golang](https://github.com/E-Health/gocdm)

## Supported Databases

- PostgreSQL
- MySQL
- SQLite

## Contributing

Pull requests are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributors

- [Bell Eapen](https://nuchange.ca) [![Twitter Follow](https://img.shields.io/twitter/follow/beapen?style=social)](https://twitter.com/beapen)

---

⭐️ If you find this project useful, please give us