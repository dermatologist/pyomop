# pyomop

[![Release](https://img.shields.io/github/v/release/dermatologist/pyomop)](https://img.shields.io/github/v/release/dermatologist/pyomop)
[![Build status](https://img.shields.io/github/actions/workflow/status/dermatologist/pyomop/pytest.yml?branch=develop)](https://github.com/dermatologist/pyomop/actions/workflows/pytest.yml?query=branch%3Adevelop)
[![codecov](https://codecov.io/gh/dermatologist/pyomop/branch/develop/graph/badge.svg)](https://codecov.io/gh/dermatologist/pyomop)
[![Commit activity](https://img.shields.io/github/commit-activity/m/dermatologist/pyomop)](https://img.shields.io/github/commit-activity/m/dermatologist/pyomop)
[![License](https://img.shields.io/github/license/dermatologist/pyomop)](https://img.shields.io/github/license/dermatologist/pyomop)
[![Downloads](https://img.shields.io/pypi/dm/pyomop)](https://pypi.org/project/pyomop)
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
    cdm = CdmEngineFactory() # Creates SQLite database by default for fast testing
    # cdm = CdmEngineFactory(db='pgsql', host='', port=5432,
    #                       user='', pw='',
    #                       name='', schema='public')
    # cdm = CdmEngineFactory(db='mysql', host='', port=3306,
    #                       user='', pw='',
    #                       name='')
    engine = cdm.engine
    # Comment the following line if using an existing database. Both cdm6 and cdm54 are supported, see the import statements above
    await cdm.init_models(Base.metadata) # Initializes the database with the OMOP CDM tables
    vocab = CdmVocabulary(cdm, version='cdm54') # or 'cdm6' for v6
    # Uncomment the following line to create a new vocabulary from CSV files
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


## 🔥 FHIR to OMOP mapping

pyomop can load FHIR Bulk Export (NDJSON) files into an OMOP CDM database.

- Sample datasets: https://github.com/smart-on-fhir/sample-bulk-fhir-datasets
- Remove any non-FHIR files (for example, `log.ndjson`) from the input folder.
- Download OMOP vocabulary CSV files (for example from OHDSI Athena) and place them in a folder.

Run:

```bash
pyomop --create --vocab ~/Downloads/omop-vocab/ --input ~/Downloads/fhir/
```

This will create an OMOP CDM in SQLite, load the vocabulary files, and import the FHIR data from the input folder and reconcile vocabulary, mapping source_value to concept_id. The mapping is defined in the `mapping.example.json` file. The default mapping is [here](src/pyomop/mapping.default.json). Mapping happens in 5 steps as implemented [here](src/pyomop/loader.py).

* FHIR to data frame mapping is done with [FHIRy](https://github.com/dermatologist/fhiry)
* Most of the code for this functionality was written by an LLM agent. The prompts used are [here](notes/prompt.md)

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

⭐️ If you find this project useful!