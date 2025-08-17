# pyomop: OMOP Swiss Army Knife 🔧

[![Release](https://img.shields.io/github/v/release/dermatologist/pyomop)](https://img.shields.io/github/v/release/dermatologist/pyomop)
[![Build status](https://img.shields.io/github/actions/workflow/status/dermatologist/pyomop/pytest.yml?branch=develop)](https://github.com/dermatologist/pyomop/actions/workflows/pytest.yml?query=branch%3Adevelop)
[![codecov](https://codecov.io/gh/dermatologist/pyomop/branch/develop/graph/badge.svg)](https://codecov.io/gh/dermatologist/pyomop)
[![Commit activity](https://img.shields.io/github/commit-activity/m/dermatologist/pyomop)](https://img.shields.io/github/commit-activity/m/dermatologist/pyomop)
[![License](https://img.shields.io/github/license/dermatologist/pyomop)](https://img.shields.io/github/license/dermatologist/pyomop)
[![Downloads](https://img.shields.io/pypi/dm/pyomop)](https://pypi.org/project/pyomop)
[![Documentation](https://badgen.net/badge/icon/documentation?icon=libraries&label)](https://dermatologist.github.io/pyomop/)

## ✨ Overview

**pyomop** is your OMOP Swiss Army Knife 🔧 for working with [OHDSI](https://www.ohdsi.org/) OMOP Common Data Model (CDM) v5.4 or v6 compliant databases using SQLAlchemy as the ORM. It supports converting query results to pandas DataFrames for machine learning pipelines and provides utilities for working with OMOP vocabularies. Table definitions are based on the [omop-cdm](https://github.com/thehyve/omop-cdm) library. Pyomop is designed to be a lightweight, easy-to-use library for researchers and developers experimenting and testing with OMOP CDM databases. It can be used both as a commandline tool and as an imported library in your code.

- Supports SQLite, PostgreSQL, and MySQL. CDM and Vocab tables are created in the same schema. (See usage below for more details)
- LLM-based natural language queries via llama-index. [Usage](examples/llm_example.py).
- 🔥 FHIR to OMOP conversion utilities. (See usage below for more details)
- Execute [QueryLibrary](https://github.com/OHDSI/QueryLibrary). (See usage below for more details)

Please ⭐️ If you find this project useful!

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

## Docker

* A [docker-compose](/docker-compose.yml) is provided to quickly set up an environment with postgrs, [webapi](https://github.com/OHDSI/WebAPI), [atlas](https://github.com/OHDSI/atlas) and a [sql script](/examples/webapi_source.sql) to create a source in webapi. The script can be run using the `psql` command line tool or via the webapi UI. Please refresh after running the script by sending a request to /WebAPI/source/refresh.

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

    # Add Persons
    async with cdm.session() as session:  # type: ignore
        async with session.begin():
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
            session.add(
                Person(
                    person_id=101,
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

    # Query the Person
    stmt = select(Person).where(Person.person_id == 100)
    result = await session.execute(stmt)
    for row in result.scalars():
        print(row)
        assert row.person_id == 100

    # Query the person pattern 2
    person = await session.get(Person, 100)
    print(person)
    assert person.person_id == 100  # type: ignore

    # Convert result to a pandas dataframe
    vec = CdmVector()

    # https://github.com/OHDSI/QueryLibrary/blob/master/inst/shinyApps/QueryLibrary/queries/person/PE02.md
    result = await vec.query_library(cdm, resource='person', query_name='PE02')
    df = vec.result_to_df(result)
    print("DataFrame from result:")
    print(df.head())

    result = await vec.execute(cdm, query='SELECT * from person;')
    print("Executing custom query:")
    df = vec.result_to_df(result)
    print("DataFrame from result:")
    print(df.head())

    # access sqlalchemy result directly
    for row in result:
        print(row)

    # Close session
    await session.close()
    await engine.dispose() # type: ignore

# Run the main function
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

* Example using postgres (Docker)
```bash
pyomop --dbtype pgsql --host localhost --user postgres --pw mypass  --create --vocab ~/Downloads/omop-vocab/ --input ~/Downloads/fhir/
```

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


