# Generic Database Loader (`CdmGenericLoader`)

The `CdmGenericLoader` lets you read data from **any** SQLAlchemy-compatible
source database and write it into an OMOP CDM target database, guided by a
JSON mapping file.

## Overview

| Aspect | Detail |
|---|---|
| Source | Any database accessible via an async SQLAlchemy engine (SQLite, PostgreSQL, MySQL, …) |
| Target | Any OMOP CDM database initialised via `CdmEngineFactory` |
| Mapping | JSON file describing which source tables/columns map to which OMOP tables/columns |
| Post-processing | Automatic `person_id` FK normalisation, birth-date backfill, gender concept mapping, concept-code lookups |

## Installation

`CdmGenericLoader` is included in the standard `pyomop` package – no extra
dependencies are required beyond what is already installed.

## Quick Start

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from pyomop import CdmEngineFactory, CdmGenericLoader
from pyomop.cdm54 import Base

async def main():
    # 1. Connect to the source database (read-only is fine)
    source_engine = create_async_engine("sqlite+aiosqlite:///source.sqlite")

    # 2. Create / connect to the target OMOP CDM database
    target_cdm = CdmEngineFactory(db="sqlite", name="omop.sqlite")
    _ = target_cdm.engine                       # initialise engine
    await target_cdm.init_models(Base.metadata) # create CDM tables

    # 3. Run the loader
    loader = CdmGenericLoader(source_engine, target_cdm)
    await loader.load("mapping.json", batch_size=500)

asyncio.run(main())
```

The convenience factory `create_source_engine` is available as an alias for
`sqlalchemy.ext.asyncio.create_async_engine`:

```python
from pyomop.generic_loader import create_source_engine

source_engine = create_source_engine("postgresql+asyncpg://user:pass@host/sourcedb")
```

## Mapping File Format

The mapping file is a JSON document.  The bundled example
[`src/pyomop/mapping.generic.example.json`](https://github.com/dermatologist/pyomop/blob/develop/src/pyomop/mapping.generic.example.json)
demonstrates the full structure.

```json
{
  "tables": [
    {
      "source_table": "patients",
      "name": "person",
      "filters": [
        {"column": "active", "equals": 1}
      ],
      "columns": {
        "person_id":              "id",
        "gender_concept_id":      {"const": 0},
        "gender_source_value":    "gender",
        "birth_datetime":         "date_of_birth",
        "year_of_birth":          {"const": 0},
        "race_concept_id":        {"const": 0},
        "race_source_value":      "race",
        "ethnicity_concept_id":   {"const": 0},
        "ethnicity_source_value": "ethnicity",
        "person_source_value":    "patient_key"
      }
    }
  ],
  "concept": [
    {
      "table": "condition_occurrence",
      "mappings": [
        {
          "source": "condition_source_value",
          "target": "condition_concept_id"
        }
      ]
    }
  ],
  "force_text_fields": ["drug_source_value"]
}
```

### Top-level keys

| Key | Required | Description |
|---|---|---|
| `tables` | ✅ | List of table-mapping objects (see below) |
| `concept` | ☐ | Concept-code lookup definitions (same format as `mapping.default.json`) |
| `force_text_fields` | ☐ | Column names that are always stored as plain text |

### Table-mapping object

| Key | Required | Description |
|---|---|---|
| `source_table` | ✅ | Table name in the **source** database |
| `name` | ✅ | Table name in the **target** OMOP CDM database |
| `filters` | ☐ | List of row-level filter objects (see below) |
| `columns` | ✅ | `{target_col: source_col}` or `{target_col: {"const": value}}` |

### Filter object

| Key | Description |
|---|---|
| `column` | Source column name |
| `equals` | Row is included only when the column equals this value |
| `not_empty` | If `true`, row is included only when the column is non-null and non-empty |

### Column mapping values

| Value | Meaning |
|---|---|
| `"some_column"` | Copy the value from the named source column |
| `{"const": 0}` | Use a constant value (any JSON scalar) |

## Post-load Steps

After inserting all rows the loader automatically performs the same cleanup
steps as `CdmCsvLoader`:

1. **`fix_person_id`** – Resolves any rows where `person_id` contains a string
   source-value (e.g. UUID) and replaces it with the integer PK assigned to
   that person.
2. **`backfill_person_birth_fields`** – Derives `year_of_birth`,
   `month_of_birth`, `day_of_birth` from `birth_datetime` where those fields
   are NULL or 0.
3. **`update_person_gender_concept_id`** – Maps `gender_source_value`
   (`male`/`female`/other) to the standard OMOP concept IDs (8507/8532/0).
4. **`apply_concept_mappings`** – Looks up `concept.concept_id` for each
   source-value code defined in the `"concept"` section of the mapping.

## Command-line Usage (`--migrate`)

`CdmGenericLoader` is also available directly from the `pyomop` CLI via the
`--migrate` flag.  Source-database connection details are provided with
`--src-*` options; the target OMOP CDM database uses the standard `--dbtype`,
`--host`, `--port`, `--user`, `--pw`, `--name`, and `--schema` options.

### Environment variables for source connection

As an alternative to CLI flags, all source-database connection parameters can
be set as environment variables.  This is recommended for production use
because credentials are never part of the shell history:

| Environment variable | Equivalent CLI option |
|---|---|
| `SRC_DB_HOST` | `--src-host` |
| `SRC_DB_PORT` | `--src-port` |
| `SRC_DB_USER` | `--src-user` |
| `SRC_DB_PASSWORD` | `--src-pw` |
| `SRC_DB_NAME` | `--src-name` |

CLI flags take precedence over environment variables.

```bash
export SRC_DB_HOST=db.hospital.org
export SRC_DB_USER=readonly
export SRC_DB_PASSWORD=secret
export SRC_DB_NAME=ehr_db

pyomop --migrate \
  --src-dbtype pgsql \
  --dbtype sqlite --name omop.sqlite \
  --mapping ehr_to_omop.json
```

```
pyomop --migrate [OPTIONS]
```

### Required options

| Option | Description |
|---|---|
| `--mapping FILE` / `-m FILE` | Path to the JSON mapping file |

### Target database options (OMOP CDM)

| Option | Default | Description |
|---|---|---|
| `--dbtype` | `sqlite` | Target DB type: `sqlite`, `mysql`, or `pgsql` |
| `--host` | `localhost` | Target DB host |
| `--port` | `5432` | Target DB port |
| `--user` | `root` | Target DB user |
| `--pw` | `pass` | Target DB password |
| `--name` | `cdm.sqlite` | Target DB name or SQLite file path |
| `--schema` | _(empty)_ | Target DB schema (PostgreSQL) |
| `--version` | `cdm54` | OMOP CDM version (`cdm54` or `cdm6`) |

### Source database options

| Option | Env var | Default | Description |
|---|---|---|---|
| `--src-dbtype` | — | `sqlite` | Source DB type: `sqlite`, `mysql`, or `pgsql` |
| `--src-host` | `SRC_DB_HOST` | `localhost` | Source DB host |
| `--src-port` | `SRC_DB_PORT` | `5432` | Source DB port |
| `--src-user` | `SRC_DB_USER` | `root` | Source DB user |
| `--src-pw` | `SRC_DB_PASSWORD` | `pass` | Source DB password |
| `--src-name` | `SRC_DB_NAME` | `source.sqlite` | Source DB name or SQLite file path |
| `--src-schema` | — | _(empty)_ | Source DB schema (PostgreSQL) |

### Tuning

| Option | Default | Description |
|---|---|---|
| `--batch-size` | `1000` | Rows per INSERT batch |

### Examples

**SQLite → SQLite**

```bash
pyomop --migrate \
  --src-dbtype sqlite --src-name /data/hospital_ehr.sqlite \
  --dbtype sqlite --name /data/omop.sqlite \
  --mapping /etc/pyomop/ehr_to_omop.json
```

**PostgreSQL source → PostgreSQL OMOP target**

```bash
pyomop --migrate \
  --src-dbtype pgsql --src-host srchost --src-port 5432 \
  --src-user readonly --src-pw secret --src-name ehr_db \
  --dbtype pgsql --host omophost --port 5432 \
  --user omop_writer --pw secret --name omop_db --schema cdm \
  --mapping ehr_to_omop.json --batch-size 500
```

**MySQL source → SQLite OMOP target**

```bash
pyomop --migrate \
  --src-dbtype mysql --src-host 192.168.1.10 --src-user reader --src-pw pass --src-name clinic \
  --dbtype sqlite --name omop.sqlite \
  --mapping clinic_to_omop.json
```

## Schema Extraction (`--extract-schema`)

Before writing a mapping file you need to understand the structure of your
source database.  The `--extract-schema` option introspects the source database
and writes a Markdown document containing:

- A summary table listing all tables with row counts and primary keys.
- Per-table column details: name, data type, nullable, default value, and
  PK/FK annotations.
- Foreign key relationship sections.

You can feed this Markdown directly to an AI assistant and ask it to generate
the appropriate mapping JSON.

### Usage

```bash
pyomop --extract-schema \
  --src-dbtype sqlite --src-name hospital.sqlite \
  --schema-output hospital_schema.md
```

The same `SRC_DB_*` environment variables apply here too:

```bash
export SRC_DB_HOST=db.hospital.org
export SRC_DB_USER=readonly
export SRC_DB_PASSWORD=secret
export SRC_DB_NAME=ehr_db

pyomop --extract-schema \
  --src-dbtype pgsql \
  --schema-output ehr_schema.md
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `--src-dbtype` | `sqlite` | Source DB type |
| `--src-host` / `SRC_DB_HOST` | `localhost` | Source DB host |
| `--src-port` / `SRC_DB_PORT` | `5432` | Source DB port |
| `--src-user` / `SRC_DB_USER` | `root` | Source DB user |
| `--src-pw` / `SRC_DB_PASSWORD` | `pass` | Source DB password |
| `--src-name` / `SRC_DB_NAME` | `source.sqlite` | Source DB name |
| `--schema-output` | `schema.md` | Output Markdown file path |

### Python API

```python
import asyncio
from pyomop.generic_loader import create_source_engine, extract_schema_to_markdown

async def run():
    engine = create_source_engine("sqlite+aiosqlite:///source.sqlite")
    path = await extract_schema_to_markdown(engine, "schema.md")
    print(f"Schema written to {path}")

asyncio.run(run())
```

## API Reference

::: generic_loader.CdmGenericLoader

## Supported Databases

| Database | Async driver | Example URL |
|---|---|---|
| SQLite | `aiosqlite` | `sqlite+aiosqlite:///path/to/db.sqlite` |
| PostgreSQL | `asyncpg` | `postgresql+asyncpg://user:pass@host/db` |
| MySQL | `aiomysql` | `mysql+aiomysql://user:pass@host/db` |

Both the source and target can independently use any of the above backends.

## Differences from `CdmCsvLoader`

| Feature | `CdmCsvLoader` | `CdmGenericLoader` |
|---|---|---|
| Source | CSV file (via pandas) | Any SQL database |
| Filters | Pandas boolean masks | SQL WHERE clauses |
| Source identification | `csv_key` field | `source_table` field per mapping entry |
| Batching | Per-table chunked INSERT | Per-table chunked INSERT |
| Post-processing steps | ✅ identical | ✅ identical |

## Error Handling

- Missing source tables are **warned and skipped** rather than raising an
  exception, so a partial mapping can still load valid tables.
- Missing target tables produce a warning and are skipped.
- Individual value-coercion failures fall back to string representation.
- All errors are logged via the standard Python `logging` module (logger name
  `pyomop.generic_loader`).

## Example: PostgreSQL source → SQLite OMOP target

```python
import asyncio
from pyomop import CdmEngineFactory
from pyomop.cdm54 import Base
from pyomop.generic_loader import CdmGenericLoader, create_source_engine

async def main():
    source = create_source_engine(
        "postgresql+asyncpg://readonly_user:secret@db.hospital.org/ehr"
    )
    target = CdmEngineFactory(db="sqlite", name="omop_ehr.sqlite")
    _ = target.engine
    await target.init_models(Base.metadata)

    loader = CdmGenericLoader(source, target)
    await loader.load("ehr_to_omop_mapping.json", batch_size=1000)

asyncio.run(main())
```
