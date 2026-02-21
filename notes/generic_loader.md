# Generic Loader – Implementation Notes

## Motivation

`loader.py` / `CdmCsvLoader` is tightly coupled to a flat CSV source
(produced by FHIR Bulk Export via *fhiry*).  Many real-world ETL pipelines need
to pull from an existing relational database rather than an intermediate file.
`CdmGenericLoader` fills this gap.

## Design Decisions

### Async-first

The existing code base uses SQLAlchemy's async API throughout
(`create_async_engine`, `AsyncSession`).  The generic loader follows the same
pattern so that it fits naturally into async coroutines and event loops.  A
synchronous shim could be added later if needed (e.g. via `asyncio.run()`).

### Source engine as a parameter

The caller constructs the source engine and passes it in rather than having the
loader accept a URL string.  This gives callers full control over engine
options (pool size, connect args, SSL, etc.) without the loader needing to
know about every driver-specific parameter.

The convenience function `create_source_engine(url)` is provided as a
thin wrapper for discoverability.

### Mapping file structure

The mapping format deliberately mirrors `mapping.default.json` to minimise the
learning curve.  The only structural addition is `source_table`, which names
the source table to read from.  All other keys (`name`, `filters`, `columns`,
`concept`, `force_text_fields`) are identical in meaning.

### Row fetching strategy

All rows from a source table are fetched into memory before inserting into the
target.  This is intentional for simplicity and works well for datasets up to
a few million rows.  For very large tables (tens of millions of rows) a
server-side cursor / streaming approach would be more memory-efficient – see
**Future Work** below.

### Filter implementation

`CdmCsvLoader` applies filters as pandas boolean masks on a DataFrame.
`CdmGenericLoader` translates the same filter dictionaries into SQLAlchemy
`WHERE` clauses, which means filtering happens in the database engine and
avoids transferring unwanted rows over the wire.

### Post-load steps

Steps 2–5 (`fix_person_id`, `backfill_person_birth_fields`,
`update_person_gender_concept_id`, `apply_concept_mappings`) are implemented
independently in `CdmGenericLoader` rather than shared with `CdmCsvLoader`.
This avoids introducing a tight coupling between the two classes (e.g. via
inheritance or a shared mixin) at the cost of some code duplication.  If the
project grows, a `BaseCdmLoader` mixin could be extracted to consolidate these
methods.

### Error handling philosophy

A missing source or target table should not abort an entire ETL run.  The
loader logs a `WARNING` and continues to the next mapping entry.  This matches
the existing loader's behaviour for empty/filtered DataFrames.

## Module Layout

```
src/pyomop/
├── generic_loader.py             # CdmGenericLoader + extract_schema_to_markdown + helpers
├── mapping.generic.example.json  # annotated example mapping file
```

## Testing Strategy

Tests live in `tests/test_generic_loader.py` and use two in-memory (or
`tmp_path`) SQLite databases – one source and one OMOP CDM target – to avoid
external service dependencies.  Each test covers a distinct behaviour:

| Test | What it validates |
|---|---|
| `test_generic_loader_loads_persons` | Basic row insertion |
| `test_generic_loader_gender_concept_id_set` | Gender post-processing |
| `test_generic_loader_birth_fields_backfilled` | Birth-date backfill |
| `test_generic_loader_filter_applied` | SQL WHERE filter |
| `test_generic_loader_missing_source_table_skipped` | Graceful skip on missing table |
| `test_generic_loader_multi_table_mapping` | Multiple source→target table pairs |
| `test_generic_loader_batch_size_respected` | Batched INSERT correctness |
| `test_create_source_engine` | Convenience factory |
| `test_load_mapping` | JSON parsing helper |
| `test_generic_loader_example_mapping_is_valid` | Bundled example file validity |
| `test_migrate_cli_option` | End-to-end CLI migrate |
| `test_migrate_cli_requires_mapping` | Missing mapping error |
| `test_migrate_cli_bad_src_dbtype` | Unsupported source DB type |
| `test_extract_schema_to_markdown_basic` | Schema Markdown file content |
| `test_extract_schema_to_markdown_pk_fk` | PK/FK rendering |
| `test_extract_schema_cli_option` | End-to-end `--extract-schema` CLI |
| `test_extract_schema_cli_bad_dbtype` | Unsupported source DB type |
| `test_build_source_url_sqlite` | URL building for SQLite |
| `test_build_source_url_mysql` | URL building for MySQL |
| `test_build_source_url_pgsql` | URL building for PostgreSQL |
| `test_build_source_url_invalid` | ValueError for unknown dbtype |
| `test_build_source_url_env_vars` | Env var override via `SRC_DB_*` |

## Schema Extraction Design

`extract_schema_to_markdown` runs entirely through `conn.run_sync` so it can
use the synchronous `sqlalchemy.inspect` API (which is more complete for FK
introspection than the async equivalents).  The resulting Markdown is useful
both for human review and for feeding to AI assistants to generate mapping JSON.

### Environment variable security model

Source database credentials can be supplied via environment variables
(`SRC_DB_HOST`, `SRC_DB_PORT`, `SRC_DB_USER`, `SRC_DB_PASSWORD`, `SRC_DB_NAME`)
rather than CLI flags.  This prevents passwords appearing in shell history or
`ps aux` output.  The `build_source_url` helper reads these variables via
`os.environ.get`, falling back to the CLI-supplied defaults.  CLI flags always
take precedence when they are explicitly set to non-default values; however,
since Click defaults are applied before the function is called, both mechanisms
are honoured in order: env var → default.  Users who need strict CLI-override
semantics should unset the env vars.

## Future Work

- **Streaming / server-side cursors**: Use `yield_per` or raw `FETCH` for
  very large tables to avoid loading all rows into memory.
- **Incremental / delta loads**: Add support for watermark columns
  (e.g. `updated_at > ?`) in the filter specification to enable incremental
  ETL.
- **Transformation functions**: Allow mapping values to specify a Python
  callable or a simple expression (e.g. string prefix/suffix, date arithmetic)
  rather than just a direct column copy or constant.
- **Dry-run mode**: Log what would be inserted without writing to the target,
  useful for validating mappings before production runs.
- **Parallel table loading**: Load independent OMOP tables concurrently using
  `asyncio.gather` for throughput on large datasets.
- **Schema-to-mapping AI integration**: Pipe the output of `--extract-schema`
  directly into an LLM prompt to auto-generate a first-pass mapping JSON.
- **Shared base class**: Extract common post-load methods
  (`fix_person_id`, `backfill_person_birth_fields`, etc.) into a
  `BaseCdmLoader` to reduce code duplication between `CdmCsvLoader` and
  `CdmGenericLoader`.
