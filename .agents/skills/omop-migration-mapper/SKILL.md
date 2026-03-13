---
name: omop-migration-mapper
description: Generates a mapping schema to transform any EMR/healthcare database to OHDSI OMOP CDM format. Use when the user needs to map source healthcare data tables to OMOP using pyomop-migrate, extract a source database schema, define column mappings, resolve concept lookups, validate data quality, and produce a migration-ready JSON mapping file. Don't use for general OMOP queries or post-migration analytics.
---

## Skill purpose

Automate the creation of a production-ready `mapping.json` file for migrating healthcare data from any source EMR/database to OHDSI OMOP Common Data Model (CDM v5.4 or v6) using the `pyomop-migrate` tool.

## Workflow overview

The skill executes 9 sequential steps: **pyomop install** → **schema extraction** → **mapping skeleton** → **column mapping** → **concept mapping** → **special-case handling** → **staged validation** → **iteration & refinement** → **handoff**.

## Step 0: Install pyomop

**Input:** User confirms they have Python 3.11+ and pip installed.
**Action:** Run `pip install pyomop` to install the latest version of the pyomop package, which includes the `pyomop-migrate` CLI tool.

```bash
pip install pyomop
```

## Step 1: Extract source database schema

**Input:** Source database connection details (type, host, port, user, password, name) provided by user or a pre-extracted schema markdown file.

**Action:** Run `pyomop-migrate --extract-schema` to produce a source schema markdown describing all tables, columns, types, and FK relationships.

```bash
pyomop-migrate --extract-schema \
  --src-dbtype <sqlite|mysql|pgsql> \
  --src-name <db_name> \
  --src-host <host> \
  --src-port <port> \
  --src-user <user> \
  --src-pw <password> \
  --schema-output schema.md
```

**Output:** `schema.md` — human-readable Markdown document describing the source schema.

**Decision tree:**
- **If user provides connection details:** Run the extraction command and inspect the output.
- **If user provides a schema markdown file:** Skip extraction; read and analyze the provided file directly.
- **If neither is available:** Ask the user for one.

---

## Step 2: Create mapping skeleton

**Input:** `schema.md` from Step 1 and an example mapping template. Read `assets/mapping.template.json` for the JSON structure.

**Action:** Create a new `mapping.json` file with top-level keys:
- `tables` (required): list of table-mapping objects.
- `concept` (optional): concept lookup definitions.
- `force_text_fields` (optional): field names to preserve as text.

For each **source table** identified in `schema.md`:
1. Create one entry in the `tables` list.
2. Set `source_table` = source table name.
3. Set `name` = target OMOP table name (e.g., `person`, `condition_occurrence`, `drug_exposure`).
4. Stub out a `columns` map (will be filled in Step 3).
5. Add optional `filters` if rows should be pre-filtered (e.g., exclude deleted records).

**Output:** A partial `mapping.json` skeleton with table entries and empty column maps.

**Guidance:** See `references/schema-guides.md` for patterns on identifying which source tables map to which OMOP tables (e.g., patient demographics → `person`, diagnoses → `condition_occurrence`).

---

## Step 3: Map columns

**Input:** `mapping.json` skeleton from Step 2 and source schema `schema.md`.

**Action:** For each source table → OMOP table pair, populate the `columns` map:

```json
{
  "source_table": "patient_demographics",
  "name": "person",
  "columns": {
    "person_id": "patient_id",
    "year_of_birth": "birth_year",
    "gender_concept_id": {"const": 8532},
    "race_concept_id": ""
  }
}
```

**Column mapping rules:**
- **Simple copy:** Map as a string (`"target": "source_col"`).
- **Constant:** Use `{"const": <value>}` for hardcoded values (e.g., CDM version, source type).
- **Null/empty:** Use empty string `""` to produce empty values (for nullable columns).
- **Multi-valued:** For comma-separated source values, map the field; the loader will use the first element for concept lookups.

**Special handling:**
- **Person ID:** If source uses non-numeric person identifiers (e.g., MRN strings), map to `person_source_value` and the loader will resolve numeric `person_id` via `fix_person_id` post-step.
- **Dates/times:** Map as-is; the loader will coerce to `Date` or `DateTime` based on target column type.
- **Numerics:** Map as-is; the loader will parse to `Numeric` or `Integer` based on target.
- **Text fields with complex types (JSON, arrays, lists):** Add the target column name to `force_text_fields` to preserve as text (JSON or comma-joined strings).

**Output:** Complete `mapping.json` with all `columns` entries populated.

---

## Step 4: Define concept mappings

**Input:** `mapping.json` from Step 3 and the OMOP `concept` vocabulary (available via `pyomop --vocab <dir>` or pre-loaded).

**Action:** Identify source columns that reference standardized vocabularies (ICD-10, SNOMED, RxNorm, etc.) and add a `concept` section:

```json
{
  "concept": [
    {
      "table": "condition_occurrence",
      "mappings": [
        {
          "source": "diagnosis_code",
          "target": "condition_concept_id"
        }
      ]
    }
  ]
}
```

The loader will:
1. Use `source.diagnosis_code` to look up `concept.concept_id` from `concept.concept_code` (or `source_to_concept_map`).
2. Populate the target `condition_concept_id` with the matched concept.
3. Also populate `condition_source_concept_id` if available.

**Fallback:** If a source code is not found in the vocabulary, record it in a post-migration report and optionally leave `*_concept_id` as 0 (requires post-processing).

**Guidance:** See `references/decisions.md` for common vocabulary mappings by EMR type (Epic, Cerner, etc.).

---

## Step 5: Handle special cases

**Input:** `mapping.json` from Step 4 and OMOP CDM schema. Read `src/pyomop/cdm6/cdm6_tables.py` for authoritative target column names and types.

**Action:** Review and adjust mappings for common special cases:

1. **Foreign keys (visit, provider, care_site):**
   - If a source table references a visit/provider/care_site, ensure the mapping includes the FK column (e.g., `visit_occurrence_id`).
   - If the source does not have these, use `{"const": null}` or leave blank.

2. **Person birth components (year, month, day):**
   - Source may have a full `date_of_birth`; split into `year_of_birth`, `month_of_birth`, `day_of_birth` during migration or pre-process.
   - Ensure `month_of_birth` and `day_of_birth` are nullable if granularity varies.

3. **Gender/race/ethnicity concept IDs:**
   - Source may store as codes (e.g., 'M', 'F'); map to OMOP standard concept IDs (8532 = Male, 8507 = Female).
   - Use the `concept` section to resolve source codes to concept IDs if vocabulary is available.

4. **Visit types (inpatient, outpatient, ED):**
   - Map source visit types to OMOP visit concept IDs.
   - Typical mappings: office visit → 9202, inpatient → 9201, ED → 9203.

5. **Multiple observation/measurement types in a single table:**
   - If a source table contains heterogeneous measurements (labs, vitals, custom fields), consider creating multiple target table entries with filters.

6. **Multi-valued coalesce:**
   - If multiple source columns can populate a single OMOP field, use a pre-migration view or script to coalesce them into a single source column, then map normally.

**Output:** Updated `mapping.json` with special cases resolved.

---

## Step 6: Validate with staged migration

**Input:** `mapping.json` from Step 5, source DB connection, and target OMOP database (SQLite or test database).

**Action:** Run a small-scale test load using `pyomop-migrate --migrate`:

```bash
pyomop-migrate --migrate \
  --src-dbtype <type> --src-name <db> --src-host <host> --src-user <user> --src-pw <password> \
  --mapping mapping.json \
  --dbtype sqlite \
  --name test_omop.db \
  --batch-size 100
```

**Checks to perform** (use `scripts/validate_mapping.py` or manual SQL queries):

1. **Row counts:** Compare source table row counts with imports in target OMOP tables. Expected ratio depends on filters and 1:N mappings (e.g., one patient → many measurements).
2. **Null checks:** Verify required (non-nullable) OMOP columns are populated; flag null counts.
3. **Concept coverage:** Query `condition_occurrence.condition_concept_id` for concept_id = 0 (unmapped codes); list them.
4. **Sample joins:** Spot-check 5–10 patient records end-to-end (demographic → condition → measurement) to ensure referential integrity.
5. **Data type coercion:** Confirm dates parse correctly, numbers are numeric, and strings are reasonable lengths.

**Decision tree:**
- **If validation passes:** Proceed to Step 7.
- **If row counts don't match expectations:** Review filters and mapping; re-run migration with `-v` (verbose) flag to debug.
- **If concept mapping is incomplete:** Update vocabulary or add fallback concept mappings; re-run Step 4.
- **If referential integrity fails:** Check FK columns (person_id, visit_occurrence_id) and adjust filters or mapping; re-run Step 6.

**Output:** Validation report (pass/fail/warnings).

---

## Step 7: Iterate and refine

**Input:** Validation results from Step 6 and updated `mapping.json` as needed.

**Action:**
1. If validation revealed issues, update `mapping.json` (modify filters, add concept mappings, adjust column mappings).
2. Re-run `pyomop-migrate --migrate` with the updated file.
3. Re-run validation checks in Step 6.
4. Repeat until validation passes.

**Record decisions:** As you iterate, update `references/decisions.md` with:
- Which source columns map to which OMOP tables (e.g., "patient.diagnosis_code → condition_occurrence.condition_source_value").
- Vocabulary mappings applied (e.g., "ICD-10 codes resolved via OMOP Athena").
- Filters used (e.g., "Excluded deleted records: `WHERE is_deleted = 0`").
- Known issues or caveats (e.g., "Lab unit codes incomplete; will require post-processing").

---

## Step 8: Handoff

**Input:** Final validated `mapping.json` and iteration notes from Step 7.

**Action:**
1. Rename `mapping.json` to `mapping.<site>.json` (e.g., `mapping.epic.json`, `mapping.cerner.json`).
2. Generate a `migration-report.md` summary:
   - Source database schema overview (table + column count).
   - OMOP tables targeted and row counts.
   - Concept mappings applied.
   - Known unmapped source codes (from Step 6 concept coverage check).
   - Filters applied (e.g., "Active patients only").
   - Caveats and post-migration steps (e.g., "Unit conversions needed for lab results").
   - Record location: `references/migration-report.md`.
3. Archive schema and mapping files in `references/` and `assets/`.

**Final deliverables:**
- `mapping.<site>.json` — Ready for production migration.
- `migration-report.md` — Human-readable summary of decisions and known issues.
- `schema.md` — Source schema snapshot for reference.

---

## Error handling

**No source database credentials available:**
- Ask the user to provide a pre-extracted `schema.md` file, or provide connection credentials.

**Source table not found:**
- The extraction step will warn or skip missing tables. Review the source schema and correct the table name in `mapping.json`.

**Concept codes not in vocabulary:**
- Record unmapped codes in the validation report. Suggest user download vocabulary updates from OHDSI Athena or run `pyomop --vocab <dir>`.

**Referential integrity failure (FK not found):**
- Check that person_id, visit_occurrence_id, provider_id are correctly mapped and populated. Adjust filters to exclude orphaned records, or add pre-processing steps.

**Data type coercion error (date parse fails):**
- Verify source column is in ISO 8601 format (YYYY-MM-DD). If not, pre-process with a SQL view or transformation script.

**Validation fails unexpectedly:**
- Run `pyomop-migrate --migrate` with `--verbose` or `--debug` flag for detailed logs. Check target database for partial inserts or constraint violations.

---

## References

- **Source extraction guide:** See `references/schema-guides.md`.
- **Mapping template:** See `assets/mapping.template.json`.
- **Validation script:** Run `scripts/validate_mapping.py`.
- **Recurrent patterns:** See `references/decisions.md` for EMR-specific mapping tips (Epic, Cerner, etc.).
- **External resources:**
  - [OMOP CDM v6 schema documentation](https://ohdsi.github.io/CommonDataModel/)
  - [pyomop-migrate documentation](docs/pyomop_migrate.md)
  - [OMOP Concept Codes & Vocabulary](https://athena.ohdsi.org/)
