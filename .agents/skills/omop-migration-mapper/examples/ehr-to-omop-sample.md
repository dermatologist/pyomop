# Example: Mapping a Sample EHR to OMOP CDM

This example demonstrates the full workflow of mapping a hypothetical hospital's EHR database to OHDSI OMOP CDM using the `omop-migration-mapper` skill.

## Scenario

A small regional hospital (Acme Health) runs an internally-developed EHR system with the following schema:
- `patient` — demographic data.
- `diagnosis` — active problem list.
- `medication` — active and historical medications.
- `lab_result` — laboratory test results.
- `visit` — inpatient and outpatient encounters.
- `provider` — clinician data.

**Goal:** Migrate 10 years of historical patient data to OMOP CDM for disease surveillance and population health research.

---

## Step 1: Invoke the skill

**User prompt:**
```
I need to map our Acme Health EHR to OMOP CDM v6.
Our source database has patient, diagnosis, medication, lab_result, visit, and provider tables.
Here are the connection details: SQLite, file=/var/datastore/acme_ehr.db
Please extract the schema, generate a mapping.json, validate with a test load, and produce a final migration report.
```

**Agent response:** The skill triggers. Agent asks for (or retrieves from context):
- Source DB type: `sqlite`
- Source DB path: `/var/datastore/acme_ehr.db`
- OMOP version: `v6` (default)
- Validation sample size: `1000 patients` (default; agent proposes and user confirms)

---

## Step 2: Schema Extraction

**Command:**
```bash
pyomop-migrate --extract-schema \
  --src-dbtype sqlite \
  --src-name /var/datastore/acme_ehr.db \
  --schema-output schema.md
```

**Output (schema.md):**
```markdown
# Source Database Schema

## Tables and Columns

### patient (450,000 rows)
- patient_id (INTEGER, PK)
- mrn (TEXT, unique) — medical record number
- first_name (TEXT)
- last_name (TEXT)
- date_of_birth (DATE)
- gender (TEXT) — 'M', 'F', 'U'
- race (TEXT) — 'White', 'Black', 'Asian', 'Hispanic', 'Other', 'Unknown'
- is_active (INTEGER) — 1 = active, 0 = inactive
- created_at (TIMESTAMP)

### diagnosis (2,100,000 rows)
- diagnosis_id (INTEGER, PK)
- patient_id (INTEGER, FK → patient)
- icd_code (TEXT) — ICD-10 code
- onset_date (DATE)
- resolution_date (DATE, nullable)
- status (TEXT) — 'active', 'resolved', 'suspected', 'ruled_out'
- created_at (TIMESTAMP)

### medication (1,800,000 rows)
- medication_id (INTEGER, PK)
- patient_id (INTEGER, FK → patient)
- rx_code (TEXT) — NDC code
- drug_name (TEXT)
- start_date (DATE)
- end_date (DATE, nullable)
- quantity (NUMERIC, nullable)
- days_supply (INTEGER, nullable)
- frequency (TEXT) — e.g., 'BID', 'TID', 'QID'
- route (TEXT) — 'PO', 'IV', 'IM', etc.
- is_active (INTEGER)

### lab_result (5,200,000 rows)
- lab_result_id (INTEGER, PK)
- patient_id (INTEGER, FK → patient)
- test_code (TEXT) — LOINC or local lab code
- test_name (TEXT)
- result_value (TEXT) — numeric or text
- unit (TEXT)
- reference_range (TEXT, nullable)
- test_date (TIMESTAMP)
- status (TEXT) — 'FINAL', 'PRELIMINARY', 'CANCELED'

### visit (320,000 rows)
- visit_id (INTEGER, PK)
- patient_id (INTEGER, FK → patient)
- visit_type (TEXT) — 'IP' (inpatient), 'OP' (outpatient), 'ED' (emergency)
- admit_date (TIMESTAMP)
- discharge_date (TIMESTAMP, nullable)
- provider_id (INTEGER, FK → provider)
- facility_name (TEXT)

### provider (800 rows)
- provider_id (INTEGER, PK)
- npi (TEXT, unique) — National Provider Identifier
- first_name (TEXT)
- last_name (TEXT)
- specialty (TEXT) — e.g., 'Internal Medicine', 'Cardiology'

## Foreign Keys
- diagnosis.patient_id → patient.patient_id
- medication.patient_id → patient.patient_id
- lab_result.patient_id → patient.patient_id
- visit.patient_id → patient.patient_id
- visit.provider_id → provider.provider_id
```

**Agent review:** Schema extracted successfully. Agent notes:
- 450K patients, 10M+ clinical events (diagnoses, meds, labs).
- Gender and race are text codes; will need concept lookup.
- ICD-10 and NDC codes are source values; will require OMOP vocabulary mapping.
- Dates are consistently ISO 8601 format.

---

## Step 3: Create Mapping Skeleton

**Agent creates initial mapping.json** using `assets/mapping.template.json`:

```json
{
  "tables": [
    {
      "source_table": "patient",
      "name": "person",
      "filters": [
        {
          "column": "is_active",
          "equals": 1
        }
      ],
      "columns": {
        "person_id": "",
        "person_source_value": "mrn",
        "gender_concept_id": "",
        "gender_source_value": "gender",
        "year_of_birth": "",
        "birth_datetime": "date_of_birth"
      }
    },
    {
      "source_table": "diagnosis",
      "name": "condition_occurrence",
      "filters": [],
      "columns": {
        "person_id": "patient_id",
        "condition_source_value": "icd_code",
        "condition_concept_id": 0,
        "condition_start_date": "onset_date",
        "condition_end_date": "resolution_date",
        "condition_type_concept_id": {"const": 32817}
      }
    },
    {
      "source_table": "medication",
      "name": "drug_exposure",
      "filters": [
        {
          "column": "is_active",
          "equals": 1
        }
      ],
      "columns": {
        "person_id": "patient_id",
        "drug_source_value": "rx_code",
        "drug_concept_id": 0,
        "drug_exposure_start_date": "start_date",
        "drug_exposure_end_date": "end_date",
        "drug_type_concept_id": {"const": 38000177},
        "quantity": "quantity",
        "days_supply": "days_supply"
      }
    },
    {
      "source_table": "lab_result",
      "name": "measurement",
      "filters": [
        {
          "column": "status",
          "equals": "FINAL"
        }
      ],
      "columns": {
        "person_id": "patient_id",
        "measurement_source_value": "test_code",
        "measurement_concept_id": 0,
        "measurement_date": "test_date",
        "value_as_number": "result_value",
        "unit_source_value": "unit"
      }
    },
    {
      "source_table": "visit",
      "name": "visit_occurrence",
      "columns": {
        "person_id": "patient_id",
        "visit_concept_id": "",
        "visit_source_value": "visit_type",
        "visit_start_datetime": "admit_date",
        "visit_end_datetime": "discharge_date",
        "visit_type_concept_id": {"const": 32817},
        "provider_id": "provider_id"
      }
    }
  ],
  "concept": [
    {
      "table": "person",
      "mappings": [
        {
          "source": "gender",
          "target": "gender_concept_id"
        }
      ]
    },
    {
      "table": "condition_occurrence",
      "mappings": [
        {
          "source": "icd_code",
          "target": "condition_concept_id"
        }
      ]
    },
    {
      "table": "drug_exposure",
      "mappings": [
        {
          "source": "rx_code",
          "target": "drug_concept_id"
        }
      ]
    },
    {
      "table": "measurement",
      "mappings": [
        {
          "source": "test_code",
          "target": "measurement_concept_id"
        }
      ]
    },
    {
      "table": "visit_occurrence",
      "mappings": [
        {
          "source": "visit_type",
          "target": "visit_concept_id"
        }
      ]
    }
  ],
  "force_text_fields": ["reference_range"]
}
```

**Agent notes:** Skeleton created. Still need to:
- Resolve gender/race/visit_type codes to concepts.
- Ensure person_id resolution (using person_source_value + fix_person_id post-step).
- Populate year_of_birth from date_of_birth.

---

## Step 4: Column & Concept Mapping

**Agent refines column mappings:**

1. **Gender mapping:** Agent queries OMOP vocabulary:
   - 'M' → 8532 (Male)
   - 'F' → 8507 (Female)
   - 'U' → 8551 (Unknown)
   - Creates a lookup in `source_to_concept_map` or uses the `concept` section post-processing.

2. **Race mapping:**
   - 'White' → 8515
   - 'Black' → 8516
   - 'Asian' → 8557
   - 'Hispanic' → 38003563
   - 'Unknown' → 8657

3. **Visit type mapping:**
   - 'IP' → 9201 (Inpatient)
   - 'OP' → 9202 (Outpatient)
   - 'ED' → 9203 (Emergency)

4. **Person ID:** Agent decides to use non-numeric MRN as `person_source_value` and rely on the loader's `fix_person_id` step to generate numeric `person_id`.

5. **Year of birth:** Agent adds a pre-processing note: extract YEAR(date_of_birth) from the source.

**Updated mapping.json (partial):**
```json
{
  "tables": [
    {
      "source_table": "patient",
      "name": "person",
      "filters": [{"column": "is_active", "equals": 1}],
      "columns": {
        "person_id": "",
        "person_source_value": "mrn",
        "gender_concept_id": 0,
        "gender_source_value": "gender",
        "year_of_birth": "date_of_birth",
        "birth_datetime": "date_of_birth",
        "race_concept_id": 0,
        "race_source_value": "race"
      }
    }
  ],
  "concept": [
    {
      "table": "person",
      "mappings": [
        {"source": "gender", "target": "gender_concept_id"},
        {"source": "race", "target": "race_concept_id"}
      ]
    }
  ]
}
```

---

## Step 5-6: Validation & Iteration

**Agent runs a test migration** (first 1,000 patients):

```bash
pyomop-migrate --migrate \
  --src-dbtype sqlite \
  --src-name /var/datastore/acme_ehr.db \
  --mapping mapping.json \
  --dbtype sqlite \
  --name test_omop.db \
  --batch-size 100 \
  --verbose
```

**Validation checks** (using `scripts/validate_mapping.py`):

```bash
python validate_mapping.py \
  --target-db test_omop.db \
  --mapping mapping.json
```

**Output:**
```
=== OMOP Migration Validation Report ===

### Row Counts ###
[OK] person: 1000 rows
[OK] condition_occurrence: 4200 rows
[OK] drug_exposure: 3600 rows
[OK] measurement: 5400 rows
[OK] visit_occurrence: 2000 rows

### Null Checks ###
[OK] person.person_id: 0 nulls
[WARN] person.year_of_birth: 5 nulls (invalid dates in source)
[OK] condition_occurrence.condition_concept_id: 180 concept_id = 0 (unmapped ICD codes)

### Concept Coverage ###
[WARN] condition_occurrence: 180 unmapped (concept_id = 0)
[WARN] drug_exposure: 45 unmapped
[WARN] measurement: 120 unmapped

### Data Quality Sample ###
[OK] Sampled 5 persons (valid birth years)
[OK] Sampled 5 conditions (FK valid)
```

**Issues identified:**
1. 5 persons with invalid dates → agent adjusts filter to exclude NULL birth_datetime.
2. ~180 ICD-10 codes not in OMOP vocabulary → agent notes these in the final report.
3. ~45 NDC codes unmapped → similar note.

**Agent iterates:** Updates mapping to filter out NULL dates, re-runs validation. Second pass shows 0 nulls and reduced unmapped count (unknown codes are typical).

---

## Step 7: Final report and handoff

**Agent produces final deliverables:**

### mapping.epic.json (final)
Validated mapping file ready for production use.

### migration-report.md
```markdown
# Acme Health EHR → OMOP CDM v6 Migration Report

## Overview
- **Source:** Acme Health EHR (SQLite)
- **Target:** OMOP CDM v6
- **Patient Population:** 450K patients, active records only
- **Date Range:** 2014–2024 (10 years)
- **Migration Date:** 2024-03-11

## Data Summary
| OMOP Table | Row Count | Notes |
|------------|-----------|-------|
| person | 420,000 | Active patients after filtering |
| condition_occurrence | 1,890,000 | Diagnoses with valid dates |
| drug_exposure | 1,700,000 | Active medications only |
| measurement | 4,900,000 | Final lab results (status = FINAL) |
| visit_occurrence | 315,000 | Inpatient + outpatient visits |

## Vocabulary Mappings Applied
- **Gender:** 'M' → OMOP 8532, 'F' → 8507, 'U' → 8551
- **Race:** White/Black/Asian/Hispanic/Unknown → OMOP standard concepts
- **Visit type:** IP → 9201, OP → 9202, ED → 9203
- **ICD-10 codes:** ~99.9% coverage; 180 unmapped codes listed below
- **NDC codes:** ~99.7% coverage; 45 unmapped codes listed below
- **LOINC codes:** ~99.8% coverage; 120 unmapped codes listed below

## Known Limitations
1. **Birth year precision:** 5 records with missing birth dates set to NULL (nullability allowed in OMOP).
2. **Unmapped codes:** 180 ICD-10, 45 NDC, 120 LOINC codes not in standard OMOP vocabulary.
   - Recommendation: Use OHDSI Athena to add custom mappings or create `source_to_concept_map` entries.
3. **Person ID:** Using MRN as `person_source_value`; numeric `person_id` assigned by loader's `fix_person_id` post-step.
4. **Units:** Lab units not fully standardized (e.g., 'mg/dL' vs 'mg/dl'); recommend post-processing to standardize.

## Post-Migration Validation Steps
1. Run `pytest tests/test_pyomop_migrate.py` to verify mapping logic.
2. Spot-check 50 patient records for referential integrity (person FK, visit FK).
3. Query unmapped codes and decide: ignore (for rare codes), create custom concept, or ask domain expert.

## Next Steps
1. Run full production migration: `pyomop-migrate --migrate --mapping mapping.epic.json ...` (no batch-size limit).
2. Generate derived tables (eras, cohorts) as needed for analytics.
3. Share OMOP database with research team; document per OHDSI governance.

## Files
- `mapping.epic.json` — This mapping file (ready for production).
- `schema.md` — Extracted source schema.
- `references/decisions.md` — Mapping rationale and patterns.
```

---

## Conclusion

The agent has successfully:
1. ✅ Extracted source schema.
2. ✅ Generated and refined mapping.json.
3. ✅ Validated with test load (1,000 patients).
4. ✅ Produced final mapping and migration report.

The Acme Health EHR is now ready for full OMOP migration using the generated `mapping.epic.json`.

---

## For users: next steps

To **invoke this skill** in your agent:

```
"Map our EHR database to OMOP CDM v6.
Source: [database type and connection],
Schema: [paste schema markdown or provide connection details].
Please extract schema, generate mapping.json, validate with test load, and provide final migration report."
```

The agent will follow the 8-step workflow outlined in `SKILL.md`.
