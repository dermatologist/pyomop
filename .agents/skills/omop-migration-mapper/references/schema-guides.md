# Interpreting Source Database Schema

This guide helps you identify which source tables and columns map to OMOP tables and concepts.

## Table mapping patterns

### Patient/Person tables
- **Source pattern:** Usually named `patient`, `member`, `person`, `account`, or similar.
- **OMOP target:** `person` table.
- **Key columns:** Patient ID (becomes `person_id` or `person_source_value`), date of birth, gender, race, ethnicity.
- **Common pitfall:** Non-numeric patient IDs (MRN, account number). Map to `person_source_value`; the loader will resolve `person_id` numerically.

### Clinical event tables
- **Diagnoses:** Source name usually `diagnosis`, `icd_diagnosis`, `problem_list`, or similar.
  - **OMOP target:** `condition_occurrence`.
  - **Key columns:** Patient ID, diagnosis code (ICD-9, ICD-10), onset/resolution dates.

- **Medications/Prescriptions:** Source name `medication`, `drug`, `prescription`, `pharmacy`, or similar.
  - **OMOP target:** `drug_exposure`.
  - **Key columns:** Patient ID, medication code (NDC, RxNorm), start/end dates, quantity, days supply.

- **Lab results:** Source name `lab`, `laboratory_result`, `lab_value`, `test_result`, or similar.
  - **OMOP target:** `measurement`.
  - **Key columns:** Patient ID, lab code (LOINC, local), result value, unit, reference range, test date.

- **Vital signs:** Often in a `vital_sign`, `observation`, or `measurement` table.
  - **OMOP target:** `measurement` (with concept mapping to OMOP vital sign codes).
  - **Key columns:** Patient ID, vital code, result numeric value, unit, date/time.

- **Procedures:** Source name `procedure`, `surgical_procedure`, `clinical_procedure`.
  - **OMOP target:** `procedure_occurrence`.
  - **Key columns:** Patient ID, procedure code (CPT, ICD), date.

### Visit/Encounter tables
- **Source pattern:** Named `visit`, `encounter`, `admission`, `admission_detail`.
- **OMOP target:** `visit_occurrence` (or `visit_detail` for granular e.g., ICU vs ward).
- **Key columns:** Visit ID, patient ID, visit type (inpatient, outpatient, ED, etc.), start/end date, provider, care site (hospital, clinic).

### Provider/Facility tables
- **Providers:** Source `provider`, `physician`, `clinician`, `staff`.
  - **OMOP target:** `provider`.
  - **Key columns:** Provider ID, name, specialty, facility/care site assignment.

- **Care sites (hospitals, clinics):** Source `facility`, `organization`, `care_site`.
  - **OMOP target:** `care_site`.
  - **Key columns:** Care site ID, name, address, place of service (IP, OP, ED, etc.).

- **Locations:** Addresses and geographic data.
  - **OMOP target:** `location`.
  - **Key columns:** Address, city, state, ZIP, county.

## Column type coercion rules

| Source type | OMOP target type | Coercion rule |
|-------------|------------------|---------------|
| VARCHAR, TEXT, CHAR | String | As-is; trim whitespace. |
| INT, BIGINT, SMALLINT | Integer | Parse as integer; null if unparseable. |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | Numeric | Parse as decimal; null if unparseable. |
| DATE | Date | Parse as ISO 8601 (YYYY-MM-DD) or common US format (MM/DD/YYYY); null if invalid. |
| TIMESTAMP, DATETIME | DateTime | Parse as ISO 8601 or common format; null if invalid. |
| BOOLEAN, BIT | Integer | Convert TRUE/1 → 1, FALSE/0 → 0. |

## Vocabulary mapping hints

- **ICD-9 diagnosis codes:** Use `concept_code` in `concept` table; domain = 'Condition'; vocabulary = 'ICD-9 CM'.
- **ICD-10 diagnosis codes:** Vocabulary = 'ICD-10 CM'.
- **CPT procedure codes:** Vocabulary = 'CPT-4'.
- **NDC medication codes:** Vocabulary = 'NDC'.
- **LOINC lab codes:** Vocabulary = 'LOINC'.
- **SNOMED concepts:** Vocabulary = 'SNOMED'; often standard (standard_concept = 'S').

If source codes don't match OMOP vocabularies, use `source_to_concept_map` for a custom lookup table (includes fallback concept IDs).

## Filtering tips

Common filters to exclude incomplete or erroneous data:

- **Active records only:** `WHERE is_active = 1` or `WHERE deleted_at IS NULL`.
- **Valid dates:** `WHERE diagnosis_date >= '2010-01-01'` or `WHERE NOT (admission_date > discharge_date)`.
- **Exclude "test" data:** `WHERE provider_id <> 'TEST'` or `WHERE patient_class <> 'UNKNOWN'`.
- **Demographics sanity:** `WHERE year_of_birth > 1850 AND year_of_birth < 2024`.

---

For advanced patterns specific to your EMR, see `references/decisions.md`.
