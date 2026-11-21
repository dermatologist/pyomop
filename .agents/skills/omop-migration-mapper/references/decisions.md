# Mapping Decisions Reference

This document captures recurrent mapping patterns by EMR type and clinical domain.

## Epic EHR mappings

### Patient demographics
- Patient ID (`patient_id` table) → `person.person_id` (numeric) or `person.person_source_value` (MRN).
- Date of birth (components in `patient` table) → `year_of_birth`, `month_of_birth`, `day_of_birth`.
- Gender code (e.g., 'M', 'F' in `patient.sex`) → `gender_concept_id` (8532 = Male, 8507 = Female).

### Diagnoses (active problem list)
- Problem code (ICD-9 or ICD-10) in `diagnosis` table → `condition_occurrence.condition_source_value`.
- Map to `condition_concept_id` via OMOP `concept` vocab lookup.
- Onset date in `problem.date_of_onset` → `condition_start_date`.
- Resolution date (if recorded) → `condition_end_date`.
- Status (active, resolved, inactive) → `condition_status_concept_id` or filter to active only.

### Medications (active medication list)
- Drug code (NDC or generic formula) in `medication` table → `drug_exposure.drug_source_value`.
- Quantity and dose fields → `quantity`, `days_supply`.
- Medication start date → `drug_exposure_start_date`.
- Medication end date (if discontinued) → `drug_exposure_end_date` or NULL.

### Lab results
- Order code (LOINC or local) → `measurement.measurement_source_value`.
- Result value (numeric) → `measurement.value_as_number`.
- Result value (text, e.g., "positive", "negative") → `measurement.value_as_string`.
- Unit code → `measurement.unit_source_value` (map to concept if known).
- Lab order date → `measurement.measurement_date`.
- Filter to completed orders only: `WHERE status = 'FINAL'`.

## Cerner EHR mappings

### Patient demographics
- Encntr ID (encounter) + Person ID (patient) → `person.person_id` or `person_source_value`.
- Birthdate (single column DATETIME) → split to `year_of_birth`, `month_of_birth`, `day_of_birth`.
- Gender (e.g., 'M', 'F') → concept lookup (8532, 8507).

### Diagnoses
- Diagnosis code + coding system (e.g., ICD-10, ICD-9) in `diagnosis` table → `condition_source_value`.
- Use the coding system to determine OMOP vocabulary (ICD-10 CM vs ICD-9 CM).
- Condition status (e.g., 'confirmed', 'suspected', 'ruled out') → `condition_status_concept_id`.

### Medications
- Medication mnemonic (e.g., "APAP") + strength → `drug_source_value`.
- Route (e.g., 'PO', 'IV') → `drug.route_source_value`.
- Frequency (e.g., 'BID', 'TID') → may require post-processing or stored as text in an observation.

## Common column mappings

| OMOP Column | Mapping Rule | Example |
|-------------|--------------|---------|
| `person_id` | Numeric patient ID; if unavailable, use `person_source_value` + `fix_person_id` post-step. | Patient ID = 12345 |
| `gender_concept_id` | 8532 (Male), 8507 (Female), 8521 (Unknown) | Source 'M' → 8532 |
| `race_concept_id` | 8515 (White), 8516 (Black), 8557 (Asian), 8657 (Unknown) | Source 'Caucasian' → 8515 |
| `*_source_value` | Raw, unmodified source code/text | ICD-10 'E11.9' |
| `*_concept_id` | OMOP standard concept ID; 0 if unmapped | Looked up via OHDSI Athena |
| `*_date` | ISO 8601 (YYYY-MM-DD) | '2023-05-15' |
| `*_datetime` | ISO 8601 with time (YYYY-MM-DD HH:MM:SS) | '2023-05-15 14:30:00' |
| `visit_occurrence_id` | FK to visit/encounter; required for fact tables | Encounter ID = 98765 |
| `provider_id` | FK to provider; often optional (NULL allowed) | Provider code = 'PCP123' |

## Post-migration steps (often required)

1. **Concept reconciliation:** Run OHDSI Athena or the loader's built-in `concept` post-step to resolve unmapped source codes.
2. **Unit conversions:** Lab values may need conversion (e.g., mg/dL to mmol/L); prepare a conversion script.
3. **Date corrections:** Validate dates are within expected ranges (birth year > 1850, clinical dates within data collection period).
4. **Person ID resolution:** If using `person_source_value`, run `fix_person_id` to assign numeric `person_id`.
5. **Referential integrity checks:** Ensure all `person_id`, `visit_occurrence_id`, and `provider_id` FKs are valid.

---

For source-specific decisions, extend this document with your EMR and mapping lessons learned.
