# coding: utf-8
from sqlalchemy import Column, Float, MetaData, Table, Text

metadata = MetaData()


t_CARE_SITE = Table(
    'CARE_SITE', metadata,
    Column('CARE_SITE_ID', Float),
    Column('CARE_SITE_NAME', Text),
    Column('PLACE_OF_SERVICE_CONCEPT_ID', Float),
    Column('LOCATION_ID', Float),
    Column('CARE_SITE_SOURCE_VALUE', Text),
    Column('PLACE_OF_SERVICE_SOURCE_VALUE', Text)
)


t_CDM_SOURCE = Table(
    'CDM_SOURCE', metadata,
    Column('CDM_SOURCE_NAME', Text),
    Column('CDM_SOURCE_ABBREVIATION', Text),
    Column('CDM_HOLDER', Text),
    Column('SOURCE_DESCRIPTION', Text),
    Column('SOURCE_DOCUMENTATION_REFERENCE', Text),
    Column('CDM_ETL_REFERENCE', Text),
    Column('SOURCE_RELEASE_DATE', Float),
    Column('CDM_RELEASE_DATE', Float),
    Column('CDM_VERSION', Text),
    Column('VOCABULARY_VERSION', Text)
)


t_COHORT = Table(
    'COHORT', metadata,
    Column('COHORT_DEFINITION_ID', Float),
    Column('SUBJECT_ID', Float),
    Column('COHORT_START_DATE', Float),
    Column('COHORT_END_DATE', Float)
)


t_COHORT_ATTRIBUTE = Table(
    'COHORT_ATTRIBUTE', metadata,
    Column('COHORT_DEFINITION_ID', Float),
    Column('SUBJECT_ID', Float),
    Column('COHORT_START_DATE', Float),
    Column('COHORT_END_DATE', Float),
    Column('ATTRIBUTE_DEFINITION_ID', Float),
    Column('VALUE_AS_NUMBER', Float),
    Column('VALUE_AS_CONCEPT_ID', Float)
)


t_CONCEPT = Table(
    'CONCEPT', metadata,
    Column('CONCEPT_ID', Float),
    Column('CONCEPT_NAME', Text),
    Column('DOMAIN_ID', Text),
    Column('VOCABULARY_ID', Text),
    Column('CONCEPT_CLASS_ID', Text),
    Column('STANDARD_CONCEPT', Text),
    Column('CONCEPT_CODE', Text),
    Column('VALID_START_DATE', Float),
    Column('VALID_END_DATE', Float),
    Column('INVALID_REASON', Text)
)


t_CONCEPT_ANCESTOR = Table(
    'CONCEPT_ANCESTOR', metadata,
    Column('ANCESTOR_CONCEPT_ID', Float),
    Column('DESCENDANT_CONCEPT_ID', Float),
    Column('MIN_LEVELS_OF_SEPARATION', Float),
    Column('MAX_LEVELS_OF_SEPARATION', Float)
)


t_CONCEPT_CLASS = Table(
    'CONCEPT_CLASS', metadata,
    Column('CONCEPT_CLASS_ID', Text),
    Column('CONCEPT_CLASS_NAME', Text),
    Column('CONCEPT_CLASS_CONCEPT_ID', Float)
)


t_CONCEPT_RELATIONSHIP = Table(
    'CONCEPT_RELATIONSHIP', metadata,
    Column('CONCEPT_ID_1', Float),
    Column('CONCEPT_ID_2', Float),
    Column('RELATIONSHIP_ID', Text),
    Column('VALID_START_DATE', Float),
    Column('VALID_END_DATE', Float),
    Column('INVALID_REASON', Text)
)


t_CONCEPT_SYNONYM = Table(
    'CONCEPT_SYNONYM', metadata,
    Column('CONCEPT_ID', Float),
    Column('CONCEPT_SYNONYM_NAME', Text),
    Column('LANGUAGE_CONCEPT_ID', Float)
)


t_CONDITION_ERA = Table(
    'CONDITION_ERA', metadata,
    Column('CONDITION_ERA_ID', Float),
    Column('PERSON_ID', Float),
    Column('CONDITION_CONCEPT_ID', Float),
    Column('CONDITION_ERA_START_DATE', Float),
    Column('CONDITION_ERA_END_DATE', Float),
    Column('CONDITION_OCCURRENCE_COUNT', Float)
)


t_CONDITION_OCCURRENCE = Table(
    'CONDITION_OCCURRENCE', metadata,
    Column('CONDITION_OCCURRENCE_ID', Float),
    Column('PERSON_ID', Float),
    Column('CONDITION_CONCEPT_ID', Float),
    Column('CONDITION_START_DATE', Float),
    Column('CONDITION_START_DATETIME', Float),
    Column('CONDITION_END_DATE', Float),
    Column('CONDITION_END_DATETIME', Float),
    Column('CONDITION_TYPE_CONCEPT_ID', Float),
    Column('STOP_REASON', Text),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('CONDITION_SOURCE_VALUE', Text),
    Column('CONDITION_SOURCE_CONCEPT_ID', Float),
    Column('CONDITION_STATUS_SOURCE_VALUE', Text),
    Column('CONDITION_STATUS_CONCEPT_ID', Float)
)


t_COST = Table(
    'COST', metadata,
    Column('COST_ID', Float),
    Column('COST_EVENT_ID', Float),
    Column('COST_DOMAIN_ID', Text),
    Column('COST_TYPE_CONCEPT_ID', Float),
    Column('CURRENCY_CONCEPT_ID', Float),
    Column('TOTAL_CHARGE', Float),
    Column('TOTAL_COST', Float),
    Column('TOTAL_PAID', Float),
    Column('PAID_BY_PAYER', Float),
    Column('PAID_BY_PATIENT', Float),
    Column('PAID_PATIENT_COPAY', Float),
    Column('PAID_PATIENT_COINSURANCE', Float),
    Column('PAID_PATIENT_DEDUCTIBLE', Float),
    Column('PAID_BY_PRIMARY', Float),
    Column('PAID_INGREDIENT_COST', Float),
    Column('PAID_DISPENSING_FEE', Float),
    Column('PAYER_PLAN_PERIOD_ID', Float),
    Column('AMOUNT_ALLOWED', Float),
    Column('REVENUE_CODE_CONCEPT_ID', Float),
    Column('REVEUE_CODE_SOURCE_VALUE', Text),
    Column('DRG_CONCEPT_ID', Float),
    Column('DRG_SOURCE_VALUE', Text)
)


t_DEATH = Table(
    'DEATH', metadata,
    Column('PERSON_ID', Float),
    Column('DEATH_DATE', Float),
    Column('DEATH_DATETIME', Float),
    Column('DEATH_TYPE_CONCEPT_ID', Float),
    Column('CAUSE_CONCEPT_ID', Float),
    Column('CAUSE_SOURCE_VALUE', Text),
    Column('CAUSE_SOURCE_CONCEPT_ID', Float)
)


t_DEVICE_EXPOSURE = Table(
    'DEVICE_EXPOSURE', metadata,
    Column('DEVICE_EXPOSURE_ID', Float),
    Column('PERSON_ID', Float),
    Column('DEVICE_CONCEPT_ID', Float),
    Column('DEVICE_EXPOSURE_START_DATE', Float),
    Column('DEVICE_EXPOSURE_START_DATETIME', Float),
    Column('DEVICE_EXPOSURE_END_DATE', Float),
    Column('DEVICE_EXPOSURE_END_DATETIME', Float),
    Column('DEVICE_TYPE_CONCEPT_ID', Float),
    Column('UNIQUE_DEVICE_ID', Text),
    Column('QUANTITY', Float),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('DEVICE_SOURCE_VALUE', Text),
    Column('DEVICE_SOURCE_CONCEPT_ID', Float)
)


t_DOMAIN = Table(
    'DOMAIN', metadata,
    Column('DOMAIN_ID', Text),
    Column('DOMAIN_NAME', Text),
    Column('DOMAIN_CONCEPT_ID', Float)
)


t_DOSE_ERA = Table(
    'DOSE_ERA', metadata,
    Column('DOSE_ERA_ID', Float),
    Column('PERSON_ID', Float),
    Column('DRUG_CONCEPT_ID', Float),
    Column('UNIT_CONCEPT_ID', Float),
    Column('DOSE_VALUE', Float),
    Column('DOSE_ERA_START_DATE', Float),
    Column('DOSE_ERA_END_DATE', Float)
)


t_DRUG_ERA = Table(
    'DRUG_ERA', metadata,
    Column('DRUG_ERA_ID', Float),
    Column('PERSON_ID', Float),
    Column('DRUG_CONCEPT_ID', Float),
    Column('DRUG_ERA_START_DATE', Float),
    Column('DRUG_ERA_END_DATE', Float),
    Column('DRUG_EXPOSURE_COUNT', Float),
    Column('GAP_DAYS', Float)
)


t_DRUG_EXPOSURE = Table(
    'DRUG_EXPOSURE', metadata,
    Column('DRUG_EXPOSURE_ID', Float),
    Column('PERSON_ID', Float),
    Column('DRUG_CONCEPT_ID', Float),
    Column('DRUG_EXPOSURE_START_DATE', Float),
    Column('DRUG_EXPOSURE_START_DATETIME', Float),
    Column('DRUG_EXPOSURE_END_DATE', Float),
    Column('DRUG_EXPOSURE_END_DATETIME', Float),
    Column('VERBATIM_END_DATE', Float),
    Column('DRUG_TYPE_CONCEPT_ID', Float),
    Column('STOP_REASON', Text),
    Column('REFILLS', Float),
    Column('QUANTITY', Float),
    Column('DAYS_SUPPLY', Float),
    Column('SIG', Text),
    Column('ROUTE_CONCEPT_ID', Float),
    Column('LOT_NUMBER', Text),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('DRUG_SOURCE_VALUE', Text),
    Column('DRUG_SOURCE_CONCEPT_ID', Float),
    Column('ROUTE_SOURCE_VALUE', Text),
    Column('DOSE_UNIT_SOURCE_VALUE', Text)
)


t_DRUG_STRENGTH = Table(
    'DRUG_STRENGTH', metadata,
    Column('DRUG_CONCEPT_ID', Float),
    Column('INGREDIENT_CONCEPT_ID', Float),
    Column('AMOUNT_VALUE', Float),
    Column('AMOUNT_UNIT_CONCEPT_ID', Float),
    Column('NUMERATOR_VALUE', Float),
    Column('NUMERATOR_UNIT_CONCEPT_ID', Float),
    Column('DENOMINATOR_VALUE', Float),
    Column('DENOMINATOR_UNIT_CONCEPT_ID', Float),
    Column('BOX_SIZE', Float),
    Column('VALID_START_DATE', Float),
    Column('VALID_END_DATE', Float),
    Column('INVALID_REASON', Text)
)


t_FACT_RELATIONSHIP = Table(
    'FACT_RELATIONSHIP', metadata,
    Column('DOMAIN_CONCEPT_ID_1', Float),
    Column('FACT_ID_1', Float),
    Column('DOMAIN_CONCEPT_ID_2', Float),
    Column('FACT_ID_2', Float),
    Column('RELATIONSHIP_CONCEPT_ID', Float)
)


t_LOCATION = Table(
    'LOCATION', metadata,
    Column('LOCATION_ID', Float),
    Column('ADDRESS_1', Text),
    Column('ADDRESS_2', Text),
    Column('CITY', Text),
    Column('STATE', Text),
    Column('ZIP', Text),
    Column('COUNTY', Text),
    Column('LOCATION_SOURCE_VALUE', Text)
)


t_MEASUREMENT = Table(
    'MEASUREMENT', metadata,
    Column('MEASUREMENT_ID', Float),
    Column('PERSON_ID', Float),
    Column('MEASUREMENT_CONCEPT_ID', Float),
    Column('MEASUREMENT_DATE', Float),
    Column('MEASUREMENT_DATETIME', Float),
    Column('MEASUREMENT_TIME', Text),
    Column('MEASUREMENT_TYPE_CONCEPT_ID', Float),
    Column('OPERATOR_CONCEPT_ID', Float),
    Column('VALUE_AS_NUMBER', Float),
    Column('VALUE_AS_CONCEPT_ID', Float),
    Column('UNIT_CONCEPT_ID', Float),
    Column('RANGE_LOW', Float),
    Column('RANGE_HIGH', Float),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('MEASUREMENT_SOURCE_VALUE', Text),
    Column('MEASUREMENT_SOURCE_CONCEPT_ID', Float),
    Column('UNIT_SOURCE_VALUE', Text),
    Column('VALUE_SOURCE_VALUE', Text)
)


t_METADATA = Table(
    'METADATA', metadata,
    Column('METADATA_CONCEPT_ID', Float),
    Column('METADATA_TYPE_CONCEPT_ID', Float),
    Column('NAME', Text),
    Column('VALUE_AS_STRING', Text),
    Column('VALUE_AS_CONCEPT_ID', Float),
    Column('METADATA_DATE', Float),
    Column('METADATA_DATETIME', Float)
)


t_NOTE = Table(
    'NOTE', metadata,
    Column('NOTE_ID', Float),
    Column('PERSON_ID', Float),
    Column('NOTE_DATE', Float),
    Column('NOTE_DATETIME', Float),
    Column('NOTE_TYPE_CONCEPT_ID', Float),
    Column('NOTE_CLASS_CONCEPT_ID', Float),
    Column('NOTE_TITLE', Text),
    Column('NOTE_TEXT', Text),
    Column('ENCODING_CONCEPT_ID', Float),
    Column('LANGUAGE_CONCEPT_ID', Float),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('NOTE_SOURCE_VALUE', Text)
)


t_NOTE_NLP = Table(
    'NOTE_NLP', metadata,
    Column('NOTE_NLP_ID', Float),
    Column('NOTE_ID', Float),
    Column('SECTION_CONCEPT_ID', Float),
    Column('SNIPPET', Text),
    Column('OFFSET', Text),
    Column('LEXICAL_VARIANT', Text),
    Column('NOTE_NLP_CONCEPT_ID', Float),
    Column('NOTE_NLP_SOURCE_CONCEPT_ID', Float),
    Column('NLP_SYSTEM', Text),
    Column('NLP_DATE', Float),
    Column('NLP_DATETIME', Float),
    Column('TERM_EXISTS', Text),
    Column('TERM_TEMPORAL', Text),
    Column('TERM_MODIFIERS', Text)
)


t_OBSERVATION = Table(
    'OBSERVATION', metadata,
    Column('OBSERVATION_ID', Float),
    Column('PERSON_ID', Float),
    Column('OBSERVATION_CONCEPT_ID', Float),
    Column('OBSERVATION_DATE', Float),
    Column('OBSERVATION_DATETIME', Float),
    Column('OBSERVATION_TYPE_CONCEPT_ID', Float),
    Column('VALUE_AS_NUMBER', Float),
    Column('VALUE_AS_STRING', Text),
    Column('VALUE_AS_CONCEPT_ID', Float),
    Column('QUALIFIER_CONCEPT_ID', Float),
    Column('UNIT_CONCEPT_ID', Float),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('OBSERVATION_SOURCE_VALUE', Text),
    Column('OBSERVATION_SOURCE_CONCEPT_ID', Float),
    Column('UNIT_SOURCE_VALUE', Text),
    Column('QUALIFIER_SOURCE_VALUE', Text)
)


t_OBSERVATION_PERIOD = Table(
    'OBSERVATION_PERIOD', metadata,
    Column('OBSERVATION_PERIOD_ID', Float),
    Column('PERSON_ID', Float),
    Column('OBSERVATION_PERIOD_START_DATE', Float),
    Column('OBSERVATION_PERIOD_END_DATE', Float),
    Column('PERIOD_TYPE_CONCEPT_ID', Float)
)


t_PAYER_PLAN_PERIOD = Table(
    'PAYER_PLAN_PERIOD', metadata,
    Column('PAYER_PLAN_PERIOD_ID', Float),
    Column('PERSON_ID', Float),
    Column('PAYER_PLAN_PERIOD_START_DATE', Float),
    Column('PAYER_PLAN_PERIOD_END_DATE', Float),
    Column('PAYER_CONCEPT_ID', Float),
    Column('PAYER_SOURCE_VALUE', Text),
    Column('PAYER_SOURCE_CONCEPT_ID', Float),
    Column('PLAN_CONCEPT_ID', Float),
    Column('PLAN_SOURCE_VALUE', Text),
    Column('PLAN_SOURCE_CONCEPT_ID', Float),
    Column('SPONSOR_CONCEPT_ID', Float),
    Column('SPONSOR_SOURCE_VALUE', Text),
    Column('SPONSOR_SOURCE_CONCEPT_ID', Float),
    Column('FAMILY_SOURCE_VALUE', Text),
    Column('STOP_REASON_CONCEPT_ID', Float),
    Column('STOP_REASON_SOURCE_VALUE', Text),
    Column('STOP_REASON_SOURCE_CONCEPT_ID', Float)
)


t_PERSON = Table(
    'PERSON', metadata,
    Column('PERSON_ID', Float),
    Column('GENDER_CONCEPT_ID', Float),
    Column('YEAR_OF_BIRTH', Float),
    Column('MONTH_OF_BIRTH', Float),
    Column('DAY_OF_BIRTH', Float),
    Column('BIRTH_DATETIME', Float),
    Column('RACE_CONCEPT_ID', Float),
    Column('ETHNICITY_CONCEPT_ID', Float),
    Column('LOCATION_ID', Float),
    Column('PROVIDER_ID', Float),
    Column('CARE_SITE_ID', Float),
    Column('PERSON_SOURCE_VALUE', Text),
    Column('GENDER_SOURCE_VALUE', Text),
    Column('GENDER_SOURCE_CONCEPT_ID', Float),
    Column('RACE_SOURCE_VALUE', Text),
    Column('RACE_SOURCE_CONCEPT_ID', Float),
    Column('ETHNICITY_SOURCE_VALUE', Text),
    Column('ETHNICITY_SOURCE_CONCEPT_ID', Float)
)


t_PROCEDURE_OCCURRENCE = Table(
    'PROCEDURE_OCCURRENCE', metadata,
    Column('PROCEDURE_OCCURRENCE_ID', Float),
    Column('PERSON_ID', Float),
    Column('PROCEDURE_CONCEPT_ID', Float),
    Column('PROCEDURE_DATE', Float),
    Column('PROCEDURE_DATETIME', Float),
    Column('PROCEDURE_TYPE_CONCEPT_ID', Float),
    Column('MODIFIER_CONCEPT_ID', Float),
    Column('QUANTITY', Float),
    Column('PROVIDER_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('VISIT_DETAIL_ID', Float),
    Column('PROCEDURE_SOURCE_VALUE', Text),
    Column('PROCEDURE_SOURCE_CONCEPT_ID', Float),
    Column('MODIFIER_SOURCE_VALUE', Text)
)


t_PROVIDER = Table(
    'PROVIDER', metadata,
    Column('PROVIDER_ID', Float),
    Column('PROVIDER_NAME', Text),
    Column('NPI', Text),
    Column('DEA', Text),
    Column('SPECIALTY_CONCEPT_ID', Float),
    Column('CARE_SITE_ID', Float),
    Column('YEAR_OF_BIRTH', Float),
    Column('GENDER_CONCEPT_ID', Float),
    Column('PROVIDER_SOURCE_VALUE', Text),
    Column('SPECIALTY_SOURCE_VALUE', Text),
    Column('SPECIALTY_SOURCE_CONCEPT_ID', Float),
    Column('GENDER_SOURCE_VALUE', Text),
    Column('GENDER_SOURCE_CONCEPT_ID', Float)
)


t_RELATIONSHIP = Table(
    'RELATIONSHIP', metadata,
    Column('RELATIONSHIP_ID', Text),
    Column('RELATIONSHIP_NAME', Text),
    Column('IS_HIERARCHICAL', Text),
    Column('DEFINES_ANCESTRY', Text),
    Column('REVERSE_RELATIONSHIP_ID', Text),
    Column('RELATIONSHIP_CONCEPT_ID', Float)
)


t_SOURCE_TO_CONCEPT_MAP = Table(
    'SOURCE_TO_CONCEPT_MAP', metadata,
    Column('SOURCE_CODE', Text),
    Column('SOURCE_CONCEPT_ID', Float),
    Column('SOURCE_VOCABULARY_ID', Text),
    Column('SOURCE_CODE_DESCRIPTION', Text),
    Column('TARGET_CONCEPT_ID', Float),
    Column('TARGET_VOCABULARY_ID', Text),
    Column('VALID_START_DATE', Float),
    Column('VALID_END_DATE', Float),
    Column('INVALID_REASON', Text)
)


t_SPECIMEN = Table(
    'SPECIMEN', metadata,
    Column('SPECIMEN_ID', Float),
    Column('PERSON_ID', Float),
    Column('SPECIMEN_CONCEPT_ID', Float),
    Column('SPECIMEN_TYPE_CONCEPT_ID', Float),
    Column('SPECIMEN_DATE', Float),
    Column('SPECIMEN_DATETIME', Float),
    Column('QUANTITY', Float),
    Column('UNIT_CONCEPT_ID', Float),
    Column('ANATOMIC_SITE_CONCEPT_ID', Float),
    Column('DISEASE_STATUS_CONCEPT_ID', Float),
    Column('SPECIMEN_SOURCE_ID', Text),
    Column('SPECIMEN_SOURCE_VALUE', Text),
    Column('UNIT_SOURCE_VALUE', Text),
    Column('ANATOMIC_SITE_SOURCE_VALUE', Text),
    Column('DISEASE_STATUS_SOURCE_VALUE', Text)
)


t_VISIT_DETAIL = Table(
    'VISIT_DETAIL', metadata,
    Column('VISIT_DETAIL_ID', Float),
    Column('PERSON_ID', Float),
    Column('VISIT_DETAIL_CONCEPT_ID', Float),
    Column('VISIT_DETAIL_START_DATE', Float),
    Column('VISIT_DETAIL_START_DATETIME', Float),
    Column('VISIT_DETAIL_END_DATE', Float),
    Column('VISIT_DETAIL_END_DATETIME', Float),
    Column('VISIT_DETAIL_TYPE_CONCEPT_ID', Float),
    Column('PROVIDER_ID', Float),
    Column('CARE_SITE_ID', Float),
    Column('ADMITTING_SOURCE_CONCEPT_ID', Float),
    Column('DISCHARGE_TO_CONCEPT_ID', Float),
    Column('PRECEDING_VISIT_DETAIL_ID', Float),
    Column('VISIT_DETAIL_SOURCE_VALUE', Text),
    Column('VISIT_DETAIL_SOURCE_CONCEPT_ID', Float),
    Column('ADMITTING_SOURCE_VALUE', Text),
    Column('DISCHARGE_TO_SOURCE_VALUE', Text),
    Column('VISIT_DETAIL_PARENT_ID', Float),
    Column('VISIT_OCCURRENCE_ID', Float)
)


t_VISIT_OCCURRENCE = Table(
    'VISIT_OCCURRENCE', metadata,
    Column('VISIT_OCCURRENCE_ID', Float),
    Column('PERSON_ID', Float),
    Column('VISIT_CONCEPT_ID', Float),
    Column('VISIT_START_DATE', Float),
    Column('VISIT_START_DATETIME', Float),
    Column('VISIT_END_DATE', Float),
    Column('VISIT_END_DATETIME', Float),
    Column('VISIT_TYPE_CONCEPT_ID', Float),
    Column('PROVIDER_ID', Float),
    Column('CARE_SITE_ID', Float),
    Column('VISIT_SOURCE_VALUE', Text),
    Column('VISIT_SOURCE_CONCEPT_ID', Float),
    Column('ADMITTING_SOURCE_CONCEPT_ID', Float),
    Column('ADMITTING_SOURCE_VALUE', Text),
    Column('DISCHARGE_TO_CONCEPT_ID', Float),
    Column('DISCHARGE_TO_SOURCE_VALUE', Text),
    Column('PRECEDING_VISIT_OCCURRENCE_ID', Float)
)


t_VOCABULARY = Table(
    'VOCABULARY', metadata,
    Column('VOCABULARY_ID', Text),
    Column('VOCABULARY_NAME', Text),
    Column('VOCABULARY_REFERENCE', Text),
    Column('VOCABULARY_VERSION', Text),
    Column('VOCABULARY_CONCEPT_ID', Float)
)
