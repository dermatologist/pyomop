"""
 Copyright (C) 2020 Bell Eapen

 This file is part of PyOMOP.

 PyOMOP is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 PyOMOP is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with PyOMOP.  If not, see <http://www.gnu.org/licenses/>.
"""

# coding: utf-8
from sqlalchemy import BigInteger, Column, Integer, Numeric, String, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AttributeDefinition(Base):
    __tablename__ = 'attribute_definition'

    # _id = Column(Integer, primary_key=True)
    attribute_definition_id = Column(Integer, primary_key=True)
    attribute_name = Column(String(255), nullable=False)
    attribute_description = Column(Text)
    attribute_type_concept_id = Column(Integer, nullable=False)
    attribute_syntax = Column(Text)


class CareSite(Base):
    __tablename__ = 'care_site'

    # _id = Column(Integer, primary_key=True)
    care_site_id = Column(Integer, primary_key=True)
    care_site_name = Column(String(255))
    place_of_service_concept_id = Column(Integer)
    location_id = Column(Integer)
    care_site_source_value = Column(String(50))
    place_of_service_source_value = Column(String(50))


class CdmSource(Base):
    __tablename__ = 'cdm_source'

    # _id = Column(Integer, primary_key=True)
    cdm_source_name = Column(String(255), primary_key=True)
    cdm_source_abbreviation = Column(String(25))
    cdm_holder = Column(String(255))
    source_description = Column(Text)
    source_documentation_reference = Column(String(255))
    cdm_etl_reference = Column(String(255))
    source_release_date = Column(String(30))
    cdm_release_date = Column(String(30))
    cdm_version = Column(String(10))
    vocabulary_version = Column(String(20))


class Cohort(Base):
    __tablename__ = 'cohort'

    _id = Column(Integer, primary_key=True)
    cohort_definition_id = Column(Integer, nullable=False)
    subject_id = Column(Integer, nullable=False)
    cohort_start_date = Column(String(30), nullable=False)
    cohort_end_date = Column(String(30), nullable=False)


class CohortAttribute(Base):
    __tablename__ = 'cohort_attribute'

    _id = Column(Integer, primary_key=True)
    cohort_definition_id = Column(Integer, nullable=False)
    cohort_start_date = Column(String(30), nullable=False)
    cohort_end_date = Column(String(30), nullable=False)
    subject_id = Column(Integer, nullable=False)
    attribute_definition_id = Column(Integer, nullable=False)
    value_as_number = Column(Numeric)
    value_as_concept_id = Column(Integer)


class CohortDefinition(Base):
    __tablename__ = 'cohort_definition'

    # _id = Column(Integer, primary_key=True)
    cohort_definition_id = Column(Integer, primary_key=True)
    cohort_definition_name = Column(String(255), nullable=False)
    cohort_definition_description = Column(Text)
    definition_type_concept_id = Column(Integer, nullable=False)
    cohort_definition_syntax = Column(Text)
    subject_concept_id = Column(Integer, nullable=False)
    cohort_initiation_date = Column(String(30))


class Concept(Base):
    __tablename__ = 'concept'

    # _id = Column(Integer, primary_key=True)
    concept_id = Column(Integer, primary_key=True)
    concept_name = Column(String(255), nullable=False)
    domain_id = Column(String(20), nullable=False)
    vocabulary_id = Column(String(20), nullable=False)
    concept_class_id = Column(String(20), nullable=False)
    standard_concept = Column(String(1))
    concept_code = Column(String(50), nullable=False)
    valid_start_date = Column(String(30), nullable=False)
    valid_end_date = Column(String(30), nullable=False)
    invalid_reason = Column(String(1))


class ConceptAncestor(Base):
    __tablename__ = 'concept_ancestor'

    _id = Column(Integer, primary_key=True)
    ancestor_concept_id = Column(Integer, nullable=False)
    descendant_concept_id = Column(Integer, nullable=False)
    min_levels_of_separation = Column(Integer, nullable=False)
    max_levels_of_separation = Column(Integer, nullable=False)


class ConceptClass(Base):
    __tablename__ = 'concept_class'

    # _id = Column(Integer, primary_key=True)
    concept_class_id = Column(String(20), primary_key=True)
    concept_class_name = Column(String(255), nullable=False)
    concept_class_concept_id = Column(Integer, nullable=False)


class ConceptRelationship(Base):
    __tablename__ = 'concept_relationship'

    _id = Column(Integer, primary_key=True)
    concept_id_1 = Column(Integer, nullable=False)
    concept_id_2 = Column(Integer, nullable=False)
    relationship_id = Column(String(20), nullable=False)
    valid_start_date = Column(String(30), nullable=False)
    valid_end_date = Column(String(30), nullable=False)
    invalid_reason = Column(String(1))


class ConceptSynonym(Base):
    __tablename__ = 'concept_synonym'

    _id = Column(Integer, primary_key=True)
    concept_id = Column(Integer, nullable=False)
    concept_synonym_name = Column(String(1000), nullable=False)
    language_concept_id = Column(Integer, nullable=False)


class ConditionEra(Base):
    __tablename__ = 'condition_era'

    # _id = Column(Integer, primary_key=True)
    condition_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    condition_concept_id = Column(Integer, nullable=False)
    condition_era_start_date = Column(String(30), nullable=False)
    condition_era_end_date = Column(String(30), nullable=False)
    condition_occurrence_count = Column(Integer)


class ConditionOccurrence(Base):
    __tablename__ = 'condition_occurrence'

    # _id = Column(Integer, primary_key=True)
    condition_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    condition_concept_id = Column(Integer, nullable=False)
    condition_start_date = Column(String(30), nullable=False)
    condition_end_date = Column(String(30))
    condition_type_concept_id = Column(Integer, nullable=False)
    stop_reason = Column(String(20))
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    condition_source_value = Column(String(50))
    condition_source_concept_id = Column(Integer)


class Death(Base):
    __tablename__ = 'death'

    _id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    death_date = Column(String(30), nullable=False)
    death_type_concept_id = Column(Integer, nullable=False)
    cause_concept_id = Column(Integer)
    cause_source_value = Column(String(50))
    cause_source_concept_id = Column(Integer)


class DeviceCost(Base):
    __tablename__ = 'device_cost'

    # _id = Column(Integer, primary_key=True)
    device_cost_id = Column(Integer, primary_key=True)
    device_exposure_id = Column(Integer, nullable=False)
    currency_concept_id = Column(Integer)
    paid_copay = Column(Numeric)
    paid_coinsurance = Column(Numeric)
    paid_toward_deductible = Column(Numeric)
    paid_by_payer = Column(Numeric)
    paid_by_coordination_benefits = Column(Numeric)
    total_out_of_pocket = Column(Numeric)
    total_paid = Column(Numeric)
    payer_plan_period_id = Column(Integer)


class DeviceExposure(Base):
    __tablename__ = 'device_exposure'

    # _id = Column(Integer, primary_key=True)
    device_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    device_concept_id = Column(Integer, nullable=False)
    device_exposure_start_date = Column(String(30), nullable=False)
    device_exposure_end_date = Column(String(30))
    device_type_concept_id = Column(Integer, nullable=False)
    unique_device_id = Column(String(50))
    quantity = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    device_source_value = Column(String(100))
    device_source_concept_id = Column(Integer)


class Domain(Base):
    __tablename__ = 'domain'

    # _id = Column(Integer, primary_key=True, nullable=False)
    domain_id = Column(String(20), primary_key=True)
    domain_name = Column(String(255), nullable=False)
    domain_concept_id = Column(Integer, nullable=False)


class DoseEra(Base):
    __tablename__ = 'dose_era'

    # _id = Column(Integer, primary_key=True)
    dose_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    unit_concept_id = Column(Integer, nullable=False)
    dose_value = Column(Numeric, nullable=False)
    dose_era_start_date = Column(String(30), nullable=False)
    dose_era_end_date = Column(String(30), nullable=False)


class DrugCost(Base):
    __tablename__ = 'drug_cost'

    # _id = Column(Integer, primary_key=True)
    drug_cost_id = Column(Integer, primary_key=True)
    drug_exposure_id = Column(Integer, nullable=False)
    currency_concept_id = Column(Integer)
    paid_copay = Column(Numeric)
    paid_coinsurance = Column(Numeric)
    paid_toward_deductible = Column(Numeric)
    paid_by_payer = Column(Numeric)
    paid_by_coordination_benefits = Column(Numeric)
    total_out_of_pocket = Column(Numeric)
    total_paid = Column(Numeric)
    ingredient_cost = Column(Numeric)
    dispensing_fee = Column(Numeric)
    average_wholesale_price = Column(Numeric)
    payer_plan_period_id = Column(Integer)


class DrugEra(Base):
    __tablename__ = 'drug_era'

    # _id = Column(Integer, primary_key=True)
    drug_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    drug_era_start_date = Column(String(30), nullable=False)
    drug_era_end_date = Column(String(30), nullable=False)
    drug_exposure_count = Column(Integer)
    gap_days = Column(Integer)


class DrugExposure(Base):
    __tablename__ = 'drug_exposure'

    # _id = Column(Integer, primary_key=True)
    drug_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    drug_exposure_start_date = Column(String(30), nullable=False)
    drug_exposure_end_date = Column(String(30))
    drug_type_concept_id = Column(Integer, nullable=False)
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)
    route_concept_id = Column(Integer)
    effective_drug_dose = Column(Numeric)
    dose_unit_concept_id = Column(Integer)
    lot_number = Column(String(50))
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    drug_source_value = Column(String(50))
    drug_source_concept_id = Column(Integer)
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))


class DrugStrength(Base):
    __tablename__ = 'drug_strength'

    # TODO Check this
    # _id = Column(Integer, primary_key=True)
    # drug_concept_id = Column(Integer, nullable=False)
    drug_concept_id = Column(Integer, primary_key=True)
    ingredient_concept_id = Column(Integer, nullable=False)
    amount_value = Column(Numeric)
    amount_unit_concept_id = Column(Integer)
    numerator_value = Column(Numeric)
    numerator_unit_concept_id = Column(Integer)
    denominator_value = Column(Numeric)
    denominator_unit_concept_id = Column(Integer)
    box_size = Column(Integer)
    valid_start_date = Column(String(30), nullable=False)
    valid_end_date = Column(String(30), nullable=False)
    invalid_reason = Column(String(1))


class FactRelationship(Base):
    __tablename__ = 'fact_relationship'

    _id = Column(Integer, primary_key=True)
    domain_concept_id_1 = Column(Integer, nullable=False)
    fact_id_1 = Column(Integer, nullable=False)
    domain_concept_id_2 = Column(Integer, nullable=False)
    fact_id_2 = Column(Integer, nullable=False)
    relationship_concept_id = Column(Integer, nullable=False)


class Location(Base):
    __tablename__ = 'location'

    # _id = Column(Integer, primary_key=True)
    location_id = Column(Integer, primary_key=True)
    address_1 = Column(String(50))
    address_2 = Column(String(50))
    city = Column(String(50))
    state = Column(String(2))
    zip = Column(String(9))
    county = Column(String(20))
    location_source_value = Column(String(50))


class Measurement(Base):
    __tablename__ = 'measurement'

    # _id = Column(Integer, primary_key=True)
    measurement_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    measurement_concept_id = Column(Integer, nullable=False)
    measurement_date = Column(String(30), nullable=False)
    measurement_time = Column(String(10))
    measurement_type_concept_id = Column(Integer, nullable=False)
    operator_concept_id = Column(Integer)
    value_as_number = Column(Numeric)
    value_as_concept_id = Column(Integer)
    unit_concept_id = Column(Integer)
    range_low = Column(Numeric)
    range_high = Column(Numeric)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    measurement_source_value = Column(String(50))
    measurement_source_concept_id = Column(Integer)
    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))


class Note(Base):
    __tablename__ = 'note'

    # _id = Column(Integer, primary_key=True)
    note_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    note_date = Column(String(30), nullable=False)
    note_time = Column(String(10))
    note_type_concept_id = Column(Integer, nullable=False)
    note_text = Column(Text, nullable=False)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    note_source_value = Column(String(50))


class Observation(Base):
    __tablename__ = 'observation'

    # _id = Column(Integer, primary_key=True)
    observation_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    observation_concept_id = Column(Integer, nullable=False)
    observation_date = Column(String(30), nullable=False)
    observation_time = Column(String(10))
    observation_type_concept_id = Column(Integer, nullable=False)
    value_as_number = Column(Numeric)
    value_as_string = Column(String(60))
    value_as_concept_id = Column(Integer)
    qualifier_concept_id = Column(Integer)
    unit_concept_id = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    observation_source_value = Column(String(50))
    observation_source_concept_id = Column(Integer)
    unit_source_value = Column(String(50))
    qualifier_source_value = Column(String(50))


class ObservationPeriod(Base):
    __tablename__ = 'observation_period'

    # _id = Column(Integer, primary_key=True)
    observation_period_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    observation_period_start_date = Column(String(30), nullable=False)
    observation_period_end_date = Column(String(30), nullable=False)
    period_type_concept_id = Column(Integer, nullable=False)


class PayerPlanPeriod(Base):
    __tablename__ = 'payer_plan_period'

    # _id = Column(Integer, primary_key=True)
    payer_plan_period_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    payer_plan_period_start_date = Column(String(30), nullable=False)
    payer_plan_period_end_date = Column(String(30), nullable=False)
    payer_source_value = Column(String(50))
    plan_source_value = Column(String(50))
    family_source_value = Column(String(50))


class Person(Base):
    __tablename__ = 'person'

    #_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, primary_key=True)
    gender_concept_id = Column(Integer, nullable=False)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer)
    day_of_birth = Column(Integer)
    #time_of_birth = Column(String(10))
    birth_datetime = Column(String(10))
    death_datetime = Column(String(10))
    race_concept_id = Column(Integer, nullable=False)
    ethnicity_concept_id = Column(Integer, nullable=False)
    location_id = Column(Integer)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    person_source_value = Column(String(50))
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(Integer)
    race_source_value = Column(String(50))
    race_source_concept_id = Column(Integer)
    ethnicity_source_value = Column(String(50))
    ethnicity_source_concept_id = Column(Integer)


class ProcedureCost(Base):
    __tablename__ = 'procedure_cost'

    # _id = Column(Integer, primary_key=True)
    procedure_cost_id = Column(Integer, primary_key=True)
    procedure_occurrence_id = Column(Integer, nullable=False)
    currency_concept_id = Column(Integer)
    paid_copay = Column(Numeric)
    paid_coinsurance = Column(Numeric)
    paid_toward_deductible = Column(Numeric)
    paid_by_payer = Column(Numeric)
    paid_by_coordination_benefits = Column(Numeric)
    total_out_of_pocket = Column(Numeric)
    total_paid = Column(Numeric)
    revenue_code_concept_id = Column(Integer)
    payer_plan_period_id = Column(Integer)
    revenue_code_source_value = Column(String(50))


class ProcedureOccurrence(Base):
    __tablename__ = 'procedure_occurrence'

    # _id = Column(Integer, primary_key=True)
    procedure_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    procedure_concept_id = Column(Integer, nullable=False)
    procedure_date = Column(String(30), nullable=False)
    procedure_type_concept_id = Column(Integer, nullable=False)
    modifier_concept_id = Column(Integer)
    quantity = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(BigInteger)
    procedure_source_value = Column(String(50))
    procedure_source_concept_id = Column(Integer)
    qualifier_source_value = Column(String(50))


class Provider(Base):
    __tablename__ = 'provider'

    # _id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, primary_key=True)
    provider_name = Column(String(255))
    npi = Column(String(20))
    dea = Column(String(20))
    specialty_concept_id = Column(Integer)
    care_site_id = Column(Integer)
    year_of_birth = Column(Integer)
    gender_concept_id = Column(Integer)
    provider_source_value = Column(String(50))
    specialty_source_value = Column(String(50))
    specialty_source_concept_id = Column(Integer)
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(Integer)


class Relationship(Base):
    __tablename__ = 'relationship'

    # _id = Column(Integer, primary_key=True)
    relationship_id = Column(String(20), primary_key=True)
    relationship_name = Column(String(255), nullable=False)
    is_hierarchical = Column(String(1), nullable=False)
    defines_ancestry = Column(String(1), nullable=False)
    reverse_relationship_id = Column(String(20), nullable=False)
    relationship_concept_id = Column(Integer, nullable=False)


class SourceToConceptMap(Base):
    __tablename__ = 'source_to_concept_map'

    _id = Column(Integer, primary_key=True)
    source_code = Column(String(50), nullable=False)
    source_concept_id = Column(Integer, nullable=False)
    source_vocabulary_id = Column(String(20), nullable=False)
    source_code_description = Column(String(255))
    target_concept_id = Column(Integer, nullable=False)
    target_vocabulary_id = Column(String(20), nullable=False)
    valid_start_date = Column(String(30), nullable=False)
    valid_end_date = Column(String(30), nullable=False)
    invalid_reason = Column(String(1))


class Speciman(Base):
    __tablename__ = 'specimen'

    # _id = Column(Integer, primary_key=True)
    specimen_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    specimen_concept_id = Column(Integer, nullable=False)
    specimen_type_concept_id = Column(Integer, nullable=False)
    specimen_date = Column(String(30), nullable=False)
    specimen_time = Column(String(10))
    quantity = Column(Numeric)
    unit_concept_id = Column(Integer)
    anatomic_site_concept_id = Column(Integer)
    disease_status_concept_id = Column(Integer)
    specimen_source_id = Column(String(50))
    specimen_source_value = Column(String(50))
    unit_source_value = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))


class VisitCost(Base):
    __tablename__ = 'visit_cost'

    # _id = Column(Integer, primary_key=True)
    visit_cost_id = Column(Integer, primary_key=True)
    visit_occurrence_id = Column(BigInteger, nullable=False)
    currency_concept_id = Column(Integer)
    paid_copay = Column(Numeric)
    paid_coinsurance = Column(Numeric)
    paid_toward_deductible = Column(Numeric)
    paid_by_payer = Column(Numeric)
    paid_by_coordination_benefits = Column(Numeric)
    total_out_of_pocket = Column(Numeric)
    total_paid = Column(Numeric)
    payer_plan_period_id = Column(Integer)


class VisitOccurrence(Base):
    __tablename__ = 'visit_occurrence'

    # _id = Column(Integer, primary_key=True)
    visit_occurrence_id = Column(BigInteger, primary_key=True)
    person_id = Column(Integer, nullable=False)
    visit_concept_id = Column(Integer, nullable=False)
    visit_start_date = Column(String(30), nullable=False)
    visit_start_time = Column(String(10))
    visit_end_date = Column(String(30), nullable=False)
    visit_end_time = Column(String(10))
    visit_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    visit_source_value = Column(String(50))
    visit_source_concept_id = Column(Integer)


class Vocabulary(Base):
    __tablename__ = 'vocabulary'

    # _id = Column(Integer, primary_key=True)
    vocabulary_id = Column(String(20), primary_key=True)
    vocabulary_name = Column(String(255), nullable=False)
    vocabulary_reference = Column(String(255), nullable=False)
    vocabulary_version = Column(String(255))
    vocabulary_concept_id = Column(Integer, nullable=False)
