"""
Derivative based on the original work here: https://github.com/thehyve/omop-cdm/blob/main/src/omop_cdm/regular/cdm600/tables.py
Modifications made to this file:
- Removed support for schema.
- Added new tables

* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
"""

"""OMOP CDM 6.0.0 tables."""

import datetime
import decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


Base.metadata = MetaData()

CDM_SCHEMA = None
VOCAB_SCHEMA = None
FK_CONCEPT_ID = "concept.concept_id"
FK_VOCABULARY_ID = "vocabulary.vocabulary_id"
FK_DOMAIN_ID = "domain.domain_id"
FK_CONCEPT_CLASS_ID = "concept_class.concept_class_id"
FK_PERSON_ID = "person.person_id"
FK_VISIT_OCCURRENCE_ID = "visit_occurrence.visit_occurrence_id"
FK_VISIT_DETAIL_ID = "visit_detail.visit_detail_id"
FK_PROVIDER_ID = "provider.provider_id"
FK_CARE_SITE_ID = "care_site.care_site_id"
FK_LOCATION_ID = "location.location_id"


class AttributeDefinition(Base):
    __tablename__ = "attribute_definition"

    # _id = Column(Integer, primary_key=True)
    attribute_definition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    attribute_name: Mapped[str] = mapped_column(String(255), nullable=False)
    attribute_description: Mapped[str] = mapped_column(Text)
    attribute_type_concept_id: Mapped[int] = mapped_column(Integer, nullable=False)
    attribute_syntax: Mapped[str] = mapped_column(Text)


class Cohort(Base):
    __tablename__ = "cohort"

    _id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cohort_definition_id: Mapped[int] = mapped_column(Integer, nullable=False)
    subject_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cohort_start_date: Mapped[str] = mapped_column(String(30), nullable=False)
    cohort_end_date: Mapped[str] = mapped_column(String(30), nullable=False)


class CohortAttribute(Base):
    __tablename__ = "cohort_attribute"

    _id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cohort_definition_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cohort_start_date: Mapped[str] = mapped_column(String(30), nullable=False)
    cohort_end_date: Mapped[str] = mapped_column(String(30), nullable=False)
    subject_id: Mapped[int] = mapped_column(Integer, nullable=False)
    attribute_definition_id: Mapped[int] = mapped_column(Integer, nullable=False)
    value_as_number: Mapped[decimal.Decimal] = mapped_column(Numeric)
    value_as_concept_id: Mapped[int] = mapped_column(Integer)


class CohortDefinition(Base):
    __tablename__ = "cohort_definition"

    # _id = Column(Integer, primary_key=True)
    cohort_definition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cohort_definition_name: Mapped[str] = mapped_column(String(255), nullable=False)
    cohort_definition_description: Mapped[str] = mapped_column(Text)
    definition_type_concept_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cohort_definition_syntax: Mapped[str] = mapped_column(Text)
    subject_concept_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cohort_initiation_date: Mapped[str] = mapped_column(String(30))


class DeviceCost(Base):
    __tablename__ = "device_cost"

    # _id = Column(Integer, primary_key=True)
    device_cost_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_exposure_id: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_concept_id: Mapped[int] = mapped_column(Integer)
    paid_copay: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_coinsurance: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_toward_deductible: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_payer: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_coordination_benefits: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_out_of_pocket: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_paid: Mapped[decimal.Decimal] = mapped_column(Numeric)
    payer_plan_period_id: Mapped[int] = mapped_column(Integer)


class DrugCost(Base):
    __tablename__ = "drug_cost"

    # _id = Column(Integer, primary_key=True)
    drug_cost_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    drug_exposure_id: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_concept_id: Mapped[int] = mapped_column(Integer)
    paid_copay: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_coinsurance: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_toward_deductible: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_payer: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_coordination_benefits: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_out_of_pocket: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_paid: Mapped[decimal.Decimal] = mapped_column(Numeric)
    ingredient_cost: Mapped[decimal.Decimal] = mapped_column(Numeric)
    dispensing_fee: Mapped[decimal.Decimal] = mapped_column(Numeric)
    average_wholesale_price: Mapped[decimal.Decimal] = mapped_column(Numeric)
    payer_plan_period_id: Mapped[int] = mapped_column(Integer)


class ProcedureCost(Base):
    __tablename__ = "procedure_cost"

    # _id = Column(Integer, primary_key=True)
    procedure_cost_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    procedure_occurrence_id: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_concept_id: Mapped[int] = mapped_column(Integer)
    paid_copay: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_coinsurance: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_toward_deductible: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_payer: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_coordination_benefits: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_out_of_pocket: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_paid: Mapped[decimal.Decimal] = mapped_column(Numeric)
    revenue_code_concept_id: Mapped[int] = mapped_column(Integer)
    payer_plan_period_id: Mapped[int] = mapped_column(Integer)
    revenue_code_source_value: Mapped[str] = mapped_column(String(50))


class Specimen(Base):
    __tablename__ = "specimen"

    # _id = Column(Integer, primary_key=True)
    specimen_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int] = mapped_column(Integer, nullable=False)
    specimen_concept_id: Mapped[int] = mapped_column(Integer, nullable=False)
    specimen_type_concept_id: Mapped[int] = mapped_column(Integer, nullable=False)
    specimen_date: Mapped[str] = mapped_column(String(30), nullable=False)
    specimen_time: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[decimal.Decimal] = mapped_column(Numeric)
    unit_concept_id: Mapped[int] = mapped_column(Integer)
    anatomic_site_concept_id: Mapped[int] = mapped_column(Integer)
    disease_status_concept_id: Mapped[int] = mapped_column(Integer)
    specimen_source_id: Mapped[str] = mapped_column(String(50))
    specimen_source_value: Mapped[str] = mapped_column(String(50))
    unit_source_value: Mapped[str] = mapped_column(String(50))
    anatomic_site_source_value: Mapped[str] = mapped_column(String(50))
    disease_status_source_value: Mapped[str] = mapped_column(String(50))


class VisitCost(Base):
    __tablename__ = "visit_cost"

    # _id = Column(Integer, primary_key=True)
    visit_cost_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    visit_occurrence_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency_concept_id: Mapped[int] = mapped_column(Integer)
    paid_copay: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_coinsurance: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_toward_deductible: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_payer: Mapped[decimal.Decimal] = mapped_column(Numeric)
    paid_by_coordination_benefits: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_out_of_pocket: Mapped[decimal.Decimal] = mapped_column(Numeric)
    total_paid: Mapped[decimal.Decimal] = mapped_column(Numeric)
    payer_plan_period_id: Mapped[int] = mapped_column(Integer)


#####################################################################################


class FactRelationship(Base):
    __tablename__ = "fact_relationship"
    __table_args__ = {"schema": CDM_SCHEMA}

    fact_relationship_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain_concept_id_1: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    fact_id_1: Mapped[int] = mapped_column(Integer)
    domain_concept_id_2: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    fact_id_2: Mapped[int] = mapped_column(Integer)
    relationship_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )

    domain_concept_1: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="FactRelationship.domain_concept_id_1"
    )
    domain_concept_2: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="FactRelationship.domain_concept_id_2"
    )
    relationship_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="FactRelationship.relationship_concept_id"
    )


class Person(Base):
    __tablename__ = "person"
    __table_args__ = {"schema": CDM_SCHEMA}

    person_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    gender_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    year_of_birth: Mapped[int] = mapped_column(Integer)
    month_of_birth: Mapped[Optional[int]] = mapped_column(Integer)
    day_of_birth: Mapped[Optional[int]] = mapped_column(Integer)
    birth_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    death_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    race_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    ethnicity_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_LOCATION_ID))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    care_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CARE_SITE_ID))
    person_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    race_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    race_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    ethnicity_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    ethnicity_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    care_site: Mapped["CareSite"] = relationship(
        "CareSite", foreign_keys="Person.care_site_id"
    )
    ethnicity_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.ethnicity_concept_id"
    )
    ethnicity_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.ethnicity_source_concept_id"
    )
    gender_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.gender_concept_id"
    )
    gender_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.gender_source_concept_id"
    )
    location: Mapped["Location"] = relationship(
        "Location", foreign_keys="Person.location_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="Person.provider_id"
    )
    race_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.race_concept_id"
    )
    race_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Person.race_source_concept_id"
    )


class ObservationPeriod(Base):
    __tablename__ = "observation_period"
    __table_args__ = {"schema": CDM_SCHEMA}

    observation_period_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    observation_period_start_date: Mapped[datetime.date] = mapped_column(Date)
    observation_period_end_date: Mapped[datetime.date] = mapped_column(Date)
    period_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    period_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ObservationPeriod.period_type_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="ObservationPeriod.person_id"
    )


class VisitOccurrence(Base):
    __tablename__ = "visit_occurrence"
    __table_args__ = {"schema": CDM_SCHEMA}

    visit_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    visit_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID), index=True)
    visit_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    visit_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    visit_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    visit_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    visit_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    care_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CARE_SITE_ID))
    visit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    visit_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    admitted_from_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    admitted_from_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    discharge_to_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    discharge_to_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    preceding_visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID)
    )

    admitted_from_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitOccurrence.admitted_from_concept_id"
    )
    care_site: Mapped["CareSite"] = relationship(
        "CareSite", foreign_keys="VisitOccurrence.care_site_id"
    )
    discharge_to_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitOccurrence.discharge_to_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="VisitOccurrence.person_id"
    )
    preceding_visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="VisitOccurrence.preceding_visit_occurrence_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="VisitOccurrence.provider_id"
    )
    visit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitOccurrence.visit_concept_id"
    )
    visit_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitOccurrence.visit_source_concept_id"
    )
    visit_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitOccurrence.visit_type_concept_id"
    )


class VisitDetail(Base):
    __tablename__ = "visit_detail"
    __table_args__ = {"schema": CDM_SCHEMA}

    visit_detail_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    visit_detail_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    visit_detail_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    visit_detail_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    visit_detail_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    visit_detail_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    visit_detail_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    care_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CARE_SITE_ID))
    discharge_to_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    admitted_from_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    admitted_from_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    visit_detail_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    visit_detail_source_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    discharge_to_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    preceding_visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    visit_detail_parent_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    visit_occurrence_id: Mapped[int] = mapped_column(ForeignKey(FK_VISIT_OCCURRENCE_ID))

    admitted_from_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitDetail.admitted_from_concept_id"
    )
    care_site: Mapped["CareSite"] = relationship(
        "CareSite", foreign_keys="VisitDetail.care_site_id"
    )
    discharge_to_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitDetail.discharge_to_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="VisitDetail.person_id"
    )
    preceding_visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="VisitDetail.preceding_visit_detail_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="VisitDetail.provider_id"
    )
    visit_detail_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitDetail.visit_detail_concept_id"
    )
    visit_detail_parent: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="VisitDetail.visit_detail_parent_id"
    )
    visit_detail_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitDetail.visit_detail_source_concept_id"
    )
    visit_detail_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="VisitDetail.visit_detail_type_concept_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="VisitDetail.visit_occurrence_id"
    )


class ConditionOccurrence(Base):
    __tablename__ = "condition_occurrence"
    __table_args__ = {"schema": CDM_SCHEMA}

    condition_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    condition_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    condition_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    condition_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    condition_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    condition_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    condition_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    stop_reason: Mapped[Optional[str]] = mapped_column(String(20))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    condition_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    condition_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    condition_status_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    condition_status_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    condition_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConditionOccurrence.condition_concept_id"
    )
    condition_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConditionOccurrence.condition_source_concept_id"
    )
    condition_status_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConditionOccurrence.condition_status_concept_id"
    )
    condition_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConditionOccurrence.condition_type_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="ConditionOccurrence.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="ConditionOccurrence.provider_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="ConditionOccurrence.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="ConditionOccurrence.visit_occurrence_id"
    )


class DeviceExposure(Base):
    __tablename__ = "device_exposure"
    __table_args__ = {"schema": CDM_SCHEMA}

    device_exposure_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    device_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    device_exposure_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    device_exposure_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    device_exposure_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    device_exposure_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    device_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    unique_device_id: Mapped[Optional[str]] = mapped_column(String(50))
    quantity: Mapped[Optional[int]] = mapped_column(Integer)
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    device_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    device_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    device_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DeviceExposure.device_concept_id"
    )
    device_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DeviceExposure.device_source_concept_id"
    )
    device_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DeviceExposure.device_type_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="DeviceExposure.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="DeviceExposure.provider_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="DeviceExposure.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="DeviceExposure.visit_occurrence_id"
    )


class DrugExposure(Base):
    __tablename__ = "drug_exposure"
    __table_args__ = {"schema": CDM_SCHEMA}

    drug_exposure_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    drug_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID), index=True)
    drug_exposure_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    drug_exposure_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    drug_exposure_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    drug_exposure_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    verbatim_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    drug_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    stop_reason: Mapped[Optional[str]] = mapped_column(String(20))
    refills: Mapped[Optional[int]] = mapped_column(Integer)
    quantity: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    days_supply: Mapped[Optional[int]] = mapped_column(Integer)
    sig: Mapped[Optional[str]] = mapped_column(Text)
    route_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    lot_number: Mapped[Optional[str]] = mapped_column(String(50))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    drug_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    drug_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    route_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    dose_unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugExposure.drug_concept_id"
    )
    drug_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugExposure.drug_source_concept_id"
    )
    drug_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugExposure.drug_type_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="DrugExposure.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="DrugExposure.provider_id"
    )
    route_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugExposure.route_concept_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="DrugExposure.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="DrugExposure.visit_occurrence_id"
    )


class Measurement(Base):
    __tablename__ = "measurement"
    __table_args__ = {"schema": CDM_SCHEMA}

    measurement_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    measurement_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    measurement_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    measurement_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    measurement_time: Mapped[Optional[str]] = mapped_column(String(10))
    measurement_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    operator_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    value_as_number: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    unit_concept_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    range_low: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    range_high: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    measurement_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    measurement_source_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    value_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    measurement_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.measurement_concept_id"
    )
    measurement_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.measurement_source_concept_id"
    )
    measurement_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.measurement_type_concept_id"
    )
    operator_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.operator_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="Measurement.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="Measurement.provider_id"
    )
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.unit_concept_id"
    )
    value_as_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Measurement.value_as_concept_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="Measurement.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="Measurement.visit_occurrence_id"
    )


class Note(Base):
    __tablename__ = "note"
    __table_args__ = {"schema": CDM_SCHEMA}

    note_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    note_event_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    note_event_field_concept_id: Mapped[int] = mapped_column(Integer)
    note_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    note_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    note_type_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    note_class_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    note_title: Mapped[Optional[str]] = mapped_column(String(250))
    note_text: Mapped[str] = mapped_column(Text)
    encoding_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    language_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    note_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    encoding_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Note.encoding_concept_id"
    )
    language_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Note.language_concept_id"
    )
    note_class_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Note.note_class_concept_id"
    )
    note_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Note.note_type_concept_id"
    )
    person: Mapped["Person"] = relationship("Person", foreign_keys="Note.person_id")
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="Note.provider_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="Note.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="Note.visit_occurrence_id"
    )


class Observation(Base):
    __tablename__ = "observation"
    __table_args__ = {"schema": CDM_SCHEMA}

    observation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    observation_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    observation_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    observation_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    observation_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    value_as_number: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    value_as_string: Mapped[Optional[str]] = mapped_column(String(60))
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    value_as_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    qualifier_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    unit_concept_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    observation_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    observation_source_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    qualifier_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    observation_event_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    obs_event_field_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    obs_event_field_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.obs_event_field_concept_id"
    )
    observation_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.observation_concept_id"
    )
    observation_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.observation_source_concept_id"
    )
    observation_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.observation_type_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="Observation.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="Observation.provider_id"
    )
    qualifier_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.qualifier_concept_id"
    )
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.unit_concept_id"
    )
    value_as_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Observation.value_as_concept_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="Observation.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="Observation.visit_occurrence_id"
    )


class ProcedureOccurrence(Base):
    __tablename__ = "procedure_occurrence"
    __table_args__ = {"schema": CDM_SCHEMA}

    procedure_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    procedure_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    procedure_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    procedure_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    procedure_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    modifier_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    quantity: Mapped[Optional[int]] = mapped_column(Integer)
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID), index=True
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    procedure_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    procedure_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    modifier_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    modifier_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ProcedureOccurrence.modifier_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="ProcedureOccurrence.person_id"
    )
    procedure_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ProcedureOccurrence.procedure_concept_id"
    )
    procedure_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ProcedureOccurrence.procedure_source_concept_id"
    )
    procedure_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ProcedureOccurrence.procedure_type_concept_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="ProcedureOccurrence.provider_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="ProcedureOccurrence.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="ProcedureOccurrence.visit_occurrence_id"
    )


class SurveyConduct(Base):
    __tablename__ = "survey_conduct"
    __table_args__ = {"schema": CDM_SCHEMA}

    survey_conduct_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    survey_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    survey_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    survey_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    survey_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    survey_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    provider_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PROVIDER_ID))
    assisted_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    respondent_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    timing_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    collection_method_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    assisted_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    respondent_type_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    timing_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    collection_method_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    survey_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    survey_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    survey_source_identifier: Mapped[Optional[str]] = mapped_column(String(100))
    validated_survey_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    validated_survey_source_value: Mapped[Optional[str]] = mapped_column(String(100))
    survey_version_number: Mapped[Optional[str]] = mapped_column(String(20))
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID)
    )
    visit_detail_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_DETAIL_ID)
    )
    response_visit_occurrence_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_VISIT_OCCURRENCE_ID)
    )

    assisted_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.assisted_concept_id"
    )
    collection_method_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.collection_method_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="SurveyConduct.person_id"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", foreign_keys="SurveyConduct.provider_id"
    )
    respondent_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.respondent_type_concept_id"
    )
    response_visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="SurveyConduct.response_visit_occurrence_id"
    )
    survey_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.survey_concept_id"
    )
    survey_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.survey_source_concept_id"
    )
    timing_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.timing_concept_id"
    )
    validated_survey_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SurveyConduct.validated_survey_concept_id"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", foreign_keys="SurveyConduct.visit_detail_id"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", foreign_keys="SurveyConduct.visit_occurrence_id"
    )


class NoteNlp(Base):
    __tablename__ = "note_nlp"
    __table_args__ = {"schema": CDM_SCHEMA}

    note_nlp_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    note_id: Mapped[int] = mapped_column(ForeignKey("note.note_id"), index=True)
    section_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    snippet: Mapped[Optional[str]] = mapped_column(String(250))
    offset: Mapped[Optional[str]] = mapped_column(String(250))
    lexical_variant: Mapped[str] = mapped_column(String(250))
    note_nlp_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    nlp_system: Mapped[Optional[str]] = mapped_column(String(250))
    nlp_date: Mapped[datetime.date] = mapped_column(Date)
    nlp_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    term_exists: Mapped[Optional[str]] = mapped_column(String(1))
    term_temporal: Mapped[Optional[str]] = mapped_column(String(50))
    term_modifiers: Mapped[Optional[str]] = mapped_column(String(2000))
    note_nlp_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    note: Mapped["Note"] = relationship("Note", foreign_keys="NoteNlp.note_id")
    note_nlp_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="NoteNlp.note_nlp_concept_id"
    )
    note_nlp_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="NoteNlp.note_nlp_source_concept_id"
    )
    section_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="NoteNlp.section_concept_id"
    )


class ConditionEra(Base):
    __tablename__ = "condition_era"
    __table_args__ = {"schema": CDM_SCHEMA}

    condition_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    condition_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), index=True
    )
    condition_era_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    condition_era_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    condition_occurrence_count: Mapped[Optional[int]] = mapped_column(Integer)

    condition_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConditionEra.condition_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="ConditionEra.person_id"
    )


class DoseEra(Base):
    __tablename__ = "dose_era"
    __table_args__ = {"schema": CDM_SCHEMA}

    dose_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    drug_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID), index=True)
    unit_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    dose_value: Mapped[decimal.Decimal] = mapped_column(Numeric)
    dose_era_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    dose_era_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)

    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DoseEra.drug_concept_id"
    )
    person: Mapped["Person"] = relationship("Person", foreign_keys="DoseEra.person_id")
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DoseEra.unit_concept_id"
    )


class DrugEra(Base):
    __tablename__ = "drug_era"
    __table_args__ = {"schema": CDM_SCHEMA}

    drug_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    drug_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID), index=True)
    drug_era_start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    drug_era_end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime)
    drug_exposure_count: Mapped[Optional[int]] = mapped_column(Integer)
    gap_days: Mapped[Optional[int]] = mapped_column(Integer)

    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugEra.drug_concept_id"
    )
    person: Mapped["Person"] = relationship("Person", foreign_keys="DrugEra.person_id")


class Cost(Base):
    __tablename__ = "cost"
    __table_args__ = {"schema": CDM_SCHEMA}

    cost_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    cost_event_id: Mapped[int] = mapped_column(BigInteger)
    cost_event_field_concept_id: Mapped[int] = mapped_column(Integer)
    cost_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    cost_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    currency_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    cost: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    incurred_date: Mapped[datetime.date] = mapped_column(Date)
    billed_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    paid_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    revenue_code_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    drg_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    cost_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    cost_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    revenue_code_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    drg_source_value: Mapped[Optional[str]] = mapped_column(String(3))
    payer_plan_period_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("payer_plan_period.payer_plan_period_id")
    )

    cost_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.cost_concept_id"
    )
    cost_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.cost_source_concept_id"
    )
    cost_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.cost_type_concept_id"
    )
    currency_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.currency_concept_id"
    )
    drg_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.drg_concept_id"
    )
    payer_plan_period: Mapped["PayerPlanPeriod"] = relationship(
        "PayerPlanPeriod", foreign_keys="Cost.payer_plan_period_id"
    )
    person: Mapped["Person"] = relationship("Person", foreign_keys="Cost.person_id")
    revenue_code_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Cost.revenue_code_concept_id"
    )


class PayerPlanPeriod(Base):
    __tablename__ = "payer_plan_period"
    __table_args__ = {"schema": CDM_SCHEMA}

    payer_plan_period_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey(FK_PERSON_ID), index=True)
    contract_person_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_PERSON_ID))
    payer_plan_period_start_date: Mapped[datetime.date] = mapped_column(Date)
    payer_plan_period_end_date: Mapped[datetime.date] = mapped_column(Date)
    payer_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    plan_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    contract_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    sponsor_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    stop_reason_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    payer_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    payer_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    plan_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    plan_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    contract_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    contract_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    sponsor_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    sponsor_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    family_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    stop_reason_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    stop_reason_source_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )

    contract_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.contract_concept_id"
    )
    contract_person: Mapped["Person"] = relationship(
        "Person", foreign_keys="PayerPlanPeriod.contract_person_id"
    )
    contract_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.contract_source_concept_id"
    )
    payer_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.payer_concept_id"
    )
    payer_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.payer_source_concept_id"
    )
    person: Mapped["Person"] = relationship(
        "Person", foreign_keys="PayerPlanPeriod.person_id"
    )
    plan_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.plan_concept_id"
    )
    plan_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.plan_source_concept_id"
    )
    sponsor_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.sponsor_concept_id"
    )
    sponsor_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.sponsor_source_concept_id"
    )
    stop_reason_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.stop_reason_concept_id"
    )
    stop_reason_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="PayerPlanPeriod.stop_reason_source_concept_id"
    )


class CareSite(Base):
    __tablename__ = "care_site"
    __table_args__ = {"schema": CDM_SCHEMA}

    care_site_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    care_site_name: Mapped[Optional[str]] = mapped_column(String(255))
    place_of_service_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_LOCATION_ID))
    care_site_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    place_of_service_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    location: Mapped["Location"] = relationship(
        "Location", foreign_keys="CareSite.location_id"
    )
    place_of_service_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="CareSite.place_of_service_concept_id"
    )


class Location(Base):
    __tablename__ = "location"
    __table_args__ = {"schema": CDM_SCHEMA}

    location_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    address_1: Mapped[Optional[str]] = mapped_column(String(50))
    address_2: Mapped[Optional[str]] = mapped_column(String(50))
    city: Mapped[Optional[str]] = mapped_column(String(50))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    zip: Mapped[Optional[str]] = mapped_column(String(9))
    county: Mapped[Optional[str]] = mapped_column(String(20))
    country: Mapped[Optional[str]] = mapped_column(String(100))
    location_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    latitude: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    longitude: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    region_concept_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    region_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Location.region_concept_id"
    )


class LocationHistory(Base):
    __tablename__ = "location_history"
    __table_args__ = {"schema": CDM_SCHEMA}

    location_history_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey(FK_LOCATION_ID))
    relationship_type_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    domain_id: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[int] = mapped_column(BigInteger)
    start_date: Mapped[datetime.date] = mapped_column(Date)
    end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)

    location: Mapped["Location"] = relationship(
        "Location", foreign_keys="LocationHistory.location_id"
    )
    relationship_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="LocationHistory.relationship_type_concept_id"
    )


class Provider(Base):
    __tablename__ = "provider"
    __table_args__ = {"schema": CDM_SCHEMA}

    provider_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    provider_name: Mapped[Optional[str]] = mapped_column(String(255))
    npi: Mapped[Optional[str]] = mapped_column(String(20))
    dea: Mapped[Optional[str]] = mapped_column(String(20))
    specialty_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    care_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey(FK_CARE_SITE_ID))
    year_of_birth: Mapped[Optional[int]] = mapped_column(Integer)
    gender_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    provider_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    specialty_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    specialty_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))
    gender_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    care_site: Mapped["CareSite"] = relationship(
        "CareSite", foreign_keys="Provider.care_site_id"
    )
    gender_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Provider.gender_concept_id"
    )
    gender_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Provider.gender_source_concept_id"
    )
    specialty_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Provider.specialty_concept_id"
    )
    specialty_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Provider.specialty_source_concept_id"
    )


class CdmSource(Base):
    __tablename__ = "cdm_source"
    __table_args__ = {"schema": CDM_SCHEMA}

    cdm_source_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    cdm_source_abbreviation: Mapped[Optional[str]] = mapped_column(String(25))
    cdm_holder: Mapped[Optional[str]] = mapped_column(String(255))
    source_description: Mapped[Optional[str]] = mapped_column(Text)
    source_documentation_reference: Mapped[Optional[str]] = mapped_column(String(255))
    cdm_etl_reference: Mapped[Optional[str]] = mapped_column(String(255))
    source_release_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    cdm_release_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    cdm_version: Mapped[Optional[str]] = mapped_column(String(10))
    vocabulary_version: Mapped[Optional[str]] = mapped_column(String(20))


class Metadata(Base):
    __tablename__ = "metadata"
    __table_args__ = {"schema": CDM_SCHEMA}

    metadata_concept_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, index=True
    )
    metadata_type_concept_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(250), primary_key=True)
    value_as_string: Mapped[Optional[str]] = mapped_column(Text)
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(Integer)
    metadata_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    metadata_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)


class Concept(Base):
    __tablename__ = "concept"
    __table_args__ = (
        CheckConstraint(
            "(COALESCE(invalid_reason,'D') in ('D','U'))", name="chk_c_invalid_reason"
        ),
        CheckConstraint(
            "(COALESCE(standard_concept,'C') in ('C','S'))",
            name="chk_c_standard_concept",
        ),
        CheckConstraint("(concept_code <> '')", name="chk_c_concept_code"),
        CheckConstraint("(concept_name <> '')", name="chk_c_concept_name"),
        {"schema": VOCAB_SCHEMA},
    )

    concept_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    concept_name: Mapped[str] = mapped_column(String(255))
    domain_id: Mapped[str] = mapped_column(ForeignKey(FK_DOMAIN_ID), index=True)
    vocabulary_id: Mapped[str] = mapped_column(ForeignKey(FK_VOCABULARY_ID), index=True)
    concept_class_id: Mapped[str] = mapped_column(
        ForeignKey(FK_CONCEPT_CLASS_ID), index=True
    )
    standard_concept: Mapped[Optional[str]] = mapped_column(String(1))
    concept_code: Mapped[str] = mapped_column(String(50), index=True)
    valid_start_date: Mapped[datetime.date] = mapped_column(Date)
    valid_end_date: Mapped[datetime.date] = mapped_column(Date)
    invalid_reason: Mapped[Optional[str]] = mapped_column(String(1))

    concept_class: Mapped["ConceptClass"] = relationship(
        "ConceptClass", foreign_keys="Concept.concept_class_id"
    )
    domain: Mapped["Domain"] = relationship("Domain", foreign_keys="Concept.domain_id")
    vocabulary: Mapped["Vocabulary"] = relationship(
        "Vocabulary", foreign_keys="Concept.vocabulary_id"
    )


class ConceptAncestor(Base):
    __tablename__ = "concept_ancestor"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    ancestor_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    descendant_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    min_levels_of_separation: Mapped[int] = mapped_column(Integer)
    max_levels_of_separation: Mapped[int] = mapped_column(Integer)

    ancestor_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptAncestor.ancestor_concept_id"
    )
    descendant_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptAncestor.descendant_concept_id"
    )


class ConceptClass(Base):
    __tablename__ = "concept_class"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    concept_class_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    concept_class_name: Mapped[str] = mapped_column(String(255))
    concept_class_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    concept_class_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptClass.concept_class_concept_id"
    )


class ConceptRelationship(Base):
    __tablename__ = "concept_relationship"
    __table_args__ = (
        CheckConstraint(
            "(COALESCE(invalid_reason,'D')='D')", name="chk_cr_invalid_reason"
        ),
        {"schema": VOCAB_SCHEMA},
    )

    concept_id_1: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    concept_id_2: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    relationship_id: Mapped[str] = mapped_column(
        ForeignKey("relationship.relationship_id"),
        primary_key=True,
        index=True,
    )
    valid_start_date: Mapped[datetime.date] = mapped_column(Date)
    valid_end_date: Mapped[datetime.date] = mapped_column(Date)
    invalid_reason: Mapped[Optional[str]] = mapped_column(String(1))

    concept_1: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptRelationship.concept_id_1"
    )
    concept_2: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptRelationship.concept_id_2"
    )
    relationship: Mapped["Relationship"] = relationship(
        "Relationship", foreign_keys="ConceptRelationship.relationship_id"
    )


class ConceptSynonym(Base):
    __tablename__ = "concept_synonym"
    __table_args__ = (
        # UniqueConstraint uq_concept_synonym is not added, because
        # there is already a PK on the same columns
        CheckConstraint(
            "(concept_synonym_name <> '')", name="chk_csyn_concept_synonym_name"
        ),
        {"schema": VOCAB_SCHEMA},
    )

    concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    concept_synonym_name: Mapped[str] = mapped_column(String(1000), primary_key=True)
    language_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True
    )

    concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptSynonym.concept_id"
    )
    language_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="ConceptSynonym.language_concept_id"
    )


class Domain(Base):
    __tablename__ = "domain"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    domain_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    domain_name: Mapped[str] = mapped_column(String(255))
    domain_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    domain_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Domain.domain_concept_id"
    )


class DrugStrength(Base):
    __tablename__ = "drug_strength"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    drug_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    ingredient_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    amount_value: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    amount_unit_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    numerator_value: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    numerator_unit_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    denominator_value: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    denominator_unit_concept_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(FK_CONCEPT_ID)
    )
    box_size: Mapped[Optional[int]] = mapped_column(Integer)
    valid_start_date: Mapped[datetime.date] = mapped_column(Date)
    valid_end_date: Mapped[datetime.date] = mapped_column(Date)
    invalid_reason: Mapped[Optional[str]] = mapped_column(String(1))

    amount_unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugStrength.amount_unit_concept_id"
    )
    denominator_unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugStrength.denominator_unit_concept_id"
    )
    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugStrength.drug_concept_id"
    )
    ingredient_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugStrength.ingredient_concept_id"
    )
    numerator_unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="DrugStrength.numerator_unit_concept_id"
    )


class Relationship(Base):
    __tablename__ = "relationship"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    relationship_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    relationship_name: Mapped[str] = mapped_column(String(255))
    is_hierarchical: Mapped[str] = mapped_column(String(1))
    defines_ancestry: Mapped[str] = mapped_column(String(1))
    reverse_relationship_id: Mapped[str] = mapped_column(
        ForeignKey("relationship.relationship_id")
    )
    relationship_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    relationship_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Relationship.relationship_concept_id"
    )
    reverse_relationship: Mapped["Relationship"] = relationship(
        "Relationship", foreign_keys="Relationship.reverse_relationship_id"
    )


class Vocabulary(Base):
    __tablename__ = "vocabulary"
    __table_args__ = {"schema": VOCAB_SCHEMA}

    vocabulary_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    vocabulary_name: Mapped[str] = mapped_column(String(255))
    vocabulary_reference: Mapped[str] = mapped_column(String(255))
    vocabulary_version: Mapped[Optional[str]] = mapped_column(String(255))
    vocabulary_concept_id: Mapped[int] = mapped_column(ForeignKey(FK_CONCEPT_ID))

    vocabulary_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="Vocabulary.vocabulary_concept_id"
    )


class SourceToConceptMap(Base):
    __tablename__ = "source_to_concept_map"
    __table_args__ = {"schema": CDM_SCHEMA}

    source_code: Mapped[str] = mapped_column(String(1000), primary_key=True, index=True)
    source_concept_id: Mapped[int] = mapped_column(Integer)
    source_vocabulary_id: Mapped[str] = mapped_column(
        ForeignKey(FK_VOCABULARY_ID), primary_key=True, index=True
    )
    source_code_description: Mapped[Optional[str]] = mapped_column(String(255))
    target_concept_id: Mapped[int] = mapped_column(
        ForeignKey(FK_CONCEPT_ID), primary_key=True, index=True
    )
    target_vocabulary_id: Mapped[str] = mapped_column(
        ForeignKey(FK_VOCABULARY_ID), index=True
    )
    valid_start_date: Mapped[datetime.date] = mapped_column(Date)
    valid_end_date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    invalid_reason: Mapped[Optional[str]] = mapped_column(String(1))

    source_vocabulary: Mapped["Vocabulary"] = relationship(
        "Vocabulary", foreign_keys="SourceToConceptMap.source_vocabulary_id"
    )
    target_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys="SourceToConceptMap.target_concept_id"
    )
    target_vocabulary: Mapped["Vocabulary"] = relationship(
        "Vocabulary", foreign_keys="SourceToConceptMap.target_vocabulary_id"
    )
