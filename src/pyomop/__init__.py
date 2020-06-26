# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

from .engine_factory import CdmEngineFactory
from .cdm6_tables import metadata
from .vocabulary import CdmVocabulary

from .cdm6_tables import AttributeDefinition
from .cdm6_tables import CareSite
from .cdm6_tables import CdmSource
from .cdm6_tables import Cohort
from .cdm6_tables import CohortAttribute
from .cdm6_tables import CohortDefinition
from .cdm6_tables import Concept
from .cdm6_tables import ConceptAncestor
from .cdm6_tables import ConceptClass
from .cdm6_tables import ConceptRelationship
from .cdm6_tables import ConceptSynonym
from .cdm6_tables import ConditionEra
from .cdm6_tables import ConditionOccurrence
from .cdm6_tables import Death
from .cdm6_tables import DeviceCost

from .cdm6_tables import DeviceExposure
from .cdm6_tables import Domain
from .cdm6_tables import DoseEra
from .cdm6_tables import DrugCost
from .cdm6_tables import DrugEra

from .cdm6_tables import DrugExposure
from .cdm6_tables import DrugStrength
from .cdm6_tables import FactRelationship
from .cdm6_tables import Location
from .cdm6_tables import Measurement

from .cdm6_tables import Note
from .cdm6_tables import Observation
from .cdm6_tables import ObservationPeriod
from .cdm6_tables import PayerPlanPeriod
from .cdm6_tables import Person

from .cdm6_tables import ProcedureCost
from .cdm6_tables import ProcedureOccurrence
from .cdm6_tables import Provider
from .cdm6_tables import Relationship
from .cdm6_tables import SourceToConceptMap

from .cdm6_tables import Speciman
from .cdm6_tables import VisitCost
from .cdm6_tables import VisitOccurrence
from .cdm6_tables import Vocabulary

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = 'unknown'
finally:
    del get_distribution, DistributionNotFound
