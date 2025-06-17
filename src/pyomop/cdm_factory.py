from omop_cdm.dynamic import cdm54
from omop_cdm.dynamic import cdm531
from omop_cdm.dynamic import cdm600

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def class_factory(
    class_name: str,
    cdm_version: str = "Cdm54",
):
    """
    Factory function to create a class based on the CDM version and class name.

    Args:
        cdm_version (str): The version of the OMOP CDM (e.g., 'cdm54', 'cdm531', 'cdm600').
        class_name (str): The name of the class to create (e.g., 'Person', 'ConditionOccurrence').

    Returns:
        type: The class corresponding to the specified CDM version and class name.
    """
    if cdm_version == "Cdm54":
        name = f"Base{class_name}Cdm54"
        Klass = getattr(cdm54, name)
        new_class = type(
            class_name,
            (Klass, Base),
            {"__tablename__": class_name.lower(), "__table_args__": {"schema": None}},
        )
        return new_class
    elif cdm_version == "Cdm531":
        return getattr(cdm531, class_name)
    elif cdm_version == "Cdm600":
        return getattr(cdm600, class_name)
    else:
        raise ValueError(f"Unsupported CDM version: {cdm_version}")
