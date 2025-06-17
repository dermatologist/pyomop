from omop_cdm.regular import cdm54
from omop_cdm.regular import cdm531
from omop_cdm.regular import cdm600


def class_factory(
    class_name: str,
    cdm_version: str = "cdm54",
):
    """
    Factory function to create a class based on the CDM version and class name.

    Args:
        cdm_version (str): The version of the OMOP CDM (e.g., 'cdm54', 'cdm531', 'cdm600').
        class_name (str): The name of the class to create (e.g., 'Person', 'ConditionOccurrence').

    Returns:
        type: The class corresponding to the specified CDM version and class name.
    """
    if cdm_version == "cdm54":
        return getattr(cdm54, class_name)
    elif cdm_version == "cdm531":
        return getattr(cdm531, class_name)
    elif cdm_version == "cdm600":
        return getattr(cdm600, class_name)
    else:
        raise ValueError(f"Unsupported CDM version: {cdm_version}")
