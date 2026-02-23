"""pyomop.migrate – database-to-OMOP CDM ETL tools.

This sub-package provides :class:`~pyomop.migrate.pyomop_migrate.CdmGenericLoader`
for loading data from any SQLAlchemy-compatible source database into an OMOP CDM
target database, along with schema-extraction utilities and the ``pyomop-migrate``
command-line entry point.
"""

from .pyomop_migrate import (
    CdmGenericLoader,
    build_source_url,
    create_source_engine,
    extract_schema_to_markdown,
    load_mapping,
)

__all__ = [
    "CdmGenericLoader",
    "build_source_url",
    "create_source_engine",
    "extract_schema_to_markdown",
    "load_mapping",
]
