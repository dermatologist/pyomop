"""Backward-compatibility shim for pyomop.generic_loader.

.. deprecated::
    This module has been moved to :mod:`pyomop.migrate.pyomop_migrate`.
    Import directly from there or from the :mod:`pyomop.migrate` sub-package::

        from pyomop.migrate import CdmGenericLoader
        from pyomop.migrate.pyomop_migrate import build_source_url, extract_schema_to_markdown
"""

import warnings

warnings.warn(
    "pyomop.generic_loader is deprecated. Use pyomop.migrate or "
    "pyomop.migrate.pyomop_migrate instead.",
    DeprecationWarning,
    stacklevel=2,
)

from pyomop.migrate.pyomop_migrate import (  # noqa: E402, F401
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
