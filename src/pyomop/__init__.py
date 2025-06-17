import sys
import logging

_logger = logging.getLogger(__name__)
from .engine_factory import CdmEngineFactory
from .vocabulary import CdmVocabulary
from .vector import CdmVector


try:
    from .llm_query import CdmLLMQuery
    from .llm_engine import CDMDatabase
except ImportError:
    _logger.warning(
        "WARNING: LLM is not installed. Please install extras with pip install pyomop[llm] to use LLM functions."
    )

if sys.version_info[:2] >= (3, 8):
    # TODO: Import directly (no need for conditional) when `python_requires = >= 3.8`
    from importlib.metadata import PackageNotFoundError, version  # pragma: no cover
else:
    from importlib_metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
