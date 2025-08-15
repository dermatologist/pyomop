import logging
import sys

_logger = logging.getLogger(__name__)
from .engine_factory import CdmEngineFactory
from .loader import CdmCsvLoader
from .vector import CdmVector
from .vocabulary import CdmVocabulary

try:
    from .llm_engine import CDMDatabase
    from .llm_query import CdmLLMQuery
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
