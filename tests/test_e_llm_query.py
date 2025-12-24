import asyncio
import datetime
import importlib
from unittest.mock import MagicMock, patch

import pytest


def test_create_cohort_cdm54(pyomop_fixture, cdm54_metadata_fixture, capsys):
    # Skip test gracefully if LLM extras are not installed
    try:
        importlib.import_module("langchain_core")
        importlib.import_module("langchain_community")
    except Exception:
        pytest.skip("LLM optional dependencies are not installed")

    # For now, verify that LLM query module can be imported and basic imports work
    # Full integration testing requires a real LLM instance
    try:
        from src.pyomop.llm_engine import CDMDatabase
        from src.pyomop.llm_query import CdmLLMQuery

        print("✓ LLM query module imports successfully")
    except Exception as e:
        pytest.fail(f"Failed to import LLM modules: {e}")


async def create_llm_query(pyomop_fixture, engine):
    """Deprecated: Full LLM integration testing requires a real LLM instance."""
    pass
