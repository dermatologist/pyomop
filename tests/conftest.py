# -*- coding: utf-8 -*-
"""
Dummy conftest.py for pyomop.

If you don't know what this is for, just leave it empty.
Read more about conftest.py under:
https://pytest.org/latest/plugins.html
"""

import pytest
import asyncio
import nest_asyncio

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()


@pytest.fixture(scope="function")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory

    cdm = CdmEngineFactory()
    return cdm


@pytest.fixture
def cdm6_metadata_fixture():
    from src.pyomop.cdm6 import Base

    return Base.metadata


@pytest.fixture
def cdm54_metadata_fixture():
    from src.pyomop.cdm54 import Base

    return Base.metadata


@pytest.fixture
def vector_fixture():
    from src.pyomop import CdmVector

    return CdmVector()
