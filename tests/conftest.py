# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for pyomop.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

import asyncio
import pytest

@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory
    cdm = CdmEngineFactory()
    return cdm

@pytest.fixture
def metadata_fixture():
    from src.pyomop import metadata
    return metadata


@pytest.fixture
def vector_fixture():
    from src.pyomop import CdmVector
    return CdmVector()