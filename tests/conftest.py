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
