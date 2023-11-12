import asyncio
import pytest

def test_create_vocab(pyomop_fixture, metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    create_vocab(pyomop_fixture, engine)

def create_vocab(pyomop_fixture,engine):
    from src.pyomop import CdmVocabulary
    vocab = CdmVocabulary(pyomop_fixture)
    vocab.create_vocab('tests', 10)
    print("Done")







# import pytest
# import os

# @pytest.fixture
# def pyomop_fixture():
#     from src.pyomop import CdmEngineFactory
#     cdm = CdmEngineFactory()
#     return cdm

# @pytest.fixture
# def metadata_fixture():
#     from src.pyomop import metadata
#     return metadata

# def test_create_vocab(pyomop_fixture, metadata_fixture, capsys):
#     from src.pyomop import CdmVocabulary
#     vocab = CdmVocabulary(pyomop_fixture)
#     vocab.create_vocab('tests', 10)
#     print("Done")
