import asyncio
import os

import pytest


@staticmethod
def test_create_tables_cdm54(pyomop_fixture, cdm54_metadata_fixture, capsys):
    engine = pyomop_fixture.engine
    # Delete cdm.sqlite if it exists
    if os.path.exists("cdm.sqlite"):
        os.remove("cdm.sqlite")
    # create tables
    asyncio.run(pyomop_fixture.init_models(cdm54_metadata_fixture))


def test_create_vocab_cdm54(pyomop_fixture, capsys):
    engine = pyomop_fixture.engine
    create_vocab_cdm54(pyomop_fixture, engine)


def create_vocab_cdm54(pyomop_fixture, engine):
    from src.pyomop import CdmVocabulary

    vocab = CdmVocabulary(pyomop_fixture, version="cdm54")
    asyncio.run(vocab.create_vocab("tests", 10))
    print("Done")
