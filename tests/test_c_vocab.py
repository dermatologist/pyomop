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

def test_vocab(pyomop_fixture, metadata_fixture, capsys):
    from src.pyomop import CdmVocabulary
    # vocab = CdmVocabulary(pyomop_fixture)
    for x in pyomop_fixture.base:
        print(x)
    # setters are called like this
    # vocab.concept_id = 2 # 'C0020538' Hypertension
    # print(vocab.concept_name)


