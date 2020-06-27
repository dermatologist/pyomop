import pytest

@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory
    cdm = CdmEngineFactory()
    return cdm

def test_vocab(pyomop_fixture, capsys):
    from src.pyomop import CdmVocabulary
    vocab = CdmVocabulary(pyomop_fixture)
    # for x in pyomop_fixture.base:
    #     print(x)
    # setters are called like this
    vocab.concept_id = 45956935 # 'C0020538' Hypertension
    assert(vocab.concept_name == 'Sibutramine hydrochloride')


