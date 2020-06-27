import pytest

@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory
    cdm = CdmEngineFactory()
    return cdm

@pytest.fixture
def vector_fixture():
    from src.pyomop import CdmVector
    return CdmVector()

def test_create_db(pyomop_fixture, vector_fixture, capsys):
    import datetime
    from sqlalchemy.sql import select
    engine = pyomop_fixture.engine
    from src.pyomop import Cohort
    # Cohort = pyomop_fixture.base.cohort
    session =  pyomop_fixture.session
    session.add(Cohort(cohort_definition_id=2, subject_id=100, 
            cohort_end_date=datetime.datetime.now(), 
            cohort_start_date=datetime.datetime.now()))
    session.commit()

    s = select([Cohort])
    result = session.execute(s)
    vector_fixture.result = result
    print(vector_fixture.df.head(2))
    result.close()
    assert row['subject_id'] == 100