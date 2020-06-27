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

def test_create_db(pyomop_fixture, metadata_fixture, capsys):
    import datetime
    from sqlalchemy.sql import select
    engine = pyomop_fixture.engine
    metadata_fixture.create_all(engine)
    from src.pyomop import Cohort
    # Cohort = pyomop_fixture.base.cohort
    session =  pyomop_fixture.session
    session.add(Cohort(cohort_definition_id=2, subject_id=100, 
            cohort_end_date=datetime.datetime.now(), 
            cohort_start_date=datetime.datetime.now()))
    session.commit()

    s = select([Cohort])
    result = session.execute(s)
    for row in result:
        print(row)
    result.close()
    assert row['subject_id'] == 100