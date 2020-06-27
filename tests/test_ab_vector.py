import pytest

@pytest.fixture
def pyomop_fixture():
    from src.pyomop import CdmEngineFactory
    cdm = CdmEngineFactory(

        )
    return cdm

@pytest.fixture
def metadata_fixture():
    from src.pyomop import metadata
    return metadata

def test_search_person(pyomop_fixture, metadata_fixture, capsys):
    import datetime
    from sqlalchemy.sql import select
    engine = pyomop_fixture.engine
    from src.pyomop import Person
    session =  pyomop_fixture.session
    _ids = []
    session.add(Person(
        gender_concept_id=100,
        year_of_birth=2000,
        race_concept_id=200,
        ethnicity_concept_id=300
    ))
    session.commit()
    for p in session.query(Person).order_by(Person.person_id)[0:5]:
        _ids.append(p.person_id)
    assert _ids

def test_add_cohort(pyomop_fixture, metadata_fixture, capsys):
    import datetime
    from sqlalchemy.sql import select
    engine = pyomop_fixture.engine
    from src.pyomop import Cohort
    session =  pyomop_fixture.session
    session.add(Cohort(cohort_definition_id=2, subject_id=100, 
        cohort_end_date=datetime.datetime.now(), 
        cohort_start_date=datetime.datetime.now()))
    session.add(Cohort(cohort_definition_id=3, subject_id=100, 
        cohort_end_date=datetime.datetime.now(), 
        cohort_start_date=datetime.datetime.now()))
    session.commit()
    _ids = []
    for p in session.query(Cohort).order_by(Cohort._id)[0:5]:
        _ids.append(p._id)
    assert _ids