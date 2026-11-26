import asyncio

from sqlalchemy import text


def test_create_vocab_parses_yyyymmdd_dates(tmp_path):
    """Athena writes valid_start_date/valid_end_date as YYYYMMDD integers."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.vocabulary import CdmVocabulary

    async def run():
        cdm = CdmEngineFactory(db="sqlite", name=str(tmp_path / "vocab_dates.sqlite"))
        _ = cdm.engine
        try:
            await cdm.init_models(Base.metadata)
            await CdmVocabulary(cdm).create_vocab("tests")
            dates = {}
            async with cdm.session() as session:
                for table in ("concept", "concept_relationship", "drug_strength"):
                    result = await session.execute(
                        text(f"SELECT DISTINCT valid_start_date, valid_end_date FROM {table}")  # noqa: S608
                    )
                    dates[table] = set(result.all())
            return dates
        finally:
            await cdm.dispose()

    dates = asyncio.run(run())

    # Values from tests/CONCEPT.csv, CONCEPT_RELATIONSHIP.csv and DRUG_STRENGTH.csv
    assert dates["concept"] == {("1970-01-01", "2099-12-31")}
    assert dates["concept_relationship"] == {("2015-08-17", "2099-12-31"), ("2018-04-20", "2099-12-31")}
    assert dates["drug_strength"] == {("1970-01-01", "2099-12-31")}
