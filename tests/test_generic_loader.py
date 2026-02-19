"""Unit tests for the CdmGenericLoader (generic database-to-OMOP loader)."""

import asyncio
import json
from pathlib import Path

from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select, func


def _make_source_patients_ddl():
    """Return minimal CREATE TABLE SQL for a source 'patients' table."""
    return (
        "CREATE TABLE IF NOT EXISTS patients ("
        "  id INTEGER PRIMARY KEY,"
        "  patient_key TEXT,"
        "  gender TEXT,"
        "  date_of_birth TEXT,"
        "  race TEXT,"
        "  ethnicity TEXT"
        ")"
    )


def _make_source_encounters_ddl():
    return (
        "CREATE TABLE IF NOT EXISTS encounters ("
        "  id INTEGER PRIMARY KEY,"
        "  patient_id INTEGER,"
        "  start_date TEXT,"
        "  end_date TEXT,"
        "  encounter_type TEXT"
        ")"
    )


def _make_source_diagnoses_ddl():
    return (
        "CREATE TABLE IF NOT EXISTS diagnoses ("
        "  id INTEGER PRIMARY KEY,"
        "  patient_id INTEGER,"
        "  diagnosis_date TEXT,"
        "  icd_code TEXT"
        ")"
    )


def _minimal_mapping(tmp_path: Path) -> str:
    """Write a minimal mapping JSON and return its path as a string."""
    mapping = {
        "tables": [
            {
                "source_table": "patients",
                "name": "person",
                "columns": {
                    "person_id": "id",
                    "gender_concept_id": {"const": 0},
                    "gender_source_value": "gender",
                    "birth_datetime": "date_of_birth",
                    "year_of_birth": {"const": 0},
                    "race_concept_id": {"const": 0},
                    "race_source_value": "race",
                    "ethnicity_concept_id": {"const": 0},
                    "ethnicity_source_value": "ethnicity",
                    "person_source_value": "patient_key",
                },
            }
        ]
    }
    p = tmp_path / "test_mapping.json"
    p.write_text(json.dumps(mapping))
    return str(p)


def _mapping_with_encounters(tmp_path: Path) -> str:
    """Mapping for patients + encounters."""
    mapping = {
        "tables": [
            {
                "source_table": "patients",
                "name": "person",
                "columns": {
                    "person_id": "id",
                    "gender_concept_id": {"const": 0},
                    "gender_source_value": "gender",
                    "birth_datetime": "date_of_birth",
                    "year_of_birth": {"const": 0},
                    "race_concept_id": {"const": 0},
                    "ethnicity_concept_id": {"const": 0},
                    "person_source_value": "patient_key",
                },
            },
            {
                "source_table": "encounters",
                "name": "visit_occurrence",
                "columns": {
                    "person_id": "patient_id",
                    "visit_start_date": "start_date",
                    "visit_start_datetime": "start_date",
                    "visit_end_date": "end_date",
                    "visit_end_datetime": "end_date",
                    "visit_concept_id": {"const": 0},
                    "visit_type_concept_id": {"const": 0},
                    "visit_source_value": "encounter_type",
                    "visit_source_concept_id": {"const": 0},
                },
            },
        ]
    }
    p = tmp_path / "test_mapping_encounters.json"
    p.write_text(json.dumps(mapping))
    return str(p)


def _mapping_with_filter(tmp_path: Path) -> str:
    """Mapping that filters patients by gender."""
    mapping = {
        "tables": [
            {
                "source_table": "patients",
                "name": "person",
                "filters": [{"column": "gender", "equals": "female"}],
                "columns": {
                    "person_id": "id",
                    "gender_concept_id": {"const": 0},
                    "gender_source_value": "gender",
                    "year_of_birth": {"const": 0},
                    "race_concept_id": {"const": 0},
                    "ethnicity_concept_id": {"const": 0},
                    "person_source_value": "patient_key",
                },
            }
        ]
    }
    p = tmp_path / "test_mapping_filter.json"
    p.write_text(json.dumps(mapping))
    return str(p)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_generic_loader_loads_persons(tmp_path):
    """Persons from the source database are loaded into the target OMOP person table."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        # --- Source DB ---
        src_path = tmp_path / "source.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'pk-001', 'male', '1985-03-15', 'white', 'not hispanic'),"
                    "        (2, 'pk-002', 'female', '1992-07-22', 'black', 'not hispanic')"
                )
            )

        # --- Target OMOP DB ---
        tgt_path = tmp_path / "target.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        mapping_path = _minimal_mapping(tmp_path)
        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(mapping_path, batch_size=10)

        # --- Verify ---
        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            count = (
                await session.execute(select(func.count()).select_from(person_tbl))
            ).scalar_one()
            assert count == 2

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_gender_concept_id_set(tmp_path):
    """gender_concept_id is set correctly from gender_source_value."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_g.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'p1', 'male', '1980-01-01', '', ''),"
                    "        (2, 'p2', 'female', '1990-06-15', '', '')"
                )
            )

        tgt_path = tmp_path / "target_g.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(_minimal_mapping(tmp_path), batch_size=100)

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            rows = (await session.execute(select(person_tbl))).fetchall()
            gcids = {row.person_id: row.gender_concept_id for row in rows}
            assert gcids[1] == 8507  # male
            assert gcids[2] == 8532  # female

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_birth_fields_backfilled(tmp_path):
    """year_of_birth / month_of_birth / day_of_birth are backfilled from birth_datetime."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_b.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'pk-b1', 'male', '1975-11-30', '', '')"
                )
            )

        tgt_path = tmp_path / "target_b.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(_minimal_mapping(tmp_path), batch_size=100)

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            row = (await session.execute(select(person_tbl).where(person_tbl.c.person_id == 1))).first()
            assert row is not None
            assert row.year_of_birth == 1975
            assert row.month_of_birth == 11
            assert row.day_of_birth == 30

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_filter_applied(tmp_path):
    """Row-level filters on source table are respected."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_f.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'f1', 'male', '1980-01-01', '', ''),"
                    "        (2, 'f2', 'female', '1990-06-15', '', ''),"
                    "        (3, 'f3', 'female', '1995-12-31', '', '')"
                )
            )

        tgt_path = tmp_path / "target_f.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(_mapping_with_filter(tmp_path), batch_size=100)

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            count = (
                await session.execute(select(func.count()).select_from(person_tbl))
            ).scalar_one()
            # Only female rows (2 of 3) should be loaded
            assert count == 2

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_missing_source_table_skipped(tmp_path):
    """A mapping entry referencing a non-existent source table is skipped gracefully."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_skip.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'skip1', 'male', '1970-05-20', '', '')"
                )
            )

        tgt_path = tmp_path / "target_skip.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        # Mapping references 'nonexistent_table' which does not exist in source
        bad_mapping = {
            "tables": [
                {
                    "source_table": "nonexistent_table",
                    "name": "person",
                    "columns": {
                        "person_id": "id",
                        "gender_concept_id": {"const": 0},
                        "year_of_birth": {"const": 0},
                        "race_concept_id": {"const": 0},
                        "ethnicity_concept_id": {"const": 0},
                    },
                }
            ]
        }
        bad_mapping_path = tmp_path / "bad_mapping.json"
        bad_mapping_path.write_text(json.dumps(bad_mapping))

        loader = CdmGenericLoader(src_engine, cdm)
        # Should not raise; missing table is warned and skipped
        await loader.load(str(bad_mapping_path))

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            count = (
                await session.execute(select(func.count()).select_from(person_tbl))
            ).scalar_one()
            assert count == 0  # nothing loaded

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_multi_table_mapping(tmp_path):
    """Multiple source tables can be mapped to multiple OMOP CDM tables."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_multi.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(text(_make_source_encounters_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'multi1', 'female', '1988-04-10', '', '')"
                )
            )
            await conn.execute(
                text(
                    "INSERT INTO encounters (id, patient_id, start_date, end_date, encounter_type)"
                    " VALUES (1, 1, '2023-01-01', '2023-01-02', 'outpatient'),"
                    "        (2, 1, '2023-03-15', '2023-03-15', 'lab')"
                )
            )

        tgt_path = tmp_path / "target_multi.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(_mapping_with_encounters(tmp_path), batch_size=100)

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)

            person_tbl = getattr(BaseMap.classes, "person").__table__
            visit_tbl = getattr(BaseMap.classes, "visit_occurrence").__table__

            person_count = (
                await session.execute(select(func.count()).select_from(person_tbl))
            ).scalar_one()
            visit_count = (
                await session.execute(select(func.count()).select_from(visit_tbl))
            ).scalar_one()

            assert person_count == 1
            assert visit_count == 2

        await src_engine.dispose()

    asyncio.run(run())


def test_generic_loader_batch_size_respected(tmp_path):
    """All rows are inserted correctly regardless of batch_size."""
    from src.pyomop import CdmEngineFactory
    from src.pyomop.cdm54 import Base
    from src.pyomop.generic_loader import CdmGenericLoader

    async def run():
        src_path = tmp_path / "source_batch.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            values = ", ".join(
                f"({i}, 'key-{i}', 'male', '1970-01-01', '', '')" for i in range(1, 21)
            )
            await conn.execute(
                text(f"INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity) VALUES {values}")
            )

        tgt_path = tmp_path / "target_batch.sqlite"
        cdm = CdmEngineFactory(db="sqlite", name=str(tgt_path))
        _ = cdm.engine
        await cdm.init_models(Base.metadata)

        # Use a very small batch_size to exercise batching logic
        loader = CdmGenericLoader(src_engine, cdm)
        await loader.load(_minimal_mapping(tmp_path), batch_size=3)

        async_session = cdm.session
        async with async_session() as session:
            conn = await session.connection()
            BaseMap = automap_base()

            def _prep(sync_conn):
                BaseMap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prep)
            person_tbl = getattr(BaseMap.classes, "person").__table__

            count = (
                await session.execute(select(func.count()).select_from(person_tbl))
            ).scalar_one()
            assert count == 20

        await src_engine.dispose()

    asyncio.run(run())


def test_create_source_engine():
    """create_source_engine returns a usable async engine."""
    from src.pyomop.generic_loader import create_source_engine

    engine = create_source_engine("sqlite+aiosqlite:///:memory:")
    assert engine is not None
    # The engine should have a dialect
    assert engine.dialect is not None


def test_load_mapping(tmp_path):
    """load_mapping parses a valid JSON mapping file."""
    from src.pyomop.generic_loader import load_mapping

    mapping_data = {"tables": [], "concept": []}
    p = tmp_path / "sample.json"
    p.write_text(json.dumps(mapping_data))

    result = load_mapping(str(p))
    assert result == mapping_data


def test_generic_loader_example_mapping_is_valid():
    """The bundled example mapping file is valid JSON with the expected structure."""
    from src.pyomop.generic_loader import load_mapping

    example_path = Path("src/pyomop/mapping.generic.example.json")
    mapping = load_mapping(str(example_path))

    assert "tables" in mapping
    assert len(mapping["tables"]) > 0
    for tbl in mapping["tables"]:
        assert "source_table" in tbl
        assert "name" in tbl
        assert "columns" in tbl
