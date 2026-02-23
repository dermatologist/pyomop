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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import CdmGenericLoader

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
    from src.pyomop.migrate.pyomop_migrate import create_source_engine

    engine = create_source_engine("sqlite+aiosqlite:///:memory:")
    assert engine is not None
    # The engine should have a dialect
    assert engine.dialect is not None


def test_load_mapping(tmp_path):
    """load_mapping parses a valid JSON mapping file."""
    from src.pyomop.migrate.pyomop_migrate import load_mapping

    mapping_data = {"tables": [], "concept": []}
    p = tmp_path / "sample.json"
    p.write_text(json.dumps(mapping_data))

    result = load_mapping(str(p))
    assert result == mapping_data


def test_generic_loader_example_mapping_is_valid():
    """The bundled example mapping file is valid JSON with the expected structure."""
    from src.pyomop.migrate.pyomop_migrate import load_mapping

    example_path = Path("src/pyomop/mapping.generic.example.json")
    mapping = load_mapping(str(example_path))

    assert "tables" in mapping
    assert len(mapping["tables"]) > 0
    for tbl in mapping["tables"]:
        assert "source_table" in tbl
        assert "name" in tbl
        assert "columns" in tbl


def test_migrate_cli_option(tmp_path):
    """The --migrate CLI option runs CdmGenericLoader end-to-end."""
    import asyncio
    from click.testing import CliRunner
    from sqlalchemy.ext.asyncio import create_async_engine
    from src.pyomop.main import cli
    from src.pyomop.migrate.pyomop_migrate import migrate_cli

    async def _setup():
        src_path = tmp_path / "cli_source.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(
                text(
                    "INSERT INTO patients (id, patient_key, gender, date_of_birth, race, ethnicity)"
                    " VALUES (1, 'cli1', 'male', '1985-06-15', '', ''),"
                    "        (2, 'cli2', 'female', '1990-11-30', '', '')"
                )
            )
        await src_engine.dispose()
        return str(src_path)

    src_db = asyncio.run(_setup())
    tgt_db = str(tmp_path / "cli_target.sqlite")
    mapping_path = _minimal_mapping(tmp_path)

    # First create the target OMOP CDM tables using the main pyomop CLI
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--create",
            "--dbtype", "sqlite",
            "--name", tgt_db,
        ],
    )
    assert result.exit_code == 0, f"--create failed: {result.output}"

    # Now migrate using pyomop-migrate CLI
    result = runner.invoke(
        migrate_cli,
        [
            "--migrate",
            "--src-dbtype", "sqlite",
            "--src-name", src_db,
            "--dbtype", "sqlite",
            "--name", tgt_db,
            "--mapping", mapping_path,
            "--batch-size", "10",
        ],
    )
    assert result.exit_code == 0, f"--migrate failed: {result.output}\n{result.exception}"
    assert "Migration complete" in result.output


def test_migrate_cli_requires_mapping(tmp_path):
    """--migrate without --mapping exits with an error."""
    from click.testing import CliRunner
    from src.pyomop.migrate.pyomop_migrate import migrate_cli

    tgt_db = str(tmp_path / "no_mapping.sqlite")
    runner = CliRunner()
    result = runner.invoke(
        migrate_cli,
        [
            "--migrate",
            "--src-dbtype", "sqlite",
            "--src-name", "source.sqlite",
            "--dbtype", "sqlite",
            "--name", tgt_db,
        ],
    )
    assert result.exit_code != 0
    assert "--mapping is required" in result.output


def test_migrate_cli_bad_src_dbtype(tmp_path):
    """--migrate with an unknown --src-dbtype exits with an error."""
    from click.testing import CliRunner
    from src.pyomop.migrate.pyomop_migrate import migrate_cli

    mapping_path = _minimal_mapping(tmp_path)
    tgt_db = str(tmp_path / "bad_src.sqlite")
    runner = CliRunner()
    result = runner.invoke(
        migrate_cli,
        [
            "--migrate",
            "--src-dbtype", "oracle",
            "--src-name", "source.sqlite",
            "--dbtype", "sqlite",
            "--name", tgt_db,
            "--mapping", mapping_path,
        ],
    )
    assert result.exit_code != 0
    assert "Unknown --src-dbtype" in result.output


# ---------------------------------------------------------------------------
# Schema extraction tests
# ---------------------------------------------------------------------------


def test_extract_schema_to_markdown_basic(tmp_path):
    """extract_schema_to_markdown produces a Markdown file with table/column info."""
    import asyncio as _asyncio
    from src.pyomop.migrate.pyomop_migrate import extract_schema_to_markdown

    async def run():
        src_path = tmp_path / "schema_src.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(text(_make_source_encounters_ddl()))

        out_path = tmp_path / "schema.md"
        result_path = await extract_schema_to_markdown(src_engine, str(out_path))
        await src_engine.dispose()
        return result_path, out_path.read_text(encoding="utf-8")

    result_path, content = asyncio.run(run())

    # File should exist
    assert Path(result_path).exists()

    # Header present
    assert "# Source Database Schema" in content

    # Both tables should appear
    assert "patients" in content
    assert "encounters" in content

    # Column names should be listed
    assert "patient_key" in content
    assert "encounter_type" in content

    # Table summary section present
    assert "## Table Summary" in content


def test_extract_schema_to_markdown_pk_fk(tmp_path):
    """PK and FK information is captured in the markdown output."""
    import asyncio as _asyncio
    from src.pyomop.migrate.pyomop_migrate import extract_schema_to_markdown

    fk_ddl = (
        "CREATE TABLE IF NOT EXISTS orders ("
        "  order_id INTEGER PRIMARY KEY,"
        "  patient_id INTEGER REFERENCES patients(id),"
        "  order_date TEXT"
        ")"
    )

    async def run():
        src_path = tmp_path / "fk_src.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
            await conn.execute(text(fk_ddl))

        out_path = tmp_path / "fk_schema.md"
        await extract_schema_to_markdown(src_engine, str(out_path))
        await src_engine.dispose()
        return out_path.read_text(encoding="utf-8")

    content = asyncio.run(run())

    # PK marker present
    assert "PK" in content
    # FK section should be rendered
    assert "Foreign Keys" in content or "FK" in content


def test_extract_schema_cli_option(tmp_path):
    """--extract-schema writes a schema file and exits cleanly."""
    from click.testing import CliRunner
    from src.pyomop.migrate.pyomop_migrate import migrate_cli

    async def _setup():
        src_path = tmp_path / "es_source.sqlite"
        src_engine = create_async_engine(f"sqlite+aiosqlite:///{src_path}")
        async with src_engine.begin() as conn:
            await conn.execute(text(_make_source_patients_ddl()))
        await src_engine.dispose()
        return str(src_path)

    src_db = asyncio.run(_setup())
    out_md = str(tmp_path / "out_schema.md")

    runner = CliRunner()
    result = runner.invoke(
        migrate_cli,
        [
            "--extract-schema",
            "--src-dbtype", "sqlite",
            "--src-name", src_db,
            "--schema-output", out_md,
        ],
    )
    assert result.exit_code == 0, f"--extract-schema failed: {result.output}\n{result.exception}"
    assert "Schema written to" in result.output
    assert Path(out_md).exists()
    content = Path(out_md).read_text(encoding="utf-8")
    assert "patients" in content


def test_extract_schema_cli_bad_dbtype(tmp_path):
    """--extract-schema with an unknown --src-dbtype exits with an error."""
    from click.testing import CliRunner
    from src.pyomop.migrate.pyomop_migrate import migrate_cli

    out_md = str(tmp_path / "bad.md")
    runner = CliRunner()
    result = runner.invoke(
        migrate_cli,
        [
            "--extract-schema",
            "--src-dbtype", "oracle",
            "--src-name", "irrelevant.db",
            "--schema-output", out_md,
        ],
    )
    assert result.exit_code != 0
    assert "Unknown --src-dbtype" in result.output


def test_build_source_url_sqlite():
    """build_source_url produces a valid sqlite+aiosqlite URL."""
    from src.pyomop.migrate.pyomop_migrate import build_source_url

    url = build_source_url("sqlite", "/tmp/test.sqlite")
    assert url == "sqlite+aiosqlite:////tmp/test.sqlite"


def test_build_source_url_mysql():
    """build_source_url produces a valid mysql+aiomysql URL."""
    from src.pyomop.migrate.pyomop_migrate import build_source_url

    url = build_source_url("mysql", "mydb", host="db.host", port="3306", user="usr", pw="s3cr3t")
    assert url.startswith("mysql+aiomysql://usr:s3cr3t@db.host:3306/mydb")


def test_build_source_url_pgsql():
    """build_source_url produces a valid postgresql+asyncpg URL."""
    from src.pyomop.migrate.pyomop_migrate import build_source_url

    url = build_source_url("pgsql", "pgdb", host="pg.host", port="5432", user="admin", pw="pw")
    assert url.startswith("postgresql+asyncpg://admin:pw@pg.host:5432/pgdb")


def test_build_source_url_invalid():
    """build_source_url raises ValueError for an unsupported database type."""
    from src.pyomop.migrate.pyomop_migrate import build_source_url

    try:
        build_source_url("oracle", "mydb")
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "oracle" in str(e)


def test_build_source_url_env_vars(tmp_path, monkeypatch):
    """build_source_url picks up SRC_DB_* environment variables."""
    from src.pyomop.migrate.pyomop_migrate import build_source_url

    monkeypatch.setenv("SRC_DB_HOST", "envhost")
    monkeypatch.setenv("SRC_DB_PORT", "9999")
    monkeypatch.setenv("SRC_DB_USER", "envuser")
    monkeypatch.setenv("SRC_DB_PASSWORD", "envpass")
    monkeypatch.setenv("SRC_DB_NAME", "envdb")

    url = build_source_url("pgsql", "default_name")
    assert "envhost" in url
    assert "9999" in url
    assert "envuser" in url
    assert "envpass" in url
    assert "envdb" in url
