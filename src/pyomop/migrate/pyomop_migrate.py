"""pyomop-migrate: database-to-OMOP CDM ETL module.

Part of the ``pyomop.migrate`` sub-package.

This module provides a loader that reads from any SQLAlchemy-compatible source
database and writes to an OMOP CDM target database, guided by a JSON mapping file.

The source database can have any schema. The mapping file specifies how to map
source tables and columns to target OMOP CDM tables and columns.

Example::

    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    from pyomop import CdmEngineFactory
    from pyomop.cdm54 import Base
    from pyomop.migrate.pyomop_migrate import CdmGenericLoader

    async def run():
        source_engine = create_async_engine("sqlite+aiosqlite:///source.sqlite")
        target_cdm = CdmEngineFactory(db="sqlite", name="omop.sqlite")
        _ = target_cdm.engine
        await target_cdm.init_models(Base.metadata)

        loader = CdmGenericLoader(source_engine, target_cdm)
        await loader.load("mapping.generic.json", batch_size=500)

    asyncio.run(run())
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    and_,
    cast,
    insert,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
)
from sqlalchemy.ext.automap import AutomapBase, automap_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CdmGenericLoader:
    """Load data from any database into OMOP CDM tables using a JSON mapping file.

    The source database can have any schema; the target must be an OMOP CDM
    database initialised via :class:`~pyomop.CdmEngineFactory`.

    Mapping file format (JSON):

    .. code-block:: json

        {
          "tables": [
            {
              "source_table": "patients",
              "name": "person",
              "filters": [
                {"column": "active", "equals": 1}
              ],
              "columns": {
                "person_id": "patient_id",
                "gender_concept_id": {"const": 0},
                "gender_source_value": "gender",
                "year_of_birth": "birth_year",
                "race_concept_id": {"const": 0},
                "ethnicity_concept_id": {"const": 0}
              }
            }
          ],
          "concept": [
            {
              "table": "observation",
              "mappings": [
                {
                  "source": "observation_source_value",
                  "target": "observation_concept_id"
                }
              ]
            }
          ],
          "force_text_fields": ["production_id"]
        }

    Mapping keys:

    - ``source_table``: Name of the table in the source database to read from.
      Falls back to ``name`` when not provided.
    - ``name``: Name of the target OMOP CDM table.
    - ``filters``: Optional list of row-level filters applied to source rows.
      Supported operators: ``equals``, ``not_empty``.
    - ``columns``: Mapping of ``target_column -> source_column`` or
      ``target_column -> {"const": value}``.
    - ``concept``: List of concept-code lookup definitions (same format as the
      FHIR/CSV loader ``mapping.default.json``).
    - ``force_text_fields``: Column names always stored as plain text regardless
      of the inferred SQLAlchemy type.
    """

    def __init__(
        self,
        source_engine: AsyncEngine,
        target_cdm: Any,
        version: str = "cdm54",
    ) -> None:
        """Create a loader bound to source and target database engines.

        Args:
            source_engine: An async SQLAlchemy engine pointing at the source
                database (read-only access is sufficient).
            target_cdm: An initialised :class:`~pyomop.CdmEngineFactory` for the
                target OMOP CDM database.
            version: OMOP CDM version label (``"cdm54"`` or ``"cdm6"``).
        """
        self._source_engine = source_engine
        self._cdm = target_cdm
        self._target_engine = target_cdm.engine
        self._maker = async_sessionmaker(self._target_engine, class_=AsyncSession)
        self._scope = async_scoped_session(self._maker, scopefunc=asyncio.current_task)
        self._version = version

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _get_target_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Yield a scoped async session bound to the target engine."""
        async with self._scope() as session:
            yield session

    def _load_mapping(self, mapping_path: str) -> Dict[str, Any]:
        """Load a JSON mapping file from disk.

        Args:
            mapping_path: Filesystem path to the JSON mapping file.

        Returns:
            Parsed mapping dictionary.
        """
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    async def _prepare_automap(self, engine: AsyncEngine) -> AutomapBase:
        """Reflect a database and return an automapped base.

        Args:
            engine: The async engine to reflect.

        Returns:
            A populated :class:`~sqlalchemy.ext.automap.AutomapBase`.
        """
        automap: AutomapBase = automap_base()

        async with engine.connect() as conn:
            def _prepare(sync_conn: Any) -> None:
                automap.prepare(autoload_with=sync_conn)

            await conn.run_sync(_prepare)
        return automap

    async def _prepare_target_automap(self, conn: AsyncConnection) -> AutomapBase:
        """Reflect the target database via an existing connection.

        Args:
            conn: An open async connection to the target database.

        Returns:
            A populated :class:`~sqlalchemy.ext.automap.AutomapBase`.
        """
        automap: AutomapBase = automap_base()

        def _prepare(sync_conn: Any) -> None:
            automap.prepare(autoload_with=sync_conn)

        await conn.run_sync(_prepare)
        return automap

    def _build_filter_clause(self, table: Any, filters: Optional[List[Dict[str, Any]]]) -> Any:
        """Build a SQLAlchemy WHERE expression from a list of filter dicts.

        Args:
            table: SQLAlchemy :class:`~sqlalchemy.schema.Table` object.
            filters: List of filter dicts.  Each dict may contain:
                - ``column``: source column name
                - ``equals``: value that the column must equal
                - ``not_empty``: if ``True``, the column must be non-null and non-empty

        Returns:
            A SQLAlchemy clause, or ``None`` when no applicable filters exist.
        """
        if not filters:
            return None
        conditions: List[Any] = []
        for flt in filters:
            col_name = flt.get("column")
            if col_name is None or col_name not in table.c:
                continue
            if "equals" in flt:
                conditions.append(table.c[col_name] == flt["equals"])
            elif flt.get("not_empty"):
                conditions.append(
                    and_(
                        table.c[col_name].isnot(None),
                        cast(table.c[col_name], String()) != "",
                    )
                )
        if not conditions:
            return None
        return and_(*conditions)

    def _convert_value(self, sa_type: Any, value: Any) -> Any:
        """Coerce a single source value to the target column's Python type.

        Mirrors the coercion logic in :class:`~pyomop.loader.CdmCsvLoader`.

        Args:
            sa_type: SQLAlchemy column type instance.
            value: Raw value from the source database.

        Returns:
            Coerced Python value, or ``None`` when conversion is not possible.
        """
        if value is None:
            return None
        # Safely check for pandas NA/NaN
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        try:
            if isinstance(sa_type, Date):
                dt = pd.to_datetime(value, errors="coerce")
                return None if pd.isna(dt) else dt.date()
            if isinstance(sa_type, DateTime):
                ts = pd.to_datetime(value, errors="coerce", utc=True)
                if pd.isna(ts):
                    return None
                py = ts.to_pydatetime()
                if getattr(py, "tzinfo", None) is not None:
                    py = py.astimezone(timezone.utc).replace(tzinfo=None)
                return py
            if isinstance(sa_type, (Integer, BigInteger)):
                return int(value)
            if isinstance(sa_type, Numeric):
                return Decimal(str(value))
        except Exception:
            return value
        if isinstance(value, str):
            return value[:50]
        return value

    def _coerce_record_to_table_types(
        self,
        table: Any,
        rec: Dict[str, Any],
        force_text_fields: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """Coerce all record values to the SQL types of the target OMOP table.

        Mirrors the coercion logic in :class:`~pyomop.loader.CdmCsvLoader`.

        Args:
            table: The target SQLAlchemy :class:`~sqlalchemy.schema.Table`.
            rec: Record dictionary to coerce in-place.
            force_text_fields: Column names that must always be stored as text.

        Returns:
            The coerced record dictionary.
        """
        force_text: Set[str] = set(force_text_fields or [])

        def _s(v: Any) -> str:
            try:
                if isinstance(v, float):
                    return str(int(v)) if v.is_integer() else str(v)
                if isinstance(v, Decimal):
                    if v == v.to_integral_value():
                        return str(int(v))
                    return format(v.normalize(), "f")
                return "" if v is None else str(v)
            except Exception:
                return str(v)

        for col in table.columns:
            name = col.name
            if name not in rec:
                continue
            val = rec[name]
            if val is None:
                continue

            t = col.type

            if name in force_text:
                if isinstance(val, (list, tuple)):
                    sval = ",".join([_s(v) for v in val])
                elif isinstance(val, dict):
                    try:
                        import json as _json

                        sval = _json.dumps(val, ensure_ascii=False)
                    except Exception:
                        sval = _s(val)
                else:
                    sval = _s(val)
                max_len = getattr(t, "length", None)
                rec[name] = sval[: max_len or 255]
                continue

            try:
                if isinstance(t, Date):
                    dt = pd.to_datetime(val, errors="coerce")
                    rec[name] = None if pd.isna(dt) else dt.date()
                elif isinstance(t, DateTime):
                    ts = pd.to_datetime(val, errors="coerce", utc=True)
                    if pd.isna(ts):
                        rec[name] = None
                    else:
                        py = ts.to_pydatetime()
                        rec[name] = py.astimezone(timezone.utc).replace(tzinfo=None)
                elif isinstance(t, (Integer, BigInteger)):
                    num = pd.to_numeric(val, errors="coerce")
                    rec[name] = None if pd.isna(num) else int(num)
                elif isinstance(t, Numeric):
                    num = pd.to_numeric(val, errors="coerce")
                    rec[name] = None if pd.isna(num) else Decimal(str(num))
                elif isinstance(t, (String, Text)):
                    if isinstance(val, (list, tuple)):
                        sval = ",".join([_s(v) for v in val])
                    elif isinstance(val, dict):
                        try:
                            import json as _json

                            sval = _json.dumps(val, ensure_ascii=False)
                        except Exception:
                            sval = _s(val)
                    else:
                        sval = _s(val)
                    max_len = getattr(t, "length", None)
                    rec[name] = sval[: max_len or 255]
                # other types: leave as-is
            except Exception:
                try:
                    s = _s(val)
                    max_len = getattr(getattr(col, "type", None), "length", None)
                    rec[name] = s[: max_len or 255]
                except Exception:
                    rec[name] = val

        return rec

    async def _list_tables_with_person_id(self, conn: AsyncConnection) -> List[str]:
        """Return target table names that contain a ``person_id`` column, excluding ``person``."""

        def _inner(sync_conn: Any) -> List[str]:
            from sqlalchemy import inspect as _inspect

            insp = _inspect(sync_conn)
            tables = insp.get_table_names()
            result: List[str] = []
            for t in tables:
                try:
                    cols = insp.get_columns(t)
                except Exception:
                    continue
                if any(c.get("name") == "person_id" for c in cols) and t != "person":
                    result.append(t)
            return result

        return await conn.run_sync(_inner)

    async def _add_person_id_text_columns(self, session: AsyncSession) -> None:
        """Add a temporary ``person_id_text`` TEXT column to staging tables.

        Used to handle non-numeric person identifiers during bulk load before
        they are resolved to integer ``person_id`` values.
        """
        conn = await session.connection()
        tables = await self._list_tables_with_person_id(conn)
        dialect = self._target_engine.dialect.name
        for t in tables:
            if dialect == "postgresql":
                ddl = f'ALTER TABLE "{t}" ADD COLUMN IF NOT EXISTS person_id_text TEXT'
            else:
                ddl = f'ALTER TABLE "{t}" ADD COLUMN person_id_text TEXT'
            try:
                await session.execute(text(ddl))
            except Exception:
                pass

    async def _drop_person_id_text_columns(self, session: AsyncSession) -> None:
        """Drop the temporary ``person_id_text`` columns after FK normalisation."""
        conn = await session.connection()
        tables = await self._list_tables_with_person_id(conn)
        dialect = self._target_engine.dialect.name
        for t in tables:
            if dialect == "postgresql":
                ddl = f'ALTER TABLE "{t}" DROP COLUMN IF EXISTS person_id_text'
            else:
                ddl = f'ALTER TABLE "{t}" DROP COLUMN person_id_text'
            try:
                await session.execute(text(ddl))
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def load(self, mapping_path: str, batch_size: int = 1000) -> None:
        """Load data from the source database into the target OMOP CDM database.

        The method executes the following steps:

        1. Reflect source and target database schemas.
        2. For each table mapping: read source rows, map columns, insert in batches.
        3. Normalise ``person_id`` foreign keys.
        4. Backfill ``year_of_birth``, ``month_of_birth``, ``day_of_birth``.
        5. Set ``gender_concept_id`` from ``gender_source_value``.
        6. Apply concept-code lookups defined in the mapping.

        Args:
            mapping_path: Path to the JSON mapping file.
            batch_size: Number of rows inserted per database round-trip.
        """
        start_time = time.time()
        logger.info("Loading mapping from %s", mapping_path)
        mapping = self._load_mapping(mapping_path)

        logger.info("Reflecting source database schema")
        source_automap = await self._prepare_automap(self._source_engine)

        async with self._get_target_session() as session:
            is_pg = str(self._target_engine.dialect.name).startswith("postgres")
            if is_pg:
                try:
                    await session.execute(text("SET session_replication_role = replica"))
                except Exception:
                    pass

            try:
                conn = await session.connection()
                await self._add_person_id_text_columns(session)
                target_automap = await self._prepare_target_automap(conn)

                for tbl in mapping.get("tables", []):
                    source_table_name: str = tbl.get("source_table") or tbl.get("name", "")
                    target_table_name: str = tbl.get("name", "")
                    if not target_table_name or not source_table_name:
                        continue

                    # Resolve source table
                    src_table = source_automap.metadata.tables.get(source_table_name)
                    if src_table is None:
                        logger.warning("Source table '%s' not found; skipping.", source_table_name)
                        continue

                    # Resolve target mapper
                    try:
                        target_mapper = getattr(target_automap.classes, target_table_name)
                    except AttributeError:
                        logger.warning("Target table '%s' not found; skipping.", target_table_name)
                        continue

                    col_map: Dict[str, Any] = tbl.get("columns", {})
                    filters = tbl.get("filters")

                    # Build and execute source query
                    query = select(src_table)
                    where_clause = self._build_filter_clause(src_table, filters)
                    if where_clause is not None:
                        query = query.where(where_clause)

                    async with self._source_engine.connect() as src_conn:
                        result = await src_conn.execute(query)
                        src_rows = result.mappings().fetchall()

                    if not src_rows:
                        logger.info(
                            "No rows found in source table '%s'; skipping.",
                            source_table_name,
                        )
                        continue

                    logger.info(
                        "Processing %d row(s) from '%s' -> '%s'",
                        len(src_rows),
                        source_table_name,
                        target_table_name,
                    )

                    target_table = target_mapper.__table__
                    sa_cols = {c.name: c.type for c in target_table.columns}
                    sa_col_objs = {c.name: c for c in target_table.columns}
                    force_text_fields: Set[str] = set(mapping.get("force_text_fields", []))

                    records: List[Dict[str, Any]] = []
                    for row in src_rows:
                        rec: Dict[str, Any] = {}
                        for target_col, src in col_map.items():
                            if isinstance(src, dict) and "const" in src:
                                value: Any = src["const"]
                            elif isinstance(src, str):
                                value = row.get(src)
                            else:
                                value = None

                            # Keep person_id raw so staging logic below can handle it
                            if target_col != "person_id":
                                sa_t = sa_cols.get(target_col)
                                if sa_t is not None:
                                    value = self._convert_value(sa_t, value)
                            rec[target_col] = value

                        # Route non-numeric person identifiers through staging column
                        if "person_id" in rec:
                            pid = rec.get("person_id")
                            if pid is None:
                                col_obj = sa_col_objs.get("person_id")
                                if col_obj is not None and not getattr(col_obj, "nullable", True):
                                    rec["person_id"] = 0
                            elif isinstance(pid, int):
                                pass  # already numeric
                            elif isinstance(pid, str) and pid.strip().isdigit():
                                try:
                                    rec["person_id"] = int(pid.strip())
                                except Exception:
                                    if "person_id_text" in sa_cols:
                                        rec["person_id_text"] = str(pid)
                                    col_obj = sa_col_objs.get("person_id")
                                    if col_obj is not None and not getattr(col_obj, "nullable", True):
                                        rec["person_id"] = 0
                                    else:
                                        rec["person_id"] = None
                            else:
                                if "person_id_text" in sa_cols:
                                    rec["person_id_text"] = str(pid)
                                col_obj = sa_col_objs.get("person_id")
                                if col_obj is not None and not getattr(col_obj, "nullable", True):
                                    rec["person_id"] = 0
                                else:
                                    rec["person_id"] = None

                        rec = self._coerce_record_to_table_types(
                            target_table, rec, force_text_fields
                        )
                        records.append(rec)

                    if not records:
                        continue

                    stmt = insert(target_mapper)
                    for i in range(0, len(records), batch_size):
                        batch = records[i : i + batch_size]
                        await session.execute(stmt, batch)
                        logger.debug(
                            "Inserted rows %d-%d into '%s'",
                            i,
                            i + len(batch),
                            target_table_name,
                        )

                elapsed = time.time() - start_time
                logger.info("Data loaded into target tables (elapsed: %.2fs)", elapsed)

                # Step 2: Normalise person_id FKs
                step_time = time.time()
                logger.info("Normalizing person_id foreign keys")
                await self.fix_person_id(session, target_automap)
                await self._drop_person_id_text_columns(session)
                logger.info("Person_id normalized (elapsed: %.2fs)", time.time() - step_time)

                # Step 3: Backfill birth date fields
                step_time = time.time()
                logger.info("Backfilling person birth fields")
                await self.backfill_person_birth_fields(session, target_automap)
                logger.info("Birth fields backfilled (elapsed: %.2fs)", time.time() - step_time)

                # Step 4: Set gender_concept_id
                step_time = time.time()
                logger.info("Setting person.gender_concept_id")
                await self.update_person_gender_concept_id(session, target_automap)
                logger.info("Gender concept_id set (elapsed: %.2fs)", time.time() - step_time)

                # Step 5: Apply concept code mappings
                step_time = time.time()
                logger.info("Applying concept mappings")
                await self.apply_concept_mappings(session, target_automap, mapping)
                logger.info("Concept mappings applied (elapsed: %.2fs)", time.time() - step_time)

                await session.commit()

            finally:
                if is_pg:
                    try:
                        await session.execute(text("SET session_replication_role = origin"))
                    except Exception:
                        pass
                await session.close()

    async def fix_person_id(self, session: AsyncSession, automap: AutomapBase) -> None:
        """Normalise ``person_id`` foreign keys across all OMOP tables.

        Resolves any rows where ``person_id`` contains a source-value string
        (e.g. UUID) instead of the integer primary key assigned to the person.

        Args:
            session: Active async session on the target database.
            automap: Automapped base reflected from the target database.
        """
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return

        person_table = person_cls.__table__
        res = await session.execute(
            select(person_table.c.person_source_value, person_table.c.person_id).where(
                person_table.c.person_source_value.isnot(None)
            )
        )
        pairs = res.fetchall()
        if not pairs:
            return

        psv_to_id: Dict[str, int] = {}
        for psv, pid in pairs:
            if psv is None or pid is None:
                continue
            psv_to_id[str(psv)] = int(pid)

        if not psv_to_id:
            return

        for _tbl_name, table in automap.metadata.tables.items():
            if _tbl_name == person_table.name:
                continue
            if "person_id" not in table.c:
                continue

            has_text = "person_id_text" in table.c

            for psv, pid in psv_to_id.items():
                if has_text:
                    stmt = (
                        update(table)
                        .where(table.c.person_id_text == psv)
                        .values(person_id=pid)
                    )
                    await session.execute(stmt)
                stmt2 = (
                    update(table)
                    .where(cast(table.c.person_id, String()) == psv)
                    .values(person_id=pid)
                )
                await session.execute(stmt2)

            if has_text:
                try:
                    await session.execute(
                        update(table)
                        .where(table.c.person_id_text.isnot(None))
                        .values(person_id_text=None)
                    )
                except Exception:
                    pass

    async def update_person_gender_concept_id(
        self, session: AsyncSession, automap: AutomapBase
    ) -> None:
        """Update ``person.gender_concept_id`` from ``gender_source_value``.

        Uses the standard OMOP concept IDs:
        - ``male`` / ``m`` → 8507
        - ``female`` / ``f`` → 8532
        - anything else → 0

        Args:
            session: Active async session on the target database.
            automap: Automapped base reflected from the target database.
        """
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return

        person_table = person_cls.__table__
        res = await session.execute(
            select(
                person_table.c.person_id,
                person_table.c.gender_source_value,
                person_table.c.gender_concept_id,
            ).where(person_table.c.gender_source_value.isnot(None))
        )
        rows = res.fetchall()
        if not rows:
            return

        def _map_gender(val: Optional[str]) -> int:
            if val is None:
                return 0
            s = str(val).strip().lower()
            if s in {"male", "m"}:
                return 8507
            if s in {"female", "f"}:
                return 8532
            return 0

        for pid, gsrc, gcid in rows:
            target_gcid = _map_gender(gsrc)
            if gcid == target_gcid:
                continue
            stmt = (
                update(person_table)
                .where(person_table.c.person_id == pid)
                .values(gender_concept_id=target_gcid)
            )
            await session.execute(stmt)

    async def backfill_person_birth_fields(
        self, session: AsyncSession, automap: AutomapBase
    ) -> None:
        """Backfill ``year_of_birth``, ``month_of_birth``, ``day_of_birth`` from ``birth_datetime``.

        Replaces ``NULL`` or ``0`` values where ``birth_datetime`` is available.

        Args:
            session: Active async session on the target database.
            automap: Automapped base reflected from the target database.
        """
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return

        person_table = person_cls.__table__
        res = await session.execute(
            select(
                person_table.c.person_id,
                person_table.c.birth_datetime,
                person_table.c.year_of_birth,
                person_table.c.month_of_birth,
                person_table.c.day_of_birth,
            ).where(person_table.c.birth_datetime.isnot(None))
        )
        rows = res.fetchall()
        if not rows:
            return

        for pid, birth_dt, y, m, d in rows:
            bd: Optional[datetime]
            if isinstance(birth_dt, datetime):
                bd = birth_dt.astimezone(timezone.utc).replace(tzinfo=None) if birth_dt.tzinfo else birth_dt
            elif isinstance(birth_dt, date):
                bd = datetime(birth_dt.year, birth_dt.month, birth_dt.day)
            else:
                try:
                    tmp = pd.to_datetime(birth_dt, errors="coerce", utc=True)
                    if pd.isna(tmp):
                        bd = None
                    else:
                        py = tmp.to_pydatetime()
                        bd = py.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    bd = None

            if bd is None:
                continue

            new_y = y if (y is not None and int(y or 0) != 0) else bd.year
            new_m = m if (m is not None and int(m or 0) != 0) else bd.month
            new_d = d if (d is not None and int(d or 0) != 0) else bd.day

            if new_y != y or new_m != m or new_d != d:
                stmt = (
                    update(person_table)
                    .where(person_table.c.person_id == pid)
                    .values(year_of_birth=new_y, month_of_birth=new_m, day_of_birth=new_d)
                )
                await session.execute(stmt)

    async def apply_concept_mappings(
        self,
        session: AsyncSession,
        automap: AutomapBase,
        mapping: Dict[str, Any],
    ) -> None:
        """Populate ``*_concept_id`` columns via concept-code lookups.

        Based on the ``"concept"`` key in the mapping JSON.  For each defined
        mapping, rows where the target concept column is ``NULL`` or ``0`` are
        updated by looking up ``concept.concept_id`` using the source column
        value.  If the source value is comma-separated, only the first element
        is used.

        Args:
            session: Active async session on the target database.
            automap: Automapped base reflected from the target database.
            mapping: Parsed mapping dictionary.
        """
        if not mapping or "concept" not in mapping:
            return

        try:
            concept_cls = getattr(automap.classes, "concept")
        except AttributeError:
            return

        concept_table = concept_cls.__table__
        code_to_cid: Dict[str, Optional[int]] = {}

        async def _lookup(code: str) -> Optional[int]:
            if code in code_to_cid:
                return code_to_cid[code]
            res = await session.execute(
                select(concept_table.c.concept_id).where(concept_table.c.concept_code == code)
            )
            row = res.first()
            cid = int(row[0]) if row and row[0] is not None else None
            code_to_cid[code] = cid
            return cid

        for item in mapping.get("concept", []):
            table_name = item.get("table")
            if not table_name:
                continue
            try:
                mapper = getattr(automap.classes, table_name)
            except AttributeError:
                continue

            table = mapper.__table__
            pk_cols = list(table.primary_key.columns)
            if not pk_cols:
                continue

            for m in item.get("mappings", []):
                source_col = m.get("source")
                target_col = m.get("target")
                if not source_col or not target_col:
                    continue
                if source_col not in table.c or target_col not in table.c:
                    continue

                res = await session.execute(
                    select(
                        *pk_cols,
                        table.c[source_col].label("_src"),
                        table.c[target_col].label("_tgt"),
                    ).where(
                        or_(table.c[target_col].is_(None), table.c[target_col] == 0),
                        table.c[source_col].isnot(None),
                    )
                )
                concept_rows = res.fetchall()
                if not concept_rows:
                    continue

                for row in concept_rows:
                    pk_vals = row[: len(pk_cols)]
                    src_val = row[len(pk_cols)]

                    code: Optional[str] = None
                    if isinstance(src_val, str):
                        first = src_val.split(",")[0].strip()
                        code = first if first else None
                    elif isinstance(src_val, list) and src_val:
                        code = str(src_val[0])
                    else:
                        code = str(src_val) if src_val is not None else None

                    if not code:
                        continue

                    cid = await _lookup(code)
                    if cid is None:
                        continue

                    where_clause = and_(
                        *[(pk_col == pk_val) for pk_col, pk_val in zip(pk_cols, pk_vals)]
                    )
                    stmt = update(table).where(where_clause).values({target_col: cid})
                    await session.execute(stmt)


def create_source_engine(url: str) -> AsyncEngine:
    """Convenience factory to create an async source engine from a URL string.

    This is a thin wrapper around
    :func:`~sqlalchemy.ext.asyncio.create_async_engine` provided for
    discoverability.

    Args:
        url: Async-compatible SQLAlchemy database URL, e.g.
            ``"sqlite+aiosqlite:///source.sqlite"`` or
            ``"postgresql+asyncpg://user:pass@host/db"``.

    Returns:
        An :class:`~sqlalchemy.ext.asyncio.AsyncEngine` instance.

    Example::

        from pyomop.migrate.pyomop_migrate import create_source_engine
        engine = create_source_engine("sqlite+aiosqlite:///mydb.sqlite")
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(url, pool_pre_ping=True)


def load_mapping(mapping_path: str) -> Dict[str, Any]:
    """Load and parse a generic-loader JSON mapping file.

    Args:
        mapping_path: Path to the JSON mapping file.

    Returns:
        Parsed mapping dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file is not valid JSON.
    """
    path = Path(mapping_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_source_url(
    dbtype: str,
    name: str,
    host: str = "localhost",
    port: str = "5432",
    user: str = "root",
    pw: str = "pass",
) -> str:
    """Build an async SQLAlchemy URL for a source database.

    Centralises the driver-specific URL construction used by both the
    ``--migrate`` and ``--extract-schema`` CLI commands.

    Connection parameters are resolved in order:
    1. Explicit arguments passed to this function.
    2. Environment variables ``SRC_DB_HOST``, ``SRC_DB_PORT``, ``SRC_DB_USER``,
       ``SRC_DB_PASSWORD``, ``SRC_DB_NAME`` (when the corresponding argument
       still holds its default value).

    The environment variable names follow a ``SRC_DB_`` prefix convention so
    they don't collide with any existing ``PYOMOP_*`` variables:

    .. code-block:: bash

        export SRC_DB_HOST=myserver
        export SRC_DB_PORT=5432
        export SRC_DB_USER=reader
        export SRC_DB_PASSWORD=secret
        export SRC_DB_NAME=ehr_db

    Args:
        dbtype: Database type: ``"sqlite"``, ``"mysql"``, or ``"pgsql"``.
        name: Database name or SQLite file path.
        host: Database host (ignored for SQLite).
        port: Database port as a string (ignored for SQLite).
        user: Database user (ignored for SQLite).
        pw: Database password (ignored for SQLite).

    Returns:
        An async-compatible SQLAlchemy URL string.

    Raises:
        ValueError: If *dbtype* is not one of the supported values.
    """
    import os

    # Fall back to environment variables when explicit args still hold defaults
    host = os.environ.get("SRC_DB_HOST", host)
    port = os.environ.get("SRC_DB_PORT", port)
    user = os.environ.get("SRC_DB_USER", user)
    pw = os.environ.get("SRC_DB_PASSWORD", pw)
    name = os.environ.get("SRC_DB_NAME", name)

    if dbtype == "sqlite":
        return f"sqlite+aiosqlite:///{name}"
    if dbtype == "mysql":
        return f"mysql+aiomysql://{user}:{pw}@{host}:{port}/{name}"
    if dbtype == "pgsql":
        return f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{name}"
    raise ValueError(
        f"Unknown database type '{dbtype}'. Use 'sqlite', 'mysql', or 'pgsql'."
    )


async def extract_schema_to_markdown(engine: AsyncEngine, output_path: str) -> str:
    """Introspect a database and write its schema as a Markdown document.

    The generated document includes:

    - A summary table listing all user tables with their row counts.
    - Per-table sections with:
        - Column name, data type, nullable flag, and default value.
        - Primary key column(s).
        - Foreign key relationships (referencing table and column).

    This is especially useful for giving AI agents enough context to write
    an accurate mapping JSON file for :class:`CdmGenericLoader`.

    Args:
        engine: An async SQLAlchemy engine connected to the source database.
        output_path: Filesystem path where the Markdown file will be written.
            Parent directories must already exist.

    Returns:
        The absolute path of the written Markdown file.

    Example::

        import asyncio
        from pyomop.migrate.pyomop_migrate import create_source_engine, extract_schema_to_markdown

        async def run():
            engine = create_source_engine("sqlite+aiosqlite:///source.sqlite")
            path = await extract_schema_to_markdown(engine, "schema.md")
            print(f"Schema written to {path}")

        asyncio.run(run())
    """

    def _collect_schema(sync_conn: Any) -> Dict[str, Any]:
        """Run synchronous SQLAlchemy inspection inside ``run_sync``."""
        from sqlalchemy import inspect as _inspect

        insp = _inspect(sync_conn)
        schema_info: Dict[str, Any] = {}

        for table_name in insp.get_table_names():
            columns = insp.get_columns(table_name)
            pk_cols = insp.get_pk_constraint(table_name).get("constrained_columns", [])
            fks = insp.get_foreign_keys(table_name)

            # Attempt a quick COUNT(*) — may fail on views or restricted objects
            try:
                row = sync_conn.execute(
                    _inspect(sync_conn).bind.execute(  # type: ignore[attr-defined]
                        __import__("sqlalchemy").text(f"SELECT COUNT(*) FROM \"{table_name}\"")
                    )
                ).scalar()
                row_count: Optional[int] = int(row) if row is not None else None
            except Exception:
                row_count = None

            schema_info[table_name] = {
                "columns": columns,
                "pk_columns": pk_cols,
                "foreign_keys": fks,
                "row_count": row_count,
            }
        return schema_info

    # ---- collect schema info ------------------------------------------------
    schema: Dict[str, Any] = {}
    async with engine.connect() as conn:
        # We need the row count too, so build a second pass with raw SQL
        def _collect(sync_conn: Any) -> Dict[str, Any]:
            from sqlalchemy import inspect as _inspect, text as _text

            insp = _inspect(sync_conn)
            result: Dict[str, Any] = {}
            for table_name in insp.get_table_names():
                columns = insp.get_columns(table_name)
                pk_info = insp.get_pk_constraint(table_name)
                pk_cols: List[str] = pk_info.get("constrained_columns", [])
                fks: List[Dict[str, Any]] = insp.get_foreign_keys(table_name)

                try:
                    row_count: Optional[int] = sync_conn.execute(
                        _text(f'SELECT COUNT(*) FROM "{table_name}"')
                    ).scalar()
                except Exception:
                    row_count = None

                result[table_name] = {
                    "columns": columns,
                    "pk_columns": pk_cols,
                    "foreign_keys": fks,
                    "row_count": row_count,
                }
            return result

        schema = await conn.run_sync(_collect)

    # ---- render Markdown ----------------------------------------------------
    lines: List[str] = []
    dialect = engine.dialect.name
    lines.append("# Source Database Schema")
    lines.append("")
    lines.append(f"**Dialect:** `{dialect}`  ")
    lines.append(f"**Tables:** {len(schema)}")
    lines.append("")

    # Summary table
    lines.append("## Table Summary")
    lines.append("")
    lines.append("| Table | Rows | Primary Key(s) |")
    lines.append("|---|---|---|")
    for tbl_name, info in sorted(schema.items()):
        pk_str = ", ".join(f"`{c}`" for c in info["pk_columns"]) if info["pk_columns"] else "*(none)*"
        row_str = str(info["row_count"]) if info["row_count"] is not None else "N/A"
        lines.append(f"| `{tbl_name}` | {row_str} | {pk_str} |")
    lines.append("")

    # Per-table detail
    for tbl_name, info in sorted(schema.items()):
        lines.append(f"## `{tbl_name}`")
        lines.append("")

        pk_cols_set = set(info["pk_columns"])

        # Column table
        lines.append("### Columns")
        lines.append("")
        lines.append("| Column | Type | Nullable | Default | Key |")
        lines.append("|---|---|---|---|---|")
        for col in info["columns"]:
            col_name: str = col["name"]
            col_type: str = str(col["type"])
            nullable: str = "Yes" if col.get("nullable", True) else "No"
            default: str = str(col.get("default") or "")
            key_flags: List[str] = []
            if col_name in pk_cols_set:
                key_flags.append("PK")
            for fk in info["foreign_keys"]:
                if col_name in fk.get("constrained_columns", []):
                    ref_tbl = fk.get("referred_table", "")
                    ref_cols = ", ".join(fk.get("referred_columns", []))
                    key_flags.append(f"FK → `{ref_tbl}`.`{ref_cols}`")
            key_str = ", ".join(key_flags) if key_flags else ""
            lines.append(f"| `{col_name}` | `{col_type}` | {nullable} | {default} | {key_str} |")
        lines.append("")

        # Foreign key summary
        if info["foreign_keys"]:
            lines.append("### Foreign Keys")
            lines.append("")
            lines.append("| Columns | References |")
            lines.append("|---|---|")
            for fk in info["foreign_keys"]:
                local_cols = ", ".join(f"`{c}`" for c in fk.get("constrained_columns", []))
                ref_tbl = fk.get("referred_table", "")
                ref_cols = ", ".join(f"`{c}`" for c in fk.get("referred_columns", []))
                lines.append(f"| {local_cols} | `{ref_tbl}` ({ref_cols}) |")
            lines.append("")

    # Write output
    out = Path(output_path)
    out.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Schema written to %s", out.resolve())
    return str(out.resolve())


# ---------------------------------------------------------------------------
# Command-line interface for ``pyomop-migrate``
# ---------------------------------------------------------------------------

import click


@click.command()
@click.option(
    "--migrate",
    "do_migrate",
    is_flag=True,
    help="Migrate data from a source database into the target OMOP CDM database using a mapping file.",
)
@click.option(
    "--extract-schema",
    "do_extract_schema",
    is_flag=True,
    help="Introspect the source database and write its schema to a Markdown file.",
)
# ---- source database options -----------------------------------------------
@click.option(
    "--src-dbtype",
    default="sqlite",
    help="Source database type (sqlite, mysql or pgsql).",
)
@click.option(
    "--src-host",
    default="localhost",
    envvar="SRC_DB_HOST",
    help="Source database host. Env: SRC_DB_HOST.",
)
@click.option(
    "--src-port",
    default="5432",
    envvar="SRC_DB_PORT",
    help="Source database port. Env: SRC_DB_PORT.",
)
@click.option(
    "--src-user",
    default="root",
    envvar="SRC_DB_USER",
    help="Source database user. Env: SRC_DB_USER.",
)
@click.option(
    "--src-pw",
    default="pass",
    envvar="SRC_DB_PASSWORD",
    help="Source database password. Env: SRC_DB_PASSWORD.",
)
@click.option(
    "--src-name",
    default="source.sqlite",
    envvar="SRC_DB_NAME",
    help="Source database name or SQLite file path. Env: SRC_DB_NAME.",
)
@click.option(
    "--src-schema",
    default="",
    help="Source database schema (PostgreSQL).",
)
# ---- target database options -----------------------------------------------
@click.option(
    "--dbtype",
    "-t",
    default="sqlite",
    help="Target OMOP CDM database type (sqlite, mysql or pgsql).",
)
@click.option(
    "--host",
    "-h",
    default="localhost",
    help="Target database host.",
)
@click.option(
    "--port",
    "-p",
    default="5432",
    help="Target database port.",
)
@click.option(
    "--user",
    "-u",
    default="root",
    help="Target database user.",
)
@click.option(
    "--pw",
    "-w",
    default="pass",
    help="Target database password.",
)
@click.option(
    "--name",
    "-n",
    default="cdm.sqlite",
    help="Target database name or SQLite file path.",
)
@click.option(
    "--schema",
    "-s",
    default="",
    help="Target database schema (PostgreSQL).",
)
@click.option(
    "--version",
    "-v",
    default="cdm54",
    help="OMOP CDM version (cdm54 (default) or cdm6).",
)
# ---- mapping / schema-output -----------------------------------------------
@click.option(
    "--mapping",
    "-m",
    "mapping_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the JSON mapping file. Required with --migrate.",
)
@click.option(
    "--batch-size",
    default=1000,
    show_default=True,
    help="Number of rows per INSERT batch.",
)
@click.option(
    "--schema-output",
    default="schema.md",
    show_default=True,
    help="Output path for the schema Markdown file. Used with --extract-schema.",
)
def migrate_cli(
    do_migrate,
    do_extract_schema,
    src_dbtype,
    src_host,
    src_port,
    src_user,
    src_pw,
    src_name,
    src_schema,
    dbtype,
    host,
    port,
    user,
    pw,
    name,
    schema,
    version,
    mapping_path,
    batch_size,
    schema_output,
):
    """pyomop-migrate: migrate a source database to OMOP CDM or extract its schema.

    Examples:

    \b
        # Migrate SQLite source to SQLite OMOP target
        pyomop-migrate --migrate \\
          --src-dbtype sqlite --src-name hospital.sqlite \\
          --dbtype sqlite --name omop.sqlite \\
          --mapping ehr_to_omop.json

    \b
        # Extract source database schema to Markdown
        pyomop-migrate --extract-schema \\
          --src-dbtype sqlite --src-name hospital.sqlite \\
          --schema-output hospital_schema.md
    """
    import asyncio as _asyncio
    import sys

    if do_migrate:
        if not mapping_path:
            click.echo("--mapping is required when using --migrate.", err=True)
            sys.exit(1)

        click.echo(
            f"Migrating data from {src_dbtype} database '{src_name}' "
            f"into {dbtype} database '{name}' using mapping '{mapping_path}'"
        )

        try:
            src_url = build_source_url(src_dbtype, src_name, src_host, src_port, src_user, src_pw)
        except ValueError:
            click.echo(
                f"Unknown --src-dbtype '{src_dbtype}'. Use sqlite, mysql, or pgsql.", err=True
            )
            sys.exit(1)

        from pyomop import CdmEngineFactory

        source_engine = create_source_engine(src_url)
        cdm = CdmEngineFactory(dbtype, host, port, user, pw, name, schema)
        _ = cdm.engine

        loader = CdmGenericLoader(source_engine, cdm, version=version)
        try:
            _asyncio.run(loader.load(mapping_path, batch_size=batch_size))
            click.echo("Migration complete.")
        except Exception as e:
            click.echo(f"Error during migration: {e}", err=True)
            sys.exit(1)
        finally:
            _asyncio.run(source_engine.dispose())
            _asyncio.run(cdm.dispose())

    if do_extract_schema:
        import sys

        click.echo(
            f"Extracting schema from {src_dbtype} database '{src_name}' to '{schema_output}'"
        )

        try:
            src_url = build_source_url(src_dbtype, src_name, src_host, src_port, src_user, src_pw)
        except ValueError:
            click.echo(
                f"Unknown --src-dbtype '{src_dbtype}'. Use sqlite, mysql, or pgsql.", err=True
            )
            sys.exit(1)

        source_engine = create_source_engine(src_url)
        try:
            import asyncio as _asyncio2

            output = _asyncio2.run(extract_schema_to_markdown(source_engine, schema_output))
            click.echo(f"Schema written to {output}")
        except Exception as e:
            click.echo(f"Error extracting schema: {e}", err=True)
            sys.exit(1)
        finally:
            import asyncio as _asyncio3

            _asyncio3.run(source_engine.dispose())


def main():
    """Entry point for the ``pyomop-migrate`` command."""
    migrate_cli()


if __name__ == "__main__":
    main()
