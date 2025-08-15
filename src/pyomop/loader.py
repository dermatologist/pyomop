"""CSV-to-OMOP loader.

This module implements a flexible CSV loader that can populate multiple OMOP
CDM tables according to a JSON mapping file. It also performs helpful cleanup
operations like foreign key normalization, birthdate backfilling, gender
mapping, and concept code lookups.
"""

import asyncio
import json

# setup logging
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    and_,
    cast,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
)
from sqlalchemy.ext.automap import AutomapBase, automap_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CdmCsvLoader:
    """
    Load a single CSV into multiple OMOP CDM tables using a JSON mapping file.

    Mapping file format (JSON):

    {
      "csv_key": "patient_id",            # optional, CSV column that contains the patient/person identifier
      "tables": [
        {
          "name": "cohort",              # target table name as in the database
          "filters": [                     # optional row filters applied to CSV before mapping
            {"column": "resourceType", "equals": "Encounter"}
          ],
          "columns": {                     # mapping of target_table_column -> value
            "cohort_definition_id": {"const": 1},              # constant value
            "subject_id": "patient_id",                         # copy from CSV column
            "cohort_start_date": "period.start",                # copy from CSV column
            "cohort_end_date": "period.end"                     # copy from CSV column
          }
        }
      ]
    }

    Notes:
      - Constants are provided via {"const": value}.
      - If a required column is missing from mapping, it's left as None (DB default or nullable required).
      - Primary keys that are Integer types will autoincrement where supported (SQLite/PostgreSQL typical behavior).
      - Dates/times are converted to proper Python types where possible based on reflected column types.
    """

    def __init__(self, cdm_engine_factory, version: str = "cdm54") -> None:
        """Create a loader bound to a specific database engine.

        Args:
            cdm_engine_factory: An initialized ``CdmEngineFactory``.
            version: OMOP CDM version label ("cdm54" or "cdm6").
        """
        self._cdm = cdm_engine_factory
        self._engine = cdm_engine_factory.engine
        self._maker = async_sessionmaker(self._engine, class_=AsyncSession)
        self._scope = async_scoped_session(self._maker, scopefunc=asyncio.current_task)
        self._version = version

    @asynccontextmanager
    async def _get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Yield a scoped async session bound to the engine."""
        async with self._scope() as session:
            yield session

    async def _prepare_automap(self, conn: AsyncConnection) -> AutomapBase:
        """Reflect the database and return an automapped base."""
        automap: AutomapBase = automap_base()

        def _prepare(sync_conn):
            automap.prepare(autoload_with=sync_conn)

        await conn.run_sync(_prepare)
        return automap

    def _load_mapping(self, mapping_path: str) -> Dict[str, Any]:
        """Load a JSON mapping file from disk."""
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _apply_filters(self, df: pd.DataFrame, filters: Optional[List[Dict[str, Any]]]):
        """Apply optional row filters to a DataFrame prior to mapping."""
        if not filters:
            return df
        mask = pd.Series([True] * len(df), index=df.index)
        for flt in filters:
            col = flt.get("column")
            if col is None or col not in df.columns:
                continue
            if "equals" in flt:
                mask &= (df[col] == flt["equals"]) | (
                    df[col].astype(str) == str(flt["equals"])
                )
            elif "not_empty" in flt and flt["not_empty"]:
                mask &= df[col].notna() & (df[col].astype(str).str.len() > 0)
        result = df.loc[mask, :].copy()
        return result

    def _convert_value(self, sa_type: Any, value: Any) -> Any:
        """Coerce a CSV value into an appropriate Python type for insert.

        The conversion is guided by the SQLAlchemy column type.
        """
        if pd.isna(value) or value == "":
            return None
        try:
            if isinstance(sa_type, Date):
                # Accept many input formats
                dt = pd.to_datetime(value, errors="coerce")
                return None if pd.isna(dt) else dt.date()
            if isinstance(sa_type, DateTime):
                dt = pd.to_datetime(value, errors="coerce")
                return None if pd.isna(dt) else dt.to_pydatetime()
            if isinstance(sa_type, (Integer, BigInteger)):
                return int(value)
            if isinstance(sa_type, Numeric):
                return Decimal(str(value))
        except Exception:
            return value
        # Trim string values to 50 characters before insert
        if isinstance(value, str):
            return value[:50]
        return value

    async def load(
        self, csv_path: str, mapping_path: str | None = None, chunk_size: int = 1000
    ) -> None:
        """Load a CSV into multiple OMOP tables based on a mapping file.

        Args:
            csv_path: Path to the input CSV file.
            mapping_path: Path to the JSON mapping file. Defaults to the
                package's ``mapping.default.json`` when not provided.
            chunk_size: Batch size for INSERT statements.
        """
        # If mapping path is None, load mapping.default.json from the current directory
        logger.info(f"Loading CSV data from {csv_path}")
        if mapping_path is None:
            mapping_path = str(Path(__file__).parent / "mapping.default.json")
        mapping = self._load_mapping(mapping_path)
        df = pd.read_csv(csv_path)

        async with self._get_session() as session:
            conn = await session.connection()
            automap = await self._prepare_automap(conn)

            for tbl in mapping.get("tables", []):
                table_name = tbl.get("name")
                if not table_name:
                    continue
                # obtain mapped class
                try:
                    mapper = getattr(automap.classes, table_name)
                except AttributeError:
                    raise ValueError(f"Table '{table_name}' not found in database.")

                # compute filtered dataframe
                df_tbl = self._apply_filters(df, tbl.get("filters"))
                if df_tbl.empty:
                    continue

                col_map: Dict[str, Any] = tbl.get("columns", {})
                # Gather target SQLA column types
                sa_cols = {c.name: c.type for c in mapper.__table__.columns}

                # Build records
                records: List[Dict[str, Any]] = []
                for _, row in df_tbl.iterrows():
                    rec: Dict[str, Any] = {}
                    for target_col, src in col_map.items():
                        if isinstance(src, dict) and "const" in src:
                            value = src["const"]
                        elif isinstance(src, str):
                            value = row.get(src)
                        else:
                            value = None

                        # Convert based on SA type if available
                        sa_t = sa_cols.get(target_col)
                        if sa_t is not None:
                            value = self._convert_value(sa_t, value)
                        rec[target_col] = value
                    records.append(rec)

                if not records:
                    continue

                stmt = insert(mapper)
                # Chunked insert
                for i in range(0, len(records), chunk_size):
                    batch = records[i : i + chunk_size]
                    await session.execute(stmt, batch)

            # Step 2: Normalize person_id FKs using person.person_id (not person_source_value)
            logger.info("Normalizing person_id foreign keys")
            await self.fix_person_id(session, automap)

            # Step 3: Backfill year/month/day of birth from birth_datetime where missing or zero
            logger.info("Backfilling person birth fields")
            await self.backfill_person_birth_fields(session, automap)

            # Step 4: Set gender_concept_id from gender_source_value using standard IDs
            logger.info("Setting person.gender_concept_id from gender_source_value")
            await self.update_person_gender_concept_id(session, automap)

            # Step 5: Apply concept mappings defined in the JSON mapping
            logger.info("Applying concept mappings")
            await self.apply_concept_mappings(session, automap, mapping)

            await session.commit()
            await session.close()

    async def fix_person_id(self, session: AsyncSession, automap: AutomapBase) -> None:
        """
        Update all tables so that person_id foreign keys store the canonical
        person.person_id (integer), replacing any rows where person_id currently
        contains the person_source_value (string/UUID).

        Approach:
        - Build a mapping from person_source_value -> person_id from the person table.
        - For each table (except person) having a person_id column, run updates:
                    SET person_id = person.person_id WHERE CAST(person_id AS TEXT) = person_source_value.
                - This is safe for SQLite (used in examples). For stricter RDBMS, ensure types
                    are compatible or adjust as needed.
        """
        # Resolve person table from automap
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return  # No person table; nothing to do

        person_table = person_cls.__table__

        # Build mapping of person_source_value -> person_id
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

        # Iterate all tables and update person_id where it matches a known person_source_value
        for table in automap.metadata.sorted_tables:
            if table.name == person_table.name:
                continue
            if "person_id" not in table.c:
                continue

            # Run per-psv updates; small and explicit for clarity
            for psv, pid in psv_to_id.items():
                stmt = (
                    update(table)
                    .where(cast(table.c.person_id, String()) == psv)
                    .values(person_id=pid)
                )
                await session.execute(stmt)

    async def update_person_gender_concept_id(
        self, session: AsyncSession, automap: AutomapBase
    ) -> None:
        """
            Update person.gender_concept_id from person.gender_source_value using static mapping:
            - male (or 'm')   -> 8507
            - female (or 'f') -> 8532
            - anything else   -> 0 (unknown)

        Only updates rows where the computed value differs from the current value
        or where gender_concept_id is NULL.
        """
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return

        person_table = person_cls.__table__

        # Fetch rows to evaluate. We consider all rows with a non-null gender_source_value
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

        def map_gender(val: str | None) -> int:
            if val is None:
                return 0
            s = str(val).strip().lower()
            if s in {"male", "m"}:
                return 8507
            if s in {"female", "f"}:
                return 8532
            return 0

        for pid, gsrc, gcid in rows:
            target = map_gender(gsrc)
            # Skip if already correct
            if gcid == target:
                continue
            stmt = (
                update(person_table)
                .where(person_table.c.person_id == pid)
                .values(gender_concept_id=target)
            )
            await session.execute(stmt)

    async def backfill_person_birth_fields(
        self, session: AsyncSession, automap: AutomapBase
    ) -> None:
        """
            In the person table, replace 0 or NULL values in year_of_birth, month_of_birth,
            and day_of_birth with values derived from birth_datetime.

        This runs in Python for portability across backends.
        """
        # Resolve person table from automap
        try:
            person_cls = getattr(automap.classes, "person")
        except AttributeError:
            return

        person_table = person_cls.__table__

        # Fetch necessary columns
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
            # Parse birth_dt to a datetime if needed
            bd: Optional[datetime]
            if isinstance(birth_dt, datetime):
                bd = birth_dt
            elif isinstance(birth_dt, date):
                bd = datetime(birth_dt.year, birth_dt.month, birth_dt.day)
            else:
                try:
                    tmp = pd.to_datetime(birth_dt, errors="coerce")
                    bd = None if pd.isna(tmp) else tmp.to_pydatetime()
                except Exception:
                    bd = None

            if bd is None:
                continue

            new_y = y if (y is not None and int(y or 0) != 0) else bd.year
            new_m = m if (m is not None and int(m or 0) != 0) else bd.month
            new_d = d if (d is not None and int(d or 0) != 0) else bd.day

            # Only update when something changes
            if new_y != y or new_m != m or new_d != d:
                stmt = (
                    update(person_table)
                    .where(person_table.c.person_id == pid)
                    .values(
                        year_of_birth=new_y,
                        month_of_birth=new_m,
                        day_of_birth=new_d,
                    )
                )
                await session.execute(stmt)

    async def apply_concept_mappings(
        self,
        session: AsyncSession,
        automap: AutomapBase,
        mapping: Dict[str, Any],
    ) -> None:
        """
            Based on the "concept" key in the mapping JSON, populate target *_concept_id columns
            by looking up concept.concept_id using codes found in the specified source column.

            Rules:
        - If the source value is a comma-separated string, use only the first element for lookup.
        - Find by equality on concept.concept_code.
        - Update the target column with the matching concept.concept_id.
        """
        if not mapping or "concept" not in mapping:
            return

        # Resolve concept table
        try:
            concept_cls = getattr(automap.classes, "concept")
        except AttributeError:
            return

        concept_table = concept_cls.__table__

        # Simple in-memory cache to avoid repeated lookups
        code_to_cid: Dict[str, Optional[int]] = {}

        async def lookup_concept_id(code: str) -> Optional[int]:
            if code in code_to_cid:
                return code_to_cid[code]
            res = await session.execute(
                select(concept_table.c.concept_id).where(
                    concept_table.c.concept_code == code
                )
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
                # Target table not found; skip
                continue

            table = mapper.__table__
            pk_cols = list(table.primary_key.columns)
            if not pk_cols:
                # Cannot safely update without a primary key
                continue

            for m in item.get("mappings", []):
                source_col = m.get("source")
                target_col = m.get("target")
                if not source_col or not target_col:
                    continue
                if source_col not in table.c or target_col not in table.c:
                    continue

                # Fetch candidate rows: target is NULL or 0, and source is not NULL/empty
                res = await session.execute(
                    select(
                        *pk_cols,
                        table.c[source_col].label("_src"),
                        table.c[target_col].label("_tgt"),
                    ).where(
                        or_(
                            table.c[target_col].is_(None),
                            table.c[target_col] == 0,
                        ),
                        table.c[source_col].isnot(None),
                    )
                )

                rows = res.fetchall()
                if not rows:
                    continue

                for row in rows:
                    # row is a tuple: (*pk_vals, _src, _tgt)
                    pk_vals = row[: len(pk_cols)]
                    src_val = row[len(pk_cols)]

                    # Only care about non-empty strings; if comma-separated, take first element
                    code: Optional[str] = None
                    if isinstance(src_val, str):
                        # Split on comma and strip whitespace
                        first = src_val.split(",")[0].strip()
                        code = first if first else None
                    elif isinstance(src_val, list) and src_val:
                        # If a list somehow made it into the DB, use first element's string
                        code = str(src_val[0])
                    else:
                        # Fallback to simple string conversion if it's a scalar
                        code = str(src_val) if src_val is not None else None

                    if not code:
                        continue

                    cid = await lookup_concept_id(code)
                    if cid is None:
                        continue

                    # Build WHERE with PK columns
                    where_clause = and_(
                        *[
                            (pk_col == pk_val)
                            for pk_col, pk_val in zip(pk_cols, pk_vals)
                        ]
                    )

                    stmt = update(table).where(where_clause).values({target_col: cid})
                    await session.execute(stmt)
