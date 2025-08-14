import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncGenerator, Dict, List, Optional

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    cast,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
)
from sqlalchemy.ext.automap import AutomapBase, automap_base


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
        self._cdm = cdm_engine_factory
        self._engine = cdm_engine_factory.engine
        self._maker = async_sessionmaker(self._engine, class_=AsyncSession)
        self._scope = async_scoped_session(self._maker, scopefunc=asyncio.current_task)
        self._version = version

    @asynccontextmanager
    async def _get_session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._scope() as session:
            yield session

    async def _prepare_automap(self, conn: Connection) -> AutomapBase:
        automap: AutomapBase = automap_base()

        def _prepare(sync_conn):
            automap.prepare(autoload_with=sync_conn)

        await conn.run_sync(_prepare)
        return automap

    def _load_mapping(self, mapping_path: str) -> Dict[str, Any]:
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _apply_filters(self, df: pd.DataFrame, filters: Optional[List[Dict[str, Any]]]):
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
        self, csv_path: str, mapping_path: str, chunk_size: int = 1000
    ) -> None:
        """
        Load a CSV into multiple OMOP tables based on a mapping file.

        Args:
            csv_path: Path to the input CSV file.
            mapping_path: Path to the JSON mapping file.
            chunk_size: Batch size for INSERT statements.
        """
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
            await self.fix_person_id(session, automap)

            # Step 3: Backfill year/month/day of birth from birth_datetime where missing or zero
            await self.backfill_person_birth_fields(session, automap)

            # Step 4: Apply concept lookups defined in mapping["concept"]
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
        self, session: AsyncSession, automap: AutomapBase, mapping: Dict[str, Any]
    ) -> None:
        """
        Based on mapping["concept"], update target *_concept_id fields by looking up
        concept.concept_id where concept.concept_code equals the source field value.
        If the source is an array-like string, only the first element is used.
        """
        # Get vocabulary concept table
        try:
            concept_cls = getattr(automap.classes, "concept")
        except AttributeError:
            return
        concept_table = concept_cls.__table__

        concept_entries = mapping.get("concept") or []
        if not concept_entries:
            return

        def first_code(val: Any) -> Optional[str]:
            if val is None:
                return None
            if isinstance(val, (list, tuple)):
                return None if not val else str(val[0]).strip()
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return None
                # JSON array encoded as string
                if s.startswith("[") and s.endswith("]"):
                    try:
                        arr = json.loads(s)
                        if isinstance(arr, list) and arr:
                            return str(arr[0]).strip()
                    except Exception:
                        pass
                # Delimited list: try | then ,
                for sep in ("|", ","):
                    if sep in s:
                        return s.split(sep)[0].strip()
                return s
            return str(val)

        for entry in concept_entries:
            table_name = entry.get("table")
            mappings = entry.get("mappings") or []
            if not table_name or not mappings:
                continue
            try:
                tbl_cls = getattr(automap.classes, table_name)
            except AttributeError:
                continue
            table = tbl_cls.__table__

            for m in mappings:
                src_col_name = m.get("source")
                tgt_col_name = m.get("target")
                if not src_col_name or not tgt_col_name:
                    continue
                if src_col_name not in table.c or tgt_col_name not in table.c:
                    continue

                src_col = table.c[src_col_name]
                tgt_col = table.c[tgt_col_name]

                # Get distinct source values needing mapping
                res = await session.execute(
                    select(src_col)
                    .where(src_col.isnot(None), or_(tgt_col.is_(None), tgt_col == 0))
                    .distinct()
                )
                raw_values = [r[0] for r in res.fetchall() if r[0] is not None]
                # Example raw values: ["['5803-2']", "['8867-4']", "['5794-3']", "['74006-8']", "['10834-0']", "['1191']", "['442571000124108']", "['102263004']"]
                # Transform this into ['5803-2', '8867-4','5794-3', '10834-0', '1191', '442571000124108', '102263004']
                _raw_values = []
                for rv in raw_values:
                    # remove [ ' ' ]
                    rv = rv.strip("[]'\"")
                    _raw_values.append(rv)

                if not _raw_values:
                    continue

                # Compute first codes and list unique
                raw_to_first: Dict[str, Optional[str]] = {}
                first_list: List[str] = []
                for rv in _raw_values:
                    fc = first_code(rv)
                    raw_to_first[str(rv)] = fc
                    if fc:
                        first_list.append(fc)
                if not first_list:
                    continue

                # Lookup concept ids for these codes
                cres = await session.execute(
                    select(
                        concept_table.c.concept_code,
                        concept_table.c.concept_id,
                        concept_table.c.standard_concept,
                    ).where(concept_table.c.concept_code.in_(list(set(first_list))))
                )
                rows = cres.fetchall()
                if not rows:
                    continue

                # Choose best per code (prefer standard 'S')
                by_code: Dict[str, List[tuple]] = {}
                for code, cid, std in rows:
                    by_code.setdefault(code, []).append((cid, std))
                best_for_code: Dict[str, int] = {}
                for code, lst in by_code.items():
                    lst_sorted = sorted(
                        lst, key=lambda t: (0 if (t[1] or "") == "S" else 1, t[0])
                    )
                    best_for_code[code] = lst_sorted[0][0]

                # Map full raw value to chosen concept id via its first code
                raw_to_cid: Dict[str, int] = {}
                for rv, fc in raw_to_first.items():
                    if fc and fc in best_for_code:
                        raw_to_cid[rv] = best_for_code[fc]

                # Apply updates
                for rv, cid in raw_to_cid.items():
                    stmt = (
                        update(table)
                        .where(src_col == rv, or_(tgt_col.is_(None), tgt_col == 0))
                        .values({tgt_col.name: cid})
                    )
                    await session.execute(stmt)
