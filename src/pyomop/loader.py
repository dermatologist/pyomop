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
