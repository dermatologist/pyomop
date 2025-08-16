"""Vocabulary utilities for loading and querying OMOP vocab tables.

Provides helpers to import vocabulary CSVs into the database and to look up
concepts by id or code. Uses async SQLAlchemy sessions.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import AsyncGenerator

import numpy as np
import pandas as pd
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
)
from sqlalchemy.ext.automap import AutomapBase, automap_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CdmVocabulary(object):
    """Helpers for OMOP Vocabulary management and lookups.

    Args:
        cdm: An initialized ``CdmEngineFactory`` instance.
        version: CDM version string ("cdm54" or "cdm6"). Defaults to "cdm54".
    """

    def __init__(self, cdm, version="cdm54"):
        self._concept_id = 0
        self._concept_name = ""
        self._domain_id = ""
        self._vocabulary_id = ""
        self._concept_class_id = ""
        self._concept_code = ""
        self._cdm = cdm
        self._engine = cdm.engine
        self._maker = async_sessionmaker(self._engine, class_=AsyncSession)
        self._scope = async_scoped_session(self._maker, scopefunc=asyncio.current_task)
        self._version = version

    @property
    def concept_id(self):
        """Current concept_id for this helper (if set)."""
        return self._concept_id

    @property
    def concept_code(self):
        """Current concept_code for this helper (if set)."""
        return self._concept_code

    @property
    def concept_name(self):
        """Current concept_name for this helper (if set)."""
        return self._concept_name

    @property
    def vocabulary_id(self):
        """Current vocabulary_id for this helper (if set)."""
        return self._vocabulary_id

    @property
    def domain_id(self):
        """Current domain_id for this helper (if set)."""
        return self._domain_id

    @concept_id.setter
    def concept_id(self, concept_id):
        """Set the active concept context by concept_id.

        Side effects: populates concept name, domain, vocabulary, class, and code
        on this helper instance for convenience.

        Args:
            concept_id: The concept_id to fetch and set.
        """
        self._concept_id = concept_id
        _concept = asyncio.run(self.get_concept(concept_id))
        self._concept_name = _concept.concept_name
        self._domain_id = _concept.domain_id
        self._vocabulary_id = _concept.vocabulary_id
        self._concept_class_id = _concept.concept_class_id
        self._concept_code = _concept.concept_code

    async def get_concept(self, concept_id):
        """Fetch a concept row by id.

        Args:
            concept_id: Concept identifier.

        Returns:
            The ORM Concept instance.
        """
        if self._version == "cdm6":
            from .cdm6 import Concept
        else:
            from .cdm54 import Concept
        stmt = select(Concept).where(Concept.concept_id == concept_id)
        async with self._cdm.session() as session:
            _concept = await session.execute(stmt)
        return _concept.scalar_one()

    async def get_concept_by_code(self, concept_code, vocabulary_id):
        """Fetch a concept by code within a vocabulary.

        Args:
            concept_code: The vocabulary-specific code string.
            vocabulary_id: Vocabulary identifier (e.g., 'SNOMED', 'LOINC').

        Returns:
            The ORM Concept instance.
        """
        if self._version == "cdm6":
            from .cdm6 import Concept
        else:
            from .cdm54 import Concept
        stmt = (
            select(Concept)
            .where(Concept.concept_code == concept_code)
            .where(Concept.vocabulary_id == vocabulary_id)
        )
        async with self._cdm.session() as session:
            _concept = await session.execute(stmt)
        return _concept.scalar_one()

    def set_concept(self, concept_code, vocabulary_id=None):
        """Set the active concept context by code and vocabulary.

        Args:
            concept_code: The concept code string to resolve.
            vocabulary_id: Vocabulary identifier. Required.

        Notes:
            On success, populates concept fields on this instance. On failure,
            sets ``_vocabulary_id`` and ``_concept_id`` to 0.
        """
        self._concept_code = concept_code
        try:
            if vocabulary_id is not None:
                self._vocabulary_id = vocabulary_id
                _concept = asyncio.run(
                    self.get_concept_by_code(concept_code, vocabulary_id)
                )
            else:
                raise ValueError(
                    "vocabulary_id must be provided when setting concept by code."
                )

            self._concept_name = _concept.concept_name
            self._domain_id = _concept.domain_id
            self._concept_id = _concept.concept_id
            self._concept_class_id = _concept.concept_class_id
            self._concept_code = _concept.concept_code

        except:
            self._vocabulary_id = 0
            self._concept_id = 0

    async def create_vocab(self, folder, sample=None):
        """Load vocabulary CSV files from a folder into the database.

        This imports the standard OMOP vocab tables (drug_strength, concept,
        concept_relationship, concept_ancestor, concept_synonym, vocabulary,
        relationship, concept_class, domain).

        Args:
            folder: Path to the folder containing OMOP vocabulary CSVs.
            sample: Optional number of rows to limit per file during import.
        """
        try:
            # Parents first (for concept FKs): DOMAIN, CONCEPT_CLASS
            df = pd.read_csv(
                folder + "/DOMAIN.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "domain", "replace")

            df = pd.read_csv(
                folder + "/CONCEPT_CLASS.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "concept_class", "replace")

            # Then CONCEPT (uses domain_id and concept_class_id)
            df = pd.read_csv(
                folder + "/CONCEPT.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            await self.write_vocab(df, "concept", "replace")

            # Then VOCABULARY (uses vocabulary_concept_id -> concept)
            df = pd.read_csv(
                folder + "/VOCABULARY.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "vocabulary", "replace")

            # Relationship depends on concept for relationship_concept_id
            df = pd.read_csv(
                folder + "/RELATIONSHIP.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "relationship", "replace")

            # Post-concept tables
            df = pd.read_csv(
                folder + "/CONCEPT_RELATIONSHIP.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            await self.write_vocab(df, "concept_relationship", "replace")

            df = pd.read_csv(
                folder + "/CONCEPT_SYNONYM.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "concept_synonym", "replace")

            df = pd.read_csv(
                folder + "/DRUG_STRENGTH.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            df["valid_start_date"] = pd.to_datetime(
                df["valid_start_date"], errors="coerce"
            )
            df["valid_end_date"] = pd.to_datetime(df["valid_end_date"], errors="coerce")
            await self.write_vocab(df, "drug_strength", "replace")

            df = pd.read_csv(
                folder + "/CONCEPT_ANCESTOR.csv",
                sep="\t",
                nrows=sample,
                on_bad_lines="skip",
                low_memory=False,
            )
            await self.write_vocab(df, "concept_ancestor", "replace")
        except Exception as e:
            logger.error(f"An error occurred while creating the vocabulary: {e}")

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Yield an async session bound to the current engine.

        Yields:
            AsyncSession: An async SQLAlchemy session.
        """
        async with self._scope() as session:
            yield session

    async def write_vocab(self, df, table, if_exists="replace", chunk_size=1000):
        """Write a DataFrame to a vocabulary table with type-safe defaults.

        Ensures required columns exist with reasonable defaults, coerces types,
        and performs chunked inserts via SQLAlchemy core for performance.

        Args:
            df: Pandas DataFrame with data to insert.
            table: Target table name (e.g., 'concept').
            if_exists: Compatibility only. This method always inserts.
            chunk_size: Number of rows per batch insert.
        """
        async with self.get_session() as session:
            # For PostgreSQL, temporarily relax constraint enforcement during bulk loads
            is_pg = False
            try:
                is_pg = self._engine.dialect.name.startswith("postgres")
            except Exception:
                is_pg = False
            if is_pg:
                logger.info(
                    "Temporarily disabling replication role for bulk load on postgres"
                )
                try:
                    await session.execute(
                        text("SET session_replication_role = replica")
                    )
                except Exception:
                    # Ignore if not permitted or unsupported
                    logger.warning("Failed to set session_replication_role to replica")

            conn = await session.connection()
            automap: AutomapBase = automap_base()

            def prepare_automap(sync_conn):
                automap.prepare(autoload_with=sync_conn)

            await conn.run_sync(prepare_automap)
            mapper = getattr(automap.classes, table)

            # Build defaults for non-nullable columns based on SQL types
            sa_cols = {c.name: c for c in mapper.__table__.columns}

            def default_for(col):
                from sqlalchemy import (
                    BigInteger,
                    Date,
                    DateTime,
                    Integer,
                    Numeric,
                    String,
                    Text,
                )

                t = col.type
                if isinstance(t, (Integer, BigInteger)):
                    return 0
                if isinstance(t, Numeric):
                    return 0
                if isinstance(t, (String, Text)):
                    return "UNKNOWN"
                if isinstance(t, Date):
                    return date(1970, 1, 1)
                if isinstance(t, DateTime):
                    return datetime(1970, 1, 1)
                return None

            # Work on a copy so we can normalize types and fill required fields
            df2 = df.copy()

            for name, col in sa_cols.items():
                # Ensure column exists
                if name not in df2.columns:
                    # For nullable columns, start with None; for required, use default
                    df2[name] = None if col.nullable else default_for(col)
                    continue

                # Coerce types and handle missing values
                if str(df2[name].dtype) == "object":
                    # Treat empty strings as missing
                    df2[name] = df2[name].replace("", np.nan)

                from sqlalchemy import BigInteger
                from sqlalchemy import Date as SA_Date
                from sqlalchemy import DateTime as SA_DateTime
                from sqlalchemy import Integer, Numeric, String, Text

                t = col.type
                if isinstance(t, SA_Date):
                    ser = pd.to_datetime(df2[name], errors="coerce").dt.date
                    df2[name] = (
                        ser.where(pd.notna(ser), None)
                        if col.nullable
                        else ser.fillna(default_for(col))
                    )
                elif isinstance(t, SA_DateTime):
                    # Normalize to UTC-naive to avoid tz-aware vs tz-naive issues in Postgres
                    ser = pd.to_datetime(df2[name], errors="coerce", utc=True)

                    # Convert to Python datetime and drop tzinfo
                    def _to_naive(dt):
                        try:
                            if pd.isna(dt):
                                return None
                        except Exception:
                            pass
                        if hasattr(dt, "to_pydatetime"):
                            py = dt.to_pydatetime()
                        else:
                            py = dt
                        if getattr(py, "tzinfo", None) is not None:
                            py = (
                                py.tz_convert("UTC").tz_localize(None)
                                if hasattr(py, "tz_convert")
                                else py.replace(tzinfo=None)
                            )
                        return py

                    ser = ser.map(_to_naive)
                    df2[name] = (
                        ser.where(pd.notna(ser), None)
                        if col.nullable
                        else ser.fillna(default_for(col))
                    )
                elif isinstance(t, (Integer, BigInteger)):
                    ser = pd.to_numeric(df2[name], errors="coerce")
                    df2[name] = (
                        ser.where(pd.notna(ser), None)
                        if col.nullable
                        else ser.fillna(default_for(col))
                    )
                elif isinstance(t, Numeric):
                    ser = pd.to_numeric(df2[name], errors="coerce")
                    df2[name] = (
                        ser.where(pd.notna(ser), None)
                        if col.nullable
                        else ser.fillna(default_for(col))
                    )
                elif isinstance(t, (String, Text)):
                    # Only cast non-null values to str and trim; keep nulls as None
                    ser = df2[name].astype(object)
                    mask = ser.notna()
                    ser.loc[mask] = ser.loc[mask].astype(str).str.slice(0, 255)
                    if col.nullable:
                        ser = ser.where(pd.notna(ser), None)
                    else:
                        # Required string columns get a default
                        ser = ser.where(pd.notna(ser), default_for(col))
                    df2[name] = ser
                else:
                    # Fallback: ensure NaN/NaT -> None for nullable cols, else fill default
                    df2[name] = (
                        df2[name].where(pd.notna(df2[name]), None)
                        if col.nullable
                        else df2[name].fillna(default_for(col))
                    )

            # Final safety pass: replace any remaining NaN/NaT with None across all columns
            df2 = df2.where(pd.notna(df2), None)

            stmt = insert(mapper)

            try:
                for _, group in df2.groupby(
                    np.arange(df2.shape[0], dtype=int) // chunk_size
                ):
                    records = group.to_dict("records")
                    try:
                        # Fast path: batch insert
                        await session.execute(stmt, records)
                    except Exception:
                        logger.warning(
                            "Batch insert failed, falling back to row-by-row insert."
                        )
                        # Fallback: insert row-by-row, skipping bad rows
                        for row in records:
                            try:
                                await session.execute(stmt, [row])
                            except Exception:
                                # Ignore duplicates/FK issues per row
                                logger.warning(
                                    f"Failed to insert row: {row}. Skipping."
                                )
                                continue
                    # Commit after each group
                    await session.commit()
            finally:
                if is_pg:
                    try:
                        await session.execute(
                            text("SET session_replication_role = origin")
                        )
                    except Exception:
                        pass
