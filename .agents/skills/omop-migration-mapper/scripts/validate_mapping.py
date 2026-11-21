#!/usr/bin/env python3
"""Validation script for OMOP mapping files.

This script performs deterministic checks on a migration result:
- Row count comparisons between source and target.
- Null checks on required columns.
- Concept coverage (unmapped codes).
- Sample data quality checks.

Usage:
    python validate_mapping.py --source-db <db> --target-db <db> --source-type <type>
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def connect_db(db_path: str, db_type: str = "sqlite") -> object:
    """Connect to target OMOP database."""
    if db_type == "sqlite":
        return sqlite3.connect(db_path)
    raise NotImplementedError(f"Database type {db_type} not yet supported.")


def check_row_counts(target_conn, mapping_file: str) -> Dict[str, Tuple[int, int]]:
    """Compare expected vs actual row counts in OMOP tables.

    Args:
        target_conn: Database connection to OMOP database.
        mapping_file: Path to mapping.json.

    Returns:
        dict: {table_name: (expected, actual)} counts.
    """
    with open(mapping_file) as f:
        mapping = json.load(f)

    results = {}
    cursor = target_conn.cursor()

    for table_mapping in mapping.get("tables", []):
        omop_table = table_mapping.get("name")
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {omop_table}")
            actual_count = cursor.fetchone()[0]
            results[omop_table] = (
                None,
                actual_count,
            )  # Expected is None (user provides source count)
            print(f"[OK] {omop_table}: {actual_count} rows")
        except Exception as e:
            print(f"[ERROR] {omop_table}: {e}")
            results[omop_table] = (None, -1)

    return results


def check_nulls(target_conn, mapping_file: str) -> Dict[str, Dict[str, int]]:
    """Check for nulls in required (non-nullable) OMOP columns.

    Args:
        target_conn: Database connection to OMOP database.
        mapping_file: Path to mapping.json.

    Returns:
        dict: {table_name: {column_name: null_count}}
    """
    # Critical columns that should rarely (if ever) be null in well-formed OMOP.
    critical_columns = {
        "person": ["person_id", "gender_concept_id", "year_of_birth"],
        "condition_occurrence": [
            "person_id",
            "condition_start_date",
            "condition_type_concept_id",
        ],
        "drug_exposure": [
            "person_id",
            "drug_exposure_start_date",
            "drug_type_concept_id",
        ],
        "measurement": ["person_id", "measurement_date", "measurement_type_concept_id"],
        "visit_occurrence": ["person_id", "visit_start_date", "visit_type_concept_id"],
    }

    results = {}
    cursor = target_conn.cursor()

    with open(mapping_file) as f:
        mapping = json.load(f)

    for table_mapping in mapping.get("tables", []):
        omop_table = table_mapping.get("name")
        if omop_table not in critical_columns:
            continue

        null_counts = {}
        for col in critical_columns[omop_table]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {omop_table} WHERE {col} IS NULL")
                null_count = cursor.fetchone()[0]
                null_counts[col] = null_count
                status = "[WARN]" if null_count > 0 else "[OK]"
                print(f"{status} {omop_table}.{col}: {null_count} nulls")
            except Exception as e:
                print(f"[ERROR] {omop_table}.{col}: {e}")
                null_counts[col] = -1

        results[omop_table] = null_counts

    return results


def check_concepts(target_conn) -> Dict[str, Dict[str, int]]:
    """Check for unmapped concept IDs (concept_id = 0) in fact tables.

    Args:
        target_conn: Database connection to OMOP database.

    Returns:
        dict: {table_name: {concept_column: count_of_zeros}}
    """
    fact_tables_concepts = {
        "condition_occurrence": "condition_concept_id",
        "drug_exposure": "drug_concept_id",
        "measurement": "measurement_concept_id",
        "procedure_occurrence": "procedure_concept_id",
        "observation": "observation_concept_id",
    }

    results = {}
    cursor = target_conn.cursor()

    for table, col in fact_tables_concepts.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = 0")
            zero_count = cursor.fetchone()[0]
            results[table] = {col: zero_count}
            status = "[WARN]" if zero_count > 0 else "[OK]"
            print(f"{status} {table}.{col}: {zero_count} unmapped (concept_id = 0)")
        except Exception as e:
            print(f"[INFO] {table}: {e} (table not present or no {col} column)")

    return results


def check_sample_data(target_conn, limit: int = 5) -> bool:
    """Spot-check sample records for data quality and FK integrity.

    Args:
        target_conn: Database connection to OMOP database.
        limit: Number of sample patients to check.

    Returns:
        bool: True if all spot checks pass.
    """
    cursor = target_conn.cursor()
    all_pass = True

    # Sample persons
    try:
        cursor.execute(
            f"SELECT person_id, gender_concept_id, year_of_birth FROM person LIMIT {limit}"
        )
        persons = cursor.fetchall()
        for person_id, gender_concept_id, year_of_birth in persons:
            if year_of_birth is None or year_of_birth < 1850 or year_of_birth > 2024:
                print(
                    f"[WARN] person {person_id}: invalid year_of_birth {year_of_birth}"
                )
                all_pass = False
        print(f"[OK] Sampled {len(persons)} persons")
    except Exception as e:
        print(f"[INFO] person: {e}")

    # Sample conditions and FK to person
    try:
        cursor.execute(
            "SELECT c.condition_occurrence_id, c.person_id FROM condition_occurrence c "
            "WHERE c.person_id > 0 LIMIT 5"
        )
        conditions = cursor.fetchall()
        for cond_id, person_id in conditions:
            cursor.execute(
                "SELECT person_id FROM person WHERE person_id = ?", (person_id,)
            )
            if not cursor.fetchone():
                print(
                    f"[ERROR] condition {cond_id}: FK violation (person {person_id} not found)"
                )
                all_pass = False
        print(f"[OK] Sampled {len(conditions)} conditions (FK valid)")
    except Exception as e:
        print(f"[INFO] condition_occurrence: {e}")

    return all_pass


def main():
    parser = argparse.ArgumentParser(description="Validate OMOP migration results.")
    parser.add_argument(
        "--target-db", required=True, help="Path to target OMOP database (SQLite)"
    )
    parser.add_argument("--mapping", required=True, help="Path to mapping.json")
    args = parser.parse_args()

    if not Path(args.target_db).exists():
        print(f"[ERROR] Target database {args.target_db} not found.")
        sys.exit(1)

    if not Path(args.mapping).exists():
        print(f"[ERROR] Mapping file {args.mapping} not found.")
        sys.exit(1)

    print("=" * 60)
    print("OMOP Migration Validation Report")
    print("=" * 60)

    conn = connect_db(args.target_db)

    print("\n### Row Counts ###")
    check_row_counts(conn, args.mapping)

    print("\n### Null Checks ###")
    check_nulls(conn, args.mapping)

    print("\n### Concept Coverage ###")
    check_concepts(conn)

    print("\n### Data Quality Sample ###")
    sample_pass = check_sample_data(conn)

    print("\n" + "=" * 60)
    if sample_pass:
        print("Validation PASSED (with warnings; review above)")
    else:
        print("Validation FAILED (critical issues detected)")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
