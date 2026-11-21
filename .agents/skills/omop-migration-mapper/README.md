# OMOP Migration Mapper Skill

A comprehensive agent skill for generating production-ready mapping schemas to transform any healthcare EMR or database to OHDSI OMOP CDM format.

## Overview

This skill automates the creation of a `mapping.json` file used by `pyomop-migrate` to load healthcare data from any source database into OMOP CDM (v5.4 or v6). It guides the agent through 8 steps: schema extraction, mapping skeleton creation, column mapping, concept mapping, special-case handling, validation, iteration, and handoff.

## What it does

- **Extracts** source database schema using `pyomop-migrate --extract-schema`.
- **Generates** a structured mapping JSON skeleton with table, column, and concept definitions.
- **Validates** mappings with staged small-scale migrations and data quality checks.
- **Iterates** to resolve missing concepts, referential integrity issues, and data type mismatches.
- **Hands off** a final mapping file and migration report ready for production use.

## Who should use this

- Healthcare data engineers migrating from Epic, Cerner, or any relational EHR to OMOP CDM.
- Research teams standardizing multi-hospital datasets to OHDSI standards.
- Data analysts needing deterministic, reproducible mappings for compliance or validation.

## Skill components

- **SKILL.md** — Core step-by-step workflow (8 steps with decision trees and error handling).
- **assets/mapping.template.json** — Pre-filled JSON template to copy and adapt.
- **scripts/validate_mapping.py** — Automated validation script (row counts, nulls, concept coverage, sample data quality).
- **references/schema-guides.md** — Guidance on interpreting source database schemas.
- **references/decisions.md** — Recurrent EMR-specific mapping patterns (Epic, Cerner, etc.).
- **examples/ehr-to-omop-sample.md** — Concrete example showing the full workflow.

## Quick start

1. **Provide source DB connection info** (or a pre-extracted schema markdown).
2. **Agent extracts schema** using `pyomop-migrate --extract-schema`.
3. **Agent creates mapping.json** using `assets/mapping.template.json` as a scaffold.
4. **Agent validates** with a small test load and `scripts/validate_mapping.py`.
5. **Agent iterates** until validation passes.
6. **Handoff:** Final `mapping.<site>.json` and `migration-report.md`.

## Prerequisites

- `pyomop` installed (with `pyomop-migrate` CLI available).
- Source database accessible (or schema markdown file).
- OMOP vocabulary loaded (via `pyomop --vocab <dir>` or OHDSI Athena).
- Target database (test environment; SQLite recommended for initial validation).

## Output files

- `mapping.<site>.json` — Final validated mapping file.
- `migration-report.md` — Summary of decisions, vocabulary mappings, unmapped codes, and caveats.
- `schema.md` — Source schema snapshot.
- `test_omop.db` (or similar) — Test database with small-scale load for validation.

## Example

See `examples/ehr-to-omop-sample.md` for a complete walkthrough of mapping a sample EHR to OMOP.

## References

- [OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)
- [pyomop-migrate User Guide](../../docs/pyomop_migrate.md)
- [OHDSI Athena (Concept Search)](https://athena.ohdsi.org/)
- [Agent Skills Best Practices](https://github.com/mgechev/skills-best-practices)
