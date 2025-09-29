# pyomop: OMOP Swiss Army Knife

pyomop is a Python library for working with OHDSI OMOP Common Data Model (CDM) databases using SQLAlchemy. It supports SQLite, PostgreSQL, and MySQL databases, provides utilities for working with OMOP vocabularies, and includes optional LLM-based natural language query capabilities.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively


### Build and Test Commands
- **CRITICAL: NEVER CANCEL BUILDS OR TESTS** - Always use appropriate timeouts not exceeding 10 minutes
- Use the virtual environment at ~/venv/default
- Run tests: `pytest tests --cov --cov-config=pyproject.toml --cov-report=xml -v` -- takes 49 seconds. NEVER CANCEL. Set timeout to 90+ minutes.
- Run specific test: `pytest tests/test_a_pyomop.py -v`
- Build documentation: `uv run mkdocs build` -- takes 1 second (non-strict mode works). Set timeout to 30+ minutes.
- **WARNING**: `uv run mkdocs build -s` fails in strict mode due to documentation warnings

### Code Quality Tools
- Type checking: `uv run mypy` -- has 81 type errors but doesn't prevent functionality
- Linting: `uv run ruff check .` -- has 258 linting issues but doesn't prevent functionality
- Dependency checking: `uv run deptry src` -- shows some unused/missing dependencies but doesn't prevent functionality
- Lock file check: `uv lock --locked` -- takes under 1 second

### CLI Usage and Testing
- Test CLI help: `uv run pyomop --help`
- Create SQLite CDM: `uv run pyomop --create --version cdm54`
- Database types supported: sqlite (default), mysql, pgsql
- CLI creates `cdm.sqlite` database file in current directory


## Project Structure and Key Components

### Core Modules (src/pyomop/)
- `engine_factory.py`: Database connection factory for SQLite, PostgreSQL, MySQL
- `vector.py`: Query execution and pandas DataFrame conversion utilities
- `vocabulary.py`: OMOP vocabulary management and CSV loading
- `loader.py`: FHIR to OMOP data loading functionality
- `main.py`: CLI entry point and command processing
- `cdm54/` and `cdm6/`: SQLAlchemy table definitions for CDM versions
- `llm_engine.py`, `llm_query.py`: Optional LLM integration (requires extras)


