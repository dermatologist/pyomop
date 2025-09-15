# pyomop: OMOP Swiss Army Knife

pyomop is a Python library for working with OHDSI OMOP Common Data Model (CDM) databases using SQLAlchemy. It supports SQLite, PostgreSQL, and MySQL databases, provides utilities for working with OMOP vocabularies, and includes optional LLM-based natural language query capabilities.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Initial Setup and Development Environment
- Install uv package manager: `python3 -m pip install uv`
- Bootstrap the development environment:
  - `uv sync` -- takes 10 seconds, installs all core dependencies. NEVER CANCEL.
  - `uv run pre-commit install` -- takes 1 second, sets up git hooks
- Install with LLM support (optional):
  - `uv sync --all-extras` -- takes 43 seconds, includes LLM dependencies. NEVER CANCEL. Set timeout to 60+ minutes.

### Build and Test Commands
- **CRITICAL: NEVER CANCEL BUILDS OR TESTS** - Always use appropriate timeouts
- Run tests: `uv run python -m pytest tests --cov --cov-config=pyproject.toml --cov-report=xml -v` -- takes 49 seconds. NEVER CANCEL. Set timeout to 90+ minutes.
- Run specific test: `uv run python -m pytest tests/test_a_pyomop.py -v`
- Build package: `uvx --from build pyproject-build --installer uv` -- takes 1 second. Set timeout to 30+ minutes.
- Build documentation: `uv run mkdocs build` -- takes 1 second (non-strict mode works). Set timeout to 30+ minutes.
- **WARNING**: `uv run mkdocs build -s` fails in strict mode due to documentation warnings
- **WARNING**: `uv run pre-commit run -a` may fail due to network timeouts - this is normal in constrained environments

### Code Quality Tools
- Type checking: `uv run mypy` -- has 81 type errors but doesn't prevent functionality
- Linting: `uv run ruff check .` -- has 258 linting issues but doesn't prevent functionality
- Dependency checking: `uv run deptry src` -- shows some unused/missing dependencies but doesn't prevent functionality
- Lock file check: `uv lock --locked` -- takes under 1 second

### CLI Usage and Testing
- Test CLI help: `uv run pyomop --help`
- Create SQLite CDM: `uv run pyomop --create --version cdm54`
- Create with vocabulary: `uv run pyomop --create --version cdm54 --vocab tests/`
- Database types supported: sqlite (default), mysql, pgsql
- CLI creates `cdm.sqlite` database file in current directory

## Validation Scenarios

### Always validate changes with these scenarios:
1. **Basic functionality test**: Run the integration test: `uv run python t_install.py` -- takes 2 seconds
2. **CLI functionality**: Test CLI creation and help: `uv run pyomop --help && uv run pyomop --create --version cdm54`
3. **Package build**: Ensure package builds: `uvx --from build pyproject-build --installer uv`
4. **Test suite**: Run core tests: `uv run python -m pytest tests/test_a_pyomop.py tests/test_ab_vector.py -v`

### Manual Testing of Core Features
After making changes, always test the core functionality by running this example:
```python
import asyncio
from pyomop import CdmEngineFactory, CdmVector
from pyomop.cdm54 import Person, Base
from sqlalchemy.future import select

async def test():
    cdm = CdmEngineFactory()
    await cdm.init_models(Base.metadata)
    
    async with cdm.session() as session:
        async with session.begin():
            session.add(Person(person_id=100, gender_concept_id=8532, year_of_birth=1980))
        await session.commit()
        
    result = await session.execute(select(Person).where(Person.person_id == 100))
    person = result.scalar_one()
    assert person.person_id == 100
    print("✓ Core functionality works")
    
    await session.close()
    await cdm.engine.dispose()

asyncio.run(test())
```

## Project Structure and Key Components

### Core Modules (src/pyomop/)
- `engine_factory.py`: Database connection factory for SQLite, PostgreSQL, MySQL
- `vector.py`: Query execution and pandas DataFrame conversion utilities
- `vocabulary.py`: OMOP vocabulary management and CSV loading
- `loader.py`: FHIR to OMOP data loading functionality
- `main.py`: CLI entry point and command processing
- `cdm54/` and `cdm6/`: SQLAlchemy table definitions for CDM versions
- `llm_engine.py`, `llm_query.py`: Optional LLM integration (requires extras)

### Important Files
- `pyproject.toml`: Package configuration, dependencies, build settings
- `Makefile`: Convenient commands for common tasks
- `tox.ini`: Multi-environment testing configuration
- `tests/`: Comprehensive test suite with sample data files
- `examples/`: Usage examples including LLM integration
- `docker-compose.yml`: OHDSI stack setup (postgres, webapi, atlas)

### Build Artifacts and Outputs
- `dist/`: Built packages (created by build command)
- `site/`: Generated documentation (created by mkdocs)
- `cdm.sqlite`: Default SQLite database file created by CLI
- `.coverage.xml`: Test coverage report

## LLM Features and Limitations

### LLM Support
- Install with: `uv sync --all-extras` (includes llama-index, langchain, transformers)
- **WARNING**: LLM tests require internet access to Hugging Face models
- **WARNING**: `tests/test_e_llm_query.py` will fail in offline environments
- LLM functionality requires API keys (e.g., GOOGLE_GENAI_API_KEY for Gemini)
- See `examples/llm_example.py` for usage patterns

## Database Support and Docker

### Supported Databases
- SQLite: Default, no additional setup required
- PostgreSQL: Requires asyncpg (`pip install asyncpg`)
- MySQL: Requires aiomysql (`pip install aiomysql`)

### Docker Environment
- `docker-compose.yml` provides OHDSI stack (PostgreSQL, WebAPI, Atlas)
- Default ports: PostgreSQL (5432), WebAPI (9876), Atlas (8080)
- **WARNING**: Docker environment not validated in these instructions

## Known Issues and Workarounds

### Network-Related Issues
- Pre-commit hooks may fail due to network timeouts in constrained environments
- LLM functionality requires internet access for model downloads
- Some dependency resolution may be slow due to package registry access

### Code Quality Issues (Non-blocking)
- Mypy reports 81 type errors - primarily missing type annotations
- Ruff reports 258 linting issues - primarily formatting and import organization
- These do not prevent core functionality but should be addressed incrementally

### Build Considerations
- Documentation builds succeed in non-strict mode only
- Package builds successfully despite linting issues
- Test suite has good coverage (83%) and passes consistently

## Common Commands Reference

```bash
# Development setup
uv sync                                    # Install dependencies (10s)
uv sync --all-extras                      # Install with LLM support (43s)
uv run pre-commit install                 # Setup git hooks (1s)

# Testing and validation
uv run python -m pytest tests --cov -v   # Full test suite (49s)
uv run python t_install.py               # Integration test (2s)
uv run pyomop --help                      # CLI help
uv run pyomop --create --version cdm54    # Create CDM database

# Building and documentation
uvx --from build pyproject-build --installer uv  # Build package (1s)
uv run mkdocs build                       # Build docs (1s)

# Code quality (optional - has known issues)
uv run mypy                               # Type checking
uv run ruff check .                       # Linting
uv run deptry src                         # Dependency analysis
```

Always run the validation scenarios after making changes to ensure core functionality remains intact.