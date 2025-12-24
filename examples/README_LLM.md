# PyOMOP LLM Examples

This directory contains examples demonstrating how to use PyOMOP with Large Language Models (LLMs) to query OMOP Common Data Model (CDM) databases using natural language.

## Prerequisites

Install PyOMOP with LLM support:
```bash
pip install pyomop[llm]
```

## Setup

### 1. Create and Populate a Sample Database

Before running the examples, create a database with sample data:

```bash
# Create and populate with Synthea synthetic data (recommended for testing)
python -m pyomop -e Synthea27Nj -v 5.4 -n cdm.sqlite
```

This will:
- Download the Synthea27Nj dataset from the OHDSI EunomiaDatasets repository
- Create a SQLite database named `cdm.sqlite`
- Load approximately 28 patients with:
  - 470 condition occurrences
  - 883 drug exposures
  - 1,649 procedure occurrences
  - 10,040 measurements
  - 8,099 observations
  - 1,791 visit occurrences

### 2. Configure LLM API Keys

Set up your LLM provider's API key as an environment variable:

**Google Gemini** (used in examples):
```bash
export GOOGLE_API_KEY="your-api-key-here"
```

**OpenAI** (alternative):
```bash
export OPENAI_API_KEY="your-api-key-here"
```

**Anthropic Claude** (alternative):
```bash
export ANTHROPIC_API_KEY="your-api-key-here"
```

## Examples

### llm_example.py

A concise example demonstrating basic LLM query capabilities with PyOMOP.

**Features:**
- Connects to a populated OMOP CDM database
- Configures an LLM-powered query engine with key OMOP tables
- Executes sample natural language queries
- Demonstrates both LLM query execution and manual SQL verification

**Usage:**
```bash
python llm_example.py
```

### llm_example_improved.py

A comprehensive example with detailed documentation and advanced queries.

**Features:**
- Extensive inline documentation explaining OMOP CDM concepts
- Database statistics display
- Multiple example queries of varying complexity:
  - Simple patient counts
  - Demographic analysis (age distribution by gender)
  - Drug exposure analysis (most prescribed drugs)
  - Duration analysis (patients on long-term medications)
  - Condition-drug associations
  - Measurement summaries
  - Visit utilization analysis
- Well-structured output formatting
- Error handling and verification

**Usage:**
```bash
python llm_example_improved.py
```

## Important OMOP CDM Tables

The examples use these key OMOP CDM tables:

| Table | Description |
|-------|-------------|
| `person` | Patient demographics (age, gender, race, ethnicity) |
| `observation_period` | Time periods when patients are observed in the system |
| `visit_occurrence` | Healthcare visits and encounters (inpatient, outpatient, ER) |
| `condition_occurrence` | Diagnoses and medical conditions |
| `drug_exposure` | Medication prescriptions and administrations |
| `procedure_occurrence` | Medical procedures performed |
| `measurement` | Laboratory test results and vital signs |
| `observation` | General clinical observations |
| `death` | Mortality information |
| `concept` | Standardized medical vocabularies and terminologies |
| `provider` | Healthcare provider information |

## Query Examples

### Simple Queries
- "How many patients are in the database?"
- "Show me the age distribution of patients"
- "List all providers"

### Intermediate Queries
- "What are the most common conditions?"
- "Show me visit counts by type"
- "Find patients with diabetes"

### Complex Queries
- "What are the top 5 most prescribed drugs? Include drug names and counts."
- "Find patients on drug exposures longer than 30 days with start/end dates."
- "Show the most common drugs prescribed for patients with diabetes."
- "Analyze the relationship between age, gender, and common conditions."

## Switching LLM Providers

The examples use Google Gemini by default, but you can easily switch to other providers:

### OpenAI GPT-4
```python
from llama_index.llms.openai import OpenAI

llm = OpenAI(
    model="gpt-4",
    api_key=os.getenv("OPENAI_API_KEY"),
)
```

### Anthropic Claude
```python
from llama_index.llms.anthropic import Anthropic

llm = Anthropic(
    model="claude-3-opus-20240229",
    api_key=os.getenv("ANTHROPIC_API_KEY"),
)
```

### Local/Open Source Models (via Ollama)
```python
from llama_index.llms.ollama import Ollama

llm = Ollama(
    model="llama3",
    request_timeout=120.0,
)
```

## Customizing Embedding Models

The query engine uses HuggingFace embeddings by default. You can customize the embedding model in two ways:

### Option 1: Pass a Model Name (String)
```python
# Use a different HuggingFace model by name
query_engine = CdmLLMQuery(
    sql_database,
    llm=llm,
    similarity_top_k=3,
    embed_model="sentence-transformers/all-MiniLM-L6-v2"  # Smaller, faster model
).query_engine
```

### Option 2: Pass an Embedding Model Instance
```python
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.embeddings.openai import OpenAIEmbedding

# Use a custom HuggingFace embedding with specific settings
custom_embed = HuggingFaceEmbedding(
    model_name="BAAI/bge-large-en-v1.5",
    cache_folder="/path/to/cache"
)

query_engine = CdmLLMQuery(
    sql_database,
    llm=llm,
    similarity_top_k=3,
    embed_model=custom_embed
).query_engine

# Or use OpenAI embeddings (requires OpenAI API key)
openai_embed = OpenAIEmbedding(
    model="text-embedding-3-small",
    api_key=os.getenv("OPENAI_API_KEY")
)

query_engine = CdmLLMQuery(
    sql_database,
    llm=llm,
    similarity_top_k=3,
    embed_model=openai_embed
).query_engine
```

### Popular Embedding Models

| Model | Size | Performance | Use Case |
|-------|------|-------------|----------|
| `BAAI/bge-small-en-v1.5` | 33MB | Fast | Default, good balance |
| `BAAI/bge-large-en-v1.5` | 438MB | Best | High accuracy needed |
| `sentence-transformers/all-MiniLM-L6-v2` | 23MB | Fastest | Quick prototyping |
| `text-embedding-3-small` (OpenAI) | API | Good | Using OpenAI already |
```

## Understanding the Output

The examples display:
1. **LLM Response**: The natural language answer from the LLM
2. **Generated SQL**: The SQL query the LLM created to answer the question
3. **Query Results**: The actual data retrieved from the database

The LLM's query engine can:
- Automatically select relevant tables based on the query
- Generate syntactically correct SQL
- Join multiple tables when needed
- Apply filters and aggregations
- Return results in a human-readable format

## Database Connection Options

### SQLite (Local File)
```python
cdm = CdmEngineFactory(
    db="sqlite",
    name="cdm.sqlite",
)
```

### PostgreSQL
```python
cdm = CdmEngineFactory(
    db="pgsql",
    host="localhost",
    port=5432,
    user="username",
    pw="password",
    name="omop_cdm",
    schema="cdm54"
)
```

### MySQL
```python
cdm = CdmEngineFactory(
    db="mysql",
    host="localhost",
    port=3306,
    user="username",
    pw="password",
    name="omop_cdm",
)
```

## Troubleshooting

### "No module named 'llama_index'"
Install the LLM extras:
```bash
pip install pyomop[llm]
```

### "API key not found"
Ensure your LLM provider's API key is set as an environment variable:
```bash
export GOOGLE_API_KEY="your-key"
```

### "Table not found" errors
Ensure the database is properly populated:
```bash
python -m pyomop -e Synthea27Nj -v 5.4 -n cdm.sqlite
```

### Empty query results
Check that the database has data:
```bash
sqlite3 cdm.sqlite "SELECT COUNT(*) FROM person;"
```

## Additional Resources

- [OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)
- [OHDSI Eunomia Datasets](https://github.com/OHDSI/EunomiaDatasets)
- [LlamaIndex Documentation](https://docs.llamaindex.ai/)
- [PyOMOP Documentation](https://dermatologist.github.io/pyomop/)

## Contributing

Found an issue or have a suggestion? Please open an issue on the [PyOMOP GitHub repository](https://github.com/dermatologist/pyomop).
