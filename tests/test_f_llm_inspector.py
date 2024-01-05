import asyncio
import pytest
from llama_index.llms import Vertex
from src.pyomop.llm_inspector import LLMInspetor

def test_create_cohort(pyomop_fixture, metadata_fixture, capsys):
    table_names = metadata_fixture.tables.keys()
    print(table_names)




