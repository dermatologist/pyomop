from typing import Any
from llama_index.indices.struct_store.sql_query import (
    SQLTableRetrieverQueryEngine,
)
from llama_index.objects import (
    SQLTableNodeMapping,
    ObjectIndex,
    SQLTableSchema,
)
from llama_index import VectorStoreIndex
from llama_index import SQLDatabase, ServiceContext
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from llama_index.prompts import BasePromptTemplate, PromptTemplate
from llama_index.objects.base import ObjectRetriever


class LLMQueryEngine(SQLTableRetrieverQueryEngine):
    def __init__(
        self,
            sql_database: SQLDatabase,
            table_retriever: ObjectRetriever[SQLTableSchema],
            text_to_sql_prompt: Optional[BasePromptTemplate] = None,
            context_query_kwargs: Optional[dict] = None,
            synthesize_response: bool = True,
            response_synthesis_prompt: Optional[BasePromptTemplate] = None,
            service_context: Optional[ServiceContext] = None,
            context_str_prefix: Optional[str] = None,
            sql_only: bool = False,
            llm: Optional[LLM] = None,
            **kwargs: Any,
        ):
        super().__init__(
            sql_database,
            table_retriever,
            text_to_sql_prompt,
            context_query_kwargs,
            synthesize_response,
            response_synthesis_prompt,
            service_context,
            context_str_prefix,
            sql_only,
            **kwargs,
        )
        self._llm = kwargs.get('llm')
        self._sql_database = sql_database
        self._table_node_mapping = SQLTableNodeMapping(sql_database)
        self._table_schema_objs = [
            (SQLTableSchema(table_name='care_site')),
            (SQLTableSchema(table_name='condition_occurrence')),
            (SQLTableSchema(table_name='cohort')),
            (SQLTableSchema(table_name='concept')),
            (SQLTableSchema(table_name='death')),
            (SQLTableSchema(table_name='device_exposure')),
            (SQLTableSchema(table_name='drug_exposure')),
            (SQLTableSchema(table_name='location')),
            (SQLTableSchema(table_name='measurement')),
            (SQLTableSchema(table_name='observation')),
            (SQLTableSchema(table_name='observation_period')),
            (SQLTableSchema(table_name='person')),
            (SQLTableSchema(table_name='procedure_occurrence')),
            (SQLTableSchema(table_name='provider')),
            (SQLTableSchema(table_name='visit_occurrence')),
            (SQLTableSchema(table_name='note'))
        ]  # add a SQLTableSchema for each table

        self._object_index = ObjectIndex.from_objects(
            self._table_schema_objs,
            self._table_node_mapping,
            VectorStoreIndex,
        )



    def _get_table_node_mapping(self, table_name: str) -> SQLTableNodeMapping:
        return self._index._table_node_mappings[table_name]

    def _get_table_schema(self, table_name: str) -> SQLTableSchema:
        return self._index._table_schemas[table_name]

    def _get_object_index(self, table_name: str) -> ObjectIndex:
        return self._index._object_indices[table_name]