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
from llama_index import ServiceContext
from typing import Any, Optional
from llama_index.prompts import BasePromptTemplate
from llama_index.objects.base import ObjectRetriever
from langchain.embeddings import HuggingFaceEmbeddings
from .llm_engine import CDMDatabase

class CdmLLMQuery(SQLTableRetrieverQueryEngine):
    def __init__(
        self,
            sql_database: CDMDatabase,
            table_retriever: ObjectRetriever[SQLTableSchema] = None,
            text_to_sql_prompt: Optional[BasePromptTemplate] = None,
            context_query_kwargs: Optional[dict] = None,
            synthesize_response: bool = True,
            response_synthesis_prompt: Optional[BasePromptTemplate] = None,
            service_context: Optional[ServiceContext] = None,
            context_str_prefix: Optional[str] = None,
            sql_only: bool = False,
            llm: Optional[Any] = None, # FIXME: type
            similarity_top_k: int = 1,
            **kwargs: Any,
        ):
        self._sql_database = sql_database
        self._similarity_top_k = similarity_top_k
        embed_model = HuggingFaceEmbeddings(model_name="BAAI/bge-small-en-v1.5")

        if service_context is None:
            if llm is None:
                raise ValueError("Must provide either llm or service_context")
            service_context = ServiceContext.from_defaults(
                llm=llm,
                embed_model=embed_model,
            )
            self._llm = llm

        self._service_context = service_context

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
            service_context=self._service_context,
        )

        if table_retriever is None:
            table_retriever = self._object_index.as_retriever(similarity_top_k=similarity_top_k)

        self._table_retriever = table_retriever


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

