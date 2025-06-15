from typing import Any
from llama_index.core.indices.struct_store.sql_query import (
    SQLTableRetrieverQueryEngine,
)
from llama_index.core.objects import (
    SQLTableNodeMapping,
    ObjectIndex,
    SQLTableSchema,
)
from llama_index.core import VectorStoreIndex
from langchain_huggingface import HuggingFaceEmbeddings
from llama_index.core import Settings
from .llm_engine import CDMDatabase


class CdmLLMQuery:
    def __init__(
        self,
        sql_database: CDMDatabase,
        llm: Any = None,  # FIXME: type
        similarity_top_k: int = 1,
        embed_model: str = "BAAI/bge-small-en-v1.5",
        **kwargs: Any,
    ):
        self._sql_database = sql_database
        self._similarity_top_k = similarity_top_k
        self._embed_model = HuggingFaceEmbeddings(model_name=embed_model)
        self._llm = llm
        Settings.llm = llm
        Settings.embed_model = self._embed_model
        self._table_node_mapping = SQLTableNodeMapping(sql_database)
        self._table_schema_objs = [
            (SQLTableSchema(table_name="care_site")),
            (SQLTableSchema(table_name="condition_occurrence")),
            (SQLTableSchema(table_name="cohort")),
            (SQLTableSchema(table_name="concept")),
            (SQLTableSchema(table_name="death")),
            (SQLTableSchema(table_name="device_exposure")),
            (SQLTableSchema(table_name="drug_exposure")),
            (SQLTableSchema(table_name="location")),
            (SQLTableSchema(table_name="measurement")),
            (SQLTableSchema(table_name="observation")),
            (SQLTableSchema(table_name="observation_period")),
            (SQLTableSchema(table_name="person")),
            (SQLTableSchema(table_name="procedure_occurrence")),
            (SQLTableSchema(table_name="provider")),
            (SQLTableSchema(table_name="visit_occurrence")),
            (SQLTableSchema(table_name="note")),
        ]  # add a SQLTableSchema for each table

        self._object_index = ObjectIndex.from_objects(
            self._table_schema_objs,
            self._table_node_mapping,
            VectorStoreIndex,  # type: ignore
        )

        self._query_engine = SQLTableRetrieverQueryEngine(
            self._sql_database,
            self._object_index.as_retriever(similarity_top_k=1),
            sql_only=True,
        )

    @property
    def table_node_mapping(self) -> SQLTableNodeMapping:
        return self._table_node_mapping

    @property
    def table_schema_objs(self) -> list[SQLTableSchema]:
        return self._table_schema_objs

    @property
    def object_index(self) -> ObjectIndex:
        return self._object_index

    @property
    def query_engine(self) -> SQLTableRetrieverQueryEngine:
        return self._query_engine
