"""LLM query utilities over the OMOP CDM schema.

This module wires llama-index components to an OMOP-aware ``CDMDatabase`` so
you can build semantic and SQL-first query engines that know about your CDM
tables. All LLM-related imports are optional and performed lazily at runtime.
"""

import importlib
from typing import Any

from .llm_engine import CDMDatabase


class CdmLLMQuery:
    """Helper that prepares an LLM-backed SQL query engine for OMOP.

    It constructs an object index of selected CDM tables and exposes a
    retriever-backed query engine that can generate SQL or run SQL-only queries
    depending on configuration.

    Args:
        sql_database: A ``CDMDatabase`` instance connected to the OMOP DB.
        llm: Optional LLM implementation to plug into llama-index settings.
        similarity_top_k: Top-k tables to retrieve for each query.
        embed_model: Embedding model - either a string (HuggingFace model name like
                    "BAAI/bge-small-en-v1.5") or an embedding model instance 
                    (e.g., HuggingFaceEmbedding, OpenAIEmbedding).
        **kwargs: Reserved for future expansion.
    """

    def __init__(
        self,
        sql_database: CDMDatabase,
        llm: Any = None,  # FIXME: type
        similarity_top_k: int = 1,
        embed_model: Any = "BAAI/bge-small-en-v1.5",  # Can be string or embedding model instance
        **kwargs: Any,
    ):
        # Lazy import optional dependencies so the package imports without them
        try:
            sql_query_mod = importlib.import_module(
                "llama_index.core.indices.struct_store.sql_query"
            )
            objects_mod = importlib.import_module("llama_index.core.objects")
            core_mod = importlib.import_module("llama_index.core")
            embed_mod = importlib.import_module("llama_index.embeddings.huggingface")

            SQLTableRetrieverQueryEngine = sql_query_mod.SQLTableRetrieverQueryEngine
            SQLTableNodeMapping = objects_mod.SQLTableNodeMapping
            ObjectIndex = objects_mod.ObjectIndex
            SQLTableSchema = objects_mod.SQLTableSchema
            VectorStoreIndex = core_mod.VectorStoreIndex
            Settings = core_mod.Settings
            HuggingFaceEmbedding = embed_mod.HuggingFaceEmbedding
        except Exception as e:  # pragma: no cover
            raise ImportError("Install 'pyomop[llm]' to use LLM query features.") from e
        self._sql_database = sql_database
        self._similarity_top_k = similarity_top_k
        
        # Handle embed_model as either a string (model name) or an instance
        if isinstance(embed_model, str):
            # If it's a string, create a HuggingFaceEmbedding instance
            self._embed_model = HuggingFaceEmbedding(model_name=embed_model)
        else:
            # If it's already an embedding model instance, use it directly
            self._embed_model = embed_model
        
        self._llm = llm
        Settings.llm = llm
        Settings.embed_model = self._embed_model
        self._table_node_mapping = SQLTableNodeMapping(sql_database)
        usable_tables = []
        if hasattr(sql_database, "usable_tables"):
            usable_tables = list(sql_database.usable_tables())  # type: ignore[attr-defined]
        elif hasattr(sql_database, "get_usable_table_names"):
            usable_tables = list(sql_database.get_usable_table_names())  # type: ignore[attr-defined]
        self._table_schema_objs = [
            SQLTableSchema(table_name=t) for t in sorted(set(usable_tables))
        ]

        self._object_index = ObjectIndex.from_objects(
            self._table_schema_objs,
            self._table_node_mapping,
            VectorStoreIndex,  # type: ignore
        )

        self._query_engine = SQLTableRetrieverQueryEngine(
            self._sql_database,
            self._object_index.as_retriever(similarity_top_k=self._similarity_top_k),
            sql_only=False,  # Set to False to enable query execution
        )

    @property
    def table_node_mapping(self) -> Any:
        """Mapping between tables and nodes used by the object index."""
        return self._table_node_mapping

    @property
    def table_schema_objs(self) -> list[Any]:
        """List of table schema objects indexed for retrieval."""
        return self._table_schema_objs

    @property
    def object_index(self) -> Any:
        """The underlying llama-index object index used for retrieval."""
        return self._object_index

    @property
    def query_engine(self) -> Any:
        """A retriever-backed SQL query engine over the CDM tables."""
        return self._query_engine
