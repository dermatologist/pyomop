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
        embed_model: HuggingFace embedding model name.
        **kwargs: Reserved for future expansion.
    """

    def __init__(
        self,
        sql_database: CDMDatabase,
        llm: Any = None,  # FIXME: type
        similarity_top_k: int = 1,
        embed_model: str = "BAAI/bge-small-en-v1.5",
        **kwargs: Any,
    ):
        # Lazy import optional dependencies so the package imports without them
        try:
            sql_query_mod = importlib.import_module(
                "llama_index.core.indices.struct_store.sql_query"
            )
            objects_mod = importlib.import_module("llama_index.core.objects")
            core_mod = importlib.import_module("llama_index.core")
            hf_mod = importlib.import_module("langchain_huggingface")

            SQLTableRetrieverQueryEngine = getattr(
                sql_query_mod, "SQLTableRetrieverQueryEngine"
            )
            SQLTableNodeMapping = getattr(objects_mod, "SQLTableNodeMapping")
            ObjectIndex = getattr(objects_mod, "ObjectIndex")
            SQLTableSchema = getattr(objects_mod, "SQLTableSchema")
            VectorStoreIndex = getattr(core_mod, "VectorStoreIndex")
            Settings = getattr(core_mod, "Settings")
            HuggingFaceEmbeddings = getattr(hf_mod, "HuggingFaceEmbeddings")
        except Exception as e:  # pragma: no cover
            raise ImportError("Install 'pyomop[llm]' to use LLM query features.") from e
        self._sql_database = sql_database
        self._similarity_top_k = similarity_top_k
        self._embed_model = HuggingFaceEmbeddings(model_name=embed_model)
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
            self._object_index.as_retriever(similarity_top_k=1),
            sql_only=True,
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
