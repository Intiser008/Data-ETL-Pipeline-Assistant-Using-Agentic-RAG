"""Chroma-based retriever for schema and business documentation."""

from __future__ import annotations

from typing import List

import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.utils import embedding_functions

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class RetrievalError(RuntimeError):
    """Raised when the retriever cannot fetch context."""


class ChromaRetriever:
    """Wrapper around Chroma persistent collection for document retrieval."""

    def __init__(self) -> None:
        settings = get_settings().vector_store
        logger.info("Connecting to ChromaDB at %s", settings.persist_directory)
        self._client = chromadb.PersistentClient(path=settings.persist_directory)

        embedding_deployment = settings.embedding_deployment or get_settings().azure_openai.deployment_name
        azure = get_settings().azure_openai

        self._embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
            api_base=azure.endpoint,
            api_key=azure.api_key,
            api_type="azure",
            api_version=azure.api_version,
            model_name=embedding_deployment,
        )

        self._collection = self._client.get_or_create_collection(
            name=settings.collection_name,
            embedding_function=self._embedding_fn,
        )

    @property
    def collection(self) -> Collection:
        return self._collection

    def retrieve(self, query: str, *, top_k: int = 4) -> List[str]:
        """Return relevant context chunks for the provided query."""
        results = self._collection.query(
            query_texts=[query],
            n_results=top_k,
        )
        documents = results.get("documents")
        if not documents or not documents[0]:
            raise RetrievalError("No documents retrieved from vector store.")
        return documents[0]

