"""Chroma-based retriever for schema and business documentation."""

from __future__ import annotations

from typing import List

import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.utils import embedding_functions

from app.core.config import get_settings
from app.core.embeddings import LambdaEmbeddingFunction
from app.core.logging import get_logger

logger = get_logger(__name__)


class RetrievalError(RuntimeError):
    """Raised when the retriever cannot fetch context."""


class ChromaRetriever:
    """Wrapper around Chroma persistent collection for document retrieval."""

    def __init__(self) -> None:
        settings = get_settings()
        vector_store = settings.vector_store
        logger.info("Connecting to ChromaDB at %s", vector_store.persist_directory)
        self._client = chromadb.PersistentClient(path=vector_store.persist_directory)

        azure = settings.azure_openai
        proxy_url = vector_store.embedding_proxy_url

        if proxy_url:
            logger.info("Using embedding proxy at %s", proxy_url)
            embedding_fn = LambdaEmbeddingFunction(proxy_url)
        elif azure.is_configured:
            embedding_deployment = vector_store.embedding_deployment or azure.deployment_name
            logger.info("Using Azure OpenAI embedding deployment '%s'", embedding_deployment)
            embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
                api_base=azure.endpoint,
                api_key=azure.api_key,
                api_type="azure",
                api_version=azure.api_version,
                model_name=embedding_deployment,
            )
        else:
            logger.info("Azure OpenAI not configured; falling back to default embedding function.")
            embedding_fn = embedding_functions.DefaultEmbeddingFunction()

        self._embedding_fn = embedding_fn

        collection_name = vector_store.collection_name
        try:
            collection = self._client.get_or_create_collection(
                name=collection_name,
                embedding_function=self._embedding_fn,
            )
        except ValueError as exc:
            if "embedding function conflict" in str(exc).lower():
                logger.info("Existing collection uses different embedding; recreating '%s'.", collection_name)
                self._client.delete_collection(collection_name)
                collection = self._client.get_or_create_collection(
                    name=collection_name,
                    embedding_function=self._embedding_fn,
                )
            else:
                raise

        self._collection = collection

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

