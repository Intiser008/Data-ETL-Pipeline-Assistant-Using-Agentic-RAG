"""Embedding function adapters used by retriever and indexing scripts."""

from __future__ import annotations

from typing import Iterable, List, Sequence

import httpx

from app.core.logging import get_logger

logger = get_logger(__name__)

class LambdaEmbeddingFunction:
    """Call an HTTP embedding proxy that returns vectors for provided inputs."""

    def __init__(self, url: str, *, timeout: float = 30.0) -> None:
        self._url = url
        self._client = httpx.Client(timeout=timeout)

    def __call__(self, input: Sequence[str] | str) -> List[List[float]]:
        if isinstance(input, str):
            texts = [input]
        else:
            texts = list(input)
        return self.embed_documents(texts)

    def embed_documents(self, texts: Sequence[str] | None = None, **_: str) -> List[List[float]]:
        if texts is None:
            texts = []
        return self._embed(list(texts))

    def embed_query(self, input: str | Sequence[str], **_: str) -> List[List[float]]:
        if isinstance(input, str):
            items = [input]
        else:
            items = list(input)
        return self._embed(items)

    def _embed(self, inputs: Sequence[str]) -> List[List[float]]:
        if not inputs:
            return []

        payload = {"inputs": list(inputs)}
        logger.debug("Embedding proxy request: %s", payload)
        response = self._client.post(self._url, json=payload)
        if response.status_code >= 400:
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise RuntimeError(f"Embedding proxy request failed ({response.status_code}): {detail}")

        data = response.json()
        embeddings = data.get("embeddings")
        if embeddings is None:
            raise RuntimeError("Embedding proxy response missing 'embeddings' key.")

        if embeddings and isinstance(embeddings[0], (float, int)):
            return [embeddings]
        return embeddings

    def name(self) -> str:
        """Return identifier used by Chroma to detect embedding configuration."""
        return "lambda_embedding_proxy"

    def close(self) -> None:
        self._client.close()
