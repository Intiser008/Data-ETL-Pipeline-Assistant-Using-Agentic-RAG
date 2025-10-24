"""Build Chroma index from project documentation."""

from __future__ import annotations

import argparse
import uuid
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from langchain.text_splitter import RecursiveCharacterTextSplitter

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


DOC_PATHS = [
    Path("schema_docs.md"),
    Path("glossary-v1.md"),
    Path("fewshots-v1.md"),
    Path("pandas_snippets.md"),
]


def load_documents() -> list[tuple[str, str]]:
    docs: list[tuple[str, str]] = []
    for path in DOC_PATHS:
        if not path.exists():
            logger.warning("Skipping missing doc: %s", path)
            continue
        docs.append((path.name, path.read_text(encoding="utf-8")))
    if not docs:
        raise RuntimeError("No documentation files found.")
    return docs


def split_documents(docs: list[tuple[str, str]]) -> list[dict[str, str]]:
    splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)
    chunks: list[dict[str, str]] = []
    for name, text in docs:
        for idx, chunk in enumerate(splitter.split_text(text)):
            chunks.append(
                {
                    "id": f"{name}-{idx}-{uuid.uuid4()}",
                    "source": name,
                    "content": chunk,
                }
            )
    logger.info("Generated %s chunks from %s documents", len(chunks), len(docs))
    return chunks


def build_index() -> None:
    settings = get_settings()
    docs = load_documents()
    chunks = split_documents(docs)

    client = chromadb.PersistentClient(path=settings.vector_store.persist_directory)

    embedding_deployment = settings.vector_store.embedding_deployment or settings.azure_openai.deployment_name
    embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
        api_base=settings.azure_openai.endpoint,
        api_key=settings.azure_openai.api_key,
        api_type="azure",
        api_version=settings.azure_openai.api_version,
        model_name=embedding_deployment,
    )

    collection = client.get_or_create_collection(
        name=settings.vector_store.collection_name,
        embedding_function=embedding_fn,
    )

    existing = collection.get(ids=None)
    existing_ids = existing.get("ids", []) if existing else []
    if existing_ids:
        collection.delete(ids=existing_ids)
        logger.info(
            "Cleared %s existing vectors from collection '%s'",
            len(existing_ids),
            settings.vector_store.collection_name,
        )

    collection.add(
        ids=[chunk["id"] for chunk in chunks],
        documents=[chunk["content"] for chunk in chunks],
        metadatas=[{"source": chunk["source"]} for chunk in chunks],
    )
    logger.info("Inserted %s chunks into Chroma collection '%s'", len(chunks), settings.vector_store.collection_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Chroma index from docs.")
    parser.parse_args()
    build_index()


if __name__ == "__main__":
    main()
