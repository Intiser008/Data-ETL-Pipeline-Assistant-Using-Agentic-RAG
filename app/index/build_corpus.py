"""Build Chroma index from project documentation."""

from __future__ import annotations

import argparse
import csv
import sys
import uuid
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import chromadb
from chromadb.utils import embedding_functions
try:
    from langchain.text_splitter import RecursiveCharacterTextSplitter
except ImportError:
    try:  # pragma: no cover - compatibility with newer LangChain packaging
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ImportError as exc:  # pragma: no cover - surface actionable guidance
        raise RuntimeError(
            "Install 'langchain-text-splitters' (pip install langchain-text-splitters) before building the corpus."
        ) from exc

from app.core.config import get_settings
from app.core.embeddings import LambdaEmbeddingFunction
from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


DOC_SOURCES = [
    Path("rag_docs"),
]
ALLOWED_EXTENSIONS = {".md", ".txt", ".csv"}


def load_documents() -> list[tuple[str, str]]:
    docs: list[tuple[str, str]] = []
    for path in _iter_doc_paths():
        try:
            content = _read_document(path)
        except Exception as exc:  # pragma: no cover - surface failing docs early
            logger.warning("Skipping doc %s due to read error: %s", path, exc)
            continue
        try:
            relative = path.relative_to(ROOT_DIR)
        except ValueError:
            relative = path
        docs.append((str(relative), content))
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

    azure = settings.azure_openai
    proxy_url = settings.vector_store.embedding_proxy_url
    if proxy_url:
        logger.info("Using embedding proxy at %s", proxy_url)
        embedding_fn = LambdaEmbeddingFunction(proxy_url)
    elif azure.is_configured:
        embedding_deployment = settings.vector_store.embedding_deployment or azure.deployment_name
        logger.info("Using Azure OpenAI embedding deployment '%s' for corpus build", embedding_deployment)
        embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
            api_base=azure.endpoint,
            api_key=azure.api_key,
            api_type="azure",
            api_version=azure.api_version,
            model_name=embedding_deployment,
        )
    else:
        logger.info("Azure OpenAI not configured; using default embedding function for corpus build.")
        embedding_fn = embedding_functions.DefaultEmbeddingFunction()

    collection_name = settings.vector_store.collection_name
    try:
        collection = client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_fn,
        )
    except ValueError as exc:
        if "embedding function conflict" in str(exc).lower():
            logger.info("Existing collection uses different embedding; recreating '%s'.", collection_name)
            client.delete_collection(collection_name)
            collection = client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_fn,
            )
        else:
            raise

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


def _iter_doc_paths():
    for source in DOC_SOURCES:
        if not source.exists():
            logger.warning("Doc source missing: %s", source)
            continue
        if source.is_dir():
            for path in source.rglob("*"):
                if path.is_file() and path.suffix.lower() in ALLOWED_EXTENSIONS:
                    yield path
        else:
            if source.suffix.lower() in ALLOWED_EXTENSIONS:
                yield source


def _read_document(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _csv_to_text(path)
    return path.read_text(encoding="utf-8")


def _csv_to_text(path: Path) -> str:
    with path.open("r", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        return f"{path.name} (empty)"

    fieldnames = [field.strip() for field in (reader.fieldnames or [])]
    normalized_fields = {field.lower() for field in fieldnames}

    # Special handling for NL->SQL examples
    if {"question", "sql"}.issubset(normalized_fields):
        sections = []
        for idx, row in enumerate(rows, 1):
            question = row.get("question", "").strip()
            sql = row.get("sql", "").strip()
            sections.append(
                f"Question #{idx}: {question}\nSQL:\n{sql}"
            )
        return f"Few-shot examples sourced from {path.name}\n\n" + "\n\n---\n\n".join(sections)

    # Fallback: serialize each row as key-value pairs
    serialized_rows = []
    for idx, row in enumerate(rows, 1):
        kv_pairs = "\n".join(
            f"{str(key).strip()}: {str(value).strip()}"
            for key, value in row.items()
            if value not in (None, "")
        )
    serialized_rows.append(f"Row #{idx}\n{kv_pairs}")
    return f"CSV contents for {path.name}\n\n" + "\n\n---\n\n".join(serialized_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Chroma index from docs.")
    parser.parse_args()
    build_index()


if __name__ == "__main__":
    main()
