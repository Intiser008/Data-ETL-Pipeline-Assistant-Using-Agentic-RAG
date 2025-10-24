"""Application configuration helpers.

Centralises loading of environment variables so the rest of the codebase can
depend on typed settings instead of reaching into ``os.environ`` directly.

This module intentionally avoids external dependencies (e.g., Pydantic) to
keep bootstrap minimal. Environment values are read lazily and cached on the
``Settings`` singleton.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _get_env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    """Fetch an environment variable with optional required flag."""
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable '{name}' must be set.")
    return value


@dataclass(frozen=True)
class AzureOpenAISettings:
    """Azure OpenAI configuration."""

    endpoint: str
    api_key: str
    deployment_name: str
    api_version: str = "2023-12-01-preview"


@dataclass(frozen=True)
class DatabaseSettings:
    """Database connection settings."""

    url: str
    pool_size: int = 5
    max_overflow: int = 5


@dataclass(frozen=True)
class VectorStoreSettings:
    """Settings for the Chroma vector store used in retrieval."""

    persist_directory: str = ".dist/chroma"
    collection_name: str = "finance_demo_docs"
    embedding_deployment: str | None = None


@dataclass(frozen=True)
class Settings:
    """Aggregated application settings."""

    azure_openai: AzureOpenAISettings
    database: DatabaseSettings
    vector_store: VectorStoreSettings
    llm_proxy_url: str
    default_result_limit: int = 1000


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""
    azure = AzureOpenAISettings(
        endpoint=_get_env("AZURE_OPENAI_ENDPOINT", required=True),
        api_key=_get_env("AZURE_OPENAI_API_KEY", required=True),
        deployment_name=_get_env("AZURE_OPENAI_DEPLOYMENT_NAME", required=True),
        api_version=_get_env("AZURE_OPENAI_API_VERSION", "2023-12-01-preview"),
    )
    db = DatabaseSettings(
        url=_get_env("DATABASE_URL", required=True),
        pool_size=int(_get_env("DATABASE_POOL_SIZE", "5")),
        max_overflow=int(_get_env("DATABASE_MAX_OVERFLOW", "5")),
    )
    vs = VectorStoreSettings(
        persist_directory=_get_env("CHROMA_PERSIST_DIR", ".dist/chroma"),
        collection_name=_get_env("CHROMA_COLLECTION", "finance_demo_docs"),
        embedding_deployment=_get_env("CHROMA_EMBEDDING_DEPLOYMENT"),
    )
    llm_proxy_url = _get_env("LLM_PROXY_URL", required=True)
    return Settings(
        azure_openai=azure,
        database=db,
        vector_store=vs,
        llm_proxy_url=llm_proxy_url,
        default_result_limit=int(_get_env("DEFAULT_RESULT_LIMIT", "1000")),
    )
