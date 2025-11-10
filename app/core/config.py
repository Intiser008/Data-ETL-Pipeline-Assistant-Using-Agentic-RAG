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

from dotenv import load_dotenv

load_dotenv()


def _get_env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    """Fetch an environment variable with optional required flag."""
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable '{name}' must be set.")
    return value


@dataclass(frozen=True)
class AzureOpenAISettings:
    """Azure OpenAI configuration."""

    endpoint: str | None = None
    api_key: str | None = None
    deployment_name: str | None = None
    api_version: str = "2023-12-01-preview"

    @property
    def is_configured(self) -> bool:
        """Return True when all required Azure values are present."""
        return bool(self.endpoint and self.api_key and self.deployment_name)


@dataclass(frozen=True)
class DatabaseSettings:
    """Database connection settings."""

    url: str
    pool_size: int = 5
    max_overflow: int = 5
    statement_timeout_ms: int | None = None


@dataclass(frozen=True)
class VectorStoreSettings:
    """Settings for the Chroma vector store used in retrieval."""

    persist_directory: str = ".dist/chroma"
    collection_name: str = "healthcare_demo_docs"
    embedding_deployment: str | None = None
    embedding_proxy_url: str | None = None


@dataclass(frozen=True)
class ETLSettings:
    """Settings for the raw JSON -> CSV -> S3 ETL pipeline."""

    raw_dir: str
    processed_dir: str
    schema_config_path: str | None = None
    s3_bucket: str | None = None
    s3_prefix: str = ""
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    enable_s3: bool = True
    max_records: int = 0
    enable_db_load: bool = False
    truncate_before_load: bool = False
    db_chunksize: int = 1000
    source_pattern: str = "*.json"
    manifest_path: str | None = None


@dataclass(frozen=True)
class CacheSettings:
    """Settings for the application cache (Redis)."""

    redis_url: str | None = None
    ttl_seconds: int = 3600


@dataclass(frozen=True)
class Settings:
    """Aggregated application settings."""

    azure_openai: AzureOpenAISettings
    database: DatabaseSettings
    vector_store: VectorStoreSettings
    etl: ETLSettings
    llm_proxy_url: str
    llm_timeout_seconds: float = 30.0
    default_result_limit: int = 1000
    agent_max_retries: int = 3
    cache: CacheSettings | None = None
    # Empty-result stability controls
    empty_result_stability_enabled: bool = True
    empty_result_min_attempts: int = 2
    sql_semantic_compare_via_llm: bool = False
    # Logging
    log_sql_text: bool = False
    # Vocabulary guidance for the SQL prompt
    vocabulary_guidance_enabled: bool = True
    vocabulary_guidance_extra: str | None = None
    # Retrieval preferences
    retrieval_top_k: int = 4
    retrieval_bias_schema_docs: bool = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""
    azure = AzureOpenAISettings(
        endpoint=_get_env("AZURE_OPENAI_ENDPOINT"),
        api_key=_get_env("AZURE_OPENAI_API_KEY"),
        deployment_name=_get_env("AZURE_OPENAI_DEPLOYMENT_NAME"),
        api_version=_get_env("AZURE_OPENAI_API_VERSION", "2023-12-01-preview"),
    )
    db = DatabaseSettings(
        url=_get_env("DATABASE_URL", required=True),
        pool_size=int(_get_env("DATABASE_POOL_SIZE", "5")),
        max_overflow=int(_get_env("DATABASE_MAX_OVERFLOW", "5")),
        statement_timeout_ms=int(_get_env("DATABASE_STATEMENT_TIMEOUT_MS", "0")) or None,
    )
    vs = VectorStoreSettings(
        persist_directory=_get_env("CHROMA_PERSIST_DIR", ".dist/chroma"),
        collection_name=_get_env("CHROMA_COLLECTION", "healthcare_demo_docs"),
        embedding_deployment=_get_env("CHROMA_EMBEDDING_DEPLOYMENT"),
        embedding_proxy_url=_get_env("EMBEDDING_PROXY_URL"),
    )
    etl = ETLSettings(
        raw_dir=_get_env("ETL_RAW_DIR", "data/raw"),
        processed_dir=_get_env("ETL_PROCESSED_DIR", "data/processed/etl"),
        schema_config_path=_get_env("ETL_SCHEMA_CONFIG"),
        s3_bucket=_get_env("S3_BUCKET"),
        s3_prefix=_get_env("S3_PREFIX", ""),
        aws_region=_get_env("AWS_REGION"),
        aws_access_key_id=_get_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_get_env("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=_get_env("AWS_SESSION_TOKEN"),
        enable_s3=_get_env("ETL_ENABLE_S3", "true").lower() not in {"false", "0", "no"},
        max_records=int(_get_env("ETL_MAX_RECORDS", "0")),
        enable_db_load=_get_env("ETL_ENABLE_DB_LOAD", "false").lower() in {"true", "1", "yes"},
        truncate_before_load=_get_env("ETL_DB_TRUNCATE", "false").lower() in {"true", "1", "yes"},
        db_chunksize=int(_get_env("ETL_DB_CHUNKSIZE", "1000")),
        source_pattern=_get_env("ETL_SOURCE_PATTERN", "*.json"),
        manifest_path=_get_env("ETL_MANIFEST_PATH"),
    )
    llm_proxy_url = _get_env("LLM_PROXY_URL", required=True)
    cache = CacheSettings(
        redis_url=_get_env("CACHE_REDIS_URL"),
        ttl_seconds=int(_get_env("CACHE_TTL_SECONDS", "3600")),
    )
    return Settings(
        azure_openai=azure,
        database=db,
        vector_store=vs,
        etl=etl,
        llm_proxy_url=llm_proxy_url,
        llm_timeout_seconds=float(_get_env("LLM_TIMEOUT_SECONDS", "30")),
        default_result_limit=int(_get_env("DEFAULT_RESULT_LIMIT", "1000")),
        agent_max_retries=int(_get_env("AGENT_MAX_RETRIES", "3")),
        cache=cache,
        empty_result_stability_enabled=_get_env("EMPTY_RESULT_STABILITY_ENABLED", "true").lower() in {"true", "1", "yes"},
        empty_result_min_attempts=int(_get_env("EMPTY_RESULT_MIN_ATTEMPTS", "2")),
        sql_semantic_compare_via_llm=_get_env("SQL_SEMANTIC_COMPARE_VIA_LLM", "false").lower() in {"true", "1", "yes"},
        log_sql_text=_get_env("LOG_SQL_TEXT", "false").lower() in {"true", "1", "yes"},
        vocabulary_guidance_enabled=_get_env("VOCAB_GUIDANCE_ENABLED", "true").lower() in {"true", "1", "yes"},
        vocabulary_guidance_extra=_get_env("VOCAB_GUIDANCE_EXTRA"),
        retrieval_top_k=int(_get_env("RETRIEVAL_TOP_K", "4")),
        retrieval_bias_schema_docs=_get_env("RETRIEVAL_BIAS_SCHEMA_DOCS", "true").lower() in {"true", "1", "yes"},
    )
