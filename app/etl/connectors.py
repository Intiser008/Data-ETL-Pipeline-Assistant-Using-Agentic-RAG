"""Connector abstractions for writing ETL outputs to various backends."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from app.core.logging import get_logger

logger = get_logger(__name__)


class StorageError(RuntimeError):
    """Raised when a storage connector fails."""


class LocalFileConnector:
    """Writes ETL outputs to the local filesystem."""

    def __init__(self, root_dir: str | Path):
        self._root = Path(root_dir)

    def write(self, table: str, df: pd.DataFrame, filename: str) -> Path:
        table_dir = self._root / table
        table_dir.mkdir(parents=True, exist_ok=True)
        output_path = table_dir / filename
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info("Wrote %s rows to %s", df.shape[0], output_path)
        return output_path


class S3Connector:
    """Uploads ETL outputs to Amazon S3."""

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        region_name: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region_name,
        )
        self._client = self._session.client("s3")

    def write(self, table: str, local_path: Path, *, key: Optional[str] = None) -> str:
        key_parts = [part for part in [self._prefix, table, local_path.name] if part]
        resolved_key = key or "/".join(key_parts)

        try:
            self._client.upload_file(str(local_path), self._bucket, resolved_key)
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"Failed to upload {local_path} to s3://{self._bucket}/{resolved_key}") from exc

        uri = f"s3://{self._bucket}/{resolved_key}"
        logger.info("Uploaded %s to %s", local_path, uri)
        return uri
