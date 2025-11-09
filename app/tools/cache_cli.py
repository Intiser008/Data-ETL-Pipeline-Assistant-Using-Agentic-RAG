from __future__ import annotations

import argparse
from pathlib import Path

from app.core.cache import get_client
from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

ETL_CACHE_PATH = Path(".cache/etl_cache.json")
REPAIR_CACHE_PATH = Path(".cache/etl_repair_knowledge.json")


def clear_redis() -> None:
    client = get_client()
    if not client:
        logger.info("Redis cache is not configured or unavailable.")
        return
    client.flushdb()
    logger.info("Redis cache flushed.")


def clear_file(path: Path) -> None:
    if path.exists():
        path.unlink()
        logger.info("Removed %s", path)
    else:
        logger.info("File %s does not exist; nothing to remove.", path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintenance utilities for cache and repair stores.")
    parser.add_argument("--redis", action="store_true", help="Clear Redis cache data.")
    parser.add_argument("--etl-cache", action="store_true", help="Remove the ETL response cache file.")
    parser.add_argument("--repair-cache", action="store_true", help="Remove the ETL repair knowledge file.")
    parser.add_argument("--all", action="store_true", help="Perform all cleanup actions.")
    args = parser.parse_args()

    if not any([args.redis, args.etl_cache, args.repair_cache, args.all]):
        parser.print_help()
        return

    if args.all or args.redis:
        clear_redis()
    if args.all or args.etl_cache:
        clear_file(ETL_CACHE_PATH)
    if args.all or args.repair_cache:
        clear_file(REPAIR_CACHE_PATH)


if __name__ == "__main__":
    main()

