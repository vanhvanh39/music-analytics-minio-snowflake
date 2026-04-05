from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv
from minio import Minio
import snowflake.connector

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"


def load_env() -> None:
    load_dotenv(ROOT_DIR / ".env")


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_minio_client() -> Tuple[Minio, str]:
    load_env()
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    secure = get_bool("MINIO_SECURE", False)
    bucket = os.getenv("MINIO_BUCKET", "de-project")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    return client, bucket


def get_snowflake_connection():
    load_env()
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "MUSIC_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def ensure_local_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CURATED_DIR.mkdir(parents=True, exist_ok=True)


def get_itunes_terms() -> list[str]:
    """
    Đọc tất cả biến môi trường bắt đầu bằng ITUNES_TERMS_BATCH_
    rồi tách thành list term duy nhất.
    """
    load_env()

    batch_keys = sorted(
        key for key in os.environ.keys()
        if key.startswith("ITUNES_TERMS_BATCH_")
    )

    terms: list[str] = []
    for key in batch_keys:
        raw_value = os.getenv(key, "")
        for term in raw_value.split(","):
            cleaned = term.strip()
            if cleaned:
                terms.append(cleaned)

    # bỏ trùng nhưng giữ thứ tự
    unique_terms = list(dict.fromkeys(terms))
    return unique_terms