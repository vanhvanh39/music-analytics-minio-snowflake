from __future__ import annotations

import json
import os
from datetime import datetime
from io import BytesIO

import requests

from common import get_minio_client, ensure_local_dirs, RAW_DIR, load_env, get_itunes_terms


def safe_name(value: str) -> str:
    return (
        value.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("&", "and")
    )


def main() -> None:
    load_env()
    ensure_local_dirs()
    client, bucket = get_minio_client()

    terms = get_itunes_terms()
    if not terms:
        raise ValueError("Không tìm thấy term nào từ ITUNES_TERMS_BATCH_* trong file .env")

    media = os.getenv("ITUNES_MEDIA", "music")
    entity = os.getenv("ITUNES_ENTITY", "song")
    limit = int(os.getenv("ITUNES_LIMIT", "50"))
    country = os.getenv("ITUNES_COUNTRY", "US")

    url = "https://itunes.apple.com/search"
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_results = 0

    print(f"Total terms to ingest: {len(terms)}")

    for idx, term in enumerate(terms, start=1):
        params = {
            "term": term,
            "media": media,
            "entity": entity,
            "limit": limit,
            "country": country,
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        api_data = response.json()

        payload_dict = {
            "search_term": term,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "result_count": api_data.get("resultCount", 0),
            "results": api_data.get("results", []),
        }

        total_results += payload_dict["result_count"]

        safe_term = safe_name(term)
        now = datetime.now()

        object_name = (
            f"itunes/raw/search/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{safe_term}_{run_ts}.json"
        )

        payload = json.dumps(payload_dict, ensure_ascii=False, indent=2).encode("utf-8")

        client.put_object(
            bucket,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )

        local_file = RAW_DIR / f"{safe_term}_{run_ts}.json"
        local_file.write_bytes(payload)

        print(f"[{idx}/{len(terms)}] Uploaded raw JSON to MinIO: {bucket}/{object_name}")
        print(f"Saved local backup: {local_file}")
        print(f"Term: {term} | Result count: {payload_dict['result_count']}")

    print(f"Finished ingesting {len(terms)} terms | Total rows fetched: {total_results}")


if __name__ == "__main__":
    main()