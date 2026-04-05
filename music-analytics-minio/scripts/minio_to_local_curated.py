from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pandas as pd

from common import get_minio_client, ensure_local_dirs, CURATED_DIR


def main() -> None:
    ensure_local_dirs()
    client, bucket = get_minio_client()

    object_names = [
        obj.object_name
        for obj in client.list_objects(bucket, prefix="itunes/raw/search/", recursive=True)
        if obj.object_name.endswith(".json")
    ]

    if not object_names:
        raise FileNotFoundError("No raw files found in MinIO under prefix itunes/raw/search/")

    rows: list[dict] = []
    base_now = datetime.now()

    for object_name in sorted(object_names):
        response = client.get_object(bucket, object_name)
        raw_payload = response.read().decode("utf-8")
        data = json.loads(raw_payload)

        source_term = data.get("search_term", "unknown")

        for item in data.get("results", []):
            fake_ingested_at = (
                base_now - timedelta(days=random.randint(0, 6))
            ).replace(
                hour=random.randint(8, 22),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                microsecond=0,
            )

            rows.append(
                {
                    "track_id": item.get("trackId"),
                    "artist_id": item.get("artistId"),
                    "collection_id": item.get("collectionId"),
                    "artist_name": item.get("artistName"),
                    "collection_name": item.get("collectionName"),
                    "track_name": item.get("trackName"),
                    "primary_genre_name": item.get("primaryGenreName"),
                    "track_price": item.get("trackPrice"),
                    "currency": item.get("currency"),
                    "release_date": item.get("releaseDate"),
                    "country": item.get("country"),
                    "ingested_at": fake_ingested_at.isoformat(timespec="seconds"),
                    "source_term": source_term,
                }
            )

    if not rows:
        raise ValueError("No rows found in raw JSON files.")

    df = pd.DataFrame(rows)

    df = df.drop_duplicates(subset=["track_id", "source_term", "ingested_at"], keep="first")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    curated_file = CURATED_DIR / f"itunes_tracks_curated_{timestamp}.csv"
    df.to_csv(curated_file, index=False, encoding="utf-8-sig")

    print(f"Created curated CSV: {curated_file}")
    print(f"Rows written: {len(df)}")
    print(f"Unique terms: {df['source_term'].nunique()}")
    print("Sample fake dates:")
    print(df["ingested_at"].head(10).to_list())


if __name__ == "__main__":
    main()