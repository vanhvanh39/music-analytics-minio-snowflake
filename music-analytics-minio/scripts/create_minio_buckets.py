from __future__ import annotations

from common import get_minio_client


def main() -> None:
    client, bucket = get_minio_client()

    if client.bucket_exists(bucket):
        print(f"Bucket already exists: {bucket}")
    else:
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")


if __name__ == "__main__":
    main()
