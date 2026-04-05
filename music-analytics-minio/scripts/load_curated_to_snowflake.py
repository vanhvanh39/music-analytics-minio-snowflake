from __future__ import annotations

import glob
import os
from pathlib import Path

from common import get_snowflake_connection, load_env


def latest_csv() -> Path:
    files = sorted(glob.glob("data/curated/*.csv"))
    if not files:
        raise FileNotFoundError("No curated CSV found in data/curated/")
    return Path(files[-1]).resolve()


def main() -> None:
    load_env()
    csv_path = latest_csv()
    stage_name = os.getenv("SNOWFLAKE_STAGE", "ITUNES_STAGE")
    table_name = os.getenv("SNOWFLAKE_TABLE", "ITUNES_TRACKS_RAW")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
                TRACK_ID STRING,
                ARTIST_ID STRING,
                COLLECTION_ID STRING,
                ARTIST_NAME STRING,
                COLLECTION_NAME STRING,
                TRACK_NAME STRING,
                PRIMARY_GENRE_NAME STRING,
                TRACK_PRICE FLOAT,
                CURRENCY STRING,
                RELEASE_DATE STRING,
                COUNTRY STRING,
                INGESTED_AT STRING,
                SOURCE_TERM STRING
            )
            """
        )

        cur.execute(f"CREATE OR REPLACE STAGE {stage_name}")

        put_sql = f"PUT file://{csv_path.as_posix()} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        print(put_sql)
        cur.execute(put_sql)

        copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                NULL_IF = ('NULL', 'null', '')
            )
            ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)

        print(f"Loaded curated CSV into Snowflake: {csv_path.name}")
        print(f"Target table: {table_name}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()