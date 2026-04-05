from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

SCRIPTS = [
    [sys.executable, "scripts/create_minio_buckets.py"],
    [sys.executable, "scripts/ingest_itunes_to_minio.py"],
    [sys.executable, "scripts/minio_to_local_curated.py"],
    [sys.executable, "scripts/load_curated_to_snowflake.py"],
]


def run_step(command: list[str]) -> None:
    print("\n" + "=" * 80)
    print("Running:", " ".join(command))
    print("=" * 80)
    subprocess.run(command, cwd=ROOT, check=True)


if __name__ == "__main__":
    for cmd in SCRIPTS:
        run_step(cmd)

    print("\nPipeline completed successfully.")
