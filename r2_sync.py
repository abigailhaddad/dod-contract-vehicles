"""
r2_sync.py -- Sync bulk checkpoint files to/from Cloudflare R2.

Separate R2 prefix from procurement-methods to avoid collisions.
Credentials from environment variables:
  CF_R2_ACCOUNT_ID, CF_R2_BUCKET, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY
"""

import os
from pathlib import Path

import boto3
from botocore.config import Config

ACCOUNT_ID = os.environ["CF_R2_ACCOUNT_ID"]
BUCKET     = os.environ["CF_R2_BUCKET"]
ACCESS_KEY = os.environ["CF_R2_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["CF_R2_SECRET_ACCESS_KEY"]

PREFIX = "dod_vehicles/"


def _client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def download_state(local_dir: Path) -> int:
    """Download all checkpoint files from R2 to local_dir. Returns count."""
    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = local_dir / Path(key).name
            if not local_path.exists() or local_path.stat().st_mtime < obj["LastModified"].timestamp():
                print(f"  R2 -> {local_path.name}")
                s3.download_file(BUCKET, key, str(local_path))
                count += 1
    print(f"Downloaded {count} files from R2")
    return count


def upload_state(local_dir: Path) -> int:
    """Upload all checkpoint files from local_dir to R2. Returns count."""
    s3 = _client()
    count = 0
    for f in sorted(local_dir.iterdir()):
        if f.suffix in {".csv", ".not_found"}:
            key = PREFIX + f.name
            s3.upload_file(str(f), BUCKET, key)
            count += 1
    print(f"Uploaded {count} files to R2")
    return count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download"])
    parser.add_argument("--dir", default="data/bulk_checkpoints")
    args = parser.parse_args()
    d = Path(args.dir)
    if args.action == "download":
        download_state(d)
    else:
        upload_state(d)
