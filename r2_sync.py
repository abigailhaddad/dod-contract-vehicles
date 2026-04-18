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

DEFAULT_PREFIX = "dod_vehicles/"
DEFAULT_SUFFIXES = (".csv", ".not_found")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def download_state(local_dir: Path, prefix: str = DEFAULT_PREFIX) -> int:
    """Download all files under `prefix` from R2 to local_dir."""
    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = local_dir / Path(key).name
            if not local_path.exists() or local_path.stat().st_mtime < obj["LastModified"].timestamp():
                print(f"  R2 -> {local_path.name}")
                s3.download_file(BUCKET, key, str(local_path))
                count += 1
    print(f"Downloaded {count} files from R2")
    return count


def upload_state(local_dir: Path, prefix: str = DEFAULT_PREFIX,
                 suffixes: tuple = DEFAULT_SUFFIXES, mirror: bool = False) -> int:
    """Upload files matching `suffixes` from local_dir to R2 under `prefix`.

    If mirror=True, also delete any object under `prefix` that wasn't part
    of this upload -- keeps R2 in sync with local_dir, no orphan files.
    """
    s3 = _client()
    uploaded_keys = set()
    count = 0
    for f in sorted(local_dir.iterdir()):
        if f.suffix in suffixes:
            key = prefix + f.name
            s3.upload_file(str(f), BUCKET, key)
            uploaded_keys.add(key)
            count += 1
    print(f"Uploaded {count} files to R2")

    if mirror:
        paginator = s3.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"] not in uploaded_keys:
                    s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
                    deleted += 1
                    print(f"  Deleted orphan: {obj['Key']}")
        if deleted:
            print(f"Deleted {deleted} orphan files")

    return count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download"])
    parser.add_argument("--dir", default="data/bulk_checkpoints")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--suffix", action="append",
                        help="File extension to include (upload only). "
                             "Repeat for multiple. Default: .csv, .not_found")
    parser.add_argument("--mirror", action="store_true",
                        help="Upload only: delete any R2 objects under --prefix "
                             "that weren't part of this upload (keeps R2 in sync).")
    args = parser.parse_args()
    d = Path(args.dir)
    if args.action == "download":
        download_state(d, prefix=args.prefix)
    else:
        suffixes = tuple(args.suffix) if args.suffix else DEFAULT_SUFFIXES
        upload_state(d, prefix=args.prefix, suffixes=suffixes, mirror=args.mirror)
