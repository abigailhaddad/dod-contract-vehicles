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


def promote(src_prefix: str, dst_prefix: str) -> int:
    """Server-side copy all objects from src_prefix to dst_prefix within the
    same bucket. Deletes any objects at dst_prefix that aren't in src (so dst
    becomes an exact mirror)."""
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")

    # Collect source keys
    src_keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_keys.append(obj["Key"])
    if not src_keys:
        print(f"No objects under {src_prefix} -- nothing to promote")
        return 0

    new_keys = set()
    copied = 0
    for src_key in src_keys:
        rel = src_key[len(src_prefix):]
        dst_key = dst_prefix + rel
        s3.copy_object(
            Bucket=BUCKET,
            Key=dst_key,
            CopySource={"Bucket": BUCKET, "Key": src_key},
        )
        new_keys.add(dst_key)
        copied += 1
        print(f"  {src_key} -> {dst_key}")

    # Delete stale keys at dst not present in src
    deleted = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=dst_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"] not in new_keys:
                s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
                deleted += 1
                print(f"  Deleted stale: {obj['Key']}")
    print(f"Promoted {copied} objects ({deleted} stale removed) from {src_prefix} -> {dst_prefix}")
    return copied


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download", "promote"])
    parser.add_argument("--dir", default="data/bulk_checkpoints")
    parser.add_argument("--src", help="promote only: source prefix")
    parser.add_argument("--dst", help="promote only: destination prefix")
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
    elif args.action == "promote":
        if not args.src or not args.dst:
            parser.error("promote requires --src and --dst")
        promote(args.src, args.dst)
    else:
        suffixes = tuple(args.suffix) if args.suffix else DEFAULT_SUFFIXES
        upload_state(d, prefix=args.prefix, suffixes=suffixes, mirror=args.mirror)
