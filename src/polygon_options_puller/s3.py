"""Thin wrapper around boto3 for Polygon / Massive S3 access."""

from __future__ import annotations

import boto3
from botocore.config import Config

from .config import S3_BUCKET, S3_ENDPOINT


def get_s3_client(access_key: str, secret_key: str):
    """Return a boto3 S3 client configured for the Polygon/Massive endpoint."""
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )


def list_keys(s3_client, prefix: str) -> list[str]:
    """List all object keys under *prefix* in the flat-files bucket."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_file(s3_client, key: str, dest: str) -> None:
    """Download a single S3 object to a local path."""
    s3_client.download_file(S3_BUCKET, key, dest)
