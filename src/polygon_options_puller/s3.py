"""Thin wrapper around boto3 for Polygon / Massive S3 access."""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.config import Config

from .config import S3_BUCKET, S3_ENDPOINT


@dataclass(frozen=True)
class S3Object:
    """Listing metadata for one object in the bucket."""

    key: str
    etag: str
    size: int
    last_modified: datetime | None = None


def get_s3_client(access_key: str, secret_key: str, endpoint_url: str = S3_ENDPOINT):
    """Return a boto3 S3 client configured for the Polygon/Massive endpoint."""
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            read_timeout=600,
            retries={"max_attempts": 3},
        ),
    )


def list_objects(s3_client, prefix: str, bucket: str = S3_BUCKET) -> list[S3Object]:
    """List every object under *prefix*, with ETag and size taken from the listing."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: list[S3Object] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            objects.append(
                S3Object(
                    key=obj["Key"],
                    etag=str(obj.get("ETag", "")).strip('"'),
                    size=int(obj.get("Size", 0) or 0),
                    last_modified=obj.get("LastModified"),
                )
            )
    return objects


def list_common_prefixes(s3_client, prefix: str, bucket: str = S3_BUCKET) -> list[str]:
    """List the immediate "sub-directories" under *prefix* (S3 common prefixes)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    prefixes: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            prefixes.append(cp["Prefix"])
    return sorted(set(prefixes))


def list_keys(s3_client, prefix: str) -> list[str]:
    """List all object keys under *prefix* in the flat-files bucket."""
    return [obj.key for obj in list_objects(s3_client, prefix)]


def get_streaming_body(s3_client, key: str, bucket: str = S3_BUCKET):
    """Return the raw botocore streaming body for a single S3 object."""
    return s3_client.get_object(Bucket=bucket, Key=key)["Body"]


class _BodyReader(io.RawIOBase):
    """Adapt any object exposing ``read(n)`` to the ``io.RawIOBase`` protocol."""

    def __init__(self, body):
        super().__init__()
        self._body = body

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        data = self._body.read(len(buffer))
        n = len(data)
        buffer[:n] = data
        return n

    def close(self) -> None:
        try:
            close = getattr(self._body, "close", None)
            if close is not None:
                close()
        finally:
            super().close()


def open_object(
    s3_client, key: str, bucket: str = S3_BUCKET, buffer_size: int = 1 << 20
) -> io.BufferedReader:
    """Open an S3 object as a buffered binary file object that streams from S3."""
    body = get_streaming_body(s3_client, key, bucket=bucket)
    return io.BufferedReader(_BodyReader(body), buffer_size=buffer_size)
