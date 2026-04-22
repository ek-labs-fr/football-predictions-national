"""Storage abstraction for feature-engineering modules.

One seam that reads and writes to either the local filesystem or an S3
bucket, switched by the ``DATA_BUCKET`` environment variable. When set, the
backend is S3 (Lambda / production). When unset, all calls fall through to
the local filesystem — preserving the existing local dev/test workflow.

Accepts either a ``pathlib.Path`` (e.g. ``tmp_path / "foo.csv"`` from pytest)
or a relative string like ``"data/processed/foo.csv"``. When the backend is
S3, a leading ``"data/"`` segment is stripped so local-style paths map
cleanly onto bucket keys (e.g. ``data/processed/all_fixtures.csv`` →
``processed/all_fixtures.csv``).
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import boto3
import pandas as pd

_BUCKET_ENV = "DATA_BUCKET"


def _bucket() -> str | None:
    return os.environ.get(_BUCKET_ENV) or None


def using_s3() -> bool:
    return _bucket() is not None


@lru_cache(maxsize=1)
def _client():  # noqa: ANN202
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-3"))


def _to_key(arg: str | Path) -> str:
    """Normalize a filesystem-style path into a bucket-relative S3 key."""
    s = str(arg).replace("\\", "/").lstrip("/")
    if s.startswith("data/"):
        s = s[len("data/") :]
    return s


def _get(key: str) -> bytes:
    return _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()


def _put(key: str, body: bytes, content_type: str) -> None:
    _client().put_object(Bucket=_bucket(), Key=key, Body=body, ContentType=content_type)


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def exists(arg: str | Path) -> bool:
    if using_s3():
        try:
            _client().head_object(Bucket=_bucket(), Key=_to_key(arg))
            return True
        except _client().exceptions.ClientError:
            return False
    return Path(arg).exists()


def read_json(arg: str | Path) -> Any:
    if using_s3():
        return json.loads(_get(_to_key(arg)))
    return json.loads(Path(arg).read_text(encoding="utf-8"))


def write_json(arg: str | Path, payload: Any) -> None:
    data = json.dumps(payload, default=str).encode("utf-8")
    if using_s3():
        _put(_to_key(arg), data, "application/json")
        return
    path = Path(arg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def read_csv(arg: str | Path, **kwargs: Any) -> pd.DataFrame:
    if using_s3():
        return pd.read_csv(BytesIO(_get(_to_key(arg))), **kwargs)
    return pd.read_csv(arg, **kwargs)


def write_csv(arg: str | Path, df: pd.DataFrame, **kwargs: Any) -> None:
    kwargs.setdefault("index", False)
    if using_s3():
        buf = StringIO()
        df.to_csv(buf, **kwargs)
        _put(_to_key(arg), buf.getvalue().encode("utf-8"), "text/csv")
        return
    path = Path(arg)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, **kwargs)


def read_parquet(arg: str | Path, **kwargs: Any) -> pd.DataFrame:
    if using_s3():
        return pd.read_parquet(BytesIO(_get(_to_key(arg))), **kwargs)
    return pd.read_parquet(arg, **kwargs)


def write_parquet(arg: str | Path, df: pd.DataFrame, **kwargs: Any) -> None:
    kwargs.setdefault("index", False)
    kwargs.setdefault("engine", "pyarrow")
    if using_s3():
        buf = BytesIO()
        df.to_parquet(buf, **kwargs)
        _put(_to_key(arg), buf.getvalue(), "application/octet-stream")
        return
    path = Path(arg)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, **kwargs)


def list_keys(prefix: str | Path) -> list[str]:
    """Return keys under ``prefix`` (recursive)."""
    if using_s3():
        key_prefix = _to_key(prefix)
        paginator = _client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=_bucket(), Prefix=key_prefix):
            for item in page.get("Contents", []):
                keys.append(item["Key"])
        return keys
    root = Path(prefix)
    if not root.exists():
        return []
    if root.is_file():
        return [str(root).replace(os.sep, "/")]
    return [str(p).replace(os.sep, "/") for p in root.rglob("*") if p.is_file()]
