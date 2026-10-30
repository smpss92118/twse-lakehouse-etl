"""Filesystem-agnostic IO: the same calls work on a local path and on s3://."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])


def _resolve(uri: str) -> tuple[fs.FileSystem, str]:
    return fs.FileSystem.from_uri(uri)


def write_bytes(uri: str, data: bytes) -> str:
    filesystem, path = _resolve(uri)
    filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
    with filesystem.open_output_stream(path) as out:
        out.write(data)
    return uri


def read_bytes(uri: str) -> bytes:
    filesystem, path = _resolve(uri)
    with filesystem.open_input_stream(path) as src:
        return src.read()


def list_files(prefix_uri: str, suffix: str = "") -> list[str]:
    """Sorted file URIs under a prefix (Bronze names start with a UTC timestamp, so sorted = chronological)."""
    filesystem, path = _resolve(prefix_uri)
    infos = filesystem.get_file_info(fs.FileSelector(path, allow_not_found=True, recursive=False))
    names = sorted(i.base_name for i in infos if i.type == fs.FileType.File and i.base_name.endswith(suffix))
    return [join(prefix_uri, name) for name in names]


def write_parquet(uri: str, table: pa.Table) -> str:
    filesystem, path = _resolve(uri)
    filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
    pq.write_table(table, path, filesystem=filesystem)
    return uri


def read_parquet(uri: str) -> pa.Table:
    filesystem, path = _resolve(uri)
    return pq.read_table(path, filesystem=filesystem)
