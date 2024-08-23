import json
from typing import List
import zstandard as zstd
import gzip
import jsonlines
import io
from typing import BinaryIO, List

import os
from pathlib import Path as LocalPath


def is_compressed(file_path: str):
    return any(file_path.endswith(z) for z in (".zst", ".zstd", ".gz"))


def _jsonl_bytes_reader(fh: BinaryIO):
    with io.TextIOWrapper(fh, encoding="utf-8") as text_reader:
        with jsonlines.Reader(text_reader) as jsonl_reader:
            for item in jsonl_reader:
                yield item

def read_jsonl(file_path: str):
    """Read a JSONL file from a given local path."""
    path = LocalPath(file_path)

    if any(file_path.endswith(z) for z in (".zst", ".zstd")):
        with path.open('rb') as f:
            with zstd.ZstdDecompressor().stream_reader(f) as reader:
                for line in _jsonl_bytes_reader(reader):
                    yield line
    elif file_path.endswith(".gz"):
        with gzip.open(path, 'rb') as f:
            for line in _jsonl_bytes_reader(f):
                yield line
    else:
        with path.open('rb') as f:    
            for line in _jsonl_bytes_reader(f):
                yield line

def write_jsonl(data, file_path: str, mode: str = "w"):
    """Write data to a JSONL file at a given local path."""
    path = LocalPath(file_path)

    if is_compressed(file_path):
        data = [json.dumps(d) for d in data]
        data = "\n".join(data).encode('utf8')

    if any(file_path.endswith(z) for z in (".zst", ".zstd")):
        with path.open("wb") as f:
            with zstd.ZstdCompressor().stream_writer(f) as writer:
                writer.write(data)
    elif file_path.endswith(".gz"):
        with path.open("wb") as f:
            f.write(gzip.compress(data))
    else:
        with path.open(mode) as f:
            for item in data:
                json_str = json.dumps(item)
                f.write(f"{json_str}\n")

def makedirs_if_missing(dir_path: str):
    """Create directories for the provided path if they do not exist."""
    os.makedirs(dir_path, exist_ok=True)


