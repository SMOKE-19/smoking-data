from __future__ import annotations

from smoking_data.core.types import DatasetFile


def file_fingerprint(file: DatasetFile) -> str:
    return f"{file.path}|{file.size_bytes}|{file.modified_ns}"


def combined_fingerprint(files: list[DatasetFile]) -> str:
    import hashlib

    digest = hashlib.sha256()
    for item in files:
        digest.update(file_fingerprint(item).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
