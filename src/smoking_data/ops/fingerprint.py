from __future__ import annotations

from smoking_data.core.types import DatasetFile


def file_fingerprint(file: DatasetFile) -> str:
    content_sha256 = getattr(file, "content_sha256", None)
    if content_sha256:
        return "|".join(
            [
                str(getattr(file, "source_kind", "local")),
                str(getattr(file, "dataset_id", None) or ""),
                str(
                    getattr(file, "file_id", None)
                    or getattr(file, "relative_path", None)
                    or file.path
                ),
                str(content_sha256),
            ]
        )
    return f"{file.path}|{file.size_bytes}|{file.modified_ns}"


def combined_fingerprint(files: list[DatasetFile]) -> str:
    import hashlib

    digest = hashlib.sha256()
    for item in files:
        digest.update(file_fingerprint(item).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
