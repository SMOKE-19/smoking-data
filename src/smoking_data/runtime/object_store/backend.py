from __future__ import annotations

import hashlib
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from smoking_data.core.exceptions import SmokingDataError

from .config import ObjectStoreTarget


class ConditionalWriteConflict(SmokingDataError):
    code = "remote.pointer_cas_conflict"


@dataclass(frozen=True, slots=True)
class ObjectMetadata:
    key: str
    size_bytes: int
    etag: str | None
    version_id: str | None
    checksum_sha256: str | None


@dataclass(frozen=True, slots=True)
class ListedObject:
    key: str
    size_bytes: int
    last_modified: str | None


class ObjectStore(Protocol):
    def head(self, key: str) -> ObjectMetadata | None: ...

    def get(self, key: str) -> tuple[bytes, ObjectMetadata]: ...

    def get_range(self, key: str, start: int, end_exclusive: int) -> bytes: ...

    def get_ranges(self, key: str, ranges: list[tuple[int, int]]) -> list[bytes]: ...

    def download_to_path(self, key: str, path: Path) -> ObjectMetadata: ...

    def list_prefix(self, prefix: str) -> list[ListedObject]: ...

    def delete(self, key: str) -> None: ...

    def put_immutable(self, key: str, path: Path, *, sha256: str) -> ObjectMetadata: ...

    def put_conditional(
        self,
        key: str,
        payload: bytes,
        *,
        previous_etag: str | None,
        create_only: bool,
    ) -> ObjectMetadata: ...


class S3ObjectStore:
    """Target-scoped S3 client; constructing it never mutates AWS_PROFILE."""

    def __init__(self, target: ObjectStoreTarget) -> None:
        try:
            import boto3
            from botocore.config import Config
            from botocore.exceptions import BotoCoreError, ClientError, ProfileNotFound
        except ImportError as exc:  # pragma: no cover - packaging contract prevents this
            raise SmokingDataError(
                "S3 publication requires the boto3 package.",
                code="object_store.sdk_unavailable",
            ) from exc
        self._client_error = ClientError
        self._sdk_errors = (BotoCoreError, ClientError)
        try:
            session = boto3.Session(
                profile_name=target.credentials.profile,
                region_name=target.region,
            )
        except ProfileNotFound as exc:
            raise SmokingDataError(
                "Configured AWS profile was not found.",
                code="object_store.profile_not_found",
                context={"target": target.name, "profile": target.credentials.profile},
            ) from exc
        addressing_style = "path" if target.path_style else "virtual"
        self._client = session.client(
            "s3",
            endpoint_url=target.endpoint_url,
            config=Config(s3={"addressing_style": addressing_style}),
        )
        self._target = target

    def preflight(self) -> None:
        try:
            credentials = self._client._request_signer._credentials  # noqa: SLF001
            if credentials is None:
                raise SmokingDataError(
                    "AWS credentials are unavailable.",
                    code="object_store.credentials_unavailable",
                    context={"target": self._target.name},
                )
            credentials.get_frozen_credentials()
        except SmokingDataError:
            raise
        except Exception as exc:
            raise SmokingDataError(
                "AWS credentials could not be resolved.",
                code="object_store.credentials_unavailable",
                context={"target": self._target.name},
            ) from exc

    def rust_credential_payload(self) -> dict[str, str]:
        """Return ephemeral credentials for the Rust range client; never persist this value."""
        credentials = self._client._request_signer._credentials  # noqa: SLF001
        if credentials is None:
            raise SmokingDataError(
                "AWS credentials are unavailable.",
                code="object_store.credentials_unavailable",
                context={"target": self._target.name},
            )
        try:
            frozen = credentials.get_frozen_credentials()
        except Exception as exc:
            raise SmokingDataError(
                "AWS credentials could not be refreshed.",
                code="object_store.credentials_expired",
                context={"target": self._target.name},
            ) from exc
        payload = {
            "access_key_id": frozen.access_key,
            "secret_access_key": frozen.secret_key,
        }
        if frozen.token:
            payload["session_token"] = frozen.token
        return payload

    def head(self, key: str) -> ObjectMetadata | None:
        try:
            response = self._client.head_object(Bucket=self._target.bucket, Key=key)
        except self._client_error as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise self._safe_error("head", key, exc) from exc
        return _metadata(key, response)

    def get(self, key: str) -> tuple[bytes, ObjectMetadata]:
        try:
            response = self._client.get_object(Bucket=self._target.bucket, Key=key)
            payload = response["Body"].read()
        except self._sdk_errors as exc:
            raise self._safe_error("get", key, exc) from exc
        return payload, _metadata(key, response)

    def get_range(self, key: str, start: int, end_exclusive: int) -> bytes:
        if start < 0 or end_exclusive <= start:
            raise ValueError("Object range must satisfy 0 <= start < end_exclusive.")
        try:
            response = self._client.get_object(
                Bucket=self._target.bucket,
                Key=key,
                Range=f"bytes={start}-{end_exclusive - 1}",
            )
            return response["Body"].read()
        except self._sdk_errors as exc:
            raise self._safe_error("get_range", key, exc) from exc

    def get_ranges(self, key: str, ranges: list[tuple[int, int]]) -> list[bytes]:
        """Fetch bounded ranges concurrently while preserving caller order."""
        if any(start < 0 or end <= start for start, end in ranges):
            raise ValueError("Object ranges must satisfy 0 <= start < end.")
        if not ranges:
            return []
        workers = min(self._target.multipart_concurrency, len(ranges))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="s3-range") as pool:
            futures = [pool.submit(self.get_range, key, start, end) for start, end in ranges]
            return [future.result() for future in futures]

    def download_to_path(self, key: str, path: Path) -> ObjectMetadata:
        """Download an object with bounded parallel byte ranges into a local file."""
        metadata = self.head(key)
        if metadata is None:
            raise SmokingDataError(
                "Remote object does not exist.",
                code="object_store.object_not_found",
                context={"target": self._target.name, "object_key": key},
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        path_tmp = path.with_suffix(path.suffix + ".tmp")
        ranges = _partition_ranges(metadata.size_bytes, self._target.multipart_chunk_bytes)
        try:
            with path_tmp.open("w+b") as handle:
                handle.truncate(metadata.size_bytes)

            def fetch_and_write(item: tuple[int, int]) -> None:
                start, end = item
                payload = self.get_range(key, start, end)
                if len(payload) != end - start:
                    raise SmokingDataError(
                        "S3 range response length differs from the requested range.",
                        code="object_store.range_length_mismatch",
                        context={"target": self._target.name, "object_key": key},
                    )
                with path_tmp.open("r+b") as handle:
                    if hasattr(os, "pwrite"):
                        os.pwrite(handle.fileno(), payload, start)
                    else:  # pragma: no cover - Windows fallback
                        handle.seek(start)
                        handle.write(payload)

            workers = min(self._target.multipart_concurrency, max(1, len(ranges)))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="s3-download") as pool:
                pending = {pool.submit(fetch_and_write, item) for item in ranges}
                for future in pending:
                    future.result()
            path_tmp.replace(path)
            return metadata
        except Exception:
            path_tmp.unlink(missing_ok=True)
            raise

    def list_prefix(self, prefix: str) -> list[ListedObject]:
        result: list[ListedObject] = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._target.bucket, Prefix=prefix):
                for item in page.get("Contents") or []:
                    modified = item.get("LastModified")
                    result.append(
                        ListedObject(
                            key=str(item["Key"]),
                            size_bytes=int(item.get("Size") or 0),
                            last_modified=modified.isoformat() if modified is not None else None,
                        )
                    )
        except self._sdk_errors as exc:
            raise self._safe_error("list", prefix, exc) from exc
        return result

    def delete(self, key: str) -> None:
        try:
            self._client.delete_object(Bucket=self._target.bucket, Key=key)
        except self._sdk_errors as exc:
            raise self._safe_error("delete", key, exc) from exc

    def put_immutable(self, key: str, path: Path, *, sha256: str) -> ObjectMetadata:
        existing = self.head(key)
        if existing is not None:
            if existing.size_bytes == path.stat().st_size and existing.checksum_sha256 == sha256:
                return existing
            raise SmokingDataError(
                "Immutable remote object already exists with different content.",
                code="remote.immutable_object_conflict",
                context={"target": self._target.name, "object_key": key},
            )
        file_size = path.stat().st_size
        if file_size >= self._target.multipart_threshold_bytes:
            return self._put_multipart_immutable(key, path, sha256=sha256)
        kwargs: dict[str, Any] = {
            "Bucket": self._target.bucket,
            "Key": key,
            "Metadata": {"smoking-data-sha256": sha256},
            "IfNoneMatch": "*",
        }
        mode = self._target.server_side_encryption.get("mode")
        if mode:
            kwargs["ServerSideEncryption"] = mode
        kms_key = self._target.server_side_encryption.get("kms_key_id")
        if kms_key:
            kwargs["SSEKMSKeyId"] = kms_key
        try:
            with path.open("rb") as handle:
                response = self._client.put_object(Body=handle, **kwargs)
        except self._client_error as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if status in {409, 412}:
                existing = self.head(key)
                if existing and existing.size_bytes == path.stat().st_size and existing.checksum_sha256 == sha256:
                    return existing
                raise SmokingDataError(
                    "Immutable remote object write conflicted.",
                    code="remote.immutable_object_conflict",
                    context={"target": self._target.name, "object_key": key},
                ) from exc
            raise self._safe_error("put", key, exc) from exc
        return _metadata(key, {**response, "ContentLength": file_size, "Metadata": {"smoking-data-sha256": sha256}})

    def _put_multipart_immutable(self, key: str, path: Path, *, sha256: str) -> ObjectMetadata:
        """Upload to a content-addressed temporary key, then conditional-copy to final."""
        upload_key = f"{key}.multipart/{sha256}.upload"
        common: dict[str, Any] = {"Metadata": {"smoking-data-sha256": sha256}}
        mode = self._target.server_side_encryption.get("mode")
        if mode:
            common["ServerSideEncryption"] = mode
        kms_key = self._target.server_side_encryption.get("kms_key_id")
        if kms_key:
            common["SSEKMSKeyId"] = kms_key
        upload_id: str | None = None
        try:
            response = self._client.create_multipart_upload(
                Bucket=self._target.bucket, Key=upload_key, **common
            )
            upload_id = str(response["UploadId"])
            parts: list[dict[str, Any]] = []
            with path.open("rb") as source, ThreadPoolExecutor(
                max_workers=self._target.multipart_concurrency,
                thread_name_prefix="s3-upload",
            ) as pool:
                pending: dict[Any, int] = {}
                next_number = 1
                exhausted = False
                while pending or not exhausted:
                    while not exhausted and len(pending) < self._target.multipart_concurrency * 2:
                        payload = source.read(self._target.multipart_chunk_bytes)
                        if not payload:
                            exhausted = True
                            break
                        future = pool.submit(
                            self._client.upload_part,
                            Bucket=self._target.bucket,
                            Key=upload_key,
                            UploadId=upload_id,
                            PartNumber=next_number,
                            Body=payload,
                        )
                        pending[future] = next_number
                        next_number += 1
                    if pending:
                        done, _ = wait(pending, return_when=FIRST_COMPLETED)
                        for future in done:
                            number = pending.pop(future)
                            result = future.result()
                            parts.append({"PartNumber": number, "ETag": result["ETag"]})
            parts.sort(key=lambda item: item["PartNumber"])
            self._client.complete_multipart_upload(
                Bucket=self._target.bucket,
                Key=upload_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            # CopyObject has no portable destination-side If-None-Match across
            # S3-compatible implementations. The immutable preflight above,
            # followed by a final identity check below, preserves the normal
            # contract while keeping MinIO/AWS behavior compatible.
            existing = self.head(key)
            if existing is not None:
                if existing.size_bytes == path.stat().st_size and existing.checksum_sha256 == sha256:
                    return existing
                raise SmokingDataError(
                    "Immutable remote object already exists with different content.",
                    code="remote.immutable_object_conflict",
                    context={"target": self._target.name, "object_key": key},
                )
            copy_kwargs: dict[str, Any] = {
                "Bucket": self._target.bucket,
                "Key": key,
                "CopySource": {"Bucket": self._target.bucket, "Key": upload_key},
                "MetadataDirective": "REPLACE",
                "Metadata": {"smoking-data-sha256": sha256},
            }
            if mode:
                copy_kwargs["ServerSideEncryption"] = mode
            if kms_key:
                copy_kwargs["SSEKMSKeyId"] = kms_key
            self._client.copy_object(**copy_kwargs)
            existing = self.head(key)
            if existing is None:
                raise SmokingDataError(
                    "Multipart object was not visible after commit.",
                    code="remote.multipart_commit_incomplete",
                    context={"target": self._target.name, "object_key": key},
                )
            if existing.size_bytes != path.stat().st_size or existing.checksum_sha256 != sha256:
                raise SmokingDataError(
                    "Multipart object checksum differs from the source.",
                    code="remote.immutable_object_conflict",
                    context={"target": self._target.name, "object_key": key},
                )
            return existing
        except self._sdk_errors as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if status in {409, 412}:
                existing = self.head(key)
                if existing and existing.size_bytes == path.stat().st_size and existing.checksum_sha256 == sha256:
                    return existing
                raise SmokingDataError(
                    "Immutable remote object write conflicted.",
                    code="remote.immutable_object_conflict",
                    context={"target": self._target.name, "object_key": key},
                ) from exc
            raise self._safe_error("multipart_put", key, exc) from exc
        finally:
            if upload_id:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._target.bucket, Key=upload_key, UploadId=upload_id
                    )
                except self._sdk_errors:
                    pass
            try:
                self._client.delete_object(Bucket=self._target.bucket, Key=upload_key)
            except self._sdk_errors:
                pass

    def put_conditional(
        self,
        key: str,
        payload: bytes,
        *,
        previous_etag: str | None,
        create_only: bool,
    ) -> ObjectMetadata:
        kwargs: dict[str, Any] = {
            "Bucket": self._target.bucket,
            "Key": key,
            "Body": payload,
            "ContentType": "application/json",
            "Metadata": {"smoking-data-sha256": hashlib.sha256(payload).hexdigest()},
        }
        if create_only:
            kwargs["IfNoneMatch"] = "*"
        elif previous_etag:
            kwargs["IfMatch"] = previous_etag
        else:
            raise ValueError("Conditional replacement requires previous_etag.")
        try:
            response = self._client.put_object(**kwargs)
        except self._client_error as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if status in {409, 412}:
                raise ConditionalWriteConflict(
                    "Remote catalog pointer changed concurrently.",
                    context={"target": self._target.name, "object_key": key},
                ) from exc
            raise self._safe_error("conditional_put", key, exc) from exc
        return _metadata(key, {**response, "ContentLength": len(payload), "Metadata": kwargs["Metadata"]})

    def _safe_error(self, operation: str, key: str, exc: Exception) -> SmokingDataError:
        return SmokingDataError(
            f"S3 {operation} failed.",
            code="object_store.request_failed",
            context={"target": self._target.name, "object_key": key, "error_type": type(exc).__name__},
        )


def _metadata(key: str, response: dict[str, Any]) -> ObjectMetadata:
    metadata = response.get("Metadata") or {}
    etag = response.get("ETag")
    return ObjectMetadata(
        key=key,
        size_bytes=int(response.get("ContentLength") or 0),
        etag=str(etag).strip('"') if etag else None,
        version_id=str(response["VersionId"]) if response.get("VersionId") else None,
        checksum_sha256=metadata.get("smoking-data-sha256"),
    )


def _partition_ranges(size: int, chunk: int) -> list[tuple[int, int]]:
    return [(start, min(start + chunk, size)) for start in range(0, size, chunk)]
