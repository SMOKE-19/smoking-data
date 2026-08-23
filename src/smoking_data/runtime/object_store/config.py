from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from smoking_data.core.exceptions import ConfigError

_TARGET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_POWER_OF_TWO_MAX = 65_536


@dataclass(frozen=True, slots=True)
class CredentialSpec:
    provider: str
    profile: str | None = None
    shared_files: str = "os_default"

    @property
    def resolution_mode(self) -> str:
        if os.environ.get("AWS_CONFIG_FILE") or os.environ.get(
            "AWS_SHARED_CREDENTIALS_FILE"
        ):
            return "environment_override"
        return "os_default"

    def metadata(self) -> dict[str, str | None]:
        return {
            "provider": self.provider,
            "profile": self.profile,
            "shared_files": self.resolution_mode,
        }


@dataclass(frozen=True, slots=True)
class ObjectStoreTarget:
    name: str
    bucket: str
    base_prefix: str
    region: str | None
    endpoint_url: str | None
    path_style: bool
    credentials: CredentialSpec
    server_side_encryption: Mapping[str, str] = field(default_factory=dict)
    multipart_threshold_bytes: int = 64 * 1024 * 1024
    multipart_chunk_bytes: int = 16 * 1024 * 1024
    multipart_concurrency: int = 8

    def client_cache_key(self) -> tuple[str, str | None, str | None, str | None, bool]:
        return (
            self.name,
            self.credentials.profile,
            self.region,
            self.endpoint_url,
            self.path_style,
        )

    def object_key(self, relative: str) -> str:
        relative = validate_relative_prefix(relative, path="object_key")
        return "/".join(part for part in (self.base_prefix, relative) if part)

    def safe_identity(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": "s3",
            "bucket": self.bucket,
            "base_prefix": self.base_prefix,
            "region": self.region,
            "endpoint_configured": self.endpoint_url is not None,
            "path_style": self.path_style,
            "multipart": {
                "threshold_bytes": self.multipart_threshold_bytes,
                "chunk_bytes": self.multipart_chunk_bytes,
                "concurrency": self.multipart_concurrency,
            },
        }


@dataclass(frozen=True, slots=True)
class ParquetPublicationSpec:
    enabled: bool = True
    index_level: str = "row_group"
    writer_page_index: bool = False
    key_columns: tuple[str, ...] = ()
    key_null_policy: str = "error"
    key_hash: str = "sha256_trunc128_v1"
    hash_buckets: int = 256


@dataclass(frozen=True, slots=True)
class SbdfPublicationSpec:
    enabled: bool = False
    shard_policy: str = "mirror_parquet_parts"
    row_key_columns: tuple[str, ...] = ()
    batch_size: int = 50_000
    encoding_rle: bool = True
    key_hash: str = "sha256_trunc128_v1"
    hash_buckets: int = 256


@dataclass(frozen=True, slots=True)
class VerificationSpec:
    checksum: str = "sha256"
    verify_remote_head: bool = True
    verify_sidecar_references: bool = True


@dataclass(frozen=True, slots=True)
class PublicationSpec:
    enabled: bool
    target: str
    dataset_prefix: str
    mode: str
    failure_policy: str
    parquet: ParquetPublicationSpec
    sbdf: SbdfPublicationSpec
    verification: VerificationSpec

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> PublicationSpec | None:
        if value is None:
            return None
        raw = _mapping(value, path="output.artifact.publication")
        _unknown(
            raw,
            {
                "enabled",
                "target",
                "dataset_prefix",
                "mode",
                "failure_policy",
                "parquet",
                "sbdf",
                "verification",
            },
            path="output.artifact.publication",
        )
        enabled = bool(raw.get("enabled", True))
        target = _required_string(raw.get("target"), path="publication.target")
        if not _TARGET_NAME.fullmatch(target):
            raise ConfigError("publication.target contains unsupported characters.")
        dataset_prefix = validate_relative_prefix(
            _required_string(raw.get("dataset_prefix"), path="publication.dataset_prefix"),
            path="publication.dataset_prefix",
        )
        mode = str(raw.get("mode") or "mirror_after_local_commit")
        if mode != "mirror_after_local_commit":
            raise ConfigError("publication.mode must be mirror_after_local_commit.")
        failure_policy = str(raw.get("failure_policy") or "required")
        if failure_policy not in {"required", "warn_and_retry"}:
            raise ConfigError("publication.failure_policy must be required or warn_and_retry.")
        return cls(
            enabled=enabled,
            target=target,
            dataset_prefix=dataset_prefix,
            mode=mode,
            failure_policy=failure_policy,
            parquet=_parquet_spec(raw.get("parquet")),
            sbdf=_sbdf_spec(raw.get("sbdf")),
            verification=_verification_spec(raw.get("verification")),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "target": self.target,
            "dataset_prefix": self.dataset_prefix,
            "mode": self.mode,
            "failure_policy": self.failure_policy,
            "parquet": {
                "enabled": self.parquet.enabled,
                "random_access_index": {
                    "level": self.parquet.index_level,
                    "writer_page_index": self.parquet.writer_page_index,
                    "key_columns": list(self.parquet.key_columns),
                    "key_null_policy": self.parquet.key_null_policy,
                    "key_hash": self.parquet.key_hash,
                    "hash_buckets": self.parquet.hash_buckets,
                },
            },
            "sbdf": {
                "enabled": self.sbdf.enabled,
                "shard_policy": self.sbdf.shard_policy,
                "row_key_columns": list(self.sbdf.row_key_columns),
                "batch_size": self.sbdf.batch_size,
                "encoding_rle": self.sbdf.encoding_rle,
                "key_hash": self.sbdf.key_hash,
                "hash_buckets": self.sbdf.hash_buckets,
            },
            "verification": {
                "checksum": self.verification.checksum,
                "verify_remote_head": self.verification.verify_remote_head,
                "verify_sidecar_references": self.verification.verify_sidecar_references,
            },
        }


def load_object_store_target(
    project_root: str | Path,
    name: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> ObjectStoreTarget:
    root = Path(project_root).expanduser().resolve()
    path = root / ".smoking-data" / "object-stores.yaml"
    if not path.is_file():
        raise ConfigError(f"Object-store configuration does not exist: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    stores = _mapping(payload.get("object_stores"), path="object_stores")
    raw = _mapping(stores.get(name), path=f"object_stores.{name}")
    _unknown(
        raw,
        {
            "type",
            "bucket",
            "base_prefix",
            "region",
            "endpoint_url_env",
            "credentials",
            "path_style",
            "server_side_encryption",
            "multipart",
        },
        path=f"object_stores.{name}",
    )
    if raw.get("type") != "s3":
        raise ConfigError(f"object_stores.{name}.type must be s3.")
    credentials = _credential_spec(raw.get("credentials"), target=name)
    endpoint_env = raw.get("endpoint_url_env")
    if endpoint_env is not None and not isinstance(endpoint_env, str):
        raise ConfigError(f"object_stores.{name}.endpoint_url_env must be a string.")
    environment = environ if environ is not None else os.environ
    endpoint_url = environment.get(endpoint_env) if endpoint_env else None
    encryption = _encryption_spec(raw.get("server_side_encryption"), environment)
    multipart = _multipart_spec(raw.get("multipart"))
    return ObjectStoreTarget(
        name=name,
        bucket=_required_string(raw.get("bucket"), path=f"object_stores.{name}.bucket"),
        base_prefix=validate_relative_prefix(
            str(raw.get("base_prefix") or ""), path=f"object_stores.{name}.base_prefix", allow_empty=True
        ),
        region=str(raw["region"]) if raw.get("region") else None,
        endpoint_url=endpoint_url,
        path_style=bool(raw.get("path_style", False)),
        credentials=credentials,
        server_side_encryption=encryption,
        multipart_threshold_bytes=multipart[0],
        multipart_chunk_bytes=multipart[1],
        multipart_concurrency=multipart[2],
    )


def validate_relative_prefix(value: str, *, path: str, allow_empty: bool = False) -> str:
    normalized = value.strip().replace("\\", "/").strip("/")
    if not normalized and not allow_empty:
        raise ConfigError(f"{path} must be a non-empty target-relative path.")
    if value.startswith(("/", "\\")) or "://" in value:
        raise ConfigError(f"{path} must not be an absolute path or URI.")
    parts = normalized.split("/") if normalized else []
    if any(part in {"", ".", ".."} for part in parts):
        raise ConfigError(f"{path} contains an unsafe path segment.")
    return "/".join(parts)


def _credential_spec(value: Any, *, target: str) -> CredentialSpec:
    raw = _mapping(value, path=f"object_stores.{target}.credentials")
    _unknown(raw, {"provider", "profile", "shared_files"}, path=f"object_stores.{target}.credentials")
    provider = str(raw.get("provider") or "")
    profile = str(raw["profile"]).strip() if raw.get("profile") else None
    shared_files = str(raw.get("shared_files") or "os_default")
    if shared_files != "os_default":
        raise ConfigError("object_store.shared_files must be os_default.")
    if provider == "shared_profile":
        if not profile:
            raise ConfigError("object_store.profile_required: shared_profile requires profile.")
    elif provider == "default_chain":
        if profile is not None:
            raise ConfigError("default_chain must not define profile.")
    else:
        raise ConfigError("credentials.provider must be shared_profile or default_chain.")
    return CredentialSpec(provider=provider, profile=profile, shared_files=shared_files)


def _multipart_spec(value: Any) -> tuple[int, int, int]:
    raw = _mapping(value or {}, path="object_store.multipart")
    _unknown(
        raw,
        {"threshold_bytes", "chunk_bytes", "concurrency"},
        path="object_store.multipart",
    )
    threshold = _positive_int(raw.get("threshold_bytes", 64 * 1024 * 1024), path="object_store.multipart.threshold_bytes")
    chunk = _positive_int(raw.get("chunk_bytes", 16 * 1024 * 1024), path="object_store.multipart.chunk_bytes")
    concurrency = _positive_int(raw.get("concurrency", 8), path="object_store.multipart.concurrency")
    if chunk < 5 * 1024 * 1024:
        raise ConfigError("object_store.multipart.chunk_bytes must be at least 5 MiB.")
    if concurrency > 32:
        raise ConfigError("object_store.multipart.concurrency must be <= 32.")
    return threshold, chunk, concurrency


def _parquet_spec(value: Any) -> ParquetPublicationSpec:
    raw = _mapping(value or {}, path="publication.parquet")
    _unknown(raw, {"enabled", "random_access_index"}, path="publication.parquet")
    index = _mapping(raw.get("random_access_index") or {}, path="publication.parquet.random_access_index")
    _unknown(index, {"level", "writer_page_index", "key_columns", "key_null_policy", "key_hash", "hash_buckets"}, path="publication.parquet.random_access_index")
    level = str(index.get("level") or "row_group")
    if level not in {"row_group", "page_if_available", "page_required", "key"}:
        raise ConfigError("publication.parquet.random_access_index.level is invalid.")
    writer_page_index = index.get("writer_page_index", False)
    if isinstance(writer_page_index, str):
        writer_page_index = writer_page_index == "enabled"
    key_columns = _strings(index.get("key_columns"), path="publication.parquet.random_access_index.key_columns")
    if level == "key" and not key_columns:
        raise ConfigError("key index requires key_columns.")
    buckets = _hash_buckets(index.get("hash_buckets", 256), path="publication.parquet.random_access_index.hash_buckets")
    return ParquetPublicationSpec(
        enabled=bool(raw.get("enabled", True)),
        index_level=level,
        writer_page_index=bool(writer_page_index),
        key_columns=key_columns,
        key_null_policy=str(index.get("key_null_policy") or "error"),
        key_hash=str(index.get("key_hash") or "sha256_trunc128_v1"),
        hash_buckets=buckets,
    )


def _sbdf_spec(value: Any) -> SbdfPublicationSpec:
    raw = _mapping(value or {}, path="publication.sbdf")
    _unknown(raw, {"enabled", "shard_policy", "row_key_columns", "batch_size", "encoding_rle", "key_hash", "hash_buckets"}, path="publication.sbdf")
    return SbdfPublicationSpec(
        enabled=bool(raw.get("enabled", False)),
        shard_policy=str(raw.get("shard_policy") or "mirror_parquet_parts"),
        row_key_columns=_strings(raw.get("row_key_columns"), path="publication.sbdf.row_key_columns"),
        batch_size=_positive_int(raw.get("batch_size", 50_000), path="publication.sbdf.batch_size"),
        encoding_rle=bool(raw.get("encoding_rle", True)),
        key_hash=str(raw.get("key_hash") or "sha256_trunc128_v1"),
        hash_buckets=_hash_buckets(raw.get("hash_buckets", 256), path="publication.sbdf.hash_buckets"),
    )


def _verification_spec(value: Any) -> VerificationSpec:
    raw = _mapping(value or {}, path="publication.verification")
    _unknown(raw, {"checksum", "verify_remote_head", "verify_sidecar_references"}, path="publication.verification")
    if str(raw.get("checksum") or "sha256") != "sha256":
        raise ConfigError("publication.verification.checksum must be sha256.")
    return VerificationSpec(
        verify_remote_head=bool(raw.get("verify_remote_head", True)),
        verify_sidecar_references=bool(raw.get("verify_sidecar_references", True)),
    )


def _encryption_spec(value: Any, environment: Mapping[str, str]) -> dict[str, str]:
    if value is None:
        return {}
    raw = _mapping(value, path="server_side_encryption")
    _unknown(raw, {"mode", "kms_key_id_env"}, path="server_side_encryption")
    mode = str(raw.get("mode") or "")
    if mode not in {"AES256", "aws:kms"}:
        raise ConfigError("server_side_encryption.mode must be AES256 or aws:kms.")
    result = {"mode": mode}
    if mode == "aws:kms":
        env_name = _required_string(raw.get("kms_key_id_env"), path="server_side_encryption.kms_key_id_env")
        key_id = environment.get(env_name)
        if not key_id:
            raise ConfigError(f"KMS key environment variable is unavailable: {env_name}")
        result["kms_key_id"] = key_id
    return result


def _hash_buckets(value: Any, *, path: str) -> int:
    parsed = _positive_int(value, path=path)
    if parsed > _POWER_OF_TWO_MAX or parsed & (parsed - 1):
        raise ConfigError(f"{path} must be a power of two <= {_POWER_OF_TWO_MAX}.")
    return parsed


def _strings(value: Any, *, path: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ConfigError(f"{path} must be an array of non-empty strings.")
    result = tuple(item.strip() for item in value)
    if len(result) != len(set(result)):
        raise ConfigError(f"{path} must not contain duplicates.")
    return result


def _positive_int(value: Any, *, path: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"{path} must be an integer >= 1.")
    parsed = int(value)
    if parsed < 1:
        raise ConfigError(f"{path} must be an integer >= 1.")
    return parsed


def _required_string(value: Any, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path} must be a non-empty string.")
    return value.strip()


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{path} must be a mapping.")
    return dict(value)


def _unknown(value: Mapping[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(f"{path} contains unsupported fields: {', '.join(unknown)}")
