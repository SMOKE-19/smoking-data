from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Iterator
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from smoking_data.core.exceptions import SmokingDataError

from .spec import CsvSourceSpec


@dataclass(frozen=True, slots=True)
class PreparedCsvSource:
    root: Path
    metadata: dict[str, Any]


@contextmanager
def prepare_csv_source(spec: CsvSourceSpec) -> Iterator[PreparedCsvSource]:
    if spec.source_transport == "directory":
        if spec.source_directory is None:
            raise SmokingDataError("0103 directory transport is missing its directory.")
        yield PreparedCsvSource(
            root=spec.source_directory,
            metadata={"transport": "directory"},
        )
        return

    options = dict(spec.download_options or {})
    temp_parent = spec.project_root / ".temp"
    temp_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="0103-http-", dir=temp_parent) as raw_temp:
        temp_root = Path(raw_temp)
        previous_entry = _previous_gdelt_entry(spec, options.get("discovery"))
        resolved_options, discovery_metadata = _resolve_download(
            options,
            previous_entry=previous_entry,
        )
        download_path = temp_root / str(resolved_options["file_name"])
        metadata = _download(resolved_options, download_path)
        if discovery_metadata is not None:
            metadata["discovery"] = discovery_metadata
        source_root = temp_root / "source"
        source_root.mkdir()
        archive_format = _detect_format(resolved_options, download_path, metadata)
        if archive_format == "zip":
            _safe_extract_zip(download_path, source_root, resolved_options)
        else:
            shutil.move(str(download_path), source_root / download_path.name)
        metadata["format"] = archive_format
        yield PreparedCsvSource(root=source_root, metadata=metadata)


def _resolve_download(
    options: dict[str, Any],
    *,
    previous_entry: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    discovery = options.get("discovery")
    if not isinstance(discovery, dict):
        return options, None
    if discovery.get("type") != "gdelt_v2_masterfile":
        raise SmokingDataError("Unsupported 0103 discovery type.", code="0103.discovery.unsupported")
    master_url = str(discovery["url"])
    file_kind = str(discovery["file_kind"])
    timeout_sec = float(options["timeout_sec"])
    lastupdate_url = _gdelt_lastupdate_url(master_url)
    selection_source: str | None = None
    selected: dict[str, Any] | None = None
    lastupdate_bytes = 0
    availability_status: int | None = None
    fast_path_error_type: str | None = None
    try:
        latest, lastupdate_bytes = _fetch_gdelt_listing(
            lastupdate_url,
            file_kind=file_kind,
            timeout_sec=timeout_sec,
            max_bytes=1024 * 1024,
            enforce_cutoff=False,
        )
        latest = {
            **latest,
            "url": _validated_gdelt_data_url(str(latest["url"]), listing_url=lastupdate_url),
        }
        available, availability_status = _gdelt_file_available(
            str(latest["url"]), timeout_sec=timeout_sec
        )
        if available:
            selected = latest
            selection_source = "lastupdate"
    except Exception as exc:  # noqa: BLE001 - fast path falls back without exposing URLs.
        fast_path_error_type = type(exc).__name__

    master_bytes = 0
    if selected is None and previous_entry is not None:
        selected = previous_entry
        selection_source = "previous_success"
    if selected is None:
        try:
            selected, master_bytes = _fetch_gdelt_listing(
                master_url,
                file_kind=file_kind,
                timeout_sec=timeout_sec,
                max_bytes=512 * 1024 * 1024,
                stability_lag_minutes=int(discovery.get("stability_lag_minutes") or 0),
            )
            selected = {
                **selected,
                "url": _validated_gdelt_data_url(
                    str(selected["url"]), listing_url=master_url
                ),
            }
            selection_source = "masterfile_fallback"
        except SmokingDataError:
            raise
        except Exception as exc:  # noqa: BLE001 - URL values are redacted at the boundary.
            raise SmokingDataError(
                f"0103 GDELT master discovery failed for {_safe_url(master_url)}: {type(exc).__name__}",
                code="0103.discovery.failed",
            ) from exc
    selected_url = _gdelt_https_url(str(selected["url"]))
    file_name = Path(urlsplit(selected_url).path).name
    resolved = {
        **options,
        "url": selected_url,
        "file_name": file_name,
        "expected_size_bytes": selected["size_bytes"],
        "expected_md5": selected["md5"],
    }
    return resolved, {
        "type": "gdelt_v2_masterfile",
        "master_url": _safe_url(master_url),
        "lastupdate_url": _safe_url(lastupdate_url),
        "file_kind": file_kind,
        "selection": "latest",
        "selection_source": selection_source,
        "stability_lag_minutes": int(discovery.get("stability_lag_minutes") or 0),
        "selected_url": _safe_url(selected_url),
        "selected_file_name": file_name,
        "expected_size_bytes": selected["size_bytes"],
        "expected_md5": selected["md5"],
        "lastupdate_bytes_scanned": lastupdate_bytes,
        "lastupdate_availability_status": availability_status,
        "lastupdate_error_type": fast_path_error_type,
        "master_bytes_scanned": master_bytes,
    }


def _fetch_gdelt_listing(
    url: str,
    *,
    file_kind: str,
    timeout_sec: float,
    max_bytes: int,
    stability_lag_minutes: int = 0,
    enforce_cutoff: bool = True,
) -> tuple[dict[str, Any], int]:
    request = Request(
        url,
        headers={"User-Agent": "smoking-data-gdelt-source/1.0"},
        method="GET",
    )
    with urlopen(request, timeout=timeout_sec) as response:  # noqa: S310 - validated HTTP(S) URL.
        return _select_gdelt_stream(
            response,
            file_kind=file_kind,
            stability_lag_minutes=stability_lag_minutes,
            max_bytes=max_bytes,
            enforce_cutoff=enforce_cutoff,
        )


def _gdelt_file_available(url: str, *, timeout_sec: float) -> tuple[bool, int | None]:
    request = Request(
        url,
        headers={"User-Agent": "smoking-data-gdelt-source/1.0"},
        method="HEAD",
    )
    try:
        with urlopen(request, timeout=timeout_sec) as response:  # noqa: S310 - validated GDELT URL.
            status = int(getattr(response, "status", 200))
            return 200 <= status < 400, status
    except HTTPError as exc:
        return False, int(exc.code)


def _gdelt_lastupdate_url(master_url: str) -> str:
    parts = urlsplit(master_url)
    parent = parts.path.rsplit("/", 1)[0]
    return urlunsplit((parts.scheme, parts.netloc, f"{parent}/lastupdate.txt", "", ""))


def _gdelt_https_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme == "http" and parts.netloc == "data.gdeltproject.org":
        return urlunsplit(("https", parts.netloc, parts.path, parts.query, parts.fragment))
    return value


def _validated_gdelt_data_url(value: str, *, listing_url: str) -> str:
    data_parts = urlsplit(value)
    listing_parts = urlsplit(listing_url)
    listing_parent = listing_parts.path.rsplit("/", 1)[0].rstrip("/") + "/"
    if (
        data_parts.scheme not in {"http", "https"}
        or data_parts.netloc != listing_parts.netloc
        or not data_parts.path.startswith(listing_parent)
    ):
        raise SmokingDataError(
            "0103 GDELT listing contains an out-of-scope data URL.",
            code="0103.discovery.invalid_data_url",
        )
    return _gdelt_https_url(value)


def _previous_gdelt_entry(
    spec: CsvSourceSpec,
    discovery: Any,
) -> dict[str, Any] | None:
    if not isinstance(discovery, dict) or discovery.get("type") != "gdelt_v2_masterfile":
        return None
    metadata_path = spec.output_root / "_smoking_data" / "metadata.json"
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        previous = payload["acquisition"]["discovery"]
        if (
            previous.get("type") != "gdelt_v2_masterfile"
            or previous.get("file_kind") != discovery.get("file_kind")
            or previous.get("master_url") != _safe_url(str(discovery["url"]))
        ):
            return None
        url = str(previous["selected_url"])
        master_url = str(discovery["url"])
        url_parts = urlsplit(url)
        master_parts = urlsplit(master_url)
        master_parent = master_parts.path.rsplit("/", 1)[0].rstrip("/") + "/"
        size_bytes = int(previous["expected_size_bytes"])
        checksum = str(previous["expected_md5"]).lower()
        if (
            not url.startswith(("http://", "https://"))
            or url_parts.netloc != master_parts.netloc
            or not url_parts.path.startswith(master_parent)
            or size_bytes < 1
            or not re.fullmatch(r"[0-9a-f]{32}", checksum)
        ):
            return None
        return {"url": url, "size_bytes": size_bytes, "md5": checksum}
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _select_gdelt_entry(
    payload: bytes, *, file_kind: str, stability_lag_minutes: int = 0
) -> dict[str, Any]:
    selected, _ = _select_gdelt_stream(
        BytesIO(payload),
        file_kind=file_kind,
        stability_lag_minutes=stability_lag_minutes,
    )
    return selected


def _select_gdelt_stream(
    stream: Any,
    *,
    file_kind: str,
    stability_lag_minutes: int = 0,
    max_bytes: int = 512 * 1024 * 1024,
    enforce_cutoff: bool = True,
) -> tuple[dict[str, Any], int]:
    suffix = {
        "events": ".export.CSV.zip",
        "mentions": ".mentions.CSV.zip",
        "gkg": ".gkg.csv.zip",
    }[file_kind]
    selected: dict[str, Any] | None = None
    total_bytes = 0
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stability_lag_minutes)
    for line_number, raw_line in enumerate(stream, start=1):
        total_bytes += len(raw_line)
        if total_bytes > max_bytes:
            raise SmokingDataError("0103 GDELT listing exceeds its size limit.", code="0103.discovery.size_limit")
        try:
            line = raw_line.decode("utf-8-sig").strip()
        except UnicodeDecodeError as exc:
            raise SmokingDataError("0103 GDELT master file is not UTF-8.", code="0103.discovery.invalid_masterfile") from exc
        parts = line.split()
        if len(parts) != 3 or not parts[2].endswith(suffix):
            continue
        timestamp_match = re.search(r"/(\d{14})\.", parts[2])
        if timestamp_match is not None:
            published_slot = datetime.strptime(
                timestamp_match.group(1), "%Y%m%d%H%M%S"
            ).replace(tzinfo=timezone.utc)
            if enforce_cutoff and published_slot > cutoff:
                continue
        try:
            size_bytes = int(parts[0])
        except ValueError as exc:
            raise SmokingDataError(
                f"0103 GDELT master line {line_number} has an invalid size.",
                code="0103.discovery.invalid_masterfile",
            ) from exc
        checksum = parts[1].lower()
        if size_bytes < 1 or not re.fullmatch(r"[0-9a-f]{32}", checksum):
            raise SmokingDataError(
                f"0103 GDELT master line {line_number} is invalid.",
                code="0103.discovery.invalid_masterfile",
            )
        selected = {"size_bytes": size_bytes, "md5": checksum, "url": parts[2]}
    if selected is None:
        raise SmokingDataError(
            f"0103 GDELT master has no {file_kind} entries.",
            code="0103.discovery.no_match",
        )
    return selected, total_bytes


def _download(options: dict[str, Any], target: Path) -> dict[str, Any]:
    url = _request_url(str(options["url"]), dict(options.get("query") or {}))
    headers = {
        name: _expand_environment(value)
        for name, value in dict(options.get("headers") or {}).items()
    }
    request = Request(url, headers=headers, method="GET")
    digest = hashlib.sha256()
    downloaded = 0
    md5_digest = hashlib.md5(usedforsecurity=False)
    try:
        with urlopen(request, timeout=float(options["timeout_sec"])) as response, target.open("wb") as stream:  # noqa: S310 - validated HTTP(S) URL.
            status_code = int(getattr(response, "status", 200))
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                downloaded += len(chunk)
                if downloaded > int(options["max_download_bytes"]):
                    raise SmokingDataError(
                        "0103 HTTP download exceeded max_download_bytes.",
                        code="0103.download.size_limit",
                    )
                digest.update(chunk)
                md5_digest.update(chunk)
                stream.write(chunk)
            response_headers = response.headers
    except SmokingDataError:
        raise
    except Exception as exc:  # noqa: BLE001 - redact request values at boundary.
        raise SmokingDataError(
            f"0103 HTTP download failed for {_safe_url(str(options['url']))}: {type(exc).__name__}",
            code="0103.download.failed",
        ) from exc
    expected_size = options.get("expected_size_bytes")
    expected_md5 = options.get("expected_md5")
    if expected_size is not None and downloaded != int(expected_size):
        raise SmokingDataError("0103 downloaded size does not match discovery metadata.", code="0103.download.size_mismatch")
    if expected_md5 is not None and md5_digest.hexdigest() != str(expected_md5):
        raise SmokingDataError("0103 downloaded checksum does not match discovery metadata.", code="0103.download.checksum_mismatch")
    return {
        "transport": "http_download",
        "url": _safe_url(str(options["url"])),
        "query_parameter_names": sorted(dict(options.get("query") or {})),
        "header_names": sorted(headers),
        "status_code": status_code,
        "content_type": response_headers.get("Content-Type"),
        "content_length": response_headers.get("Content-Length"),
        "etag": response_headers.get("ETag"),
        "last_modified": response_headers.get("Last-Modified"),
        "downloaded_bytes": downloaded,
        "sha256": digest.hexdigest(),
    }


def _request_url(base_url: str, query: dict[str, str]) -> str:
    parts = urlsplit(base_url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise SmokingDataError("0103 download URL must use http or https.")
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    pairs.extend((name, _expand_environment(value)) for name, value in query.items())
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))


def _expand_environment(value: str) -> str:
    expanded = os.path.expandvars(value)
    if "${" in expanded:
        raise SmokingDataError(
            "0103 HTTP setting references an unavailable environment variable.",
            code="0103.download.environment_missing",
        )
    return expanded


def _detect_format(options: dict[str, Any], path: Path, metadata: dict[str, Any]) -> str:
    configured = str(options["format"])
    if configured != "auto":
        if configured == "zip" and not zipfile.is_zipfile(path):
            raise SmokingDataError("0103 response is not a valid ZIP archive.", code="0103.download.invalid_zip")
        return configured
    content_type = str(metadata.get("content_type") or "").lower()
    return "zip" if "zip" in content_type or zipfile.is_zipfile(path) else "dsv"


def _safe_extract_zip(path: Path, target: Path, options: dict[str, Any]) -> None:
    total_size = 0
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        if len(members) > int(options["max_archive_members"]):
            raise SmokingDataError("0103 ZIP exceeds max_archive_members.", code="0103.download.archive_limit")
        for member in members:
            pure = PurePosixPath(member.filename.replace("\\", "/"))
            if pure.is_absolute() or ".." in pure.parts:
                raise SmokingDataError("0103 ZIP contains an unsafe path.", code="0103.download.unsafe_zip")
            if stat.S_ISLNK(member.external_attr >> 16):
                raise SmokingDataError("0103 ZIP contains a symbolic link.", code="0103.download.unsafe_zip")
            total_size += member.file_size
            if total_size > int(options["max_extracted_bytes"]):
                raise SmokingDataError("0103 ZIP exceeds max_extracted_bytes.", code="0103.download.archive_limit")
        for member in members:
            destination = target.joinpath(*PurePosixPath(member.filename.replace("\\", "/")).parts)
            if member.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, destination.open("wb") as output:
                shutil.copyfileobj(source, output)


def _safe_url(value: str) -> str:
    parts = urlsplit(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
