"""Immutable object-store publication and remote dataset contracts."""

from .config import ObjectStoreTarget, PublicationSpec, load_object_store_target
from .publication import PublicationResult, publish_committed_dataset
from .remote_reader import (
    RemoteGenerationHandle,
    lookup_remote_parquet_key_coordinates,
    open_remote_generation,
    read_remote_parquet_key_to_ipc,
    read_remote_parquet_to_ipc,
)

__all__ = [
    "ObjectStoreTarget",
    "PublicationResult",
    "PublicationSpec",
    "RemoteGenerationHandle",
    "load_object_store_target",
    "lookup_remote_parquet_key_coordinates",
    "publish_committed_dataset",
    "open_remote_generation",
    "read_remote_parquet_key_to_ipc",
    "read_remote_parquet_to_ipc",
]
