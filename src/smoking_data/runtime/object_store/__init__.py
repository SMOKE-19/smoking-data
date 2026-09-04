"""Immutable object-store publication and remote dataset contracts."""

from .config import ObjectStoreTarget, PublicationSpec, load_object_store_target
from .publication import PublicationResult, publish_committed_dataset
from .remote_reader import (
    RemoteGenerationHandle,
    lookup_remote_parquet_key_coordinates,
    open_remote_generation,
    read_remote_parquet_file_index,
    read_remote_parquet_key_to_ipc,
    read_remote_parquet_planning_index,
    read_remote_parquet_row_group_index,
    read_remote_parquet_to_ipc,
    remote_parquet_objects,
)
from .remote_upstream import (
    RemoteSelectorContext,
    materialize_remote_active_payload,
    materialize_remote_parquet_files,
    materialize_remote_projected_selector_proxies,
    materialize_remote_selector_proxies,
)

__all__ = [
    "ObjectStoreTarget",
    "PublicationResult",
    "PublicationSpec",
    "RemoteGenerationHandle",
    "RemoteSelectorContext",
    "load_object_store_target",
    "lookup_remote_parquet_key_coordinates",
    "materialize_remote_active_payload",
    "materialize_remote_parquet_files",
    "materialize_remote_projected_selector_proxies",
    "materialize_remote_selector_proxies",
    "publish_committed_dataset",
    "open_remote_generation",
    "read_remote_parquet_file_index",
    "read_remote_parquet_key_to_ipc",
    "read_remote_parquet_planning_index",
    "read_remote_parquet_row_group_index",
    "read_remote_parquet_to_ipc",
    "remote_parquet_objects",
]
