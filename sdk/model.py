# Copyright (C) 2024 Akave
# See LICENSE for copying information.

"""
Module containing data model classes for the Akave SDK.
These classes are the Python equivalent of Go structs from model.go.
"""

import time
from typing import List, Optional, Union, Any
from dataclasses import dataclass


@dataclass
class BucketCreateResult:
    """Result of bucket creation."""
    name: str
    created_at: Any  # Could be time.time or a datetime object


@dataclass
class Bucket:
    """A bucket."""
    name: str
    created_at: Any


@dataclass
class Chunk:
    """A piece of metadata of some file."""
    cid: str
    encoded_size: int
    size: int
    index: int


@dataclass
class AkaveBlockData:
    """Akavenode block metadata."""
    permit: str
    node_address: str
    node_id: str


@dataclass
class FilecoinBlockData:
    """Filecoin block metadata."""
    base_url: str


@dataclass
class FileBlockUpload:
    """A piece of metadata of some file used for upload."""
    cid: str
    data: bytes
    permit: str = ""
    node_address: str = ""
    node_id: str = ""


@dataclass
class FileBlockDownload:
    """A piece of metadata of some file used for download."""
    cid: str
    data: bytes
    filecoin: Optional[FilecoinBlockData] = None
    akave: Optional[AkaveBlockData] = None


@dataclass
class FileListItem:
    """Contains bucket file list file meta information."""
    root_cid: str
    name: str
    size: int
    created_at: Any


@dataclass
class FileUpload:
    """Contains single file meta information."""
    bucket_name: str
    name: str
    stream_id: str
    created_at: Any


@dataclass
class FileChunkUpload:
    """Contains single file chunk meta information."""
    stream_id: str
    index: int
    chunk_cid: Any  # CID object
    actual_size: int
    raw_data_size: int
    proto_node_size: int
    blocks: List[FileBlockUpload]


@dataclass
class FileDownload:
    """Contains single file meta information."""
    stream_id: str
    bucket_name: str
    name: str
    chunks: List[Chunk]


@dataclass
class FileChunkDownload:
    """Contains single file chunk meta information."""
    cid: str
    index: int
    encoded_size: int
    size: int
    blocks: List[FileBlockDownload]


@dataclass
class FileMeta:
    """Contains single file meta information."""
    stream_id: str
    root_cid: str
    bucket_name: str
    name: str
    encoded_size: int
    size: int
    created_at: Any
    committed_at: Any  # Note: Fixed typo from "CommitedAt" to "committed_at"


@dataclass
class IPCBucketCreateResult:
    """Result of IPC bucket creation."""
    name: str
    created_at: Any


@dataclass
class IPCBucket:
    """An IPC bucket."""
    id: str
    name: str
    created_at: Any


@dataclass
class IPCFileDownload:
    """Represents an IPC file download and some metadata."""
    bucket_name: str
    name: str
    chunks: List[Chunk]


@dataclass
class IPCFileListItem:
    """Contains IPC bucket file list file meta information."""
    root_cid: str
    name: str
    encoded_size: int
    created_at: Any


@dataclass
class IPCFileMeta:
    """Contains single IPC file meta information."""
    root_cid: str
    name: str
    bucket_name: str
    encoded_size: int
    created_at: Any


@dataclass
class IPCFileMetaV2:
    """Contains single file meta information."""
    root_cid: str
    bucket_name: str
    name: str
    encoded_size: int
    size: int = 0
    created_at: Any = None
    committed_at: Any = None


@dataclass
class IPCFileChunkUploadV2:
    """Contains single file chunk meta information."""
    index: int
    chunk_cid: Any  # CID object
    actual_size: int
    raw_data_size: int
    proto_node_size: int
    blocks: List[FileBlockUpload]
    bucket_id: bytes  # 32-byte array in Go, using bytes in Python
    file_name: str
