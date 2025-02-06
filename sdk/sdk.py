import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from . import ipc, memory, pb, spclient
from private.encryption import derive_key
from typing import List, Optional

BLOCK_SIZE = 1 * memory.MB
ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_BUCKET_NAME_LENGTH = 3
MIN_FILE_SIZE = 127  # 127 bytes

from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI

class SDKError(Exception):
    pass

class SDK:
    def __init__(self, address: str, max_concurrency: int, block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes] = None, private_key: Optional[str] = None,
                 streaming_max_blocks_in_chunk: int = 32, parity_blocks_count: int = 0):
        self.client = None
        self.conn = None
        self.sp_client = None
        self.streaming_erasure_code = None

        # Initializing variables
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.private_key = private_key
        self.encryption_key = encryption_key or []
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count

        if self.block_part_size <= 0 or self.block_part_size > memory.BLOCK_SIZE_LIMIT:
            raise SDKError(f"Invalid blockPartSize: {block_part_size}. Valid range is 1-{memory.BLOCK_SIZE_LIMIT}")

        # gRPC client connection
        self.conn = grpc.insecure_channel(address)
        self.client = pb.NodeAPIClient(self.conn)

        # Additional configurations (erasure coding, encryption validation)
        if len(self.encryption_key) != 0 and len(self.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        if self.parity_blocks_count > self.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.parity_blocks_count} should be <= {self.streaming_max_blocks_in_chunk // 2}")

        if self.parity_blocks_count > 0:
            self.streaming_erasure_code = self.new_erasure_code(self.streaming_max_blocks_in_chunk - self.parity_blocks_count, self.parity_blocks_count)

        self.sp_client = spclient.SPClient()

    def close(self):
        if self.conn:
            self.conn.close()

    def streaming_api(self):
        return StreamingAPI(self.conn, self.client, self.streaming_erasure_code, self.max_concurrency,
                            self.block_part_size, self.use_connection_pool, self.encryption_key,
                            self.streaming_max_blocks_in_chunk)

    def ipc(self):
        client = pb.IPCNodeAPIClient(self.conn)
        ipc_instance = ipc.Dial(self.conn, self.private_key, client)
        return IPC(client, self.conn, self.max_concurrency, self.block_part_size, self.use_connection_pool, self.encryption_key, ipc_instance)

    def create_bucket(self, name: str):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")

        # Call to gRPC API
        response = self.client.bucket_create(pb.BucketCreateRequest(name=name))
        return BucketCreateResult(name=response.name, created_at=response.created_at)

    def view_bucket(self, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")

        # Call to gRPC API
        response = self.client.bucket_view(pb.BucketViewRequest(name=name))
        return Bucket(name=response.name, created_at=response.created_at)

    def delete_bucket(self, name:str):
       # Call to gRPC API
       try:
           self.client.bucket_delete(pb.BucketDeleteRequest(name=name))
       except SDKError as err:
              logging.error(f"Error deleting bucket: {err}")
              return False

    def new_erasure_code(self, data_blocks: int, parity_blocks: int):
        # Placeholder for erasure coding logic
        pass


class BucketCreateResult:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

class Bucket:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

# Encryption key derivation
def encryption_key_derivation(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return None

    info = "/".join(info_data)
    key = derive_key(parent_key, info.encode())
    return key
