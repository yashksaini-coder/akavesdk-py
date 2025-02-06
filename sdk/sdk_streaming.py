import grpc
from typing import Optional

from private import pb
from .erasure_code import ErasureCode

class StreamingAPI:
    """
    Exposes SDK file streaming API in Python.
    """
    def __init__(self, conn: grpc.ClientConn, client: pb.StreamAPIClient,
                 erasure_code: Optional['ErasureCode'], max_concurrency: int,
                 block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes], max_blocks_in_chunk: int):
        self.client = client
        self.conn = conn
        self.erasure_code = erasure_code
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key  # None means no encryption
        self.max_blocks_in_chunk = max_blocks_in_chunk
