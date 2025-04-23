import grpc
import ipfshttpclient
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from private.pb import nodeapi_pb2, nodeapi_pb2_grpc, ipcnodeapi_pb2, ipcnodeapi_pb2_grpc
from private.ipc.client import Client, Config
from private.spclient.spclient import SPClient
from private.encryption import derive_key
from typing import List, Optional
from multiformats import cid
from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI
from .erasure_code import ErasureCode
from .common import SDKError, BLOCK_SIZE, MIN_BUCKET_NAME_LENGTH
import os
import time

class SDK:
    def __init__(self, address: str, max_concurrency: int, block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes] = None, private_key: Optional[str] = None,
                 streaming_max_blocks_in_chunk: int = 32, parity_blocks_count: int = 0):
        self.client = None
        self.conn = None
        self.sp_client = None
        self.streaming_erasure_code = None
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.private_key = private_key
        self.encryption_key = encryption_key or []
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count

        if self.block_part_size <= 0 or self.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        # Create gRPC channel and clients
        self.conn = grpc.insecure_channel(address)
        self.client = nodeapi_pb2_grpc.NodeAPIStub(self.conn)
        self.ipc_client = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.conn)

        if len(self.encryption_key) != 0 and len(self.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        if self.parity_blocks_count > self.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.parity_blocks_count} should be <= {self.streaming_max_blocks_in_chunk // 2}")

        if self.parity_blocks_count > 0:
            self.streaming_erasure_code = ErasureCode(self.streaming_max_blocks_in_chunk - self.parity_blocks_count, self.parity_blocks_count)

        self.sp_client = SPClient()

    def close(self):
        """Close the gRPC channel."""
        if self.conn:
            self.conn.close()

    def streaming_api(self):
        """Returns SDK streaming API."""
        return StreamingAPI(
            conn=self.conn,
            client=nodeapi_pb2_grpc.StreamAPIStub(self.conn),
            erasure_code=self.streaming_erasure_code,
            max_concurrency=self.max_concurrency,
            block_part_size=self.block_part_size,
            use_connection_pool=self.use_connection_pool,
            encryption_key=self.encryption_key,
            max_blocks_in_chunk=self.streaming_max_blocks_in_chunk
        )

    def ipc(self):
        """Returns SDK IPC API."""
        try:
            # Get connection parameters from the node
            try:
                params_request = ipcnodeapi_pb2.ConnectionParamsRequest()
                conn_params = self.ipc_client.ConnectionParams(params_request)
                logging.info(f"Received connection params - dial_uri: {conn_params.dial_uri}, contract: {conn_params.contract_address}")
            except Exception as e:
                logging.warning(f"Could not get connection parameters, using defaults: {str(e)}")
                # Use default values if connection params call fails
                conn_params = type('ConnectionParams', (), {
                    'dial_uri': os.getenv('ETHEREUM_NODE_URL', 'https://n1-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc'),
                    'contract_address': os.getenv('STORAGE_CONTRACT_ADDRESS', '')
                })
            
            if not self.private_key:
                raise SDKError("Private key is required for IPC operations")
            
            # Prepare IPC client configuration
            config = Config(
                dial_uri=conn_params.dial_uri,
                private_key=self.private_key,
                storage_contract_address=conn_params.contract_address,
                access_contract_address=os.getenv('ACCESS_CONTRACT_ADDRESS', '')
            )
            
            # Create IPC instance with retries
            max_retries = 3
            retry_delay = 1  
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    ipc_instance = Client.dial(config)
                    if ipc_instance:
                        logging.info("Successfully connected to Ethereum node")
                        break
                except Exception as e:
                    last_error = e
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    continue
            else:
                raise SDKError(f"Failed to dial IPC client after {max_retries} attempts: {str(last_error)}")
            
            return IPC(
                client=self.ipc_client,
                conn=self.conn,
                ipc_instance=ipc_instance,
                max_concurrency=self.max_concurrency,
                block_part_size=self.block_part_size,
                use_connection_pool=self.use_connection_pool,
                encryption_key=self.encryption_key,
                max_blocks_in_chunk=self.streaming_max_blocks_in_chunk
            )
        except Exception as e:
            raise SDKError(f"Failed to initialize IPC API: {str(e)}")

    def create_bucket(self, ctx, name: str):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketCreateRequest(name=name)
        response = self.client.BucketCreate(request)
        return BucketCreateResult(name=response.name, created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at)

    def view_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketViewRequest(bucket_name=name)
        response = self.client.BucketView(request)
        return Bucket(
            name=response.name, 
            created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at
        )

    def delete_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")
           
        try:
            request = nodeapi_pb2.BucketDeleteRequest(name=name)
            self.client.BucketDelete(request)
            return True
        except Exception as err:
            logging.error(f"Error deleting bucket: {err}")
            raise SDKError(f"Failed to delete bucket: {err}")

    def extract_block_data(id_str: str, data: bytes) -> bytes:
        try:
         block_cid = cid.decode(id_str)
        except Exception as e:
          raise ValueError(f"Invalid CID: {e}")

        if block_cid.codec == "dag-pb":
          try:
            dag_node = ipfshttpclient.codec.decode("dag-pb", data) #Decoding the DAG node
            unixfs_data = dag_node["Data"] 
            return unixfs_data
          except Exception as e:
            raise ValueError(f"Failed to decode DAG node: {e}")
    
        elif block_cid.codec == "raw":
         return data 
     
        else:
         raise ValueError(f"Unknown CID type: {block_cid.codec}")


class BucketCreateResult:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

class Bucket:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

def encryption_key_derivation(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return None

    info = "/".join(info_data)
    key = derive_key(parent_key, info.encode())
    return key
