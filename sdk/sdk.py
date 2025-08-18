import grpc
import logging
from private.pb import nodeapi_pb2, nodeapi_pb2_grpc, ipcnodeapi_pb2, ipcnodeapi_pb2_grpc
from private.ipc.client import Client
from private.spclient.spclient import SPClient
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, TypeVar
from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI
from .erasure_code import ErasureCode
from .config import Config, SDKConfig, SDKError, BLOCK_SIZE, MIN_BUCKET_NAME_LENGTH
from private.encryption import derive_key
from .shared.grpc_base import GrpcClientBase
import time


class AkaveContractFetcher:
    """Fetches contract addresses from Akave node"""
    
    def __init__(self, node_address: str):
        self.node_address = node_address
        self.channel = None
        self.stub = None
    
    def connect(self) -> bool:
        """Connect to the Akave node"""
        try:
            logging.info(f"🔗 Connecting to {self.node_address}...")
            self.channel = grpc.insecure_channel(self.node_address)
            self.stub = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.channel)
            return True
            
        except grpc.RpcError as e:
            logging.error(f"❌ gRPC error: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            logging.error(f"❌ Connection error: {type(e).__name__}: {str(e)}")
            return False
    
    def fetch_contract_addresses(self) -> Optional[dict]:
        """Fetch contract addresses from the node"""
        if not self.stub:
            return None
        
        try:
            request = ipcnodeapi_pb2.ConnectionParamsRequest()
            response = self.stub.ConnectionParams(request)
            
            contract_info = {
                'dial_uri': response.dial_uri if hasattr(response, 'dial_uri') else None,
                'contract_address': response.storage_address if hasattr(response, 'storage_address') else None,
            }
            
            if hasattr(response, 'access_address'):
                contract_info['access_address'] = response.access_address
            
            return contract_info
        except Exception as e:
            logging.error(f"❌ Error fetching contract info: {e}")
            return None
    
    def close(self):
        """Close the gRPC connection"""
        if self.channel:
            self.channel.close()


T = TypeVar("T")  # Generic return type for gRPC calls

@dataclass
class BucketCreateResult:
    name: str
    created_at: datetime


@dataclass
class Bucket:
    name: str
    created_at: datetime

class SDK():
    def __init__(self, config: SDKConfig):
        self.conn = None
        self.ipc_conn = None
        self.ipc_client = None
        self.sp_client = None
        self.streaming_erasure_code = None
        self.config = config
        self._grpc_base = GrpcClientBase(self.config.connection_timeout)

        self.config.encryption_key = config.encryption_key or []
        self.ipc_address = config.ipc_address or config.address  # Use provided IPC address or fallback to main address
        self._contract_info = None

        if self.config.block_part_size <= 0 or self.config.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {config.block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        # Create gRPC channel and clients for SDK operations
        self.conn = grpc.insecure_channel(config.address)
        self.client = nodeapi_pb2_grpc.NodeAPIStub(self.conn)
       
        # Create separate gRPC channel for IPC operations if needed
        if self.ipc_address == config.address:
            # Reuse main connection for IPC
            self.ipc_conn = self.conn
        else:
            # Create separate connection for IPC
            self.ipc_conn = grpc.insecure_channel(self.ipc_address)
        
        self.ipc_client = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.ipc_conn)

        if len(self.config.encryption_key) != 0 and len(self.config.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        if self.config.parity_blocks_count > self.config.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.config.parity_blocks_count} should be <= {self.config.streaming_max_blocks_in_chunk // 2}")

        if self.config.parity_blocks_count > 0:
            self.streaming_erasure_code = ErasureCode(self.config.streaming_max_blocks_in_chunk - self.config.parity_blocks_count, self.config.parity_blocks_count)

        self.sp_client = SPClient()

    def _fetch_contract_info(self) -> Optional[dict]:
        """Dynamically fetch contract information using multiple endpoints"""
        if self._contract_info:
            return self._contract_info
            
        endpoints = [
            'yucca.akave.ai:5500',
            # 'connect.akave.ai:5500'  # DNS resolution failing
        ]
        
        for endpoint in endpoints:
            logging.info(f"🔄 Trying endpoint: {endpoint}")
            fetcher = AkaveContractFetcher(endpoint)
            
            if fetcher.connect():
                logging.info("✅ Connected successfully!")
                
                info = fetcher.fetch_contract_addresses()
                fetcher.close()
                
                if info and info.get('contract_address') and info.get('dial_uri'):
                    logging.info("✅ Successfully fetched contract information!")
                    logging.info(f"📍 Contract Details: dial_uri={info.get('dial_uri')}, contract_address={info.get('contract_address')}")
                    self._contract_info = info
                    return info
                else:
                    logging.warning("❌ Failed to fetch complete contract information")
            else:
                logging.warning(f"❌ Failed to connect to {endpoint}")
                fetcher.close()
        
        logging.error("❌ All endpoints failed for contract fetching")
        return None

    def close(self):
        """Close the gRPC channels."""
        if self.conn:
            self.conn.close()
        if self.ipc_conn and self.ipc_conn != self.conn:
            self.ipc_conn.close()

    def streaming_api(self):
        """Returns SDK streaming API."""
        return StreamingAPI(
            conn=self.conn,
            client=nodeapi_pb2_grpc.StreamAPIStub(self.conn),
            erasure_code=self.streaming_erasure_code,
            max_concurrency=self.config.max_concurrency,
            block_part_size=self.config.block_part_size,
            use_connection_pool=self.config.use_connection_pool,
            encryption_key=self.config.encryption_key,
            max_blocks_in_chunk=self.config.streaming_max_blocks_in_chunk
        )

    def ipc(self):
        """Returns SDK IPC API."""
        try:
            # Get connection parameters dynamically
            conn_params = self._fetch_contract_info()
            
            if not conn_params:
                raise SDKError("Could not fetch contract information from any Akave node")
            
            if not self.config.private_key:
                raise SDKError("Private key is required for IPC operations")
            
            config = Config(
                dial_uri=conn_params['dial_uri'],
                private_key=self.config.private_key,
                storage_contract_address=conn_params['contract_address'],
                access_contract_address=conn_params.get('access_address', '')
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
                conn=self.ipc_conn,  # Use the IPC connection
                ipc_instance=ipc_instance,
                config=self.config
            )
        except Exception as e:
            raise SDKError(f"Failed to initialize IPC API: {str(e)}")
    
    def _validate_bucket_name(self, name: str, method_name: str) -> None:
        if not name or len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError(
                f"{method_name}: Invalid bucket name '{name}'. "
                f"Must be at least {MIN_BUCKET_NAME_LENGTH} characters "
                f"(got {len(name) if name else 0})."
            )
    def _do_grpc_call(self, method_name: str, grpc_method: Callable[..., T], request) -> T:
        try:
            return grpc_method(request, timeout=self.config.connection_timeout)
        except grpc.RpcError as e:
            self._grpc_base._handle_grpc_error(method_name, e)
            raise  # for making type checkers happy

    def create_bucket(self, name: str) -> BucketCreateResult:
        self._validate_bucket_name(name, "BucketCreate")
        request = nodeapi_pb2.BucketCreateRequest(name=name)
        response = self._do_grpc_call("BucketCreate", self.client.BucketCreate, request)
        return BucketCreateResult(
            name=response.name,
            created_at=parse_timestamp(response.created_at)
        )
    
    def view_bucket(self, name: str)-> Bucket:
        self._validate_bucket_name(name, "BucketView")
        request = nodeapi_pb2.BucketViewRequest(bucket_name=name)
        response = self._do_grpc_call("BucketView", self.client.BucketView, request)
        return Bucket(
            name=response.name,
            created_at=parse_timestamp(response.created_at)
        )
    
    def delete_bucket(self, name: str):
        self._validate_bucket_name(name, "BucketDelete")
        request = nodeapi_pb2.BucketDeleteRequest(name=name)
        self._do_grpc_call("BucketDelete", self.client.BucketDelete, request)
        return True
    
def encryption_key_derivation(parent_key: bytes, *info_data: str) -> bytes:
    if not parent_key:
        raise SDKError("Parent key is required for key derivation")
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def parse_timestamp(ts) -> Optional[datetime]:
    if ts is None:
        return None
    return ts.AsTime() if hasattr(ts, "AsTime") else ts
