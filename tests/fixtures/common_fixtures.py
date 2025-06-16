"""
Common test fixtures for Akave SDK unit tests.
"""

import os
import time
from unittest.mock import MagicMock, Mock, patch
from datetime import datetime
from typing import Dict, Any, List, Optional

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

# Sample test data
def sample_encryption_key() -> bytes:
    """Returns a sample 32-byte encryption key for testing."""
    return b'a' * 32

def sample_config_data() -> Dict[str, Any]:
    """Returns sample configuration data for testing."""
    return {
        "address": "localhost:5000",
        "max_concurrency": 10,
        "block_part_size": 1024 * 1024,
        "use_connection_pool": True,
        "encryption_key": sample_encryption_key(),
        "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "max_blocks_in_chunk": 32
    }

def sample_bucket_data() -> Dict[str, Any]:
    """Returns sample bucket data for testing."""
    return {
        "name": "test-bucket",
        "id": "bucket_id_123",
        "created_at": int(time.time()),
        "owner_address": "0x1234567890abcdef"
    }

def sample_file_data() -> Dict[str, Any]:
    """Returns sample file data for testing."""
    return {
        "name": "test-file.txt",
        "bucket_name": "test-bucket",
        "root_cid": "QmTest123456789",
        "encoded_size": 1024,
        "size": 512,
        "created_at": int(time.time()),
        "stream_id": "stream_123",
        "chunks": [
            {
                "cid": "QmChunk123",
                "index": 0,
                "encoded_size": 512,
                "size": 256
            }
        ]
    }

# Mock factories
def mock_grpc_channel() -> MagicMock:
    """Creates a mock gRPC channel."""
    channel = MagicMock()
    channel.close = MagicMock()
    return channel

def mock_node_api_client() -> MagicMock:
    """Creates a mock NodeAPI client with standard responses."""
    client = MagicMock()
    
    # Mock timestamp
    mock_timestamp = MagicMock()
    mock_timestamp.seconds = sample_bucket_data()["created_at"]
    
    # Mock bucket responses
    bucket_response = MagicMock()
    bucket_response.name = sample_bucket_data()["name"]
    bucket_response.created_at = mock_timestamp
    client.BucketCreate.return_value = bucket_response
    client.BucketView.return_value = bucket_response
    
    bucket_list_response = MagicMock()
    bucket_list_response.buckets = [bucket_response]
    client.BucketList.return_value = bucket_list_response
    
    # Mock file responses
    file_data = sample_file_data()
    file_response = MagicMock()
    file_response.file_name = file_data["name"]
    file_response.bucket_name = file_data["bucket_name"]
    file_response.root_cid = file_data["root_cid"]
    file_response.encoded_size = file_data["encoded_size"]
    file_response.created_at = mock_timestamp
    
    file_list_response = MagicMock()
    file_list_response.list = [file_response]
    client.FileList.return_value = file_list_response
    
    return client

def mock_ipc_client() -> MagicMock:
    """Creates a mock IPC client with standard responses."""
    client = MagicMock()
    
    # Mock timestamp
    mock_timestamp = MagicMock()
    mock_timestamp.seconds = sample_bucket_data()["created_at"]
    
    # Mock bucket responses
    bucket_response = MagicMock()
    bucket_response.name = sample_bucket_data()["name"]
    bucket_response.id = sample_bucket_data()["id"]
    bucket_response.created_at = mock_timestamp
    client.BucketView.return_value = bucket_response
    
    bucket_list_response = MagicMock()
    bucket_list_response.buckets = [bucket_response]
    client.BucketList.return_value = bucket_list_response
    
    # Mock file responses
    file_data = sample_file_data()
    file_response = MagicMock()
    file_response.file_name = file_data["name"]
    file_response.name = file_data["name"]  # Some endpoints use 'name' instead of 'file_name'
    file_response.bucket_name = file_data["bucket_name"]
    file_response.root_cid = file_data["root_cid"]
    file_response.encoded_size = file_data["encoded_size"]
    file_response.created_at = mock_timestamp
    client.FileView.return_value = file_response
    
    file_list_response = MagicMock()
    file_list_response.list = [file_response]
    client.FileList.return_value = file_list_response
    
    return client

def mock_streaming_client() -> MagicMock:
    """Creates a mock streaming client with standard responses."""
    client = MagicMock()
    
    # Mock timestamp
    mock_timestamp = MagicMock()
    mock_timestamp.seconds = sample_file_data()["created_at"]
    
    # Mock file upload responses
    file_data = sample_file_data()
    upload_response = MagicMock()
    upload_response.bucket_name = file_data["bucket_name"]
    upload_response.file_name = file_data["name"]
    upload_response.stream_id = file_data["stream_id"]
    upload_response.created_at = mock_timestamp
    client.FileUploadCreate.return_value = upload_response
    
    # Mock file download responses
    chunk_response = MagicMock()
    chunk_response.cid = file_data["chunks"][0]["cid"]
    chunk_response.encoded_size = file_data["chunks"][0]["encoded_size"]
    chunk_response.size = file_data["chunks"][0]["size"]
    
    download_response = MagicMock()
    download_response.stream_id = file_data["stream_id"]
    download_response.bucket_name = file_data["bucket_name"]
    download_response.chunks = [chunk_response]
    client.FileDownloadCreate.return_value = download_response
    client.FileDownloadRangeCreate.return_value = download_response
    
    # Mock chunk upload responses
    chunk_upload_response = MagicMock()
    chunk_upload_response.blocks = []
    client.FileUploadChunkCreate.return_value = chunk_upload_response
    
    return client

def mock_web3_instance() -> MagicMock:
    """Creates a mock Web3 instance."""
    web3 = MagicMock()
    web3.is_connected.return_value = True
    web3.keccak.return_value = MagicMock()
    web3.keccak.return_value.hex.return_value = "0x1234567890abcdef"
    
    # Mock eth interface
    web3.eth = MagicMock()
    web3.eth.get_transaction_count.return_value = 1
    web3.eth.gas_price = 20000000000  # 20 gwei
    web3.eth.wait_for_transaction_receipt.return_value = MagicMock(status=1, blockNumber=12345)
    web3.eth.get_block.return_value = MagicMock(timestamp=int(time.time()))
    
    return web3

def mock_ipc_instance() -> MagicMock:
    """Creates a mock IPC instance with auth and storage components."""
    ipc = MagicMock()
    
    # Mock auth
    ipc.auth = MagicMock()
    ipc.auth.address = "0x1234567890abcdef"
    ipc.auth.key = "0x" + "a" * 64  # Mock private key
    
    # Mock storage contract
    ipc.storage = MagicMock()
    ipc.storage.create_bucket.return_value = "0xtx123"
    ipc.storage.delete_bucket.return_value = "0xtx456"
    ipc.storage.create_file.return_value = "0xtx789"
    ipc.storage.delete_file.return_value = "0xtxabc"
    
    # Mock web3 instance
    ipc.web3 = mock_web3_instance()
    
    return ipc

# Error scenario factories
def create_grpc_error(code: grpc.StatusCode, details: str = "Test error") -> grpc.RpcError:
    """Creates a mock gRPC error."""
    error = MagicMock(spec=grpc.RpcError)
    error.code.return_value = code
    error.details.return_value = details
    return error

def create_not_found_error() -> grpc.RpcError:
    """Creates a NOT_FOUND gRPC error."""
    return create_grpc_error(grpc.StatusCode.NOT_FOUND, "Resource not found")

def create_invalid_argument_error() -> grpc.RpcError:
    """Creates an INVALID_ARGUMENT gRPC error."""
    return create_grpc_error(grpc.StatusCode.INVALID_ARGUMENT, "Invalid argument")

def create_internal_error() -> grpc.RpcError:
    """Creates an INTERNAL gRPC error."""
    return create_grpc_error(grpc.StatusCode.INTERNAL, "Internal server error")

# Test environment setup
def create_test_environment() -> Dict[str, Any]:
    """Creates a complete test environment with all necessary mocks."""
    return {
        "config": sample_config_data(),
        "bucket_data": sample_bucket_data(),
        "file_data": sample_file_data(),
        "encryption_key": sample_encryption_key(),
        "grpc_channel": mock_grpc_channel(),
        "node_client": mock_node_api_client(),
        "ipc_client": mock_ipc_client(),
        "streaming_client": mock_streaming_client(),
        "web3": mock_web3_instance(),
        "ipc_instance": mock_ipc_instance()
    }

# Validation test data
def get_validation_test_cases() -> List[Dict[str, Any]]:
    """Returns validation test cases for various scenarios."""
    return [
        {
            "name": "empty_bucket_name",
            "bucket_name": "",
            "expected_error": "empty bucket name"
        },
        {
            "name": "short_bucket_name", 
            "bucket_name": "a",
            "expected_error": "invalid bucket name"
        },
        {
            "name": "empty_file_name",
            "file_name": "",
            "expected_error": "empty file name"
        },
        {
            "name": "invalid_encryption_key",
            "encryption_key": b"short",
            "expected_error": "Encryption key length should be 32 bytes long"
        }
    ] 