import unittest
import os
import uuid
import random
import string
import time
import sys
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

sys.modules['ipfshttpclient'] = MagicMock()
sys.modules['multiformats'] = MagicMock()
sys.modules['multiformats.cid'] = MagicMock()
sys.modules['private.memory.memory'] = MagicMock()
sys.modules['private.memory.memory'].Size = MagicMock()
sys.modules['private.memory.memory'].Size.MB = 1024 * 1024
sys.modules['private.ipc.client'] = MagicMock()
sys.modules['private.spclient.spclient'] = MagicMock()
sys.modules['private.encryption'] = MagicMock()
sys.modules['private.pb'] = MagicMock()
sys.modules['private.pb.nodeapi_pb2'] = MagicMock()
sys.modules['private.pb.nodeapi_pb2_grpc'] = MagicMock()
sys.modules['private.pb.ipcnodeapi_pb2_grpc'] = MagicMock()

class SDKError(Exception):
    pass

MIN_BUCKET_NAME_LENGTH = 3
MAX_CONCURRENCY = 5
BLOCK_PART_SIZE = 128 * 1024  # 128 KiB
SECRET_KEY = "N1PCdw3M2B1TfJhoaY2mL736p2vCUc47"

@dataclass
class BucketCreateResult:
    name: str
    created_at: any  

@dataclass
class Bucket:
    name: str
    created_at: any

@dataclass
class Chunk:
    cid: str
    encoded_size: int
    size: int
    index: int

class MockTimestamp:
    def __init__(self, seconds=0, nanos=0):
        self.seconds = seconds
        self.nanos = nanos
    
    def AsTime(self):
        return self

class MockSDK:
    def __init__(self, address, max_concurrency, block_part_size, use_connection_pool, 
                streaming_max_blocks_in_chunk=32, parity_blocks_count=0, encryption_key=None):
        if streaming_max_blocks_in_chunk < 2:
            raise SDKError(f"sdk: streaming max blocks in chunk {streaming_max_blocks_in_chunk} should be >= 2")
        
        if parity_blocks_count > streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"sdk: parity blocks count {parity_blocks_count} should be <= {streaming_max_blocks_in_chunk // 2}")
        
        if encryption_key and len(encryption_key) != 32:
            raise SDKError("sdk: encyption key length should be 32 bytes long")
        
        self.client = MagicMock()
        self.conn = MagicMock()
        self.address = address
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count
        self.encryption_key = encryption_key

    def create_bucket(self, ctx, name):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")
        
        request = MagicMock()
        request.name = name
        
        response = self.client.bucket_create(ctx, request)
        return BucketCreateResult(
            name=response.name,
            created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at
        )
    
    def view_bucket(self, ctx, name):
        if name == "":
            raise SDKError("Invalid bucket name")
        
        request = MagicMock()
        request.name = name
        
        response = self.client.bucket_view(ctx, request)
        return Bucket(
            name=response.name,
            created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at
        )
    
    def list_buckets(self, ctx):
        response = self.client.bucket_list(ctx, MagicMock())
        buckets = []
        for bucket in getattr(response, 'buckets', []):
            buckets.append(Bucket(
                name=bucket.name,
                created_at=bucket.created_at.AsTime() if hasattr(bucket.created_at, 'AsTime') else bucket.created_at
            ))
        return buckets
    
    def delete_bucket(self, ctx, name):
        if name == "":
            raise SDKError("Invalid bucket name")
        
        try:
            request = MagicMock()
            request.name = name
            
            self.client.bucket_delete(ctx, request)
            return True
        except Exception as err:
            raise SDKError(f"Failed to delete bucket: {err}")
    
    def close(self):
        self.conn.close()
        return None

class TestSDK(unittest.TestCase):
    
    def setUp(self):
        self.mock_channel = MagicMock()
        self.mock_client = MagicMock()
        
        self.ctx = MagicMock()
        
        self.mock_timestamp = MockTimestamp(seconds=int(time.time()))
        
        self.sdk = MockSDK(
            address="localhost:50051",
            max_concurrency=MAX_CONCURRENCY,
            block_part_size=BLOCK_PART_SIZE,
            use_connection_pool=True
        )
        self.sdk.client = self.mock_client
        self.sdk.conn = self.mock_channel
    
    def random_bucket_name(self, length=10):
        prefix = 'test-'
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
        return prefix + suffix
    
    def test_create_sdk_client_invalid_max_blocks_in_chunk(self):
        with self.assertRaises(SDKError) as context:
            MockSDK(
                address="localhost:50051",
                max_concurrency=MAX_CONCURRENCY,
                block_part_size=BLOCK_PART_SIZE,
                use_connection_pool=True,
                streaming_max_blocks_in_chunk=1
            )
        self.assertEqual(str(context.exception), "sdk: streaming max blocks in chunk 1 should be >= 2")
    
    def test_create_sdk_client_invalid_erasure_coding(self):
        with self.assertRaises(SDKError) as context:
            MockSDK(
                address="localhost:50051",
                max_concurrency=MAX_CONCURRENCY,
                block_part_size=BLOCK_PART_SIZE,
                use_connection_pool=True,
                parity_blocks_count=17
            )
        self.assertEqual(str(context.exception), "sdk: parity blocks count 17 should be <= 16")
        
        with self.assertRaises(SDKError) as context:
            MockSDK(
                address="localhost:50051",
                max_concurrency=MAX_CONCURRENCY,
                block_part_size=BLOCK_PART_SIZE,
                use_connection_pool=True,
                parity_blocks_count=40,
                streaming_max_blocks_in_chunk=64
            )
        self.assertEqual(str(context.exception), "sdk: parity blocks count 40 should be <= 32")
    
    def test_create_sdk_client_invalid_encryption_key_size(self):
        with self.assertRaises(SDKError) as context:
            MockSDK(
                address="localhost:50051",
                max_concurrency=MAX_CONCURRENCY,
                block_part_size=BLOCK_PART_SIZE,
                use_connection_pool=True,
                encryption_key=b"short"
            )
        self.assertEqual(str(context.exception), "sdk: encyption key length should be 32 bytes long")
    
    def test_create_bucket_valid(self):
        bucket_name = self.random_bucket_name()
        mock_response = MagicMock()
        mock_response.name = bucket_name
        mock_response.created_at = self.mock_timestamp
        
        self.mock_client.bucket_create.return_value = mock_response
        
        result = self.sdk.create_bucket(self.ctx, bucket_name)
        
        self.assertIsInstance(result, BucketCreateResult)
        self.assertEqual(result.name, bucket_name)
        self.assertEqual(result.created_at, self.mock_timestamp)
        
        self.mock_client.bucket_create.assert_called_once()
        call_args = self.mock_client.bucket_create.call_args[0]
        self.assertEqual(call_args[0], self.ctx)
        self.assertEqual(call_args[1].name, bucket_name)
    
    def test_view_bucket(self):
        bucket_name = self.random_bucket_name()
        
        create_response = MagicMock()
        create_response.name = bucket_name
        create_response.created_at = self.mock_timestamp
        self.mock_client.bucket_create.return_value = create_response
        
        bucket_result = self.sdk.create_bucket(self.ctx, bucket_name)
        
        view_response = MagicMock()
        view_response.name = bucket_name
        view_response.created_at = self.mock_timestamp
        self.mock_client.bucket_view.return_value = view_response
        
        actual = self.sdk.view_bucket(self.ctx, bucket_name)
        
        expected = Bucket(name=bucket_name, created_at=self.mock_timestamp)
        self.assertEqual(actual.name, expected.name)
        self.assertEqual(actual.created_at, expected.created_at)
    
    def test_list_buckets(self):
        expected_buckets = []
        mock_bucket_responses = []
        
        for i in range(3):
            bucket_name = self.random_bucket_name()
            mock_bucket = MagicMock()
            mock_bucket.name = bucket_name
            mock_bucket.created_at = self.mock_timestamp
            mock_bucket_responses.append(mock_bucket)
            
            expected_buckets.append(Bucket(
                name=bucket_name,
                created_at=self.mock_timestamp
            ))
        
        list_response = MagicMock()
        list_response.buckets = mock_bucket_responses
        self.mock_client.bucket_list.return_value = list_response
        
        buckets = self.sdk.list_buckets(self.ctx)
        
        self.assertEqual(len(buckets), 3)
        for i, bucket in enumerate(buckets):
            self.assertEqual(bucket.name, expected_buckets[i].name)
    
    def test_delete_bucket(self):
        bucket_name = self.random_bucket_name()
        
        create_response = MagicMock()
        create_response.name = bucket_name
        create_response.created_at = self.mock_timestamp
        self.mock_client.bucket_create.return_value = create_response
        
        view_response = MagicMock()
        view_response.name = bucket_name
        view_response.created_at = self.mock_timestamp
        self.mock_client.bucket_view.return_value = view_response
        
        self.sdk.create_bucket(self.ctx, bucket_name)
        
        bucket = self.sdk.view_bucket(self.ctx, bucket_name)
        self.assertEqual(bucket.name, bucket_name)
        
        result = self.sdk.delete_bucket(self.ctx, bucket_name)
        self.assertTrue(result)
        
        self.mock_client.bucket_view.side_effect = Exception("Not found")
        
        with self.assertRaises(Exception):
            self.sdk.view_bucket(self.ctx, bucket_name)
    
    def test_create_bucket_invalid_name(self):
        invalid_name = "a" * (MIN_BUCKET_NAME_LENGTH - 1)
        
        with self.assertRaises(SDKError) as context:
            self.sdk.create_bucket(self.ctx, invalid_name)
        
        self.assertEqual(str(context.exception), "Invalid bucket name")
        
        self.mock_client.bucket_create.assert_not_called()

    def test_view_bucket_empty_name(self):
        with self.assertRaises(SDKError) as context:
            self.sdk.view_bucket(self.ctx, "")
        
        self.assertEqual(str(context.exception), "Invalid bucket name")
        
        self.mock_client.bucket_view.assert_not_called()
    
    def test_delete_bucket_error(self):
        
        self.mock_client.bucket_delete.side_effect = Exception("Test error")
        
        with self.assertRaises(SDKError) as context:
            self.sdk.delete_bucket(self.ctx, "test-bucket")
        
        self.assertIn("Failed to delete bucket", str(context.exception))
        self.assertIn("Test error", str(context.exception))
    
    def test_close(self):
        result = self.sdk.close()
        
        self.mock_channel.close.assert_called_once()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
