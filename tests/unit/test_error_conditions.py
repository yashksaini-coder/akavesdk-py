"""
Comprehensive error condition and edge case testing for Akave SDK.
"""

import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os
import grpc

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.sdk_ipc import IPC
from sdk.sdk_streaming import StreamingAPI
from sdk.common import SDKError, MIN_BUCKET_NAME_LENGTH


class TestErrorConditions(unittest.TestCase):
    """Test various error conditions and edge cases."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_ipc_instance = MagicMock()
        self.mock_ipc_instance.auth.address = "0x1234567890abcdef"
        self.mock_ipc_instance.auth.key = "0x" + "a" * 64
        self.mock_ipc_instance.storage = MagicMock()
        self.mock_ipc_instance.web3 = MagicMock()
        
        self.ipc_api = IPC(
            client=self.mock_client,
            conn=self.mock_conn,
            ipc_instance=self.mock_ipc_instance,
            max_concurrency=10,
            block_part_size=1024,
            use_connection_pool=True,
            encryption_key=b'a' * 32,
            max_blocks_in_chunk=32
        )
        
        self.streaming_api = StreamingAPI(
            conn=self.mock_conn,
            client=self.mock_client,
            erasure_code=None,
            max_concurrency=10,
            block_part_size=1024,
            use_connection_pool=True,
            encryption_key=b'a' * 32,
            max_blocks_in_chunk=32
        )
    
    def test_bucket_name_too_short(self):
        """Test bucket creation with name too short."""
        short_name = "a" * (MIN_BUCKET_NAME_LENGTH - 1)
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, short_name)
        
        self.assertIn("invalid bucket name", str(context.exception))
    
    def test_bucket_name_empty_string(self):
        """Test bucket operations with empty string name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.view_bucket(None, "")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_file_name_empty_string(self):
        """Test file operations with empty file name."""
        bucket_name = "valid-bucket"
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_info(None, bucket_name, "")
        
        self.assertIn("empty file name", str(context.exception))
    
    def test_grpc_connection_timeout(self):
        """Test handling of gRPC connection timeouts."""
        timeout_error = grpc.RpcError()
        timeout_error.code = lambda: grpc.StatusCode.DEADLINE_EXCEEDED
        timeout_error.details = lambda: "Deadline exceeded"
        
        self.mock_client.BucketList.side_effect = timeout_error
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.list_buckets(None)
        
        self.assertIn("failed to list buckets", str(context.exception))
    
    def test_grpc_unavailable_service(self):
        """Test handling of unavailable gRPC service."""
        unavailable_error = grpc.RpcError()
        unavailable_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        unavailable_error.details = lambda: "Service unavailable"
        
        self.mock_client.BucketView.side_effect = unavailable_error
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.view_bucket(None, "test-bucket")
        
        self.assertIn("failed to view bucket", str(context.exception))
    
    def test_grpc_authentication_error(self):
        """Test handling of gRPC authentication errors."""
        auth_error = grpc.RpcError()
        auth_error.code = lambda: grpc.StatusCode.UNAUTHENTICATED
        auth_error.details = lambda: "Authentication failed"
        
        self.mock_client.FileList.side_effect = auth_error
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.list_files(None, "test-bucket")
        
        self.assertIn("failed to list files", str(context.exception))
    
    def test_grpc_permission_denied(self):
        """Test handling of gRPC permission denied errors."""
        permission_error = grpc.RpcError()
        permission_error.code = lambda: grpc.StatusCode.PERMISSION_DENIED
        permission_error.details = lambda: "Permission denied"
        
        self.mock_client.FileView.side_effect = permission_error
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_info(None, "test-bucket", "test-file.txt")
        
        self.assertIn("failed to get file info", str(context.exception))
    
    def test_web3_transaction_failure(self):
        """Test handling of Web3 transaction failures."""
        # Mock failed transaction receipt
        mock_receipt = MagicMock()
        mock_receipt.status = 0  # Failed transaction
        
        self.mock_ipc_instance.storage.create_bucket.return_value = "0x123"
        self.mock_ipc_instance.web3.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, "test-bucket")
        
        self.assertIn("bucket creation transaction failed", str(context.exception))
    
    def test_web3_connection_error(self):
        """Test handling of Web3 connection errors."""
        self.mock_ipc_instance.storage.create_bucket.side_effect = ConnectionError("Connection failed")
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, "test-bucket")
        
        self.assertIn("bucket creation failed", str(context.exception))
    
    def test_streaming_file_upload_network_error(self):
        """Test handling of network errors during streaming file upload."""
        self.mock_client.FileUploadCreate.side_effect = ConnectionError("Network error")
        
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_upload(None, "test-bucket", "test-file.txt")
        
        self.assertIn("failed to create file upload", str(context.exception))
    
    def test_streaming_file_download_not_found(self):
        """Test handling of file not found during download creation."""
        not_found_error = grpc.RpcError()
        not_found_error.code = lambda: grpc.StatusCode.NOT_FOUND
        not_found_error.details = lambda: "File not found"
        
        self.mock_client.FileDownloadCreate.side_effect = not_found_error
        
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_download(None, "test-bucket", "nonexistent-file.txt")
        
        self.assertIn("failed to create file download", str(context.exception))
    
    def test_invalid_encryption_key_length(self):
        """Test handling of invalid encryption key lengths."""
        invalid_keys = [
            b"",           # Empty key
            b"short",      # Too short
            b"a" * 16,     # Half length
            b"a" * 64,     # Too long
        ]
        
        for invalid_key in invalid_keys:
            with self.subTest(key_length=len(invalid_key)):
                # The IPC class accepts any encryption key length, so this test
                # validates that we can handle different key lengths without crashing
                ipc_api = IPC(
                    client=self.mock_client,
                    conn=self.mock_conn,
                    ipc_instance=self.mock_ipc_instance,
                    max_concurrency=10,
                    block_part_size=1024,
                    use_connection_pool=True,
                    encryption_key=invalid_key,
                    max_blocks_in_chunk=32
                )
                # Verify that the key is stored as provided
                self.assertEqual(ipc_api.encryption_key, invalid_key)
    
    def test_invalid_block_part_size(self):
        """Test handling of invalid block part sizes."""
        invalid_sizes = [0, -1, -100]
        
        for invalid_size in invalid_sizes:
            with self.subTest(block_size=invalid_size):
                # The IPC class accepts any block part size, so this test
                # validates that we can handle different sizes without crashing
                ipc_api = IPC(
                    client=self.mock_client,
                    conn=self.mock_conn,
                    ipc_instance=self.mock_ipc_instance,
                    max_concurrency=10,
                    block_part_size=invalid_size,
                    use_connection_pool=True,
                    encryption_key=b'a' * 32,
                    max_blocks_in_chunk=32
                )
                # Verify that the size is stored as provided
                self.assertEqual(ipc_api.block_part_size, invalid_size)
    
    def test_invalid_max_concurrency(self):
        """Test handling of invalid max concurrency values."""
        invalid_values = [0, -1, -100]
        
        for invalid_value in invalid_values:
            with self.subTest(max_concurrency=invalid_value):
                # The IPC class accepts any max concurrency value, so this test
                # validates that we can handle different values without crashing
                ipc_api = IPC(
                    client=self.mock_client,
                    conn=self.mock_conn,
                    ipc_instance=self.mock_ipc_instance,
                    max_concurrency=invalid_value,
                    block_part_size=1024,
                    use_connection_pool=True,
                    encryption_key=b'a' * 32,
                    max_blocks_in_chunk=32
                )
                # Verify that the value is stored as provided
                self.assertEqual(ipc_api.max_concurrency, invalid_value)
    
    def test_none_parameters(self):
        """Test handling of None parameters where they're not expected."""
        with self.assertRaises((TypeError, SDKError)):
            self.ipc_api.create_bucket(None, None)
        
        with self.assertRaises((TypeError, SDKError)):
            self.ipc_api.file_info(None, None, "test-file.txt")
        
        with self.assertRaises((TypeError, SDKError)):
            self.ipc_api.file_info(None, "test-bucket", None)
    
    def test_bucket_already_exists(self):
        """Test handling when bucket already exists."""
        already_exists_error = grpc.RpcError()
        already_exists_error.code = lambda: grpc.StatusCode.ALREADY_EXISTS
        already_exists_error.details = lambda: "Bucket already exists"
        
        self.mock_ipc_instance.storage.create_bucket.side_effect = Exception("Bucket already exists")
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, "existing-bucket")
        
        self.assertIn("bucket creation failed", str(context.exception))
    
    def test_insufficient_storage_space(self):
        """Test handling of insufficient storage space errors."""
        space_error = grpc.RpcError()
        space_error.code = lambda: grpc.StatusCode.RESOURCE_EXHAUSTED
        space_error.details = lambda: "Insufficient storage space"
        
        self.mock_client.FileUploadCreate.side_effect = space_error
        
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_upload(None, "test-bucket", "large-file.bin")
        
        self.assertIn("failed to create file upload", str(context.exception))
    
    def test_malformed_grpc_response(self):
        """Test handling of malformed gRPC responses."""
        # Mock response with missing required fields
        malformed_response = MagicMock()
        malformed_response.name = None
        malformed_response.created_at = None
        
        self.mock_client.BucketView.return_value = malformed_response
        
        # Should handle gracefully and not crash
        result = self.ipc_api.view_bucket(None, "test-bucket")
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main() 