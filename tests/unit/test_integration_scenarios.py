"""
Integration scenario tests for Akave SDK with comprehensive edge cases.
"""

import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os
import grpc
import io
import time
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.sdk_ipc import IPC
from sdk.sdk_streaming import StreamingAPI, FileUpload, FileDownload, Chunk
from sdk.common import SDKError


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios and complex workflows."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_ipc_instance = MagicMock()
        self.mock_ipc_instance.auth.address = "0x1234567890abcdef"
        self.mock_ipc_instance.auth.key = "0x" + "a" * 64
        self.mock_ipc_instance.storage = MagicMock()
        self.mock_ipc_instance.web3 = MagicMock()
        
        # Setup successful default responses
        self._setup_default_mocks()
        
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
    
    def _setup_default_mocks(self):
        """Setup default mock responses for successful operations."""
        # Mock successful transaction
        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 12345
        
        mock_block = MagicMock()
        mock_block.timestamp = int(time.time())
        
        self.mock_ipc_instance.storage.create_bucket.return_value = "0x123abc"
        self.mock_ipc_instance.web3.eth.wait_for_transaction_receipt.return_value = mock_receipt
        self.mock_ipc_instance.web3.eth.get_block.return_value = mock_block
        
        # Mock bucket view response - make it dynamic based on input
        def mock_bucket_view(request):
            mock_bucket_response = MagicMock()
            mock_bucket_response.id = "bucket_id_123"
            mock_bucket_response.name = request.name  # Use the requested bucket name
            mock_bucket_response.created_at.seconds = int(time.time())
            return mock_bucket_response
        
        self.mock_client.BucketView.side_effect = mock_bucket_view
        
        # Mock file responses
        mock_file_response = MagicMock()
        mock_file_response.file_name = "test-file.txt"
        mock_file_response.name = "test-file.txt"
        mock_file_response.root_cid = "QmTest123"
        mock_file_response.encoded_size = 1024
        mock_file_response.created_at.seconds = int(time.time())
        self.mock_client.FileView.return_value = mock_file_response
        
        # Mock upload responses - make it dynamic based on input
        def mock_file_upload_create(request):
            mock_upload_response = MagicMock()
            mock_upload_response.bucket_name = request.bucket_name  # Use the requested bucket name
            mock_upload_response.file_name = request.file_name  # Use the requested file name
            mock_upload_response.stream_id = "stream123"
            mock_upload_response.created_at = MagicMock()
            return mock_upload_response
        
        self.mock_client.FileUploadCreate.side_effect = mock_file_upload_create
    
    def test_complete_bucket_lifecycle(self):
        """Test complete bucket lifecycle: create, view, list, delete."""
        bucket_name = "lifecycle-bucket"
        
        # 1. Create bucket
        result = self.ipc_api.create_bucket(None, bucket_name)
        self.assertIsNotNone(result)
        self.assertEqual(result.name, bucket_name)
        
        # 2. View bucket
        bucket = self.ipc_api.view_bucket(None, bucket_name)
        self.assertIsNotNone(bucket)
        self.assertEqual(bucket.name, bucket_name)
        
        # 3. List buckets
        buckets = self.ipc_api.list_buckets(None)
        self.assertIsInstance(buckets, list)
        
        # 4. Delete bucket
        self.ipc_api.delete_bucket(None, bucket_name)
        # Should complete without error
    
    def test_file_upload_workflow(self):
        """Test complete file upload workflow."""
        bucket_name = "upload-bucket"
        file_name = "upload-test.txt"
        
        # 1. Create file upload
        upload = self.streaming_api.create_file_upload(None, bucket_name, file_name)
        self.assertIsInstance(upload, FileUpload)
        self.assertEqual(upload.BucketName, bucket_name)
        self.assertEqual(upload.Name, file_name)
        
        # 2. Verify file info can be retrieved
        file_info = self.ipc_api.file_info(None, bucket_name, file_name)
        self.assertIsNotNone(file_info)
        self.assertEqual(file_info.name, file_name)
    
    def test_file_download_workflow(self):
        """Test complete file download workflow."""
        bucket_name = "download-bucket"
        file_name = "download-test.txt"
        root_cid = "QmDownloadTest123"
        
        # Mock download response
        mock_chunk = MagicMock()
        mock_chunk.cid = "QmChunk123"
        mock_chunk.encoded_size = 1024
        mock_chunk.size = 512
        
        mock_download_response = MagicMock()
        mock_download_response.stream_id = "download_stream123"
        mock_download_response.bucket_name = bucket_name
        mock_download_response.chunks = [mock_chunk]
        self.mock_client.FileDownloadCreate.return_value = mock_download_response
        
        # Create file download
        download = self.streaming_api.create_file_download(None, bucket_name, file_name, root_cid)
        self.assertIsInstance(download, FileDownload)
        self.assertEqual(download.BucketName, bucket_name)
        self.assertEqual(download.Name, file_name)
        self.assertEqual(len(download.Chunks), 1)
    
    def test_range_download_workflow(self):
        """Test range file download workflow."""
        bucket_name = "range-bucket"
        file_name = "range-test.txt"
        start_byte = 100
        end_byte = 200
        
        # Mock range download response
        mock_chunk = MagicMock()
        mock_chunk.cid = "QmRangeChunk123"
        mock_chunk.encoded_size = 512
        mock_chunk.size = 100  # end - start
        
        mock_range_response = MagicMock()
        mock_range_response.stream_id = "range_stream123"
        mock_range_response.bucket_name = bucket_name
        mock_range_response.chunks = [mock_chunk]
        self.mock_client.FileDownloadRangeCreate.return_value = mock_range_response
        
        # Create range download
        download = self.streaming_api.create_range_file_download(
            None, bucket_name, file_name, start_byte, end_byte
        )
        self.assertIsInstance(download, FileDownload)
        self.assertEqual(download.BucketName, bucket_name)
        self.assertEqual(download.Name, file_name)
    
    def test_file_listing_and_management(self):
        """Test file listing and management operations."""
        bucket_name = "file-mgmt-bucket"
        
        # Mock file list response
        mock_file1 = MagicMock()
        mock_file1.name = "file1.txt"
        mock_file1.root_cid = "QmFile1"
        mock_file1.encoded_size = 1024
        mock_file1.created_at.seconds = int(time.time())
        
        mock_file2 = MagicMock()
        mock_file2.name = "file2.txt"
        mock_file2.root_cid = "QmFile2"
        mock_file2.encoded_size = 2048
        mock_file2.created_at.seconds = int(time.time())
        
        mock_list_response = MagicMock()
        mock_list_response.list = [mock_file1, mock_file2]
        self.mock_client.FileList.return_value = mock_list_response
        
        # List files
        files = self.ipc_api.list_files(None, bucket_name)
        self.assertIsInstance(files, list)
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].name, "file1.txt")
        self.assertEqual(files[1].name, "file2.txt")
        
        # Delete a file
        self.ipc_api.file_delete(None, bucket_name, "file1.txt")
        # Should complete without error
    
    def test_error_recovery_scenarios(self):
        """Test various error recovery scenarios."""
        bucket_name = "error-bucket"
        
        # Test bucket creation failure followed by retry
        self.mock_ipc_instance.storage.create_bucket.side_effect = [
            Exception("Network error"),  # First call fails
            "0x123abc"  # Second call succeeds
        ]
        
        # First attempt should fail
        with self.assertRaises(SDKError):
            self.ipc_api.create_bucket(None, bucket_name)
        
        # Reset the mock receipt for the second attempt
        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 12345
        self.mock_ipc_instance.web3.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        # Second attempt should succeed
        result = self.ipc_api.create_bucket(None, bucket_name)
        self.assertIsNotNone(result)
    
    def test_concurrent_operations_simulation(self):
        """Test simulation of concurrent operations."""
        # This test simulates multiple operations that might happen concurrently
        bucket_names = ["concurrent-bucket-1", "concurrent-bucket-2", "concurrent-bucket-3"]
        
        for bucket_name in bucket_names:
            # Each operation should work independently
            result = self.ipc_api.create_bucket(None, bucket_name)
            self.assertIsNotNone(result)
            self.assertEqual(result.name, bucket_name)
            
            # View the created bucket
            bucket = self.ipc_api.view_bucket(None, bucket_name)
            self.assertIsNotNone(bucket)
            self.assertEqual(bucket.name, bucket_name)
    
    def test_large_file_simulation(self):
        """Test simulation of large file operations."""
        bucket_name = "large-file-bucket"
        file_name = "large-file.bin"
        
        # Simulate large file metadata
        mock_large_file = MagicMock()
        mock_large_file.name = file_name
        mock_large_file.root_cid = "QmLargeFile123"
        mock_large_file.encoded_size = 100 * 1024 * 1024  # 100MB
        mock_large_file.created_at.seconds = int(time.time())
        self.mock_client.FileView.return_value = mock_large_file
        
        # Get file info
        file_info = self.ipc_api.file_info(None, bucket_name, file_name)
        self.assertIsNotNone(file_info)
        self.assertEqual(file_info.encoded_size, 100 * 1024 * 1024)
    
    def test_empty_responses_handling(self):
        """Test handling of empty or null responses."""
        bucket_name = "empty-bucket"
        file_name = "nonexistent-file.txt"
        
        # Test file not found
        not_found_error = grpc.RpcError()
        not_found_error.code = lambda: grpc.StatusCode.NOT_FOUND
        self.mock_client.FileView.side_effect = not_found_error
        
        result = self.ipc_api.file_info(None, bucket_name, file_name)
        self.assertIsNone(result)
        
        # Test bucket not found
        self.mock_client.BucketView.side_effect = not_found_error
        result = self.ipc_api.view_bucket(None, "nonexistent-bucket")
        self.assertIsNone(result)
    
    def test_api_parameter_validation(self):
        """Test API parameter validation."""
        # Test various invalid parameter combinations
        invalid_params = [
            ("", "valid-file.txt"),  # Empty bucket name
            ("valid-bucket", ""),    # Empty file name
            (None, "valid-file.txt"), # None bucket name
            ("valid-bucket", None),   # None file name
        ]
        
        for bucket_name, file_name in invalid_params:
            with self.subTest(bucket=bucket_name, file=file_name):
                if bucket_name is None or bucket_name == "":
                    with self.assertRaises((SDKError, TypeError)):
                        self.ipc_api.file_info(None, bucket_name, file_name or "dummy")
                
                if file_name is None or file_name == "":
                    with self.assertRaises((SDKError, TypeError)):
                        self.ipc_api.file_info(None, bucket_name or "dummy", file_name)


if __name__ == '__main__':
    unittest.main() 