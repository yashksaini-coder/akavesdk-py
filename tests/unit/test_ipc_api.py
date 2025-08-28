import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.sdk_ipc import IPC, IPCFileMeta, IPCFileListItem
from sdk.common import SDKError
import grpc


class TestIPCAPI(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_ipc_instance = MagicMock()
        self.mock_ipc_instance.auth.address = "0x1234567890abcdef"
        self.mock_ipc_instance.storage = MagicMock()
        
        self.test_max_concurrency = 10
        self.test_block_part_size = 1024
        self.test_use_connection_pool = True
        self.test_encryption_key = b'a' * 32
        self.test_max_blocks_in_chunk = 32
        
        self.ipc_api = IPC(
            client=self.mock_client,
            conn=self.mock_conn,
            ipc_instance=self.mock_ipc_instance,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool,
            encryption_key=self.test_encryption_key,
            max_blocks_in_chunk=self.test_max_blocks_in_chunk
        )
    
    def test_ipc_api_initialization(self):
        """Test IPC API initialization."""
        self.assertEqual(self.ipc_api.max_concurrency, self.test_max_concurrency)
        self.assertEqual(self.ipc_api.block_part_size, self.test_block_part_size)
        self.assertEqual(self.ipc_api.use_connection_pool, self.test_use_connection_pool)
        self.assertEqual(self.ipc_api.encryption_key, self.test_encryption_key)
        self.assertEqual(self.ipc_api.max_blocks_in_chunk, self.test_max_blocks_in_chunk)
    
    def test_create_bucket_success(self):
        """Test successful bucket creation via IPC."""
        # Arrange
        bucket_name = "test-bucket"
        tx_hash = "0x123abc"
        
        # Mock transaction receipt
        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 12345
        
        # Mock block
        mock_block = MagicMock()
        mock_block.timestamp = 1234567890
        
        # Set up the chain of mock calls
        self.mock_ipc_instance.storage.create_bucket.return_value = tx_hash
        self.mock_ipc_instance.web3.eth.wait_for_transaction_receipt.return_value = mock_receipt
        self.mock_ipc_instance.web3.eth.get_block.return_value = mock_block
        
        # Act
        result = self.ipc_api.create_bucket(None, bucket_name)
        
        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.name, bucket_name)
        self.assertEqual(result.created_at, 1234567890)
        self.mock_ipc_instance.storage.create_bucket.assert_called_once_with(
            bucket_name=bucket_name,
            from_address=self.mock_ipc_instance.auth.address,
            private_key=self.mock_ipc_instance.auth.key,
            gas_limit=500000
        )
    
    def test_create_bucket_empty_name(self):
        """Test bucket creation with empty name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, "")
        
        self.assertIn("invalid bucket name", str(context.exception))
    
    def test_create_bucket_blockchain_error(self):
        """Test bucket creation with blockchain error."""
        # Arrange
        bucket_name = "test-bucket"
        self.mock_ipc_instance.storage.create_bucket.side_effect = Exception("Blockchain error")
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_bucket(None, bucket_name)
        
        self.assertIn("bucket creation failed", str(context.exception))
    
    def test_delete_bucket_success(self):
        """Test successful bucket deletion via IPC."""
        # Arrange
        bucket_name = "test-bucket"
        
        # Mock the BucketView response first
        mock_bucket_view_response = MagicMock()
        mock_bucket_view_response.id = "bucket_id_123"
        self.mock_client.BucketView.return_value = mock_bucket_view_response
        
        self.mock_ipc_instance.storage.delete_bucket.return_value = None
        
        # Act
        result = self.ipc_api.delete_bucket(None, bucket_name)
        
        # Assert
        self.assertIsNone(result)
        self.mock_ipc_instance.storage.delete_bucket.assert_called_once_with(
            bucket_name=bucket_name,
            from_address=self.mock_ipc_instance.auth.address,
            private_key=self.mock_ipc_instance.auth.key,
            bucket_id_hex="bucket_id_123"
        )
    
    def test_delete_bucket_empty_name(self):
        """Test bucket deletion with empty name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.delete_bucket(None, "")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_file_info_success(self):
        """Test successful file info retrieval."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        mock_response = MagicMock()
        mock_response.root_cid = "QmTest123"
        mock_response.file_name = file_name
        mock_response.bucket_name = bucket_name
        mock_response.encoded_size = 1024
        mock_response.created_at.seconds = 1234567890
        
        self.mock_client.FileView.return_value = mock_response
        
        # Act
        result = self.ipc_api.file_info(None, bucket_name, file_name)
        
        # Assert
        self.assertIsInstance(result, IPCFileMeta)
        self.assertEqual(result.root_cid, "QmTest123")
        self.assertEqual(result.name, file_name)
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.encoded_size, 1024)
        self.assertEqual(result.created_at, 1234567890)
        self.mock_client.FileView.assert_called_once()
    
    def test_file_info_empty_bucket_name(self):
        """Test file info with empty bucket name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_info(None, "", "test-file.txt")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_file_info_empty_file_name(self):
        """Test file info with empty file name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_info(None, "test-bucket", "")
        
        self.assertIn("empty file name", str(context.exception))
    
    def test_file_info_not_found(self):
        """Test file info when file is not found."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "nonexistent-file.txt"
        
        grpc_error = grpc.RpcError()
        grpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        self.mock_client.FileView.side_effect = grpc_error
        
        # Act
        result = self.ipc_api.file_info(None, bucket_name, file_name)
        
        # Assert
        self.assertIsNone(result)
    
    def test_list_files_success(self):
        """Test successful file listing."""
        # Arrange
        bucket_name = "test-bucket"
        
        mock_file_item = MagicMock()
        mock_file_item.name = "test-file.txt"  
        mock_file_item.root_cid = "QmTest123"
        mock_file_item.encoded_size = 1024
        mock_file_item.created_at.seconds = 1234567890
        
        mock_response = MagicMock()
        mock_response.list = [mock_file_item]
        
        self.mock_client.FileList.return_value = mock_response
        
        # Act
        result = self.ipc_api.list_files(None, bucket_name)
        
        # Assert
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], IPCFileListItem)
        self.assertEqual(result[0].name, "test-file.txt")
        self.assertEqual(result[0].root_cid, "QmTest123")
        self.mock_client.FileList.assert_called_once()
    
    def test_list_files_empty_bucket_name(self):
        """Test file listing with empty bucket name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.list_files(None, "")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_file_delete_success(self):
        """Test successful file deletion."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Mock the bucket and file retrieval chain
        mock_bucket = [b"bucket_id_123"]  # bytes32 bucket ID
        mock_file = [b"file_id_456"]      # bytes32 file ID  
        mock_file_index = 0
        
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.get_file_by_name.return_value = mock_file
        self.mock_ipc_instance.storage.get_file_index_by_id.return_value = mock_file_index
        self.mock_ipc_instance.storage.delete_file.return_value = "0xabcdef"
        
        # Act
        result = self.ipc_api.file_delete(None, bucket_name, file_name)
        
        # Assert
        self.assertIsNone(result)
        self.mock_ipc_instance.storage.delete_file.assert_called_once_with(
            self.mock_ipc_instance.auth,
            mock_file[0],
            mock_bucket[0], 
            file_name,
            mock_file_index
        )
    
    def test_file_delete_bucket_not_found(self):
        """Test file deletion when bucket does not exist."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Mock bucket retrieval to return None (bucket not found)
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = None
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, bucket_name, file_name)
        
        self.assertIn("failed to retrieve bucket - bucket does not exist", str(context.exception))
    
    def test_file_delete_file_not_found(self):
        """Test file deletion when file does not exist."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test_file.txt"
        
        # Mock bucket retrieval to succeed but file retrieval to fail
        mock_bucket = [b"bucket_id_123"]
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.get_file_by_name.return_value = None
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, bucket_name, file_name)
        
        self.assertIn("failed to retrieve file - file does not exist", str(context.exception))
    
    def test_file_delete_index_retrieval_fails(self):
        """Test file deletion when file index retrieval fails."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test_file.txt"
        
        # Mock bucket and file retrieval to succeed but index retrieval to fail
        mock_bucket = [b"bucket_id_123"]
        mock_file = [b"file_id_456"]
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.get_file_by_name.return_value = mock_file
        self.mock_ipc_instance.storage.get_file_index_by_id.side_effect = Exception("Contract call failed")
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, bucket_name, file_name)
        
        self.assertIn("failed to retrieve file index", str(context.exception))
    
    def test_file_delete_invalid_index(self):
        """Test file deletion when invalid index is returned."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test_file.txt"
        
        # Mock all retrievals to succeed but return invalid index
        mock_bucket = [b"bucket_id_123"]
        mock_file = [b"file_id_456"]
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.get_file_by_name.return_value = mock_file
        self.mock_ipc_instance.storage.get_file_index_by_id.return_value = -1  # Invalid index
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, bucket_name, file_name)
        
        self.assertIn("invalid file index returned from contract", str(context.exception))
    
    def test_file_delete_delete_transaction_fails(self):
        """Test file deletion when the delete transaction fails."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test_file.txt"
        
        # Mock all retrievals to succeed but delete transaction to fail
        mock_bucket = [b"bucket_id_123"]
        mock_file = [b"file_id_456"]
        mock_file_index = 0
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.get_file_by_name.return_value = mock_file
        self.mock_ipc_instance.storage.get_file_index_by_id.return_value = mock_file_index
        self.mock_ipc_instance.storage.delete_file.side_effect = Exception("Transaction failed")
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, bucket_name, file_name)
        
        self.assertIn("failed to delete file", str(context.exception))
    
    def test_file_delete_empty_names(self):
        """Test file deletion with empty bucket or file name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, "", "test-file.txt")
        
        self.assertIn("empty bucket or file name", str(context.exception))
        
        with self.assertRaises(SDKError) as context:
            self.ipc_api.file_delete(None, "test-bucket", "")
        
        self.assertIn("empty bucket or file name", str(context.exception))
    
    def test_create_file_upload_success(self):
        """Test successful file upload creation."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test_file.txt"
        
        # Mock the bucket retrieval and create_file call
        mock_bucket = [b"bucket_id_123"]  # bytes32 bucket ID
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = mock_bucket
        self.mock_ipc_instance.storage.create_file.return_value = "0xabcdef123456"
        
        # Act
        result = self.ipc_api.create_file_upload(None, bucket_name, file_name)
        
        # Assert
        self.assertIsNone(result)
        self.mock_ipc_instance.storage.create_file.assert_called_once_with(
            self.mock_ipc_instance.auth.address,
            self.mock_ipc_instance.auth.key,
            mock_bucket[0],
            file_name
        )
    
    def test_create_file_upload_empty_bucket_name(self):
        """Test file upload creation with empty bucket name."""
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_file_upload(None, "", "test-file.txt")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_create_file_upload_no_web3(self):
        """Test file upload creation when bucket is not found."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Mock bucket retrieval to return None (bucket not found)
        self.mock_ipc_instance.storage.get_bucket_by_name.return_value = None
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.ipc_api.create_file_upload(None, bucket_name, file_name)
        
        self.assertIn("failed to retrieve bucket", str(context.exception))

    def test_placeholder(self):
        """Placeholder test for IPC API."""
        self.assertTrue(True)

    def test_create_file_download_success(self):
        """Test successful file download creation."""
        self.ipc_api.client = MagicMock()
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_response = MagicMock()
        mock_response.bucket_name = bucket_name
        mock_response.chunks = [MagicMock(cid='cid1', encoded_size=100, size=50)]
        self.ipc_api.client.FileDownloadCreate.return_value = mock_response
        result = self.ipc_api.create_file_download(None, bucket_name, file_name)
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.chunks[0].cid, 'cid1')

    def test_create_file_download_empty_params(self):
        """Test file download creation with empty parameters."""
        self.ipc_api.client = MagicMock()
        with self.assertRaises(SDKError):
            self.ipc_api.create_file_download(None, '', 'file.txt')
        with self.assertRaises(SDKError):
            self.ipc_api.create_file_download(None, 'bucket', '')

    def test_create_file_download_error(self):
        """Test file download creation with error."""
        self.ipc_api.client = MagicMock()
        self.ipc_api.client.FileDownloadCreate.side_effect = Exception('fail')
        with self.assertRaises(SDKError):
            self.ipc_api.create_file_download(None, 'bucket', 'file.txt')

    def test_create_range_file_download_success(self):
        """Test successful range file download creation."""
        self.ipc_api.client = MagicMock()
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_response = MagicMock()
        mock_response.bucket_name = bucket_name
        mock_response.chunks = [MagicMock(cid='cid1', encoded_size=100, size=50)]
        self.ipc_api.client.FileDownloadRangeCreate.return_value = mock_response
        result = self.ipc_api.create_range_file_download(None, bucket_name, file_name, 0, 1)
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.chunks[0].cid, 'cid1')

    def test_create_range_file_download_error(self):
        """Test range file download creation with error."""
        self.ipc_api.client = MagicMock()
        self.ipc_api.client.FileDownloadRangeCreate.side_effect = Exception('fail')
        with self.assertRaises(SDKError):
            self.ipc_api.create_range_file_download(None, 'bucket', 'file.txt', 0, 1)

    @patch.object(IPC, 'create_chunk_download')
    @patch.object(IPC, 'download_chunk_blocks')
    def test_download_success(self, mock_download_chunk_blocks, mock_create_chunk_download):
        """Test successful download operation."""
        file_download = MagicMock()
        file_download.bucket_name = 'bucket'
        file_download.name = 'file.txt'
        chunk1 = MagicMock()
        chunk2 = MagicMock()
        file_download.chunks = [chunk1, chunk2]
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        mock_create_chunk_download.return_value = MagicMock()
        self.ipc_api.download(ctx, file_download, writer)
        self.assertEqual(mock_create_chunk_download.call_count, 2)
        self.assertEqual(mock_download_chunk_blocks.call_count, 2)

    @patch.object(IPC, 'create_chunk_download')
    @patch.object(IPC, 'download_chunk_blocks')
    def test_download_context_cancel(self, mock_download_chunk_blocks, mock_create_chunk_download):
        """Test download stops on context cancellation."""
        file_download = MagicMock()
        file_download.bucket_name = 'bucket'
        file_download.name = 'file.txt'
        chunk1 = MagicMock()
        chunk2 = MagicMock()
        file_download.chunks = [chunk1, chunk2]
        ctx = MagicMock()
        ctx.done.side_effect = [False, True]
        writer = MagicMock()
        mock_create_chunk_download.return_value = MagicMock()
        with self.assertRaises(SDKError) as context:
            self.ipc_api.download(ctx, file_download, writer)
        self.assertIn('failed to download file: context cancelled', str(context.exception))

    @patch.object(IPC, 'create_chunk_download')
    @patch.object(IPC, 'download_chunk_blocks')
    def test_download_error_handling(self, mock_download_chunk_blocks, mock_create_chunk_download):
        """Test download raises SDKError on exception."""
        file_download = MagicMock()
        file_download.bucket_name = 'bucket'
        file_download.name = 'file.txt'
        file_download.chunks = [MagicMock()]
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        mock_create_chunk_download.side_effect = Exception('fail')
        with self.assertRaises(SDKError):
            self.ipc_api.download(ctx, file_download, writer)


if __name__ == '__main__':
    unittest.main() 