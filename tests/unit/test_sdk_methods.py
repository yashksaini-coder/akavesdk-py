import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.sdk import SDK, SDKError, BucketCreateResult, Bucket
from sdk.common import MIN_BUCKET_NAME_LENGTH, BLOCK_SIZE
import grpc
from private.pb import nodeapi_pb2


class TestSDKMethods(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_address = "localhost:5000"
        self.test_max_concurrency = 10
        self.test_block_part_size = 1024
        self.test_use_connection_pool = True
        self.test_encryption_key = b'a' * 32  # 32 bytes key
        self.test_private_key = "0x1234567890abcdef"
        
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_sdk_initialization_success(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test successful SDK initialization."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_node_stub.return_value = MagicMock()
        mock_ipc_stub.return_value = MagicMock()
        
        # Act
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool,
            encryption_key=self.test_encryption_key,
            private_key=self.test_private_key
        )
        
        # Assert
        self.assertEqual(sdk.max_concurrency, self.test_max_concurrency)
        self.assertEqual(sdk.block_part_size, self.test_block_part_size)
        self.assertEqual(sdk.use_connection_pool, self.test_use_connection_pool)
        self.assertEqual(sdk.encryption_key, self.test_encryption_key)
        self.assertEqual(sdk.private_key, self.test_private_key)
        mock_channel.assert_called()
        mock_node_stub.assert_called()
        mock_ipc_stub.assert_called()
    
    def test_sdk_initialization_invalid_block_part_size(self):
        """Test SDK initialization with invalid block part size."""
        with self.assertRaises(SDKError) as context:
            SDK(
                address=self.test_address,
                max_concurrency=self.test_max_concurrency,
                block_part_size=0,  # Invalid size
                use_connection_pool=self.test_use_connection_pool
            )
        
        self.assertIn("Invalid blockPartSize", str(context.exception))
    
    def test_sdk_initialization_invalid_encryption_key_length(self):
        """Test SDK initialization with invalid encryption key length."""
        with self.assertRaises(SDKError) as context:
            SDK(
                address=self.test_address,
                max_concurrency=self.test_max_concurrency,
                block_part_size=self.test_block_part_size,
                use_connection_pool=self.test_use_connection_pool,
                encryption_key=b'short_key'  # Invalid length
            )
        
        self.assertIn("Encryption key length should be 32 bytes long", str(context.exception))
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_create_bucket_success(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test successful bucket creation."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_client = MagicMock()
        mock_node_stub.return_value = mock_client
        mock_ipc_stub.return_value = MagicMock()
        
        # Mock the response
        mock_response = MagicMock()
        mock_response.name = "test-bucket"
        mock_response.created_at = MagicMock()
        mock_client.BucketCreate.return_value = mock_response
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act
        result = sdk.create_bucket(None, "test-bucket")
        
        # Assert
        self.assertIsInstance(result, BucketCreateResult)
        self.assertEqual(result.name, "test-bucket")
        mock_client.BucketCreate.assert_called_once()
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_create_bucket_invalid_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test bucket creation with invalid name."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_node_stub.return_value = MagicMock()
        mock_ipc_stub.return_value = MagicMock()
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            sdk.create_bucket(None, "a")  # Too short name
        
        self.assertIn("Invalid bucket name", str(context.exception))
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_view_bucket_success(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test successful bucket viewing."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_client = MagicMock()
        mock_node_stub.return_value = mock_client
        mock_ipc_stub.return_value = MagicMock()
        
        # Mock the response
        mock_response = MagicMock()
        mock_response.name = "test-bucket"
        mock_response.created_at = MagicMock()
        mock_client.BucketView.return_value = mock_response
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act
        result = sdk.view_bucket(None, "test-bucket")
        
        # Assert
        self.assertIsInstance(result, Bucket)
        self.assertEqual(result.name, "test-bucket")
        mock_client.BucketView.assert_called_once()
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_view_bucket_empty_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test bucket viewing with empty name."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_node_stub.return_value = MagicMock()
        mock_ipc_stub.return_value = MagicMock()
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            sdk.view_bucket(None, "")
        
        self.assertIn("Invalid bucket name", str(context.exception))
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_delete_bucket_success(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test successful bucket deletion."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_client = MagicMock()
        mock_node_stub.return_value = mock_client
        mock_ipc_stub.return_value = MagicMock()
        
        # Mock successful deletion (no exception)
        mock_client.BucketDelete.return_value = None
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act
        result = sdk.delete_bucket(None, "test-bucket")
        
        # Assert
        self.assertTrue(result)
        mock_client.BucketDelete.assert_called_once()
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_delete_bucket_empty_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test bucket deletion with empty name."""
        # Arrange
        mock_channel.return_value = MagicMock()
        mock_node_stub.return_value = MagicMock()
        mock_ipc_stub.return_value = MagicMock()
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool
        )
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            sdk.delete_bucket(None, "")
        
        self.assertIn("Invalid bucket name", str(context.exception))
    
    @patch('grpc.insecure_channel')
    @patch('private.pb.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('private.pb.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_close_connections(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test closing SDK connections."""
        # Arrange
        mock_conn = MagicMock()
        mock_ipc_conn = MagicMock()
        mock_channel.side_effect = [mock_conn, mock_ipc_conn]
        mock_node_stub.return_value = MagicMock()
        mock_ipc_stub.return_value = MagicMock()
        
        sdk = SDK(
            address=self.test_address,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool,
            ipc_address="different_address:5001"  # Different IPC address
        )
        
        # Act
        sdk.close()
        
        # Assert
        mock_conn.close.assert_called_once()
        mock_ipc_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main() 