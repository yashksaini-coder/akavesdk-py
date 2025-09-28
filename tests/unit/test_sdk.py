
import pytest
from unittest.mock import Mock, MagicMock, patch, call
import grpc
import io
import time
from datetime import datetime

from sdk.sdk import (
    SDK, 
    AkaveContractFetcher,
    BucketCreateResult, 
    Bucket, 
    MonkitStats,
    WithRetry,
    SDKOption,
    WithMetadataEncryption,
    WithEncryptionKey,
    WithPrivateKey,
    WithStreamingMaxBlocksInChunk,
    WithErasureCoding,
    WithChunkBuffer,
    WithoutRetry,
    get_monkit_stats,
    extract_block_data,
    encryption_key_derivation,
    is_retryable_tx_error,
    skip_to_position,
    parse_timestamp
)
from sdk.config import SDKConfig, SDKError
from tests.fixtures.common_fixtures import mock_sdk_config


class TestAkaveContractFetcher:
    
    def test_init(self):
        """Test AkaveContractFetcher initialization."""
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        assert fetcher.node_address == "test.node.ai:5500"
        assert fetcher.channel is None
        assert fetcher.stub is None
    
    @patch('grpc.insecure_channel')
    @patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_connect_success(self, mock_stub_class, mock_channel):
        mock_channel_instance = Mock()
        mock_stub_instance = Mock()
        mock_channel.return_value = mock_channel_instance
        mock_stub_class.return_value = mock_stub_instance
        
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.connect()
        
        assert result is True
        assert fetcher.channel == mock_channel_instance
        assert fetcher.stub == mock_stub_instance
        mock_channel.assert_called_once_with("test.node.ai:5500")
    
    @patch('grpc.insecure_channel')
    def test_connect_grpc_error(self, mock_channel):
        mock_channel.side_effect = grpc.RpcError("Connection failed")
        
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.connect()
        
        assert result is False
        assert fetcher.channel is None
        assert fetcher.stub is None
    
    def test_fetch_contract_addresses_no_stub(self):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.fetch_contract_addresses()
        assert result is None
    
    @patch('sdk.sdk.ipcnodeapi_pb2.ConnectionParamsRequest')
    def test_fetch_contract_addresses_success(self, mock_request):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        
        # Mock stub and response
        mock_stub = Mock()
        mock_response = Mock()
        mock_response.dial_uri = "https://dial.uri"
        mock_response.storage_address = "0x123..."
        mock_response.access_address = "0x456..."
        
        mock_stub.ConnectionParams.return_value = mock_response
        fetcher.stub = mock_stub
        
        result = fetcher.fetch_contract_addresses()
        
        expected = {
            'dial_uri': "https://dial.uri",
            'contract_address': "0x123...",
            'access_address': "0x456..."
        }
        assert result == expected
    
    def test_close(self):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        mock_channel = Mock()
        fetcher.channel = mock_channel
        
        fetcher.close()
        mock_channel.close.assert_called_once()


class TestWithRetry:
    
    def test_init_defaults(self):
        retry = WithRetry()
        assert retry.max_attempts == 5
        assert retry.base_delay == 0.1
    
    def test_init_custom(self):
        retry = WithRetry(max_attempts=3, base_delay=0.5)
        assert retry.max_attempts == 3
        assert retry.base_delay == 0.5
    
    def test_do_success_first_try(self):
        retry = WithRetry()
        
        def success_func():
            return False, None  # No retry needed, no error
        
        result = retry.do(None, success_func)
        assert result is None
    
    def test_do_non_retryable_error(self):
        retry = WithRetry()
        error = Exception("Non-retryable error")
        
        def non_retryable_func():
            return False, error  # No retry needed, but error
        
        result = retry.do(None, non_retryable_func)
        assert result == error
    
    @patch('time.sleep')
    def test_do_retryable_success(self, mock_sleep):
        retry = WithRetry(max_attempts=3, base_delay=0.1)
        call_count = 0
        
        def retryable_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return True, Exception("Retryable error")  # Retry needed
            return False, None  # Success on 3rd try
        
        result = retry.do(None, retryable_func)
        assert result is None
        assert call_count == 3
        assert mock_sleep.call_count == 2  # 2 retries
    
    @patch('time.sleep')
    def test_do_max_retries_exceeded(self, mock_sleep):
        retry = WithRetry(max_attempts=2, base_delay=0.1)
        original_error = Exception("Persistent error")
        
        def failing_func():
            return True, original_error  # Always retryable error
        
        result = retry.do(None, failing_func)
        assert "max retries exceeded" in str(result)
        assert mock_sleep.call_count == 2  # 2 retries


class TestSDKOptions:
    
    def test_with_metadata_encryption(self):
        option = WithMetadataEncryption()
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.use_metadata_encryption is True
    
    def test_with_encryption_key(self):
        key = b"test_encryption_key_32_bytes123"
        option = WithEncryptionKey(key)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.encryption_key == key
    
    def test_with_private_key(self):
        key = "0x123456789..."
        option = WithPrivateKey(key)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.private_key == key
    
    def test_with_streaming_max_blocks_in_chunk(self):
        max_blocks = 10
        option = WithStreamingMaxBlocksInChunk(max_blocks)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.streaming_max_blocks_in_chunk == max_blocks
    
    def test_with_erasure_coding(self):
        parity_blocks = 3
        option = WithErasureCoding(parity_blocks)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.parity_blocks_count == parity_blocks
    
    def test_with_chunk_buffer(self):
        buffer_size = 20
        option = WithChunkBuffer(buffer_size)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.chunk_buffer == buffer_size
    
    def test_without_retry(self):
        option = WithoutRetry()
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert isinstance(mock_sdk.with_retry, WithRetry)
        assert mock_sdk.with_retry.max_attempts == 0


@patch('grpc.insecure_channel')
@patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub')
@patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
class TestSDK:
    
    def test_init_valid_config(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test SDK initialization with valid config."""
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=1024
        )
        
        mock_channel_instance = Mock()
        mock_channel.return_value = mock_channel_instance
        
        sdk = SDK(config)
        
        assert sdk.config == config
        assert sdk.conn == mock_channel_instance
        assert sdk.ipc_conn == mock_channel_instance  # Same address
        mock_channel.assert_called_once_with("test.node.ai:5500")
    
    def test_init_invalid_block_part_size(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=0  # Invalid
        )
        
        with pytest.raises(SDKError, match="Invalid blockPartSize"):
            SDK(config)
    
    def test_init_invalid_encryption_key_length(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            encryption_key=b"short_key"  # Not 32 bytes
        )
        
        mock_channel.return_value = Mock()
        
        with pytest.raises(SDKError, match="Encryption key length should be 32 bytes"):
            SDK(config)
    
    def test_init_with_different_ipc_address(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            ipc_address="ipc.node.ai:5501",
            private_key="test_key"
        )
        
        mock_channel_instance1 = Mock()
        mock_channel_instance2 = Mock()
        mock_channel.side_effect = [mock_channel_instance1, mock_channel_instance2]
        
        sdk = SDK(config)
        
        assert sdk.conn == mock_channel_instance1
        assert sdk.ipc_conn == mock_channel_instance2
        assert mock_channel.call_count == 2
    
    def test_close(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_conn = Mock()
        mock_ipc_conn = Mock()
        mock_channel.side_effect = [mock_conn, mock_ipc_conn]
        
        config.ipc_address = "different.ipc.ai:5501"  # Different IPC address
        sdk = SDK(config)
        
        sdk.close()
        
        mock_conn.close.assert_called_once()
        mock_ipc_conn.close.assert_called_once()
    
    def test_validate_bucket_name_valid(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Should not raise an exception
        sdk._validate_bucket_name("valid-bucket-name", "TestMethod")
    
    def test_validate_bucket_name_invalid(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk._validate_bucket_name("", "TestMethod")
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk._validate_bucket_name("ab", "TestMethod")
    
    @patch('sdk.sdk.nodeapi_pb2.BucketCreateRequest')
    def test_create_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.name = "test-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.AsTime.return_value = datetime.now()
        
        sdk.client.BucketCreate.return_value = mock_response
        
        result = sdk.create_bucket("test-bucket")
        
        assert isinstance(result, BucketCreateResult)
        assert result.name == "test-bucket"
        sdk.client.BucketCreate.assert_called_once()
    
    def test_create_bucket_invalid_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk.create_bucket("ab")  # Too short
    
    @patch('sdk.sdk.nodeapi_pb2.BucketViewRequest')
    def test_view_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.name = "test-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.AsTime.return_value = datetime.now()
        
        sdk.client.BucketView.return_value = mock_response
        
        result = sdk.view_bucket("test-bucket")
        
        assert isinstance(result, Bucket)
        assert result.name == "test-bucket"
        sdk.client.BucketView.assert_called_once()
    
    @patch('sdk.sdk.nodeapi_pb2.BucketDeleteRequest')
    def test_delete_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        sdk.client.BucketDelete.return_value = Mock()
        
        result = sdk.delete_bucket("test-bucket")
        
        assert result is True
        sdk.client.BucketDelete.assert_called_once()
    
    @patch('sdk.sdk.StreamingAPI')
    def test_streaming_api(self, mock_streaming_api, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        mock_streaming_instance = Mock()
        mock_streaming_api.return_value = mock_streaming_instance
        
        result = sdk.streaming_api()
        
        assert result == mock_streaming_instance
        mock_streaming_api.assert_called_once()
    
    @patch('sdk.sdk.IPC')
    @patch('sdk.sdk.Client.dial')
    def test_ipc_success(self, mock_dial, mock_ipc_class, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500", private_key="test_key")
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        sdk._contract_info = {
            'dial_uri': 'https://test.dial.uri',
            'contract_address': '0x123...',
            'access_address': '0x456...'
        }
        
        mock_ipc_instance = Mock()
        mock_dial.return_value = mock_ipc_instance
        
        mock_ipc_result = Mock()
        mock_ipc_class.return_value = mock_ipc_result
        
        result = sdk.ipc()
        
        assert result == mock_ipc_result
        mock_dial.assert_called_once()
        mock_ipc_class.assert_called_once()
    
    def test_ipc_no_private_key(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(address="test.node.ai:5500")  # No private key
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Private key is required"):
            sdk.ipc()


class TestUtilityFunctions:
    
    def test_get_monkit_stats(self):
        stats = get_monkit_stats()
        assert isinstance(stats, list)
        assert len(stats) == 0
    
    @patch('sdk.sdk.CID.decode')
    def test_extract_block_data_raw(self, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "raw"
        mock_cid_decode.return_value = mock_cid
        
        data = b"test data"
        result = extract_block_data("test_cid", data)
        assert result == data
    
    @patch('sdk.sdk.CID.decode')
    @patch('sdk.dag.extract_block_data')
    def test_extract_block_data_dag_pb(self, mock_dag_extract, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "dag-pb"
        mock_cid_decode.return_value = mock_cid
        
        data = b"test data"
        expected_result = b"extracted data"
        mock_dag_extract.return_value = expected_result
        
        result = extract_block_data("test_cid", data)
        assert result == expected_result
        mock_dag_extract.assert_called_once_with("test_cid", data)
    
    @patch('sdk.sdk.CID.decode')
    def test_extract_block_data_unknown_codec(self, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "unknown"
        mock_cid_decode.return_value = mock_cid
        
        with pytest.raises(ValueError, match="unknown cid type"):
            extract_block_data("test_cid", b"test data")
    
    @patch('sdk.sdk.derive_key')
    def test_encryption_key_derivation_success(self, mock_derive_key):
        parent_key = b"parent_key_32_bytes_long_test123"
        info_data = ["bucket", "file"]
        expected_key = b"derived_key"
        
        mock_derive_key.return_value = expected_key
        
        result = encryption_key_derivation(parent_key, *info_data)
        
        assert result == expected_key
        mock_derive_key.assert_called_once_with(parent_key, "bucket/file".encode())
    
    def test_encryption_key_derivation_empty_key(self):
        result = encryption_key_derivation(b"", "bucket", "file")
        assert result == b""
    
    def test_is_retryable_tx_error(self):
        assert is_retryable_tx_error(Exception("nonce too low"))
        assert is_retryable_tx_error(Exception("REPLACEMENT TRANSACTION UNDERPRICED"))
        assert is_retryable_tx_error(Exception("EOF error"))
        
        assert not is_retryable_tx_error(Exception("other error"))
        assert not is_retryable_tx_error(None)
    
    def test_skip_to_position_seekable(self):
        data = b"0123456789"
        reader = io.BytesIO(data)
        
        skip_to_position(reader, 5)
        
        # Should be at position 5
        remaining = reader.read()
        assert remaining == b"56789"
    
    def test_skip_to_position_non_seekable(self):
        class NonSeekableReader:
            def __init__(self, data):
                self.data = data
                self.pos = 0
            
            def read(self, size=-1):
                if size == -1:
                    result = self.data[self.pos:]
                    self.pos = len(self.data)
                else:
                    result = self.data[self.pos:self.pos + size]
                    self.pos += len(result)
                return result
        
        reader = NonSeekableReader(b"0123456789")
        
        skip_to_position(reader, 5)
        
        remaining = reader.read()
        assert remaining == b"56789"
    
    def test_skip_to_position_zero(self):
        reader = io.BytesIO(b"test data")
        original_pos = reader.tell()
        
        skip_to_position(reader, 0)
        
        assert reader.tell() == original_pos
    
    def test_parse_timestamp_none(self):
        result = parse_timestamp(None)
        assert result is None
    
    def test_parse_timestamp_with_astime(self):
        mock_ts = Mock()
        expected_datetime = datetime.now()
        mock_ts.AsTime.return_value = expected_datetime
        
        result = parse_timestamp(mock_ts)
        assert result == expected_datetime
    
    def test_parse_timestamp_without_astime(self):
        mock_ts = Mock()
        del mock_ts.AsTime  # Remove AsTime method
        
        result = parse_timestamp(mock_ts)
        assert result == mock_ts  # Should return the object itself


@pytest.mark.integration
class TestSDKIntegration:
    
    def test_sdk_lifecycle(self, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)   
            assert sdk.config == mock_sdk_config 
            sdk.close()
            sdk.conn.close.assert_called_once()
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_success(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            mock_fetcher = Mock()
            mock_fetcher.connect.return_value = True
            mock_fetcher.fetch_contract_addresses.return_value = {
                'dial_uri': 'https://test.uri',
                'contract_address': '0x123...'
            }
            mock_fetcher_class.return_value = mock_fetcher
            
            result = sdk._fetch_contract_info()
            
            assert result is not None
            assert result['dial_uri'] == 'https://test.uri'
            assert result['contract_address'] == '0x123...'
            
            result2 = sdk._fetch_contract_info()
            assert result2 == result
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_failure(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            mock_fetcher = Mock()
            mock_fetcher.connect.return_value = False
            mock_fetcher_class.return_value = mock_fetcher
            
            result = sdk._fetch_contract_info()
            
            assert result is None
