import unittest
import io
import os
import random
import string
import time
import sys
import logging
import pytest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timezone
import grpc
from google.protobuf.timestamp_pb2 import Timestamp

# Import our actual implementation
from sdk.sdk_streaming import (
    StreamingAPI, Chunk, FileUpload, FileDownload, FileChunkUpload, 
    FileBlockUpload, FileChunkDownload, FileBlockDownload, AkaveBlockData,
    FilecoinBlockData, ConnectionPool, DAGRoot
)
from sdk.sdk import SDKError
from sdk.model import FileMeta
from sdk.erasure_code import ErasureCode

# Constants for testing
MAX_CONCURRENCY = 5
BLOCK_PART_SIZE = 128 * 1024  # 128 KiB
MB = 1024 * 1024  # 1 MB in bytes

# Mock modules that our actual implementation uses
sys.modules['multiformats'] = MagicMock()
sys.modules['multiformats.cid'] = MagicMock()
sys.modules['private.memory.memory'] = MagicMock()
sys.modules['private.memory.memory'].Size = MagicMock()
sys.modules['private.memory.memory'].Size.MB = 1024 * 1024
sys.modules['private.spclient.spclient'] = MagicMock()
sys.modules['private.spclient.spclient'].SPClient = MagicMock
sys.modules['private.encryption'] = MagicMock()
sys.modules['private.encryption'].encrypt = MagicMock(return_value=b'encrypted_data')
sys.modules['private.encryption'].decrypt = MagicMock(return_value=b'decrypted_data')
sys.modules['private.encryption'].derive_key = MagicMock(return_value=b'derived_key')
sys.modules['private.pb'] = MagicMock()
sys.modules['private.pb.nodeapi_pb2'] = MagicMock()
sys.modules['private.pb.nodeapi_pb2_grpc'] = MagicMock()
sys.modules['sdk.dag'] = MagicMock()
sys.modules['sdk.dag'].build_dag = MagicMock()
sys.modules['sdk.dag'].extract_block_data = MagicMock(return_value=b'test_block_data')

# We don't need to redefine these classes since we're using the real ones
# class SDKError, FileMeta, ErasureCode, StreamingAPI

class BackgroundContext:
    """Simple context implementation similar to Go's context.Background()"""
    def __init__(self):
        self._cancelled = False

    def done(self):
        return self._cancelled

    def cancel(self):
        self._cancelled = True

def random_string(length=10):
    """Generate a random string of fixed length"""
    prefix = 'test-'
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return prefix + suffix

class MockTimestamp:
    """Mock Google Protobuf Timestamp"""
    def __init__(self, timestamp=None):
        self.timestamp = timestamp or time.time()
        
    def ToDatetime(self):
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)

class MockStreamAPIClient:
    """Mock StreamAPI client for testing"""
    def __init__(self):
        self.test_files = {}
        self.test_uploads = {}
        self.test_downloads = {}
    
    def FileView(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.file_name == "":
            raise grpc.RpcError("empty file name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        if request.file_name == "nonexistent":
            raise grpc.RpcError("file not found")
        
        # Create a timestamp
        timestamp = MockTimestamp()
        
        # Return a mocked response
        res = MagicMock()
        res.stream_id = "test-stream-id"
        res.root_cid = "test-root-cid"
        res.bucket_name = request.bucket_name
        res.file_name = request.file_name
        res.encoded_size = 1024
        res.size = 1000
        res.created_at = timestamp
        res.committed_at = timestamp
        
        return res
    
    def FileList(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        
        # Create a timestamp
        timestamp = MockTimestamp()
        
        # Return a mocked response with 3 files
        res = MagicMock()
        res.files = []
        
        for i in range(3):
            file_meta = MagicMock()
            file_meta.stream_id = f"test-stream-id-{i}"
            file_meta.root_cid = f"test-root-cid-{i}"
            file_meta.file_name = f"test-file-{i}.txt"
            file_meta.encoded_size = 1024 * (i + 1)
            file_meta.size = 1000 * (i + 1)
            file_meta.created_at = timestamp
            file_meta.committed_at = timestamp
            
            res.files.append(file_meta)
        
        return res
    
    def FileVersions(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        
        # Create a timestamp
        timestamp = MockTimestamp()
        
        # Return a mocked response with 2 versions
        res = MagicMock()
        res.versions = []
        
        for i in range(2):
            file_meta = MagicMock()
            file_meta.stream_id = f"test-stream-id-{i}"
            file_meta.root_cid = f"test-root-cid-{i}"
            file_meta.file_name = request.file_name
            file_meta.encoded_size = 1024 * (i + 1)
            file_meta.size = 1000 * (i + 1)
            file_meta.created_at = timestamp
            file_meta.committed_at = timestamp
            
            res.versions.append(file_meta)
        
        return res
    
    def FileDelete(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.file_name == "":
            raise grpc.RpcError("empty file name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        if request.file_name == "nonexistent":
            raise grpc.RpcError("file not found")
        
        return MagicMock()
    
    def FileUploadCreate(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        
        # Create a timestamp
        timestamp = MockTimestamp()
        
        # Return a mocked response
        res = MagicMock()
        res.bucket_name = request.bucket_name
        res.file_name = request.file_name
        res.stream_id = "test-upload-stream-id"
        res.created_at = timestamp
        
        return res
    
    def FileUploadChunkCreate(self, ctx, request):
        # Return a mocked response with blocks
        res = MagicMock()
        res.blocks = []
        
        for i, block in enumerate(request.chunk.blocks):
            block_meta = MagicMock()
            block_meta.cid = block.cid
            block_meta.node_id = f"test-node-id-{i}"
            block_meta.node_address = f"test-node-address-{i}"
            block_meta.permit = b"test-permit"
            
            res.blocks.append(block_meta)
        
        return res
    
    def FileUploadCommit(self, ctx, request):
        # Create a timestamp
        timestamp = MockTimestamp()
        
        # Return a mocked response
        res = MagicMock()
        res.stream_id = request.stream_id
        res.bucket_name = "test-bucket"
        res.file_name = "test-file.txt"
        res.encoded_size = 1024
        res.size = 1000
        res.committed_at = timestamp
        
        return res
    
    def FileDownloadCreate(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        
        # Return a mocked response with chunks
        res = MagicMock()
        res.stream_id = "test-download-stream-id"
        res.bucket_name = request.bucket_name
        res.chunks = []
        
        for i in range(2):
            chunk = MagicMock()
            chunk.cid = f"test-chunk-cid-{i}"
            chunk.encoded_size = 1024
            chunk.size = 1000
            chunk.index = i
            
            res.chunks.append(chunk)
        
        return res
    
    def FileDownloadRangeCreate(self, ctx, request):
        if request.bucket_name == "":
            raise grpc.RpcError("empty bucket name")
        if request.bucket_name == "nonexistent":
            raise grpc.RpcError("bucket not found")
        
        # Return a mocked response with chunks
        res = MagicMock()
        res.stream_id = "test-download-stream-id"
        res.bucket_name = request.bucket_name
        res.chunks = []
        
        for i in range(request.start_index, request.end_index + 1):
            chunk = MagicMock()
            chunk.cid = f"test-chunk-cid-{i}"
            chunk.encoded_size = 1024
            chunk.size = 1000
            chunk.index = i - request.start_index
            
            res.chunks.append(chunk)
        
        return res
    
    def FileDownloadChunkCreate(self, ctx, request):
        # Return a mocked response with blocks
        res = MagicMock()
        res.blocks = []
        
        for i in range(3):
            block = MagicMock()
            block.cid = f"test-block-cid-{i}"
            block.node_id = f"test-node-id-{i}"
            block.node_address = f"test-node-address-{i}"
            block.permit = b"test-permit"
            
            res.blocks.append(block)
        
        return res
    
    def FileDownloadChunkCreateV2(self, ctx, request):
        # Return a mocked response with blocks
        res = MagicMock()
        res.blocks = []
        
        for i in range(3):
            block = MagicMock()
            block.cid = f"test-block-cid-{i}"
            
            # Alternate between Akave and Filecoin blocks
            if i % 2 == 0:
                block.akave = MagicMock()
                block.akave.node_id = f"test-node-id-{i}"
                block.akave.node_address = f"test-node-address-{i}"
                block.filecoin = None
            else:
                block.filecoin = MagicMock()
                block.filecoin.sp_address = f"test-sp-address-{i}"
                block.akave = None
            
            res.blocks.append(block)
        
        return res
    
    def FileUploadBlock(self, ctx):
        # Return a mocked stream
        stream = MagicMock()
        stream.send = MagicMock()
        stream.close_and_recv = MagicMock(return_value=(None, None))
        return stream
    
    def FileDownloadBlock(self, ctx, request):
        # Return a mocked stream
        stream = MagicMock()
        
        # Mock receiving data
        mock_data = MagicMock()
        mock_data.data = b"test-block-data"
        
        stream.recv = MagicMock(side_effect=[mock_data, None])
        return stream

class MockErasureCode(ErasureCode):
    """Mock erasure code for testing"""
    def __init__(self, data_blocks=2, parity_blocks=1):
        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
    
    def encode(self, data):
        """Mock encoding - just duplicate the data"""
        return data * (self.data_blocks + self.parity_blocks)
    
    def extract_data(self, blocks, size):
        """Mock extraction - just return test data of the right size"""
        return b"test-decoded-data" * (size // 16 + 1)[:size]

# Mock for connection pool - replace with a patch instead of redefining
@patch('sdk.sdk_streaming.ConnectionPool')
@patch('sdk.sdk_streaming.build_dag')
@patch('sdk.sdk_streaming.extract_block_data', return_value=b"test-block-data")
class TestStreamingAPI(unittest.TestCase):
    """Tests for the StreamingAPI class"""
    
    def setUp(self):
        self.conn = MagicMock()
        self.client = MockStreamAPIClient()
        self.erasure_code = MockErasureCode()
        
        # Create a StreamingAPI instance
        self.streaming_api = StreamingAPI(
            conn=self.conn,
            client=self.client,
            erasure_code=self.erasure_code,
            max_concurrency=2,
            block_part_size=512,
            use_connection_pool=True,
            encryption_key=b"test-encryption-key",
            max_blocks_in_chunk=4
        )
    
    def test_file_info_success(self, mock_extract, mock_build, mock_pool):
        # Test successful file info retrieval
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Call the method
        result = self.streaming_api.file_info(None, bucket_name, file_name)
        
        # Check the result
        self.assertEqual(result.stream_id, "test-stream-id")
        self.assertEqual(result.root_cid, "test-root-cid")
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.name, file_name)
        self.assertEqual(result.encoded_size, 1024)
        self.assertEqual(result.size, 1000)
    
    def test_file_info_empty_bucket(self, mock_extract, mock_build, mock_pool):
        # Test with empty bucket name
        with self.assertRaises(SDKError):
            self.streaming_api.file_info(None, "", "test-file.txt")
    
    def test_file_info_empty_filename(self, mock_extract, mock_build, mock_pool):
        # Test with empty file name
        with self.assertRaises(SDKError):
            self.streaming_api.file_info(None, "test-bucket", "")
    
    def test_file_info_nonexistent_file(self, mock_extract, mock_build, mock_pool):
        # Test with nonexistent file
        with self.assertRaises(SDKError):
            self.streaming_api.file_info(None, "nonexistent", "nonexistent")
    
    def test_list_files_success(self, mock_extract, mock_build, mock_pool):
        # Test successful file listing
        bucket_name = "test-bucket"
        
        # Call the method
        result = self.streaming_api.list_files(None, bucket_name)
        
        # Check the result
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].stream_id, "test-stream-id-0")
        self.assertEqual(result[1].stream_id, "test-stream-id-1")
        self.assertEqual(result[2].stream_id, "test-stream-id-2")
    
    def test_list_files_empty_bucket_name(self, mock_extract, mock_build, mock_pool):
        # Test with empty bucket name
        with self.assertRaises(SDKError):
            self.streaming_api.list_files(None, "")
    
    def test_list_files_nonexistent_bucket(self, mock_extract, mock_build, mock_pool):
        # Test with nonexistent bucket
        with self.assertRaises(SDKError):
            self.streaming_api.list_files(None, "nonexistent")
    
    def test_file_versions(self, mock_extract, mock_build, mock_pool):
        # Test file versions retrieval
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Call the method
        result = self.streaming_api.file_versions(None, bucket_name, file_name)
        
        # Check the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].stream_id, "test-stream-id-0")
        self.assertEqual(result[1].stream_id, "test-stream-id-1")
        self.assertEqual(result[0].name, file_name)
        self.assertEqual(result[1].name, file_name)
    
    def test_file_delete(self, mock_extract, mock_build, mock_pool):
        # Test file deletion
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Call the method
        self.streaming_api.file_delete(None, bucket_name, file_name)
        
        # No exception should be raised
    
    def test_file_delete_empty_bucket(self, mock_extract, mock_build, mock_pool):
        # Test with empty bucket name
        with self.assertRaises(SDKError):
            self.streaming_api.file_delete(None, "", "test-file.txt")
    
    def test_create_file_upload(self, mock_extract, mock_build, mock_pool):
        # Test create file upload
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Call the method
        result = self.streaming_api.create_file_upload(None, bucket_name, file_name)
        
        # Check the result
        self.assertEqual(result.BucketName, bucket_name)
        self.assertEqual(result.Name, file_name)
        self.assertEqual(result.StreamID, "test-upload-stream-id")
    
    def test_create_file_upload_empty_bucket(self, mock_extract, mock_build, mock_pool):
        # Test with empty bucket name
        with self.assertRaises(SDKError):
            self.streaming_api.create_file_upload(None, "", "test-file.txt")
    
    def test_upload(self, mock_extract, mock_build, mock_pool):
        # Mock DAG and chunk upload
        mock_chunk_dag = MagicMock()
        mock_chunk_dag.cid = MagicMock()
        mock_chunk_dag.cid.string = MagicMock(return_value="test-cid")
        mock_chunk_dag.raw_data_size = 1000
        mock_chunk_dag.proto_node_size = 1100
        mock_chunk_dag.blocks = [
            {"cid": "test-block-cid-1", "data": b"test-data-1"},
            {"cid": "test-block-cid-2", "data": b"test-data-2"}
        ]
        mock_build.return_value = mock_chunk_dag
        
        # Mock file upload
        upload = FileUpload(
            BucketName="test-bucket",
            Name="test-file.txt",
            StreamID="test-upload-stream-id",
            CreatedAt=datetime.now()
        )
        
        # Create a test file-like reader
        test_data = b"test-file-content" * 1000  # Create some test data
        reader = io.BytesIO(test_data)
        
        # Call the method
        result = self.streaming_api.upload(None, upload, reader)
        
        # Check the result
        self.assertEqual(result.stream_id, "test-stream-id")
        self.assertEqual(result.bucket_name, "test-bucket")
        self.assertEqual(result.name, "test-file.txt")
    
    def test_upload_empty_file(self, mock_extract, mock_build, mock_pool):
        # Mock file upload
        upload = FileUpload(
            BucketName="test-bucket",
            Name="test-file.txt",
            StreamID="test-upload-stream-id",
            CreatedAt=datetime.now()
        )
        
        # Create an empty file-like reader
        reader = io.BytesIO(b"")
        
        # Call the method with empty file
        with self.assertRaises(SDKError):
            self.streaming_api.upload(None, upload, reader)
    
    def test_create_file_download(self, mock_extract, mock_build, mock_pool):
        # Test create file download
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        # Call the method
        result = self.streaming_api.create_file_download(None, bucket_name, file_name)
        
        # Check the result
        self.assertEqual(result.StreamID, "test-download-stream-id")
        self.assertEqual(result.BucketName, bucket_name)
        self.assertEqual(result.Name, file_name)
        self.assertEqual(len(result.Chunks), 2)
        self.assertEqual(result.Chunks[0].CID, "test-chunk-cid-0")
        self.assertEqual(result.Chunks[1].CID, "test-chunk-cid-1")
    
    def test_create_range_file_download(self, mock_extract, mock_build, mock_pool):
        # Test create range file download
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        start = 2
        end = 4
        
        # Call the method
        result = self.streaming_api.create_range_file_download(None, bucket_name, file_name, start, end)
        
        # Check the result
        self.assertEqual(result.StreamID, "test-download-stream-id")
        self.assertEqual(result.BucketName, bucket_name)
        self.assertEqual(result.Name, file_name)
        self.assertEqual(len(result.Chunks), 3)  # 3 chunks (2, 3, 4)
        self.assertEqual(result.Chunks[0].Index, 2)
        self.assertEqual(result.Chunks[1].Index, 3)
        self.assertEqual(result.Chunks[2].Index, 4)
    
    def test_download(self, mock_extract, mock_build, mock_pool):
        # Mock file download
        file_download = FileDownload(
            StreamID="test-download-stream-id",
            BucketName="test-bucket",
            Name="test-file.txt",
            Chunks=[
                Chunk(CID="test-chunk-cid-1", EncodedSize=1024, Size=1000, Index=0),
                Chunk(CID="test-chunk-cid-2", EncodedSize=1024, Size=1000, Index=1)
            ]
        )
        
        # Create a file-like writer
        writer = io.BytesIO()
        
        # Call the method
        self.streaming_api.download(None, file_download, writer)
        
        # The test should pass if no exception is raised
    
    def test_download_v2(self, mock_extract, mock_build, mock_pool):
        # Mock file download
        file_download = FileDownload(
            StreamID="test-download-stream-id",
            BucketName="test-bucket",
            Name="test-file.txt",
            Chunks=[
                Chunk(CID="test-chunk-cid-1", EncodedSize=1024, Size=1000, Index=0),
                Chunk(CID="test-chunk-cid-2", EncodedSize=1024, Size=1000, Index=1)
            ]
        )
        
        # Create a file-like writer
        writer = io.BytesIO()
        
        # Call the method
        self.streaming_api.download_v2(None, file_download, writer)
        
        # The test should pass if no exception is raised
    
    def test_download_random(self, mock_extract, mock_build, mock_pool):
        # Mock file download
        file_download = FileDownload(
            StreamID="test-download-stream-id",
            BucketName="test-bucket",
            Name="test-file.txt",
            Chunks=[
                Chunk(CID="test-chunk-cid-1", EncodedSize=1024, Size=1000, Index=0),
                Chunk(CID="test-chunk-cid-2", EncodedSize=1024, Size=1000, Index=1)
            ]
        )
        
        # Create a file-like writer
        writer = io.BytesIO()
        
        # Call the method
        self.streaming_api.download_random(None, file_download, writer)
        
        # The test should pass if no exception is raised
    
    def test_download_random_no_erasure_code(self, mock_extract, mock_build, mock_pool):
        # Create a StreamingAPI instance without erasure code
        streaming_api = StreamingAPI(
            conn=self.conn,
            client=self.client,
            erasure_code=None,
            max_concurrency=2,
            block_part_size=512,
            use_connection_pool=True
        )
        
        # Mock file download
        file_download = FileDownload(
            StreamID="test-download-stream-id",
            BucketName="test-bucket",
            Name="test-file.txt",
            Chunks=[
                Chunk(CID="test-chunk-cid-1", EncodedSize=1024, Size=1000, Index=0),
                Chunk(CID="test-chunk-cid-2", EncodedSize=1024, Size=1000, Index=1)
            ]
        )
        
        # Create a file-like writer
        writer = io.BytesIO()
        
        # Call the method - should raise an error because erasure coding is required
        with self.assertRaises(SDKError):
            streaming_api.download_random(None, file_download, writer)
    
    def test_with_encryption_key(self, mock_extract, mock_build, mock_pool):
        # Create a StreamingAPI instance with encryption key
        streaming_api = StreamingAPI(
            conn=self.conn,
            client=self.client,
            erasure_code=None,  # Make these parameters explicit now
            max_concurrency=2,
            block_part_size=512,
            use_connection_pool=True,
            encryption_key=b"test-encryption-key"
        )
        
        # Test that encryption is properly configured
        self.assertEqual(streaming_api.encryption_key, b"test-encryption-key")
        
        # Try a file info call to verify the instance works
        result = streaming_api.file_info(None, "test-bucket", "test-file.txt")
        self.assertEqual(result.stream_id, "test-stream-id")
    
    def test_with_erasure_code(self, mock_extract, mock_build, mock_pool):
        # Create a StreamingAPI instance with custom erasure code
        erasure_code = MockErasureCode(data_blocks=3, parity_blocks=2)
        streaming_api = StreamingAPI(
            conn=self.conn,
            client=self.client,
            erasure_code=erasure_code,
            max_concurrency=2,  # Make these parameters explicit now
            block_part_size=512,
            use_connection_pool=True
        )
        
        # Test that erasure code is properly configured
        self.assertEqual(streaming_api.erasure_code.data_blocks, 3)
        self.assertEqual(streaming_api.erasure_code.parity_blocks, 2)
        
        # Try a file info call to verify the instance works
        result = streaming_api.file_info(None, "test-bucket", "test-file.txt")
        self.assertEqual(result.stream_id, "test-stream-id")

if __name__ == '__main__':
    unittest.main() 