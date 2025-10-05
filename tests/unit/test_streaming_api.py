import pytest
from unittest.mock import Mock, MagicMock, patch, call
import io
import grpc
from datetime import datetime

from sdk.sdk_streaming import (
    StreamingAPI, DAGRoot, encryption_key, to_proto_chunk
)
from sdk.config import SDKConfig, SDKError
from sdk.model import (
    FileMeta, FileUpload, FileDownload, Chunk, 
    FileBlockUpload, FileChunkUpload, FileBlockDownload, FileChunkDownload,
    AkaveBlockData, FilecoinBlockData
)


class TestDAGRoot:
    
    def test_init(self):
        dag_root = DAGRoot()
        assert dag_root.links == []
    
    def test_new_classmethod(self):
        dag_root = DAGRoot.new()
        assert isinstance(dag_root, DAGRoot)
        assert dag_root.links == []
    
    def test_add_link(self):
        dag_root = DAGRoot()
        result = dag_root.add_link("cid123", 1024, 1200)
        assert result is None
        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid"] == "cid123"
        assert dag_root.links[0]["raw_data_size"] == 1024
        assert dag_root.links[0]["proto_node_size"] == 1200
    
    def test_build(self):
        dag_root = DAGRoot()
        result = dag_root.build()
        assert hasattr(result, 'string')
        cid_str = result.string()
        assert isinstance(cid_str, str)
        assert cid_str.startswith("Qm")


class TestEncryptionKey:
    
    def test_encryption_key_with_empty_parent(self):
        result = encryption_key(b"", "bucket", "file")
        assert result == b""
    
    @patch('sdk.sdk_streaming.derive_key')
    def test_encryption_key_with_valid_parent(self, mock_derive):
        parent = b"parent_key_32bytes_test123456789"
        expected = b"derived_key"
        mock_derive.return_value = expected
        
        result = encryption_key(parent, "bucket", "file")
        
        assert result == expected
        mock_derive.assert_called_once_with(parent, b"bucket/file")
    
    @patch('sdk.sdk_streaming.derive_key')
    def test_encryption_key_single_info(self, mock_derive):
        parent = b"parent_key"
        mock_derive.return_value = b"key"
        
        result = encryption_key(parent, "info")
        
        mock_derive.assert_called_once_with(parent, b"info")


class TestToProtoChunk:
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_to_proto_chunk_empty_blocks(self, mock_pb2):
        mock_chunk = Mock()
        mock_pb2.Chunk.return_value = mock_chunk
        mock_pb2.Chunk.Block = Mock()
        
        result = to_proto_chunk("stream123", "cid123", 0, 1024, [])
        
        assert result == mock_chunk
        mock_pb2.Chunk.assert_called_once_with(
            stream_id="stream123",
            cid="cid123",
            index=0,
            size=1024,
            blocks=[]
        )
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_to_proto_chunk_with_blocks(self, mock_pb2):
        mock_block_class = Mock()
        mock_pb2.Chunk.Block = mock_block_class
        mock_pb2.Chunk = Mock()
        
        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
            FileBlockUpload(cid="cid2", data=b"data2"),
        ]
        
        result = to_proto_chunk("stream123", "cid456", 1, 2048, blocks)
        
        assert mock_block_class.call_count == 2


class TestStreamingAPIInit:
    
    def test_init(self):
        mock_conn = Mock()
        mock_client = Mock()
        config = SDKConfig(
            address="test.node:5500",
            max_concurrency=5,
            block_part_size=128*1024,
            encryption_key=b"test_key_32_bytes_test123456789"
        )
        
        api = StreamingAPI(mock_conn, mock_client, config)
        
        assert api.client == mock_client
        assert api.conn == mock_conn
        assert api.max_concurrency == 5
        assert api.block_part_size == 128*1024
        assert api.encryption_key == b"test_key_32_bytes_test123456789"


class TestFileInfo:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_file_info_empty_bucket_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.api.file_info(None, "", "file.txt")
    
    def test_file_info_empty_file_name(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.api.file_info(None, "bucket", "")
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_file_info_success(self, mock_pb2):
        mock_response = Mock()
        mock_response.stream_id = "stream123"
        mock_response.root_cid = "root_cid"
        mock_response.name = "file.txt"
        mock_response.encoded_size = 2048
        mock_response.size = 1024
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1234567890
        mock_response.created_at.ToDatetime = Mock(return_value=datetime.now())
        
        self.mock_client.FileView.return_value = mock_response
        
        result = self.api.file_info(None, "bucket", "file.txt")
        
        assert isinstance(result, FileMeta)
        assert result.name == "file.txt"
        assert result.size == 1024


class TestListFiles:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_list_files_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.api.list_files(None, "")
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_list_files_success(self, mock_pb2):
        mock_file1 = Mock()
        mock_file1.stream_id = "s1"
        mock_file1.root_cid = "cid1"
        mock_file1.name = "file1.txt"
        mock_file1.encoded_size = 1024
        mock_file1.size = 512
        mock_file1.created_at = Mock()
        mock_file1.created_at.seconds = 100
        mock_file1.created_at.ToDatetime = Mock(return_value=datetime.now())
        
        mock_response = Mock()
        mock_response.files = [mock_file1]
        self.mock_client.FileList.return_value = mock_response
        
        result = self.api.list_files(None, "bucket")
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].name == "file1.txt"


class TestFileVersions:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_file_versions_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.api.file_versions(None, "", "file.txt")
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_file_versions_success(self, mock_pb2):
        mock_version = Mock()
        mock_version.stream_id = "v1"
        mock_version.root_cid = "cid_v1"
        mock_version.name = "file.txt"
        mock_version.encoded_size = 2048
        mock_version.size = 1024
        mock_version.created_at = Mock()
        mock_version.created_at.seconds = 200
        mock_version.created_at.ToDatetime = Mock(return_value=datetime.now())
        
        mock_response = Mock()
        mock_response.versions = [mock_version]
        self.mock_client.FileVersions.return_value = mock_response
        
        result = self.api.file_versions(None, "bucket", "file.txt")
        
        assert len(result) == 1
        assert result[0].root_cid == "cid_v1"


class TestCreateFileUpload:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_create_file_upload_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.api.create_file_upload(None, "", "file.txt")
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_create_file_upload_success(self, mock_pb2):
        mock_response = Mock()
        mock_response.bucket_name = "bucket"
        mock_response.file_name = "file.txt"
        mock_response.stream_id = "stream123"
        mock_response.created_at = Mock()
        mock_response.created_at.ToDatetime = Mock(return_value=datetime.now())
        
        self.mock_client.FileUploadCreate.return_value = mock_response
        
        result = self.api.create_file_upload(None, "bucket", "file.txt")
        
        assert isinstance(result, FileUpload)
        assert result.bucket_name == "bucket"
        assert result.name == "file.txt"
        assert result.stream_id == "stream123"


class TestUpload:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500", block_part_size=1024)
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_upload_empty_file(self):
        upload = FileUpload(bucket_name="b", name="f", stream_id="s1", created_at=datetime.now())
        reader = io.BytesIO(b"")
        
        with pytest.raises(SDKError, match="empty file"):
            self.api.upload(None, upload, reader)
    
    @patch('sdk.sdk_streaming.build_dag')
    @patch.object(StreamingAPI, '_upload_chunk')
    @patch.object(StreamingAPI, '_commit_stream')
    def test_upload_success(self, mock_commit, mock_upload_chunk, mock_build_dag):
        upload = FileUpload(bucket_name="b", name="f.txt", stream_id="s1", created_at=datetime.now())
        reader = io.BytesIO(b"test data")
        
        mock_chunk_dag = Mock()
        mock_chunk_dag.cid = "chunk_cid"
        mock_chunk_dag.raw_data_size = 9
        mock_chunk_dag.proto_node_size = 100
        mock_chunk_dag.blocks = [FileBlockUpload(cid="b1", data=b"test data")]
        mock_build_dag.return_value = mock_chunk_dag
        
        mock_response = Mock()
        mock_response.blocks = [Mock(cid="b1", node_address="node1", node_id="n1", permit="p1")]
        self.mock_client.FileUploadChunkCreate.return_value = mock_response
        
        mock_file_meta = FileMeta(
            stream_id="s1", root_cid="root", bucket_name="b",
            name="f.txt", encoded_size=100, size=9,
            created_at=datetime.now(), committed_at=None
        )
        mock_commit.return_value = mock_file_meta
        
        result = self.api.upload(None, upload, reader)
        
        assert isinstance(result, FileMeta)
        assert mock_upload_chunk.called
        assert mock_commit.called


class TestCreateFileDownload:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_create_file_download_success(self, mock_pb2):
        mock_chunk = Mock()
        mock_chunk.cid = "chunk1"
        mock_chunk.encoded_size = 2048
        mock_chunk.size = 1024
        
        mock_response = Mock()
        mock_response.stream_id = "s1"
        mock_response.bucket_name = "bucket"
        mock_response.chunks = [mock_chunk]
        
        self.mock_client.FileDownloadCreate.return_value = mock_response
        
        result = self.api.create_file_download(None, "bucket", "file.txt")
        
        assert isinstance(result, FileDownload)
        assert result.stream_id == "s1"
        assert len(result.chunks) == 1


class TestCreateRangeFileDownload:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_create_range_download_success(self, mock_pb2):
        mock_chunk = Mock()
        mock_chunk.cid = "chunk_range"
        mock_chunk.encoded_size = 1024
        mock_chunk.size = 512
        mock_chunk.index = 0
        
        mock_response = Mock()
        mock_response.stream_id = "s1"
        mock_response.bucket_name = "bucket"
        mock_response.chunks = [mock_chunk]
        
        self.mock_client.FileDownloadRangeCreate.return_value = mock_response
        
        result = self.api.create_range_file_download(None, "bucket", "file.txt", 5, 10)
        
        assert isinstance(result, FileDownload)
        assert result.chunks[0].index == 5


class TestDownload:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500", max_concurrency=2)
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch.object(StreamingAPI, '_download_chunk_blocks')
    def test_download_success(self, mock_download_blocks):
        chunks = [
            Chunk(cid="c1", encoded_size=100, size=50, index=0),
            Chunk(cid="c2", encoded_size=100, size=50, index=1)
        ]
        file_download = FileDownload(stream_id="s1", bucket_name="b", name="f", chunks=chunks)
        writer = io.BytesIO()
        
        self.api.download(None, file_download, writer)
        
        assert mock_download_blocks.call_count == 2


class TestDownloadRandom:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_download_random_no_erasure_code(self):
        file_download = FileDownload(stream_id="s1", bucket_name="b", name="f", chunks=[])
        writer = io.BytesIO()
        
        with pytest.raises(SDKError, match="erasure coding is not enabled"):
            self.api.download_random(None, file_download, writer)


class TestFileDelete:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_file_delete_success(self, mock_pb2):
        self.mock_client.FileDelete.return_value = Mock()
        
        self.api.file_delete(None, "bucket", "file.txt")
        
        self.mock_client.FileDelete.assert_called_once()


class TestHelperMethods:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    def test_to_file_meta(self):
        mock_meta = Mock()
        mock_meta.stream_id = "s1"
        mock_meta.root_cid = "root"
        mock_meta.name = "file.txt"
        mock_meta.encoded_size = 2048
        mock_meta.size = 1024
        mock_meta.created_at = Mock()
        mock_meta.created_at.seconds = 1234567890
        mock_meta.created_at.ToDatetime = Mock(return_value=datetime.now())
        mock_meta.committed_at = None
        
        result = self.api._to_file_meta(mock_meta, "bucket")
        
        assert isinstance(result, FileMeta)
        assert result.bucket_name == "bucket"
        assert result.name == "file.txt"
    
    def test_create_dag_root(self):
        result = self.api._create_dag_root()
        assert isinstance(result, DAGRoot)
    
    def test_add_dag_link(self):
        dag_root = DAGRoot()
        self.api._add_dag_link(dag_root, "cid", 100, 120)
        assert len(dag_root.links) == 1
    
    def test_build_dag_root(self):
        dag_root = DAGRoot()
        result = self.api._build_dag_root(dag_root)
        assert isinstance(result, str)


class TestChunkUploadCreation:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500", block_part_size=1024)
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch('sdk.sdk_streaming.build_dag')
    @patch('sdk.sdk_streaming.to_proto_chunk')
    def test_create_chunk_upload_no_encryption(self, mock_proto, mock_dag):
        upload = FileUpload(bucket_name="b", name="f", stream_id="s1", created_at=datetime.now())
        
        mock_chunk_dag = Mock()
        mock_chunk_dag.cid = "chunk_cid"
        mock_chunk_dag.raw_data_size = 10
        mock_chunk_dag.proto_node_size = 100
        mock_chunk_dag.blocks = [FileBlockUpload(cid="b1", data=b"data")]
        mock_dag.return_value = mock_chunk_dag
        
        mock_proto.return_value = Mock()
        
        mock_response = Mock()
        mock_response.blocks = [Mock(cid="b1", node_address="n1", node_id="id1", permit="p1")]
        self.mock_client.FileUploadChunkCreate.return_value = mock_response
        
        result = self.api._create_chunk_upload(None, upload, 0, b"", b"test data!")
        
        assert isinstance(result, FileChunkUpload)
        assert result.stream_id == "s1"
        assert result.index == 0


class TestChunkDownloadCreation:
    
    def setup_method(self):
        self.mock_conn = Mock()
        self.mock_client = Mock()
        self.config = SDKConfig(address="test:5500")
        self.api = StreamingAPI(self.mock_conn, self.mock_client, self.config)
    
    @patch('sdk.sdk_streaming.nodeapi_pb2')
    def test_create_chunk_download(self, mock_pb2):
        chunk = Chunk(cid="c1", encoded_size=100, size=50, index=0)
        
        mock_block = Mock()
        mock_block.cid = "b1"
        mock_block.node_id = "n1"
        mock_block.node_address = "addr1"
        mock_block.permit = "p1"
        mock_block.filecoin = Mock()
        mock_block.filecoin.sp_address = "sp1"
        
        mock_response = Mock()
        mock_response.blocks = [mock_block]
        self.mock_client.FileDownloadChunkCreate.return_value = mock_response
        
        result = self.api._create_chunk_download(None, "s1", chunk)
        
        assert isinstance(result, FileChunkDownload)
        assert result.cid == "c1"
        assert len(result.blocks) == 1


@pytest.mark.integration
class TestStreamingIntegration:
    
    @patch('grpc.insecure_channel')
    def test_streaming_api_lifecycle(self, mock_channel):
        mock_conn = Mock()
        mock_client = Mock()
        config = SDKConfig(address="test:5500")
        
        api = StreamingAPI(mock_conn, mock_client, config)
        assert api.client == mock_client

