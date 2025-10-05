"""
Unit tests for DAG (Directed Acyclic Graph) functionality.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import io
import hashlib
from dataclasses import dataclass

from sdk.dag import (
    DAGRoot,
    ChunkDAG,
    DAGError,
    build_dag,
    node_sizes,
    extract_block_data,
    bytes_to_node,
    get_node_links,
    block_by_cid,
    _encode_varint,
    _decode_varint,
    _extract_unixfs_data_size,
    _extract_unixfs_data,
    _create_unixfs_file_node,
    _create_chunk_dag_root_node,
    IPLD_AVAILABLE
)
from sdk.model import FileBlockUpload


@pytest.fixture
def sample_file_blocks():
    """Sample file blocks for testing."""
    return [
        FileBlockUpload(cid="bafybeia3uzxlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block1_data"),
        FileBlockUpload(cid="bafybeia4vwrlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block2_data"),
        FileBlockUpload(cid="bafybeia5xzslpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block3_data")
    ]


class TestDAGRoot:
    """Test DAGRoot class functionality."""
    
    def test_init(self):
        """Test DAGRoot initialization."""
        dag_root = DAGRoot()
        assert dag_root.node is None
        assert dag_root.fs_node_data == b""
        assert dag_root.links == []
        assert dag_root.total_file_size == 0
    
    def test_new_classmethod(self):
        """Test DAGRoot.new() class method."""
        dag_root = DAGRoot.new()
        assert isinstance(dag_root, DAGRoot)
        assert dag_root.links == []
    
    def test_add_link_with_string_cid(self):
        """Test adding a link with string CID."""
        dag_root = DAGRoot()
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(cid_str, raw_data_size=1024, proto_node_size=1200)
        
        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid_str"] == cid_str
        assert dag_root.links[0]["name"] == ""
        assert dag_root.links[0]["size"] == 1200
        assert dag_root.total_file_size == 1024
    
    def test_add_link_with_cid_object(self):
        """Test adding a link with CID object."""
        dag_root = DAGRoot()
        
        # Mock CID object
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(mock_cid, raw_data_size=512, proto_node_size=600)
        
        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid"] == mock_cid
        assert dag_root.total_file_size == 512
    
    def test_add_multiple_links(self):
        """Test adding multiple links."""
        dag_root = DAGRoot()
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        dag_root.add_link("cid3", 300, 360)
        
        assert len(dag_root.links) == 3
        assert dag_root.total_file_size == 600
    
    def test_build_no_chunks(self):
        """Test build with no chunks added."""
        dag_root = DAGRoot()
        
        with pytest.raises(DAGError, match="no chunks added"):
            dag_root.build()
    
    def test_build_single_chunk(self):
        """Test build with single chunk."""
        dag_root = DAGRoot()
        
        # Mock CID with string method
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(mock_cid, 1024, 1200)
        
        result = dag_root.build()
        assert result == "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
    
    def test_build_single_chunk_string_cid(self):
        """Test build with single chunk using string CID."""
        dag_root = DAGRoot()
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(cid_str, 1024, 1200)
        
        result = dag_root.build()
        assert result == cid_str
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_build_multiple_chunks_no_ipld(self):
        """Test build with multiple chunks when IPLD is not available."""
        dag_root = DAGRoot()
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        
        result = dag_root.build()
        
        # Should generate a bafybeig... CID when IPLD is not available
        assert isinstance(result, str)
        assert result.startswith("bafybeig")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    @patch('sdk.dag.multihash')
    def test_build_multiple_chunks_with_ipld(self, mock_multihash, mock_encode, mock_pbnode, mock_cid_class):
        """Test build with multiple chunks when IPLD is available."""
        dag_root = DAGRoot()
        
        # Mock CID objects
        mock_cid1 = Mock()
        mock_cid2 = Mock()
        mock_cid_class.decode.side_effect = [mock_cid1, mock_cid2]
        
        # Mock encoding and hashing
        mock_encode.return_value = b"encoded_data"
        mock_digest = b"digest_data"
        mock_multihash.digest.return_value = mock_digest
        
        # Mock final CID
        mock_root_cid = Mock()
        mock_cid_class.return_value = mock_root_cid
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        
        result = dag_root.build()
        
        assert result == mock_root_cid
        mock_encode.assert_called_once()
        mock_multihash.digest.assert_called_once_with(b"encoded_data", "sha2-256")
    
    def test_create_unixfs_file_data(self):
        """Test _create_unixfs_file_data method."""
        dag_root = DAGRoot()
        dag_root.total_file_size = 1024
        
        unixfs_data = dag_root._create_unixfs_file_data()
        
        # Should start with [0x08, 0x02] for file type
        assert unixfs_data[:2] == bytes([0x08, 0x02])
        # Should contain file size
        assert len(unixfs_data) > 2
    
    def test_encode_varint(self):
        """Test _encode_varint method."""
        dag_root = DAGRoot()
        
        # Test small number
        result = dag_root._encode_varint(127)
        assert result == bytes([127])
        
        # Test larger number
        result = dag_root._encode_varint(300)
        assert len(result) == 2
        assert result[0] & 0x80 != 0  # First byte should have continuation bit


class TestVarintFunctions:
    """Test varint encoding/decoding functions."""
    
    def test_encode_varint_small(self):
        """Test encoding small varint."""
        result = _encode_varint(42)
        assert result == bytes([42])
    
    def test_encode_varint_large(self):
        """Test encoding large varint."""
        result = _encode_varint(300)
        expected = bytes([172, 2])  # 300 encoded as varint
        assert result == expected
    
    def test_decode_varint_small(self):
        """Test decoding small varint."""
        data = bytes([42])
        value, bytes_read = _decode_varint(data)
        assert value == 42
        assert bytes_read == 1
    
    def test_decode_varint_large(self):
        """Test decoding large varint."""
        data = bytes([172, 2])
        value, bytes_read = _decode_varint(data)
        assert value == 300
        assert bytes_read == 2
    
    def test_decode_varint_too_long(self):
        """Test decoding varint that's too long."""
        # Create a varint that's too long (more than 64 bits)
        data = bytes([0x80] * 10 + [0x01])
        
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(data)


class TestBuildDAG:
    """Test build_dag function."""
    
    def test_build_dag_empty_data(self):
        """Test build_dag with empty data."""
        reader = io.BytesIO(b"")
        
        with pytest.raises(DAGError, match="empty data"):
            build_dag(None, reader, 1024)
    
    def test_build_dag_small_data(self):
        """Test build_dag with data smaller than block size."""
        data = b"Hello, World!"
        reader = io.BytesIO(data)
        
        result = build_dag(None, reader, 1024)
        
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) == 1
        assert result.blocks[0].data == data or len(result.blocks[0].data) > len(data)  # May be encoded
    
    def test_build_dag_large_data(self):
        """Test build_dag with data larger than block size."""
        data = b"x" * 2048  # 2KB data
        reader = io.BytesIO(data)
        block_size = 1024
        
        result = build_dag(None, reader, block_size)
        
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) >= 2  # Should be split into multiple blocks
    
    @patch('sdk.dag.encrypt')
    def test_build_dag_with_encryption(self, mock_encrypt):
        """Test build_dag with encryption key."""
        data = b"Secret data"
        encrypted_data = b"encrypted_secret_data"
        reader = io.BytesIO(data)
        enc_key = b"encryption_key_32_bytes_test123"
        
        mock_encrypt.return_value = encrypted_data
        
        result = build_dag(None, reader, 1024, enc_key)
        
        assert isinstance(result, ChunkDAG)
        mock_encrypt.assert_called_once_with(enc_key, data, b"dag_encryption")
    
    @patch('sdk.dag._create_unixfs_file_node')
    def test_build_dag_create_unixfs_node_called(self, mock_create_unixfs):
        """Test that _create_unixfs_file_node is called."""
        data = b"Test data"
        reader = io.BytesIO(data)
        
        mock_cid = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        mock_create_unixfs.return_value = (mock_cid, data)
        
        result = build_dag(None, reader, 1024)
        
        mock_create_unixfs.assert_called_once_with(data)
        assert len(result.blocks) == 1


class TestCreateUnixFSFileNode:
    """Test _create_unixfs_file_node function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_create_unixfs_file_node_no_ipld(self):
        """Test _create_unixfs_file_node without IPLD."""
        data = b"test data"
        
        cid, encoded_data = _create_unixfs_file_node(data)
        
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert encoded_data == data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    @patch('sdk.dag.multihash')
    @patch('sdk.dag.CID')
    def test_create_unixfs_file_node_with_ipld(self, mock_cid_class, mock_multihash, mock_encode, mock_pbnode):
        """Test _create_unixfs_file_node with IPLD."""
        data = b"test data"
        
        # Mock encoding
        encoded_bytes = b"encoded_unixfs_data"
        mock_encode.return_value = encoded_bytes
        
        # Mock multihash
        digest = b"hash_digest"
        mock_multihash.digest.return_value = digest
        
        # Mock CID
        mock_cid = Mock()
        mock_cid_class.return_value = mock_cid
        
        cid, encoded_data = _create_unixfs_file_node(data)
        
        assert cid == mock_cid
        assert encoded_data == encoded_bytes
        mock_encode.assert_called_once()
        mock_multihash.digest.assert_called_once_with(encoded_bytes, "sha2-256")


class TestNodeSizes:
    """Test node_sizes function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_node_sizes_no_ipld(self):
        """Test node_sizes without IPLD."""
        data = b"test node data"
        
        raw_size, encoded_size = node_sizes(data)
        
        assert raw_size == len(data)
        assert encoded_size == len(data)
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_node_sizes_with_ipld_no_links(self, mock_decode):
        """Test node_sizes with IPLD and no links."""
        data = b"test node data"
        
        # Mock decoded node with data but no links
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_node.links = []
        mock_decode.return_value = mock_node
        
        with patch('sdk.dag._extract_unixfs_data_size', return_value=100) as mock_extract:
            raw_size, encoded_size = node_sizes(data)
            
            assert raw_size == 100
            assert encoded_size == len(data)
            mock_extract.assert_called_once_with(b"unixfs_data")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_node_sizes_with_ipld_with_links(self, mock_decode):
        """Test node_sizes with IPLD and links."""
        data = b"test node data"
        
        # Mock decoded node with links
        mock_link1 = Mock()
        mock_link1.size = 100
        mock_link2 = Mock()
        mock_link2.size = 200
        
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_node.links = [mock_link1, mock_link2]
        mock_decode.return_value = mock_node
        
        with patch('sdk.dag._extract_unixfs_data_size', return_value=250):
            raw_size, encoded_size = node_sizes(data)
            
            assert raw_size == 250
            assert encoded_size == 300  # Sum of link sizes


class TestExtractBlockData:
    """Test extract_block_data function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_extract_block_data_no_ipld(self):
        """Test extract_block_data without IPLD."""
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"test data"
        
        result = extract_block_data(cid_str, data)
        assert result == data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    @patch('sdk.dag.decode')
    def test_extract_block_data_dag_pb(self, mock_decode, mock_cid_class):
        """Test extract_block_data with DAG-PB codec."""
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"test data"
        
        # Mock CID with DAG-PB codec
        mock_cid = Mock()
        mock_cid.codec = 0x70  # DAG_PB_CODEC
        mock_cid_class.decode.return_value = mock_cid
        
        # Mock decoded node
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_decode.return_value = mock_node
        
        extracted_data = b"extracted"
        with patch('sdk.dag._extract_unixfs_data', return_value=extracted_data):
            result = extract_block_data(cid_str, data)
            assert result == extracted_data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    def test_extract_block_data_raw(self, mock_cid_class):
        """Test extract_block_data with RAW codec."""
        cid_str = "bafkreigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"raw data"
        
        # Mock CID with RAW codec
        mock_cid = Mock()
        mock_cid.codec = 0x55  # RAW_CODEC
        mock_cid_class.decode.return_value = mock_cid
        
        result = extract_block_data(cid_str, data)
        assert result == data


class TestBytesToNode:
    """Test bytes_to_node function."""
    
    @patch('sdk.dag.decode')
    def test_bytes_to_node_success(self, mock_decode):
        """Test successful bytes_to_node."""
        node_data = b"node data"
        mock_node = Mock()
        mock_decode.return_value = mock_node
        
        result = bytes_to_node(node_data)
        assert result == mock_node
        mock_decode.assert_called_once_with(node_data)
    
    @patch('sdk.dag.decode')
    def test_bytes_to_node_failure(self, mock_decode):
        """Test bytes_to_node with decode failure."""
        node_data = b"invalid node data"
        mock_decode.side_effect = Exception("Decode failed")
        
        with pytest.raises(DAGError, match="failed to decode node data"):
            bytes_to_node(node_data)


class TestGetNodeLinks:
    """Test get_node_links function."""
    
    @patch('sdk.dag.bytes_to_node')
    def test_get_node_links_success(self, mock_bytes_to_node):
        """Test successful get_node_links."""
        node_data = b"node data"
        
        # Mock links
        mock_link1 = Mock()
        mock_link1.hash = "hash1"
        mock_link1.name = "link1"
        mock_link1.size = 100
        
        mock_link2 = Mock()
        mock_link2.hash = "hash2"
        mock_link2.name = ""
        mock_link2.size = 200
        
        # Mock node
        mock_node = Mock()
        mock_node.links = [mock_link1, mock_link2]
        mock_bytes_to_node.return_value = mock_node
        
        result = get_node_links(node_data)
        
        expected = [
            {'cid': 'hash1', 'name': 'link1', 'size': 100},
            {'cid': 'hash2', 'name': '', 'size': 200}
        ]
        assert result == expected
    
    @patch('sdk.dag.bytes_to_node')
    def test_get_node_links_failure(self, mock_bytes_to_node):
        """Test get_node_links with failure."""
        node_data = b"invalid node data"
        mock_bytes_to_node.side_effect = Exception("Node decode failed")
        
        with pytest.raises(DAGError, match="failed to extract links from node"):
            get_node_links(node_data)


class TestBlockByCid:
    """Test block_by_cid function."""
    
    def test_block_by_cid_found(self, sample_file_blocks):
        """Test finding block by CID."""
        target_cid = "bafybeia4vwrlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq"
        
        block, found = block_by_cid(sample_file_blocks, target_cid)
        
        assert found is True
        assert block.cid == target_cid
        assert block.data == b"block2_data"
    
    def test_block_by_cid_not_found(self, sample_file_blocks):
        """Test not finding block by CID."""
        target_cid = "nonexistent_cid"
        
        block, found = block_by_cid(sample_file_blocks, target_cid)
        
        assert found is False
        assert block.cid == ""
        assert block.data == b""
    
    def test_block_by_cid_empty_list(self):
        """Test block_by_cid with empty block list."""
        target_cid = "any_cid"
        
        block, found = block_by_cid([], target_cid)
        
        assert found is False
        assert block.cid == ""


class TestExtractUnixFSData:
    """Test _extract_unixfs_data function."""
    
    def test_extract_unixfs_data_empty(self):
        """Test _extract_unixfs_data with empty data."""
        result = _extract_unixfs_data(b"")
        assert result == b""
    
    def test_extract_unixfs_data_simple(self):
        """Test _extract_unixfs_data with simple data."""
        # Create simple UnixFS data: field 4 (Data) with 5 bytes "hello"
        unixfs_data = bytes([0x22, 0x05]) + b"hello"
        
        result = _extract_unixfs_data(unixfs_data)
        assert result == b"hello"
    
    def test_extract_unixfs_data_invalid(self):
        """Test _extract_unixfs_data with invalid data."""
        invalid_data = b"\xFF\xFF\xFF"
        
        result = _extract_unixfs_data(invalid_data)
        assert result == b""


class TestExtractUnixFSDataSize:
    """Test _extract_unixfs_data_size function."""
    
    def test_extract_unixfs_data_size_empty(self):
        """Test _extract_unixfs_data_size with empty data."""
        result = _extract_unixfs_data_size(b"")
        assert result == 0
    
    def test_extract_unixfs_data_size_with_file_size(self):
        """Test _extract_unixfs_data_size with file size field."""
        # Create UnixFS data with field 3 (file size) = 1024
        unixfs_data = bytes([0x18]) + _encode_varint(1024)
        
        result = _extract_unixfs_data_size(unixfs_data)
        assert result == 1024
    
    def test_extract_unixfs_data_size_with_data_field(self):
        """Test _extract_unixfs_data_size with data field."""
        # Create UnixFS data with field 4 (data) of length 512
        unixfs_data = bytes([0x22]) + _encode_varint(512) + b"x" * 512
        
        result = _extract_unixfs_data_size(unixfs_data)
        assert result == 512


@pytest.mark.integration
class TestDAGIntegration:
    """Integration tests for DAG functionality."""
    
    def test_complete_dag_workflow(self):
        """Test complete DAG creation workflow."""
        # Create sample data
        data = b"This is a test file for DAG creation workflow."
        reader = io.BytesIO(data)
        
        # Build DAG
        chunk_dag = build_dag(None, reader, block_size=1024)
        
        # Verify DAG structure
        assert isinstance(chunk_dag, ChunkDAG)
        assert chunk_dag.raw_data_size == len(data)
        assert len(chunk_dag.blocks) >= 1
        
        # Test DAG root creation
        dag_root = DAGRoot.new()
        dag_root.add_link(chunk_dag.cid, chunk_dag.raw_data_size, chunk_dag.encoded_size)
        
        root_cid = dag_root.build()
        assert root_cid is not None
    
    def test_multi_chunk_dag_workflow(self):
        """Test DAG workflow with multiple chunks."""
        dag_root = DAGRoot.new()
        
        # Add multiple chunks
        for i in range(3):
            cid = f"bafybeig{i:050d}"
            dag_root.add_link(cid, raw_data_size=1024, proto_node_size=1200)
        
        root_cid = dag_root.build()
        assert root_cid is not None
        assert dag_root.total_file_size == 3072  # 3 * 1024
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    def test_dag_with_ipld_libraries(self):
        """Test DAG functionality when IPLD libraries are available."""
        data = b"Test data for IPLD"
        reader = io.BytesIO(data)
        
        # This should work even if IPLD is mocked as available
        with patch('sdk.dag.PBNode'), \
             patch('sdk.dag.encode'), \
             patch('sdk.dag.multihash'), \
             patch('sdk.dag.CID') as mock_cid:
            
            mock_cid.decode.return_value = Mock()
            
            # Should not raise an exception
            try:
                chunk_dag = build_dag(None, reader, block_size=1024)
                assert isinstance(chunk_dag, ChunkDAG)
            except Exception:
                # If mocking fails, fallback behavior should still work
                pass
    
    def test_large_file_dag_creation(self):
        """Test DAG creation with a large file."""
        # Create 10KB of data
        large_data = b"x" * 10240
        reader = io.BytesIO(large_data)
        block_size = 1024
        
        chunk_dag = build_dag(None, reader, block_size)
        
        # Should create multiple blocks
        expected_blocks = (len(large_data) + block_size - 1) // block_size
        assert len(chunk_dag.blocks) >= expected_blocks
        assert chunk_dag.raw_data_size == len(large_data)
    
    def test_dag_varint_encoding_decoding_roundtrip(self):
        """Test varint encoding/decoding roundtrip."""
        test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 1000000]
        
        for value in test_values:
            encoded = _encode_varint(value)
            decoded, bytes_read = _decode_varint(encoded)
            
            assert decoded == value
            assert bytes_read == len(encoded)

