import pytest
from unittest.mock import Mock, patch
import io

from private.encryption.splitter import Splitter, new_splitter


class TestSplitterInit:
    
    def test_init(self):
        key = b"splitter_key_32bytes_test1234567"
        reader = io.BytesIO(b"test data")
        block_size = 1024
        
        splitter = Splitter(key, reader, block_size)
        
        assert splitter.key == key
        assert splitter.reader == reader
        assert splitter.block_size == block_size
        assert splitter.counter == 0
        assert splitter._eof_reached is False
    
    def test_new_splitter_valid(self):
        key = b"splitter_key_32bytes_test1234567"
        reader = io.BytesIO(b"data")
        block_size = 512
        
        splitter = new_splitter(key, reader, block_size)
        
        assert isinstance(splitter, Splitter)
        assert splitter.block_size == 512
    
    def test_new_splitter_empty_key(self):
        key = b""
        reader = io.BytesIO(b"data")
        
        with pytest.raises(ValueError, match="encryption key cannot be empty"):
            new_splitter(key, reader, 1024)
    
    def test_new_splitter_wrong_key_length(self):
        key = b"short_key"
        reader = io.BytesIO(b"data")
        
        with pytest.raises(ValueError, match="encryption key must be 32 bytes long"):
            new_splitter(key, reader, 1024)
    
    def test_new_splitter_exact_key_length(self):
        key = b"x" * 32
        reader = io.BytesIO(b"data")
        
        splitter = new_splitter(key, reader, 1024)
        
        assert isinstance(splitter, Splitter)


class TestNextBytes:
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_single_block(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"small data"
        reader = io.BytesIO(data)
        block_size = 1024
        
        mock_encrypt.return_value = b"encrypted_data"
        
        splitter = Splitter(key, reader, block_size)
        result = splitter.next_bytes()
        
        assert result == b"encrypted_data"
        assert splitter.counter == 1
        mock_encrypt.assert_called_once_with(key, data, b"block_0")
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_multiple_blocks(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"x" * 100
        reader = io.BytesIO(data)
        block_size = 30
        
        mock_encrypt.side_effect = [b"enc1", b"enc2", b"enc3", b"enc4"]
        
        splitter = Splitter(key, reader, block_size)
        
        result1 = splitter.next_bytes()
        assert result1 == b"enc1"
        assert splitter.counter == 1
        
        result2 = splitter.next_bytes()
        assert result2 == b"enc2"
        assert splitter.counter == 2
        
        result3 = splitter.next_bytes()
        assert result3 == b"enc3"
        assert splitter.counter == 3
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_eof(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"")
        block_size = 1024
        
        splitter = Splitter(key, reader, block_size)
        result = splitter.next_bytes()
        
        assert result is None
        assert splitter._eof_reached is True
        mock_encrypt.assert_not_called()
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_after_eof(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"data"
        reader = io.BytesIO(data)
        block_size = 1024
        
        mock_encrypt.return_value = b"encrypted"
        
        splitter = Splitter(key, reader, block_size)
        
        result1 = splitter.next_bytes()
        assert result1 == b"encrypted"
        
        result2 = splitter.next_bytes()
        assert result2 is None
        
        result3 = splitter.next_bytes()
        assert result3 is None
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_exact_block_size(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"x" * 50
        reader = io.BytesIO(data)
        block_size = 50
        
        mock_encrypt.return_value = b"encrypted_50"
        
        splitter = Splitter(key, reader, block_size)
        result = splitter.next_bytes()
        
        assert result == b"encrypted_50"
        mock_encrypt.assert_called_once_with(key, data, b"block_0")
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_counter_increment(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"a" * 100)
        block_size = 20
        
        mock_encrypt.return_value = b"enc"
        
        splitter = Splitter(key, reader, block_size)
        
        for i in range(5):
            result = splitter.next_bytes()
            assert result == b"enc"
            assert splitter.counter == i + 1
    
    @patch('private.encryption.splitter.encrypt')
    def test_next_bytes_encryption_error(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"data")
        block_size = 1024
        
        mock_encrypt.side_effect = Exception("Encryption failed")
        
        splitter = Splitter(key, reader, block_size)
        
        with pytest.raises(Exception, match="splitter error"):
            splitter.next_bytes()


class TestIterator:
    
    @patch('private.encryption.splitter.encrypt')
    def test_iterator_basic(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"x" * 60
        reader = io.BytesIO(data)
        block_size = 20
        
        mock_encrypt.side_effect = [b"enc1", b"enc2", b"enc3"]
        
        splitter = Splitter(key, reader, block_size)
        
        results = list(splitter)
        
        assert len(results) == 3
        assert results == [b"enc1", b"enc2", b"enc3"]
    
    @patch('private.encryption.splitter.encrypt')
    def test_iterator_empty_data(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"")
        block_size = 1024
        
        splitter = Splitter(key, reader, block_size)
        
        results = list(splitter)
        
        assert len(results) == 0
    
    @patch('private.encryption.splitter.encrypt')
    def test_iterator_single_block(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"small")
        block_size = 1024
        
        mock_encrypt.return_value = b"encrypted_small"
        
        splitter = Splitter(key, reader, block_size)
        
        results = list(splitter)
        
        assert len(results) == 1
        assert results[0] == b"encrypted_small"
    
    @patch('private.encryption.splitter.encrypt')
    def test_iterator_for_loop(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"y" * 100)
        block_size = 25
        
        mock_encrypt.side_effect = [b"e1", b"e2", b"e3", b"e4"]
        
        splitter = Splitter(key, reader, block_size)
        
        count = 0
        for chunk in splitter:
            assert chunk in [b"e1", b"e2", b"e3", b"e4"]
            count += 1
        
        assert count == 4


class TestReset:
    
    @patch('private.encryption.splitter.encrypt')
    def test_reset_with_new_reader(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader1 = io.BytesIO(b"data1")
        reader2 = io.BytesIO(b"data2")
        block_size = 1024
        
        mock_encrypt.side_effect = [b"enc1", b"enc2"]
        
        splitter = Splitter(key, reader1, block_size)
        
        result1 = splitter.next_bytes()
        assert result1 == b"enc1"
        assert splitter.counter == 1
        
        splitter.reset(reader2)
        
        assert splitter.counter == 0
        assert splitter._eof_reached is False
        assert splitter.reader == reader2
        
        result2 = splitter.next_bytes()
        assert result2 == b"enc2"
        assert splitter.counter == 1
    
    @patch('private.encryption.splitter.encrypt')
    def test_reset_without_new_reader(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = b"reusable data"
        reader = io.BytesIO(data)
        block_size = 1024
        
        mock_encrypt.side_effect = [b"enc1", b"enc2"]
        
        splitter = Splitter(key, reader, block_size)
        
        result1 = splitter.next_bytes()
        assert result1 == b"enc1"
        assert splitter.counter == 1
        
        splitter.reset()
        
        assert splitter.counter == 0
        assert splitter._eof_reached is False
        
        result2 = splitter.next_bytes()
        assert result2 == b"enc2"
    
    @patch('private.encryption.splitter.encrypt')
    def test_reset_after_eof(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"data")
        block_size = 1024
        
        mock_encrypt.side_effect = [b"enc1", b"enc2"]
        
        splitter = Splitter(key, reader, block_size)
        
        splitter.next_bytes()
        splitter.next_bytes()
        assert splitter._eof_reached is True
        
        splitter.reset()
        
        assert splitter._eof_reached is False
        assert splitter.counter == 0
        
        result = splitter.next_bytes()
        assert result == b"enc2"
    
    def test_reset_non_seekable_reader(self):
        key = b"test_key_32bytes_for_splitting12"
        
        class NonSeekableReader:
            def __init__(self, data):
                self.data = data
                self.pos = 0
            
            def read(self, size):
                result = self.data[self.pos:self.pos + size]
                self.pos += len(result)
                return result
        
        reader = NonSeekableReader(b"test data")
        splitter = Splitter(key, reader, 1024)
        
        splitter.counter = 5
        splitter._eof_reached = True
        
        splitter.reset()
        
        assert splitter.counter == 0
        assert splitter._eof_reached is False


class TestSplitterEdgeCases:
    
    @patch('private.encryption.splitter.encrypt')
    def test_single_byte_data(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"x")
        block_size = 1024
        
        mock_encrypt.return_value = b"encrypted_x"
        
        splitter = Splitter(key, reader, block_size)
        result = splitter.next_bytes()
        
        assert result == b"encrypted_x"
        mock_encrypt.assert_called_once_with(key, b"x", b"block_0")
    
    @patch('private.encryption.splitter.encrypt')
    def test_very_small_block_size(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"hello")
        block_size = 1
        
        mock_encrypt.side_effect = [b"e1", b"e2", b"e3", b"e4", b"e5"]
        
        splitter = Splitter(key, reader, block_size)
        
        results = list(splitter)
        
        assert len(results) == 5
    
    @patch('private.encryption.splitter.encrypt')
    def test_large_block_size(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        reader = io.BytesIO(b"small data")
        block_size = 1024 * 1024
        
        mock_encrypt.return_value = b"encrypted_all"
        
        splitter = Splitter(key, reader, block_size)
        result = splitter.next_bytes()
        
        assert result == b"encrypted_all"
        assert splitter.counter == 1
    
    @patch('private.encryption.splitter.encrypt')
    def test_binary_data_splitting(self, mock_encrypt):
        key = b"test_key_32bytes_for_splitting12"
        data = bytes(range(256))
        reader = io.BytesIO(data)
        block_size = 100
        
        mock_encrypt.side_effect = [b"enc1", b"enc2", b"enc3"]
        
        splitter = Splitter(key, reader, block_size)
        
        results = list(splitter)
        
        assert len(results) == 3


@pytest.mark.integration
class TestSplitterIntegration:
    
    @patch('private.encryption.splitter.encrypt')
    def test_full_splitting_workflow(self, mock_encrypt):
        key = b"integration_key_32bytes_test1234"
        data = b"This is a complete file content that needs to be split into chunks"
        reader = io.BytesIO(data)
        block_size = 20
        
        encrypted_chunks = [f"enc_{i}".encode() for i in range(4)]
        mock_encrypt.side_effect = encrypted_chunks
        
        splitter = new_splitter(key, reader, block_size)
        
        chunks = list(splitter)
        
        assert len(chunks) == 4
        assert all(chunk in encrypted_chunks for chunk in chunks)
    
    @patch('private.encryption.splitter.encrypt')
    def test_multiple_iterations(self, mock_encrypt):
        key = b"integration_key_32bytes_test1234"
        data = b"Reusable data"
        
        mock_encrypt.side_effect = [b"enc1", b"enc2", b"enc3", b"enc4"]
        
        reader1 = io.BytesIO(data)
        splitter = new_splitter(key, reader1, 10)
        
        chunks1 = list(splitter)
        assert len(chunks1) == 2
        
        reader2 = io.BytesIO(data)
        splitter.reset(reader2)
        
        chunks2 = list(splitter)
        assert len(chunks2) == 2
    
    def test_real_encryption_integration(self):
        from private.encryption.encryption import encrypt as real_encrypt
        
        key = b"real_encryption_key_32bytes_test"
        data = b"Real data to split and encrypt"
        reader = io.BytesIO(data)
        block_size = 10
        
        splitter = new_splitter(key, reader, block_size)
        
        encrypted_chunks = list(splitter)
        
        assert len(encrypted_chunks) > 0
        for chunk in encrypted_chunks:
            assert isinstance(chunk, bytes)
            assert len(chunk) > 0

