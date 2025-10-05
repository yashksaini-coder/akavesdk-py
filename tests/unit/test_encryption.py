import pytest
from unittest.mock import Mock, patch
import os

from private.encryption.encryption import (
    derive_key, make_gcm_cipher, encrypt, decrypt, KEY_LENGTH
)


class TestDeriveKey:
    
    def test_derive_key_basic(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b"test_info"
        
        derived = derive_key(key, info)
        
        assert isinstance(derived, bytes)
        assert len(derived) == KEY_LENGTH
    
    def test_derive_key_deterministic(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b"same_info"
        
        derived1 = derive_key(key, info)
        derived2 = derive_key(key, info)
        
        assert derived1 == derived2
    
    def test_derive_key_different_info(self):
        key = b"parent_key_32_bytes_test12345678"
        info1 = b"info_one"
        info2 = b"info_two"
        
        derived1 = derive_key(key, info1)
        derived2 = derive_key(key, info2)
        
        assert derived1 != derived2
    
    def test_derive_key_different_parent(self):
        key1 = b"parent_key_1_32bytes_test1234567"
        key2 = b"parent_key_2_32bytes_test1234567"
        info = b"same_info"
        
        derived1 = derive_key(key1, info)
        derived2 = derive_key(key2, info)
        
        assert derived1 != derived2
    
    def test_derive_key_empty_info(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b""
        
        derived = derive_key(key, info)
        
        assert isinstance(derived, bytes)
        assert len(derived) == KEY_LENGTH


class TestMakeGCMCipher:
    
    def test_make_gcm_cipher_basic(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        info = b"cipher_info"
        
        cipher, nonce = make_gcm_cipher(key, info)
        
        assert cipher is not None
        assert isinstance(nonce, bytes)
        assert len(nonce) == 12
    
    def test_make_gcm_cipher_invalid_key_length_short(self):
        key = b"short_key"
        info = b"info"
        
        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(key, info)
    
    def test_make_gcm_cipher_invalid_key_length_long(self):
        key = b"x" * 64
        info = b"info"
        
        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(key, info)
    
    def test_make_gcm_cipher_nonce_randomness(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        info = b"cipher_info"
        
        cipher1, nonce1 = make_gcm_cipher(key, info)
        cipher2, nonce2 = make_gcm_cipher(key, info)
        
        assert nonce1 != nonce2
    
    def test_make_gcm_cipher_valid_32_byte_key(self):
        key = b"a" * 32
        info = b"test"
        
        cipher, nonce = make_gcm_cipher(key, info)
        
        assert cipher is not None
        assert len(nonce) == 12


class TestEncrypt:
    
    def test_encrypt_basic(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Hello, World!"
        info = b"test_encryption"
        
        encrypted = encrypt(key, data, info)
        
        assert isinstance(encrypted, bytes)
        assert len(encrypted) > len(data)
        assert encrypted != data
    
    def test_encrypt_empty_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b""
        info = b"empty_test"
        
        encrypted = encrypt(key, data, info)
        
        assert isinstance(encrypted, bytes)
        assert len(encrypted) == 12 + 16
    
    def test_encrypt_large_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"x" * 10000
        info = b"large_data"
        
        encrypted = encrypt(key, data, info)
        
        assert len(encrypted) == len(data) + 12 + 16
    
    def test_encrypt_different_each_time(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Same data"
        info = b"test"
        
        encrypted1 = encrypt(key, data, info)
        encrypted2 = encrypt(key, data, info)
        
        assert encrypted1 != encrypted2
    
    def test_encrypt_binary_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = bytes(range(256))
        info = b"binary"
        
        encrypted = encrypt(key, data, info)
        
        assert isinstance(encrypted, bytes)
        assert len(encrypted) == len(data) + 12 + 16


class TestDecrypt:
    
    def test_decrypt_basic(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Hello, World!"
        info = b"test_decryption"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
    
    def test_decrypt_empty_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b""
        info = b"empty"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
    
    def test_decrypt_large_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"y" * 10000
        info = b"large"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
    
    def test_decrypt_wrong_key(self):
        key1 = b"encryption_key_1_32bytes_test123"
        key2 = b"encryption_key_2_32bytes_test123"
        data = b"Secret data"
        info = b"test"
        
        encrypted = encrypt(key1, data, info)
        
        with pytest.raises(Exception):
            decrypt(key2, encrypted, info)
    
    def test_decrypt_wrong_info(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Secret data"
        info1 = b"info_one"
        info2 = b"info_two"
        
        encrypted = encrypt(key, data, info1)
        
        with pytest.raises(Exception):
            decrypt(key, encrypted, info2)
    
    def test_decrypt_corrupted_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Original data"
        info = b"test"
        
        encrypted = encrypt(key, data, info)
        
        corrupted = bytearray(encrypted)
        corrupted[20] ^= 0xFF
        
        with pytest.raises(Exception):
            decrypt(key, bytes(corrupted), info)
    
    def test_decrypt_insufficient_length(self):
        key = b"encryption_key_32bytes_test12345"
        info = b"test"
        invalid_data = b"short"
        
        with pytest.raises(ValueError, match="Invalid encrypted data: insufficient length"):
            decrypt(key, invalid_data, info)
    
    def test_decrypt_exactly_min_length(self):
        key = b"encryption_key_32bytes_test12345"
        info = b"test"
        min_data = b"x" * 28
        
        with pytest.raises(Exception):
            decrypt(key, min_data, info)


class TestEncryptionRoundtrip:
    
    def test_roundtrip_simple(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"Roundtrip test data"
        info = b"roundtrip"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
    
    def test_roundtrip_unicode(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = "Hello 世界! 🌍".encode('utf-8')
        info = b"unicode"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
        assert decrypted.decode('utf-8') == "Hello 世界! 🌍"
    
    def test_roundtrip_multiple_times(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"Test data"
        info = b"multi"
        
        for _ in range(10):
            encrypted = encrypt(key, data, info)
            decrypted = decrypt(key, encrypted, info)
            assert decrypted == data
    
    def test_roundtrip_different_data_sizes(self):
        key = b"roundtrip_key_32bytes_test123456"
        info = b"sizes"
        
        for size in [1, 10, 100, 1000, 5000]:
            data = b"x" * size
            encrypted = encrypt(key, data, info)
            decrypted = decrypt(key, encrypted, info)
            assert decrypted == data
            assert len(decrypted) == size
    
    def test_roundtrip_special_characters(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"\x00\x01\xff\xfe\n\r\t"
        info = b"special"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data


class TestEncryptionEdgeCases:
    
    def test_single_byte_encryption(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"x"
        info = b"single"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
    
    def test_encryption_with_null_bytes(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"\x00" * 100
        info = b"nulls"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
        assert len(decrypted) == 100
    
    def test_encryption_deterministic_with_same_nonce(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"Test data"
        info = b"test"
        
        encrypted1 = encrypt(key, data, info)
        encrypted2 = encrypt(key, data, info)
        
        nonce1 = encrypted1[:12]
        nonce2 = encrypted2[:12]
        
        assert nonce1 != nonce2
    
    def test_max_data_size(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"z" * 100000
        info = b"max"
        
        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)
        
        assert decrypted == data
        assert len(decrypted) == 100000


@pytest.mark.integration
class TestEncryptionIntegration:
    
    def test_full_encryption_workflow(self):
        master_key = b"master_key_32bytes_for_testing12"
        
        file_info = b"bucket/file.txt"
        derived_key = derive_key(master_key, file_info)
        
        original_data = b"Important file content that needs encryption"
        
        encrypted_data = encrypt(derived_key, original_data, b"file_encryption")
        
        decrypted_data = decrypt(derived_key, encrypted_data, b"file_encryption")
        
        assert decrypted_data == original_data
    
    def test_multiple_files_with_different_keys(self):
        master_key = b"master_key_32bytes_for_testing12"
        
        files = [
            (b"bucket1/file1.txt", b"Content of file 1"),
            (b"bucket1/file2.txt", b"Content of file 2"),
            (b"bucket2/file1.txt", b"Content of file 3"),
        ]
        
        encrypted_files = []
        for file_path, content in files:
            file_key = derive_key(master_key, file_path)
            encrypted = encrypt(file_key, content, b"metadata")
            encrypted_files.append((file_path, encrypted))
        
        for (file_path, content), (_, encrypted) in zip(files, encrypted_files):
            file_key = derive_key(master_key, file_path)
            decrypted = decrypt(file_key, encrypted, b"metadata")
            assert decrypted == content

