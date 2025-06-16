import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import tempfile
import json

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.common import SDKError


class TestConfigLoading(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_config = {
            'address': 'localhost:5000',
            'max_concurrency': 10,
            'block_part_size': 1024,
            'use_connection_pool': True,
            'private_key': '0x1234567890abcdef',
            'encryption_key': 'a' * 64,  # hex encoded 32 bytes
            'streaming_max_blocks_in_chunk': 32,
            'parity_blocks_count': 0
        }
    
    def test_load_config_from_file_success(self):
        """Test successful configuration loading from file."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.test_config, f)
            config_file = f.name
        
        try:
            # Test loading config (this would be implemented in actual config module)
            with open(config_file, 'r') as f:
                loaded_config = json.load(f)
            
            # Assert
            self.assertEqual(loaded_config['address'], 'localhost:5000')
            self.assertEqual(loaded_config['max_concurrency'], 10)
            self.assertEqual(loaded_config['block_part_size'], 1024)
            self.assertTrue(loaded_config['use_connection_pool'])
        finally:
            # Clean up
            os.unlink(config_file)
    
    def test_load_config_from_env_variables(self):
        """Test configuration loading from environment variables."""
        env_vars = {
            'AKAVE_ADDRESS': 'env-localhost:5000',
            'AKAVE_MAX_CONCURRENCY': '20',
            'AKAVE_BLOCK_PART_SIZE': '2048',
            'AKAVE_USE_CONNECTION_POOL': 'true',
            'AKAVE_PRIVATE_KEY': '0xenv1234567890abcdef',
            'AKAVE_ENCRYPTION_KEY': 'b' * 64
        }
        
        with patch.dict(os.environ, env_vars):
            # Test loading from environment
            loaded_config = {
                'address': os.getenv('AKAVE_ADDRESS'),
                'max_concurrency': int(os.getenv('AKAVE_MAX_CONCURRENCY', 10)),
                'block_part_size': int(os.getenv('AKAVE_BLOCK_PART_SIZE', 1024)),
                'use_connection_pool': os.getenv('AKAVE_USE_CONNECTION_POOL', 'false').lower() == 'true',
                'private_key': os.getenv('AKAVE_PRIVATE_KEY'),
                'encryption_key': os.getenv('AKAVE_ENCRYPTION_KEY')
            }
            
            # Assert
            self.assertEqual(loaded_config['address'], 'env-localhost:5000')
            self.assertEqual(loaded_config['max_concurrency'], 20)
            self.assertEqual(loaded_config['block_part_size'], 2048)
            self.assertTrue(loaded_config['use_connection_pool'])
            self.assertEqual(loaded_config['private_key'], '0xenv1234567890abcdef')
    
    def test_config_validation_success(self):
        """Test successful configuration validation."""
        # Test valid configuration
        valid_config = {
            'address': 'localhost:5000',
            'max_concurrency': 10,
            'block_part_size': 1024,
            'use_connection_pool': True,
            'encryption_key': b'a' * 32,  # 32 bytes
            'streaming_max_blocks_in_chunk': 32,
            'parity_blocks_count': 4
        }
        
        # Validate configuration (this would be implemented in actual config module)
        self.assertTrue(self._validate_config(valid_config))
    
    def test_config_validation_invalid_block_size(self):
        """Test configuration validation with invalid block size."""
        invalid_config = self.test_config.copy()
        invalid_config['block_part_size'] = 0  # Invalid
        
        with self.assertRaises(ValueError):
            self._validate_config_strict(invalid_config)
    
    def test_config_validation_invalid_encryption_key(self):
        """Test configuration validation with invalid encryption key."""
        invalid_config = self.test_config.copy()
        invalid_config['encryption_key'] = b'short'  # Too short
        
        with self.assertRaises(ValueError):
            self._validate_config_strict(invalid_config)
    
    def test_config_validation_invalid_parity_blocks(self):
        """Test configuration validation with invalid parity blocks count."""
        invalid_config = self.test_config.copy()
        invalid_config['parity_blocks_count'] = 100  # Too high
        invalid_config['streaming_max_blocks_in_chunk'] = 32
        
        with self.assertRaises(ValueError):
            self._validate_config_strict(invalid_config)
    
    def test_config_merge_defaults(self):
        """Test merging configuration with defaults."""
        partial_config = {
            'address': 'localhost:5000',
            'private_key': '0x1234567890abcdef'
        }
        
        defaults = {
            'max_concurrency': 10,
            'block_part_size': 1024,
            'use_connection_pool': True,
            'streaming_max_blocks_in_chunk': 32,
            'parity_blocks_count': 0
        }
        
        # Merge with defaults
        merged_config = {**defaults, **partial_config}
        
        # Assert
        self.assertEqual(merged_config['address'], 'localhost:5000')
        self.assertEqual(merged_config['max_concurrency'], 10)  # From defaults
        self.assertEqual(merged_config['private_key'], '0x1234567890abcdef')
    
    def test_config_file_not_found(self):
        """Test handling of missing configuration file."""
        non_existent_file = '/path/that/does/not/exist/config.json'
        
        # Test file not found handling
        with self.assertRaises(FileNotFoundError):
            with open(non_existent_file, 'r') as f:
                json.load(f)
    
    def test_config_invalid_json(self):
        """Test handling of invalid JSON in configuration file."""
        # Create a temporary file with invalid JSON
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{ invalid json content }')
            config_file = f.name
        
        try:
            # Test invalid JSON handling
            with self.assertRaises(json.JSONDecodeError):
                with open(config_file, 'r') as f:
                    json.load(f)
        finally:
            # Clean up
            os.unlink(config_file)
    
    def test_encryption_key_hex_decoding(self):
        """Test hex decoding of encryption key."""
        hex_key = 'a' * 64  # 64 hex characters = 32 bytes
        
        # Decode hex key
        decoded_key = bytes.fromhex(hex_key)
        
        # Assert
        self.assertEqual(len(decoded_key), 32)
        # 'aa' in hex = 170 in decimal = b'\xaa'
        self.assertEqual(decoded_key, b'\xaa' * 32)
    
    def test_encryption_key_invalid_hex(self):
        """Test handling of invalid hex encryption key."""
        invalid_hex_key = 'invalid_hex_string'
        
        # Test invalid hex handling
        with self.assertRaises(ValueError):
            bytes.fromhex(invalid_hex_key)
    
    def _validate_config(self, config):
        """Helper method to validate configuration (basic validation)."""
        required_fields = ['address']
        for field in required_fields:
            if field not in config:
                return False
        return True
    
    def _validate_config_strict(self, config):
        """Helper method to validate configuration with strict rules."""
        # Validate block part size
        if 'block_part_size' in config and config['block_part_size'] <= 0:
            raise ValueError("Invalid block_part_size")
        
        # Validate encryption key length
        if 'encryption_key' in config:
            key = config['encryption_key']
            if isinstance(key, bytes) and len(key) != 32:
                raise ValueError("Invalid encryption key length")
        
        # Validate parity blocks count
        if ('parity_blocks_count' in config and 
            'streaming_max_blocks_in_chunk' in config):
            parity = config['parity_blocks_count']
            max_blocks = config['streaming_max_blocks_in_chunk']
            if parity > max_blocks // 2:
                raise ValueError("Invalid parity blocks count")
        
        return True


if __name__ == '__main__':
    unittest.main() 