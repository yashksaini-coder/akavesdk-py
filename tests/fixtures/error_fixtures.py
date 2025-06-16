"""
Error scenario fixtures for comprehensive testing of the Akave SDK.
"""
import grpc
from unittest.mock import MagicMock


def get_grpc_error_scenarios():
    """Get various gRPC error scenarios for testing."""
    
    # Create mock gRPC errors
    def create_grpc_error(code, details):
        error = grpc.RpcError()
        error.code = lambda: code
        error.details = lambda: details
        return error
    
    return {
        'connection_failed': create_grpc_error(
            grpc.StatusCode.UNAVAILABLE, 
            'Connection failed'
        ),
        'timeout': create_grpc_error(
            grpc.StatusCode.DEADLINE_EXCEEDED, 
            'Request timeout'
        ),
        'not_found': create_grpc_error(
            grpc.StatusCode.NOT_FOUND, 
            'Resource not found'
        ),
        'permission_denied': create_grpc_error(
            grpc.StatusCode.PERMISSION_DENIED, 
            'Permission denied'
        ),
        'invalid_argument': create_grpc_error(
            grpc.StatusCode.INVALID_ARGUMENT, 
            'Invalid argument provided'
        ),
        'already_exists': create_grpc_error(
            grpc.StatusCode.ALREADY_EXISTS, 
            'Resource already exists'
        ),
        'resource_exhausted': create_grpc_error(
            grpc.StatusCode.RESOURCE_EXHAUSTED, 
            'Resource exhausted'
        ),
        'internal_error': create_grpc_error(
            grpc.StatusCode.INTERNAL, 
            'Internal server error'
        ),
        'unimplemented': create_grpc_error(
            grpc.StatusCode.UNIMPLEMENTED, 
            'Method not implemented'
        )
    }


def get_validation_error_scenarios():
    """Get validation error scenarios for testing."""
    return {
        'empty_strings': {
            'bucket_name': '',
            'file_name': '',
            'address': '',
            'private_key': ''
        },
        'invalid_sizes': {
            'zero_block_size': 0,
            'negative_block_size': -1,
            'oversized_block': 1024 * 1024 * 10,  # 10MB
            'zero_concurrency': 0,
            'negative_concurrency': -5
        },
        'invalid_keys': {
            'short_encryption_key': b'short',
            'long_encryption_key': b'a' * 64,  # 64 bytes instead of 32
            'empty_encryption_key': b'',
            'invalid_private_key': 'not_a_hex_key',
            'short_private_key': '0x123'
        },
        'invalid_parity_settings': {
            'parity_too_high': {
                'parity_blocks_count': 20,
                'streaming_max_blocks_in_chunk': 32
            },
            'parity_equal_to_max': {
                'parity_blocks_count': 32,
                'streaming_max_blocks_in_chunk': 32
            },
            'negative_parity': {
                'parity_blocks_count': -1,
                'streaming_max_blocks_in_chunk': 32
            }
        }
    }


def get_network_error_scenarios():
    """Get network-related error scenarios."""
    return {
        'connection_errors': [
            ConnectionError('Connection refused'),
            ConnectionResetError('Connection reset by peer'),
            ConnectionAbortedError('Connection aborted'),
            ConnectionRefusedError('Connection refused by server')
        ],
        'timeout_errors': [
            TimeoutError('Operation timed out'),
            Exception('Request timeout'),
            Exception('Connection timeout')
        ],
        'dns_errors': [
            Exception('Name resolution failed'),
            Exception('Host not found'),
            Exception('DNS lookup failed')
        ]
    }


def get_blockchain_error_scenarios():
    """Get blockchain-related error scenarios."""
    return {
        'transaction_errors': [
            Exception('Transaction failed'),
            Exception('Insufficient gas'),
            Exception('Gas limit exceeded'),
            Exception('Transaction reverted')
        ],
        'contract_errors': [
            Exception('Contract execution failed'),
            Exception('Contract not found'),
            Exception('Invalid contract address'),
            Exception('Contract method not found')
        ],
        'authentication_errors': [
            Exception('Invalid private key'),
            Exception('Signature verification failed'),
            Exception('Unauthorized access'),
            Exception('Account not found')
        ],
        'balance_errors': [
            Exception('Insufficient funds'),
            Exception('Account balance too low'),
            Exception('Token transfer failed')
        ]
    }


def get_file_system_error_scenarios():
    """Get file system related error scenarios."""
    return {
        'file_errors': [
            FileNotFoundError('File not found'),
            PermissionError('Permission denied'),
            IsADirectoryError('Is a directory'),
            FileExistsError('File already exists')
        ],
        'io_errors': [
            IOError('I/O operation failed'),
            OSError('Operating system error'),
            Exception('Disk full'),
            Exception('Read-only file system')
        ]
    }


def get_data_corruption_scenarios():
    """Get data corruption and integrity error scenarios."""
    return {
        'corrupted_data': {
            'invalid_cid': 'invalid_cid_format',
            'corrupted_block': b'\x00\x01\x02corrupted_data',
            'truncated_data': b'truncated',
            'empty_data': b'',
            'oversized_data': b'a' * (1024 * 1024 * 100)  # 100MB
        },
        'checksum_errors': [
            Exception('Checksum mismatch'),
            Exception('Data integrity check failed'),
            Exception('Hash verification failed'),
            Exception('Block verification failed')
        ],
        'encoding_errors': [
            UnicodeDecodeError('utf-8', b'\xff\xfe', 0, 1, 'invalid start byte'),
            UnicodeEncodeError('utf-8', '\udcff', 0, 1, 'surrogates not allowed'),
            Exception('Encoding format not supported'),
            Exception('Invalid character encoding')
        ]
    }


def get_resource_exhaustion_scenarios():
    """Get resource exhaustion scenarios."""
    return {
        'memory_errors': [
            MemoryError('Out of memory'),
            Exception('Memory allocation failed'),
            Exception('Buffer overflow')
        ],
        'disk_errors': [
            Exception('Disk full'),
            Exception('No space left on device'),
            Exception('Disk quota exceeded')
        ],
        'connection_pool_errors': [
            Exception('Connection pool exhausted'),
            Exception('Too many open connections'),
            Exception('Connection limit reached')
        ]
    }


def get_concurrent_access_scenarios():
    """Get concurrent access and race condition scenarios."""
    return {
        'race_conditions': [
            Exception('Resource locked by another process'),
            Exception('Concurrent modification detected'),
            Exception('Deadlock detected')
        ],
        'threading_errors': [
            Exception('Thread pool exhausted'),
            Exception('Thread synchronization failed'),
            Exception('Concurrent access violation')
        ]
    }


def create_comprehensive_error_test_suite():
    """Create a comprehensive error test suite combining all scenarios."""
    return {
        'grpc_errors': get_grpc_error_scenarios(),
        'validation_errors': get_validation_error_scenarios(),
        'network_errors': get_network_error_scenarios(),
        'blockchain_errors': get_blockchain_error_scenarios(),
        'file_system_errors': get_file_system_error_scenarios(),
        'data_corruption': get_data_corruption_scenarios(),
        'resource_exhaustion': get_resource_exhaustion_scenarios(),
        'concurrent_access': get_concurrent_access_scenarios()
    }


def get_edge_case_scenarios():
    """Get edge case scenarios for boundary testing."""
    return {
        'boundary_values': {
            'max_int': 2**31 - 1,
            'min_int': -2**31,
            'max_block_size': 1024 * 1024,  # 1MB
            'min_block_size': 1,
            'max_concurrency': 1000,
            'min_concurrency': 1
        },
        'special_characters': {
            'unicode_bucket_name': 'test-bucket-🚀-测试',
            'special_chars_file': 'file@#$%^&*()_+.txt',
            'long_name': 'a' * 255,  # Maximum filename length
            'empty_content': b'',
            'binary_content': bytes(range(256))
        },
        'timing_scenarios': {
            'very_slow_response': 30.0,  # 30 seconds
            'immediate_response': 0.0,
            'intermittent_failures': [True, False, True, False, True]
        }
    } 