"""
Test fixtures for Akave SDK unit tests.

This package contains reusable test data and mock objects:
- common_fixtures.py: Common test data and configurations
- error_fixtures.py: Error scenarios and edge cases

Usage:
    from tests.fixtures.common_fixtures import get_test_sdk_config
    from tests.fixtures.error_fixtures import get_grpc_error_scenarios
"""

from .common_fixtures import (
    mock_grpc_channel,
    mock_node_api_client,
    mock_ipc_client,
    mock_streaming_client,
    mock_web3_instance,
    mock_ipc_instance,
    sample_bucket_data,
    sample_file_data,
    sample_encryption_key,
    sample_config_data
)

from .error_fixtures import (
    get_grpc_error_scenarios,
    get_validation_error_scenarios,
    get_network_error_scenarios,
    get_blockchain_error_scenarios,
    create_comprehensive_error_test_suite,
    get_edge_case_scenarios
)

__all__ = [
    'mock_grpc_channel',
    'mock_node_api_client', 
    'mock_ipc_client',
    'mock_streaming_client',
    'mock_web3_instance',
    'mock_ipc_instance',
    'sample_bucket_data',
    'sample_file_data',
    'sample_encryption_key',
    'sample_config_data',
    'get_grpc_error_scenarios',
    'get_validation_error_scenarios',
    'get_network_error_scenarios',
    'get_blockchain_error_scenarios',
    'create_comprehensive_error_test_suite',
    'get_edge_case_scenarios'
] 