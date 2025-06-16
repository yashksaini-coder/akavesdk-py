"""
Unit tests package for the Akave SDK.

This package contains comprehensive unit tests for all SDK components:
- SDK core functionality (test_sdk_methods.py)
- Streaming API (test_streaming_api.py) 
- IPC API (test_ipc_api.py)
- Configuration loading (test_config_loading.py)

To run all unit tests:
    python -m pytest tests/unit/ -v

To run a specific test file:
    python -m pytest tests/unit/test_sdk_methods.py -v

To run tests with coverage:
    python -m pytest tests/unit/ --cov=sdk --cov-report=html
"""

__version__ = "1.0.0"
__author__ = "Akave SDK Team" 