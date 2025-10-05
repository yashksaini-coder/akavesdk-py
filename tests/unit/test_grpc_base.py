import pytest
from unittest.mock import Mock, patch
import grpc
import logging

from sdk.shared.grpc_base import GrpcClientBase
from sdk.config import SDKError


class TestGrpcClientBaseInit:
    
    def test_init_default_timeout(self):
        client = GrpcClientBase()
        assert client.connection_timeout is None
    
    def test_init_with_timeout(self):
        client = GrpcClientBase(connection_timeout=30)
        assert client.connection_timeout == 30
    
    def test_init_zero_timeout(self):
        client = GrpcClientBase(connection_timeout=0)
        assert client.connection_timeout == 0
    
    def test_init_large_timeout(self):
        client = GrpcClientBase(connection_timeout=3600)
        assert client.connection_timeout == 3600


class TestHandleGrpcError:
    
    def test_handle_deadline_exceeded(self):
        client = GrpcClientBase(connection_timeout=10)
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
        mock_error.details.return_value = "Timeout details"
        
        with pytest.raises(SDKError, match="request timed out after 10s"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_deadline_exceeded_no_timeout_set(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
        mock_error.details.return_value = "Timeout"
        
        with pytest.raises(SDKError, match="request timed out after Nones"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_unavailable_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.UNAVAILABLE
        mock_error.details.return_value = "Service unavailable"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: UNAVAILABLE"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_not_found_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.NOT_FOUND
        mock_error.details.return_value = "Resource not found"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: NOT_FOUND"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_permission_denied_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.PERMISSION_DENIED
        mock_error.details.return_value = "Access denied"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: PERMISSION_DENIED"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_invalid_argument_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.INVALID_ARGUMENT
        mock_error.details.return_value = "Invalid argument provided"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: INVALID_ARGUMENT"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_internal_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.INTERNAL
        mock_error.details.return_value = "Internal server error"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: INTERNAL"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_error_no_details(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.UNKNOWN
        mock_error.details.return_value = None
        
        with pytest.raises(SDKError, match="No details provided"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_error_empty_details(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.UNKNOWN
        mock_error.details.return_value = ""
        
        with pytest.raises(SDKError, match="No details provided"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_cancelled_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.CANCELLED
        mock_error.details.return_value = "Request cancelled"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: CANCELLED"):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_resource_exhausted_error(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.RESOURCE_EXHAUSTED
        mock_error.details.return_value = "Too many requests"
        
        with pytest.raises(SDKError, match="gRPC call TestMethod failed: RESOURCE_EXHAUSTED"):
            client._handle_grpc_error("TestMethod", mock_error)


class TestHandleGrpcErrorLogging:
    
    @patch('sdk.shared.grpc_base.logging.warning')
    def test_deadline_exceeded_logs_warning(self, mock_warning):
        client = GrpcClientBase(connection_timeout=5)
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
        mock_error.details.return_value = "Timeout"
        
        try:
            client._handle_grpc_error("TestMethod", mock_error)
        except SDKError:
            pass
        
        mock_warning.assert_called_once()
        call_args = str(mock_warning.call_args)
        assert "TestMethod" in call_args
        assert "timed out" in call_args
    
    @patch('sdk.shared.grpc_base.logging.error')
    def test_other_errors_log_error(self, mock_error_log):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.INTERNAL
        mock_error.details.return_value = "Internal error"
        
        try:
            client._handle_grpc_error("TestMethod", mock_error)
        except SDKError:
            pass
        
        mock_error_log.assert_called_once()
        call_args = str(mock_error_log.call_args)
        assert "TestMethod" in call_args
        assert "INTERNAL" in call_args


class TestGrpcClientBaseEdgeCases:
    
    def test_handle_error_with_special_characters(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.INVALID_ARGUMENT
        mock_error.details.return_value = "Error: <special> & 'chars' \"test\""
        
        with pytest.raises(SDKError):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_error_with_unicode(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.INVALID_ARGUMENT
        mock_error.details.return_value = "错误信息 🔥"
        
        with pytest.raises(SDKError):
            client._handle_grpc_error("TestMethod", mock_error)
    
    def test_handle_error_with_very_long_method_name(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.UNKNOWN
        mock_error.details.return_value = "Error"
        
        long_method_name = "VeryLongMethodName" * 20
        
        with pytest.raises(SDKError):
            client._handle_grpc_error(long_method_name, mock_error)
    
    def test_handle_error_with_very_long_details(self):
        client = GrpcClientBase()
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.UNKNOWN
        mock_error.details.return_value = "x" * 10000
        
        with pytest.raises(SDKError):
            client._handle_grpc_error("TestMethod", mock_error)


class TestGrpcErrorChaining:
    
    def test_error_preserves_original_exception(self):
        client = GrpcClientBase()
        
        original_error = Mock(spec=grpc.RpcError)
        original_error.code.return_value = grpc.StatusCode.UNAVAILABLE
        original_error.details.return_value = "Original error"
        
        try:
            client._handle_grpc_error("TestMethod", original_error)
        except SDKError as e:
            assert e.__cause__ == original_error
    
    def test_deadline_exceeded_preserves_original(self):
        client = GrpcClientBase(connection_timeout=10)
        
        original_error = Mock(spec=grpc.RpcError)
        original_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
        original_error.details.return_value = "Timeout"
        
        try:
            client._handle_grpc_error("TestMethod", original_error)
        except SDKError as e:
            assert e.__cause__ == original_error


class TestGrpcStatusCodes:
    
    def test_all_major_status_codes(self):
        client = GrpcClientBase()
        
        status_codes = [
            grpc.StatusCode.OK,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.ALREADY_EXISTS,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.OUT_OF_RANGE,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DATA_LOSS,
            grpc.StatusCode.UNAUTHENTICATED,
        ]
        
        for status_code in status_codes:
            mock_error = Mock(spec=grpc.RpcError)
            mock_error.code.return_value = status_code
            mock_error.details.return_value = f"Error for {status_code.name}"
            
            with pytest.raises(SDKError):
                client._handle_grpc_error("TestMethod", mock_error)


@pytest.mark.integration
class TestGrpcClientBaseIntegration:
    
    def test_custom_subclass(self):
        class CustomGrpcClient(GrpcClientBase):
            def __init__(self, timeout):
                super().__init__(connection_timeout=timeout)
            
            def call_method(self):
                mock_error = Mock(spec=grpc.RpcError)
                mock_error.code.return_value = grpc.StatusCode.UNAVAILABLE
                mock_error.details.return_value = "Service down"
                self._handle_grpc_error("call_method", mock_error)
        
        client = CustomGrpcClient(timeout=15)
        assert client.connection_timeout == 15
        
        with pytest.raises(SDKError, match="Service down"):
            client.call_method()
    
    def test_multiple_error_handling(self):
        client = GrpcClientBase(connection_timeout=30)
        
        errors = [
            (grpc.StatusCode.UNAVAILABLE, "Service unavailable"),
            (grpc.StatusCode.DEADLINE_EXCEEDED, "Timeout"),
            (grpc.StatusCode.INTERNAL, "Internal error"),
        ]
        
        for status_code, details in errors:
            mock_error = Mock(spec=grpc.RpcError)
            mock_error.code.return_value = status_code
            mock_error.details.return_value = details
            
            with pytest.raises(SDKError):
                client._handle_grpc_error("MultipleErrors", mock_error)
    
    @patch('sdk.shared.grpc_base.logging')
    def test_logging_integration(self, mock_logging):
        client = GrpcClientBase(connection_timeout=20)
        
        mock_error = Mock(spec=grpc.RpcError)
        mock_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
        mock_error.details.return_value = "Test timeout"
        
        try:
            client._handle_grpc_error("IntegrationTest", mock_error)
        except SDKError:
            pass
        
        assert mock_logging.warning.called or mock_logging.error.called

