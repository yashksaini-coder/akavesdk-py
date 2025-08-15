import grpc
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, TypeVar
from private.pb import nodeapi_pb2_grpc, nodeapi_pb2
from private.encryption import derive_key

from .config import SDKError, MIN_BUCKET_NAME_LENGTH
from .shared.grpc_base import GrpcClientBase

T = TypeVar("T")  # Generic return type for gRPC calls

@dataclass
class BucketCreateResult:
    name: str
    created_at: datetime


@dataclass
class Bucket:
    name: str
    created_at: datetime


class BucketClient(GrpcClientBase):
    def __init__(self, channel: grpc.Channel, connection_timeout: int):
        super().__init__(connection_timeout=connection_timeout)
        self.client = nodeapi_pb2_grpc.NodeAPIStub(channel)

    def bucket_create(self, name: str) -> BucketCreateResult:
        self._validate_bucket_name(name, "BucketCreate")
        request = nodeapi_pb2.BucketCreateRequest(name=name)
        response = self._do_grpc_call("BucketCreate", self.client.BucketCreate, request)
        return BucketCreateResult(
            name=response.name,
            created_at=parse_timestamp(response.created_at)
        )

    def bucket_view(self, name: str) -> Bucket:
        self._validate_bucket_name(name, "BucketView")
        request = nodeapi_pb2.BucketViewRequest(bucket_name=name)
        response = self._do_grpc_call("BucketView", self.client.BucketView, request)
        return Bucket(
            name=response.name,
            created_at=parse_timestamp(response.created_at)
        )

    def bucket_delete(self, name: str) -> bool:
        self._validate_bucket_name(name, "BucketDelete")
        request = nodeapi_pb2.BucketDeleteRequest(name=name)
        self._do_grpc_call("BucketDelete", self.client.BucketDelete, request)
        return True

    def _validate_bucket_name(self, name: str, method_name: str) -> None:
        if not name or len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError(
                f"{method_name}: Invalid bucket name '{name}'. "
                f"Must be at least {MIN_BUCKET_NAME_LENGTH} characters "
                f"(got {len(name) if name else 0})."
            )

    def _do_grpc_call(self, method_name: str, grpc_method: Callable[..., T], request) -> T:
        try:
            return grpc_method(request, timeout=self.connection_timeout)
        except grpc.RpcError as e:
            self._handle_grpc_error(method_name, e)
            raise  # for making type checkers happy

def encryption_key_derivation(parent_key: bytes, *info_data: str) -> bytes:
    if not parent_key:
        raise SDKError("Parent key is required for key derivation")
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def parse_timestamp(ts) -> datetime | None:
    if ts is None:
        return None
    return ts.AsTime() if hasattr(ts, "AsTime") else ts
