import grpc
import time
import threading
from private.pb import nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc


class ConnectionPool:
    # by default , retry 3 times with exponential backoff.
    # The init function sets the retries and delay parameters.
    def _retry(func, retries=3, delay=1):
        def wrapper(self, **kwargs):
            current_retry = 0
            current_delay = delay
            while current_retry < retries:
                try:
                    return func(self, **kwargs)
                except Exception as e:
                    current_retry += 1
                    if current_retry >= retries:
                        raise e
                    print(f"Attempt {current_retry} failed: {e}. Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= 2
            return None
        return wrapper
    
    # The retries and delay can be configured.
    def __init__(self, retries=3, delay=1):
        self._lock = threading.RLock()
        self._connections = {}
        self.use_connection_pool = False
        self.create_client = self._retry(self.create_client, retries, delay)
        self.create_ipc_client = self._retry(self.create_ipc_client, retries, delay)
        self.get = self._retry(self.get, retries, delay)
        self._new_connection = self._retry(self._new_connection, retries, delay)
        self.close = self._retry(self.close, retries, delay) 

    def create_client(self, addr: str, pooled: bool):
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return nodeapi_pb2_grpc.NodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return nodeapi_pb2_grpc.NodeAPIStub(conn), lambda: conn.close(), None

    def create_ipc_client(self, addr: str, pooled: bool):
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), lambda: conn.close(), None

    def get(self, addr: str):
        with self._lock:
            return self._connections.get(addr)

    def _new_connection(self, addr: str):
        try:
            conn = grpc.insecure_channel(addr)
            with self._lock:
                self._connections[addr] = conn
            return conn
        except Exception as e:
            return None

    def close(self):
        with self._lock:
            errors = []
            for addr, conn in self._connections.items():
                try:
                    conn.close()
                except Exception as e:
                    errors.append(f"Failed to close connection to {addr}: {e}")
            self._connections.clear()
            if errors:
                return Exception("Encountered errors while closing connections: " + ", ".join(errors))
            return None
