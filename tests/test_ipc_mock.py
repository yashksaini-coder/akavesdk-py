import unittest
import io
import os
import random
import string
import time
import sys
import logging
import pytest
from unittest.mock import MagicMock, patch

MAX_CONCURRENCY = 5
BLOCK_PART_SIZE = 128 * 1024
MB = 1024 * 1024
MIN_BUCKET_NAME_LENGTH = 3

class SDKError(Exception):
    pass

def random_string(length=10):
    prefix = 'test-'
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return prefix + suffix

def check_file_contents(test_instance, expected_data, actual_data):
    test_instance.assertEqual(len(expected_data), len(actual_data), 
                             f"Data lengths don't match: expected {len(expected_data)}, got {len(actual_data)}")
    test_instance.assertEqual(expected_data, actual_data, "File contents don't match")

def random_bytes(size_mb):
    size_bytes = size_mb * MB
    return bytes([random.randint(0, 255) for _ in range(size_bytes)])

class BackgroundContext:
    def __init__(self):
        self._cancelled = False

    def done(self):
        return self._cancelled

    def cancel(self):
        self._cancelled = True

class MockTransaction:
    def __init__(self, hash_value):
        self._hash = hash_value
    
    def hash(self):
        return self._hash

class MockBucket:
    def __init__(self, name, created_at=None):
        self.id = "".join([random.choice("0123456789abcdef") for _ in range(64)])
        self.name = name
        self.created_at = created_at or time.time()

class MockIPCStorage:
    def __init__(self):
        self.buckets = {}
        self.files = {}
    
    def create_bucket(self, auth, name):
        self.buckets[name] = MockBucket(name)
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))
    
    def get_bucket_by_name(self, auth_params, name):
        return self.buckets.get(name)
    
    def get_bucket_index_by_name(self, auth_params, name):
        return list(self.buckets.keys()).index(name) if name in self.buckets else -1
    
    def create_file(self, auth, bucket_id, file_name):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name:
            return None
        
        if bucket_name not in self.files:
            self.files[bucket_name] = {}
        
        self.files[bucket_name][file_name] = {
            "id": "".join([random.choice("0123456789abcdef") for _ in range(64)]),
            "created_at": time.time(),
            "chunks": []
        }
        
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))
    
    def get_file_by_name(self, auth_params, bucket_id, file_name):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name or bucket_name not in self.files or file_name not in self.files[bucket_name]:
            return None
        
        file_data = self.files[bucket_name][file_name]
        file = MagicMock()
        file.id = file_data["id"]
        file.created_at = file_data["created_at"]
        return file
    
    def add_file_chunk(self, auth, chunk_cid_bytes, bucket_id, file_name, size, cids, sizes, index):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name or bucket_name not in self.files or file_name not in self.files[bucket_name]:
            return None
        
        self.files[bucket_name][file_name]["chunks"].append({
            "index": index,
            "size": size,
            "cid": "Qm" + "".join([random.choice("0123456789abcdef") for _ in range(44)])
        })
        
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))
    
    def commit_file(self, auth, bucket_id, file_name, file_size, root_cid_bytes):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name or bucket_name not in self.files or file_name not in self.files[bucket_name]:
            return None
        
        self.files[bucket_name][file_name]["size"] = file_size
        self.files[bucket_name][file_name]["root_cid"] = "Qm" + "".join([random.choice("0123456789abcdef") for _ in range(44)])
        
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))
    
    def get_file_index_by_id(self, auth_params, file_name, bucket_id):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name or bucket_name not in self.files:
            return -1
        
        return list(self.files[bucket_name].keys()).index(file_name) if file_name in self.files[bucket_name] else -1
    
    def delete_file(self, auth, file_id, bucket_id, file_name, file_idx):
        bucket_name = next((name for name, bucket in self.buckets.items() if bucket.id == bucket_id), None)
        if not bucket_name or bucket_name not in self.files or file_name not in self.files[bucket_name]:
            return None
        
        del self.files[bucket_name][file_name]
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))
    
    def delete_bucket(self, auth, bucket_id, name, bucket_idx):
        if name not in self.buckets:
            return None
        
        del self.buckets[name]
        if name in self.files:
            del self.files[name]
        
        return MockTransaction("tx_" + "".join([random.choice("0123456789abcdef") for _ in range(32)]))

class MockIPCClient:
    def __init__(self):
        self.auth = MagicMock()
        self.auth.from_address = "0x" + "".join([random.choice("0123456789abcdef") for _ in range(40)])
        self.storage = MockIPCStorage()
    
    def wait_for_tx(self, ctx, tx_hash):
        return True

class IPCBucketCreateResult:
    def __init__(self, name, created_at):
        self.name = name
        self.created_at = created_at

class Chunk:
    def __init__(self, cid, encoded_size, size, index):
        self.cid = cid
        self.encoded_size = encoded_size
        self.size = size
        self.index = index

class IPCFileDownload:
    def __init__(self, bucket_name, name, chunks):
        self.bucket_name = bucket_name
        self.name = name
        self.chunks = chunks

class IPCFileMetaV2:
    def __init__(self, root_cid, bucket_name, name, encoded_size, created_at, committed_at):
        self.root_cid = root_cid
        self.bucket_name = bucket_name
        self.name = name
        self.encoded_size = encoded_size
        self.created_at = created_at
        self.committed_at = committed_at

class MockIPC:
    def __init__(self, client, conn, ipc, max_concurrency, block_part_size, use_connection_pool):
        self.client = client
        self.conn = conn
        self.ipc = ipc
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
    
    def create_bucket(self, ctx, name):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")
        
        tx = self.ipc.storage.create_bucket(self.ipc.auth, name)
        if not tx:
            raise SDKError("failed to create bucket transaction")
        
        if not self.ipc.wait_for_tx(ctx, tx.hash()):
            raise SDKError("failed waiting for transaction")
        
        bucket = self.ipc.storage.get_bucket_by_name(
            {"from": self.ipc.auth.from_address},
            name
        )
        
        if not bucket:
            raise SDKError("failed to retrieve bucket")
        
        return IPCBucketCreateResult(
            name=bucket.name,
            created_at=bucket.created_at
        )
    
    def create_file_upload(self, ctx, bucket_name, file_name):
        if not bucket_name:
            raise SDKError("empty bucket name")
        
        bucket = self.ipc.storage.get_bucket_by_name(
            {"from": self.ipc.auth.from_address},
            bucket_name
        )
        
        if not bucket:
            raise SDKError("failed to retrieve bucket")
        
        tx = self.ipc.storage.create_file(
            self.ipc.auth,
            bucket.id,
            file_name
        )
        
        if not tx:
            raise SDKError("failed to create file transaction")
        
        if not self.ipc.wait_for_tx(ctx, tx.hash()):
            raise SDKError("failed waiting for file creation transaction")
        
        return None
    
    def upload(self, ctx, bucket_name, file_name, reader):
        data = reader.read()
        
        bucket = self.ipc.storage.get_bucket_by_name(
            {"from": self.ipc.auth.from_address},
            bucket_name
        )
        
        if not bucket:
            raise SDKError("failed to retrieve bucket")
        
        tx = self.ipc.storage.commit_file(
            self.ipc.auth,
            bucket.id,
            file_name,
            len(data),
            b"root_cid"
        )
        
        if not self.ipc.wait_for_tx(ctx, tx.hash()):
            raise SDKError("failed waiting for file commit transaction")
        
        file_meta = self.ipc.storage.get_file_by_name(
            {"from": self.ipc.auth.from_address},
            bucket.id,
            file_name
        )
        
        return IPCFileMetaV2(
            root_cid="QmRootCid",
            bucket_name=bucket_name,
            name=file_name,
            encoded_size=len(data),
            created_at=file_meta.created_at,
            committed_at=time.time()
        )
    
    def create_file_download(self, ctx, bucket_name, file_name):
        if not bucket_name:
            raise SDKError("empty bucket id")
            
        if not file_name:
            raise SDKError("empty file name")
        
        chunks = [Chunk(
            cid=f"QmChunk{i}",
            encoded_size=1024*1024,
            size=1024*1024,
            index=i
        ) for i in range(1)]
        
        return IPCFileDownload(
            bucket_name=bucket_name,
            name=file_name,
            chunks=chunks
        )
    
    def download(self, ctx, file_download, writer):
        pass

class TestIPCMock(unittest.TestCase):
    
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_ipc_client = MockIPCClient()
        
        self.ipc = MockIPC(
            client=self.mock_client,
            conn=self.mock_conn,
            ipc=self.mock_ipc_client,
            max_concurrency=MAX_CONCURRENCY,
            block_part_size=BLOCK_PART_SIZE,
            use_connection_pool=True
        )
    
    def tearDown(self):
        pass
    
    def test_upload_download_ipc_1mb(self):
        self._test_upload_download_ipc(1)
    
    @pytest.mark.slow
    def test_upload_download_ipc_5mb(self):
        self._test_upload_download_ipc(5)
    
    @pytest.mark.slow
    def test_upload_download_ipc_15mb(self):
        self._test_upload_download_ipc(15)
    
    @pytest.mark.slow
    def test_upload_download_ipc_35mb(self):
        self._test_upload_download_ipc(35)
    
    def _test_upload_download_ipc(self, file_size_mb):
        data = random_bytes(file_size_mb)
        file = io.BytesIO(data)
        
        bucket_name = random_string(10)
        file_name = random_string(10)
        
        ctx = BackgroundContext()
        
        bucket_result = self.ipc.create_bucket(ctx, bucket_name)
        self.assertEqual(bucket_result.name, bucket_name)
        
        start_time = time.time()
        self.ipc.create_file_upload(ctx, bucket_name, file_name)
        file_upload_duration = time.time() - start_time
        logging.info(f"Create file upload duration: {file_upload_duration}s")
        
        start_time = time.time()
        up_result = self.ipc.upload(ctx, bucket_name, file_name, file)
        upload_duration = time.time() - start_time
        logging.info(f"Upload duration: {upload_duration}s")
        
        self.assertEqual(up_result.name, file_name)
        
        downloaded = io.BytesIO()
        file_download = self.ipc.create_file_download(ctx, up_result.bucket_name, up_result.name)
        self.assertTrue(len(file_download.chunks) > 0)
        
        def mock_download(ctx, file_download, writer):
            writer.write(data)
            return None
        
        with patch.object(self.ipc, 'download', side_effect=mock_download):
            start_time = time.time()
            self.ipc.download(ctx, file_download, downloaded)
            download_duration = time.time() - start_time
            logging.info(f"Download duration: {download_duration}s")
            
            check_file_contents(self, data, downloaded.getvalue())

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()