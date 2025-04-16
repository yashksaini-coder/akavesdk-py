import time
import logging
import binascii
import io
import math
import concurrent.futures
from hashlib import sha256
from typing import List, Optional, Callable, Dict, Any, Union, Tuple

from .common import MIN_BUCKET_NAME_LENGTH, SDKError, BLOCK_SIZE, ENCRYPTION_OVERHEAD
from .erasure_code import ErasureCode
from .dag import build_dag, extract_block_data
from .connection import ConnectionPool
from .model import (
    IPCBucketCreateResult, IPCBucket, IPCFileMeta, IPCFileListItem,
    IPCFileMetaV2, IPCFileChunkUploadV2, AkaveBlockData, FileBlockUpload,
    FileBlockDownload, Chunk, IPCFileDownload, FileChunkDownload
)
from private.encryption import encrypt, derive_key, decrypt
from private.pb import ipcnodeapi_pb2_grpc, ipcnodeapi_pb2

try:
    from multiformats import cid as cidlib
except ImportError:
    pass

BlockSize = BLOCK_SIZE
EncryptionOverhead = ENCRYPTION_OVERHEAD

class DAGRoot:
    def __init__(self):
        self.links = []
    
    @classmethod
    def new(cls):
        return cls()
    
    def add_link(self, chunk_cid, raw_data_size: int, proto_node_size: int):
        self.links.append({
            "cid": chunk_cid,
            "raw_data_size": raw_data_size,
            "proto_node_size": proto_node_size
        })
        return None
    
    def build(self):
        root_cid = cidlib.make_cid(f"dag_root_{len(self.links)}")
        return root_cid

def encryption_key(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def to_ipc_proto_chunk(chunk_cid: str, index: int, size: int, blocks):
    cids = []
    sizes = []
    
    pb_blocks = []
    for block in blocks:
        pb_block = {
            "cid": block["cid"],
            "size": len(block["data"])
        }
        pb_blocks.append(pb_block)
        
        try:
            c = cidlib.decode(block["cid"])
            b_cid = bytearray(32)
            cid_bytes = c.buffer if hasattr(c, 'buffer') else c._buffer
            copy_len = min(len(cid_bytes) - 4, 32)
            b_cid[:copy_len] = cid_bytes[4:4+copy_len]
            cids.append(b_cid)
            sizes.append(len(block["data"]))
        except Exception as e:
            return None, None, None, SDKError(f"failed to decode CID: {str(e)}")
    
    proto_chunk = {
        "cid": chunk_cid,
        "index": index,
        "size": size,
        "blocks": pb_blocks
    }
    
    return cids, sizes, proto_chunk, None

class IPC:
    def __init__(self, client, conn, ipc_instance, max_concurrency, block_part_size, use_connection_pool, encryption_key=None, max_blocks_in_chunk=32, erasure_code=None):
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key if encryption_key else b''
        self.max_blocks_in_chunk = max_blocks_in_chunk
        self.erasure_code = erasure_code

    def create_bucket(self, ctx, name: str) -> IPCBucketCreateResult:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")

        try:
            request = ipcnodeapi_pb2_grpc.IPCBucketCreateRequest(name=name)
            response = self.client.BucketCreate(request)
            return IPCBucketCreateResult(name=response.name, created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at)
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

        except Exception as e:
            raise SDKError(f"bucket creation failed: {str(e)}")

    def view_bucket(self, ctx, bucket_name: str) -> IPCBucket:
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
            request = ipcnodeapi_pb2_grpc.IPCNodeAPIStub.IPCBucketViewRequest(
                name=bucket_name,
                address=self.ipc.auth.from_address
            )
            
            res = self.client.bucket_view(ctx, request)
            
            return IPCBucket(
                id=res.id,
                name=res.name,
                created_at=res.created_at.AsTime() if hasattr(res.created_at, 'AsTime') else res.created_at
            )
        except Exception as err:
            raise SDKError(f"failed to view bucket: {str(err)}")

    def list_buckets(self, ctx) -> list[IPCBucket]:
        try:
            request = ipcnodeapi_pb2.IPCBucketListRequest(address=self.ipc.auth.from_address)
            
            res = self.client.bucket_list(ctx, request)
            
            buckets = []
            for bucket in res.buckets:
                buckets.append(IPCBucket(
                    id="",
                    name=bucket.name,
                    created_at=bucket.created_at.AsTime() if hasattr(bucket.created_at, 'AsTime') else bucket.created_at
                ))
            
            return buckets
        except Exception as err:
            raise SDKError(f"failed to list buckets: {str(err)}")

    def delete_bucket(self, ctx, name: str) -> None:
        try:
            request = ipcnodeapi_pb2_grpc.IPCBucketViewRequest(
                name=name,
                address=self.ipc.auth.from_address
            )
            bucket = self.client.bucket_view(ctx, request)
            
            try:
                id_bytes = binascii.unhexlify(bucket.id)
            except Exception as e:
                raise SDKError(f"invalid bucket ID format: {str(e)}")
            
            bucket_id = bytearray(32)
            bucket_id[:len(id_bytes)] = id_bytes
            
            bucket_idx = self.ipc.storage.get_bucket_index_by_name(
                {"from": self.ipc.auth.from_address},
                name
            )
            
            tx = self.ipc.storage.delete_bucket(
                self.ipc.auth, 
                bucket_id, 
                name, 
                bucket_idx
            )
            
            if not self.ipc.wait_for_tx(ctx, tx.hash()):
                raise SDKError("failed waiting for delete transaction")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to delete bucket: {str(err)}")

    def file_info(self, ctx, bucket_name: str, file_name: str) -> IPCFileMeta:
        try:
            request = ipcnodeapi_pb2.IPCFileViewRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.from_address
            )
            
            res = self.client.file_view(ctx, request)
            
            return IPCFileMeta(
                root_cid=res.root_cid,
                name=res.file_name,
                bucket_name=res.bucket_name,
                encoded_size=res.encoded_size,
                created_at=res.created_at.AsTime() if hasattr(res.created_at, 'AsTime') else res.created_at
            )
        except Exception as err:
            raise SDKError(f"failed to get file info: {str(err)}")

    def list_files(self, ctx, bucket_name: str) -> list[IPCFileListItem]:
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
            request = ipcnodeapi_pb2.IPCFileListRequest(
                bucket_name=bucket_name,
                address=self.ipc.auth.from_address
            )
            
            resp = self.client.file_list(ctx, request)

            files = []
            for file_meta in resp.list:
                files.append(IPCFileListItem(
                    root_cid=file_meta.root_cid,
                    name=file_meta.name,
                    encoded_size=file_meta.encoded_size,
                    created_at=file_meta.created_at.AsTime() if hasattr(file_meta.created_at, 'AsTime') else file_meta.created_at
                ))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list files: {str(err)}")

    def file_delete(self, ctx, bucket_name: str, file_name: str) -> None:
        if not bucket_name.strip() or not file_name.strip():
            raise SDKError(f"empty bucket or file name. Bucket: '{bucket_name}', File: '{file_name}'")

        try:
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.from_address},
                bucket_name
            )
            
            file = self.ipc.storage.get_file_by_name(
                {},
                bucket.id,
                file_name
            )
            
            file_idx = self.ipc.storage.get_file_index_by_id(
                {},
                file_name,
                bucket.id
            )
            
            tx = self.ipc.storage.delete_file(
                self.ipc.auth,
                file.id,
                bucket.id,
                file_name,
                file_idx
            )
            
            if not self.ipc.wait_for_tx(ctx, tx.hash()):
                raise SDKError("failed waiting for file delete transaction")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to delete file: {str(err)}")

    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> None:
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
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
        except Exception as err:
            raise SDKError(f"failed to create file upload: {str(err)}")

    def upload(self, ctx, bucket_name: str, file_name: str, reader: io.IOBase) -> IPCFileMetaV2:
        try:
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.from_address},
                bucket_name
            )
            if not bucket:
                raise SDKError("failed to retrieve bucket")
            
            chunk_enc_overhead = 0
            try:
                file_enc_key = encryption_key(self.encryption_key, bucket_name, file_name)
                if len(file_enc_key) > 0:
                    chunk_enc_overhead = EncryptionOverhead
            except Exception as e:
                raise SDKError(f"encryption key derivation failed: {str(e)}")
            
            is_empty_file = True
            
            buffer_size = self.max_blocks_in_chunk * int(BlockSize)
            if self.erasure_code:
                buffer_size = self.erasure_code.data_blocks * int(BlockSize)
            buffer_size -= chunk_enc_overhead
            buf = bytearray(buffer_size)
            
            dag_root = DAGRoot.new()
            
            i = 0
            file_size = 0
            
            while True:
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("context cancelled")
                
                try:
                    n = reader.readinto(buf)
                    if n == 0:
                        if is_empty_file:
                            raise SDKError("empty file")
                        break
                    is_empty_file = False
                except Exception as e:
                    if isinstance(e, EOFError):
                        if is_empty_file:
                            raise SDKError("empty file")
                        break
                    raise SDKError(f"failed to read file: {str(e)}")
                
                chunk_upload = self.create_chunk_upload(ctx, i, file_enc_key, buf[:n], bucket.id, file_name)
                file_size += chunk_upload.actual_size
                
                dag_root.add_link(chunk_upload.chunk_cid, chunk_upload.raw_data_size, chunk_upload.proto_node_size)
                
                self.upload_chunk(ctx, chunk_upload)
                
                i += 1
            
            root_cid = dag_root.build()
            
            file_meta = self.ipc.storage.get_file_by_name(
                {"from": self.ipc.auth.from_address},
                bucket.id,
                file_name
            )
            
            tx = self.ipc.storage.commit_file(
                self.ipc.auth,
                bucket.id,
                file_name,
                file_size,
                root_cid.bytes()
            )
            
            if not self.ipc.wait_for_tx(ctx, tx.hash()):
                raise SDKError("failed waiting for file commit transaction")
            
            return IPCFileMetaV2(
                root_cid=root_cid.string(),
                bucket_name=bucket_name,
                name=file_name,
                encoded_size=file_size,
                created_at=time.unix(file_meta.created_at, 0) if isinstance(file_meta.created_at, int) else file_meta.created_at,
                committed_at=time.time()
            )
        except Exception as err:
            raise SDKError(f"failed to upload file: {str(err)}")

    def create_chunk_upload(self, ctx, index: int, file_encryption_key: bytes, data: bytes, bucket_id, file_name: str) -> IPCFileChunkUploadV2:
        try:
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            
            block_size = BlockSize
            if self.erasure_code:
                data = self.erasure_code.encode(data)
                blocks_count = self.erasure_code.data_blocks + self.erasure_code.parity_blocks
                block_size = len(data) // blocks_count
            
            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size)
            
            cids, sizes, proto_chunk, _ = to_ipc_proto_chunk(
                chunk_dag.cid.string(),
                index,
                size,
                chunk_dag.blocks
            )
            
            req = {
                "chunk": proto_chunk,
                "bucket_id": bucket_id,
                "file_name": file_name
            }
            
            res = self.client.file_upload_chunk_create(ctx, req)
            
            if len(res.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(res.blocks)}, expected {len(chunk_dag.blocks)}")
            
            for i, upload in enumerate(res.blocks):
                if chunk_dag.blocks[i]["cid"] != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                chunk_dag.blocks[i]["node_address"] = upload.node_address
                chunk_dag.blocks[i]["node_id"] = upload.node_id
                chunk_dag.blocks[i]["permit"] = upload.permit
            
            tx = self.ipc.storage.add_file_chunk(
                self.ipc.auth,
                chunk_dag.cid.bytes(),
                bucket_id,
                file_name,
                size,
                cids,
                sizes,
                index
            )
            
            if not self.ipc.wait_for_tx(ctx, tx.hash()):
                raise SDKError("failed waiting for add file chunk transaction")
            
            return IPCFileChunkUploadV2(
                index=index,
                chunk_cid=chunk_dag.cid,
                actual_size=size,
                raw_data_size=chunk_dag.raw_data_size,
                proto_node_size=chunk_dag.proto_node_size,
                blocks=chunk_dag.blocks,
                bucket_id=bucket_id,
                file_name=file_name
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {str(err)}")

    def upload_chunk(self, ctx, file_chunk_upload: IPCFileChunkUploadV2) -> None:
        try:
            pool = ConnectionPool()
            
            try:
                _, _, proto_chunk, _ = to_ipc_proto_chunk(
                    file_chunk_upload.chunk_cid.string(),
                    file_chunk_upload.index,
                    file_chunk_upload.actual_size,
                    file_chunk_upload.blocks
                )
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []
                    
                    for i, block in enumerate(file_chunk_upload.blocks):
                        futures.append(executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk, 
                            file_chunk_upload.bucket_id, file_chunk_upload.file_name
                        ))
                    
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
            finally:
                err = pool.close()
                if err:
                    logging.warning(f"Error closing connection pool: {str(err)}")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to upload chunk: {str(err)}")

    def _upload_block(self, ctx, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str) -> None:
        try:
            client, closer, err = pool.create_ipc_client(block["node_address"], self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                sender = client.file_upload_block(ctx)
                
                block_data = {
                    "data": block["data"],
                    "cid": block["cid"],
                    "index": block_index,
                    "chunk": proto_chunk,
                    "bucket_id": bucket_id,
                    "file_name": file_name
                }
                
                self._upload_ipc_block_segments(ctx, block_data, sender.send)
                
                _, close_err = sender.close_and_recv()
                if close_err:
                    raise close_err
            finally:
                if closer:
                    closer()
        except Exception as err:
            raise SDKError(f"failed to upload block {block['cid']}: {str(err)}")

    def _upload_ipc_block_segments(self, ctx, block_data, send_func: Callable) -> None:
        try:
            data = block_data.get("data", b"")
            data_len = len(data)
            
            if data_len == 0:
                return None
                
            i = 0
            while i < data_len:
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("context cancelled")
                    
                end = i + self.block_part_size
                if end > data_len:
                    end = data_len
                    
                segment_data = dict(block_data)
                segment_data["data"] = data[i:end]
                
                send_func(segment_data)
                
                segment_data["chunk"] = None
                segment_data["cid"] = ""
                
                i += self.block_part_size
                
            return None
        except Exception as err:
            raise SDKError(f"failed to upload block segments: {str(err)}")

    def fetch_block_data(
        self, 
        ctx, 
        pool: ConnectionPool,
        chunk_cid: str, 
        bucket_name: str, 
        file_name: str, 
        address: str,
        chunk_index: int, 
        block_index: int,
        block
    ) -> bytes:
        try:
            if not hasattr(block, 'akave') or not block.akave and not hasattr(block, 'filecoin') or not block.filecoin:
                raise SDKError("missing block metadata")
            
            client, closer, err = pool.create_ipc_client(block.akave.node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                download_req = {
                    "chunk_cid": chunk_cid,
                    "chunk_index": chunk_index,
                    "block_cid": block.cid,
                    "block_index": block_index,
                    "bucket_name": bucket_name,
                    "file_name": file_name,
                    "address": address
                }
                
                download_client = client.file_download_block(ctx, download_req)
                if not download_client:
                    raise SDKError("failed to get download client")
                
                buffer = io.BytesIO()
                
                while True:
                    try:
                        block_data = download_client.recv()
                        if not block_data:
                            break
                        buffer.write(block_data.data)
                    except EOFError:
                        break
                    except Exception as e:
                        if isinstance(e, io.EOF):
                            break
                        raise SDKError(f"error receiving block data: {str(e)}")
                
                return buffer.getvalue()
            finally:
                if closer:
                    try:
                        closer()
                    except Exception as e:
                        logging.warning(f"Failed to close connection for block {block.cid}: {str(e)}")
        except Exception as e:
            raise SDKError(f"failed to fetch block data: {str(e)}")

    def create_file_download(self, ctx, bucket_name: str, file_name: str):
        try:
            if not bucket_name:
                raise SDKError("empty bucket id")
                
            if not file_name:
                raise SDKError("empty file name")
                
            request = ipcnodeapi_pb2_grpc.IPCNodeAPIStub.IPCFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.from_address
            )
            
            res = self.client.file_download_create(ctx, request)
            
            chunks = []
            for i, chunk in enumerate(res.chunks):
                chunks.append(Chunk(
                    cid=chunk.cid,
                    encoded_size=chunk.encoded_size,
                    size=chunk.size,
                    index=i
                ))
            
            return IPCFileDownload(
                bucket_name=res.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
            
    def download(self, ctx, file_download, writer: io.IOBase):
        try:
            file_enc_key = encryption_key(
                self.encryption_key, 
                file_download.bucket_name, 
                file_download.name
            )
            
            for chunk in file_download.chunks:
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("context cancelled")
                
                chunk_download = self.create_chunk_download(
                    ctx, 
                    file_download.bucket_name, 
                    file_download.name, 
                    chunk
                )
                
                self.download_chunk_blocks(
                    ctx,
                    file_download.bucket_name,
                    file_download.name,
                    self.ipc.auth.from_address,
                    chunk_download,
                    file_enc_key,
                    writer
                )
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
            
    def create_chunk_download(self, ctx, bucket_name: str, file_name: str, chunk):
        try:
            request = ipcnodeapi_pb2_grpc.IPCNodeAPIStub.FileDownloadChunkCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                chunk_cid=chunk.cid,
                address=self.ipc.auth.from_address
            )
            
            res = self.client.file_download_chunk_create(ctx, request)
            
            blocks = []
            for block in res.blocks:
                blocks.append(FileBlockDownload(
                    cid=block.cid,
                    data=b"",
                    akave=AkaveBlockData(
                        node_id=block.node_id,
                        node_address=block.node_address,
                        permit=block.permit
                    ),
                    filecoin=None
                ))
            
            return FileChunkDownload(
                cid=chunk.cid,
                index=chunk.index,
                encoded_size=chunk.encoded_size,
                size=chunk.size,
                blocks=blocks
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk download: {str(err)}")
            
    def download_chunk_blocks(self, ctx, bucket_name: str, file_name: str, address: str, 
                             chunk_download, file_encryption_key: bytes, writer: io.IOBase):
        try:
            pool = ConnectionPool()
            
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    for i, block in enumerate(chunk_download.blocks):
                        futures[executor.submit(
                            self.fetch_block_data,
                            ctx, pool, chunk_download.cid, bucket_name, file_name, 
                            address, chunk_download.index, i, block
                        )] = i
                    
                    blocks = [None] * len(chunk_download.blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            data = future.result()
                            from .dag import extract_block_data
                            blocks[index] = extract_block_data(chunk_download.blocks[index].cid, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                if self.erasure_code:
                    data = self.erasure_code.extract_data_blocks(blocks, int(chunk_download.size))
                else:
                    data = b"".join([b for b in blocks if b is not None])
                
                if file_encryption_key:
                    from private.encryption import decrypt
                    data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())
                
                writer.write(data)
            finally:
                err = pool.close()
                if err:
                    logging.warning(f"Error closing connection pool: {str(err)}")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download chunk blocks: {str(err)}")