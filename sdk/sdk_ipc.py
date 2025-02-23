import time
import logging
from hashlib import sha256

from .sdk import MIN_BUCKET_NAME_LENGTH

class IPC:
    def __init__(self, client, conn, ipc, max_concurrency, block_part_size, use_connection_pool, encryption_key=None):
        self.client = client
        self.conn = conn
        self.ipc = ipc
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key if encryption_key else b''

    def create_bucket(self, ctx, name):
        try:
            if len(name) < MIN_BUCKET_NAME_LENGTH:
                raise ValueError("Invalid bucket name")

            # Create the bucket using the IPC storage client
            tx = self.ipc.storage.create_bucket(self.ipc.auth, name)
            if tx is None:
                raise Exception("Failed to create bucket")

            # Wait for the transaction to be completed
            self.ipc.wait_for_tx(ctx, tx.hash())

            # Retrieve the bucket by name after the transaction is successful
            bucket = self.ipc.storage.get_bucket_by_name(self.ipc.auth.from_address, name)
            if bucket is None:
                raise Exception("Failed to retrieve the bucket")

            # Return the bucket creation result
            return {
                "name": sha256(bucket.id).hexdigest(),  # Assuming bucket.id is in a format that needs encoding like in Go's hex.EncodeToString
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bucket.created_at))  # Assuming bucket.created_at is in Unix timestamp format
            }

        except Exception as err:
            logging.error(f"Error creating bucket: {str(err)}")
            raise

    def view_bucket(self, ctx, bucket_name):
        try:
            if not bucket_name:
                raise ValueError("Empty bucket name")

            res = self.client.bucket_view(ctx, {
                "name": bucket_name,
                "address": self.ipc.auth.from_address
            })

            return {
                "id": res.get("id"),
                "name": res.get("name"),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(res.get("created_at")))
            }
        except Exception as err:
            logging.error(f"Error viewing bucket: {str(err)}")
            raise

    def list_buckets(self, ctx):
        try:
            res = self.client.bucket_list(ctx, {
                "address": self.ipc.auth.from_address
            })
            return [{
                "name": bucket.get("name"),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bucket.get("created_at")))
            } for bucket in res.get("buckets", [])]
        except Exception as err:
            logging.error(f"Error listing buckets: {str(err)}")
            raise

    def delete_bucket(self, ctx, name):
        try:
            bucket = self.client.bucket_view(ctx, {
                "name": name,
                "address": self.ipc.auth.from_address
            })
            bucket_id = bytes.fromhex(bucket["id"])
            bucket_idx = self.ipc.storage.get_bucket_index_by_name(name, self.ipc.auth.from_address)

            tx = self.ipc.storage.delete_bucket(self.ipc.auth, bucket_id, name, bucket_idx)
            self.ipc.wait_for_tx(ctx, tx.hash())
        except Exception as err:
            logging.error(f"Error deleting bucket: {str(err)}")
            raise

    def file_info(self, ctx, bucket_name, file_name):
        try:
            res = self.client.file_view(ctx, {
                "bucket_name": bucket_name,
                "file_name": file_name,
                "address": self.ipc.auth.from_address
            })
            return {
                "root_cid": res.get("root_cid"),
                "name": res.get("file_name"),
                "bucket_name": res.get("bucket_name"),
                "encoded_size": res.get("encoded_size"),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(res.get("created_at")))
            }
        except Exception as err:
            logging.error(f"Error retrieving file info: {str(err)}")
            raise

    def list_files(self, ctx, bucket_name):
        try:
            if not bucket_name:
                raise ValueError("Empty bucket name")

            resp = self.client.file_list(ctx, {
                "bucket_name": bucket_name,
                "address": self.ipc.auth.from_address
            })

            return [{
                "root_cid": file_meta.get("root_cid"),
                "name": file_meta.get("name"),
                "encoded_size": file_meta.get("encoded_size"),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(file_meta.get("created_at")))
            } for file_meta in resp.get("list", [])]
        except Exception as err:
            logging.error(f"Error listing files: {str(err)}")
            raise

    def file_delete(self, ctx, bucket_name, file_name):
        try:
            if not bucket_name.strip() or not file_name.strip():
                raise ValueError("Empty bucket or file name")

            bucket = self.ipc.storage.get_bucket_by_name(bucket_name)
            file = self.ipc.storage.get_file_by_name(bucket["id"], file_name)
            file_idx = self.ipc.storage.get_file_index_by_id(file_name, bucket["id"])

            tx = self.ipc.storage.delete_file(self.ipc.auth, file["id"], bucket["id"], file_name, file_idx)
            self.ipc.wait_for_tx(ctx, tx.hash())
        except Exception as err:
            logging.error(f"Error deleting file: {str(err)}")
            raise
