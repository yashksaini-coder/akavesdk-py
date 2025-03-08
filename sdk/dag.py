import io
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from ..private.encryption.encryption import encrypt, decrypt  

def chunk_data(reader, block_size):
    """Splits data into chunks of given block size."""
    while True:
        chunk = reader.read(block_size)
        if not chunk:
            break
        yield chunk

def build_dag(reader: io.BytesIO, block_size: int, enc_key: bytes):
    """Builds a DAG with optional encryption."""
    chunks = []
    
    for chunk in chunk_data(reader, block_size):
        if enc_key:
            info = os.urandom(16)  # Random info for key derivation
            encrypted_chunk = encrypt(enc_key, chunk, info)
            chunks.append((encrypted_chunk, info))  # Storing info with encrypted data
        else:
            chunks.append((chunk, None))
    
    # Simulating DAG structure by linking chunks (normally would use IPFS or IPLD for this)
    dag = {f"node_{i}": {"data": data, "info": info} for i, (data, info) in enumerate(chunks)}
    
    return dag

