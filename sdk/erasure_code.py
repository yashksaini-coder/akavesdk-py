import io
import numpy as np
from reedsolo import RSCodec, ReedSolomonError

class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        if data_blocks <= 0 or parity_blocks <= 0:
            raise ValueError("Data and parity shards must be > 0")

        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self.encoder = RSCodec(parity_blocks * 2)  

    def encode(self, data: bytes) -> bytes:
        try:
            shards = np.array_split(data, self.data_blocks)
            encoded_shards = [self.encoder.encode(shard.tobytes()) for shard in shards]

            return b"".join(encoded_shards)
        except ReedSolomonError as e:
            raise RuntimeError(f"Erasure coding failed: {str(e)}")

    def extract_data(self, encoded_data: bytes, original_data_size: int) -> bytes:
        """Extracts the original data from encoded data."""
        try:
            shard_size = original_data_size // self.data_blocks
            shards = [encoded_data[i : i + shard_size + self.parity_blocks * 2] for i in range(0, len(encoded_data), shard_size + self.parity_blocks * 2)]

            decoded_shards = [self.encoder.decode(shard) for shard in shards]

            return b"".join(decoded_shards)[:original_data_size]  
        except ReedSolomonError as e:
            raise RuntimeError(f"Data reconstruction failed: {str(e)}")
