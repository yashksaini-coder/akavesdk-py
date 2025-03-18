import math
from reedsolo import RSCodec, ReedSolomonError
from itertools import combinations

def missing_shards_idx(n, k):
    return [list(combo) for combo in combinations(range(n), k)]

def split_into_blocks(encoded: bytes, shard_size: int):
    blocks = []
    for offset in range(0, len(encoded), shard_size):
        block = encoded[offset: offset + shard_size]
        if len(block) < shard_size:
            block = block.ljust(shard_size, b'\x00')
        blocks.append(block)
    return blocks

class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        if data_blocks <= 0 or parity_blocks <= 0:
            raise ValueError("Data and parity shards must be > 0")
        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self.total_shards = data_blocks + parity_blocks

    @classmethod
    def new(cls, data_blocks: int, parity_blocks: int):
        return cls(data_blocks, parity_blocks)

    def encode(self, data: bytes) -> bytes:
        shard_size = math.ceil(len(data) / self.data_blocks)
        padded_data = data.ljust(self.data_blocks * shard_size, b'\x00')
        nsym = self.parity_blocks * shard_size
        rsc = RSCodec(nsym)
        encoded = rsc.encode(padded_data)
        expected_len = self.total_shards * shard_size
        if len(encoded) < expected_len:
            encoded = encoded.ljust(expected_len, b'\x00')
        return encoded

    def extract_data(self, encoded: bytes, original_data_size: int, erase_pos=None) -> bytes:
        shard_size = len(encoded) // self.total_shards
        nsym = self.parity_blocks * shard_size
        rsc = RSCodec(nsym)
        try:
            if erase_pos is not None:
                decoded, _, _ = rsc.decode(encoded, erase_pos=erase_pos)
            else:
                decoded, _, _ = rsc.decode(encoded)
            return decoded[:original_data_size]
        except ReedSolomonError as e:
            raise ValueError("Decoding error: " + str(e))

    def extract_data_blocks(self, blocks, original_data_size: int) -> bytes:
        if not blocks:
            raise ValueError("No blocks provided")
        shard_size = None
        for b in blocks:
            if b is not None:
                shard_size = len(b)
                break
        if shard_size is None:
            raise ValueError("All blocks are missing")
        if len(blocks) != self.total_shards:
            raise ValueError(f"Expected {self.total_shards} blocks, got {len(blocks)}")
        erase_pos = []
        for i, block in enumerate(blocks):
            if block is None:
                start = i * shard_size
                erase_pos.extend(range(start, start + shard_size))
        fixed_blocks = [block if block is not None else b'\x00' * shard_size for block in blocks]
        encoded = b"".join(fixed_blocks)
        return self.extract_data(encoded, original_data_size, erase_pos=erase_pos)
