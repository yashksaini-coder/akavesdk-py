import zfec

class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self.total_blocks = data_blocks + parity_blocks
        self.fec = zfec.Encoder(data_blocks, self.total_blocks)

    def encode(self, data: bytes) -> list:
        # Split data into data_blocks shards
        shard_size = (len(data) + self.data_blocks - 1) // self.data_blocks
        padded_data = data.ljust(shard_size * self.data_blocks, b'\0')
        shards = [padded_data[i*shard_size:(i+1)*shard_size] for i in range(self.data_blocks)]
        # Generate parity shards
        return self.fec.encode(shards)

    def decode(self, shards, shard_ids):
        # Requires at least data_blocks shards to recover
        decoder = zfec.Decoder(self.data_blocks, self.total_blocks)
        return decoder.decode(shards, shard_ids)

    def extract_data(self, blocks, original_data_size):
        present_shards = []
        present_indices = []
        for idx, shard in enumerate(blocks):
            if shard is not None:
                present_shards.append(shard)
                present_indices.append(idx)
        if len(present_shards) < self.data_blocks:
            raise RuntimeError("Not enough shards to recover data")
        decoded_shards = self.decode(present_shards, present_indices)
        combined = b''.join(decoded_shards)[:original_data_size]
        return combined