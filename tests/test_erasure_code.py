import sys 
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import numpy as np
from itertools import combinations
from sdk.erasure_code import ErasureCode  

class TestErasureCode(unittest.TestCase):

    def test_erasure_code_invalid_params(self):
        with self.assertRaises(ValueError):
            ErasureCode(0, 0)

        with self.assertRaises(ValueError):
            ErasureCode(16, 0)

    def test_erasure_code(self):
        data = b"Quick brown fox jumps over the lazy dog"
        data_shards = 5
        parity_shards = 3

        encoder = ErasureCode(data_shards, parity_shards)
        self.assertEqual(encoder.data_blocks, data_shards)
        self.assertEqual(encoder.parity_blocks, parity_shards)

        encoded = encoder.encode(data)
        shard_size = len(encoded[0])  

        def split_into_blocks(encoded_shards):
            return [bytearray(shard) for shard in encoded_shards]

        def missing_shards_idx(n, k):
            return list(combinations(range(n), k))

        with self.subTest("no missing shards"):
            blocks = split_into_blocks(encoded)
            extracted = encoder.extract_data(blocks, len(data))
            self.assertEqual(data, extracted)

        with self.subTest(f"missing no more than {parity_shards} shards"):
            all_combos = []
            for k in range(1, parity_shards + 1):
                all_combos.extend(missing_shards_idx(data_shards + parity_shards, k))

            encoded = encoder.encode(data)

            for missing_idxs in all_combos:
                blocks = split_into_blocks(encoded)

                for idx in missing_idxs:
                    blocks[idx] = None #Marked shard as missing

                extracted = int.from_bytes(encoder.extract_data(blocks, len(data)),byteorder='big')
                self.assertEqual(data, extracted)

        # Missing more than parity shards
        with self.subTest(f"missing more than {parity_shards} shards"):
         blocks = split_into_blocks(encoded)
    # Mark more than parity_shards as missing (None)
        for i in range(parity_shards + 1):
         blocks[i] = None
        with self.assertRaises(RuntimeError):
         encoder.extract_data(blocks, len(data))



if __name__ == "__main__":
    unittest.main()
