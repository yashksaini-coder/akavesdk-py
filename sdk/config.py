from typing import Optional, List
from private.memory.memory import Size

BLOCK_SIZE = 1 * Size.MB
ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_BUCKET_NAME_LENGTH = 3
MIN_FILE_SIZE = 127  # 127 bytes


# List of known error strings from the smart contracts
# Replace these with the actual error strings from your contracts
_KNOWN_ERROR_STRINGS: List[str] = [
    "Storage: bucket doesn't exist",
    "Storage: bucket exists",
    "Storage: file doesn't exist",
    "Storage: file exists",
    "AccessManager: caller is not the owner",
    "AccessManager: caller is not authorized",
    # Add all other known error strings here...
]

# Default config for the streaming connection test 
DEFAULT_CONFIG = {
    'AKAVE_SDK_NODE': 'connect.akave.ai:5000',  
    'ENCRYPTION_KEY': '',  
}

# Default CID version and codecs for IPFS
# used in the DAG operations
DEFAULT_CID_VERSION = 1
DAG_PB_CODEC = 0x70
RAW_CODEC = 0x55


class Config:
    """Configuration for the Ethereum storage contract client."""
    def __init__(self, dial_uri: str, private_key: str, storage_contract_address: str, access_contract_address: Optional[str] = None):
        self.dial_uri = dial_uri
        self.private_key = private_key
        self.storage_contract_address = storage_contract_address
        self.access_contract_address = access_contract_address

    @staticmethod
    def default():
        return Config(dial_uri="", private_key="", storage_contract_address="", access_contract_address="")


class SDKError(Exception):
    pass 


## Validation 

# Basic validation: expect hex string like '0x' + 8 hex chars (4 bytes) minimum

def validate_hex_string(hex_string: str) -> bool:
    if not hex_string.startswith("0x"):
        return False
    if len(hex_string) < 10:
        return False
    return True