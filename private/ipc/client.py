import time
from typing import Optional
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from eth_account.signers.local import LocalAccount
from .contracts import StorageContract, AccessManagerContract

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

class Client:
    """Represents the Ethereum storage client."""
    def __init__(self, web3: Web3, auth: LocalAccount, storage: StorageContract, access_manager: Optional[AccessManagerContract] = None):
        self.web3 = web3
        self.auth = auth
        self.storage = storage
        self.access_manager = access_manager
        self.ticker = 0.2  # 200ms polling interval

    @classmethod
    def dial(cls, config: Config) -> 'Client':
        """Creates a new IPC client with the given configuration.
        
        Args:
            config: Client configuration
            
        Returns:
            A new IPC client instance
        """
        # Initialize Web3
        web3 = Web3(Web3.HTTPProvider(config.dial_uri))
        if not web3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")

        # Add POA middleware for POA chains
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # Create account from private key
        account = Account.from_key(config.private_key)

        # Initialize contracts
        storage = StorageContract(web3, config.storage_contract_address)
        access_manager = None
        if config.access_contract_address:
            access_manager = AccessManagerContract(web3, config.access_contract_address)

        return cls(web3, account, storage, access_manager)

    @classmethod
    def deploy_storage(cls, config: Config):
        """Deploys a storage smart contract and returns its client and address."""

        web3 = Web3(Web3.HTTPProvider(config.dial_uri))

        if not web3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")

        # Add POA middleware for POA chains
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        account = Account.from_key(config.private_key)

        # Get current gas price
        gas_price = web3.eth.gas_price

        # Deploy Storage Contract
        storage_contract = web3.eth.contract(abi=contracts.Storage.abi, bytecode=contracts.Storage.bytecode)
        tx = storage_contract.constructor().build_transaction({
            'from': account.address,
            'gas': 5000000,
            'gasPrice': gas_price,
            'nonce': web3.eth.get_transaction_count(account.address)
        })
        signed_tx = account.sign_transaction(tx)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        storage_receipt = cls.wait_for_tx(web3, tx_hash)
        storage_address = storage_receipt.contractAddress

        # Deploy Access Manager Contract
        access_manager_contract = web3.eth.contract(abi=contracts.AccessManager.abi, bytecode=contracts.AccessManager.bytecode)
        tx = access_manager_contract.constructor(storage_address).build_transaction({
            'from': account.address,
            'gas': 5000000,
            'gasPrice': gas_price,
            'nonce': web3.eth.get_transaction_count(account.address)
        })
        signed_tx = account.sign_transaction(tx)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        access_manager_receipt = cls.wait_for_tx(web3, tx_hash)
        access_manager_address = access_manager_receipt.contractAddress

        storage_instance = web3.eth.contract(address=storage_address, abi=contracts.Storage.abi)
        access_manager_instance = web3.eth.contract(address=access_manager_address, abi=contracts.AccessManager.abi)

        return cls(web3, account, storage_instance, access_manager_instance), storage_address

    def wait_for_tx(self, ctx, tx_hash: str) -> bool:
        """Waits for a transaction to be mined.
        
        Args:
            ctx: Context for cancellation
            tx_hash: Transaction hash to wait for
            
        Returns:
            True if transaction was successful, False otherwise
        """
        try:
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            return receipt.status == 1
        except Exception as e:
            return False
