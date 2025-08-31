"""
PolicyFactory contract bindings for Python.
This file is auto-generated - DO NOT EDIT manually.
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple
from eth_typing import Address, HexStr, HexAddress
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount
from eth_account import Account


class PolicyFactoryMetaData:
    """Metadata for the PolicyFactory contract."""
    
    ABI = [
        {
            "inputs": [{"internalType": "address", "name": "_basePolicyImplementation", "type": "address"}],
            "stateMutability": "nonpayable",
            "type": "constructor"
        },
        {
            "inputs": [],
            "name": "FailedDeployment",
            "type": "error"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "balance", "type": "uint256"},
                {"internalType": "uint256", "name": "needed", "type": "uint256"}
            ],
            "name": "InsufficientBalance",
            "type": "error"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "policyInstance", "type": "address"}
            ],
            "name": "PolicyDeployed",
            "type": "event"
        },
        {
            "inputs": [],
            "name": "basePolicyImplementation",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "bytes", "name": "initData", "type": "bytes"}],
            "name": "deployPolicy",
            "outputs": [{"internalType": "address", "name": "policyInstance", "type": "address"}],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ]

    BIN = "0x60a0604052348015600e575f5ffd5b506040516103d83803806103d8833981016040819052602b91603b565b6001600160a01b03166080526066565b5f60208284031215604a575f5ffd5b81516001600160a01b0381168114605f575f5ffd5b9392505050565b6080516103556100835f395f8181603d0152608f01526103555ff3fe608060405234801561000f575f5ffd5b5060043610610034575f3560e01c8063200afae814610038578063b8dc780f1461007b575b5f5ffd5b61005f7f000000000000000000000000000000000000000000000000000000000000000081565b6040516001600160a01b03909116815260200160405180910390f35b61005f610089366004610256565b5f6100b37f000000000000000000000000000000000000000000000000000000000000000061019d565b90505f816001600160a01b0316836040516100ce9190610309565b5f604051808303815f865af19150503d805f8114610107576040519150601f19603f3d011682016040523d82523d5f602084013e61010c565b606091505b50509050806101625760405162461bcd60e51b815260206004820152601c60248201527f506f6c69637920696e697469616c697a6174696f6e206661696c65640000000060448201526064015b60405180910390fd5b6040516001600160a01b0383169033907f87ba47a73518e5c03313f0d265288539fb71194e940ca6698184d22ae045ef95905f90a350919050565b5f6101a8825f6101ae565b92915050565b5f814710156101d95760405163cf47918160e01b815247600482015260248101839052604401610159565b763d602d80600a3d3981f3363d3d373d3d3d363d730000008360601b60e81c175f526e5af43d82803e903d91602b57fd5bf38360781b176020526037600983f090506001600160a01b0381166101a85760405163b06ebf3d60e01b815260040160405180910390fd5b634e487b7160e01b5f52604160045260245ffd5b5f60208284031215610266575f5ffd5b813567ffffffffffffffff81111561027c575f5ffd5b8201601f8101841361028c575f5ffd5b803567ffffffffffffffff8111156102a6576102a6610242565b604051601f8201601f19908116603f0116810167ffffffffffffffff811182821017156102d5576102d5610242565b6040528181528282016020018610156102ec575f5ffd5b816020840160208301375f91810160200191909152949350505050565b5f82518060208501845e5f92019182525091905056fea2646970667358221220de4a1c12bba0e6de7bde317336cee926549e661147e4d4d13ada2d879a85047d64736f6c634300081c0033"


class PolicyFactoryContract:
    """Main class for interacting with the PolicyFactory contract."""
    
    def __init__(self, w3: Web3, address: str):
        """Initialize PolicyFactory contract interface."""
        self.w3 = w3
        self.address = Web3.to_checksum_address(address)
        self.contract = w3.eth.contract(
            address=self.address,
            abi=PolicyFactoryMetaData.ABI
        )
    
    # View functions
    def base_policy_implementation(self) -> str:
        """Get the base policy implementation address."""
        return self.contract.functions.basePolicyImplementation().call()
    
    # Transaction functions
    def deploy_policy(self, account: LocalAccount, init_data: bytes, gas_limit: int = 500000) -> str:
        """Deploy a new policy contract."""
        tx = self.contract.functions.deployPolicy(init_data).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    # Event filtering
    def get_policy_deployed_events(self, from_block: int = 0, to_block: int = 'latest'):
        """Get PolicyDeployed events."""
        return self.contract.events.PolicyDeployed.get_logs(
            fromBlock=from_block, 
            toBlock=to_block
        )


def new_policy_factory(w3: Web3, address: str) -> PolicyFactoryContract:
    """Create a new PolicyFactory contract interface."""
    return PolicyFactoryContract(w3, address)


def deploy_policy_factory(w3: Web3, account: LocalAccount, base_policy_implementation: str, gas_limit: int = 2000000) -> Tuple[str, str]:
    """
    Deploy a new PolicyFactory contract.
    
    Returns:
        Tuple of (contract_address, transaction_hash)
    """
    contract_factory = w3.eth.contract(
        abi=PolicyFactoryMetaData.ABI,
        bytecode=PolicyFactoryMetaData.BIN
    )
    
    tx = contract_factory.constructor(base_policy_implementation).build_transaction({
        'from': account.address,
        'gas': gas_limit,
        'nonce': w3.eth.get_transaction_count(account.address),
    })
    
    signed_tx = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    
    # Wait for transaction receipt
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    
    return receipt.contractAddress, tx_hash.hex()