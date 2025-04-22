import json
import contextlib
from web3 import Web3
from decimal import Decimal
from typing import Dict, List, Tuple, Any, Optional, Union, Generator, Iterator, ContextManager, TypeVar

# Import ABIs
from app.backend.contract_abis.aave_abis import ERC20_ABI

# Import constants to get wallet addresses with aave active protocol
from app.backend.consts import PORTFOLIOS, TOKENS, RPCS

# Dictionary to store web3 instances for different networks
web3_instances = {}

T = TypeVar('T')  # Type variable for the contract

def get_web3_instance(network: str) -> Web3:
    """Get or create a Web3 instance for the specified network"""
    if network in web3_instances:
        return web3_instances[network]
    
    if network in RPCS:
        rpc_url = RPCS[network]
        web3_instance = Web3(Web3.HTTPProvider(rpc_url))
        web3_instances[network] = web3_instance
        return web3_instance
    else:
        raise ValueError(f"No RPC URL configured for network: {network}")

def get_aave_wallet_addresses() -> List[Dict[str, Any]]:
    """Get wallet addresses from PORTFOLIOS for Aave processing"""
    wallet_addresses = []
    
    for portfolio_name, portfolio_data in PORTFOLIOS.items():
        # Check if portfolio has STRATEGY_WALLETS
        if "STRATEGY_WALLETS" in portfolio_data:
            for strategy_name, strategy_data in portfolio_data["STRATEGY_WALLETS"].items():
                # Add wallets that explicitly have aave as an active protocol
                if "active_protocols" in strategy_data and "aave" in strategy_data["active_protocols"]:
                    # Get the active network for this strategy
                    networks = []
                    if "active_networks" in strategy_data and strategy_data["active_networks"]:
                        networks = strategy_data["active_networks"]
                    else:
                        print(f"Warning: No active networks defined for strategy {strategy_name} in portfolio {portfolio_name}")
                        continue  # Skip this strategy/wallet
                    
                    wallet_info = {
                        "address": strategy_data["address"],
                        "portfolio": portfolio_name,
                        "strategy": strategy_name,
                        "networks": networks
                    }
                    print(f"Found Aave wallet: {strategy_data['address']} in portfolio {portfolio_name}, networks: {networks}")
                    wallet_addresses.append(wallet_info)
    
    if not wallet_addresses:
        print("No wallets with Aave protocol found in portfolios.")
        
    return wallet_addresses

@contextlib.contextmanager
def web3_contract(address: str, abi: List[Dict], network: str) -> Generator[Any, None, None]:
    """Context manager for web3 contract interactions"""
    try:
        web3 = get_web3_instance(network)
        contract = web3.eth.contract(address=address, abi=abi)
        yield contract
    except Exception as e:
        print(f"Error with contract {address} on network {network}: {e}")
        raise
    finally:
        # Any cleanup if needed in the future
        pass

def get_token_info(token_address: str, network: str) -> Dict[str, Union[str, int]]:
    """Get token name, symbol and decimals"""
    try:
        with web3_contract(token_address, ERC20_ABI, network) as token_contract:
            name = token_contract.functions.name().call()
            symbol = token_contract.functions.symbol().call()
            decimals = token_contract.functions.decimals().call()
            return {"name": name, "symbol": symbol, "decimals": decimals}
    except Exception as e:
        print(f"Error fetching token info for {token_address} on network {network}: {e}")
        return {"name": "Unknown", "symbol": "Unknown", "decimals": 18}

def format_with_decimals(value: Decimal, decimals: int) -> str:
    """Format a decimal value with the specified number of decimal places"""
    formatted = f"{value:.{decimals}f}"
    # Remove trailing zeros while preserving decimal places up to the token's precision
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.') if '.' in formatted else formatted
    return formatted

def get_aave_token_balance(wallet_address: str, token_address: str, network: str) -> Dict[str, Any]:
    """Get balance of an Aave token for a specific wallet"""
    try:
        wallet_address = Web3.to_checksum_address(wallet_address)
        token_address = Web3.to_checksum_address(token_address)
        
        # Get token information
        token_info = get_token_info(token_address, network)
        
        # Get token balance
        with web3_contract(token_address, ERC20_ABI, network) as token_contract:
            balance = token_contract.functions.balanceOf(wallet_address).call()
            
            # Convert to human-readable format with proper decimals
            balance_decimal = Decimal(balance) / Decimal(10 ** token_info['decimals'])
            
            return {
                "address": token_address,
                "symbol": token_info['symbol'],
                "name": token_info['name'],
                "decimals": token_info['decimals'],
                "amount": format_with_decimals(balance_decimal, token_info['decimals']),
                "raw_amount": str(balance)
            }
    except Exception as e:
        print(f"Error getting Aave token balance for {wallet_address} on network {network}: {e}")
        return {
            "address": token_address,
            "symbol": "Unknown",
            "name": "Unknown",
            "decimals": 18,
            "amount": "0",
            "raw_amount": "0",
            "error": str(e)
        }

def get_wallet_aave_positions(wallet_info: Dict[str, Any]) -> Dict[str, Any]:
    """Get all Aave positions for a specific wallet"""
    wallet_address = wallet_info["address"]
    portfolio = wallet_info["portfolio"]
    strategy = wallet_info.get("strategy", "")
    networks = wallet_info["networks"]
    
    positions = {
        "wallet_address": wallet_address,
        "portfolio": portfolio,
        "strategy": strategy,
        "tokens": []
    }
    
    print(f"\nChecking Aave positions for wallet {wallet_address} on networks: {networks}")
    
    # Iterate through each network associated with the wallet
    for network in networks:
        print(f"  Checking network: {network}")
        aave_tokens_found = False
        
        # Find all Aave tokens for this network in the TOKENS dictionary
        for token_category, token_data in TOKENS.items():
            if token_category == "AAVE":
                # Find tokens for the current network
                for token_name, token_info in token_data.items():
                    # Check if token belongs to the current network
                    if network.lower() in token_name.lower():
                        aave_tokens_found = True
                        print(f"    Found Aave token: {token_name} ({token_info['address']})")
                        
                        # Get token balance
                        token_balance = get_aave_token_balance(
                            wallet_address, 
                            token_info["address"], 
                            network
                        )
                        
                        # Add network information to the token data
                        token_balance["network"] = network
                        token_balance["token_id"] = token_name
                        
                        # Only include tokens with non-zero balance
                        if token_balance.get("amount") != "0":
                            positions["tokens"].append(token_balance)
                            print(f"      Balance: {token_balance.get('amount')}")
                        else:
                            print(f"      Balance: 0 (not including in results)")
        
        if not aave_tokens_found:
            print(f"    No Aave tokens found for network: {network}")
    
    return positions

def process_aave_positions() -> List[Dict[str, Any]]:
    """Process all Aave positions for all wallets"""
    all_positions = []
    
    # Get all wallets with Aave as active protocol
    wallets = get_aave_wallet_addresses()
    
    for wallet_info in wallets:
        try:
            wallet_positions = get_wallet_aave_positions(wallet_info)
            all_positions.append(wallet_positions)
            print_position_summary(wallet_positions)
        except Exception as e:
            print(f"Error processing wallet {wallet_info['address']}: {e}")
    
    return all_positions

def print_position_summary(position: Dict[str, Any]) -> None:
    """Print a summary of a wallet's Aave positions"""
    wallet_address = position["wallet_address"]
    portfolio = position["portfolio"]
    strategy = position["strategy"]
    tokens = position["tokens"]
    
    print(f"\n============= Aave Positions for {wallet_address} =============")
    print(f"Portfolio: {portfolio}")
    print(f"Strategy: {strategy}")
    
    if not tokens:
        print("No Aave tokens found for this wallet.")
        return
    
    print("\nToken Details:")
    for token in tokens:
        print(f"  {token['symbol']} ({token['token_id']})")
        print(f"    Network: {token['network']}")
        print(f"    Amount: {token['amount']}")
        print(f"    Address: {token['address']}")
        print("")

def main():
    """Main function to process all Aave positions"""
    print("Starting Aave position calculator...")
    all_positions = process_aave_positions()
    
    # Optionally, save the results to a JSON file
    with open("aave_positions.json", "w") as f:
        json.dump(all_positions, f, indent=2)
    
    print(f"\nProcessed {len(all_positions)} wallets with Aave positions.")
    print("Results saved to aave_positions.json")

if __name__ == "__main__":
    main() 