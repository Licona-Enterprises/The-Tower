import json
import math
import contextlib
from web3 import Web3
from decimal import Decimal
from typing import Dict, List, Tuple, Any, Optional, Union, Generator, Iterator, ContextManager, TypeVar

# Import ABIs directly from the Python module
from app.backend.contract_abis.uniswapv3_position_calculator_minimal_abis import (
    UNISWAP_V3_POOL_ABI,
    UNISWAP_V3_FACTORY_ABI,
    ERC20_ABI,
    POSITION_MANAGER_ABI,
    ERC721_ABI
)

# Import PORTFOLIOS to get wallet addresses with uniswap active protocol
from app.backend.consts import PORTFOLIOS, UNISWAP_V3_FACTORY_ADDRESS, UNISWAP_V3_POSITIONS_NFT_IDS, RPCS

# Dictionary to store web3 instances for different networks
web3_instances = {}

# Uniswap V3 Factory address
UNISWAP_V3_FACTORY_ADDRESS = Web3.to_checksum_address(UNISWAP_V3_FACTORY_ADDRESS)

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

def get_uniswap_wallet_addresses():
    """
    Get wallet addresses with Uniswap V3 positions from the PORTFOLIOS configuration.
    
    Returns:
        List of wallet info dictionaries with addresses and position NFT IDs
    """
    wallet_addresses = []
    
    print("Searching for Uniswap wallets in PORTFOLIOS config...")
    print(f"Available portfolios: {list(PORTFOLIOS.keys())}")
    
    # Iterate through portfolios and strategies
    for portfolio_name, portfolio_data in PORTFOLIOS.items():
        # Get strategy wallets
        strategy_wallets = portfolio_data.get("STRATEGY_WALLETS", {})
        
        print(f"Portfolio {portfolio_name} has {len(strategy_wallets)} strategy wallets")
        
        for strategy_name, strategy_data in strategy_wallets.items():
            # Check if this wallet has Uniswap as an active protocol
            active_protocols = strategy_data.get("active_protocols", [])
            
            print(f"  Strategy {strategy_name} has active protocols: {active_protocols}")
            
            if "uniswap" in [p.lower() for p in active_protocols]:
                # Get the wallet address
                wallet_address = strategy_data.get("address", "")
                
                # Check if this wallet has Uniswap V3 positions NFT IDs
                nft_ids = strategy_data.get("uniswapv3_positions_nft_ids", [])
                
                # Get the active network for this strategy
                active_networks = strategy_data.get("active_networks", [])
                if not active_networks:
                    print(f"  Warning: No active networks for strategy {strategy_name}")
                    continue
                
                network = active_networks[0]  # Use the first network
                
                print(f"  Strategy {strategy_name} has Uniswap NFT IDs: {nft_ids} on network {network}")
                
                # Only include wallets with both an address and NFT IDs
                if wallet_address and nft_ids:
                    # Create wallet info
                    wallet_info = {
                        "address": wallet_address,
                        "portfolio": portfolio_name,
                        "strategy": strategy_name,
                        "nft_ids": nft_ids,
                        "network": network
                    }
                    
                    wallet_addresses.append(wallet_info)
                    print(f"  Added wallet {wallet_address} to results with network {network}")
    
    print(f"Found {len(wallet_addresses)} wallets with Uniswap positions")
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

def get_sqrt_ratio_at_tick(tick: int) -> int:
    """Calculate sqrtPriceX96 from tick"""
    return int(1.0001 ** (tick / 2) * 2 ** 96)

def get_token_amounts_from_liquidity(
    liquidity: int, 
    tick_lower: int, 
    tick_upper: int, 
    current_sqrt_price_x96: int
) -> Tuple[int, int]:
    """Calculate token amounts from liquidity, tick range, and current price"""
    liquidity = int(liquidity)
    sqrt_ratio_a_x96 = get_sqrt_ratio_at_tick(tick_lower)
    sqrt_ratio_b_x96 = get_sqrt_ratio_at_tick(tick_upper)
    
    # Ensure sqrt_ratio_a_x96 <= sqrt_ratio_b_x96
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96
    
    current_sqrt_price_x96 = int(current_sqrt_price_x96)
    
    if current_sqrt_price_x96 <= sqrt_ratio_a_x96:
        # Only token0
        amount0 = (liquidity * (sqrt_ratio_b_x96 - sqrt_ratio_a_x96)) // (sqrt_ratio_a_x96 * sqrt_ratio_b_x96 // (2**96))
        amount1 = 0
    elif current_sqrt_price_x96 >= sqrt_ratio_b_x96:
        # Only token1
        amount0 = 0
        amount1 = (liquidity * (sqrt_ratio_b_x96 - sqrt_ratio_a_x96)) // (2**96)
    else:
        # Both tokens
        amount0 = (liquidity * (sqrt_ratio_b_x96 - current_sqrt_price_x96)) // (current_sqrt_price_x96 * sqrt_ratio_b_x96 // (2**96))
        amount1 = (liquidity * (current_sqrt_price_x96 - sqrt_ratio_a_x96)) // (2**96)
    
    return amount0, amount1

def format_with_decimals(value: Decimal, decimals: int) -> str:
    """Format a decimal value with the specified number of decimal places"""
    formatted = f"{value:.{decimals}f}"
    # Remove trailing zeros while preserving decimal places up to the token's precision
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.') if '.' in formatted else formatted
    return formatted

def calculate_position_details(token_id: int, position_manager_address: str, network: str) -> Dict[str, Any]:
    """Calculate full details of a Uniswap V3 position"""
    try:
        # Use context managers for contract interactions
        with web3_contract(position_manager_address, POSITION_MANAGER_ABI, network) as position_manager:
            # Get position data
            position = position_manager.functions.positions(token_id).call()
            
            token0_address = position[2]
            token1_address = position[3]
            fee = position[4]
            tick_lower = position[5]
            tick_upper = position[6]
            liquidity = position[7]
            tokensOwed0 = position[10]  # Uncollected fees token0
            tokensOwed1 = position[11]  # Uncollected fees token1
            
            # Get token info
            token0_info = get_token_info(token0_address, network)
            token1_info = get_token_info(token1_address, network)
            
            # Get pool address from factory
            with web3_contract(UNISWAP_V3_FACTORY_ADDRESS, UNISWAP_V3_FACTORY_ABI, network) as factory:
                pool_address = factory.functions.getPool(token0_address, token1_address, fee).call()
                
                if pool_address == '0x0000000000000000000000000000000000000000':
                    return {
                        "error": f"Pool not found for {token0_info['symbol']}/{token1_info['symbol']} with fee {fee/10000}%"
                    }
                
                # Get current price from pool
                with web3_contract(pool_address, UNISWAP_V3_POOL_ABI, network) as pool_contract:
                    slot0 = pool_contract.functions.slot0().call()
                    current_sqrt_price_x96 = slot0[0]
                    current_tick = slot0[1]
        
        # Calculate token amounts
        amount0, amount1 = get_token_amounts_from_liquidity(liquidity, tick_lower, tick_upper, current_sqrt_price_x96)
        
        # Convert to human-readable format with proper decimals
        amount0_decimal = Decimal(amount0) / Decimal(10 ** token0_info['decimals'])
        amount1_decimal = Decimal(amount1) / Decimal(10 ** token1_info['decimals'])
        
        # Convert uncollected fees to proper decimals
        fees0_decimal = Decimal(tokensOwed0) / Decimal(10 ** token0_info['decimals'])
        fees1_decimal = Decimal(tokensOwed1) / Decimal(10 ** token1_info['decimals'])
        
        # Store current tick information for reference
        current_tick_info = current_tick
            
        return {
            "token_id": token_id,
            "position_manager": position_manager_address,
            "token0": {
                "address": token0_address,
                "symbol": token0_info['symbol'],
                "name": token0_info['name'],
                "decimals": token0_info['decimals'],
                "amount": format_with_decimals(amount0_decimal, token0_info['decimals']),
                "uncollected_fees": format_with_decimals(fees0_decimal, token0_info['decimals'])
            },
            "token1": {
                "address": token1_address,
                "symbol": token1_info['symbol'],
                "name": token1_info['name'],
                "decimals": token1_info['decimals'],
                "amount": format_with_decimals(amount1_decimal, token1_info['decimals']),
                "uncollected_fees": format_with_decimals(fees1_decimal, token1_info['decimals'])
            },
            "pool": {
                "address": pool_address,
                "fee": fee / 10000,  # Convert to percentage
                "current_tick": current_tick,
                "current_sqrt_price_x96": str(current_sqrt_price_x96),
            },
            "position": {
                "liquidity": str(liquidity),
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "in_range": tick_lower <= current_tick <= tick_upper
            }
        }
    
    except Exception as e:
        return {"error": f"Error calculating position details for token ID {token_id}: {str(e)}"}

def format_decimal_str(decimal_str: str, token_symbol: str, token_decimals: int) -> str:
    """Format a decimal string for display with appropriate precision based on token and its decimals"""
    parts = decimal_str.split('.')
    
    # Ensure we display the full precision based on token decimals
    if len(parts) == 1:
        # No decimal part, add one with appropriate zeros
        return f"{parts[0]}.{'0' * token_decimals}"
    elif len(parts) == 2:
        # Pad or truncate decimal part to match token's decimals
        return f"{parts[0]}.{parts[1].ljust(token_decimals, '0')[:token_decimals]}"
    
    # Fallback
    return decimal_str

def get_token_ids(wallet_address: str, nft_manager_address: str, network: str) -> Generator[int, None, None]:
    """Generator that yields token IDs owned by the given wallet"""
    with web3_contract(nft_manager_address, ERC721_ABI, network) as erc721_contract:
        balance = erc721_contract.functions.balanceOf(wallet_address).call()
        
        if balance == 0:
            return
            
        for i in range(balance):
            token_id = erc721_contract.functions.tokenOfOwnerByIndex(wallet_address, i).call()
            yield token_id

def process_positions(wallet_info: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    """Generator that processes positions and yields position details"""
    wallet_address = wallet_info["address"]
    network = wallet_info["network"]
    
    for nft_id in wallet_info["nft_ids"]:
        if nft_id in UNISWAP_V3_POSITIONS_NFT_IDS:
            position_manager_address = Web3.to_checksum_address(
                UNISWAP_V3_POSITIONS_NFT_IDS[nft_id]["address"]
            )
            
            print(f"Checking positions using {nft_id} contract at {position_manager_address} on network {network}")
            
            for token_id in get_token_ids(wallet_address, position_manager_address, network):
                position_details = calculate_position_details(token_id, position_manager_address, network)
                yield position_details
        else:
            print(f"Warning: NFT ID '{nft_id}' not found in UNISWAP_V3_POSITIONS_NFT_IDS")

def print_position_summary(position: Dict[str, Any]) -> None:
    """Print a summary of a Uniswap V3 position"""
    print("\n" + "=" * 80)
    print(f"Position ID: {position['token_id']}")
    print(f"Position Manager: {position['position_manager']}")
    print(f"Pool: {position['pool']['address']}")
    print(f"Tokens: {position['token0']['symbol']}/{position['token1']['symbol']}")
    print(f"Fee Tier: {position['pool']['fee']}%")
    print(f"Price Range: {position['position']['tick_lower']} - {position['position']['tick_upper']}")
    
    # Token amounts
    token0_amount = float(position['token0']['amount'])
    token1_amount = float(position['token1']['amount'])
    
    # Assuming we don't have price data in the position object
    # We'll just show the amounts without USD values for now
    print(f"\nToken Amounts:")
    print(f"  {position['token0']['symbol']}: {token0_amount}")
    print(f"  {position['token1']['symbol']}: {token1_amount}")
    
    # Fees
    fees0 = float(position['token0']['uncollected_fees'])
    fees1 = float(position['token1']['uncollected_fees'])
    
    print(f"\nUncollected Fees:")
    print(f"  {position['token0']['symbol']}: {fees0}")
    print(f"  {position['token1']['symbol']}: {fees1}")
    
    # Position status
    liquidity = int(position['position']['liquidity'])
    status = "Active" if liquidity > 0 else "Closed"
    print(f"\nStatus: {status}")
    
    if liquidity > 0:
        current_tick = position['pool']['current_tick']
        tick_lower = position['position']['tick_lower']
        tick_upper = position['position']['tick_upper']
        
        if tick_lower <= current_tick <= tick_upper:
            price_status = "In Range"
        elif current_tick < tick_lower:
            price_status = "Below Range"
        else:
            price_status = "Above Range"
        
        print(f"Current Tick: {current_tick} ({price_status})")
        print(f"Position Range: {tick_lower} to {tick_upper}")
    
    print("=" * 80)

def main():
    """Main function to calculate Uniswap V3 position details"""
    try:
        # Get wallet addresses with NFT IDs
        wallet_addresses = get_uniswap_wallet_addresses()
        
        if not wallet_addresses:
            print("No wallet addresses found for Uniswap")
            return
            
        for wallet_info in wallet_addresses:
            print(f"\nProcessing wallet: {wallet_info['address']}")
            
            try:
                # Use a context manager for the file output
                positions_data = list(process_positions(wallet_info))
                
                if not positions_data:
                    print(f"No positions found for wallet {wallet_info['address']}")
                    continue
                    
                for position in positions_data:
                    if "error" in position:
                        print(f"Error: {position['error']}")
                        continue
                    
                    print_position_summary(position)
                
            except Exception as e:
                print(f"Error processing wallet {wallet_info['address']}: {str(e)}")
    
    except Exception as e:
        print(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main() 