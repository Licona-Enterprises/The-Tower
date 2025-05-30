import requests
import polars as pl
import os
import logging
import sys
import datetime
from dotenv import load_dotenv
from app.backend.consts import PORTFOLIOS, TOKENS, BASE_URLS, UNISWAP_V3_POOLS
from typing import Dict, List, Generator, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("uniswap_reconciliation.log"), # Changed log file name
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def get_uniswap_wallets(portfolios, network="polygon"): # Renamed function
    try:
        wallets = []
        for portfolio_data in portfolios.values():
            strategy_wallets = portfolio_data.get("STRATEGY_WALLETS", {})
            for wallet_info in strategy_wallets.values(): # Changed variable name for clarity
                # Filter for "uniswap" and the specified network
                if "uniswap" in wallet_info.get("active_protocols", []) and \
                   network in wallet_info.get("active_networks", []):
                    wallets.append(wallet_info["address"])
        
        if not wallets:
            logger.warning(f"No Uniswap wallets found for network {network}")
        else:
            logger.info(f"Found {len(wallets)} Uniswap wallets for network {network}")
        
        return wallets
    except Exception as e:
        logger.error(f"Error getting Uniswap wallets: {str(e)}")
        return []

def get_all_token_contracts(tokens_config: Dict, network: str) -> List[Dict]:
    """Get all relevant token contracts for the specified network."""
    logger.debug(f"Starting get_all_token_contracts for network: {network}")
    processed_tokens_list = [] # Renamed to avoid conflict with outer scope 'tokens'
    network_lower = network.lower()

    def check_and_add(token_data: Dict, token_key: str, current_network_lower: str):
        # Helper to check if token is active and add it
        if not (isinstance(token_data, dict) and all(k in token_data for k in ["address", "decimals", "symbol"])):
            logger.debug(f"Skipping {token_key} - not a valid token data structure.")
            return

        active_networks_val = token_data.get("active_networks")
        is_active_on_network = False
        
        if isinstance(active_networks_val, str):
            is_active_on_network = active_networks_val.lower() == current_network_lower
        elif isinstance(active_networks_val, list):
            is_active_on_network = current_network_lower in [n.lower() for n in active_networks_val]
        
        # Fallback: if active_networks is missing or doesn't match, check if network name is in the token_key itself
        if not is_active_on_network and current_network_lower in token_key.lower():
             is_active_on_network = True
             logger.debug(f"Token {token_key} matched network {current_network_lower} by key as fallback.")

        if is_active_on_network:
            # Add a copy to avoid modifying the original TOKENS structure if we were to add keys like 'original_key' here
            processed_tokens_list.append(token_data.copy())
            logger.debug(f"Added token for general fetch: {token_data.get('symbol')} ({token_data.get('address')}) from key {token_key}")

    for key, value in tokens_config.items():
        # Scenario 1: The value is directly a token definition dictionary (e.g., TOKENS["ARBITRUM_WETH"])
        if isinstance(value, dict) and all(k in value for k in ["address", "decimals", "symbol"]):
            check_and_add(value, key, network_lower)
        # Scenario 2: The value is a dictionary of token definitions (a category like TOKENS["AAVE"])
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                # sub_value should be the actual token definition dict
                check_and_add(sub_value, sub_key, network_lower)
    
    # Remove duplicates based on contract address to ensure each token is processed once by fetch_transactions
    unique_addresses = set()
    unique_tokens = []
    for token in processed_tokens_list:
        if token["address"].lower() not in unique_addresses:
            unique_addresses.add(token["address"].lower())
            unique_tokens.append(token)
    
    logger.info(f"Found {len(unique_tokens)} unique token contracts for network {network} relevant to Uniswap strategies (general scan).")
    return unique_tokens

def find_token_definition_by_key(tokens_config: Dict, target_key_to_find: str, network_context: Optional[str] = None) -> Optional[Dict]:
    """
    Searches the tokens_config for a token definition that matches target_key_to_find.
    A token definition is a dict with "address", "decimals", "symbol".
    The target_key_to_find is the exact key as it appears, e.g., in uniswapv3_pools.
    """
    # Check top-level keys first
    if target_key_to_find in tokens_config:
        item = tokens_config[target_key_to_find]
        if isinstance(item, dict) and all(k in item for k in ["address", "decimals", "symbol"]):
            # Optionally, verify if this token is active on the network_context if provided
            if network_context:
                active_networks = item.get("active_networks", [])
                if isinstance(active_networks, str): active_networks = [active_networks]
                if network_context.lower() not in [n.lower() for n in active_networks] and \
                   network_context.upper() not in target_key_to_find.upper(): # Fallback check in key name
                    logger.debug(f"Token key '{target_key_to_find}' found but not active on network '{network_context}'. Skipping.")
                    return None
            return item

    # If not found at top-level, check nested dictionaries (categories like "LP_TOKENS", "STABLECOINS" etc.)
    for category_name, category_data in tokens_config.items():
        if isinstance(category_data, dict):
            if target_key_to_find in category_data:
                item = category_data[target_key_to_find]
                if isinstance(item, dict) and all(k in item for k in ["address", "decimals", "symbol"]):
                    if network_context:
                        active_networks = item.get("active_networks", [])
                        if isinstance(active_networks, str): active_networks = [active_networks]
                        if network_context.lower() not in [n.lower() for n in active_networks] and \
                           network_context.upper() not in target_key_to_find.upper(): # Fallback check in key name
                             logger.debug(f"Token key '{target_key_to_find}' in category '{category_name}' found but not active on network '{network_context}'. Skipping.")
                             return None
                    return item
    
    logger.warning(f"Token definition for key '{target_key_to_find}' not found in TOKENS config or not active on network '{network_context}'.")
    return None

def get_network_active_tokens_for_columns(tokens_config: Dict, network_name: str) -> List[Dict]:
    """
    Identifies all tokens from tokens_config that are active on the specified network.
    Used to determine which assets need dedicated price columns in the output.
    (This function is largely reusable as is, assuming TOKENS structure is consistent)
    """
    network_specific_tokens = []
    processed_addresses = set() 

    def check_and_add_token(token_data: Dict, token_key: str, network_name_lower: str):
        if isinstance(token_data, dict) and all(k in token_data for k in ["address", "decimals", "symbol"]):
            active_networks_val = token_data.get("active_networks")
            is_active_on_network = False
            
            if isinstance(active_networks_val, str):
                is_active_on_network = active_networks_val.lower() == network_name_lower
            elif isinstance(active_networks_val, list):
                is_active_on_network = network_name_lower in [n.lower() for n in active_networks_val]
            
            if not is_active_on_network and network_name_lower in token_key.lower():
                is_active_on_network = True
                logger.debug(f"Token {token_key} matched network {network_name_lower} by key as fallback for pricing columns.")

            if is_active_on_network:
                address = token_data["address"].lower()
                if address not in processed_addresses:
                    token_info = token_data.copy()
                    token_info["original_key"] = token_key 
                    network_specific_tokens.append(token_info)
                    processed_addresses.add(address)
                    logger.debug(f"Added token {token_key} (address: {address}) for network {network_name_lower} pricing columns.")

    network_name_lower = network_name.lower()
    for key, value in tokens_config.items():
        if isinstance(value, dict) and all(k in value for k in ["address", "decimals", "symbol"]):
            check_and_add_token(value, key, network_name_lower)
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict) and all(k in sub_value for k in ["address", "decimals", "symbol"]):
                    check_and_add_token(sub_value, sub_key, network_name_lower)
    
    logger.info(f"Found {len(network_specific_tokens)} distinct tokens active on network '{network_name}' for pricing columns.")
    return network_specific_tokens

def fetch_transactions(address, contract_address, api_key, network="polygon", page=1, offset=1000):
    """Fetch transactions from the block explorer API. (Largely reusable)"""
    try:
        api_url = BASE_URLS.get(network)
        if not api_url:
            logger.error(f"Error: Network {network} not supported")
            return []
            
        params = {
            "module": "account",
            "action": "tokentx", # This action should be suitable for most ERC20 tokens including LP tokens
            "contractaddress": contract_address,
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": offset,
            "sort": "asc",
            "apikey": api_key
        }
        
        response = requests.get(api_url, params=params, timeout=30, verify=False)
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") != "1":
            # It's common for this API to return "No transactions found" with status "0" if a wallet has no txs for a specific token
            if data.get("message") == "No transactions found":
                 logger.info(f"No transactions found for {address} with token {contract_address}")
            else:
                logger.warning(f"API returned non-success status for {address} with token {contract_address}: {data.get('message')} (Status: {data.get('status')})")
            return []
            
        logger.info(f"Fetched {len(data.get('result', []))} transactions for address {address} with token {contract_address}")
        return data.get("result", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching transactions for {address} with token {contract_address}: {str(e)}")
        return []
    except ValueError as e: # Includes JSONDecodeError
        logger.error(f"JSON decoding error for {address} with token {contract_address}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching transactions for {address}, token {contract_address}: {str(e)}")
        return []

def get_wallet_metadata(portfolios, address):
    """Get portfolio and strategy name for a wallet address. (Reusable)"""
    for portfolio_name, portfolio_data in portfolios.items():
        strategy_wallets = portfolio_data.get("STRATEGY_WALLETS", {})
        for strategy_name, wallet in strategy_wallets.items():
            if wallet.get("address", "").lower() == address.lower():
                return {
                    "portfolio_name": portfolio_name,
                    "strategy_name": strategy_name
                }
    return {
        "portfolio_name": "Unknown",
        "strategy_name": "Unknown"
    }

def stream_wallet_transactions(wallet, token_contracts, api_key, network="polygon"):
    """
    Generator that streams transactions for a wallet without loading all into memory.
    Yields one transaction at a time for each token in token_contracts where the wallet is involved.
    This includes:
    1. All transactions for general network-active ERC20 tokens involving the wallet.
    2. All transactions for Uniswap pool contract addresses (if configured and treated as tokens for tx fetching) involving the wallet.
    """
    logger.info(f"Streaming ALL transactions for wallet {wallet} for {len(token_contracts)} configured tokens/pools on {network}")
    
    wallet_metadata = get_wallet_metadata(PORTFOLIOS, wallet)
    
    for token in token_contracts: # token_contracts is final_token_contracts from main
        logger.info(f"Processing token: {token['symbol']} ({token['address']}) for wallet {wallet}")
        # current_asset_address = token["address"].lower() # No longer needed for a separate filter logic here
        try:
            txs = fetch_transactions(wallet, token["address"], api_key, network)
            
            for tx in txs:
                try:
                    # No more complex filtering here; if fetch_transactions returned it for this token, we process it.
                    
                    # Ensure token decimals are available and valid
                    if token.get("decimals") is None:
                        logger.warning(f"Token {token['symbol']} missing 'decimals' field. Skipping value calculation for tx {tx.get('hash', 'unknown')}.")
                        value = None 
                    else:
                        value = int(tx["value"]) / (10 ** token["decimals"])
                    
                    yield {
                        "wallet": wallet,
                        "portfolio_name": wallet_metadata["portfolio_name"],
                        "strategy_name": wallet_metadata["strategy_name"],
                        "network": network,
                        "token_symbol_onchain": tx["tokenSymbol"],
                        "token_symbol_config": token["symbol"],   
                        "token_address": token["address"], # This is the address of the token contract being processed
                        "value": value,
                        "from": tx["from"],
                        "to": tx["to"],
                        "hash": tx["hash"],
                        "timestamp": tx["timeStamp"]
                    }
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error processing transaction {tx.get('hash', 'unknown')} for token {token['symbol']}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error processing token {token['symbol']} for wallet {wallet}: {str(e)}")
            continue

def batch_process_dataframe(transaction_gen, batch_size=50):
    """
    Process transactions in batches to avoid loading everything at once.
    Returns a Polars DataFrame. (Adjusted schema slightly)
    """
    try:
        all_dfs = []
        batch = []
        total_processed = 0
        
        for tx in transaction_gen:
            batch.append(tx)
            
            if len(batch) >= batch_size:
                try:
                    df_batch = pl.DataFrame(batch)
                    all_dfs.append(df_batch)
                    total_processed += len(batch)
                    logger.info(f"Processed batch of {len(batch)} transactions. Total: {total_processed}")
                except Exception as e:
                    logger.error(f"Error creating DataFrame from batch: {str(e)}")
                batch = []
        
        if batch:
            try:
                df_batch = pl.DataFrame(batch)
                all_dfs.append(df_batch)
                total_processed += len(batch)
                logger.info(f"Processed final batch of {len(batch)} transactions. Total: {total_processed}")
            except Exception as e:
                logger.error(f"Error creating DataFrame from final batch: {str(e)}")
        
        empty_df_schema = {
            "portfolio_name": pl.Utf8, "strategy_name": pl.Utf8, "wallet": pl.Utf8,
            "network": pl.Utf8, "token_symbol_onchain": pl.Utf8, "token_symbol_config": pl.Utf8,
            "token_address": pl.Utf8, "value": pl.Float64, "from": pl.Utf8, "to": pl.Utf8,
            "hash": pl.Utf8, "timestamp": pl.Datetime
        }

        if all_dfs:
            try:
                final_df = pl.concat(all_dfs, how="diagonal") # Use diagonal to handle potential schema differences if some batches are empty or have missing optional fields
                
                # Ensure all columns from schema exist, fill with null if not
                for col, dtype in empty_df_schema.items():
                    if col not in final_df.columns:
                        final_df = final_df.with_columns(pl.lit(None, dtype=dtype).alias(col))
                
                # Cast columns to desired types, especially timestamp
                final_df = final_df.with_columns(
                    pl.col("timestamp").cast(pl.Int64).pipe(pl.from_epoch, time_unit="s").alias("timestamp") # Assuming seconds
                ).select(list(empty_df_schema.keys())) # Reorder and select columns to match schema

                logger.info(f"Successfully created DataFrame with {len(final_df)} rows")
                return final_df
            except Exception as e:
                logger.error(f"Error during final DataFrame processing: {str(e)}")
                if all_dfs: # Fallback if complex processing fails
                    logger.warning("Returning concatenated DataFrame without full schema enforcement or timestamp conversion due to error.")
                    try:
                        # Simpler concat and basic timestamp conversion
                        simple_concat_df = pl.concat(all_dfs)
                        if "timestamp" in simple_concat_df.columns:
                             simple_concat_df = simple_concat_df.with_columns(
                                pl.col("timestamp").cast(pl.Int64).pipe(pl.from_epoch, time_unit="s").alias("timestamp")
                             )
                        return simple_concat_df
                    except Exception as concat_err:
                        logger.error(f"Fallback concat also failed: {concat_err}")
                        return pl.DataFrame(schema=empty_df_schema) # Return empty with schema

        logger.warning("No transactions processed, returning empty DataFrame with Uniswap schema")
        return pl.DataFrame(schema=empty_df_schema)
    except Exception as e:
        logger.error(f"Unexpected error in batch processing: {str(e)}")
        return pl.DataFrame(schema=empty_df_schema)

def get_underlying_asset_uniswap(symbol): # Renamed for Uniswap context
    """
    For Uniswap, LP token symbols (e.g., UNI-V2, CAKE-LP) or standard token symbols are common.
    This function can be adapted if there's a specific transformation needed,
    e.g., extracting constituents from an LP token symbol for price lookups.
    For now, it returns the symbol as is, as CoinMetrics typically uses the base asset symbols.
    """
    # Example: If symbol is "USDC-ETH LP", we might want "USDC" or "ETH" for pricing.
    # This requires more complex logic and knowledge of LP token naming conventions.
    # For simplicity, returning the original symbol. The caller (fetch_coinmetrics_price_data)
    # might need to be smarter or we need a map from LP to underlying.
    logger.debug(f"get_underlying_asset_uniswap called with: {symbol}, returning as is.")
    return symbol.upper() # Ensure consistent casing for lookups

def fetch_coinmetrics_price_data(symbol, start_time, end_time=None, frequency="1d"):
    """
    Fetch historical price data from CoinMetrics API. (Largely reusable)
    The 'symbol' argument should be the asset symbol CoinMetrics recognizes (e.g., "USDC", "WETH").
    'start_time' and 'end_time' (if provided) should be strings in "YYYY-MM-DDTHH:MM:SS" format.
    """
    try:
        api_key = os.getenv("COINMETRICS_API_KEY")
        if not api_key:
            logger.error("COINMETRICS_API_KEY not found in environment variables")
            return None
            
        asset_to_price = get_underlying_asset_uniswap(symbol) 
        logger.info(f"Fetching price data for asset {asset_to_price} (derived from {symbol})")
        
        # 'start_time' is received as a pre-formatted string.
        # 'end_time' if provided should also be a pre-formatted string.
        # If 'end_time' is not provided, generate it in the correct format.
        current_end_time_str: str
        if end_time:
            # Assuming end_time, if provided, is already correctly formatted.
            current_end_time_str = end_time
        else:
            current_end_time_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            
        # The check 'if isinstance(start_time, datetime.datetime): start_time = start_time.isoformat()'
        # has been removed as start_time is now expected to be a correctly formatted string from main.

        url = "https://api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": asset_to_price,
            "metrics": "PriceUSD",
            "frequency": frequency,
            "start_time": start_time, # Should be in "YYYY-MM-DDTHH:MM:SS" format
            "end_time": current_end_time_str, # Should be in "YYYY-MM-DDTHH:MM:SS" format
            "page_size": 1000, # Max page size
            "api_key": api_key
        }
        
        logger.info(f"Requesting CoinMetrics price data for {asset_to_price} from {start_time} to {end_time}")
        response = requests.get(url, params=params, timeout=30, verify=False)
        response.raise_for_status()
        
        data = response.json()
        if "data" not in data or not data["data"]:
            logger.warning(f"No price data returned from CoinMetrics for {asset_to_price}")
            return None
            
        price_data = []
        for item in data["data"]:
            if "time" in item and "PriceUSD" in item and item["PriceUSD"] is not None: # Check for null price
                try:
                    price_data.append({
                        # CoinMetrics time is ISO 8601 format
                        "timestamp": item["time"], 
                        "price": float(item["PriceUSD"])
                    })
                except ValueError:
                    logger.warning(f"Could not convert price '{item['PriceUSD']}' to float for {asset_to_price} at {item['time']}")
                    continue
        
        if not price_data:
            logger.warning(f"Price data was empty or malformed after processing for {asset_to_price}")
            return None
            
        df = pl.DataFrame(price_data)
        # Convert timestamp string to datetime objects
        df = df.with_columns(pl.col("timestamp").str.to_datetime().alias("timestamp"))
        logger.info(f"Retrieved {len(df)} price points for {asset_to_price}")
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error fetching CoinMetrics price data for {symbol} (asset: {asset_to_price}): {str(e)}")
        return None
    except (ValueError, KeyError, TypeError) as e:
        logger.error(f"Data parsing error for CoinMetrics {symbol} (asset: {asset_to_price}): {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching CoinMetrics price data for {symbol} (asset: {asset_to_price}): {str(e)}")
        return None

def find_price_at_date(price_lookup_dict: Dict[datetime.date, float], target_date: datetime.date) -> Optional[float]:
    """Helper function to find price at a specific date. (Reusable)"""
    if not price_lookup_dict:
        return None
    if target_date in price_lookup_dict:
        return price_lookup_dict[target_date]
    
    dates = sorted(list(price_lookup_dict.keys())) # Sort dates to find closest correctly
    if not dates:
        return None
        
    # If target_date is before the first known price or after the last, decide on behavior
    # Option 1: Return None if out of range
    # if target_date < dates[0] or target_date > dates[-1]:
    #     logger.debug(f"Target date {target_date} is outside known price range {dates[0]} - {dates[-1]}")
    #     return None

    # Option 2: Use closest (current behavior)
    closest_date = min(dates, key=lambda d: abs((d - target_date).days))
    # Optional: Add a threshold for proximity
    # max_days_diff = 7 
    # if abs((closest_date - target_date).days) > max_days_diff:
    #    logger.debug(f"Closest date {closest_date} for {target_date} is too far (diff > {max_days_diff} days). No price used.")
    #    return None
    logger.debug(f"No exact price for {target_date}, using closest date {closest_date} (diff: {abs((closest_date - target_date).days)} days)")
    return price_lookup_dict[closest_date]

def main(network="polygon", batch_size=100): # Increased default batch_size
    try:
        logger.info(f"Starting Uniswap reconciliation for network: {network}")
        
        load_dotenv()
        
        API_KEYS = {
            "polygon": os.getenv("POLYGONSCAN_API_KEY"), "ethereum": os.getenv("ETHERSCAN_API_KEY"),
            "arbitrum": os.getenv("ARBISCAN_API_KEY"), "optimism": os.getenv("OPTIMISM_API_KEY"),
            "base": os.getenv("BASESCAN_API_KEY")
        }
        
        api_key = API_KEYS.get(network)
        if not api_key:
            logger.error(f"No API key available for {network}")
            return
            
        uniswap_wallets_addresses = get_uniswap_wallets(PORTFOLIOS, network) # Use new function
        if not uniswap_wallets_addresses:
            logger.warning(f"No Uniswap wallets found for network {network}. Exiting.") # Changed to warning
            return # Exit if no relevant wallets
            
        # Get tokens generally active on the network
        general_network_tokens = get_all_token_contracts(TOKENS, network)
        
        # Get strategy-specific pool tokens for Uniswap wallets
        strategy_specific_pool_tokens = []
        logger.info(f"Checking for strategy-specific Uniswap V3 pool tokens for {len(uniswap_wallets_addresses)} wallet(s) on network {network}...")
        for portfolio_name, portfolio_data in PORTFOLIOS.items():
            for strategy_name, wallet_details in portfolio_data.get("STRATEGY_WALLETS", {}).items():
                wallet_addr = wallet_details.get("address")
                # Check if this wallet is one of the Uniswap wallets we are processing for the current network
                if wallet_addr and wallet_addr.lower() in [uwa.lower() for uwa in uniswap_wallets_addresses]:
                    if "uniswap" in wallet_details.get("active_protocols", []) and \
                       network.lower() in [n.lower() for n in wallet_details.get("active_networks", [])]:
                        
                        pool_keys_from_portfolio = wallet_details.get("uniswapv3_pools", [])
                        if pool_keys_from_portfolio:
                            logger.info(f"Wallet {wallet_addr} ({portfolio_name} - {strategy_name}) has uniswapv3_pools defined: {pool_keys_from_portfolio}")
                            for pool_key in pool_keys_from_portfolio:
                                logger.info(f"Looking up token definition for pool key: {pool_key} from TOKENS config for network {network}")
                                # Pass network to ensure the pool token is relevant for the current network context
                                token_def = find_token_definition_by_key(TOKENS, pool_key, network_context=network)
                                if token_def:
                                    logger.info(f"Found definition for pool key '{pool_key}' in TOKENS: Symbol {token_def['symbol']}, Address {token_def['address']}")
                                    strategy_specific_pool_tokens.append(token_def)
                                else:
                                    logger.info(f"Pool key '{pool_key}' not found in TOKENS, checking UNISWAP_V3_POOLS for network {network}.")
                                    pool_data_from_uniswap_pools = UNISWAP_V3_POOLS.get(pool_key)
                                    if pool_data_from_uniswap_pools:
                                        # Check if this pool is active on the current network
                                        pool_active_networks = pool_data_from_uniswap_pools.get("active_networks", [])
                                        if isinstance(pool_active_networks, str): pool_active_networks = [pool_active_networks]
                                        
                                        is_pool_active_on_network = network.lower() in [n.lower() for n in pool_active_networks]

                                        if is_pool_active_on_network:
                                            logger.info(f"Found definition for pool key '{pool_key}' in UNISWAP_V3_POOLS. Address: {pool_data_from_uniswap_pools['address']}")
                                            # Construct a token-like definition for this pool
                                            # IMPORTANT: Uniswap V3 LP positions are often NFTs. The 'address' here is the pool contract.
                                            # The 'value' from 'tokentx' might be the NFT ID if this is how LP tokens are transferred.
                                            # For fungible LP tokens (V2-like), decimals would be relevant.
                                            # For NFTs, decimals are typically 0.
                                            # We will use 0 as a placeholder and log a warning.
                                            # The symbol will be the pool_key itself.
                                            strategy_specific_pool_tokens.append({
                                                "address": pool_data_from_uniswap_pools["address"],
                                                "decimals": 0, # Assuming 0 for NFT-like V3 LP tokens
                                                "symbol": pool_key, # Use the pool key as the symbol
                                                "original_key": pool_key # Store original key for reference
                                            })
                                            logger.warning(f"Using 0 decimals for Uniswap V3 pool '{pool_key}' found in UNISWAP_V3_POOLS. "
                                                           f"If 'tokentx' is used, the 'value' might represent an NFT ID.")
                                        else:
                                            logger.warning(f"Pool key '{pool_key}' found in UNISWAP_V3_POOLS but not active on network '{network}'. Skipping.")
                                    else:
                                        logger.warning(f"Could not find token definition for pool key '{pool_key}' in TOKENS or UNISWAP_V3_POOLS for network {network}.")
        
        # Combine general network tokens and strategy-specific pool tokens, ensuring no duplicates
        final_token_contracts = list(general_network_tokens) # Start with a mutable copy
        seen_addresses = {token['address'].lower() for token in general_network_tokens}
        
        if strategy_specific_pool_tokens:
            logger.info(f"Adding {len(strategy_specific_pool_tokens)} strategy-specific pool tokens to the fetch list.")
            for token_def in strategy_specific_pool_tokens:
                if token_def['address'].lower() not in seen_addresses:
                    final_token_contracts.append(token_def)
                    seen_addresses.add(token_def['address'].lower())
                    logger.info(f"Added unique strategy pool token: {token_def['symbol']} ({token_def['address']})")
                else:
                    logger.info(f"Strategy pool token {token_def['symbol']} ({token_def['address']}) already in general list.")

        token_contracts = final_token_contracts # This list will be used for fetching transactions
        
        if not token_contracts:
            logger.error(f"No relevant token contracts (general or strategy-specific) found for network {network}. Exiting.")
            return
            
        logger.info(f"Compiled {len(token_contracts)} unique token contracts for Uniswap wallets on {network}:")
        for token in token_contracts:
            logger.info(f" - {token['symbol']} ({token['address']})")

        def stream_all_transactions():
            for wallet in uniswap_wallets_addresses:
                logger.info(f"Streaming transactions for Uniswap wallet: {wallet}")
                try:
                    # Pass only tokens that are relevant for this wallet's network (already filtered by get_all_token_contracts)
                    yield from stream_wallet_transactions(wallet, token_contracts, api_key, network)
                except Exception as e:
                    logger.error(f"Error streaming transactions for wallet {wallet}: {str(e)}")
                    continue
        
        df = batch_process_dataframe(stream_all_transactions(), batch_size)
        
        if df.height > 0: # Use Polars df.height for row count
            logger.info(f"Retrieved {df.height} transactions for network {network}")
            
            try:
                min_timestamp_val = df["timestamp"].min()
                start_time_str: str # Renamed from start_time_iso and type hinted for clarity
                
                if min_timestamp_val:
                    # Ensure min_timestamp_val is timezone-aware (UTC) if not already before formatting
                    dt_object = min_timestamp_val
                    if dt_object.tzinfo is None:
                        dt_object = dt_object.replace(tzinfo=datetime.timezone.utc)
                    start_time_str = dt_object.strftime("%Y-%m-%dT%H:%M:%S")
                else: # if no transactions, use a default start_time (e.g., 30 days ago)
                    default_start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
                    start_time_str = default_start_dt.strftime("%Y-%m-%dT%H:%M:%S")

                # Symbols requiring prices: from 'token_symbol_config' which is from our TOKENS
                # And also symbols for price columns.
                symbols_from_transactions = set(df.drop_nulls("token_symbol_config")["token_symbol_config"].unique().to_list())
                
                network_price_column_tokens = get_network_active_tokens_for_columns(TOKENS, network)
                symbols_for_price_columns = {token_detail['symbol'] for token_detail in network_price_column_tokens}
                
                symbols_requiring_prices = symbols_from_transactions.union(symbols_for_price_columns)
                logger.info(f"Identified {len(symbols_requiring_prices)} unique token symbols for price fetching: {symbols_requiring_prices}")

                price_data_cache: Dict[str, Dict[datetime.date, float]] = {} 
                
                for token_symbol_cm in symbols_requiring_prices: # token_symbol_cm is the symbol to use with CoinMetrics
                    logger.info(f"Fetching price data for symbol (for CoinMetrics): {token_symbol_cm}")
                    
                    token_price_df = fetch_coinmetrics_price_data(
                        symbol=token_symbol_cm, 
                        start_time=start_time_str, # Pass the correctly formatted string
                        frequency="1d"
                    )
                    
                    current_token_price_dict = {}
                    if token_price_df is not None and token_price_df.height > 0:
                        # Convert CoinMetrics 'timestamp' (datetime) to 'date' for the cache
                        token_price_df = token_price_df.with_columns(
                            pl.col("timestamp").dt.date().alias("date")
                        )
                        for row in token_price_df.iter_rows(named=True):
                            current_token_price_dict[row["date"]] = row["price"]
                        logger.info(f"Cached {len(current_token_price_dict)} price points for {token_symbol_cm}")
                    else:
                        logger.warning(f"No price data found or processed for {token_symbol_cm}")
                    price_data_cache[token_symbol_cm] = current_token_price_dict
                
                # Populate "price_usd" for transacted tokens ('token_symbol_config')
                price_usd_values = []
                # Ensure 'timestamp' column is datetime before extracting date
                if df['timestamp'].dtype != pl.Datetime:
                     df = df.with_columns(pl.col('timestamp').cast(pl.Int64).pipe(pl.from_epoch, time_unit="s"))

                transaction_dates = df["timestamp"].dt.date().to_list()

                for i, transacted_token_symbol_cfg in enumerate(df["token_symbol_config"].to_list()):
                    price = None
                    if transacted_token_symbol_cfg: # Ensure symbol is not None
                        tx_date = transaction_dates[i]
                        # The key for price_data_cache should be what we used to fetch from CoinMetrics.
                        # This assumes get_underlying_asset_uniswap(transacted_token_symbol_cfg) would give the correct CM symbol.
                        # Or, more simply, that transacted_token_symbol_cfg is directly usable if it's a base asset.
                        # For now, we assume transacted_token_symbol_cfg is the key used for cache.
                        prices_for_transacted_token = price_data_cache.get(transacted_token_symbol_cfg, {})
                        price = find_price_at_date(prices_for_transacted_token, tx_date)
                    price_usd_values.append(price)

                df = df.with_columns(pl.Series("price_usd", price_usd_values, dtype=pl.Float64))
                
                df = df.with_columns(
                    (pl.col("value") * pl.col("price_usd")).alias("value_usd")
                ).with_columns(pl.col("value_usd").cast(pl.Float64)) # Ensure correct type
                logger.info("Populated 'price_usd' and 'value_usd' columns for transactions.")

                logger.info(f"Adding price columns for {len(network_price_column_tokens)} network-active tokens (Uniswap context).")
                for token_detail in network_price_column_tokens:
                    original_key = token_detail['original_key']
                    col_name = f"price_{original_key.replace('-', '_').replace('.', '_').replace(' ', '_').lower()}" # Ensure lowercase and sanitized
                    
                    # Symbol from TOKENS config (e.g., "USDC", "WETH") - this should be the key for price_data_cache
                    symbol_for_lookup = token_detail['symbol'] 
                    
                    prices_for_column_asset = price_data_cache.get(symbol_for_lookup, {})
                    
                    current_column_price_values = [
                        find_price_at_date(prices_for_column_asset, tx_date) for tx_date in transaction_dates
                    ]
                    
                    df = df.with_columns(pl.Series(name=col_name, values=current_column_price_values, dtype=pl.Float64))
                    logger.info(f"Added and populated price column: {col_name} using symbol {symbol_for_lookup}")
                
                print(f"\n🔢 Uniswap Transaction Data for {network} with prices:")
                print(df)
                
                csv_filename = f"uniswap_transactions_{network}.csv" # Changed filename
                df.write_csv(csv_filename)
                logger.info(f"Data saved to {csv_filename}")
                print(f"\n💾 Data saved to {csv_filename}")
                
            except Exception as e:
                logger.error(f"Error processing price data for Uniswap: {str(e)}", exc_info=True)
                print(f"Error adding price data: {str(e)}")
                
                csv_filename = f"uniswap_transactions_{network}_no_prices.csv" # Changed filename
                if df.height > 0: # Only write if df exists
                    df.write_csv(csv_filename)
                    logger.info(f"Transaction data without prices saved to {csv_filename}")
                    print(f"\n💾 Transaction data without prices saved to {csv_filename}")
                else:
                    logger.info("No transaction data to save (even without prices).")
        else:
            logger.warning(f"No Uniswap transactions found for network: {network}")
            print(f"No Uniswap transactions found for network: {network}")
    except Exception as e:
        logger.critical(f"Critical error in Uniswap main function: {str(e)}", exc_info=True)
        print(f"An critical error occurred: {str(e)}")

if __name__ == "__main__":
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Example: Run for arbitrum network, which has a Uniswap wallet in consts.py
        # main("arbitrum") 
        main("arbitrum") # Defaulting to polygon for now, can be changed
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error at script execution: {str(e)}", exc_info=True)
        print(f"Critical error: {str(e)}") 