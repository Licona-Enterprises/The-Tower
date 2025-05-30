import requests
import polars as pl
import os
import logging
import sys
import datetime
from dotenv import load_dotenv
from app.backend.consts import PORTFOLIOS, TOKENS, BASE_URLS
from typing import Dict, List, Generator, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("aave_reconciliation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def get_aave_wallets(portfolios, network="polygon"):
    try:
        wallets = []
        for portfolio_data in portfolios.values():
            strategy_wallets = portfolio_data.get("STRATEGY_WALLETS", {})
            for wallet in strategy_wallets.values():
                if "aave" in wallet.get("active_protocols", []) and network in wallet.get("active_networks", []):
                    wallets.append(wallet["address"])
        
        if not wallets:
            logger.warning(f"No Aave wallets found for network {network}")
        else:
            logger.info(f"Found {len(wallets)} Aave wallets for network {network}")
        
        return wallets
    except Exception as e:
        logger.error(f"Error getting Aave wallets: {str(e)}")
        return []

def get_aave_token_contracts(tokens, network="polygon"):
    try:
        token_contracts = []
        network_upper = network.upper()
        for key, token_data in tokens.get("AAVE", {}).items():
            if network_upper in key:
                token_contracts.append({
                    "address": token_data["address"],
                    "decimals": token_data["decimals"],
                    "symbol": token_data["symbol"]
                })
        
        if not token_contracts:
            logger.warning(f"No Aave token contracts found for network {network}")
        else:
            logger.info(f"Found {len(token_contracts)} Aave token contracts for network {network}")
            
        return token_contracts
    except Exception as e:
        logger.error(f"Error getting Aave token contracts: {str(e)}")
        return []

def get_all_token_contracts(tokens, network="polygon"):
    """Get all token contracts for a network, not just Aave tokens."""
    try:
        all_token_contracts = []
        network_upper = network.upper()
        
        # First, add all Aave tokens
        aave_tokens = get_aave_token_contracts(tokens, network)
        all_token_contracts.extend(aave_tokens)
        
        # Then add all other tokens with network in their key names
        for key, token_data in tokens.items():
            # Skip the AAVE key as we've already processed it
            if key == "AAVE":
                continue
                
            # Check if this is a token entry (has address, decimals, symbol)
            if isinstance(token_data, dict) and all(k in token_data for k in ["address", "decimals", "symbol"]):
                # If token key contains network name, add it
                if network_upper in key:
                    all_token_contracts.append(token_data)
            
            # Check if this is a nested dictionary of tokens
            elif isinstance(token_data, dict):
                for subkey, subtoken_data in token_data.items():
                    if network_upper in subkey and isinstance(subtoken_data, dict) and all(k in subtoken_data for k in ["address", "decimals", "symbol"]):
                        all_token_contracts.append(subtoken_data)
        
        # Remove duplicates based on contract address
        unique_addresses = set()
        unique_tokens = []
        
        for token in all_token_contracts:
            if token["address"].lower() not in unique_addresses:
                unique_addresses.add(token["address"].lower())
                unique_tokens.append(token)
        
        logger.info(f"Found {len(unique_tokens)} total token contracts for network {network}")
        return unique_tokens
    except Exception as e:
        logger.error(f"Error getting all token contracts: {str(e)}")
        return []

def get_network_active_tokens_for_columns(tokens_config: Dict, network_name: str) -> List[Dict]:
    """
    Identifies all tokens from tokens_config that are active on the specified network.
    Used to determine which assets need dedicated price columns in the output.
    """
    network_specific_tokens = []
    processed_addresses = set() # To ensure unique tokens if multiple keys point to same address

    def check_and_add_token(token_data: Dict, token_key: str, network_name_lower: str):
        if isinstance(token_data, dict) and all(k in token_data for k in ["address", "decimals", "symbol"]):
            active_networks_val = token_data.get("active_networks")
            is_active_on_network = False
            
            if isinstance(active_networks_val, str):
                is_active_on_network = active_networks_val.lower() == network_name_lower
            elif isinstance(active_networks_val, list):
                is_active_on_network = network_name_lower in [n.lower() for n in active_networks_val]
            
            # Fallback: if active_networks is missing or doesn't match, check if network name is in the token_key itself
            # This maintains some of the old logic from get_all_token_contracts if active_networks is not explicit
            if not is_active_on_network and network_name_lower in token_key.lower():
                is_active_on_network = True
                logger.debug(f"Token {token_key} matched network {network_name_lower} by key as fallback.")

            if is_active_on_network:
                address = token_data["address"].lower()
                if address not in processed_addresses:
                    token_info = token_data.copy()
                    token_info["original_key"] = token_key  # Store the original key for column naming
                    network_specific_tokens.append(token_info)
                    processed_addresses.add(address)
                    logger.debug(f"Added token {token_key} (address: {address}) for network {network_name_lower} pricing columns.")

    network_name_lower = network_name.lower()
    for key, value in tokens_config.items():
        # Check if the value itself is a token definition
        if isinstance(value, dict) and all(k in value for k in ["address", "decimals", "symbol"]):
            check_and_add_token(value, key, network_name_lower)
        # Else, check if it's a dictionary континенты other tokens (like "AAVE" or other categories)
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict) and all(k in sub_value for k in ["address", "decimals", "symbol"]):
                    check_and_add_token(sub_value, sub_key, network_name_lower)
    
    logger.info(f"Found {len(network_specific_tokens)} distinct tokens active on network '{network_name}' for pricing columns.")
    return network_specific_tokens

def fetch_transactions(address, contract_address, api_key, network="polygon", page=1, offset=100):
    """Fetch transactions from the block explorer API."""
    try:
        api_url = BASE_URLS.get(network)
        if not api_url:
            logger.error(f"Error: Network {network} not supported")
            return []
            
        params = {
            "module": "account",
            "action": "tokentx",
            "contractaddress": contract_address,
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": offset,
            "sort": "asc",
            "apikey": api_key
        }
        
        response = requests.get(api_url, params=params, timeout=30, verify=False)  # Direct request with verify=False
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        data = response.json()
        if data.get("status") != "1":
            logger.warning(f"API returned error for {address} with token {contract_address}: {data.get('message')}")
            return []
            
        logger.info(f"Fetched {len(data.get('result', []))} transactions for address {address} with token {contract_address}")
        return data.get("result", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching transactions for {address} with token {contract_address}: {str(e)}")
        return []
    except ValueError as e:
        logger.error(f"JSON decoding error for {address} with token {contract_address}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching transactions: {str(e)}")
        return []

def get_wallet_metadata(portfolios, address):
    """Get portfolio and strategy name for a wallet address."""
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
    Yields one transaction at a time.
    """
    logger.info(f"Fetching Aave transactions for wallet: {wallet} on {network}")
    
    # Get portfolio and strategy names for this wallet
    wallet_metadata = get_wallet_metadata(PORTFOLIOS, wallet)
    
    for token in token_contracts:
        logger.info(f"Processing token: {token['symbol']} ({token['address']})")
        try:
            txs = fetch_transactions(wallet, token["address"], api_key, network)
            
            for tx in txs:
                try:
                    value = int(tx["value"]) / (10 ** token["decimals"])
                    yield {
                        "wallet": wallet,
                        "portfolio_name": wallet_metadata["portfolio_name"],
                        "strategy_name": wallet_metadata["strategy_name"],
                        "network": network,
                        "token": tx["tokenSymbol"],
                        "value": value,
                        "from": tx["from"],
                        "to": tx["to"],
                        "hash": tx["hash"],
                        "timestamp": tx["timeStamp"]
                    }
                except (KeyError, ValueError) as e:
                    logger.warning(f"Error processing transaction {tx.get('hash', 'unknown')}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error processing token {token['symbol']}: {str(e)}")
            continue

def batch_process_dataframe(transaction_gen, batch_size=50):
    """
    Process transactions in batches to avoid loading everything at once.
    Returns a Polars DataFrame.
    """
    try:
        all_dfs = []
        batch = []
        total_processed = 0
        
        for tx in transaction_gen:
            batch.append(tx)
            
            if len(batch) >= batch_size:
                # Process current batch
                try:
                    df_batch = pl.DataFrame(batch)
                    all_dfs.append(df_batch)
                    total_processed += len(batch)
                    logger.info(f"Processed batch of {len(batch)} transactions. Total: {total_processed}")
                except Exception as e:
                    logger.error(f"Error creating DataFrame from batch: {str(e)}")
                # Clear batch for next round
                batch = []
        
        # Process any remaining transactions
        if batch:
            try:
                df_batch = pl.DataFrame(batch)
                all_dfs.append(df_batch)
                total_processed += len(batch)
                logger.info(f"Processed final batch of {len(batch)} transactions. Total: {total_processed}")
            except Exception as e:
                logger.error(f"Error creating DataFrame from final batch: {str(e)}")
        
        # Combine all batches if we have any
        if all_dfs:
            try:
                final_df = pl.concat(all_dfs)
                # Convert timestamp to datetime (from Unix epoch)
                result_df = final_df.with_columns(
                    pl.col("timestamp").cast(pl.Int64).pipe(pl.from_epoch)
                )
                logger.info(f"Successfully created DataFrame with {len(result_df)} rows")
                return result_df
            except Exception as e:
                logger.error(f"Error during final DataFrame processing: {str(e)}")
                # If timestamp conversion fails, return the DataFrame without conversion
                if all_dfs:
                    logger.warning("Returning DataFrame without timestamp conversion")
                    return pl.concat(all_dfs)
        
        # Return empty DataFrame with expected schema
        logger.warning("No transactions processed, returning empty DataFrame")
        return pl.DataFrame({
            "portfolio_name": [],
            "strategy_name": [],
            "wallet": [],
            "network": [],
            "token": [],
            "value": [],            
            "from": [],
            "to": [],
            "hash": [],
            "timestamp": []
        })
    except Exception as e:
        logger.error(f"Unexpected error in batch processing: {str(e)}")
        return pl.DataFrame({
            "portfolio_name": [],
            "strategy_name": [],
            "wallet": [],
            "network": [],
            "token": [],
            "value": [],
            "from": [],
            "to": [],
            "hash": [],
            "timestamp": []
        })

def get_underlying_asset(symbol):
    """Convert Aave aToken symbol to underlying asset symbol."""
    symbol = symbol.upper()
    
    # Handle specific Aave token patterns
    if "USDC" in symbol:
        return "USDC"
    elif "USDT" in symbol:
        return "USDT"
    elif "DAI" in symbol:
        return "DAI"
    elif "WETH" in symbol or "ETH" in symbol:
        return "ETH"
    elif "WBTC" in symbol or "BTC" in symbol:
        return "BTC"
    elif "WMATIC" in symbol or "MATIC" in symbol:
        return "MATIC"
    elif symbol.startswith('A'):
        # For other aTokens, try to extract the underlying token
        # Remove common prefixes like "a", "aPol", etc.
        for prefix in ["A", "APOL", "AARB", "AOPT"]:
            if symbol.startswith(prefix):
                return symbol[len(prefix):]
    
    # If no pattern matches, return the original symbol
    return symbol

def fetch_coinmetrics_price_data(symbol, start_time, end_time=None, frequency="1d"):
    """
    Fetch historical price data from CoinMetrics API.
    
    Args:
        symbol: The token symbol (aToken symbol like aUSDC)
        start_time: Start time as ISO format string
        end_time: End time as ISO format string (defaults to now)
        frequency: Data frequency (1d, 1h, 1m, etc.)
        
    Returns:
        DataFrame with timestamp and price data
    """
    try:
        # Load API key from environment
        api_key = os.getenv("COINMETRICS_API_KEY")
        if not api_key:
            logger.error("COINMETRICS_API_KEY not found in environment variables")
            return None
            
        # Convert aToken symbol to underlying asset
        asset = get_underlying_asset(symbol)
        logger.info(f"Fetching price data for asset {asset} (from {symbol})")
        
        # Set end time to now if not provided
        if not end_time:
            end_time = datetime.datetime.now().isoformat()
            
        # Prepare API request
        url = "https://api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": asset,
            "metrics": "PriceUSD",
            "frequency": frequency,
            "start_time": start_time,
            "end_time": end_time,
            "page_size": 999,
            "api_key": api_key
        }
        
        # Make request
        logger.info(f"Requesting price data for {asset} from {start_time} to {end_time}")
        response = requests.get(url, params=params, timeout=30, verify=False)  # Direct request with verify=False
        response.raise_for_status()
        
        # Process response
        data = response.json()
        if "data" not in data or not data["data"]:
            logger.warning(f"No price data returned for {asset}")
            return None
            
        # Convert to DataFrame
        price_data = []
        for item in data["data"]:
            if "time" in item and "PriceUSD" in item:
                price_data.append({
                    "timestamp": item["time"],
                    "price": float(item["PriceUSD"])
                })
        
        if not price_data:
            logger.warning(f"Price data format unexpected for {asset}")
            return None
            
        df = pl.DataFrame(price_data)
        logger.info(f"Retrieved {len(df)} price points for {asset}")
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error fetching price data for {symbol}: {str(e)}")
        return None
    except (ValueError, KeyError, TypeError) as e:
        logger.error(f"Data parsing error for {symbol}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching price data for {symbol}: {str(e)}")
        return None

# Helper function to find price at a specific date from a lookup dictionary
def find_price_at_date(price_lookup_dict: Dict[datetime.date, float], target_date: datetime.date) -> Optional[float]:
    if not price_lookup_dict:
        return None
    if target_date in price_lookup_dict:
        return price_lookup_dict[target_date]
    
    # Find closest date if exact not found
    dates = list(price_lookup_dict.keys())
    if not dates: # Should not happen if price_lookup_dict is not empty, but good practice
        return None
        
    closest_date = min(dates, key=lambda d: abs((d - target_date).days))
    # Optional: Add a threshold for proximity, e.g., if abs((closest_date - target_date).days) > 7: return None
    # For now, always return the closest if an exact match isn't found.
    logger.debug(f"No exact price for {target_date}, using closest date {closest_date} (diff: {abs((closest_date - target_date).days)} days)")
    return price_lookup_dict[closest_date]

def main(network="polygon", batch_size=50):
    try:
        logger.info(f"Starting Aave reconciliation for network: {network}")
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Get API keys from environment variables
        API_KEYS = {
            "polygon": os.getenv("POLYGONSCAN_API_KEY"),
            "ethereum": os.getenv("ETHERSCAN_API_KEY"),
            "arbitrum": os.getenv("ARBISCAN_API_KEY"),
            "optimism": os.getenv("OPTIMISM_API_KEY"),
            "base": os.getenv("BASESCAN_API_KEY")
        }
        
        api_key = API_KEYS.get(network)
        if not api_key:
            logger.error(f"No API key available for {network}")
            return
            
        aave_wallets = get_aave_wallets(PORTFOLIOS, network)
        if not aave_wallets:
            logger.error(f"No Aave wallets found for network {network}")
            return
            
        # Get all token contracts instead of just Aave tokens
        token_contracts = get_all_token_contracts(TOKENS, network)
        if not token_contracts:
            logger.error(f"No token contracts found for network {network}")
            return
            
        # Log the token contracts we're going to query
        for token in token_contracts:
            logger.info(f"Will query transactions for token: {token['symbol']} ({token['address']})")

        # Generator to lazily stream all transactions
        def stream_all_transactions():
            for wallet in aave_wallets:
                try:
                    yield from stream_wallet_transactions(wallet, token_contracts, api_key, network)
                except Exception as e:
                    logger.error(f"Error processing wallet {wallet}: {str(e)}")
                    continue
        
        # Process transactions in batches
        df = batch_process_dataframe(stream_all_transactions(), batch_size)
        
        # Display the DataFrame
        if len(df) > 0:
            logger.info(f"Retrieved {len(df)} transactions for network {network}")
            
            try:
                # --- Centralized Price Fetching ---
                min_timestamp = df["timestamp"].min()
                start_time = datetime.datetime.now().isoformat() # Default if no transactions
                if min_timestamp:
                    start_time = min_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

                # 1. Identify all unique token symbols needing price data
                symbols_requiring_prices = set(df["token"].unique().to_list())
                
                # Get tokens for new price columns
                network_price_column_tokens = get_network_active_tokens_for_columns(TOKENS, network)
                for token_detail in network_price_column_tokens:
                    symbols_requiring_prices.add(token_detail['symbol']) # Add their 'symbol' field

                logger.info(f"Identified {len(symbols_requiring_prices)} unique token symbols for price fetching: {symbols_requiring_prices}")

                # 2. Fetch prices and build a comprehensive cache
                # price_data cache: Key is the symbol passed to fetch_coinmetrics_price_data, Value is dict {date: price}
                price_data_cache: Dict[str, Dict[datetime.date, float]] = {} 
                
                for token_symbol_to_fetch in symbols_requiring_prices:
                    logger.info(f"Fetching price data for symbol: {token_symbol_to_fetch} (used for cache key)")
                    
                    token_price_df = fetch_coinmetrics_price_data(
                        symbol=token_symbol_to_fetch, # This symbol is what CoinMetrics needs (e.g. aUSDC, USDC)
                        start_time=start_time,
                        frequency="1d"
                    )
                    
                    current_token_price_dict = {}
                    if token_price_df is not None and len(token_price_df) > 0:
                        # Process timestamps to dates for easier matching
                        token_price_df = token_price_df.with_columns(
                            pl.col("timestamp").str.to_datetime().dt.replace_time_zone(None).alias("timestamp_dt")
                        ).with_columns(
                            pl.col("timestamp_dt").dt.date().alias("date") # Ensure 'date' column is datetime.date
                        )
                        
                        for row in token_price_df.iter_rows(named=True):
                            current_token_price_dict[row["date"]] = row["price"]
                        logger.info(f"Found {len(current_token_price_dict)} price points for {token_symbol_to_fetch}")
                    else:
                        logger.warning(f"No price data found for {token_symbol_to_fetch}")
                    price_data_cache[token_symbol_to_fetch] = current_token_price_dict

                # --- Populate DataFrame with Prices ---

                # 3. Populate the existing "price_usd" column for transacted tokens
                price_usd_values = []
                transaction_dates = df["timestamp"].dt.date().to_list() # Pre-calculate dates

                for i, transacted_token_symbol in enumerate(df["token"].to_list()):
                    tx_date = transaction_dates[i]
                    prices_for_transacted_token = price_data_cache.get(transacted_token_symbol, {})
                    price = find_price_at_date(prices_for_transacted_token, tx_date)
                    price_usd_values.append(price)

                df = df.with_columns(pl.Series("price_usd", price_usd_values).cast(pl.Float64))
                
                # Calculate USD value for transactions
                df = df.with_columns(
                    pl.when(pl.col("price_usd").is_not_null())
                    .then(pl.col("value") * pl.col("price_usd"))
                    .otherwise(None)
                    .alias("value_usd")
                )
                logger.info("Populated 'price_usd' and 'value_usd' columns.")

                # 4. Add and populate new price columns for all network-active tokens
                logger.info(f"Adding price columns for {len(network_price_column_tokens)} network-active tokens.")
                for token_detail in network_price_column_tokens:
                    original_key = token_detail['original_key']
                    # Sanitize original_key for column name (basic sanitization)
                    col_name = f"price_{original_key.replace('-', '_').replace('.', '_').replace(' ', '_')}"
                    
                    # The 'symbol' from token_detail is used to look up in price_data_cache
                    # This is the symbol as defined in TOKENS (e.g., "aUSDC", "USDC")
                    symbol_for_lookup = token_detail['symbol'] 
                    
                    prices_for_column_asset = price_data_cache.get(symbol_for_lookup, {})
                    
                    current_column_price_values = []
                    for tx_date in transaction_dates: # Reuse pre-calculated transaction dates
                        price = find_price_at_date(prices_for_column_asset, tx_date)
                        current_column_price_values.append(price)
                    
                    df = df.with_columns(pl.Series(name=col_name, values=current_column_price_values).cast(pl.Float64))
                    logger.info(f"Added and populated price column: {col_name}")
                
                # Display the DataFrame with prices
                print(f"\n🔢 Transaction Data for {network} with prices:")
                print(df)
                
                # Save DataFrame to CSV
                csv_filename = f"aave_transactions_{network}.csv"
                df.write_csv(csv_filename)
                logger.info(f"Data saved to {csv_filename}")
                print(f"\n💾 Data saved to {csv_filename}")
                
            except Exception as e:
                logger.error(f"Error processing price data: {str(e)}")
                print(f"Error adding price data: {str(e)}")
                
                # Fallback: save transactions without prices
                csv_filename = f"aave_transactions_{network}_no_prices.csv"
                df.write_csv(csv_filename)
                logger.info(f"Transaction data without prices saved to {csv_filename}")
                print(f"\n💾 Transaction data without prices saved to {csv_filename}")
        else:
            logger.warning(f"No transactions found for network: {network}")
            print(f"No transactions found for network: {network}")
    except Exception as e:
        logger.error(f"Unexpected error in main function: {str(e)}")
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    try:
        # Disable SSL warnings since we're using verify=False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # You can change the network here or pass as a parameter when calling the script
        main("polygon")
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        print(f"Critical error: {str(e)}")
