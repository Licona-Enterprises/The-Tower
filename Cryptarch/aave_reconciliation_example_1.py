import requests
import polars as pl
import os
import logging
import sys
from dotenv import load_dotenv
from app.backend.consts import PORTFOLIOS, TOKENS, BASE_URLS
from contextlib import contextmanager
from typing import Dict, List, Generator, Any

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

@contextmanager
def get_session():
    """Context manager for requests.Session to ensure proper cleanup."""
    session = requests.Session()
    try:
        yield session
    finally:
        session.close()

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

def fetch_transactions(session, address, contract_address, api_key, network="polygon", page=1, offset=100):
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
        
        response = session.get(api_url, params=params, timeout=30)  # Added timeout
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

def stream_wallet_transactions(session, wallet, token_contracts, api_key, network="polygon"):
    """
    Generator that streams transactions for a wallet without loading all into memory.
    Yields one transaction at a time.
    """
    logger.info(f"Fetching Aave transactions for wallet: {wallet} on {network}")
    
    for token in token_contracts:
        logger.info(f"Processing token: {token['symbol']} ({token['address']})")
        try:
            txs = fetch_transactions(session, wallet, token["address"], api_key, network)
            
            for tx in txs:
                try:
                    value = int(tx["value"]) / (10 ** token["decimals"])
                    yield {
                        "wallet": wallet,
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
            "timestamp": [],
            "token": [],
            "value": [],
            "network": [],
            "wallet": [],            
            "from": [],
            "to": [],
            "hash": []
        })
    except Exception as e:
        logger.error(f"Unexpected error in batch processing: {str(e)}")
        return pl.DataFrame({
            "wallet": [],
            "network": [],
            "token": [],
            "value": [],
            "from": [],
            "to": [],
            "hash": [],
            "timestamp": []
        })

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
            
        token_contracts = get_aave_token_contracts(TOKENS, network)
        if not token_contracts:
            logger.error(f"No token contracts found for network {network}")
            return

        # Create a generator for streaming transactions
        with get_session() as session:
            # Generator to lazily stream all transactions
            def stream_all_transactions():
                for wallet in aave_wallets:
                    try:
                        yield from stream_wallet_transactions(session, wallet, token_contracts, api_key, network)
                    except Exception as e:
                        logger.error(f"Error processing wallet {wallet}: {str(e)}")
                        continue
            
            # Process transactions in batches
            df = batch_process_dataframe(stream_all_transactions(), batch_size)
        
        # Display the DataFrame
        if len(df) > 0:
            logger.info(f"Retrieved {len(df)} transactions for network {network}")
            print(f"\n🔢 Transaction Data for {network}:")
            print(df)
            
            try:
                # Save DataFrame to CSV
                csv_filename = f"aave_transactions_{network}.csv"
                df.write_csv(csv_filename)
                logger.info(f"Data saved to {csv_filename}")
                print(f"\n💾 Data saved to {csv_filename}")
            except Exception as e:
                logger.error(f"Error saving data to CSV: {str(e)}")
        else:
            logger.warning(f"No transactions found for network: {network}")
            print(f"No transactions found for network: {network}")
    except Exception as e:
        logger.error(f"Unexpected error in main function: {str(e)}")
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    try:
        # You can change the network here or pass as a parameter when calling the script
        main("polygon")
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        print(f"Critical error: {str(e)}")
