import aiohttp
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import asyncio
import requests

class CoinMetricsService:
    """Service for handling CoinMetrics API calls and data processing."""
    
    def __init__(self):
        self.api_key = os.getenv("COINMETRICS_API_KEY")
        self.api_url = "https://api.coinmetrics.io/v4/timeseries/asset-metrics"
        
    async def fetch_metric_data(self, session: aiohttp.ClientSession, metric: str, frequency: str, assets: List[str]):
        """Fetch data for a specific metric and frequency."""
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Convert assets to a comma-separated string
        assets_str = ",".join(assets)
        
        # Log what we're requesting
        print(f"Fetching {metric} with frequency {frequency} for assets: {assets_str}")
        
        params = {
            "assets": assets_str,
            "metrics": metric,
            "start_time": yesterday,
            "end_time": today,
            "api_key": self.api_key,
            "frequency": frequency
        }
        
        # Log the full request URL for debugging
        full_url = f"{self.api_url}?{'&'.join([f'{key}={value}' for key, value in params.items() if key != 'api_key'])}"
        print(f"Making API request to: {full_url}")
        
        try:
            async with session.get(self.api_url, params=params) as response:
                if response.status != 200:
                    print(f"Error response for {metric}: {response.status}")
                    return {"error": f"API request failed for {metric} with status {response.status}"}
                
                data = await response.json()
                
                if not data.get("data"):
                    print(f"No data returned for {metric}")
                    return {"error": f"No data available for {metric}"}
                    
                print(f"Got {len(data['data'])} records for {metric}")
                if data["data"]:
                    print(f"Sample record for {metric}: {data['data'][0]}")
                return data
        except Exception as e:
            print(f"Exception fetching {metric}: {e}")
            return {"error": f"Exception fetching {metric}: {e}"}

    async def fetch_market_data(self, metrics: List[str], metric_frequencies: Dict[str, str], assets: List[str]):
        """Fetch market data for multiple metrics with their frequencies."""
        combined_data = []
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            # Create a task for each metric with its appropriate frequency
            for metric in metrics:
                if metric in metric_frequencies:
                    frequency = metric_frequencies[metric]
                    tasks.append(self.fetch_metric_data(session, metric, frequency, assets))
            
            # Run all tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Combine all results
            for result in results:
                if "data" in result and result["data"]:
                    combined_data.extend(result["data"])
            
        return {"data": combined_data}
    
    def fetch_token_prices_sync(self, token_symbols: List[str]) -> Dict[str, float]:
        """
        Fetch token prices synchronously for a list of token symbols.
        Returns a dictionary of token symbols and their prices.
        
        Note: This method will raise exceptions if prices cannot be fetched.
        No fallback prices are used to ensure data accuracy.
        """
        # Lowercase tokens and remove the 'W' prefix from wrapped tokens
        normalized_symbols = []
        symbol_map = {}  # Maps normalized symbol to original symbol
        
        for symbol in token_symbols:
            normalized = symbol.lower()
            # Handle wrapped tokens (WETH -> eth, WBTC -> btc, etc.)
            if normalized.startswith('w') and len(normalized) > 1:
                base_symbol = normalized[1:]
                normalized_symbols.append(base_symbol)
                symbol_map[base_symbol] = symbol
            else:
                normalized_symbols.append(normalized)
                symbol_map[normalized] = symbol
        
        # Remove duplicates
        normalized_symbols = list(set(normalized_symbols))
        
        # Current time and one hour ago
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_time = (datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Use ReferenceRate metric with 1s frequency
        params = {
            "assets": ",".join(normalized_symbols),
            "metrics": "ReferenceRate",
            "frequency": "1s",
            "api_key": self.api_key,
            "start_time": start_time,
            "end_time": end_time,
            "page_size": 100  # Use pagination parameters that are supported
        }
        
        response = requests.get(self.api_url, params=params)
        if response.status_code != 200:
            error_msg = f"Error fetching token prices: {response.status_code}"
            try:
                error_details = response.json()
                error_msg += f" - {error_details}"
            except:
                pass
            raise Exception(error_msg)
        
        data = response.json()
        if not data.get("data"):
            raise Exception("No price data returned from CoinMetrics API")
        
        # Process the results - get the most recent price for each asset
        result = {}
        asset_latest_data = {}
        
        # Group by asset and find the latest data point for each
        for item in data["data"]:
            if "asset" in item and "ReferenceRate" in item and "time" in item:
                normalized_symbol = item["asset"]
                time_str = item["time"]
                
                # Update if this is the first entry or a more recent one
                if normalized_symbol not in asset_latest_data or time_str > asset_latest_data[normalized_symbol]["time"]:
                    asset_latest_data[normalized_symbol] = {
                        "time": time_str,
                        "price": float(item["ReferenceRate"])
                    }
        
        # Map the normalized symbols back to original symbols
        for normalized_symbol, data_point in asset_latest_data.items():
            if normalized_symbol in symbol_map:
                original_symbol = symbol_map[normalized_symbol]
                result[original_symbol] = data_point["price"]
        
        # Check if we got all requested tokens
        missing_tokens = [symbol for symbol in token_symbols if symbol not in result]
        if missing_tokens:
            raise Exception(f"Could not fetch prices for these tokens: {', '.join(missing_tokens)}")
        
        
        print(f"fetch_token_prices_sync() {result}")
            
        return result 