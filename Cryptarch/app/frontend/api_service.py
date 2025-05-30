import requests
import streamlit as st
from backend.consts import DEFAULT_ASSETS # Import DEFAULT_ASSETS directly assuming 'app' is in sys.path

class ApiService:
    """Service for handling API interactions from the frontend."""
    
    def __init__(self, api_base_url="http://localhost:8000"):
        self.api_base_url = api_base_url
    
    def get(self, endpoint, params=None):
        """Generic method to make GET requests to any API endpoint."""
        try:
            response = requests.get(
                f"{self.api_base_url}/api/{endpoint}",
                params=params
            )
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.json()
        except Exception as e:
            st.error(f"Error fetching data from /{endpoint}: {e}")
            return None
        
    def fetch_single_asset(self, asset, metrics):
        """Fetch data for a single asset with specified metrics."""
        try:
            # Make API request for a single asset
            response = requests.get(
                f"{self.api_base_url}/api/market-data",
                params={
                    'metrics': metrics,
                    'assets': [asset]  # Request just one asset
                }
            )
            
            data = response.json()
            return data
        except Exception as e:
            st.error(f"Error fetching data for {asset}: {e}")
            return None
            
    def fetch_market_data(self, assets, metrics):
        """Fetch market data for multiple assets and metrics."""
        all_data = []
        asset_statuses = {}
        
        # Make individual requests for each asset
        for asset in assets:
            asset_data = self.fetch_single_asset(asset, metrics)
            if asset_data and 'data' in asset_data and asset_data['data']:
                all_data.extend(asset_data['data'])
                asset_statuses[asset] = "Success"
            else:
                asset_statuses[asset] = "No data"
        
        # Return combined data
        return {"data": all_data, "asset_statuses": asset_statuses}
        
    def fetch_eth_balance(self, address, chain):
        """Fetch ETH balance for a single address."""
        try:
            response = requests.get(
                f"{self.api_base_url}/api/eth-balance/{address}",
                params={'chain': chain}
            )
            return response.json()
        except Exception as e:
            st.error(f"Error fetching ETH balance for {address}: {e}")
            return None
            
    def fetch_eth_balances(self, addresses, chain):
        """Fetch ETH balances for multiple addresses."""
        try:
            response = requests.get(
                f"{self.api_base_url}/api/eth-balances",
                params={
                    'chain': chain,
                    'addresses': addresses
                }
            )
            return response.json()
        except Exception as e:
            st.error(f"Error fetching ETH balances: {e}")
            return None

    def fetch_historical_prices(self, assets, days, indicators=None, with_volatility=False):
        params = {
            "assets": assets,
            "days": days,
            "with_indicators": True, # Assuming we always want indicators for now
            "with_volatility": with_volatility
        }
        if indicators:
            params["indicators"] = indicators
        
        response = requests.get(f"{self.api_base_url}/api/historical-prices", params=params)
        response.raise_for_status() # Raise an exception for bad status codes
        return response.json()
    
    def get_available_assets(self):
        # Using the imported DEFAULT_ASSETS from the backend constants
        return DEFAULT_ASSETS

    # Placeholder for fetching available indicators - implement if needed
    # def get_available_indicators(self):
    #     # Fetch from an endpoint or define statically
    #     response = requests.get(f"{self.api_base_url}/api/available-indicators") 
    #     response.raise_for_status()
    #     return response.json().get("indicators", [])

# Example usage:
if __name__ == "__main__":
    pass 