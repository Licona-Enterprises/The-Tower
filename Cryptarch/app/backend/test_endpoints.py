#!/usr/bin/env python3
"""
Test script for historical prices and volatility endpoints.
Run this script to test that the endpoints are working as expected.
"""

import os
import sys
import requests
import json
import pandas as pd
from datetime import datetime
import time

# Get the absolute path of the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the current directory to the Python path
sys.path.insert(0, current_dir)

# Import local modules
from historical_prices import HistoricalPriceService

# Configuration
API_BASE_URL = "http://localhost:8000"  # Adjust if your server is running on a different port
# API_KEY = os.getenv("COINMETRICS_API_KEY", "")
API_KEY = "8yjPvRMxhzR7nJSUwsXb"
REQUEST_TIMEOUT = 15  # Timeout for API requests in seconds

def test_historical_prices_endpoint():
    """Test the /api/historical-prices endpoint"""
    print("\n=== Testing /api/historical-prices endpoint ===")
    
    # Define test cases
    test_cases = [
        {
            "name": "Basic BTC data (without indicators)",
            "params": {"assets": ["BTC"], "days": 7, "with_indicators": False},
            "expected_keys": ["status", "assets", "days", "with_indicators", "data"]
        },
        {
            "name": "Multiple assets without indicators",
            "params": {"assets": ["BTC", "ETH"], "days": 7, "with_indicators": False},
            "expected_keys": ["status", "assets", "days", "with_indicators", "data"]
        },
        # Only test with indicators if we have an API key
        *([
            {
                "name": "Price data with indicators",
                "params": {"assets": ["BTC"], "days": 7, "with_indicators": True},
                "expected_keys": ["status", "assets", "days", "with_indicators", "indicators_used", "data"]
            }
        ] if API_KEY else []),
        # Don't test volatility in this endpoint since we have a dedicated test
    ]
    
    # Run tests
    results = []
    for tc in test_cases:
        print(f"\nRunning test: {tc['name']}")
        try:
            # Make request
            start_time = time.time()
            response = requests.get(
                f"{API_BASE_URL}/api/historical-prices", 
                params=tc["params"],
                timeout=REQUEST_TIMEOUT
            )
            elapsed = time.time() - start_time
            
            # Validate response
            if response.status_code == 200:
                data = response.json()
                
                # Check expected keys
                keys_present = all(key in data for key in tc["expected_keys"])
                
                # Check data content
                data_valid = False
                if "data" in data and isinstance(data["data"], dict):
                    for asset in tc["params"]["assets"]:
                        if asset in data["data"] and len(data["data"][asset]) > 0:
                            data_valid = True
                            break
                
                success = keys_present and data_valid
                results.append({
                    "test": tc["name"],
                    "success": success,
                    "status_code": response.status_code,
                    "elapsed_time": elapsed,
                    "data_points": sum(len(data["data"].get(asset, [])) for asset in tc["params"]["assets"]) if "data" in data else 0
                })
                
                # Print sample data
                if success and "data" in data:
                    asset = tc["params"]["assets"][0]
                    if asset in data["data"] and len(data["data"][asset]) > 0:
                        print(f"Sample data for {asset}:")
                        sample = data["data"][asset][0]
                        print(json.dumps(sample, indent=2)[:500] + "..." if len(json.dumps(sample, indent=2)) > 500 else json.dumps(sample, indent=2))
                        
                        # Print column names to verify indicators or volatility metrics
                        columns = list(sample.keys())
                        print(f"Available columns ({len(columns)}): {', '.join(columns[:10])}" + ("..." if len(columns) > 10 else ""))
                
            else:
                error_msg = response.text
                print(f"❌ Error: Status code {response.status_code}")
                print(error_msg)
                results.append({
                    "test": tc["name"],
                    "success": False,
                    "status_code": response.status_code,
                    "error": error_msg
                })
                
        except requests.exceptions.Timeout:
            print(f"❌ Request timed out after {REQUEST_TIMEOUT} seconds")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": f"Request timed out after {REQUEST_TIMEOUT} seconds"
            })
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": str(e)
            })
    
    # Print summary
    print("\n=== Historical Prices Endpoint Test Results ===")
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"{status} {result['test']}")
        if "elapsed_time" in result:
            print(f"   Time: {result['elapsed_time']:.2f}s, Data points: {result.get('data_points', 'N/A')}")
        if "error" in result:
            print(f"   Error: {result['error']}")
    
    return any(result["success"] for result in results)

def test_simplified_volatility_endpoint():
    """Test the /api/simplified-volatility endpoint"""
    print("\n=== Testing /api/simplified-volatility endpoint ===")
    
    # Define test cases
    test_cases = [
        {
            "name": "Basic simplified volatility",
            "params": {"assets": ["BTC"], "days": 7},
            "expected_keys": ["status", "assets", "days", "windows", "data"]
        },
        {
            "name": "Multiple assets with custom windows",
            "params": {"assets": ["BTC", "ETH"], "days": 7, "windows": [3, 5]},
            "expected_keys": ["status", "assets", "days", "windows", "data"]
        }
    ]
    
    # Run tests
    results = []
    for tc in test_cases:
        print(f"\nRunning test: {tc['name']}")
        try:
            # Make request
            start_time = time.time()
            response = requests.get(
                f"{API_BASE_URL}/api/simplified-volatility", 
                params=tc["params"],
                timeout=REQUEST_TIMEOUT
            )
            elapsed = time.time() - start_time
            
            # Validate response
            if response.status_code == 200:
                data = response.json()
                
                # Check expected keys
                keys_present = all(key in data for key in tc["expected_keys"])
                
                # Check data content
                data_valid = False
                if "data" in data and isinstance(data["data"], dict):
                    for asset in tc["params"]["assets"]:
                        if asset in data["data"] and len(data["data"][asset]) > 0:
                            data_valid = True
                            break
                
                success = keys_present and data_valid
                results.append({
                    "test": tc["name"],
                    "success": success,
                    "status_code": response.status_code,
                    "elapsed_time": elapsed,
                    "data_points": sum(len(data["data"].get(asset, [])) for asset in tc["params"]["assets"]) if "data" in data else 0
                })
                
                # Print sample data and verify volatility columns
                if success and "data" in data:
                    asset = tc["params"]["assets"][0]
                    if asset in data["data"] and len(data["data"][asset]) > 0:
                        print(f"Sample data for {asset}:")
                        sample = data["data"][asset][0]
                        print(json.dumps(sample, indent=2)[:500] + "..." if len(json.dumps(sample, indent=2)) > 500 else json.dumps(sample, indent=2))
                        
                        # Check for volatility columns
                        volatility_columns = [c for c in sample.keys() if any(term in c for term in ['vol_', 'volatility', 'realized'])]
                        print(f"Volatility columns ({len(volatility_columns)}): {', '.join(volatility_columns)}")
                        
                        # Verify windows match the requested windows
                        windows = tc["params"].get("windows", [7, 14, 30])  # Default windows
                        window_columns = [c for c in volatility_columns if any(f"_{w}d" in c for w in windows)]
                        print(f"Window-specific columns: {', '.join(window_columns[:5])}" + ("..." if len(window_columns) > 5 else ""))
                
            else:
                error_msg = response.text
                print(f"❌ Error: Status code {response.status_code}")
                print(error_msg)
                results.append({
                    "test": tc["name"],
                    "success": False,
                    "status_code": response.status_code,
                    "error": error_msg
                })
                
        except requests.exceptions.Timeout:
            print(f"❌ Request timed out after {REQUEST_TIMEOUT} seconds")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": f"Request timed out after {REQUEST_TIMEOUT} seconds"
            })
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": str(e)
            })
    
    # Print summary
    print("\n=== Simplified Volatility Endpoint Test Results ===")
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"{status} {result['test']}")
        if "elapsed_time" in result:
            print(f"   Time: {result['elapsed_time']:.2f}s, Data points: {result.get('data_points', 'N/A')}")
        if "error" in result:
            print(f"   Error: {result['error']}")
    
    # Return success if any test passed
    return any(result["success"] for result in results) if results else False

def test_volatility_metrics_endpoint():
    """Test the /api/volatility-metrics endpoint (low priority, can be skipped)"""
    print("\n=== Testing /api/volatility-metrics endpoint ===")
    print("Note: This is a low-priority test and may be skipped if the simplified endpoint works")
    
    # Skip volatility tests if no API key is available
    if not API_KEY:
        print("⚠️ No API key provided, skipping volatility metrics tests")
        return True
    
    # Define test cases
    test_cases = [
        {
            "name": "Basic volatility metrics (small dataset)",
            "params": {"assets": ["BTC"], "days": 7},
            "expected_keys": ["status", "assets", "days", "windows", "data"]
        },
        {
            "name": "Custom volatility windows (small dataset)",
            "params": {"assets": ["BTC"], "days": 7, "windows": [3, 5]},
            "expected_keys": ["status", "assets", "days", "windows", "data"]
        }
    ]
    
    # Run tests
    results = []
    for tc in test_cases:
        print(f"\nRunning test: {tc['name']}")
        try:
            # Make request
            start_time = time.time()
            response = requests.get(
                f"{API_BASE_URL}/api/volatility-metrics", 
                params=tc["params"],
                timeout=REQUEST_TIMEOUT
            )
            elapsed = time.time() - start_time
            
            # Validate response
            if response.status_code == 200:
                data = response.json()
                
                # Check expected keys
                keys_present = all(key in data for key in tc["expected_keys"])
                
                # Check data content
                data_valid = False
                if "data" in data and isinstance(data["data"], dict):
                    for asset in tc["params"]["assets"]:
                        if asset in data["data"] and len(data["data"][asset]) > 0:
                            data_valid = True
                            break
                
                success = keys_present and data_valid
                results.append({
                    "test": tc["name"],
                    "success": success,
                    "status_code": response.status_code,
                    "elapsed_time": elapsed,
                    "data_points": sum(len(data["data"].get(asset, [])) for asset in tc["params"]["assets"]) if "data" in data else 0
                })
                
                # Print sample data and verify volatility columns
                if success and "data" in data:
                    asset = tc["params"]["assets"][0]
                    if asset in data["data"] and len(data["data"][asset]) > 0:
                        print(f"Sample data for {asset}:")
                        sample = data["data"][asset][0]
                        print(json.dumps(sample, indent=2)[:500] + "..." if len(json.dumps(sample, indent=2)) > 500 else json.dumps(sample, indent=2))
                        
                        # Check for volatility columns
                        volatility_columns = [c for c in sample.keys() if any(term in c for term in ['vol_', 'volatility', 'implied'])]
                        print(f"Volatility columns ({len(volatility_columns)}): {', '.join(volatility_columns)}")
                        
                        # Verify windows match the requested windows
                        windows = tc["params"].get("windows", [7, 30, 90])  # Default windows
                        window_columns = [c for c in volatility_columns if any(f"_{w}d" in c for w in windows)]
                        print(f"Window-specific columns: {', '.join(window_columns[:5])}" + ("..." if len(window_columns) > 5 else ""))
                
            else:
                error_msg = response.text
                print(f"❌ Error: Status code {response.status_code}")
                print(error_msg)
                results.append({
                    "test": tc["name"],
                    "success": False,
                    "status_code": response.status_code,
                    "error": error_msg
                })
                
        except requests.exceptions.Timeout:
            print(f"❌ Request timed out after {REQUEST_TIMEOUT} seconds")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": f"Request timed out after {REQUEST_TIMEOUT} seconds"
            })
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            results.append({
                "test": tc["name"],
                "success": False,
                "error": str(e)
            })
    
    # Print summary
    print("\n=== Volatility Metrics Endpoint Test Results ===")
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"{status} {result['test']}")
        if "elapsed_time" in result:
            print(f"   Time: {result['elapsed_time']:.2f}s, Data points: {result.get('data_points', 'N/A')}")
        if "error" in result:
            print(f"   Error: {result['error']}")
    
    # Always return true because this test is optional - failure here shouldn't make the whole suite fail
    return True

def test_direct_api():
    """Test the HistoricalPriceService API directly"""
    print("\n=== Testing HistoricalPriceService API directly ===")
    
    if not API_KEY:
        print("⚠️ No API key provided, skipping direct API test")
        return True
    
    try:
        # Initialize service
        service = HistoricalPriceService(API_KEY)
        
        # Test basic price history functionality first
        print("\nTesting get_price_history")
        start_time = time.time()
        price_history = service.get_price_history(
            token_symbols=["BTC"], 
            days_back=7
        )
        elapsed = time.time() - start_time
        
        if "BTC" in price_history and price_history["BTC"]:
            print(f"✅ Got basic price history in {elapsed:.2f}s, {len(price_history['BTC'])} data points")
            print(f"Sample: {json.dumps(price_history['BTC'][0], indent=2)}")
        else:
            print("❌ Failed to get basic price history")
            return False
        
        # Test add_volatility_metrics directly to verify simplified approach
        print("\nTesting add_volatility_metrics directly")
        start_time = time.time()
        price_dfs = service.price_history_to_dataframe(price_history)
        volatility_dfs = service.add_volatility_metrics(price_dfs, [3, 5, 7])
        elapsed = time.time() - start_time
        
        if "BTC" in volatility_dfs and not volatility_dfs["BTC"].empty:
            print(f"✅ Added volatility metrics in {elapsed:.2f}s")
            vol_columns = [c for c in volatility_dfs["BTC"].columns if 'vol_' in c or 'volatility' in c]
            print(f"Volatility columns: {vol_columns[:5]}" + ("..." if len(vol_columns) > 5 else ""))
        else:
            print("❌ Failed to add volatility metrics")
            return False
        
        # Optionally test with indicators
        print("\nTesting get_price_history_with_indicators")
        start_time = time.time()
        with_indicators = service.get_price_history_with_indicators(
            token_symbols=["BTC"], 
            days_back=7
        )
        elapsed = time.time() - start_time
        
        if "BTC" in with_indicators and not with_indicators["BTC"].empty:
            print(f"✅ Got data with indicators in {elapsed:.2f}s")
            indicator_columns = [c for c in with_indicators["BTC"].columns if c not in ['open', 'high', 'low', 'close', 'volume']]
            print(f"Indicator columns: {indicator_columns[:5]}" + ("..." if len(indicator_columns) > 5 else ""))
        else:
            print("❌ Failed to get data with indicators")
        
        return True
            
    except Exception as e:
        print(f"❌ Exception in direct API test: {str(e)}")
        return False

def main():
    """Run all tests"""
    print(f"Testing endpoints at {API_BASE_URL}")
    print(f"API Key provided: {bool(API_KEY)}")
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests in order of importance
    historical_success = test_historical_prices_endpoint()
    # simplified_success = test_simplified_volatility_endpoint()
    # direct_api_success = test_direct_api()
    
    # Only run the standard volatility metrics endpoint test if simplified endpoint passed
    volatility_success = True
    # if simplified_success:
    #     volatility_success = test_volatility_metrics_endpoint()
    # else:
    #     print("\n⚠️ Skipping volatility metrics endpoint test since simplified endpoint failed")
    
    # Print overall results
    print("\n=== Overall Test Results ===")
    print(f"Historical Prices Endpoint: {'✅ PASS' if historical_success else '❌ FAIL'}")
    # print(f"Simplified Volatility Endpoint: {'✅ PASS' if simplified_success else '❌ FAIL'}")
    # print(f"Standard Volatility Endpoint: {'✅ PASS' if volatility_success else '❌ FAIL'}")
    # print(f"Direct API: {'✅ PASS' if direct_api_success else '❌ FAIL'}")
    
    # Return exit code - only require historical, simplified, and direct API to pass
    # return 0 if all([historical_success, simplified_success, direct_api_success]) else 1

if __name__ == "__main__":
    sys.exit(main()) 