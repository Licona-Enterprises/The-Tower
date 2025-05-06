import requests
import logging
from typing import Dict, List, Generator, Any, Optional, Tuple, Set, Union
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import ta
import math
import re
from scipy.stats import norm

class HistoricalPriceService:
    """
    Service to fetch historical price data from CoinMetrics API
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = "https://api.coinmetrics.io/v4/timeseries/asset-metrics"
        self.logger = logging.getLogger(__name__)
    
    def fetch_historical_prices(
        self, 
        token_symbols: List[str], 
        days_back: int = 365,
        batch_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch historical price data for a list of token symbols.
        
        Args:
            token_symbols: List of token symbols to fetch data for
            days_back: Number of days to look back from today (default 365)
            batch_size: Number of records to request in each API call
            
        Returns:
            Generator yielding batches of historical price data
        """
        # Normalize token symbols similar to the existing implementation
        normalized_symbols, symbol_map = self._normalize_symbols(token_symbols)
        
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        # Format times for API request
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Process each token symbol separately to avoid overwhelming the API
        for symbol_batch in self._batch_symbols(normalized_symbols, 5):
            yield from self._fetch_price_data_batched(
                symbol_batch,
                symbol_map,
                start_time_str,
                end_time_str,
                batch_size
            )
    
    def _normalize_symbols(self, token_symbols: List[str]) -> Tuple[List[str], Dict[str, str]]:
        """
        Normalize token symbols and create a mapping back to original symbols.
        Handles wrapped tokens (e.g., WETH -> eth).
        
        Returns:
            Tuple of (normalized_symbols, symbol_map)
        """
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
        
        return normalized_symbols, symbol_map
    
    def _batch_symbols(self, symbols: List[str], batch_size: int) -> Generator[List[str], None, None]:
        """
        Split a list of symbols into smaller batches.
        """
        for i in range(0, len(symbols), batch_size):
            yield symbols[i:i + batch_size]
    
    def _fetch_price_data_batched(
        self, 
        symbols: List[str],
        symbol_map: Dict[str, str],
        start_time: str,
        end_time: str,
        page_size: int
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch historical price data in batches using pagination.
        Yields batches of price data as they are received.
        """
        if not symbols:
            return
            
        next_page_token = None
        
        while True:
            try:
                # Prepare API parameters
                params = {
                    "assets": ",".join(symbols),
                    "metrics": "PriceUSD",
                    "frequency": "1d",
                    "api_key": self.api_key,
                    "start_time": start_time,
                    "end_time": end_time,
                    "page_size": page_size
                }
                
                # Add pagination token if we have one
                if next_page_token:
                    params["next_page_token"] = next_page_token
                
                # Make the API request
                self.logger.info(f"Fetching historical prices for {symbols}")
                response = requests.get(self.api_url, params=params)
                
                # Handle errors
                if response.status_code != 200:
                    error_msg = f"Error fetching historical prices: {response.status_code}"
                    try:
                        error_details = response.json()
                        error_msg += f" - {error_details}"
                    except:
                        pass
                    self.logger.error(error_msg)
                    break
                
                # Parse response
                data = response.json()
                
                # Check if we have data
                if not data.get("data"):
                    self.logger.warning(f"No price data returned for {symbols}")
                    break
                
                # Map normalized symbols back to original symbols in the response data
                processed_data = self._process_response_data(data["data"], symbol_map)
                
                # Yield the processed data batch
                yield processed_data
                
                # Check for pagination
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break
                    
                # Add a small delay to avoid API rate limits
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error fetching historical prices: {str(e)}")
                break
    
    def _process_response_data(self, data: List[Dict[str, Any]], symbol_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Process the response data to map normalized symbols back to original symbols.
        
        Returns a dictionary with historical price data organized by token symbol.
        """
        result = {}
        
        for item in data:
            if "asset" in item and "PriceUSD" in item and "time" in item:
                normalized_symbol = item["asset"]
                
                # Skip if we don't have a mapping for this symbol
                if normalized_symbol not in symbol_map:
                    continue
                    
                original_symbol = symbol_map[normalized_symbol]
                
                # Initialize entry for this symbol if it doesn't exist
                if original_symbol not in result:
                    result[original_symbol] = []
                
                # Add the price data point
                result[original_symbol].append({
                    "time": item["time"],
                    "price": float(item["PriceUSD"])
                })
        
        return result

    def get_price_history(
        self, 
        token_symbols: List[str], 
        days_back: int = 365
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get complete historical price data for the specified tokens.
        
        This is a convenience method that collects all data from the generator
        and returns a complete dataset.
        
        Args:
            token_symbols: List of token symbols to fetch data for
            days_back: Number of days to look back from today
            
        Returns:
            Dictionary mapping token symbols to their historical price data
        """
        result = {}
        
        for batch in self.fetch_historical_prices(token_symbols, days_back):
            for symbol, price_data in batch.items():
                if symbol not in result:
                    result[symbol] = []
                result[symbol].extend(price_data)
        
        # Sort price data by time for each token
        for symbol in result:
            result[symbol].sort(key=lambda x: x["time"])
            
        return result

    def price_history_to_dataframe(
        self, 
        price_history: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, pd.DataFrame]:
        """
        Convert price history data to pandas DataFrames for each token.
        
        Args:
            price_history: Dictionary of price history data as returned by get_price_history
            
        Returns:
            Dictionary mapping token symbols to pandas DataFrames with price history
        """
        result = {}
        
        for symbol, price_data in price_history.items():
            if not price_data:
                continue
                
            # Create DataFrame
            df = pd.DataFrame(price_data)
            
            # Convert time strings to datetime objects
            df['time'] = pd.to_datetime(df['time'])
            
            # Set time as index
            df.set_index('time', inplace=True)
            
            # Rename price column to 'close' for compatibility with TA libraries
            df.rename(columns={'price': 'close'}, inplace=True)
            
            # Add required columns for TA calculations (imputed with close price)
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
            df['volume'] = 0  # We don't have volume data
            
            result[symbol] = df
            
        return result
        
    def add_technical_indicators(
        self, 
        dataframes: Dict[str, pd.DataFrame], 
        indicators: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Add technical indicators to price DataFrames.
        
        Args:
            dataframes: Dictionary mapping token symbols to price DataFrames
            indicators: List of indicator types to add. If None, adds a default set.
            
        Returns:
            Dictionary mapping token symbols to DataFrames with added indicators
        """
        # Define default indicators if none provided
        if indicators is None:
            indicators = self.get_indicator_names()
            
        result = {}
        
        for symbol, df in dataframes.items():
            # Create a copy to avoid modifying the original
            df_with_indicators = df.copy()
            
            # Add indicators based on the list
            if 'sma' in indicators:
                # Simple Moving Averages
                df_with_indicators['sma_20'] = ta.trend.sma_indicator(df_with_indicators['close'], window=20)
                df_with_indicators['sma_50'] = ta.trend.sma_indicator(df_with_indicators['close'], window=50)
                df_with_indicators['sma_200'] = ta.trend.sma_indicator(df_with_indicators['close'], window=200)
                
            if 'ema' in indicators:
                # Exponential Moving Averages
                df_with_indicators['ema_20'] = ta.trend.ema_indicator(df_with_indicators['close'], window=20)
                df_with_indicators['ema_50'] = ta.trend.ema_indicator(df_with_indicators['close'], window=50)
                df_with_indicators['ema_200'] = ta.trend.ema_indicator(df_with_indicators['close'], window=200)
                
            if 'rsi' in indicators:
                # Relative Strength Index
                df_with_indicators['rsi_14'] = ta.momentum.rsi(df_with_indicators['close'], window=14)
                
            if 'macd' in indicators:
                # MACD
                macd = ta.trend.MACD(df_with_indicators['close'], window_fast=12, window_slow=26, window_sign=9)
                df_with_indicators['macd'] = macd.macd()
                df_with_indicators['macd_signal'] = macd.macd_signal()
                df_with_indicators['macd_diff'] = macd.macd_diff()
                
            if 'bbands' in indicators:
                # Bollinger Bands
                bollinger = ta.volatility.BollingerBands(df_with_indicators['close'], window=20, window_dev=2)
                df_with_indicators['bollinger_mavg'] = bollinger.bollinger_mavg()
                df_with_indicators['bollinger_hband'] = bollinger.bollinger_hband()
                df_with_indicators['bollinger_lband'] = bollinger.bollinger_lband()
                
            if 'stoch' in indicators:
                # Stochastic Oscillator
                stoch = ta.momentum.StochasticOscillator(
                    high=df_with_indicators['high'], 
                    low=df_with_indicators['low'], 
                    close=df_with_indicators['close'],
                    window=14, 
                    smooth_window=3
                )
                df_with_indicators['stoch_k'] = stoch.stoch()
                df_with_indicators['stoch_d'] = stoch.stoch_signal()
                
            # Drop rows with NaN values that occur at the beginning due to indicator calculations
            df_with_indicators.dropna(inplace=True)
            
            result[symbol] = df_with_indicators
            
        return result
        
    def get_price_history_with_indicators(
        self, 
        token_symbols: List[str], 
        days_back: int = 365,
        indicators: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Get complete historical price data with technical indicators for the specified tokens.
        
        Args:
            token_symbols: List of token symbols to fetch data for
            days_back: Number of days to look back from today
            indicators: List of indicator types to add. If None, adds a default set.
            
        Returns:
            Dictionary mapping token symbols to DataFrames with price history and indicators
        """
        # Get raw price history
        price_history = self.get_price_history(token_symbols, days_back)
        
        # Convert to DataFrames
        price_dfs = self.price_history_to_dataframe(price_history)
        
        # Add technical indicators
        result_dfs = self.add_technical_indicators(price_dfs, indicators)
        
        return result_dfs
        
    def get_indicator_names(self) -> List[str]:
        """
        Get a list of available technical indicator types.
        
        Returns:
            List of indicator type names that can be passed to add_technical_indicators
        """
        return [
            'sma',      # Simple Moving Average
            'ema',      # Exponential Moving Average
            'rsi',      # Relative Strength Index
            'macd',     # Moving Average Convergence Divergence
            'bbands',   # Bollinger Bands
            'stoch',    # Stochastic Oscillator
        ]
        
    def calculate_realized_volatility(
        self, 
        dataframe: pd.DataFrame, 
        window: int = 30, 
        trading_periods: int = 365,
        column: str = 'close'
    ) -> pd.Series:
        """
        Calculate realized volatility (historical volatility) over a specified window.
        
        Args:
            dataframe: DataFrame containing price data
            window: Window size in days for volatility calculation
            trading_periods: Number of trading periods in a year (365 for crypto)
            column: Column name to use for price data
            
        Returns:
            Series containing annualized realized volatility
        """
        # Calculate log returns
        log_returns = np.log(dataframe[column] / dataframe[column].shift(1))
        
        # Calculate rolling standard deviation of log returns
        rolling_std = log_returns.rolling(window=window).std()
        
        # Annualize the volatility
        realized_vol = rolling_std * np.sqrt(trading_periods)
        
        return realized_vol
    
    def add_volatility_metrics(
        self, 
        dataframes: Dict[str, pd.DataFrame],
        windows: List[int] = [7, 30, 90]
    ) -> Dict[str, pd.DataFrame]:
        """
        Add various volatility metrics to price DataFrames.
        
        Args:
            dataframes: Dictionary mapping token symbols to price DataFrames
            windows: List of window sizes in days for volatility calculations
            
        Returns:
            Dictionary mapping token symbols to DataFrames with added volatility metrics
        """
        result = {}
        
        for symbol, df in dataframes.items():
            # Create a copy to avoid modifying the original
            df_with_volatility = df.copy()
            
            # Calculate realized volatility for different windows
            for window in windows:
                vol_series = self.calculate_realized_volatility(df, window=window)
                df_with_volatility[f'realized_vol_{window}d'] = vol_series
                
                # Calculate rolling high and low volatility (highest/lowest in the period)
                if window > 1:
                    df_with_volatility[f'realized_vol_high_{window}d'] = vol_series.rolling(window=window).max()
                    df_with_volatility[f'realized_vol_low_{window}d'] = vol_series.rolling(window=window).min()
            
            # Volatility ratio (short-term vs long-term)
            if len(windows) >= 2:
                shortest = min(windows)
                longest = max(windows)
                df_with_volatility[f'vol_ratio_{shortest}d_{longest}d'] = (
                    df_with_volatility[f'realized_vol_{shortest}d'] / 
                    df_with_volatility[f'realized_vol_{longest}d']
                )
            
            # Conditional calculations: 
            # - GARCH or other advanced volatility models could be added here
            # - Implied volatility approximations from market data could be added here
                
            # Calculate Parkinson volatility (uses high-low range)
            if 'high' in df.columns and 'low' in df.columns and not (df['high'] == df['low']).all():
                # Calculate Parkinson volatility - uses high-low range
                # This is useful when we have proper high/low values
                ln_hl_ratio = np.log(df['high'] / df['low'])
                parkinson_vol = np.sqrt(1 / (4 * np.log(2)) * ln_hl_ratio**2)
                
                # Rolling Parkinson volatility (annualized)
                for window in windows:
                    df_with_volatility[f'parkinson_vol_{window}d'] = (
                        parkinson_vol.rolling(window=window).mean() * np.sqrt(365)
                    )
                    
            # Drop rows with NaN values that occur at the beginning 
            # due to volatility calculations
            df_with_volatility.dropna(inplace=True)
            
            result[symbol] = df_with_volatility
            
        return result
    
    def get_price_history_with_volatility(
        self, 
        token_symbols: List[str], 
        days_back: int = 365,
        with_indicators: bool = True,
        indicators: Optional[List[str]] = None,
        volatility_windows: List[int] = [7, 30, 90],
        use_option_data: bool = True,
        option_days_back: int = 30,
        exchange: str = "deribit"
    ) -> Dict[str, pd.DataFrame]:
        """
        Get complete historical price data with volatility metrics and optional indicators.
        
        Args:
            token_symbols: List of token symbols to fetch data for
            days_back: Number of days to look back from today
            with_indicators: Whether to include technical indicators
            indicators: List of indicator types to add if with_indicators is True
            volatility_windows: List of window sizes for volatility calculations
            use_option_data: Always true - using real option data for implied volatility
            option_days_back: Number of days of option data to fetch
            exchange: Exchange to fetch option data from
            
        Returns:
            Dictionary mapping token symbols to DataFrames with price history and metrics
        """
        # Get raw price history
        price_history = self.get_price_history(token_symbols, days_back)
        
        # Convert to DataFrames
        price_dfs = self.price_history_to_dataframe(price_history)
        
        # Add technical indicators if requested
        if with_indicators:
            price_dfs = self.add_technical_indicators(price_dfs, indicators)
        
        # Add volatility metrics
        result_dfs = self.add_volatility_metrics(price_dfs, volatility_windows)
        
        # Always calculate implied volatility from options
        self.logger.info(f"Fetching option data for {token_symbols} over {option_days_back} days from {exchange}")
        # Fetch option prices and calculate real implied volatility
        option_days = min(option_days_back, days_back)
        option_data = {}
        
        # Fetch option data
        try:
            option_data = self.fetch_option_prices(token_symbols, option_days, exchange)
            if not option_data:
                raise ValueError("No option data returned from API")
        except Exception as fetch_error:
            self.logger.error(f"Error fetching option prices: {str(fetch_error)}")
            raise ValueError(f"Failed to fetch option data. Cannot calculate implied volatility: {str(fetch_error)}")
        
        # Flag to track if any IV was successfully calculated
        iv_calculated = False
        
        # Process each token with option data
        for symbol in token_symbols:
            if symbol in option_data and symbol in result_dfs:
                self.logger.info(f"Calculating IV for {symbol}")
                
                try:
                    # Calculate IV from option prices
                    iv_df = self.calculate_implied_volatility_from_options(
                        option_data[symbol],
                        price_dfs[symbol]  # Use the original price data for this
                    )
                    
                    if not iv_df.empty:
                        # For each date in the IV data, find the corresponding date in the result
                        for date in iv_df['date'].unique():
                            try:
                                # Filter IV data for this date
                                date_iv = iv_df[iv_df['date'] == date]
                                
                                # Calculate average IV (weight by moneyness, favoring at-the-money)
                                # At-the-money options (moneyness close to 1.0) are more relevant
                                date_iv['weight'] = 1.0 / (abs(date_iv['moneyness'] - 1.0) + 0.1)
                                weighted_iv = (date_iv['implied_volatility'] * date_iv['weight']).sum() / date_iv['weight'].sum()
                                
                                # Also get IV for different moneyness levels
                                atm_iv = date_iv[(date_iv['moneyness'] >= 0.95) & (date_iv['moneyness'] <= 1.05)]['implied_volatility'].mean()
                                otm_put_iv = date_iv[(date_iv['moneyness'] < 0.95) & (date_iv['option_type'] == 'put')]['implied_volatility'].mean()
                                otm_call_iv = date_iv[(date_iv['moneyness'] > 1.05) & (date_iv['option_type'] == 'call')]['implied_volatility'].mean()
                                
                                # Find the matching date in the price data
                                # Date from IV data is pandas Timestamp
                                match_date = pd.Timestamp(date).normalize()
                                
                                # Add to the result if the date exists
                                if match_date in result_dfs[symbol].index:
                                    result_dfs[symbol].loc[match_date, 'implied_volatility'] = weighted_iv
                                    iv_calculated = True
                                    
                                    # Add more detailed IV metrics if available
                                    if not pd.isna(atm_iv):
                                        result_dfs[symbol].loc[match_date, 'iv_atm'] = atm_iv
                                    if not pd.isna(otm_put_iv):
                                        result_dfs[symbol].loc[match_date, 'iv_otm_put'] = otm_put_iv
                                    if not pd.isna(otm_call_iv):
                                        result_dfs[symbol].loc[match_date, 'iv_otm_call'] = otm_call_iv
                                        
                                    # Calculate volatility skew if we have both put and call IV
                                    if not pd.isna(otm_put_iv) and not pd.isna(otm_call_iv):
                                        result_dfs[symbol].loc[match_date, 'iv_skew'] = otm_put_iv - otm_call_iv
                            except Exception as e:
                                self.logger.warning(f"Error processing IV for date {date}: {str(e)}")
                                continue
                            
                        # Note that we used real option data
                        result_dfs[symbol]['used_option_data'] = True
                except Exception as calc_error:
                    self.logger.error(f"Error calculating IV for {symbol}: {str(calc_error)}")
                    # Raise error if implied volatility calculation fails
                    raise ValueError(f"Failed to calculate implied volatility for {symbol}: {str(calc_error)}")
        
        # If we couldn't calculate IV for any token, raise an error
        if not iv_calculated:
            self.logger.error("No implied volatility could be calculated from option data")
            raise ValueError("Unable to calculate implied volatility from options data")
        
        return result_dfs

    def fetch_option_prices(
        self,
        token_symbols: List[str],
        days_back: int = 30,
        exchange: str = "deribit",
        granularity: str = "1d",
        max_retries: int = 2,
        timeout: int = 30
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch option prices from CoinMetrics API.
        
        Args:
            token_symbols: List of token symbols to fetch option data for
            days_back: Number of days to look back from today
            exchange: Exchange to fetch option data from (default: deribit)
            granularity: Data granularity (default: 1d)
            max_retries: Maximum number of retry attempts for API calls
            timeout: Timeout in seconds for API requests
            
        Returns:
            Dictionary mapping token symbols to DataFrames with option price data
        """
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        # Format times for API request
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        result = {}
        
        for symbol in token_symbols:
            self.logger.info(f"Fetching option prices for {symbol}")
            
            # Special handling for each token symbol
            normalized_symbol = symbol.lower()
            # Handle wrapped tokens
            if normalized_symbol.startswith('w') and len(normalized_symbol) > 1:
                normalized_symbol = normalized_symbol[1:]
            
            # Format the market parameter
            market_param = f"{exchange}-{normalized_symbol.upper()}-*-option"
            
            # Fetch option data with pagination
            all_option_data = []
            next_page_token = None
            retry_count = 0
            
            while True:
                try:
                    # API endpoint
                    api_url = "https://api.coinmetrics.io/v4/timeseries/market-contract-prices"
                    
                    # Prepare parameters
                    params = {
                        "markets": market_param,
                        "start_time": start_time_str,
                        "end_time": end_time_str,
                        "page_size": 1000,
                        "api_key": self.api_key,
                        "granularity": granularity
                    }
                    
                    # Add pagination token if we have one
                    if next_page_token:
                        params["next_page_token"] = next_page_token
                        
                    # Make the API request with timeout
                    self.logger.info(f"Fetching option data page for {symbol} (retry: {retry_count})")
                    response = requests.get(api_url, params=params, timeout=timeout)
                    
                    # Handle errors
                    if response.status_code != 200:
                        error_msg = f"Error fetching option prices: {response.status_code}"
                        try:
                            error_details = response.json()
                            error_msg += f" - {error_details}"
                        except:
                            pass
                        
                        # Determine if we should retry
                        retry_status_codes = [429, 500, 502, 503, 504, 524]
                        if response.status_code in retry_status_codes and retry_count < max_retries:
                            retry_count += 1
                            retry_delay = 2 ** retry_count  # Exponential backoff
                            self.logger.warning(f"{error_msg}. Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            self.logger.error(error_msg)
                            break
                    
                    # Reset retry count on success
                    retry_count = 0
                    
                    # Parse response
                    data = response.json()
                    
                    # Check if we have data
                    if not data.get("data"):
                        self.logger.warning(f"No option data returned for {symbol}")
                        break
                    
                    # Extend our dataset
                    all_option_data.extend(data["data"])
                    
                    # Check for pagination
                    next_page_token = data.get("next_page_token")
                    if not next_page_token:
                        break
                        
                    # Add a small delay to avoid API rate limits
                    time.sleep(0.5)
                    
                except requests.exceptions.Timeout:
                    error_msg = f"Timeout error fetching option prices for {symbol}"
                    # Retry on timeout
                    if retry_count < max_retries:
                        retry_count += 1
                        retry_delay = 2 ** retry_count
                        self.logger.warning(f"{error_msg}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.logger.error(f"{error_msg}. Max retries exceeded.")
                        break
                except requests.exceptions.ConnectionError:
                    error_msg = f"Connection error fetching option prices for {symbol}"
                    # Retry on connection error
                    if retry_count < max_retries:
                        retry_count += 1
                        retry_delay = 2 ** retry_count
                        self.logger.warning(f"{error_msg}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.logger.error(f"{error_msg}. Max retries exceeded.")
                        break
                except Exception as e:
                    self.logger.error(f"Error fetching option prices: {str(e)}")
                    break
            
            # Process option data
            if all_option_data:
                df = self._process_option_data(all_option_data, symbol)
                if not df.empty:
                    result[symbol] = df
                    
        return result
    
    def _process_option_data(self, data: List[Dict[str, Any]], symbol: str) -> pd.DataFrame:
        """
        Process option data into a structured DataFrame.
        
        Args:
            data: List of option data items from the API
            symbol: Symbol the data is for
            
        Returns:
            DataFrame with processed option data
        """
        if not data:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Extract contract details from market name
        df['symbol'] = symbol.upper()
        
        # Parse market name to extract option details
        # Example market: deribit-BTC-24JUN22-20000-C (format may vary)
        df['expiry_date'] = None
        df['strike'] = None
        df['option_type'] = None
        
        # Function to parse market name
        def parse_market(market_name):
            # Regular expression to extract details from deribit market name
            # Note: Pattern may need adjustment based on actual format
            pattern = r'([a-zA-Z0-9]+)-([A-Z]+)-([^-]+)-(\d+)-([CP])'
            match = re.match(pattern, market_name)
            
            if match:
                exchange, base, expiry, strike, option_type = match.groups()
                return {
                    'exchange': exchange,
                    'base_currency': base,
                    'expiry': expiry,
                    'strike': float(strike),
                    'option_type': 'call' if option_type == 'C' else 'put'
                }
            return {
                'exchange': '',
                'base_currency': '',
                'expiry': '',
                'strike': np.nan,
                'option_type': ''
            }
        
        # Apply parsing to each market name
        parsed_markets = df['market'].apply(parse_market)
        
        # Add parsed columns to the DataFrame
        df['exchange'] = parsed_markets.apply(lambda x: x['exchange'])
        df['base_currency'] = parsed_markets.apply(lambda x: x['base_currency'])
        df['expiry'] = parsed_markets.apply(lambda x: x['expiry'])
        df['strike'] = parsed_markets.apply(lambda x: x['strike'])
        df['option_type'] = parsed_markets.apply(lambda x: x['option_type'])
        
        # Convert time to datetime and set as index
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
        
        # Handle pricing columns
        required_columns = ['price', 'volume']
        for col in required_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    def calculate_implied_volatility_from_options(
        self,
        option_data: pd.DataFrame,
        spot_prices: pd.DataFrame,
        risk_free_rate: float = 0.03  # Approximate risk-free rate
    ) -> pd.DataFrame:
        """
        Calculate implied volatility from option prices using the Black-Scholes model.
        
        Args:
            option_data: DataFrame with option prices
            spot_prices: DataFrame with spot prices
            risk_free_rate: Risk-free interest rate (approximation)
            
        Returns:
            DataFrame with implied volatility for different strikes and expiries
        """
        def black_scholes_iv(row):
            """Calculate implied volatility for a single option."""
            try:
                # Extract required data
                S = spot_price  # Current spot price
                K = row['strike']  # Strike price
                
                # Make sure both current_date and expiry_date are timezone-aware or both are timezone-naive
                if hasattr(expiry_date, 'tzinfo') and expiry_date.tzinfo is not None:
                    # If expiry_date is timezone-aware, ensure current_date is also timezone-aware
                    if hasattr(current_date, 'tzinfo') and current_date.tzinfo is None:
                        # Convert current_date to timezone-aware with UTC
                        aware_current_date = pd.Timestamp(current_date).tz_localize('UTC')
                        T = (expiry_date - aware_current_date).total_seconds() / (365.25 * 24 * 3600)  # Time to expiry in years
                    else:
                        T = (expiry_date - current_date).total_seconds() / (365.25 * 24 * 3600)
                else:
                    # If expiry_date is timezone-naive, ensure current_date is also timezone-naive
                    if hasattr(current_date, 'tzinfo') and current_date.tzinfo is not None:
                        # Convert current_date to timezone-naive
                        naive_current_date = pd.Timestamp(current_date).tz_localize(None)
                        T = (expiry_date - naive_current_date).total_seconds() / (365.25 * 24 * 3600)
                    else:
                        T = (expiry_date - current_date).total_seconds() / (365.25 * 24 * 3600)
                
                r = risk_free_rate  # Risk-free rate
                option_price = row['price']
                option_type = row['option_type']
                
                # Initial volatility guess
                sigma = 0.5
                precision = 0.00001
                max_iterations = 100
                
                # Newton-Raphson method to find IV
                for i in range(max_iterations):
                    d1 = (math.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * math.sqrt(T))
                    d2 = d1 - sigma * math.sqrt(T)
                    
                    if option_type == 'call':
                        price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
                        vega = S * math.sqrt(T) * norm.pdf(d1)
                    else:  # put
                        price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                        vega = S * math.sqrt(T) * norm.pdf(d1)
                    
                    diff = option_price - price
                    if abs(diff) < precision:
                        return sigma
                    
                    # Update volatility
                    sigma = sigma + diff / vega
                    
                    # Break if volatility is out of reasonable bounds
                    if sigma <= 0 or sigma > 5:
                        return np.nan
                
                return np.nan  # Failed to converge
            except Exception as e:
                self.logger.warning(f"IV calculation error: {str(e)}")
                return np.nan
        
        # Get unique dates in the option data
        option_dates = option_data.index.unique()
        
        # Initialize result DataFrame
        result = pd.DataFrame()
        
        for date in option_dates:
            # Get options for this date
            options_on_date = option_data.loc[date]
            
            # Get spot price for this date
            try:
                spot_price = spot_prices.loc[date, 'close']
                current_date = date
            except KeyError:
                # Use nearest available date if exact match not found
                nearest_date = spot_prices.index[spot_prices.index.get_indexer([date], method='nearest')[0]]
                spot_price = spot_prices.loc[nearest_date, 'close']
                current_date = nearest_date
            
            # Get unique expiry dates for this date
            expiry_strings = options_on_date['expiry'].unique()
            
            # Create rows for IV surface
            iv_rows = []
            
            # Process each expiry
            for expiry_str in expiry_strings:
                # Parse expiry date string to datetime (format may need adjustment)
                try:
                    # Deribit format is often like 24JUN22
                    expiry_date = pd.to_datetime(expiry_str, format='%d%b%y', errors='coerce')
                    if pd.isna(expiry_date):
                        # Try alternative formats if the first one fails
                        formats = ['%Y%m%d', '%d%B%Y', '%b-%d-%Y']
                        for fmt in formats:
                            try:
                                expiry_date = pd.to_datetime(expiry_str, format=fmt)
                                if not pd.isna(expiry_date):
                                    break
                            except:
                                continue
                except:
                    # Skip this expiry if we can't parse the date
                    continue
                
                # Skip if we couldn't parse the date
                if pd.isna(expiry_date):
                    continue
                
                # Make sure expiry_date is timezone-naive if current_date is timezone-naive
                # Or make expiry_date timezone-aware if current_date is timezone-aware
                if hasattr(current_date, 'tzinfo') and current_date.tzinfo is not None:
                    # current_date is timezone-aware, ensure expiry_date is also timezone-aware
                    if hasattr(expiry_date, 'tzinfo') and expiry_date.tzinfo is None:
                        expiry_date = pd.Timestamp(expiry_date).tz_localize('UTC')
                else:
                    # current_date is timezone-naive, ensure expiry_date is also timezone-naive
                    if hasattr(expiry_date, 'tzinfo') and expiry_date.tzinfo is not None:
                        expiry_date = pd.Timestamp(expiry_date).tz_localize(None)
                
                # Calculate days to expiry (handle timezone-aware or timezone-naive dates)
                try:
                    days_to_expiry = (expiry_date - current_date).days
                except TypeError:
                    # Handle the case where there's still a mismatch
                    self.logger.warning(f"Timezone mismatch: current_date={current_date} ({type(current_date)}), expiry_date={expiry_date} ({type(expiry_date)})")
                    # Convert both to timezone-naive as a fallback
                    naive_current = pd.Timestamp(current_date).tz_localize(None) if hasattr(current_date, 'tzinfo') and current_date.tzinfo is not None else current_date
                    naive_expiry = pd.Timestamp(expiry_date).tz_localize(None) if hasattr(expiry_date, 'tzinfo') and expiry_date.tzinfo is not None else expiry_date
                    days_to_expiry = (naive_expiry - naive_current).days
                
                if days_to_expiry <= 0:
                    continue  # Skip expired options
                
                # Get options for this expiry
                expiry_options = options_on_date[options_on_date['expiry'] == expiry_str]
                
                # Calculate IV for each strike and option type
                for _, option_row in expiry_options.iterrows():
                    try:
                        iv = black_scholes_iv(option_row)
                        
                        if not pd.isna(iv):
                            iv_rows.append({
                                'date': date,
                                'expiry_date': expiry_date,
                                'days_to_expiry': days_to_expiry,
                                'strike': option_row['strike'],
                                'option_type': option_row['option_type'],
                                'spot_price': spot_price,
                                'option_price': option_row['price'],
                                'implied_volatility': iv,
                                'moneyness': option_row['strike'] / spot_price
                            })
                    except Exception as e:
                        # Skip this option on error
                        self.logger.warning(f"Error calculating IV: {str(e)}")
                        continue
            
            # Create DataFrame for this date and append to result
            if iv_rows:
                date_df = pd.DataFrame(iv_rows)
                result = pd.concat([result, date_df])
        
        return result
    
    def get_implied_volatility_surface(
        self,
        token_symbols: List[str],
        days_back: int = 30,
        exchange: str = "deribit"
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get implied volatility surface data for specified tokens.
        
        Args:
            token_symbols: List of token symbols to fetch IV data for
            days_back: Number of days to look back
            exchange: Exchange to fetch option data from
            
        Returns:
            Dictionary mapping token symbols to dictionaries containing:
                - 'options': DataFrame with raw option data
                - 'spot': DataFrame with spot price data
                - 'iv': DataFrame with calculated implied volatility data
        """
        result = {}
        
        # Fetch spot prices for the tokens
        spot_price_history = self.get_price_history(token_symbols, days_back)
        spot_price_dfs = self.price_history_to_dataframe(spot_price_history)
        
        # Fetch option prices
        option_data = self.fetch_option_prices(token_symbols, days_back, exchange)
        
        # Calculate implied volatility for each token
        for symbol in token_symbols:
            if symbol in option_data and symbol in spot_price_dfs:
                # Calculate IV using the option data and spot prices
                iv_data = self.calculate_implied_volatility_from_options(
                    option_data[symbol],
                    spot_price_dfs[symbol]
                )
                
                # Store results
                result[symbol] = {
                    'options': option_data[symbol],
                    'spot': spot_price_dfs[symbol],
                    'iv': iv_data
                }
            else:
                self.logger.warning(f"Missing option data or spot prices for {symbol}")
                
        return result 