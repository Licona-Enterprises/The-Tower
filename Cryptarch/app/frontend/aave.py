import streamlit as st
import pandas as pd
import sys
import os
import json
import requests
from io import BytesIO
import time

# Direct import using explicit file path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backend.consts as consts
PORTFOLIOS = consts.PORTFOLIOS

def render_aave_page(api_service):
    """
    Render the AAVE positions page.
    
    Args:
        api_service: Instance of ApiService for making API calls
    """
    st.subheader("AAVE Lending Positions")
    
    # Create filter options
    col1, col2 = st.columns(2)
    
    with col1:
        portfolio_filter = st.text_input("Filter by Portfolio", key="aave_portfolio_filter")
    
    with col2:
        strategy_filter = st.text_input("Filter by Strategy", key="aave_strategy_filter")
    
    # Function to fetch token prices using CoinMetricsService
    def fetch_token_prices(token_symbols):
        """Fetch real-time token prices from CoinMetrics API"""
        try:
            # Initialize CoinMetricsService
            from backend.coinmetrics import CoinMetricsService
            coinmetrics = CoinMetricsService()
            
            # Fetch prices using the synchronous method
            prices = coinmetrics.fetch_token_prices_sync(token_symbols)
            
            return prices
        except Exception as e:
            st.error(f"Error fetching token prices: {str(e)}")
            st.warning("Please make sure the COINMETRICS_API_KEY environment variable is set correctly.")
            # Return empty dict - the UI will need to handle missing prices
            return {}
    
    # Function to fetch AAVE positions data
    def fetch_aave_positions():
        params = {}
        if portfolio_filter:
            params['portfolio'] = portfolio_filter
        if strategy_filter:
            params['strategy'] = strategy_filter
            
        try:
            # Use the protocol-only endpoint to get AAVE positions
            response = api_service.get("aave/positions/protocol-only", params)
            return response
        except Exception as e:
            st.error(f"Error fetching AAVE positions: {str(e)}")
            return None
    
    # Add a debug checkbox
    debug_mode = st.checkbox("Debug Mode", value=False, key="aave_debug_mode")
    
    # Fetch AAVE positions data for the table
    with st.spinner("Loading AAVE positions..."):
        positions_data = fetch_aave_positions()
    
    # Display raw API response in debug mode
    if debug_mode and positions_data:
        st.subheader("Raw API Response")
        st.json(positions_data)
    
    if positions_data and debug_mode:
        # Check if wallets key exists and has data
        if "wallets" in positions_data and positions_data["wallets"]:
            st.success(f"API returned {len(positions_data['wallets'])} wallets with AAVE positions.")
        else:
            st.warning("API returned a response but no wallet data was found.")
            st.write("API response keys:", list(positions_data.keys()))
    
    # Process data according to the actual API response structure
    position_table_data = []
    
    if positions_data and "wallets" in positions_data:
        wallets = positions_data["wallets"]
        
        # Collect all token symbols
        token_symbols = set()
        
        # Process each wallet
        for wallet in wallets:
            wallet_address = wallet.get('wallet_address', 'Unknown')
            portfolio = wallet.get('portfolio', 'Unknown')
            strategy = wallet.get('strategy', 'Unknown')
            
            if debug_mode:
                st.write(f"Processing wallet: {wallet_address}")
            
            # Process tokens in the wallet
            tokens = wallet.get('tokens', [])
            for token in tokens:
                token_symbol = token.get('symbol', 'Unknown')
                token_name = token.get('name', 'Unknown')
                
                # Add to token symbols set
                if token_symbol:
                    token_symbols.add(token_symbol)
                
                # Handle potential errors when converting amount to float
                try:
                    token_amount = float(token.get('amount', 0))
                except (ValueError, TypeError):
                    token_amount = 0
                    if debug_mode:
                        st.error(f"Error converting amount to float: {token.get('amount', 0)}")
                
                # Add to table data
                position_table_data.append({
                    "Portfolio": portfolio,
                    "Strategy": strategy,
                    "Token": token_symbol,
                    "Token Name": token_name,
                    "Amount": token_amount,
                    "Network": token.get('network', 'Unknown'),
                    "Wallet": wallet_address
                })
                
                if debug_mode:
                    st.write(f"Added token: {token_symbol} - {token_amount}")
        
        if debug_mode:
            st.write("Found token symbols:", list(token_symbols))
    
    # Create a DataFrame
    if position_table_data:
        positions_df = pd.DataFrame(position_table_data)
        
        # Display the filtered table with enhanced styling 
        st.dataframe(
            positions_df,
            column_config={
                "Portfolio": st.column_config.TextColumn("Portfolio", width="medium"),
                "Strategy": st.column_config.TextColumn("Strategy", width="medium"),
                "Wallet": st.column_config.TextColumn("Wallet Address", width="large"),
                "Token": st.column_config.TextColumn("Token", width="small"),
                "Token Name": st.column_config.TextColumn("Token Name", width="medium"),
                "Amount": st.column_config.NumberColumn("Amount", width="medium", format="%.6f"),
                "Network": st.column_config.TextColumn("Network", width="small")
            },
            use_container_width=True,
            hide_index=True
        )
        
        # Add visualization of token distribution if we have data
        if not positions_df.empty:
            st.subheader("Token Distribution")
            
            # Group by token and sum amounts
            token_amounts = positions_df.groupby("Token")["Amount"].sum().reset_index()
            
            # Create a bar chart of token amounts
            st.bar_chart(token_amounts.set_index("Token"))
            
            # Also display as a pie chart
            try:
                import plotly.express as px
                
                fig = px.pie(token_amounts, values='Amount', names='Token',
                            title='AAVE Token Distribution',
                            hover_data=['Amount'], 
                            labels={'Amount':'Amount'})
                
                # Update traces
                fig.update_traces(textposition='inside', textinfo='percent+label')
                
                # Display the pie chart
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.warning("Install plotly to see pie chart visualization.")
    else:
        # Display an error message if no data is available
        st.error("No AAVE positions data available.")
        
        # Display a more detailed message with troubleshooting steps
        st.warning("""
        Possible reasons for no data:
        1. The backend API is down or not properly configured
        2. There are no AAVE positions in the portfolio
        3. The API endpoint is not accessible
        
        Steps to troubleshoot:
        1. Check that the backend server is running
        2. Verify network connectivity
        3. Check the API logs for any errors
        4. Try refreshing the page
        """)
        
        # Provide button to retry
        if st.button("Retry Connection"):
            st.experimental_rerun() 