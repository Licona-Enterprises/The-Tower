import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime

# Direct import using explicit file path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backend.consts as consts
from app.frontend.api_service import ApiService  # Fix the import path

def render_aave_token_balances_page():
    """
    Render the AAVE token balances page showing token balances from block explorers.
    """
    st.subheader("AAVE Token Balances")
    
    # Create filter options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        portfolio_filter = st.text_input("Filter by Portfolio", key="aave_balances_portfolio_filter")
    
    with col2:
        strategy_filter = st.text_input("Filter by Strategy", key="aave_balances_strategy_filter")
    
    with col3:
        chain_filter = st.selectbox(
            "Filter by Chain", 
            ["All", "polygon", "arbitrum", "optimism", "base"], 
            key="aave_balances_chain_filter"
        )
    
    # Add refresh button and last refresh time display
    col_refresh, col_time = st.columns([1, 3])
    
    with col_refresh:
        refresh_pressed = st.button("🔄 Refresh Data", key="refresh_aave_balances")
    
    with col_time:
        if 'aave_balances_last_refresh' not in st.session_state:
            st.session_state.aave_balances_last_refresh = datetime.now()
        
        st.markdown(f"<p class='refresh-time'>Last refreshed: {st.session_state.aave_balances_last_refresh.strftime('%Y-%m-%d %H:%M:%S')}</p>", 
                  unsafe_allow_html=True)
    
    # Function to fetch AAVE token balances from API
    def fetch_aave_balances_from_api():
        try:
            # Update refresh timestamp
            st.session_state.aave_balances_last_refresh = datetime.now()
            
            # Initialize API service
            api_service = ApiService()
            
            # Prepare query parameters
            params = {}
            if portfolio_filter:
                params['portfolio'] = portfolio_filter
            if strategy_filter:
                params['strategy'] = strategy_filter
            if chain_filter != "All":
                params['chain'] = chain_filter
            
            # Fetch data from API
            response = api_service.get("aave-token-balances", params)
            
            if response and "balances" in response and response["balances"]:
                # Convert API response to DataFrame
                balances_data = response["balances"]
                return pd.DataFrame(balances_data)
            else:
                error_msg = "No data available"
                if response and "error" in response:
                    error_msg = response["error"]
                elif response and "count" in response and response["count"] == 0:
                    error_msg = "No AAVE token balances found with current filters"
                st.warning(f"{error_msg}")
                return pd.DataFrame()
                
        except Exception as e:
            st.error(f"Error fetching AAVE token balances: {str(e)}")
            return pd.DataFrame()
    
    # Fetch the data (with caching to avoid redundant API calls)
    if 'aave_balances_df' not in st.session_state or refresh_pressed:
        with st.spinner("Loading AAVE token balances..."):
            st.session_state.aave_balances_df = fetch_aave_balances_from_api()
    
    # Get the current data
    df = st.session_state.aave_balances_df.copy()
    
    if not df.empty:
        # Display the data table
        st.dataframe(
            df,
            column_config={
                "chain": st.column_config.TextColumn("Chain", width="small"),
                "portfolio": st.column_config.TextColumn("Portfolio", width="medium"),
                "strategy": st.column_config.TextColumn("Strategy", width="medium"),
                "wallet": st.column_config.TextColumn("Wallet Address", width="large"),
                "token": st.column_config.TextColumn("Token", width="small"),
                "balance": st.column_config.NumberColumn("Balance", width="medium", format="%.6f")
            },
            use_container_width=True,
            hide_index=True
        )
        
        # Add visualizations
        st.subheader("Token Distribution by Chain")
        
        # Group by chain and token, then sum balances
        chain_token_balances = df.groupby(['chain', 'token'])['balance'].sum().reset_index()
        
        # Create a pivot table for visualization
        pivot_df = chain_token_balances.pivot(index='token', columns='chain', values='balance').fillna(0)
        
        # Display the chart
        st.bar_chart(pivot_df)
        
        # Also display a pie chart for overall token distribution
        st.subheader("Overall Token Distribution")
        token_balances = df.groupby('token')['balance'].sum().reset_index()
        
        try:
            import plotly.express as px
            
            fig = px.pie(token_balances, values='balance', names='token',
                        title='AAVE Token Distribution',
                        hover_data=['balance'], 
                        labels={'balance':'Balance'})
            
            # Update traces
            fig.update_traces(textposition='inside', textinfo='percent+label')
            
            # Display the pie chart
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.warning("Install plotly to see pie chart visualization.")
    else:
        st.info("No AAVE token balances data available with the current filters.")
        
        # Display a more detailed message with troubleshooting steps
        with st.expander("Troubleshooting"):
            st.write("""
            Possible reasons for no data:
            1. No Aave tokens in the portfolios or strategies
            2. API keys for block explorers not configured correctly
            3. Connection issues with block explorer APIs
            4. The filters you applied don't match any available data
            
            Steps to troubleshoot:
            1. Check that the required API keys are set in .env
            2. Verify network connectivity
            3. Try removing any filters
            4. Check the TOKENS dictionary in backend/consts.py for correct token addresses
            """)
        
        # Provide button to retry
        if st.button("Retry Connection", key="retry_aave_balances"):
            st.session_state.pop('aave_balances_df', None)
            st.experimental_rerun() 