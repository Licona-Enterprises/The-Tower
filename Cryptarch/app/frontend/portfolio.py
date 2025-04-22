import streamlit as st
import pandas as pd
import sys
import os

# Direct import using explicit file path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backend.consts as consts
DEFAULT_ETH_ADDRESSES = consts.DEFAULT_ETH_ADDRESSES

def render_portfolio_page(api_service):
    """
    Render the portfolio balances page.
    
    Args:
        api_service: Instance of ApiService for making API calls
    """
    st.subheader("Portfolio Balances")
    
    # Initialize addresses in session state if not present
    if 'addresses' not in st.session_state:
        st.session_state.addresses = DEFAULT_ETH_ADDRESSES
    
    # Addresses input
    st.markdown("<div class='wallet-section'>", unsafe_allow_html=True)
    
    # Edit addresses
    st.text_area("Enter Ethereum addresses (one per line)", 
                value="\n".join(st.session_state.addresses),
                key="address_input", 
                height=150)
    
    # Parse addresses from text area
    if st.session_state.address_input:
        addresses = [addr.strip() for addr in st.session_state.address_input.split('\n') if addr.strip()]
        st.session_state.addresses = addresses
    
    # Fetch button
    if st.button("Fetch Portfolio Balances"):
        with st.spinner("Fetching balances..."):
            # Add a network selection
            network = "arbitrum"  # Default to arbitrum for now, but this could be a dropdown
            
            # Use the API service to fetch ETH balances
            balances_data = api_service.fetch_eth_balances(st.session_state.addresses, chain=network)
            
            if balances_data and balances_data.get('status') == 'success' and 'data' in balances_data:
                # Create DataFrame from balance data
                balance_df = pd.DataFrame(balances_data['data'])
                
                # Format for display
                balance_df = balance_df.rename(columns={
                    'account': 'Address',
                    'balance': 'Balance (Wei)',
                    'balance_eth': 'ETH Balance'
                })
                
                # Calculate total ETH
                total_eth = balance_df['ETH Balance'].sum()
                
                # Create a DataFrame for the total
                total_df = pd.DataFrame({
                    'Metric': ['Total ETH Balance', 'Number of Addresses'],
                    'Value': [f"{total_eth:.6f} ETH", f"{len(balance_df)}"]
                })
                
                # Display total in a card at the top
                st.metric(
                    label="Total ETH Balance", 
                    value=f"{total_eth:.6f} ETH",
                    delta=f"{len(balance_df)} addresses"
                )
                
                # Create two columns for the summary and details
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.subheader("Portfolio Summary")
                    # Configure columns for total table
                    summary_config = {
                        "Metric": st.column_config.TextColumn(
                            "Metric",
                            help="Balance summary",
                            width="medium"
                        ),
                        "Value": st.column_config.TextColumn(
                            "Value",
                            help="Value",
                            width="medium"
                        )
                    }
                    
                    # Display the summary table
                    st.dataframe(
                        total_df,
                        column_config=summary_config,
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # Show average ETH per address if we have addresses
                    if len(balance_df) > 0:
                        avg_eth = total_eth / len(balance_df)
                        st.info(f"Average ETH per address: {avg_eth:.6f} ETH")
                        
                        # Add USD value estimate if price is known
                        if 'eth_price' in st.session_state and st.session_state.eth_price:
                            usd_value = total_eth * st.session_state.eth_price
                            st.success(f"Estimated USD value: ${usd_value:,.2f}")
                
                with col2:
                    st.subheader("Address Details")
                    # Configure columns for address details
                    balance_config = {
                        "Address": st.column_config.TextColumn(
                            "Address",
                            help="Ethereum address",
                            width="large"
                        ),
                        "ETH Balance": st.column_config.NumberColumn(
                            "ETH Balance",
                            help="ETH balance on Arbitrum",
                            format="%.6f",
                            width="medium"
                        )
                    }
                    
                    # Display the ETH balances
                    st.dataframe(
                        balance_df,
                        column_config=balance_config,
                        hide_index=True,
                        use_container_width=True
                    )
            else:
                error_msg = balances_data.get('error', 'Unknown error') if balances_data else 'Failed to fetch data'
                st.error(f"Error fetching balances: {error_msg}")
    
    st.markdown("</div>", unsafe_allow_html=True) 