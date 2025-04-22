import streamlit as st
import time
import pandas as pd
import sys
import os

# Direct import using explicit file path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backend.consts as consts
from backend.coinmetrics import CoinMetricsService
PORTFOLIOS = consts.PORTFOLIOS

def plot_tick_range(current_tick, lower_tick, upper_tick):
    """
    Create a visual representation of a Uniswap V3 position's tick range.
    
    Args:
        current_tick: Current tick of the pool
        lower_tick: Lower tick of the position
        upper_tick: Upper tick of the position
    
    Returns:
        Matplotlib figure
    """
    fig, ax = plt.subplots(figsize=(8, 2))
    
    # Calculate range for visualization
    range_size = upper_tick - lower_tick
    buffer = range_size * 0.3  # Add 30% buffer on each side
    
    # Define the x-axis range
    x_min = lower_tick - buffer
    x_max = upper_tick + buffer
    
    # Create the plot
    # The base line representing the tick range
    ax.plot([x_min, x_max], [1, 1], color='#dddddd', linewidth=2)
    
    # The active position range
    ax.plot([lower_tick, upper_tick], [1, 1], color='blue', linewidth=6)
    
    # Markers for the ticks
    ax.scatter([lower_tick], [1], color='blue', s=100, zorder=5, label='Lower Tick')
    ax.scatter([upper_tick], [1], color='blue', s=100, zorder=5, label='Upper Tick')
    
    # Current tick marker
    ax.scatter([current_tick], [1], color='red', s=150, marker='*', zorder=10, label='Current Tick')
    
    # Add tick values as text
    ax.text(lower_tick, 0.85, f"Lower: {lower_tick}", ha='center', fontsize=9)
    ax.text(upper_tick, 0.85, f"Upper: {upper_tick}", ha='center', fontsize=9)
    ax.text(current_tick, 1.15, f"Current: {current_tick}", ha='center', fontsize=9, 
            color='red', fontweight='bold')
    
    # Customize the plot
    ax.set_ylim(0.5, 1.5)
    ax.set_xlim(x_min, x_max)
    ax.set_ylabel('')
    ax.set_title('Position Tick Range Visualization')
    
    # Remove y-axis ticks and labels
    ax.set_yticks([])
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    
    # Add "In Range" or "Out of Range" indicator
    if lower_tick <= current_tick <= upper_tick:
        status_text = "IN RANGE"
        text_color = 'green'
    else:
        status_text = "OUT OF RANGE"
        text_color = 'red'
    
    ax.annotate(status_text, xy=(0.5, 0.5), xycoords='axes fraction',
                fontsize=15, color=text_color, fontweight='bold',
                ha='center', va='center', alpha=0.3)
    
    fig.tight_layout()
    return fig

def render_uniswap_page(api_service):
    """
    Render the Uniswap V3 positions page.
    
    Args:
        api_service: Instance of ApiService for making API calls
    """
    st.subheader("Uniswap V3 Positions")
    
    # Create filter options
    col1, col2 = st.columns(2)
    
    with col1:
        portfolio_filter = st.text_input("Filter by Portfolio", key="portfolio_filter")
    
    with col2:
        strategy_filter = st.text_input("Filter by Strategy", key="strategy_filter")
            
    # Function to fetch token prices using CoinMetricsService
    def fetch_token_prices(token_symbols):
        """Fetch real-time token prices from CoinMetrics API"""
        try:
            # Initialize CoinMetricsService
            coinmetrics = CoinMetricsService()
            
            # Fetch prices using the synchronous method
            prices = coinmetrics.fetch_token_prices_sync(token_symbols)
            
            return prices
        except Exception as e:
            st.error(f"Error fetching token prices: {str(e)}")
            st.warning("Please make sure the COINMETRICS_API_KEY environment variable is set correctly.")
            # Return empty dict - the UI will need to handle missing prices
            return {}
    
    # Function to fetch Uniswap positions data - moved up to use in both table and positions
    def fetch_uniswap_positions():
        params = {}
        if portfolio_filter:
            params['portfolio'] = portfolio_filter
        if strategy_filter:
            params['strategy'] = strategy_filter
            
        try:
            response = api_service.get("uniswap/positions", params)
            return response
        except Exception as e:
            st.error(f"Error fetching positions: {str(e)}")
            return None
    
    # Fetch Uniswap positions data for the table
    with st.spinner("Loading Uniswap positions..."):
        positions_data = fetch_uniswap_positions()
    
    # Store the selected position ID in session state if not already there
    if 'selected_position_id' not in st.session_state:
        st.session_state.selected_position_id = None
    
    # Display position summary without heading
    
    if positions_data and "positions" in positions_data:
        positions = positions_data["positions"]
        
        # Collect all token symbols to fetch prices
        token_symbols = set()
        for position in positions:
            token_symbols.add(position['token0']['symbol'])
            token_symbols.add(position['token1']['symbol'])
        
        # Fetch token prices
        with st.spinner("Fetching token prices..."):
            token_prices = fetch_token_prices(list(token_symbols))
            
        # Create a structured table of positions with status information
        position_table_data = []
        for position in positions:
            # Calculate status
            liquidity = int(position['position']['liquidity'])
            status = "Active" if liquidity > 0 else "Closed"
            
            # Calculate price status
            price_status = "N/A"
            if liquidity > 0:
                current_tick = position['pool']['current_tick']
                tick_lower = position['position']['tick_lower']
                tick_upper = position['position']['tick_upper']
                
                if tick_lower <= current_tick <= tick_upper:
                    price_status = "In Range"
                elif current_tick < tick_lower:
                    price_status = "Below Range"
                else:
                    price_status = "Above Range"
            
            # Only include positions that are "In Range"
            if price_status == "In Range":
                # Get token amounts
                token0_amount = float(position['token0']['amount'])
                token1_amount = float(position['token1']['amount'])
                
                # Calculate USD values
                token0_symbol = position['token0']['symbol']
                token1_symbol = position['token1']['symbol']
                token0_price = token_prices.get(token0_symbol, 0)
                token1_price = token_prices.get(token1_symbol, 0)
                
                # Show warning if prices are missing
                if token0_price == 0 or token1_price == 0:
                    st.warning(f"Could not fetch real-time prices for {token0_symbol if token0_price == 0 else ''} {token1_symbol if token1_price == 0 else ''}. Value calculations may be inaccurate.")
                
                token0_value_usd = token0_amount * token0_price
                token1_value_usd = token1_amount * token1_price
                total_value_usd = token0_value_usd + token1_value_usd
                
                # Add to table data
                position_table_data.append({
                    "Portfolio": position.get('portfolio', 'Unknown'),
                    "Strategy": position.get('strategy', 'N/A'),
                    "Tokens": f"{token0_symbol} - {token1_symbol}",
                    "Token0 Amount": token0_amount,
                    "Token1 Amount": token1_amount,
                    "Token0 USD": token0_value_usd,
                    "Token1 USD": token1_value_usd,
                    "Total USD": total_value_usd,
                    "Status": status,
                    "Price Status": price_status,
                    "Position ID": position['token_id'],
                    "Wallet": position['wallet_address'],
                    "Fee Tier": f"{position['pool']['fee']}%"
                })
        
        # Create a DataFrame
        if position_table_data:
            positions_df = pd.DataFrame(position_table_data)
            
            # Add compact metrics row
            total_positions = len(positions)
            active_count = sum(1 for p in positions if int(p['position']['liquidity']) > 0)
            in_range_count = len(position_table_data)
            
            # Display the filtered table with enhanced styling 
            st.dataframe(
                positions_df,
                column_config={
                    "Position ID": st.column_config.TextColumn("Position ID", width="small"),
                    "Portfolio": st.column_config.TextColumn("Portfolio", width="medium"),
                    "Strategy": st.column_config.TextColumn("Strategy", width="medium"),
                    "Wallet": st.column_config.TextColumn("Wallet Address", width="large"),
                    "Tokens": st.column_config.TextColumn("Tokens", width="small"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "Price Status": st.column_config.TextColumn("Price Status", width="medium"),
                    "Token0 Amount": st.column_config.NumberColumn("Token0", width="medium", format="%.6f"),
                    "Token1 Amount": st.column_config.NumberColumn("Token1", width="medium", format="%.6f"),
                    "Token0 USD": st.column_config.NumberColumn("Token0 USD", width="medium", format="$%.2f"),
                    "Token1 USD": st.column_config.NumberColumn("Token1 USD", width="medium", format="$%.2f"),
                    "Total USD": st.column_config.NumberColumn("Total USD", width="medium", format="$%.2f"),
                    "Fee Tier": st.column_config.TextColumn("Fee Tier", width="small")
                },
                use_container_width=True,
                hide_index=True
            )
            
            # Use a selectbox for position selection instead
            position_ids = positions_df["Position ID"].tolist()
            position_labels = [f"Position {pid} - {row['Tokens']}" 
                              for pid, row in zip(position_ids, positions_df.to_dict('records'))]
            
            selected_position_label = st.selectbox(
                "Select a position for details:",
                ["None"] + position_labels,
                index=0
            )
            
            # Handle selection
            if selected_position_label != "None":
                # Extract position ID from the label
                selected_index = position_labels.index(selected_position_label)
                selected_position_id = position_ids[selected_index]
                st.session_state.selected_position_id = selected_position_id
        else:
            st.info("No in-range positions found with the current filters.")
            
        # Show position details if a position is selected
        if st.session_state.selected_position_id:
            # Find the selected position
            selected_position = None
            for position in positions:
                if position['token_id'] == st.session_state.selected_position_id:
                    selected_position = position
                    break
            
            if selected_position:
                st.markdown("### Selected Position Details")
                
                # Status details
                liquidity = int(selected_position['position']['liquidity'])
                status = "Active" if liquidity > 0 else "Closed"
                
                current_tick = selected_position['pool']['current_tick']
                tick_lower = selected_position['position']['tick_lower']
                tick_upper = selected_position['position']['tick_upper']
                
                # Get token amounts and calculate USD values
                token0_symbol = selected_position['token0']['symbol']
                token1_symbol = selected_position['token1']['symbol']
                token0_amount = float(selected_position['token0']['amount'])
                token1_amount = float(selected_position['token1']['amount'])
                
                # Calculate USD values if prices are available
                token0_price = token_prices.get(token0_symbol, 0)
                token1_price = token_prices.get(token1_symbol, 0)
                
                # Show warning if prices are missing
                if token0_price == 0 or token1_price == 0:
                    st.warning(f"Could not fetch real-time prices for {token0_symbol if token0_price == 0 else ''} {token1_symbol if token1_price == 0 else ''}. Value calculations may be inaccurate.")
                
                token0_value_usd = token0_amount * token0_price
                token1_value_usd = token1_amount * token1_price
                total_value_usd = token0_value_usd + token1_value_usd
                
                price_status = "N/A"
                if liquidity > 0:
                    if tick_lower <= current_tick <= tick_upper:
                        price_status = "In Range"
                    elif current_tick < tick_lower:
                        price_status = "Below Range"
                    else:
                        price_status = "Above Range"
                
                # Create a two-column layout
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    # Position details in a table
                    combined_details = {
                        "Position ID": selected_position['token_id'],
                        "Portfolio": selected_position.get('portfolio', 'Unknown'),
                        "Strategy": selected_position.get('strategy', 'N/A'),
                        "Status": status,
                        "Price Status": price_status,
                        "Liquidity": format(liquidity, ","),
                        "Fee Tier": f"{selected_position['pool']['fee']}%",
                        f"{token0_symbol} Amount": token0_amount,
                        f"{token1_symbol} Amount": token1_amount,
                        f"{token0_symbol} Value (USD)": f"${token0_value_usd:.2f}",
                        f"{token1_symbol} Value (USD)": f"${token1_value_usd:.2f}",
                        "Total Value (USD)": f"${total_value_usd:.2f}",
                        f"{token0_symbol} Uncollected Fees": float(selected_position['token0']['uncollected_fees']),
                        f"{token1_symbol} Uncollected Fees": float(selected_position['token1']['uncollected_fees']),
                    }
                    
                    # Create DataFrame
                    combined_df = pd.DataFrame(combined_details.items(), columns=["Metric", "Value"])
                    
                    # Add color coding for price status
                    if price_status == "In Range":
                        st.markdown(f"<div style='color:green;font-weight:bold;'>Price Status: {price_status}</div>", unsafe_allow_html=True)
                    elif price_status in ["Below Range", "Above Range"]:
                        st.markdown(f"<div style='color:red;font-weight:bold;'>Price Status: {price_status}</div>", unsafe_allow_html=True)
                    
                    # Display the table
                    st.dataframe(combined_df, hide_index=True, use_container_width=True)
                
                with col2:
                    # Tick range visualization using Streamlit components
                    
                    # Calculate range
                    range_size = tick_upper - tick_lower
                    tick_min = tick_lower - range_size * 0.2
                    tick_max = tick_upper + range_size * 0.2
                    
                    # Display tick information
                    st.markdown("#### Tick Information")
                    st.markdown(f"**Lower Tick:** {tick_lower}")
                    st.markdown(f"**Current Tick:** {current_tick}")
                    st.markdown(f"**Upper Tick:** {tick_upper}")
                    
                    # Convert ticks to relative positions on a 0-100 scale
                    range_width = tick_max - tick_min
                    lower_pos = (tick_lower - tick_min) / range_width * 100
                    upper_pos = (tick_upper - tick_min) / range_width * 100
                    current_pos = (current_tick - tick_min) / range_width * 100
                    
                    # Draw a container to visualize the range
                    st.markdown("""
                    <style>
                    .tick-container {
                        width: 100%;
                        height: 40px;
                        background-color: #f0f2f6;
                        position: relative;
                        border-radius: 5px;
                        margin-top: 10px;
                        margin-bottom: 20px;
                    }
                    .tick-range {
                        position: absolute;
                        height: 100%;
                        background-color: #4CAF50;
                        opacity: 0.7;
                    }
                    .tick-marker {
                        position: absolute;
                        width: 3px;
                        height: 60px;
                        top: -10px;
                        background-color: #F63366;
                    }
                    .tick-label {
                        position: absolute;
                        font-size: 12px;
                        top: 45px;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    # Determine color based on in-range status
                    range_color = "#4CAF50" if tick_lower <= current_tick <= tick_upper else "#F63366"
                    
                    # Construct the HTML for the visualization
                    range_html = f"""
                    <div class="tick-container">
                        <div class="tick-range" style="left: {lower_pos}%; width: {upper_pos - lower_pos}%; background-color: {range_color};"></div>
                        <div class="tick-marker" style="left: {current_pos}%;"></div>
                        <div class="tick-label" style="left: {lower_pos}%;">Lower</div>
                        <div class="tick-label" style="left: {current_pos}%;">Current</div>
                        <div class="tick-label" style="left: {upper_pos}%;">Upper</div>
                    </div>
                    """
                    
                    st.markdown(range_html, unsafe_allow_html=True)
                    
                    # Display status in a large, colored box
                    if price_status == "In Range":
                        status_color = "green"
                    elif price_status in ["Below Range", "Above Range"]:
                        status_color = "red" 
                    else:
                        status_color = "gray"
                        
                    st.markdown(f"""
                    <div style="background-color: {status_color}; color: white; padding: 10px; 
                                border-radius: 5px; text-align: center; font-weight: bold; 
                                margin-top: 20px;">
                        {price_status}
                    </div>
                    """, unsafe_allow_html=True)
                
                # Add a button to clear selection
                if st.button("Clear Selection"):
                    st.session_state.selected_position_id = None
                    st.rerun()
    else:
        st.info("No position data available. Please check API connection.") 
        