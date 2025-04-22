import streamlit as st
import time
import pandas as pd
from datetime import datetime
import sys
import os
import requests
from io import BytesIO

# Initialize session state for refresh tracking and previous values
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
    st.session_state.refresh_count = 0
    st.session_state.previous_prices = {}
    st.session_state.eth_price = 0
    st.session_state.tab_change_enabled = True
    st.session_state._current_tab = 0
    st.session_state._previous_tab = 0
    # Initialize refresh tracking for Uniswap and AAVE tabs
    st.session_state.uniswap_last_refresh = time.time()
    st.session_state.uniswap_refresh_count = 0
    st.session_state.aave_last_refresh = time.time()
    st.session_state.aave_refresh_count = 0
    # Initialize Excel download states
    st.session_state.show_download = False

# Direct import using explicit file path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backend.consts as consts
DEFAULT_ASSETS = consts.DEFAULT_ASSETS
METRIC_FREQUENCIES = consts.METRIC_FREQUENCIES
AUTO_REFRESH_INTERVAL = consts.AUTO_REFRESH_INTERVAL

from uniswap import render_uniswap_page  # Import the uniswap module
from aave import render_aave_page  # Import the aave module
from api_service import ApiService  # Import the API service

# Initialize API service
api_service = ApiService()

# Set dark theme and wide layout
st.set_page_config(
    page_title="Quant Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom styling
st.markdown("""
<style>
    .stDataFrame {
        font-size: 1.2rem;
    }
    [data-testid="stHeader"] {
        background-color: rgba(0,0,0,0);
    }
    .refresh-time {
        color: #888;
        font-size: 0.8rem;
    }
    .wallet-section {
        margin-top: 2rem;
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: rgba(0, 0, 0, 0.1);
    }
    .positive {
        color: #00cc00 !important;
        font-weight: 600;
    }
    .negative {
        color: #ff4444 !important;
        font-weight: 600;
    }
    .up-arrow {
        color: #00cc00;
        font-weight: bold;
    }
    .down-arrow {
        color: #ff4444;
        font-weight: bold;
    }
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .header-title {
        margin: 0;
    }
    .download-button {
        padding: 0.5rem 1rem;
    }
    [data-testid="stRadio"] {display: none}
</style>
""", unsafe_allow_html=True)

# Function to download the Excel report with all data
def get_excel_report():
    """Download Excel report with Uniswap data"""
    try:
        # Get the API base URL
        api_base_url = api_service.api_base_url
        
        # Simple direct URL
        download_url = f"{api_base_url}/api/report/positions-excel?include_uniswap=true"
        
        print(f"REQUESTING EXCEL REPORT FROM: {download_url}")
        
        # Make the request
        response = requests.get(download_url)
        
        # Check response
        if response.status_code == 200:
            content_size = len(response.content)
            print(f"SUCCESS! Got Excel file: {content_size} bytes")
            return response.content, None
        else:
            error_msg = f"Error: {response.status_code} - {response.text}"
            print(f"API ERROR: {error_msg}")
            return None, error_msg
            
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        print(f"EXCEPTION: {error_msg}")
        return None, error_msg

# Create header with report download button
header_col1, header_col2 = st.columns([3, 1])

with header_col1:
    st.title("Quant Dashboard")

with header_col2:
    # Create a direct download button that properly handles Excel data
    
    # Show different UI based on download state
    if 'download_error' in st.session_state:
        # Show error from previous attempt
        st.error(f"Download failed: {st.session_state.download_error}")
        if st.button("Try Again"):
            del st.session_state.download_error
            st.rerun()
    else:
        # Show either the download button or a spinner
        if st.button("📊 Snapshot All Position Data", key="excel_button"):
            # Start the downloading process
            with st.spinner("Preparing Excel report..."):
                try:
                    # Fetch the Excel data
                    excel_data, error = get_excel_report()
                    
                    if excel_data:
                        # Store the data in session state for the rerun
                        st.session_state.excel_data = excel_data
                        st.session_state.show_download = True
                    else:
                        st.session_state.download_error = error
                except Exception as e:
                    st.session_state.download_error = str(e)
                
                # Force a rerun to show the download button or error
                st.rerun()
                
        # Show the actual download button if we have data
        if 'show_download' in st.session_state and st.session_state.show_download:
            # Display the download button with the data
            st.download_button(
                label="📥 Download Excel Report",
                data=st.session_state.excel_data,
                file_name="positions_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_file",
                on_click=lambda: (
                    setattr(st.session_state, 'show_download', False),
                    st.session_state.pop('excel_data', None)
                )
            )

# Define assets we want to track - use constants
ASSETS = DEFAULT_ASSETS

# Define refresh intervals
MARKET_DATA_REFRESH_INTERVAL = AUTO_REFRESH_INTERVAL  # From consts.py
UNISWAP_REFRESH_INTERVAL = AUTO_REFRESH_INTERVAL      # From consts.py
AAVE_REFRESH_INTERVAL = AUTO_REFRESH_INTERVAL         # From consts.py

# Define metrics with their frequencies and display names
METRICS = {
    'ReferenceRate': {'freq': METRIC_FREQUENCIES.get('ReferenceRate', '1s'), 'display': 'Price'},
    'ROI30d': {'freq': METRIC_FREQUENCIES.get('ROI30d', '1d'), 'display': '30D Returns'},
    'VtyDayRet30d': {'freq': METRIC_FREQUENCIES.get('VtyDayRet30d', '1d'), 'display': '30D Volatility'},
    'volatility_realized_usd_rolling_7d': {'freq': METRIC_FREQUENCIES.get('volatility_realized_usd_rolling_7d', '1d'), 'display': '7D Volatility'},
    'futures_aggregate_funding_rate_all_margin_8h_period': {'freq': METRIC_FREQUENCIES.get('futures_aggregate_funding_rate_all_margin_8h_period', '1h'), 'display': 'Funding Rate'},
    'volume_reported_spot_usd_1h': {'freq': METRIC_FREQUENCIES.get('volume_reported_spot_usd_1h', '1h'), 'display': 'Volume (1h)'}
}

# Fetch data for all assets
def fetch_market_data():
    # Get list of metrics to request
    metrics = list(METRICS.keys())
    
    # Use API service to fetch market data
    market_data = api_service.fetch_market_data(ASSETS, metrics)
    
    # Update session state
    st.session_state.refresh_count += 1
    st.session_state.last_refresh = time.time()
    
    return market_data

# Create tabs for different sections
tab_names = ["Market Data", "Uniswap Positions", "AAVE Positions"]

# Create a hidden radio button that tracks the tab state 
# This is necessary because Streamlit doesn't provide a direct way to detect tab changes
# The radio button acts as a state tracker but is hidden from users
st.markdown("""
<style>
    [data-testid="stRadio"] {display: none}
</style>
""", unsafe_allow_html=True)

current_tab_radio = st.radio(
    "Tabs",
    tab_names,
    key="tab_selection",
    index=st.session_state.get('_current_tab', 0),
    horizontal=True,
    label_visibility="collapsed"
)

# Update the current tab in session state
current_tab_index = tab_names.index(current_tab_radio)
prev_tab = st.session_state.get('_current_tab', 0)

# Check if tab has changed
if current_tab_index != prev_tab:
    # Update last refresh time when changing to a new tab
    if current_tab_index == 1 and prev_tab != 1:  # Changed to Uniswap
        st.session_state.uniswap_last_refresh = time.time()
    elif current_tab_index == 2 and prev_tab != 2:  # Changed to AAVE
        st.session_state.aave_last_refresh = time.time()
    
    # Store the new tab index
    st.session_state._previous_tab = prev_tab
    st.session_state._current_tab = current_tab_index

# Create the tabs that respond to the radio selection
tabs = st.tabs(tab_names)

with tabs[0]:
    # Create columns for header area
    col1, col2 = st.columns([3, 1])

    with col1:
        st.subheader("Latest Cryptocurrency Prices")

    with col2:
        st.markdown(f"""
            <div class="refresh-time">
                Last updated: {datetime.fromtimestamp(st.session_state.last_refresh).strftime('%H:%M:%S')}
                <br>Updates: {st.session_state.refresh_count}
            </div>
            """, 
            unsafe_allow_html=True
        )

    # Fetch fresh data
    market_data = fetch_market_data()

    # Create a base DataFrame with all assets we want to track
    display_df = pd.DataFrame({'asset': ASSETS})
    for metric in METRICS:
        display_df[metric] = None  # Initialize with None

    if market_data and 'data' in market_data:
        # Show status of each asset request
        if 'asset_statuses' in market_data:
            status_text = ", ".join([f"{asset.upper()}: {status}" for asset, status in market_data['asset_statuses'].items()])
            st.write(f"API status: {status_text}")
        
        # Convert to DataFrame
        df = pd.DataFrame(market_data['data'])
        
        # Convert numeric columns to numeric
        for metric in METRICS:
            if not df.empty and metric in df.columns:
                df[metric] = pd.to_numeric(df[metric], errors='coerce')
        
        # Check if we have 'metric' column - if yes, process differently
        if not df.empty and 'metric' in df.columns:
            # For each metric and asset, get the latest value
            for metric in METRICS:
                metric_data = df[df['metric'] == metric]
                if not metric_data.empty:
                    # Get latest data for each asset
                    latest_metric = metric_data.sort_values('time').groupby('asset').last().reset_index()
                    for idx, row in latest_metric.iterrows():
                        asset = row['asset']
                        value = row[metric] if metric in row else row['value'] if 'value' in row else None
                        if value is not None:
                            # Update in display DataFrame
                            display_df.loc[display_df['asset'] == asset, metric] = value
        else:
            # If each metric is a separate column
            if not df.empty and 'time' in df.columns and 'asset' in df.columns:
                latest_data = df.sort_values('time').groupby('asset').last().reset_index()
                
                # Update display DataFrame with latest values
                for idx, row in latest_data.iterrows():
                    asset = row['asset']
                    asset_idx = display_df.index[display_df['asset'] == asset].tolist()
                    if asset_idx:
                        for metric in METRICS:
                            if metric in row and not pd.isna(row[metric]):
                                display_df.loc[asset_idx[0], metric] = row[metric]

    # Rename columns for display
    column_mapping = {'asset': 'Asset'}
    for metric, info in METRICS.items():
        column_mapping[metric] = info['display']

    display_df = display_df.rename(columns=column_mapping)

    # Convert asset names to uppercase
    display_df['Asset'] = display_df['Asset'].str.upper()
    
    # Store ETH price for use in portfolio tab
    if 'Price' in display_df.columns:
        eth_row = display_df[display_df['Asset'] == 'ETH']
        if not eth_row.empty and not pd.isna(eth_row['Price'].values[0]):
            st.session_state.eth_price = eth_row['Price'].values[0]

    # Add price change indicators with color coding
    if 'Price' in display_df.columns:
        # Create a new column for formatted change indicators
        display_df['Change'] = ''
        
        for idx, row in display_df.iterrows():
            asset = row['Asset']
            current_price = row['Price']
            
            # Skip None values
            if pd.isna(current_price):
                continue
                
            if asset in st.session_state.previous_prices:
                prev_price = st.session_state.previous_prices[asset]
                if current_price > prev_price:
                    display_df.loc[idx, 'Change'] = '↑'  # Up arrow
                elif current_price < prev_price:
                    display_df.loc[idx, 'Change'] = '↓'  # Down arrow
            
            # Store current price for next comparison
            st.session_state.previous_prices[asset] = current_price
    
    # Format returns for display
    if '30D Returns' in display_df.columns:
        # Create a new column for formatted returns with colored text using HTML
        display_df['Returns Text'] = display_df['30D Returns'].apply(
            lambda x: f"<span class='positive'>+{x:.2f}%</span>" if pd.notna(x) and x > 0 else 
                    f"<span class='negative'>{x:.2f}%</span>" if pd.notna(x) and x < 0 else
                    f"{x:.2f}%" if pd.notna(x) else ""
        )
    
    # Format funding rate with color coding
    if 'Funding Rate' in display_df.columns:
        # Create a new column for formatted funding rate with colored text using HTML
        display_df['Funding Text'] = display_df['Funding Rate'].apply(
            lambda x: f"<span class='positive'>+{x:.4f}%</span>" if pd.notna(x) and x > 0 else 
                    f"<span class='negative'>{x:.4f}%</span>" if pd.notna(x) and x < 0 else
                    f"{x:.4f}%" if pd.notna(x) else ""
        )

    # Add HTML styling to change arrows
    if 'Change' in display_df.columns:
        for idx, row in display_df.iterrows():
            if row['Change'] == '↑':
                display_df.loc[idx, 'Change'] = "<span class='up-arrow'>↑</span>"
            elif row['Change'] == '↓':
                display_df.loc[idx, 'Change'] = "<span class='down-arrow'>↓</span>"

    # Configure columns for display
    column_config = {
        "Asset": st.column_config.TextColumn(
            "Asset",
            help="Cryptocurrency symbol",
            width="small"
        ),
        "Price": st.column_config.NumberColumn(
            "Price",
            help="Current price in USD (refreshes every second)",
            format="$%.2f",
            width="medium"
        ),
        "Returns Text": st.column_config.TextColumn(
            "30D Returns",  # Override column name display
            help="Return over the last 30 days (refreshes daily)",
            width="medium"
        ),
        "30D Volatility": st.column_config.NumberColumn(
            "30D Volatility",
            help="Volatility over the last 30 days (refreshes daily)",
            format="%.2f%%",
            width="medium"
        ),
        "7D Volatility": st.column_config.NumberColumn(
            "7D Volatility",
            help="Realized volatility over the last 7 days (refreshes daily)",
            format="%.2f%%",
            width="medium"
        ),
        "Funding Rate": st.column_config.NumberColumn(
            "Funding Rate",
            help="8-hour funding rate for perpetual futures (refreshes hourly)",
            format="%.4f%%",
            width="medium"
        ),
        "Volume (1h)": st.column_config.NumberColumn(
            "Volume (1h)",
            help="Trading volume in the last hour (refreshes hourly)",
            format="$%.2fM",
            width="medium"
        ),
        "Change": st.column_config.TextColumn(
            "Change",
            help="Price movement since last update",
            width="small"
        )
    }
    
    # Hide the numeric columns we'll replace with styled text
    cols_to_display = [col for col in display_df.columns if col not in ['30D Returns', 'Funding Rate']]
    
    # Display the data using st.write with HTML
    st.markdown("""
    <style>
        .styled-table {
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 16px;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);
        }
        .styled-table thead tr {
            background-color: #262730;
            color: #ffffff;
            text-align: left;
        }
        .styled-table th,
        .styled-table td {
            padding: 12px 15px;
        }
        .styled-table tbody tr {
            border-bottom: thin solid #444;
        }
        .styled-table tbody tr:nth-of-type(even) {
            background-color: #1e1e2e;
        }
        .styled-table tbody tr:last-of-type {
            border-bottom: 2px solid #262730;
        }
        .positive {
            color: #00cc00 !important;
            font-weight: 600;
        }
        .negative {
            color: #ff4444 !important;
            font-weight: 600;
        }
        .up-arrow {
            color: #00cc00;
            font-weight: bold;
        }
        .down-arrow {
            color: #ff4444;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Convert the dataframe to an HTML table and add the styled-table class
    # First, rename the Returns Text column to ensure it displays correctly in HTML
    html_display_df = display_df[cols_to_display].copy()
    if 'Returns Text' in html_display_df.columns:
        # Rename the column for HTML display
        html_display_df = html_display_df.rename(columns={'Returns Text': '30D Returns'})
    if 'Funding Text' in html_display_df.columns:
        # Rename the column for HTML display
        html_display_df = html_display_df.rename(columns={'Funding Text': 'Funding Rate'})
    
    html_table = html_display_df.to_html(escape=False, index=False, classes='styled-table')
    st.write(html_table, unsafe_allow_html=True)

    # Add a warning if some assets have no data
    missing_assets = display_df[display_df['Price'].isna()]['Asset'].tolist()
    if missing_assets:
        st.warning(f"No price data available for: {', '.join(missing_assets)}. This may be due to API limitations.")

with tabs[1]:
    # Render uniswap page from imported module
    render_uniswap_page(api_service)

with tabs[2]:
    # Render aave page from imported module
    render_aave_page(api_service)

# Store the current tab for refresh logic
if "tab_change_enabled" not in st.session_state:
    st.session_state.tab_change_enabled = True
    st.session_state._current_tab = 0
    st.session_state._previous_tab = 0

# Force refresh for tabs based on their specific refresh intervals
current_tab = st.session_state.get('_current_tab', 0)

if current_tab == 0:  # Market Data tab
    time_since_refresh = time.time() - st.session_state.last_refresh
    if time_since_refresh >= MARKET_DATA_REFRESH_INTERVAL:
        # We'll fetch fresh data next cycle
        pass
    time.sleep(1)  # Small sleep to prevent excessive CPU usage
    st.rerun()
elif current_tab == 1:  # Uniswap tab
    time_since_refresh = time.time() - st.session_state.uniswap_last_refresh
    if time_since_refresh >= UNISWAP_REFRESH_INTERVAL:
        st.session_state.uniswap_last_refresh = time.time()
        st.session_state.uniswap_refresh_count += 1
        st.rerun()
    time.sleep(1)  # Small sleep to prevent excessive CPU usage
    st.rerun()
elif current_tab == 2:  # AAVE tab
    time_since_refresh = time.time() - st.session_state.aave_last_refresh
    if time_since_refresh >= AAVE_REFRESH_INTERVAL:
        st.session_state.aave_last_refresh = time.time()
        st.session_state.aave_refresh_count += 1
        st.rerun()
    time.sleep(1)  # Small sleep to prevent excessive CPU usage
    st.rerun()
