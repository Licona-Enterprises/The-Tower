import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from api_service import ApiService
import traceback
import datetime # Added for date calculations

api_service = ApiService()

def render_historical_prices_page():
    st.title("Historical Prices")

    # Initialize session state for storing charts if it doesn't exist
    if 'historical_price_charts' not in st.session_state:
        st.session_state.historical_price_charts = {}
    if 'last_fetched_assets_days' not in st.session_state: # To help reset charts if inputs change
        st.session_state.last_fetched_assets_days = None

    assets_selection = st.multiselect("Select Assets:", options=api_service.get_available_assets(), default=["btc", "eth"])
    
    today = datetime.date.today()
    one_year_ago = today - datetime.timedelta(days=365)
    
    start_date_selection = st.date_input("Start Date", value=one_year_ago, max_value=today)
    end_date_selection = st.date_input("End Date", value=today, min_value=start_date_selection, max_value=today)
    
    current_fetch_params = (tuple(sorted(assets_selection)), start_date_selection, end_date_selection)

    # Clear old charts if inputs change before fetching new data
    if st.session_state.last_fetched_assets_days != current_fetch_params and not st.button("Clear Previous Charts", key="clear_charts_button"):
        # This condition is a bit tricky. We want to clear if inputs changed AND fetch isn't immediately clicked.
        # A dedicated clear button or more sophisticated state management might be better.
        # For now, let's clear if assets or dates change and Fetch Data isn't the immediate next action.
        pass # Decided to remove automatic clearing for now to avoid complexity, explicit fetch will override.

    if st.button("Fetch Data"):
        if not assets_selection:
            st.warning("Please select at least one asset.")
            return

        if start_date_selection > end_date_selection:
            st.warning("End date must be after start date.")
            return
        
        # Clear previously stored charts for the new fetch
        st.session_state.historical_price_charts = {}
        st.session_state.last_fetched_assets_days = current_fetch_params

        try:
            days_to_fetch = (end_date_selection - start_date_selection).days
            if days_to_fetch < 1:
                st.warning("The selected date range must be at least one day.")
                return

            response_data = api_service.fetch_historical_prices(assets_selection, days=days_to_fetch)

            if response_data.get("status") == "success":
                st.success("Data fetched successfully!")
                api_returned_data = response_data.get("data", {})
                # indicators_used_by_api = response_data.get("indicators_used", []) # No longer directly used for plotting

                if not api_returned_data:
                    st.info("No data blocks returned from the API.")

                for asset, price_data_list in api_returned_data.items():
                    if price_data_list:
                        df = pd.DataFrame(price_data_list)
                        if df.empty:
                            st.warning(f"No price records found for {asset}.")
                            continue

                        df['time'] = pd.to_datetime(df['time'])
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        
                        if 'close' in df.columns:
                            fig.add_trace(go.Scatter(x=df['time'], y=df['close'], mode='lines', name=f'{asset} Close Price'), secondary_y=False)
                        else:
                            st.warning(f"'close' price data missing for {asset}.")

                        if 'volume' in df.columns:
                            fig.add_trace(go.Bar(x=df['time'], y=df['volume'], name=f'{asset} Volume', opacity=0.5), secondary_y=True)
                        
                        primary_metric_columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                        for col_name in df.columns:
                            if col_name not in primary_metric_columns and pd.api.types.is_numeric_dtype(df[col_name]):
                                fig.add_trace(go.Scatter(x=df['time'], y=df[col_name], mode='lines', name=col_name.replace('_', ' ').title()), secondary_y=False)
                        
                        fig.update_layout(title_text=f"{asset} Price, Volume, and Technical Indicators", legend_title="Legend")
                        fig.update_yaxes(title_text="<b>Price / Indicator Value</b>", secondary_y=False)
                        fig.update_yaxes(title_text="<b>Volume</b>", secondary_y=True, showgrid=False)
                        
                        st.session_state.historical_price_charts[asset] = fig # Store fig in session state
                        
                        # Display DataFrame and Download Button immediately as well
                        st.subheader(f"Data Table for {asset}")
                        st.dataframe(df)
                        csv_data = df.to_csv(index=False).encode('utf-8')
                        st.download_button(label=f"Download {asset} Data as CSV", data=csv_data, file_name=f'{asset}_historical_data.csv', mime='text/csv', key=f"download_csv_{asset}")
                    else:
                        st.warning(f"No data available in the price_data_list for {asset}.")
            else:
                st.error(f"Error fetching data: {response_data.get('error', 'Unknown error')}")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.error(f"Traceback: {traceback.format_exc()}")

    # Always try to display charts from session state if they exist
    if not st.session_state.historical_price_charts:
        st.info("Click 'Fetch Data' to load and display charts.")
    else:
        if assets_selection and st.session_state.last_fetched_assets_days == current_fetch_params:
            for asset_name in assets_selection: # Display charts only for currently selected assets
                if asset_name in st.session_state.historical_price_charts:
                    st.plotly_chart(st.session_state.historical_price_charts[asset_name], use_container_width=True)
        elif st.session_state.last_fetched_assets_days != current_fetch_params:
             st.info("Asset or date selection has changed. Click 'Fetch Data' to update charts.")
             # Optionally, explicitly clear or hide stale charts here
             # For now, they just won't be re-rendered if params don't match. 