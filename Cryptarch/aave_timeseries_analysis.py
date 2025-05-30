import requests
import polars as pl
import os
import logging
import sys
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from dotenv import load_dotenv
from pathlib import Path
from app.backend.consts import PORTFOLIOS, TOKENS, BASE_URLS
from typing import Dict, List, Any
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("aave_timeseries.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import functions from aave_reconciliation_example_6.py
from aave_reconciliation_example_6 import (
    get_aave_wallets, 
    get_all_token_contracts, 
    batch_process_dataframe, 
    stream_wallet_transactions, 
    get_wallet_metadata,
    get_underlying_asset,
    fetch_coinmetrics_price_data
)

def load_transaction_data(network="polygon"):
    """Load existing transaction data from CSV or generate new data if it doesn't exist."""
    csv_filename = f"aave_transactions_{network}.csv"
    csv_path = Path(csv_filename)
    
    if csv_path.exists():
        logger.info(f"Loading existing transaction data from {csv_filename}")
        try:
            # Read the CSV and convert timestamp column to datetime
            df = pl.read_csv(csv_filename)
            
            # Convert timestamp from string to datetime
            if "timestamp" in df.columns:
                logger.info("Converting timestamp column from string to datetime")
                df = df.with_columns(
                    pl.col("timestamp").str.to_datetime()
                )
            
            return df
        except Exception as e:
            logger.error(f"Error reading existing CSV: {str(e)}")
            return None
    else:
        logger.info(f"No existing transaction data found for {network}. Run aave_reconciliation_example_6.py first.")
        return None

def prepare_timeseries_data(df, group_by="wallet"):
    """
    Prepare transaction data for timeseries analysis.
    
    Args:
        df: Transaction DataFrame
        group_by: The column to group by (wallet, portfolio_name, strategy_name)
        
    Returns:
        Dictionary of grouped timeseries data
    """
    if df is None or len(df) == 0:
        logger.warning(f"No data to prepare for timeseries analysis")
        return {}
        
    try:
        # Ensure we have the necessary columns
        required_cols = ["timestamp", "value", "value_usd", group_by, "token"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            if "value_usd" in missing_cols and "value" in df.columns and "price_usd" in df.columns:
                # Try to calculate value_usd if missing
                logger.info("Calculating value_usd from value and price_usd")
                df = df.with_columns(
                    (pl.col("value") * pl.col("price_usd")).alias("value_usd")
                )
                missing_cols.remove("value_usd")
                
            if missing_cols:
                return {}
        
        # Sort by timestamp
        df = df.sort("timestamp")
        
        # Create a dictionary to hold data for each group
        grouped_data = {}
        
        # Group by the specified column
        unique_groups = df[group_by].unique().to_list()
        
        for group in unique_groups:
            # Filter data for this group
            group_df = df.filter(pl.col(group_by) == group)
            
            # Get unique tokens for this group
            tokens = group_df["token"].unique().to_list()
            
            token_data = {}
            for token in tokens:
                # Filter data for this token
                token_df = group_df.filter(pl.col("token") == token)
                
                # Prepare timeseries data
                dates = token_df["timestamp"].to_list()
                values = token_df["value"].to_list()
                usd_values = token_df["value_usd"].to_list() if "value_usd" in token_df.columns else None
                
                token_data[token] = {
                    "dates": dates,
                    "values": values,
                    "usd_values": usd_values,
                    "transaction_count": len(token_df)
                }
            
            # Add metadata
            if group_by == "wallet":
                metadata = get_wallet_metadata(PORTFOLIOS, group)
                portfolio = metadata.get("portfolio_name", "Unknown")
                strategy = metadata.get("strategy_name", "Unknown")
            else:
                portfolio = "N/A" if group_by != "portfolio_name" else group
                strategy = "N/A" if group_by != "strategy_name" else group
            
            grouped_data[group] = {
                "tokens": token_data,
                "portfolio": portfolio,
                "strategy": strategy,
                "total_transactions": len(group_df)
            }
            
        logger.info(f"Prepared timeseries data for {len(grouped_data)} {group_by}s")
        return grouped_data
        
    except Exception as e:
        logger.error(f"Error preparing timeseries data: {str(e)}")
        return {}

def create_historical_balance_charts(grouped_data, network, output_dir="timeseries_charts"):
    """
    Create historical balance charts for each group, showing asset amounts at each point in time.
    
    Args:
        grouped_data: Dictionary of grouped timeseries data
        network: Network name for file naming
        output_dir: Directory to save charts
    """
    if not grouped_data:
        logger.warning("No grouped data available for charting")
        return
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        for group_name, group_data in grouped_data.items():
            tokens = group_data["tokens"]
            portfolio = group_data["portfolio"]
            strategy = group_data["strategy"]
            
            # Skip if no tokens
            if not tokens:
                continue
                
            logger.info(f"Creating historical balance charts for {group_name} ({portfolio}/{strategy})")
            
            # Dictionary to track token balance at each point in time
            token_balance_series = {}
            token_dates_series = {}
            usd_balance_series = {}
            usd_dates_series = {}
            
            # Process each token separately
            for token_name, token_data in tokens.items():
                dates = token_data["dates"]
                values = token_data["values"]
                usd_values = token_data["usd_values"]
                
                if not dates or not values or len(dates) != len(values):
                    continue
                
                # Initialize this token's balance series
                token_balance_series[token_name] = []
                token_dates_series[token_name] = []
                
                # Each data point represents a transaction, not a cumulative balance
                for i, date in enumerate(dates):
                    # For point-in-time chart, we just store the individual transaction values
                    token_dates_series[token_name].append(date)
                    token_balance_series[token_name].append(values[i])
                
                # Process USD values if available
                if usd_values and all(v is not None for v in usd_values):
                    usd_balance_series[token_name] = []
                    usd_dates_series[token_name] = []
                    
                    for i, date in enumerate(dates):
                        if usd_values[i] is not None:
                            usd_dates_series[token_name].append(date)
                            usd_balance_series[token_name].append(usd_values[i])
            
            # TOKEN TRANSACTION CHART (Bar Chart for non-cumulative)
            plt.figure(figsize=(16, 10))
            
            token_chart_filename = f"{output_dir}/{network}_{group_name}_transaction_values.png"
            
            # Set style
            sns.set(style="whitegrid")
            
            # Prepare data for bar chart
            all_dates = []
            all_values = []
            all_tokens = []
            
            for token in token_balance_series:
                if token_balance_series[token]:
                    all_dates.extend(token_dates_series[token])
                    all_values.extend(token_balance_series[token])
                    all_tokens.extend([token] * len(token_balance_series[token]))
            
            # Create a DataFrame for easier plotting with seaborn
            if all_dates and all_values and all_tokens:
                df_plot = pl.DataFrame({
                    "date": all_dates,
                    "value": all_values,
                    "token": all_tokens
                })
                
                # Convert to pandas for seaborn
                import pandas as pd
                pd_df = df_plot.to_pandas()
                
                # Sort by date
                pd_df = pd_df.sort_values(by="date")
                
                # Plot bar chart
                ax = sns.barplot(x="date", y="value", hue="token", data=pd_df, dodge=False)
                
                # Format x-axis to show dates nicely
                plt.xticks(rotation=45)
                date_labels = pd_df["date"].dt.strftime('%Y-%m-%d').unique()
                # Limit the number of date labels to avoid overcrowding
                max_labels = 20
                if len(date_labels) > max_labels:
                    step = len(date_labels) // max_labels
                    ax.set_xticks(range(0, len(date_labels), step))
                    ax.set_xticklabels(date_labels[::step])
                else:
                    ax.set_xticklabels(date_labels)
                
                plt.title(f"{portfolio} - {strategy}\nToken Transaction Values on {network.capitalize()}")
                plt.xlabel("Date")
                plt.ylabel("Transaction Value")
                plt.legend(loc="upper left")
                plt.grid(True, axis='y')
                
                # Add a horizontal line at y=0 to distinguish positive and negative transactions
                plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
                
                # Adjust layout
                plt.tight_layout()
            
                # Save the chart
                plt.savefig(token_chart_filename)
                plt.close()
                logger.info(f"Saved transaction values chart to {token_chart_filename}")
            else:
                logger.warning(f"No transaction data to plot for {group_name}")
            
            # USD VALUE CHART (if data available)
            if usd_balance_series:
                plt.figure(figsize=(16, 10))
                
                usd_chart_filename = f"{output_dir}/{network}_{group_name}_transaction_usd_values.png"
                
                # Prepare data for USD bar chart
                all_usd_dates = []
                all_usd_values = []
                all_usd_tokens = []
                
                for token in usd_balance_series:
                    if usd_balance_series[token]:
                        all_usd_dates.extend(usd_dates_series[token])
                        all_usd_values.extend(usd_balance_series[token])
                        all_usd_tokens.extend([token] * len(usd_balance_series[token]))
                
                # Create a DataFrame for easier plotting with seaborn
                if all_usd_dates and all_usd_values and all_usd_tokens:
                    df_usd_plot = pl.DataFrame({
                        "date": all_usd_dates,
                        "value": all_usd_values,
                        "token": all_usd_tokens
                    })
                    
                    # Convert to pandas for seaborn
                    pd_df_usd = df_usd_plot.to_pandas()
                    
                    # Sort by date
                    pd_df_usd = pd_df_usd.sort_values(by="date")
                    
                    # Plot bar chart
                    ax = sns.barplot(x="date", y="value", hue="token", data=pd_df_usd, dodge=False)
                    
                    # Format x-axis to show dates nicely
                    plt.xticks(rotation=45)
                    date_labels = pd_df_usd["date"].dt.strftime('%Y-%m-%d').unique()
                    # Limit the number of date labels to avoid overcrowding
                    if len(date_labels) > max_labels:
                        step = len(date_labels) // max_labels
                        ax.set_xticks(range(0, len(date_labels), step))
                        ax.set_xticklabels(date_labels[::step])
                    else:
                        ax.set_xticklabels(date_labels)
                    
                    plt.title(f"{portfolio} - {strategy}\nToken Transaction USD Values on {network.capitalize()}")
                    plt.xlabel("Date")
                    plt.ylabel("Transaction USD Value")
                    plt.legend(loc="upper left")
                    plt.grid(True, axis='y')
                    
                    # Add a horizontal line at y=0
                    plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
                    
                    # Adjust layout
                    plt.tight_layout()
                    
                    # Save the chart
                    plt.savefig(usd_chart_filename)
                    plt.close()
                    logger.info(f"Saved transaction USD values chart to {usd_chart_filename}")
                else:
                    logger.warning(f"No USD transaction data to plot for {group_name}")
                
    except Exception as e:
        logger.error(f"Error creating historical balance charts: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")

def create_portfolio_level_chart(grouped_data, network, output_dir="timeseries_charts"):
    """
    Create portfolio-level historical balance charts combining all strategies.
    
    Args:
        grouped_data: Dictionary of grouped timeseries data
        network: Network name for file naming
        output_dir: Directory to save charts
    """
    if not grouped_data:
        logger.warning("No grouped data available for portfolio charting")
        return
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Group data by portfolio
        portfolio_data = {}
        
        for group_name, group_data in grouped_data.items():
            portfolio = group_data["portfolio"]
            
            if portfolio not in portfolio_data:
                portfolio_data[portfolio] = {
                    "wallets": [],
                    "all_tokens": set(),
                    "token_transactions": {},
                    "usd_transactions": {}
                }
            
            portfolio_data[portfolio]["wallets"].append(group_name)
            
            # Collect all transaction data for this portfolio by token
            for token_name, token_data in group_data["tokens"].items():
                dates = token_data["dates"]
                values = token_data["values"]
                usd_values = token_data["usd_values"]
                
                if not dates or not values:
                    continue
                
                # Add token to portfolio's token set
                portfolio_data[portfolio]["all_tokens"].add(token_name)
                
                # Initialize token in transactions dict if not present
                if token_name not in portfolio_data[portfolio]["token_transactions"]:
                    portfolio_data[portfolio]["token_transactions"][token_name] = {
                        "dates": [],
                        "values": []
                    }
                
                # Add token transactions data
                for i, date in enumerate(dates):
                    portfolio_data[portfolio]["token_transactions"][token_name]["dates"].append(date)
                    portfolio_data[portfolio]["token_transactions"][token_name]["values"].append(values[i])
                
                # Process USD values if available
                if usd_values and any(v is not None for v in usd_values):
                    if token_name not in portfolio_data[portfolio]["usd_transactions"]:
                        portfolio_data[portfolio]["usd_transactions"][token_name] = {
                            "dates": [],
                            "values": []
                        }
                    
                    for i, date in enumerate(dates):
                        if usd_values[i] is not None:
                            portfolio_data[portfolio]["usd_transactions"][token_name]["dates"].append(date)
                            portfolio_data[portfolio]["usd_transactions"][token_name]["values"].append(usd_values[i])
        
        # Create portfolio-level charts
        for portfolio_name, data in portfolio_data.items():
            if portfolio_name == "Unknown" or not data["token_transactions"]:
                continue
                
            logger.info(f"Creating portfolio-level transaction charts for {portfolio_name}")
            
            # TOKEN TRANSACTION CHART (Bar Chart)
            plt.figure(figsize=(16, 10))
            
            token_chart_filename = f"{output_dir}/{network}_{portfolio_name}_portfolio_transactions.png"
            
            # Set style
            sns.set(style="whitegrid")
            
            # Prepare data for bar chart
            all_dates = []
            all_values = []
            all_tokens = []
            
            for token_name, token_data in data["token_transactions"].items():
                if token_data["dates"] and token_data["values"]:
                    all_dates.extend(token_data["dates"])
                    all_values.extend(token_data["values"])
                    all_tokens.extend([token_name] * len(token_data["dates"]))
            
            # Create a DataFrame for easier plotting with seaborn
            if all_dates and all_values and all_tokens:
                import pandas as pd
                df_plot = pl.DataFrame({
                    "date": all_dates,
                    "value": all_values,
                    "token": all_tokens
                })
                
                # Convert to pandas for seaborn
                pd_df = df_plot.to_pandas()
                
                # Sort by date
                pd_df = pd_df.sort_values(by="date")
                
                # Plot bar chart
                ax = sns.barplot(x="date", y="value", hue="token", data=pd_df, dodge=False)
                
                # Format x-axis to show dates nicely
                plt.xticks(rotation=45)
                date_labels = pd_df["date"].dt.strftime('%Y-%m-%d').unique()
                # Limit the number of date labels to avoid overcrowding
                max_labels = 20
                if len(date_labels) > max_labels:
                    step = len(date_labels) // max_labels
                    ax.set_xticks(range(0, len(date_labels), step))
                    ax.set_xticklabels(date_labels[::step])
                else:
                    ax.set_xticklabels(date_labels)
                
                plt.title(f"Portfolio: {portfolio_name}\nToken Transaction Values on {network.capitalize()}")
                plt.xlabel("Date")
                plt.ylabel("Transaction Value")
                plt.legend(loc="upper left")
                plt.grid(True, axis='y')
                
                # Add a horizontal line at y=0
                plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            
                # Adjust layout
                plt.tight_layout()
            
                # Save the chart
                plt.savefig(token_chart_filename)
                plt.close()
                logger.info(f"Saved portfolio transaction values chart to {token_chart_filename}")
            else:
                logger.warning(f"No transaction data to plot for portfolio {portfolio_name}")
            
            # USD TRANSACTION CHART (if available)
            if data["usd_transactions"]:
                plt.figure(figsize=(16, 10))
                
                usd_chart_filename = f"{output_dir}/{network}_{portfolio_name}_portfolio_usd_transactions.png"
                
                # Prepare data for USD bar chart
                all_usd_dates = []
                all_usd_values = []
                all_usd_tokens = []
                
                for token_name, token_data in data["usd_transactions"].items():
                    if token_data["dates"] and token_data["values"]:
                        all_usd_dates.extend(token_data["dates"])
                        all_usd_values.extend(token_data["values"])
                        all_usd_tokens.extend([token_name] * len(token_data["dates"]))
                
                # Create a DataFrame for easier plotting with seaborn
                if all_usd_dates and all_usd_values and all_usd_tokens:
                    df_usd_plot = pl.DataFrame({
                        "date": all_usd_dates,
                        "value": all_usd_values,
                        "token": all_usd_tokens
                    })
                    
                    # Convert to pandas for seaborn
                    pd_df_usd = df_usd_plot.to_pandas()
                    
                    # Sort by date
                    pd_df_usd = pd_df_usd.sort_values(by="date")
                    
                    # Plot bar chart
                    ax = sns.barplot(x="date", y="value", hue="token", data=pd_df_usd, dodge=False)
                    
                    # Format x-axis to show dates nicely
                    plt.xticks(rotation=45)
                    date_labels = pd_df_usd["date"].dt.strftime('%Y-%m-%d').unique()
                    # Limit the number of date labels to avoid overcrowding
                    if len(date_labels) > max_labels:
                        step = len(date_labels) // max_labels
                        ax.set_xticks(range(0, len(date_labels), step))
                        ax.set_xticklabels(date_labels[::step])
                    else:
                        ax.set_xticklabels(date_labels)
                    
                    plt.title(f"Portfolio: {portfolio_name}\nToken Transaction USD Values on {network.capitalize()}")
                    plt.xlabel("Date")
                    plt.ylabel("Transaction USD Value")
                    plt.legend(loc="upper left")
                    plt.grid(True, axis='y')
                    
                    # Add a horizontal line at y=0
                    plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
                    
                    # Adjust layout
                    plt.tight_layout()
                    
                    # Save the chart
                    plt.savefig(usd_chart_filename)
                    plt.close()
                    logger.info(f"Saved portfolio USD transaction values chart to {usd_chart_filename}")
                else:
                    logger.warning(f"No USD transaction data to plot for portfolio {portfolio_name}")
                
    except Exception as e:
        logger.error(f"Error creating portfolio charts: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")

def generate_aggregate_stats(grouped_data, network, output_dir="timeseries_stats"):
    """
    Generate aggregate statistics for each wallet and portfolio.
    
    Args:
        grouped_data: Dictionary of grouped timeseries data
        network: Network name for file naming
        output_dir: Directory to save statistics
    """
    if not grouped_data:
        logger.warning("No grouped data available for statistics")
        return
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Prepare data structure for statistics
        stats = []
        
        for group_name, group_data in grouped_data.items():
            portfolio = group_data["portfolio"]
            strategy = group_data["strategy"]
            tokens = group_data["tokens"]
            
            for token_name, token_data in tokens.items():
                transaction_count = token_data["transaction_count"]
                
                if transaction_count == 0:
                    continue
                
                values = token_data["values"]
                usd_values = token_data["usd_values"]
                
                # Calculate statistics
                total_token_value = sum(values)
                token_inflow = sum(v for v in values if v > 0)
                token_outflow = sum(v for v in values if v < 0)
                
                # USD statistics if available
                total_usd_value = None
                usd_inflow = None
                usd_outflow = None
                
                if usd_values and all(v is not None for v in usd_values):
                    total_usd_value = sum(usd_values)
                    usd_inflow = sum(v for v in usd_values if v > 0)
                    usd_outflow = sum(v for v in usd_values if v < 0)
                
                # Add to statistics
                stats.append({
                    "wallet": group_name,
                    "portfolio": portfolio,
                    "strategy": strategy,
                    "token": token_name,
                    "transaction_count": transaction_count,
                    "token_balance": total_token_value,
                    "token_inflow": token_inflow,
                    "token_outflow": token_outflow,
                    "usd_balance": total_usd_value,
                    "usd_inflow": usd_inflow,
                    "usd_outflow": usd_outflow
                })
        
        # Create DataFrame from statistics
        stats_df = pl.DataFrame(stats)
        
        # Save to CSV
        stats_filename = f"{output_dir}/{network}_timeseries_stats.csv"
        stats_df.write_csv(stats_filename)
        logger.info(f"Saved statistics to {stats_filename}")
        
        # Create portfolio-level summary
        portfolio_summary = stats_df.group_by("portfolio").agg(
            pl.count("wallet").alias("wallet_count"),
            pl.n_unique("token").alias("token_count"),
            pl.sum("transaction_count").alias("total_transactions"),
            pl.sum("usd_balance").alias("total_usd_balance")
        )
        
        portfolio_summary_filename = f"{output_dir}/{network}_portfolio_summary.csv"
        portfolio_summary.write_csv(portfolio_summary_filename)
        logger.info(f"Saved portfolio summary to {portfolio_summary_filename}")
        
    except Exception as e:
        logger.error(f"Error generating statistics: {str(e)}")

def main(networks=None):
    """
    Generate timeseries analysis and charts for each wallet on specified networks.
    
    Args:
        networks: List of networks to analyze (default: ["polygon", "ethereum", "arbitrum", "optimism"])
    """
    # Load environment variables from .env file
    load_dotenv()
    
    if networks is None:
        networks = ["polygon", "ethereum", "arbitrum", "optimism"]
    
    for network in networks:
        try:
            logger.info(f"Starting timeseries analysis for network: {network}")
            
            # Load transaction data
            df = load_transaction_data(network)
            
            if df is None or len(df) == 0:
                logger.warning(f"No transaction data available for {network}. Skipping.")
                continue
            
            # Prepare timeseries data grouped by wallet
            wallet_data = prepare_timeseries_data(df, group_by="wallet")
            
            if wallet_data:
                # Create historical balance charts for each wallet (instead of cumulative)
                create_historical_balance_charts(wallet_data, network)
                
                # Create portfolio-level charts
                create_portfolio_level_chart(wallet_data, network)
                
                # Generate statistics
                generate_aggregate_stats(wallet_data, network)
                
                logger.info(f"Completed timeseries analysis for {network}")
                print(f"✅ Completed timeseries analysis for {network}")
            else:
                logger.warning(f"No wallet data available for {network}")
                print(f"⚠️ No wallet data available for {network}")
                
        except Exception as e:
            logger.error(f"Error analyzing network {network}: {str(e)}")
            print(f"❌ Error analyzing network {network}: {str(e)}")

if __name__ == "__main__":
    try:
        # You can specify networks to analyze here
        networks_to_analyze = ["polygon"]  # Add more as needed: "ethereum", "arbitrum", "optimism"
        main(networks_to_analyze)
        
        print("\n📊 Timeseries analysis complete! Charts and statistics have been generated.")
        print("📁 Check the 'timeseries_charts' and 'timeseries_stats' directories for results.")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        print(f"Critical error: {str(e)}") 