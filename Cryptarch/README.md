# 💠 Cryptarch

"Decoding the unknown, one artifact at a time."

Cryptarch is a human-facing monitoring tool built to act as the central observation deck for onchain activity. Inspired by the Cryptarchs of the Destiny universe — the lorekeepers and decryptors of the Tower — this tool helps you uncover, visualize, and manage insights from your on chain activity.

Whether you're monitoring live agent activity, visualizing data pipelines, or tracking wallet activity — Cryptarch is designed to give you clarity and control from a single Streamlit-powered interface.

Cryptarch is the first module in the The Tower — a collection of agent tools named after locations, factions, and lore from the Destiny universe.

## 🧪 Installation — Preparing Your Guardian Toolkit

Before you can decrypt data like a seasoned Cryptarch, make sure your system is ready for the task.

1. **Install Python 3.12**  
   Equip your system with the latest Light-infused tools.  
   Download Python 3.12 from the official source:  
   👉 [Python 3.12 Downloads](https://www.python.org/downloads/)

2. **Clone the Repository**  
   Retrieve the engram from the archives:  
   ```bash
   git clone https://github.com/Licona_Enterprises/TheTower.git
   cd TheTower/Cryptarch
   ```

3. **Install Dependencies**  
   Unpack the contents using your Ghost's trusted package handler:  
   ```bash
   pip install -r requirements.txt
   ```

4. **Launch Cryptarch**  
   Begin your data decryption journey:  
   ```bash
   python run.py
   ```

---

You're now equipped. Return to the Tower when you're ready to monitor your agents.  
_The Light will guide you — or at least your logs will._

---

The application is built on FastAPI and provides endpoints for accessing data from various DeFi protocols and services:

- **Market Data Service**: Integrates with CoinMetrics API to fetch current cryptocurrency prices and metrics
- **Web3 Integration**: Connects to Ethereum blockchain to retrieve on-chain Uniswap and Aave position data
- **Excel Report Generation**: Creates detailed reports with pandas and xlsxwriter

## API Endpoints

### Market Data
```
GET /api/market-data
```
Fetches current pricing and metrics for cryptocurrency assets.

**Parameters:**
- `metrics` (optional): List of metrics to retrieve (default: "ReferenceRate")
- `assets` (optional): List of asset symbols (default: predefined in DEFAULT_ASSETS)

### Uniswap Positions
```
GET /api/uniswap/positions
```
Retrieves Uniswap V3 liquidity positions across configured wallets.

**Parameters:**
- `portfolio` (optional): Filter by portfolio name
- `wallet_address` (optional): Filter by specific wallet address

### Aave Positions
```
GET /api/aave/positions
```
Retrieves Aave lending/borrowing positions across configured wallets.

**Parameters:**
- `portfolio` (optional): Filter by portfolio name
- `wallet_address` (optional): Filter by specific wallet address
- `aave_protocol_only` (optional): If true, only return positions from wallets with Aave as active protocol

### Aave Protocol-Only Positions
```
GET /api/aave/positions/protocol-only
```
Convenience endpoint to get Aave positions where Aave is an active protocol.

**Parameters:**
- `portfolio` (optional): Filter by portfolio name
- `wallet_address` (optional): Filter by specific wallet address

### Excel Report Generation
```
GET /api/report/positions-excel
```
Generates a comprehensive Excel report with detailed information about portfolio positions.

**Parameters:**
- `portfolio` (optional): Filter by portfolio name
- `include_aave` (optional): Whether to include Aave positions (default: true)
- `include_uniswap` (optional): Whether to include Uniswap positions (default: true)

**Response:**
Returns a downloadable Excel file with the following sheets:
- Summary
- AAVE Positions
- Uniswap Positions
- Market Data
- Debug Info

### Configuration Testing
```
GET /api/test/uniswap-config
```
Test endpoint to verify Uniswap wallet configuration.

## Implementation Details

### Data Processing
- Token amounts less than 0.00001 are filtered out to focus on meaningful positions
- USD values are calculated for all token positions using current market prices
- Wrapped tokens (e.g., WETH) are properly handled by attempting to match with their unwrapped counterparts

### Excel Report Features
- Automatically formatted currency and numeric values
- Column width optimization for readability
- Position data sorting by value
- Summary totals and statistics
- Active/inactive position status indicators
- In-range/out-of-range indicators for Uniswap positions

## Configuration

The application is configured via environment variables (loaded using python-dotenv) and constant definitions in the `consts.py` file, which includes:

- `DEFAULT_ASSETS`: List of default cryptocurrency assets to track
- `METRIC_FREQUENCIES`: Frequency settings for data metrics
- `PORTFOLIOS`: Definition of portfolios, strategies, and wallet addresses

## Frontend Dashboard (app/frontend/main.py)

The frontend dashboard is built with Streamlit and provides a user-friendly interface to monitor cryptocurrency market data and DeFi positions:

### Dashboard Features

- **Market Data Tab**: Displays real-time cryptocurrency prices, 30-day returns, volatility metrics, funding rates, and trading volume with color-coded indicators for price movements
- **Uniswap Positions Tab**: Shows detailed information about Uniswap V3 liquidity positions including:
  - Current values and token balances
  - Position health indicators
  - Range status (in-range/out-of-range)
  - Historical performance metrics
- **AAVE Positions Tab**: Displays lending and borrowing positions on the AAVE protocol with:
  - Supply and borrow balances
  - Interest rates
  - Health factors
  - Collateral status

### Technical Implementation

- **Automatic Refresh**: Each tab has independent refresh timers to ensure data stays up-to-date without excessive API calls
- **Session State Management**: Uses Streamlit's session state to track tab changes and refresh rates
- **Data Export**: Provides Excel report generation with a single click for comprehensive position data
- **Styling**: Custom CSS styling for improved readability with color-coded indicators for price movements and performance metrics
- **Modular Design**: Tab rendering is separated into modules (uniswap.py and aave.py) for maintainable code organization

The dashboard automatically refreshes based on configurable intervals defined in consts.py, providing real-time monitoring capabilities while managing API request frequency.


