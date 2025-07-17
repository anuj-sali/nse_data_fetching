# NSE Financial Data Dashboard

A comprehensive real-time Streamlit dashboard that fetches and displays multiple types of financial market data from NSE India and Dhan platform APIs. The dashboard provides live market analysis across different sections with auto-refresh capabilities.

## Features

### 🎯 **Multi-Section Dashboard**
- **📈 Live NSE OI Spurts**: Real-time Open Interest spurts data from NSE
- **🚀 Daily Gainers F&O**: Top performing F&O stocks from Dhan platform
- **📉 Daily Losers F&O**: Worst performing F&O stocks from Dhan platform
- **📊 OI Trend**: Filtered OI data showing stocks with avgInOI > 2%
- **⭐ Shortlisted Stocks**: Combined analysis of high-performing stocks
- **🔍 Stock Detail View**: Detailed buildup data for individual stocks

### 🔄 **Real-time Features**
- Auto-refresh every 60 seconds across all sections
- Live session management with automatic session refresh
- Real-time data visualization with interactive charts
- Historical data tracking (last 10 data points)

### 📊 **Data Visualization & Analysis**
- Interactive Plotly charts for trend analysis
- Comprehensive metrics cards showing key statistics
- Tabular data views with sorting and filtering
- Stock-specific buildup analysis with detailed metrics

### ⚙️ **Advanced Controls**
- Section-wise navigation via sidebar
- Manual refresh capabilities
- Session status monitoring
- Clear history functionality
- Responsive design for desktop and mobile

## Installation

### Prerequisites
- Python 3.10 or higher
- API tokens from Dhan platform (for Daily Gainers/Losers and Buildup data)

### Setup Steps

1. **Clone or download** this repository
2. **Navigate** to the project directory:
   ```bash
   cd nse_data_fetching
   ```

3. **Install dependencies** (choose one method):
   
   **Using pip:**
   ```bash
   pip install -r requirements.txt
   ```
   
   **Using uv (recommended):**
   ```bash
   uv sync
   ```

4. **Generate futures mapping** (optional, for stock detail features):
   ```bash
   python fetch_and_extract.py
   ```

## Usage

1. **Run the Streamlit app**:
   ```bash
   streamlit run app.py
   ```

2. **Open your browser** and go to `http://localhost:8501`

3. **Navigate the dashboard**:
   - Use the **sidebar** to switch between different sections:
     - 📈 **Live NSE OI Spurts**: Real-time OI data from NSE
     - 🚀 **Daily Gainers F&O**: Top performing stocks
     - 📉 **Daily Losers F&O**: Worst performing stocks
     - 📊 **OI Trend**: Filtered OI data (avgInOI > 2%)
     - ⭐ **Shortlisted Stocks**: Combined high-performance analysis

4. **Dashboard controls**:
   - Toggle **"Auto Refresh"** to enable/disable 60-second automatic updates
   - Click **"Refresh Now"** for immediate data updates
   - Use **"Clear History"** to reset stored data points
   - Monitor **session status** in the sidebar

5. **Interactive features**:
   - Click on stock symbols in OI Trend section for detailed buildup analysis
   - View interactive charts and metrics for trend analysis
   - Access historical data (last 10 data points) for each section

## Dependencies

### Core Dependencies
- `streamlit>=1.28.0` - Web app framework for the dashboard
- `cloudscraper>=1.2.71` - Bypasses Cloudflare protection for NSE APIs
- `brotli>=1.0.9` - Brotli compression support for API responses
- `pandas>=2.0.0` - Data manipulation and analysis
- `plotly>=5.15.0` - Interactive charts and visualizations
- `requests>=2.31.0` - HTTP requests for API calls
- `python-dotenv>=1.0.0` - Environment variable management

### Development Tools
- `uv` - Fast Python package installer and resolver
- `.devcontainer` - VS Code development container configuration
- `pyproject.toml` - Modern Python project configuration

## Data Sources

### NSE India APIs
- **OI Spurts Data**: `https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings`
  - Real-time Open Interest spurts data
  - Used for OI Spurts and OI Trend sections

### Dhan Platform APIs
- **Daily Gainers/Losers**: `https://scanx.dhan.co/scanx/daygnl`
  - F&O stocks performance data
  - Requires authentication tokens
  - Used for Daily Gainers and Daily Losers sections

- **Buildup OI Data**: `https://options-trader.dhan.co/api/v1/buildup-oi`
  - Detailed stock buildup analysis
  - Options chain and futures data
  - Used for individual stock detail views

### Futures Mapping Data
- **API Scrip Master**: `https://images.dhan.co/api-data/api-scrip-master.csv`
  - Symbol to security ID mapping
  - Generated via `fetch_and_extract.py`
  - Stored in `futstk_mapping.json`

## Features Overview

### Dashboard Sections

#### 📈 Live NSE OI Spurts
- Real-time Open Interest spurts data from NSE
- Top 20 symbols with highest OI changes
- Interactive charts showing OI percentage trends
- Historical tracking of OI movements

#### 🚀 Daily Gainers F&O Stocks
- Top 50 best performing F&O stocks
- Price change percentages and volume data
- Company names and trading symbols
- Real-time LTP (Last Traded Price) updates

#### 📉 Daily Losers F&O Stocks
- Top 50 worst performing F&O stocks
- Negative price movements and volume analysis
- Risk assessment and market sentiment indicators

#### 📊 OI Trend (Advanced)
- Filtered OI data showing only stocks with avgInOI > 2%
- Click-to-view detailed buildup analysis
- Individual stock performance metrics
- Options chain and futures data integration

#### ⭐ Shortlisted Stocks
- Combined analysis of high-performing stocks
- Criteria-based filtering from multiple data sources
- Consolidated view of top opportunities

#### 🔍 Stock Detail View
- Comprehensive buildup data for individual stocks
- Options chain analysis
- Futures position data
- Historical performance metrics

### Technical Features
- **Session Management**: Automatic session refresh for API reliability
- **Error Handling**: Robust error handling with retry mechanisms
- **Data Caching**: Efficient data storage and retrieval
- **Responsive Design**: Mobile and desktop optimized interface
- **Real-time Updates**: Live data refresh every 60 seconds

## Project Structure

```
nse_data_fetching/
├── .devcontainer/          # VS Code development container
│   └── devcontainer.json
├── app.py                  # Main Streamlit application
├── app-backup.py          # Backup version of the app
├── fetch_and_extract.py   # Futures mapping data generator
├── main.py                # Entry point (minimal)
├── futstk_mapping.json    # Generated futures symbol mappings
├── requirements.txt       # Python dependencies
├── pyproject.toml         # Modern Python project config
├── uv.lock               # UV package lock file
├── .python-version       # Python version specification
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

## Technical Notes

### Session Management
- Uses cloudscraper to handle NSE's Cloudflare protection
- Automatic session refresh every 30 minutes
- Session status monitoring in sidebar
- Retry mechanisms for failed requests

### Data Handling
- Real-time data refresh every 60 seconds
- Historical data storage (last 10 data points per section)
- Efficient data processing with pandas
- Brotli and gzip decompression support

### Security
- Environment variables for sensitive API tokens
- No hardcoded credentials in source code
- Secure session management
- CORS and XSRF protection disabled for local development

### Performance
- Optimized API calls with minimal headers
- Efficient data caching and state management
- Responsive UI with loading indicators
- Error boundaries for graceful failure handling

## Development

### Using Development Container
The project includes a `.devcontainer` configuration for VS Code:

1. Open the project in VS Code
2. Install the "Dev Containers" extension
3. Press `Ctrl+Shift+P` and select "Dev Containers: Reopen in Container"
4. The container will automatically install dependencies and start the Streamlit server

### Environment Setup
- Python 3.11+ recommended
- Uses `uv` for fast package management
- Automatic port forwarding on 8501
- Pre-configured VS Code extensions for Python development

### Data Generation
To regenerate the futures mapping data:
```bash
python fetch_and_extract.py --help
```

## Troubleshooting

### Common Issues

**Connection Issues:**
- NSE API might be temporarily unavailable during market hours
- Check session status in sidebar and refresh if needed
- Verify internet connection and firewall settings

**Authentication Errors:**
- Ensure Dhan API tokens are correctly set in `.env` file
- Check token expiration dates (visible in `.env` comments)
- Verify token permissions for the required APIs

**Data Loading Issues:**
- Clear browser cache and refresh the page
- Check if `futstk_mapping.json` exists (run `fetch_and_extract.py` if missing)
- Verify all dependencies are installed correctly

**Performance Issues:**
- Disable auto-refresh if experiencing slow performance
- Clear history data using the "Clear History" button
- Check system resources and close unnecessary applications

### Debug Information
- Session status and age displayed in sidebar
- Error messages shown in the main interface
- Detailed logging available in browser console
- API response status codes displayed for failed requests