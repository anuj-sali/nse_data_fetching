# NSE OI Spurts Live Dashboard

A real-time Streamlit dashboard that fetches and displays Open Interest (OI) spurts data from NSE India every 60 seconds.

## Features

- 🔄 **Auto-refresh**: Automatically fetches data every 60 seconds
- 📊 **Live Dashboard**: Real-time display of OI spurts data
- 📈 **Data Visualization**: Charts showing top symbols by OI percentage change
- 📋 **Data Table**: Tabular view of the latest market data
- ⚙️ **Controls**: Toggle auto-refresh, manual refresh, and clear history
- 📱 **Responsive**: Works on desktop and mobile devices

## Installation

1. **Clone or download** this repository
2. **Navigate** to the project directory:
   ```bash
   cd nse_data_fetching
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Set up environment variables**:
   - Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` file and replace placeholder values with your actual API tokens from Dhan platform
   - **Important**: Never commit the `.env` file to version control as it contains sensitive information

## Usage

1. **Run the Streamlit app**:
   ```bash
   streamlit run app.py
   ```

2. **Open your browser** and go to `http://localhost:8501`

3. **Use the dashboard**:
   - Toggle "Auto Refresh" to enable/disable automatic data fetching
   - Click "Refresh Now" for manual data updates
   - View real-time OI spurts data in the table
   - Monitor trends with the interactive charts

## Dependencies

- `streamlit` - Web app framework
- `cloudscraper` - Bypasses Cloudflare protection
- `brotli` - Brotli compression support
- `pandas` - Data manipulation
- `plotly` - Interactive charts
- `requests` - HTTP requests

## Data Source

The application fetches data from NSE India's live analysis API:
`https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings`

## Features Overview

### Dashboard Components
- **Status Panel**: Shows connection status and last update time
- **Metrics Cards**: Display key statistics (total symbols, average OI change, etc.)
- **Data Table**: Shows top 20 symbols with their OI data
- **Charts**: Visual representation of top performers
- **History**: Maintains last 10 data points for trend analysis

### Controls
- **Auto Refresh Toggle**: Enable/disable 60-second auto-refresh
- **Manual Refresh**: Fetch data immediately
- **Clear History**: Reset stored data points

## Notes

- The application uses cloudscraper to handle NSE's Cloudflare protection
- Data is refreshed every 60 seconds when auto-refresh is enabled
- The dashboard maintains a history of the last 10 data fetches
- All timestamps are in local time

## Troubleshooting

- If you encounter connection issues, the NSE API might be temporarily unavailable
- Ensure all dependencies are installed correctly
- Check your internet connection
- The API requires specific headers and cookies which are included in the code