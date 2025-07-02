import streamlit as st
import cloudscraper
import brotli
import gzip
import io
import json
import pandas as pd
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="NSE OI Spurts Live Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'data_history' not in st.session_state:
    st.session_state.data_history = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True
if 'gainers_data_history' not in st.session_state:
    st.session_state.gainers_data_history = []
if 'gainers_last_update' not in st.session_state:
    st.session_state.gainers_last_update = None
if 'selected_section' not in st.session_state:
    st.session_state.selected_section = "OI Spurts"
if 'losers_data_history' not in st.session_state:
    st.session_state.losers_data_history = []
if 'losers_last_update' not in st.session_state:
    st.session_state.losers_last_update = None
if 'shortlisted_data_history' not in st.session_state:
    st.session_state.shortlisted_data_history = []
if 'shortlisted_last_update' not in st.session_state:
    st.session_state.shortlisted_last_update = None
# Session management for scrapers
if 'nse_scraper' not in st.session_state:
    st.session_state.nse_scraper = None
if 'nse_session_established' not in st.session_state:
    st.session_state.nse_session_established = False
if 'session_creation_time' not in st.session_state:
    st.session_state.session_creation_time = None

def create_nse_session():
    """Create a new NSE session with cloudscraper"""
    try:
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=1,
            debug=False
        )
        
        # Visit the main page to establish session
        main_page_url = "https://www.nseindia.com/market-data/oi-spurts"
        main_response = scraper.get(main_page_url, timeout=30)
        
        if main_response.status_code == 200:
            st.session_state.nse_scraper = scraper
            st.session_state.nse_session_established = True
            st.session_state.session_creation_time = datetime.now()
            return scraper, None
        else:
            return None, f"Failed to establish session: {main_response.status_code}"
            
    except Exception as e:
        return None, f"Session creation failed: {str(e)}"

def refresh_nse_session():
    """Refresh the NSE session"""
    st.session_state.nse_scraper = None
    st.session_state.nse_session_established = False
    st.session_state.session_creation_time = None
    return create_nse_session()

def fetch_nse_data():
    """Fetch data from NSE API using cloudscraper with dynamic session management"""
    try:
        # Check if we need to create or refresh session
        session_age_limit = 30 * 60  # 30 minutes
        current_time = datetime.now()
        
        need_new_session = (
            not st.session_state.nse_session_established or
            st.session_state.nse_scraper is None or
            (st.session_state.session_creation_time and 
             (current_time - st.session_state.session_creation_time).seconds > session_age_limit)
        )
        
        if need_new_session:
            scraper, error = create_nse_session()
            if error:
                return None, error
        else:
            scraper = st.session_state.nse_scraper
        
        # API endpoint
        api_url = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
        
        # Use minimal headers
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.nseindia.com/market-data/oi-spurts",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest"
        }
        
        # Make the API request with retry and session refresh logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = scraper.get(api_url, headers=headers, timeout=30, allow_redirects=True)
                
                # If we get 401, try refreshing session once
                if response.status_code == 401 and attempt == 0:
                    scraper, refresh_error = refresh_nse_session()
                    if refresh_error:
                        return None, f"Session refresh failed: {refresh_error}"
                    continue
                    
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    return None, f"Request failed after {max_retries} attempts: {str(e)}"
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Decompression logic with better error handling
        content_encoding = response.headers.get("Content-Encoding", "")
        
        try:
            if "br" in content_encoding:
                decoded = brotli.decompress(response.content).decode("utf-8")
            elif "gzip" in content_encoding:
                buf = io.BytesIO(response.content)
                decoded = gzip.GzipFile(fileobj=buf).read().decode("utf-8")
            else:
                decoded = response.text
        except Exception as decomp_error:
            # Fallback: try to use response.text directly
            try:
                decoded = response.text
            except Exception:
                return None, f"Decompression failed: {str(decomp_error)}"
        
        if response.status_code == 200:
            data = json.loads(decoded)
            return data, None
        else:
            return None, f"HTTP Error: {response.status_code}"
            
    except Exception as e:
        return None, str(e)

def fetch_daily_gainers():
    """Fetch Daily Gainers F&O Stocks data using cloudscraper"""
    try:
        scraper = cloudscraper.create_scraper()
        
        url = "https://scanx.dhan.co/scanx/daygnl"
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "auth": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwiZXhwIjoxNzUxMTk0MzgxLCJjbGllbnRfaWQiOiIxMTAxMDM2MTEwIn0.f5RbyEmHolU_zKAjzREcHXAnoN3O3E0Gfz8Ig4eV-4QbDDCxtRSC-oprbWrj68-pAlPRNajVir4Qob_RHWSGeQ",
            "authorisation": "Token eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI4NjY5MTgwNTYyIiwicm9sZSI6IkFkbWluIiwiZXhwIjoxNzUxMjYzMTQwfQ._v2jjgVcral-A8l_E2LiVmnljzsmQ7jZKTdpeSQ0ZlnKmgGZpFMjVggLpVOcj2CJovG33g-6WqcK7ptRFKDbQg",
            "origin": "https://web.dhan.co",
            "referer": "https://web.dhan.co/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        }
        
        payload = {
            "Data": {
                "Seg": 1,
                "SecIdxCode": 311,
                "Count": 50,
                "TypeFlag": "G",
                "DayLevelIndicator": 1,
                "ExpCode": -1,
                "Instrument": "EQUITY"
            }
        }
        
        response = scraper.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data, None
        else:
            return None, f"HTTP Error: {response.status_code}"
            
    except Exception as e:
        return None, str(e)

def fetch_daily_losers():
    """Fetch Daily Losers F&O Stocks data using cloudscraper"""
    try:
        scraper = cloudscraper.create_scraper()
        
        url = "https://scanx.dhan.co/scanx/daygnl"
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "auth": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwiZXhwIjoxNzUxMTk0MzgxLCJjbGllbnRfaWQiOiIxMTAxMDM2MTEwIn0.f5RbyEmHolU_zKAjzREcHXAnoN3O3E0Gfz8Ig4eV-4QbDDCxtRSC-oprbWrj68-pAlPRNajVir4Qob_RHWSGeQ",
            "authorisation": "Token eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI4NjY5MTgwNTYyIiwicm9sZSI6IkFkbWluIiwiZXhwIjoxNzUxMjYzMTQwfQ._v2jjgVcral-A8l_E2LiVmnljzsmQ7jZKTdpeSQ0ZlnKmgGZpFMjVggLpVOcj2CJovG33g-6WqcK7ptRFKDbQg",
            "origin": "https://web.dhan.co",
            "referer": "https://web.dhan.co/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        }
        
        payload = {
            "Data": {
                "Seg": 1,
                "SecIdxCode": 311,
                "Count": 50,
                "TypeFlag": "L",
                "DayLevelIndicator": 1,
                "ExpCode": -1,
                "Instrument": "EQUITY"
            }
        }
        
        response = scraper.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data, None
        else:
            return None, f"HTTP Error: {response.status_code}"
            
    except Exception as e:
        return None, str(e)

def process_data(raw_data):
    """Process raw API data into a pandas DataFrame"""
    try:
        if 'data' in raw_data:
            df = pd.DataFrame(raw_data['data'])
            df['timestamp'] = datetime.now()
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return pd.DataFrame()

def process_gainers_data(raw_data):
    """Process Daily Gainers API data into a pandas DataFrame"""
    try:
        if 'data' in raw_data:
            df = pd.DataFrame(raw_data['data'])
            df['timestamp'] = datetime.now()
            # Rename columns for better display
            column_mapping = {
                'sym': 'Symbol',
                'disp': 'Company Name',
                'ltp': 'LTP',
                'chng': 'Change',
                'pchng': '% Change',
                'tvol': 'Volume',
                'tval': 'Turnover'
            }
            df = df.rename(columns=column_mapping)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error processing gainers data: {e}")
        return pd.DataFrame()

def process_losers_data(raw_data):
    """Process Daily Losers API data into a pandas DataFrame"""
    try:
        if 'data' in raw_data:
            df = pd.DataFrame(raw_data['data'])
            df['timestamp'] = datetime.now()
            # Rename columns for better display
            column_mapping = {
                'sym': 'Symbol',
                'disp': 'Company Name',
                'ltp': 'LTP',
                'chng': 'Change',
                'pchng': '% Change',
                'tvol': 'Volume',
                'tval': 'Turnover'
            }
            df = df.rename(columns=column_mapping)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error processing losers data: {e}")
        return pd.DataFrame()

def process_shortlisted_stocks():
    """Process and combine data for shortlisted stocks based on criteria"""
    try:
        shortlisted_stocks = []
        
        # Fetch all three data sources
        gainers_data, gainers_error = fetch_daily_gainers()
        losers_data, losers_error = fetch_daily_losers()
        oi_data, oi_error = fetch_nse_data()
        
        if gainers_error or losers_error or oi_error:
            return pd.DataFrame(), f"Error fetching data: {gainers_error or losers_error or oi_error}"
        
        # Process OI data
        oi_df = process_data(oi_data) if oi_data else pd.DataFrame()
        
        # Process gainers (>= 2% change)
        if gainers_data:
            gainers_df = process_gainers_data(gainers_data)
            if not gainers_df.empty and '% Change' in gainers_df.columns:
                filtered_gainers = gainers_df[gainers_df['% Change'] >= 2.0].copy()
                filtered_gainers['Movement Type'] = 'Gainer'
                
                # Match with OI data
                if not oi_df.empty and 'symbol' in oi_df.columns:
                    for _, gainer in filtered_gainers.iterrows():
                        symbol = gainer['Symbol']
                        oi_match = oi_df[oi_df['symbol'] == symbol]
                        if not oi_match.empty:
                            oi_row = oi_match.iloc[0]
                            if 'avgInOI' in oi_row and pd.notna(oi_row['avgInOI']) and oi_row['avgInOI'] > 7:
                                combined_row = {
                                    'Symbol': symbol,
                                    'Company Name': gainer['Company Name'],
                                    'LTP': gainer['LTP'],
                                    'Change': gainer['Change'],
                                    '% Change': gainer['% Change'],
                                    'Volume': gainer['Volume'],
                                    'Movement Type': 'Gainer',
                                    'avgInOI': oi_row['avgInOI'],
                                    'chngInOI': oi_row.get('chngInOI', 0),
                                    'pctChngInOI': oi_row.get('pctChngInOI', 0)
                                }
                                shortlisted_stocks.append(combined_row)
        
        # Process losers (<= -2% change)
        if losers_data:
            losers_df = process_losers_data(losers_data)
            if not losers_df.empty and '% Change' in losers_df.columns:
                filtered_losers = losers_df[losers_df['% Change'] <= -2.0].copy()
                filtered_losers['Movement Type'] = 'Loser'
                
                # Match with OI data
                if not oi_df.empty and 'symbol' in oi_df.columns:
                    for _, loser in filtered_losers.iterrows():
                        symbol = loser['Symbol']
                        oi_match = oi_df[oi_df['symbol'] == symbol]
                        if not oi_match.empty:
                            oi_row = oi_match.iloc[0]
                            if 'avgInOI' in oi_row and pd.notna(oi_row['avgInOI']) and oi_row['avgInOI'] > 7:
                                combined_row = {
                                    'Symbol': symbol,
                                    'Company Name': loser['Company Name'],
                                    'LTP': loser['LTP'],
                                    'Change': loser['Change'],
                                    '% Change': loser['% Change'],
                                    'Volume': loser['Volume'],
                                    'Movement Type': 'Loser',
                                    'avgInOI': oi_row['avgInOI'],
                                    'chngInOI': oi_row.get('chngInOI', 0),
                                    'pctChngInOI': oi_row.get('pctChngInOI', 0)
                                }
                                shortlisted_stocks.append(combined_row)
        
        # Create final DataFrame
        if shortlisted_stocks:
            df = pd.DataFrame(shortlisted_stocks)
            df['timestamp'] = datetime.now()
            return df, None
        else:
            return pd.DataFrame(), None
            
    except Exception as e:
        return pd.DataFrame(), str(e)

def main():
    # Remove default Streamlit padding and margins
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        margin-top: 0rem;
    }
    header[data-testid="stHeader"] {
        height: 0px;
    }
    .stApp > header {
        background-color: transparent;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Title with inline metrics and refresh button
    title_col1, title_col2, title_col3, title_col4 = st.columns([3, 1, 1, 1])
    
    with title_col1:
        if st.session_state.selected_section == "OI Spurts":
            st.markdown("### 📈 NSE OI Spurts Live Dashboard")
        elif st.session_state.selected_section == "Daily Gainers":
            st.markdown("### 🚀 Daily Gainers F&O Stocks Dashboard")
        elif st.session_state.selected_section == "Daily Losers":
            st.markdown("### 📉 Daily Losers F&O Stocks Dashboard")
        else:
            st.markdown("### ⭐ Shortlisted Stocks Dashboard")
    
    with title_col2:
        symbols_placeholder = st.empty()
    
    with title_col3:
        updated_placeholder = st.empty()
    
    with title_col4:
        col_refresh, col_session = st.columns(2)
        
        with col_refresh:
            if st.button("🔄 Refresh", type="primary", use_container_width=True):
                # Clear the appropriate data history to force refresh
                if st.session_state.selected_section == "OI Spurts":
                    st.session_state.data_history = []
                elif st.session_state.selected_section == "Daily Gainers":
                    st.session_state.gainers_data_history = []
                elif st.session_state.selected_section == "Daily Losers":
                    st.session_state.losers_data_history = []
                else:
                    st.session_state.shortlisted_data_history = []
                st.rerun()
        
        with col_session:
            if st.button("🔄 Session", help="Refresh NSE session", use_container_width=True):
                refresh_nse_session()
                success_placeholder = st.empty()
                success_placeholder.success("Session refreshed!")
                time.sleep(2)
                success_placeholder.empty()
                st.rerun()
    
    # Sidebar - Section selection
    st.sidebar.header("📊 Dashboard Sections")
    
    # Section selection buttons
    if st.sidebar.button("📈 Live NSE OI Spurts", use_container_width=True, type="primary" if st.session_state.selected_section == "OI Spurts" else "secondary"):
        st.session_state.selected_section = "OI Spurts"
        st.rerun()
    
    if st.sidebar.button("🚀 DAILY GAINERS F&O STOCKS", use_container_width=True, type="primary" if st.session_state.selected_section == "Daily Gainers" else "secondary"):
        st.session_state.selected_section = "Daily Gainers"
        st.rerun()
    
    if st.sidebar.button("📉 DAILY LOSERS F&O STOCKS", use_container_width=True, type="primary" if st.session_state.selected_section == "Daily Losers" else "secondary"):
        st.session_state.selected_section = "Daily Losers"
        st.rerun()
    
    if st.sidebar.button("⭐ SHORTLISTED STOCKS", use_container_width=True, type="primary" if st.session_state.selected_section == "Shortlisted Stocks" else "secondary"):
        st.session_state.selected_section = "Shortlisted Stocks"
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Current Section:** {st.session_state.selected_section}")
    
    # Session status information
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Session Status:**")
    
    if st.session_state.nse_session_established and st.session_state.session_creation_time:
        session_age = (datetime.now() - st.session_state.session_creation_time).seconds
        session_age_minutes = session_age // 60
        
        if session_age < 30 * 60:  # Less than 30 minutes
            st.sidebar.success(f"✅ Active ({session_age_minutes}m old)")
        else:
            st.sidebar.warning(f"⚠️ Aging ({session_age_minutes}m old)")
            
        st.sidebar.caption(f"Created: {st.session_state.session_creation_time.strftime('%H:%M:%S')}")
    else:
        st.sidebar.error("❌ No active session")
        
    if st.sidebar.button("🔄 Refresh Session", use_container_width=True):
        with st.sidebar:
            with st.spinner("Refreshing session..."):
                scraper, error = refresh_nse_session()
                if error:
                    error_placeholder = st.empty()
                    error_placeholder.error(f"Failed: {error}")
                    time.sleep(3)
                    error_placeholder.empty()
                else:
                    success_placeholder = st.empty()
                    success_placeholder.success("Session refreshed!")
                    time.sleep(2)
                    success_placeholder.empty()
        st.rerun()
    
    # Keep auto-refresh functionality but without UI control
    auto_refresh = st.session_state.auto_refresh
    
    # Compact status area
    col1, col2 = st.columns([3, 1])
    
    with col1:
        status_placeholder = st.empty()
    
    with col2:
        countdown_placeholder = st.empty()
    
    # Data display area - moved up to be more prominent
    data_placeholder = st.empty()
    chart_placeholder = st.empty()
    
    # Auto-refresh logic based on selected section
    if auto_refresh:
        if st.session_state.selected_section == "OI Spurts":
            # Fetch NSE OI Spurts data
            with status_placeholder:
                with st.spinner("Fetching OI Spurts data..."):
                    data, error = fetch_nse_data()
            
            if error:
                st.error(f"Error fetching data: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.last_update = datetime.now()
                
                # Process and display data
                if data:
                    df = process_data(data)
                    
                    if not df.empty:
                        # Store in history
                        st.session_state.data_history.append({
                            'timestamp': datetime.now(),
                            'data': df
                        })
                        
                        # Keep only last 10 data points
                        if len(st.session_state.data_history) > 10:
                            st.session_state.data_history = st.session_state.data_history[-10:]
                        
                        # Update title row metrics
                        if len(df) > 0:
                            symbols_placeholder.metric("Symbols", len(df))
                            current_time = datetime.now().strftime("%H:%M:%S")
                            updated_placeholder.metric("Updated", current_time)
                        
                        # Display current data
                        with data_placeholder.container():
                            # Compact metrics in a single row
                            if len(df) > 0:
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    if 'chngInOI' in df.columns:
                                        avg_oi_change = df['chngInOI'].mean() if pd.api.types.is_numeric_dtype(df['chngInOI']) else 0
                                        st.metric("Avg OI", f"{avg_oi_change:.1f}")
                                
                                with col2:
                                    if 'pctChngInOI' in df.columns:
                                        max_pct_change = df['pctChngInOI'].max() if pd.api.types.is_numeric_dtype(df['pctChngInOI']) else 0
                                        st.metric("Max %", f"{max_pct_change:.1f}%")
                            
                            # Main data table - larger and more prominent
                            st.dataframe(
                                df,  # Show all rows
                                use_container_width=True,
                                height=700
                            )
                        
                        # Compact charts section
                        if len(st.session_state.data_history) > 1:
                            with chart_placeholder.container():
                                # Create trend chart if we have numeric data
                                if 'pctChngInOI' in df.columns and pd.api.types.is_numeric_dtype(df['pctChngInOI']):
                                    # Top 10 symbols by percentage change
                                    top_symbols = df.nlargest(10, 'pctChngInOI')
                                    
                                    fig = px.bar(
                                        top_symbols,
                                        x='symbol' if 'symbol' in df.columns else df.index,
                                        y='pctChngInOI',
                                        title="Top 10 OI % Changes",
                                        labels={'pctChngInOI': 'OI % Change', 'symbol': 'Symbol'}
                                    )
                                    fig.update_layout(height=300, margin=dict(t=40, b=20))
                                    st.plotly_chart(fig, use_container_width=True)
                    
                    else:
                        st.warning("No data available in the response")
        
        elif st.session_state.selected_section == "Daily Gainers":
            # Fetch Daily Gainers data
            with status_placeholder:
                with st.spinner("Fetching Daily Gainers data..."):
                    data, error = fetch_daily_gainers()
            
            if error:
                st.error(f"Error fetching gainers data: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.gainers_last_update = datetime.now()
                
                # Process and display data
                if data:
                    df = process_gainers_data(data)
                    
                    if not df.empty:
                        # Store in history
                        st.session_state.gainers_data_history.append({
                            'timestamp': datetime.now(),
                            'data': df
                        })
                        
                        # Keep only last 10 data points
                        if len(st.session_state.gainers_data_history) > 10:
                            st.session_state.gainers_data_history = st.session_state.gainers_data_history[-10:]
                        
                        # Update title row metrics
                        if len(df) > 0:
                            symbols_placeholder.metric("Gainers", len(df))
                            current_time = datetime.now().strftime("%H:%M:%S")
                            updated_placeholder.metric("Updated", current_time)
                        
                        # Display current data
                        with data_placeholder.container():
                            # Main data table - larger and more prominent
                            display_columns = ['Symbol', 'Company Name', 'LTP', 'Change', '% Change', 'Volume']
                            available_columns = [col for col in display_columns if col in df.columns]
                            
                            st.dataframe(
                                df[available_columns] if available_columns else df,
                                use_container_width=True,
                                height=700
                            )
                        
                        # Compact charts section
                        if len(st.session_state.gainers_data_history) > 1:
                            with chart_placeholder.container():
                                # Create trend chart if we have numeric data
                                if '% Change' in df.columns and pd.api.types.is_numeric_dtype(df['% Change']):
                                    # Top 10 gainers by percentage change
                                    top_gainers = df.nlargest(10, '% Change')
                                    
                                    fig = px.bar(
                                        top_gainers,
                                        x='Symbol',
                                        y='% Change',
                                        title="Top 10 Daily Gainers",
                                        labels={'% Change': '% Change', 'Symbol': 'Symbol'},
                                        color='% Change',
                                        color_continuous_scale='Greens'
                                    )
                                    fig.update_layout(height=300, margin=dict(t=40, b=20))
                                    st.plotly_chart(fig, use_container_width=True)
                    
                    else:
                        st.warning("No gainers data available in the response")
        
        elif st.session_state.selected_section == "Daily Losers":
            # Fetch Daily Losers data
            with status_placeholder:
                with st.spinner("Fetching Daily Losers data..."):
                    data, error = fetch_daily_losers()
            
            if error:
                st.error(f"Error fetching losers data: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.losers_last_update = datetime.now()
                
                # Process and display data
                if data:
                    df = process_losers_data(data)
                    
                    if not df.empty:
                        # Store in history
                        st.session_state.losers_data_history.append({
                            'timestamp': datetime.now(),
                            'data': df
                        })
                        
                        # Keep only last 10 data points
                        if len(st.session_state.losers_data_history) > 10:
                            st.session_state.losers_data_history = st.session_state.losers_data_history[-10:]
                        
                        # Update title row metrics
                        if len(df) > 0:
                            symbols_placeholder.metric("Losers", len(df))
                            current_time = datetime.now().strftime("%H:%M:%S")
                            updated_placeholder.metric("Updated", current_time)
                        
                        # Display current data
                        with data_placeholder.container():
                            # Main data table - larger and more prominent
                            display_columns = ['Symbol', 'Company Name', 'LTP', 'Change', '% Change', 'Volume']
                            available_columns = [col for col in display_columns if col in df.columns]
                            
                            st.dataframe(
                                df[available_columns] if available_columns else df,
                                use_container_width=True,
                                height=700
                            )
                        
                        # Compact charts section
                        if len(st.session_state.losers_data_history) > 1:
                            with chart_placeholder.container():
                                # Create trend chart if we have numeric data
                                if '% Change' in df.columns and pd.api.types.is_numeric_dtype(df['% Change']):
                                    # Top 10 losers by percentage change (most negative)
                                    top_losers = df.nsmallest(10, '% Change')
                                    
                                    fig = px.bar(
                                        top_losers,
                                        x='Symbol',
                                        y='% Change',
                                        title="Top 10 Daily Losers",
                                        labels={'% Change': '% Change', 'Symbol': 'Symbol'},
                                        color='% Change',
                                        color_continuous_scale='Reds'
                                    )
                                    fig.update_layout(height=300, margin=dict(t=40, b=20))
                                    st.plotly_chart(fig, use_container_width=True)
                    
                    else:
                        st.warning("No losers data available in the response")
        
        else:  # Shortlisted Stocks section
            # Fetch Shortlisted Stocks data
            with status_placeholder:
                with st.spinner("Processing Shortlisted Stocks data..."):
                    df, error = process_shortlisted_stocks()
            
            if error:
                st.error(f"Error processing shortlisted stocks: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.shortlisted_last_update = datetime.now()
                
                # Process and display data
                if not df.empty:
                    # Store in history
                    st.session_state.shortlisted_data_history.append({
                        'timestamp': datetime.now(),
                        'data': df
                    })
                    
                    # Keep only last 10 data points
                    if len(st.session_state.shortlisted_data_history) > 10:
                        st.session_state.shortlisted_data_history = st.session_state.shortlisted_data_history[-10:]
                    
                    # Update title row metrics
                    symbols_placeholder.metric("Shortlisted", len(df))
                    current_time = datetime.now().strftime("%H:%M:%S")
                    updated_placeholder.metric("Updated", current_time)
                    
                    # Display current data with enhanced metrics
                    with data_placeholder.container():
                        # Enhanced metrics for shortlisted stocks
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            gainers_count = len(df[df['Movement Type'] == 'Gainer'])
                            st.metric("Gainers", gainers_count)
                        
                        with col2:
                            losers_count = len(df[df['Movement Type'] == 'Loser'])
                            st.metric("Losers", losers_count)
                        
                        with col3:
                            avg_oi = df['avgInOI'].mean() if 'avgInOI' in df.columns else 0
                            st.metric("Avg OI", f"{avg_oi:.1f}")
                        
                        with col4:
                            avg_change = abs(df['% Change']).mean() if '% Change' in df.columns else 0
                            st.metric("Avg % Change", f"{avg_change:.1f}%")
                        
                        # Main data table with color coding
                        st.dataframe(
                            df,
                            use_container_width=True,
                            height=700
                        )
                    
                    # Enhanced charts section
                    with chart_placeholder.container():
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Scatter plot: % Change vs avgInOI
                            fig_scatter = px.scatter(
                                df,
                                x='% Change',
                                y='avgInOI',
                                color='Movement Type',
                                title="% Change vs Average OI",
                                hover_data=['Symbol', 'Company Name'],
                                color_discrete_map={'Gainer': 'green', 'Loser': 'red'}
                            )
                            fig_scatter.update_layout(height=400)
                            st.plotly_chart(fig_scatter, use_container_width=True)
                        
                        with col2:
                            # Bar chart: Top stocks by avgInOI
                            top_oi = df.nlargest(10, 'avgInOI')
                            fig_bar = px.bar(
                                top_oi,
                                x='Symbol',
                                y='avgInOI',
                                color='Movement Type',
                                title="Top 10 by Average OI",
                                color_discrete_map={'Gainer': 'green', 'Loser': 'red'}
                            )
                            fig_bar.update_layout(height=400)
                            st.plotly_chart(fig_bar, use_container_width=True)
                
                else:
                    st.info("No stocks meet the shortlisting criteria (>2% movement + avgInOI >7)")
        
        # Countdown for next refresh
        if auto_refresh:
            for i in range(60, 0, -1):
                countdown_placeholder.info(f"⏱️ Next refresh in: {i}s")
                time.sleep(1)
            st.rerun()
    
    else:
        # Manual mode
        countdown_placeholder.info("⏸️ Auto-refresh disabled")
        
        # Show last data if available based on selected section
        if st.session_state.selected_section == "OI Spurts":
            if st.session_state.data_history:
                latest_data = st.session_state.data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("Symbols", len(latest_data))
                    if st.session_state.last_update:
                        updated_time = st.session_state.last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                with data_placeholder.container():
                    st.dataframe(
                        latest_data,
                        use_container_width=True,
                        height=700
                    )
            else:
                data_placeholder.info("No OI Spurts data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")
        
        elif st.session_state.selected_section == "Daily Gainers":
            if st.session_state.gainers_data_history:
                latest_data = st.session_state.gainers_data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("Gainers", len(latest_data))
                    if st.session_state.gainers_last_update:
                        updated_time = st.session_state.gainers_last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                with data_placeholder.container():
                    # Display gainers data with selected columns
                    display_columns = ['Symbol', 'Company Name', 'LTP', 'Change', '% Change', 'Volume']
                    available_columns = [col for col in display_columns if col in latest_data.columns]
                    
                    st.dataframe(
                        latest_data[available_columns] if available_columns else latest_data,
                        use_container_width=True,
                        height=700
                    )
            else:
                data_placeholder.info("No Daily Gainers data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")
        
        elif st.session_state.selected_section == "Daily Losers":
            if st.session_state.losers_data_history:
                latest_data = st.session_state.losers_data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("Losers", len(latest_data))
                    if st.session_state.losers_last_update:
                        updated_time = st.session_state.losers_last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                with data_placeholder.container():
                    # Display losers data with selected columns
                    display_columns = ['Symbol', 'Company Name', 'LTP', 'Change', '% Change', 'Volume']
                    available_columns = [col for col in display_columns if col in latest_data.columns]
                    
                    st.dataframe(
                        latest_data[available_columns] if available_columns else latest_data,
                        use_container_width=True,
                        height=700
                    )
            else:
                data_placeholder.info("No Daily Losers data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")
        
        else:  # Shortlisted Stocks section
            if st.session_state.shortlisted_data_history:
                latest_data = st.session_state.shortlisted_data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("Shortlisted", len(latest_data))
                    if st.session_state.shortlisted_last_update:
                        updated_time = st.session_state.shortlisted_last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                # Display current data with enhanced metrics
                with data_placeholder.container():
                    # Enhanced metrics for shortlisted stocks
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        gainers_count = len(latest_data[latest_data['Movement Type'] == 'Gainer'])
                        st.metric("Gainers", gainers_count)
                    
                    with col2:
                        losers_count = len(latest_data[latest_data['Movement Type'] == 'Loser'])
                        st.metric("Losers", losers_count)
                    
                    with col3:
                        avg_oi = latest_data['avgInOI'].mean() if 'avgInOI' in latest_data.columns else 0
                        st.metric("Avg OI", f"{avg_oi:.1f}")
                    
                    with col4:
                        avg_change = abs(latest_data['% Change']).mean() if '% Change' in latest_data.columns else 0
                        st.metric("Avg % Change", f"{avg_change:.1f}%")
                    
                    # Main data table
                    st.dataframe(
                        latest_data,
                        use_container_width=True,
                        height=700
                    )
                
                # Enhanced charts section
                with chart_placeholder.container():
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Scatter plot: % Change vs avgInOI
                        fig_scatter = px.scatter(
                            latest_data,
                            x='% Change',
                            y='avgInOI',
                            color='Movement Type',
                            title="% Change vs Average OI",
                            hover_data=['Symbol', 'Company Name'],
                            color_discrete_map={'Gainer': 'green', 'Loser': 'red'}
                        )
                        fig_scatter.update_layout(height=400)
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    
                    with col2:
                        # Bar chart: Top stocks by avgInOI
                        top_oi = latest_data.nlargest(10, 'avgInOI')
                        fig_bar = px.bar(
                            top_oi,
                            x='Symbol',
                            y='avgInOI',
                            color='Movement Type',
                            title="Top 10 by Average OI",
                            color_discrete_map={'Gainer': 'green', 'Loser': 'red'}
                        )
                        fig_bar.update_layout(height=400)
                        st.plotly_chart(fig_bar, use_container_width=True)
            else:
                data_placeholder.info("No Shortlisted Stocks data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")

if __name__ == "__main__":
    main()