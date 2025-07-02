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

def fetch_nse_data():
    """Fetch data from NSE API using cloudscraper"""
    try:
        # Create scraper with better configuration
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        url = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
        
        headers = {
            "authority": "www.nseindia.com",
            "method": "GET",
            "scheme": "https",
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cookie": '_ga=GA1.1.18464289.1730266031; _ga_WM2NSQKJEK=GS1.1.1735875695.44.1.1735875696.0.0.0; RT="z=1&dm=nseindia.com&si=96331740-7614-46dc-9e3d-d7767f37861d&ss=mclesgis&sl=0&se=8c&tt=0&bcn=%2F%2F684d0d41.akstat.io%2F"; nsit=UkPn2El-OD2HVC-9lCF9_Qwi; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTc1MTQ1NjUzNywiZXhwIjoxNzUxNDYzNzM3fQ.rU6cmfzkOsSyG5qNIJMBRkuJzDaqRtpFO_XLNatP9e4; AKA_A2=A; bm_sz=CA6006F39A25B75879CFED8262273578~YAAQVqXBF8s+msaXAQAAeN3xyhzSrsKD22q9+71Wjn13MwMlm8x/3afE3NttBmXqSu2QHWULilv5Ch5KP8XItXOeENQzWA64z69YHZnQHne3MpZ5DLiCEPEV1trF26A+rZNvaz9UYf+Pa3/aM7Tna8D3IzoZHCdLTyT7GomloOn/3xSQurGkC7fVqMzBc9t/hYCG0ssvHbGefaWcmYZ0JPurTX/NMCFmVX35KU/ojR6taGcyeGemwvgfyRAfr1fJdDVNYVNeBMMb8FnsTszmeC3y5WmQ0o8d26XPBRI4trI6AddtyGXdhWDJnvGFC8IJGPPmJmhBV2hehpihDcXV3KMiYCHENDypHpgjFD9Yiq1M6Po7Vj28Q8Te1YImjmaJf6WxX1cfJD+3B36b+Xiliw==~3421753~3425350; _abck=F1A8BF8BE1D7DEEE6716BEE0DF25EC28~0~YAAQVqXBF0M/msaXAQAAyuDxyg7FvgMEPDjpKfQnUNY9eGvyDUiQZnEeoul7arU7S3UusQ/VkYcVLlASaxZUepoev2jRVzbf9mFqJVPGnb4C+XHBkj8hRKNog2RPKjefkCbCYLBO5R2Y77W5RrrEf6LEMZ441Tr6oyKIIuwCImeczLDjIpJMYoDq2wnELe7CI0MiarE5gd3ySe6g3elmkeldQbNLqg8CEjXd/MFBfB7K/UIFAm0Mi2+zlowjh/5lSfqABPlSYobUB+4Qi18TKsBFkV9rfhq7U1tHMmmfKMxchJqHhPsyu7go4jWswXOCC1jrbBkeqotJIMMIgZvc+UCEUg0nCWYqsYuodXPq7MRKl8FDlHvDqd3FjbdHtCkrbc0RUclChBS0oXOCCftKVVPack3kHzvJPYTQ9v3TbSLAEaEgtg9ZwuOhi5zpua4LT4BvftaVSuS9Q9SoYK3tgKzT9hogTApPf9Dw1qxyasad8wQsQOcvUKB03K5QpdYod8oFuIQ74dtB7m9BK5Xqe/EhILzXZ6LbQOMHfcPlJb8ZpRhcCrBr+Xo3WVutIAkWtijFI/D6nVWf~-1~-1~-1; bm_sv=BC8EC02FCFD5F4B70FC5EEFA1E6B94A0~YAAQVqXBF4M/msaXAQAAVuLxyhymlyr7sIX08KxzrLfHlh0qYbuU1+E5/KlWkXotmcTWZu6SL4b85VtPCavMcu8r3h2THpgCVU9fo+l6IrkbFntO0ZEI7t2XCDBLvpf8unN5yfuG00ONDauZ39hAd5/qfRvwT+a8BECTNjchorvbdJ3MAiOg9wTN5Bq+VAElR3+nSu6kb/82cpKwt98jJdxnFLYKByUGwSwHMkXoYwf9OSOF+8o9/sYB/Nj0wHPWcB8=~1',
            "priority": "u=1, i",
            "referer": "https://www.nseindia.com/market-data/oi-spurts",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        }
        
        # Add timeout and session handling
        response = scraper.get(url, headers=headers, timeout=30, allow_redirects=True)
        
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

def main():
    # Title and header
    st.title("📈 NSE OI Spurts Live Dashboard")
    st.markdown("Real-time monitoring of Open Interest spurts from NSE India")
    
    # Sidebar controls
    st.sidebar.header("⚙️ Controls")
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto Refresh (60s)", value=st.session_state.auto_refresh)
    st.session_state.auto_refresh = auto_refresh
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now", type="primary"):
        st.rerun()
    
    # Clear history button
    if st.sidebar.button("🗑️ Clear History"):
        st.session_state.data_history = []
        st.session_state.last_update = None
        st.rerun()
    
    # Status section
    st.sidebar.header("📊 Status")
    if st.session_state.last_update:
        st.sidebar.success(f"Last Update: {st.session_state.last_update.strftime('%H:%M:%S')}")
    else:
        st.sidebar.info("No data fetched yet")
    
    st.sidebar.info(f"Data Points: {len(st.session_state.data_history)}")
    
    # Main content area
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.subheader("🔄 Live Data Feed")
    
    with col2:
        status_placeholder = st.empty()
    
    with col3:
        countdown_placeholder = st.empty()
    
    # Data display area
    data_placeholder = st.empty()
    chart_placeholder = st.empty()
    
    # Auto-refresh logic
    if auto_refresh:
        # Fetch data
        with status_placeholder:
            with st.spinner("Fetching data..."):
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
                    
                    # Display current data
                    with data_placeholder.container():
                        st.subheader("📋 Current Data")
                        
                        # Key metrics
                        if len(df) > 0:
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Total Symbols", len(df))
                            
                            with col2:
                                if 'chngInOI' in df.columns:
                                    avg_oi_change = df['chngInOI'].mean() if pd.api.types.is_numeric_dtype(df['chngInOI']) else 0
                                    st.metric("Avg OI Change", f"{avg_oi_change:.2f}")
                            
                            with col3:
                                if 'pctChngInOI' in df.columns:
                                    max_pct_change = df['pctChngInOI'].max() if pd.api.types.is_numeric_dtype(df['pctChngInOI']) else 0
                                    st.metric("Max % Change", f"{max_pct_change:.2f}%")
                            
                            with col4:
                                current_time = datetime.now().strftime("%H:%M:%S")
                                st.metric("Last Updated", current_time)
                        
                        # Data table
                        st.dataframe(
                            df,  # Show all rows
                            use_container_width=True,
                            height=600
                        )
                    
                    # Charts
                    if len(st.session_state.data_history) > 1:
                        with chart_placeholder.container():
                            st.subheader("📈 Trends")
                            
                            # Create trend chart if we have numeric data
                            if 'pctChngInOI' in df.columns and pd.api.types.is_numeric_dtype(df['pctChngInOI']):
                                # Top 10 symbols by percentage change
                                top_symbols = df.nlargest(10, 'pctChngInOI')
                                
                                fig = px.bar(
                                    top_symbols,
                                    x='symbol' if 'symbol' in df.columns else df.index,
                                    y='pctChngInOI',
                                    title="Top 10 Symbols by OI % Change",
                                    labels={'pctChngInOI': 'OI % Change', 'symbol': 'Symbol'}
                                )
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, use_container_width=True)
                
                else:
                    st.warning("No data available in the response")
        
        # Countdown for next refresh
        if auto_refresh:
            for i in range(60, 0, -1):
                countdown_placeholder.info(f"⏱️ Next refresh in: {i}s")
                time.sleep(1)
            st.rerun()
    
    else:
        # Manual mode
        countdown_placeholder.info("⏸️ Auto-refresh disabled")
        
        # Show last data if available
        if st.session_state.data_history:
            latest_data = st.session_state.data_history[-1]['data']
            
            with data_placeholder.container():
                st.subheader("📋 Latest Data")
                st.dataframe(
                    latest_data,
                    use_container_width=True,
                    height=600
                )
        else:
            data_placeholder.info("No data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")

if __name__ == "__main__":
    main()