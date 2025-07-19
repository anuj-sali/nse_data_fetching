import streamlit as st
import cloudscraper
import brotli
import gzip
import io
import json
import pandas as pd
import time
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv,find_dotenv
from http.cookies import SimpleCookie

# Load environment variables
load_dotenv(find_dotenv())

# Page configuration
st.set_page_config(
    page_title="NSE OI Spurts Live Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_futstk_mapping():
    """Load the futstk mapping JSON file"""
    try:
        with open('futstk_mapping.json', 'r') as f:
            mapping = json.load(f)
        return mapping
    except Exception as e:
        st.error(f"Error loading futstk mapping: {e}")
        return {}

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
if 'oi_trend_data_history' not in st.session_state:
    st.session_state.oi_trend_data_history = []
if 'oi_trend_last_update' not in st.session_state:
    st.session_state.oi_trend_last_update = None
if 'selected_stock_symbol' not in st.session_state:
    st.session_state.selected_stock_symbol = None
if 'buildup_data' not in st.session_state:
    st.session_state.buildup_data = None
if 'futstk_mapping' not in st.session_state:
    st.session_state.futstk_mapping = load_futstk_mapping()
if 'show_stock_detail_page' not in st.session_state:
    st.session_state.show_stock_detail_page = False
if 'oi_based_shortlist_data_history' not in st.session_state:
    st.session_state.oi_based_shortlist_data_history = []
if 'oi_based_shortlist_last_update' not in st.session_state:
    st.session_state.oi_based_shortlist_last_update = None
# Session management for scrapers
if 'nse_scraper' not in st.session_state:
    st.session_state.nse_scraper = None
if 'nse_session_established' not in st.session_state:
    st.session_state.nse_session_established = False
if 'session_creation_time' not in st.session_state:
    st.session_state.session_creation_time = None

#latest one
def create_nse_session():
    try:
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            delay=1,
            debug=False
        )
        
        # nse_cookie = st.secrets["NSE_COOKIE"] #os.getenv("NSE_COOKIE")  # or 

        # if nse_cookie:
        #     # parse cookie string manually and set each cookie
        #     for cookie in nse_cookie.split(';'):
        #         cookie = cookie.strip()
        #         if '=' not in cookie:
        #             continue
        #         name, value = cookie.split('=', 1)
        #         scraper.cookies.set(name.strip(), value.strip(), domain=".nseindia.com", path='/')

        # Make a GET request to NSE main page to establish session
        main_url = 'https://www.nseindia.com'
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        }

        response = scraper.get(main_url, headers=headers, timeout=45)
        if response.status_code == 200:
            print("Session created successfully")
            return scraper, None
        else:
            return None, f"Failed to establish session: {response.status_code}"

    except Exception as e:
        return None, f"Session creation failed: {str(e)}"
# def create_nse_session():
#     """Create a new NSE session with cloudscraper and inject NSE_COOKIE into cookie jar"""
#     try:
#         scraper = cloudscraper.create_scraper(
#             browser={
#                 'browser': 'chrome',
#                 'platform': 'windows',
#                 'mobile': False
#             },
#             delay=1,
#             debug=False
#         )

#         nse_cookie = st.secrets["NSE_COOKIE"]
#         set_scraper_cookies(scraper, nse_cookie)  # <-- inject cookies here

#         headers = {
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                           "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
#             "accept-language": "en-US,en;q=0.9",
#             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#             "referer": "https://www.nseindia.com/",
#         }

#         main_page_url = os.getenv('NSE_MAIN_PAGE_URL', 'https://www.nseindia.com/market-data/oi-spurts')
#         main_response = scraper.get(main_page_url, headers=headers, timeout=45)

#         if main_response.status_code == 200:
#             st.session_state.nse_scraper = scraper
#             st.session_state.nse_session_established = True
#             st.session_state.session_creation_time = datetime.now()
#             return scraper, None
#         else:
#             return None, f"Failed to establish session: {main_response.status_code}"

#     except Exception as e:
#         return None, f"Session creation failed: {str(e)}"

# def create_nse_session():
#     """Create a new NSE session with cloudscraper and cookie from .env"""
#     try:
#         scraper = cloudscraper.create_scraper(
#             browser={
#                 'browser': 'chrome',
#                 'platform': 'windows',
#                 'mobile': False
#             },
#             delay=1,
#             debug=False
#         )

#         main_page_url = os.getenv('NSE_MAIN_PAGE_URL', 'https://www.nseindia.com/market-data/oi-spurts')
#         nse_cookie = st.secrets["NSE_COOKIE"]
#         # nse_cookie = os.getenv("NSE_COOKIE")

#         headers = {
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
#             "accept-language": "en-US,en;q=0.9",
#             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#             "referer": "https://www.nseindia.com/",
#         }

#         if nse_cookie:
#             headers["cookie"] = nse_cookie

#         main_response = scraper.get(main_page_url, headers=headers, timeout=45)

#         if main_response.status_code == 200:
#             st.session_state.nse_scraper = scraper
#             st.session_state.nse_session_established = True
#             st.session_state.session_creation_time = datetime.now()
#             return scraper, None
#         else:
#             return None, f"Failed to establish session: {main_response.status_code}"
            
#     except Exception as e:
#         return None, f"Session creation failed: {str(e)}"


# def create_nse_session():
#     """Create a new NSE session with cloudscraper"""
#     try:
#         scraper = cloudscraper.create_scraper(
#             browser={
#                 'browser': 'chrome',
#                 'platform': 'windows',
#                 'mobile': False
#             },
#             delay=1,
#             debug=False
#         )

#         # Visit the main page to establish session with headers
#         main_page_url = os.getenv('NSE_MAIN_PAGE_URL', 'https://www.nseindia.com/market-data/oi-spurts')
#         headers = {
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
#             "accept-language": "en-US,en;q=0.9",
#             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
#             "referer": "https://www.nseindia.com/",
#         }

#         main_response = scraper.get(main_page_url, headers=headers, timeout=45)

#         if main_response.status_code == 200:
#             st.session_state.nse_scraper = scraper
#             st.session_state.nse_session_established = True
#             st.session_state.session_creation_time = datetime.now()
#             return scraper, None
#         else:
#             return None, f"Failed to establish session: {main_response.status_code}"
            
#     except Exception as e:
#         return None, f"Session creation failed: {str(e)}"


#old one
# def create_nse_session():
#     """Create a new NSE session with cloudscraper"""
#     try:
#         scraper = cloudscraper.create_scraper(
#             browser={
#                 'browser': 'chrome',
#                 'platform': 'windows',
#                 'mobile': False
#             },
#             delay=1,
#             debug=False
#         )
        
#         # Visit the main page to establish session
#         main_page_url = os.getenv('NSE_MAIN_PAGE_URL', 'https://www.nseindia.com/market-data/oi-spurts')
#         main_response = scraper.get(main_page_url, timeout=45)
        
#         if main_response.status_code == 200:
#             st.session_state.nse_scraper = scraper
#             st.session_state.nse_session_established = True
#             st.session_state.session_creation_time = datetime.now()
#             return scraper, None
#         else:
#             return None, f"Failed to establish session: {main_response.status_code}"
            
#     except Exception as e:
#         return None, f"Session creation failed: {str(e)}"

def refresh_nse_session():
    """Refresh the NSE session"""
    st.session_state.nse_scraper = None
    st.session_state.nse_session_established = False
    st.session_state.session_creation_time = None
    return create_nse_session()

def create_buildup_session():
    """Create a new Buildup OI API session with cloudscraper"""
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
        
        # Visit the main Dhan options trader page to establish session
        main_page_url = os.getenv('DHAN_OPTIONS_TRADER_URL', 'https://options-trader.dhan.co/')
        main_response = scraper.get(main_page_url, timeout=30)
        
        if main_response.status_code == 200:
            st.session_state.buildup_scraper = scraper
            st.session_state.buildup_session_established = True
            st.session_state.buildup_session_creation_time = datetime.now()
            return scraper, None
        else:
            return None, f"Failed to establish buildup session: {main_response.status_code}"
            
    except Exception as e:
        return None, f"Buildup session creation failed: {str(e)}"

def refresh_buildup_session():
    """Refresh the Buildup OI API session"""
    st.session_state.buildup_scraper = None
    st.session_state.buildup_session_established = False
    st.session_state.buildup_session_creation_time = None
    st.session_state.buildup_data = None  # Clear cached data
    return create_buildup_session()

#old one
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
        api_url = os.getenv('NSE_OI_SPURTS_API_URL', 'https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings')
        
        # Use minimal headers
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.nseindia.com/market-data/oi-spurts",
            "origin": "https://www.nseindia.com",
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

# def fetch_nse_data():
#     """Fetch data from NSE API using cloudscraper with dynamic session management"""
#     try:
#         session_age_limit = 30 * 60
#         current_time = datetime.now()
        
#         need_new_session = (
#             not st.session_state.nse_session_established or
#             st.session_state.nse_scraper is None or
#             (st.session_state.session_creation_time and 
#              (current_time - st.session_state.session_creation_time).seconds > session_age_limit)
#         )

#         if need_new_session:
#             scraper, error = create_nse_session()
#             if error:
#                 return None, error
#         else:
#             scraper = st.session_state.nse_scraper

#         api_url = os.getenv('NSE_OI_SPURTS_API_URL', 'https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings')
#         nse_cookie = st.secrets["NSE_COOKIE"]

#         headers = {
#             "accept": "*/*",
#             "accept-language": "en-US,en;q=0.9",
#             "referer": "https://www.nseindia.com/market-data/oi-spurts",
#             "sec-fetch-dest": "empty",
#             "sec-fetch-mode": "cors",
#             "sec-fetch-site": "same-origin",
#             "x-requested-with": "XMLHttpRequest",
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
#         }

#         if nse_cookie:
#             headers["cookie"] = nse_cookie

#         max_retries = 3
#         for attempt in range(max_retries):
#             try:
#                 response = scraper.get(api_url, headers=headers, timeout=30, allow_redirects=True)

#                 if response.status_code == 401 and attempt == 0:
#                     scraper, refresh_error = refresh_nse_session()
#                     if refresh_error:
#                         return None, f"Session refresh failed: {refresh_error}"
#                     continue
                    
#                 break
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     return None, f"Request failed after {max_retries} attempts: {str(e)}"
#                 time.sleep(2 ** attempt)

#         content_encoding = response.headers.get("Content-Encoding", "")
#         try:
#             if "br" in content_encoding:
#                 decoded = brotli.decompress(response.content).decode("utf-8")
#             elif "gzip" in content_encoding:
#                 buf = io.BytesIO(response.content)
#                 decoded = gzip.GzipFile(fileobj=buf).read().decode("utf-8")
#             else:
#                 decoded = response.text
#         except Exception as decomp_error:
#             try:
#                 decoded = response.text
#             except Exception:
#                 return None, f"Decompression failed: {str(decomp_error)}"
        
#         if response.status_code == 200:
#             data = json.loads(decoded)
#             return data, None
#         else:
#             return None, f"HTTP Error: {response.status_code}"
            
#     except Exception as e:
#         return None, str(e)


#latest one
# def fetch_nse_data():
#     """Fetch data from NSE API using cloudscraper with dynamic session management"""
#     try:
#         session_age_limit = 30 * 60  # 30 minutes
#         current_time = datetime.now()

#         need_new_session = (
#             not st.session_state.get('nse_session_established', False) or
#             st.session_state.get('nse_scraper', None) is None or
#             (st.session_state.get('session_creation_time', None) and 
#              (current_time - st.session_state.session_creation_time).seconds > session_age_limit)
#         )

#         if need_new_session:
#             scraper, error = create_nse_session()
#             if error:
#                 return None, error
#         else:
#             scraper = st.session_state.nse_scraper

#         api_url = os.getenv('NSE_OI_SPURTS_API_URL', 'https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings')
#         # nse_cookie = st.secrets["NSE_COOKIE"]

#         headers = {
#             "accept": "*/*",
#             "accept-language": "en-US,en;q=0.9",
#             "referer": "https://www.nseindia.com/market-data/oi-spurts",
#             "sec-fetch-dest": "empty",
#             "sec-fetch-mode": "cors",
#             "sec-fetch-site": "same-origin",
#             "x-requested-with": "XMLHttpRequest",
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                           "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
#         }

#         # Optional: You can omit this since cookies are in scraper.cookies, but keep if needed
#         # if nse_cookie:
#         #     headers["cookie"] = nse_cookie

#         max_retries = 3
#         for attempt in range(max_retries):
#             try:
#                 response = scraper.get(api_url, headers=headers, timeout=30, allow_redirects=True)

#                 if response.status_code == 401 and attempt == 0:
#                     scraper, refresh_error = refresh_nse_session()
#                     if refresh_error:
#                         return None, f"Session refresh failed: {refresh_error}"
#                     continue

#                 break
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     return None, f"Request failed after {max_retries} attempts: {str(e)}"
#                 time.sleep(2 ** attempt)

#         content_encoding = response.headers.get("Content-Encoding", "")
#         try:
#             if "br" in content_encoding:
#                 decoded = brotli.decompress(response.content).decode("utf-8")
#             elif "gzip" in content_encoding:
#                 buf = io.BytesIO(response.content)
#                 decoded = gzip.GzipFile(fileobj=buf).read().decode("utf-8")
#             else:
#                 decoded = response.text
#         except Exception as decomp_error:
#             try:
#                 decoded = response.text
#             except Exception:
#                 return None, f"Decompression failed: {str(decomp_error)}"

#         if response.status_code == 200:
#             data = json.loads(decoded)
#             return data, None
#         else:
#             return None, f"HTTP Error: {response.status_code}"

#     except Exception as e:
#         return None, str(e)


def fetch_daily_gainers():
    """Fetch Daily Gainers F&O Stocks data using cloudscraper"""
    try:
        scraper = cloudscraper.create_scraper()
        
        url = os.getenv('DHAN_DAILY_API_URL', 'https://scanx.dhan.co/scanx/daygnl')
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "auth": os.getenv('DHAN_DAILY_AUTH_TOKEN'),
            "authorisation": os.getenv('DHAN_DAILY_AUTHORIZATION_TOKEN'),
            "origin": "https://web.dhan.co",
            "referer": "https://web.dhan.co/",
            "user-agent": os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
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
        
        url = os.getenv('DHAN_DAILY_API_URL', 'https://scanx.dhan.co/scanx/daygnl')
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "auth": os.getenv('DHAN_DAILY_AUTH_TOKEN'),
            "authorisation": os.getenv('DHAN_DAILY_AUTHORIZATION_TOKEN'),
            "origin": "https://web.dhan.co",
            "referer": "https://web.dhan.co/",
            "user-agent": os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
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



def get_secid_for_symbol(symbol, mapping):
    """Get secid for a symbol from July 2025 futures mapping"""
    try:
        # Look for July 2025 futures mapping
        key = f"{symbol}-Jul2025-FUT"
        if key in mapping:
            secid = mapping[key]
            # Debug logging for RBLBANK specifically
            if symbol == "RBLBANK":
                st.info(f"✅ Found mapping for {symbol}: {key} -> secid: {secid}")
            return secid
        else:
            # Debug logging when mapping not found
            if symbol == "RBLBANK":
                st.warning(f"❌ No mapping found for {key} in futstk_mapping.json")
                st.write(f"Available keys containing '{symbol}': {[k for k in mapping.keys() if symbol in k]}")
            return None
    except Exception as e:
        st.error(f"Error getting secid for {symbol}: {e}")
        return None

def fetch_buildup_data(secid):
    """Fetch buildup data for a specific secid using the provided API"""
    try:
        # Debug logging for RBLBANK secid specifically
        if secid == "53427":
            st.info(f"🔍 Fetching buildup data for RBLBANK with secid: {secid}")
        
        # Use existing buildup session or create new one
        if not st.session_state.get('buildup_session_established', False) or st.session_state.get('buildup_scraper') is None:
            scraper, error = create_buildup_session()
            if error:
                return None, f"Failed to create buildup session: {error}"
        else:
            scraper = st.session_state.buildup_scraper
            
            # Check if session is older than 30 minutes and refresh if needed
            if st.session_state.get('buildup_session_creation_time'):
                session_age = datetime.now() - st.session_state.buildup_session_creation_time
                if session_age.total_seconds() > 1800:  # 30 minutes
                    scraper, error = refresh_buildup_session()
                    if error:
                        return None, f"Failed to refresh buildup session: {error}"
        
        url = os.getenv('DHAN_BUILDUP_API_URL', 'https://ticks.dhan.co/builtup')
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "access-control-allow-origin": "true",
            "auth": os.getenv('DHAN_BUILDUP_AUTH_TOKEN'),
            "connection": "keep-alive",
            "content-type": "application/json",
            "host": "ticks.dhan.co",
            "origin": "https://options-trader.dhan.co",
            "referer": "https://options-trader.dhan.co/",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "src": "Y",
            "user-agent": os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        }
        
        payload = {
            "Data": {
                "exch": "NSE",
                "seg": "D",
                "inst": "FUTSTK",
                "timeinterval": "15",
                "secid": int(secid)
            }
        }
        
        response = scraper.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            response_data = response.json()
            raw_data = response_data.get("data", [])
            
            # Debug information
            if not raw_data:
                # Check if the response has any useful information
                if "message" in response_data:
                    return None, f"API Message: {response_data['message']}"
                elif "error" in response_data:
                    return None, f"API Error: {response_data['error']}"
                else:
                    return None, f"No buildup data available from API for secid {secid}. Response: {response_data}"
            
            return raw_data, None
        else:
            return None, f"HTTP Error: {response.status_code} - {response.text}"
            
    except Exception as e:
        return None, str(e)

def format_buildup_data(raw_data):
    """Format buildup data into a readable DataFrame"""
    try:
        def format_time(ts):
            return (datetime.utcfromtimestamp(ts) + timedelta(hours=5, minutes=30)).strftime('%H:%M')
        
        df = pd.DataFrame([
            {
                "Interval": f"{format_time(row['st'])} - {format_time(row['et'])}",
                "Trading Zone": {
                    "LB": "Long Buildup",
                    "SB": "Short Buildup",
                    "LU": "Long Unwinding",
                    "SC": "Short Covering"
                }.get(row["btc"], row["btc"]),
                "Price Range": f"{row['l']:.2f} - {row['h']:.2f}",
                "Open Interest (OI)": f"{row['toi']:,}",
                "OI Change (%)": f"{row['oipch']:.2f}%",
                "Fresh": f"{row['fr']:,}.00",
                "Square-Off": f"{row['sqf']:,}.00",
                "Traded Contracts": f"{row['vol']:,}"
            }
            for row in raw_data
        ])
        
        return df
    except Exception as e:
        st.error(f"Error formatting buildup data: {e}")
        return pd.DataFrame()

def process_oi_trend_data(raw_data):
    """Process NSE OI data and filter for avgInOI > 2%"""
    try:
        if 'data' in raw_data:
            df = pd.DataFrame(raw_data['data'])
            df['timestamp'] = datetime.now()
            
            # Filter for avgInOI > 2%
            if 'avgInOI' in df.columns:
                filtered_df = df[df['avgInOI'] > 2.0].copy()
                return filtered_df
            else:
                return pd.DataFrame()
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error processing OI trend data: {e}")
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

def process_oi_based_shortlisted_stocks():
    """Process and filter stocks based on matching buildup patterns in 9:15-9:30 and 9:30-9:45 intervals"""
    try:
        shortlisted_stocks = []
        
        # Fetch OI trend data (stocks with avgInOI > 2%)
        oi_data, oi_error = fetch_nse_data()
        if oi_error:
            return pd.DataFrame(), f"Error fetching OI data: {oi_error}"
        
        # Process OI data to get filtered stocks
        oi_df = process_oi_trend_data(oi_data) if oi_data else pd.DataFrame()
        
        if oi_df.empty or 'symbol' not in oi_df.columns:
            return pd.DataFrame(), "No OI trend data available"
        
        # For each stock in OI trend, check buildup patterns
        for _, stock_row in oi_df.iterrows():
            symbol = stock_row['symbol']
            
            # Get secid for the symbol
            secid = get_secid_for_symbol(symbol, st.session_state.futstk_mapping)
            if not secid:
                continue
            
            # Fetch buildup data
            buildup_raw, buildup_error = fetch_buildup_data(secid)
            if buildup_error or not buildup_raw:
                continue
            
            # Check for matching patterns in 9:15-9:30 and 9:30-9:45 intervals
            matching_pattern = check_matching_buildup_patterns(buildup_raw)
            if matching_pattern:
                combined_row = {
                    'Symbol': symbol,
                    'avgInOI': stock_row.get('avgInOI', 0),
                    'chngInOI': stock_row.get('chngInOI', 0),
                    'pctChngInOI': stock_row.get('pctChngInOI', 0),
                    'Buildup Pattern': matching_pattern,
                    'Pattern Intervals': '9:15-9:30 & 9:30-9:45'
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

def check_matching_buildup_patterns(buildup_raw):
    """Check if 9:15-9:30 and 9:30-9:45 intervals have matching buildup patterns"""
    try:
        if not buildup_raw or len(buildup_raw) < 2:
            return None
        
        # Convert timestamps and find the target intervals
        interval_915_930 = None
        interval_930_945 = None
        
        for row in buildup_raw:
            start_time = datetime.utcfromtimestamp(row['st']) + timedelta(hours=5, minutes=30)
            end_time = datetime.utcfromtimestamp(row['et']) + timedelta(hours=5, minutes=30)
            
            start_str = start_time.strftime('%H:%M')
            end_str = end_time.strftime('%H:%M')
            
            # Check for 9:15-9:30 interval
            if start_str == '09:15' and end_str == '09:30':
                interval_915_930 = row['btc']
            # Check for 9:30-9:45 interval
            elif start_str == '09:30' and end_str == '09:45':
                interval_930_945 = row['btc']
        
        # Check if we have both intervals and they match
        if interval_915_930 and interval_930_945 and interval_915_930 == interval_930_945:
            pattern_map = {
                'LB': 'Long Buildup',
                'SB': 'Short Buildup', 
                'LU': 'Long Unwinding',
                'SC': 'Short Covering'
            }
            return pattern_map.get(interval_915_930, interval_915_930)
        
        return None
        
    except Exception as e:
        return None

def show_stock_detail_page():
    """Show detailed tabular data for the selected stock"""
    st.title(f"📊 Stock Details: {st.session_state.selected_stock_symbol}")
    
    # Back button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("← Back to OI Trend", type="primary"):
            st.session_state.show_stock_detail_page = False
            st.session_state.selected_stock_symbol = None
            st.session_state.buildup_data = None
            st.rerun()
    
    with col2:
        st.markdown(f"### Buildup Data for **{st.session_state.selected_stock_symbol}**")
    
    with col3:
        # Create two columns for the buttons
        btn_col1, btn_col2 = st.columns(2)
        
        with btn_col1:
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                # Refresh the buildup data
                secid = get_secid_for_symbol(st.session_state.selected_stock_symbol, st.session_state.futstk_mapping)
                if secid:
                    with st.spinner(f"Refreshing buildup data for {st.session_state.selected_stock_symbol}..."):
                        buildup_raw, error = fetch_buildup_data(secid)
                        if error:
                            st.error(f"Error fetching buildup data: {error}")
                            st.session_state.buildup_data = None
                        else:
                            formatted_data = format_buildup_data(buildup_raw)
                            st.session_state.buildup_data = formatted_data
                    st.rerun()
        
        with btn_col2:
            if st.button("🔄 Session", type="secondary", use_container_width=True, help="Refresh session for Buildup OI data API"):
                # Refresh the buildup session properly
                with st.spinner("Refreshing session for Buildup OI data API..."):
                    scraper, error = refresh_buildup_session()
                    if error:
                        st.error(f"Failed to refresh buildup session: {error}")
                    else:
                        success_placeholder = st.empty()
                        success_placeholder.success("✅ Buildup OI API session refreshed successfully!")
                        time.sleep(2)
                        success_placeholder.empty()
                st.rerun()
    
    st.markdown("---")
    
    # Display the buildup data
    if st.session_state.buildup_data is not None:
        if not st.session_state.buildup_data.empty:
            # Show summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_intervals = len(st.session_state.buildup_data)
                st.metric("Total Intervals", total_intervals)
            
            with col2:
                if 'OI Change' in st.session_state.buildup_data.columns:
                    avg_oi_change = st.session_state.buildup_data['OI Change'].mean()
                    st.metric("Avg OI Change", f"{avg_oi_change:.0f}")
            
            with col3:
                if 'Volume' in st.session_state.buildup_data.columns:
                    total_volume = st.session_state.buildup_data['Volume'].sum()
                    st.metric("Total Volume", f"{total_volume:.0f}")
            
            with col4:
                if 'Price Change' in st.session_state.buildup_data.columns:
                    avg_price_change = st.session_state.buildup_data['Price Change'].mean()
                    st.metric("Avg Price Change", f"{avg_price_change:.2f}")
            
            st.markdown("### 📈 Detailed Buildup Data")
            
            # Display the full table with enhanced formatting
            st.dataframe(
                st.session_state.buildup_data,
                use_container_width=True,
                height=600
            )
            
            # Add charts if data is available
            if len(st.session_state.buildup_data) > 1:
                st.markdown("### 📊 Visual Analysis")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'OI Change' in st.session_state.buildup_data.columns:
                        fig_oi = px.line(
                            st.session_state.buildup_data,
                            x='Interval',
                            y='OI Change',
                            title="OI Change Over Time",
                            markers=True
                        )
                        fig_oi.update_layout(height=400)
                        st.plotly_chart(fig_oi, use_container_width=True)
                
                with col2:
                    if 'Volume' in st.session_state.buildup_data.columns:
                        fig_vol = px.bar(
                            st.session_state.buildup_data,
                            x='Interval',
                            y='Volume',
                            title="Volume Distribution"
                        )
                        fig_vol.update_layout(height=400)
                        st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.warning(f"No buildup data available for {st.session_state.selected_stock_symbol}")
            st.info("This could be due to:")
            st.markdown("""
            - No trading activity in the recent intervals
            - Data not available for this symbol
            - API limitations or temporary issues
            """)
    else:
        st.info(f"Loading buildup data for {st.session_state.selected_stock_symbol}...")
        # Auto-fetch data if not available
        secid = get_secid_for_symbol(st.session_state.selected_stock_symbol, st.session_state.futstk_mapping)
        if secid:
            with st.spinner(f"Fetching buildup data for {st.session_state.selected_stock_symbol}..."):
                buildup_raw, error = fetch_buildup_data(secid)
                if error:
                    st.error(f"Error fetching buildup data: {error}")
                    st.session_state.buildup_data = None
                else:
                    formatted_data = format_buildup_data(buildup_raw)
                    st.session_state.buildup_data = formatted_data
            st.rerun()
        else:
            st.error(f"No secid found for {st.session_state.selected_stock_symbol} in July 2025 futures")

def main():
    # Check if we should show stock detail page
    if st.session_state.show_stock_detail_page and st.session_state.selected_stock_symbol:
        show_stock_detail_page()
        return
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
        elif st.session_state.selected_section == "OI Trend":
            st.markdown("### 📊 OI TREND - Live NSE OI Spurts (avgInOI > 2%)")
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
                elif st.session_state.selected_section == "OI Trend":
                    st.session_state.oi_trend_data_history = []
                else:
                    st.session_state.shortlisted_data_history = []
                st.rerun()
        
        with col_session:
            if st.button("🔄 Session", help="Refresh NSE session", use_container_width=True):
                refresh_nse_session()
                success_placeholder = st.empty()
                success_placeholder.success("Session refreshed!")
                time.sleep(1)
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
    
    if st.sidebar.button("📊 OI TREND", use_container_width=True, type="primary" if st.session_state.selected_section == "OI Trend" else "secondary"):
        st.session_state.selected_section = "OI Trend"
        st.rerun()
    
    if st.sidebar.button("⭐ SHORTLISTED STOCKS", use_container_width=True, type="primary" if st.session_state.selected_section == "Shortlisted Stocks" else "secondary"):
        st.session_state.selected_section = "Shortlisted Stocks"
        st.rerun()
    
    if st.sidebar.button("🎯 SHORTLISTED STOCKS BASED ON OI", use_container_width=True, type="primary" if st.session_state.selected_section == "OI Based Shortlist" else "secondary"):
        st.session_state.selected_section = "OI Based Shortlist"
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
                    time.sleep(1)
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
        
        elif st.session_state.selected_section == "OI Trend":
            # Fetch OI Trend data (NSE OI Spurts with avgInOI > 2%)
            with status_placeholder:
                with st.spinner("Fetching OI Trend data..."):
                    data, error = fetch_nse_data()
            
            if error:
                st.error(f"Error fetching OI trend data: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.oi_trend_last_update = datetime.now()
                
                # Process and display data
                if data:
                    df = process_oi_trend_data(data)
                    
                    if not df.empty:
                        # Store in history
                        st.session_state.oi_trend_data_history.append({
                            'timestamp': datetime.now(),
                            'data': df
                        })
                        
                        # Keep only last 10 data points
                        if len(st.session_state.oi_trend_data_history) > 10:
                            st.session_state.oi_trend_data_history = st.session_state.oi_trend_data_history[-10:]
                        
                        # Update title row metrics
                        if len(df) > 0:
                            symbols_placeholder.metric("OI Trend Stocks", len(df))
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
                                        st.metric("Avg OI Change", f"{avg_oi_change:.1f}")
                                
                                with col2:
                                    if 'pctChngInOI' in df.columns:
                                        max_pct_change = df['pctChngInOI'].max() if pd.api.types.is_numeric_dtype(df['pctChngInOI']) else 0
                                        st.metric("Max % Change", f"{max_pct_change:.1f}%")
                            
                            # Main data table - larger and more prominent with click functionality
                            st.subheader("📊 OI Trend Stocks (Click on a stock to see buildup data)")
                            
                            # Create clickable buttons for each stock
                            if 'symbol' in df.columns:
                                # Display stocks in a grid format
                                cols_per_row = 4
                                for i in range(0, len(df), cols_per_row):
                                    cols = st.columns(cols_per_row)
                                    for j, col in enumerate(cols):
                                        if i + j < len(df):
                                            stock_row = df.iloc[i + j]
                                            symbol = stock_row['symbol']
                                            avg_oi = stock_row.get('avgInOI', 0)
                                            
                                            with col:
                                                if st.button(
                                                    f"**{symbol}**\n{avg_oi:.1f}% OI",
                                                    key=f"stock_{symbol}_{i}_{j}",
                                                    use_container_width=True,
                                                    type="primary" if st.session_state.selected_stock_symbol == symbol else "secondary"
                                                ):
                                                    st.session_state.selected_stock_symbol = symbol
                                                    st.session_state.show_stock_detail_page = True
                                                    st.session_state.buildup_data = None  # Reset data to force fresh fetch
                                                    st.rerun()
                            
                            # Display full dataframe below the buttons
                            st.dataframe(
                                df,
                                use_container_width=True,
                                height=400
                            )
                            
                            # Show instruction for users
                            st.info("💡 Click on any stock button above to view detailed buildup data on a separate page.")
                        
                        # Charts section removed
                    
                    else:
                        st.info("No stocks found with avgInOI > 2% in current data")
                else:
                    st.warning("No OI trend data available in the response")
        
        elif st.session_state.selected_section == "Shortlisted Stocks":
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
        
        else:  # OI Based Shortlist section
            # Fetch OI Based Shortlisted Stocks data
            with status_placeholder:
                with st.spinner("Processing OI Based Shortlisted Stocks data..."):
                    df, error = process_oi_based_shortlisted_stocks()
            
            if error:
                st.error(f"Error processing OI based shortlisted stocks: {error}")
                status_placeholder.error("❌ Failed")
            else:
                status_placeholder.success("✅ Success")
                st.session_state.oi_based_shortlist_last_update = datetime.now()
                
                # Process and display data
                if not df.empty:
                    # Store in history
                    st.session_state.oi_based_shortlist_data_history.append({
                        'timestamp': datetime.now(),
                        'data': df
                    })
                    
                    # Keep only last 10 data points
                    if len(st.session_state.oi_based_shortlist_data_history) > 10:
                        st.session_state.oi_based_shortlist_data_history = st.session_state.oi_based_shortlist_data_history[-10:]
                    
                    # Update title row metrics
                    symbols_placeholder.metric("OI Shortlisted", len(df))
                    current_time = datetime.now().strftime("%H:%M:%S")
                    updated_placeholder.metric("Updated", current_time)
                    
                    # Display current data with enhanced metrics
                    with data_placeholder.container():
                        # Enhanced metrics for OI-based shortlisted stocks
                        if len(df) > 0:
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                long_buildup_count = len(df[df['Buildup Pattern'] == 'Long Buildup'])
                                st.metric("Long Buildup", long_buildup_count)
                            
                            with col2:
                                short_buildup_count = len(df[df['Buildup Pattern'] == 'Short Buildup'])
                                st.metric("Short Buildup", short_buildup_count)
                            
                            with col3:
                                long_unwinding_count = len(df[df['Buildup Pattern'] == 'Long Unwinding'])
                                st.metric("Long Unwinding", long_unwinding_count)
                            
                            with col4:
                                short_covering_count = len(df[df['Buildup Pattern'] == 'Short Covering'])
                                st.metric("Short Covering", short_covering_count)
                        
                        # Main data table
                        st.dataframe(
                            df,
                            use_container_width=True,
                            height=700
                        )
                
                else:
                    st.info("No stocks found with matching buildup patterns between 9:15-9:30 and 9:30-9:45 intervals")
        
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
        
        elif st.session_state.selected_section == "OI Trend":
            if st.session_state.oi_trend_data_history:
                latest_data = st.session_state.oi_trend_data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("OI Trend Stocks", len(latest_data))
                    if st.session_state.oi_trend_last_update:
                        updated_time = st.session_state.oi_trend_last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                with data_placeholder.container():
                    # Display metrics
                    if len(latest_data) > 0:
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            if 'avgInOI' in latest_data.columns:
                                avg_oi = latest_data['avgInOI'].mean() if pd.api.types.is_numeric_dtype(latest_data['avgInOI']) else 0
                                st.metric("Avg OI", f"{avg_oi:.1f}%")
                        
                        with col2:
                            if 'chngInOI' in latest_data.columns:
                                avg_oi_change = latest_data['chngInOI'].mean() if pd.api.types.is_numeric_dtype(latest_data['chngInOI']) else 0
                                st.metric("Avg OI Change", f"{avg_oi_change:.1f}")
                        
                        with col3:
                            if 'pctChngInOI' in latest_data.columns:
                                max_pct_change = latest_data['pctChngInOI'].max() if pd.api.types.is_numeric_dtype(latest_data['pctChngInOI']) else 0
                                st.metric("Max % Change", f"{max_pct_change:.1f}%")
                    
                    # Display clickable stock buttons
                    st.subheader("📊 OI Trend Stocks (Click on a stock to see buildup data)")
                    
                    # Create clickable buttons for each stock
                    if 'symbol' in latest_data.columns:
                        # Display stocks in a grid format
                        cols_per_row = 4
                        for i in range(0, len(latest_data), cols_per_row):
                            cols = st.columns(cols_per_row)
                            for j, col in enumerate(cols):
                                if i + j < len(latest_data):
                                    stock_row = latest_data.iloc[i + j]
                                    symbol = stock_row['symbol']
                                    avg_oi = stock_row.get('avgInOI', 0)
                                    
                                    with col:
                                        if st.button(
                                            f"**{symbol}**\n{avg_oi:.1f}% OI",
                                            key=f"stock_static_{symbol}_{i}_{j}",
                                            use_container_width=True,
                                            type="primary" if st.session_state.selected_stock_symbol == symbol else "secondary"
                                        ):
                                            st.session_state.selected_stock_symbol = symbol
                                            # Fetch buildup data
                                            secid = get_secid_for_symbol(symbol, st.session_state.futstk_mapping)
                                            if secid:
                                                with st.spinner(f"Fetching buildup data for {symbol}..."):
                                                    # Debug info for RBLBANK
                                                    if symbol == "RBLBANK":
                                                        st.write(f"🔍 Debug (Static): Calling fetch_buildup_data with secid: {secid}")
                                                    
                                                    buildup_raw, error = fetch_buildup_data(secid)
                                                    
                                                    # Debug the API response for RBLBANK
                                                    if symbol == "RBLBANK":
                                                        st.write(f"📊 Debug (Static): API Response - Raw data length: {len(buildup_raw) if buildup_raw else 0}")
                                                        st.write(f"❌ Debug (Static): Error message: {error if error else 'None'}")
                                                        if buildup_raw:
                                                            st.write(f"📋 Debug (Static): First few raw data items: {buildup_raw[:2] if len(buildup_raw) > 0 else 'Empty list'}")
                                                    
                                                    if error:
                                                        st.error(f"Error fetching buildup data: {error}")
                                                        st.session_state.buildup_data = None
                                                    else:
                                                        formatted_data = format_buildup_data(buildup_raw)
                                                        if symbol == "RBLBANK":
                                                            st.write(f"📈 Debug (Static): Formatted data shape: {formatted_data.shape if not formatted_data.empty else 'Empty DataFrame'}")
                                                        st.session_state.buildup_data = formatted_data
                                            else:
                                                st.error(f"No secid found for {symbol} in July 2025 futures")
                                                st.session_state.buildup_data = None
                                            st.rerun()
                    
                    # Display data table
                    st.dataframe(
                        latest_data,
                        use_container_width=True,
                        height=400
                    )
                    
                    # Display buildup data if a stock is selected
                    if st.session_state.selected_stock_symbol and st.session_state.buildup_data is not None:
                        st.subheader(f"📈 Buildup Data for {st.session_state.selected_stock_symbol}")
                        
                        # Clear button
                        if st.button("🔄 Clear Selection", key="clear_static", type="secondary"):
                            st.session_state.selected_stock_symbol = None
                            st.session_state.buildup_data = None
                            st.rerun()
                        
                        if not st.session_state.buildup_data.empty:
                            st.dataframe(
                                st.session_state.buildup_data,
                                use_container_width=True,
                                height=300
                            )
                        else:
                            st.info(f"No buildup data available for {st.session_state.selected_stock_symbol}")
                    elif st.session_state.selected_stock_symbol:
                        st.info(f"Loading buildup data for {st.session_state.selected_stock_symbol}...")
                
                # Charts section removed
            else:
                data_placeholder.info("No OI Trend data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")
        
        elif st.session_state.selected_section == "Shortlisted Stocks":
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
        
        elif st.session_state.selected_section == "OI Based Shortlist":
            if st.session_state.oi_based_shortlist_data_history:
                latest_data = st.session_state.oi_based_shortlist_data_history[-1]['data']
                
                # Update title row metrics
                if len(latest_data) > 0:
                    symbols_placeholder.metric("OI Shortlisted", len(latest_data))
                    if st.session_state.oi_based_shortlist_last_update:
                        updated_time = st.session_state.oi_based_shortlist_last_update.strftime("%H:%M:%S")
                        updated_placeholder.metric("Updated", updated_time)
                
                # Display current data
                with data_placeholder.container():
                    # Display metrics for OI-based shortlisted stocks
                    if len(latest_data) > 0:
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            long_buildup_count = len(latest_data[latest_data['Buildup Pattern'] == 'Long Buildup'])
                            st.metric("Long Buildup", long_buildup_count)
                        
                        with col2:
                            short_buildup_count = len(latest_data[latest_data['Buildup Pattern'] == 'Short Buildup'])
                            st.metric("Short Buildup", short_buildup_count)
                        
                        with col3:
                            long_unwinding_count = len(latest_data[latest_data['Buildup Pattern'] == 'Long Unwinding'])
                            st.metric("Long Unwinding", long_unwinding_count)
                        
                        with col4:
                            short_covering_count = len(latest_data[latest_data['Buildup Pattern'] == 'Short Covering'])
                            st.metric("Short Covering", short_covering_count)
                    
                    # Main data table
                    st.dataframe(
                        latest_data,
                        use_container_width=True,
                        height=700
                    )
            else:
                data_placeholder.info("No OI Based Shortlisted data available. Enable auto-refresh or click 'Refresh Now' to fetch data.")

if __name__ == "__main__":
    main()