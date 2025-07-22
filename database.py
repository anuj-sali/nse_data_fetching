import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
import threading
from contextlib import contextmanager
from typing import List, Dict, Optional, Tuple
import os

class FinancialDataDatabase:
    """Database manager for Financial Market data with snapshot-based storage"""
    
    def __init__(self, db_path: str = "oi_spurts_data.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_database()
    
    def _migrate_database(self):
        """Migrate existing database to new schema if needed"""
        try:
            with self.get_connection() as conn:
                # Check if old columns exist
                cursor = conn.execute("PRAGMA table_info(oi_snapshot_details)")
                columns = [row[1] for row in cursor.fetchall()]
                
                # If old column names exist, migrate to new schema
                if 'chngInOI' in columns:
                    print("Migrating database schema to new column names...")
                    
                    # Create new table with correct schema
                    conn.execute("""
                        CREATE TABLE oi_snapshot_details_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            snapshot_id INTEGER NOT NULL,
                            symbol TEXT NOT NULL,
                            avgInOI REAL,
                            changeInOI REAL,
                            pctChngInOI REAL,
                            total_value REAL,
                            underlying_value REAL,
                            FOREIGN KEY (snapshot_id) REFERENCES oi_snapshots(snapshot_id)
                        )
                    """)
                    
                    # Copy data from old table to new table
                    conn.execute("""
                        INSERT INTO oi_snapshot_details_new 
                        (id, snapshot_id, symbol, avgInOI, changeInOI, pctChngInOI, total_value, underlying_value)
                        SELECT id, snapshot_id, symbol, avgInOI, chngInOI, pctChngInOI, totalTurnover, underlying
                        FROM oi_snapshot_details
                    """)
                    
                    # Drop old table and rename new table
                    conn.execute("DROP TABLE oi_snapshot_details")
                    conn.execute("ALTER TABLE oi_snapshot_details_new RENAME TO oi_snapshot_details")
                    
                    conn.commit()
                    print("Database migration completed successfully!")
                    
        except Exception as e:
            print(f"Database migration error (this is normal for new databases): {e}")

    def _init_database(self):
        """Initialize database with required tables and indexes"""
        with self.get_connection() as conn:
            # Create snapshots metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS oi_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_time DATETIME NOT NULL,
                    total_stocks INTEGER NOT NULL,
                    api_fetch_time DATETIME NOT NULL,
                    processing_time_ms INTEGER,
                    raw_data_json TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create individual stock data table for OI Spurts
            conn.execute("""
                CREATE TABLE IF NOT EXISTS oi_snapshot_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    avgInOI REAL,
                    changeInOI REAL,
                    pctChngInOI REAL,
                    total_value REAL,
                    underlying_value REAL,
                    FOREIGN KEY (snapshot_id) REFERENCES oi_snapshots(snapshot_id)
                )
            """)
            
            # Create Daily Gainers snapshots table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_gainers_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_time DATETIME NOT NULL,
                    total_stocks INTEGER NOT NULL,
                    api_fetch_time DATETIME NOT NULL,
                    processing_time_ms INTEGER,
                    raw_data_json TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create Daily Gainers stock details table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_gainers_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    change_value REAL,
                    change_percent REAL,
                    volume INTEGER,
                    turnover REAL,
                    FOREIGN KEY (snapshot_id) REFERENCES daily_gainers_snapshots(snapshot_id)
                )
            """)
            
            # Create Daily Losers snapshots table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_losers_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_time DATETIME NOT NULL,
                    total_stocks INTEGER NOT NULL,
                    api_fetch_time DATETIME NOT NULL,
                    processing_time_ms INTEGER,
                    raw_data_json TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create Daily Losers stock details table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_losers_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    change_value REAL,
                    change_percent REAL,
                    volume INTEGER,
                    turnover REAL,
                    FOREIGN KEY (snapshot_id) REFERENCES daily_losers_snapshots(snapshot_id)
                )
            """)
            
            # Create indexes for performance - OI Spurts
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_oi_snapshot_time 
                ON oi_snapshots(snapshot_time)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_oi_symbol_snapshot 
                ON oi_snapshot_details(symbol, snapshot_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_oi_snapshot_details_id 
                ON oi_snapshot_details(snapshot_id)
            """)
            
            # Create indexes for Daily Gainers
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gainers_snapshot_time 
                ON daily_gainers_snapshots(snapshot_time)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gainers_symbol_snapshot 
                ON daily_gainers_details(symbol, snapshot_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gainers_details_id 
                ON daily_gainers_details(snapshot_id)
            """)
            
            # Create indexes for Daily Losers
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_losers_snapshot_time 
                ON daily_losers_snapshots(snapshot_time)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_losers_symbol_snapshot 
                ON daily_losers_details(symbol, snapshot_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_losers_details_id 
                ON daily_losers_details(snapshot_id)
            """)
            
            conn.commit()
        
        # Run migration after initial table creation
        self._migrate_database()
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance safety and performance
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    def store_snapshot(self, data: Dict, fetch_time: datetime, processing_time_ms: int = 0) -> Optional[int]:
        """
        Store a complete OI spurts snapshot
        
        Args:
            data: Raw API response data
            fetch_time: When the API was called
            processing_time_ms: Time taken to process the data
            
        Returns:
            snapshot_id if successful, None if failed
        """
        try:
            with self.lock:
                with self.get_connection() as conn:
                    # Extract data from API response
                    stocks_data = data.get('data', []) if data else []
                    total_stocks = len(stocks_data)
                    snapshot_time = fetch_time
                    
                    # Insert snapshot metadata
                    cursor = conn.execute("""
                        INSERT INTO oi_snapshots 
                        (snapshot_time, total_stocks, api_fetch_time, processing_time_ms, raw_data_json)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        snapshot_time,
                        total_stocks,
                        fetch_time,
                        processing_time_ms,
                        json.dumps(data) if data else None
                    ))
                    
                    snapshot_id = cursor.lastrowid
                    
                    # Insert individual stock details
                    if stocks_data:
                        stock_records = []
                        for stock in stocks_data:
                            # Calculate percentage change in OI
                            latest_oi = stock.get('latestOI', 0)
                            prev_oi = stock.get('prevOI', 0)
                            pct_change_oi = ((latest_oi - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0
                            
                            stock_records.append((
                                snapshot_id,
                                stock.get('symbol', ''),
                                stock.get('avgInOI'),
                                stock.get('changeInOI'),  # Fixed: was 'chngInOI'
                                pct_change_oi,  # Calculated percentage change
                                stock.get('total'),  # Fixed: was 'totalTurnover'
                                stock.get('underlyingValue')  # Fixed: was 'underlying'
                            ))
                        
                        conn.executemany("""
                            INSERT INTO oi_snapshot_details 
                            (snapshot_id, symbol, avgInOI, changeInOI, pctChngInOI, total_value, underlying_value)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, stock_records)
                    
                    conn.commit()
                    return snapshot_id
                    
        except Exception as e:
            print(f"Error storing snapshot: {e}")
            return None
    
    def store_daily_gainers_snapshot(self, data: Dict, fetch_time: datetime, processing_time_ms: int = 0) -> Optional[int]:
        """
        Store a complete Daily Gainers snapshot
        
        Args:
            data: Raw API response data from Daily Gainers
            fetch_time: When the API was called
            processing_time_ms: Time taken to process the data
            
        Returns:
            snapshot_id if successful, None if failed
        """
        try:
            with self.lock:
                with self.get_connection() as conn:
                    # Extract data from API response
                    stocks_data = data.get('data', []) if data else []
                    total_stocks = len(stocks_data)
                    snapshot_time = fetch_time
                    
                    # Insert snapshot metadata
                    cursor = conn.execute("""
                        INSERT INTO daily_gainers_snapshots 
                        (snapshot_time, total_stocks, api_fetch_time, processing_time_ms, raw_data_json)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        snapshot_time,
                        total_stocks,
                        fetch_time,
                        processing_time_ms,
                        json.dumps(data) if data else None
                    ))
                    
                    snapshot_id = cursor.lastrowid
                    
                    # Insert individual stock details
                    if stocks_data:
                        stock_records = []
                        for stock in stocks_data:
                            stock_records.append((
                                snapshot_id,
                                stock.get('sym', ''),  # symbol
                                stock.get('disp', ''),  # company name
                                stock.get('ltp'),  # last traded price
                                stock.get('chng'),  # change value
                                stock.get('pchng'),  # percentage change
                                stock.get('tvol'),  # volume
                                stock.get('tval')  # turnover
                            ))
                        
                        conn.executemany("""
                            INSERT INTO daily_gainers_details 
                            (snapshot_id, symbol, company_name, ltp, change_value, change_percent, volume, turnover)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, stock_records)
                    
                    conn.commit()
                    return snapshot_id
                    
        except Exception as e:
            print(f"Error storing daily gainers snapshot: {e}")
            return None
    
    def store_daily_losers_snapshot(self, data: Dict, fetch_time: datetime, processing_time_ms: int = 0) -> Optional[int]:
        """
        Store a complete Daily Losers snapshot
        
        Args:
            data: Raw API response data from Daily Losers
            fetch_time: When the API was called
            processing_time_ms: Time taken to process the data
            
        Returns:
            snapshot_id if successful, None if failed
        """
        try:
            with self.lock:
                with self.get_connection() as conn:
                    # Extract data from API response
                    stocks_data = data.get('data', []) if data else []
                    total_stocks = len(stocks_data)
                    snapshot_time = fetch_time
                    
                    # Insert snapshot metadata
                    cursor = conn.execute("""
                        INSERT INTO daily_losers_snapshots 
                        (snapshot_time, total_stocks, api_fetch_time, processing_time_ms, raw_data_json)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        snapshot_time,
                        total_stocks,
                        fetch_time,
                        processing_time_ms,
                        json.dumps(data) if data else None
                    ))
                    
                    snapshot_id = cursor.lastrowid
                    
                    # Insert individual stock details
                    if stocks_data:
                        stock_records = []
                        for stock in stocks_data:
                            stock_records.append((
                                snapshot_id,
                                stock.get('sym', ''),  # symbol
                                stock.get('disp', ''),  # company name
                                stock.get('ltp'),  # last traded price
                                stock.get('chng'),  # change value
                                stock.get('pchng'),  # percentage change
                                stock.get('tvol'),  # volume
                                stock.get('tval')  # turnover
                            ))
                        
                        conn.executemany("""
                            INSERT INTO daily_losers_details 
                            (snapshot_id, symbol, company_name, ltp, change_value, change_percent, volume, turnover)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, stock_records)
                    
                    conn.commit()
                    return snapshot_id
                    
        except Exception as e:
            print(f"Error storing daily losers snapshot: {e}")
            return None
    
    def get_snapshot_at_time(self, target_time: datetime) -> Optional[Dict]:
        """
        Get the snapshot closest to the target time
        
        Args:
            target_time: The time to search for
            
        Returns:
            Dictionary with snapshot data or None
        """
        try:
            with self.get_connection() as conn:
                # Find closest snapshot
                cursor = conn.execute("""
                    SELECT snapshot_id, snapshot_time, total_stocks, api_fetch_time
                    FROM oi_snapshots 
                    WHERE ABS(julianday(snapshot_time) - julianday(?)) = (
                        SELECT MIN(ABS(julianday(snapshot_time) - julianday(?)))
                        FROM oi_snapshots
                    )
                    LIMIT 1
                """, (target_time, target_time))
                
                snapshot_row = cursor.fetchone()
                if not snapshot_row:
                    return None
                
                snapshot_id, snapshot_time, total_stocks, api_fetch_time = snapshot_row
                
                # Get stock details for this snapshot
                cursor = conn.execute("""
                    SELECT symbol, avgInOI, changeInOI, pctChngInOI, total_value, underlying_value
                    FROM oi_snapshot_details 
                    WHERE snapshot_id = ?
                    ORDER BY symbol
                """, (snapshot_id,))
                
                stocks = []
                for row in cursor.fetchall():
                    stocks.append({
                        'symbol': row[0],
                        'avgInOI': row[1],
                        'changeInOI': row[2],
                        'pctChngInOI': row[3],
                        'total_value': row[4],
                        'underlying_value': row[5]
                    })
                
                return {
                    'snapshot_id': snapshot_id,
                    'snapshot_time': snapshot_time,
                    'total_stocks': total_stocks,
                    'api_fetch_time': api_fetch_time,
                    'stocks': stocks
                }
                
        except Exception as e:
            print(f"Error getting snapshot at time: {e}")
            return None
    
    def get_daily_gainers_snapshot_at_time(self, target_time: datetime) -> Optional[Dict]:
        """Get Daily Gainers snapshot closest to the target time"""
        try:
            with self.get_connection() as conn:
                # Find closest snapshot
                cursor = conn.execute("""
                    SELECT snapshot_id, snapshot_time, total_stocks, api_fetch_time
                    FROM daily_gainers_snapshots 
                    WHERE ABS(julianday(snapshot_time) - julianday(?)) = (
                        SELECT MIN(ABS(julianday(snapshot_time) - julianday(?)))
                        FROM daily_gainers_snapshots
                    )
                    LIMIT 1
                """, (target_time, target_time))
                
                snapshot_row = cursor.fetchone()
                if not snapshot_row:
                    return None
                
                snapshot_id, snapshot_time, total_stocks, api_fetch_time = snapshot_row
                
                # Get stock details for this snapshot
                cursor = conn.execute("""
                    SELECT symbol, company_name, ltp, change_value, change_percent, volume, turnover
                    FROM daily_gainers_details 
                    WHERE snapshot_id = ?
                    ORDER BY change_percent DESC
                """, (snapshot_id,))
                
                stocks = []
                for row in cursor.fetchall():
                    stocks.append({
                        'symbol': row[0],
                        'company_name': row[1],
                        'ltp': row[2],
                        'change_value': row[3],
                        'change_percent': row[4],
                        'volume': row[5],
                        'turnover': row[6]
                    })
                
                return {
                    'snapshot_id': snapshot_id,
                    'snapshot_time': snapshot_time,
                    'total_stocks': total_stocks,
                    'api_fetch_time': api_fetch_time,
                    'stocks': stocks
                }
                
        except Exception as e:
            print(f"Error getting daily gainers snapshot at time: {e}")
            return None
    
    def get_daily_losers_snapshot_at_time(self, target_time: datetime) -> Optional[Dict]:
        """Get Daily Losers snapshot closest to the target time"""
        try:
            with self.get_connection() as conn:
                # Find closest snapshot
                cursor = conn.execute("""
                    SELECT snapshot_id, snapshot_time, total_stocks, api_fetch_time
                    FROM daily_losers_snapshots 
                    WHERE ABS(julianday(snapshot_time) - julianday(?)) = (
                        SELECT MIN(ABS(julianday(snapshot_time) - julianday(?)))
                        FROM daily_losers_snapshots
                    )
                    LIMIT 1
                """, (target_time, target_time))
                
                snapshot_row = cursor.fetchone()
                if not snapshot_row:
                    return None
                
                snapshot_id, snapshot_time, total_stocks, api_fetch_time = snapshot_row
                
                # Get stock details for this snapshot
                cursor = conn.execute("""
                    SELECT symbol, company_name, ltp, change_value, change_percent, volume, turnover
                    FROM daily_losers_details 
                    WHERE snapshot_id = ?
                    ORDER BY change_percent ASC
                """, (snapshot_id,))
                
                stocks = []
                for row in cursor.fetchall():
                    stocks.append({
                        'symbol': row[0],
                        'company_name': row[1],
                        'ltp': row[2],
                        'change_value': row[3],
                        'change_percent': row[4],
                        'volume': row[5],
                        'turnover': row[6]
                    })
                
                return {
                    'snapshot_id': snapshot_id,
                    'snapshot_time': snapshot_time,
                    'total_stocks': total_stocks,
                    'api_fetch_time': api_fetch_time,
                    'stocks': stocks
                }
                
        except Exception as e:
            print(f"Error getting daily losers snapshot at time: {e}")
            return None
    
    def get_intraday_timeline(self, date: datetime) -> List[Dict]:
        """
        Get timeline of stock counts throughout a trading day
        
        Args:
            date: The date to get timeline for
            
        Returns:
            List of timeline points with time and stock count
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT snapshot_time, total_stocks, snapshot_id
                    FROM oi_snapshots 
                    WHERE DATE(snapshot_time) = DATE(?)
                    ORDER BY snapshot_time
                """, (date,))
                
                timeline = []
                for row in cursor.fetchall():
                    timeline.append({
                        'time': row[0],
                        'total_stocks': row[1],
                        'snapshot_id': row[2]
                    })
                
                return timeline
                
        except Exception as e:
            print(f"Error getting intraday timeline: {e}")
            return []
    
    def get_daily_gainers_timeline(self, date: datetime) -> List[Dict]:
        """Get timeline of Daily Gainers stock counts throughout a trading day"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT snapshot_time, total_stocks, snapshot_id
                    FROM daily_gainers_snapshots 
                    WHERE DATE(snapshot_time) = DATE(?)
                    ORDER BY snapshot_time
                """, (date,))
                
                timeline = []
                for row in cursor.fetchall():
                    timeline.append({
                        'time': row[0],
                        'total_stocks': row[1],
                        'snapshot_id': row[2]
                    })
                
                return timeline
                
        except Exception as e:
            print(f"Error getting daily gainers timeline: {e}")
            return []
    
    def get_daily_losers_timeline(self, date: datetime) -> List[Dict]:
        """Get timeline of Daily Losers stock counts throughout a trading day"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT snapshot_time, total_stocks, snapshot_id
                    FROM daily_losers_snapshots 
                    WHERE DATE(snapshot_time) = DATE(?)
                    ORDER BY snapshot_time
                """, (date,))
                
                timeline = []
                for row in cursor.fetchall():
                    timeline.append({
                        'time': row[0],
                        'total_stocks': row[1],
                        'snapshot_id': row[2]
                    })
                
                return timeline
                
        except Exception as e:
            print(f"Error getting daily losers timeline: {e}")
            return []
    
    def get_peak_activity_times(self, date: datetime, limit: int = 10) -> List[Dict]:
        """
        Get times with highest stock counts for a given date
        
        Args:
            date: The date to analyze
            limit: Number of peak times to return
            
        Returns:
            List of peak activity times
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT snapshot_time, total_stocks, snapshot_id
                    FROM oi_snapshots 
                    WHERE DATE(snapshot_time) = DATE(?)
                    ORDER BY total_stocks DESC, snapshot_time
                    LIMIT ?
                """, (date, limit))
                
                peaks = []
                for row in cursor.fetchall():
                    peaks.append({
                        'time': row[0],
                        'total_stocks': row[1],
                        'snapshot_id': row[2]
                    })
                
                return peaks
                
        except Exception as e:
            print(f"Error getting peak activity times: {e}")
            return []
    
    def get_stocks_at_time_range(self, start_time: datetime, end_time: datetime) -> List[str]:
        """
        Get unique stocks that appeared in OI spurts during a time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of unique stock symbols
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT DISTINCT d.symbol
                    FROM oi_snapshot_details d
                    JOIN oi_snapshots s ON d.snapshot_id = s.snapshot_id
                    WHERE s.snapshot_time BETWEEN ? AND ?
                    ORDER BY d.symbol
                """, (start_time, end_time))
                
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            print(f"Error getting stocks in time range: {e}")
            return []
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """
        Clean up old data beyond specified days
        
        Args:
            days_to_keep: Number of days of data to retain
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            with self.lock:
                with self.get_connection() as conn:
                    # Delete old OI snapshot details first (foreign key constraint)
                    cursor = conn.execute("""
                        DELETE FROM oi_snapshot_details 
                        WHERE snapshot_id IN (
                            SELECT snapshot_id FROM oi_snapshots 
                            WHERE snapshot_time < ?
                        )
                    """, (cutoff_date,))
                    oi_details_deleted = cursor.rowcount
                    
                    # Delete old OI snapshots
                    cursor = conn.execute("""
                        DELETE FROM oi_snapshots 
                        WHERE snapshot_time < ?
                    """, (cutoff_date,))
                    oi_snapshots_deleted = cursor.rowcount
                    
                    # Delete old Daily Gainers details
                    cursor = conn.execute("""
                        DELETE FROM daily_gainers_details 
                        WHERE snapshot_id IN (
                            SELECT snapshot_id FROM daily_gainers_snapshots 
                            WHERE snapshot_time < ?
                        )
                    """, (cutoff_date,))
                    gainers_details_deleted = cursor.rowcount
                    
                    # Delete old Daily Gainers snapshots
                    cursor = conn.execute("""
                        DELETE FROM daily_gainers_snapshots 
                        WHERE snapshot_time < ?
                    """, (cutoff_date,))
                    gainers_snapshots_deleted = cursor.rowcount
                    
                    # Delete old Daily Losers details
                    cursor = conn.execute("""
                        DELETE FROM daily_losers_details 
                        WHERE snapshot_id IN (
                            SELECT snapshot_id FROM daily_losers_snapshots 
                            WHERE snapshot_time < ?
                        )
                    """, (cutoff_date,))
                    losers_details_deleted = cursor.rowcount
                    
                    # Delete old Daily Losers snapshots
                    cursor = conn.execute("""
                        DELETE FROM daily_losers_snapshots 
                        WHERE snapshot_time < ?
                    """, (cutoff_date,))
                    losers_snapshots_deleted = cursor.rowcount
                    
                    conn.commit()
                    
                    total_snapshots = oi_snapshots_deleted + gainers_snapshots_deleted + losers_snapshots_deleted
                    total_details = oi_details_deleted + gainers_details_deleted + losers_details_deleted
                    
                    print(f"Cleanup completed: {total_snapshots} snapshots and {total_details} detail records deleted")
                    print(f"  OI: {oi_snapshots_deleted} snapshots, {oi_details_deleted} details")
                    print(f"  Gainers: {gainers_snapshots_deleted} snapshots, {gainers_details_deleted} details")
                    print(f"  Losers: {losers_snapshots_deleted} snapshots, {losers_details_deleted} details")
                    
        except Exception as e:
            print(f"Error during cleanup: {e}")
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            with self.get_connection() as conn:
                # Get OI snapshots and records
                cursor = conn.execute("SELECT COUNT(*) FROM oi_snapshots")
                oi_snapshots = cursor.fetchone()[0]
                
                cursor = conn.execute("SELECT COUNT(*) FROM oi_snapshot_details")
                oi_records = cursor.fetchone()[0]
                
                # Get Daily Gainers snapshots and records
                cursor = conn.execute("SELECT COUNT(*) FROM daily_gainers_snapshots")
                gainers_snapshots = cursor.fetchone()[0]
                
                cursor = conn.execute("SELECT COUNT(*) FROM daily_gainers_details")
                gainers_records = cursor.fetchone()[0]
                
                # Get Daily Losers snapshots and records
                cursor = conn.execute("SELECT COUNT(*) FROM daily_losers_snapshots")
                losers_snapshots = cursor.fetchone()[0]
                
                cursor = conn.execute("SELECT COUNT(*) FROM daily_losers_details")
                losers_records = cursor.fetchone()[0]
                
                # Get overall date range (from all tables)
                cursor = conn.execute("""
                    SELECT MIN(earliest), MAX(latest) FROM (
                        SELECT MIN(snapshot_time) as earliest, MAX(snapshot_time) as latest FROM oi_snapshots
                        UNION ALL
                        SELECT MIN(snapshot_time) as earliest, MAX(snapshot_time) as latest FROM daily_gainers_snapshots
                        UNION ALL
                        SELECT MIN(snapshot_time) as earliest, MAX(snapshot_time) as latest FROM daily_losers_snapshots
                    )
                """)
                date_range = cursor.fetchone()
                
                # Get database file size
                db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
                
                return {
                    'total_snapshots': oi_snapshots + gainers_snapshots + losers_snapshots,
                    'total_stock_records': oi_records + gainers_records + losers_records,
                    'oi_snapshots': oi_snapshots,
                    'oi_records': oi_records,
                    'gainers_snapshots': gainers_snapshots,
                    'gainers_records': gainers_records,
                    'losers_snapshots': losers_snapshots,
                    'losers_records': losers_records,
                    'earliest_data': date_range[0],
                    'latest_data': date_range[1],
                    'database_size_mb': round(db_size_mb, 2)
                }
                
        except Exception as e:
            print(f"Error getting database stats: {e}")
            return {}

# Global database instance
financial_db = FinancialDataDatabase()