"""
Simple Market Data Backfill Script
For AWS Cloud Portfolio Project
Enhanced with pandas for data validation and processing
"""
import os
import time
import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
FMP_API_KEY = os.getenv('FMP_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# List of tickers to process (50 major stocks)
TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'V', 'UNH',
    'JNJ', 'WMT', 'JPM', 'MA', 'PG', 'XOM', 'HD', 'CVX', 'MRK', 'ABBV',
    'KO', 'PEP', 'COST', 'AVGO', 'TMO', 'MCD', 'CSCO', 'ACN', 'ABT', 'CRM',
    'NFLX', 'AMD', 'NKE', 'DHR', 'TXN', 'ORCL', 'QCOM', 'DIS', 'PM', 'VZ',
    'INTC', 'UPS', 'HON', 'NEE', 'CMCSA', 'AMGN', 'T', 'IBM', 'BA', 'GE'
]

def get_db_connection():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info("✅ Database connection established")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def create_table(conn):
    """Create market_data table if it doesn't exist"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS market_data (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        date DATE NOT NULL,
        open_price NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        close_price NUMERIC,
        volume BIGINT,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(ticker, timestamp)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        logger.info("✅ Table created/verified")
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        raise

def get_last_date_for_ticker(conn, ticker):
    """Get the most recent date we have data for this ticker"""
    query = """
    SELECT MAX(date) FROM market_data WHERE ticker = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (ticker,))
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            return None
    except Exception as e:
        logger.warning(f"Could not fetch last date for {ticker}: {e}")
        return None

def fetch_intraday_data(ticker, from_date, to_date):
    """Fetch intraday data from Financial Modeling Prep API"""
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{ticker}"
    params = {
        'from': from_date,
        'to': to_date,
        'apikey': FMP_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"✅ Fetched {len(data)} rows for {ticker}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to fetch {ticker}: {e}")
        return []

def validate_and_clean_data(data, ticker):
    """
    Validate and clean market data using pandas
    - Remove duplicates
    - Handle missing values
    - Validate data types
    - Remove invalid records (negative prices, etc.)
    """
    if not data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        return df
    
    initial_count = len(df)
    
    # Add ticker column
    df['ticker'] = ticker
    
    # Parse timestamps
    df['timestamp'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['date_only'] = df['timestamp'].dt.date
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['timestamp'])
    
    # Validate numeric columns
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with missing price data
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    
    # Data quality checks
    df = df[df['close'] > 0]  # Remove negative/zero prices
    df = df[df['volume'] >= 0]  # Remove negative volumes
    df = df[df['high'] >= df['low']]  # High must be >= Low
    df = df[df['high'] >= df['close']]  # High must be >= Close
    df = df[df['low'] <= df['close']]  # Low must be <= Close
    
    # Remove duplicates based on ticker and timestamp
    df = df.drop_duplicates(subset=['ticker', 'timestamp'], keep='first')
    
    # Sort by timestamp
    df = df.sort_values('timestamp')
    
    cleaned_count = len(df)
    removed_count = initial_count - cleaned_count
    
    if removed_count > 0:
        logger.info(f"📊 Data validation for {ticker}: {removed_count} invalid rows removed, {cleaned_count} rows retained")
    
    return df

def insert_data_pandas(conn, df):
    """Insert market data into database using pandas-validated data"""
    if df.empty:
        return 0
    
    insert_sql = """
    INSERT INTO market_data (ticker, timestamp, date, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, timestamp) DO NOTHING
    """
    
    inserted = 0
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                try:
                    cur.execute(insert_sql, (
                        row['ticker'],
                        row['timestamp'],
                        row['date_only'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row['volume'])
                    ))
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Skipped row: {e}")
                    continue
        
        conn.commit()
        logger.info(f"✅ Inserted {inserted} rows for {df['ticker'].iloc[0]}")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Insert failed: {e}")
        return 0

def main():
    """Main backfill process"""
    logger.info("🚀 Starting market data backfill")
    
    # Connect to database
    conn = get_db_connection()
    create_table(conn)
    
    # End date is always today
    end_date = datetime.now().date()
    
    # Process each ticker
    total_inserted = 0
    total_validated = 0
    total_skipped = 0
    
    for i, ticker in enumerate(TICKERS, 1):
        # Check last date we have for this ticker
        last_date = get_last_date_for_ticker(conn, ticker)
        
        if last_date:
            # Start from day after last date
            start_date = last_date + timedelta(days=1)
            
            # Skip if we're already up to date
            if start_date > end_date:
                logger.info(f"[{i}/{len(TICKERS)}] {ticker} - Already up to date (last date: {last_date})")
                total_skipped += 1
                continue
                
            logger.info(f"[{i}/{len(TICKERS)}] Processing {ticker} from {start_date} to {end_date} (incremental)")
        else:
            # No data exists, backfill last 30 days
            start_date = end_date - timedelta(days=30)
            logger.info(f"[{i}/{len(TICKERS)}] Processing {ticker} from {start_date} to {end_date} (initial backfill)")
        
        # Fetch raw data
        raw_data = fetch_intraday_data(ticker, start_date.isoformat(), end_date.isoformat())
        
        # Validate and clean with pandas
        validated_df = validate_and_clean_data(raw_data, ticker)
        total_validated += len(validated_df)
        
        # Insert cleaned data
        inserted = insert_data_pandas(conn, validated_df)
        total_inserted += inserted
        
        # Rate limiting
        time.sleep(0.5)
    
    conn.close()
    logger.info(f"✅ Backfill complete!")
    logger.info(f"📊 Tickers already up to date: {total_skipped}")
    logger.info(f"📊 Total rows validated: {total_validated}")
    logger.info(f"📊 Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    main()
