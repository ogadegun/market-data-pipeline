"""
Simple Market Data Backfill Script
For AWS Cloud Portfolio Project
"""
import os
import time
import requests
import psycopg2
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

def insert_data(conn, ticker, data):
    """Insert market data into database"""
    if not data:
        return 0
    
    insert_sql = """
    INSERT INTO market_data (ticker, timestamp, date, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, timestamp) DO NOTHING
    """
    
    inserted = 0
    try:
        with conn.cursor() as cur:
            for row in data:
                try:
                    timestamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
                    date = timestamp.date()
                    
                    cur.execute(insert_sql, (
                        ticker,
                        timestamp,
                        date,
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row['volume'])
                    ))
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Skipped row for {ticker}: {e}")
                    continue
        
        conn.commit()
        logger.info(f"✅ Inserted {inserted} rows for {ticker}")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Insert failed for {ticker}: {e}")
        return 0

def main():
    """Main backfill process"""
    logger.info("🚀 Starting market data backfill")
    
    # Date range: last 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    logger.info(f"📊 Processing {len(TICKERS)} tickers")
    
    # Connect to database
    conn = get_db_connection()
    create_table(conn)
    
    # Process each ticker
    total_inserted = 0
    for i, ticker in enumerate(TICKERS, 1):
        logger.info(f"[{i}/{len(TICKERS)}] Processing {ticker}...")
        
        data = fetch_intraday_data(ticker, start_date.isoformat(), end_date.isoformat())
        inserted = insert_data(conn, ticker, data)
        total_inserted += inserted
        
        # Rate limiting
        time.sleep(0.5)
    
    conn.close()
    logger.info(f"✅ Backfill complete! Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    main()