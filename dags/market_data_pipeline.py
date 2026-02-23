from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'airflow')
DB_PORT = os.getenv('DB_PORT', '5432')

def create_table():
    """Create stock prices table with UPSERT support"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            date TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_ticker_date ON stock_prices(ticker, date);
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table created successfully with UNIQUE constraint")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise

def fetch_and_store_stock_data(ticker, **context):
    """Fetch YESTERDAY's stock data and store/upsert in PostgreSQL (NO DUPLICATES)"""
    try:
        # Calculate yesterday (or last business day for stocks)
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        
        print(f"Fetching {ticker} data for {start_date}")
        
        # Fetch only yesterday's data
        data = yf.download(tickers=ticker, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"No data found for {ticker} on {start_date}")
            return {"status": "no_data", "records": 0}
        
        data.reset_index(inplace=True)
        
        # Connect to database
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        # Prepare data for insertion
        values = []
        for _, row in data.iterrows():
            date_val = row['Date'] if 'Date' in data.columns else row.name
            values.append((
                ticker,
                date_val,
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                int(row['Volume'])
            ))
        
        # UPSERT: Insert or update if exists (no duplicates!)
        upsert_query = """
        INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (ticker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
        """
        
        execute_values(cursor, upsert_query, values)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print(f"Successfully upserted {len(values)} records for {ticker}")
        return {"status": "success", "records": len(values)}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "failed", "error": str(e)}

# DAG Definition
with DAG('market_momentum_extraction',
         start_date=datetime(2026, 1, 1),
         schedule='@daily',
         catchup=False,
         description='Pipeline to extract market data and store in PostgreSQL') as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    extract_task = PythonOperator(
        task_id='fetch_yfinance_data',
        python_callable=fetch_and_store_stock_data,
        op_kwargs={'ticker': 'AAPL'}
    )

    create_table_task >> extract_task
